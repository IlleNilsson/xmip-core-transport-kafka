#![forbid(unsafe_code)]

//! Streams that arrive as Kafka records. One record is one Stream, the
//! topic, partition and offset kept beside it.
//!
//! Kafka is the event backbone: partitioned logs on a cluster of brokers,
//! producers that append, consumers that read on from an offset they keep,
//! on port 9092. A Receive Location fetches its partition from its cursor
//! and hands each record's value up, the offset it read to being the cursor
//! for next time; a Send Location produces. Either may instead accept
//! clients directly through [`Session`], one broker's worth of protocol for
//! one connection over logs kept in memory.
//!
//! What is here is the protocol as it stands since message format 2:
//! Metadata v1 to find the leader, Produce v3 with acknowledgement from the
//! leader, Fetch v4, record batches with their CRC-32C. Consumer groups,
//! SASL, TLS, compression and idempotent producers are the next layers.
//!
//! The origin URI carries what the fetch knew: `kafka://broker/orders/0?offset=41`.

pub mod client;
pub mod records;
pub mod session;
pub mod wire;

use std::net::TcpListener;
use std::sync::Mutex;
use std::time::Duration;

pub use client::{Client, TopicMetadata};
pub use records::Record;
pub use session::{Event, Session};
use transport::error::Result;
use transport::socket;
use transport::{Arrived, Directions, Transport};

pub struct KafkaTransport {
    bootstrap: String,
    topic: String,
    partition: i32,
    client: String,
    cursor: Mutex<i64>,
    timeout: Option<Duration>,
}

impl KafkaTransport {
    /// Speak to the cluster at `bootstrap` about `topic`, partition 0,
    /// reading from its beginning.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            topic: topic.into(),
            partition: 0,
            client: "xmip".to_string(),
            cursor: Mutex::new(0),
            timeout: None,
        }
    }

    /// This partition rather than 0.
    #[must_use]
    pub const fn on_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Start reading from this offset rather than the beginning.
    #[must_use]
    pub fn from_offset(self, offset: i64) -> Self {
        *self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset;
        self
    }

    /// Give up on a broker that stops mid-message.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The offset the next receive reads from.
    #[must_use]
    pub fn cursor(&self) -> i64 {
        *self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Connect to the partition's leader, asking the bootstrap broker who
    /// that is.
    ///
    /// # Errors
    /// Where no broker could be reached or the topic has no leader.
    pub fn connect(&self) -> Result<Client> {
        let mut client = Client::connect(&self.bootstrap, &self.client, self.timeout)?;
        let metadata = client.metadata(&self.topic)?;
        if metadata.leader.is_empty() || metadata.leader == self.bootstrap {
            return Ok(client);
        }
        Client::connect(&metadata.leader, &self.client, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.bootstrap)
    }

    /// Accept one client on an already-bound listener.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.timeout)
    }

    /// Where a target names the broker and topic itself —
    /// `kafka://host:9092/orders` — or is a topic alone on this transport's
    /// cluster.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match socket::target("kafka", target) {
            Some((peer, "")) => (peer, &self.topic),
            Some(pair) => pair,
            None => (&self.bootstrap, target),
        }
    }
}

impl Transport for KafkaTransport {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// The records from the cursor on, the cursor moved past the last.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        let records = client.fetch(&self.topic, self.partition, self.cursor())?;
        let mut arrived = Vec::with_capacity(records.len());
        for record in records {
            arrived.push(Arrived::new(
                format!(
                    "kafka://{}/{}/{}?offset={}",
                    self.bootstrap, self.topic, self.partition, record.offset
                ),
                record.value.unwrap_or_default(),
            ));
            *self
                .cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = record.offset + 1;
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, topic) = self.resolve(target);
        let mut client = Client::connect(broker, &self.client, self.timeout)?;
        client
            .produce(topic, self.partition, None, bytes)
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_producer_appends_to_a_session_and_the_offsets_count() {
        let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let producer = std::thread::spawn(move || {
            let mut client = Client::connect(&address, "probe", Some(secs(2)))?;
            let metadata = client.metadata("orders")?;
            let first = client.produce("orders", 0, Some(b"k"), b"first")?;
            let second = client.produce("orders", 0, None, b"second\r\n\0")?;
            let fetched = client.fetch("orders", 0, 1)?;
            Ok::<_, transport::TransportError>((metadata, first, second, fetched))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        let one = session.next_produce().expect("first").expect("a record");
        assert_eq!(one.bytes, b"first");
        assert!(one.origin_uri.ends_with("/orders/0?offset=0"));
        let two = session.next_produce().expect("second").expect("a record");
        assert_eq!(two.bytes, b"second\r\n\0");
        assert!(two.origin_uri.ends_with("?offset=1"));
        assert!(session.next_produce().expect("closed").is_none());
        let (metadata, first, second, fetched) =
            producer.join().expect("thread").expect("producing");
        assert_eq!(metadata.partitions, [0]);
        assert!(metadata.leader.starts_with("127.0.0.1:"));
        assert_eq!((first, second), (0, 1));
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].offset, 1);
        assert_eq!(fetched[0].value.as_deref(), Some(&b"second\r\n\0"[..]));
    }

    #[test]
    fn the_transport_trait_sends_and_receives_on_from_its_cursor() {
        let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let near = KafkaTransport::new(address.clone(), "orders")
                .from_offset(1)
                .timing_out_after(secs(2));
            near.send(&format!("kafka://{address}/orders"), b"produced")?;
            let arrived = near.receive()?;
            Ok::<_, transport::TransportError>((arrived, near.cursor()))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(
            session.next_produce().expect("produce").expect("one").bytes,
            b"produced"
        );
        assert!(session.next_produce().expect("closed").is_none());
        let mut session = far_end
            .accept_one(&listener)
            .expect("second")
            .with_records("orders", &[b"zero", b"one", b"two"]);
        while session.next_event().expect("serving").is_some() {}
        let (arrived, cursor) = near.join().expect("thread").expect("round trip");
        assert_eq!(arrived.len(), 2, "from offset 1");
        assert_eq!(arrived[0].bytes, b"one");
        assert!(arrived[1].origin_uri.ends_with("/orders/0?offset=2"));
        assert_eq!(cursor, 3);
        assert!(far_end.claims().is_none());
    }
}
