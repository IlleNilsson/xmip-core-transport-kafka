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
use transport::error::{Result, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
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

impl KafkaTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one topic called `probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

/// A bound listener waiting for its one producer and its one record. It
/// holds the timeout rather than the transport: the transport carries a
/// cursor under a lock, and a far end has no offset to keep.
struct Listening {
    timeout: Option<Duration>,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Listening {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(self: Box<Self>) -> Result<Arrived> {
        let mut session = Session::accept(&self.listener, self.timeout)?;
        session
            .next_produce()?
            .ok_or_else(|| protocol_error("the client closed without producing"))
    }
}

impl Loopback for KafkaTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = self.bind()?;
        Ok(Box::new(Listening {
            timeout: self.timeout,
            listener,
            address,
        }))
    }

    /// A fresh producer to `address`, one record on this transport's topic,
    /// acknowledged by the leader before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let mut near = Self::new(address, &self.topic).on_partition(self.partition);
        near.timeout = self.timeout;
        near.send(&self.topic, payload)
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

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = KafkaTransport::loopback();
        let arrived = loopback.round(b"record").expect("round");
        assert_eq!(arrived.bytes, b"record");
        assert!(arrived.origin_uri.starts_with("kafka://127.0.0.1:"));
        assert!(arrived.origin_uri.ends_with("/probe/0?offset=0"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = KafkaTransport::loopback();
        for (name, payload) in edge_payloads() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it: the shapes a framing fault changes.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("mtu minus one", patterned(1_471)),
            ("mtu", patterned(1_472)),
            ("mtu plus one", patterned(1_473)),
            ("udp maximum", patterned(65_507)),
            ("sixteen bits plus one", patterned(65_537)),
            ("a mebibyte", patterned(1 << 20)),
        ]
    }

    /// `len` bytes a truncation, a reorder or a duplicate would change.
    fn patterned(len: usize) -> Vec<u8> {
        (0..len)
            .map(|at| u8::try_from((at * 31 + at / 251) % 256).unwrap_or(0))
            .collect()
    }
}
