#![forbid(unsafe_code)]

//! Streams that arrive as Kafka records. One record is one Stream, the
//! topic, partition and offset kept beside it.
//!
//! Kafka is the event backbone: partitioned logs on a cluster of brokers,
//! producers that append, consumers that read on from an offset they keep,
//! on port 9092. A Receive Location fetches its partition from its cursor
//! and hands each record's value up; the record's acknowledgement, after
//! the runtime's receive cycle, moves the cursor past it when accepted or
//! refused for good and leaves it when the cycle failed, so a failed record
//! is fetched again. A Send Location produces. Either may instead accept
//! clients directly through [`Session`], one broker's worth of protocol for
//! one connection over logs kept in memory.
//!
//! What is here is the protocol as it stands since message format 2:
//! Metadata v1 to find the leader, Produce v3 with acknowledgement from the
//! leader, Fetch v4, record batches with their CRC-32C. Consumer groups,
//! SASL, TLS, compression and idempotent producers are the next layers.
//!
//! The origin URI carries what the fetch knew: `kafka://broker/orders/0?offset=41`.
//!
//! The event capability rides this transport (ADR-0065 clause 3):
//! [`event_wire`] produces a `WireEvent` as one record — the body
//! its value, the attributes and the content type its headers, as the
//! wire event's Kafka binding says — and reads one back off a record.

pub mod client;
pub mod event_wire;
pub mod records;
pub mod session;
pub mod wire;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, TopicMetadata};
use net::Target;
pub use records::Record;
pub use session::{Event, Session};
use transport::Configured;
use transport::arrived::next_arrival;
use transport::contiguous::Contiguous;
use transport::error::Result;
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Pool, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

pub struct KafkaTransport {
    bootstrap: String,
    topic: String,
    partition: i32,
    client: String,
    /// The offset the next receive reads from, which a record's
    /// acknowledgement moves.
    cursor: Contiguous<i64>,
    timeout: Option<Duration>,
    /// The connections a send produces on, connected once per broker and
    /// kept.
    producers: Pool<Client>,
    /// The connection a receive fetches on, to the partition's leader:
    /// connected on the first receive and kept.
    fetchers: Pool<Client>,
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
            cursor: Contiguous::new(0),
            timeout: None,
            producers: Pool::new(),
            fetchers: Pool::new(),
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
        self.cursor.set(offset);
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
        self.cursor.at()
    }

    /// Connect to the partition's leader, asking the bootstrap broker who
    /// that is.
    ///
    /// # Errors
    /// Where no broker could be reached or the topic has no leader.
    pub fn connect(&self) -> Result<Client> {
        Client::to_leader(&self.bootstrap, &self.topic, &self.client, self.timeout)
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
        match Target::under(&["kafka"], target).map(|named| (named.authority(), named.path())) {
            Some((peer, "")) => (peer, &self.topic),
            Some(pair) => pair,
            None => (&self.bootstrap, target),
        }
    }
}

impl Configured for KafkaTransport {
    /// The address is the bootstrap broker, `host:9092`, both sides ask
    /// first.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "topic",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The topic a Receive Location fetches, or a Send Location produces to.",
                applies: Applies::Both,
            },
            Setting {
                name: "partition",
                kind: Kind::Integer {
                    minimum: 0,
                    maximum: i32::MAX as i64,
                },
                presence: Presence::Optional,
                meaning: "The partition fetched from and produced to; partition 0 when left out.",
                applies: Applies::Both,
            },
            Setting {
                name: "offset",
                kind: Kind::Integer {
                    minimum: 0,
                    maximum: i64::MAX,
                },
                presence: Presence::Optional,
                meaning: "The offset the first receive reads from; the beginning when left out.",
                applies: Applies::Receive,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a broker that stops mid-message is waited on.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let mut transport = Self::new(address, settings.text("topic"));
        if let Some(partition) = settings.optional_integer("partition") {
            // The declaration holds it within 31 bits.
            transport = transport.on_partition(i32::try_from(partition).unwrap_or(0));
        }
        if let Some(offset) = settings.optional_integer("offset") {
            transport = transport.from_offset(offset);
        }
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl Transport for KafkaTransport {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a cursor moves only contiguously")
    }

    /// The records from the cursor on, fetched on the connection the first
    /// receive opened to the leader and kept. Nothing moves the cursor
    /// here: a record's [`transport::Verdict::Accepted`] moves it past that
    /// record, and only where it stands at that record, so it advances
    /// contiguously ([`Contiguous`]); [`transport::Verdict::Refused`] moves
    /// it the same way, as a log has no place to reject a record into and a
    /// refused one is not read again; [`transport::Verdict::Failed`] leaves
    /// it, and the failed record and those after it are fetched again (at
    /// least once, never a skip).
    fn receive(&self) -> Result<Vec<Arrived>> {
        let records = self.fetchers.exchange(
            self.bootstrap.as_str(),
            || self.connect(),
            |client| client.fetch(&self.topic, self.partition, self.cursor()),
        )?;
        let mut arrived = Vec::with_capacity(records.len());
        for record in records {
            let offset = record.offset;
            arrived.push(Arrived::whole(
                format!(
                    "kafka://{}/{}/{}?offset={offset}",
                    self.bootstrap, self.topic, self.partition
                ),
                record.value.unwrap_or_default(),
                self.cursor.advancing(offset, offset + 1),
            ));
        }
        Ok(arrived)
    }

    /// Produce on the connection kept for the broker, connected on the
    /// first send to it, acknowledged by the leader.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        self.produce(target, bytes, None)
    }

    /// The key is the record's key. A broker appends a record produced
    /// again — an idempotent producer's deduplication holds within one
    /// producer session only — so the key is what a consumer recognises a
    /// repeat by, and what a compacted topic keeps one record of.
    fn send_keyed(&self, target: &str, bytes: &[u8], key: &str) -> Result<()> {
        self.produce(target, bytes, Some(key))
    }
}

impl KafkaTransport {
    /// The one send: one record on the target's topic, under `key` where
    /// there is one, on the connection kept for its broker.
    fn produce(&self, target: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        let (broker, topic) = self.resolve(target);
        self.producers.exchange(
            broker,
            || Client::connect(broker, &self.client, self.timeout),
            |client| {
                client
                    .produce(topic, self.partition, key.map(str::as_bytes), bytes)
                    .map(|_| ())
            },
        )
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

/// What a far end does with its one producer and its one record, waiting
/// `timeout` on each: the leader's side of a loopback round, and of every
/// technology that speaks Kafka on the wire (redpanda). It holds the timeout
/// rather than the transport: the transport carries a cursor under a lock,
/// and a far end has no offset to keep.
#[must_use]
pub fn producing(timeout: Option<Duration>) -> impl Accepting {
    move |listener: &TcpListener| {
        next_arrival(
            Session::accept(listener, timeout)?.next_produce()?,
            "the client closed without producing",
        )
    }
}

impl Loopback for KafkaTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(
            producing(self.timeout),
            self.bind()?,
        )))
    }

    /// A producer of its own to `address`, one record on this transport's
    /// topic, acknowledged by the leader before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let mut near = Self::new(address, &self.topic).on_partition(self.partition);
        near.timeout = self.timeout;
        near.send(&self.topic, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::Refusal;
    use transport::payload::{edge_payloads, sized_payloads};

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn kafka_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert!(KafkaTransport::SETTINGS.problems().is_empty());
        let given = [
            ("topic".to_string(), Given::Text("orders".to_string())),
            ("partition".to_string(), Given::Integer(3)),
            ("offset".to_string(), Given::Integer(41)),
            ("timeout".to_string(), Given::Text("5s".to_string())),
        ];
        let built = KafkaTransport::open("broker:9092", Applies::Receive, &given).expect("built");
        assert_eq!(
            (built.bootstrap.as_str(), built.topic.as_str()),
            ("broker:9092", "orders")
        );
        assert_eq!((built.partition, built.cursor()), (3, 41));
        assert_eq!(built.timeout, Some(secs(5)));
        let Err(refused) = KafkaTransport::open("broker:9092", Applies::Send, &given) else {
            panic!("a Send Location reads from no offset");
        };
        assert!(refused.message.contains("\"offset\""), "{refused}");
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
            let mut arrived = near.receive()?.into_iter();
            let one = arrived.next().expect("offset 1").taken()?;
            let accepted = near.cursor();
            arrived
                .next()
                .expect("offset 2")
                .refused(Refusal::Unacceptable)?;
            let refused = near.cursor();
            arrived.next().expect("offset 3").failed()?;
            let failed = near.cursor();
            let again = next_arrival(near.receive()?, "offset 3 again")?.taken()?;
            let cursors = [accepted, refused, failed, near.cursor()];
            Ok::<_, transport::TransportError>((one, cursors, again))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(
            session.next_produce().expect("produce").expect("one").bytes,
            b"produced"
        );
        // The producer keeps its connection; the receive opens its own.
        drop(session);
        let mut session = far_end
            .accept_one(&listener)
            .expect("second")
            .with_records("orders", &[b"zero", b"one", b"two", b"three"]);
        let mut fetched_from = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            if let Event::Fetched { offset, .. } = event {
                fetched_from.push(offset);
            }
        }
        let (one, cursors, again) = near.join().expect("thread").expect("round trip");
        assert_eq!(one.bytes, b"one");
        assert_eq!(
            cursors,
            [2, 3, 3, 4],
            "accepted and refused move the cursor past their record, failed leaves it"
        );
        assert_eq!(again.bytes, b"three");
        assert!(again.origin_uri.ends_with("/orders/0?offset=3"));
        assert_eq!(
            fetched_from,
            [1, 3],
            "only the failed record is fetched again"
        );
        assert!(far_end.claims().is_none());
    }

    #[test]
    fn a_thousand_receives_connect_once_and_a_connection_the_broker_closed_is_replaced() {
        const RECEIVES: usize = 1000;
        let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = KafkaTransport::new(address, "orders").timing_out_after(secs(5));
        let fetched = |session: &mut Session, from: i64| {
            let event = session.next_event().expect("fetch");
            assert!(
                matches!(event, Some(Event::Fetched { offset, .. }) if offset == from),
                "{event:?}"
            );
        };
        std::thread::scope(|scope| {
            let receiver = scope.spawn(|| {
                let began = std::time::Instant::now();
                let mut arrived = 0;
                for _ in 0..RECEIVES {
                    for record in near.receive()? {
                        record.taken()?;
                        arrived += 1;
                    }
                }
                let took = began.elapsed();
                // Generous for a debug build under load: a millisecond a fetch.
                assert!(took < Duration::from_millis(RECEIVES as u64), "{took:?}");
                for record in near.receive()? {
                    record.taken()?;
                    arrived += 1;
                }
                Ok::<_, transport::TransportError>(arrived)
            });
            // One connection for every fetch: one session accepted.
            let mut session = far_end
                .accept_one(&listener)
                .expect("accepting")
                .with_records("orders", &[b"zero"]);
            fetched(&mut session, 0);
            for _ in 1..RECEIVES {
                fetched(&mut session, 1);
            }
            drop(session);
            let mut again = far_end
                .accept_one(&listener)
                .expect("a new connection")
                .with_records("orders", &[b"zero", b"one"]);
            fetched(&mut again, 1);
            assert_eq!(receiver.join().expect("thread").expect("fetched"), 2);
        });
        assert_eq!(near.cursor(), 2);
        assert_eq!(near.fetchers.opened(), 2);
    }

    #[test]
    fn a_thousand_sends_connect_once_and_a_connection_the_broker_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near =
            std::sync::Arc::new(KafkaTransport::new(address, "orders").timing_out_after(secs(5)));
        let sending = std::sync::Arc::clone(&near);
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("orders", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a send.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("orders", b"after the close")
        });
        // One connection for every produce: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for n in 0..SENDS {
            let produced = session.next_produce().expect("produce").expect("one");
            assert_eq!(produced.bytes, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new connection");
        let last = again.next_produce().expect("produce").expect("one");
        assert_eq!(last.bytes, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.producers.opened(), 2);
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
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }
}
