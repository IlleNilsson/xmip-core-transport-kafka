//! The event capability over Kafka (ADR-0065 clause 3): a subscription
//! forwarded through this transport's wire to its own far end, read back
//! as the Event that was published — in both modes of the binding, at
//! least once through a leader that refuses, and near, very near real
//! time (the owner, 2026-09-26).

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use event::Event;
use event::binding::{Binding, Mode};
use event::filter::Filter;
use event::forward::Forwarder;
use event::hub::{Hub, Subscription};
use event::outcome::Outcome;
use event::subscriber::{SameProcess, Subscriber};
use node::Stage;
use resilience::Guard;
use retry::Retry;
use transport::latency::{TcpControl, spin, spread};
use xcore::{JourneyId, PartyId};
use xmip_core_transport_kafka::Session;
use xmip_core_transport_kafka::event_wire::{EventWire, Topic, carried};
use xmip_core_transport_kafka::records::Record;

/// The subscriber, a remote Party.
const PARTY: PartyId = PartyId::new(21);

const TOPIC: &str = "xmip.events";

/// How many Events the latency test forwards.
const ROUNDS: usize = 300;

/// The bound: about a millisecond, apart from load.
const BOUND: Duration = Duration::from_millis(1);

/// The tail's bound, apart from load: two exchanges on loopback TCP and
/// the thread wakes between them.
const TAIL: Duration = Duration::from_millis(5);

const TIMEOUT: Duration = Duration::from_secs(2);

/// A temporary audit directory of its own, never empty and never the
/// estate's.
fn audit_at(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-kafka-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

fn subscribed(hub: &Hub, at: &Path) -> Subscription {
    let audit = ProgramAudit::new("xmip-core-transport-kafka tests", Some(at));
    hub.subscribe(
        Subscriber::in_process(PARTY, audit),
        Filter::everything(),
        0,
    )
    .expect("allowed")
}

fn published() -> Event {
    Event::completed(
        Stage::Send,
        Outcome::Failure,
        "xmip:///c/node/n/send/billing",
    )
    .in_journey(JourneyId::new(7))
    .on_artifact("billing")
    .about(PARTY)
    .saying("status", "503 Service Unavailable")
}

/// The far end: one broker session after another, the first `refusing`
/// produces answered `REQUEST_TIMED_OUT`, each record kept handed over with
/// the moment it arrived.
fn far_end(refusing: u32) -> (String, Receiver<(Instant, Record)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let address = listener.local_addr().expect("address").to_string();
    let (tell, told) = mpsc::channel();
    thread::spawn(move || {
        let mut refusing = refusing;
        while let Ok(session) = Session::accept(&listener, Some(TIMEOUT)) {
            // A refused attempt drops its connection, so each session
            // refuses once while refusals are left.
            let mut session = session.refusing(7, u32::from(refusing > 0));
            refusing = refusing.saturating_sub(1);
            while let Ok(Some(_)) = session.next_produce() {
                let now = Instant::now();
                let record = session.log(TOPIC, 0).last().expect("kept").clone();
                if tell.send((now, record)).is_err() {
                    return;
                }
            }
        }
    });
    (address, told)
}

fn wire(address: &str) -> EventWire {
    EventWire::new()
        .to(PARTY, Topic::new(address, TOPIC).presenting("xmip-events"))
        .timing_out_after(TIMEOUT)
}

#[test]
fn a_published_event_arrives_as_the_same_event_in_either_mode() {
    let at = audit_at("modes");
    for mode in [Mode::Structured, Mode::Binary] {
        let hub = Hub::new(vec![Arc::new(SameProcess)]);
        let (address, told) = far_end(0);
        let mut forwarder =
            Forwarder::new(subscribed(&hub, &at), Binding::Kafka, mode, wire(&address));
        let event = published();
        hub.publish(event.clone());

        let once = Retry::new(1, Duration::ZERO);
        let pumped = forwarder.pump(TIMEOUT, &[&once as &dyn Guard]);

        assert_eq!((pumped.carried, pumped.pending), (1, 0), "{mode:?}");
        let (_, record) = told.recv_timeout(TIMEOUT).expect("produced");
        let names: Vec<&str> = record
            .headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        match mode {
            Mode::Structured => assert_eq!(names, ["content-type"]),
            Mode::Binary => assert!(names.contains(&"ce_id") && names.contains(&"ce_xmipparty")),
        }
        let wire_event = Binding::Kafka.read(&carried(&record)).expect("a WireEvent");
        assert_eq!(
            wire_event.event().expect("an Xmip Event"),
            event,
            "{mode:?}"
        );
    }
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn a_leader_that_refuses_is_retried_and_the_events_arrive_in_order() {
    let at = audit_at("again");
    let hub = Hub::new(vec![Arc::new(SameProcess)]);
    let (address, told) = far_end(2);
    let subscription = subscribed(&hub, &at);
    let mut forwarder = Forwarder::new(subscription, Binding::Kafka, Mode::Binary, wire(&address));
    let first = published();
    let second = published();
    hub.publish(first.clone());
    hub.publish(second.clone());

    let thrice = Retry::new(3, Duration::ZERO);
    let pumped = forwarder.pump(TIMEOUT, &[&thrice as &dyn Guard]);

    assert_eq!((pumped.carried, pumped.pending), (2, 0), "{pumped:?}");
    let arrived: Vec<Event> = (0..2)
        .map(|_| {
            let (_, record) = told.recv_timeout(TIMEOUT).expect("produced");
            let wire_event = Binding::Kafka.read(&carried(&record)).expect("a WireEvent");
            wire_event.event().expect("an Xmip Event")
        })
        .collect();
    assert_eq!(arrived, [first, second], "once each, in order");
    let _ = fs::remove_dir_all(&at);
}

#[test]
fn an_event_reaches_the_far_end_within_a_millisecond_apart_from_load() {
    let at = audit_at("latency");
    let hub = Arc::new(Hub::new(vec![Arc::new(SameProcess)]));
    let (address, told) = far_end(0);
    let mut forwarder = Forwarder::new(
        subscribed(&hub, &at),
        Binding::Kafka,
        Mode::Binary,
        wire(&address),
    );
    // The connection is opened once, before the rounds: what is measured
    // is an Event's way, not a connect.
    hub.publish(published());
    let once = Retry::new(1, Duration::ZERO);
    assert_eq!(forwarder.pump(TIMEOUT, &[&once as &dyn Guard]).carried, 1);
    told.recv_timeout(TIMEOUT).expect("the first, produced");
    let forwarding = thread::spawn(move || {
        let once = Retry::new(1, Duration::ZERO);
        let mut carried = 0;
        while carried < ROUNDS {
            carried += forwarder
                .pump(Duration::from_secs(5), &[&once as &dyn Guard])
                .carried;
        }
    });
    let sent = Arc::new(Mutex::new(Vec::with_capacity(ROUNDS)));
    let receiving = {
        let sent = Arc::clone(&sent);
        thread::spawn(move || {
            (0..ROUNDS)
                .map(|index| {
                    let (arrived, _) = told.recv_timeout(Duration::from_secs(5)).expect("produced");
                    arrived - sent.lock().expect("sent")[index]
                })
                .collect::<Vec<Duration>>()
        })
    };
    let mut control = TcpControl::start();

    for _ in 0..ROUNDS {
        spin(Duration::from_micros(500));
        let event = published();
        sent.lock().expect("sent").push(Instant::now());
        hub.publish(event);
        spin(Duration::from_micros(500));
        control.poke();
    }

    let taken = spread(
        "publish to the Kafka far end",
        receiving.join().expect("received"),
    );
    forwarding.join().expect("forwarded");
    let machine = control.load();
    assert!(
        taken.median < BOUND + machine.median,
        "median {:?}, the machine's own {:?}",
        taken.median,
        machine.median
    );
    assert!(
        taken.p99 < TAIL + machine.p99,
        "p99 {:?}, the machine's own {:?}",
        taken.p99,
        machine.p99
    );
    let _ = fs::remove_dir_all(&at);
}
