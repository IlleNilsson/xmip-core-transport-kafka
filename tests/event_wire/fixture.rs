//! What the event-wire tests and the timing program share: the Party's
//! hub, its subscription and Event, the leader that takes its records and
//! the wire that reaches it.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use audit::program_audit::ProgramAudit;
use authorize_party::PartyPolicy;
use event::Event;
use event::filter::Filter;
use event::hub::{EventSubscription, Hub};
use event::outcome::Outcome;
use event::subscriber::Subscriber;
use node::Stage;
use xcore::{JourneyId, PartyId};
use xmip_core_transport_kafka::Session;
use xmip_core_transport_kafka::event_wire::{EventWire, Topic};
use xmip_core_transport_kafka::records::Record;

/// The subscriber, a remote Party.
pub const PARTY: PartyId = PartyId::new(21);

/// A hub whose policy allows the Party these tests subscribe as: being in
/// this process admits nobody (ADR-0065, amendment 2026-09-26).
pub fn allowing() -> Hub {
    Hub::new(vec![Arc::new(PartyPolicy::new().allow(PARTY))])
}

pub const TOPIC: &str = "xmip.events";

pub const TIMEOUT: Duration = Duration::from_secs(2);

/// A temporary audit directory of its own, never empty and never the
/// estate's.
pub fn audit_at(name: &str) -> PathBuf {
    let at = std::env::temp_dir().join(format!("xmip-kafka-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&at);
    at
}

pub fn subscribed(hub: &Hub, at: &Path) -> EventSubscription {
    let audit = ProgramAudit::new("xmip-core-transport-kafka tests", Some(at));
    hub.subscribe(
        Subscriber::in_process(PARTY, audit),
        Filter::everything(),
        0,
    )
    .expect("allowed")
}

pub fn published() -> Event {
    Event::completed(
        Stage::Send,
        Outcome::Failure,
        format!(
            "{}/send/billing",
            configure::fixture::test_cluster().node_scope(0)
        ),
    )
    .in_journey(JourneyId::new(7))
    .on_artifact("billing")
    .about(PARTY)
    .saying("status", "503 Service Unavailable")
}

/// The far end: one broker session after another, the first `refusing`
/// produces answered `REQUEST_TIMED_OUT`, each record kept handed over with
/// the moment it arrived.
pub fn far_end(refusing: u32) -> (String, Receiver<(Instant, Record)>) {
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

pub fn wire(address: &str) -> EventWire {
    EventWire::new()
        .to(PARTY, Topic::new(address, TOPIC).presenting("xmip-events"))
        .timing_out_after(TIMEOUT)
}
