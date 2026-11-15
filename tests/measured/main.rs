//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand fetches on one connection take a millisecond each at most
//!   beyond their wakes;
//! - a thousand sends on one connection take a millisecond each at most
//!   beyond their wakes;
//! - an Event forwarded over Kafka reaches the far end within a millisecond
//!   apart from load, and this process's processor time across it is under
//!   a millisecond beyond a plain wake's (the shared far end is
//!   `tests/event_wire/fixture.rs`).
//!
//! Each round beside a plain wake, the fastest of five
//! (`xcore::latency::Rounds`).

use std::assert_matches;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use event::binding::Mode;
use resilience::Guard;
use retry::Retry;
use transport::Transport;
use xcluster::delivery::forward::Forwarder;
use xcore::latency::{Rounds, spin};
use xcore::processor_time::ProcessorTime;
use xmip_core_transport_kafka::{Event, KafkaTransport};

#[allow(dead_code)]
#[path = "../event_wire/fixture.rs"]
mod fixture;

use fixture::{TIMEOUT, allowing, audit_at, far_end, published, subscribed, wire};

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_thousand_fetches_take_a_millisecond_each_at_most() {
    const RECEIVES: usize = 1000;
    let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = KafkaTransport::new(address, "orders").timing_out_after(secs(5));
    thread::scope(|scope| {
        let receiver = scope.spawn(|| {
            let (mut rounds, mut arrived) = (Rounds::crossing(EXCHANGE_WAKES), 0);
            for _ in 0..RECEIVES {
                arrived += rounds.round(|| {
                    let mut taken = 0;
                    for record in near.receive()? {
                        record.taken()?;
                        taken += 1;
                    }
                    Ok::<_, transport::TransportError>(taken)
                })?;
            }
            Ok::<_, transport::TransportError>((rounds, arrived))
        });
        let mut session = far_end
            .accept_one(&listener)
            .expect("accepting")
            .with_records("orders", &[b"zero"]);
        for _ in 0..RECEIVES {
            let event = session.next_event().expect("fetch");
            assert_matches!(event, Some(Event::Fetched { .. }), "{event:?}");
        }
        let (rounds, arrived) = receiver.join().expect("thread").expect("fetched");
        assert_eq!(arrived, 1);
        rounds.hold("a fetch");
    });
}

/// What a fetch or a produce wakes where a plain wake wakes one: the far
/// end at the request, and this end at its answer
/// (`xcore::latency::Rounds`).
const EXCHANGE_WAKES: u32 = 2;

#[test]
fn a_thousand_sends_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = KafkaTransport::new(address, "orders").timing_out_after(secs(5));
    let sender = thread::spawn(move || {
        let mut rounds = Rounds::crossing(EXCHANGE_WAKES);
        for n in 0..SENDS {
            rounds.round(|| near.send("orders", n.to_string().as_bytes()))?;
        }
        Ok::<_, transport::TransportError>(rounds)
    });
    let mut session = far_end.accept_one(&listener).expect("accepting");
    for n in 0..SENDS {
        let produced = session.next_produce().expect("produce").expect("one");
        assert_eq!(produced.bytes, n.to_string().as_bytes());
    }
    let rounds = sender.join().expect("thread").expect("sending");
    rounds.hold("a produce, acknowledged");
}

/// How many Events the latency test forwards.
const ROUNDS: usize = 300;

/// What an Event's way to the far end wakes where a plain wake wakes one:
/// the forwarder at the publish, and the far end at its produce.
const EVENT_WAKES: u32 = 2;

#[test]
fn an_event_reaches_the_far_end_within_a_millisecond_apart_from_load() {
    let at = audit_at("latency");
    let hub = Arc::new(allowing());
    let (address, told) = far_end(0);
    let mut forwarder = Forwarder::new(subscribed(&hub, &at), Mode::Binary, wire(&address));
    // The connection is opened once, before the rounds: what is measured
    // is an Event's way, not a connect.
    let _ = hub.publish(published());
    let once = Retry::new(1, Duration::ZERO);
    assert_eq!(forwarder.pump(TIMEOUT, &[&once as &dyn Guard]).carried, 1);
    told.recv_timeout(TIMEOUT).expect("the first, produced");
    // The forwarding thread's processor time over every Event: its waits
    // are not counted, only its work.
    let forwarding = thread::spawn(move || {
        let once = Retry::new(1, Duration::ZERO);
        let mut carried = 0;
        while carried < ROUNDS {
            carried += forwarder
                .pump(Duration::from_secs(5), &[&once as &dyn Guard])
                .carried;
        }
    });
    // From the publish to the far end's arrival, and this process's
    // processor time across it — the publisher, the forwarder and the far
    // end — round by round beside a plain wake. Until 2026-11-15 the
    // processor time was an Event's share of the whole run, held flat.
    let mut rounds = Rounds::crossing(EVENT_WAKES);
    for _ in 0..ROUNDS {
        spin(Duration::from_micros(500));
        let event = published();
        let from = ProcessorTime::process();
        let sent = Instant::now();
        let _ = hub.publish(event);
        let (arrived, _) = told.recv_timeout(Duration::from_secs(5)).expect("produced");
        rounds.took.push(arrived - sent);
        rounds.worked.push(from.elapsed());
        rounds.beside();
    }
    forwarding.join().expect("forwarded");
    rounds.hold("publish to the Kafka far end");
    let _ = fs::remove_dir_all(&at);
}
