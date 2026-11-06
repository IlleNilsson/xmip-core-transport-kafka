//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand fetches on one connection take a millisecond each at most;
//! - a thousand sends on one connection take a millisecond each at most;
//! - an Event forwarded over Kafka reaches the far end within a millisecond
//!   apart from load, and Xmip's own processor time an Event is under a
//!   millisecond (the shared far end is `tests/event_wire/fixture.rs`).

use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use event::binding::{Binding, Mode};
use event::forward::Forwarder;
use resilience::Guard;
use retry::Retry;
use transport::Transport;
use transport::latency::TcpControl;
use xcore::latency::{spin, spread};
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
            let began = Instant::now();
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
            Ok::<_, transport::TransportError>(arrived)
        });
        let mut session = far_end
            .accept_one(&listener)
            .expect("accepting")
            .with_records("orders", &[b"zero"]);
        for _ in 0..RECEIVES {
            let event = session.next_event().expect("fetch");
            assert!(matches!(event, Some(Event::Fetched { .. })), "{event:?}");
        }
        assert_eq!(receiver.join().expect("thread").expect("fetched"), 1);
    });
}

#[test]
fn a_thousand_sends_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let far_end = KafkaTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = KafkaTransport::new(address, "orders").timing_out_after(secs(5));
    let sender = thread::spawn(move || {
        let began = Instant::now();
        for n in 0..SENDS {
            near.send("orders", n.to_string().as_bytes())?;
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond a send.
        assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
        Ok::<_, transport::TransportError>(())
    });
    let mut session = far_end.accept_one(&listener).expect("accepting");
    for n in 0..SENDS {
        let produced = session.next_produce().expect("produce").expect("one");
        assert_eq!(produced.bytes, n.to_string().as_bytes());
    }
    sender.join().expect("thread").expect("sending");
}

/// How many Events the latency test forwards.
const ROUNDS: usize = 300;

/// The bound: about a millisecond, apart from load.
const BOUND: Duration = Duration::from_millis(1);

#[test]
fn an_event_reaches_the_far_end_within_a_millisecond_apart_from_load() {
    let at = audit_at("latency");
    let hub = Arc::new(allowing());
    let (address, told) = far_end(0);
    let mut forwarder = Forwarder::new(
        subscribed(&hub, &at),
        Binding::Kafka,
        Mode::Binary,
        wire(&address),
    );
    // The connection is opened once, before the rounds: what is measured
    // is an Event's way, not a connect.
    let _ = hub.publish(published());
    let once = Retry::new(1, Duration::ZERO);
    assert_eq!(forwarder.pump(TIMEOUT, &[&once as &dyn Guard]).carried, 1);
    told.recv_timeout(TIMEOUT).expect("the first, produced");
    // The forwarding thread's processor time over every Event: its waits
    // are not counted, only its work.
    let forwarding = thread::spawn(move || {
        let spent = ProcessorTime::thread();
        let once = Retry::new(1, Duration::ZERO);
        let mut carried = 0;
        while carried < ROUNDS {
            carried += forwarder
                .pump(Duration::from_secs(5), &[&once as &dyn Guard])
                .carried;
        }
        spent.elapsed()
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
    let mut publishing = Duration::ZERO;

    for _ in 0..ROUNDS {
        spin(Duration::from_micros(500));
        let event = published();
        sent.lock().expect("sent").push(Instant::now());
        let from = ProcessorTime::thread();
        let _ = hub.publish(event);
        publishing += from.elapsed();
        spin(Duration::from_micros(500));
        control.poke();
    }

    let taken = spread(
        "publish to the Kafka far end",
        receiving.join().expect("received"),
    );
    let forwarding_time = forwarding.join().expect("forwarded");
    let machine = control.load();
    // The wake is a wake: the fastest of each five against the machine's.
    assert!(
        taken.fastest_of_five < BOUND + machine.fastest_of_five,
        "the fastest of five {:?}, the machine's own {:?}",
        taken.fastest_of_five,
        machine.fastest_of_five
    );
    // Xmip's own work: the publisher's and the forwarder's processor time,
    // an Event's share.
    let own = (publishing + forwarding_time) / u32::try_from(ROUNDS).expect("a count");
    println!("Xmip's own processor time: {own:?} an Event");
    assert!(own < BOUND, "Xmip's own processor time, {own:?} an Event");
    let _ = fs::remove_dir_all(&at);
}
