//! The event capability over Kafka (ADR-0065 clause 3): a subscription
//! forwarded through this transport's wire to its own far end, read back
//! as the Event that was published — in both modes of the binding, at
//! least once through a leader that refuses. How near real time it is (the
//! owner, 2026-09-26) is held in the module's timing program,
//! `tests/measured`.

use std::fs;
use std::time::Duration;

use event::Event;
use event::binding::{Binding, Mode};
use event::forward::Forwarder;
use resilience::Guard;
use retry::Retry;
use xmip_core_transport_kafka::event_wire::carried;

#[path = "event_wire/fixture.rs"]
mod fixture;

use fixture::{TIMEOUT, allowing, audit_at, far_end, published, subscribed, wire};
#[test]
fn a_published_event_arrives_as_the_same_event_in_either_mode() {
    let at = audit_at("modes");
    for mode in [Mode::Structured, Mode::Binary] {
        let hub = allowing();
        let (address, told) = far_end(0);
        let mut forwarder =
            Forwarder::new(subscribed(&hub, &at), Binding::Kafka, mode, wire(&address));
        let event = published();
        let _ = hub.publish(event.clone());

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
    let hub = allowing();
    let (address, told) = far_end(2);
    let subscription = subscribed(&hub, &at);
    let mut forwarder = Forwarder::new(subscription, Binding::Kafka, Mode::Binary, wire(&address));
    let first = published();
    let second = published();
    let _ = hub.publish(first.clone());
    let _ = hub.publish(second.clone());

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
