//! Wire events over Kafka, as the standard they follow (`CloudEvents` 1.0)
//! binds them: the wire the event capability forwards a
//! subscription over (ADR-0065 clause 3), and the read side a receiving
//! Xmip turns a record back into an event with.
//!
//! What goes on the record is decided once, in `xmip-core-event`'s
//! binding: this file only puts a [`Carried`] where the Kafka protocol
//! binding 1.0.2 puts it — the body as the record's value, every `ce_`
//! attribute as a record header, and the content type as the
//! `content-type` header — and takes it back off a record the same way.
//! One record per event, produced as soon as the forwarder has it, and
//! acknowledged by the leader before [`Wire::carry`] answers `Ok`: at
//! least once, the resilience guards deciding each attempt.
//!
//! **The identity presented** is configured per Party, as ADR-0019 clause
//! 3 has a Send side present the identity configured for the Party it
//! reaches: here, the topic, the broker and the `client.id` the producer
//! names itself with — what a broker keys its quotas and its logs on.
//! That is accountability, not proof: this transport speaks neither SASL
//! nor TLS yet (they are its next layers), and when it does, the
//! credential joins the [`Topic`] a Party is configured with.
//!
//! One connection per Party is kept open between events, so an event
//! costs one produce and not a connect; a failed attempt drops it and the
//! next attempt connects afresh.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry as Slot;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use event::binding::Carried;
use event::forward::Wire;
use resilience::Failure;
use xcore::PartyId;

use crate::client::Client;
use crate::records::{Entry, Header, Record};

/// The record header a `WireEvent`'s content type travels in.
pub const CONTENT_TYPE: &str = "content-type";

/// Where one Party's events are produced, and the name the producer
/// presents there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topic {
    /// The broker, `host:port`, that leads the partition.
    pub broker: String,
    pub topic: String,
    pub partition: i32,
    /// The `client.id` the producer presents.
    pub client: String,
}

impl Topic {
    /// `topic`, partition 0, on the broker at `broker`, presenting `xmip`.
    #[must_use]
    pub fn new(broker: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            topic: topic.into(),
            partition: 0,
            client: "xmip".to_string(),
        }
    }

    /// This partition rather than 0.
    #[must_use]
    pub const fn on_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Present this `client.id` rather than `xmip`.
    #[must_use]
    pub fn presenting(mut self, client: impl Into<String>) -> Self {
        self.client = client.into();
        self
    }
}

/// The Kafka wire: each Party's topic, and a connection kept to each.
pub struct EventWire {
    topics: BTreeMap<PartyId, Topic>,
    timeout: Option<Duration>,
    connections: Mutex<BTreeMap<PartyId, Client>>,
}

impl EventWire {
    /// A wire configured for no Party yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            topics: BTreeMap::new(),
            timeout: None,
            connections: Mutex::new(BTreeMap::new()),
        }
    }

    /// Carry `party`'s events to `topic`.
    #[must_use]
    pub fn to(mut self, party: PartyId, topic: Topic) -> Self {
        self.topics.insert(party, topic);
        self
    }

    /// Give up on a broker that does not answer within `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl Default for EventWire {
    fn default() -> Self {
        Self::new()
    }
}

impl Wire for EventWire {
    /// One record on the Party's topic, acknowledged by the leader.
    fn carry(&self, party: PartyId, carried: &Carried) -> Result<(), Failure> {
        let topic = self.topics.get(&party).ok_or_else(|| {
            Failure::permanent(format!("no Kafka topic is configured for Party {party}"))
        })?;
        let headers = headers(carried);
        let entry = Entry::new(None, Some(&carried.body)).with_headers(&headers);
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let client = match connections.entry(party) {
            Slot::Occupied(open) => open.into_mut(),
            Slot::Vacant(slot) => {
                slot.insert(Client::connect(&topic.broker, &topic.client, self.timeout)?)
            }
        };
        match client.produce_entry(&topic.topic, topic.partition, entry) {
            Ok(_) => Ok(()),
            Err(error) => {
                connections.remove(&party);
                Err(error.into())
            }
        }
    }
}

/// The record headers `carried` travels with: its content type first,
/// then its attributes, each value as its UTF-8.
#[must_use]
pub fn headers(carried: &Carried) -> Vec<Header> {
    let content_type = carried
        .content_type
        .iter()
        .map(|media| (CONTENT_TYPE.to_string(), media.as_bytes().to_vec()));
    let attributes = carried
        .headers
        .iter()
        .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()));
    content_type.chain(attributes).collect()
}

/// What `record` carries, for the binding to read a `WireEvent` from:
/// its `content-type` header as the content type, every other header as
/// text, its value as the body.
#[must_use]
pub fn carried(record: &Record) -> Carried {
    let mut carried = Carried {
        body: record.value.clone().unwrap_or_default(),
        ..Carried::default()
    };
    for (name, value) in &record.headers {
        let text = String::from_utf8_lossy(value).into_owned();
        if name == CONTENT_TYPE {
            carried.content_type = Some(text);
        } else {
            carried.headers.push((name.clone(), text));
        }
    }
    carried
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_carried_event_is_its_record_and_its_record_is_the_carried_event() {
        let carried = Carried {
            content_type: Some("application/json".to_string()),
            headers: vec![
                ("ce_specversion".to_string(), "1.0".to_string()),
                ("ce_id".to_string(), "a b%".to_string()),
            ],
            body: b"{\"status\":\"503\"}".to_vec(),
        };
        let headers = headers(&carried);
        assert_eq!(
            headers[0],
            (CONTENT_TYPE.to_string(), b"application/json".to_vec())
        );
        let record = Record {
            offset: 7,
            key: None,
            value: Some(carried.body.clone()),
            headers,
        };
        assert_eq!(super::carried(&record), carried);
        let bare = Record {
            offset: 0,
            key: None,
            value: None,
            headers: Vec::new(),
        };
        assert_eq!(super::carried(&bare), Carried::default());
    }

    #[test]
    fn a_party_with_no_topic_is_refused_for_good() {
        let wire = EventWire::new().to(PartyId::new(1), Topic::new("127.0.0.1:1", "t"));
        let refused = wire
            .carry(PartyId::new(2), &Carried::default())
            .expect_err("no topic");
        assert!(!refused.is_retryable());
        assert!(refused.reason.contains("no Kafka topic"), "{refused}");
        let unreachable = wire
            .timing_out_after(Duration::from_secs(1))
            .carry(PartyId::new(1), &Carried::default())
            .expect_err("nothing listens");
        assert!(unreachable.is_retryable(), "{unreachable}");
    }
}
