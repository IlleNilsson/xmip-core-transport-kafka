# xmip-core-transport-kafka

Kafka transport: one record is one Stream, the topic, partition and offset beside it; a Location produces or fetches on from its cursor through a broker, or accepts clients directly. Message format 2. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Record headers, and the event capability's wire events

A record carries its headers — the v2 record format's count, then each key
and value behind its length — written by `records::encode_batch` from an
`Entry` and read back into `Record::headers` by `decode_batches`; until
2026-09-26 every record was written with none and any it came with were
dropped. `Client::produce_entry` produces one with its headers, and a
`Session` keeps them in its log (`Session::log`) and can refuse produces
with a broker error (`Session::refusing`), as a far end shows a producer a
leader that moved.

On them rides the event capability (ADR-0065 clause 3):
`event_wire::EventWire` implements
[xmip-core-event](https://github.com/IlleNilsson/xmip-core-event)'s `Wire`
and produces what the event crate's Kafka binding wrote as one record — the
body its value, the `ce_` attributes and `content-type` its headers —
acknowledged by the leader before it counts: at least once, the resilience
guards deciding each attempt. The identity presented for a Party is its
topic and the `client.id` it produces under; SASL and TLS are this
transport's next layers, and a credential joins the Party's `Topic` then.
The connections to each Party are the capability's `Pool`, kept between
events. `event_wire::carried`
is the read side: a record a receiving Xmip fetched, as the binding reads a
`WireEvent` from. `tests/event_wire.rs` holds publish to far-end receipt
to a millisecond at the median and five at the 99th percentile, apart from
load.

A Send Location produces on a connection kept per broker (`transport::Pool`). Until 2026-09-27 every send connected.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
