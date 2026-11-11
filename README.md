# xmip-core-transport-kafka

Kafka transport: one record is one Stream, the topic, partition and offset beside it; a Location produces or fetches on from its cursor through a broker, or accepts clients directly. Message format 2. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

A Send Location produces to the topic its `topic` setting names, on the broker its address names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which `transport::destination::on_server` reads as that topic. A target naming another still wins — `invoices`, or `kafka://host:9092/invoices` (ADR-0064, amendment 2026-11-10).

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
acknowledged by all in-sync replicas before it counts: at least once, the resilience
guards deciding each attempt. The identity presented for a Party is its
topic and the `client.id` it produces under; SASL and TLS are this
transport's next layers, and a credential joins the Party's `Topic` then.
The connections to each Party are the capability's `Pool`, kept between
events. `event_wire::carried`
is the read side: a record a receiving Xmip fetched, as the binding reads a
`WireEvent` from. The module's timing program, `tests/measured`, holds
publish to far-end receipt to a millisecond at the median and five at the
99th percentile, apart from load.

A Send Location produces on a connection kept per broker (`transport::Pool`). Until 2026-09-27 every send connected.

Every produce asks for `acks=-1` (`client::ALL_IN_SYNC_REPLICAS`): the broker answers once every in-sync replica has the record, how many at least being the topic's `min.insync.replicas`. Xmip acknowledges only what is durable, so there is no setting for it. Until 2026-11-09 a produce asked the leader alone (`acks=1`), and a leader that failed over before a follower copied the record lost a record Xmip had counted as sent.

A Receive Location fetches on a connection to the partition's leader, found and connected on its first receive and kept; the offset it reads from is the transport's cursor. A connection the broker closed is replaced, and the leader asked again. Until 2026-09-28 every receive asked for metadata and connected. The leader lookup is `Client::to_leader`, the one the redpanda technology calls too.

A fetched record is acknowledged after the runtime's whole receive cycle, not as it is fetched: `Accepted` moves the cursor past the record, and only where the cursor stands at it, so it advances contiguously (`transport::contiguous::Contiguous`); `Refused` moves it the same way, since a log has no place to reject a record into and a refused record is not read again (the runtime audited the refusal); `Failed` leaves the cursor, and the failed record and those after it are fetched again — at least once, never a skip. This crate keeps no consumer group, so the acknowledgement is an in-memory step with no broker round trip, and a Location that starts again reads again from its configured offset. Until 2026-10-02 a fetch moved the cursor past what it fetched.

Each record is handed up once (`reading::Reading`, which the redpanda technology can call too). A Fetch answers whole record batches, so it may return records below the offset asked for; those are skipped. The next receive fetches past what was handed up, not from the cursor, since the runtime asks a waiting Location again while what it handed up is still in its receive cycle; a failed record, or one dropped untold, moves that mark back, so it and those after it are fetched again. Each acknowledgement moves the cursor from where the record before it ended, so the gaps a compacted topic leaves are crossed. Until 2026-11-09 a fetch read from the cursor: records still in their cycle were handed up again, and a batch's records below the offset were handed up, which held the cursor where it stood whenever a receive took no more than those.

## The deduplication key

A keyed send (the key `Transport::send_from` is handed, `Outgoing::key`; built 2026-10-04) carries the Journey's identifier as the record's key, the same on every attempt on one Journey; the partition stays the Location's. A broker appends a record produced again — an idempotent producer's deduplication, not spoken here, holds within one producer session only — so the key is what a consumer recognizes a repeat by, and what a compacted topic keeps one record of. An unkeyed `send` writes a null key.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
