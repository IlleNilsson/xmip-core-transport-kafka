# xmip-core-transport-kafka

Kafka transport: one record is one Stream, the topic, partition and offset beside it; a Location produces or fetches on from its cursor through a broker, or accepts clients directly. Message format 2. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
