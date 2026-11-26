//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48).

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the bootstrap
/// broker, `host:9092`, both sides ask first.
pub const SETTINGS: &Settings = &Settings {
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
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a broker that stops mid-message is waited on.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["kafka"];
