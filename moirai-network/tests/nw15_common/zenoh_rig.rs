//! NW.15: Zenoh sessions for the tests, built by the tests themselves.
//!
//! Every session the tests open, whether a `ZenohTransport` under test or a
//! raw observer, is in peer mode, listens on `tcp/127.0.0.1:0`, connects to
//! the locators it is given, and has multicast scouting and gossip off, as
//! Zenoh's own tests open theirs (note section 12, Z18). The configuration
//! is written here with `Config::insert_json5`, not taken from the code
//! under test, so the transport tests do not depend on the configuration
//! builder that `nw15_zenoh_config.rs` checks.
//!
//! Each scenario uses a fresh Moirai session id, so the key spaces of two
//! scenarios in one process never meet.
#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};

use zenoh::{Config, Session, Wait};

pub fn loopback_peer(connect: &[String]) -> Config {
    let mut config = Config::default();
    config.insert_json5("mode", r#""peer""#).expect("mode");
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .expect("listen/endpoints");
    config
        .insert_json5(
            "connect/endpoints",
            &serde_json::to_string(connect).expect("endpoints as JSON"),
        )
        .expect("connect/endpoints");
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .expect("scouting/multicast/enabled");
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .expect("scouting/gossip/enabled");
    config
}

/// The TCP locator on 127.0.0.1 among `locators`, as Zenoh reports them
/// after a bind to port 0.
pub fn tcp_locator(locators: Vec<String>) -> String {
    locators
        .iter()
        .find(|l| l.starts_with("tcp/127.0.0.1:"))
        .cloned()
        .unwrap_or_else(|| panic!("no tcp/127.0.0.1 locator among {locators:?}"))
}

/// A raw Zenoh session: the observer, the slow receiver, the foreign
/// publisher. Not a `ZenohTransport`.
pub fn raw_session(connect: &[String]) -> Session {
    zenoh::open(loopback_peer(connect))
        .wait()
        .expect("open a raw Zenoh session")
}

pub fn raw_locator(session: &Session) -> String {
    tcp_locator(
        session
            .info()
            .locators()
            .wait()
            .into_iter()
            .map(|l| l.to_string())
            .collect(),
    )
}

static NEXT_SESSION: AtomicUsize = AtomicUsize::new(0);

/// A Moirai session id no other scenario in this process uses. Letters and
/// digits only, so it is a key chunk.
pub fn fresh_session(tag: &str) -> String {
    format!(
        "nw15{tag}{}n{}",
        std::process::id(),
        NEXT_SESSION.fetch_add(1, Ordering::SeqCst)
    )
}

/// The last chunk of a key expression.
pub fn last_chunk(key: &str) -> String {
    key.rsplit('/').next().unwrap_or_default().to_string()
}
