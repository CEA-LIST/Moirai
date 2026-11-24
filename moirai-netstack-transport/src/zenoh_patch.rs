//! The start-up check that the linked Zenoh is the vendored, patched copy.
//!
//! Zenoh 1.10.1 deadlocks a peer session whose gossip links are established
//! while routing holds the router lock; `vendor/zenoh-1.10.1` carries
//! eclipse-zenoh/zenoh PR #2779, which makes the peer connectors' mutex
//! synchronous. A consuming workspace gets that copy only by repeating the
//! patch line the crate's README gives; without it, cargo resolves stock
//! 1.10.1 from the registry and the deadlock is back, silently.
//!
//! [`check`] answers from the lock of the directory cargo is started in:
//! `build.rs` records, in a compile-time environment variable, whether that
//! lock names `zenoh` as a path package (this copy) or a registry package
//! (stock). Run cargo from the consuming workspace: `--manifest-path` from
//! another directory reads that directory's lock, and any path-patched zenoh
//! counts as this copy.
//! The launcher calls it at start-up and prints the error; nothing here panics.

/// `Ok(())` when this build links `vendor/zenoh-1.10.1`; `Err` naming the
/// `[patch.crates-io]` line to add when it links stock zenoh instead.
pub fn check() -> Result<(), String> {
    match option_env!("NW31B_ZENOH_VENDORED") {
        Some("1") => Ok(()),
        _ => Err(
            "the zenoh this build links is not vendor/zenoh-1.10.1: add \
             `zenoh = { path = \"vendor/zenoh-1.10.1\" }` under [patch.crates-io] in this \
             workspace's Cargo.toml, or rebuild with it"
                .to_string(),
        ),
    }
}
