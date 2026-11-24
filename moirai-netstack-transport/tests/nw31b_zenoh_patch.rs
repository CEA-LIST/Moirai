//! NW.31b (vault `spec/items/NW.31b.md`): the check a launcher calls at start-up
//! reports the Zenoh linked into this build as the vendored, patched copy
//! (`vendor/zenoh-1.10.1`: zenoh 1.10.1 plus eclipse-zenoh/zenoh PR #2779). Its
//! other half, a build that links stock zenoh 1.10.1 is reported as an error
//! naming the `[patch.crates-io]` line, needs a workspace without the patch and
//! runs in `tests/nw31b/gate.sh`.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-netstack-transport --test nw31b_zenoh_patch
//! ```
#![cfg(feature = "zenoh")]

#[test]
fn nw31b_the_linked_zenoh_is_the_vendored_patched_copy() {
    if let Err(why) = moirai_netstack_transport::zenoh_patch::check() {
        panic!("NW31B PATCH: this workspace patches zenoh, yet the check reports: {why}");
    }
}
