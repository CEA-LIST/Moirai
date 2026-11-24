//! NW.16b, R0 at L0, ported for NW.31b (vault `spec/items/NW.31b.md`) from
//! 601c887's `moirai-network/tests/nw16b_router_pin_l0.rs`: the transport crate
//! builds on the pinned Zenoh, `zenoh = "=1.10.1"`, default features off, its
//! features naming `transport_tcp` within (`transport_tcp`, `unstable`), so a
//! node and the router run one Zenoh build, and the lock holds the Zenoh crates
//! it held at 6522637, each once, at 1.10.1.
//!
//! 601c887's test reads these off the `moirai-router` crate, which this workspace
//! does not have yet: `the_router_is_a_workspace_crate_on_the_pinned_zenoh` is
//! deferred to the step that ports the router binary (design section 6, row
//! binaries); the router half of it (a member with a bin, a lib and a test
//! target) waits there, and the pin half runs here against this crate.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-netstack-transport --test nw16b_zenoh_pin_l0
//! ```

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn metadata() -> Value {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(&cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
        ])
        .arg("--manifest-path")
        .arg(workspace().join("Cargo.toml"))
        .output()
        .unwrap_or_else(|e| panic!("set-up: running {cargo} metadata: {e}"));
    assert!(
        output.status.success(),
        "set-up: cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("set-up: cargo metadata prints JSON")
}

/// The Zenoh crates in the lock at 6522637, the commit this item starts
/// from, every one at 1.10.1.
const ZENOH_CRATES_AT_START: [&str; 20] = [
    "zenoh",
    "zenoh-buffers",
    "zenoh-codec",
    "zenoh-collections",
    "zenoh-config",
    "zenoh-core",
    "zenoh-crypto",
    "zenoh-keyexpr",
    "zenoh-link",
    "zenoh-link-commons",
    "zenoh-link-tcp",
    "zenoh-macros",
    "zenoh-plugin-trait",
    "zenoh-protocol",
    "zenoh-result",
    "zenoh-runtime",
    "zenoh-sync",
    "zenoh-task",
    "zenoh-transport",
    "zenoh-util",
];

#[test]
fn the_transport_crate_is_on_the_pinned_zenoh() {
    let meta = metadata();
    let packages = meta["packages"].as_array().expect("packages");
    let crate_ = packages
        .iter()
        .find(|p| p["name"] == "moirai-netstack-transport")
        .expect("R0: no package `moirai-netstack-transport` in the workspace");
    let zenoh: Vec<&Value> = crate_["dependencies"]
        .as_array()
        .expect("dependencies")
        .iter()
        .filter(|d| d["name"] == "zenoh" && d["kind"].is_null())
        .collect();
    let [zenoh] = zenoh.as_slice() else {
        panic!(
            "R0: `moirai-netstack-transport` declares {} normal dependencies on `zenoh`, not one",
            zenoh.len()
        );
    };
    assert_eq!(
        zenoh["req"], "=1.10.1",
        "R0: the transport crate's zenoh is not pinned to =1.10.1: {zenoh}"
    );
    assert_eq!(
        zenoh["uses_default_features"], false,
        "R0: the transport crate takes zenoh's default features: {zenoh}"
    );
    assert_eq!(
        zenoh["optional"], true,
        "R0: zenoh is not optional, so a build without the zenoh feature still links it: {zenoh}"
    );
    let features: BTreeSet<&str> = zenoh["features"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let allowed: BTreeSet<&str> = ["transport_tcp", "unstable"].into_iter().collect();
    assert!(
        features.contains("transport_tcp") && features.is_subset(&allowed),
        "R0: the transport crate's zenoh features are {features:?}; they must name transport_tcp \
         and stay within {allowed:?}"
    );

    // The lock: one Zenoh build, the crates it had, all at 1.10.1.
    let lock = std::fs::read_to_string(workspace().join("Cargo.lock")).expect("Cargo.lock");
    let mut zenoh_crates: Vec<(String, String)> = Vec::new();
    let mut name: Option<String> = None;
    for line in lock.lines() {
        if let Some(n) = line.strip_prefix("name = ") {
            name = Some(n.trim_matches('"').to_string());
        } else if let Some(v) = line.strip_prefix("version = ") {
            if let Some(n) = name.take() {
                if n == "zenoh" || n.starts_with("zenoh-") {
                    zenoh_crates.push((n, v.trim_matches('"').to_string()));
                }
            }
        }
    }
    let names: BTreeSet<&str> = zenoh_crates.iter().map(|(n, _)| n.as_str()).collect();
    let at_start: BTreeSet<&str> = ZENOH_CRATES_AT_START.into_iter().collect();
    assert_eq!(
        names, at_start,
        "R0: the Zenoh crates in Cargo.lock changed from 6522637's"
    );
    assert!(
        zenoh_crates.len() == ZENOH_CRATES_AT_START.len()
            && zenoh_crates.iter().all(|(_, v)| v == "1.10.1"),
        "R0: Cargo.lock holds a Zenoh crate twice or at another version than 1.10.1: \
         {zenoh_crates:?}"
    );
}
