//! NW.16b, R0 at L0 (vault `spec/items/NW.16b.md`): the in-tree router is a
//! workspace crate, `moirai-router`, with a library `moirai_router` and a
//! binary `moirai-router`, built from the same pinned Zenoh as the nodes:
//! `zenoh = "=1.10.1"`, default features off, its features a subset of
//! moirai-network's own (`transport_tcp`, `unstable`) that names
//! `transport_tcp`, so that router and nodes run one Zenoh build by
//! construction (NW.14 note, Q3 and Q4) and the lock gains no Zenoh crate.
//!
//! The manifest is read through `cargo metadata --no-deps`, which resolves
//! inline tables, dotted tables and workspace inheritance alike; the lock is
//! read as text. The crate's test target `nw16b_router_config_l1` must be
//! discovered, since the spec runs it before any router process starts on
//! the host.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16b_router_pin_l0
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
fn the_router_is_a_workspace_crate_on_the_pinned_zenoh() {
    let meta = metadata();
    let members: Vec<&str> = meta["workspace_members"]
        .as_array()
        .expect("workspace_members")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let packages = meta["packages"].as_array().expect("packages");
    let router = packages
        .iter()
        .find(|p| p["name"] == "moirai-router")
        .unwrap_or_else(|| {
            panic!(
                "R0: no package `moirai-router` in the workspace; members are {:?}",
                packages
                    .iter()
                    .filter_map(|p| p["name"].as_str())
                    .collect::<Vec<_>>()
            )
        });
    let id = router["id"].as_str().unwrap_or("");
    assert!(
        members.contains(&id),
        "R0: `moirai-router` ({id}) is not a workspace member"
    );

    let targets: BTreeSet<(String, String)> = router["targets"]
        .as_array()
        .expect("targets")
        .iter()
        .flat_map(|t| {
            let name = t["name"].as_str().unwrap_or("").to_string();
            t["kind"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(move |kind| (kind.to_string(), name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    for wanted in [
        ("bin", "moirai-router"),
        ("lib", "moirai_router"),
        ("test", "nw16b_router_config_l1"),
    ] {
        assert!(
            targets.contains(&(wanted.0.to_string(), wanted.1.to_string())),
            "R0: `moirai-router` has no {} target `{}` (its targets: {targets:?}); the test \
             target is the oracle's and must be discovered (no `autotests = false`, no \
             `[[test]]` entry)",
            wanted.0,
            wanted.1
        );
    }

    let zenoh: Vec<&Value> = router["dependencies"]
        .as_array()
        .expect("dependencies")
        .iter()
        .filter(|d| d["name"] == "zenoh" && d["kind"].is_null())
        .collect();
    let [zenoh] = zenoh.as_slice() else {
        panic!(
            "R0: `moirai-router` declares {} normal dependencies on `zenoh`, not one",
            zenoh.len()
        );
    };
    assert_eq!(
        zenoh["req"], "=1.10.1",
        "R0: the router's zenoh is not pinned to =1.10.1: {zenoh}"
    );
    assert_eq!(
        zenoh["uses_default_features"], false,
        "R0: the router takes zenoh's default features: {zenoh}"
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
        "R0: the router's zenoh features are {features:?}; they must name transport_tcp and \
         stay within moirai-network's own {allowed:?}"
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
