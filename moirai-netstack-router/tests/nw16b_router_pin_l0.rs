//! NW.16b, R0 at L0, the router half (601c887's
//! `moirai-network/tests/nw16b_router_pin_l0.rs`, ported to NW.31h's
//! `moirai-netstack-router`): the router is a workspace crate with a library
//! `moirai_netstack_router` and a binary `moirai-netstack-router`, its
//! ported tests discovered, built from the same Zenoh as
//! `moirai-netstack-transport`: one normal, non-optional `zenoh` dependency
//! whose requirement, default-features flag and feature set equal the
//! transport's, and which resolves to the very package the transport's does,
//! the vendored patched copy `vendor/zenoh-1.10.1` the workspace's
//! `[patch.crates-io]` names. The lock holds the 20 Zenoh crates it held at
//! 5485f5f, each once, at 1.10.1. NW.31h adds the second test: no `moirai*`
//! package in the router's dependency closure (normal, dev and build, every
//! feature), as on 601c887 where `moirai-router` depends on `zenoh` alone.
//!
//! `cargo metadata` is read with the reader in `json/mod.rs` (the one of
//! `nw16b_router_config_l1.rs`), so the crate needs no JSON dependency.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-netstack-router --test nw16b_router_pin_l0
//! ```

mod json;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

use json::Json;

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// `cargo metadata` of the workspace, every feature, resolve included.
fn metadata() -> Json {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(&cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--all-features",
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
    let text = String::from_utf8(output.stdout).expect("set-up: cargo metadata prints UTF-8");
    Json::parse(&text).unwrap_or_else(|e| panic!("set-up: cargo metadata is not JSON: {e}"))
}

fn get<'a>(value: &'a Json, key: &str) -> &'a Json {
    match value {
        Json::Object(members) => members
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
            .unwrap_or(&Json::Null),
        _ => &Json::Null,
    }
}

fn items(value: &Json) -> &[Json] {
    match value {
        Json::Array(items) => items,
        _ => &[],
    }
}

fn text(value: &Json) -> &str {
    match value {
        Json::String(s) => s,
        _ => "",
    }
}

fn package<'a>(meta: &'a Json, name: &str) -> &'a Json {
    items(get(meta, "packages"))
        .iter()
        .find(|p| text(get(p, "name")) == name && is_member(meta, text(get(p, "id"))))
        .unwrap_or_else(|| {
            panic!(
                "R0: no workspace member `{name}`; members are {:?}",
                items(get(meta, "workspace_members"))
                    .iter()
                    .map(text)
                    .collect::<Vec<_>>()
            )
        })
}

fn is_member(meta: &Json, id: &str) -> bool {
    items(get(meta, "workspace_members"))
        .iter()
        .any(|m| text(m) == id)
}

/// The one normal `zenoh` dependency of `pkg`: (req, default features,
/// optional, features).
fn zenoh_dependency(pkg: &Json) -> (String, Json, Json, BTreeSet<String>) {
    let name = text(get(pkg, "name")).to_string();
    let found: Vec<&Json> = items(get(pkg, "dependencies"))
        .iter()
        .filter(|d| text(get(d, "name")) == "zenoh" && *get(d, "kind") == Json::Null)
        .collect();
    let [dep] = found.as_slice() else {
        panic!(
            "R0: `{name}` declares {} normal dependencies on `zenoh`, not one",
            found.len()
        );
    };
    (
        text(get(dep, "req")).to_string(),
        get(dep, "uses_default_features").clone(),
        get(dep, "optional").clone(),
        items(get(dep, "features"))
            .iter()
            .map(|f| text(f).to_string())
            .collect(),
    )
}

/// The resolve graph: package id to the ids it depends on (every kind).
fn resolve(meta: &Json) -> BTreeMap<String, Vec<(String, String)>> {
    items(get(get(meta, "resolve"), "nodes"))
        .iter()
        .map(|node| {
            let deps = items(get(node, "deps"))
                .iter()
                .map(|d| {
                    (
                        text(get(d, "name")).to_string(),
                        text(get(d, "pkg")).to_string(),
                    )
                })
                .collect();
            (text(get(node, "id")).to_string(), deps)
        })
        .collect()
}

/// The Zenoh crates in the lock at 5485f5f (akira/moirai-network), each at
/// 1.10.1: the transport's Zenoh build.
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
    let router = package(&meta, "moirai-netstack-router");
    let transport = package(&meta, "moirai-netstack-transport");

    let targets: BTreeSet<(String, String)> = items(get(router, "targets"))
        .iter()
        .flat_map(|t| {
            let name = text(get(t, "name")).to_string();
            items(get(t, "kind"))
                .iter()
                .map(move |kind| (text(kind).to_string(), name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    for wanted in [
        ("bin", "moirai-netstack-router"),
        ("lib", "moirai_netstack_router"),
        ("test", "nw16b_router_config_l1"),
        ("test", "nw16e_router_regions_l1"),
        ("test", "nw16b_router_pin_l0"),
    ] {
        assert!(
            targets.contains(&(wanted.0.to_string(), wanted.1.to_string())),
            "R0: `moirai-netstack-router` has no {} target `{}` (its targets: {targets:?}); \
             a test target is the oracle's and must be discovered (no `autotests = false`, \
             no `[[test]]` entry)",
            wanted.0,
            wanted.1
        );
    }

    let (req, defaults, optional, features) = zenoh_dependency(router);
    let (t_req, t_defaults, _, t_features) = zenoh_dependency(transport);
    assert_eq!(
        req, "=1.10.1",
        "R0: the router's zenoh is not pinned to =1.10.1"
    );
    assert_eq!(
        req, t_req,
        "R0: the router's zenoh requirement differs from the transport's"
    );
    assert_eq!(
        defaults,
        Json::Bool(false),
        "R0: the router takes zenoh's default features"
    );
    assert_eq!(
        defaults, t_defaults,
        "R0: default features differ from the transport's"
    );
    assert_eq!(
        optional,
        Json::Bool(false),
        "R0: the router's zenoh is optional"
    );
    assert_eq!(
        features, t_features,
        "R0: the router's zenoh features differ from the transport's, so they are two builds"
    );

    // Resolved: the router links the package the transport links, the
    // vendored patched copy.
    let graph = resolve(&meta);
    let zenoh_of = |id: &str| -> String {
        graph
            .get(id)
            .unwrap_or_else(|| panic!("set-up: no resolve node {id}"))
            .iter()
            .find(|(name, _)| name == "zenoh")
            .map(|(_, pkg)| pkg.clone())
            .unwrap_or_else(|| panic!("R0: {id} resolves no zenoh"))
    };
    let router_zenoh = zenoh_of(text(get(router, "id")));
    let transport_zenoh = zenoh_of(text(get(transport, "id")));
    assert_eq!(
        router_zenoh, transport_zenoh,
        "R0: the router links another zenoh package than the transport"
    );
    assert!(
        router_zenoh.starts_with("path+file://") && router_zenoh.contains("/vendor/zenoh-1.10.1#"),
        "R0: the router's zenoh is {router_zenoh}, not the vendored patched copy vendor/zenoh-1.10.1"
    );

    // The lock: one Zenoh build, the crates it had, all at 1.10.1.
    let lock = std::fs::read_to_string(workspace().join("Cargo.lock")).expect("Cargo.lock");
    let mut zenoh_crates: Vec<(String, String)> = Vec::new();
    let mut name: Option<String> = None;
    for line in lock.lines() {
        if let Some(n) = line.strip_prefix("name = ") {
            name = Some(n.trim_matches('"').to_string());
        } else if let Some(v) = line.strip_prefix("version = ")
            && let Some(n) = name.take()
            && (n == "zenoh" || n.starts_with("zenoh-"))
        {
            zenoh_crates.push((n, v.trim_matches('"').to_string()));
        }
    }
    let names: BTreeSet<&str> = zenoh_crates.iter().map(|(n, _)| n.as_str()).collect();
    let at_start: BTreeSet<&str> = ZENOH_CRATES_AT_START.into_iter().collect();
    assert_eq!(
        names, at_start,
        "R0: the Zenoh crates in Cargo.lock changed from 5485f5f's"
    );
    assert!(
        zenoh_crates.len() == ZENOH_CRATES_AT_START.len()
            && zenoh_crates.iter().all(|(_, v)| v == "1.10.1"),
        "R0: Cargo.lock holds a Zenoh crate twice or at another version than 1.10.1: \
         {zenoh_crates:?}"
    );
}

#[test]
fn nw31h_the_router_depends_on_no_moirai_crate() {
    let meta = metadata();
    let router = text(get(package(&meta, "moirai-netstack-router"), "id")).to_string();
    let graph = resolve(&meta);
    let names: BTreeMap<&str, &str> = items(get(&meta, "packages"))
        .iter()
        .map(|p| (text(get(p, "id")), text(get(p, "name"))))
        .collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut todo = vec![router.clone()];
    while let Some(id) = todo.pop() {
        for (_, dep) in graph.get(&id).map(Vec::as_slice).unwrap_or(&[]) {
            if seen.insert(dep.clone()) {
                todo.push(dep.clone());
            }
        }
    }
    let moirai: Vec<&str> = seen
        .iter()
        .filter_map(|id| names.get(id.as_str()).copied())
        .filter(|name| name.starts_with("moirai"))
        .collect();
    assert!(
        moirai.is_empty(),
        "NW.31h: the router's dependency closure (normal, dev, build, every feature) holds \
         {moirai:?}; it must hold no moirai crate"
    );
}
