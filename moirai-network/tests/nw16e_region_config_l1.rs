//! NW.16e, the node at L1 (vault `spec/items/NW.16e.md`): `ZENOH_REGION`
//! in `TransportConfig::from_lookup` and `zenoh_config`, in the style of
//! NW.15's `nw15_zenoh_config.rs`.
//!
//! - NA: beside `ZENOH_ROUTER` in peer mode (`ZENOH_MODE` unset or `peer`,
//!   `PEERS` unset or empty), the Zenoh configuration's `region_name` is
//!   the value given, as written, and nothing else in the configuration
//!   differs from the same variables without `ZENOH_REGION`.
//! - NB: without `ZENOH_REGION`, `region_name` stays unset in every mode.
//! - NC: `ZENOH_REGION` is refused, at either step (`from_lookup` or
//!   `zenoh_config`, given the peers `PEERS` lists, as `model_node` passes
//!   them), with a message naming both variables, beside `PEERS`
//!   (non-empty), beside `ZENOH_MODE=client`, beside `ZENOH_CONFIG`, and
//!   without `ZENOH_ROUTER`; and with a message naming `ZENOH_REGION` when
//!   the value is empty (set but empty) or over 32 bytes (Zenoh's limit,
//!   counted in bytes).
//!
//! Variables are passed through `TransportConfig::from_lookup`, never the
//! process environment, so these tests may run in parallel. Whole
//! configurations are compared as the JSON `Config`'s `Display` writes.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16e_region_config_l1
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use moirai_network::transport::PeerId;
use moirai_network::transport_config::TransportConfig;
use moirai_network::HashMap;
use serde_json::Value;
use zenoh::Config;

/// The `LISTEN_PORT` written into the configurations below. A number in a
/// configuration only: nothing here opens a session or binds a port.
const PORT: u16 = 7701;
const ROUTER: &str = "tcp/10.0.0.9:7447";
const REGION: &str = "ZENOH_REGION";

fn transport_config(vars: &[(&str, &str)]) -> Result<TransportConfig, String> {
    let map: BTreeMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    TransportConfig::from_lookup(|name: &str| map.get(name).cloned()).map_err(|e| e.to_string())
}

/// The peers `PEERS` lists, as `model_node` parses them (`id:host:port`).
fn peers_of(vars: &[(&str, &str)]) -> HashMap<PeerId, String> {
    let mut peers: HashMap<PeerId, String> = HashMap::default();
    if let Some((_, text)) = vars.iter().find(|(k, _)| *k == "PEERS") {
        for spec in text.split(',').filter(|s| !s.is_empty()) {
            let parts: Vec<&str> = spec.split(':').collect();
            if parts.len() >= 3 {
                peers.insert(parts[0].to_string(), format!("{}:{}", parts[1], parts[2]));
            }
        }
    }
    peers
}

/// The Zenoh configuration a node with these variables and
/// `LISTEN_PORT=7701` opens; `Err` if they are refused at either step.
fn zenoh_config(vars: &[(&str, &str)]) -> Result<Config, String> {
    let config = transport_config(vars)?;
    config
        .zenoh_config(PORT, &peers_of(vars))
        .map_err(|e| e.to_string())
}

fn built(vars: &[(&str, &str)]) -> Config {
    zenoh_config(vars).unwrap_or_else(|e| panic!("{vars:?} refused: {e}"))
}

fn whole(config: &Config) -> Value {
    let text = config.to_string();
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

fn region_name(config: &Config) -> Value {
    let text = config
        .get_json("region_name")
        .unwrap_or_else(|e| panic!("get_json(region_name): {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("get_json(region_name) = {text}: {e}"))
}

/// The paths where two JSON documents differ.
fn differences(left: &Value, right: &Value, path: &str, out: &mut Vec<String>) {
    match (left, right) {
        (Value::Object(a), Value::Object(b)) => {
            for key in a.keys().chain(b.keys().filter(|k| !a.contains_key(*k))) {
                match (a.get(key), b.get(key)) {
                    (Some(x), Some(y)) => differences(x, y, &format!("{path}/{key}"), out),
                    (x, y) => out.push(format!("{path}/{key}: {x:?} against {y:?}")),
                }
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (i, (x, y)) in a.iter().zip(b).enumerate() {
                differences(x, y, &format!("{path}/{i}"), out);
            }
        }
        (a, b) if a == b => {}
        (a, b) => out.push(format!("{path}: {a} against {b}")),
    }
}

/// The refusal of these variables at either step, if any.
fn refusal(vars: &[(&str, &str)]) -> Option<String> {
    zenoh_config(vars).err()
}

/// A Zenoh configuration file that keeps a node on loopback with scouting
/// off, written once per call under the system's temporary directory.
fn config_file(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nw16e-l1-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let path = dir.join(name);
    std::fs::write(
        &path,
        r#"{
  mode: "peer",
  listen: { endpoints: ["tcp/127.0.0.1:0"] },
  connect: { endpoints: [] },
  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
}"#,
    )
    .expect("the file is written");
    path
}

/// The region names a node may claim: plain, the longest ASCII, the
/// longest in two-byte characters, JSON's special characters, an inner
/// space.
const NAMES: &[&str] = &[
    "core",
    "site-h",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}",
    "a\"b\\c",
    "site h",
];

/// Peer mode beside a router, in the forms the claim accepts.
fn accepted_forms() -> Vec<Vec<(&'static str, &'static str)>> {
    let base = vec![("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_ROUTER", ROUTER)];
    let mut with_peer = base.clone();
    with_peer.push(("ZENOH_MODE", "peer"));
    let mut with_empty_peers = base.clone();
    with_empty_peers.push(("PEERS", ""));
    let mut with_session = base.clone();
    with_session.push(("SESSION_ID", "nw16e"));
    vec![base, with_peer, with_empty_peers, with_session]
}

#[test]
fn na_zenoh_region_beside_a_router_in_peer_mode_is_the_region_name_and_changes_nothing_else() {
    for form in accepted_forms() {
        let without = built(&form);
        for name in NAMES {
            let mut vars = form.clone();
            vars.push((REGION, name));
            let with = zenoh_config(&vars).unwrap_or_else(|e| panic!("NA: {vars:?} refused: {e}"));
            assert_eq!(
                region_name(&with),
                Value::String(name.to_string()),
                "NA: {vars:?} builds a configuration whose region_name is not `{name}`"
            );
            let (mut a, mut b) = (whole(&with), whole(&without));
            for doc in [&mut a, &mut b] {
                if let Value::Object(members) = doc {
                    members.remove("region_name");
                }
            }
            let mut diff = Vec::new();
            differences(&a, &b, "", &mut diff);
            assert!(
                diff.is_empty(),
                "NA: {vars:?} changes more than region_name against {form:?}:\n{}",
                diff.join("\n")
            );
        }
    }
}

#[test]
fn nb_without_zenoh_region_the_region_name_stays_unset() {
    let file = config_file("nb.json5");
    let file = file.to_str().expect("a UTF-8 path");
    let mut forms = accepted_forms();
    forms.push(vec![("MOIRAI_TRANSPORT", "zenoh")]);
    forms.push(vec![
        ("MOIRAI_TRANSPORT", "zenoh"),
        ("ZENOH_ROUTER", ROUTER),
        ("ZENOH_MODE", "client"),
    ]);
    forms.push(vec![
        ("MOIRAI_TRANSPORT", "zenoh"),
        ("PEERS", "b:127.0.0.1:7702"),
    ]);
    forms.push(vec![("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_CONFIG", file)]);
    for vars in forms {
        assert_eq!(
            region_name(&built(&vars)),
            Value::Null,
            "NB: {vars:?}, without ZENOH_REGION, builds a configuration with a region_name"
        );
    }
}

/// `vars` must be refused at either step with a message naming `names`.
#[track_caller]
fn refused(point: &str, vars: &[(&str, &str)], names: &[&str]) {
    match refusal(vars) {
        None => panic!("{point}: {vars:?} is accepted; ZENOH_REGION must be refused there"),
        Some(message) => assert!(
            names.iter().all(|name| message.contains(name)),
            "{point}: {vars:?} is refused, but the message names not all of {names:?}: {message}"
        ),
    }
}

#[test]
fn nc_peers_zenoh_region_is_refused_beside_peers() {
    for peers in ["b:127.0.0.1:7702", "b:127.0.0.1:7702,c:10.0.0.3:7703"] {
        refused(
            "NC-PEERS",
            &[
                ("MOIRAI_TRANSPORT", "zenoh"),
                ("ZENOH_ROUTER", ROUTER),
                ("PEERS", peers),
                (REGION, "core"),
            ],
            &[REGION, "PEERS"],
        );
    }
}

#[test]
fn nc_client_zenoh_region_is_refused_beside_client_mode() {
    refused(
        "NC-CLIENT",
        &[
            ("MOIRAI_TRANSPORT", "zenoh"),
            ("ZENOH_ROUTER", ROUTER),
            ("ZENOH_MODE", "client"),
            (REGION, "core"),
        ],
        &[REGION, "ZENOH_MODE"],
    );
}

#[test]
fn nc_config_zenoh_region_is_refused_beside_zenoh_config() {
    let file = config_file("nc.json5");
    let file = file.to_str().expect("a UTF-8 path");
    refused(
        "NC-CONFIG",
        &[
            ("MOIRAI_TRANSPORT", "zenoh"),
            ("ZENOH_CONFIG", file),
            (REGION, "core"),
        ],
        &[REGION, "ZENOH_CONFIG"],
    );
}

#[test]
fn nc_router_zenoh_region_is_refused_without_a_router() {
    for extra in [None, Some(("ZENOH_MODE", "peer")), Some(("PEERS", ""))] {
        let mut vars = vec![("MOIRAI_TRANSPORT", "zenoh"), (REGION, "core")];
        vars.extend(extra);
        refused("NC-ROUTER", &vars, &[REGION, "ZENOH_ROUTER"]);
    }
}

#[test]
fn nc_empty_zenoh_region_is_refused_when_set_but_empty() {
    refused(
        "NC-EMPTY",
        &[
            ("MOIRAI_TRANSPORT", "zenoh"),
            ("ZENOH_ROUTER", ROUTER),
            (REGION, ""),
        ],
        &[REGION],
    );
}

#[test]
fn nc_long_zenoh_region_is_refused_over_32_bytes() {
    let long_ascii = "x".repeat(33);
    let long_three_byte = "\u{20ac}".repeat(11); // 11 characters, 33 bytes
    let long_two_byte = "\u{e9}".repeat(17); // 17 characters, 34 bytes
    for name in [long_ascii.as_str(), &long_three_byte, &long_two_byte] {
        refused(
            "NC-LONG",
            &[
                ("MOIRAI_TRANSPORT", "zenoh"),
                ("ZENOH_ROUTER", ROUTER),
                (REGION, name),
            ],
            &[REGION],
        );
    }
}
