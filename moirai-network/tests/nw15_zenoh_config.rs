//! NW.15, Z6 (L1), and two L0 checks (vault `spec/items/NW.15.md`): the
//! transport is chosen by `MOIRAI_TRANSPORT`, TCP when unset, and the Zenoh
//! session's configuration is built as sections 6.1 to 6.3 of the NW.14 note
//! say; `zenoh` is pinned as Q4 says; the backend's code names no frame
//! variant but the two it produces.
//!
//! The configuration is read back with `zenoh::Config::get_json`. Zenoh
//! leaves a key nobody set `null` and applies its own default when the
//! session starts, so each key is read for its effect, with Zenoh 1.10.1's
//! defaults filled in from `zenoh-config/src/defaults.rs`: mode `peer`;
//! multicast scouting on, at `224.0.0.224:7446`; gossip on, one hop; a
//! peer listening on `tcp/[::]:0`, a router on `tcp/[::]:7447`, a client on
//! nothing; no endpoint to connect to. A builder that leaves multicast
//! scouting `null` has left it on.
//!
//! Variables are passed through `TransportConfig::from_lookup`, never the
//! process environment, so these tests may run in parallel.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_zenoh_config
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use moirai_network::transport::PeerId;
use moirai_network::transport_config::{TransportConfig, TransportKind, DEFAULT_TRANSPORT};
use moirai_network::HashMap;
use serde_json::Value;
use zenoh::Config;

/// The `LISTEN_PORT` written into the configurations below. A number in a
/// configuration only: nothing here opens a session or binds a port.
const PORT: u16 = 7701;
const ROUTER: &str = "tcp/10.0.0.9:7447";
/// Zenoh's (and Ledgera's) multicast scouting port, which Moirai must not
/// share (note 6.1).
const ZENOH_SCOUTING_PORT: u16 = 7446;

fn transport_config(vars: &[(&str, &str)]) -> Result<TransportConfig, String> {
    let map: BTreeMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    TransportConfig::from_lookup(|name: &str| map.get(name).cloned()).map_err(|e| e.to_string())
}

/// The Zenoh configuration a node with these variables, `LISTEN_PORT=7701`
/// and these `PEERS` opens; `Err` if the variables are refused at either step.
fn zenoh_config(vars: &[(&str, &str)], peers: &[(&str, &str)]) -> Result<Config, String> {
    let config = transport_config(vars)?;
    let peers: HashMap<PeerId, String> = peers
        .iter()
        .map(|(id, addr)| (id.to_string(), addr.to_string()))
        .collect();
    config.zenoh_config(PORT, &peers).map_err(|e| e.to_string())
}

fn built(vars: &[(&str, &str)], peers: &[(&str, &str)]) -> Config {
    zenoh_config(vars, peers).unwrap_or_else(|e| panic!("{vars:?} {peers:?} refused: {e}"))
}

fn raw(config: &Config, key: &str) -> Value {
    let text = config
        .get_json(key)
        .unwrap_or_else(|e| panic!("get_json({key}): {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("get_json({key}) = {text}: {e}"))
}

/// A configuration read for what the session will do.
#[derive(Debug)]
struct Effective {
    mode: String,
    listen: BTreeSet<String>,
    connect: BTreeSet<String>,
    multicast: bool,
    multicast_address: String,
    gossip: bool,
    gossip_multihop: bool,
}

fn for_mode(value: Value, mode: &str) -> Value {
    match value {
        Value::Object(per_mode) => per_mode.get(mode).cloned().unwrap_or(Value::Null),
        other => other,
    }
}

fn endpoints(value: Value, default: &[&str]) -> BTreeSet<String> {
    match value {
        Value::Null => default.iter().map(|e| e.to_string()).collect(),
        Value::Array(list) => list
            .iter()
            .map(|e| e.as_str().expect("an endpoint string").to_string())
            .collect(),
        other => panic!("endpoints that are neither a list nor null: {other}"),
    }
}

fn flag(value: Value, default: bool, key: &str) -> bool {
    match value {
        Value::Null => default,
        Value::Bool(b) => b,
        other => panic!("{key} is {other}"),
    }
}

fn effective(config: &Config) -> Effective {
    let mode = match raw(config, "mode") {
        Value::Null => "peer".to_string(),
        Value::String(mode) => mode,
        other => panic!("mode is {other}"),
    };
    let listen_default: &[&str] = match mode.as_str() {
        "router" => &["tcp/[::]:7447"],
        "peer" => &["tcp/[::]:0"],
        _ => &[],
    };
    Effective {
        listen: endpoints(
            for_mode(raw(config, "listen/endpoints"), &mode),
            listen_default,
        ),
        connect: endpoints(for_mode(raw(config, "connect/endpoints"), &mode), &[]),
        multicast: flag(raw(config, "scouting/multicast/enabled"), true, "multicast"),
        multicast_address: match raw(config, "scouting/multicast/address") {
            Value::Null => "224.0.0.224:7446".to_string(),
            Value::String(address) => address,
            other => panic!("scouting/multicast/address is {other}"),
        },
        gossip: flag(raw(config, "scouting/gossip/enabled"), true, "gossip"),
        gossip_multihop: flag(raw(config, "scouting/gossip/multihop"), false, "multihop"),
        mode,
    }
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|i| i.to_string()).collect()
}

#[test]
fn moirai_transport_chooses_the_transport_and_tcp_is_the_default() {
    assert_eq!(
        DEFAULT_TRANSPORT,
        TransportKind::Tcp,
        "the default before NW.19 and NW.20"
    );
    let kind = |vars: &[(&str, &str)]| transport_config(vars).map(|c| c.kind());
    assert_eq!(kind(&[]), Ok(TransportKind::Tcp), "MOIRAI_TRANSPORT unset");
    assert_eq!(kind(&[("MOIRAI_TRANSPORT", "tcp")]), Ok(TransportKind::Tcp));
    assert_eq!(
        kind(&[("MOIRAI_TRANSPORT", "zenoh")]),
        Ok(TransportKind::Zenoh)
    );
    assert!(
        kind(&[("MOIRAI_TRANSPORT", "udp")]).is_err(),
        "an unknown transport is accepted"
    );
    assert_eq!(
        kind(&[("BOOTNODE_URL", "http://127.0.0.1:9/")]),
        Ok(TransportKind::Tcp),
        "a bootnode under TCP, as today"
    );
}

/// 6.1: peer mode, listening where today's replication listener binds,
/// dialling every `PEERS` entry, multicast scouting and gossip off.
#[test]
fn peer_mode_listens_where_tcp_did_and_dials_the_listed_peers_with_scouting_off() {
    let e = effective(&built(
        &[("MOIRAI_TRANSPORT", "zenoh")],
        &[("b", "127.0.0.1:7702"), ("c", "10.0.0.3:7703")],
    ));
    assert_eq!(e.mode, "peer");
    assert_eq!(e.listen, set(&["tcp/0.0.0.0:7701"]), "listen");
    assert_eq!(
        e.connect,
        set(&["tcp/127.0.0.1:7702", "tcp/10.0.0.3:7703"]),
        "connect"
    );
    assert!(!e.multicast, "multicast scouting is on: {e:?}");
    assert!(!e.gossip, "gossip is on without a router: {e:?}");

    let alone = effective(&built(&[("MOIRAI_TRANSPORT", "zenoh")], &[]));
    assert!(
        alone.connect.is_empty(),
        "a node with no PEERS dials {:?}",
        alone.connect
    );
    assert!(
        !alone.multicast && !alone.gossip,
        "a node with no PEERS scouts: {alone:?}"
    );
}

#[test]
fn the_session_chunk_is_session_id_and_defaults_to_default() {
    let unset = transport_config(&[("MOIRAI_TRANSPORT", "zenoh")]).expect("zenoh");
    assert_eq!(unset.session_id(), "default");
    let named = transport_config(&[("MOIRAI_TRANSPORT", "zenoh"), ("SESSION_ID", "s-7")])
        .expect("zenoh with SESSION_ID");
    assert_eq!(named.session_id(), "s-7");
}

/// 6.2: a router introduces a peer-mode node: it still listens, dials the
/// router (and any `PEERS`), keeps multicast off and turns gossip on, one hop.
#[test]
fn a_router_introduces_a_peer_mode_node_with_gossip_on_and_multicast_off() {
    let e = effective(&built(
        &[("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_ROUTER", ROUTER)],
        &[],
    ));
    assert_eq!(e.mode, "peer", "a node introduced by a router is a peer");
    assert_eq!(e.listen, set(&["tcp/0.0.0.0:7701"]), "listen");
    assert_eq!(e.connect, set(&[ROUTER]), "connect");
    assert!(!e.multicast, "multicast scouting is on: {e:?}");
    assert!(
        e.gossip,
        "gossip is off, so the router introduces nobody: {e:?}"
    );
    assert!(!e.gossip_multihop, "gossip is multi-hop: {e:?}");

    let mixed = effective(&built(
        &[("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_ROUTER", ROUTER)],
        &[("b", "127.0.0.1:7702")],
    ));
    assert_eq!(
        mixed.connect,
        set(&[ROUTER, "tcp/127.0.0.1:7702"]),
        "router and PEERS"
    );
}

/// 6.2: client mode, everything through the router, listening on nothing;
/// refused without a router.
#[test]
fn client_mode_goes_through_the_router_and_is_refused_without_one() {
    let e = effective(&built(
        &[
            ("MOIRAI_TRANSPORT", "zenoh"),
            ("ZENOH_ROUTER", ROUTER),
            ("ZENOH_MODE", "client"),
        ],
        &[],
    ));
    assert_eq!(e.mode, "client");
    assert_eq!(e.connect, set(&[ROUTER]), "connect");
    assert!(e.listen.is_empty(), "a client listens on {:?}", e.listen);
    assert!(!e.multicast, "multicast scouting is on: {e:?}");
    assert!(
        zenoh_config(
            &[("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_MODE", "client")],
            &[]
        )
        .is_err(),
        "ZENOH_MODE=client without ZENOH_ROUTER is accepted"
    );
}

/// 6.1, Q1: `ZENOH_SCOUTING=multicast` turns multicast scouting on, on a
/// multicast address whose port is not Zenoh's 7446; no other value turns
/// any scouting on.
#[test]
fn multicast_scouting_is_the_only_opt_in_and_on_a_port_of_its_own() {
    let e = effective(&built(
        &[
            ("MOIRAI_TRANSPORT", "zenoh"),
            ("ZENOH_SCOUTING", "multicast"),
        ],
        &[],
    ));
    assert!(
        e.multicast,
        "ZENOH_SCOUTING=multicast left scouting off: {e:?}"
    );
    let (ip, port) = e
        .multicast_address
        .rsplit_once(':')
        .map(|(ip, port)| {
            (
                ip.trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>(),
                port.parse::<u16>(),
            )
        })
        .unwrap_or_else(|| {
            panic!(
                "a multicast address without a port: {}",
                e.multicast_address
            )
        });
    let ip = ip.unwrap_or_else(|err| panic!("{}: {err}", e.multicast_address));
    let port = port.unwrap_or_else(|err| panic!("{}: {err}", e.multicast_address));
    assert!(ip.is_multicast(), "{ip} is not a multicast group");
    assert_ne!(
        port, ZENOH_SCOUTING_PORT,
        "Moirai scouts on Zenoh's and Ledgera's port"
    );

    for other in ["gossip", "on", "true", "MULTICAST"] {
        match zenoh_config(
            &[("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_SCOUTING", other)],
            &[],
        ) {
            Err(_) => {}
            Ok(config) => {
                let e = effective(&config);
                assert!(
                    !e.multicast && !e.gossip,
                    "ZENOH_SCOUTING={other} turned scouting on: {e:?}"
                );
            }
        }
    }
}

fn scratch_file(name: &str, text: &str) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    std::fs::write(&path, text).expect("write a configuration file");
    path
}

const FILE: &str = r#"{
  mode: "peer",
  listen: { endpoints: ["tcp/127.0.0.1:7999"] },
  connect: { endpoints: ["tcp/127.0.0.1:7998"] },
  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
  metadata: { name: "nw15-as-is" },
}"#;

/// 6.3: `ZENOH_CONFIG` is used as is: what the file says, and not the
/// `LISTEN_PORT` or `PEERS` the node was also given. It is refused beside
/// `ZENOH_ROUTER`, `ZENOH_MODE` or `ZENOH_SCOUTING`, which would change it.
#[test]
fn zenoh_config_is_used_as_is_and_refused_beside_the_variables_that_would_change_it() {
    let path = scratch_file("nw15_as_is.json5", FILE);
    let path = path.to_str().expect("a UTF-8 path");
    let from_file = Config::from_file(path).expect("the file is a Zenoh configuration");
    let used = built(
        &[("MOIRAI_TRANSPORT", "zenoh"), ("ZENOH_CONFIG", path)],
        &[("b", "127.0.0.1:7702")],
    );
    for key in [
        "mode",
        "listen/endpoints",
        "connect/endpoints",
        "scouting/multicast/enabled",
        "scouting/gossip/enabled",
        "metadata",
    ] {
        assert_eq!(
            raw(&used, key),
            raw(&from_file, key),
            "{key} differs from the file's"
        );
    }
    let e = effective(&used);
    assert!(
        !e.connect.contains("tcp/127.0.0.1:7702"),
        "PEERS were added to the file's"
    );
    assert!(
        !e.listen.contains("tcp/0.0.0.0:7701"),
        "LISTEN_PORT was added to the file's"
    );

    for (var, value) in [
        ("ZENOH_ROUTER", ROUTER),
        ("ZENOH_MODE", "client"),
        ("ZENOH_SCOUTING", "multicast"),
    ] {
        assert!(
            zenoh_config(
                &[
                    ("MOIRAI_TRANSPORT", "zenoh"),
                    ("ZENOH_CONFIG", path),
                    (var, value)
                ],
                &[],
            )
            .is_err(),
            "ZENOH_CONFIG beside {var} is accepted"
        );
    }
}

/// Q7: `BOOTNODE_URL` under Zenoh is refused, naming the way out.
#[test]
fn bootnode_url_is_refused_under_zenoh_naming_moirai_transport_tcp() {
    match zenoh_config(
        &[
            ("MOIRAI_TRANSPORT", "zenoh"),
            ("BOOTNODE_URL", "http://127.0.0.1:9/"),
        ],
        &[],
    ) {
        Err(why) => assert!(
            why.contains("MOIRAI_TRANSPORT=tcp"),
            "the refusal does not name MOIRAI_TRANSPORT=tcp: {why}"
        ),
        Ok(_) => panic!("BOOTNODE_URL under Zenoh is accepted"),
    }
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The `zenoh` declaration among the crate's normal dependencies, without
/// whitespace: an inline table under `[dependencies]` or a
/// `[dependencies.zenoh]` table.
fn zenoh_declaration(manifest: &str) -> String {
    let mut section = String::new();
    let mut taken = String::new();
    let mut depth = 0i32;
    let mut inside_table = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if depth == 0 && trimmed.starts_with('[') {
            section = trimmed.to_string();
            inside_table = section == "[dependencies.zenoh]";
            continue;
        }
        let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
        if inside_table {
            if !compact.starts_with('#') {
                taken.push_str(&compact);
                taken.push(',');
            }
            continue;
        }
        if depth > 0 || (section == "[dependencies]" && compact.starts_with("zenoh=")) {
            let code = compact.split('#').next().unwrap_or_default();
            depth += code.matches('{').count() as i32 + code.matches('[').count() as i32;
            depth -= code.matches('}').count() as i32 + code.matches(']').count() as i32;
            taken.push_str(code);
        }
    }
    taken
}

/// Q4: `zenoh = "=1.10.1"`, default features off, features `transport_tcp`
/// and `unstable`, a normal dependency of moirai-network; and the lock file
/// resolves exactly 1.10.1 with Zenoh's TCP link and no other.
#[test]
fn zenoh_is_pinned_exactly_with_tcp_only_features() {
    let manifest = std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("Cargo.toml");
    let zenoh = zenoh_declaration(&manifest);
    assert!(
        !zenoh.is_empty(),
        "no zenoh among moirai-network's [dependencies]"
    );
    assert!(
        zenoh.contains(r#"version="=1.10.1""#),
        "zenoh is not pinned =1.10.1: {zenoh}"
    );
    assert!(
        zenoh.contains("default-features=false") || zenoh.contains("default_features=false"),
        "zenoh keeps its default features: {zenoh}"
    );
    let features: BTreeSet<String> = zenoh
        .split_once("features=[")
        .and_then(|(_, rest)| rest.split_once(']'))
        .map(|(list, _)| {
            list.split(',')
                .map(|f| f.trim_matches('"').to_string())
                .filter(|f| !f.is_empty())
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        features,
        set(&["transport_tcp", "unstable"]),
        "zenoh's features: {zenoh}"
    );

    let lock = std::fs::read_to_string(manifest_dir().join("../Cargo.lock")).expect("Cargo.lock");
    let mut packages: Vec<(String, String)> = Vec::new();
    for block in lock.split("[[package]]").skip(1) {
        let field = |name: &str| {
            block
                .lines()
                .find_map(|l| l.strip_prefix(&format!("{name} = \"")))
                .map(|v| v.trim_end_matches('"').to_string())
                .unwrap_or_default()
        };
        packages.push((field("name"), field("version")));
    }
    let versions: Vec<&String> = packages
        .iter()
        .filter(|(name, _)| name == "zenoh")
        .map(|(_, version)| version)
        .collect();
    assert_eq!(versions, vec!["1.10.1"], "zenoh in Cargo.lock");
    let links: BTreeSet<String> = packages
        .iter()
        .filter(|(name, _)| name.starts_with("zenoh-link"))
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(
        links,
        set(&["zenoh-link", "zenoh-link-commons", "zenoh-link-tcp"]),
        "Zenoh links in Cargo.lock beyond TCP"
    );
    assert!(
        packages.iter().all(|(name, _)| name != "quinn"),
        "QUIC (quinn) is in Cargo.lock"
    );
}

/// The variants named right after one occurrence of `TransportMessage`:
/// `::Event`, `::<O>::Event`, `::{Hello, Goodbye}`, `::*` (named as `*`), or
/// an alias (`as M`, named as `as`). Empty for a type or a plain import.
fn variants_named(after: &str) -> Vec<String> {
    let mut rest = after.trim_start();
    if rest.starts_with("as ") {
        return vec!["as".to_string()];
    }
    let Some(path) = rest.strip_prefix("::") else {
        return Vec::new();
    };
    rest = path.trim_start();
    if rest.starts_with('<') {
        let mut depth = 0;
        let mut end = rest.len();
        for (i, c) in rest.char_indices() {
            match c {
                '<' => depth += 1,
                '>' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        rest = rest[end..].trim_start();
        match rest.strip_prefix("::") {
            Some(path) => rest = path.trim_start(),
            None => return Vec::new(),
        }
    }
    if rest.starts_with('*') {
        return vec!["*".to_string()];
    }
    if let Some(list) = rest.strip_prefix('{') {
        return list
            .split('}')
            .next()
            .unwrap_or_default()
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect();
    }
    let ident: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        Vec::new()
    } else {
        vec![ident]
    }
}

fn rust_files(at: &Path, into: &mut Vec<PathBuf>) {
    if at.is_file() {
        into.push(at.to_path_buf());
    } else if let Ok(entries) = std::fs::read_dir(at) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() || path.extension().is_some_and(|e| e == "rs") {
                rust_files(&path, into);
            }
        }
    }
}

/// 3.1: the backend carries `TransportMessage` as opaque JSON, so its code
/// names no variant but the two it produces, `Hello` and `Goodbye`, which
/// both integration branches have unchanged; a variant named there would not
/// compile on one branch or the other, or would make the backend read frames
/// it only has to carry. Comments are not read.
#[test]
fn the_backend_names_only_the_frame_variants_it_produces() {
    let src = manifest_dir().join("src");
    let mut files = Vec::new();
    for path in [
        src.join("zenoh_transport.rs"),
        src.join("zenoh_transport"),
        src.join("any_transport.rs"),
        src.join("transport_config.rs"),
    ] {
        rust_files(&path, &mut files);
    }
    assert!(
        files
            .iter()
            .any(|f| f.to_string_lossy().contains("zenoh_transport")),
        "no zenoh_transport source under {}",
        src.display()
    );
    let mut named = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("a source file");
        for (number, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            for (at, _) in code.match_indices("TransportMessage") {
                let after = &code[at + "TransportMessage".len()..];
                if variants_named(after)
                    .iter()
                    .any(|v| v != "Hello" && v != "Goodbye")
                {
                    named.push(format!(
                        "{}:{}: {}",
                        file.display(),
                        number + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
    assert!(
        named.is_empty(),
        "the backend names frame variants it does not produce:\n{}",
        named.join("\n")
    );
}
