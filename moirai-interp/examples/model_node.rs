//! Network node for the interpreted model plane: **one** binary, hosting a
//! model of any metamodel it holds a descriptor for.
//!
//! Runs one replica with TCP peer-to-peer sync and an HTTP API, hosting one
//! [`ModelLog`] per model it is asked to. Its generated twin,
//! `arachne/generated/json_crdt/examples/network_node.rs`, is the same file
//! for the same `GenericNode`; everything below is written against that one,
//! and the three differences are named where they are.
//!
//! # Usage
//!
//! ```bash
//! # Single node
//! REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 \
//!     cargo run -p moirai-interp --features network --example model_node
//!
//! # Two-node cluster
//! REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 PEERS=b:127.0.0.1:9002 \
//!     cargo run -p moirai-interp --features network --example model_node &
//! REPLICA_ID=b LISTEN_PORT=9002 HTTP_PORT=8082 PEERS=a:127.0.0.1:9001 \
//!     cargo run -p moirai-interp --features network --example model_node &
//! ```
//!
//! # Peer discovery
//!
//! Set `BOOTNODE_URL` to have the replica register with a bootnode session
//! directory every `RECONCILE_SECS` and dial whatever the roster returns. When
//! it is unset the replica behaves exactly as it always has: `PEERS` only,
//! dialled once. `PEERS` keeps working when both are set, as a static override.
//!
//! - `BOOTNODE_URL`   — unset means no discovery at all
//! - `SESSION_ID`     — session to join, default `default`
//! - `ADVERTISE_ADDR` — `host:port` peers dial, default `$HOSTNAME:$LISTEN_PORT`
//! - `RECONCILE_SECS` — re-register interval, default `5`
//!
//! # Monitoring
//!
//! Set `DASHBOARD_URL` to have the replica post what it delivers, and what the
//! CRDT did with it, to a `moirai-dashboard`. Outbound only, so it works from
//! behind NAT; unset means no thread and no request.
//!
//! - `DASHBOARD_URL`         — unset means no reporting at all
//! - `DASHBOARD_INTERVAL_MS` — gap between state snapshots, default `1000`
//!
//! # Metamodel discovery, and a metamodel arriving later
//!
//! The node serves metamodel descriptors, so a metamodel-agnostic client can
//! shape itself to whatever node it connects to. `METAMODEL_PATH` names one
//! descriptor file, served on `GET /api/metamodel`; unset, the node tries
//! `metamodel.json` in the working directory. `METAMODEL_DIR` names a
//! directory whose `.json` files are all served, listed on
//! `GET /api/metamodels` as `{nsURI, package, digest}` and offered at model
//! registration. Every descriptor is keyed by its digest, which is what a
//! registration's `metamodel_id` names. Both variables read exactly as they do
//! on the generated node, so a rig configured for one runs the other.
//!
//! `POST /api/metamodels` adds one **while the node runs**: the body is a
//! descriptor's text, and a model registers under it on the next request, with
//! nothing restarted and nothing regenerated. That is decision D8 and
//! criterion I-A6, and it is the half of the claim a generated binary cannot
//! answer at all — there, a new metamodel is a new Rust crate.
//!
//! A descriptor this node cannot read is not served: `json.metamodel.json`
//! needs the keyed and transparent containments v1 refuses (decision D6), and
//! it is skipped at start-up with the sentence the parser gave, or answered
//! 422 with it when it is posted.
//!
//! - `METAMODEL_PATH` — descriptor file, default `metamodel.json`
//! - `METAMODEL_DIR`  — directory of descriptors, unset means none
//!
//! # Model identity and the header
//!
//! A model is its log: the log id is the model id. A metamodel is its
//! descriptor: the digest is SHA-256 over the compact serialization of the
//! parsed descriptor, so formatting never changes an identity and an edit
//! always does. `POST /api/models` names a metamodel by that digest (or by
//! `{nsURI, digest}`); a create writes the log's **first and only opening
//! operation**, one [`ModelOp::Install`] carrying the model id, the digest and
//! the descriptor text, and a join writes nothing and receives it by transfer.
//!
//! This is the first difference from the generated node, and it is decision
//! D2. There, the header is a JSON document written one character at a time —
//! about a hundred and fifty `String.Insert` operations under `Object.Update`
//! keys. Here it is one operation, and it carries the metamodel itself rather
//! than a name for it, because what a joiner replaying a delta needs is the
//! bytes: causal delivery puts `Install` before everything else its creator
//! wrote, and a joiner by state transfer gets the parsed table inside the
//! serialized log.
//!
//! # Conformance: there is no guard here
//!
//! The second difference. The generated node installs an intake guard
//! (`enable_op_guard`) that checks every local operation against a `Schema`
//! parsed from the descriptor and held in a process-wide `SCHEMAS` static,
//! because its log type is a JSON document that would accept anything.
//!
//! This node installs none, and holds no such static, because
//! `ModelLog::is_enabled` **is** that check (criterion I-A9): the table
//! arrived with the log's first operation, so an operation naming a feature
//! its class does not declare, or writing a leaf with the wrong kind of
//! operation, is refused at the local intake by the log itself, and the
//! caller is answered `success: false`. Two replicas holding the same table
//! give the same verdict, and a remote operation is never refused — it is
//! applied, or counted in `unresolved`, which is the log's business and not
//! this file's.
//!
//! # Log identity
//!
//! Every replica of a session must host the same default log, and `LOG_ID`
//! names it: 32 lowercase hex characters, the same value on every replica.
//! Unset, the replica mints a fresh id and prints it — right for the replica
//! that creates a session, wrong for one joining it. This is the node's
//! *default* log, the one the unscoped routes serve; every model it hosts is
//! registered through `POST /api/models`.
//!
//! - `LOG_ID` — the default log this replica hosts; unset mints a fresh one
//!
//! # HTTP API
//!
//! - `GET  /api/models`               — the hosted models
//! - `POST /api/models`               — register a model: create, or join by id
//! - `GET  /api/metamodels`           — the descriptors this node holds
//! - `POST /api/metamodels`           — serve one more, now
//! - `GET  /api/model/<id>/state`     — that model's state, in canonical form
//! - `POST /api/model/<id>/op`        — submit an operation to that model
//! - `GET  /api/model/<id>/metamodel` — that model's descriptor
//! - `GET  /api/model/<id>/metrics`   — that model's counters
//! - `POST /api/op`        — submit an operation to the default log
//! - `GET  /api/state`     — query the default log's state
//! - `GET  /api/metamodel` — the first metamodel descriptor, when configured
//! - `GET  /api/metrics`   — causal-stability and log-size counters
//! - `GET  /api/health`    — health check
//! - `GET  /api/peers`     — list connected peers
//! - `POST /api/leave`     — deregister from the bootnode session
//! - `POST /api/pause/<id>`  — simulate disconnection from a peer
//! - `POST /api/resume/<id>` — resume and auto-sync with a peer
//! - `POST /api/pause-all`   — pause all peers
//! - `POST /api/resume-all`  — resume all peers

use std::collections::BTreeSet;
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use moirai_interp::{ModelLog, ModelOp};
use moirai_network::HashMap;
use moirai_network::dashboard::DashboardConfig;
use moirai_network::discovery::DiscoveryConfig;
use moirai_network::generic::{LogReplica, Node, ServedDescriptor};
use moirai_protocol::log_id::LogId;
use moirai_semantics::{from_descriptor, metamodel_digest};
use serde_json::{Value, json};

/// How other replicas reach this one's replication listener.
///
/// Not the same as what the process binds: in a container the bind is
/// `0.0.0.0:9001` while the reachable address is the container's name on the
/// user-defined network. Docker sets `HOSTNAME` to the container id, and
/// Compose registers that as a DNS alias, so it is the right default; override
/// with `ADVERTISE_ADDR` when a stable service name is wanted instead.
fn advertise_addr(listen_port: u16) -> String {
    env::var("ADVERTISE_ADDR").unwrap_or_else(|_| {
        let host = env::var("HOSTNAME")
            .ok()
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        format!("{host}:{listen_port}")
    })
}

/// A descriptor as the node serves it: keyed by its digest, which is what a
/// registration's `metamodel_id` names, and listed as
/// `{nsURI, package, digest}`.
///
/// The parse is not decoration. A descriptor this node serves is one a
/// [`ModelLog`] will accept from `Install`, so refusing it here — with the
/// parser's own sentence — is what keeps a model from being registered under
/// a metamodel whose first operation would then be refused by every replica.
/// It is also the whole of what the node layer means by *describing* a
/// descriptor: `moirai-network` calls this and reads nothing itself.
fn describe_descriptor(text: &str) -> Result<ServedDescriptor, String> {
    let parsed: Value = serde_json::from_str(text).map_err(|err| format!("not JSON: {err}"))?;
    let ns_uri = parsed
        .get("nsURI")
        .and_then(Value::as_str)
        .ok_or_else(|| "no `nsURI`".to_string())?;
    let package = parsed
        .get("package")
        .and_then(Value::as_str)
        .unwrap_or_default();
    from_descriptor(&parsed).map_err(|why| format!("no merge table: {why}"))?;
    let digest = metamodel_digest(&parsed);
    Ok(ServedDescriptor {
        listing: json!({ "nsURI": ns_uri, "package": package, "digest": digest }),
        key: digest,
        text: text.to_string(),
    })
}

/// The descriptor files of `dir`, in name order.
fn descriptor_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    Ok(paths)
}

/// The key a registration's `metamodel_id` names: the digest, given either as
/// `{"nsURI": ..., "digest": ...}` or as a bare string. An `nsURI` beside it
/// is not checked: the log is opened from the descriptor the digest names,
/// never from what the caller said.
fn descriptor_key(metamodel_id: &Value) -> Option<String> {
    match metamodel_id {
        Value::String(digest) => Some(digest.clone()),
        Value::Object(fields) => fields
            .get("digest")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

/// The operations that open a newly created model's log: exactly one, the
/// [`ModelOp::Install`] that installs the merge table and names what the model
/// is (decision D2). Written by the creating node and by nobody else — a
/// joiner receives it by transfer, or replays it first because causal delivery
/// puts it before everything else.
/// The descriptor a hosted log turned out to carry, for a log joined under a
/// metamodel this node holds no descriptor for.
///
/// This is the whole of the in-band half of metamodel distribution, and it is
/// three lines because everything under it was already built: the creator's
/// [`ModelOp::Install`] carries the descriptor's text, the log keeps it, and
/// state transfer and delta replay both bring it. `moirai-network` calls this
/// on a pending binding, hands what comes back to its own `add_metamodel`,
/// and so re-describes it through [`describe_descriptor`] above — which
/// recomputes the digest from the bytes, so a peer cannot make this node
/// serve a descriptor under a digest it does not hash to.
fn adopt_descriptor(replica: &LogReplica<ModelLog>) -> Option<String> {
    replica.state().descriptor().map(str::to_string)
}

fn install_ops(model_id: &LogId, descriptor: &ServedDescriptor) -> Vec<ModelOp> {
    vec![ModelOp::Install {
        model_id: model_id.to_string(),
        metamodel_id: descriptor.key.clone(),
        descriptor: descriptor.text.clone(),
    }]
}

fn main() {
    let replica_id = env::var("REPLICA_ID").unwrap_or_else(|_| "replica-a".to_string());
    let listen_port: u16 = env::var("LISTEN_PORT")
        .unwrap_or_else(|_| "9001".to_string())
        .parse()
        .expect("Invalid LISTEN_PORT");
    let http_port: Option<u16> = env::var("HTTP_PORT").ok().and_then(|p| p.parse().ok());
    let peers_str = env::var("PEERS").unwrap_or_default();

    // Parse PEERS=id:host:port,...
    let mut peer_addresses: HashMap<String, String> = HashMap::default();
    let mut all_members: Vec<String> = vec![replica_id.clone()];
    for spec in peers_str.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() >= 3 {
            let peer_id = parts[0].to_string();
            let addr = format!("{}:{}", parts[1], parts[2]);
            all_members.push(peer_id.clone());
            peer_addresses.insert(peer_id, addr);
        }
    }

    let member_refs: Vec<&str> = all_members.iter().map(|s| s.as_str()).collect();

    // The default log this replica hosts. Set, `LOG_ID` means join that log;
    // unset, a fresh id is minted and printed, which is right only for the
    // replica that creates the session — peers hosting a different log refuse
    // each other's events.
    let log_id = match env::var("LOG_ID") {
        Ok(raw) => LogId::parse(&raw).unwrap_or_else(|err| {
            eprintln!("[{replica_id}] invalid LOG_ID `{raw}`: {err}");
            std::process::exit(1);
        }),
        Err(_) => {
            let id = LogId::generate();
            eprintln!("[{replica_id}] log id: {id}");
            id
        }
    };

    let mut node = Node::<ModelLog>::new_with_log_id(
        replica_id.clone(),
        &member_refs,
        listen_port,
        peer_addresses,
        log_id,
    );

    node.enable_state_query();
    node.enable_state_transfer();

    // Metamodel discovery, on the same terms as everything below: no readable
    // descriptor, no `/api/metamodel` — the endpoint answers 404 exactly as it
    // always has. `METAMODEL_PATH` comes first, so the unscoped
    // `/api/metamodel` keeps answering with it; `METAMODEL_DIR` adds the rest.
    let mut descriptors: Vec<ServedDescriptor> = Vec::new();
    let mut held: BTreeSet<String> = BTreeSet::new();
    let metamodel_path = env::var("METAMODEL_PATH").ok();
    let metamodel_explicit = metamodel_path.is_some();
    let metamodel_path = metamodel_path.unwrap_or_else(|| "metamodel.json".to_string());
    match std::fs::read_to_string(&metamodel_path) {
        Ok(text) => match describe_descriptor(&text) {
            Ok(descriptor) => {
                eprintln!("[{replica_id}] serving metamodel descriptor from `{metamodel_path}`");
                held.insert(descriptor.key.clone());
                descriptors.push(descriptor);
            }
            Err(why) => {
                eprintln!(
                    "[{replica_id}] METAMODEL_PATH `{metamodel_path}` is not a descriptor \
                     this node can serve ({why}); not served"
                );
            }
        },
        Err(err) if metamodel_explicit => {
            eprintln!(
                "[{replica_id}] cannot read METAMODEL_PATH `{metamodel_path}`: {err}; \
                 /api/metamodel stays 404"
            );
        }
        Err(_) => {}
    }
    if let Some(dir) = env::var("METAMODEL_DIR").ok().filter(|dir| !dir.is_empty()) {
        match descriptor_files(Path::new(&dir)) {
            Ok(paths) => {
                for path in paths {
                    let described = std::fs::read_to_string(&path)
                        .map_err(|err| err.to_string())
                        .and_then(|text| describe_descriptor(&text));
                    match described {
                        // Usually the METAMODEL_PATH file seen again through
                        // its directory; the first copy is the one served.
                        Ok(descriptor) if held.contains(&descriptor.key) => {}
                        Ok(descriptor) => {
                            eprintln!(
                                "[{replica_id}] serving metamodel descriptor from `{}`",
                                path.display()
                            );
                            held.insert(descriptor.key.clone());
                            descriptors.push(descriptor);
                        }
                        Err(why) => {
                            eprintln!(
                                "[{replica_id}] skipping `{}` in METAMODEL_DIR: {why}",
                                path.display()
                            );
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("[{replica_id}] cannot read METAMODEL_DIR `{dir}`: {err}");
            }
        }
    }
    node.serve_metamodels(descriptors);
    node.enable_registration(descriptor_key, install_ops);
    // Decision D8: a descriptor this node was never started with is described
    // by the same function that read the ones on disk, and served from the
    // moment the event loop takes the command.
    node.enable_metamodel_upload(describe_descriptor);
    // The in-band half of the same decision: a model joined under a metamodel
    // this node holds no descriptor for is hosted anyway, and the descriptor
    // its first operation carries is served from the moment it lands. Needs
    // the upload hook above, because that is what re-derives the digest.
    node.enable_descriptor_adoption(adopt_descriptor);
    // No `enable_op_guard`: `ModelLog::is_enabled` is the structural check.
    // See the header.

    if let Some(port) = http_port {
        node.start_http(port);
    }

    // Discovery is opt-in. Unset `BOOTNODE_URL` and everything below behaves
    // exactly as it did before phase 1, which is what keeps the existing e2e
    // suite an honest guard rail.
    if let Ok(bootnode_url) = env::var("BOOTNODE_URL")
        && !bootnode_url.is_empty()
    {
        node.enable_discovery(DiscoveryConfig {
            bootnode_url,
            session: env::var("SESSION_ID").unwrap_or_else(|_| "default".to_string()),
            replica_id: replica_id.clone(),
            advertise_addr: advertise_addr(listen_port),
            interval: Duration::from_secs(
                env::var("RECONCILE_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(5),
            ),
        });
    }

    // Monitoring is opt-in for the same reason and on the same terms: no
    // `DASHBOARD_URL`, no thread, no outbound request, no delivery trace.
    if let Some(config) = DashboardConfig::from_env(&replica_id) {
        node.enable_dashboard(config);
    }

    // Give peers time to start, then connect
    thread::sleep(Duration::from_secs(2));
    node.connect();

    eprintln!(
        "[{}] Running. POST ops to http://localhost:{}/api/op",
        replica_id,
        http_port.unwrap_or(0)
    );

    node.run();
}
