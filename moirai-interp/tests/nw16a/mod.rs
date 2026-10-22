//! NW.16a, the part its loopback tests share (vault `spec/items/NW.16a.md`):
//! `model_node` processes started on NW.02's launcher with the transport a
//! test names, driven over HTTP, and Zenoh read from outside the nodes.
//!
//! Ground truth never comes from the code under test:
//!
//! - which transport a node runs is witnessed by a raw Zenoh session this
//!   module opens itself, which connects to the port the node reported in
//!   `PORTS_FILE` and must see the node's liveliness token
//!   `moirai/<session>/node/<id>` there (note 5.1); a node on TCP has no
//!   Zenoh session on that port, so it can never pass as one on Zenoh;
//! - which socket holds a port is read in the kernel's own tables,
//!   `/proc/net/tcp` and `/proc/net/tcp6` against `/proc/<pid>/fd`;
//! - what a node sends is read by a raw Zenoh observer subscribed to the
//!   keys the note fixes;
//! - what a node holds is read through its HTTP API, the class names of one
//!   SimpleUML model, each written by hand in the tests.
#![allow(dead_code)]

#[path = "../nw02/launch.rs"]
pub mod launch;
#[path = "../si01/mod.rs"]
pub mod si01;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use serde_json::{Value, json};
use zenoh::sample::{Sample, SampleKind};
use zenoh::{Config, Session, Wait};

pub use launch::{NodeSpec, Reported, alarms, read_ports_file, tail, write_atomically};

/// The default log every node of these tests hosts: 32 hex characters.
pub const LOG_ID: &str = "16a016a016a016a016a016a016a016a0";
pub const UML: &str = include_str!("../fixtures/uml.metamodel.json");

pub const POLL: Duration = Duration::from_millis(100);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
pub const MESH_TIMEOUT: Duration = Duration::from_secs(60);
pub const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a raw session may take to see a node's token once the node
/// runs.
pub const WITNESS_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a state that must not change is watched before it is believed.
pub const QUIET: Duration = Duration::from_secs(3);

static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

/// The transport a node is started on, as `MOIRAI_TRANSPORT` names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Tcp,
    Zenoh,
}

impl Transport {
    pub fn var(self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::Zenoh => "zenoh",
        }
    }
}

/// The `model_node` binary: `MOIRAI_E2E_INTERP_NODE_BIN`, or the workspace's
/// debug build.
pub fn node_binary() -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var("MOIRAI_E2E_INTERP_NODE_BIN") {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "MOIRAI_E2E_INTERP_NODE_BIN points at `{raw}`, which is not a file"
            ))
        };
    }
    let built =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/examples/model_node");
    if built.is_file() {
        Ok(built)
    } else {
        Err(format!(
            "no `model_node` at {}; build it with `cargo build -p moirai-interp --features \
             network --example model_node`",
            built.display()
        ))
    }
}

/// A fresh scratch directory for one scenario, with the SimpleUML descriptor
/// in `metamodels/`. Under `TMPDIR`, which the spec's commands point at the
/// SSD scratch.
pub fn scratch(tag: &str) -> Result<PathBuf, String> {
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("moirai-nw16a-{tag}-{}-{run}", std::process::id()));
    let metamodels = dir.join("metamodels");
    fs::create_dir_all(&metamodels).map_err(|e| format!("{}: {e}", metamodels.display()))?;
    fs::write(metamodels.join("uml.json"), UML).map_err(|e| e.to_string())?;
    Ok(dir)
}

/// The environment every node of these tests gets besides its ports and
/// peers.
pub fn node_env(transport: Transport, scratch: &Path) -> Vec<(String, String)> {
    vec![
        ("MOIRAI_TRANSPORT".into(), transport.var().into()),
        ("LOG_ID".into(), LOG_ID.into()),
        (
            "METAMODEL_DIR".into(),
            scratch.join("metamodels").to_string_lossy().into_owned(),
        ),
        ("HTTP_BIND_ADDR".into(), "127.0.0.1".into()),
    ]
}

/// Refuse a start whose nodes could scout outside an isolated network
/// namespace: a Zenoh variable inherited from the test process (NW.02's
/// launcher passes the environment on), or multicast scouting asked for
/// anywhere but in the spec's container.
pub fn guard_scouting(extra: &[(String, String)]) -> Result<(), String> {
    for name in [
        "ZENOH_SCOUTING",
        "ZENOH_CONFIG",
        "ZENOH_ROUTER",
        "ZENOH_MODE",
    ] {
        if std::env::var_os(name).is_some() && !extra.iter().any(|(k, _)| k == name) {
            return Err(format!(
                "the test process carries {name}, which every node would inherit; NW.16a sets                  the Zenoh variables per test"
            ));
        }
    }
    if extra
        .iter()
        .any(|(k, v)| k == "ZENOH_SCOUTING" && v == "multicast")
    {
        isolated().map_err(|why| {
            format!(
                "refusing to start a node with multicast scouting outside an isolated network                  namespace ({why}); run the spec's container command"
            )
        })?;
    }
    Ok(())
}

/// Whether this process runs where multicast cannot leave the machine: a
/// network namespace with no interface but `lo`, entered on purpose
/// (`NW16A_ISOLATED_NETNS=1`).
pub fn isolated() -> Result<(), String> {
    if std::env::var("NW16A_ISOLATED_NETNS").as_deref() != Ok("1") {
        return Err("NW16A_ISOLATED_NETNS is not 1".into());
    }
    let interfaces: Vec<String> = fs::read_dir("/sys/class/net")
        .map_err(|e| format!("/sys/class/net: {e}"))?
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    if interfaces != ["lo"] {
        return Err(format!(
            "the network namespace has interfaces {interfaces:?}, not lo alone"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// One node
// ---------------------------------------------------------------------------

pub struct Node {
    pub id: String,
    pub http: u16,
    pub listen: u16,
    pub pid: u32,
    pub transport: Transport,
    child: Option<Child>,
    pub log: PathBuf,
}

impl Node {
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    pub fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    pub fn tail(&self) -> String {
        tail(&self.log, 25)
    }

    /// Whether this node's log says it adopted a transferred state for
    /// `model`: `GenericNode::adopt_state`'s line, as ST.01's harness reads
    /// it.
    pub fn adopted(&self, model: &str) -> bool {
        self.log_text()
            .lines()
            .any(|line| line.contains("adopted state from") && line.contains(model))
    }

    /// Send `signal` (`STOP`, `CONT`, `KILL`) to the process.
    pub fn signal(&self, signal: &str) -> Result<(), String> {
        let status = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.pid.to_string())
            .status()
            .map_err(|e| format!("kill -{signal} {}: {e}", self.pid))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("kill -{signal} {} exited with {status}", self.pid))
        }
    }

    /// SIGKILL and reap.
    pub fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            // A stopped process takes a SIGKILL as well as a running one.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A node spawned on `LISTEN_PORT=0` and `HTTP_PORT=0` that has not been
/// told its peers yet: NW.02's protocol, step by step, so that a test can
/// look at the node between its report and its peers.
pub struct Pending {
    pub id: String,
    pub child: Child,
    pub log: PathBuf,
    pub ports: PathBuf,
    pub peers: PathBuf,
    pub transport: Transport,
}

/// Spawn `id` as NW.02's launcher does (`nw02/launch.rs`), with `env` on
/// top: ports chosen by the kernel and reported in `PORTS_FILE`, peers read
/// from `PEERS_FILE`.
pub fn spawn(
    binary: &Path,
    scratch: &Path,
    id: &str,
    transport: Transport,
    env: &[(String, String)],
) -> Result<Pending, String> {
    let log = scratch.join(format!(
        "{id}.{}.log",
        RUN_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let ports = log.with_extension("ports.json");
    let peers = log.with_extension("peers");
    let out = fs::File::create(&log).map_err(|e| e.to_string())?;
    let err_out = out.try_clone().map_err(|e| e.to_string())?;
    let child = Command::new(binary)
        .env("REPLICA_ID", id)
        .env("LISTEN_PORT", "0")
        .env("HTTP_PORT", "0")
        .env("PORTS_FILE", &ports)
        .env("PEERS_FILE", &peers)
        .env_remove("PEERS")
        .env_remove("BOOTNODE_URL")
        .env_remove("DASHBOARD_URL")
        .env_remove("METAMODEL_PATH")
        .env_remove("ADVERTISE_ADDR")
        .env_remove("ZENOH_SCOUTING")
        .env_remove("ZENOH_ROUTER")
        .env_remove("ZENOH_MODE")
        .env_remove("ZENOH_CONFIG")
        .env_remove("SESSION_ID")
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|e| format!("spawning {id}: {e}"))?;
    Ok(Pending {
        id: id.to_string(),
        child,
        log,
        ports,
        peers,
        transport,
    })
}

impl Pending {
    /// The ports the node reported, once it has; checked as NW.02 checks
    /// them (its own id and pid, two distinct ports that are not 0).
    pub fn await_ports(&mut self) -> Result<Reported, String> {
        let deadline = Instant::now() + launch::PORTS_TIMEOUT;
        loop {
            if let Some(parsed) = read_ports_file(&self.ports) {
                let reported = parsed.map_err(|why| format!("{}'s ports file: {why}", self.id))?;
                let http = reported.http_port.unwrap_or(0);
                if reported.replica_id != self.id
                    || reported.pid != self.child.id()
                    || reported.listen_port == 0
                    || http == 0
                    || reported.listen_port == http
                {
                    return Err(format!(
                        "{}'s ports file is not its own or names an unbound port: {reported:?}",
                        self.id
                    ));
                }
                return Ok(reported);
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!(
                    "{} exited with {status} before reporting its ports; alarms:\n{}\nlast log \
                     lines:\n{}",
                    self.id,
                    alarms(&self.log),
                    tail(&self.log, 25)
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never reported its ports:\n{}",
                    self.id,
                    tail(&self.log, 25)
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// Tell the node its peers, in NW.02's text: `id:127.0.0.1:port,...`.
    pub fn tell_peers(&self, peers: &[(&str, u16)]) -> Result<(), String> {
        let text = peers
            .iter()
            .map(|(id, port)| format!("{id}:127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",");
        write_atomically(&self.peers, &text)
    }

    pub fn into_node(self, reported: &Reported) -> Node {
        Node {
            id: self.id,
            http: reported.http_port.expect("checked in await_ports"),
            listen: reported.listen_port,
            pid: reported.pid,
            transport: self.transport,
            child: Some(self.child),
            log: self.log,
        }
    }
}

// ---------------------------------------------------------------------------
// A cluster
// ---------------------------------------------------------------------------

pub struct Cluster {
    pub transport: Transport,
    pub nodes: Vec<Node>,
    pub scratch: PathBuf,
    pub binary: PathBuf,
    http: reqwest::blocking::Client,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }
}

impl Cluster {
    /// `ids` started by NW.02's launcher on `transport`, meshed by
    /// `PEERS_FILE`, answering `/api/health` as themselves, each listing
    /// every other one `Connected`; on Zenoh, each witnessed on Zenoh
    /// ([`witness`]).
    pub fn start(transport: Transport, tag: &str, ids: &[&str]) -> Result<Cluster, String> {
        Cluster::start_with(transport, tag, ids, &[])
    }

    /// [`Cluster::start`], with `extra` added to every node's environment.
    pub fn start_with(
        transport: Transport,
        tag: &str,
        ids: &[&str],
        extra: &[(String, String)],
    ) -> Result<Cluster, String> {
        guard_scouting(extra)?;
        let binary = node_binary()?;
        let scratch = scratch(tag)?;
        let mut env = node_env(transport, &scratch);
        env.extend(extra.iter().cloned());
        let specs: Vec<NodeSpec> = ids
            .iter()
            .map(|id| NodeSpec {
                id: id.to_string(),
                env: env.clone(),
            })
            .collect();
        let launched = launch::launch(&binary, &scratch, &specs)?;
        let nodes = launched
            .into_iter()
            .map(|l| Node {
                pid: l.child.id(),
                id: l.id,
                http: l.http,
                listen: l.listen,
                transport,
                child: Some(l.child),
                log: l.log,
            })
            .collect();
        let cluster = Cluster {
            transport,
            nodes,
            scratch,
            binary,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| e.to_string())?,
        };
        for index in 0..cluster.nodes.len() {
            cluster.await_health(index)?;
        }
        cluster.await_mesh(&(0..cluster.nodes.len()).collect::<Vec<_>>())?;
        if transport == Transport::Zenoh {
            for node in &cluster.nodes {
                witness(node)?;
            }
        }
        Ok(cluster)
    }

    /// A cluster of nodes a test started itself, step by step.
    pub fn from_nodes(
        transport: Transport,
        nodes: Vec<Node>,
        scratch: PathBuf,
        binary: PathBuf,
    ) -> Result<Cluster, String> {
        Ok(Cluster {
            transport,
            nodes,
            scratch,
            binary,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| e.to_string())?,
        })
    }

    /// Start one more node, `id`, whose peers are the nodes at `peers`, by
    /// NW.02's protocol; healthy, and on Zenoh witnessed. Answers its index.
    pub fn add(&mut self, id: &str, peers: &[usize]) -> Result<usize, String> {
        let env = node_env(self.transport, &self.scratch);
        let mut pending = spawn(&self.binary, &self.scratch, id, self.transport, &env)?;
        let reported = pending.await_ports()?;
        let list: Vec<(&str, u16)> = peers
            .iter()
            .map(|&i| (self.nodes[i].id.as_str(), self.nodes[i].listen))
            .collect();
        pending.tell_peers(&list)?;
        self.nodes.push(pending.into_node(&reported));
        let index = self.nodes.len() - 1;
        self.await_health(index)?;
        if self.transport == Transport::Zenoh {
            witness(&self.nodes[index])?;
        }
        Ok(index)
    }

    pub fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .http
            .get(self.nodes[node].url(path))
            .send()
            .map_err(|e| format!("GET {path} on {}: {e}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|e| e.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    pub fn post(&self, node: usize, path: &str, body: String) -> Result<(u16, Value), String> {
        let response = self
            .http
            .post(self.nodes[node].url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|e| format!("POST {path} on {}: {e}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|e| e.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    pub fn await_health(&self, index: usize) -> Result<(), String> {
        let node = &self.nodes[index];
        let deadline = Instant::now() + HEALTH_TIMEOUT;
        loop {
            if let Ok((200, body)) = self.get(index, "/api/health") {
                if body["replica_id"] == Value::String(node.id.clone()) {
                    return Ok(());
                }
                return Err(format!(
                    "the HTTP port {} reported by {} answered as {body}",
                    node.http, node.id
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never answered /api/health; alarms:\n{}\nlast log lines:\n{}",
                    node.id,
                    alarms(&node.log),
                    node.tail()
                ));
            }
            thread::sleep(POLL);
        }
    }

    /// The status `node` gives `peer` in `/api/peers`, if it lists it.
    pub fn status_of(&self, node: usize, peer: &str) -> Option<String> {
        let (_, peers) = self.get(node, "/api/peers").ok()?;
        peers["peers"].as_array()?.iter().find_map(|p| {
            (p["id"] == Value::String(peer.to_string()))
                .then(|| p["status"].as_str().unwrap_or("").to_string())
        })
    }

    /// The ids `node` lists in `/api/peers`, and their statuses.
    pub fn peers_of(&self, node: usize) -> Vec<(String, String)> {
        match self.get(node, "/api/peers") {
            Ok((200, peers)) => peers["peers"]
                .as_array()
                .map(|list| {
                    list.iter()
                        .map(|p| {
                            (
                                p["id"].as_str().unwrap_or("").to_string(),
                                p["status"].as_str().unwrap_or("").to_string(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Wait until every node of `indices` lists every other one of them
    /// `Connected`.
    pub fn await_mesh(&self, indices: &[usize]) -> Result<(), String> {
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            let missing: Vec<String> = indices
                .iter()
                .flat_map(|&i| {
                    indices
                        .iter()
                        .filter(move |&&j| j != i)
                        .filter_map(move |&j| {
                            let status = self.status_of(i, &self.nodes[j].id);
                            (status.as_deref() != Some("Connected")).then(|| {
                                format!(
                                    "{} reads {} {status:?}",
                                    self.nodes[i].id, self.nodes[j].id
                                )
                            })
                        })
                })
                .collect();
            if missing.is_empty() {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "the mesh never formed: {}\n{}",
                    missing.join("; "),
                    self.nodes[indices[0]].tail()
                ));
            }
            thread::sleep(POLL);
        }
    }

    pub fn control(&self, node: usize, path: &str) -> Result<(), String> {
        let (status, body) = self.post(node, path, String::new())?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!(
                "{path} on {} answered {status}: {body}",
                self.nodes[node].id
            ))
        }
    }

    /// Create the SimpleUML model on `node`; answers its id.
    pub fn create_model(&self, node: usize) -> Result<String, String> {
        let body = json!({ "metamodel_id": digest(UML) });
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "creating on {} answered {status}: {answer}",
                self.nodes[node].id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    /// Join `model` by its id on `node`.
    pub fn join_model(&self, node: usize, model: &str) -> Result<(), String> {
        let body = json!({ "model_id": model, "metamodel_id": digest(UML) });
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!(
                "joining {model} on {} answered {status}: {answer}",
                self.nodes[node].id
            ))
        }
    }

    /// Wait until `node` holds the model's table (its `Install` delivered).
    pub fn await_table(&self, node: usize, model: &str) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Ok((200, metrics)) = self.get(node, &format!("/api/model/{model}/metrics"))
                && metrics["delivered_ops"].as_u64().unwrap_or(0) >= 1
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never received the table of {model}:\n{}",
                    self.nodes[node].id,
                    self.nodes[node].tail()
                ));
            }
            thread::sleep(POLL);
        }
    }

    pub fn submit(&self, node: usize, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|e| e.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!(
                "{} refused an operation ({status}): {answer}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    /// Post one operation to the node's default log (`/api/op`).
    pub fn submit_default(&self, node: usize, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|e| e.to_string())?;
        let (status, answer) = self.post(node, "/api/op", body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!(
                "{} refused an operation on the default log ({status}): {answer}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    /// The `Install` that opens the default log as a SimpleUML model.
    pub fn default_install() -> ModelOp {
        ModelOp::Install {
            model_id: LOG_ID.to_string(),
            metamodel_id: digest(UML),
            descriptor: UML.to_string(),
        }
    }

    /// The class names `node` serves on its default log (`/api/state`).
    pub fn default_names(&self, node: usize) -> Result<BTreeSet<String>, String> {
        match self.get(node, "/api/state")? {
            (200, doc) => Ok(names_in(&doc)),
            (status, body) => Err(format!(
                "{} answered /api/state with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// Wait until `node` serves exactly `expected` on its default log.
    pub fn await_default_names(
        &self,
        node: usize,
        expected: &BTreeSet<String>,
    ) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let held = self.default_names(node);
            if held.as_ref() == Ok(expected) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} serves {held:?} on the default log, never {expected:?}:\n{}",
                    self.nodes[node].id,
                    self.nodes[node].tail()
                ));
            }
            thread::sleep(POLL);
        }
    }

    /// Write a class named `name` at the head of `Model.ownedElements`.
    pub fn write_class(&self, node: usize, model: &str, name: &str) -> Result<(), String> {
        for op in si01::Uml::new().named_class(0, name) {
            self.submit(node, model, &op)?;
        }
        Ok(())
    }

    /// The class names `node` serves for `model`.
    pub fn names(&self, node: usize, model: &str) -> Result<BTreeSet<String>, String> {
        let path = format!("/api/model/{model}/state");
        match self.get(node, &path)? {
            (200, doc) => Ok(names_in(&doc)),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// Wait until `node` serves exactly `expected` for `model`.
    pub fn await_names(
        &self,
        node: usize,
        model: &str,
        expected: &BTreeSet<String>,
    ) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let held = self.names(node, model);
            if held.as_ref() == Ok(expected) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} serves {held:?} for {model}, never {expected:?}:\n{}",
                    self.nodes[node].id,
                    self.nodes[node].tail()
                ));
            }
            thread::sleep(POLL);
        }
    }

    /// `node` keeps serving exactly `expected` for [`QUIET`].
    pub fn holds_names(
        &self,
        node: usize,
        model: &str,
        expected: &BTreeSet<String>,
    ) -> Result<(), String> {
        let until = Instant::now() + QUIET;
        while Instant::now() < until {
            let held = self.names(node, model)?;
            if &held != expected {
                return Err(format!(
                    "{} serves {held:?} for {model} where {expected:?} must hold",
                    self.nodes[node].id
                ));
            }
            thread::sleep(POLL);
        }
        Ok(())
    }
}

/// The class names of a served SimpleUML document; none for a model
/// nobody has written into yet (it reads `null`).
pub fn names_in(doc: &Value) -> BTreeSet<String> {
    doc["ownedElements"]
        .as_array()
        .map(|owned| {
            owned
                .iter()
                .map(|element| si01::name_of(element).to_string())
                .collect()
        })
        .unwrap_or_default()
}

pub fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|n| n.to_string()).collect()
}

pub fn digest(text: &str) -> String {
    moirai_semantics::metamodel_digest(&serde_json::from_str::<Value>(text).expect("JSON"))
}

// ---------------------------------------------------------------------------
// Zenoh, read from outside
// ---------------------------------------------------------------------------

/// A raw Zenoh session in peer mode that listens on nothing, dials the
/// given loopback ports, and has multicast scouting and gossip off: it finds
/// nothing it was not pointed at.
pub fn raw_session(ports: &[u16]) -> Result<Session, String> {
    let mut config = Config::default();
    let endpoints: Vec<String> = ports.iter().map(|p| format!("tcp/127.0.0.1:{p}")).collect();
    let set = |config: &mut Config, key: &str, value: String| {
        config
            .insert_json5(key, &value)
            .map_err(|e| format!("raw session {key}: {e}"))
    };
    set(&mut config, "mode", "\"peer\"".into())?;
    set(&mut config, "listen/endpoints", "[]".into())?;
    set(
        &mut config,
        "connect/endpoints",
        serde_json::to_string(&endpoints).map_err(|e| e.to_string())?,
    )?;
    set(&mut config, "scouting/multicast/enabled", "false".into())?;
    set(&mut config, "scouting/gossip/enabled", "false".into())?;
    zenoh::open(config)
        .wait()
        .map_err(|e| format!("opening a raw Zenoh session to {endpoints:?}: {e}"))
}

/// The key of a liveliness token `moirai/<session>/node/<id>` that a raw
/// session connected to `127.0.0.1:port` sees, within `timeout`.
pub fn token_at(port: u16, id: &str, timeout: Duration) -> Result<String, String> {
    let session = raw_session(&[port])?;
    let seen: Arc<Mutex<Option<String>>> = Arc::default();
    let into = Arc::clone(&seen);
    let _subscriber = session
        .liveliness()
        .declare_subscriber(format!("moirai/*/node/{id}"))
        .history(true)
        .callback(move |sample: Sample| {
            if sample.kind() == SampleKind::Put {
                *into.lock().unwrap() = Some(sample.key_expr().as_str().to_string());
            }
        })
        .wait()
        .map_err(|e| format!("liveliness subscriber: {e}"))?;
    let deadline = Instant::now() + timeout;
    let found = loop {
        if let Some(key) = seen.lock().unwrap().clone() {
            break Ok(key);
        }
        if Instant::now() > deadline {
            break Err(format!(
                "no Zenoh session at 127.0.0.1:{port} declares a token moirai/*/node/{id} within \
                 {timeout:?}"
            ));
        }
        thread::sleep(Duration::from_millis(20));
    };
    let _ = session.close().wait();
    found
}

/// The witness that `node` runs on Zenoh: its token, seen at the port it
/// reported. A node on TCP has no Zenoh session there.
pub fn witness(node: &Node) -> Result<String, String> {
    token_at(node.listen, &node.id, WITNESS_TIMEOUT).map_err(|why| {
        format!(
            "NW16A WITNESS: {} (pid {}) was started with MOIRAI_TRANSPORT=zenoh and reported \
             listen port {}, but {why}; alarms:\n{}\nlast log lines:\n{}",
            node.id,
            node.pid,
            node.listen,
            alarms(&node.log),
            node.tail()
        )
    })
}

/// One frame a raw observer saw: its key, its `type`, its size in bytes,
/// its text when it is small (a `SyncRequest`, never a state transfer), and
/// when.
#[derive(Clone, Debug)]
pub struct Seen {
    pub key: String,
    pub kind: String,
    pub bytes: usize,
    pub text: Option<String>,
    pub at: Instant,
}

/// A raw Zenoh session connected to some nodes, recording every frame put
/// on keys matching `key`.
pub struct Observer {
    session: Session,
    pub seen: Arc<Mutex<Vec<Seen>>>,
    _subscriber: zenoh::pubsub::Subscriber<()>,
}

impl Observer {
    pub fn start(ports: &[u16], key: &str) -> Result<Observer, String> {
        let session = raw_session(ports)?;
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let into = Arc::clone(&seen);
        let subscriber = session
            .declare_subscriber(key.to_string())
            .callback(move |sample: Sample| {
                let bytes = sample.payload().to_bytes();
                let kind = serde_json::from_slice::<Value>(&bytes)
                    .ok()
                    .and_then(|v| v["type"].as_str().map(str::to_string))
                    .unwrap_or_else(|| "unparsed".into());
                let text =
                    (bytes.len() <= 4096).then(|| String::from_utf8_lossy(&bytes).into_owned());
                into.lock().unwrap().push(Seen {
                    key: sample.key_expr().as_str().to_string(),
                    kind,
                    bytes: bytes.len(),
                    text,
                    at: Instant::now(),
                });
            })
            .wait()
            .map_err(|e| format!("observer subscriber on {key}: {e}"))?;
        Ok(Observer {
            session,
            seen,
            _subscriber: subscriber,
        })
    }

    pub fn frames(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        let _ = self.session.close().wait();
    }
}

// ---------------------------------------------------------------------------
// The kernel's tables
// ---------------------------------------------------------------------------

/// A TCP socket in state LISTEN on `port`: its inode and local address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listening {
    pub inode: u64,
    pub local: String,
}

/// Every LISTEN socket on `port`, from `/proc/net/tcp` and `/proc/net/tcp6`.
pub fn listening_on(port: u16) -> Vec<Listening> {
    let mut found = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 10 || fields[3] != "0A" {
                continue;
            }
            let Some((addr, hex_port)) = fields[1].rsplit_once(':') else {
                continue;
            };
            if u16::from_str_radix(hex_port, 16).ok() != Some(port) {
                continue;
            }
            if let Ok(inode) = fields[9].parse::<u64>() {
                found.push(Listening {
                    inode,
                    local: addr.to_string(),
                });
            }
        }
    }
    found
}

/// The socket inodes `pid` holds open, from `/proc/<pid>/fd`.
pub fn socket_inodes(pid: u32) -> BTreeSet<u64> {
    let mut inodes = BTreeSet::new();
    if let Ok(entries) = fs::read_dir(format!("/proc/{pid}/fd")) {
        for entry in entries.flatten() {
            if let Ok(target) = fs::read_link(entry.path()) {
                let text = target.to_string_lossy().into_owned();
                if let Some(inode) = text
                    .strip_prefix("socket:[")
                    .and_then(|rest| rest.strip_suffix(']'))
                    .and_then(|n| n.parse().ok())
                {
                    inodes.insert(inode);
                }
            }
        }
    }
    inodes
}

/// The one LISTEN socket on `port`, which must be held by `pid`.
pub fn the_listener(pid: u32, port: u16) -> Result<Listening, String> {
    let all = listening_on(port);
    let held = socket_inodes(pid);
    match all.as_slice() {
        [one] if held.contains(&one.inode) => Ok(one.clone()),
        [one] => Err(format!(
            "the one LISTEN socket on port {port} (inode {}) is not held by pid {pid}",
            one.inode
        )),
        [] => Err(format!("no socket listens on port {port}")),
        many => Err(format!(
            "{} sockets listen on port {port}: {many:?}",
            many.len()
        )),
    }
}

/// The UDP ports the sockets of `pid` are bound to, read in its own network
/// namespace's table.
pub fn udp_ports_of(pid: u32) -> BTreeSet<u16> {
    let held = socket_inodes(pid);
    let mut ports = BTreeSet::new();
    for table in ["udp", "udp6"] {
        let Ok(text) = fs::read_to_string(format!("/proc/{pid}/net/{table}")) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 10 {
                continue;
            }
            let inode: u64 = fields[9].parse().unwrap_or(0);
            if !held.contains(&inode) {
                continue;
            }
            if let Some((_, hex_port)) = fields[1].rsplit_once(':')
                && let Ok(port) = u16::from_str_radix(hex_port, 16)
            {
                ports.insert(port);
            }
        }
    }
    ports
}
