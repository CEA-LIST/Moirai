//! SI.13's node cluster: a `moirai-bootnode`, a `moirai-relay`, and
//! `model_node` processes on loopback, some of which reach each other
//! directly and some only through the relay.
//!
//! How a node comes to reach a peer only through the relay, on one host.
//! Every node is started with `BOOTNODE_URL`, so it learns its peers and the
//! relay from the bootnode's roster, and with `ADVERTISE_ADDR` set to
//! [`UNROUTABLE`], a loopback port nothing listens on, so every address the
//! roster hands out refuses a direct dial and the node routes that peer
//! through the relay (`moirai-network/src/composite.rs`, `connect_to_peers`).
//! A pair meant to be direct is also given each other's real replication
//! port in `PEERS_FILE` (NW.02's file, read before the node first dials), so
//! it dials that address before any roster arrives and holds the stream. The
//! test checks the topology rather than assuming it: every node's
//! `/api/metrics` must report each peer `direct` or `relayed` as the specs
//! say ([`Cluster::await_routes`]).
//!
//! Ports. Each `model_node` binds port 0 and reports what it bound (NW.02,
//! `launch::read_ports_file`); the bootnode and the relay take their ports
//! from the environment, so the harness picks free ones for them first
//! ([`free_port`]), a short window NW.02 removed for the nodes and that is
//! left here for two processes the harness starts before any node.
#![allow(dead_code)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use serde_json::{Value, json};

use super::launch;
use super::si12::{D2, digest};
use super::status13;

pub const MESH_TIMEOUT: Duration = Duration::from_secs(90);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
pub const CONVERGE_TIMEOUT: Duration = Duration::from_secs(300);
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
pub const POLL: Duration = Duration::from_millis(50);
/// What every node advertises to the bootnode: a loopback port nobody can
/// listen on without privileges, so a dial to it is refused at once.
pub const UNROUTABLE: &str = "127.0.0.1:1";
/// Operations per batch when a node writes a model.
pub const BATCH: usize = 250;

/// A binary the tests start: `var`, or the workspace's debug build.
fn binary(var: &str, built: &str, how: &str) -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var(var) {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!("{var} points at `{raw}`, which is not a file"))
        };
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(built);
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!(
            "no binary at {}; build it with `{how}`, or name one in {var}",
            path.display()
        ))
    }
}

pub fn node_binary() -> Result<PathBuf, String> {
    binary(
        "MOIRAI_E2E_INTERP_NODE_BIN",
        "../target/debug/examples/model_node",
        "cargo build -p moirai-interp --features network --example model_node",
    )
}

pub fn relay_binary() -> Result<PathBuf, String> {
    binary(
        "MOIRAI_E2E_RELAY_BIN",
        "../target/debug/moirai-relay",
        "cargo build -p moirai-relay",
    )
}

pub fn bootnode_binary() -> Result<PathBuf, String> {
    binary(
        "MOIRAI_E2E_BOOTNODE_BIN",
        "../target/debug/moirai-bootnode",
        "cargo build -p moirai-bootnode",
    )
}

/// A port nobody listens on now: bound and released at once.
pub fn free_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    let port = listener.local_addr().map_err(|err| err.to_string())?.port();
    Ok(port)
}

/// An error and every source under it.
pub fn with_sources(err: &dyn std::error::Error) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text
}

/// The bootnode or the relay.
pub struct Service {
    pub name: &'static str,
    pub child: Child,
    pub log: PathBuf,
    pub port: u16,
    pub http: u16,
}

/// A `model_node` process.
pub struct Node {
    pub id: String,
    pub http: u16,
    pub listen: u16,
    pub child: Child,
    pub log: PathBuf,
    /// The peers it reaches directly; every other peer goes through the relay.
    pub direct: Vec<String>,
}

impl Node {
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }
}

/// One node to start: its id, the peers it reaches directly, and the
/// descriptors it serves (file name, text).
pub struct Spec {
    pub id: String,
    pub direct: Vec<String>,
    pub served: Vec<(&'static str, &'static str)>,
}

impl Spec {
    pub fn new(id: &str, direct: &[&str], served: &[(&'static str, &'static str)]) -> Self {
        Spec {
            id: id.to_string(),
            direct: direct.iter().map(|peer| peer.to_string()).collect(),
            served: served.to_vec(),
        }
    }
}

pub struct Cluster {
    pub scratch: PathBuf,
    pub binary: PathBuf,
    pub session: String,
    pub log_id: String,
    pub bootnode: Option<Service>,
    pub relay: Option<Service>,
    pub nodes: Vec<Node>,
    pub http: reqwest::blocking::Client,
    /// When the run must stop, for a run that has a ceiling.
    pub ceiling: Option<Instant>,
    /// Whether a wait gave up at the ceiling rather than at its own bound.
    pub stopped: Cell<bool>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
        for service in [self.relay.as_mut(), self.bootnode.as_mut()]
            .into_iter()
            .flatten()
        {
            let _ = service.child.kill();
            let _ = service.child.wait();
        }
        let _ = fs::remove_dir_all(&self.scratch);
    }
}

fn spawn(
    binary: &Path,
    log: &Path,
    env: &[(&str, String)],
    remove: &[&str],
) -> Result<Child, String> {
    let out = fs::File::create(log).map_err(|err| err.to_string())?;
    let err_out = out.try_clone().map_err(|err| err.to_string())?;
    let mut command = Command::new(binary);
    for key in remove {
        command.env_remove(key);
    }
    for (key, value) in env {
        command.env(key, value);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|err| format!("spawning {}: {err}", binary.display()))
}

impl Cluster {
    /// The bootnode, the relay, and the nodes of `specs`, every route as the
    /// specs say. `name` keeps two clusters of one test process apart.
    pub fn start(
        name: &str,
        log_id: &str,
        specs: &[Spec],
        ceiling: Option<Instant>,
    ) -> Result<Self, String> {
        let binary = node_binary()?;
        let relay_bin = relay_binary()?;
        let bootnode_bin = bootnode_binary()?;
        if TcpStream::connect_timeout(
            &UNROUTABLE.parse().expect("an address"),
            Duration::from_secs(2),
        )
        .is_ok()
        {
            return Err(format!(
                "premise: something listens on {UNROUTABLE}, so the nodes' advertised address \
                 would be dialled; the relay-only topology cannot be built here"
            ));
        }
        let scratch =
            std::env::temp_dir().join(format!("moirai-si13-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        fs::create_dir_all(&scratch).map_err(|err| err.to_string())?;
        let session = format!("si13-{name}-{}", std::process::id());
        let mut cluster = Cluster {
            scratch: scratch.clone(),
            binary,
            session,
            log_id: log_id.to_string(),
            bootnode: None,
            relay: None,
            nodes: Vec::new(),
            http: reqwest::blocking::Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .map_err(|err| err.to_string())?,
            ceiling,
            stopped: Cell::new(false),
        };
        // The relay first, so the bootnode can name it.
        let (port, http) = (free_port()?, free_port()?);
        let log = scratch.join("relay.log");
        let child = spawn(
            &relay_bin,
            &log,
            &[
                ("RELAY_PORT", port.to_string()),
                ("RELAY_HTTP_PORT", http.to_string()),
                ("RELAY_IDLE_TIMEOUT_SECS", "3600".to_string()),
            ],
            &["RELAY_VERBOSE"],
        )?;
        cluster.relay = Some(Service {
            name: "relay",
            child,
            log,
            port,
            http,
        });
        cluster.await_service("relay")?;
        let (port, http) = (free_port()?, 0);
        let log = scratch.join("bootnode.log");
        let relay_port = cluster.relay.as_ref().expect("started").port;
        let child = spawn(
            &bootnode_bin,
            &log,
            &[
                ("BOOTNODE_PORT", port.to_string()),
                ("BOOTNODE_TTL_SECS", "10".to_string()),
                ("RELAY_ADDR", format!("127.0.0.1:{relay_port}")),
            ],
            &[],
        )?;
        cluster.bootnode = Some(Service {
            name: "bootnode",
            child,
            log,
            port,
            http,
        });
        cluster.await_service("bootnode")?;
        cluster.add(specs)?;
        Ok(cluster)
    }

    /// `GET /health` on the relay's HTTP port or the bootnode's port answers
    /// 200 within [`HEALTH_TIMEOUT`].
    fn await_service(&self, which: &str) -> Result<(), String> {
        let service = match which {
            "relay" => self.relay.as_ref(),
            _ => self.bootnode.as_ref(),
        }
        .expect("started");
        let port = if which == "relay" {
            service.http
        } else {
            service.port
        };
        let deadline = Instant::now() + HEALTH_TIMEOUT;
        loop {
            if let Ok(response) = self
                .http
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                && response.status().as_u16() == 200
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "the {which} never answered /health on {port}:\n{}",
                    launch::tail(&service.log, 20)
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Start the nodes of `specs` beside those already up: each binds its
    /// own ports, learns the bootnode, advertises [`UNROUTABLE`], and is told
    /// in its peers file only the peers it reaches directly.
    pub fn add(&mut self, specs: &[Spec]) -> Result<(), String> {
        let bootnode = format!(
            "http://127.0.0.1:{}",
            self.bootnode.as_ref().expect("started").port
        );
        let mut pending = Vec::new();
        for spec in specs {
            let dir = self.scratch.join(format!("{}-metamodels", spec.id));
            fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
            for (name, text) in &spec.served {
                fs::write(dir.join(format!("{name}.json")), text).map_err(|err| err.to_string())?;
            }
            let log = self.scratch.join(format!("{}.log", spec.id));
            let ports = self.scratch.join(format!("{}.ports.json", spec.id));
            let peers = self.scratch.join(format!("{}.peers", spec.id));
            let child = spawn(
                &self.binary,
                &log,
                &[
                    ("REPLICA_ID", spec.id.clone()),
                    ("LISTEN_PORT", "0".into()),
                    ("HTTP_PORT", "0".into()),
                    ("HTTP_BIND_ADDR", "127.0.0.1".into()),
                    ("PORTS_FILE", ports.to_string_lossy().into_owned()),
                    ("PEERS_FILE", peers.to_string_lossy().into_owned()),
                    ("LOG_ID", self.log_id.clone()),
                    ("METAMODEL_DIR", dir.to_string_lossy().into_owned()),
                    ("BOOTNODE_URL", bootnode.clone()),
                    ("SESSION_ID", self.session.clone()),
                    ("ADVERTISE_ADDR", UNROUTABLE.to_string()),
                    ("RECONCILE_SECS", "1".into()),
                ],
                &["PEERS", "DASHBOARD_URL", "METAMODEL_PATH"],
            )?;
            pending.push((spec, child, log, ports, peers));
        }
        // Each node's reported ports, checked as NW.02 checks them.
        let deadline = Instant::now() + launch::PORTS_TIMEOUT;
        let mut reported: Vec<Option<launch::Reported>> = vec![None; pending.len()];
        while reported.iter().any(Option::is_none) {
            for (slot, (spec, child, log, ports, _)) in reported.iter_mut().zip(pending.iter_mut())
            {
                if slot.is_some() {
                    continue;
                }
                if let Some(read) = launch::read_ports_file(ports) {
                    let read = read.map_err(|why| format!("{}'s ports file: {why}", spec.id))?;
                    if read.replica_id != spec.id || read.pid != child.id() {
                        return Err(format!("{}'s ports file is not its own: {read:?}", spec.id));
                    }
                    *slot = Some(read);
                } else if let Ok(Some(status)) = child.try_wait() {
                    return Err(format!(
                        "{} exited with {status} before reporting its ports; alarms:\n{}\n{}",
                        spec.id,
                        launch::alarms(log),
                        launch::tail(log, 25)
                    ));
                }
            }
            if Instant::now() > deadline {
                return Err("a node never reported its ports".to_string());
            }
            std::thread::sleep(POLL);
        }
        let mut started = Vec::new();
        for ((spec, child, log, _, peers), ports) in pending.into_iter().zip(reported) {
            let ports = ports.expect("every node reported");
            started.push((
                Node {
                    id: spec.id.clone(),
                    http: ports
                        .http_port
                        .expect("checked by read_ports_file's caller"),
                    listen: ports.listen_port,
                    child,
                    log,
                    direct: spec.direct.clone(),
                },
                peers,
            ));
        }
        // The peers files: the direct peers among every node now known.
        for (node, peers) in &started {
            let text = node
                .direct
                .iter()
                .map(|peer| {
                    let listen = self
                        .nodes
                        .iter()
                        .map(|other| (&other.id, other.listen))
                        .chain(started.iter().map(|(other, _)| (&other.id, other.listen)))
                        .find(|(id, _)| *id == peer)
                        .map(|(_, listen)| listen)
                        .ok_or_else(|| {
                            format!("{} names an unknown direct peer {peer}", node.id)
                        })?;
                    Ok(format!("{peer}:127.0.0.1:{listen}"))
                })
                .collect::<Result<Vec<String>, String>>()?
                .join(",");
            launch::write_atomically(peers, &text)?;
        }
        let first = self.nodes.len();
        self.nodes.extend(started.into_iter().map(|(node, _)| node));
        for index in first..self.nodes.len() {
            self.await_health(index)?;
        }
        Ok(())
    }

    pub fn index(&self, id: &str) -> usize {
        self.nodes
            .iter()
            .position(|node| node.id == id)
            .unwrap_or_else(|| panic!("no node {id}"))
    }

    pub fn deadline(&self, wait: Duration) -> Instant {
        let own = Instant::now() + wait;
        self.ceiling.map_or(own, |ceiling| ceiling.min(own))
    }

    pub fn at_ceiling(&self, deadline: Instant) -> bool {
        let reached = self.ceiling == Some(deadline);
        if reached {
            self.stopped.set(true);
        }
        reached
    }

    pub fn tails(&self) -> String {
        let mut out: Vec<String> = self
            .nodes
            .iter()
            .map(|node| {
                format!(
                    "--- {} (alarms: {})\n{}",
                    node.id,
                    launch::alarms(&node.log),
                    launch::tail(&node.log, 8)
                )
            })
            .collect();
        if let Some(relay) = &self.relay {
            out.push(format!("--- relay\n{}", launch::tail(&relay.log, 8)));
        }
        out.join("\n")
    }

    pub fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .http
            .get(self.nodes[node].url(path))
            .send()
            .map_err(|err| {
                format!(
                    "GET {path} on {}: {}",
                    self.nodes[node].id,
                    with_sources(&err)
                )
            })?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| with_sources(&err))?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    pub fn post(&self, node: usize, path: &str, body: String) -> Result<(u16, Value), String> {
        self.post_with(&self.http, node, path, body)
    }

    pub fn post_with(
        &self,
        client: &reqwest::blocking::Client,
        node: usize,
        path: &str,
        body: String,
    ) -> Result<(u16, Value), String> {
        let response = client
            .post(self.nodes[node].url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| {
                format!(
                    "POST {path} on {}: {}",
                    self.nodes[node].id,
                    with_sources(&err)
                )
            })?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| with_sources(&err))?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    pub fn await_health(&self, node: usize) -> Result<(), String> {
        let deadline = Instant::now() + HEALTH_TIMEOUT;
        loop {
            if let Ok((200, body)) = self.get(node, "/api/health") {
                if body["replica_id"] == json!(self.nodes[node].id) {
                    return Ok(());
                }
                return Err(format!(
                    "the HTTP port of {} answered as {body}",
                    self.nodes[node].id
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never answered /api/health:\n{}",
                    self.nodes[node].id,
                    self.tails()
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// How `node` reports reaching each peer: `direct`, `relayed` or
    /// `unreachable`, from `/api/metrics`.
    pub fn routes(&self, node: usize) -> Result<BTreeMap<String, String>, String> {
        let metrics = self.read(node, "/api/metrics")?;
        let routes = metrics["routes"]
            .as_object()
            .ok_or_else(|| format!("{} reports no routes: {metrics}", self.nodes[node].id))?;
        Ok(routes
            .iter()
            .filter_map(|(peer, route)| route.as_str().map(|r| (peer.clone(), r.to_string())))
            .collect())
    }

    /// The status `/api/peers` reports for each of `node`'s peers.
    pub fn peer_status(&self, node: usize) -> Result<BTreeMap<String, String>, String> {
        let peers = self.read(node, "/api/peers")?;
        Ok(peers["peers"]
            .as_array()
            .map(|list| {
                list.iter()
                    .filter_map(|peer| {
                        Some((
                            peer["id"].as_str()?.to_string(),
                            peer["status"].as_str().unwrap_or_default().to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// The route the specs give `node` for `peer`: direct when both name the
    /// other, relayed otherwise.
    pub fn expected_route(&self, node: usize, peer: usize) -> &'static str {
        let (x, y) = (&self.nodes[node], &self.nodes[peer]);
        if x.direct.contains(&y.id) && y.direct.contains(&x.id) {
            "direct"
        } else {
            "relayed"
        }
    }

    /// Every node reaches every other node by the route the specs give it,
    /// and lists it `Connected`, within [`MESH_TIMEOUT`].
    pub fn await_routes(&self) -> Result<(), String> {
        let deadline = self.deadline(MESH_TIMEOUT);
        loop {
            let mut wrong = Vec::new();
            for node in 0..self.nodes.len() {
                let routes = self.routes(node).unwrap_or_default();
                let status = self.peer_status(node).unwrap_or_default();
                for peer in 0..self.nodes.len() {
                    if peer == node {
                        continue;
                    }
                    let id = &self.nodes[peer].id;
                    let want = self.expected_route(node, peer);
                    let route = routes.get(id).map(String::as_str);
                    let connected = status.get(id).map(String::as_str);
                    if route != Some(want) || connected != Some("Connected") {
                        wrong.push(format!(
                            "{} reaches {id} {route:?} ({connected:?}), not {want}",
                            self.nodes[node].id
                        ));
                    }
                }
            }
            if wrong.is_empty() {
                return Ok(());
            }
            if Instant::now() > deadline {
                let when = if self.at_ceiling(deadline) {
                    " before the run's ceiling"
                } else {
                    ""
                };
                return Err(format!(
                    "the topology never formed{when}: {}\n{}",
                    wrong.join("; "),
                    self.tails()
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn control(&self, node: usize, path: &str) -> Result<(), String> {
        let (status, body) = self.post(node, path, String::new())?;
        if (200..300).contains(&status) && body["success"] != json!(false) {
            Ok(())
        } else {
            Err(format!(
                "{path} on {} answered {status}: {body}",
                self.nodes[node].id
            ))
        }
    }

    /// Every node pauses every peer, and lists each `Paused`.
    pub fn pause_all(&self) -> Result<(), String> {
        for node in 0..self.nodes.len() {
            self.control(node, "/api/pause-all")?;
        }
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            let open: Vec<String> = (0..self.nodes.len())
                .flat_map(|node| {
                    self.peer_status(node)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|(_, status)| status != "Paused")
                        .map(move |(peer, status)| format!("{node}->{peer} {status}"))
                })
                .collect();
            if open.is_empty() {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("peers left unpaused: {open:?}"));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Resume the link between `x` and `y` at both ends, and wait until each
    /// lists the other `Connected`.
    pub fn resume_link(&self, x: usize, y: usize) -> Result<(), String> {
        let (x_id, y_id) = (self.nodes[x].id.clone(), self.nodes[y].id.clone());
        self.control(x, &format!("/api/resume/{y_id}"))?;
        self.control(y, &format!("/api/resume/{x_id}"))?;
        let deadline = self.deadline(MESH_TIMEOUT);
        loop {
            let xs = self.peer_status(x).unwrap_or_default();
            let ys = self.peer_status(y).unwrap_or_default();
            if xs.get(&y_id).map(String::as_str) == Some("Connected")
                && ys.get(&x_id).map(String::as_str) == Some("Connected")
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "the link {x_id}-{y_id} did not come back: {x_id} lists {:?}, {y_id} lists {:?}",
                    xs.get(&y_id),
                    ys.get(&x_id)
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Every node resumes every peer; the topology must form again.
    pub fn resume_all(&self) -> Result<(), String> {
        for node in 0..self.nodes.len() {
            self.control(node, "/api/resume-all")?;
        }
        self.await_routes()
    }

    /// Create a model on `node` under `digest`, or join `model` naming it.
    pub fn register(
        &self,
        node: usize,
        model: Option<&str>,
        digest: &str,
    ) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": digest }),
            None => json!({ "metamodel_id": digest }),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "registering on {} answered {status}: {answer}",
                self.nodes[node].id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    /// Join `model` on `node` by its id alone; a node that hosts it already
    /// (409) is as good (SI.12, R2).
    pub fn join_by_id(&self, node: usize, model: &str) -> Result<(), String> {
        let (status, answer) = self.post(
            node,
            "/api/models",
            json!({ "model_id": model }).to_string(),
        )?;
        match status {
            200 | 201 | 409 => Ok(()),
            _ => Err(format!(
                "joining {model} by id on {} answered {status}: {answer}",
                self.nodes[node].id
            )),
        }
    }

    pub fn post_op(&self, node: usize, model: &str, op: &ModelOp) -> Result<Value, String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 {
            return Err(format!(
                "{} answered {status} to an operation on {model}: {answer}",
                self.nodes[node].id
            ));
        }
        Ok(answer)
    }

    /// Post operations one by one; each must be applied.
    pub fn submit(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        for op in ops {
            let answer = self.post_op(node, model, op)?;
            if answer["success"] != Value::Bool(true) {
                return Err(format!(
                    "{} refused an operation on {model}: {answer}",
                    self.nodes[node].id
                ));
            }
        }
        Ok(())
    }

    /// Post operations in batches of [`BATCH`], each applied whole; a batch
    /// answered 504 `expired` was applied nowhere and is posted again, at most
    /// three times (SI.12's harness).
    pub fn submit_batches(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        let batches = ops.len().div_ceil(BATCH);
        for (index, chunk) in ops.chunks(BATCH).enumerate() {
            let batch = index + 1;
            if self.ceiling.is_some_and(|ceiling| Instant::now() > ceiling) {
                self.stopped.set(true);
                return Err(format!(
                    "the run reached its ceiling while writing {model}, before batch {batch} of \
                     {batches}"
                ));
            }
            let body = json!({ "ops": chunk }).to_string();
            let mut expired = 0;
            loop {
                let (status, answer) = self
                    .post(node, &format!("/api/model/{model}/ops"), body.clone())
                    .map_err(|why| format!("batch {batch} of {batches}: {why}"))?;
                if status == 200 && answer["applied"] == json!(chunk.len()) {
                    break;
                }
                if status == 504 && answer["outcome"] == json!("expired") && expired < 3 {
                    expired += 1;
                    continue;
                }
                return Err(format!(
                    "{} answered batch {batch} of {batches} on {model} with {status}: {answer}",
                    self.nodes[node].id
                ));
            }
        }
        Ok(())
    }

    pub fn read(&self, node: usize, path: &str) -> Result<Value, String> {
        match self.get(node, path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    pub fn identified(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/state/identified"))
    }

    pub fn plain(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/state"))
    }

    pub fn metrics(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/metrics"))
    }

    /// The migration status as served, every key.
    pub fn migration_raw(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/migration"))
    }

    /// The migration status's three keys SI.13 reads ([`status13`]).
    pub fn migration(&self, node: usize, model: &str) -> Result<Value, String> {
        self.migration_raw(node, model)
            .map(|status| status13(&status))
    }

    /// The models `node` hosts, by id, with the listing each is under.
    pub fn models(&self, node: usize) -> Result<BTreeMap<String, Value>, String> {
        let body = self.read(node, "/api/models")?;
        Ok(body["models"]
            .as_array()
            .ok_or_else(|| format!("no `models` in {body}"))?
            .iter()
            .filter_map(|entry| {
                Some((
                    entry["model_id"].as_str()?.to_string(),
                    entry["metamodel_id"].clone(),
                ))
            })
            .collect())
    }

    /// Whether `node` serves the descriptor `text` (its digest is listed by
    /// `GET /api/metamodels`).
    pub fn serves(&self, node: usize, text: &str) -> Result<bool, String> {
        let body = self.read(node, "/api/metamodels")?;
        Ok(body.to_string().contains(&digest(text)))
    }

    /// Whether `node` lists `model` under `D2`'s digest.
    pub fn lists_under_d2(&self, node: usize, model: &str) -> Result<(), String> {
        let listed = self.models(node)?.get(model).cloned();
        let digest_of = |value: &Value| match value {
            Value::String(digest) => Some(digest.clone()),
            Value::Object(map) => map
                .get("digest")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        };
        match listed {
            Some(metamodel) if digest_of(&metamodel) == Some(digest(D2)) => Ok(()),
            other => Err(format!(
                "{} lists {model} as {other:?}, not under D2's digest",
                self.nodes[node].id
            )),
        }
    }

    /// Poll until `check` holds on every node of `nodes`, or answer the last
    /// reason it did not, after `wait` (or the run's ceiling).
    pub fn until_on(
        &self,
        nodes: &[usize],
        wait: Duration,
        what: &str,
        check: impl Fn(usize) -> Result<(), String>,
    ) -> Result<(), String> {
        let deadline = self.deadline(wait);
        loop {
            match nodes.iter().try_for_each(|node| check(*node)) {
                Ok(()) => return Ok(()),
                Err(why) if Instant::now() > deadline => {
                    if self.at_ceiling(deadline) {
                        return Err(format!("{what}, stopped at the run's ceiling: {why}"));
                    }
                    return Err(format!("{what}, not within {} s: {why}", wait.as_secs()));
                }
                Err(_) => std::thread::sleep(POLL),
            }
        }
    }

    /// [`Cluster::until_on`] every node, for the convergence bound.
    pub fn until(
        &self,
        what: &str,
        check: impl Fn(usize) -> Result<(), String>,
    ) -> Result<(), String> {
        let all: Vec<usize> = (0..self.nodes.len()).collect();
        self.until_on(&all, CONVERGE_TIMEOUT, what, check)
    }

    /// The relay's `/health` counters.
    pub fn relay_health(&self) -> Result<Value, String> {
        let relay = self.relay.as_ref().ok_or("no relay")?;
        let response = self
            .http
            .get(format!("http://127.0.0.1:{}/health", relay.http))
            .send()
            .map_err(|err| with_sources(&err))?;
        let body = response.text().map_err(|err| with_sources(&err))?;
        serde_json::from_str(&body).map_err(|err| format!("{err}: {body}"))
    }

    /// M10, the relay's side: every line of its log that says it read a
    /// frame over its bound, failed to read one, or could not parse one.
    pub fn relay_alarms(&self) -> Vec<String> {
        let Some(relay) = &self.relay else {
            return vec!["no relay".to_string()];
        };
        fs::read_to_string(&relay.log)
            .unwrap_or_default()
            .lines()
            .filter(|line| {
                line.contains("frame exceeds")
                    || line.contains("read error")
                    || line.contains("unparseable frame")
            })
            .map(str::to_string)
            .collect()
    }

    /// M10, the nodes' side: every line of a node's log that says its relay
    /// session closed or failed, that it dropped a relayed frame, or (SI.13
    /// round 2, (d)) that it dropped a chunked message: one that joined but
    /// will not parse, one left incomplete, or one dropped at the receiver's
    /// memory bound (`relay_transport.rs`, `Joiner::take` and the reader).
    pub fn relay_session_alarms(&self) -> Vec<String> {
        self.nodes
            .iter()
            .flat_map(|node| {
                fs::read_to_string(&node.log)
                    .unwrap_or_default()
                    .lines()
                    .filter(|line| {
                        (line.contains("Relay session to") && line.contains("closed"))
                            || line.contains("Read error on the relay session")
                            || line.contains("Dropping an unparseable relay frame")
                            || line.contains("relay broadcast failed")
                            || line.contains("Dropping a joined message")
                            || line.contains("dropping an incomplete chunked message")
                            || line.contains("dropping a chunked message")
                    })
                    .map(|line| format!("{}: {line}", node.id))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The size a node printed for `model`'s log when it served or refused a
    /// state transfer of it, and which it did (SI.12's harness).
    pub fn transfer_line(&self, node: usize, model: &str) -> Option<(usize, &'static str)> {
        let text = fs::read_to_string(&self.nodes[node].log).ok()?;
        text.lines()
            .filter(|line| line.contains(model) && line.contains("state transfer"))
            .find_map(|line| {
                if let Some((_, rest)) = line.split_once("the log is ") {
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    digits.parse().ok().map(|bytes| (bytes, "refused"))
                } else if line.contains("serving a state transfer") {
                    let (_, rest) = line.rsplit_once(": ")?;
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    digits.parse().ok().map(|bytes| (bytes, "served"))
                } else {
                    None
                }
            })
    }
}
