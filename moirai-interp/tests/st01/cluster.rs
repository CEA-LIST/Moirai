//! ST.01's loopback harness: `model_node` processes meshed by a static
//! `PEERS` list, driven over HTTP. Modelled on SI.01's
//! (`si01_store_identity_l3.rs`), which is kept as it is; this one adds a
//! wait on the number of operations a node has delivered, so that two nodes
//! are compared once each has delivered every operation, and a difference
//! then is a divergence and not a lag.
#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;
use std::process::Child;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use moirai_semantics::metamodel_digest;
use serde_json::Value;

// NW.02: nodes are started on ports they bind themselves and report.
#[path = "../nw02/launch.rs"]
mod launch;

pub const MESH_TIMEOUT: Duration = Duration::from_secs(60);
pub const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
pub const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

/// The `model_node` binary: `MOIRAI_E2E_INTERP_NODE_BIN`, or the workspace's
/// debug build. `Err` says how to build it; the caller prints a skip, and a
/// skip is not a pass.
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

pub struct Node {
    pub id: String,
    pub http: u16,
    child: Child,
    pub log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    pub fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    pub fn tail(&self) -> String {
        self.log_text()
            .lines()
            .rev()
            .take(25)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Whether this node's log says it adopted a transferred state for
    /// `model`: `GenericNode::adopt_state`'s line.
    pub fn adopted(&self, model: &str) -> bool {
        self.log_text()
            .lines()
            .any(|line| line.contains("adopted state from") && line.contains(model))
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `n` replicas of one binary, each serving the descriptors given.
pub struct Cluster {
    pub nodes: Vec<Node>,
    _scratch: PathBuf,
    http: reqwest::blocking::Client,
}

impl Cluster {
    pub fn start(
        binary: &PathBuf,
        ids: &[&str],
        descriptors: &[(&str, &str)],
    ) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-st01-{}-{run}", std::process::id()));
        let metamodels = scratch.join("metamodels");
        fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
        for (name, text) in descriptors {
            fs::write(metamodels.join(format!("{name}.json")), text)
                .map_err(|err| err.to_string())?;
        }
        // NW.02: no port is chosen here. Each node binds what the kernel
        // gives it and reports it; its peers are then written in the order
        // and the text `PEERS` had before.
        let metamodels = metamodels.to_string_lossy().into_owned();
        let specs: Vec<launch::NodeSpec> = ids
            .iter()
            .map(|id| {
                launch::NodeSpec::new(
                    id,
                    &[
                        ("LOG_ID", "57015701570157015701570157015701"),
                        ("METAMODEL_DIR", metamodels.as_str()),
                    ],
                )
            })
            .collect();
        let nodes: Vec<Node> = launch::launch(binary, &scratch, &specs)?
            .into_iter()
            .map(|node| Node {
                id: node.id,
                http: node.http,
                child: node.child,
                log: node.log,
            })
            .collect();
        let cluster = Cluster {
            nodes,
            _scratch: scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|err| err.to_string())?,
        };
        cluster.await_health()?;
        cluster.await_mesh()?;
        Ok(cluster)
    }

    pub fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .http
            .get(self.nodes[node].url(path))
            .send()
            .map_err(|err| format!("GET {path} on {}: {err}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
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
            .map_err(|err| format!("POST {path} on {}: {err}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn await_health(&self) -> Result<(), String> {
        for (index, node) in self.nodes.iter().enumerate() {
            let deadline = Instant::now() + HEALTH_TIMEOUT;
            loop {
                // NW.02: the answer must come from the node this port was
                // reported by, never from whichever process holds the port.
                if let Ok((200, body)) = self.get(index, "/api/health") {
                    if body["replica_id"] == Value::String(node.id.clone()) {
                        break;
                    }
                    return Err(format!(
                        "the HTTP port {} reported by {} answered as {body}",
                        node.http, node.id
                    ));
                }
                if Instant::now() > deadline {
                    return Err(format!(
                        "{} never answered /api/health; start-up alarms:\n{}\nlast log lines:\n{}",
                        node.id,
                        launch::alarms(&node.log),
                        node.tail()
                    ));
                }
                std::thread::sleep(POLL);
            }
        }
        Ok(())
    }

    fn await_mesh(&self) -> Result<(), String> {
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            let ready = (0..self.nodes.len()).all(|index| {
                matches!(self.get(index, "/api/peers"), Ok((200, peers))
                    if connected(&peers) + 1 >= self.nodes.len())
            });
            if ready {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("the mesh never formed:\n{}", self.nodes[0].tail()));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Create (`model` none) or join a model; answers its id.
    pub fn register(&self, node: usize, model: Option<&str>, key: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => serde_json::json!({ "model_id": id, "metamodel_id": key }),
            None => serde_json::json!({ "metamodel_id": key }),
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

    pub fn submit(&self, node: usize, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!(
                "{} refused {op:?} ({status}): {answer}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    /// The identified read, `GET /api/model/{id}/state/identified`.
    pub fn identified(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/state/identified");
        match self.get(node, &path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// The default read, `GET /api/model/{id}/state`.
    pub fn default_state(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/state");
        match self.get(node, &path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// How many operations `node` has delivered for `model`, its `Install`
    /// included.
    pub fn delivered(&self, node: usize, model: &str) -> u64 {
        match self.get(node, &format!("/api/model/{model}/metrics")) {
            Ok((200, metrics)) => metrics["delivered_ops"].as_u64().unwrap_or(0),
            _ => 0,
        }
    }

    /// Poll until `node` has delivered at least `count` operations of
    /// `model`.
    pub fn until_delivered(&self, node: usize, model: &str, count: u64) -> Result<(), String> {
        let deadline = Instant::now() + DELIVERY_TIMEOUT;
        loop {
            let held = self.delivered(node, model);
            if held >= count {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} delivered {held} operations of {model}, never {count}:\n{}",
                    self.nodes[node].id,
                    self.nodes[node].tail()
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    pub fn pause_all(&self, node: usize) -> Result<(), String> {
        self.control(node, "/api/pause-all")
    }

    pub fn resume_all(&self, node: usize) -> Result<(), String> {
        self.control(node, "/api/resume-all")
    }

    fn control(&self, node: usize, path: &str) -> Result<(), String> {
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
}

fn connected(peers: &Value) -> usize {
    peers["peers"]
        .as_array()
        .map(|list| {
            list.iter()
                .filter(|peer| peer["status"] == Value::String("Connected".into()))
                .count()
        })
        .unwrap_or(0)
}

pub fn digest(text: &str) -> String {
    metamodel_digest(&serde_json::from_str::<Value>(text).expect("the fixture is JSON"))
}
