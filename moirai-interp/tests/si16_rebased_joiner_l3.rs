//! SI.16 at L3: a `model_node` that joins by state transfer a model whose
//! history holds a rebase (SI.14), during the rebase's window and after it
//! is stable, on `model_node` processes over loopback.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.16.md`; the contract (J1
//! to J6) is restated in `si16_rebased_joiner.rs`. Over HTTP, J1 is: the
//! joiner lists the model on `GET /api/models` under the revision's digest
//! and serves the revision's text on `GET /api/model/{m}/metamodel`, which
//! it held nowhere else (every joiner here serves D1 alone), beside both
//! reads. Every operation is posted on `POST /api/model/{m}/op`, every
//! migration on `POST /api/model/{m}/migrate`, every status read on
//! `GET /api/model/{m}/migration`.
//!
//! Each join is made a state transfer by its order, as SI.14's round-3 test
//! made its joins (its R7): the joiner hosts the model only after the
//! rebase, it pauses every peer but the donor, and it registers the model by
//! id; its log has no history, so it asks its one connected peer that hosts
//! the model for its state. The node's log line `adopted state from <donor>
//! for log <m>` is checked after each join, and its absence is a set-up
//! failure, never a pass.
//!
//! - `si16_l3_w`, a join during the window, three nodes: `si16-a` serves
//!   D1, D2 and D2_DROP, `si16-b` D1 and D2, `si16-c` D1 alone.
//!   - W1: `a` writes the running example on `m` under D1; `b` joins it by
//!     `a`'s state (set-up, SI.14's shape before any migration).
//!   - W2: every node pauses every peer; `b` writes `s` at the end of
//!     `Door`'s name and `persistent` on `Car` under D1; `a` rebases `m` to
//!     D2 (`si16-a@71`).
//!   - W3: `c` and `a` resume each other, `b` stays cut off; `c` joins `m`
//!     under D1's digest by `a`'s state. On `c` (J1) the running example
//!     under D2, listed under D2's digest, D2 served; (J3) the status `a`
//!     reads but waiting for `si16-b` alone, D1 held; (J2) an edit only D1
//!     routes refused, as on `a`; (J4) all of it unchanged for 20 s.
//!   - W4: every node resumes; `b`'s late edits reach `a` and `c`, which
//!     translate them (J5): one document with `Doors` and `persistent`; `c`
//!     still waits for `si16-b`.
//!   - W5: `b` writes `hb` under D2: `c` lets D1 go (J4); `a` and `b` wait
//!     for `si16-c`.
//!   - W6: `c` writes `hc` under D2 (J2): all three wait for no one, list
//!     `m` under D2, serve D2 and one document (J6).
//! - `si16_l3_s`, a join after the rebase is stable on every member: the
//!   same three nodes and a fourth, `si16-d`, serving D1 alone, started
//!   only once the rebase is stable on all three.
//!   - S1: `a` writes the running example on `m` under D1; `b`, then `c`,
//!     join it by `a`'s state.
//!   - S2: every node pauses every peer; `c` writes `0` into `wheels`'s
//!     value under D1 (`si16-c@1`); `a` rebases `m` to D2_DROP naming
//!     `ModelElement.taggedValue` and `TaggedValue` (`si16-a@71`).
//!   - S3: every node resumes; `b` and `c` write `hb` and `hc` under
//!     D2_DROP: all three read `wheels` hidden, `si16-c@1` dropped, waiting
//!     for no one, D1 let go.
//!   - S4: `si16-d` starts, pauses `si16-b` and `si16-c`, and joins `m` by
//!     its id alone (no metamodel) by `a`'s state. On `d` (J1) listed under
//!     D2_DROP's digest, D2_DROP served, the document; (J3) the status the
//!     others read; (J2) an edit only D1 routes refused, as on `a`.
//!   - S5: `d` writes `hd` under D2_DROP (J2): all four list `m` under
//!     D2_DROP, serve it, wait for no one, one document (J6).
//!
//! What the three nodes other than the joiner read between the fourth
//! node's join and its first write is not read: they admit a member then,
//! which is the governance plane's (spec, named limit L3).
//!
//! A check of the claim that fails is recorded and the schedule goes on; the
//! test fails at its end with every recorded failure. A set-up step that
//! fails (a node, a join, an adoption line, a registration, a migrate
//! answer, an edit a node must accept) stops it at once.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si16_rebased_joiner_l3 -- --test-threads=1 --nocapture
//! ```
//!
//! The binary is `MOIRAI_E2E_INTERP_NODE_BIN` or
//! `target/debug/examples/model_node`; a missing one fails the test (it is
//! not a skip).

mod si14;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use moirai_interp::leaf::{LeafOp, Scalar};
use serde_json::{Value, json};

use si14::si12::{
    CAR, CAR_WHEELS, D1, D2, DOOR, SEDAN, Uml, digest, ids_by_label, labels, project_with,
    strip_ids,
};
use si14::*;

const MESH_TIMEOUT: Duration = Duration::from_secs(60);
/// SI.14's convergence bound, a liveness bound and not a timing claim.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(120);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
/// SI.14's M15 bound on a migrate answer.
const ANSWER_BOUND: Duration = Duration::from_secs(6);
/// J4: how long the joiner is watched holding D1 while the member it waits
/// for is cut off (SI.14's window hold).
const WINDOW_HOLD: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(100);
const LOG_ID: &str = "5116c0de5116c0de5116c0de5116c0de";

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const D: usize = 3;
const NAMES: [&str; 4] = ["si16-a", "si16-b", "si16-c", "si16-d"];

fn node_binary() -> Result<PathBuf, String> {
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

struct Node {
    id: String,
    http: u16,
    listen: u16,
    child: Child,
    log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }
}

struct Cluster {
    nodes: Vec<Node>,
    scratch: PathBuf,
    http: reqwest::blocking::Client,
    failures: Vec<String>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
        let _ = fs::remove_dir_all(&self.scratch);
    }
}

/// The descriptors node `id` serves, by file name.
fn served(id: &str) -> Vec<(&'static str, &'static str)> {
    match id {
        "si16-a" => vec![("d1", D1), ("d2", D2), ("d2-drop", D2_DROP)],
        "si16-b" => vec![("d1", D1), ("d2", D2)],
        _ => vec![("d1", D1)],
    }
}

/// The metamodel directory of node `id`, written under `scratch`.
fn metamodel_dir(scratch: &Path, id: &str) -> Result<String, String> {
    let dir = scratch.join(format!("{id}-metamodels"));
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    for (name, text) in served(id) {
        fs::write(dir.join(format!("{name}.json")), text).map_err(|err| err.to_string())?;
    }
    Ok(dir.to_string_lossy().into_owned())
}

impl Cluster {
    /// `si16-a`, `si16-b` and `si16-c`, meshed by static peers (NW.02's
    /// launcher), in a scratch directory of the test's own.
    fn start(binary: &Path, tag: &str) -> Result<Self, String> {
        let scratch =
            std::env::temp_dir().join(format!("moirai-si16-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        let mut specs = Vec::new();
        for id in &NAMES[..3] {
            let dir = metamodel_dir(&scratch, id)?;
            specs.push(launch::NodeSpec::new(
                id,
                &[("LOG_ID", LOG_ID), ("METAMODEL_DIR", dir.as_str())],
            ));
        }
        let nodes: Vec<Node> = launch::launch(binary, &scratch, &specs)?
            .into_iter()
            .map(|node| Node {
                id: node.id,
                http: node.http,
                listen: node.listen,
                child: node.child,
                log: node.log,
            })
            .collect();
        let pids: Vec<String> = nodes
            .iter()
            .map(|node| format!("{} pid {}", node.id, node.child.id()))
            .collect();
        println!("SI16-L3-{tag} nodes: {}", pids.join(", "));
        let cluster = Cluster {
            nodes,
            scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|err| err.to_string())?,
            failures: Vec::new(),
        };
        cluster.await_health()?;
        cluster.await_mesh(&[A, B, C], 2)?;
        Ok(cluster)
    }

    /// `si16-d`, started on ports it binds itself (NW.02) once the others
    /// run, with them as its peers; none of them has it in its own peers
    /// file. NW.04's way of starting a late node.
    fn start_late(&mut self, binary: &Path) -> Result<(), String> {
        let id = NAMES[D];
        let dir = metamodel_dir(&self.scratch, id)?;
        let log = self.scratch.join(format!("{id}.log"));
        let ports = self.scratch.join(format!("{id}.ports.json"));
        let peers_file = self.scratch.join(format!("{id}.peers"));
        let _ = fs::remove_file(&ports);
        let _ = fs::remove_file(&peers_file);
        let out = fs::File::create(&log).map_err(|err| err.to_string())?;
        let err_out = out.try_clone().map_err(|err| err.to_string())?;
        let mut child = Command::new(binary)
            .env("REPLICA_ID", id)
            .env("LISTEN_PORT", "0")
            .env("HTTP_PORT", "0")
            .env("PORTS_FILE", &ports)
            .env("PEERS_FILE", &peers_file)
            .env("LOG_ID", LOG_ID)
            .env("METAMODEL_DIR", &dir)
            .env_remove("PEERS")
            .env_remove("BOOTNODE_URL")
            .env_remove("DASHBOARD_URL")
            .env_remove("METAMODEL_PATH")
            .env_remove("ADVERTISE_ADDR")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err_out))
            .spawn()
            .map_err(|err| format!("spawning {id}: {err}"))?;
        let deadline = Instant::now() + launch::PORTS_TIMEOUT;
        let reported = loop {
            if let Some(parsed) = launch::read_ports_file(&ports) {
                let reported = parsed?;
                if reported.replica_id != id || reported.pid != child.id() {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("{id}'s ports file is not its own: {reported:?}"));
                }
                break reported;
            }
            if let Ok(Some(status)) = child.try_wait() {
                return Err(format!(
                    "{id} exited with {status} before reporting its ports:\n{}",
                    launch::tail(&log, 25)
                ));
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "{id} never reported its ports:\n{}",
                    launch::tail(&log, 25)
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let peers = self.nodes[..3]
            .iter()
            .map(|node| format!("{}:127.0.0.1:{}", node.id, node.listen))
            .collect::<Vec<_>>()
            .join(",");
        let http = match reported.http_port {
            Some(port) => port,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{id} reported no HTTP port"));
            }
        };
        self.nodes.push(Node {
            id: id.to_string(),
            http,
            listen: reported.listen_port,
            child,
            log,
        });
        launch::write_atomically(&peers_file, &peers)?;
        println!("SI16-L3-S late node: {id} pid {}", self.nodes[D].child.id());
        self.await_health()?;
        self.await_mesh(&[D], 3)
    }

    fn tails(&self) -> String {
        self.nodes
            .iter()
            .map(|node| {
                format!(
                    "--- {} (alarms: {})\n{}",
                    node.id,
                    launch::alarms(&node.log),
                    launch::tail(&node.log, 20)
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn request(
        &self,
        node: usize,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
    ) -> Result<(u16, Value), String> {
        let mut request = self
            .http
            .request(method.clone(), self.nodes[node].url(path));
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }
        let response = request
            .send()
            .map_err(|err| format!("{method} {path} on {}: {err}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let text = response.text().map_err(|err| err.to_string())?;
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        self.request(node, reqwest::Method::GET, path, None)
    }

    fn post(&self, node: usize, path: &str, body: String) -> Result<(u16, Value), String> {
        self.request(node, reqwest::Method::POST, path, Some(body))
    }

    fn await_health(&self) -> Result<(), String> {
        for (index, node) in self.nodes.iter().enumerate() {
            let deadline = Instant::now() + HEALTH_TIMEOUT;
            loop {
                if let Ok((200, body)) = self.get(index, "/api/health") {
                    if body["replica_id"] == json!(node.id) {
                        break;
                    }
                    return Err(format!("the HTTP port of {} answered as {body}", node.id));
                }
                if Instant::now() > deadline {
                    return Err(format!(
                        "{} never answered /api/health:\n{}",
                        node.id,
                        self.tails()
                    ));
                }
                std::thread::sleep(POLL);
            }
        }
        Ok(())
    }

    fn connected(&self, node: usize) -> usize {
        match self.get(node, "/api/peers") {
            Ok((200, peers)) => peers["peers"]
                .as_array()
                .map(|list| {
                    list.iter()
                        .filter(|peer| peer["status"] == Value::String("Connected".into()))
                        .count()
                })
                .unwrap_or(0),
            _ => 0,
        }
    }

    /// Every node of `nodes` connected to at least `peers` peers.
    fn await_mesh(&self, nodes: &[usize], peers: usize) -> Result<(), String> {
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            if nodes.iter().all(|node| self.connected(*node) >= peers) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("the mesh never formed:\n{}", self.tails()));
            }
            std::thread::sleep(POLL);
        }
    }

    fn control(&self, node: usize, path: &str) -> Result<(), String> {
        let (status, body) = self.post(node, path, String::new())?;
        if (200..300).contains(&status) && body["success"] != Value::Bool(false) {
            Ok(())
        } else {
            Err(format!(
                "set-up: {path} on {} answered {status}: {body}",
                self.nodes[node].id
            ))
        }
    }

    fn pause_all(&self) -> Result<(), String> {
        (0..self.nodes.len()).try_for_each(|node| self.control(node, "/api/pause-all"))
    }

    fn resume_all(&self) -> Result<(), String> {
        (0..self.nodes.len()).try_for_each(|node| self.control(node, "/api/resume-all"))?;
        let all: Vec<usize> = (0..self.nodes.len()).collect();
        self.await_mesh(&all, self.nodes.len() - 1)
    }

    fn pause(&self, node: usize, peer: usize) -> Result<(), String> {
        self.control(node, &format!("/api/pause/{}", NAMES[peer]))
    }

    fn resume(&self, node: usize, peer: usize) -> Result<(), String> {
        self.control(node, &format!("/api/resume/{}", NAMES[peer]))
    }

    /// `POST /api/models`: a create (`model` `None`) or a join by id, under
    /// `metamodel` (a digest) or by the id alone (`None`).
    fn register(
        &self,
        node: usize,
        model: Option<&str>,
        metamodel: Option<&str>,
    ) -> Result<String, String> {
        let body = match (model, metamodel) {
            (Some(id), Some(key)) => json!({ "model_id": id, "metamodel_id": key }),
            (Some(id), None) => json!({ "model_id": id }),
            (None, Some(key)) => json!({ "metamodel_id": key }),
            (None, None) => return Err("set-up: a create names a metamodel".to_string()),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "set-up: registering {body} on {} answered {status}: {answer}",
                self.nodes[node].id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("set-up: no model id in {answer}"))
    }

    fn post_op(&self, node: usize, model: &str, op: &ModelOp) -> Result<Value, String> {
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

    /// Post operations that must all be applied (set-up).
    fn submit(&self, node: usize, model: &str, ops: &[ModelOp], what: &str) -> Result<(), String> {
        for op in ops {
            let answer = self.post_op(node, model, op)?;
            if answer["success"] != Value::Bool(true) {
                return Err(format!(
                    "{what}: {} refused {} on {model}: {answer}",
                    self.nodes[node].id,
                    serde_json::to_string(op).unwrap_or_default()
                ));
            }
        }
        Ok(())
    }

    fn read(&self, node: usize, path: &str) -> Result<Value, String> {
        match self.get(node, path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    fn identified(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/state/identified"))
    }

    fn plain(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/state"))
    }

    fn status(&self, node: usize, model: &str) -> Result<Value, String> {
        Ok(status14(
            &self.read(node, &format!("/api/model/{model}/migration"))?,
        ))
    }

    /// The digest `node` lists `model` under on `GET /api/models`.
    fn listed(&self, node: usize, model: &str) -> Result<Option<String>, String> {
        let body = self.read(node, "/api/models")?;
        Ok(body["models"]
            .as_array()
            .ok_or_else(|| format!("no `models` in {body}"))?
            .iter()
            .find(|entry| entry["model_id"] == json!(model))
            .and_then(|entry| listed_digest(&entry["metamodel_id"])))
    }

    /// The digest of the descriptor `node` serves on `model`'s metamodel
    /// route.
    fn served_metamodel(&self, node: usize, model: &str) -> Result<String, String> {
        let path = format!("/api/model/{model}/metamodel");
        let response = self
            .http
            .get(self.nodes[node].url(&path))
            .send()
            .map_err(|err| format!("GET {path}: {err}"))?;
        let status = response.status().as_u16();
        let text = response.text().map_err(|err| err.to_string())?;
        if status != 200 {
            return Err(format!(
                "{} answered {path} with {status}: {text}",
                self.nodes[node].id
            ));
        }
        Ok(digest(&text))
    }

    /// Poll until `check` holds on each of `nodes`, or answer the last reason
    /// it did not.
    fn until(
        &self,
        what: &str,
        nodes: &[usize],
        check: impl Fn(usize) -> Result<(), String>,
    ) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            match nodes.iter().try_for_each(|node| check(*node)) {
                Ok(()) => return Ok(()),
                Err(why) if Instant::now() > deadline => {
                    return Err(format!("{what}, not within {CONVERGE_TIMEOUT:?}: {why}"));
                }
                Err(_) => std::thread::sleep(POLL),
            }
        }
    }

    /// `POST /api/model/{model}/migrate` with `body`: a rebase, which must
    /// be answered 2xx naming `model` and `rebase` within SI.14's bound
    /// (set-up).
    fn rebase(&self, node: usize, model: &str, body: Value) -> Result<(), String> {
        let started = Instant::now();
        let (status, answer) = self.post(
            node,
            &format!("/api/model/{model}/migrate"),
            body.to_string(),
        )?;
        let took = started.elapsed();
        println!(
            "SI16-L3 migrate {model} on {} with {body}: {status} in {} ms",
            self.nodes[node].id,
            took.as_millis()
        );
        if !(200..300).contains(&status)
            || answer["model_id"] != json!(model)
            || answer["strategy"] != json!("rebase")
            || took > ANSWER_BOUND
        {
            return Err(format!(
                "set-up: {}'s rebase of {model} answered {status} {answer} in {took:?}; SI.14's \
                 M15 gives 2xx naming the model and `rebase` within {ANSWER_BOUND:?}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    /// Both reads of `model` on `node` are `want` (projected through
    /// `base`, the running example's labels).
    fn doc_is(
        &self,
        node: usize,
        model: &str,
        base: &BTreeMap<String, String>,
        want: &Value,
    ) -> Result<(), String> {
        let doc = self.identified(node, model)?;
        let projected = project_keep(&doc, base);
        if &projected != want {
            return Err(format!(
                "{} serves {model} as {projected}; the schedule gives {want}",
                NAMES[node]
            ));
        }
        let merged = labels_keep(&doc, base);
        let plain = project_with(&self.plain(node, model)?, &merged);
        if plain != strip_ids(want) {
            return Err(format!(
                "{} serves {model}'s default read as {plain}, not the identified read without \
                 identities",
                NAMES[node]
            ));
        }
        Ok(())
    }

    /// J1 over HTTP: `node` lists `model` under `rev`'s digest and serves
    /// `rev`'s text on its metamodel route.
    fn bound_to(&self, node: usize, model: &str, rev: Rev) -> Result<(), String> {
        let listed = self.listed(node, model)?;
        if listed.as_deref() != Some(rev.digest().as_str()) {
            return Err(format!(
                "{} lists {model} under {listed:?}, not {}",
                NAMES[node],
                rev.digest()
            ));
        }
        let served = self.served_metamodel(node, model)?;
        if served != rev.digest() {
            return Err(format!(
                "{} serves a descriptor of digest {served} on {model}'s metamodel route, not {}",
                NAMES[node],
                rev.digest()
            ));
        }
        Ok(())
    }

    /// Whether `node`'s log says it adopted `donor`'s state for `model`.
    fn adopted_from(&self, node: usize, model: &str, donor: usize) -> bool {
        let line = format!("adopted state from {} for log {model}", NAMES[donor]);
        fs::read_to_string(&self.nodes[node].log)
            .unwrap_or_default()
            .lines()
            .any(|text| text.contains(&line))
    }

    /// Join `model` on `joiner` by `donor`'s state, every other peer of
    /// `joiner` paused for the join, and resumed after when `resume` says so
    /// (set-up): the joiner must then serve `want` and its log must say it
    /// adopted `donor`'s state.
    #[allow(clippy::too_many_arguments)]
    fn join(
        &self,
        joiner: usize,
        donor: usize,
        model: &str,
        metamodel: Option<&str>,
        base: &BTreeMap<String, String>,
        want: &Value,
        resume: bool,
        what: &str,
    ) -> Result<(), String> {
        let others: Vec<usize> = (0..self.nodes.len())
            .filter(|node| *node != joiner && *node != donor)
            .collect();
        for other in &others {
            self.pause(joiner, *other)?;
        }
        let joined = self.register(joiner, Some(model), metamodel)?;
        if joined != model {
            return Err(format!(
                "set-up: {what}: {} joined {joined}, not {model}",
                NAMES[joiner]
            ));
        }
        self.until(
            &format!("set-up: {what}: {} serves {model}", NAMES[joiner]),
            &[joiner],
            |node| self.doc_is(node, model, base, want),
        )?;
        if !self.adopted_from(joiner, model, donor) {
            return Err(format!(
                "set-up: {what}: {} serves {model}, but its log does not say it adopted {}'s \
                 state for it; this test needs the join to be a state transfer:\n{}",
                NAMES[joiner],
                NAMES[donor],
                launch::tail(&self.nodes[joiner].log, 40)
            ));
        }
        if resume {
            for other in &others {
                self.resume(joiner, *other)?;
            }
        }
        println!(
            "SI16-L3 {what}: {} adopted {}'s state for {model}",
            NAMES[joiner], NAMES[donor]
        );
        Ok(())
    }

    /// Record a failing check of the claim.
    fn record(&mut self, outcome: Result<(), String>) {
        if let Err(why) = outcome {
            println!("SI16-L3 FAILED CHECK: {why}");
            self.failures.push(why);
        }
    }

    /// `node`'s status of `model` now, against `want`.
    fn status_is(&self, node: usize, model: &str, want: &Value, at: &str) -> Result<(), String> {
        let got = self.status(node, model)?;
        if &got == want {
            Ok(())
        } else {
            Err(format!(
                "{at} (J3): {} reads {got}; the schedule gives {want}",
                NAMES[node]
            ))
        }
    }

    /// J2 over HTTP: `op`, which only D1 routes, is refused on `node` as it
    /// is on `reference`, a member that wrote the rebase.
    fn refuses_as(
        &self,
        node: usize,
        reference: usize,
        model: &str,
        op: &ModelOp,
        at: &str,
    ) -> Result<(), String> {
        let premise = self.post_op(reference, model, op)?;
        if premise["success"] != Value::Bool(false) {
            return Err(format!(
                "set-up: {at}: {} accepted an edit only D1 routes: {premise}",
                NAMES[reference]
            ));
        }
        let answer = self.post_op(node, model, op)?;
        if answer["success"] == Value::Bool(false) {
            Ok(())
        } else {
            Err(format!(
                "{at} (J2): {} answered {answer} to an edit only D1 routes, which {} refuses \
                 ({premise})",
                NAMES[node], NAMES[reference]
            ))
        }
    }

    /// J2 over HTTP: `op`, written under the revision in force, is accepted
    /// on `node`, a joiner. A refusal is a failing check of the claim, and
    /// the schedule, which needs the edit, ends there.
    fn accepts(
        &mut self,
        node: usize,
        model: &str,
        op: &ModelOp,
        at: &str,
    ) -> Result<bool, String> {
        let answer = self.post_op(node, model, op)?;
        if answer["success"] == Value::Bool(true) {
            return Ok(true);
        }
        self.record(Err(format!(
            "{at} (J2): {} refused an edit written under the revision in force: {answer}; the \
             schedule ends here",
            NAMES[node]
        )));
        Ok(false)
    }

    fn finish(self, done: &str, tag: &str) {
        if self.failures.is_empty() {
            println!("{done}");
            return;
        }
        panic!(
            "si16-l3-{tag}: SI.16 on a node that joined a rebased model by state transfer, {} \
             failing checks:\n{}\n{}",
            self.failures.len(),
            self.failures.join("\n"),
            self.tails()
        );
    }
}

/// What a kept rebase lists besides whom it waits for.
#[derive(Clone, Default)]
struct Lists {
    hidden: Vec<String>,
    dropped: Vec<(String, u64, String)>,
}

/// The status of the rebase `origin@seq` to `rev`, kept, waiting for
/// `waiting`.
fn kept(origin: usize, seq: u64, rev: Rev, lists: &Lists, waiting: &[usize]) -> Value {
    RebaseStatus {
        origin: NAMES[origin].to_string(),
        seq,
        to: rev.digest(),
        hidden: lists.hidden.clone(),
        dropped: lists.dropped.clone(),
        waiting_for: waiting
            .iter()
            .map(|node| NAMES[*node].to_string())
            .collect(),
        ..RebaseStatus::default()
    }
    .render()
}

/// The running example on a new model under D1, created and written on `a`
/// alone: its id and `a`'s labels of its objects.
fn running_example(cluster: &Cluster) -> Result<(String, BTreeMap<String, String>), String> {
    let d1 = Uml::new(D1);
    let m = cluster.register(A, None, Some(&digest(D1)))?;
    cluster.submit(A, &m, &d1.structure(), "set-up: the running example on a")?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    cluster.submit(A, &m, &d1.links(&ids), "set-up: the running example on a")?;
    let base = labels(&cluster.identified(A, &m)?);
    cluster
        .doc_is(A, &m, &base, &Rev::D1.running_example())
        .map_err(|why| format!("set-up: the running example on a: {why}"))?;
    Ok((m, base))
}

// ---------------------------------------------------------------------------
// W, a join during the window
// ---------------------------------------------------------------------------

fn window(cluster: &mut Cluster) -> Result<(), String> {
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let none = Lists::default();

    // W1: the running example on a; b joins it by a's state.
    let (m, base) = running_example(cluster)?;
    let d1_doc = Rev::D1.running_example();
    cluster.join(B, A, &m, Some(&digest(D1)), &base, &d1_doc, true, "W1")?;
    cluster.await_mesh(&[A, B, C], 2)?;

    // W2: the partition; b's late edits under D1; a's rebase.
    cluster.pause_all()?;
    cluster.submit(
        B,
        &m,
        &[
            d1.name_char(DOOR, 4, 's'),
            d1.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent"))),
        ],
        "set-up: W2, b's edits under D1",
    )?;
    cluster.rebase(
        A,
        &m,
        json!({ "metamodel_id": digest(D2), "strategy": "rebase" }),
    )?;
    let mut on_b = d1_doc.clone();
    set(&mut on_b, "Door", "name", json!("Doors"));
    add_to_set(&mut on_b, "Car", "stereotype", &["persistent"]);
    cluster
        .doc_is(B, &m, &base, &on_b)
        .map_err(|why| format!("set-up: W2, b cut off: {why}"))?;
    let mut doc = Rev::D2.running_example();
    let outcome = cluster.doc_is(A, &m, &base, &doc);
    cluster.record(outcome.map_err(|why| format!("W2 (SI.14 RA) on si16-a: {why}")));
    let outcome = cluster.status_is(A, &m, &kept(A, 71, Rev::D2, &none, &[B, C]), "W2");
    cluster.record(outcome);
    println!("SI16-L3 W2: a rebased {m} to D2 (si16-a@71); b wrote under D1, cut off");

    // W3: c joins m during the window, by a's state.
    cluster.resume(A, C)?;
    cluster.resume(C, A)?;
    // si16-b stays cut off on every side: the join leaves c's pause of it.
    cluster.join(C, A, &m, Some(&digest(D1)), &base, &doc, false, "W3")?;
    let outcome = cluster.until("W3 (J1) on si16-c", &[C], |node| {
        cluster.bound_to(node, &m, Rev::D2)
    });
    cluster.record(outcome);
    let on_c = kept(A, 71, Rev::D2, &none, &[B]);
    let on_a = kept(A, 71, Rev::D2, &none, &[B, C]);
    let first = cluster.status_is(C, &m, &on_c, "W3, the first read after the join");
    let as_spec = first.is_ok();
    cluster.record(first);
    let outcome = cluster.status_is(A, &m, &on_a, "W3");
    cluster.record(outcome);
    let outcome = cluster.refuses_as(C, A, &m, &d1.name_char(CAR, 3, 's'), "W3, D1 held");
    cluster.record(outcome);
    // J4: nothing but a delivery moves the window.
    if as_spec {
        let watched = Instant::now();
        while watched.elapsed() < WINDOW_HOLD {
            let outcome = cluster
                .status_is(C, &m, &on_c, "W3, watched with si16-b cut off")
                .and_then(|()| cluster.status_is(A, &m, &on_a, "W3, watched with si16-b cut off"));
            if let Err(why) = outcome {
                cluster.record(Err(format!(
                    "{why} ({:?} into the watch)",
                    watched.elapsed()
                )));
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    println!(
        "SI16-L3 W3: c joined {m} during the window by a's state{}",
        if as_spec {
            ", watched for the hold"
        } else {
            ""
        }
    );

    // W4: every node resumes; b's late edits are translated.
    cluster.resume_all()?;
    set(&mut doc, "Door", "name", json!("Doors"));
    add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
    let outcome = cluster.until("W4 (J5): b's late edits translated", &[A, B, C], |node| {
        cluster.doc_is(node, &m, &base, &doc)
    });
    cluster.record(outcome);
    for (node, waiting) in [(C, vec![B]), (A, vec![B, C]), (B, vec![C])] {
        let want = kept(A, 71, Rev::D2, &none, &waiting);
        let outcome = cluster.until(
            &format!("W4 (J3, J4) on {}", NAMES[node]),
            &[node],
            |node| cluster.status_is(node, &m, &want, "W4"),
        );
        cluster.record(outcome);
    }
    println!("SI16-L3 W4: every node translated b's late edits");

    // W5: b writes under D2; c lets D1 go.
    cluster.submit(
        B,
        &m,
        &[d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb")))],
        "set-up: W5, b's edit under D2",
    )?;
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb"]);
    let outcome = cluster.until("W5: b's edit everywhere", &[A, B, C], |node| {
        cluster.doc_is(node, &m, &base, &doc)
    });
    cluster.record(outcome);
    for (node, waiting) in [(C, vec![]), (A, vec![C]), (B, vec![C])] {
        let want = kept(A, 71, Rev::D2, &none, &waiting);
        let outcome = cluster.until(
            &format!("W5 (J3, J4) on {}", NAMES[node]),
            &[node],
            |node| cluster.status_is(node, &m, &want, "W5"),
        );
        cluster.record(outcome);
    }
    println!("SI16-L3 W5: b wrote under D2");

    // W6: c writes under D2; stable everywhere.
    let hc = d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc")));
    if !cluster.accepts(C, &m, &hc, "W6")? {
        return Ok(());
    }
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    let outcome = cluster.until("W6 (J6): c's edit everywhere", &[A, B, C], |node| {
        cluster.doc_is(node, &m, &base, &doc)
    });
    cluster.record(outcome);
    let stable = kept(A, 71, Rev::D2, &none, &[]);
    let outcome = cluster.until("W6 (J3, J4): stable everywhere", &[A, B, C], |node| {
        cluster.status_is(node, &m, &stable, "W6")
    });
    cluster.record(outcome);
    let outcome = cluster.until("W6 (J1)", &[A, B, C], |node| {
        cluster.bound_to(node, &m, Rev::D2)
    });
    cluster.record(outcome);
    let same = (cluster.identified(A, &m)?, cluster.plain(A, &m)?);
    for node in [B, C] {
        if (cluster.identified(node, &m)?, cluster.plain(node, &m)?) != same {
            cluster.record(Err(format!(
                "W6 (J6): {} serves another document than si16-a",
                NAMES[node]
            )));
        }
    }
    println!("SI16-L3 W6: c wrote under D2");
    Ok(())
}

/// SI.16 at L3, W: a node that joins a rebased model during its window.
#[test]
fn si16_l3_w_a_node_that_joins_during_the_window() {
    let binary = node_binary().unwrap_or_else(|why| panic!("si16-l3-w: no node binary: {why}"));
    let mut cluster = match Cluster::start(&binary, "w") {
        Ok(cluster) => cluster,
        Err(why) => panic!("si16-l3-w: set-up: {why}"),
    };
    if let Err(why) = window(&mut cluster) {
        panic!(
            "si16-l3-w: {why}\n(recorded so far: {:?})\n{}",
            cluster.failures,
            cluster.tails()
        );
    }
    cluster.finish(
        "SI16-L3 W: si16-c joined the rebased model during its window by si16-a's state: listed \
         and served under D2, waited for si16-b alone, translated its late edits, let D1 go once \
         it was heard; one document",
        "w",
    );
}

// ---------------------------------------------------------------------------
// S, a join after the rebase is stable on every member
// ---------------------------------------------------------------------------

fn stable(cluster: &mut Cluster, binary: &Path) -> Result<(), String> {
    let d1 = Uml::new(D1);
    let drop_uml = Uml::new(D2_DROP);

    // S1: the running example on a; b, then c, join it by a's state.
    let (m, base) = running_example(cluster)?;
    let d1_doc = Rev::D1.running_example();
    cluster.join(B, A, &m, Some(&digest(D1)), &base, &d1_doc, true, "S1, b")?;
    cluster.join(C, A, &m, Some(&digest(D1)), &base, &d1_doc, true, "S1, c")?;
    cluster.await_mesh(&[A, B, C], 2)?;
    let wheels = ids_by_label(&cluster.identified(A, &m)?)
        .get("wheels")
        .cloned()
        .ok_or_else(|| "set-up: no object labelled wheels".to_string())?;
    let hidden = Lists {
        hidden: vec![wheels],
        ..Lists::default()
    };
    let dropped = Lists {
        dropped: vec![(
            NAMES[C].to_string(),
            1,
            "ModelElement.taggedValue".to_string(),
        )],
        ..hidden.clone()
    };

    // S2: the partition; c's late edit of what the revision removes; a's
    // rebase naming its drops.
    cluster.pause_all()?;
    cluster.submit(
        C,
        &m,
        &[d1.leaf(CAR_WHEELS, "value", LeafOp::InsertChar { pos: 1, ch: '0' })],
        "set-up: S2, c's edit under D1",
    )?;
    cluster.rebase(
        A,
        &m,
        json!({
            "metamodel_id": digest(D2_DROP),
            "strategy": "rebase",
            "drops": DROPS_TAGGED,
        }),
    )?;
    let mut doc = Rev::Drop.running_example();
    let outcome = cluster.status_is(A, &m, &kept(A, 71, Rev::Drop, &hidden, &[B, C]), "S2");
    cluster.record(outcome);
    println!("SI16-L3 S2: a rebased {m} to D2_DROP (si16-a@71); c wrote under D1, cut off");

    // S3: every node resumes; b and c write under D2_DROP; stable on all.
    cluster.resume_all()?;
    cluster.until("set-up: S3, b and c deliver the rebase", &[B, C], |node| {
        cluster.doc_is(node, &m, &base, &doc)
    })?;
    cluster.submit(
        B,
        &m,
        &[drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb")))],
        "set-up: S3, b's edit under D2_DROP",
    )?;
    cluster.submit(
        C,
        &m,
        &[drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc")))],
        "set-up: S3, c's edit under D2_DROP",
    )?;
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb", "hc"]);
    cluster.until("set-up: S3, every edit everywhere", &[A, B, C], |node| {
        cluster.doc_is(node, &m, &base, &doc)
    })?;
    let settled = kept(A, 71, Rev::Drop, &dropped, &[]);
    cluster.until(
        "set-up: S3, the rebase stable on every member, D1 let go",
        &[A, B, C],
        |node| cluster.status_is(node, &m, &settled, "S3"),
    )?;
    println!("SI16-L3 S3: the rebase is stable on a, b and c; D1 let go everywhere");

    // S4: d starts and joins m by its id alone, by a's state.
    cluster.start_late(binary)?;
    cluster.join(D, A, &m, None, &base, &doc, true, "S4")?;
    let outcome = cluster.until("S4 (J1) on si16-d", &[D], |node| {
        cluster.bound_to(node, &m, Rev::Drop)
    });
    cluster.record(outcome);
    let outcome = cluster.status_is(D, &m, &settled, "S4, the first read after the join");
    cluster.record(outcome);
    let outcome = cluster.refuses_as(
        D,
        A,
        &m,
        &d1.leaf(CAR_WHEELS, "value", LeafOp::InsertChar { pos: 0, ch: '9' }),
        "S4",
    );
    cluster.record(outcome);
    println!("SI16-L3 S4: d joined {m} after the rebase was stable, by a's state");

    // S5: d writes under D2_DROP; one document on four nodes.
    let hd = drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hd")));
    if !cluster.accepts(D, &m, &hd, "S5")? {
        return Ok(());
    }
    add_to_set(&mut doc, "Sedan", "stereotype", &["hd"]);
    let all = [A, B, C, D];
    let outcome = cluster.until("S5 (J6): d's edit everywhere", &all, |node| {
        cluster.doc_is(node, &m, &base, &doc)
    });
    cluster.record(outcome);
    let outcome = cluster.until("S5 (J3): every member", &all, |node| {
        cluster.status_is(node, &m, &settled, "S5")
    });
    cluster.record(outcome);
    let outcome = cluster.until("S5 (J1)", &all, |node| {
        cluster.bound_to(node, &m, Rev::Drop)
    });
    cluster.record(outcome);
    let same = (cluster.identified(A, &m)?, cluster.plain(A, &m)?);
    for node in [B, C, D] {
        if (cluster.identified(node, &m)?, cluster.plain(node, &m)?) != same {
            cluster.record(Err(format!(
                "S5 (J6): {} serves another document than si16-a",
                NAMES[node]
            )));
        }
    }
    println!("SI16-L3 S5: d wrote under D2_DROP");
    Ok(())
}

/// SI.16 at L3, S: a node that joins a rebased model after the rebase is
/// stable on every member.
#[test]
fn si16_l3_s_a_node_that_joins_after_the_rebase_is_stable() {
    let binary = node_binary().unwrap_or_else(|why| panic!("si16-l3-s: no node binary: {why}"));
    let mut cluster = match Cluster::start(&binary, "s") {
        Ok(cluster) => cluster,
        Err(why) => panic!("si16-l3-s: set-up: {why}"),
    };
    if let Err(why) = stable(&mut cluster, &binary) {
        panic!(
            "si16-l3-s: {why}\n(recorded so far: {:?})\n{}",
            cluster.failures,
            cluster.tails()
        );
    }
    cluster.finish(
        "SI16-L3 S: si16-d joined the rebased model by si16-a's state after the rebase was \
         stable: listed and served under D2_DROP, wheels hidden and si16-c@1 dropped as the \
         others list them, waited for no one; one document on four nodes",
        "s",
    );
}
