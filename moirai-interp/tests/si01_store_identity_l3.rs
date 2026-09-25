//! SI.01 over the wire, and the default read left as it was.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`; the contract, R', is
//! restated in `si01/mod.rs`. Four tests:
//!
//! - the oracle's own [`si01::strip`], held to hand-written documents, so the
//!   relation "the default read is the identified read stripped" is checked
//!   with a helper that has itself been checked;
//! - the default read, in process (`QueryableLog::query_state_json`) and over
//!   `GET /api/model/{id}/state` on one `model_node`, byte for byte the text
//!   the tree served before SI.01 for a SimpleUML model and a JSON model;
//! - four `model_node` processes reading every identity through
//!   `GET /api/model/{id}/state/identified` across two partitions and two
//!   joiners by state transfer, with the default read of every replica held
//!   to the identified read stripped at every point the replicas agree.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si01_store_identity_l3 \
//!     -- --test-threads=1
//! ```
//!
//! The tests that need a node follow the skip rule of `e2e_interp.rs`: no
//! binary, print `E2E-SKIP <scenario>: <why>` and return. A skip is not a
//! pass.
#![cfg(feature = "network")]

#[macro_use]
mod si01;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::testing::{JSON_DESCRIPTOR, opened};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::metamodel_digest;
use serde_json::{Value, json};

use si01::*;

// ---------------------------------------------------------------------------
// The oracle's own helper
// ---------------------------------------------------------------------------

/// `strip` on documents written by hand: a JSON model's identified form, a
/// UML one, and a conflict between two transparent values. A key named `__id`
/// inside a JSON object's entries is data and stays.
#[test]
fn si01_oracle_strip_maps_the_identified_form_onto_the_default_form() {
    let identified_json = json!({
        "eClass": "Object", "__id": "r",
        "entry": {
            "title": {"eClass": "String", "value": "x", "__id": "s1"},
            "tags": {"eClass": "Array", "__id": "a", "items": [
                {"eClass": "String", "value": "p", "__id": "s2"}
            ]},
            "n": {"eClass": "Number", "value": 3.0, "__id": "n"},
            "o": {"eClass": "Object", "__id": "o", "entry": {
                "__id": {"eClass": "Boolean", "value": true, "__id": "b"}
            }},
            "k": {"__conflict": [
                {"eClass": "Number", "value": 7.0, "__id": "x"},
                {"eClass": "String", "value": "s", "__id": "y"}
            ]}
        }
    });
    assert_eq!(
        strip(&identified_json, JSON_TRANSPARENT),
        json!({
            "title": "x", "tags": ["p"], "n": 3.0, "o": {"__id": true},
            "k": {"__conflict": [7.0, "s"]}
        })
    );
    assert_eq!(elements(&identified_json).len(), 8);

    let identified_uml = json!({
        "eClass": "Model", "__id": "m", "name": "N",
        "ownedElements": [
            {"eClass": "Class", "__id": "c", "name": "D", "stereotype": ["s"]},
            {"eClass": "Class", "__id": "u"}
        ]
    });
    assert_eq!(
        strip(&identified_uml, OPAQUE),
        json!({
            "eClass": "Model", "name": "N",
            "ownedElements": [
                {"eClass": "Class", "name": "D", "stereotype": ["s"]},
                {"eClass": "Class"}
            ]
        })
    );
    assert_eq!(strip(&Value::Null, OPAQUE), Value::Null);
}

// ---------------------------------------------------------------------------
// The default read, byte for byte
// ---------------------------------------------------------------------------

/// A SimpleUML model with a name, an abstract class carrying a stereotype, a
/// class with no name, a renamed class and a deleted one.
fn uml_schedule(uml: &Uml) -> Vec<ModelOp> {
    let mut ops = uml.named_class(0, "Door");
    ops.push(uml.make_abstract(0));
    ops.push(uml.stereotype(0, "s"));
    ops.extend(uml.named_class(1, ""));
    ops.extend(uml.named_class(2, "Dup"));
    ops.extend(uml.named_class(3, "Dup"));
    ops.push(uml.model_name(0, 'N'));
    ops.extend(uml.rename(2, "Dup", "Hatch"));
    ops.push(uml.delete(3));
    ops
}

/// A JSON document with a string, a number, an array written front first, a
/// flag, a nested object, and a key whose string was removed.
fn json_schedule(js: &Json) -> Vec<ModelOp> {
    let mut ops = js.text("title", "xy");
    ops.push(js.put("n", js.number(inc_float(3.0))));
    ops.push(js.put(
        "tags",
        js.array(InstanceOp::insert(0, js.string(chr(0, 'p')))),
    ));
    ops.push(js.put(
        "tags",
        js.array(InstanceOp::insert(0, js.string(chr(0, 'q')))),
    ));
    ops.push(js.put("flag", js.yes()));
    ops.push(js.put("o", js.entry("k", js.string(chr(0, 'v')))));
    ops.extend(js.text("gone", "z"));
    ops.push(js.remove("gone"));
    ops
}

/// What `GET /api/model/{id}/state` served for `uml_schedule` before SI.01:
/// the text of Moirai `c4052e4`, whose code is that of `770f22b`, captured
/// before any implementation and read against the canonical-form rules of
/// `eval.rs` (sorted keys, an unwritten optional absent, an unwritten text
/// `""`, an unwritten collection `[]`).
const UML_DEFAULT: &str = r#"{"eClass":"Model","name":"N","ownedElements":[{"abstract":true,"attributes":[],"eClass":"Class","generalizations":[],"name":"Door","owner":null,"stereotype":["s"],"taggedValue":[]},{"attributes":[],"eClass":"Class","generalizations":[],"owner":null,"stereotype":[],"taggedValue":[]},{"attributes":[],"eClass":"Class","generalizations":[],"name":"Hatch","owner":null,"stereotype":[],"taggedValue":[]}],"owner":null,"stereotype":[],"taggedValue":[]}"#;

/// The same for `json_schedule`: a plain JSON document, no `eClass` and no
/// record anywhere; the removed string reads `""` and keeps its key, which is
/// `eval.rs`'s documented rule for a `String` at a key.
const JSON_DEFAULT: &str =
    r#"{"flag":true,"gone":"","n":3.0,"o":{"k":"v"},"tags":["q","p"],"title":"xy"}"#;

/// The default read, in process, is the text it was.
#[test]
fn si01_default_read_in_process_is_byte_for_byte_what_it_was() {
    let uml = Uml::new();
    let (mut a, mut b) = opened("m", &uml_descriptor());
    send_all(&mut a, &mut [&mut b], uml_schedule(&uml));
    for replica in [&a, &b] {
        assert_eq!(
            default_read(replica).to_string(),
            UML_DEFAULT,
            "the UML default read moved"
        );
    }

    let js = Json::new();
    let (mut a, mut b) = opened("j", &json_descriptor());
    send_all(&mut a, &mut [&mut b], json_schedule(&js));
    for replica in [&a, &b] {
        assert_eq!(
            default_read(replica).to_string(),
            JSON_DEFAULT,
            "the JSON default read moved"
        );
    }
}

/// The default read, over the wire, is the text it was: the body of
/// `GET /api/model/{id}/state`, byte for byte, still with its version in the
/// `ETag`.
#[test]
fn si01_default_read_over_http_is_byte_for_byte_what_it_was() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si01-default-read: {why}");
            return;
        }
    };
    let run = || -> Result<(), String> {
        let cluster = Cluster::start(
            &binary,
            &["solo"],
            &[("uml", UML), ("json", JSON_DESCRIPTOR)],
        )?;
        let (uml, js) = (Uml::new(), Json::new());
        let m = cluster.register(0, None, &digest(UML))?;
        let j = cluster.register(0, None, &digest(JSON_DESCRIPTOR))?;
        cluster.submit_all(0, &m, &uml_schedule(&uml))?;
        cluster.submit_all(0, &j, &json_schedule(&js))?;
        for (model, expected) in [(&m, UML_DEFAULT), (&j, JSON_DEFAULT)] {
            let (status, etag, body) = cluster.raw(0, &format!("/api/model/{model}/state"))?;
            assert_eq!(
                status, 200,
                "the default read of {model} answered {status}: {body}"
            );
            assert_eq!(body, expected, "the default read of {model} moved");
            assert!(etag.is_some(), "the default read of {model} lost its ETag");
        }
        Ok(())
    };
    if let Err(why) = run() {
        panic!("si01-default-read: {why}");
    }
}

// ---------------------------------------------------------------------------
// L3: model_node processes on loopback
// ---------------------------------------------------------------------------

const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

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

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("an ephemeral port")
        .local_addr()
        .expect("a bound address")
        .port()
}

struct Node {
    id: String,
    http: u16,
    child: Child,
    log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn tail(&self) -> String {
        self.log_text()
            .lines()
            .rev()
            .take(25)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `n` replicas of one binary, meshed by a static `PEERS` list, each serving
/// the descriptors given.
struct Cluster {
    nodes: Vec<Node>,
    _scratch: PathBuf,
    http: reqwest::blocking::Client,
}

impl Cluster {
    fn start(binary: &PathBuf, ids: &[&str], descriptors: &[(&str, &str)]) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-si01-{}-{run}", std::process::id()));
        let metamodels = scratch.join("metamodels");
        fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
        for (name, text) in descriptors {
            fs::write(metamodels.join(format!("{name}.json")), text)
                .map_err(|err| err.to_string())?;
        }
        let ports: Vec<(u16, u16)> = ids.iter().map(|_| (free_port(), free_port())).collect();
        let mut nodes = Vec::new();
        for (index, id) in ids.iter().enumerate() {
            let peers: Vec<String> = ids
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(other, peer)| format!("{peer}:127.0.0.1:{}", ports[other].0))
                .collect();
            let log = scratch.join(format!("{id}.log"));
            let out = fs::File::create(&log).map_err(|err| err.to_string())?;
            let err_out = out.try_clone().map_err(|err| err.to_string())?;
            let child = Command::new(binary)
                .env("REPLICA_ID", id)
                .env("LISTEN_PORT", ports[index].0.to_string())
                .env("HTTP_PORT", ports[index].1.to_string())
                .env("PEERS", peers.join(","))
                .env("LOG_ID", "5101de7a5101de7a5101de7a5101de7a")
                .env("METAMODEL_DIR", &metamodels)
                .env_remove("BOOTNODE_URL")
                .env_remove("DASHBOARD_URL")
                .env_remove("METAMODEL_PATH")
                .stdout(Stdio::from(out))
                .stderr(Stdio::from(err_out))
                .spawn()
                .map_err(|err| format!("spawning {id}: {err}"))?;
            nodes.push(Node {
                id: id.to_string(),
                http: ports[index].1,
                child,
                log,
            });
        }
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

    /// A GET, as status, `ETag` and body text.
    fn raw(&self, node: usize, path: &str) -> Result<(u16, Option<String>, String), String> {
        let response = self
            .http
            .get(self.nodes[node].url(path))
            .send()
            .map_err(|err| format!("GET {path} on {}: {err}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let etag = response
            .headers()
            .get("ETag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((status, etag, body))
    }

    fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        let (status, _, body) = self.raw(node, path)?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn post(&self, node: usize, path: &str, body: String) -> Result<(u16, Value), String> {
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
            while !matches!(self.get(index, "/api/health"), Ok((200, _))) {
                if Instant::now() > deadline {
                    return Err(format!(
                        "{} never answered /api/health:\n{}",
                        node.id,
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
    fn register(&self, node: usize, model: Option<&str>, key: &str) -> Result<String, String> {
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

    fn submit(&self, node: usize, model: &str, op: &ModelOp) -> Result<(), String> {
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

    fn submit_all(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        ops.iter().try_for_each(|op| self.submit(node, model, op))
    }

    /// The identified read, `GET /api/model/{id}/state/identified`.
    fn state(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/state/identified");
        match self.get(node, &path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered the identified read {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// The default read, `GET /api/model/{id}/state`.
    fn default_state(&self, node: usize, model: &str) -> Result<Value, String> {
        match self.get(node, &format!("/api/model/{model}/state"))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("the default read answered {status}: {body}")),
        }
    }

    /// Poll until every node in `on` serves the same identified document for
    /// `model`, and answer with it. A node that answers the identified read
    /// with a status other than 200 for a model it hosts fails at once: the
    /// read is missing, and waiting will not bring it.
    fn converged_on(&self, model: &str, on: &[usize]) -> Result<Value, String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let states: Vec<Result<Value, String>> =
                on.iter().map(|index| self.state(*index, model)).collect();
            if let Some(Err(why)) = states.iter().find(|state| state.is_err()) {
                return Err(why.clone());
            }
            if let Some(Ok(first)) = states.first()
                && !first.is_null()
                && states
                    .iter()
                    .all(|state| state.as_ref().ok() == Some(first))
            {
                return Ok(first.clone());
            }
            if Instant::now() > deadline {
                let last: BTreeMap<String, String> = on
                    .iter()
                    .zip(states.iter())
                    .map(|(index, state)| {
                        (
                            self.nodes[*index].id.clone(),
                            match state {
                                Ok(value) => value.to_string(),
                                Err(why) => why.clone(),
                            },
                        )
                    })
                    .collect();
                return Err(format!(
                    "model {model} never converged; states were {last:?}"
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Every node in `on` serves, on its default read, the identified
    /// document `doc` stripped.
    fn beside(
        &self,
        model: &str,
        on: &[usize],
        doc: &Value,
        transparent: &[(&str, &str)],
    ) -> Result<(), String> {
        let want = strip(doc, transparent);
        for index in on {
            let plain = self.default_state(*index, model)?;
            if plain != want {
                return Err(format!(
                    "{}'s default read of {model} is not its identified read stripped: \
                     default {plain}, identified {doc}",
                    self.nodes[*index].id
                ));
            }
        }
        Ok(())
    }

    /// Poll until `node` holds the model's table (its `Install`).
    fn until_table(&self, node: usize, model: &str) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Ok((200, metrics)) = self.get(node, &format!("/api/model/{model}/metrics"))
                && metrics["delivered_ops"].as_u64().unwrap_or(0) >= 1
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never received the table of {model}",
                    self.nodes[node].id
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    fn pause_all(&self, node: usize) -> Result<(), String> {
        self.control(node, "/api/pause-all")
    }

    fn resume_all(&self, node: usize) -> Result<(), String> {
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

fn digest(text: &str) -> String {
    metamodel_digest(&serde_json::from_str::<Value>(text).expect("the fixture is JSON"))
}

/// Whether `node`'s log says it adopted a transferred state for `model`.
fn adopted(node: &Node, model: &str) -> bool {
    node.log_text()
        .lines()
        .any(|line| line.contains("adopted state from") && line.contains(model))
}

/// Four `model_node` processes: `a` and `b` write, partitioned twice; `c`
/// joins the SimpleUML model and `d` the JSON model afterwards, each by state
/// transfer. The three failure cases of the notation are on the UML model (an
/// empty name, two elements with one name, a rename), and a JSON document is
/// the fourth. Every identity is read through the identified read, and at
/// every point the replicas agree, each one's default read is held to the
/// identified read stripped.
#[test]
fn si01_l3_two_writers_partitioned_and_two_joiners_by_state_transfer_agree_on_every_identity() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si01-l3: {why}");
            return;
        }
    };
    if let Err(why) = l3(&binary) {
        panic!("si01-l3: {why}");
    }
}

fn l3(binary: &PathBuf) -> Result<(), String> {
    let (a, b, c, d) = (0, 1, 2, 3);
    let cluster = Cluster::start(
        binary,
        &["a", "b", "c", "d"],
        &[("uml", UML), ("json", JSON_DESCRIPTOR)],
    )?;
    let uml = Uml::new();
    let js = Json::new();

    let m = cluster.register(a, None, &digest(UML))?;
    let j = cluster.register(a, None, &digest(JSON_DESCRIPTOR))?;
    cluster.register(b, Some(&m), &digest(UML))?;
    cluster.register(b, Some(&j), &digest(JSON_DESCRIPTOR))?;
    cluster.until_table(b, &m)?;
    cluster.until_table(b, &j)?;

    // ---- partition 1: both create the root of each model concurrently ----
    cluster.pause_all(a)?;
    cluster.pause_all(b)?;

    // a: the root and Door in ONE operation, then an unnamed class.
    cluster.submit_all(a, &m, &uml.named_class(0, "Door"))?;
    cluster.submit_all(a, &m, &uml.named_class(1, ""))?;
    // b: the root again, concurrently, by naming the model; two classes
    // sharing one name.
    cluster.submit(b, &m, &uml.model_name(0, 'N'))?;
    cluster.submit_all(b, &m, &uml.named_class(0, "Dup"))?;
    cluster.submit_all(b, &m, &uml.named_class(1, "Dup"))?;

    // a: the root object and `title` in one operation, `n`, and an array
    // made with its first item in one operation.
    cluster.submit_all(a, &j, &js.text("title", "x"))?;
    cluster.submit(a, &j, &js.put("n", js.number(inc_float(3.0))))?;
    cluster.submit(
        a,
        &j,
        &js.put(
            "tags",
            js.array(InstanceOp::insert(0, js.string(chr(0, 'p')))),
        ),
    )?;
    cluster.submit(a, &j, &js.put("shared", js.string(chr(0, 'A'))))?;
    // b: the root object again, concurrently, with the same key `shared`.
    cluster.submit(b, &j, &js.put("shared", js.string(chr(0, 'B'))))?;

    cluster.resume_all(a)?;
    cluster.resume_all(b)?;

    let doc = cluster.converged_on(&m, &[a, b])?;
    cluster.beside(&m, &[a, b], &doc, OPAQUE)?;
    identified(
        &doc,
        5,
        "uml after partition 1 (root, Door, unnamed, Dup, Dup)",
    );
    let root = id_of(&doc);
    let door = id_of(named(&doc, "Door"));
    let unnamed = id_of(named(&doc, ""));
    let dups: Vec<String> = owned(&doc)
        .iter()
        .filter(|element| name_of(element) == "Dup")
        .map(id_of)
        .collect();
    assert_eq!(dups.len(), 2, "two classes named Dup in {doc}");
    let (dup1, dup2) = (dups[0].clone(), dups[1].clone());

    let jdoc = cluster.converged_on(&j, &[a, b])?;
    cluster.beside(&j, &[a, b], &jdoc, JSON_TRANSPARENT)?;
    identified(
        &jdoc,
        6,
        "json after partition 1 (root, title, n, tags, tags[0], shared)",
    );
    let jroot = id_of(&jdoc);
    let title = id_of(member(&jdoc, "title"));
    let n = id_of(member(&jdoc, "n"));
    let tags = id_of(member(&jdoc, "tags"));
    let p = id_of(&items(member(&jdoc, "tags"))[0]);
    let shared = id_of(member(&jdoc, "shared"));

    // ---- partition 2: renames, edits, a sibling, a delete and a re-create ----
    cluster.pause_all(a)?;
    cluster.pause_all(b)?;

    let on_a = cluster.state(a, &m)?;
    cluster.submit_all(a, &m, &uml.rename(pos_of(&on_a, &door), "Door", "Hatch"))?;
    cluster.submit_all(a, &m, &uml.named_class(0, "Aa"))?;

    let on_b = cluster.state(b, &m)?;
    cluster.submit(b, &m, &uml.make_abstract(pos_of(&on_b, &dup1)))?;
    cluster.submit(b, &m, &uml.stereotype(pos_of(&on_b, &unnamed), "s"))?;
    let gone = pos_of(&on_b, &dup2);
    cluster.submit(b, &m, &uml.delete(gone))?;
    cluster.submit_all(b, &m, &uml.named_class(gone, "Dup"))?;

    cluster.submit(a, &j, &js.remove("n"))?;
    cluster.submit(a, &j, &js.put("n", js.number(inc_float(5.0))))?;
    cluster.submit(
        a,
        &j,
        &js.put(
            "tags",
            js.array(InstanceOp::insert(0, js.string(chr(0, 'q')))),
        ),
    )?;
    cluster.submit(b, &j, &js.put("title", js.string(chr(1, 'y'))))?;

    cluster.resume_all(a)?;
    cluster.resume_all(b)?;

    let doc = cluster.converged_on(&m, &[a, b])?;
    cluster.beside(&m, &[a, b], &doc, OPAQUE)?;
    let ids = identified(&doc, 6, "uml after partition 2");
    assert_eq!(id_of(&doc), root, "the root kept its identity");
    assert_eq!(
        id_of(named(&doc, "Hatch")),
        door,
        "the rename kept Door's identity"
    );
    let stereotyped: Vec<&Value> = owned(&doc)
        .iter()
        .filter(|element| element["stereotype"] == serde_json::json!(["s"]))
        .collect();
    assert_eq!(stereotyped.len(), 1, "one stereotyped class in {doc}");
    assert_eq!(
        id_of(stereotyped[0]),
        unnamed,
        "the unnamed class kept its identity"
    );
    let dups: Vec<&Value> = owned(&doc)
        .iter()
        .filter(|element| name_of(element) == "Dup")
        .collect();
    assert_eq!(dups.len(), 2, "two classes named Dup again in {doc}");
    let flagged: Vec<&&Value> = dups
        .iter()
        .filter(|element| element["abstract"] == Value::Bool(true))
        .collect();
    assert_eq!(flagged.len(), 1, "one abstract Dup in {doc}");
    assert_eq!(id_of(flagged[0]), dup1, "the edited Dup kept its identity");
    let recreated = dups
        .iter()
        .find(|element| element["abstract"] != Value::Bool(true))
        .map(|element| id_of(element))
        .expect("the other Dup");
    let before: BTreeSet<&String> = [&root, &door, &unnamed, &dup1, &dup2].into_iter().collect();
    assert!(
        !before.contains(&recreated),
        "the Dup deleted and created again carries an old identity: {recreated}"
    );
    assert!(
        !ids.contains(&dup2),
        "the deleted Dup's identity is still served"
    );
    assert!(
        !before.contains(&id_of(named(&doc, "Aa"))),
        "the new class carries an old identity"
    );

    let jdoc = cluster.converged_on(&j, &[a, b])?;
    cluster.beside(&j, &[a, b], &jdoc, JSON_TRANSPARENT)?;
    let jids = identified(&jdoc, 7, "json after partition 2");
    assert_eq!(id_of(&jdoc), jroot);
    assert_eq!(
        id_of(member(&jdoc, "title")),
        title,
        "a text edit kept the identity"
    );
    assert_eq!(id_of(member(&jdoc, "tags")), tags);
    assert_eq!(
        id_of(&items(member(&jdoc, "tags"))[1]),
        p,
        "an item kept its identity with another put before it"
    );
    assert_eq!(id_of(member(&jdoc, "shared")), shared);
    assert_ne!(
        id_of(member(&jdoc, "n")),
        n,
        "a number removed and put again under its key is a new element"
    );
    assert!(
        !jids.contains(&n),
        "the removed number's identity is still served"
    );

    // ---- the joiners, by state transfer ----
    cluster.register(c, Some(&m), &digest(UML))?;
    cluster.register(d, Some(&j), &digest(JSON_DESCRIPTOR))?;
    let joined = cluster.converged_on(&m, &[a, b, c])?;
    assert_eq!(
        joined, doc,
        "the joiner serves the document the writers agreed on"
    );
    cluster.beside(&m, &[c], &joined, OPAQUE)?;
    let jjoined = cluster.converged_on(&j, &[a, b, d])?;
    assert_eq!(jjoined, jdoc);
    cluster.beside(&j, &[d], &jjoined, JSON_TRANSPARENT)?;
    for (node, model) in [(c, &m), (d, &j)] {
        assert!(
            adopted(&cluster.nodes[node], model),
            "{} did not join {model} by state transfer; its log:\n{}",
            cluster.nodes[node].id,
            cluster.nodes[node].tail()
        );
    }

    // A joiner's own edit leaves the identity where it was, everywhere.
    let on_c = cluster.state(c, &m)?;
    cluster.submit(c, &m, &uml.append(pos_of(&on_c, &door), 5, '2'))?;
    let doc = cluster.converged_on(&m, &[a, b, c])?;
    cluster.beside(&m, &[a, b, c], &doc, OPAQUE)?;
    assert_eq!(id_of(named(&doc, "Hatch2")), door);
    cluster.submit(d, &j, &js.put("title", js.string(chr(2, 'z'))))?;
    let jdoc = cluster.converged_on(&j, &[a, b, d])?;
    cluster.beside(&j, &[a, b, d], &jdoc, JSON_TRANSPARENT)?;
    assert_eq!(id_of(member(&jdoc, "title")), title);
    Ok(())
}
