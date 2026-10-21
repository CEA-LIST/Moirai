//! SI.14 at L3: rebase migration on three `model_node` processes over
//! loopback, across partitions.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.14.md`; the contract (M15
//! to M23) is restated in `si14/mod.rs`. Every operation is posted as JSON
//! on `POST /api/model/{id}/op`, every migration on
//! `POST /api/model/{id}/migrate`, and every read is
//! `GET /api/model/{id}/state/identified` beside `GET /api/model/{id}/state`,
//! `GET /api/model/{id}/migration`, `GET /api/model/{id}/metamodel` and
//! `GET /api/models`.
//!
//! Three nodes. `si14-a` serves D1, D2 and the four other revisions a phase
//! names; `si14-b` serves D1 and D2; `si14-c` serves D1 alone, so D2 reaches
//! it only inside the rebase (RF).
//!
//! - N1, the running example on `m` under D1, written on `a`, served by all
//!   three.
//! - N2, the partition (RA, RB): every node pauses every peer; `b` renames
//!   `Door` to `Doors` (a character) and makes it abstract, `c` adds the
//!   stereotype `persistent` to `Car`, and `a` rebases `m` to D2. At once on
//!   `a`: 2xx naming `m` itself and `strategy` `rebase`, the status naming
//!   the rebase from the first read, D1 held, waiting for `b` and `c`; `m`
//!   listed under D2's digest, D2 served on its metamodel route, the running
//!   example under D2 with every identity kept. `b` and `c`, cut off: D1.
//! - N3, the merge (RB, RF): every node resumes. All three list `m` under
//!   D2's digest, `c` included, serve D2's text on its metamodel route and
//!   one document, the running example under D2 with `Doors`, abstract and
//!   `persistent`, every identity kept.
//! - N4, the window (RH): `c` is cut off again; `b` writes under D2; `a` and
//!   `b` report D1 held, waiting for `si14-c` alone, and still do 20 s later;
//!   every node resumes and `c` writes under D2; all three report D1
//!   dropped, waiting for no one, one document.
//! - N5, refusals (RE): on `m2` under D1, a rebase to D2_FLAG, to D2_RENAME
//!   and to D2_KIND and one to D2_DROP without `drops` are answered 422
//!   naming the feature (copy named for D2_FLAG); `m2` stays under D1, names
//!   no migration and takes an edit.
//! - N6, a removal over the route (RD): `m3`, the running example, rebased
//!   to D2_DROP with `drops`: all three list it under D2_DROP's digest
//!   (none of `b` and `c` serves D2_DROP) and serve it without `taggedValue`,
//!   `wheels` hidden.
//! - N7, `strategy` `copy` named: `m4` migrates as SI.12 copies, to a new
//!   model, which `a` names as `m4`'s successor.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si14_rebase_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! The binary is `MOIRAI_E2E_INTERP_NODE_BIN` or
//! `target/debug/examples/model_node`; a missing one fails the test (it is
//! not a skip).

mod si14;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use moirai_interp::leaf::{LeafOp, Scalar};
use serde_json::{Value, json};

use si14::si12::{
    CAR, D1, D2, D2_RENAME, DOOR, SEDAN, Uml, digest, expected_d1, ids_by_label, labels, project,
    project_with, strip_ids,
};
use si14::*;

const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(120);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
/// M15: `REPLY_TIMEOUT` (5 s) plus one, as SI.13's M8.
const ANSWER_BOUND: Duration = Duration::from_secs(6);
/// RH: how long `a` and `b` are watched holding D1 while `c` is cut off.
const WINDOW_HOLD: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(100);
const LOG_ID: &str = "5114c0de5114c0de5114c0de5114c0de";

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const NAMES: [&str; 3] = ["si14-a", "si14-b", "si14-c"];

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

impl Cluster {
    fn start(binary: &Path) -> Result<Self, String> {
        let scratch = std::env::temp_dir().join(format!("moirai-si14-{}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        let mut specs = Vec::new();
        for (id, served) in [
            (
                NAMES[A],
                vec![
                    ("d1", D1),
                    ("d2", D2),
                    ("d2-drop", D2_DROP),
                    ("d2-flag", D2_FLAG),
                    ("d2-kind", D2_KIND),
                    ("d2-rename", D2_RENAME),
                ],
            ),
            (NAMES[B], vec![("d1", D1), ("d2", D2)]),
            (NAMES[C], vec![("d1", D1)]),
        ] {
            let dir = scratch.join(format!("{id}-metamodels"));
            fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
            for (name, text) in served {
                fs::write(dir.join(format!("{name}.json")), text).map_err(|err| err.to_string())?;
            }
            let dir = dir.to_string_lossy().into_owned();
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
                child: node.child,
                log: node.log,
            })
            .collect();
        let pids: Vec<String> = nodes
            .iter()
            .map(|node| format!("{} pid {}", node.id, node.child.id()))
            .collect();
        println!("SI14-L3 nodes: {}", pids.join(", "));
        let cluster = Cluster {
            nodes,
            scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|err| err.to_string())?,
        };
        cluster.await_health()?;
        cluster.await_mesh()?;
        Ok(cluster)
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

    fn await_mesh(&self) -> Result<(), String> {
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            if (0..self.nodes.len()).all(|node| self.connected(node) + 1 >= self.nodes.len()) {
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
        self.await_mesh()
    }

    /// Cut `c` off: `c` pauses every peer, and `a` and `b` pause `c`.
    fn isolate_c(&self) -> Result<(), String> {
        self.control(C, "/api/pause-all")?;
        self.control(A, &format!("/api/pause/{}", NAMES[C]))?;
        self.control(B, &format!("/api/pause/{}", NAMES[C]))
    }

    fn register(&self, node: usize, model: Option<&str>, digest: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": digest }),
            None => json!({ "metamodel_id": digest }),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "set-up: registering on {} answered {status}: {answer}",
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

    /// Post operations that must all be applied.
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

    /// The digest of the descriptor `node` serves on `model`'s metamodel route.
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

    fn hosted(&self, node: usize) -> Result<Vec<String>, String> {
        let body = self.read(node, "/api/models")?;
        Ok(body["models"]
            .as_array()
            .ok_or_else(|| format!("no `models` in {body}"))?
            .iter()
            .filter_map(|entry| entry["model_id"].as_str().map(str::to_string))
            .collect())
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

    /// The running example on a new model under D1, created on `a` and joined
    /// by `b` and `c`; answers its id and the labels of its objects.
    fn running_example(&self) -> Result<(String, BTreeMap<String, String>), String> {
        let d1 = Uml::new(D1);
        let m = self.register(A, None, &digest(D1))?;
        self.register(B, Some(&m), &digest(D1))?;
        self.register(C, Some(&m), &digest(D1))?;
        self.until(
            "set-up: m's Install on every node",
            &[A, B, C],
            |node| match self.get(node, &format!("/api/model/{m}/metrics"))? {
                (200, metrics) if metrics["delivered_ops"].as_u64().unwrap_or(0) >= 1 => Ok(()),
                other => Err(format!(
                    "node {node} has not delivered m's Install: {other:?}"
                )),
            },
        )?;
        self.submit(A, &m, &d1.structure(), "set-up")?;
        let ids = ids_by_label(&self.identified(A, &m)?);
        self.submit(A, &m, &d1.links(&ids), "set-up")?;
        self.until(
            "set-up: the running example on every node",
            &[A, B, C],
            |node| {
                let doc = self.identified(node, &m)?;
                if doc.is_null() || project(&doc) != expected_d1() {
                    return Err(format!("node {node} serves {doc}"));
                }
                Ok(())
            },
        )?;
        Ok((m.clone(), labels(&self.identified(A, &m)?)))
    }

    /// `POST /api/model/{model}/migrate` with `body`, timed against M15's
    /// bound.
    fn migrate(&self, node: usize, model: &str, body: Value) -> Result<(u16, Value), String> {
        let started = Instant::now();
        let answer = self.post(
            node,
            &format!("/api/model/{model}/migrate"),
            body.to_string(),
        )?;
        let took = started.elapsed();
        println!(
            "SI14-L3 migrate {model} on {} with {body}: {} in {} ms",
            self.nodes[node].id,
            answer.0,
            took.as_millis()
        );
        if took > ANSWER_BOUND {
            return Err(format!(
                "SI.14 (M15): the migrate answer took {took:?}, past {ANSWER_BOUND:?}"
            ));
        }
        Ok(answer)
    }

    /// The model `model` on `node` serves `want` (projected through `base`)
    /// in both reads, under `rev`: listed under its digest, its descriptor
    /// served on its metamodel route.
    fn serves(
        &self,
        node: usize,
        model: &str,
        base: &BTreeMap<String, String>,
        rev: Rev,
        want: &Value,
    ) -> Result<(), String> {
        let doc = self.identified(node, model)?;
        let projected = project_keep(&doc, base);
        if &projected != want {
            return Err(format!(
                "node {} serves {model} as {projected}; the schedule gives {want}",
                NAMES[node]
            ));
        }
        let merged = labels_keep(&doc, base);
        let plain = project_with(&self.plain(node, model)?, &merged);
        if plain != strip_ids(want) {
            return Err(format!(
                "node {} serves {model}'s default read as {plain}, not the identified read without \
                 identities",
                NAMES[node]
            ));
        }
        match self.listed(node, model)? {
            Some(listed) if listed == rev.digest() => {}
            other => {
                return Err(format!(
                    "SI.14 (M17, M21): node {} lists {model} under {other:?}, not {}",
                    NAMES[node],
                    rev.digest()
                ));
            }
        }
        let served = self.served_metamodel(node, model)?;
        if served != rev.digest() {
            return Err(format!(
                "SI.14 (M17, M21): node {} serves a descriptor of digest {served} on {model}'s \
                 metamodel route, not {}",
                NAMES[node],
                rev.digest()
            ));
        }
        Ok(())
    }
}

fn rebase_status(origin: &str, seq: u64, rev: Rev, hidden: Vec<String>, waiting: &[&str]) -> Value {
    RebaseStatus {
        origin: origin.into(),
        seq,
        to: rev.digest(),
        hidden,
        waiting_for: waiting.iter().map(|s| s.to_string()).collect(),
        ..RebaseStatus::default()
    }
    .render()
}

fn scenario(cluster: &Cluster) -> Result<(), String> {
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);

    // N1: the running example on m under D1.
    let (m, base) = cluster.running_example()?;
    for node in [A, B, C] {
        if cluster.status(node, &m)? != quiet() {
            return Err(format!(
                "N1: node {} names a migration of m before any",
                NAMES[node]
            ));
        }
    }
    let k = cluster.read(A, &format!("/api/model/{m}/metrics"))?["delivered_ops"]
        .as_u64()
        .unwrap_or(0);
    println!("SI14-L3 N1: the running example on {m} under D1 on three nodes, {k} events on a");

    // N2: the partition; b's and c's edits, a's rebase.
    cluster.pause_all()?;
    cluster.submit(
        B,
        &m,
        &[
            d1.name_char(DOOR, 4, 's'),
            d1.leaf(DOOR, "abstract", LeafOp::Enable),
        ],
        "N2: b's edits",
    )?;
    cluster.submit(
        C,
        &m,
        &[d1.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent")))],
        "N2: c's edit",
    )?;
    let (status, answer) = cluster.migrate(
        A,
        &m,
        json!({ "metamodel_id": digest(D2), "strategy": "rebase" }),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "SI.14 (M15): a's rebase of m answered {status}: {answer}"
        ));
    }
    if answer["model_id"] != json!(m) || answer["strategy"] != json!("rebase") {
        return Err(format!(
            "SI.14 (M15): a's rebase of {m} answered {status} {answer}: a rebase names the model \
             itself and the strategy `rebase`"
        ));
    }
    // The first read after the answer names the rebase (M15). `a`'s rebase
    // is its event k + 1, written having delivered nothing of b's and c's.
    let first = cluster.status(A, &m)?;
    let want = rebase_status(NAMES[A], k + 1, Rev::D2, Vec::new(), &[NAMES[B], NAMES[C]]);
    if first != want {
        return Err(format!(
            "SI.14 (M15, M23): the first status read on a after its rebase is {first}, not {want}"
        ));
    }
    cluster
        .serves(A, &m, &base, Rev::D2, &Rev::D2.running_example())
        .map_err(|why| format!("N2 (RA): {why}"))?;
    for node in [B, C] {
        cluster
            .serves(node, &m, &base, Rev::D1, &{
                let mut doc = Rev::D1.running_example();
                if node == B {
                    set(&mut doc, "Door", "name", json!("Doors"));
                    set(&mut doc, "Door", "abstract", json!(true));
                } else {
                    add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
                }
                doc
            })
            .map_err(|why| format!("N2: cut off, {why}"))?;
        if cluster.status(node, &m)? != quiet() {
            return Err(format!("N2: {}, cut off, names a migration", NAMES[node]));
        }
    }
    println!(
        "SI14-L3 N2: a rebased {m} to D2 in place across the partition; b and c each edited m under D1"
    );

    // N3: the merge.
    cluster.resume_all()?;
    let mut doc = Rev::D2.running_example();
    set(&mut doc, "Door", "name", json!("Doors"));
    set(&mut doc, "Door", "abstract", json!(true));
    add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
    cluster.until(
        "N3 (RB, RF): every node serves m under D2 with the three late edits",
        &[A, B, C],
        |node| cluster.serves(node, &m, &base, Rev::D2, &doc),
    )?;
    for node in [A, B, C] {
        let status = cluster.status(node, &m)?;
        if status["strategy"] != json!("rebase")
            || status["rebase"]["to"] != json!(Rev::D2.digest())
        {
            return Err(format!("N3 (M22): {} reads {status}", NAMES[node]));
        }
    }
    let same = cluster.identified(A, &m)?;
    for node in [B, C] {
        if cluster.identified(node, &m)? != same {
            return Err(format!(
                "N3: {} serves another document than a",
                NAMES[node]
            ));
        }
    }
    println!(
        "SI14-L3 N3: all three serve m under D2 with Doors, abstract and persistent; c lists it under D2's digest"
    );

    // N4: the window.
    cluster.isolate_c()?;
    cluster.submit(
        B,
        &m,
        &[d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb")))],
        "N4 (M17): b's edit under D2",
    )?;
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb"]);
    let held = rebase_status(NAMES[A], k + 1, Rev::D2, Vec::new(), &[NAMES[C]]);
    cluster.until(
        "N4 (RH): a and b hold D1 and wait for si14-c alone",
        &[A, B],
        |node| {
            cluster.serves(node, &m, &base, Rev::D2, &doc)?;
            let status = cluster.status(node, &m)?;
            if status != held {
                return Err(format!("{} reads {status}, not {held}", NAMES[node]));
            }
            Ok(())
        },
    )?;
    let watched = Instant::now();
    while watched.elapsed() < WINDOW_HOLD {
        for node in [A, B] {
            let status = cluster.status(node, &m)?;
            if status != held {
                return Err(format!(
                    "SI.14 (M23): {} dropped its hold {:?} into the window with si14-c silent: \
                     {status}",
                    NAMES[node],
                    watched.elapsed()
                ));
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    cluster.resume_all()?;
    cluster.until("N4: c delivers b's edit", &[C], |node| {
        cluster.serves(node, &m, &base, Rev::D2, &doc)
    })?;
    cluster.submit(
        C,
        &m,
        &[d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc")))],
        "N4: c's edit under D2",
    )?;
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    let stable = rebase_status(NAMES[A], k + 1, Rev::D2, Vec::new(), &[]);
    cluster.until(
        "N4 (RH): every node drops D1 once the rebase is stable",
        &[A, B, C],
        |node| {
            cluster.serves(node, &m, &base, Rev::D2, &doc)?;
            let status = cluster.status(node, &m)?;
            if status != stable {
                return Err(format!("{} reads {status}, not {stable}", NAMES[node]));
            }
            Ok(())
        },
    )?;
    println!(
        "SI14-L3 N4: a and b held D1 waiting for si14-c for {WINDOW_HOLD:?}, then all three dropped it"
    );

    // N5: refusals, on a small model m2.
    let m2 = cluster.register(A, None, &digest(D1))?;
    let small = vec![
        d1.root(),
        d1.insert(si14::si12::At::Root, "ownedElements", 0, "Class"),
        d1.leaf(CAR, "abstract", LeafOp::Enable),
        d1.leaf(CAR, "owner", LeafOp::Write(Scalar::text("Vehicle"))),
    ];
    cluster.submit(A, &m2, &small, "set-up: m2")?;
    let hosted = cluster.hosted(A)?;
    for (what, text, names, copy, drops) in [
        ("D2_FLAG", D2_FLAG, vec!["Class.abstract"], true, json!([])),
        (
            "D2_RENAME",
            D2_RENAME,
            vec!["Class.abstract"],
            false,
            json!([]),
        ),
        (
            "D2_KIND",
            D2_KIND,
            vec!["Packageable.owner"],
            false,
            json!([]),
        ),
        (
            "D2_DROP without drops",
            D2_DROP,
            vec!["ModelElement.taggedValue"],
            false,
            json!([]),
        ),
    ] {
        let (status, answer) = cluster.migrate(
            A,
            &m2,
            json!({ "metamodel_id": digest(text), "strategy": "rebase", "drops": drops }),
        )?;
        let reason = answer.to_string();
        if status != 422 || names.iter().any(|name| !reason.contains(name)) {
            return Err(format!(
                "SI.14 (M19): a rebase to {what} answered {status} {answer}; it is refused (422) \
                 naming {names:?}"
            ));
        }
        if copy && !reason.to_lowercase().contains("copy") {
            return Err(format!(
                "SI.14 (M19): copy accepts {what}, and the refusal does not say so: {answer}"
            ));
        }
    }
    if cluster.hosted(A)? != hosted {
        return Err("SI.14 (M19): a hosts another model after four refused rebases".into());
    }
    if cluster.status(A, &m2)? != quiet() || cluster.listed(A, &m2)? != Some(digest(D1)) {
        return Err("SI.14 (M19): m2 moved after four refused rebases".into());
    }
    cluster.submit(
        A,
        &m2,
        &[d1.name_char(CAR, 0, 'C')],
        "N5 (M19): m2 takes an edit",
    )?;
    println!("SI14-L3 N5: four rebases refused by name over the route, m2 still writable under D1");

    // N6: a removal over the route.
    let (m3, base3) = cluster.running_example()?;
    let (status, answer) = cluster.migrate(
        A,
        &m3,
        json!({ "metamodel_id": digest(D2_DROP), "strategy": "rebase", "drops": DROPS_TAGGED }),
    )?;
    if !(200..300).contains(&status) || answer["model_id"] != json!(m3) {
        return Err(format!(
            "SI.14 (M15, M20): a's rebase of m3 to D2_DROP answered {status}: {answer}"
        ));
    }
    let wheels = ids_by_label(&cluster.identified(A, &m3)?)
        .get("wheels")
        .cloned();
    if wheels.is_some() {
        return Err("SI.14 (M20): a still serves wheels after the removal".into());
    }
    let wheels = base3
        .iter()
        .find(|(_, label)| label.as_str() == "wheels")
        .map(|(id, _)| id.clone())
        .ok_or("set-up: no wheels in m3")?;
    cluster.until(
        "N6 (RD, RF): every node serves m3 under D2_DROP, wheels hidden",
        &[A, B, C],
        |node| {
            cluster.serves(node, &m3, &base3, Rev::Drop, &Rev::Drop.running_example())?;
            let status = cluster.status(node, &m3)?;
            if status["rebase"]["hidden"] != json!([wheels])
                || status["rebase"]["dropped"] != json!([])
            {
                return Err(format!("{} reads {status}", NAMES[node]));
            }
            Ok(())
        },
    )?;
    println!("SI14-L3 N6: m3 rebased to D2_DROP with drops on all three, wheels hidden and listed");

    // N7: strategy copy named.
    let m4 = cluster.register(A, None, &digest(D1))?;
    cluster.submit(A, &m4, &[d1.root()], "set-up: m4")?;
    let (status, answer) = cluster.migrate(
        A,
        &m4,
        json!({ "metamodel_id": digest(D2), "strategy": "copy" }),
    )?;
    let copy = answer["model_id"].as_str().unwrap_or_default().to_string();
    if !(200..300).contains(&status) || copy.is_empty() || copy == m4 {
        return Err(format!(
            "SI.14 (M15): a migration with strategy copy answered {status} {answer}; copy opens a \
             new model"
        ));
    }
    let named = cluster.status(A, &m4)?;
    if named["migrated_to"] != json!(copy) {
        return Err(format!("SI.14 (M15): after strategy copy a reads {named}"));
    }
    println!("SI14-L3 N7: strategy copy migrates m4 to {copy}, as SI.12 does");
    Ok(())
}

/// SI.14 at L3, three nodes.
#[test]
fn si14_l3_rebase_in_place_on_three_nodes() {
    let binary = node_binary().unwrap_or_else(|why| panic!("si14-l3: no node binary: {why}"));
    let cluster = match Cluster::start(&binary) {
        Ok(cluster) => cluster,
        Err(why) => panic!("si14-l3: set-up: {why}"),
    };
    if let Err(why) = scenario(&cluster) {
        panic!("si14-l3: {why}\n{}", cluster.tails());
    }
}

// ---------------------------------------------------------------------------
// RH on nodes that joined by state transfer (round 3)
// ---------------------------------------------------------------------------

impl Cluster {
    /// The three nodes of [`Cluster::start`], serving the same descriptors,
    /// in a scratch directory of their own, so that this test and the one
    /// above never share one when a run takes them in parallel.
    fn start_window(binary: &Path) -> Result<Self, String> {
        let scratch = std::env::temp_dir().join(format!("moirai-si14-w-{}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        let mut specs = Vec::new();
        for (id, served) in [
            (
                NAMES[A],
                vec![
                    ("d1", D1),
                    ("d2", D2),
                    ("d2-drop", D2_DROP),
                    ("d2-flag", D2_FLAG),
                    ("d2-kind", D2_KIND),
                    ("d2-rename", D2_RENAME),
                ],
            ),
            (NAMES[B], vec![("d1", D1), ("d2", D2)]),
            (NAMES[C], vec![("d1", D1)]),
        ] {
            let dir = scratch.join(format!("{id}-metamodels"));
            fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
            for (name, text) in served {
                fs::write(dir.join(format!("{name}.json")), text).map_err(|err| err.to_string())?;
            }
            let dir = dir.to_string_lossy().into_owned();
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
                child: node.child,
                log: node.log,
            })
            .collect();
        let pids: Vec<String> = nodes
            .iter()
            .map(|node| format!("{} pid {}", node.id, node.child.id()))
            .collect();
        println!("SI14-L3-W nodes: {}", pids.join(", "));
        let cluster = Cluster {
            nodes,
            scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|err| err.to_string())?,
        };
        cluster.await_health()?;
        cluster.await_mesh()?;
        Ok(cluster)
    }
}

/// Whether `node`'s log says it adopted `donor`'s state for `model`: the
/// line a `model_node` prints when it installs a state transfer.
fn adopted_from(node: &Node, model: &str, donor: &str) -> bool {
    let line = format!("adopted state from {donor} for log {model}");
    fs::read_to_string(&node.log)
        .unwrap_or_default()
        .lines()
        .any(|text| text.contains(&line))
}

/// What a node's `rebase.waiting_for`, `got`, says wrongly against R3's
/// `want`: the node naming itself, a member it has not heard deliver the
/// rebase left out, a member it has heard named.
fn waiting_mismatch(node: usize, got: &Value, want: &[&str]) -> String {
    let got: Vec<String> = got
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let mut notes = Vec::new();
    if got.iter().any(|member| member == NAMES[node]) {
        notes.push(format!("{} names itself", NAMES[node]));
    }
    for member in want {
        if !got.iter().any(|named| named == member) {
            notes.push(format!("{member} left out"));
        }
    }
    for member in &got {
        if member != NAMES[node] && !want.contains(&member.as_str()) {
            notes.push(format!("{member} named, already heard"));
        }
    }
    if notes.is_empty() {
        "waiting_for as R3 gives; another key differs".to_string()
    } else {
        notes.join(", ")
    }
}

/// The window on nodes that joined `m` by state transfer before any
/// migration, the joins made deterministic (round 3).
///
/// - W1, the joins: `a` creates `m` under D1 and writes the running example
///   while `b` and `c` host nothing of it. `b` pauses `c` and joins `m` by
///   its id: its log is empty, its one connected peer hosting `m` is `a`, so
///   it takes `a`'s state; then `c`, pausing `b`, does the same. Each
///   joiner's log must say it adopted `si14-a`'s state for `m`.
/// - W2: every node pauses every peer; `a`, having no rebase, adds
///   `persistent` to `Car` under D1; `b` rebases `m` to D2 (`si14-b@1`). The
///   first status read on `b`: D1 held, waiting for `si14-a` and `si14-c`.
/// - W3: `b` and `c` resume each other, `a` stays cut off. `c` delivers the
///   rebase and serves D2, which it held nowhere else (RF); `c` waits for
///   `si14-a`, `b` for `si14-a` and `si14-c`, and both still do 20 s later.
/// - W4: every node resumes. `a` delivers the rebase, `b` and `c` translate
///   `a`'s late edit; then `a` writes `ha` on `Sedan` under D2 and `c` stays
///   silent. `a` and `b` wait for `si14-c`; `c`, having heard `a` and `b`
///   deliver the rebase, holds D1 no longer, within the convergence bound.
/// - W5: `c` writes `hc` under D2: every node drops D1, one document.
///
/// Every status is R3 computed from the schedule: no node names itself, and
/// every member a node has not heard deliver the rebase is named, the donor
/// `si14-a` included. A status that differs is recorded and the schedule
/// goes on, so that one run reports every failing read; anything else that
/// fails stops the run.
fn window_after_state_transfer(cluster: &Cluster) -> Result<(), String> {
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let mut failures: Vec<String> = Vec::new();

    // W1: the running example on a alone, then two joins by state transfer.
    let m = cluster.register(A, None, &digest(D1))?;
    cluster.submit(
        A,
        &m,
        &d1.structure(),
        "set-up: W1, the running example on a",
    )?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    cluster.submit(
        A,
        &m,
        &d1.links(&ids),
        "set-up: W1, the running example on a",
    )?;
    let base = labels(&cluster.identified(A, &m)?);
    cluster
        .serves(A, &m, &base, Rev::D1, &Rev::D1.running_example())
        .map_err(|why| format!("set-up: W1: {why}"))?;
    for (joiner, other) in [(B, C), (C, B)] {
        cluster.control(joiner, &format!("/api/pause/{}", NAMES[other]))?;
        cluster.register(joiner, Some(&m), &digest(D1))?;
        cluster.until(
            &format!("set-up: W1, {} joins m", NAMES[joiner]),
            &[joiner],
            |node| cluster.serves(node, &m, &base, Rev::D1, &Rev::D1.running_example()),
        )?;
        if !adopted_from(&cluster.nodes[joiner], &m, NAMES[A]) {
            return Err(format!(
                "set-up: W1: {} serves m, but its log does not say it adopted si14-a's state for \
                 m; this test needs the join to be a state transfer:\n{}",
                NAMES[joiner],
                launch::tail(&cluster.nodes[joiner].log, 40)
            ));
        }
        cluster.control(joiner, &format!("/api/resume/{}", NAMES[other]))?;
    }
    cluster.await_mesh()?;
    for node in [A, B, C] {
        if cluster.status(node, &m)? != quiet() {
            return Err(format!(
                "set-up: W1: {} names a migration of m before any",
                NAMES[node]
            ));
        }
    }
    println!("SI14-L3 W1: si14-b and si14-c joined {m} by state transfers from si14-a");

    // W2: the partition; a's late edit; b's rebase.
    cluster.pause_all()?;
    cluster.submit(
        A,
        &m,
        &[d1.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent")))],
        "W2: a's edit under D1",
    )?;
    let (status, answer) = cluster.migrate(
        B,
        &m,
        json!({ "metamodel_id": digest(D2), "strategy": "rebase" }),
    )?;
    if !(200..300).contains(&status)
        || answer["model_id"] != json!(m)
        || answer["strategy"] != json!("rebase")
    {
        return Err(format!(
            "SI.14 (M15): b's rebase of {m} answered {status} {answer}: a rebase names the model \
             itself and the strategy `rebase`"
        ));
    }
    // b wrote nothing in m before (a join writes nothing): its rebase is
    // si14-b@1.
    let first: [(usize, &[&str]); 1] = [(B, &[NAMES[A], NAMES[C]])];
    for (node, waiting) in first {
        let got = cluster.status(node, &m)?;
        let want = rebase_status(NAMES[B], 1, Rev::D2, Vec::new(), waiting);
        if got != want {
            failures.push(format!(
                "W2 (M15, M23, R3): {}, which joined m by si14-a's state transfer, reads {got} on \
                 the first read after its own rebase; R3 gives {want} ({})",
                NAMES[node],
                waiting_mismatch(node, &got["rebase"]["waiting_for"], waiting)
            ));
        }
    }
    cluster
        .serves(B, &m, &base, Rev::D2, &Rev::D2.running_example())
        .map_err(|why| format!("W2 (RA): {why}"))?;
    let mut on_a = Rev::D1.running_example();
    add_to_set(&mut on_a, "Car", "stereotype", &["persistent"]);
    cluster
        .serves(A, &m, &base, Rev::D1, &on_a)
        .map_err(|why| format!("W2: cut off, {why}"))?;
    cluster
        .serves(C, &m, &base, Rev::D1, &Rev::D1.running_example())
        .map_err(|why| format!("W2: cut off, {why}"))?;
    for node in [A, C] {
        if cluster.status(node, &m)? != quiet() {
            return Err(format!("W2: {}, cut off, names a migration", NAMES[node]));
        }
    }
    println!("SI14-L3 W2: b rebased {m} to D2 in place; a edited m under D1, cut off");

    // W3: b and c hear each other; a stays cut off.
    cluster.control(B, &format!("/api/resume/{}", NAMES[C]))?;
    cluster.control(C, &format!("/api/resume/{}", NAMES[B]))?;
    cluster.until(
        "W3 (RF): si14-c delivers b's rebase and serves m under D2",
        &[C],
        |node| cluster.serves(node, &m, &base, Rev::D2, &Rev::D2.running_example()),
    )?;
    let window: [(usize, &[&str]); 2] = [(B, &[NAMES[A], NAMES[C]]), (C, &[NAMES[A]])];
    let mut as_r3 = true;
    for (node, waiting) in window {
        let got = cluster.status(node, &m)?;
        let want = rebase_status(NAMES[B], 1, Rev::D2, Vec::new(), waiting);
        if got != want {
            as_r3 = false;
            failures.push(format!(
                "W3 (M23, R3): {} reads {got} once b and c hear each other, si14-a cut off; R3 \
                 gives {want} ({})",
                NAMES[node],
                waiting_mismatch(node, &got["rebase"]["waiting_for"], waiting)
            ));
        }
    }
    if as_r3 {
        let watched = Instant::now();
        'watch: while watched.elapsed() < WINDOW_HOLD {
            for (node, waiting) in window {
                let got = cluster.status(node, &m)?;
                let want = rebase_status(NAMES[B], 1, Rev::D2, Vec::new(), waiting);
                if got != want {
                    failures.push(format!(
                        "W3 (M23): {} moved {:?} into the window with si14-a silent: {got}, not \
                         {want}",
                        NAMES[node],
                        watched.elapsed()
                    ));
                    break 'watch;
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    println!(
        "SI14-L3 W3: c delivered b's rebase; b and c read their window with si14-a cut off{}",
        if as_r3 { ", watched for the hold" } else { "" }
    );

    // W4: every node resumes; a writes under D2, c silent.
    cluster.resume_all()?;
    let mut doc = Rev::D2.running_example();
    add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
    cluster.until(
        "W4 (RB): every node serves m under D2 with a's late edit",
        &[A, B, C],
        |node| cluster.serves(node, &m, &base, Rev::D2, &doc),
    )?;
    cluster.submit(
        A,
        &m,
        &[d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("ha")))],
        "W4: a's edit under D2",
    )?;
    add_to_set(&mut doc, "Sedan", "stereotype", &["ha"]);
    cluster.until(
        "W4: every node serves a's edit under D2",
        &[A, B, C],
        |node| cluster.serves(node, &m, &base, Rev::D2, &doc),
    )?;
    let silent: [(usize, &[&str]); 3] = [(A, &[NAMES[C]]), (B, &[NAMES[C]]), (C, &[])];
    for (node, waiting) in silent {
        let want = rebase_status(NAMES[B], 1, Rev::D2, Vec::new(), waiting);
        if let Err(why) = cluster.until(
            &format!(
                "W4 (RH, M23): {} reads the window R3 gives, si14-c silent",
                NAMES[node]
            ),
            &[node],
            |node| {
                let got = cluster.status(node, &m)?;
                if got == want {
                    Ok(())
                } else {
                    Err(format!(
                        "{} reads {got}; R3 gives {want} ({})",
                        NAMES[node],
                        waiting_mismatch(node, &got["rebase"]["waiting_for"], waiting)
                    ))
                }
            },
        ) {
            failures.push(why);
        }
    }
    println!("SI14-L3 W4: every node translated a's late edit and served a's write under D2");

    // W5: c writes under D2; every member heard on every node.
    cluster.submit(
        C,
        &m,
        &[d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc")))],
        "W5: c's edit under D2",
    )?;
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    let stable = rebase_status(NAMES[B], 1, Rev::D2, Vec::new(), &[]);
    if let Err(why) = cluster.until(
        "W5 (RH): every node drops D1 once every member is heard",
        &[A, B, C],
        |node| {
            cluster.serves(node, &m, &base, Rev::D2, &doc)?;
            let got = cluster.status(node, &m)?;
            if got == stable {
                Ok(())
            } else {
                Err(format!("{} reads {got}, not {stable}", NAMES[node]))
            }
        },
    ) {
        failures.push(why);
    }
    let same = cluster.identified(A, &m)?;
    for node in [B, C] {
        if cluster.identified(node, &m)? != same {
            failures.push(format!(
                "W5: {} serves another document than si14-a",
                NAMES[node]
            ));
        }
    }
    if failures.is_empty() {
        println!(
            "SI14-L3 W5: nodes that joined by state transfer named every member not yet heard, \
             never themselves, and all three dropped D1 once every member was heard"
        );
        Ok(())
    } else {
        Err(format!(
            "SI.14 (RH, M23, R3) on nodes that joined by state transfer, {} failing reads:\n{}",
            failures.len(),
            failures.join("\n")
        ))
    }
}

/// SI.14 at L3, round 3: RH on nodes that joined by state transfer.
#[test]
fn si14_l3_window_on_nodes_that_joined_by_state_transfer() {
    let binary = node_binary().unwrap_or_else(|why| panic!("si14-l3-w: no node binary: {why}"));
    let cluster = match Cluster::start_window(&binary) {
        Ok(cluster) => cluster,
        Err(why) => panic!("si14-l3-w: set-up: {why}"),
    };
    if let Err(why) = window_after_state_transfer(&cluster) {
        panic!("si14-l3-w: {why}\n{}", cluster.tails());
    }
}
