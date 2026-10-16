//! SI.12 at L3: copy-migration of a SimpleUML model on three `model_node`
//! processes, across a partition.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.12.md`; the contract (M1 to
//! M7) is restated in `si12/mod.rs`. Every operation is posted as JSON on
//! `POST /api/model/{id}/op`, the migration on `POST /api/model/{id}/migrate`,
//! and every read is `GET /api/model/{id}/state/identified` beside
//! `GET /api/model/{id}/state` and `GET /api/model/{id}/migration`.
//!
//! Three nodes. `si12-a` serves `D1`, `D2` and the two refused revisions;
//! `si12-b` serves `D1` and `D2`; `si12-c` serves `D1` alone, so the copy
//! reaches it only through the copy's own history, as any model joined by id
//! does (ED.20).
//!
//! - L1, the running example on `m` under `D1`, written on `a` and served by
//!   all three.
//! - L2, the partition: every node pauses every peer. `b` renames `Door` to
//!   `Doors` and makes it abstract, `c` adds the stereotype `persistent` to
//!   `Car`, and `a` migrates `m` to `D2`. At once on `a`: the copy is listed
//!   under `D2`'s digest, `m` names it, refuses an edit naming it, and serves
//!   what it served before the migration; `b`, cut off, names no migration.
//! - L3, the merge: every node resumes. All three name the copy, list `b`'s
//!   events 1 and 2 and `c`'s event 1 as refused, serve `a`'s document from
//!   before the migration, and refuse an edit naming the copy, on the
//!   single and the batch route; a second migration of `m` is refused.
//! - L4, the copy everywhere: `b` and `c` join the copy by id alone; all
//!   three serve one document, the running example under `D2`, list the copy
//!   under `D2`'s digest and serve `D2` on its metamodel route.
//! - L5, the copy merges by `D2`: across a second partition `a` renames
//!   `Door` to `Gate` and `b` to `Hatch` in the copy; after the merge all
//!   three read one of the two names, the same.
//! - L6, refused revisions: on a second model `m2` under `D1`, a migration to
//!   the rename revision and one to the removal revision are answered 409 or
//!   422 naming `Class.abstract` and `EnumerationLiteral.name`; `a` hosts no
//!   new model after them, `m2` names no migration and still takes an edit.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si12_copy_migration_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! The binary is `MOIRAI_E2E_INTERP_NODE_BIN` or `target/debug/examples/model_node`;
//! a missing one prints `E2E-SKIP si12-l3: <why>` and returns, as the other
//! L3 files of this crate do. A skip is not a pass.

mod si12;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use moirai_protocol::log_id::LogId;
use serde_json::{Value, json};

use si12::*;

#[path = "nw02/launch.rs"]
mod launch;

const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
const LOG_ID: &str = "5112c0de5112c0de5112c0de5112c0de";

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

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;

impl Cluster {
    /// `si12-a`, `si12-b`, `si12-c`, each with the descriptors it serves.
    fn start(binary: &Path) -> Result<Self, String> {
        let scratch = std::env::temp_dir().join(format!("moirai-si12-{}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        let mut specs = Vec::new();
        for (id, served) in [
            (
                "si12-a",
                vec![
                    ("d1", D1),
                    ("d2", D2),
                    ("d2-rename", D2_RENAME),
                    ("d2-remove", D2_REMOVE),
                ],
            ),
            ("si12-b", vec![("d1", D1), ("d2", D2)]),
            ("si12-c", vec![("d1", D1)]),
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
        let cluster = Cluster {
            nodes,
            scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(10))
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
                    launch::tail(&node.log, 15)
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
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
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!(
                "{path} on {} answered {status}: {body}",
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

    /// Create a model on `node` under `digest`, or join `model` by id
    /// naming it; answers the model id.
    fn register(&self, node: usize, model: Option<&str>, digest: &str) -> Result<String, String> {
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

    /// Join `model` on `node` by its id alone, as any model is joined; a node
    /// that already hosts it (409) is as good.
    fn join_by_id(&self, node: usize, model: &str) -> Result<(), String> {
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

    /// Post one operation; answers the node's `OpResult`.
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
    fn submit(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        for op in ops {
            let answer = self.post_op(node, model, op)?;
            if answer["success"] != Value::Bool(true) {
                return Err(format!(
                    "{} refused {} on {model}: {answer}",
                    self.nodes[node].id,
                    serde_json::to_string(op).unwrap_or_default()
                ));
            }
        }
        Ok(())
    }

    fn identified(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/state/identified");
        match self.get(node, &path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    fn plain(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/state");
        match self.get(node, &path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// M2 over HTTP, the two keys the contract fixes.
    fn migration(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/migration");
        match self.get(node, &path)? {
            (200, body) => Ok(status_keys(&body)),
            (status, body) => Err(format!(
                "SI.12 (M2): {} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    /// The models `node` hosts, by id, with the metamodel each is listed
    /// under.
    fn hosted(&self, node: usize) -> Result<Vec<(String, Value)>, String> {
        match self.get(node, "/api/models")? {
            (200, body) => Ok(body["models"]
                .as_array()
                .ok_or_else(|| format!("no `models` in {body}"))?
                .iter()
                .map(|entry| {
                    (
                        entry["model_id"].as_str().unwrap_or_default().to_string(),
                        entry["metamodel_id"].clone(),
                    )
                })
                .collect()),
            (status, body) => Err(format!("/api/models answered {status}: {body}")),
        }
    }

    /// Poll until `check` holds on every node, or answer the last reason it
    /// did not.
    fn until(&self, what: &str, check: impl Fn(usize) -> Result<(), String>) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let outcome: Result<(), String> = (0..self.nodes.len()).try_for_each(&check);
            match outcome {
                Ok(()) => return Ok(()),
                Err(why) if Instant::now() > deadline => {
                    return Err(format!("{what}: {why}\n{}", self.tails()));
                }
                Err(_) => std::thread::sleep(POLL),
            }
        }
    }
}

/// The digest named by a listing entry, given as a string or as
/// `{nsURI, digest}`.
fn listed_digest(value: &Value) -> Option<String> {
    match value {
        Value::String(digest) => Some(digest.clone()),
        Value::Object(map) => map
            .get("digest")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

/// An edit that must be refused naming `copy`, on the single route and on
/// the batch route.
fn refused_edit(
    cluster: &Cluster,
    node: usize,
    model: &str,
    op: &ModelOp,
    copy: &str,
) -> Result<(), String> {
    let answer = cluster.post_op(node, model, op)?;
    let message = answer["message"].as_str().unwrap_or_default();
    if answer["success"] != Value::Bool(false) || !message.contains(copy) {
        return Err(format!(
            "SI.12 (M3): {} answered an edit of the migrated model {model} with {answer}; it must \
             refuse it naming the copy {copy}",
            cluster.nodes[node].id
        ));
    }
    let batch = json!({ "ops": [serde_json::to_value(op).map_err(|err| err.to_string())?] });
    let (status, answer) =
        cluster.post(node, &format!("/api/model/{model}/ops"), batch.to_string())?;
    if status != 422 {
        return Err(format!(
            "SI.12 (M3): {} answered a batch on the migrated model {model} with {status}: {answer}; \
             it must refuse it (422)",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

fn scenario(cluster: &Cluster) -> Result<(), String> {
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);

    // L1: the running example on m under D1, on all three.
    let m = cluster.register(A, None, &digest(D1))?;
    cluster.register(B, Some(&m), &digest(D1))?;
    cluster.register(C, Some(&m), &digest(D1))?;
    cluster.until("L1: the table of m on every node", |node| {
        match cluster.get(node, &format!("/api/model/{m}/metrics"))? {
            (200, metrics) if metrics["delivered_ops"].as_u64().unwrap_or(0) >= 1 => Ok(()),
            other => Err(format!(
                "node {node} has not delivered m's Install: {other:?}"
            )),
        }
    })?;
    cluster.submit(A, &m, &d1.structure())?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    cluster.submit(A, &m, &d1.links(&ids))?;
    cluster.until("L1: the running example on every node", |node| {
        let doc = cluster.identified(node, &m)?;
        if doc.is_null() || project(&doc) != expected_d1() {
            return Err(format!("node {node} serves {doc}"));
        }
        Ok(())
    })?;
    for node in [A, B, C] {
        if cluster.migration(node, &m)? != not_migrated() {
            return Err(format!(
                "L1 (M2): node {node} names a migration of m before any"
            ));
        }
    }
    let before: BTreeSet<String> = cluster.hosted(A)?.into_iter().map(|(id, _)| id).collect();
    println!("SI12-L3 L1: the running example on m under D1 on three nodes");

    // L2: the partition.
    cluster.pause_all()?;
    cluster.submit(B, &m, &d1.b_edits())?;
    cluster.submit(C, &m, &[d1.c_edit()])?;
    if door_name(&cluster.identified(B, &m)?) != json!("Doors") {
        return Err("set-up: b does not read its own rename before the merge".into());
    }
    let cut_doc = cluster.identified(A, &m)?;
    let cut_plain = cluster.plain(A, &m)?;
    let (status, answer) = cluster.post(
        A,
        &format!("/api/model/{m}/migrate"),
        json!({ "metamodel_id": digest(D2) }).to_string(),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "SI.12 (M1): a's migrate action on m answered {status}: {answer}"
        ));
    }
    let copy = answer["model_id"]
        .as_str()
        .ok_or_else(|| format!("SI.12 (M1): the migrate action named no model: {answer}"))?
        .to_string();
    LogId::parse(&copy)
        .map_err(|why| format!("SI.12 (M1): the copy's id {copy} is no model id: {why}"))?;
    if copy == m {
        return Err("SI.12 (M1): the copy has the source's id".into());
    }
    let listed = cluster.hosted(A)?;
    match listed.iter().find(|(id, _)| id == &copy) {
        Some((_, metamodel)) if listed_digest(metamodel) == Some(digest(D2)) => {}
        other => {
            return Err(format!(
                "SI.12 (M1): a does not list the copy {copy} under D2's digest: {other:?} in {listed:?}"
            ));
        }
    }
    if cluster.migration(A, &m)? != migrated(&copy, &[]) {
        return Err(format!(
            "SI.12 (M2): right after its migration a reads {} for m",
            cluster.migration(A, &m)?
        ));
    }
    refused_edit(cluster, A, &m, &d1.name_char(CAR, 3, 's'), &copy)?;
    if cluster.identified(A, &m)? != cut_doc || cluster.plain(A, &m)? != cut_plain {
        return Err("SI.12 (M4): a's read of m moved after its own migration".into());
    }
    if cluster.migration(B, &m)? != not_migrated() {
        return Err("SI.12 (M2): b, cut off, names a migration it has not received".into());
    }
    println!("SI12-L3 L2: a migrated m to {copy} across the partition, b and c each edited m");

    // L3: the merge.
    cluster.resume_all()?;
    let refused = [("si12-b", 1), ("si12-b", 2), ("si12-c", 1)];
    cluster.until(
        "L3 (M2, M5): every node names the copy and lists the three concurrent edits",
        |node| {
            let status = cluster.migration(node, &m)?;
            if status != migrated(&copy, &refused) {
                return Err(format!("node {node} reads {status}"));
            }
            Ok(())
        },
    )?;
    for node in [A, B, C] {
        let doc = cluster.identified(node, &m)?;
        if doc != cut_doc {
            return Err(format!(
                "SI.12 (M4): after the merge node {node} serves m as {}, not as a served it when it \
                 migrated ({})",
                project(&doc),
                project(&cut_doc)
            ));
        }
        if cluster.plain(node, &m)? != cut_plain {
            return Err(format!(
                "SI.12 (M4): node {node}'s default read of m moved after the cut"
            ));
        }
        refused_edit(
            cluster,
            node,
            &m,
            &d1.leaf(SEDAN, "abstract", moirai_interp::leaf::LeafOp::Enable),
            &copy,
        )?;
    }
    let (status, answer) = cluster.post(
        B,
        &format!("/api/model/{m}/migrate"),
        json!({ "metamodel_id": digest(D2) }).to_string(),
    )?;
    if (status != 409 && status != 422) || !answer.to_string().contains(&copy) {
        return Err(format!(
            "SI.12 (M3): b's second migration of m answered {status}: {answer}; it must be refused \
             (409 or 422) naming the copy {copy}"
        ));
    }
    println!(
        "SI12-L3 L3: all three nodes frozen at the cut, naming {copy}, the three concurrent edits reported"
    );

    // L4: the copy on every node, joined as any model is.
    cluster.join_by_id(B, &copy)?;
    cluster.join_by_id(C, &copy)?;
    cluster.until("L4 (M6): the copy on every node", |node| {
        let doc = cluster.identified(node, &copy)?;
        if doc.is_null() || project(&doc) != expected_d2() {
            return Err(format!("node {node} serves the copy as {doc}"));
        }
        Ok(())
    })?;
    let first = cluster.identified(A, &copy)?;
    for node in [A, B, C] {
        let doc = cluster.identified(node, &copy)?;
        if doc != first {
            return Err(format!(
                "SI.12 (M6): node {node} serves another copy than a: {doc}"
            ));
        }
        if project_with(&cluster.plain(node, &copy)?, &labels(&doc)) != strip_ids(&expected_d2()) {
            return Err(format!(
                "SI.12 (M6): node {node}'s default read of the copy"
            ));
        }
        let (status, served) = cluster.get(node, &format!("/api/model/{copy}/metamodel"))?;
        let served_digest = match &served {
            Value::Object(_) => moirai_semantics::metamodel_digest(&served),
            _ => String::new(),
        };
        if status != 200 || served_digest != digest(D2) {
            return Err(format!(
                "SI.12 (M6): node {node} does not serve D2 for the copy ({status}, digest {served_digest})"
            ));
        }
    }
    cluster.until("L4 (M6): every node lists the copy under D2", |node| {
        let listed = cluster.hosted(node)?;
        match listed.iter().find(|(id, _)| id == &copy) {
            Some((_, metamodel)) if listed_digest(metamodel) == Some(digest(D2)) => Ok(()),
            other => Err(format!("node {node} lists {other:?}")),
        }
    })?;
    println!(
        "SI12-L3 L4: the copy is the running example under D2 on three nodes, c holding D2 only through it"
    );

    // L5: the copy merges by D2.
    cluster.pause_all()?;
    cluster.submit(A, &copy, &[d2.rename_door("Gate")])?;
    cluster.submit(B, &copy, &[d2.rename_door("Hatch")])?;
    cluster.resume_all()?;
    cluster.until("L5 (M6): one name for Door on every node", |_node| {
        let names: Vec<Value> = (0..3)
            .map(|other| cluster.identified(other, &copy).map(|doc| door_name(&doc)))
            .collect::<Result<_, _>>()?;
        if names.iter().all(|name| name == &names[0])
            && (names[0] == json!("Gate") || names[0] == json!("Hatch"))
        {
            Ok(())
        } else {
            Err(format!("Door reads {names:?}"))
        }
    })?;
    println!("SI12-L3 L5: two concurrent renames in the copy read as one name on three nodes");

    // L6: refused revisions, on a second model.
    let m2 = cluster.register(A, None, &digest(D1))?;
    let mut small = vec![d1.root()];
    small.extend(d1.name(At::Root, "Vehicle"));
    small.push(d1.insert(At::Root, "ownedElements", 0, "Class"));
    small.extend(d1.name(CAR, "Car"));
    small.push(d1.leaf(CAR, "abstract", moirai_interp::leaf::LeafOp::Enable));
    cluster.submit(A, &m2, &small)?;
    let hosted_before: BTreeSet<String> =
        cluster.hosted(A)?.into_iter().map(|(id, _)| id).collect();
    let mut expected_hosted = before.clone();
    expected_hosted.insert(copy.clone());
    expected_hosted.insert(m2.clone());
    if hosted_before != expected_hosted {
        return Err(format!(
            "set-up: a hosts {hosted_before:?}, not {expected_hosted:?}"
        ));
    }
    for (revision, feature) in [
        (D2_RENAME, "Class.abstract"),
        (D2_REMOVE, "EnumerationLiteral.name"),
    ] {
        let (status, answer) = cluster.post(
            A,
            &format!("/api/model/{m2}/migrate"),
            json!({ "metamodel_id": digest(revision) }).to_string(),
        )?;
        let reason = answer.to_string();
        if (status != 409 && status != 422) || !reason.contains(feature) {
            return Err(format!(
                "SI.12 (M7): migrating m2 to the revision without `{feature}` answered {status}: \
                 {answer}; it must be refused (409 or 422) naming `{feature}`"
            ));
        }
    }
    let hosted_after: BTreeSet<String> = cluster.hosted(A)?.into_iter().map(|(id, _)| id).collect();
    if hosted_after != expected_hosted {
        return Err(format!(
            "SI.12 (M7): a refused migration left a model behind: a hosts {hosted_after:?}"
        ));
    }
    if cluster.migration(A, &m2)? != not_migrated() {
        return Err("SI.12 (M7): m2 names a migration after two refused ones".into());
    }
    cluster.submit(A, &m2, &[d1.name_char(CAR, 3, 's')])?;
    println!("SI12-L3 L6: the rename and the removal revisions refused by name, m2 still writable");
    Ok(())
}

/// SI.12 at L3, three nodes.
#[test]
fn si12_l3_copy_migration_across_a_partition_on_three_nodes() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si12-l3: {why}");
            return;
        }
    };
    let cluster = match Cluster::start(&binary) {
        Ok(cluster) => cluster,
        Err(why) => panic!("si12-l3: set-up: {why}"),
    };
    if let Err(why) = scenario(&cluster) {
        panic!("si12-l3: {why}\n{}", cluster.tails());
    }
}
