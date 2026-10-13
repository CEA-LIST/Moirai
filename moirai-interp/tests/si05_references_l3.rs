//! SI.05 over the wire: two `model_node` processes, a partition, and the
//! identified read of each, for claims R1, R2 and R6 at L3.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.05.md`; the contract (K1 the
//! rule, K2 the wire, K3 the read-out) is restated in `si05/mod.rs`. Every
//! operation here is posted as JSON to `POST /api/model/{id}/op`, the K2
//! operations spelled by hand, and every read is
//! `GET /api/model/{id}/state/identified` beside `GET /api/model/{id}/state`.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si05_references_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! A missing binary prints `E2E-SKIP si05-l3: <why>` and returns, as
//! `e2e_interp.rs` does; a skip is not a pass.
#![cfg(feature = "network")]

mod si05;

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Child;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_semantics::metamodel_digest;
use serde_json::{Value, json};

use si05::*;

#[path = "nw02/launch.rs"]
mod launch;

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

    fn tail(&self) -> String {
        launch::tail(&self.log, 25)
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
    _scratch: PathBuf,
    http: reqwest::blocking::Client,
}

impl Cluster {
    fn start(binary: &PathBuf, ids: &[&str], descriptors: &[(&str, &str)]) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-si05-{}-{run}", std::process::id()));
        let metamodels = scratch.join("metamodels");
        fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
        for (name, text) in descriptors {
            fs::write(metamodels.join(format!("{name}.json")), text)
                .map_err(|err| err.to_string())?;
        }
        let metamodels = metamodels.to_string_lossy().into_owned();
        let specs: Vec<launch::NodeSpec> = ids
            .iter()
            .map(|id| {
                launch::NodeSpec::new(
                    id,
                    &[
                        ("LOG_ID", "5105de7a5105de7a5105de7a5105de7a"),
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
                matches!(self.get(index, "/api/peers"), Ok((200, peers)) if connected(&peers) + 1 >= self.nodes.len())
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

    fn register(&self, node: usize, model: Option<&str>, key: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": key }),
            None => json!({ "metamodel_id": key }),
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

    /// Post one operation, given as the JSON a client sends.
    fn submit(&self, node: usize, model: &str, op: &Value) -> Result<(), String> {
        let (status, answer) =
            self.post(node, &format!("/api/model/{model}/op"), op.to_string())?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!(
                "{} refused {op} ({status}): {answer}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    fn submit_ops(
        &self,
        node: usize,
        model: &str,
        ops: &[moirai_interp::ModelOp],
    ) -> Result<(), String> {
        ops.iter().try_for_each(|op| {
            self.submit(
                node,
                model,
                &serde_json::to_value(op).map_err(|err| err.to_string())?,
            )
        })
    }

    fn state(&self, node: usize, model: &str) -> Result<Value, String> {
        let path = format!("/api/model/{model}/state/identified");
        match self.get(node, &path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    fn default_state(&self, node: usize, model: &str) -> Result<Value, String> {
        match self.get(node, &format!("/api/model/{model}/state"))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("the default read answered {status}: {body}")),
        }
    }

    /// Poll until every node serves one identified document, with `until`
    /// holding of it; answer it.
    fn converged(&self, model: &str, until: impl Fn(&Value) -> bool) -> Result<Value, String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let states: Result<Vec<Value>, String> = (0..self.nodes.len())
                .map(|index| self.state(index, model))
                .collect();
            let states = states?;
            if states.iter().all(|state| state == &states[0])
                && !states[0].is_null()
                && until(&states[0])
            {
                return Ok(states[0].clone());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "model {model} never converged; states were {states:?}"
                ));
            }
            std::thread::sleep(POLL);
        }
    }

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
    metamodel_digest(&serde_json::from_str::<Value>(text).expect("a descriptor is JSON"))
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// R1, R2 and R6 between two `model_node` processes.
///
/// 1. Connected: `a` builds a board with `x`, `y`, `z`, links `x.related` to
///    `z` and `x.blocker` to `z`; `b` renames `z` (its identifying title).
///    Both nodes read `x.related` = `x.blocker` = z's identity, in the
///    identified read and in the default read alike.
/// 2. Partitioned: `a` deletes `y`; `b`, which has not seen it, links
///    `x.related` and retargets `x.blocker` to `y`. Resumed: both nodes read
///    no link to `y` and keep z.
/// 3. Connected: `a` deletes `z`; both read no link at all.
/// 4. R6: a model on today's descriptor beside it keeps string references.
#[test]
fn si05_l3_two_nodes_across_a_partition_read_references_by_identity() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si05-l3: {why}");
            return;
        }
    };
    if let Err(why) = l3(&binary) {
        panic!("si05-l3: {why}");
    }
}

fn l3(binary: &PathBuf) -> Result<(), String> {
    let (a, b) = (0, 1);
    let cluster = Cluster::start(
        binary,
        &["si05-a", "si05-b"],
        &[("board", BOARD), ("today", BOARD_TODAY)],
    )?;
    let board = Board::new();

    let m = cluster.register(a, None, &digest(BOARD))?;
    cluster.register(b, Some(&m), &digest(BOARD))?;
    cluster.until_table(b, &m)?;

    // ---- 1. connected ----
    let mut ops = vec![
        board.root(),
        board.new_card(0),
        board.new_card(1),
        board.new_card(2),
    ];
    ops.extend(board.title_ops(At::Board(0), "x"));
    ops.extend(board.title_ops(At::Board(1), "y"));
    ops.extend(board.title_ops(At::Board(2), "z"));
    cluster.submit_ops(a, &m, &ops)?;
    let doc = cluster.converged(&m, |doc| title_at(doc, At::Board(2)) == "z")?;
    let (y, z) = (id_at(&doc, At::Board(1)), id_at(&doc, At::Board(2)));
    cluster.submit(
        a,
        &m,
        &board.reference_json(At::Board(0), board.related, "Link", &z),
    )?;
    cluster.submit(
        a,
        &m,
        &board.reference_json(At::Board(0), board.blocker, "Link", &z),
    )?;
    cluster.submit_ops(b, &m, &[board.rename(At::Board(2), 'r')])?;
    let doc = cluster.converged(&m, |doc| {
        title_at(doc, At::Board(2)) == "rz" && !refs(doc, At::Board(0), "related").is_empty()
    })?;
    if refs(&doc, At::Board(0), "related") != set(&[&z])
        || refs(&doc, At::Board(0), "blocker") != set(&[&z])
    {
        return Err(format!(
            "R1: after the rename, x's references are not z's identity {z}: {doc}"
        ));
    }
    for node in [a, b] {
        let plain = cluster.default_state(node, &m)?;
        if reference_values(plain["cards"][0].get("related"), "related") != set(&[&z]) {
            return Err(format!(
                "K3: the default read of node {node} does not serve z's identity: {plain}"
            ));
        }
    }
    println!("SI05-L3 phase 1: x.related = x.blocker = {z} on both nodes after b renamed z to rz");

    // ---- 2. partitioned: a deletes y, b links to it ----
    cluster.control(a, "/api/pause-all")?;
    cluster.control(b, "/api/pause-all")?;
    cluster.submit_ops(a, &m, &[board.delete_card(1)])?;
    cluster.submit(
        b,
        &m,
        &board.reference_json(At::Board(0), board.related, "Link", &y),
    )?;
    cluster.submit(
        b,
        &m,
        &board.reference_json(At::Board(0), board.blocker, "Unlink", &z),
    )?;
    cluster.submit(
        b,
        &m,
        &board.reference_json(At::Board(0), board.blocker, "Link", &y),
    )?;
    let seen_b = cluster.state(b, &m)?;
    if refs(&seen_b, At::Board(0), "related") != set(&[&y, &z])
        || refs(&seen_b, At::Board(0), "blocker") != set(&[&y])
    {
        return Err(format!(
            "b does not read its own links to y before the merge: {seen_b}"
        ));
    }
    cluster.control(a, "/api/resume-all")?;
    cluster.control(b, "/api/resume-all")?;
    let doc = cluster.converged(&m, |doc| {
        board_card_ids(doc).len() == 2 && !refs(doc, At::Board(0), "related").is_empty()
    })?;
    if refs(&doc, At::Board(0), "related") != set(&[&z]) {
        return Err(format!(
            "R2: after the merge x.related should read z alone, not y: {doc}"
        ));
    }
    if !refs(&doc, At::Board(0), "blocker").is_empty() {
        return Err(format!(
            "R2: x.blocker was retargeted to y concurrently with y's deletion and must read nothing: {doc}"
        ));
    }
    println!(
        "SI05-L3 phase 2: link to y concurrent with its deletion read on neither node; x.related = {{{z}}}"
    );

    // ---- 3. connected: a deletes z ----
    cluster.submit_ops(a, &m, &[board.delete_card(1)])?;
    let doc = cluster.converged(&m, |doc| board_card_ids(doc).len() == 1)?;
    if !refs(&doc, At::Board(0), "related").is_empty()
        || !refs(&doc, At::Board(0), "blocker").is_empty()
    {
        return Err(format!("R2: after z's deletion x reads no link: {doc}"));
    }
    println!("SI05-L3 phase 3: z deleted, x reads no link on both nodes");

    // ---- 4. R6: today's descriptor beside it ----
    let t = cluster.register(a, None, &digest(BOARD_TODAY))?;
    cluster.register(b, Some(&t), &digest(BOARD_TODAY))?;
    cluster.until_table(b, &t)?;
    let mut ops = vec![board.root(), board.new_card(0), board.new_card(1)];
    ops.extend(board.title_ops(At::Board(1), "y"));
    ops.push(board.write_string(At::Board(0), board.blocker, "y"));
    ops.push(board.rename(At::Board(1), 'r'));
    cluster.submit_ops(a, &t, &ops)?;
    let doc = cluster.converged(&t, |doc| title_at(doc, At::Board(1)) == "ry")?;
    if doc["cards"][0]["blocker"] != json!("y") {
        return Err(format!(
            "R6: a model on today's descriptor keeps the string written: {doc}"
        ));
    }
    if m == t {
        return Err("two registrations answered one model id".into());
    }
    println!("SI05-L3 phase 4: today's descriptor keeps x.blocker = \"y\" after renaming y");
    Ok(())
}
