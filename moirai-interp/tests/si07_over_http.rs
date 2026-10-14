//! SI.07 over the wire: claim I1 at L2 (the refusal a writer gets from
//! `POST /api/model/{id}/op` and `POST /api/model/{id}/ops`) and claim P1 at
//! L3 (two `model_node` processes across a partition).
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.07.md`; the contract (K1 the
//! rule, K2 the wire, K3 the read-out) is SI.05's, restated in `si05/mod.rs`.
//! Every operation is posted as JSON, the K2 operations spelled by hand
//! (`si05/mod.rs`, `reference_json`), and every read is
//! `GET /api/model/{id}/state/identified` beside `GET /api/model/{id}/state`.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si07_over_http \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! A missing binary prints `E2E-SKIP si07-http: <why>` and returns, as
//! `si05_references_l3.rs` does; a skip is not a pass.
#![cfg(feature = "network")]

mod si05;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::{InstanceOp, ModelOp};
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

// The cluster below is `si05_references_l3.rs`'s, which that file keeps
// private to itself: two processes started by NW.02's launcher, meshed, and
// spoken to over HTTP.

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
    fn start(binary: &Path, ids: &[&str]) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-si07-{}-{run}", std::process::id()));
        let metamodels = scratch.join("metamodels");
        fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
        fs::write(metamodels.join("board.json"), BOARD).map_err(|err| err.to_string())?;
        let metamodels = metamodels.to_string_lossy().into_owned();
        let specs: Vec<launch::NodeSpec> = ids
            .iter()
            .map(|id| {
                launch::NodeSpec::new(
                    id,
                    &[
                        ("LOG_ID", "5107de7a5107de7a5107de7a5107de7a"),
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

    /// Post one operation; answers the status and the body.
    fn post_op(&self, node: usize, model: &str, op: &Value) -> Result<(u16, Value), String> {
        self.post(node, &format!("/api/model/{model}/op"), op.to_string())
    }

    /// Post one operation, which must be applied.
    fn submit(&self, node: usize, model: &str, op: &Value) -> Result<(), String> {
        let (status, answer) = self.post_op(node, model, op)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!(
                "{} refused {op} ({status}): {answer}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    fn submit_ops(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
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

/// `Unset` of `Board.pinned`.
fn unset_pinned(board: &Board) -> ModelOp {
    ModelOp::Instance(InstanceOp::variant(
        board.board,
        InstanceOp::field(board.board_pinned, InstanceOp::unset()),
    ))
}

fn run(name: &str, body: impl FnOnce(&PathBuf) -> Result<(), String>) {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si07-http: {why}");
            return;
        }
    };
    if let Err(why) = body(&binary) {
        panic!("{name}: {why}");
    }
}

/// I1 at L2, and its remote half at L3. Two nodes serve the board.
///
/// 1. `a` builds `x`, `y`, `z` and a lane `L`; both nodes converge.
/// 2. On `a`, `POST /op` of a `Link` of `L`'s identity at `x.blocker` is an
///    ordinary refusal (200, `success: false`), and so is the same at
///    `x.related`; `POST /ops` of a one-operation batch of it is a refused
///    batch (422, `outcome: refused`, nothing applied); the model is
///    unchanged on both nodes. A `Link` of `y` at `x.blocker` is applied.
/// 3. Partitioned: `a` creates a second lane `M`; `b`, which does not serve
///    it, links `x.related` to `M`'s identity (applied: `b` does not serve
///    it). Resumed: both nodes deliver everything and serve one document.
#[test]
fn si07_i1_http_a_link_to_a_served_object_of_another_class_is_an_ordinary_refusal() {
    run("si07-i1-http", |binary| {
        let (a, b) = (0, 1);
        let cluster = Cluster::start(binary, &["si07-a", "si07-b"])?;
        let board = Board::new();
        let m = cluster.register(a, None, &digest(BOARD))?;
        cluster.register(b, Some(&m), &digest(BOARD))?;
        cluster.until_table(b, &m)?;

        let mut ops = vec![
            board.root(),
            board.new_card(0),
            board.new_card(1),
            board.new_card(2),
        ];
        ops.extend(board.title_ops(At::Board(0), "x"));
        ops.extend(board.title_ops(At::Board(1), "y"));
        ops.extend(board.title_ops(At::Board(2), "z"));
        ops.push(board.new_lane(0));
        cluster.submit_ops(a, &m, &ops)?;
        let doc = cluster.converged(&m, |doc| {
            title_at(doc, At::Board(2)) == "z" && doc["lanes"].as_array().map(Vec::len) == Some(1)
        })?;
        let y = id_at(&doc, At::Board(1));
        let lane = doc["lanes"][0][ID]
            .as_str()
            .ok_or_else(|| format!("the lane carries no identity: {doc}"))?
            .to_string();

        for (feature, name) in [(board.blocker, "blocker"), (board.related, "related")] {
            let op = board.reference_json(At::Board(0), feature, "Link", &lane);
            let (status, answer) = cluster.post_op(a, &m, &op)?;
            if status != 200 || answer["success"] != json!(false) {
                return Err(format!(
                    "I1: POST /op of a Link of the lane {lane} at x.{name} (target Card) answered \
                     {status} {answer}, not an ordinary refusal (200, success: false)"
                ));
            }
            println!("SI07-I1 /op x.{name} -> lane: {status} {answer}");
        }
        let batch = json!({
            "ops": [board.reference_json(At::Board(0), board.blocker, "Link", &lane)]
        });
        let (status, answer) =
            cluster.post(a, &format!("/api/model/{m}/ops"), batch.to_string())?;
        if status != 422 || answer["outcome"] != json!("refused") || answer["applied"] != json!(0) {
            return Err(format!(
                "I1: POST /ops of a Link of the lane at x.blocker answered {status} {answer}, not a \
                 refused batch (422, outcome refused, nothing applied)"
            ));
        }
        println!("SI07-I1 /ops x.blocker -> lane: {status} {answer}");
        for node in [a, b] {
            let now = cluster.state(node, &m)?;
            if now != doc {
                return Err(format!(
                    "I1: the refused links changed the model on node {node}: {now}"
                ));
            }
        }
        cluster.submit(
            a,
            &m,
            &board.reference_json(At::Board(0), board.blocker, "Link", &y),
        )?;
        let doc = cluster.converged(&m, |doc| !refs(doc, At::Board(0), "blocker").is_empty())?;
        if refs(&doc, At::Board(0), "blocker") != set(&[&y]) {
            return Err(format!(
                "I1: the link of y at x.blocker reads otherwise: {doc}"
            ));
        }

        cluster.control(a, "/api/pause-all")?;
        cluster.control(b, "/api/pause-all")?;
        cluster.submit_ops(a, &m, &[board.new_lane(1)])?;
        let on_a = cluster.state(a, &m)?;
        let second = on_a["lanes"][1][ID]
            .as_str()
            .ok_or_else(|| format!("a does not serve the second lane: {on_a}"))?
            .to_string();
        let on_b = cluster.state(b, &m)?;
        if on_b["lanes"].as_array().map(Vec::len) != Some(1) {
            return Err(format!(
                "set-up: b serves the second lane before the merge: {on_b}"
            ));
        }
        cluster
            .submit(
                b,
                &m,
                &board.reference_json(At::Board(0), board.related, "Link", &second),
            )
            .map_err(|why| {
                format!(
                    "I1 (reading B): b does not serve the second lane and refused its link: {why}"
                )
            })?;
        cluster.control(a, "/api/resume-all")?;
        cluster.control(b, "/api/resume-all")?;
        cluster.converged(&m, |doc| doc["lanes"].as_array().map(Vec::len) == Some(2))?;
        let plain = (cluster.default_state(a, &m)?, cluster.default_state(b, &m)?);
        if plain.0 != plain.1 {
            return Err(format!(
                "the default reads differ after the merge: {plain:?}"
            ));
        }
        println!(
            "SI07-I1 remote: a link b sent of a lane a serves was delivered; both nodes serve one document"
        );
        Ok(())
    });
}

/// P1 at L3. Two nodes serve the board.
///
/// 1. `a` builds `x`, `y`, `z` and a pinned card titled `p`; both converge.
/// 2. Partitioned: `a` unsets `Board.pinned`; `b` links the pinned card's
///    `blocker` to `y` and `related` to `z`, and reads them.
/// 3. Resumed: both nodes serve one document in which the text keeps the
///    pinned card (title `""`) and neither link is read, in the identified
///    read and in the default read.
/// 4. `a` types `r` into the pinned card's title: both nodes read the two
///    links again.
#[test]
fn si07_p1_l3_links_concurrent_with_an_unset_follow_the_pinned_cards_writes_across_a_partition() {
    run("si07-p1-l3", |binary| {
        let (a, b) = (0, 1);
        let cluster = Cluster::start(binary, &["si07-a", "si07-b"])?;
        let board = Board::new();
        let m = cluster.register(a, None, &digest(BOARD))?;
        cluster.register(b, Some(&m), &digest(BOARD))?;
        cluster.until_table(b, &m)?;

        let mut ops = vec![
            board.root(),
            board.new_card(0),
            board.new_card(1),
            board.new_card(2),
        ];
        ops.extend(board.title_ops(At::Board(0), "x"));
        ops.extend(board.title_ops(At::Board(1), "y"));
        ops.extend(board.title_ops(At::Board(2), "z"));
        ops.push(board.new_pinned());
        ops.extend(board.title_ops(At::Pinned, "p"));
        cluster.submit_ops(a, &m, &ops)?;
        let doc = cluster.converged(&m, |doc| title_at(doc, At::Pinned) == "p")?;
        let (y, z) = (id_at(&doc, At::Board(1)), id_at(&doc, At::Board(2)));

        cluster.control(a, "/api/pause-all")?;
        cluster.control(b, "/api/pause-all")?;
        // Each node ends its side of the partition with a marker in a note no
        // claim reads: once a node serves the other's marker, causal delivery
        // has given it everything the other sent before it.
        cluster.submit_ops(a, &m, &[unset_pinned(&board)])?;
        cluster.submit_ops(a, &m, &[board.type_char(At::Board(0), board.note, 0, 'a')])?;
        cluster.submit(
            b,
            &m,
            &board.reference_json(At::Pinned, board.blocker, "Link", &y),
        )?;
        cluster.submit(
            b,
            &m,
            &board.reference_json(At::Pinned, board.related, "Link", &z),
        )?;
        cluster.submit_ops(b, &m, &[board.type_char(At::Board(2), board.note, 0, 'b')])?;
        let seen_b = cluster.state(b, &m)?;
        if refs(&seen_b, At::Pinned, "blocker") != set(&[&y])
            || refs(&seen_b, At::Pinned, "related") != set(&[&z])
        {
            return Err(format!(
                "set-up: b does not read its own links before the merge: {seen_b}"
            ));
        }
        cluster.control(a, "/api/resume-all")?;
        cluster.control(b, "/api/resume-all")?;
        let note = |doc: &Value, at: At| -> String {
            card(doc, at)
                .and_then(|card| card.get("note"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let doc = cluster.converged(&m, |doc| {
            note(doc, At::Board(0)) == "a" && note(doc, At::Board(2)) == "b"
        })?;
        if !has_card(&doc, At::Pinned) || !title_at(&doc, At::Pinned).is_empty() {
            return Err(format!(
                "P1 premise: the text keeps the pinned card, its title emptied: {doc}"
            ));
        }
        if !refs(&doc, At::Pinned, "blocker").is_empty()
            || !refs(&doc, At::Pinned, "related").is_empty()
        {
            return Err(format!(
                "P1: after the merge the pinned card, kept by its emptied title and reached by no write \
                 the unset had not seen, reads the links b made concurrently with the unset: {doc}"
            ));
        }
        for node in [a, b] {
            let plain = cluster.default_state(node, &m)?;
            let pinned = &plain["pinned"];
            if pinned.is_null()
                || !reference_values(pinned.get("blocker"), "blocker").is_empty()
                || !reference_values(pinned.get("related"), "related").is_empty()
            {
                return Err(format!(
                    "P1 (K3): node {node}'s default read of the pinned card: {plain}"
                ));
            }
        }
        println!(
            "SI07-P1-L3 phase 3: pinned card kept by its emptied title, no link read on either node"
        );

        cluster.submit_ops(a, &m, &board.title_ops(At::Pinned, "r"))?;
        let doc = cluster.converged(&m, |doc| title_at(doc, At::Pinned) == "r")?;
        if refs(&doc, At::Pinned, "blocker") != set(&[&y])
            || refs(&doc, At::Pinned, "related") != set(&[&z])
        {
            return Err(format!(
                "P1: after a's write into the pinned card both links are read again on the generated \
                 runtime; here: {doc}"
            ));
        }
        println!(
            "SI07-P1-L3 phase 4: a's write keeps the card, both links read again on both nodes"
        );
        Ok(())
    });
}
