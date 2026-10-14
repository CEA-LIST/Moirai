//! SI.08 at L3: the target side of SI.07's P1 on two `model_node` processes
//! across a partition.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.08.md`; the contract (K1 the
//! rule, K2 the wire, K3 the read-out) is SI.05's, restated in `si05/mod.rs`.
//! Every operation is posted as JSON, the K2 operations spelled by hand
//! (`si05/mod.rs`, `reference_json`), and every read is
//! `GET /api/model/{id}/state/identified` beside `GET /api/model/{id}/state`.
//!
//! Two models of the board, one per phase, so that no phase reads a model a
//! revival has moved (ruling R1 of the spec, SI.07's OB2):
//!
//! - hidden: across a partition `a` unsets `Board.pinned` (the card titled
//!   `p`), `b` links `x.blocker` and `y.related` to the pinned card and reads
//!   them; after the merge both nodes serve the card with title `""` under
//!   its first identity and read neither link, in both reads;
//! - read: the same on a second model, except that `b` also types `q` into
//!   the pinned card's title; after the merge both nodes serve the card with
//!   title `q` under its first identity and read both links.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si08_target_side_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! A missing binary prints `E2E-SKIP si08-l3: <why>` and returns, as
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

// The cluster below is `si05_references_l3.rs`'s, as `si07_over_http.rs`
// copies it: two processes started by NW.02's launcher, meshed, and spoken to
// over HTTP.

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
            std::env::temp_dir().join(format!("moirai-si08-{}-{run}", std::process::id()));
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
                        ("LOG_ID", "5108de7a5108de7a5108de7a5108de7a"),
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

    /// Post one operation, which must be applied.
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

/// The note of the card at `at`, `""` when it has none.
fn note(doc: &Value, at: At) -> String {
    card(doc, at)
        .and_then(|card| card.get("note"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// One model of the board on both nodes, with `x`, `y`, `z` and a pinned
/// card titled `p` built on `a` and delivered to `b`; answers the model id,
/// and the identities of `y` and of the pinned card.
fn built_model(cluster: &Cluster, board: &Board) -> Result<(String, String), String> {
    let (a, b) = (0, 1);
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
    Ok((m, id_at(&doc, At::Pinned)))
}

/// One partition on model `m`: `a` unsets `Board.pinned`; `b` links
/// `x.blocker -> P` and `y.related -> P`, then, when `b_title` is given,
/// types it into the pinned card's title, and reads its links. Each node ends
/// its side with a marker in a note no claim reads, so that once a node
/// serves the other's marker, causal delivery has given it everything the
/// other sent before it. Answers the merged identified document.
fn partition(
    cluster: &Cluster,
    board: &Board,
    m: &str,
    p: &str,
    b_title: Option<char>,
) -> Result<Value, String> {
    let (a, b) = (0, 1);
    cluster.control(a, "/api/pause-all")?;
    cluster.control(b, "/api/pause-all")?;
    cluster.submit_ops(a, m, &[unset_pinned(board)])?;
    cluster.submit_ops(a, m, &[board.type_char(At::Board(0), board.note, 0, 'a')])?;
    cluster.submit(
        b,
        m,
        &board.reference_json(At::Board(0), board.blocker, "Link", p),
    )?;
    cluster.submit(
        b,
        m,
        &board.reference_json(At::Board(1), board.related, "Link", p),
    )?;
    if let Some(ch) = b_title {
        cluster.submit_ops(b, m, &[board.rename(At::Pinned, ch)])?;
    }
    cluster.submit_ops(b, m, &[board.type_char(At::Board(2), board.note, 0, 'b')])?;
    let seen_b = cluster.state(b, m)?;
    if refs(&seen_b, At::Board(0), "blocker") != set(&[p])
        || refs(&seen_b, At::Board(1), "related") != set(&[p])
    {
        return Err(format!(
            "set-up: b does not read its own links to the pinned card before the merge: {seen_b}"
        ));
    }
    cluster.control(a, "/api/resume-all")?;
    cluster.control(b, "/api/resume-all")?;
    cluster.converged(m, |doc| {
        note(doc, At::Board(0)) == "a" && note(doc, At::Board(2)) == "b"
    })
}

/// Both nodes' default reads hold the pinned card with `title`, and read
/// `x.blocker` and `y.related` as `expected` (K3: the identified read's
/// identity strings).
fn default_reads(
    cluster: &Cluster,
    m: &str,
    title: &str,
    expected: &BTreeSet<String>,
    label: &str,
) -> Result<(), String> {
    for node in [0, 1] {
        let plain = cluster.default_state(node, m)?;
        let pinned = &plain["pinned"];
        let x = &plain["cards"][0];
        let y = &plain["cards"][1];
        if pinned.is_null()
            || pinned.get("title").and_then(Value::as_str).unwrap_or("") != title
            || &reference_values(x.get("blocker"), "blocker") != expected
            || &reference_values(y.get("related"), "related") != expected
        {
            return Err(format!(
                "{label} (K3): node {node}'s default read holds otherwise: {plain}"
            ));
        }
    }
    Ok(())
}

/// SI.08 at L3. Two nodes serve the board; two models, one per phase.
#[test]
fn si08_l3_links_to_the_pinned_card_concurrent_with_an_unset_follow_its_writes_across_a_partition()
{
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si08-l3: {why}");
            return;
        }
    };
    let outcome = (|| -> Result<(), String> {
        let cluster = Cluster::start(&binary, &["si08-a", "si08-b"])?;
        let board = Board::new();

        // Hidden: no write the unset had not seen reaches the pinned card.
        let (m1, p1) = built_model(&cluster, &board)?;
        let doc = partition(&cluster, &board, &m1, &p1, None)?;
        if !has_card(&doc, At::Pinned)
            || !title_at(&doc, At::Pinned).is_empty()
            || id_at(&doc, At::Pinned) != p1
        {
            return Err(format!(
                "premise: the text keeps the pinned card, its title emptied, under its first identity \
                 {p1}: {doc}"
            ));
        }
        if !refs(&doc, At::Board(0), "blocker").is_empty()
            || !refs(&doc, At::Board(1), "related").is_empty()
        {
            return Err(format!(
                "SI.08 (parity): after the merge x.blocker and y.related read the pinned card, which the \
                 text keeps but no write the unset had not seen reaches; the generated graph holds no \
                 vertex for it: {doc}"
            ));
        }
        default_reads(&cluster, &m1, "", &set(&[]), "hidden")?;
        println!(
            "SI08-L3 hidden: pinned card kept by its emptied title, the links b made to it concurrently \
             with the unset read on neither node"
        );

        // Read: b's title is a write the unset had not seen.
        let (m2, p2) = built_model(&cluster, &board)?;
        let doc = partition(&cluster, &board, &m2, &p2, Some('q'))?;
        if title_at(&doc, At::Pinned) != "q" || id_at(&doc, At::Pinned) != p2 {
            return Err(format!(
                "premise: b's title keeps the pinned card, titled `q`, under its first identity {p2}: \
                 {doc}"
            ));
        }
        if refs(&doc, At::Board(0), "blocker") != set(&[&p2])
            || refs(&doc, At::Board(1), "related") != set(&[&p2])
        {
            return Err(format!(
                "SI.08: after the merge x.blocker and y.related do not read the pinned card, which b's \
                 title (a write the unset had not seen) keeps: {doc}"
            ));
        }
        default_reads(&cluster, &m2, "q", &set(&[&p2]), "read")?;
        println!("SI08-L3 read: b's title keeps the pinned card, both links read on both nodes");
        Ok(())
    })();
    if let Err(why) = outcome {
        panic!("si08-l3: {why}");
    }
}
