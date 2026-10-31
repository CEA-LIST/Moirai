//! MD.03 L3; adapted read-only NW.02 and SI.05 launch patterns.
#![cfg(feature = "network")]
#![allow(dead_code)]
mod md03_common;
use std::fs;
use std::path::PathBuf;
use std::process::Child;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_semantics::metamodel_digest;
use serde_json::{Value, json};

use md03_common::*;

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
            "MD03 requires `model_node` at {}; build it with `cargo build -p moirai-interp --features \
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
            std::env::temp_dir().join(format!("moirai-md03-{}-{run}", std::process::id()));
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
                        ("LOG_ID", MODEL),
                        ("HTTP_BIND_ADDR", "127.0.0.1"),
                        ("MOIRAI_TRANSPORT", "tcp"),
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
                for identified in [false, true] {
                    let first = self.raw_state(0, model, identified);
                    for node in 1..self.nodes.len() {
                        assert_eq!(
                            self.raw_state(node, model, identified),
                            first,
                            "MD03 read bytes differ across replicas"
                        );
                    }
                }
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

impl Cluster {
    fn raw_state(&self, node: usize, model: &str, identified: bool) -> String {
        let suffix = if identified { "/identified" } else { "" };
        let response = self
            .http
            .get(self.nodes[node].url(&format!("/api/model/{model}/state{suffix}")))
            .send()
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        response.text().unwrap()
    }
    fn count(&self, n: usize, m: &str) -> u64 {
        let (status, v) = self.get(n, &format!("/api/model/{m}/metrics")).unwrap();
        assert_eq!(status, 200);
        v["delivered_ops"].as_u64().expect("delivered count")
    }
    fn until_count(&self, n: usize, m: &str, k: u64) {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if self.count(n, m) >= k {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "MD03 setup: {} did not receive {k} events: {}",
                NAMES[n],
                self.nodes[n].tail()
            );
            std::thread::sleep(POLL);
        }
    }
    fn pause(&self) {
        for n in 0..3 {
            self.control(n, "/api/pause-all").unwrap();
        }
    }
    fn pair(&self, a: usize, b: usize, resume: bool) {
        let action = if resume { "resume" } else { "pause" };
        self.control(a, &format!("/api/{action}/{}", NAMES[b]))
            .unwrap();
        self.control(b, &format!("/api/{action}/{}", NAMES[a]))
            .unwrap();
    }
    fn heal(&self) {
        for n in 0..3 {
            self.control(n, "/api/resume-all").unwrap();
        }
    }
    fn op(&self, n: usize, m: &str, op: moirai_interp::ModelOp) {
        self.submit(n, m, &serde_json::to_value(op).unwrap())
            .unwrap();
    }
    fn ordered(
        &self,
        m: &str,
        observer: usize,
        writers: [usize; 2],
        counts: [u64; 2],
        reverse: bool,
    ) {
        let initial = self.count(observer, m);
        let order = if reverse { [1, 0] } else { [0, 1] };
        let mut k = initial;
        for i in order {
            self.pair(observer, writers[i], true);
            k += counts[i];
            self.until_count(observer, m, k);
            self.pair(observer, writers[i], false);
            println!(
                "MD03 delivery observer={} writer={} count={k}",
                NAMES[observer], NAMES[writers[i]]
            );
        }
        self.heal();
        for n in 0..3 {
            self.until_count(n, m, k);
        }
    }
}
fn fresh(join_carol: bool) -> (Cluster, Board, String) {
    let bin = node_binary().expect("MD03 must not skip a missing binary");
    let d2 = revised().to_string();
    let c = Cluster::start(&bin, &NAMES, &[("board", BOARD), ("revision", &d2)]).unwrap();
    let b = Board::new();
    let m = c.register(0, None, &digest(BOARD)).unwrap();
    c.register(1, Some(&m), &digest(BOARD)).unwrap();
    if join_carol {
        c.register(2, Some(&m), &digest(BOARD)).unwrap();
    }
    c.submit_ops(0, &m, &structure(&b)).unwrap();
    for n in 0..if join_carol { 3 } else { 2 } {
        c.until_count(n, &m, 6);
    }
    if join_carol {
        c.converged(&m, |d| title_at(d, At::Board(1)) == "y")
            .unwrap();
    }
    (c, b, m)
}
fn g1(reverse: bool) {
    let (c, b, m) = fresh(true);
    let local = id_at(&c.state(0, &m).unwrap(), At::Board(1));
    let out = foreign("carol@17.2");
    assert_eq!(
        c.count(0, &m),
        6,
        "FairPolicy fixture uses next Lamport 7, alice rank 2 > bob rank 0"
    );
    c.pause();
    c.op(0, &m, b.link(At::Board(0), b.blocker, &out).unwrap());
    c.op(1, &m, b.link(At::Board(0), b.blocker, &local).unwrap());
    c.op(0, &m, b.link(At::Board(0), b.related, LIBRARY).unwrap());
    c.op(1, &m, b.link(At::Board(0), b.related, &out).unwrap());
    c.ordered(&m, 2, [0, 1], [2, 2], reverse);
    let doc = c.converged(&m, |_| true).unwrap();
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        values(&[&out]),
        "G1: foreign FairPolicy winner hidden"
    );
    for n in 0..3 {
        assert_foreign(&c.state(n, &m).unwrap(), &[&out, LIBRARY], "G1 identified");
        assert_foreign(
            &c.default_state(n, &m).unwrap(),
            &[&out, LIBRARY],
            "G1 default",
        );
    }
}
fn g2(reverse: bool) {
    let (c, b, m) = fresh(true);
    let local = id_at(&c.state(0, &m).unwrap(), At::Board(1));
    let out = foreign(&local);
    c.op(0, &m, b.link(At::Board(0), b.related, &out).unwrap());
    c.op(0, &m, b.link(At::Board(0), b.blocker, &local).unwrap());
    for n in 0..3 {
        c.until_count(n, &m, 8);
    }
    c.pause();
    c.op(0, &m, b.delete_card(1));
    c.op(
        2,
        &m,
        b.link(At::Board(0), b.related, SYSML_LIBRARY).unwrap(),
    );
    c.ordered(&m, 1, [0, 2], [1, 1], reverse);
    let doc = c.converged(&m, |_| true).unwrap();
    assert_foreign(&doc, &[&out, SYSML_LIBRARY], "G2 local target removal");
    assert!(refs(&doc, At::Board(0), "blocker").is_empty());
    c.submit(
        0,
        &m,
        &b.reference_json(At::Board(0), b.related, "Unlink", &out),
    )
    .unwrap();
    for n in 0..3 {
        c.until_count(n, &m, 11);
    }
    let doc = c.converged(&m, |_| true).unwrap();
    assert_foreign(&doc, &[SYSML_LIBRARY], "G2 full-value unlink");
}
fn g3(reverse: bool) {
    let (c, b, m) = fresh(true);
    let out = foreign("carol@17.2");
    c.op(0, &m, b.link(At::Board(0), b.related, &out).unwrap());
    for n in 0..3 {
        c.until_count(n, &m, 7);
    }
    c.pause();
    assert_foreign(&c.state(0, &m).unwrap(), &[&out], "G3 visibility premise");
    c.op(0, &m, b.delete_card(0));
    c.op(2, &m, b.link(At::Board(0), b.related, LIBRARY).unwrap());
    c.ordered(&m, 1, [0, 2], [1, 1], reverse);
    let doc = c.converged(&m, |_| true).unwrap().to_string();
    assert!(
        !doc.contains(&out) && !doc.contains(LIBRARY),
        "G3 deleted source reference survived"
    );
}
fn g4(reverse: bool) {
    let (c, b, m) = fresh(false);
    let out = foreign("carol@17.2");
    c.pause();
    c.op(0, &m, b.link(At::Board(0), b.related, &out).unwrap());
    c.op(1, &m, b.link(At::Board(0), b.related, LIBRARY).unwrap());
    let donor = usize::from(reverse);
    c.pair(2, donor, true);
    c.register(2, Some(&m), &digest(BOARD)).unwrap();
    c.until_count(2, &m, 7);
    let log = fs::read_to_string(&c.nodes[2].log).unwrap();
    assert!(
        log.contains(&format!("adopted state from {} for log {m}", NAMES[donor])),
        "G4 requires a state transfer, not delta replay: {log}"
    );
    let first = if reverse { LIBRARY } else { &out };
    assert_foreign(
        &c.state(2, &m).unwrap(),
        &[first],
        "G4 snapshot first stream",
    );
    c.heal();
    for n in 0..3 {
        c.until_count(n, &m, 8);
    }
    let doc = c.converged(&m, |_| true).unwrap();
    assert_foreign(&doc, &[&out, LIBRARY], "G4 late second stream");
}
fn g5(reverse: bool, copy: bool) {
    let (c, b, m) = fresh(true);
    let out = foreign("carol@17.2");
    c.op(0, &m, b.link(At::Board(0), b.related, &out).unwrap());
    c.op(0, &m, b.link(At::Board(0), b.related, LIBRARY).unwrap());
    for n in 0..3 {
        c.until_count(n, &m, 8);
    }
    c.pause();
    let d2 = revised().to_string();
    let strategy = if copy { "copy" } else { "rebase" };
    let (status, answer) = c
        .post(
            0,
            &format!("/api/model/{m}/migrate"),
            json!({"metamodel_id":digest(&d2),"strategy":strategy}).to_string(),
        )
        .unwrap();
    assert!(
        (200..300).contains(&status),
        "G5 migration setup failed: {status} {answer}"
    );
    let target = answer["model_id"]
        .as_str()
        .expect("migration model id")
        .to_owned();
    c.op(
        1,
        &m,
        b.link(At::Board(0), b.related, SYSML_LIBRARY).unwrap(),
    );
    c.ordered(&m, 2, [0, 1], [1, 1], reverse);
    if copy {
        for n in [1, 2] {
            c.register(n, Some(&target), &digest(&d2)).unwrap();
            c.until_count(n, &target, c.count(0, &target));
        }
    }
    let doc = c.converged(&target, |_| true).unwrap();
    if copy {
        assert_foreign(&doc, &[&out, LIBRARY], "G5 copy");
    } else {
        assert_foreign(&doc, &[&out, LIBRARY, SYSML_LIBRARY], "G5 rebase");
    }
}
#[test]
fn md03_g1_l3_ab() {
    g1(false)
}
#[test]
fn md03_g1_l3_ba() {
    g1(true)
}
#[test]
fn md03_g2_l3_ab() {
    g2(false)
}
#[test]
fn md03_g2_l3_ba() {
    g2(true)
}
#[test]
fn md03_g3_l3_ab() {
    g3(false)
}
#[test]
fn md03_g3_l3_ba() {
    g3(true)
}
#[test]
fn md03_g4_l3_ab() {
    g4(false)
}
#[test]
fn md03_g4_l3_ba() {
    g4(true)
}
#[test]
fn md03_g5_copy_l3_ab() {
    g5(false, true)
}
#[test]
fn md03_g5_copy_l3_ba() {
    g5(true, true)
}
#[test]
fn md03_g5_rebase_l3_ab() {
    g5(false, false)
}
#[test]
fn md03_g5_rebase_l3_ba() {
    g5(true, false)
}

// Positive controls exercise the post-premise paths that G3 and G4 cannot
// reach on the old build. They also keep the local-only compatibility claim
// at the same process boundary as the foreign-reference claim.
#[test]
fn md03_g6_l3_local_source_removal_both_orders() {
    for reverse in [false, true] {
        let (c, b, m) = fresh(true);
        assert_eq!(
            c.raw_state(0, &m, true),
            r#"{"__id":"alice@2.0","cards":[{"__id":"alice@3.1","blocker":null,"eClass":"Card","related":[],"title":"x"},{"__id":"alice@4.1","blocker":null,"eClass":"Card","related":[],"title":"y"}],"eClass":"Board","lanes":[]}"#,
            "G6 frozen HTTP identified bytes"
        );
        assert_eq!(
            c.raw_state(0, &m, false),
            r#"{"cards":[{"blocker":null,"eClass":"Card","related":[],"title":"x"},{"blocker":null,"eClass":"Card","related":[],"title":"y"}],"eClass":"Board","lanes":[]}"#,
            "G6 frozen HTTP default bytes"
        );
        let target = id_at(&c.state(0, &m).unwrap(), At::Board(1));
        c.op(0, &m, b.link(At::Board(0), b.related, &target).unwrap());
        for n in 0..3 {
            c.until_count(n, &m, 7);
        }
        c.pause();
        assert_eq!(
            refs(&c.state(0, &m).unwrap(), At::Board(0), "related"),
            values(&[&target])
        );
        c.op(0, &m, b.delete_card(0));
        c.op(2, &m, b.link(At::Board(0), b.related, &target).unwrap());
        c.ordered(&m, 1, [0, 2], [1, 1], reverse);
        let doc = c.converged(&m, |_| true).unwrap();
        assert_eq!(title_at(&doc, At::Board(0)), "y");
        assert!(
            refs(&doc, At::Board(0), "related").is_empty(),
            "G6 deleted local source's links must not move to y"
        );
    }
}
#[test]
fn md03_g6_l3_local_snapshot_then_late_stream() {
    for reverse in [false, true] {
        let (c, b, m) = fresh(false);
        let target = id_at(&c.state(0, &m).unwrap(), At::Board(1));
        c.pause();
        c.op(0, &m, b.link(At::Board(0), b.related, &target).unwrap());
        c.op(1, &m, b.link(At::Board(0), b.blocker, &target).unwrap());
        let donor = usize::from(reverse);
        c.pair(2, donor, true);
        c.register(2, Some(&m), &digest(BOARD)).unwrap();
        c.until_count(2, &m, 7);
        let log = fs::read_to_string(&c.nodes[2].log).unwrap();
        assert!(log.contains(&format!("adopted state from {} for log {m}", NAMES[donor])));
        let feature = if reverse { "blocker" } else { "related" };
        assert_eq!(
            refs(&c.state(2, &m).unwrap(), At::Board(0), feature),
            values(&[&target])
        );
        c.heal();
        for n in 0..3 {
            c.until_count(n, &m, 8);
        }
        let doc = c.converged(&m, |_| true).unwrap();
        assert_eq!(refs(&doc, At::Board(0), "related"), values(&[&target]));
        assert_eq!(refs(&doc, At::Board(0), "blocker"), values(&[&target]));
    }
}
