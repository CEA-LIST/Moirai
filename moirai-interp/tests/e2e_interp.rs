//! `ip18` and `ip19`: the two level-3 scenarios of `02 Validation Plan`, which
//! are the two halves of "no rebuild, no restart".
//!
//! - **ip18** (criterion I-A5, *one binary, any metamodel it holds*): three
//!   replicas started from one `model_node` build host a behaviour-tree model
//!   and a SimpleUML model at the same time; both converge on all three, and
//!   neither document holds the other's content.
//! - **ip19** (criterion I-A6, *a metamodel arrives at run time*): three
//!   replicas start holding only `bt`; the SimpleUML descriptor is posted to
//!   each while they run; a UML model is created on one, joined on another,
//!   edited there, and converges — with no process restarted — while the
//!   behaviour-tree model open throughout keeps converging and never contains
//!   the UML content.
//!
//! # Why these live here and not in `e2e_convergence.rs`
//!
//! Two reasons, and the first is the harder one.
//!
//! `moirai-network/tests/e2e_convergence.rs` is one test binary whose backend
//! reads **one** `MOIRAI_E2E_NODE_BIN` and runs it for every scenario. The
//! phase 4 suite is run with that pointed at the generated `network_node`, and
//! `ip20` asserts that suite is unchanged at 39 passed, 0 failed, 5 ignored —
//! so adding two scenarios to that file would break the count it is there to
//! protect, and running them from it would need a second binary the backend
//! has no place for. Second, an operation on an interpreted model is a
//! `ModelOp` built from the slots of a parsed table: writing one needs
//! `moirai-interp` and `moirai-semantics`, which is this crate and its
//! dependency, and would be a new dependency edge out of `moirai-network`.
//!
//! The skip discipline is copied exactly, because that is the part CI reads:
//! no binary, print `E2E-SKIP <scenario>: <why>` and return green, and CI
//! fails the job on the marker.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test e2e_interp \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! `MOIRAI_E2E_INTERP_NODE_BIN` overrides where the binary is looked for; unset,
//! it is `target/debug/examples/model_node` of this workspace.
//!
//! # Rules, from the suite these follow
//!
//! Poll to a deadline, never sleep then assert. `/api/model/{id}/state` is the
//! only oracle. States are compared as parsed JSON. Readiness is mutual peer
//! visibility, never process start.

use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::leaf::LeafOp;
use moirai_interp::testing::{class_slot, feature_slot};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

/// The two descriptors, `formatVersion` 2, byte-identical to
/// `arachne/examples/`. Checked in beside this crate for the same reason
/// `bt.metamodel.json` already was: a test that reaches into a sibling
/// checkout is a test that fails on a machine holding one clone.
const BT: &str = include_str!("fixtures/bt.metamodel.json");
const UML: &str = include_str!("fixtures/uml.metamodel.json");

/// How long a mesh may take to form. Generous: the node sleeps 2 s before its
/// single dial attempt, and a debug build starts slowly under load.
const MESH_TIMEOUT: Duration = Duration::from_secs(60);
/// How long replicas may take to agree once every operation was accepted.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a node may take to answer `/api/health`.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);

static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

/// Where the interpreted node binary is, or the reason to skip.
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
            "no `model_node` at {}; build it with `cargo build -p moirai-interp \
             --features network --example model_node`, or point \
             MOIRAI_E2E_INTERP_NODE_BIN at one",
            built.display()
        ))
    }
}

/// The machine-readable marker CI greps for. A scenario that cannot run says
/// so and returns green; a suite that skipped silently is worse than a red one.
fn skip(scenario: &str, why: &str) {
    eprintln!("\nE2E-SKIP {scenario}: {why}");
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("an ephemeral port")
        .local_addr()
        .expect("a bound address")
        .port()
}

/// One replica process and how to reach it.
struct Replica {
    id: String,
    http: u16,
    child: Child,
    log: PathBuf,
}

impl Replica {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    fn tail(&self) -> String {
        fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .rev()
            .take(25)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Three replicas of one binary, meshed by a static `PEERS` list, each serving
/// the descriptors in `metamodel_dir`.
struct Cluster {
    replicas: Vec<Replica>,
    /// Kept so the descriptors outlive the processes reading them.
    _scratch: PathBuf,
    http: reqwest::blocking::Client,
}

impl Cluster {
    /// Start three replicas serving exactly `descriptors`, given as
    /// `(file stem, text)`.
    fn start(binary: &PathBuf, descriptors: &[(&str, &str)]) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-interp-e2e-{}-{run}", std::process::id()));
        let metamodels = scratch.join("metamodels");
        fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
        for (name, text) in descriptors {
            fs::write(metamodels.join(format!("{name}.json")), text)
                .map_err(|err| err.to_string())?;
        }

        let ids = ["a", "b", "c"];
        let ports: Vec<(u16, u16)> = ids.iter().map(|_| (free_port(), free_port())).collect();
        let mut replicas = Vec::new();
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
                // One default log for the session: replicas minting their own
                // would host different logs and refuse each other's events.
                .env("LOG_ID", "de7a17de7a17de7a17de7a17de7a17de")
                .env("METAMODEL_DIR", &metamodels)
                // Unset on purpose: no bootnode, no dashboard, no discovery.
                .env_remove("BOOTNODE_URL")
                .env_remove("DASHBOARD_URL")
                .env_remove("METAMODEL_PATH")
                .stdout(Stdio::from(out))
                .stderr(Stdio::from(err_out))
                .spawn()
                .map_err(|err| format!("spawning {id}: {err}"))?;
            replicas.push(Replica {
                id: id.to_string(),
                http: ports[index].1,
                child,
                log,
            });
        }

        let cluster = Cluster {
            replicas,
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

    fn get(&self, replica: usize, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .http
            .get(self.replicas[replica].url(path))
            .send()
            .map_err(|err| format!("GET {path} on {}: {err}", self.replicas[replica].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        let value = serde_json::from_str(&body).unwrap_or(Value::String(body));
        Ok((status, value))
    }

    fn post(&self, replica: usize, path: &str, body: String) -> Result<(u16, Value), String> {
        let response = self
            .http
            .post(self.replicas[replica].url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| format!("POST {path} on {}: {err}", self.replicas[replica].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        let value = serde_json::from_str(&body).unwrap_or(Value::String(body));
        Ok((status, value))
    }

    fn await_health(&self) -> Result<(), String> {
        for (index, replica) in self.replicas.iter().enumerate() {
            let deadline = Instant::now() + HEALTH_TIMEOUT;
            loop {
                if matches!(self.get(index, "/api/health"), Ok((200, _))) {
                    break;
                }
                if Instant::now() > deadline {
                    return Err(format!(
                        "{} never answered /api/health; last log lines:\n{}",
                        replica.id,
                        replica.tail()
                    ));
                }
                std::thread::sleep(POLL);
            }
        }
        Ok(())
    }

    /// Readiness is mutual peer visibility: each replica must see the other
    /// two connected, which is what makes an operation submitted next
    /// reachable rather than merely sent.
    fn await_mesh(&self) -> Result<(), String> {
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            let ready = (0..self.replicas.len()).all(|index| {
                matches!(self.get(index, "/api/peers"), Ok((200, peers)) if connected(&peers) >= 2)
            });
            if ready {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "the mesh never formed; last log lines from a:\n{}",
                    self.replicas[0].tail()
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Register a model: no `model_id` creates one, a `model_id` joins it.
    fn register(&self, replica: usize, model: Option<&str>, key: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": key }),
            None => json!({ "metamodel_id": key }),
        };
        let (status, answer) = self.post(replica, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "registering on {} answered {status}: {answer}",
                self.replicas[replica].id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    /// Submit one operation and require the node to have accepted it. A
    /// refusal here is `ModelLog::is_enabled` speaking, which is the whole
    /// structural check on this path.
    fn submit(&self, replica: usize, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(replica, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != json!(true) {
            return Err(format!(
                "{} refused an operation ({status}): {answer}",
                self.replicas[replica].id
            ));
        }
        Ok(())
    }

    fn state(&self, replica: usize, model: &str) -> Result<Value, String> {
        let (status, body) = self.get(replica, &format!("/api/model/{model}/state"))?;
        if status != 200 {
            return Err(format!("state answered {status}: {body}"));
        }
        Ok(body)
    }

    /// Poll until every replica reads `model` the same way, and answer with
    /// the agreed document.
    fn converged(&self, model: &str) -> Result<Value, String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let states: Vec<Result<Value, String>> = (0..self.replicas.len())
                .map(|index| self.state(index, model))
                .collect();
            if let Ok(agreed) = agreement(&states) {
                return Ok(agreed);
            }
            let last: BTreeMap<String, String> = self
                .replicas
                .iter()
                .zip(states.iter())
                .map(|(replica, state)| {
                    (
                        replica.id.clone(),
                        match state {
                            Ok(value) => value.to_string(),
                            Err(why) => why.clone(),
                        },
                    )
                })
                .collect();
            if Instant::now() > deadline {
                return Err(format!(
                    "model {model} never converged; states were {last:?}"
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Poll until a replica has the model's merge table: what a joiner waits
    /// on before writing, since it holds none until the creator's `Install`
    /// reaches it, by state transfer or by delta.
    ///
    /// Read through the log's counters and not through `/api/state`, because
    /// a model that has been opened and not yet written reads `null` — the
    /// canonical form of a root nobody has instantiated. The `Install` is the
    /// log's first operation, so one delivered operation is exactly it.
    fn until_table(&self, replica: usize, model: &str) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Ok((200, metrics)) = self.get(replica, &format!("/api/model/{model}/metrics"))
                && metrics["delivered_ops"].as_u64().unwrap_or(0) >= 1
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never received the table of model {model}; its counters read {:?}",
                    self.replicas[replica].id,
                    self.get(replica, &format!("/api/model/{model}/metrics"))
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Every replica is still the process it was: no restart happened, which
    /// is the load-bearing half of `ip19`.
    fn still_running(&mut self, pids: &[u32]) -> Result<(), String> {
        for (replica, pid) in self.replicas.iter_mut().zip(pids) {
            match replica.child.try_wait() {
                Ok(None) if replica.child.id() == *pid => {}
                Ok(None) => {
                    return Err(format!("{} is a different process now", replica.id));
                }
                Ok(Some(status)) => {
                    return Err(format!(
                        "{} exited with {status}; last log lines:\n{}",
                        replica.id,
                        replica.tail()
                    ));
                }
                Err(err) => return Err(format!("{}: {err}", replica.id)),
            }
        }
        Ok(())
    }

    fn pids(&self) -> Vec<u32> {
        self.replicas.iter().map(|r| r.child.id()).collect()
    }
}

/// How many peers a `/api/peers` answer reports as connected.
fn connected(peers: &Value) -> usize {
    peers["peers"]
        .as_array()
        .map(|list| {
            list.iter()
                .filter(|peer| peer["status"] == json!("Connected"))
                .count()
        })
        .unwrap_or(0)
}

/// The one value every replica agreed on, or the reason they did not.
fn agreement(states: &[Result<Value, String>]) -> Result<Value, ()> {
    let mut agreed: Option<&Value> = None;
    for state in states {
        let Ok(value) = state else { return Err(()) };
        match agreed {
            None => agreed = Some(value),
            Some(first) if first == value => {}
            Some(_) => return Err(()),
        }
    }
    agreed.cloned().ok_or(())
}

// ---------------------------------------------------------------------------
// Operations, built from the table the descriptor parses to
// ---------------------------------------------------------------------------

fn table(text: &str) -> MetamodelSemantics {
    let parsed: Value = serde_json::from_str(text).expect("the fixture is JSON");
    from_descriptor(&parsed).expect("the fixture is a descriptor this crate reads")
}

fn digest(text: &str) -> String {
    let parsed: Value = serde_json::from_str(text).expect("the fixture is JSON");
    metamodel_digest(&parsed)
}

/// The operations that put one object of `child` at position `pos` of the
/// root's `feature` sequence and write `text` into its single-valued text
/// attribute `attribute`, one character at a time.
///
/// The first is an insert and the rest are updates of what it created, which
/// is what a text CRDT under a freshly minted object looks like from outside.
fn named_child(
    sem: &MetamodelSemantics,
    root: &str,
    feature: &str,
    pos: usize,
    child: &str,
    attribute: &str,
    text: &str,
) -> Vec<ModelOp> {
    let root_slot = class_slot(sem, root);
    let feature_slot = feature_slot(sem, root_slot, feature);
    let child_slot = class_slot(sem, child);
    let attribute_slot = self::feature_slot(sem, child_slot, attribute);
    text.chars()
        .enumerate()
        .map(|(index, ch)| {
            let write = InstanceOp::variant(
                child_slot,
                InstanceOp::field(
                    attribute_slot,
                    InstanceOp::Leaf(LeafOp::InsertChar { pos: index, ch }),
                ),
            );
            let step = if index == 0 {
                InstanceOp::insert(pos, write)
            } else {
                InstanceOp::at(pos, write)
            };
            InstanceOp::variant(root_slot, InstanceOp::field(feature_slot, step)).into_model_op()
        })
        .collect()
}

/// A behaviour tree named `id` under `Root.behaviortrees[pos]`.
fn behaviour_tree(sem: &MetamodelSemantics, pos: usize, id: &str) -> Vec<ModelOp> {
    named_child(sem, "Root", "behaviortrees", pos, "BehaviorTree", "ID", id)
}

/// A UML class named `name` under `Model.ownedElements[pos]`.
fn uml_class(sem: &MetamodelSemantics, pos: usize, name: &str) -> Vec<ModelOp> {
    named_child(sem, "Model", "ownedElements", pos, "Class", "name", name)
}

/// Whether the canonical document holds `needle` anywhere in it.
fn mentions(state: &Value, needle: &str) -> bool {
    state.to_string().contains(needle)
}

// ---------------------------------------------------------------------------
// ip18
// ---------------------------------------------------------------------------

/// `ip18` — I-A5. One binary, two metamodels, at once.
#[test]
fn ip18_one_binary_hosts_a_behaviour_tree_and_a_uml_model_at_once() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return skip("ip18", &why),
    };
    if let Err(why) = ip18(&binary) {
        panic!("ip18: {why}");
    }
}

fn ip18(binary: &PathBuf) -> Result<(), String> {
    let cluster = Cluster::start(binary, &[("bt", BT), ("uml", UML)])?;
    let (bt, uml) = (table(BT), table(UML));

    // Both descriptors are on offer, from one build of one binary.
    let (status, listed) = cluster.get(0, "/api/metamodels")?;
    assert_eq!(status, 200);
    let keys: Vec<&str> = listed["metamodels"]
        .as_array()
        .map(|list| {
            list.iter()
                .filter_map(|entry| entry["digest"].as_str())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        keys.contains(&digest(BT).as_str()) && keys.contains(&digest(UML).as_str()),
        "one node must offer both metamodels, it offers {keys:?}"
    );

    // Two models, created on a, joined on b and c.
    let tree = cluster.register(0, None, &digest(BT))?;
    let model = cluster.register(0, None, &digest(UML))?;
    assert_ne!(tree, model, "two models must be two logs");
    for replica in [1, 2] {
        cluster.register(replica, Some(&tree), &digest(BT))?;
        cluster.register(replica, Some(&model), &digest(UML))?;
    }

    // Each is edited from a different replica, which is what makes the
    // documents' separation a claim about replication rather than about one
    // process's bookkeeping.
    cluster.until_table(1, &tree)?;
    cluster.until_table(2, &model)?;
    for op in behaviour_tree(&bt, 0, "guard") {
        cluster.submit(1, &tree, &op)?;
    }
    for op in uml_class(&uml, 0, "Door") {
        cluster.submit(2, &model, &op)?;
    }

    let tree_state = cluster.converged(&tree)?;
    let model_state = cluster.converged(&model)?;

    assert!(
        mentions(&tree_state, "guard"),
        "the behaviour tree lost its edit: {tree_state}"
    );
    assert!(
        mentions(&model_state, "Door"),
        "the UML model lost its edit: {model_state}"
    );
    assert!(
        !mentions(&tree_state, "Door"),
        "the behaviour tree holds the UML model's content: {tree_state}"
    );
    assert!(
        !mentions(&model_state, "guard"),
        "the UML model holds the behaviour tree's content: {model_state}"
    );
    // And each was merged by its own metamodel's table, not by a shared one.
    assert_eq!(tree_state["eClass"], json!("Root"));
    assert_eq!(model_state["eClass"], json!("Model"));
    Ok(())
}

// ---------------------------------------------------------------------------
// ip19
// ---------------------------------------------------------------------------

/// `ip19` — I-A6. A metamodel the node never held is posted to it while it
/// runs, and is usable at once.
#[test]
fn ip19_a_descriptor_posted_to_running_nodes_is_usable_without_a_restart() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return skip("ip19", &why),
    };
    if let Err(why) = ip19(&binary) {
        panic!("ip19: {why}");
    }
}

fn ip19(binary: &PathBuf) -> Result<(), String> {
    // Three replicas that hold `bt` and nothing else.
    let mut cluster = Cluster::start(binary, &[("bt", BT)])?;
    let pids = cluster.pids();
    let (bt, uml) = (table(BT), table(UML));

    let (_, listed) = cluster.get(0, "/api/metamodels")?;
    assert_eq!(
        listed["metamodels"].as_array().map(Vec::len),
        Some(1),
        "the nodes must start holding one descriptor: {listed}"
    );

    // A behaviour-tree model, open before the UML descriptor exists here and
    // open throughout what follows.
    let tree = cluster.register(0, None, &digest(BT))?;
    for replica in [1, 2] {
        cluster.register(replica, Some(&tree), &digest(BT))?;
    }
    for op in behaviour_tree(&bt, 0, "guard") {
        cluster.submit(0, &tree, &op)?;
    }
    let before = cluster.converged(&tree)?;
    assert!(mentions(&before, "guard"));

    // Registering under the UML digest is refused, because no node holds it.
    let (status, refusal) = cluster.post(
        0,
        "/api/models",
        json!({ "metamodel_id": digest(UML) }).to_string(),
    )?;
    assert_eq!(status, 422, "a metamodel nobody holds: {refusal}");

    // The descriptor arrives, on each running node. Distribution between
    // nodes is out of band by decision D8, so the scenario posts it three
    // times, which is what an operator would do.
    for replica in 0..3 {
        let (status, answer) = cluster.post(replica, "/api/metamodels", UML.to_string())?;
        assert_eq!(status, 201, "posting the descriptor answered: {answer}");
        assert_eq!(answer["added"], json!(true));
        assert_eq!(answer["metamodel"]["digest"], json!(digest(UML)));
        assert_eq!(
            answer["metamodels"].as_array().map(Vec::len),
            Some(2),
            "the listing must answer with both: {answer}"
        );
    }
    // Posted twice is not an error, and adds nothing.
    let (status, again) = cluster.post(0, "/api/metamodels", UML.to_string())?;
    assert_eq!((status, again["added"].clone()), (200, json!(false)));

    // Created on a, joined on b and c, edited on b — the joiner, which has the
    // table only because the creator's `Install` reached it.
    let model = cluster.register(0, None, &digest(UML))?;
    for replica in [1, 2] {
        cluster.register(replica, Some(&model), &digest(UML))?;
    }
    cluster.until_table(1, &model)?;
    for op in uml_class(&uml, 0, "Door") {
        cluster.submit(1, &model, &op)?;
    }
    let model_state = cluster.converged(&model)?;
    assert!(
        mentions(&model_state, "Door"),
        "the UML model did not converge on the edit: {model_state}"
    );
    assert_eq!(model_state["eClass"], json!("Model"));

    // The behaviour-tree model kept converging, and never learned a word of
    // the metamodel that arrived beside it.
    for op in behaviour_tree(&bt, 1, "patrol") {
        cluster.submit(2, &tree, &op)?;
    }
    let after = cluster.converged(&tree)?;
    assert!(
        mentions(&after, "guard") && mentions(&after, "patrol"),
        "the behaviour tree stopped converging: {after}"
    );
    assert!(
        !mentions(&after, "Door"),
        "the behaviour tree holds the UML model's content: {after}"
    );

    // And nothing restarted: same three processes throughout.
    cluster.still_running(&pids)?;
    Ok(())
}
