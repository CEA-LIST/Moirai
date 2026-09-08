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
//! Beside them, four scenarios about what a replica does with a metamodel it
//! does not hold, added on 2026-09-08 and answering criteria I-A11 to I-A14.
//!
//! - **ip23** (I-A11): a replica holding no descriptor drops the frames of a
//!   model it does not host and counts them in `frames_not_hosted`, hosting
//!   nothing and changing nothing.
//! - **ip24** (I-A12): joining a model whose metamodel this replica does not
//!   hold is refused before anything is hosted, which pins the bootstrap
//!   question the doc comment on `ip24` states.
//! - **ip25** (I-A13): two descriptors sharing an `nsURI` and differing in
//!   content are two metamodels, and a registration under one digest is
//!   refused by the replica holding the other.
//! - **ip26** (I-A14): the edit case end to end — the model under the old
//!   digest keeps converging while the model under the new one is refused by
//!   the replica that never got the edit.
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
        Self::start_each(binary, &[descriptors, descriptors, descriptors])
    }

    /// Start one replica per entry of `per_replica`, each serving exactly the
    /// descriptors its own entry names. The list's length is the cluster's
    /// size, and an empty entry is a replica that holds no descriptor at all,
    /// which is what `ip23` and `ip24` need.
    fn start_each(binary: &PathBuf, per_replica: &[&[(&str, &str)]]) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-interp-e2e-{}-{run}", std::process::id()));

        let ids = &["a", "b", "c"][..per_replica.len()];
        for (id, descriptors) in ids.iter().zip(per_replica) {
            let metamodels = scratch.join(format!("metamodels-{id}"));
            fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
            for (name, text) in *descriptors {
                fs::write(metamodels.join(format!("{name}.json")), text)
                    .map_err(|err| err.to_string())?;
            }
        }

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
                .env("METAMODEL_DIR", scratch.join(format!("metamodels-{id}")))
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
                matches!(self.get(index, "/api/peers"), Ok((200, peers))
                    if connected(&peers) + 1 >= self.replicas.len())
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

    /// Register a model and answer with the raw status and body, refusal
    /// included. `ip24`, `ip25` and `ip26` are about the refusal, so they need
    /// the status rather than an `Err` built from it.
    fn register_raw(
        &self,
        replica: usize,
        model: Option<&str>,
        key: &str,
    ) -> Result<(u16, Value), String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": key }),
            None => json!({ "metamodel_id": key }),
        };
        self.post(replica, "/api/models", body.to_string())
    }

    /// The node-wide counters of `GET /api/metrics`, which is where
    /// `frames_not_hosted` and `hosted_logs` live: they are the node's and not
    /// any log's, so a node that hosts nothing still answers them.
    fn node_metrics(&self, replica: usize) -> Result<Value, String> {
        match self.get(replica, "/api/metrics")? {
            (200, metrics) => Ok(metrics),
            (status, body) => Err(format!("/api/metrics answered {status}: {body}")),
        }
    }

    fn frames_not_hosted(&self, replica: usize) -> Result<u64, String> {
        self.node_metrics(replica)?["frames_not_hosted"]
            .as_u64()
            .ok_or_else(|| format!("no frames_not_hosted on {}", self.replicas[replica].id))
    }

    fn hosted_logs(&self, replica: usize) -> Result<u64, String> {
        self.node_metrics(replica)?["hosted_logs"]
            .as_u64()
            .ok_or_else(|| format!("no hosted_logs on {}", self.replicas[replica].id))
    }

    /// Register a model: no `model_id` creates one, a `model_id` joins it.
    fn register(&self, replica: usize, model: Option<&str>, key: &str) -> Result<String, String> {
        let (status, answer) = self.register_raw(replica, model, key)?;
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
        let all: Vec<usize> = (0..self.replicas.len()).collect();
        self.converged_on(model, &all)
    }

    /// The same, restricted to the replicas that host the model. A replica
    /// that refused to register holds nothing to compare, and demanding it
    /// agree would be asserting the opposite of what `ip23` and `ip26` claim.
    fn converged_on(&self, model: &str, on: &[usize]) -> Result<Value, String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let states: Vec<Result<Value, String>> =
                on.iter().map(|index| self.state(*index, model)).collect();
            if let Ok(agreed) = agreement(&states) {
                return Ok(agreed);
            }
            let last: BTreeMap<String, String> = on
                .iter()
                .map(|index| &self.replicas[*index])
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

// ---------------------------------------------------------------------------
// The edited metamodel, built here and not checked in
// ---------------------------------------------------------------------------

/// `bt.metamodel.json` with exactly one feature changed: `TreeNode.name`
/// becomes required, so its merge shape is `single` where it was `optional`.
///
/// Same `nsURI`, different bytes, therefore a different digest — which is the
/// whole of the claim `ip25` and `ip26` test, that an edited metamodel is a
/// different metamodel. Built from the fixture at run time on purpose: editing
/// the checked-in descriptor would change what `ip18`, `ip19` and the oracle
/// are run against.
fn edited_bt() -> String {
    let mut parsed: Value = serde_json::from_str(BT).expect("the fixture is JSON");
    {
        let attributes = parsed["classes"]["TreeNode"]["attributes"]
            .as_array_mut()
            .expect("TreeNode has attributes");
        let name = attributes
            .iter_mut()
            .find(|attribute| attribute["name"] == json!("name"))
            .expect("TreeNode has a `name` attribute");
        name["required"] = json!(true);
        name["merge"]["shape"] = json!({ "kind": "single" });
        name["provenance"]["presence"] = json!("declared");
    }
    parsed.to_string()
}

// ---------------------------------------------------------------------------
// ip23
// ---------------------------------------------------------------------------

/// `ip23` — I-A11. A replica that holds no descriptor is safe: it drops the
/// frames of a model it does not host, counts them, and does nothing else.
#[test]
fn ip23_a_replica_without_the_descriptor_drops_the_frames_and_counts_them() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return skip("ip23", &why),
    };
    if let Err(why) = ip23(&binary) {
        panic!("ip23: {why}");
    }
}

fn ip23(binary: &PathBuf) -> Result<(), String> {
    // a and b hold the behaviour-tree descriptor; c holds none at all.
    let mut cluster = Cluster::start_each(binary, &[&[("bt", BT)], &[("bt", BT)], &[]])?;
    let pids = cluster.pids();
    let bt = table(BT);

    let (_, listed) = cluster.get(2, "/api/metamodels")?;
    assert_eq!(
        listed["metamodels"].as_array().map(Vec::len),
        Some(0),
        "c must hold no descriptor: {listed}"
    );

    // The baseline is taken after the mesh formed and before the model exists,
    // so everything counted below is a frame of this model and nothing else.
    let dropped_before = cluster.frames_not_hosted(2)?;
    let hosted_before = cluster.hosted_logs(2)?;

    let tree = cluster.register(0, None, &digest(BT))?;
    cluster.register(1, Some(&tree), &digest(BT))?;
    cluster.until_table(1, &tree)?;
    for op in behaviour_tree(&bt, 0, "guard") {
        cluster.submit(0, &tree, &op)?;
    }
    let state = cluster.converged_on(&tree, &[0, 1])?;
    assert!(
        mentions(&state, "guard"),
        "a and b must converge without c: {state}"
    );

    // c saw the traffic and refused all of it.
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    let dropped_after = loop {
        let now = cluster.frames_not_hosted(2)?;
        if now > dropped_before {
            break now;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "c never counted a dropped frame; frames_not_hosted stayed at \
                 {dropped_before}, its metrics read {}",
                cluster.node_metrics(2)?
            ));
        }
        std::thread::sleep(POLL);
    };

    // And nothing else about c moved: it hosts what it hosted, it holds no
    // state for the model, and it is the process it was.
    assert_eq!(
        cluster.hosted_logs(2)?,
        hosted_before,
        "c hosted a log it holds no descriptor for"
    );
    let (status, body) = cluster.get(2, &format!("/api/model/{tree}/state"))?;
    assert_eq!(
        status, 404,
        "c answered for a model it does not host: {body}"
    );
    cluster.still_running(&pids)?;
    eprintln!(
        "ip23: c dropped {} frames of model {tree} ({dropped_before} -> {dropped_after}), \
         hosted_logs {hosted_before} throughout",
        dropped_after - dropped_before
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// ip24
// ---------------------------------------------------------------------------

/// `ip24` — I-A12. Joining a model whose metamodel this replica does not hold
/// is refused before anything is hosted.
///
/// # The open question this test pins
///
/// `GenericNode::register` (`moirai-network/src/generic.rs:1053-1061`) resolves
/// the descriptor and returns `RegisterRefused::UnknownMetamodel` *before* it
/// calls `host_log`, for a join exactly as for a create. So metamodel
/// distribution is entirely out of band today: `ip19` works because the test
/// posts the descriptor to all three nodes itself.
///
/// It need not be. Decision D2 put the descriptor text inside `ModelOp::Install`,
/// which is the log's own first operation, so the table already travels with
/// the model through state transfer and through delta sync. A joining node
/// could in principle host the empty log, receive the `Install`, and install
/// the table from it, which would make metamodel distribution in band and
/// delete the out-of-band step from I-A6's story.
///
/// That change is *not* made here. This test states what the node does today,
/// and it is the test that changes if we take it.
#[test]
fn ip24_joining_a_model_whose_metamodel_is_unknown_here_is_refused() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return skip("ip24", &why),
    };
    if let Err(why) = ip24(&binary) {
        panic!("ip24: {why}");
    }
}

fn ip24(binary: &PathBuf) -> Result<(), String> {
    // a holds the descriptor; b holds none.
    let cluster = Cluster::start_each(binary, &[&[("bt", BT)], &[]])?;
    let bt = table(BT);

    let tree = cluster.register(0, None, &digest(BT))?;
    for op in behaviour_tree(&bt, 0, "guard") {
        cluster.submit(0, &tree, &op)?;
    }

    let hosted_before = cluster.hosted_logs(1)?;
    let (status, refusal) = cluster.register_raw(1, Some(&tree), &digest(BT))?;
    assert_eq!(
        status, 422,
        "joining under a metamodel b does not hold answered: {refusal}"
    );
    let reason = refusal.to_string();
    assert!(
        reason.contains(&digest(BT)),
        "the refusal must name the metamodel it could not find: {refusal}"
    );

    // Refused before hosting: nothing is hosted, and nothing answers for it.
    assert_eq!(
        cluster.hosted_logs(1)?,
        hosted_before,
        "b hosted the model despite refusing to register it"
    );
    let (state_status, body) = cluster.get(1, &format!("/api/model/{tree}/state"))?;
    assert_eq!(
        state_status, 404,
        "b answered for a model it refused to host: {body}"
    );
    eprintln!("ip24: b answered {status} {refusal}, hosted_logs {hosted_before} throughout");
    Ok(())
}

// ---------------------------------------------------------------------------
// ip25
// ---------------------------------------------------------------------------

/// `ip25` — I-A13. Same `nsURI`, one feature changed, therefore a different
/// digest and a different metamodel: a registration under one digest is
/// refused by the replica holding the other, and nothing is hosted.
#[test]
fn ip25_two_descriptors_sharing_an_ns_uri_are_two_metamodels() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return skip("ip25", &why),
    };
    if let Err(why) = ip25(&binary) {
        panic!("ip25: {why}");
    }
}

fn ip25(binary: &PathBuf) -> Result<(), String> {
    let edited = edited_bt();
    let old_ns: Value = serde_json::from_str(BT).unwrap();
    let new_ns: Value = serde_json::from_str(&edited).unwrap();
    assert_eq!(
        old_ns["nsURI"], new_ns["nsURI"],
        "the two descriptors must share an nsURI, or this test tests nothing"
    );
    assert_ne!(
        digest(BT),
        digest(&edited),
        "the two descriptors must differ in digest"
    );

    // a holds the old descriptor, b holds the edited one, and nothing else.
    let cluster = Cluster::start_each(binary, &[&[("bt", BT)], &[("bt-edited", &edited)]])?;

    let (_, on_a) = cluster.get(0, "/api/metamodels")?;
    let (_, on_b) = cluster.get(1, "/api/metamodels")?;
    assert_eq!(on_a["metamodels"][0]["digest"], json!(digest(BT)));
    assert_eq!(on_b["metamodels"][0]["digest"], json!(digest(&edited)));
    assert_eq!(
        on_a["metamodels"][0]["nsURI"], on_b["metamodels"][0]["nsURI"],
        "both nodes must list the same nsURI: {on_a} against {on_b}"
    );

    // Each node refuses the digest it does not hold, and names it.
    for (replica, wanted, held) in [
        (1usize, digest(BT), digest(&edited)),
        (0usize, digest(&edited), digest(BT)),
    ] {
        let hosted_before = cluster.hosted_logs(replica)?;
        let (status, refusal) = cluster.register_raw(replica, None, &wanted)?;
        assert_eq!(
            status, 422,
            "{} holds {held} and was asked for {wanted}, answering: {refusal}",
            cluster.replicas[replica].id
        );
        assert!(
            refusal.to_string().contains(&wanted),
            "the refusal must name the digest asked for: {refusal}"
        );
        assert_eq!(
            cluster.hosted_logs(replica)?,
            hosted_before,
            "{} hosted a log for a digest it refused",
            cluster.replicas[replica].id
        );
        eprintln!(
            "ip25: {} answered {status} {refusal}",
            cluster.replicas[replica].id
        );
    }

    // And the digest a node does hold still works, so the refusal is about the
    // digest and not about the node being broken.
    let model = cluster.register(0, None, &digest(BT))?;
    let (status, refusal) = cluster.register_raw(1, Some(&model), &digest(BT))?;
    assert_eq!(
        status, 422,
        "b must refuse to join a model under the digest it does not hold: {refusal}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// ip26
// ---------------------------------------------------------------------------

/// `ip26` — I-A14. The edit case end to end: a replica holding both the old
/// and the edited descriptor keeps converging with a replica holding only the
/// old one, on the model created under the old digest, while the model created
/// under the edited digest is refused there by name and the two documents
/// never mix.
#[test]
fn ip26_an_edited_metamodel_leaves_the_old_models_converging() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return skip("ip26", &why),
    };
    if let Err(why) = ip26(&binary) {
        panic!("ip26: {why}");
    }
}

fn ip26(binary: &PathBuf) -> Result<(), String> {
    let edited = edited_bt();
    // a holds both versions; b holds only the old one.
    let cluster = Cluster::start_each(
        binary,
        &[&[("bt", BT), ("bt-edited", &edited)], &[("bt", BT)]],
    )?;
    let old = table(BT);
    let new = table(&edited);

    // A model under the old digest, created on a and joined by b.
    let before = cluster.register(0, None, &digest(BT))?;
    cluster.register(1, Some(&before), &digest(BT))?;
    cluster.until_table(1, &before)?;
    for op in behaviour_tree(&old, 0, "guard") {
        cluster.submit(0, &before, &op)?;
    }
    let converged_once = cluster.converged(&before)?;
    assert!(
        mentions(&converged_once, "guard"),
        "the old model did not converge before the edit: {converged_once}"
    );

    // A second model under the edited digest, created on a and edited there.
    let after = cluster.register(0, None, &digest(&edited))?;
    assert_ne!(before, after, "two models must be two logs");
    for op in behaviour_tree(&new, 0, "patrol") {
        cluster.submit(0, &after, &op)?;
    }

    // b refuses to join it, cleanly and by name.
    let hosted_before = cluster.hosted_logs(1)?;
    let (status, refusal) = cluster.register_raw(1, Some(&after), &digest(&edited))?;
    assert_eq!(
        status, 422,
        "b must refuse the edited metamodel it never received: {refusal}"
    );
    assert!(
        refusal.to_string().contains(&digest(&edited)),
        "the refusal must name the edited digest: {refusal}"
    );
    assert_eq!(
        cluster.hosted_logs(1)?,
        hosted_before,
        "b hosted the model under the edited digest after refusing it"
    );

    // The old model keeps converging, with a write from each replica after the
    // refusal, which is what "keeps converging throughout" has to mean.
    for op in behaviour_tree(&old, 1, "sentry") {
        cluster.submit(1, &before, &op)?;
    }
    let old_state = cluster.converged(&before)?;
    assert!(
        mentions(&old_state, "guard") && mentions(&old_state, "sentry"),
        "the old model stopped converging after the edited one appeared: {old_state}"
    );

    // Neither document holds the other's content.
    let new_state = cluster.converged_on(&after, &[0])?;
    assert!(
        mentions(&new_state, "patrol"),
        "the model under the edited digest lost its edit: {new_state}"
    );
    assert!(
        !mentions(&old_state, "patrol"),
        "the old model holds the edited model's content: {old_state}"
    );
    assert!(
        !mentions(&new_state, "guard"),
        "the edited model holds the old model's content: {new_state}"
    );
    assert!(
        !mentions(&new_state, "sentry"),
        "the edited model holds the old model's second edit: {new_state}"
    );
    eprintln!(
        "ip26: b answered {status} {refusal}; old model {before} converged as {old_state}; \
         model {after} under the edited digest reads {new_state} on a alone"
    );
    Ok(())
}
