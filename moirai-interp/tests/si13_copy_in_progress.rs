//! SI.13 round 2, claim (6): a node that is still writing the copy of `m`
//! into `m'` refuses a migration of `m'`, in process and over HTTP, with an
//! answer that names the copy in progress, writes no cut on `m'` for it and
//! opens no model for it; the copy of `m` into `m'` then completes whole.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.13.md`, section "Round 2,
//! written before implementation" (claim (6), M14). The contract, restated so
//! that a reader of the tests does not need the spec open:
//!
//! - M14, a copy in progress is not migrated. While the node's copy of `m`
//!   into `m'` is still being written (the migration of `m` answered 202, or
//!   `migrate_by` answered `whole: false`, and `m`'s status reads
//!   `copy.applied < copy.of` with no `failed`), a migration of `m'` is
//!   refused: `GenericNode::migrate(m', ..)` answers `Err` and
//!   `POST /api/model/{m'}/migrate` answers 409 or 422 within 6 s, each with a
//!   reason that holds the word "copy" and the id of `m'`. For that request
//!   the node writes no cut on `m'` (its status of `m'` names no successor,
//!   it broadcasts no `Cut` on `m'`) and hosts no model it did not host
//!   before. The copy of `m` into `m'` then completes: `m`'s status reaches
//!   `copy.applied == copy.of` with no `failed`, every read 200 within 6 s,
//!   the node's metrics of `m'` count `copy.of` operations, and `m'` serves
//!   the cut's document mapped to the revision (M6).
//!
//! Two tests:
//!
//! - `si13_r2_c1_...`, in process (L2): one `GenericNode<ModelLog, _>` over a
//!   transport that records what it broadcasts and receives nothing, with the
//!   hooks `model_node` installs (restated below from
//!   `moirai-interp/examples/model_node.rs`, which keeps them private). The
//!   running example is written, `migrate_by(m, D2, now)` leaves the copy at
//!   one operation of its plan (the deadline has passed before the first
//!   check), and `migrate(m', D2)` is asked. Then, with the event loop not yet
//!   running, `POST /api/model/{m'}/migrate` is sent and left waiting on the
//!   loop for a second; the loop is started on a thread and, in its first
//!   pass, reads that command before it applies any more of the copy
//!   (`GenericNode::run`: control commands, then `advance_copies`). The rest
//!   is read over HTTP and from the broadcast record.
//! - `si13_r2_c2_...`, on `model_node` processes (L2/L3, ignored by
//!   default, a load step): SI.12 round 4's 5,000-object model written on
//!   `si13-a` (release build), whose migration answers 202 at about 5 s with
//!   part of the copy still to apply (SI.13 round 1: 202 in every
//!   5,000-object run; this test's red run at `6f334fa`: 14,611 of 17,136
//!   operations copied at the answer, about 0.8 s of copy left); the test
//!   asks `a` to migrate `m'` the moment the 202 arrives (that run: taken 17
//!   operations later). Then `si13-b`, a direct peer that
//!   never held `m`, joins `m'` once the copy is whole and must serve `a`'s
//!   copy and name no successor for it (a replica read-back of "no cut").
//!
//! Every check is recorded, not asserted on the spot; each test prints
//! `SI13-R2-CHECK` lines and an `SI13-R2-RESULT` line and asserts at the end
//! that every check held.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si13_copy_in_progress -- --test-threads=1 --nocapture
//! # the node test, release model_node:
//! MOIRAI_E2E_INTERP_NODE_BIN=<release model_node> MOIRAI_E2E_RELAY_BIN=... \
//!   MOIRAI_E2E_BOOTNODE_BIN=... nice -n 10 cargo test -j 2 -p moirai-interp \
//!   --features network --test si13_copy_in_progress -- --ignored \
//!   --test-threads=1 --nocapture
//! ```
//!
//! `SI13_R2_REPS` (1) and `SI13_R2_SIZE` (another object count) shorten the
//! node test for a rehearsal or a control arm; either makes it print
//! `"registered": false`, and such a run cannot meet the prediction.

mod si13;

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use moirai_interp::migrate::{adapter_refusal, migrate};
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::generic::{GenericNode, LogReplica, ServedDescriptor};
use moirai_network::query::QueryableLog;
use moirai_network::transport::{
    CrdtTransport, PeerId, PeerInfo, TransportMessage, TransportResult,
};
use moirai_protocol::log_id::LogId;
use serde_json::{Value, json};

use si13::cluster::{CONVERGE_TIMEOUT, Cluster, Spec};
use si13::generated::{Generated, generate, objects, projected, to_d2};
use si13::si12::{
    D1, D2, Uml, digest, expected_d1, expected_d2, ids_by_label, labels, parsed, project,
    project_with, strip_ids,
};
use si13::{ANSWER_BOUND, copy_progress};

/// How long the copy may take to complete once the loop runs, in process.
const COPY_WAIT: Duration = Duration::from_secs(60);
/// How long `a`'s copy may take to complete, from the answer (SI.13 round 1:
/// about 5 s at 5,000 objects).
const COPY_BOUND: Duration = Duration::from_secs(900);
/// How long the in-process test leaves its route request waiting on the
/// loop before it starts the loop: the HTTP thread reads a loopback request
/// in milliseconds, and `REPLY_TIMEOUT` (5 s) leaves four seconds over.
const QUEUED: Duration = Duration::from_secs(1);
/// How long one node run may take from before its processes start.
const RUN_CEILING: Duration = Duration::from_secs(30 * 60);
const SIZE: usize = 5_000;
const REPS: usize = 2;
const LOG_ID: &str = "513c0de0513c0de0513c0de0513c0026";

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/// Every check of one test, in order, and what it found.
#[derive(Default)]
struct Checks {
    list: Vec<(String, Result<(), String>)>,
    measures: serde_json::Map<String, Value>,
}

impl Checks {
    fn check(&mut self, name: &str, result: Result<(), String>) {
        match &result {
            Err(why) => println!(
                "SI13-R2-CHECK {}",
                json!({"check": name, "held": false, "why": why})
            ),
            Ok(()) => println!("SI13-R2-CHECK {}", json!({"check": name, "held": true})),
        }
        self.list.push((name.to_string(), result));
    }

    fn measure(&mut self, key: &str, value: Value) {
        self.measures.insert(key.to_string(), value);
    }

    fn failed(&self) -> Vec<Value> {
        self.list
            .iter()
            .filter_map(|(name, result)| {
                result
                    .as_ref()
                    .err()
                    .map(|why| json!({"check": name, "why": why}))
            })
            .collect()
    }

    fn line(&self, test: &str) -> Value {
        json!({
            "test": test,
            "checks": self.list.len(),
            "held": self.list.len() - self.failed().len(),
            "failed": self.failed(),
            "measures": self.measures,
        })
    }
}

/// M14's refusal text: the word "copy" (any case) and the id of `m'`.
fn names_the_copy(text: &str, copy: &str) -> Result<(), String> {
    if text.to_lowercase().contains("copy") && text.contains(copy) {
        Ok(())
    } else {
        Err(format!(
            "the refusal does not name the copy in progress (the word \"copy\" and {copy}): {text}"
        ))
    }
}

// ---------------------------------------------------------------------------
// In process: the hooks `model_node` installs, and a transport that records
// ---------------------------------------------------------------------------

/// `model_node`'s `descriptor_key`: the digest, bare or under `digest`.
fn descriptor_key(metamodel_id: &Value) -> Option<String> {
    match metamodel_id {
        Value::String(digest) => Some(digest.clone()),
        Value::Object(fields) => fields
            .get("digest")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

/// `model_node`'s `install_ops`: the one `Install` that opens a created
/// model.
fn install_ops(model_id: &LogId, descriptor: &ServedDescriptor) -> Vec<ModelOp> {
    vec![ModelOp::Install {
        model_id: model_id.to_string(),
        metamodel_id: descriptor.key.clone(),
        descriptor: descriptor.text.clone(),
    }]
}

/// `model_node`'s `model_name`: the descriptor's `package`.
fn model_name(descriptor: &ServedDescriptor) -> Option<String> {
    let parsed: Value = serde_json::from_str(&descriptor.text).ok()?;
    parsed
        .get("package")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// `model_node`'s `migrate_model`: `moirai_interp::migrate::migrate`.
fn migrate_model(
    replica: &LogReplica<ModelLog>,
    successor: &LogId,
    descriptor: &ServedDescriptor,
) -> Result<(ModelOp, Vec<ModelOp>), String> {
    migrate(replica.state(), successor.as_str(), &descriptor.text)
        .map(|migration| (migration.cut, migration.ops))
        .map_err(|why| why.to_string())
}

/// `model_node`'s `migration_status`: `ModelLog::migration`.
fn migration_status(replica: &LogReplica<ModelLog>) -> Value {
    replica.state().migration()
}

/// A descriptor as `model_node`'s `describe_descriptor` serves it.
fn served(text: &str) -> ServedDescriptor {
    let value = parsed(text);
    ServedDescriptor {
        key: digest(text),
        listing: json!({
            "nsURI": value["nsURI"],
            "package": value["package"],
            "digest": digest(text),
        }),
        text: text.to_string(),
    }
}

/// Every frame the node broadcast, in order.
type Broadcast = Arc<Mutex<Vec<TransportMessage<ModelOp>>>>;

/// Records what the node broadcasts; sends nothing anywhere and receives
/// nothing (NW.04's `Wire`, for `ModelOp`).
struct Wire {
    id: PeerId,
    broadcast: Broadcast,
}

impl CrdtTransport for Wire {
    type Op = ModelOp;

    fn local_id(&self) -> &PeerId {
        &self.id
    }

    fn send(&mut self, _peer: &PeerId, _msg: TransportMessage<ModelOp>) -> TransportResult<()> {
        Ok(())
    }

    fn broadcast(&mut self, msg: TransportMessage<ModelOp>) -> TransportResult<()> {
        self.broadcast.lock().expect("the record").push(msg);
        Ok(())
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<ModelOp>)>> {
        Ok(None)
    }

    fn peers(&self) -> Vec<PeerInfo> {
        Vec::new()
    }

    fn is_connected(&self, _peer: &PeerId) -> bool {
        false
    }

    fn pause_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        Ok(())
    }

    fn resume_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        Ok(())
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        Ok(())
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        Ok(())
    }

    fn buffered_count(&self, _peer: &PeerId) -> usize {
        0
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(Vec::new())
    }
}

type TestNode = GenericNode<ModelLog, Wire>;

/// A node as `model_node` sets one up, minus the network: `D1` and `D2`
/// served, registration, naming and migration enabled.
fn node(id: &str) -> (TestNode, Broadcast) {
    let broadcast = Broadcast::default();
    let wire = Wire {
        id: id.to_string(),
        broadcast: Arc::clone(&broadcast),
    };
    let default_log = LogId::parse(LOG_ID).expect("a log id");
    let mut node = TestNode::with_transport_and_log_id(id.to_string(), &[id], wire, default_log);
    node.enable_state_query();
    node.enable_state_transfer();
    node.serve_metamodels(vec![served(D1), served(D2)]);
    node.enable_registration(descriptor_key, install_ops);
    node.enable_model_naming(model_name);
    node.enable_migration(migrate_model, migration_status, adapter_refusal);
    (node, broadcast)
}

/// Every `Cut` the node broadcast on `log`, as the successor it names.
fn cuts_on(broadcast: &Broadcast, log: &str) -> Vec<String> {
    let frames = broadcast.lock().expect("the record");
    let mut cuts = Vec::new();
    for frame in frames.iter() {
        let (log_id, ops): (String, Vec<&ModelOp>) = match frame {
            TransportMessage::Event { event } => {
                (event.log_id().to_string(), vec![event.event().op()])
            }
            TransportMessage::Batch { batch } => (
                batch.log_id().to_string(),
                batch.batch().events().iter().map(|e| e.op()).collect(),
            ),
            _ => continue,
        };
        if log_id != log {
            continue;
        }
        for op in ops {
            if let ModelOp::Cut { migrated_to, .. } = op {
                cuts.push(migrated_to.clone());
            }
        }
    }
    cuts
}

/// The node's identified read of a hosted model, in process.
fn identified(node: &TestNode, model: &LogId) -> Value {
    node.hosted(model)
        .map(ModelLog::query_identified_state_json)
        .unwrap_or(Value::Null)
}

/// No cut on `m'` and nothing opened: M14's "for that request".
fn no_cut_in_process(
    node: &TestNode,
    broadcast: &Broadcast,
    copy: &LogId,
    hosted_before: &BTreeSet<String>,
) -> Result<(), String> {
    let hosted: BTreeSet<String> = node.hosted_logs().map(LogId::to_string).collect();
    if &hosted != hosted_before {
        return Err(format!(
            "the node hosts {:?}, not the {:?} it hosted before",
            hosted, hosted_before
        ));
    }
    let named = node
        .hosted(copy)
        .and_then(|replica| replica.state().migrated_to().map(str::to_string));
    if let Some(to) = named {
        return Err(format!(
            "{copy} is cut: its status names the successor {to}"
        ));
    }
    let cuts = cuts_on(broadcast, copy.as_str());
    if !cuts.is_empty() {
        return Err(format!(
            "the node broadcast a cut on {copy} naming {cuts:?}"
        ));
    }
    Ok(())
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("a client")
}

fn get(client: &reqwest::blocking::Client, port: u16, path: &str) -> Result<(u16, Value), String> {
    let response = client
        .get(format!("http://127.0.0.1:{port}{path}"))
        .send()
        .map_err(|err| format!("GET {path}: {err}"))?;
    let status = response.status().as_u16();
    let body = response.text().map_err(|err| err.to_string())?;
    Ok((
        status,
        serde_json::from_str(&body).unwrap_or(Value::String(body)),
    ))
}

fn read(client: &reqwest::blocking::Client, port: u16, path: &str) -> Result<Value, String> {
    match get(client, port, path)? {
        (200, body) => Ok(body),
        (status, body) => Err(format!("GET {path} answered {status}: {body}")),
    }
}

#[test]
fn si13_r2_c1_in_process_a_copy_in_progress_is_not_migrated() {
    let mut out = Checks::default();
    let (mut node, broadcast) = node("si13-g");
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().expect("its address").port();
    node.start_http_on(listener);

    // Set-up: the running example on m, under D1.
    let uml = Uml::new(D1);
    let m = node
        .register(None, json!(digest(D1)))
        .expect("premise: the node creates m under D1")
        .model_id;
    for op in uml.structure() {
        let result = node.apply_op_to(&m, op);
        assert!(result.success, "premise: the structure: {}", result.message);
    }
    let ids = ids_by_label(&identified(&node, &m));
    for op in uml.links(&ids) {
        let result = node.apply_op_to(&m, op);
        assert!(result.success, "premise: the links: {}", result.message);
    }
    assert_eq!(
        project(&identified(&node, &m)),
        expected_d1(),
        "premise: the node serves the running example"
    );

    // The copy, left in progress: the deadline has passed before the first
    // check, so one operation of the plan is applied (`advance_copy`).
    let migrated = node
        .migrate_by(&m, json!(digest(D2)), Instant::now())
        .expect("premise: the migration of m to D2 is planned and its cut written");
    let copy = migrated.model_id.clone();
    assert!(
        !migrated.whole,
        "premise: the copy of m into {copy} must still be in progress after a deadline that \
         has passed; migrate_by answered it whole, so M14 cannot be exercised in process"
    );
    let hosted_before: BTreeSet<String> = node.hosted_logs().map(LogId::to_string).collect();
    out.measure("copy", json!(copy.as_str()));

    // C1.1 and C1.2: the in-process migrate of m'.
    let answer = node.migrate(&copy, json!(digest(D2)));
    out.measure(
        "in_process_answer",
        json!(match &answer {
            Ok(to) => format!("Ok({to})"),
            Err(why) => format!("Err({why})"),
        }),
    );
    out.check(
        "C1.1 (M14): the in-process migrate of m' is refused, naming the copy in progress",
        match &answer {
            Ok(to) => Err(format!(
                "migrate({copy}) answered Ok({to}) while the copy of m into {copy} was being \
                 written"
            )),
            Err(why) => names_the_copy(&why.to_string(), copy.as_str()),
        },
    );
    out.check(
        "C1.2 (M14): after it, no cut on m' and no model opened",
        no_cut_in_process(&node, &broadcast, &copy, &hosted_before),
    );

    // C1.3 and C1.4: the route, its command queued before the loop runs.
    let path = format!("/api/model/{copy}/migrate");
    let body = json!({ "metamodel_id": digest(D2) }).to_string();
    let request = std::thread::spawn(move || {
        let started = Instant::now();
        let answer = client()
            .post(format!("http://127.0.0.1:{port}{path}"))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| err.to_string())
            .and_then(|response| {
                let status = response.status().as_u16();
                response
                    .text()
                    .map(|text| (status, text))
                    .map_err(|err| err.to_string())
            });
        (answer, started.elapsed())
    });
    std::thread::sleep(QUEUED);
    std::thread::spawn(move || node.run());
    let (answer, took) = request.join().expect("the request thread");
    let client = client();
    out.measure("route_ms", json!(took.as_millis() as u64));
    out.measure("route_answer", json!(format!("{answer:?}")));
    out.check(
        "C1.3 (M14): the route refuses the migration of m', 409 or 422 within 6 s, naming the \
         copy in progress",
        match &answer {
            Err(why) => Err(format!("the route was not answered: {why}")),
            Ok((status, text)) => {
                if took > ANSWER_BOUND {
                    Err(format!(
                        "the route answered {status} after {} ms: {text}",
                        took.as_millis()
                    ))
                } else if *status == 409 || *status == 422 {
                    names_the_copy(text, copy.as_str())
                } else {
                    let m_copy = read(&client, port, &format!("/api/model/{m}/migration"))
                        .map(|status| status["copy"].clone())
                        .unwrap_or(Value::Null);
                    let whole = m_copy.get("failed").is_none()
                        && m_copy["applied"].is_u64()
                        && m_copy["applied"] == m_copy["of"];
                    Err(if (200..300).contains(status) && whole {
                        format!(
                            "not exercised: the route answered {status}: {text} with the copy of \
                             m already whole ({m_copy}), so the request reached the loop after \
                             its first pass"
                        )
                    } else {
                        format!(
                            "the route answered {status}: {text} (409 or 422 naming the copy \
                             expected); m's copy then reads {m_copy}"
                        )
                    })
                }
            }
        },
    );
    out.check(
        "C1.4 (M14): after it, m' names no successor, the node hosts what it hosted, no cut \
         on m' broadcast",
        (|| {
            let status = read(&client, port, &format!("/api/model/{copy}/migration"))?;
            if !status["migrated_to"].is_null() {
                return Err(format!("m' reads {status}: it is cut"));
            }
            let models = read(&client, port, "/api/models")?;
            let hosted: BTreeSet<String> = models["models"]
                .as_array()
                .map(|list| {
                    list.iter()
                        .filter_map(|entry| entry["model_id"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let opened: Vec<&String> = hosted.difference(&hosted_before).collect();
            if !opened.is_empty() {
                return Err(format!("the node opened {opened:?} for the request"));
            }
            let cuts = cuts_on(&broadcast, copy.as_str());
            if !cuts.is_empty() {
                return Err(format!(
                    "the node broadcast a cut on {copy} naming {cuts:?}"
                ));
            }
            Ok(())
        })(),
    );

    // C1.5: the copy completes whole.
    out.check(
        "C1.5 (M14, M9, M6): the copy of m into m' completes whole and m' serves the cut \
         mapped to D2",
        (|| {
            let deadline = Instant::now() + COPY_WAIT;
            let mut last = 0;
            let of = loop {
                let asked = Instant::now();
                let status = read(&client, port, &format!("/api/model/{m}/migration"))?;
                if asked.elapsed() > ANSWER_BOUND {
                    return Err(format!(
                        "a read of m's status took {} ms",
                        asked.elapsed().as_millis()
                    ));
                }
                if let Some(failed) = status["copy"].get("failed") {
                    return Err(format!("m's copy stopped: {failed} ({status})"));
                }
                let (applied, of) = copy_progress(&status)?;
                if applied < last {
                    return Err(format!("m's progress went back from {last} to {applied}"));
                }
                last = applied;
                if applied == of {
                    break of;
                }
                if Instant::now() > deadline {
                    return Err(format!("m's copy stopped at {applied} of {of}: {status}"));
                }
                std::thread::sleep(Duration::from_millis(50));
            };
            let ops =
                read(&client, port, &format!("/api/model/{copy}/metrics"))?["ops_applied"].as_u64();
            if ops != Some(of) {
                return Err(format!(
                    "m's copy reads {of} operations applied and the metrics of m' {ops:?}"
                ));
            }
            let doc = read(
                &client,
                port,
                &format!("/api/model/{copy}/state/identified"),
            )?;
            if project(&doc) != expected_d2() {
                return Err(format!(
                    "m' is not the running example mapped to D2: {}",
                    project(&doc)
                ));
            }
            let plain = read(&client, port, &format!("/api/model/{copy}/state"))?;
            if project_with(&plain, &labels(&doc)) != strip_ids(&expected_d2()) {
                return Err("m''s default read is not the running example under D2".into());
            }
            let status = read(&client, port, &format!("/api/model/{copy}/migration"))?;
            if !status["migrated_to"].is_null() {
                return Err(format!("m' reads {status} once its copy is whole"));
            }
            Ok(())
        })(),
    );

    let line = out.line("c1");
    println!("SI13-R2-RESULT {line}");
    let failed = out.failed();
    assert!(
        failed.is_empty(),
        "SI.13 round 2, C1: {} of {} checks failed: {}",
        failed.len(),
        out.list.len(),
        serde_json::to_string_pretty(&failed).unwrap_or_default()
    );
}

// ---------------------------------------------------------------------------
// On nodes, at 5,000 objects
// ---------------------------------------------------------------------------

const A: usize = 0;
const B: usize = 1;

fn pair() -> Vec<Spec> {
    vec![
        Spec::new("si13-a", &["si13-b"], &[("d1", D1), ("d2", D2)]),
        Spec::new("si13-b", &["si13-a"], &[("d1", D1), ("d2", D2)]),
    ]
}

/// `m` created on `a` under `D1` and written there; `b` does not join it.
fn write_m(cluster: &Cluster, g: &Generated, out: &mut Checks) -> Result<String, String> {
    let uml = Uml::new(D1);
    let m = cluster.register(A, None, &digest(D1))?;
    let structure = g.structure(&uml);
    let built = Instant::now();
    cluster.submit_batches(A, &m, &structure)?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    let links = g.links(&uml, &ids)?;
    cluster.submit_batches(A, &m, &links)?;
    out.measure("m_ops_posted", json!(structure.len() + links.len()));
    out.measure("build_ms", json!(built.elapsed().as_millis() as u64));
    let doc = cluster.identified(A, &m)?;
    if projected(&doc)? != g.expected_d1() {
        return Err("premise: a does not serve m as generated".into());
    }
    if objects(&doc)? != g.size {
        return Err(format!(
            "premise: a serves {} objects, the generator {}",
            objects(&doc)?,
            g.size
        ));
    }
    Ok(m)
}

fn hosted(cluster: &Cluster, node: usize) -> Result<BTreeSet<String>, String> {
    Ok(cluster.models(node)?.into_keys().collect())
}

/// One run: the pair, `m`, the migration of `m` and, the moment it answers
/// 202, the migration of `m'`.
fn one_run(g: &Generated, rep: usize) -> Checks {
    let mut out = Checks::default();
    out.measure("rep", json!(rep));
    let started = Instant::now();
    let ceiling = Some(Instant::now() + RUN_CEILING);
    let cluster = match Cluster::start(&format!("r2-copy-{rep}"), LOG_ID, &pair(), ceiling) {
        Ok(cluster) => cluster,
        Err(why) => {
            out.check("set-up", Err(format!("the cluster: {why}")));
            return out;
        }
    };
    let m = match cluster
        .await_routes()
        .and_then(|()| write_m(&cluster, g, &mut out))
    {
        Ok(m) => m,
        Err(why) => {
            out.check("set-up", Err(why));
            out.measure("node_log_tails", json!(cluster.tails()));
            return out;
        }
    };
    let before = match hosted(&cluster, A) {
        Ok(before) => before,
        Err(why) => {
            out.check("set-up", Err(why));
            return out;
        }
    };

    // The migration of m, which must leave its copy in progress (202).
    let asked = Instant::now();
    let (status, body) = cluster
        .post(
            A,
            &format!("/api/model/{m}/migrate"),
            json!({ "metamodel_id": digest(D2) }).to_string(),
        )
        .unwrap_or_else(|why| (0, Value::String(why)));
    let took = asked.elapsed();
    out.measure("migrate_m_status", json!(status));
    out.measure("migrate_m_ms", json!(took.as_millis() as u64));
    let copy = body["model_id"]
        .as_str()
        .filter(|id| *id != m && LogId::parse(id).is_ok())
        .map(str::to_string);
    let copy = match (status, copy) {
        (202, Some(copy)) if took <= ANSWER_BOUND => copy,
        (201, Some(_)) => {
            out.check(
                "C2 premise: m's migration answers 202, its copy in progress",
                Err(format!(
                    "not exercised: m's migration answered 201 (the copy whole at the answer) \
                     after {} ms, so no migration of m' can be asked while the copy is written",
                    took.as_millis()
                )),
            );
            return out;
        }
        _ => {
            out.check(
                "C2 premise: m's migration answers 202, its copy in progress",
                Err(format!(
                    "m's migration answered {status} after {} ms: {body} (M8: 202 naming m')",
                    took.as_millis()
                )),
            );
            return out;
        }
    };
    out.check(
        "C2 premise: m's migration answers 202, its copy in progress",
        Ok(()),
    );

    // C2.1: the migration of m', asked the moment m's answer arrived.
    let asked = Instant::now();
    let (status, body) = cluster
        .post(
            A,
            &format!("/api/model/{copy}/migrate"),
            json!({ "metamodel_id": digest(D2) }).to_string(),
        )
        .unwrap_or_else(|why| (0, Value::String(why)));
    let took = asked.elapsed();
    out.measure("migrate_copy_status", json!(status));
    out.measure("migrate_copy_ms", json!(took.as_millis() as u64));
    out.measure("migrate_copy_body", body.clone());
    let m_after = cluster.migration_raw(A, &m).unwrap_or(Value::Null);
    out.measure("m_copy_after_refusal", m_after["copy"].clone());
    out.check(
        "C2.1 (M14): a refuses the migration of m', 409 or 422 within 6 s, naming the copy in \
         progress",
        if took > ANSWER_BOUND {
            Err(format!(
                "a answered the migration of m' {status} after {} ms: {body}",
                took.as_millis()
            ))
        } else if status == 409 || status == 422 {
            names_the_copy(&body.to_string(), &copy)
        } else {
            let stopped = m_after["copy"].get("failed").is_some()
                || copy_progress(&m_after).is_ok_and(|(applied, of)| applied < of);
            Err(if stopped || !(200..300).contains(&status) {
                format!(
                    "a answered the migration of m' {status}: {body}, while the copy of m into \
                     m' was being written (m's copy then reads {})",
                    m_after["copy"]
                )
            } else {
                format!(
                    "not exercised: a answered the migration of m' {status}: {body} with the \
                     copy of m already whole ({})",
                    m_after["copy"]
                )
            })
        },
    );

    // C2.2: no cut on m', no model opened.
    out.check(
        "C2.2 (M14): after it, a's m' names no successor and a hosts what it hosted",
        (|| {
            let status = cluster.migration_raw(A, &copy)?;
            if !status["migrated_to"].is_null() {
                return Err(format!("a's m' reads {status}: it is cut"));
            }
            let now = hosted(&cluster, A)?;
            let opened: Vec<&String> = now.difference(&before).filter(|id| **id != copy).collect();
            if !opened.is_empty() {
                return Err(format!("a opened {opened:?} beside m'"));
            }
            Ok(())
        })(),
    );

    // C2.3: the copy completes whole.
    let copying = Instant::now();
    let completed = (|| {
        let deadline = cluster.deadline(COPY_BOUND);
        let mut last = 0;
        let mut slowest = Duration::ZERO;
        let of = loop {
            let asked = Instant::now();
            let status = cluster.migration_raw(A, &m)?;
            let took = asked.elapsed();
            slowest = slowest.max(took);
            if took > ANSWER_BOUND {
                return Err(format!("a read of m's status took {} ms", took.as_millis()));
            }
            if let Some(failed) = status["copy"].get("failed") {
                return Err(format!("m's copy stopped: {failed} ({status})"));
            }
            let (applied, of) = copy_progress(&status)?;
            if applied < last {
                return Err(format!("m's progress went back from {last} to {applied}"));
            }
            last = applied;
            if applied == of {
                break of;
            }
            if Instant::now() > deadline {
                return Err(format!("m's copy stopped at {applied} of {of}"));
            }
            std::thread::sleep(Duration::from_millis(250));
        };
        out.measure("copy_of", json!(of));
        out.measure("progress_slowest_ms", json!(slowest.as_millis() as u64));
        let ops = cluster.metrics(A, &copy)?["ops_applied"].as_u64();
        if ops != Some(of) {
            return Err(format!(
                "m's copy reads {of} operations applied and a's metrics of m' {ops:?}"
            ));
        }
        let doc = cluster.identified(A, &copy)?;
        if projected(&doc)? != to_d2(&g.expected_d1()) {
            return Err("a's m' is not m's read at the cut mapped to D2".into());
        }
        if objects(&doc)? != g.size {
            return Err(format!("a's m' holds {} objects", objects(&doc)?));
        }
        let status = cluster.migration_raw(A, &copy)?;
        if !status["migrated_to"].is_null() {
            return Err(format!("a's m' reads {status} once its copy is whole"));
        }
        Ok(())
    })();
    out.check(
        "C2.3 (M14, M9, M6): the copy of m into m' completes whole, every read in time, and is \
         the cut mapped to D2",
        completed,
    );
    out.measure("copy_ms", json!(copying.elapsed().as_millis() as u64));

    // C2.4: b, which never held m, joins m' and serves a's copy, uncut.
    let joining = Instant::now();
    out.check(
        "C2.4 (M14): b joins m' and serves a's copy, naming no successor for it",
        (|| {
            cluster.join_by_id(B, &copy)?;
            let first = cluster.identified(A, &copy)?;
            cluster.until_on(&[B], CONVERGE_TIMEOUT, "b serves a's copy", |node| {
                if cluster.identified(node, &copy)? != first {
                    return Err("b does not serve a's copy yet".into());
                }
                cluster.lists_under_d2(node, &copy)
            })?;
            let status = cluster.migration_raw(B, &copy)?;
            if !status["migrated_to"].is_null() {
                return Err(format!("b's m' reads {status}: a cut on m' reached b"));
            }
            Ok(())
        })(),
    );
    out.measure("b_join_ms", json!(joining.elapsed().as_millis() as u64));
    out.measure("run_ms", json!(started.elapsed().as_millis() as u64));
    if !out.failed().is_empty() {
        out.measure("node_log_tails", json!(cluster.tails()));
    }
    out
}

#[test]
#[ignore = "SI.13 round 2's node test at 5,000 objects: a load step; run with --ignored"]
fn si13_r2_c2_a_node_refuses_to_migrate_the_copy_it_is_still_writing() {
    let reps = std::env::var("SI13_R2_REPS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| (1..=REPS).contains(n));
    let size = std::env::var("SI13_R2_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n >= 100);
    let registered = reps.is_none() && size.is_none();
    let reps = reps.unwrap_or(REPS);
    let size = size.unwrap_or(SIZE);
    println!(
        "SI13-R2-SCHEDULE {}",
        json!({"size": size, "reps": reps, "registered": registered})
    );
    let g = generate(size);
    let mut held = 0;
    for rep in 0..reps {
        let run = one_run(&g, rep);
        if run.failed().is_empty() && !run.list.is_empty() {
            held += 1;
        }
        println!("SI13-R2-RUN {}", run.line("c2"));
    }
    println!(
        "SI13-R2-RESULT {}",
        json!({"test": "c2", "runs": reps, "runs_every_check": held, "registered": registered})
    );
    assert_eq!(
        held, reps,
        "SI.13 round 2, C2: {held} of {reps} runs held every check"
    );
}
