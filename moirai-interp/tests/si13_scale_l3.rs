//! SI.13 at 5,000 objects: SI.12 round 4's model (`generate(5000)`)
//! migrated on three `model_node` processes, one of which (`si13-c`) reaches
//! the others only through a `moirai-relay`; the migrate call answered in
//! time with its progress reported (OB6, M8, M9); no frame over the relay's
//! bound and every node holding the copy (R3-7, M10); a node that was busy
//! answering 504 only for what it never carries out (M13); and a fourth
//! node, `si13-d`, started afterwards and also relay-only, catching up on
//! `m'` and on the cut `m` (M12).
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.13.md`; the contract is
//! restated in `si13/mod.rs` and `si12/mod.rs`; the generator is SI.12's,
//! restated in `si13/generated.rs`. The registered schedule is four runs,
//! interleaved: plain, concurrent, plain, concurrent, each on a fresh
//! cluster (bootnode, relay, `si13-a` and `si13-b` direct and serving `D1`
//! and `D2`, `si13-c` relayed and serving `D1` alone).
//!
//! A run, in order (the checks named as the spec registers them):
//!
//! - set-up: the topology (`R0`); a side model `m_small` (the running
//!   example) on `a` alone, for the probe; `m` created on `a`, joined by `b`
//!   and `c`, written on `a` in batches of 250; K0, every node serves `m` as
//!   generated, 5,000 objects.
//! - concurrent runs: every node pauses every peer; `b` appends `s` to the
//!   name of `Class000_1` and makes it abstract (`si13-b` 1, 2), `c` adds the
//!   stereotype `persistent` to `Class000_0` (`si13-c` 1).
//! - M8: `a`'s migrate call answers 201 or 202 within 6 s naming `m'`.
//! - K2: at once, `a` names `m'` (nothing refused, nothing superseded), serves
//!   `m` as before the call, refuses an edit naming `m'`; in a concurrent run
//!   `b` names nothing.
//! - M9: `a`'s status reports the copy's progress, every read answered 200
//!   within 6 s, never going back, reaching the whole copy, which `a`'s
//!   metrics of `m'` then count. The first of those reads is K2's, the read
//!   right after the answer, timed too (SI.13 round 2, OB15).
//! - concurrent runs: every node resumes (`K3-resume`).
//! - K3: every node names `m'` and lists the edits concurrent with the cut
//!   (none in a plain run). K4: every node serves `m` as `a` served it when it
//!   cut. K5: every node refuses an edit (single route, and 422 on the batch
//!   route) and a further migration, naming `m'`.
//! - K6: `b` and `c` join `m'` by id; every node serves `a`'s copy, which is
//!   `to_d2` of the generator's model, 5,000 objects, in both reads, listed
//!   under `D2`. K7 (concurrent runs): the three edits are in neither model on
//!   any node.
//! - M10: the relay logged no frame over its bound and no read error; no
//!   node's relay session closed; `c` still reaches `a` and `b` relayed.
//! - M12: `si13-d` starts, relay-only; it joins `m'` by id and serves `a`'s
//!   copy; it joins `m` by id and names `m'`, supersedes nothing, serves the
//!   cut, refuses an edit; M10 again.
//! - M13, the probe, last, so that its writes touch no other check: `b`
//!   writes a calibration batch of 5,000 comment insertions into `m'`, and
//!   the time `a` then stays busy applying them is measured; then two
//!   windows, each a batch on `b` sized from the window before it to keep `a`
//!   busy about 15 s (at most eight calibration batches); the moment `b`
//!   answers, `a` applies those events in one pass of its loop (it drains
//!   every frame it holds before it reads a command), and two calls go to `a`
//!   one after the other (its HTTP thread serves one request at a time): P1
//!   migrate `m_small` and P2 create a model in the first window, P3 serve
//!   `D2_REMOVE` and P4 pause `si13-b` in the second. Each answer must match
//!   what `a` does: a call answered as not taken in time (504, or the pause
//!   route's `success: false`, `timeout`) is never carried out, any other 2xx
//!   is. `probe_exercised` records whether all four were answered as not
//!   taken in time, which the prediction registers.
//!
//! Every timing is an L3 resource check (VALIDATION.md section 1), not a
//! performance figure. A run stops at its [`RUN_CEILING`]; it is never
//! retried.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si13_scale_l3 \
//!     -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! `SI13_SCALE_REPS` (1 or 2) and `SI13_SCALE_CASES` (`plain`, `concurrent`
//! or both, comma-separated) shorten the schedule for the red proof and the
//! control arms, and `SI13_SCALE_SIZE` (another object count) rehearses the
//! harness on a smaller model; any of them makes the run print
//! `"registered": false`, and such a run cannot meet the prediction.

mod si13;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_network::log_id::LogId;
use serde_json::{Value, json};

use si13::cluster::{CONVERGE_TIMEOUT, Cluster, Spec};
use si13::generated::{
    Generated, Kind, Step, at_path, generate, insert, leaf, objects, projected, to_d2,
};
use si13::si12::{D1, D2, D2_REMOVE, Uml, digest, ids_by_label, labels, project_with, strip_ids};
use si13::{ANSWER_BOUND, copy_progress, expected13, no_copy, status13};

const SIZE: usize = 5_000;
const REPS: usize = 2;
/// How long one run may take from before its processes start.
const RUN_CEILING: Duration = Duration::from_secs(60 * 60);
/// How long `a`'s progress may take to reach the whole copy, from the
/// answer (round 4: the whole migration took 124.6 to 128.7 s on the loop).
const COPY_BOUND: Duration = Duration::from_secs(900);
/// The probe's calibration batch: comment insertions `b` writes into `m'`,
/// whose effect on `a` (how long `a` stays busy applying them) is timed, to
/// size the two probe windows. Comment insertions are cheap and grow dearer
/// as the model grows (SI.13's red runs: 3,000 took `b` 1.27 s at 5,000
/// objects; 102,589 took it 191 s at 2,000 growing to 104,589), so each
/// window is sized from the window before it, not assumed.
const CALIBRATION_OPS: usize = 5_000;
/// How long each probe window is sized to keep `a` busy: two calls of 5 s
/// each, one after the other, and a margin.
const WINDOW_TARGET: Duration = Duration::from_secs(15);
/// A window holds at most this many times the calibration batch.
const WINDOW_MAX_FACTOR: usize = 8;
const LOG_ID: &str = "513c0de0513c0de0513c0de0513c0005";

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Plain,
    Concurrent,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Case::Plain => "plain",
            Case::Concurrent => "concurrent",
        }
    }
}

/// One run's checks and measures.
struct Run {
    size: usize,
    case: Case,
    rep: usize,
    checks: Vec<(String, Result<(), String>)>,
    measures: serde_json::Map<String, Value>,
    setup: Option<String>,
}

impl Run {
    fn check(&mut self, name: &str, result: Result<(), String>) {
        self.checks.push((name.to_string(), result));
    }

    fn measure(&mut self, key: &str, value: Value) {
        self.measures.insert(key.to_string(), value);
    }

    fn ms(&mut self, key: &str, since: Instant) {
        self.measure(key, json!(since.elapsed().as_secs_f64() * 1000.0));
    }

    fn held(&self) -> bool {
        self.setup.is_none() && self.checks.iter().all(|(_, result)| result.is_ok())
    }

    fn line(&self) -> Value {
        json!({
            "size": self.size,
            "case": self.case.name(),
            "rep": self.rep,
            "completed": self.setup.is_none(),
            "setup_error": self.setup,
            "checks": self
                .checks
                .iter()
                .map(|(name, result)| (name.clone(), json!(result.is_ok())))
                .collect::<serde_json::Map<String, Value>>(),
            "failed": self
                .checks
                .iter()
                .filter_map(|(name, result)| result
                    .as_ref()
                    .err()
                    .map(|why| json!({"check": name, "why": why})))
                .collect::<Vec<_>>(),
            "measures": self.measures,
        })
    }
}

fn three() -> Vec<Spec> {
    vec![
        Spec::new("si13-a", &["si13-b"], &[("d1", D1), ("d2", D2)]),
        Spec::new("si13-b", &["si13-a"], &[("d1", D1), ("d2", D2)]),
        Spec::new("si13-c", &[], &[("d1", D1)]),
    ]
}

fn d1() -> Uml {
    Uml::new(D1)
}

/// The running example, as `si12::Uml` writes it, into a fresh model on `a`
/// alone.
fn small_model(cluster: &Cluster) -> Result<String, String> {
    let uml = d1();
    let m = cluster.register(A, None, &digest(D1))?;
    cluster.submit_batches(A, &m, &uml.structure())?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    cluster.submit_batches(A, &m, &uml.links(&ids))?;
    Ok(m)
}

/// `m` created on `a`, joined by `b` and `c`, written on `a`; K0.
fn write_m(cluster: &Cluster, g: &Generated, run: &mut Run) -> Result<String, String> {
    let uml = d1();
    let m = cluster.register(A, None, &digest(D1))?;
    cluster.register(B, Some(&m), &digest(D1))?;
    cluster.register(C, Some(&m), &digest(D1))?;
    cluster.until("m's Install on every node", |node| {
        match cluster.metrics(node, &m)?["delivered_ops"].as_u64() {
            Some(n) if n >= 1 => Ok(()),
            other => Err(format!("node {node} delivered {other:?}")),
        }
    })?;
    let structure = g.structure(&uml);
    let built = Instant::now();
    cluster.submit_batches(A, &m, &structure)?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    let links = g.links(&uml, &ids)?;
    cluster.submit_batches(A, &m, &links)?;
    run.measure("m_ops_posted", json!(structure.len() + links.len()));
    let expected = g.expected_d1();
    let expected_plain = strip_ids(&expected);
    cluster.until("m as generated on every node (K0)", |node| {
        let doc = cluster.identified(node, &m)?;
        if projected(&doc)? != expected {
            return Err(format!(
                "node {node} does not serve the generated model yet"
            ));
        }
        if project_with(&cluster.plain(node, &m)?, &labels(&doc)) != expected_plain {
            return Err(format!("node {node}'s default read of m"));
        }
        Ok(())
    })?;
    run.ms("build_ms", built);
    let mut counted = Vec::new();
    for node in [A, B, C] {
        counted.push(objects(&cluster.identified(node, &m)?)?);
    }
    run.measure("elements", json!(counted));
    run.check(
        "K0",
        if counted.iter().all(|n| *n == g.size) {
            Ok(())
        } else {
            Err(format!(
                "the nodes serve {counted:?} objects; the generator states {}",
                g.size
            ))
        },
    );
    Ok(m)
}

fn path_of(g: &Generated, index: usize) -> Vec<Step> {
    g.elements[index].path.clone()
}

/// The concurrent edits: `b` renames `Class000_1` (one character) and makes
/// it abstract, `c` adds `persistent` to `Class000_0`.
fn concurrent_edits(cluster: &Cluster, g: &Generated, m: &str) -> Result<(), String> {
    let uml = d1();
    let (first, second) = g.first_classes();
    let second_path = path_of(g, second);
    let pos = g.elements[second].name.as_ref().map_or(0, String::len);
    cluster.submit(
        B,
        m,
        &[
            leaf(
                &uml,
                &second_path,
                "name",
                LeafOp::InsertChar { pos, ch: 's' },
            ),
            leaf(&uml, &second_path, "abstract", LeafOp::Enable),
        ],
    )?;
    cluster.submit(
        C,
        m,
        &[leaf(
            &uml,
            &path_of(g, first),
            "stereotype",
            LeafOp::Add(Scalar::text("persistent")),
        )],
    )?;
    let renamed = at_path(&cluster.identified(B, m)?, &second_path)["name"].clone();
    if renamed != json!("Class000_1s") {
        return Err(format!("premise: b reads its own rename as {renamed}"));
    }
    Ok(())
}

/// An edit of `m`, refused after the cut (K5): a character into the model's
/// name.
fn probe_edit() -> ModelOp {
    leaf(&d1(), &[], "name", LeafOp::InsertChar { pos: 0, ch: 'x' })
}

/// K5 on `node`: an edit refused naming `to` (single route) and with 422
/// (batch route); with `further`, on a node that serves `D2`, a further
/// migration answered 409 or 422 naming `to` (a node that does not serve
/// `D2` is refused the descriptor first, which says nothing about the cut).
fn refusals(
    cluster: &Cluster,
    node: usize,
    m: &str,
    to: &str,
    further: bool,
) -> Result<(), String> {
    let op = probe_edit();
    let answer = cluster.post_op(node, m, &op)?;
    let message = answer["message"].as_str().unwrap_or_default();
    if answer["success"] != Value::Bool(false) || !message.contains(to) {
        return Err(format!(
            "{} answered an edit of the cut model with {answer}; it must refuse it naming {to}",
            cluster.nodes[node].id
        ));
    }
    let (status, answer) = cluster.post(
        node,
        &format!("/api/model/{m}/ops"),
        json!({ "ops": [op] }).to_string(),
    )?;
    if status != 422 {
        return Err(format!(
            "{} answered a batch on the cut model with {status}: {answer} (422 expected)",
            cluster.nodes[node].id
        ));
    }
    if !further || !cluster.serves(node, D2)? {
        return Ok(());
    }
    let (status, body) = cluster.post(
        node,
        &format!("/api/model/{m}/migrate"),
        json!({ "metamodel_id": digest(D2) }).to_string(),
    )?;
    if !(status == 409 || status == 422) || !body.to_string().contains(to) {
        return Err(format!(
            "{} answered a further migration with {status}: {body} (409 or 422 naming {to})",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

fn frozen(cluster: &Cluster, node: usize, m: &str, doc: &(Value, Value)) -> Result<(), String> {
    if cluster.identified(node, m)? != doc.0 {
        return Err(format!(
            "{} serves an identified read of m other than the cut's",
            cluster.nodes[node].id
        ));
    }
    if cluster.plain(node, m)? != doc.1 {
        return Err(format!(
            "{} serves a default read of m other than the cut's",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

/// M10 on the cluster as it stands.
fn frames(cluster: &Cluster) -> Result<(), String> {
    let alarms = cluster.relay_alarms();
    let sessions = cluster.relay_session_alarms();
    if !alarms.is_empty() || !sessions.is_empty() {
        return Err(format!(
            "the relay logged {} alarm(s) {:?}; the nodes logged {} relay session alarm(s) {:?}",
            alarms.len(),
            alarms.iter().take(5).collect::<Vec<_>>(),
            sessions.len(),
            sessions.iter().take(5).collect::<Vec<_>>()
        ));
    }
    for node in 0..cluster.nodes.len() {
        for peer in 0..cluster.nodes.len() {
            if node == peer {
                continue;
            }
            let want = cluster.expected_route(node, peer);
            let got = cluster.routes(node)?.get(&cluster.nodes[peer].id).cloned();
            if got.as_deref() != Some(want) {
                return Err(format!(
                    "{} reaches {} {got:?}, not {want}",
                    cluster.nodes[node].id, cluster.nodes[peer].id
                ));
            }
        }
    }
    Ok(())
}

/// The migrate call on `a`: M8 on its answer, and the successor, read off the
/// answer or, after a 504 or no answer, off `a`'s status (SI.12 R4-7), until
/// the run's ceiling.
fn migrate_on_a(cluster: &Cluster, m: &str, run: &mut Run) -> Option<(String, u16)> {
    let started = Instant::now();
    let (status, body) = cluster
        .post(
            A,
            &format!("/api/model/{m}/migrate"),
            json!({ "metamodel_id": digest(D2) }).to_string(),
        )
        .unwrap_or_else(|why| (0, Value::String(why)));
    let took = started.elapsed();
    run.ms("migrate_ms", started);
    run.measure("migrate_status", json!(status));
    let named = body["model_id"]
        .as_str()
        .filter(|id| *id != m && LogId::parse(id).is_ok())
        .map(str::to_string);
    let m8 = if took > ANSWER_BOUND {
        Err(format!(
            "the migrate call took {} ms, over 6,000 (M8); it answered {status}: {body}",
            took.as_millis()
        ))
    } else if !(status == 201 || status == 202) || named.is_none() {
        Err(format!(
            "the migrate call answered {status}: {body} (M8: 201 or 202 naming m')"
        ))
    } else {
        Ok(())
    };
    let failed = m8.is_err();
    run.check("M8", m8);
    if let Some(to) = named
        && !failed
    {
        return Some((to, status));
    }
    if status != 504 && status != 0 {
        return None;
    }
    // A 504 or no answer: does the node carry the migration out anyway?
    let answered = status;
    let deadline = cluster.deadline(COPY_BOUND);
    loop {
        if let Ok(read) = cluster.migration(A, m)
            && let Some(to) = read["migrated_to"].as_str()
        {
            run.ms("migrated_ms", started);
            run.check(
                "M8-504",
                Err(format!(
                    "a answered {answered} and then named the successor {to}: an answer that \
                     is not 2xx for a migration the node carried out (M8)"
                )),
            );
            return Some((to.to_string(), answered));
        }
        if Instant::now() > deadline {
            cluster.at_ceiling(deadline);
            return None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// M9: `a`'s progress, from the answer to the whole copy.
fn progress(
    cluster: &Cluster,
    m: &str,
    to: &str,
    status: u16,
    run: &mut Run,
) -> Result<(), String> {
    let started = Instant::now();
    let deadline = cluster.deadline(COPY_BOUND);
    let mut last = 0;
    let mut reads = 0u64;
    let mut slowest = Duration::ZERO;
    let result = loop {
        let asked = Instant::now();
        let raw = cluster.migration_raw(A, m);
        let took = asked.elapsed();
        slowest = slowest.max(took);
        reads += 1;
        let raw = match raw {
            Ok(raw) => raw,
            Err(why) => break Err(format!("read {reads} of a's status failed (M9): {why}")),
        };
        if took > ANSWER_BOUND {
            break Err(format!(
                "read {reads} of a's status took {} ms (M9)",
                took.as_millis()
            ));
        }
        let (applied, of) = match copy_progress(&raw) {
            Ok(progress) => progress,
            Err(why) => break Err(why),
        };
        if reads == 1 && status == 201 && applied != of {
            break Err(format!(
                "a answered 201 and its first read reports {applied} of {of}"
            ));
        }
        if applied < last {
            break Err(format!("a's progress went back from {last} to {applied}"));
        }
        last = applied;
        if applied == of {
            run.measure("copy_of", json!(of));
            let ops = cluster.metrics(A, to)?["ops_applied"].as_u64();
            break if ops == Some(of) {
                Ok(())
            } else {
                Err(format!(
                    "a reports {of} operations copied and its metrics of m' {ops:?}"
                ))
            };
        }
        if Instant::now() > deadline {
            cluster.at_ceiling(deadline);
            break Err(format!("a's progress stopped at {applied} of {of}"));
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    run.ms("copy_ms", started);
    run.measure("progress_reads", json!(reads));
    run.measure("progress_slowest_ms", json!(slowest.as_millis() as u64));
    result
}

/// Whether a call was answered as one the event loop did not take in time:
/// 504, or, on the pause and resume routes (`control()` in `http_api.rs`),
/// 200 with `{"success": false, "message": "timeout"}`.
fn timed_out(status: u16, body: &Value) -> bool {
    status == 504 || (body["success"] == json!(false) && body["message"] == json!("timeout"))
}

/// `n` comment insertions into `m'`, one at `ownedComments[0]` of each
/// model element in turn, cycling.
fn comments(g: &Generated, n: usize) -> Vec<ModelOp> {
    let uml = Uml::new(D2);
    g.elements
        .iter()
        .filter(|element| {
            matches!(
                element.kind,
                Kind::Model
                    | Kind::Package
                    | Kind::PrimitiveType
                    | Kind::Class
                    | Kind::Property
                    | Kind::Association
            )
        })
        .cycle()
        .take(n)
        .map(|element| {
            insert(
                &uml,
                &element.path,
                &Step {
                    feature: "ownedComments",
                    pos: 0,
                    class: "Comment",
                },
            )
        })
        .collect()
}

/// `b` writes `ops` into `m'` in one batch; answers how long it took.
fn write_on_b(
    cluster: &Cluster,
    client: &reqwest::blocking::Client,
    to: &str,
    ops: &[ModelOp],
) -> Result<Duration, String> {
    let started = Instant::now();
    let (status, answer) = cluster.post_with(
        client,
        B,
        &format!("/api/model/{to}/ops"),
        json!({ "ops": ops }).to_string(),
    )?;
    if status != 200 || answer["applied"] != json!(ops.len()) {
        return Err(format!(
            "b's batch of {} answered {status}: {answer}",
            ops.len()
        ));
    }
    Ok(started.elapsed())
}

/// How long after `since` `a` answers three reads in a row, each within a
/// second: how long it stayed busy. At most fifteen minutes.
fn a_busy_until_idle(cluster: &Cluster, since: Instant) -> Duration {
    let mut fast = 0;
    loop {
        let asked = Instant::now();
        if cluster.read(A, "/api/models").is_ok() && asked.elapsed() < Duration::from_secs(1) {
            fast += 1;
            if fast == 3 {
                return since.elapsed();
            }
        } else {
            fast = 0;
        }
        if since.elapsed() > Duration::from_secs(900) {
            return since.elapsed();
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The size of the next window: `n` scaled from `busy` to
/// [`WINDOW_TARGET`], within one and [`WINDOW_MAX_FACTOR`] calibration
/// batches.
fn window_size(n: usize, busy: Duration) -> usize {
    let scaled = (n as f64) * WINDOW_TARGET.as_secs_f64() / busy.as_secs_f64().max(0.5);
    (scaled as usize).clamp(CALIBRATION_OPS, WINDOW_MAX_FACTOR * CALIBRATION_OPS)
}

/// One window: `b` writes `n` comment insertions into `m'`; the moment it
/// answers, `calls` go to `a` one after the other; then the wait until `a`
/// is idle. Answers each call's status, body and time, and how long `a`
/// stayed busy after `b` answered.
#[allow(clippy::type_complexity)]
fn window(
    cluster: &Cluster,
    client: &reqwest::blocking::Client,
    g: &Generated,
    to: &str,
    n: usize,
    calls: &[(&'static str, String, String)],
) -> Result<(Vec<(&'static str, u16, Value, u64)>, Duration, Duration), String> {
    let on_b = write_on_b(cluster, client, to, &comments(g, n))?;
    let answered = Instant::now();
    let mut answers = Vec::new();
    for (name, path, body) in calls {
        let asked = Instant::now();
        let (status, body) = cluster
            .post(A, path, body.clone())
            .unwrap_or_else(|why| (0, Value::String(why)));
        answers.push((*name, status, body, asked.elapsed().as_millis() as u64));
    }
    let busy = a_busy_until_idle(cluster, answered);
    Ok((answers, on_b, busy))
}

/// M13, the probe, last in a run so that its writes touch no other check.
/// `b` writes a calibration batch of comment insertions into `m'`, and the
/// time `a` then stays busy applying them is measured; then two windows,
/// each a batch on `b` sized from the window before it to keep `a` busy
/// about [`WINDOW_TARGET`]: the moment `b` answers, `a` receives the events
/// and applies them in one pass of its loop (it drains every frame it holds
/// before it reads a command), and two calls go to `a`, one after the other
/// (its HTTP thread serves one request at a time): P1 and P2 in the first
/// window, P3 and P4 in the second.
fn probe(cluster: &Cluster, g: &Generated, m_small: &str, to: &str, run: &mut Run) {
    let models_before = match cluster.models(A) {
        Ok(models) => models,
        Err(why) => {
            run.check(
                "M13 probe",
                Err(format!("a's models before the probe: {why}")),
            );
            return;
        }
    };
    let long = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(900))
        .build()
        .expect("a client");
    let fresh_digest = digest(D2_REMOVE);
    let first: Vec<(&'static str, String, String)> = vec![
        (
            "P1 migrate",
            format!("/api/model/{m_small}/migrate"),
            json!({ "metamodel_id": digest(D2) }).to_string(),
        ),
        (
            "P2 create",
            "/api/models".into(),
            json!({ "metamodel_id": digest(D1) }).to_string(),
        ),
    ];
    let second: Vec<(&'static str, String, String)> = vec![
        (
            "P3 descriptor",
            "/api/metamodels".into(),
            D2_REMOVE.to_string(),
        ),
        // `si13-b`, the direct peer: a pause of a relayed peer does not
        // survive a re-dial of the relay session (`composite.rs`,
        // `ensure_relay_session`, re-routes the old session's peers but not
        // their pause), which at the base happens after every frame over the
        // relay's bound, and would hide a pause carried out after a timeout.
        ("P4 pause", "/api/pause/si13-b".into(), String::new()),
    ];
    let windows = (|| {
        let (_, cal_on_b, cal_busy) = window(cluster, &long, g, to, CALIBRATION_OPS, &[])?;
        let n1 = window_size(CALIBRATION_OPS, cal_busy);
        let (mut answers, on_b1, busy1) = window(cluster, &long, g, to, n1, &first)?;
        let n2 = window_size(n1, busy1);
        let (more, on_b2, busy2) = window(cluster, &long, g, to, n2, &second)?;
        answers.extend(more);
        Ok::<_, String>((
            answers,
            json!({
                "calibration": {"ops": CALIBRATION_OPS, "on_b_ms": cal_on_b.as_millis() as u64, "a_busy_ms": cal_busy.as_millis() as u64},
                "window_1": {"ops": n1, "on_b_ms": on_b1.as_millis() as u64, "a_busy_ms": busy1.as_millis() as u64},
                "window_2": {"ops": n2, "on_b_ms": on_b2.as_millis() as u64, "a_busy_ms": busy2.as_millis() as u64},
            }),
        ))
    })();
    let answers = match windows {
        Ok((answers, sizes)) => {
            run.measure("probe_windows", sizes);
            answers
        }
        Err(why) => {
            run.check("M13 probe", Err(format!("premise: a probe window: {why}")));
            return;
        }
    };
    // Ten seconds more for anything `a` might still carry out.
    std::thread::sleep(Duration::from_secs(10));
    run.measure(
        "probe_answers",
        json!(
            answers
                .iter()
                .map(|(name, status, _, ms)| json!({"call": name, "status": status, "ms": ms}))
                .collect::<Vec<_>>()
        ),
    );
    run.measure(
        "probe_exercised",
        json!(
            answers
                .iter()
                .all(|(_, status, body, _)| timed_out(*status, body))
        ),
    );
    // A call that timed out is answered as not taken; any other 2xx as done.
    let ok = |status: u16| (200..300).contains(&status);
    // P1.
    let (_, p1, body1, _) = &answers[0];
    run.check(
        "M13 P1 migrate",
        (|| {
            let named = {
                let deadline = Instant::now() + Duration::from_secs(20);
                loop {
                    let status = cluster.migration(A, m_small)?;
                    if !status["migrated_to"].is_null() || Instant::now() > deadline {
                        break status["migrated_to"].clone();
                    }
                    std::thread::sleep(Duration::from_millis(500));
                }
            };
            match (timed_out(*p1, body1), named.is_null()) {
                (true, true) => {
                    let answer = cluster.post_op(
                        A,
                        m_small,
                        &leaf(&d1(), &[], "name", LeafOp::InsertChar { pos: 0, ch: 'y' }),
                    )?;
                    if answer["success"] == json!(true) {
                        Ok(())
                    } else {
                        Err(format!("after a 504, m_small refuses an edit: {answer}"))
                    }
                }
                (true, false) => Err(format!(
                    "a answered the migration {p1} and then migrated m_small to {named} (M8, M13)"
                )),
                (false, false) if ok(*p1) => Ok(()),
                (_, _) => Err(format!(
                    "a answered the migration {p1}: {body1}; m_small names {named}"
                )),
            }
        })(),
    );
    // P2, with P1's successor accounted for.
    let (_, p2, body2, _) = &answers[1];
    run.check(
        "M13 P2 create",
        (|| {
            let after = cluster.models(A)?;
            let new: Vec<&String> = after
                .keys()
                .filter(|id| !models_before.contains_key(*id))
                .collect();
            let from_p1 = usize::from(!cluster.migration(A, m_small)?["migrated_to"].is_null());
            let expected = from_p1 + usize::from(ok(*p2) && !timed_out(*p2, body2));
            if new.len() == expected {
                Ok(())
            } else {
                Err(format!(
                    "a answered the create {p2} and now hosts {} model(s) it did not host before \
                     the probe ({new:?}); {expected} expected",
                    new.len()
                ))
            }
        })(),
    );
    // P3.
    let (_, p3, body3, _) = &answers[2];
    run.check(
        "M13 P3 descriptor",
        (|| {
            let served = cluster
                .read(A, "/api/metamodels")?
                .to_string()
                .contains(&fresh_digest);
            match (timed_out(*p3, body3), served) {
                (true, false) => Ok(()),
                (true, true) => Err(format!("a answered the descriptor {p3} and then served it")),
                (false, true) if ok(*p3) => Ok(()),
                (_, served) => Err(format!(
                    "a answered the descriptor {p3}: {body3}; served: {served}"
                )),
            }
        })(),
    );
    // P4.
    let (_, p4, body4, _) = &answers[3];
    run.check(
        "M13 P4 pause",
        (|| {
            let paused =
                cluster.peer_status(A)?.get("si13-b").map(String::as_str) == Some("Paused");
            if paused {
                let _ = cluster.control(A, "/api/resume/si13-b");
            }
            match (timed_out(*p4, body4), paused) {
                (true, false) => Ok(()),
                (true, true) => Err(format!(
                    "a answered the pause {p4} {body4} (its timeout answer) and then paused si13-b"
                )),
                (false, true) if ok(*p4) => Ok(()),
                (_, paused) => Err(format!(
                    "a answered the pause {p4}: {body4}; paused: {paused}"
                )),
            }
        })(),
    );
}

/// K7: on `node`, in `model`, `Class000_1` is named `Class000_1` and not
/// abstract, and `Class000_0` holds no `persistent`.
fn untouched(cluster: &Cluster, g: &Generated, node: usize, model: &str) -> Result<(), String> {
    let (first, second) = g.first_classes();
    let doc = cluster.identified(node, model)?;
    let renamed = at_path(&doc, &path_of(g, second));
    if renamed["name"] != json!("Class000_1") {
        return Err(format!(
            "{} serves {} for the name of Class000_1 in {model}",
            cluster.nodes[node].id, renamed["name"]
        ));
    }
    if renamed["abstract"] == json!(true) {
        return Err(format!(
            "{} serves b's concurrent edit in {model}",
            cluster.nodes[node].id
        ));
    }
    let stereotyped = at_path(&doc, &path_of(g, first));
    if stereotyped["stereotype"]
        .as_array()
        .is_some_and(|items| items.contains(&json!("persistent")))
    {
        return Err(format!(
            "{} serves c's concurrent edit in {model}",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

fn one_run(g: &Generated, case: Case, rep: usize) -> Run {
    let mut run = Run {
        size: g.size,
        case,
        rep,
        checks: Vec::new(),
        measures: serde_json::Map::new(),
        setup: None,
    };
    let ceiling = Some(Instant::now() + RUN_CEILING);
    let started = Instant::now();
    let mut cluster = match Cluster::start(
        &format!("scale-{}-{rep}", case.name()),
        LOG_ID,
        &three(),
        ceiling,
    ) {
        Ok(cluster) => cluster,
        Err(why) => {
            run.setup = Some(format!("the cluster: {why}"));
            return run;
        }
    };
    let set_up = (|| -> Result<_, String> {
        cluster.await_routes()?;
        let m_small = small_model(&cluster)?;
        let m = write_m(&cluster, g, &mut run)?;
        if case == Case::Concurrent {
            cluster.pause_all()?;
            concurrent_edits(&cluster, g, &m)?;
        }
        let doc = (cluster.identified(A, &m)?, cluster.plain(A, &m)?);
        Ok((m_small, m, doc))
    })();
    let (m_small, m, doc) = match set_up {
        Ok(done) => done,
        Err(why) => {
            run.setup = Some(why);
            run.measure("node_log_tails", json!(cluster.tails()));
            return run;
        }
    };
    run.measure(
        "cut_reads_bytes",
        json!(doc.0.to_string().len() + doc.1.to_string().len()),
    );
    let Some((to, status)) = migrate_on_a(&cluster, &m, &mut run) else {
        run.check("M8", Err("no successor to follow".into()));
        run.measure("node_log_tails", json!(cluster.tails()));
        return run;
    };
    // The first read of a's status after the answer, K2's, is one of M9's
    // reads "from the answer until n == N" and is timed against the same
    // bound (SI.13 round 2, OB15: round 1 left it untimed, and a single loop
    // step after the answer hid behind it).
    let asked = Instant::now();
    let first_read = cluster.migration_raw(A, &m);
    let first_took = asked.elapsed();
    run.measure("first_read_ms", json!(first_took.as_secs_f64() * 1000.0));
    let first_timed = match &first_read {
        Ok(_) if first_took > ANSWER_BOUND => Err(format!(
            "the first read of a's status after the answer took {} ms (M9)",
            first_took.as_millis()
        )),
        Ok(_) => Ok(()),
        Err(why) => Err(format!(
            "the first read of a's status after the answer failed after {} ms (M9): {why}",
            first_took.as_millis()
        )),
    };
    run.check(
        "K2",
        (|| {
            let got = status13(&first_read.clone()?);
            let want = expected13(Some(&to), &[], &[]);
            if got != want {
                return Err(format!("a reads {got}, not {want}"));
            }
            frozen(&cluster, A, &m, &doc)?;
            let answer = cluster.post_op(A, &m, &probe_edit())?;
            if answer["success"] != json!(false)
                || !answer["message"].as_str().unwrap_or_default().contains(&to)
            {
                return Err(format!("a answered an edit of m with {answer}"));
            }
            if case == Case::Concurrent {
                let b = cluster.migration(B, &m)?;
                if b != expected13(None, &[], &[]) {
                    return Err(format!("b, cut off, reads {b}"));
                }
            }
            Ok(())
        })(),
    );
    let m9 = first_timed.and_then(|()| progress(&cluster, &m, &to, status, &mut run));
    run.check(
        "M9",
        m9.and_then(|()| cluster.migration_raw(B, &m).and_then(|raw| no_copy(&raw))),
    );
    if case == Case::Concurrent {
        run.check("K3-resume", cluster.resume_all());
    }
    let refused: Vec<(String, u64)> = match case {
        Case::Plain => Vec::new(),
        Case::Concurrent => vec![
            ("si13-b".into(), 1),
            ("si13-b".into(), 2),
            ("si13-c".into(), 1),
        ],
    };
    let want = expected13(Some(&to), &refused, &[]);
    let k3 = Instant::now();
    run.check(
        "K3",
        cluster
            .until(
                "every node names m' and lists the concurrent edits (K3)",
                |node| {
                    let got = cluster.migration(node, &m)?;
                    if got["migrated_to"] == want["migrated_to"]
                        && got["refused"] == want["refused"]
                    {
                        Ok(())
                    } else {
                        Err(format!(
                            "{} reads {got}, not {want}",
                            cluster.nodes[node].id
                        ))
                    }
                },
            )
            .and_then(|()| {
                (0..3).try_for_each(|node| {
                    let got = cluster.migration(node, &m)?;
                    if got == want {
                        Ok(())
                    } else {
                        Err(format!(
                            "{} reads {got}, not {want}",
                            cluster.nodes[node].id
                        ))
                    }
                })
            }),
    );
    run.ms("k3_ms", k3);
    run.check(
        "K4",
        (0..3).try_for_each(|node| frozen(&cluster, node, &m, &doc)),
    );
    run.check(
        "K5",
        (0..3).try_for_each(|node| refusals(&cluster, node, &m, &to, true)),
    );
    let k6 = Instant::now();
    let expected = to_d2(&g.expected_d1());
    run.check(
        "K6",
        (|| {
            cluster.join_by_id(B, &to)?;
            cluster.join_by_id(C, &to)?;
            let first = cluster.identified(A, &to)?;
            if projected(&first)? != expected {
                return Err("a's copy is not m's read at the cut mapped to D2".into());
            }
            if objects(&first)? != g.size {
                return Err(format!("a's copy holds {} objects", objects(&first)?));
            }
            cluster.until("every node serves a's copy (K6)", |node| {
                if cluster.identified(node, &to)? != first {
                    return Err(format!(
                        "{} does not serve a's copy yet",
                        cluster.nodes[node].id
                    ));
                }
                if project_with(&cluster.plain(node, &to)?, &labels(&first)) != strip_ids(&expected)
                {
                    return Err(format!("{}'s default read of m'", cluster.nodes[node].id));
                }
                cluster.lists_under_d2(node, &to)
            })
        })(),
    );
    run.ms("k6_ms", k6);
    if case == Case::Concurrent {
        run.check(
            "K7",
            (0..3).try_for_each(|node| {
                untouched(&cluster, g, node, &m)?;
                untouched(&cluster, g, node, &to)
            }),
        );
    }
    run.check("M10", frames(&cluster));
    // d, relay-only, after everything above.
    let joined = cluster
        .add(&[Spec::new("si13-d", &[], &[("d1", D1)])])
        .and_then(|()| cluster.await_routes());
    run.check("M12 topology", joined.clone());
    if joined.is_ok() {
        let d = cluster.index("si13-d");
        let j = Instant::now();
        run.check(
            "M12 m'",
            (|| {
                cluster.join_by_id(d, &to)?;
                let first = cluster.identified(A, &to)?;
                cluster.until_on(&[d], CONVERGE_TIMEOUT, "d serves a's copy", |node| {
                    if cluster.identified(node, &to)? != first {
                        return Err("d does not serve a's copy yet".into());
                    }
                    cluster.lists_under_d2(node, &to)
                })
            })(),
        );
        run.ms("d_copy_ms", j);
        let j = Instant::now();
        run.check(
            "M12 m",
            (|| {
                cluster.join_by_id(d, &m)?;
                cluster.until_on(&[d], CONVERGE_TIMEOUT, "d serves the cut", |node| {
                    let got = cluster.migration(node, &m)?;
                    if got["migrated_to"] != json!(to) {
                        return Err(format!("d reads {got}"));
                    }
                    frozen(&cluster, node, &m, &doc)
                })?;
                let got = cluster.migration(d, &m)?;
                if got["superseded"] != json!([]) {
                    return Err(format!("d reads {got}; it must supersede nothing"));
                }
                refusals(&cluster, d, &m, &to, false)
            })(),
        );
        run.ms("d_cut_ms", j);
        run.check("M10 after d", frames(&cluster));
    }
    probe(&cluster, g, &m_small, &to, &mut run);
    let mut transfers = BTreeMap::new();
    for node in 0..cluster.nodes.len() {
        for model in [&m, &to] {
            if let Some((bytes, how)) = cluster.transfer_line(node, model) {
                transfers.insert(
                    format!(
                        "{}:{}",
                        cluster.nodes[node].id,
                        if *model == m { "m" } else { "m'" }
                    ),
                    json!({"bytes": bytes, "how": how}),
                );
            }
        }
    }
    run.measure("state_transfer_lines", json!(transfers));
    run.measure(
        "relay_health",
        cluster.relay_health().unwrap_or(Value::Null),
    );
    run.measure("stopped_at_ceiling", json!(cluster.stopped.get()));
    run.ms("run_ms", started);
    if !run.held() {
        run.measure("node_log_tails", json!(cluster.tails()));
    }
    run
}

#[test]
#[ignore = "SI.13's 5,000-object runs: four clusters, up to an hour each; run with --ignored"]
fn si13_scale_copy_migration_of_a_5000_object_model_through_a_relay() {
    let reps = std::env::var("SI13_SCALE_REPS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| (1..=REPS).contains(n));
    let cases: Option<Vec<Case>> = std::env::var("SI13_SCALE_CASES").ok().map(|raw| {
        raw.split(',')
            .filter_map(|name| match name.trim() {
                "plain" => Some(Case::Plain),
                "concurrent" => Some(Case::Concurrent),
                _ => None,
            })
            .collect()
    });
    let size = std::env::var("SI13_SCALE_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n >= 100);
    let registered = reps.is_none() && cases.is_none() && size.is_none();
    let size = size.unwrap_or(SIZE);
    let reps = reps.unwrap_or(REPS);
    let cases = cases.unwrap_or(vec![Case::Plain, Case::Concurrent]);
    println!(
        "SI13-SCALE-SCHEDULE {}",
        json!({"size": size, "reps": reps, "cases": cases.iter().map(|c| c.name()).collect::<Vec<_>>(), "registered": registered})
    );
    let g = generate(size);
    println!(
        "SI13-SCALE-MODEL {}",
        json!({"objects": g.elements.len(), "census": g.census(), "operations_under_d1": g.structure(&d1()).len()})
    );
    let mut runs = Vec::new();
    for rep in 0..reps {
        for case in &cases {
            let run = one_run(&g, *case, rep);
            println!("SI13-SCALE-RUN {}", run.line());
            runs.push(run);
        }
    }
    let held = runs.iter().filter(|run| run.held()).count();
    let exercised = runs
        .iter()
        .filter(|run| run.measures.get("probe_exercised") == Some(&json!(true)))
        .count();
    println!(
        "SI13-SCALE-RESULT {}",
        json!({"runs": runs.len(), "completed": runs.iter().filter(|r| r.setup.is_none()).count(), "runs_every_check": held, "probe_exercised": exercised, "registered": registered})
    );
    assert_eq!(
        held,
        runs.len(),
        "SI.13 at 5,000 objects: {} of {} runs held every check",
        held,
        runs.len()
    );
}

/// A guard that the restated generator is SI.12's: `generate(5000)` makes
/// the 5,000 objects and the 58,060 operations SI.12 round 4 posted
/// (`results/validation/SI.12/round-4-5000-lines.txt`, `m_ops_posted`).
#[test]
fn si13_scale_premise_the_generator_is_si12s() {
    let g = generate(SIZE);
    assert_eq!(g.elements.len(), SIZE, "premise: 5,000 objects");
    let uml = d1();
    let structure = g.structure(&uml).len();
    // Links are one per identity reference; their count does not depend on
    // the served identities, so a placeholder map stands in for them.
    let ids: BTreeMap<String, String> = g
        .elements
        .iter()
        .enumerate()
        .map(|(index, _)| (g.label(index), format!("{index:032x}")))
        .collect();
    let links = g.links(&uml, &ids).expect("every label mapped").len();
    assert_eq!(
        structure + links,
        58_060,
        "premise: SI.12 round 4 posted 58,060 operations for generate(5000)"
    );
}
