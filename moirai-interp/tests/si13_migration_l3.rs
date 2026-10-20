//! SI.13 on `model_node` processes: two concurrent migrations named one way
//! on every node whatever order the cuts arrive in (OB2), a migrate call
//! that answers in time and reports its progress (OB6 at the running
//! example's size), a node that joins the model after the cut (OB3), all on
//! a cluster where one node reaches the others only through a `moirai-relay`.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.13.md`; the contract is
//! restated in `si13/mod.rs` (M8 to M13) and `si12/mod.rs` (M1 to M7). The
//! cluster (`si13/cluster.rs`) is a bootnode, a relay and three nodes:
//! `si13-a` and `si13-b` reach each other directly and serve `D1` and `D2`;
//! `si13-c` reaches both only through the relay and serves `D1` alone, so `D2`
//! reaches it only through the copy's history. The test checks that topology
//! on every node's `/api/metrics` before it writes anything. A fourth node,
//! `si13-d`, also relay-only and serving `D1` alone, is started after the cut.
//!
//! Every check is recorded, not asserted on the spot, so one run shows every
//! check's outcome; each test prints `SI13-L3-<TEST>` lines and asserts at
//! the end that every check held.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo build -j 2 -p moirai-relay -p moirai-bootnode
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si13_migration_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! The binaries are `MOIRAI_E2E_INTERP_NODE_BIN`, `MOIRAI_E2E_RELAY_BIN` and
//! `MOIRAI_E2E_BOOTNODE_BIN`, or the workspace's debug builds. A missing one
//! fails the test: a skip would read as a pass.

mod si13;

use std::time::{Duration, Instant};

use moirai_interp::ModelOp;
use moirai_protocol::log_id::LogId;
use serde_json::{Value, json};

use si13::cluster::{CONVERGE_TIMEOUT, Cluster, Spec};
use si13::si12::{
    CAR, D1, D2, Uml, digest, expected_d1, expected_d2, ids_by_label, labels, project,
    project_with, strip_ids,
};
use si13::{
    ANSWER_BOUND, CutInfo, copy_progress, events, expected13, no_copy, status13, unseen, winner,
};

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;

/// Every check of one test, in order, and what it found.
#[derive(Default)]
struct Checks {
    list: Vec<(String, Result<(), String>)>,
    measures: serde_json::Map<String, Value>,
}

impl Checks {
    fn check(&mut self, name: &str, result: Result<(), String>) {
        if let Err(why) = &result {
            println!(
                "SI13-L3-CHECK {}",
                json!({"check": name, "held": false, "why": why})
            );
        } else {
            println!("SI13-L3-CHECK {}", json!({"check": name, "held": true}));
        }
        self.list.push((name.to_string(), result));
    }

    fn measure(&mut self, key: &str, value: Value) {
        self.measures.insert(key.to_string(), value);
    }

    /// Print the summary and fail the test if any check did not hold.
    fn finish(self, test: &str) {
        let failed: Vec<Value> = self
            .list
            .iter()
            .filter_map(|(name, result)| {
                result
                    .as_ref()
                    .err()
                    .map(|why| json!({"check": name, "why": why}))
            })
            .collect();
        println!(
            "SI13-L3-RESULT {}",
            json!({
                "test": test,
                "checks": self.list.len(),
                "held": self.list.len() - failed.len(),
                "failed": failed,
                "measures": self.measures,
            })
        );
        assert!(
            failed.is_empty(),
            "SI.13 {test}: {} of {} checks failed: {}",
            failed.len(),
            self.list.len(),
            serde_json::to_string_pretty(&failed).unwrap_or_default()
        );
    }
}

fn three() -> Vec<Spec> {
    vec![
        Spec::new("si13-a", &["si13-b"], &[("d1", D1), ("d2", D2)]),
        Spec::new("si13-b", &["si13-a"], &[("d1", D1), ("d2", D2)]),
        Spec::new("si13-c", &[], &[("d1", D1)]),
    ]
}

/// `m` created on `a` under `D1`, joined by the other nodes named, the
/// running example written on `a`, and every one of them serving it.
/// Answers `m` and `k`, the number of `a`'s events on `m`.
fn running_example(cluster: &Cluster, joiners: &[usize]) -> Result<(String, u64), String> {
    let uml = Uml::new(D1);
    let m = cluster.register(A, None, &digest(D1))?;
    for node in joiners {
        cluster.register(*node, Some(&m), &digest(D1))?;
    }
    let mut on: Vec<usize> = vec![A];
    on.extend_from_slice(joiners);
    cluster.until_on(
        &on,
        CONVERGE_TIMEOUT,
        "m's Install everywhere",
        |node| match cluster.metrics(node, &m)?["delivered_ops"].as_u64() {
            Some(n) if n >= 1 => Ok(()),
            other => Err(format!("node {node} delivered {other:?}")),
        },
    )?;
    let structure = uml.structure();
    cluster.submit_batches(A, &m, &structure)?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    let links = uml.links(&ids);
    cluster.submit_batches(A, &m, &links)?;
    cluster.until_on(
        &on,
        CONVERGE_TIMEOUT,
        "the running example everywhere",
        |node| {
            let doc = cluster.identified(node, &m)?;
            if project(&doc) != expected_d1() {
                return Err(format!(
                    "node {node} does not serve the running example yet"
                ));
            }
            Ok(())
        },
    )?;
    let k = cluster.metrics(A, &m)?["ops_applied"]
        .as_u64()
        .ok_or("a reports no ops_applied")?;
    let posted = 1 + (structure.len() + links.len()) as u64;
    if k != posted {
        return Err(format!(
            "premise: a applied {k} operations on m, the Install and {} posted ones make {posted}",
            posted - 1
        ));
    }
    Ok((m, k))
}

/// M8: the migrate call on `node`, timed. Answers the status, the body and
/// the time it took.
fn migrate_call(cluster: &Cluster, node: usize, m: &str) -> (u16, Value, Duration) {
    let started = Instant::now();
    let (status, body) = cluster
        .post(
            node,
            &format!("/api/model/{m}/migrate"),
            json!({ "metamodel_id": digest(D2) }).to_string(),
        )
        .unwrap_or_else(|why| (0, Value::String(why)));
    (status, body, started.elapsed())
}

/// M8 on one answer: within the bound, 201 or 202, a model id other than
/// `m`. Answers the successor.
fn answered(m: &str, status: u16, body: &Value, took: Duration) -> Result<String, String> {
    if took > ANSWER_BOUND {
        return Err(format!(
            "the migrate call took {} ms, over the {} ms bound (M8); it answered {status}: {body}",
            took.as_millis(),
            ANSWER_BOUND.as_millis()
        ));
    }
    if status != 201 && status != 202 {
        return Err(format!(
            "the migrate call answered {status}, not 201 or 202 (M8): {body}"
        ));
    }
    match body["model_id"].as_str() {
        Some(id) if id != m && LogId::parse(id).is_ok() => Ok(id.to_string()),
        other => Err(format!(
            "the migrate call answered {status} naming {other:?}, not a model id other than m: \
             {body}"
        )),
    }
}

/// Which refusals a node is probed for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    /// An edit, and a further migration when the node serves `D2` (a node
    /// that does not is refused the descriptor first, which says nothing
    /// about the cut; SI.12 probed the further migration on a node serving
    /// `D2`).
    EditAndMigration,
    /// An edit only: what claim part 4 names for a joiner.
    Edit,
}

/// M3 over HTTP: an edit of `m` refused on the single route naming `to`
/// and with 422 on the batch route, and, as `probe` says, a further
/// migration answered 409 or 422 naming `to`.
fn refusals(cluster: &Cluster, node: usize, m: &str, to: &str, probe: Probe) -> Result<(), String> {
    let op: ModelOp = Uml::new(D1).name_char(CAR, 3, 's');
    let answer = cluster.post_op(node, m, &op)?;
    let message = answer["message"].as_str().unwrap_or_default();
    if answer["success"] != Value::Bool(false) || !message.contains(to) {
        return Err(format!(
            "{} answered an edit of the cut model with {answer}; it must refuse it naming {to} (M3)",
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
            "{} answered a batch on the cut model with {status}: {answer}; it must refuse it (422)",
            cluster.nodes[node].id
        ));
    }
    if probe == Probe::Edit || !cluster.serves(node, D2)? {
        return Ok(());
    }
    let (status, body, _) = migrate_call(cluster, node, m);
    if !(status == 409 || status == 422) || !body.to_string().contains(to) {
        return Err(format!(
            "{} answered a further migration of the cut model with {status}: {body}; it must \
             refuse it (409 or 422) naming {to} (M3)",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

/// Both reads of `m` on `node` equal `doc`.
fn frozen(cluster: &Cluster, node: usize, m: &str, doc: &(Value, Value)) -> Result<(), String> {
    let identified = cluster.identified(node, m)?;
    if identified != doc.0 {
        return Err(format!(
            "{} serves an identified read of the cut model other than the cut's (M4): {identified}",
            cluster.nodes[node].id
        ));
    }
    let plain = cluster.plain(node, m)?;
    if plain != doc.1 {
        return Err(format!(
            "{} serves a default read of the cut model other than the cut's (M4): {plain}",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

/// Every node of the cluster names `want`'s successor and lists `want`'s
/// refused events within the convergence bound, which is what converging
/// means here; then each reads exactly `want` (`superseded` included), read
/// once, so that a status lacking a key fails at once rather than at the
/// bound.
fn settled(cluster: &Cluster, m: &str, want: &Value) -> Result<(), String> {
    cluster.until(
        "every node names the successor and lists the refused events",
        |node| {
            let got = cluster.migration(node, m)?;
            if got["migrated_to"] == want["migrated_to"] && got["refused"] == want["refused"] {
                Ok(())
            } else {
                Err(format!(
                    "{} reads {got}, not {want}",
                    cluster.nodes[node].id
                ))
            }
        },
    )?;
    for node in 0..cluster.nodes.len() {
        let got = cluster.migration(node, m)?;
        if got != *want {
            return Err(format!(
                "{} reads {got}, not {want}",
                cluster.nodes[node].id
            ));
        }
    }
    Ok(())
}

/// The two schedules of OB2, as in process (`si13_concurrent_migrations.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    /// `a` and `b` cut with nothing of their own before: equal stamps, the
    /// least origin, `si13-a`, is kept.
    Tie,
    /// `b` writes two edits and then cuts: `b`'s stamp is greater, `b`'s cut
    /// is kept.
    Unequal,
}

/// Which of the two cuts `c` delivers first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum First {
    A,
    B,
}

/// One OB2 scenario on its own model.
fn concurrent_migrations(cluster: &Cluster, case: Case, first: First, out: &mut Checks) {
    let label = format!("OB2-{case:?}-{first:?}");
    let uml = Uml::new(D1);
    let set_up = (|| -> Result<_, String> {
        let (m, k) = running_example(cluster, &[B, C])?;
        cluster.pause_all()?;
        cluster.submit(C, &m, &[uml.c_edit()])?;
        let e = if case == Case::Unequal {
            cluster.submit(B, &m, &uml.b_edits())?;
            2
        } else {
            0
        };
        let a_doc = (cluster.identified(A, &m)?, cluster.plain(A, &m)?);
        let b_doc = (cluster.identified(B, &m)?, cluster.plain(B, &m)?);
        Ok((m, k, e, a_doc, b_doc))
    })();
    let (m, k, e, a_doc, b_doc) = match set_up {
        Ok(done) => done,
        Err(why) => {
            out.check(&format!("{label}: set-up"), Err(why));
            return;
        }
    };
    // Both migrations, each on a node that has not seen the other's cut.
    let (status, body, took) = migrate_call(cluster, B, &m);
    let to_b = answered(&m, status, &body, took);
    out.check(
        &format!("{label}: b's migrate answer (M8)"),
        to_b.clone().map(|_| ()),
    );
    let (status, body, took) = migrate_call(cluster, A, &m);
    let to_a = answered(&m, status, &body, took);
    out.check(
        &format!("{label}: a's migrate answer (M8)"),
        to_a.clone().map(|_| ()),
    );
    let (Ok(to_a), Ok(to_b)) = (to_a, to_b) else {
        return;
    };
    let cut_a = CutInfo {
        origin: "si13-a".into(),
        seq: k + 1,
        stamp: k + 1,
        to: to_a.clone(),
    };
    let cut_b = CutInfo {
        origin: "si13-b".into(),
        seq: e + 1,
        stamp: k + e + 1,
        to: to_b.clone(),
    };
    let seen_a = events("si13-a", k + 1);
    let mut seen_b = events("si13-a", k);
    seen_b.extend(events("si13-b", e + 1));
    out.check(
        &format!("{label}: right after the cuts, each writer names its own and c none"),
        (|| {
            for (node, want) in [
                (A, expected13(Some(&to_a), &[], &[])),
                (B, expected13(Some(&to_b), &[], &[])),
                (C, expected13(None, &[], &[])),
            ] {
                let got = cluster.migration(node, &m)?;
                if got != want {
                    return Err(format!(
                        "{} reads {got}, not {want}",
                        cluster.nodes[node].id
                    ));
                }
            }
            Ok(())
        })(),
    );
    // c delivers one cut first, over the relay.
    let (first_node, first_cut, first_doc) = match first {
        First::A => (A, cut_a.clone(), &a_doc),
        First::B => (B, cut_b.clone(), &b_doc),
    };
    out.check(
        &format!(
            "{label}: c after delivering {}'s cut alone",
            first_cut.origin
        ),
        (|| {
            cluster.resume_link(first_node, C)?;
            let want = expected13(Some(&first_cut.to), &[("si13-c".into(), 1)], &[]);
            cluster.until_on(&[C], CONVERGE_TIMEOUT, "c names the first cut", |node| {
                let got = cluster.migration(node, &m)?;
                if got["migrated_to"] == want["migrated_to"] {
                    Ok(())
                } else {
                    Err(format!("c reads {got}, not {want}"))
                }
            })?;
            let got = cluster.migration(C, &m)?;
            if got != want {
                return Err(format!("c reads {got}, not {want}"));
            }
            frozen(cluster, C, &m, first_doc)
        })(),
    );
    // Then every link.
    let resumed = cluster.resume_all();
    out.check(&format!("{label}: every link resumed"), resumed.clone());
    if resumed.is_err() {
        return;
    }
    let kept = winner(&[cut_a.clone(), cut_b.clone()]);
    let (lost, seen, doc) = if kept == cut_a {
        (&cut_b, &seen_a, &a_doc)
    } else {
        (&cut_a, &seen_b, &b_doc)
    };
    let mut delivered = events("si13-a", k + 1);
    delivered.extend(events("si13-b", e + 1));
    delivered.push(("si13-c".into(), 1));
    let want = expected13(
        Some(&kept.to),
        &unseen(&delivered, seen),
        std::slice::from_ref(&lost.to),
    );
    out.check(
        &format!(
            "{label}: every node names {}'s successor, supersedes the other, lists the same \
             refused events (M11, M5)",
            kept.origin
        ),
        settled(cluster, &m, &want),
    );
    out.check(
        &format!(
            "{label}: every node serves {}'s pre-cut document (M11, M4)",
            kept.origin
        ),
        (0..3).try_for_each(|node| frozen(cluster, node, &m, doc)),
    );
    out.check(
        &format!(
            "{label}: every node refuses naming {}'s successor (M11, M3)",
            kept.origin
        ),
        (0..3).try_for_each(|node| refusals(cluster, node, &m, &kept.to, Probe::EditAndMigration)),
    );
    out.measure(
        &label,
        json!({"m": m, "k": k, "kept": kept.origin, "to_a": to_a, "to_b": to_b}),
    );
}

/// OB2 over HTTP: equal and unequal stamps, each with `c` (relayed) taking
/// `a`'s cut first and then `b`'s, and the other way round.
#[test]
fn si13_l3_two_concurrent_migrations_name_one_successor_on_every_node() {
    let mut out = Checks::default();
    let cluster = match Cluster::start("ob2", "513c0de0513c0de0513c0de0513c0002", &three(), None) {
        Ok(cluster) => cluster,
        Err(why) => panic!("SI.13 (set-up): the cluster: {why}"),
    };
    out.check(
        "the topology: a-b direct, c relayed to both",
        cluster.await_routes(),
    );
    for case in [Case::Tie, Case::Unequal] {
        for first in [First::A, First::B] {
            concurrent_migrations(&cluster, case, first, &mut out);
        }
    }
    out.check("M10: the relay read no frame over its bound", {
        let alarms = cluster.relay_alarms();
        let sessions = cluster.relay_session_alarms();
        if alarms.is_empty() && sessions.is_empty() {
            Ok(())
        } else {
            Err(format!("relay: {alarms:?}; nodes: {sessions:?}"))
        }
    });
    out.measure(
        "relay_health",
        cluster.relay_health().unwrap_or(Value::Null),
    );
    out.finish("ob2");
}

/// OB6 at the running example's size, the copy through the relay, and OB3:
/// `a` migrates while cut off from `b` and `c`, which each wrote edits
/// concurrent with the cut; the answer comes within the bound and reports
/// progress (M8, M9); every node then names `m'`, serves the cut and
/// refuses; the copy reaches `c` through the relay; `d`, started after all
/// that and reaching every node through the relay, joins `m` by its id and
/// serves the cut, refuses, names `m'`, and joins `m'` and serves it.
#[test]
fn si13_l3_a_migration_answers_in_time_reports_progress_and_reaches_a_late_joiner() {
    let mut out = Checks::default();
    let mut cluster =
        match Cluster::start("ob3", "513c0de0513c0de0513c0de0513c0003", &three(), None) {
            Ok(cluster) => cluster,
            Err(why) => panic!("SI.13 (set-up): the cluster: {why}"),
        };
    out.check(
        "the topology: a-b direct, c relayed to both",
        cluster.await_routes(),
    );
    let uml = Uml::new(D1);
    let set_up = (|| -> Result<_, String> {
        let (m, k) = running_example(&cluster, &[B, C])?;
        cluster.pause_all()?;
        cluster.submit(B, &m, &uml.b_edits())?;
        cluster.submit(C, &m, &[uml.c_edit()])?;
        let doc = (cluster.identified(A, &m)?, cluster.plain(A, &m)?);
        Ok((m, k, doc))
    })();
    let (m, k, doc) = match set_up {
        Ok(done) => done,
        Err(why) => {
            out.check("set-up", Err(why));
            out.finish("ob3");
            return;
        }
    };
    let (status, body, took) = migrate_call(&cluster, A, &m);
    out.measure("migrate_status", json!(status));
    out.measure("migrate_ms", json!(took.as_millis() as u64));
    let to = answered(&m, status, &body, took);
    out.check("M8: a's migrate answer", to.clone().map(|_| ()));
    let to = match to {
        Ok(to) => to,
        Err(_) => match cluster
            .migration(A, &m)
            .ok()
            .and_then(|s| s["migrated_to"].as_str().map(str::to_string))
        {
            Some(to) => to,
            None => {
                out.finish("ob3");
                return;
            }
        },
    };
    // M9: the first read names m' and reports progress, answered within the
    // bound like every read from the answer on (SI.13 round 2, OB15); then
    // progress to the end.
    out.check("M2, M9: right after the answer, a names m' and reports the copy's progress", {
        (|| {
            let asked = Instant::now();
            let raw = cluster.migration_raw(A, &m)?;
            if asked.elapsed() > ANSWER_BOUND {
                return Err(format!(
                    "the first read of a's status after the answer took {} ms (M9)",
                    asked.elapsed().as_millis()
                ));
            }
            let got = status13(&raw);
            let want = expected13(Some(&to), &[], &[]);
            if got != want {
                return Err(format!("a reads {got}, not {want}"));
            }
            let (applied, of) = copy_progress(&raw)?;
            if status == 201 && applied != of {
                return Err(format!(
                    "a answered 201 (the copy whole) and its first read reports {applied} of {of}"
                ));
            }
            Ok(())
        })()
    });
    out.check(
        "M9: a's progress reaches the whole copy, every read answered in time",
        {
            (|| {
                let deadline = Instant::now() + CONVERGE_TIMEOUT;
                let mut last = 0;
                loop {
                    let started = Instant::now();
                    let raw = cluster.migration_raw(A, &m)?;
                    let took = started.elapsed();
                    if took > ANSWER_BOUND {
                        return Err(format!("a status read took {} ms", took.as_millis()));
                    }
                    let (applied, of) = copy_progress(&raw)?;
                    if applied < last {
                        return Err(format!("a's progress went back from {last} to {applied}"));
                    }
                    last = applied;
                    if applied == of {
                        let ops = cluster.metrics(A, &to)?["ops_applied"].as_u64();
                        return if ops == Some(of) {
                            Ok(())
                        } else {
                            Err(format!(
                                "a reports the copy whole at {of} operations and its metrics of m' \
                             report ops_applied {ops:?}"
                            ))
                        };
                    }
                    if Instant::now() > deadline {
                        return Err(format!("a's progress stopped at {applied} of {of}"));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            })()
        },
    );
    out.check(
        "M4, M3 on a at once: the cut's document, an edit refused",
        {
            frozen(&cluster, A, &m, &doc)
                .and_then(|()| refusals(&cluster, A, &m, &to, Probe::EditAndMigration))
        },
    );
    out.check("M9: b, which planned no copy, reports none", {
        cluster.migration_raw(B, &m).and_then(|raw| no_copy(&raw))
    });
    let resumed = cluster.resume_all();
    out.check("every link resumed", resumed.clone());
    // Every node: M2, M5, M4, M3.
    let refused: Vec<(String, u64)> = vec![
        ("si13-b".into(), 1),
        ("si13-b".into(), 2),
        ("si13-c".into(), 1),
    ];
    let want = expected13(Some(&to), &refused, &[]);
    out.check(
        "M2, M5: every node names m' and lists b's two edits and c's one",
        settled(&cluster, &m, &want),
    );
    out.check(
        "M4, M3: every node serves the cut's document and refuses naming m'",
        (0..3).try_for_each(|node| {
            frozen(&cluster, node, &m, &doc)
                .and_then(|()| refusals(&cluster, node, &m, &to, Probe::EditAndMigration))
        }),
    );
    // The copy on every node, c through the relay.
    out.check(
        "M6 (SI.12 claim B): b and c join m' by id and every node serves the running example \
         under D2",
        (|| {
            cluster.join_by_id(B, &to)?;
            cluster.join_by_id(C, &to)?;
            cluster.until("every node serves m'", |node| {
                let copy = cluster.identified(node, &to)?;
                if project(&copy) != expected_d2() {
                    return Err(format!(
                        "{} does not serve the copy yet",
                        cluster.nodes[node].id
                    ));
                }
                if project_with(&cluster.plain(node, &to)?, &labels(&copy))
                    != strip_ids(&expected_d2())
                {
                    return Err(format!("{}'s default read of m'", cluster.nodes[node].id));
                }
                cluster.lists_under_d2(node, &to)
            })
        })(),
    );
    // d, after the cut, reaching every node through the relay.
    let joined = cluster
        .add(&[Spec::new("si13-d", &[], &[("d1", D1)])])
        .and_then(|()| cluster.await_routes());
    out.check(
        "the topology with d: d relayed to every node",
        joined.clone(),
    );
    if joined.is_ok() {
        let d = cluster.index("si13-d");
        out.check(
            "M12 (OB3): d joins m by its id and names m', supersedes nothing, serves the cut, \
             refuses an edit",
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
                refusals(&cluster, d, &m, &to, Probe::Edit)
            })(),
        );
        out.measure(
            "d_refused",
            cluster
                .migration(d, &m)
                .map(|s| s["refused"].clone())
                .unwrap_or(Value::Null),
        );
        out.check(
            "M12: d joins m' by its id and serves a's copy",
            (|| {
                cluster.join_by_id(d, &to)?;
                let first = cluster.identified(A, &to)?;
                cluster.until_on(&[d], CONVERGE_TIMEOUT, "d serves m'", |node| {
                    if cluster.identified(node, &to)? != first {
                        return Err("d does not serve a's copy yet".into());
                    }
                    cluster.lists_under_d2(node, &to)
                })
            })(),
        );
    }
    out.check(
        "M10: the relay read no frame over its bound, no relay session closed",
        {
            let alarms = cluster.relay_alarms();
            let sessions = cluster.relay_session_alarms();
            if alarms.is_empty() && sessions.is_empty() {
                Ok(())
            } else {
                Err(format!("relay: {alarms:?}; nodes: {sessions:?}"))
            }
        },
    );
    out.measure("k", json!(k));
    out.measure(
        "relay_health",
        cluster.relay_health().unwrap_or(Value::Null),
    );
    out.finish("ob3");
}
