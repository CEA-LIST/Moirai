//! SI.13 in process: two migrations of one model made before either replica
//! had the other's cut (OB2), in every delivery order, and replicas that
//! join the model after the cuts (OB3), on real `ModelLog` replicas.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.13.md`; the contract is
//! restated in `si13/mod.rs` (M8 to M13) and `si12/mod.rs` (M1 to M7). Every
//! read is the one a node serves: `QueryableLog::query_identified_state_json`
//! and `QueryableLog::query_state_json`.
//!
//! What each check expects is computed from the schedule the test ran and
//! the rule M11 registers, never read off the implementation: the oracle
//! knows each cut's origin, its sequence number and the events its causal
//! past holds (it sent them), so it knows each cut's Lamport stamp, which
//! cut M11 keeps at every step on every replica, which events that cut had
//! not seen, and which document the replica that wrote it served when it
//! cut.
//!
//! The replicas are `a`, `b`, `c` and `j`, every one bootstrapped with the
//! same four members; each interns itself first, so their member tables
//! order the members differently (`a`: a, b, c, j; `b`: b, a, c, j; ...),
//! and a rule that read a cut's origin by its index in the table would pick
//! differently on `a` and on `b`. `j` delivers nothing until it joins.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si13_concurrent_migrations -- --test-threads=1
//! ```

mod si13;

use moirai_interp::migrate::migrate;
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_network::state_transfer::TransferableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::log_id::LogId;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::Value;

use si13::si12::{CAR, D1, D2, Uml, expected_d1, ids_by_label, install, interleavings, project};
use si13::{CutInfo, events, expected13, status13, unseen, winner};

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;
type Msg = EventMessage<ModelOp>;

const MEMBERS: [&str; 4] = ["a", "b", "c", "j"];
const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const J: usize = 3;

fn identified(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

fn plain(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

fn send(replica: &mut Rep, op: ModelOp, what: &str) -> Msg {
    replica
        .send(op)
        .unwrap_or_else(|why| panic!("{what}: the local intake refused it: {why}"))
}

/// Four replicas of one model, each with its own member table.
fn replicas() -> Vec<Rep> {
    let log_id = LogId::generate();
    MEMBERS
        .iter()
        .map(|id| Rep::bootstrap_with_log_id(id.to_string(), &MEMBERS, log_id.clone()))
        .collect()
}

/// The running example written on `a` and delivered to `b` and `c`; `j`
/// holds nothing. Answers the replicas, the messages, and `k`, the number of
/// `a`'s events (`a@1` to `a@k`).
fn base() -> (Vec<Rep>, Vec<Msg>, u64) {
    let uml = Uml::new(D1);
    let mut reps = replicas();
    let mut sent = Vec::new();
    let mut write = |reps: &mut Vec<Rep>, op: ModelOp, what: &str| {
        let message = send(&mut reps[A], op, what);
        reps[B].receive(message.clone());
        reps[C].receive(message.clone());
        sent.push(message);
    };
    write(&mut reps, install("si13-m", D1), "the model's Install");
    for op in uml.structure() {
        write(&mut reps, op, "building the running example");
    }
    let ids = ids_by_label(&identified(&reps[A]));
    for op in uml.links(&ids) {
        write(&mut reps, op, "linking the running example");
    }
    for (index, replica) in reps.iter().enumerate().take(3) {
        assert_eq!(
            project(&identified(replica)),
            expected_d1(),
            "premise: replica {} serves the running example under D1",
            MEMBERS[index]
        );
    }
    let k = sent.len() as u64;
    (reps, sent, k)
}

/// The two schedules of OB2. In both, `a` and `b` each migrate `m` before
/// either has the other's cut, and `c` writes one edit before it has either.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    /// Neither `a` nor `b` writes anything before its cut: both cuts saw
    /// `a@1` to `a@k` and themselves, equal stamps `k + 1`, so M11 keeps
    /// the cut of the least origin, `a`'s. The successor names are chosen so
    /// that "the least successor id" would keep `b`'s.
    Tie,
    /// `b` writes two edits (`Door` renamed `Doors`, `Door` made abstract)
    /// and then cuts: its stamp is `k + 3`, `a`'s `k + 1`, so M11 keeps
    /// `b`'s cut, whose origin is the greater. The successor names are
    /// chosen so that "the least successor id" would keep `a`'s.
    Unequal,
}

impl Case {
    fn successors(self) -> (&'static str, &'static str) {
        match self {
            Case::Tie => ("si13-copy-2", "si13-copy-1"),
            Case::Unequal => ("si13-copy-1", "si13-copy-2"),
        }
    }
}

/// One replica's view, as the oracle tracks it from the schedule: the
/// events it has delivered, the cuts among them, and what each cut's writer
/// served when it cut.
#[derive(Clone, Default)]
struct View {
    delivered: Vec<(String, u64)>,
    cuts: Vec<CutInfo>,
}

/// What the test knows about each cut: its facts, the events its causal past
/// holds, and the two reads its writer served when it cut.
#[derive(Clone)]
struct Known {
    info: CutInfo,
    seen: Vec<(String, u64)>,
    identified: Value,
    plain: Value,
}

/// Whether a replica got its history by delivering events or by adopting a
/// donor's state; M5's `refused` is held to the events a replica delivered,
/// which the claim of OB3 does not name for a joiner by state transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Joined {
    Delivered,
    Transferred,
}

/// M11, M2, M3, M4 and M5 on `replica`, which the oracle's `view` describes,
/// key by key: the successor, the two reads, the refusals, the refused
/// events (not for a joiner by state transfer), the superseded copies.
fn check_view(replica: &mut Rep, view: &View, known: &[Known], label: &str, joined: Joined) {
    let status = status13(&replica.state().migration());
    if view.cuts.is_empty() {
        assert_eq!(
            status,
            expected13(None, &[], &[]),
            "{label} (M2): a replica that has delivered no cut names no successor"
        );
        return;
    }
    let kept = winner(&view.cuts);
    let cut = known
        .iter()
        .find(|cut| cut.info == kept)
        .expect("the oracle knows every cut it sent");
    let cuts: Vec<String> = view
        .cuts
        .iter()
        .map(|cut| {
            format!(
                "{}@{} (stamp {}) -> {}",
                cut.origin, cut.seq, cut.stamp, cut.to
            )
        })
        .collect();
    let expected = {
        let superseded: Vec<String> = view
            .cuts
            .iter()
            .filter(|other| **other != kept)
            .map(|other| other.to.clone())
            .collect();
        expected13(
            Some(&kept.to),
            &unseen(&view.delivered, &cut.seen),
            &superseded,
        )
    };
    assert_eq!(
        status["migrated_to"], expected["migrated_to"],
        "{label} (M11): with the cuts {cuts:?} delivered, M11 keeps {}@{} (stamp {}), naming \
         `{}`; the status reads {status}",
        kept.origin, kept.seq, kept.stamp, kept.to
    );
    assert_eq!(
        identified(replica),
        cut.identified,
        "{label} (M11, M4): the identified read must be what `{}` served when it wrote the cut \
         M11 keeps (naming `{}`)",
        kept.origin,
        kept.to
    );
    assert_eq!(
        plain(replica),
        cut.plain,
        "{label} (M11, M4): the default read must be what `{}` served when it wrote the cut M11 \
         keeps",
        kept.origin
    );
    refusals(replica, &kept.to, label);
    if joined == Joined::Delivered {
        assert_eq!(
            status["refused"], expected["refused"],
            "{label} (M11, M5): the refused events must be those delivered that the kept cut \
             {}@{} had not seen; the status reads {status}",
            kept.origin, kept.seq
        );
    }
    assert_eq!(
        status["superseded"], expected["superseded"],
        "{label} (M11): with the cuts {cuts:?} delivered, the status must supersede every cut but \
         {}@{}; it reads {status}",
        kept.origin, kept.seq
    );
}

/// M3 with the successor M11 keeps: a local edit and a second migration
/// refused, each naming it, and nothing written.
fn refusals(replica: &mut Rep, to: &str, label: &str) {
    let uml = Uml::new(D1);
    let before = identified(replica);
    match replica.send(uml.name_char(CAR, 3, 's')) {
        Ok(_) => panic!("{label} (M3): a local edit of the cut model was accepted"),
        Err(why) => assert!(
            why.to_string().contains(to),
            "{label} (M3, M11): a local edit was refused with `{why}`, which does not name the \
             successor M11 keeps, `{to}`"
        ),
    }
    match migrate(replica.state(), "si13-third", D2) {
        Ok(_) => panic!("{label} (M3): a further migration of the cut model was planned"),
        Err(why) => assert!(
            why.to_string().contains(to),
            "{label} (M3, M11): a further migration was refused with `{why}`, which does not name \
             `{to}`"
        ),
    }
    assert_eq!(
        identified(replica),
        before,
        "{label} (M3): a refusal changed the read"
    );
}

/// The facts of `case` once both cuts are written: the replicas, each
/// replica's view so far, the three streams (from `a`, from `b`, from `c`),
/// and the two cuts.
struct Written {
    reps: Vec<Rep>,
    views: Vec<View>,
    base: Vec<Msg>,
    streams: [Vec<(Msg, Option<CutInfo>)>; 3],
    known: Vec<Known>,
}

/// The schedule of `case` up to both cuts, every replica cut off from the
/// others: `c` writes its edit, `b` writes its edits (`Unequal` only) and
/// cuts, `a` cuts.
fn write(case: Case, label: &str) -> Written {
    let uml = Uml::new(D1);
    let (to_a, to_b) = case.successors();
    let (mut reps, base, k) = base();
    let mut views = vec![View::default(); 4];
    for view in views.iter_mut().take(3) {
        view.delivered = events("a", k);
    }
    // c's edit, c@1.
    let c1 = send(&mut reps[C], uml.c_edit(), "c's edit");
    views[C].delivered.push(("c".into(), 1));
    // b's edits, b@1 and b@2, in the unequal case.
    let mut from_b: Vec<(Msg, Option<CutInfo>)> = Vec::new();
    if case == Case::Unequal {
        for op in uml.b_edits() {
            from_b.push((send(&mut reps[B], op, "b's edit"), None));
        }
    }
    let e = from_b.len() as u64;
    views[B].delivered.extend(events("b", e));
    // b cuts, b@(e+1), having seen a@1..a@k and its own edits.
    let b_identified = identified(&reps[B]);
    let b_plain = plain(&reps[B]);
    let plan_b = migrate(reps[B].state(), to_b, D2)
        .unwrap_or_else(|why| panic!("{label} (M1): b's migration was refused: {why}"));
    let cut_b = send(&mut reps[B], plan_b.cut, "b's cut");
    let info_b = CutInfo {
        origin: "b".into(),
        seq: e + 1,
        stamp: k + e + 1,
        to: to_b.into(),
    };
    let mut seen_b = events("a", k);
    seen_b.extend(events("b", e + 1));
    views[B].delivered.push(("b".into(), e + 1));
    views[B].cuts.push(info_b.clone());
    from_b.push((cut_b, Some(info_b.clone())));
    // a cuts, a@(k+1), having seen a@1..a@k.
    let a_identified = identified(&reps[A]);
    let a_plain = plain(&reps[A]);
    let plan_a = migrate(reps[A].state(), to_a, D2)
        .unwrap_or_else(|why| panic!("{label} (M1): a's migration was refused: {why}"));
    let cut_a = send(&mut reps[A], plan_a.cut, "a's cut");
    let info_a = CutInfo {
        origin: "a".into(),
        seq: k + 1,
        stamp: k + 1,
        to: to_a.into(),
    };
    views[A].delivered.push(("a".into(), k + 1));
    views[A].cuts.push(info_a.clone());
    let known = vec![
        Known {
            info: info_a.clone(),
            seen: events("a", k + 1),
            identified: a_identified,
            plain: a_plain,
        },
        Known {
            info: info_b,
            seen: seen_b,
            identified: b_identified,
            plain: b_plain,
        },
    ];
    Written {
        reps,
        views,
        base,
        streams: [vec![(cut_a, Some(info_a))], from_b, vec![(c1, None)]],
        known,
    }
}

/// The origin replica id of a message.
fn origin_of(message: &Msg) -> (String, u64) {
    let id = message.event().id();
    (id.origin_id().to_string(), id.seq() as u64)
}

/// Deliver to `replica` the streams `from` in `order` (each entry the index
/// in `from` of the stream whose next message comes next), checking its view
/// after every step.
fn deliver(written: &mut Written, replica: usize, from: &[usize], order: &[usize], label: &str) {
    let mut next = vec![0usize; from.len()];
    for (step, pick) in order.iter().enumerate() {
        let stream = from[*pick];
        let (message, cut) = written.streams[stream][next[*pick]].clone();
        next[*pick] += 1;
        written.views[replica].delivered.push(origin_of(&message));
        if let Some(cut) = cut {
            written.views[replica].cuts.push(cut);
        }
        written.reps[replica].receive(message);
        let view = written.views[replica].clone();
        check_view(
            &mut written.reps[replica],
            &view,
            &written.known,
            &format!("{label}, {} after step {}", MEMBERS[replica], step + 1),
            Joined::Delivered,
        );
    }
}

/// Every delivery order of `case`: `a` takes `b`'s and `c`'s streams, `b`
/// takes `a`'s and `c`'s, `c` takes `a`'s and `b`'s, each in every
/// interleaving, independently. Answers the number of runs.
fn every_order(case: Case) -> usize {
    let probe = write(case, "premise");
    let lengths: Vec<usize> = probe.streams.iter().map(Vec::len).collect();
    drop(probe);
    let a_orders = interleavings(&[lengths[B], lengths[C]]);
    let b_orders = interleavings(&[lengths[A], lengths[C]]);
    let c_orders = interleavings(&[lengths[A], lengths[B]]);
    let mut runs = 0;
    for a_order in &a_orders {
        for b_order in &b_orders {
            for c_order in &c_orders {
                runs += 1;
                let label =
                    format!("{case:?} run {runs} (a {a_order:?}, b {b_order:?}, c {c_order:?})");
                let mut written = write(case, &label);
                // Right after the cuts, before any delivery: each writer
                // names its own successor, c none.
                for replica in [A, B, C] {
                    let view = written.views[replica].clone();
                    check_view(
                        &mut written.reps[replica],
                        &view,
                        &written.known,
                        &format!("{label}, {} before any delivery", MEMBERS[replica]),
                        Joined::Delivered,
                    );
                }
                deliver(&mut written, A, &[B, C], a_order, &label);
                deliver(&mut written, B, &[A, C], b_order, &label);
                deliver(&mut written, C, &[A, B], c_order, &label);
                // Every replica has every event now: one status, one
                // document, on all three.
                let first = status13(&written.reps[A].state().migration());
                for replica in [B, C] {
                    assert_eq!(
                        status13(&written.reps[replica].state().migration()),
                        first,
                        "{label} (M11): {} and a deliver the same cuts and must read one status",
                        MEMBERS[replica]
                    );
                    assert_eq!(
                        identified(&written.reps[replica]),
                        identified(&written.reps[A]),
                        "{label} (M11, M4): {} and a must serve one frozen document",
                        MEMBERS[replica]
                    );
                }
            }
        }
    }
    runs
}

/// OB2, equal stamps. Expected everywhere at the end: `migrated_to`
/// `si13-copy-2` (`a`'s), `superseded` [`si13-copy-1`], `refused` `b@1`
/// (`b`'s cut) and `c@1`, and `a`'s pre-cut document, in all 8 orders.
#[test]
fn si13_o1_two_concurrent_cuts_with_equal_stamps_name_one_successor_in_every_order() {
    let runs = every_order(Case::Tie);
    assert_eq!(runs, 8, "premise: 2 x 2 x 2 delivery orders");
    println!(
        "SI13-O1: {runs} delivery orders; every replica keeps a's cut (equal stamps, least origin)"
    );
}

/// OB2, unequal stamps. Expected everywhere at the end: `migrated_to`
/// `si13-copy-2` (`b`'s), `superseded` [`si13-copy-1`], `refused` `a@(k+1)`
/// (`a`'s cut) and `c@1`, and `b`'s pre-cut document (`Doors`, abstract), in
/// all 32 orders; `a` serves `b`'s document although it wrote the other cut.
#[test]
fn si13_o2_two_concurrent_cuts_with_unequal_stamps_name_one_successor_in_every_order() {
    let runs = every_order(Case::Unequal);
    assert_eq!(runs, 32, "premise: 4 x 2 x 4 delivery orders");
    println!("SI13-O2: {runs} delivery orders; every replica keeps b's cut (the greater stamp)");
}

/// A fresh `j` takes `donor`'s state as a node's state transfer does: the
/// donor's snapshot, and its log through `TransferableLog`'s export and
/// import (JSON, as on the wire).
fn transfer(reps: &mut [Rep], donor: usize) {
    let snapshot = reps[donor].snapshot();
    let exported = <ModelLog as TransferableLog>::export_log(reps[donor].state());
    let text = serde_json::to_string(&exported).expect("an exported log serializes");
    let state = <ModelLog as TransferableLog>::import_log(
        serde_json::from_str(&text).expect("and parses back"),
    )
    .expect("premise: j imports the donor's exported log");
    reps[J].adopt(snapshot, state);
}

/// OB3 in process (M12). Four joiners, each a fresh `j`:
///
/// - J1, state transfer after both cuts (`Unequal`, every message delivered
///   in one order): `j` adopts `a`'s state and must name `b`'s successor,
///   supersede `a`'s, serve `b`'s pre-cut document and refuse naming it.
/// - J2, state transfer before the cut M11 keeps (`Tie`): `j` adopts `b`'s
///   state while `b` holds only its own cut, names `b`'s successor as `b`
///   does, then delivers `a`'s cut and must switch to `a`'s, as `b` does.
/// - J3, state transfer after one cut (SI.12's shape): `b` holds its own two
///   edits and `c`'s in its tree and `a`'s cut; `j` adopts `b`'s state and
///   must serve `a`'s pre-cut document, not `b`'s tree, and refuse.
/// - J4, delta (`Unequal`): `j` delivers every event after the fact, the
///   base first, then the cuts and edits, and must read what `a`, `b` and
///   `c` read.
#[test]
fn si13_o3_a_joiner_after_the_cuts_serves_the_cut_m11_keeps() {
    // J1.
    let mut written = write(Case::Unequal, "J1");
    deliver(&mut written, A, &[B, C], &[0, 0, 0, 1], "J1");
    let expected = status13(&written.reps[A].state().migration());
    transfer(&mut written.reps, A);
    let mut j_view = written.views[A].clone();
    j_view.delivered.clear();
    let joined = status13(&written.reps[J].state().migration());
    assert_eq!(
        (joined["migrated_to"].clone(), joined["superseded"].clone()),
        (
            expected["migrated_to"].clone(),
            expected["superseded"].clone()
        ),
        "J1 (M12): a joiner by state transfer must name the successor and supersede the copies \
         its donor does"
    );
    let kept = winner(&j_view.cuts);
    assert_eq!(kept.origin, "b", "premise: M11 keeps b's cut in J1");
    let cut = written.known[1].clone();
    assert_eq!(
        identified(&written.reps[J]),
        cut.identified,
        "J1 (M12, M4): the joiner must serve b's pre-cut document"
    );
    assert_eq!(
        plain(&written.reps[J]),
        cut.plain,
        "J1 (M12, M4): default read"
    );
    refusals(&mut written.reps[J], &kept.to, "J1 (M12)");
    println!(
        "SI13-O3 J1: the joiner's refused list is {} (reported, not registered)",
        joined["refused"]
    );

    // J2.
    let mut written = write(Case::Tie, "J2");
    transfer(&mut written.reps, B);
    let mut j_view = written.views[B].clone();
    check_view(
        &mut written.reps[J],
        &j_view,
        &written.known,
        "J2, j after the transfer",
        Joined::Transferred,
    );
    let (cut_a, info_a) = written.streams[A][0].clone();
    j_view.delivered.push(origin_of(&cut_a));
    j_view.cuts.push(info_a.expect("a's stream is its cut"));
    written.reps[J].receive(cut_a);
    check_view(
        &mut written.reps[J],
        &j_view,
        &written.known,
        "J2, j after a's cut",
        Joined::Transferred,
    );
    assert_eq!(
        winner(&j_view.cuts).origin,
        "a",
        "premise: M11 keeps a's cut in J2"
    );

    // J3.
    let uml = Uml::new(D1);
    let (mut reps, _base, k) = base();
    let b_edits: Vec<Msg> = uml
        .b_edits()
        .into_iter()
        .map(|op| send(&mut reps[B], op, "b's edit"))
        .collect();
    let c1 = send(&mut reps[C], uml.c_edit(), "c's edit");
    let a_identified = identified(&reps[A]);
    let a_plain = plain(&reps[A]);
    let plan = migrate(reps[A].state(), "si13-copy", D2)
        .unwrap_or_else(|why| panic!("J3 (M1): a's migration was refused: {why}"));
    let cut = send(&mut reps[A], plan.cut, "a's cut");
    reps[B].receive(c1);
    reps[B].receive(cut);
    let mut delivered = events("a", k + 1);
    delivered.extend(events("b", b_edits.len() as u64));
    delivered.push(("c".into(), 1));
    let known = vec![Known {
        info: CutInfo {
            origin: "a".into(),
            seq: k + 1,
            stamp: k + 1,
            to: "si13-copy".into(),
        },
        seen: events("a", k + 1),
        identified: a_identified,
        plain: a_plain,
    }];
    let view = View {
        delivered,
        cuts: vec![known[0].info.clone()],
    };
    check_view(
        &mut reps[B],
        &view,
        &known,
        "J3, the donor b",
        Joined::Delivered,
    );
    transfer(&mut reps, B);
    let joined = status13(&reps[J].state().migration());
    assert_eq!(
        (joined["migrated_to"].clone(), joined["superseded"].clone()),
        (Value::from("si13-copy"), serde_json::json!([])),
        "J3 (M12): a joiner by state transfer after the cut must name its successor"
    );
    assert_eq!(
        identified(&reps[J]),
        known[0].identified,
        "J3 (M12, M4): a joiner by state transfer must serve the cut's document, not its donor's \
         tree (which holds b's and c's edits concurrent with the cut)"
    );
    assert_eq!(
        plain(&reps[J]),
        known[0].plain,
        "J3 (M12, M4): the joiner's default read must be the cut's"
    );
    refusals(&mut reps[J], "si13-copy", "J3 (M12)");

    // J4.
    let mut written = write(Case::Unequal, "J4");
    let mut view = View::default();
    for message in written.base.clone() {
        view.delivered.push(origin_of(&message));
        written.reps[J].receive(message);
    }
    for stream in [C, A, B] {
        for (message, cut) in written.streams[stream].clone() {
            view.delivered.push(origin_of(&message));
            if let Some(cut) = cut {
                view.cuts.push(cut);
            }
            written.reps[J].receive(message);
            check_view(
                &mut written.reps[J],
                &view,
                &written.known,
                "J4, j by delta",
                Joined::Delivered,
            );
        }
    }
    println!("SI13-O3: J1 to J4, joiners by state transfer and by delta serve the cut M11 keeps");
}
