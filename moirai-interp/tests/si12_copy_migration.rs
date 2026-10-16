//! SI.12 in process: copy-migration of a SimpleUML model to a revised
//! descriptor, on three real `ModelLog` replicas of the source model and
//! three of the copy, in every delivery order of the concurrent schedule.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.12.md`; the contract (M1 the
//! action, M2 the cut, M3 read-only, M4 frozen, M5 reported, M6 the copy, M7
//! refused revisions) is restated in `si12/mod.rs`. Every read here is the
//! one a node serves: `QueryableLog::query_identified_state_json` and
//! `QueryableLog::query_state_json`.
//!
//! What each test expects is derived from its schedule and the contract,
//! never read off the implementation: the copy is held to the running
//! example built directly under the revision by hand (`expected_d2`, checked
//! against such a build in `si12_fixtures.rs`), the frozen source to what the
//! migrating replica served when it cut, and the refused edits to the events
//! the test itself sent concurrently with the cut.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si12_copy_migration -- --test-threads=1
//! ```

mod si12;

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::migrate::{Migration, migrate};
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::Value;

use si12::*;

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;
type Msg = EventMessage<ModelOp>;

/// One replica's part of a concurrent schedule: its index, the streams it
/// delivers, the order it takes them in, whether it holds the cut already,
/// and the edits concurrent with the cut it holds already.
type Delivery<'a> = (
    usize,
    Vec<Vec<Msg>>,
    &'a Vec<usize>,
    bool,
    Vec<(&'a str, u64)>,
);

/// The id the copy is opened under in these tests.
const M_PRIME: &str = "si12-copy";

fn identified(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

fn plain(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

/// M2: the migration status, as the contract fixes it.
fn status(replica: &Rep) -> Value {
    status_keys(&replica.state().migration())
}

/// M1, in process: the plan, or the reason it was refused.
fn planned(source: &Rep, model_id: &str, descriptor: &str) -> Result<Migration, String> {
    migrate(source.state(), model_id, descriptor).map_err(|why| why.to_string())
}

fn send(replica: &mut Rep, op: ModelOp, what: &str) -> Msg {
    replica
        .send(op)
        .unwrap_or_else(|why| panic!("{what}: the local intake refused it: {why}"))
}

/// The running example on three replicas of `m`, written on `a` and
/// delivered to `b` and `c`.
fn base() -> [Rep; 3] {
    let uml = Uml::new(D1);
    let (mut a, mut b, mut c) = triplet_log::<ModelLog>();
    let opened = send(&mut a, install("si12-m", D1), "the source's Install");
    b.receive(opened.clone());
    c.receive(opened);
    for op in uml.structure() {
        let message = send(&mut a, op, "building the running example");
        b.receive(message.clone());
        c.receive(message);
    }
    let ids = ids_by_label(&identified(&a));
    for op in uml.links(&ids) {
        let message = send(&mut a, op, "linking the running example");
        b.receive(message.clone());
        c.receive(message);
    }
    let replicas = [a, b, c];
    for (index, replica) in replicas.iter().enumerate() {
        assert_eq!(
            project(&identified(replica)),
            expected_d1(),
            "premise: replica {index} serves the running example under D1"
        );
    }
    replicas
}

/// The copy opened on three replicas of its own, `ops` sent on the replica
/// at `writer` (the migrating one) and delivered to the other two in order.
fn open_copy(ops: &[ModelOp], writer: usize) -> [Rep; 3] {
    let (a, b, c) = triplet_log::<ModelLog>();
    let mut copy = [a, b, c];
    for (index, op) in ops.iter().enumerate() {
        let message = copy[writer].send(op.clone()).unwrap_or_else(|why| {
            panic!(
                "SI.12 (M1): operation {index} of the migration was refused by the copy's own \
                 intake on the migrating replica: {why}; the operation was {op:?}"
            )
        });
        for (other, replica) in copy.iter_mut().enumerate() {
            if other != writer {
                replica.receive(message.clone());
            }
        }
    }
    copy
}

/// M1: the first operation opens the copy under `D2` with `model_id`, and
/// no other operation opens it again.
fn check_opening(ops: &[ModelOp], model_id: &str, label: &str) {
    match ops.first() {
        Some(ModelOp::Install {
            model_id: id,
            metamodel_id,
            descriptor,
            ..
        }) => {
            assert_eq!(
                id, model_id,
                "{label} (M1): the copy's Install names another model"
            );
            assert_eq!(
                metamodel_id,
                &digest(D2),
                "{label} (M1): the copy's Install names another metamodel than D2's digest"
            );
            assert_eq!(
                digest(descriptor),
                digest(D2),
                "{label} (M1): the copy's Install carries a descriptor that is not D2"
            );
        }
        other => {
            panic!("{label} (M1): the migration's first operation is not an Install: {other:?}")
        }
    }
    let installs = ops
        .iter()
        .filter(|op| matches!(op, ModelOp::Install { .. }))
        .count();
    assert_eq!(
        installs, 1,
        "{label} (M1): the migration opens the copy {installs} times"
    );
}

/// M6 on every replica of the copy: bound to `D2`, nothing unrouted, the
/// running example under `D2` in both reads, one document.
fn check_copy(copy: &[Rep; 3], label: &str) {
    let first = identified(&copy[0]);
    for (index, replica) in copy.iter().enumerate() {
        let log = replica.state();
        assert_eq!(
            log.header().map(|header| header.metamodel_id.clone()),
            Some(digest(D2)),
            "{label} (M6): replica {index} of the copy is not bound to D2"
        );
        assert_eq!(
            log.semantics().map(|sem| sem.digest.clone()),
            Some(digest(D2)),
            "{label} (M6): replica {index} of the copy holds another table than D2's"
        );
        assert_eq!(
            log.unresolved(),
            0,
            "{label} (M6): replica {index} of the copy could not route an operation"
        );
        let doc = identified(replica);
        assert_eq!(
            doc, first,
            "{label} (M6): replica {index} of the copy serves another document than replica 0"
        );
        assert_eq!(
            project(&doc),
            expected_d2(),
            "{label} (M6): replica {index} of the copy is not the cut's model mapped to D2 \
             (kept values, defaulted additions, names re-encoded, links to the copies of their \
             targets); served {doc}"
        );
        assert_eq!(
            project_with(&plain(replica), &labels(&doc)),
            strip_ids(&expected_d2()),
            "{label} (M6): replica {index}'s default read of the copy is not the running example \
             under D2"
        );
    }
}

/// M2 and M4 on one replica that has delivered the cut: the status names the
/// copy and lists exactly `refused`, and both reads are the cut's.
fn check_frozen(
    replica: &Rep,
    cut_doc: &Value,
    cut_plain: &Value,
    refused: &[(&str, u64)],
    label: &str,
) {
    assert_eq!(
        status(replica),
        migrated(M_PRIME, refused),
        "{label} (M2, M5): the migration status"
    );
    let doc = identified(replica);
    assert_eq!(
        &doc, cut_doc,
        "{label} (M4): the identified read moved after the cut; it must stay what the migrating \
         replica served when it cut"
    );
    assert_eq!(
        &plain(replica),
        cut_plain,
        "{label} (M4): the default read moved after the cut"
    );
}

/// C1. `a` migrates the running example to `D2`; the cut reaches `b` and `c`
/// and the copy's operations reach the copy's other two replicas. The copy
/// is the running example under `D2` on every replica: `Vehicle`, `Car`,
/// `Door`, `Sedan`, `String` and `hasDoors` with their names (re-encoded as
/// one register value each), `Car` abstract with stereotype `entity`, its
/// property `doors` typed `Door` and tagged value `wheels`/`4`, `Door`'s
/// owner text `Vehicle` and property `color` typed `String`, `Sedan`'s
/// substitutable generalization to `Car`, `hasDoors` from `Car` to `Door`;
/// every class `isLeaf: false`, every model element `ownedComments: []`.
#[test]
fn si12_c1_the_copy_carries_kept_defaulted_and_reencoded_values_on_every_replica() {
    let mut m = base();
    let plan = planned(&m[0], M_PRIME, D2).unwrap_or_else(|why| {
        panic!("SI.12 (C1, M1): migrating the running example to D2 was refused: {why}")
    });
    check_opening(&plan.ops, M_PRIME, "C1");
    let cut = send(
        &mut m[0],
        plan.cut.clone(),
        "C1: the cut on the migrating replica",
    );
    m[1].receive(cut.clone());
    m[2].receive(cut);
    let copy = open_copy(&plan.ops, 0);
    check_copy(&copy, "C1");
    println!("SI12-C1: the copy is the running example under D2 on three replicas");
}

/// C2. Without concurrency: before the cut reaches `b`, `b` reads no
/// migration; once each replica has the cut, it names the copy, lists no
/// refused edit, serves what `a` served when it cut, refuses every kind of
/// local edit with a reason naming the copy (and the refusal changes
/// nothing), and refuses a second migration.
#[test]
fn si12_c2_the_cut_makes_the_source_read_only_and_names_the_copy_on_every_replica() {
    let uml = Uml::new(D1);
    let mut m = base();
    let cut_doc = identified(&m[0]);
    let cut_plain = plain(&m[0]);
    let ids = ids_by_label(&cut_doc);
    for (what, op) in uml.local_edits(&ids) {
        for (index, replica) in m.iter().enumerate() {
            assert!(
                replica.state().refusal(&op).is_ok(),
                "premise: replica {index} takes {what} before any cut"
            );
        }
    }
    for (index, replica) in m.iter().enumerate() {
        assert_eq!(
            status(replica),
            not_migrated(),
            "C2 (M2): replica {index} reads a migration before any cut"
        );
    }
    let plan = planned(&m[0], M_PRIME, D2)
        .unwrap_or_else(|why| panic!("SI.12 (C2, M1): migrating to D2 was refused: {why}"));
    let cut = send(&mut m[0], plan.cut.clone(), "C2: the cut");
    check_frozen(
        &m[0],
        &cut_doc,
        &cut_plain,
        &[],
        "C2 on a, right after its cut",
    );
    assert_eq!(
        status(&m[1]),
        not_migrated(),
        "C2 (M2): b reads a migration before the cut reached it"
    );
    m[1].receive(cut.clone());
    m[2].receive(cut);
    for (index, replica) in m.iter_mut().enumerate() {
        let label = format!("C2 on replica {index}");
        check_frozen(replica, &cut_doc, &cut_plain, &[], &label);
        for (what, op) in uml.local_edits(&ids) {
            match replica.send(op) {
                Ok(_) => panic!("{label} (M3): {what} was accepted after the cut"),
                Err(why) => assert!(
                    why.to_string().contains(M_PRIME),
                    "{label} (M3): the refusal of {what} does not name the copy `{M_PRIME}`: {why}"
                ),
            }
        }
        check_frozen(
            replica,
            &cut_doc,
            &cut_plain,
            &[],
            &format!("{label}, after the refusals"),
        );
    }
    match planned(&m[1], "si12-second", D2) {
        Ok(_) => panic!("C2 (M3): b migrated the source a second time after the cut"),
        Err(why) => assert!(
            why.contains(M_PRIME),
            "C2 (M3): the refusal of a second migration does not name the copy `{M_PRIME}`: {why}"
        ),
    }
    println!("SI12-C2: the source is frozen and read-only on three replicas, naming the copy");
}

/// C3. The concurrent schedule, in every delivery order: `b` renames `Door`
/// to `Doors` and makes it abstract (its events 1 and 2), `c` adds the
/// stereotype `persistent` to `Car` (its event 1), and `a` migrates, none of
/// the three having delivered the others. `a` delivers `b`'s two edits and
/// `c`'s in any order (3), `b` the cut and `c`'s edit (2), `c` the cut and
/// `b`'s edits (3): 18 runs. At every step a replica that has the cut reads
/// what `a` served when it cut and lists exactly the edits of `b` and `c` it
/// has delivered (its own included); one that has not reads no migration.
/// After every run all three read the same frozen document and list
/// `b@1`, `b@2`, `c@1`; the copy is the running example under `D2`, without
/// `Doors`, `Door` abstract or `persistent`.
#[test]
fn si12_c3_edits_concurrent_with_the_cut_are_left_out_and_reported_in_every_order() {
    let uml = Uml::new(D1);
    let mut runs = 0;
    for at_a in interleavings(&[2, 1]) {
        for at_b in interleavings(&[1, 1]) {
            for at_c in interleavings(&[1, 2]) {
                runs += 1;
                let label = format!("C3 run {runs} (a {at_a:?}, b {at_b:?}, c {at_c:?})");
                let mut m = base();
                let cut_doc = identified(&m[0]);
                let cut_plain = plain(&m[0]);

                let from_b: Vec<Msg> = uml
                    .b_edits()
                    .into_iter()
                    .map(|op| send(&mut m[1], op, "b's edit"))
                    .collect();
                let from_c = vec![send(&mut m[2], uml.c_edit(), "c's edit")];
                let seqs: Vec<(String, u64)> = from_b
                    .iter()
                    .chain(&from_c)
                    .map(|message| {
                        let id = message.event().id();
                        (id.origin_id().to_string(), id.seq() as u64)
                    })
                    .collect();
                assert_eq!(
                    seqs,
                    vec![
                        ("b".to_string(), 1),
                        ("b".to_string(), 2),
                        ("c".to_string(), 1)
                    ],
                    "premise: b's edits are its events 1 and 2 and c's its event 1"
                );
                assert_eq!(
                    door_name(&identified(&m[1])),
                    Value::String("Doors".into()),
                    "premise: b reads its rename before the cut reaches it"
                );

                let plan = planned(&m[0], M_PRIME, D2).unwrap_or_else(|why| {
                    panic!("SI.12 ({label}, M1): migrating to D2 was refused: {why}")
                });
                let cut = send(&mut m[0], plan.cut.clone(), "the cut");
                check_frozen(
                    &m[0],
                    &cut_doc,
                    &cut_plain,
                    &[],
                    &format!("{label}, a at its cut"),
                );
                for (index, replica) in m.iter().enumerate().skip(1) {
                    assert_eq!(
                        status(replica),
                        not_migrated(),
                        "{label} (M2): replica {index} reads a migration before the cut reached it"
                    );
                }

                // Per replica: the streams it delivers, whether it has the
                // cut, and the edits concurrent with the cut it holds.
                let schedule: [Delivery<'_>; 3] = [
                    (0, vec![from_b.clone(), from_c.clone()], &at_a, true, vec![]),
                    (
                        1,
                        vec![vec![cut.clone()], from_c.clone()],
                        &at_b,
                        false,
                        vec![("b", 1), ("b", 2)],
                    ),
                    (
                        2,
                        vec![vec![cut.clone()], from_b.clone()],
                        &at_c,
                        false,
                        vec![("c", 1)],
                    ),
                ];
                for (index, streams, order, mut has_cut, mut held) in schedule {
                    let mut next = vec![0usize; streams.len()];
                    for (step, stream) in order.iter().enumerate() {
                        let message = streams[*stream][next[*stream]].clone();
                        next[*stream] += 1;
                        let origin = message.event().id().origin_id().to_string();
                        let seq = message.event().id().seq() as u64;
                        m[index].receive(message);
                        if origin == "a" {
                            has_cut = true;
                        } else {
                            held.push((if origin == "b" { "b" } else { "c" }, seq));
                        }
                        let here = format!("{label}, replica {index} step {step}");
                        if has_cut {
                            check_frozen(&m[index], &cut_doc, &cut_plain, &held, &here);
                        } else {
                            assert_eq!(
                                status(&m[index]),
                                not_migrated(),
                                "{here} (M2): a migration is read before the cut arrived"
                            );
                        }
                    }
                }
                for (index, replica) in m.iter().enumerate() {
                    check_frozen(
                        replica,
                        &cut_doc,
                        &cut_plain,
                        &[("b", 1), ("b", 2), ("c", 1)],
                        &format!("{label}, replica {index} at the end"),
                    );
                }
                let copy = open_copy(&plan.ops, 0);
                check_copy(&copy, &label);
            }
        }
    }
    assert_eq!(runs, 18, "C3 ran {runs} delivery orders, not 18");
    println!(
        "SI12-C3: 18 delivery orders, the source frozen and the three concurrent edits reported on every replica"
    );
}

/// C4. A revision that renames a feature (`Class.abstract` to `isAbstract`)
/// and one that removes a feature the model does not use
/// (`EnumerationLiteral.name`) are refused, each naming the feature; the
/// source reads no migration after them and still takes an edit on every
/// replica.
#[test]
fn si12_c4_a_revision_that_renames_or_removes_a_feature_is_refused_by_name() {
    let uml = Uml::new(D1);
    let mut m = base();
    for (descriptor, what, feature) in [
        (D2_RENAME, "the rename revision", "Class.abstract"),
        (D2_REMOVE, "the removal revision", "EnumerationLiteral.name"),
    ] {
        match planned(&m[0], "si12-refused", descriptor) {
            Ok(_) => panic!("SI.12 (C4, M7): {what} was accepted"),
            Err(why) => assert!(
                why.contains(feature),
                "SI.12 (C4, M7): the refusal of {what} does not name `{feature}`: {why}"
            ),
        }
    }
    for (index, replica) in m.iter().enumerate() {
        assert_eq!(
            status(replica),
            not_migrated(),
            "C4 (M7): replica {index} reads a migration after two refused ones"
        );
    }
    for index in 0..3 {
        let message = send(
            &mut m[index],
            uml.name_char(SEDAN, 5, 's'),
            "C4: an edit after the refused migrations",
        );
        for (other, replica) in m.iter_mut().enumerate() {
            if other != index {
                replica.receive(message.clone());
            }
        }
    }
    let doc = identified(&m[0]);
    assert_eq!(
        doc["ownedElements"][2]["name"].as_str().map(str::len),
        Some(8),
        "C4: the three edits after the refused migrations were not all applied: {doc}"
    );
    println!("SI12-C4: the rename and the removal revisions refused by name, the source writable");
}

/// C5. The copy merges by the revision's rule: after the migration, `a`
/// renames `Door` to `Gate` and `b` to `Hatch` in the copy, concurrently;
/// `c` delivers the two in either order (2 runs). Every replica reads one
/// name, the same on all three, `Gate` or `Hatch` (a fair register), where
/// the source's text rule would have kept the characters of both.
#[test]
fn si12_c5_the_copy_merges_by_the_revised_rule() {
    let uml2 = Uml::new(D2);
    let mut runs = 0;
    for at_c in interleavings(&[1, 1]) {
        runs += 1;
        let label = format!("C5 run {runs} (c {at_c:?})");
        let mut m = base();
        let plan = planned(&m[0], M_PRIME, D2).unwrap_or_else(|why| {
            panic!("SI.12 ({label}, M1): migrating to D2 was refused: {why}")
        });
        let _cut = send(&mut m[0], plan.cut.clone(), "the cut");
        let mut copy = open_copy(&plan.ops, 0);
        let gate = send(
            &mut copy[0],
            uml2.rename_door("Gate"),
            "a renames Door to Gate in the copy",
        );
        let hatch = send(
            &mut copy[1],
            uml2.rename_door("Hatch"),
            "b renames Door to Hatch in the copy",
        );
        copy[0].receive(hatch.clone());
        copy[1].receive(gate.clone());
        let streams = [gate, hatch];
        for stream in &at_c {
            copy[2].receive(streams[*stream].clone());
        }
        let names: Vec<Value> = copy
            .iter()
            .map(|replica| door_name(&identified(replica)))
            .collect();
        assert!(
            names.iter().all(|name| name == &names[0]),
            "{label} (M6): the replicas of the copy read different names for Door: {names:?}"
        );
        assert!(
            names[0] == Value::String("Gate".into()) || names[0] == Value::String("Hatch".into()),
            "{label} (M6): the copy did not merge the two renames by a register: Door reads {}",
            names[0]
        );
    }
    assert_eq!(runs, 2);
    println!("SI12-C5: two concurrent renames in the copy read as one name on every replica");
}
