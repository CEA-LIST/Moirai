//! SI.16 in process: a replica that joins by state transfer a model whose
//! history holds a rebase (SI.14) behaves like a member that delivered the
//! rebase by causal delivery.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.16.md`. SI.14's contract
//! (M15 to M23, restated in `si14/mod.rs`) stands; SI.16 holds a joiner by
//! state transfer to it, on every surface:
//!
//! - J1, the revision in force. Once it has adopted a donor's state that
//!   keeps a rebase to `D2`, the joiner's `header().metamodel_id`,
//!   `semantics().digest` and `descriptor()` are `D2`'s, and both reads
//!   serve the donor's document under `D2`, every object under the identity
//!   it had. (Over HTTP, `si16_rebased_joiner_l3.rs`: the node lists the
//!   model under `D2`'s digest and serves `D2` on its metamodel route.)
//! - J2, the intake. The joiner's `ModelLog::refusal` accepts an operation
//!   written under `D2` and refuses one only `D1` routes, as the donor does,
//!   while `D1` is held and after.
//! - J3, the status. `ModelLog::migration()` on the joiner reads what it
//!   reads on a member that delivered the same events by causal delivery:
//!   `strategy` `rebase`, the kept rebase, `hidden`, `dropped`, `discarded`,
//!   `superseded` and `superseded_rebases`; and `rebase.waiting_for` names
//!   exactly the members whose delivery of the rebase it has not heard of,
//!   never itself, `[]` once the rebase is causally stable on it, with
//!   `d1_held` true exactly while that list is not empty. What the joiner
//!   has heard is its matrix clock as `Tcsb::adopt` sets it (ruling R2 of the
//!   spec): every member's row is the donor's row of it, and the donor's own
//!   row is everything the donor delivered. So the joiner has heard its
//!   donor deliver the rebase (the donor's state holds it), and every member
//!   the donor had heard; from then on each delivery sets the row of its
//!   origin, as on any member.
//! - J4, the window. During the rebase's window the joiner holds `D1` until
//!   the rebase is causally stable on it, moved by deliveries alone, and
//!   lets it go then (`d1_held` false). In R the rebase is stable on the
//!   joiner the moment it adopts, and `d1_held` is read off `waiting_for`,
//!   so R reads whether `D1` is held where the replica shows it: its intake
//!   of a migration, which `moirai_interp::migrate::rebase` refuses as
//!   `MigrateRefusal::Rebasing` exactly while `D1` is held (spec, round 2,
//!   R14). A rebase back to `D1` is refused either way, and nothing is sent.
//! - J5, late edits. An edit written under `D1` concurrently with the rebase
//!   that reaches the joiner after its join is translated into `D2` and kept,
//!   as on the other members: `unresolved()` stays 0 and the joiner reads
//!   the document they read.
//! - J6, convergence. Once every event is delivered everywhere, one document
//!   on every member, the joiner included, in both reads.
//!
//! Four schedules, each scripted, with a check on every replica a step
//! touches, every expectation computed by hand from the schedule:
//!
//! - W, a join during the window: `c` writes two edits under `D1`, `a`
//!   rebases to `D2`, `b` delivers the rebase and `j` adopts `b`'s state
//!   while `b` waits for `c` and `j`; `j` then delivers `c`'s late edits.
//! - S, a join after the rebase is stable on the donor: `a` rebases to
//!   `D2_DROP` naming its two drops, `c`'s late edit of the removed tagged
//!   value is dropped, `b` hears `a` and `c` deliver the rebase and lets
//!   `D1` go, and `j`, a member none of `a`, `b`, `c` held, adopts `b`'s
//!   state.
//! - G, a join after two concurrent rebases: SI.14's RG-tie (`a` to `D2`,
//!   `b` to `D2_DROP`, equal stamps, `a`'s kept), `c` delivers both and `j`
//!   adopts `c`'s state, so the superseded rebase and the discarded edit
//!   come over in the state.
//! - R, a join that lets `D1` go at the adoption (spec, round 2): the donor
//!   `b` still holds `D1`, waiting for `j` alone, every other member having
//!   written under `D2` after delivering the rebase, so `j` has heard every
//!   member but itself deliver it the moment it adopts `b`'s state.
//!
//! J2's refusal half is checked with edits that only `D1` routes: in W and
//! G `D1`'s table routes them on the model under `D1` the joiner holds in
//! its window (no edit written under `D1` routes on a model under `D2`,
//! whose root object `D1`'s table reads as an abstract class); in S, whose
//! revision keeps `D1`'s slots for all it keeps, a new `TaggedValue` on
//! `Car` is routed by `D1`'s table on the model in force and refused by
//! `D2_DROP`'s (spec, round 2, R10).
//!
//! In W, S and G the donor delivered the rebase without writing anything
//! after it, so the rule of J3 on the donor is the one each check reads. A
//! failing check is recorded and the schedule goes on; the test fails at its
//! end listing every failing check. A premise that fails stops it at once.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si16_rebased_joiner -- --test-threads=1 --nocapture
//! ```

mod si14;
#[path = "ec03/side.rs"]
mod side;

use std::collections::BTreeMap;

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::migrate::{MigrateRefusal, rebase};
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_network::state_transfer::TransferableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Value, json};

use si14::si12::{
    CAR, CAR_WHEELS, D1, D2, DOOR, SEDAN, Uml, ids_by_label, install, labels, project_with,
    strip_ids,
};
use si14::*;

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;
type Msg = EventMessage<ModelOp>;

/// The model id every schedule opens `m` under.
const MODEL_ID: &str = "si16-m";

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const J: usize = 3;
const IDS: [&str; 4] = ["a", "b", "c", "j"];

/// What D2 has that D1 lacks, as a rebase from D2 back to D1 names it
/// (`<Class>` or `<Class>.<feature>` of the declaring class, in D2's slot
/// order): what R's window read is refused with once D1 is let go.
const D2_ADDS: [&str; 3] = ["Class.isLeaf", "Comment", "ModelElement.ownedComments"];

fn identified(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

fn plain(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

/// The rebase `replica` plans to `rev`, dropping `names` (SI.14, M16).
fn rebase_op(replica: &Rep, rev: Rev, names: &[&str]) -> ModelOp {
    rebase(replica.state(), rev.text(), &drops(names)).unwrap_or_else(|why| {
        panic!(
            "premise: a rebase to {} with drops {names:?} was refused: {why}",
            rev.digest()
        )
    })
}

/// Four replicas of one log, `a`, `b`, `c` and the joiner `j`, the running
/// example written on `a` (`a@1` to `a@70`) and delivered to `b` and `c`;
/// `j` holds nothing. The labels are `a`'s read before any migration.
struct World {
    reps: Vec<Rep>,
    /// Identity to label.
    labels: BTreeMap<String, String>,
    /// Label to identity.
    ids: BTreeMap<String, String>,
    failures: Vec<String>,
    schedule: &'static str,
}

impl World {
    /// `members(id)` is the member list replica `id` is bootstrapped with;
    /// each replica interns itself first.
    fn new(schedule: &'static str, members: impl Fn(&str) -> Vec<&'static str>) -> Self {
        let mut reps: Vec<Rep> = IDS
            .iter()
            .map(|id| {
                let list = members(id);
                side::bootstrap::<ModelLog>(id, &list)
            })
            .collect();
        let uml = Uml::new(D1);
        let mut sent = 0u64;
        let mut write = |reps: &mut Vec<Rep>, op: ModelOp| {
            let message = reps[A]
                .send(op)
                .unwrap_or_else(|why| panic!("premise: the running example: {why}"));
            reps[B].receive(message.clone());
            reps[C].receive(message);
            sent += 1;
        };
        write(&mut reps, install(MODEL_ID, D1));
        for op in uml.structure() {
            write(&mut reps, op);
        }
        let ids = ids_by_label(&identified(&reps[A]));
        for op in uml.links(&ids) {
            write(&mut reps, op);
        }
        assert_eq!(sent, 70, "premise: the running example is a@1 to a@70");
        let doc = identified(&reps[A]);
        for index in [A, B, C] {
            assert_eq!(
                si14::si12::project(&identified(&reps[index])),
                si14::si12::expected_d1(),
                "premise: {} serves the running example under D1",
                IDS[index]
            );
            assert_eq!(
                status14(&reps[index].state().migration()),
                quiet(),
                "premise: {} names no migration",
                IDS[index]
            );
        }
        assert!(
            reps[J].state().header().is_none(),
            "premise: j holds nothing of the model before it joins"
        );
        World {
            reps,
            labels: labels(&doc),
            ids: ids_by_label(&doc),
            failures: Vec::new(),
            schedule,
        }
    }

    /// `op` written on `origin`, which must accept it, answered as the
    /// message every other replica delivers; `want` is its id at `origin`.
    fn send(&mut self, origin: usize, op: ModelOp, want: (&str, u64), what: &str) -> Msg {
        let message = match self.reps[origin].send(op) {
            Ok(message) => message,
            // Only the joiner's own edits, written under the revision in
            // force, can meet a refusal the claim forbids (J2); the schedule
            // needs the edit and ends here, with what was recorded so far.
            Err(why) => panic!(
                "SI.16 ({}): {what}: {}'s intake refused it ({}): {why}\nrecorded so far, {} \
                 failing checks:\n{}",
                self.schedule,
                IDS[origin],
                if origin == J { "J2" } else { "premise" },
                self.failures.len(),
                self.failures.join("\n")
            ),
        };
        let id = message.event().id();
        assert_eq!(
            (id.origin_id().to_string(), id.seq() as u64),
            (want.0.to_string(), want.1),
            "premise: {}: {what} is {}@{}",
            self.schedule,
            want.0,
            want.1
        );
        message
    }

    fn deliver(&mut self, to: &[usize], message: &Msg) {
        for index in to {
            self.reps[*index].receive(message.clone());
        }
    }

    /// `j` takes `donor`'s state as a node's state transfer does: the
    /// donor's snapshot, and its log exported and imported as JSON.
    fn join(&mut self, donor: usize) {
        let snapshot = self.reps[donor].snapshot();
        let exported = <ModelLog as TransferableLog>::export_log(self.reps[donor].state());
        let text = serde_json::to_string(&exported)
            .unwrap_or_else(|err| panic!("premise: the donor's log serializes: {err}"));
        let imported = <ModelLog as TransferableLog>::import_log(
            serde_json::from_str(&text)
                .unwrap_or_else(|err| panic!("premise: the donor's log parses back: {err}")),
        )
        .unwrap_or_else(|| panic!("premise: j imports the donor's exported log"));
        self.reps[J].adopt(snapshot, imported);
    }

    /// Check one replica: the table, the header and the descriptor of
    /// `rev`, nothing unrouted, both reads `doc` (projected through the
    /// running example's labels), the status `status`. Every mismatch is
    /// recorded.
    fn check(&mut self, index: usize, rev: Rev, doc: &Value, status: &Value, at: &str) {
        let at = format!("{} {at} on {}", self.schedule, IDS[index]);
        let replica = &self.reps[index];
        let log = replica.state();
        let mut wrong = Vec::new();
        let got = status14(&log.migration());
        if &got != status {
            wrong.push(format!(
                "the status (J3) reads {got}, the schedule gives {status}"
            ));
        }
        match log.header() {
            None => wrong.push("no header (J1)".to_string()),
            Some(header) => {
                if header.model_id != MODEL_ID {
                    wrong.push(format!("the model id moved to {} (J1)", header.model_id));
                }
                if header.metamodel_id != rev.digest() {
                    wrong.push(format!(
                        "the header names metamodel {}, not {} (J1)",
                        header.metamodel_id,
                        rev.digest()
                    ));
                }
            }
        }
        let table = log.semantics().map(|sem| sem.digest.clone());
        if table != Some(rev.digest()) {
            wrong.push(format!(
                "the table in force is {table:?}, not {} (J1)",
                rev.digest()
            ));
        }
        if log.descriptor() != Some(rev.text()) {
            wrong.push("the descriptor in force is not the revision's text (J1)".to_string());
        }
        if log.unresolved() != 0 {
            wrong.push(format!(
                "{} operations went unrouted (J5, SI.14 M18/H4)",
                log.unresolved()
            ));
        }
        let read = identified(replica);
        let projected = project_keep(&read, &self.labels);
        if &projected != doc {
            wrong.push(format!(
                "the model reads {projected}; the schedule gives {doc} (J1, J5, J6)"
            ));
        } else {
            let merged = labels_keep(&read, &self.labels);
            if project_with(&plain(replica), &merged) != strip_ids(&projected) {
                wrong.push(
                    "the default read is not the identified read without identities (J1, J6)"
                        .to_string(),
                );
            }
        }
        if !wrong.is_empty() {
            self.failures.push(format!("{at}: {}", wrong.join("; ")));
        }
    }

    /// J2: the joiner's intake answers `accepted` (written under the
    /// revision in force) and `refused` (only the old descriptor routes it)
    /// as `reference`, a member that delivered the rebase by causal
    /// delivery, answers them: `accepted` accepted, `refused` refused.
    fn intake(&mut self, reference: usize, accepted: &ModelOp, refused: &ModelOp, at: &str) {
        let on_reference = (
            self.reps[reference].state().refusal(accepted).is_ok(),
            self.reps[reference].state().refusal(refused).is_ok(),
        );
        assert_eq!(
            on_reference,
            (true, false),
            "premise: {} {at}: {}, which delivered the rebase by causal delivery, accepts the \
             edit written under the revision and refuses the one only the old descriptor routes",
            self.schedule,
            IDS[reference]
        );
        let on_joiner = (
            self.reps[J].state().refusal(accepted).is_ok(),
            self.reps[J].state().refusal(refused).is_ok(),
        );
        if on_joiner != on_reference {
            self.failures.push(format!(
                "{} {at} on j (J2): the intake answers (accepted, refused) {on_joiner:?}, as {} \
                 answers {on_reference:?}",
                self.schedule, IDS[reference]
            ));
        }
    }

    /// Whether replica `index` holds `D1`, read through its intake of a
    /// migration (spec, round 2, R14): `rebase` refuses one as
    /// `MigrateRefusal::Rebasing` exactly while the replica holds the model
    /// under the descriptor the rebase in force replaced. A rebase back to
    /// `D1`, refused either way: otherwise because `D1` lacks what `D2` adds,
    /// which `drops` does not name. Nothing is sent; any other answer is a
    /// premise that fails.
    fn holds_d1(&self, index: usize) -> bool {
        match rebase(self.reps[index].state(), D1, &[]) {
            Err(MigrateRefusal::Rebasing) => true,
            Err(MigrateRefusal::Undropped(missing)) if missing == D2_ADDS => false,
            other => panic!(
                "premise: {}: a rebase of {}'s model back to D1 is refused, as `Rebasing` while \
                 D1 is held and otherwise naming what D1 lacks of D2 ({D2_ADDS:?}); it answered \
                 {other:?}",
                self.schedule, IDS[index]
            ),
        }
    }

    /// The window on replica `index` (J4 on `j`, SI.14's M23 on the others):
    /// `D1` held exactly when `held` says so. A mismatch is recorded.
    fn window(&mut self, index: usize, held: bool, at: &str) {
        let reads = self.holds_d1(index);
        if reads != held {
            let word = |held: bool| if held { "held" } else { "let go" };
            self.failures.push(format!(
                "{} {at} on {} (J4, the window): D1 {} (a rebase back to D1 {} as `Rebasing`); \
                 the schedule gives D1 {}",
                self.schedule,
                IDS[index],
                word(reads),
                if reads { "refused" } else { "not refused" },
                word(held)
            ));
        }
    }

    /// J6: every replica serves the document `a` serves, in both reads.
    fn one_document(&mut self) {
        let on_a = (identified(&self.reps[A]), plain(&self.reps[A]));
        for index in [B, C, J] {
            let here = (identified(&self.reps[index]), plain(&self.reps[index]));
            if here != on_a {
                self.failures.push(format!(
                    "{} (J6): {} and a delivered every event and serve two documents: {} | {}",
                    self.schedule, IDS[index], here.0, on_a.0
                ));
            }
        }
    }

    fn finish(self, done: &str) {
        assert!(
            self.failures.is_empty(),
            "SI.16 ({}) on a replica that joined by state transfer, {} failing checks:\n{}",
            self.schedule,
            self.failures.len(),
            self.failures.join("\n")
        );
        println!("{done}");
    }
}

/// What a kept rebase lists besides whom it waits for (SI.14, M20, M22).
#[derive(Clone, Default)]
struct Lists {
    hidden: Vec<String>,
    dropped: Vec<(&'static str, u64, &'static str)>,
    discarded: Vec<(&'static str, u64)>,
    superseded_rebases: Vec<(&'static str, u64)>,
}

/// The status of a kept rebase `origin@seq` to `rev`.
fn kept(origin: &str, seq: u64, rev: Rev, lists: &Lists, waiting: &[&str]) -> Value {
    RebaseStatus {
        origin: origin.to_string(),
        seq,
        to: rev.digest(),
        hidden: lists.hidden.clone(),
        dropped: lists
            .dropped
            .iter()
            .map(|(o, s, f)| (o.to_string(), *s, f.to_string()))
            .collect(),
        discarded: lists.discarded.iter().map(|(o, s)| ev(o, *s)).collect(),
        waiting_for: waiting.iter().map(|s| s.to_string()).collect(),
        superseded: Vec::new(),
        superseded_rebases: lists
            .superseded_rebases
            .iter()
            .map(|(o, s)| ev(o, *s))
            .collect(),
    }
    .render()
}

// ---------------------------------------------------------------------------
// W, a join during the window
// ---------------------------------------------------------------------------

/// W: a join during the window, the donor having delivered the rebase and
/// written nothing since, and late edits reaching the joiner after its join.
///
/// Members a, b, c, j on every replica (the same members everywhere, as
/// SI.14's round-3 join test).
///
/// 1. `c`, without the rebase, writes under D1 `s` at the end of `Door`'s
///    name (`c@1`) and the stereotype `persistent` on `Car` (`c@2`).
/// 2. `a` rebases to D2 (`a@71`); `b` delivers it. `a` waits for b, c, j;
///    `b` for c, j.
/// 3. `j` adopts `b`'s state: D2, the running example under D2, the status
///    `b` reads with `j` waiting for `c` alone (`b` is heard: it delivered
///    the rebase; `a` wrote it). The intake answers as `b`'s.
/// 4. `j`, `a` and `b` deliver `c@1` and `c@2`, late: `Doors` (one register
///    write) and `persistent` everywhere; `j` still waits for `c`.
/// 5. `c` delivers the rebase and writes `hc` on `Sedan` under D2 (`c@3`),
///    which `a`, `b`, `j` deliver: `j` has heard every other member deliver
///    the rebase and lets D1 go.
/// 6. `j` writes `hj` on `Sedan` (`j@1`), which `a`, `b`, `c` deliver.
/// 7. `b` writes `hb` on `Sedan` (`b@1`), which `a`, `c`, `j` deliver: the
///    rebase is stable everywhere, one document.
#[test]
fn si16_w_a_replica_that_joins_during_the_window_holds_d1_translates_and_lets_it_go() {
    let all = ["a", "b", "c", "j"];
    let mut w = World::new("W", |_| all.to_vec());
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let none = Lists::default();
    let status = |waiting: &[&str]| kept("a", 71, Rev::D2, &none, waiting);

    // 1. c's two edits under D1.
    let c1 = w.send(C, d1.name_char(DOOR, 4, 's'), ("c", 1), "c's `s` under D1");
    let c2 = w.send(
        C,
        d1.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent"))),
        ("c", 2),
        "c's `persistent` under D1",
    );
    let mut on_c = Rev::D1.running_example();
    set(&mut on_c, "Door", "name", json!("Doors"));
    add_to_set(&mut on_c, "Car", "stereotype", &["persistent"]);
    w.check(C, Rev::D1, &on_c, &quiet(), "step 1, without the rebase");

    // 2. a's rebase; b delivers it.
    let op = rebase_op(&w.reps[A], Rev::D2, &[]);
    let r = w.send(A, op, ("a", 71), "a's rebase to D2");
    w.deliver(&[B], &r);
    let mut doc = Rev::D2.running_example();
    w.check(A, Rev::D2, &doc, &status(&["b", "c", "j"]), "step 2");
    w.check(B, Rev::D2, &doc, &status(&["c", "j"]), "step 2, the donor");

    // 3. j adopts b's state during the window.
    w.join(B);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&["c"]),
        "step 3, right after adopting b's state",
    );
    w.intake(
        B,
        &d2.rename_door("Gate"),
        &d1.name_char(DOOR, 4, 'x'),
        "step 3, D1 held",
    );

    // 4. c's late edits reach j, a and b.
    for to in [J, A, B] {
        w.deliver(&[to], &c1);
        w.deliver(&[to], &c2);
    }
    set(&mut doc, "Door", "name", json!("Doors"));
    add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&["c"]),
        "step 4, having translated c's two late edits",
    );
    w.check(A, Rev::D2, &doc, &status(&["b", "c", "j"]), "step 4");
    w.check(B, Rev::D2, &doc, &status(&["c", "j"]), "step 4");

    // 5. c delivers the rebase and writes under D2.
    w.deliver(&[C], &r);
    w.check(
        C,
        Rev::D2,
        &doc,
        &status(&["b", "j"]),
        "step 5, having delivered the rebase",
    );
    let c3 = w.send(
        C,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc"))),
        ("c", 3),
        "c's `hc` under D2",
    );
    w.deliver(&[A, B, J], &c3);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&[]),
        "step 5, having heard every other member deliver the rebase",
    );
    w.check(A, Rev::D2, &doc, &status(&["b", "j"]), "step 5");
    w.check(B, Rev::D2, &doc, &status(&["j"]), "step 5");

    // 6. j writes under D2.
    let hj = w.send(
        J,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hj"))),
        ("j", 1),
        "j's `hj` under D2",
    );
    w.deliver(&[A, B, C], &hj);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hj"]);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&[]),
        "step 6, after its own write",
    );
    w.check(A, Rev::D2, &doc, &status(&["b"]), "step 6");
    w.check(B, Rev::D2, &doc, &status(&[]), "step 6");
    w.check(C, Rev::D2, &doc, &status(&["b"]), "step 6");

    // 7. b writes under D2: stable everywhere.
    let hb = w.send(
        B,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb"))),
        ("b", 1),
        "b's `hb` under D2",
    );
    w.deliver(&[A, C, J], &hb);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb"]);
    for index in [A, B, C, J] {
        w.check(
            index,
            Rev::D2,
            &doc,
            &status(&[]),
            "step 7, stable everywhere",
        );
    }
    w.one_document();
    w.finish(
        "SI16-W: j joined by b's state during the window, waited for c alone, translated c's \
         late edits, let D1 go once c was heard; one document on a, b, c and j",
    );
}

// ---------------------------------------------------------------------------
// S, a join after the rebase is stable on the donor
// ---------------------------------------------------------------------------

/// S: a join after the rebase is causally stable on the donor, which has
/// let D1 go; the revision removes what `drops` names, so the joiner must
/// list what the others list as hidden and dropped.
///
/// `a`, `b`, `c` are bootstrapped with the members a, b, c; `j`, with j, a,
/// b, c, is a member none of them held when the rebase became stable on
/// `b`: a member that joins after the rebase is stable cannot be one the
/// donor held when it became so. No two register writes tie in this
/// schedule (ST.16 parked).
///
/// 1. `c`, without the rebase, writes `0` into `wheels`'s value under D1
///    (`c@1`).
/// 2. `a` rebases to D2_DROP naming `ModelElement.taggedValue` and
///    `TaggedValue` (`a@71`): `wheels` hidden.
/// 3. `b` delivers the rebase, then `c@1`, which it drops (it reaches
///    `ModelElement.taggedValue`).
/// 4. `c` delivers the rebase (dropping its own `c@1`) and writes `hc` on
///    `Sedan` under D2_DROP (`c@2`); `b` delivers it: `b` has heard `a` and
///    `c` deliver the rebase, which is stable on `b`, and lets D1 go. `a`
///    delivers `c@1` and `c@2` and waits for `b`.
/// 5. `j` adopts `b`'s state: D2_DROP, `wheels` hidden, `c@1` dropped,
///    waiting for no one, D1 not held. The intake answers as `b`'s, for a
///    character into `wheels`'s value and for a new `TaggedValue` on `Car`,
///    which D1's table routes on the model in force.
/// 6. `j` writes `hj` on `Sedan` (`j@1`), which `a`, `b`, `c` deliver; `j`
///    still waits for no one.
/// 7. `b` writes `hb` (`b@1`), which `a`, `c`, `j` deliver: one document.
#[test]
fn si16_s_a_replica_that_joins_after_the_rebase_is_stable_lists_what_the_others_list() {
    let mut w = World::new("S", |id| {
        if id == "j" {
            vec!["j", "a", "b", "c"]
        } else {
            vec!["a", "b", "c"]
        }
    });
    let d1 = Uml::new(D1);
    let drop_uml = Uml::new(D2_DROP);
    let hidden = Lists {
        hidden: vec![w.ids["wheels"].clone()],
        ..Lists::default()
    };
    let dropped = Lists {
        dropped: vec![("c", 1, "ModelElement.taggedValue")],
        ..hidden.clone()
    };
    let status = |lists: &Lists, waiting: &[&str]| kept("a", 71, Rev::Drop, lists, waiting);

    // 1. c's edit under D1, of what the revision removes.
    let c1 = w.send(
        C,
        d1.leaf(CAR_WHEELS, "value", LeafOp::InsertChar { pos: 1, ch: '0' }),
        ("c", 1),
        "c's `0` into wheels's value under D1",
    );

    // 2. a's rebase with drops.
    let op = rebase_op(&w.reps[A], Rev::Drop, &DROPS_TAGGED);
    let r = w.send(A, op, ("a", 71), "a's rebase to D2_DROP");
    let mut doc = Rev::Drop.running_example();
    w.check(A, Rev::Drop, &doc, &status(&hidden, &["b", "c"]), "step 2");

    // 3. b delivers the rebase, then c's late edit.
    w.deliver(&[B], &r);
    w.check(
        B,
        Rev::Drop,
        &doc,
        &status(&hidden, &["c"]),
        "step 3, the rebase",
    );
    w.deliver(&[B], &c1);
    w.check(
        B,
        Rev::Drop,
        &doc,
        &status(&dropped, &["c"]),
        "step 3, c's late edit dropped",
    );

    // 4. c delivers the rebase and writes under D2_DROP; b hears it.
    w.deliver(&[C], &r);
    w.check(
        C,
        Rev::Drop,
        &doc,
        &status(&dropped, &["b"]),
        "step 4, its own edit dropped",
    );
    let c2 = w.send(
        C,
        drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc"))),
        ("c", 2),
        "c's `hc` under D2_DROP",
    );
    w.deliver(&[B], &c2);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    w.check(
        B,
        Rev::Drop,
        &doc,
        &status(&dropped, &[]),
        "step 4, the rebase stable here, D1 let go",
    );
    w.deliver(&[A], &c1);
    w.deliver(&[A], &c2);
    w.check(A, Rev::Drop, &doc, &status(&dropped, &["b"]), "step 4");

    // 5. j adopts b's state, after the rebase is stable on b.
    w.join(B);
    w.check(
        J,
        Rev::Drop,
        &doc,
        &status(&dropped, &[]),
        "step 5, right after adopting b's state",
    );
    w.intake(
        B,
        &drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("x"))),
        &d1.leaf(CAR_WHEELS, "value", LeafOp::InsertChar { pos: 0, ch: '9' }),
        "step 5",
    );
    // An edit D1's table routes on the model in force and D2_DROP's does
    // not: a new TaggedValue on Car (spec, round 2, R10).
    w.intake(
        B,
        &drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("x"))),
        &d1.insert(CAR, "taggedValue", 0, "TaggedValue"),
        "step 5, a new TaggedValue on Car",
    );

    // 6. j writes under D2_DROP.
    let hj = w.send(
        J,
        drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hj"))),
        ("j", 1),
        "j's `hj` under D2_DROP",
    );
    w.deliver(&[A, B, C], &hj);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hj"]);
    w.check(
        J,
        Rev::Drop,
        &doc,
        &status(&dropped, &[]),
        "step 6, after its own write",
    );
    w.check(A, Rev::Drop, &doc, &status(&dropped, &["b"]), "step 6");
    w.check(B, Rev::Drop, &doc, &status(&dropped, &[]), "step 6");
    w.check(C, Rev::Drop, &doc, &status(&dropped, &["b"]), "step 6");

    // 7. b writes: stable everywhere.
    let hb = w.send(
        B,
        drop_uml.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb"))),
        ("b", 1),
        "b's `hb` under D2_DROP",
    );
    w.deliver(&[A, C, J], &hb);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb"]);
    for index in [A, B, C, J] {
        w.check(
            index,
            Rev::Drop,
            &doc,
            &status(&dropped, &[]),
            "step 7, stable everywhere",
        );
    }
    w.one_document();
    w.finish(
        "SI16-S: j joined by b's state after the rebase was stable on b, listed wheels hidden \
         and c@1 dropped as the others do, waited for no one; one document on a, b, c and j",
    );
}

// ---------------------------------------------------------------------------
// G, a join after two concurrent rebases
// ---------------------------------------------------------------------------

/// G: SI.14's RG-tie in the donor's history. `a` rebases to D2 (`a@71`,
/// stamp 71) and enables `Car.isLeaf` (`a@72`); `b`, concurrently, rebases
/// to D2_DROP (`b@1`, stamp 71) and adds `final` to `Door` (`b@2`). Equal
/// stamps: `a`'s, the least origin, is kept everywhere (SI.14 M22), `b@1`
/// superseded, `b@2` discarded.
///
/// Members a, b, c, j on every replica.
///
/// 1. `c` delivers `b@1`, `b@2`, `a@71`, `a@72` in that order (it switches
///    from D2_DROP to D2 on `a@71`).
/// 2. `j` adopts `c`'s state: D2 with `isLeaf`, `b@1` superseded, `b@2`
///    discarded, waiting for `b` alone (`a` wrote the kept rebase, `c` is
///    the donor). The intake answers as `c`'s.
/// 3. `a` delivers `b@1`, `b@2`; `b` delivers `a@71`, `a@72` and writes
///    `hb` on `Sedan` under D2 (`b@3`), which `a`, `c`, `j` deliver: `j` has
///    heard every other member deliver the kept rebase.
/// 4. `j` writes `hj` (`j@1`); then `c` writes `hc` (`c@1`): one document.
#[test]
fn si16_g_a_replica_that_joins_after_two_concurrent_rebases_lists_the_superseded_and_discarded() {
    let all = ["a", "b", "c", "j"];
    let mut w = World::new("G", |_| all.to_vec());
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let drop_uml = Uml::new(D2_DROP);
    let lists = Lists {
        discarded: vec![("b", 2)],
        superseded_rebases: vec![("b", 1)],
        ..Lists::default()
    };
    let status = |waiting: &[&str]| kept("a", 71, Rev::D2, &lists, waiting);

    // a's stream and b's stream, each written before any exchange.
    let op = rebase_op(&w.reps[A], Rev::D2, &[]);
    let r1 = w.send(A, op, ("a", 71), "a's rebase to D2");
    let a72 = w.send(
        A,
        d2.leaf(CAR, "isLeaf", LeafOp::Enable),
        ("a", 72),
        "a's `isLeaf` under D2",
    );
    let op = rebase_op(&w.reps[B], Rev::Drop, &DROPS_TAGGED);
    let r2 = w.send(B, op, ("b", 1), "b's rebase to D2_DROP");
    let b2 = w.send(
        B,
        drop_uml.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("final"))),
        ("b", 2),
        "b's `final` under D2_DROP",
    );

    // 1. c delivers b's stream, then a's.
    w.deliver(&[C], &r2);
    w.deliver(&[C], &b2);
    let mut on_drop = Rev::Drop.running_example();
    add_to_set(&mut on_drop, "Door", "stereotype", &["final"]);
    let wheels = Lists {
        hidden: vec![w.ids["wheels"].clone()],
        ..Lists::default()
    };
    w.check(
        C,
        Rev::Drop,
        &on_drop,
        &kept("b", 1, Rev::Drop, &wheels, &["a", "j"]),
        "step 1, b's rebase in force",
    );
    w.deliver(&[C], &r1);
    w.deliver(&[C], &a72);
    let mut doc = Rev::D2.running_example();
    set(&mut doc, "Car", "isLeaf", json!(true));
    w.check(
        C,
        Rev::D2,
        &doc,
        &status(&["b", "j"]),
        "step 1, a's rebase kept",
    );

    // 2. j adopts c's state.
    w.join(C);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&["b"]),
        "step 2, right after adopting c's state",
    );
    w.intake(
        C,
        &d2.rename_door("Gate"),
        &d1.name_char(DOOR, 4, 'x'),
        "step 2, D1 held",
    );

    // 3. a and b deliver each other's streams; b writes under D2.
    w.deliver(&[A], &r2);
    w.deliver(&[A], &b2);
    w.check(A, Rev::D2, &doc, &status(&["b", "c", "j"]), "step 3");
    w.deliver(&[B], &r1);
    w.deliver(&[B], &a72);
    w.check(
        B,
        Rev::D2,
        &doc,
        &status(&["c", "j"]),
        "step 3, a's rebase kept",
    );
    let hb = w.send(
        B,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb"))),
        ("b", 3),
        "b's `hb` under D2",
    );
    w.deliver(&[A, C, J], &hb);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb"]);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&[]),
        "step 3, having heard every other member deliver the kept rebase",
    );
    w.check(A, Rev::D2, &doc, &status(&["c", "j"]), "step 3");
    w.check(C, Rev::D2, &doc, &status(&["j"]), "step 3");

    // 4. j writes, then c: stable everywhere.
    let hj = w.send(
        J,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hj"))),
        ("j", 1),
        "j's `hj` under D2",
    );
    w.deliver(&[A, B, C], &hj);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hj"]);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&[]),
        "step 4, after its own write",
    );
    w.check(A, Rev::D2, &doc, &status(&["c"]), "step 4");
    w.check(B, Rev::D2, &doc, &status(&["c"]), "step 4");
    w.check(C, Rev::D2, &doc, &status(&[]), "step 4");
    let hc = w.send(
        C,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc"))),
        ("c", 1),
        "c's `hc` under D2",
    );
    w.deliver(&[A, B, J], &hc);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    for index in [A, B, C, J] {
        w.check(
            index,
            Rev::D2,
            &doc,
            &status(&[]),
            "step 4, stable everywhere",
        );
    }
    w.one_document();
    w.finish(
        "SI16-G: j joined by c's state after two concurrent rebases, listed b@1 superseded and \
         b@2 discarded as the others do, waited for b alone, then for no one; one document on a, \
         b, c and j",
    );
}

// ---------------------------------------------------------------------------
// R, a join that lets D1 go at the adoption
// ---------------------------------------------------------------------------

/// R: the donor still holds D1, waiting for the joiner alone, and every
/// other member's delivery of the rebase is in the donor's state, so the
/// joiner has heard every member but itself deliver the rebase the moment it
/// adopts that state: the rebase is causally stable on the joiner at the
/// adoption, and the joiner lets D1 go there (`ModelLog::adopted_from`),
/// while the donor holds D1 until it hears the joiner (spec, round 2).
///
/// Members a, b, c, j on every replica.
///
/// 1. `a` rebases to D2 (`a@71`); `b` and `c` deliver it.
/// 2. `b` writes `hb` on `Sedan` under D2 (`b@1`), which `a` and `c`
///    deliver; then `c` writes `hc` (`c@1`), which `a` and `b` deliver. `a`,
///    `b` and `c` each wait for `j` alone and hold D1.
/// 3. `j` adopts `b`'s state: D2, the document `b` serves, waiting for no
///    one and D1 let go (J3, J4); `b` still waits for `j` and holds D1. The
///    intake answers as `b`'s.
/// 4. `j` writes `hj` on `Sedan` (`j@1`), which `a`, `b`, `c` deliver: the
///    rebase is stable everywhere, D1 let go everywhere, one document.
#[test]
fn si16_r_a_replica_that_joins_once_every_other_member_is_heard_lets_d1_go_at_the_adoption() {
    let all = ["a", "b", "c", "j"];
    let mut w = World::new("R", |_| all.to_vec());
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let none = Lists::default();
    let status = |waiting: &[&str]| kept("a", 71, Rev::D2, &none, waiting);

    // 1. a's rebase; b and c deliver it.
    let op = rebase_op(&w.reps[A], Rev::D2, &[]);
    let r = w.send(A, op, ("a", 71), "a's rebase to D2");
    w.deliver(&[B, C], &r);
    let mut doc = Rev::D2.running_example();
    w.check(A, Rev::D2, &doc, &status(&["b", "c", "j"]), "step 1");
    w.check(B, Rev::D2, &doc, &status(&["c", "j"]), "step 1");
    w.check(C, Rev::D2, &doc, &status(&["b", "j"]), "step 1");

    // 2. b and c write under D2: a, b and c each wait for j alone.
    let hb = w.send(
        B,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb"))),
        ("b", 1),
        "b's `hb` under D2",
    );
    w.deliver(&[A, C], &hb);
    let hc = w.send(
        C,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc"))),
        ("c", 1),
        "c's `hc` under D2",
    );
    w.deliver(&[A, B], &hc);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb", "hc"]);
    for index in [A, B, C] {
        w.check(
            index,
            Rev::D2,
            &doc,
            &status(&["j"]),
            "step 2, waiting for j alone",
        );
    }
    assert!(
        w.holds_d1(B),
        "premise: R step 2: b, the donor, holds D1 while it waits for j (a rebase back to D1 \
         refused as `Rebasing`)"
    );

    // 3. j adopts b's state, and lets D1 go there.
    w.join(B);
    w.check(
        J,
        Rev::D2,
        &doc,
        &status(&[]),
        "step 3, right after adopting b's state",
    );
    w.window(J, false, "step 3, right after adopting b's state");
    w.check(B, Rev::D2, &doc, &status(&["j"]), "step 3, the donor");
    w.window(B, true, "step 3, the donor");
    w.intake(
        B,
        &d2.rename_door("Gate"),
        &d1.name_char(DOOR, 4, 'x'),
        "step 3",
    );

    // 4. j writes under D2: stable everywhere.
    let hj = w.send(
        J,
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hj"))),
        ("j", 1),
        "j's `hj` under D2",
    );
    w.deliver(&[A, B, C], &hj);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hj"]);
    for index in [A, B, C, J] {
        w.check(
            index,
            Rev::D2,
            &doc,
            &status(&[]),
            "step 4, stable everywhere",
        );
        w.window(index, false, "step 4, stable everywhere");
    }
    w.one_document();
    w.finish(
        "SI16-R: j joined by b's state while b held D1 for j alone, waited for no one and let D1 \
         go at the adoption; b let it go once it heard j; one document on a, b, c and j",
    );
}
