//! ST.05 round 3, in process: K7 to K9 of
//! `cea-cdrt-knowledge/spec/items/ST.05.md` (its section "Round 3, written
//! before any rerun").
//!
//! A record class (a class with no subclass) that reaches itself through
//! keyed collections: directly, `Mid.sub` holding `Mid`, and through another
//! record class, `Box.sub` holding `Mid` and `Mid.boxes` holding `Box`. The
//! keyed read leaves out an entry that reads as a freshly minted value of the
//! collection would, and a fresh `Mid` or `Box` holds keyed collections of
//! its own; round 1's implementation (`d813a55`) asked for that fresh value
//! for every keyed collection it read, empty or not, so the first read of
//! any model of such a metamodel recursed without end and overflowed the
//! stack. Round 2's record found it with a probe
//! (`results/validation/ST.05/round-2-probe-recursive-keyed-class.rs`),
//! which K7 promotes as it was.
//!
//! Every replica is read in both forms after every send and every delivery
//! (the identified read stripped must be the default read, R'), and at every
//! checkpoint, once every operation sent has been delivered everywhere, the
//! three replicas must serve one default and one identified read, the
//! document the keyed collection's rule gives (spec section 2), every element
//! under an identity of its own, the identities of the objects kept where
//! they were and those of the objects left out served nowhere (SI.01). The
//! expected documents are written by hand below; nothing here reads the code
//! under test to decide what it expects.
//!
//! The metamodels are `st05.ecore`'s descriptor with containments added to
//! `Mid` in code, as the probe built its own: K7's gives `Mid` the keyed
//! `sub` of `Mid` alone, K8's and K9's give it `sub` and `boxes`, a keyed
//! collection of `Box`. No twin compares the generated runtime here: the
//! crate Arachne generates for such a metamodel does not compile (spec,
//! round 3).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st05_recursive_keyed_class -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod si01;

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::leaf::Scalar;
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::replica::IsReplica;
use serde_json::{Value, json};
use si01::{ID, Lang, OPAQUE, Rep, all_ids, default_read, elements, inc_int, strip, three};

/// `st05.ecore`'s descriptor, the bytes `arachne describe st05.ecore` writes.
const ST05: &str = include_str!("fixtures/st05.metamodel.json");

/// The round-2 probe's metamodel: `st05.ecore`'s descriptor with `Box.sub`,
/// a keyed containment of `Mid`, copied onto `Mid`, so `Mid.sub` holds
/// `Mid`s.
fn probe_descriptor() -> Value {
    let mut d: Value = serde_json::from_str(ST05).expect("set-up: the fixture is JSON");
    let sub = d["classes"]["Box"]["containments"][1].clone();
    assert_eq!(
        sub["name"], "sub",
        "set-up: Box's second containment is sub"
    );
    d["classes"]["Mid"]["containments"] = Value::Array(vec![sub]);
    d
}

/// The probe's metamodel, and `Holder.bag`, a keyed containment of `Box`,
/// copied onto `Mid` as `boxes`: `Mid` reaches itself through `Mid.sub`, and
/// `Box` through `Box.sub` and `Mid.boxes`.
fn cycles_descriptor() -> Value {
    let mut d = probe_descriptor();
    let mut boxes = d["classes"]["Holder"]["containments"][1].clone();
    assert_eq!(
        boxes["name"], "bag",
        "set-up: Holder's second containment is bag"
    );
    boxes["name"] = json!("boxes");
    d["classes"]["Mid"]["containments"]
        .as_array_mut()
        .expect("set-up: Mid's containments are an array")
        .push(boxes);
    d
}

// ---------------------------------------------------------------------------
// The operations
// ---------------------------------------------------------------------------

fn key(k: &str) -> Scalar {
    Scalar::text(k)
}

struct Ops(Lang);

impl Ops {
    fn new(desc: &Value) -> Self {
        Ops(Lang::parse(desc))
    }

    fn holder(&self, inner: InstanceOp) -> ModelOp {
        self.0.is("Holder", inner).into_model_op()
    }

    /// T: `Holder.tick` +1, the first write, which touches no keyed
    /// collection.
    fn tick(&self) -> ModelOp {
        self.holder(self.0.at("Holder", "tick", inc_int(1)))
    }

    /// `op` on the `Box` at key `k` of `Holder.bag`.
    fn in_box(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        self.holder(l.at(
            "Holder",
            "bag",
            InstanceOp::entry(key("k"), l.is("Box", op)),
        ))
    }

    /// `op` on the `Mid` at key `j` of that `Box`'s `sub`.
    fn in_mid(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        self.in_box(l.at("Box", "sub", InstanceOp::entry(key("j"), l.is("Mid", op))))
    }

    /// `k` +`by` on the `Mid` at key `i` of that `Mid`'s own `sub`: N1 (the
    /// probe's write, two keyed levels below `bag`), and W on `Inner`.
    fn inner(&self, by: i64) -> ModelOp {
        let l = &self.0;
        self.in_mid(l.at(
            "Mid",
            "sub",
            InstanceOp::entry(key("i"), l.is("Mid", l.at("Mid", "k", inc_int(by)))),
        ))
    }

    /// `k` +`by` on the `Box` at key `d` of that `Mid`'s `boxes`: N2, and W
    /// on `Back`.
    fn back(&self, by: i64) -> ModelOp {
        let l = &self.0;
        self.in_mid(l.at(
            "Mid",
            "boxes",
            InstanceOp::entry(key("d"), l.is("Box", l.at("Box", "k", inc_int(by)))),
        ))
    }

    /// `k` +`by` on the `Mid` at key `j`: W on `Joint`.
    fn joint(&self, by: i64) -> ModelOp {
        self.in_mid(self.0.at("Mid", "k", inc_int(by)))
    }

    /// U at a place.
    fn removal(&self, place: Place) -> ModelOp {
        let l = &self.0;
        match place {
            Place::Top => self.holder(l.at("Holder", "bag", InstanceOp::remove(key("k")))),
            Place::Direct => self.in_mid(l.at("Mid", "sub", InstanceOp::remove(key("i")))),
            Place::Indirect => self.in_mid(l.at("Mid", "boxes", InstanceOp::remove(key("d")))),
        }
    }

    /// W, +10 on its target.
    fn write(&self, target: Target) -> ModelOp {
        match target {
            Target::Joint => self.joint(10),
            Target::Inner => self.inner(10),
            Target::Back => self.back(10),
        }
    }
}

/// Where U removes a key: `Top`, key `k` of `Holder.bag`; `Direct`, key `i`
/// of the `Mid` at `j`'s own `sub` (the collection of the direct cycle);
/// `Indirect`, key `d` of that `Mid`'s `boxes` (the collection that closes
/// the indirect cycle).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Place {
    Top,
    Direct,
    Indirect,
}

/// What W increments by 10, concurrently with U at `Top`: the `Mid` at `j`
/// (`Joint`), the `Mid` at `i` in its `sub` (`Inner`), the `Box` at `d` in
/// its `boxes` (`Back`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Joint,
    Inner,
    Back,
}

// ---------------------------------------------------------------------------
// The documents the rule gives, by hand
// ---------------------------------------------------------------------------

/// A `Box` as the default read serves it: `note` and `mid` never written, so
/// absent.
fn boxed(k: i64, sub: Value) -> Value {
    json!({"eClass": "Box", "k": k, "sub": sub})
}

/// A `Mid` of the probe's metamodel.
fn probe_mid(k: i64, sub: Value) -> Value {
    json!({"eClass": "Mid", "k": k, "sub": sub})
}

/// A `Mid` of the cycles' metamodel.
fn mid(k: i64, boxes: Value, sub: Value) -> Value {
    json!({"boxes": boxes, "eClass": "Mid", "k": k, "sub": sub})
}

/// The whole document: `tick` 1, `bag` holding `held` at key `k` or
/// nothing, `inner` never written, `shapes` and `bins` empty.
fn document(held: Option<Value>) -> Value {
    let bag = match held {
        Some(held) => json!({ "k": held }),
        None => json!({}),
    };
    json!({"bag": bag, "bins": {}, "eClass": "Holder", "shapes": {}, "tick": 1})
}

/// K7, once the probe's write is delivered everywhere.
fn probe_written() -> Value {
    document(Some(boxed(
        0,
        json!({"j": probe_mid(0, json!({"i": probe_mid(1, json!({}))}))}),
    )))
}

/// K8 and K9, once N1 is delivered everywhere.
fn after_inner() -> Value {
    document(Some(boxed(
        0,
        json!({"j": mid(0, json!({}), json!({"i": mid(1, json!({}), json!({}))}))}),
    )))
}

/// K8 and K9, once N1 and N2 are delivered everywhere: the formed document.
fn formed() -> Value {
    document(Some(boxed(
        0,
        json!({"j": mid(
            0,
            json!({"d": boxed(1, json!({}))}),
            json!({"i": mid(1, json!({}), json!({}))}),
        )}),
    )))
}

/// K8, once U alone is delivered everywhere. U resets what it had seen and
/// the entry stays; an emptied `Box` or `Mid` reads as a fresh one and is
/// left out, one level after another.
fn alone(place: Place) -> Value {
    match place {
        Place::Top => document(None),
        Place::Direct => document(Some(boxed(
            0,
            json!({"j": mid(0, json!({"d": boxed(1, json!({}))}), json!({}))}),
        ))),
        Place::Indirect => after_inner(),
    }
}

/// K9, once U and W are delivered everywhere: W's +10 survives U, what U had
/// seen is reset, and every object left emptied is left out.
fn raced(target: Target) -> Value {
    let j = match target {
        Target::Joint => mid(10, json!({}), json!({})),
        Target::Inner => mid(0, json!({}), json!({"i": mid(10, json!({}), json!({}))})),
        Target::Back => mid(0, json!({"d": boxed(10, json!({}))}), json!({})),
    };
    document(Some(boxed(0, json!({ "j": j }))))
}

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

/// An element the operations create: the root `Holder`, the `Box` at `k`
/// (`BoxK`), the `Mid` at `j` in its `sub` (`MidJ`), the `Mid` at `i` in
/// that `Mid`'s `sub` (`MidI`), the `Box` at `d` in that `Mid`'s `boxes`
/// (`BoxD`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    Root,
    BoxK,
    MidJ,
    MidI,
    BoxD,
}

const ALL: [Role; 5] = [Role::Root, Role::BoxK, Role::MidJ, Role::MidI, Role::BoxD];

fn path(role: Role) -> &'static [&'static str] {
    match role {
        Role::Root => &[],
        Role::BoxK => &["bag", "k"],
        Role::MidJ => &["bag", "k", "sub", "j"],
        Role::MidI => &["bag", "k", "sub", "j", "sub", "i"],
        Role::BoxD => &["bag", "k", "sub", "j", "boxes", "d"],
    }
}

/// The identity the element of `role` carries in an identified read, if it
/// is served.
fn id_at(doc: &Value, role: Role) -> Option<String> {
    let mut at = doc;
    for step in path(role) {
        at = at.get(*step)?;
    }
    at.get(ID).and_then(Value::as_str).map(str::to_string)
}

/// The objects kept and left out once a removal alone at `place` is
/// delivered everywhere.
fn alone_roles(place: Place) -> (&'static [Role], &'static [Role]) {
    match place {
        Place::Top => (
            &[Role::Root],
            &[Role::BoxK, Role::MidJ, Role::MidI, Role::BoxD],
        ),
        Place::Direct => (
            &[Role::Root, Role::BoxK, Role::MidJ, Role::BoxD],
            &[Role::MidI],
        ),
        Place::Indirect => (
            &[Role::Root, Role::BoxK, Role::MidJ, Role::MidI],
            &[Role::BoxD],
        ),
    }
}

/// The objects kept and left out once U at `Top` and W on `target` are
/// delivered everywhere: W's writer had not delivered U, so every object on
/// W's path is alive for it and keeps its identity.
fn raced_roles(target: Target) -> (&'static [Role], &'static [Role]) {
    match target {
        Target::Joint => (
            &[Role::Root, Role::BoxK, Role::MidJ],
            &[Role::MidI, Role::BoxD],
        ),
        Target::Inner => (
            &[Role::Root, Role::BoxK, Role::MidJ, Role::MidI],
            &[Role::BoxD],
        ),
        Target::Back => (
            &[Role::Root, Role::BoxK, Role::MidJ, Role::BoxD],
            &[Role::MidI],
        ),
    }
}

/// Every element's identity in an identified read, each non-empty and no two
/// alike.
fn element_ids(doc: &Value) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    for element in elements(doc) {
        match element.get(ID).and_then(Value::as_str) {
            Some(id) if !id.is_empty() => ids.push(id.to_string()),
            other => {
                return Err(format!(
                    "an element is served with no identity ({other:?}): {}",
                    Value::Object(element.clone())
                ));
            }
        }
    }
    let distinct: BTreeSet<&String> = ids.iter().collect();
    if distinct.len() != ids.len() {
        return Err(format!("two elements share an identity: {ids:?}"));
    }
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Replicas, reads and verdicts
// ---------------------------------------------------------------------------

/// The identified read, `QueryableLog::query_identified_state_json`.
fn identified_read(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

/// Every read made and every failure seen, by kind; a checkpoint counts once
/// per kind it fails.
#[derive(Default)]
struct Tally {
    checkpoints: usize,
    parted: usize,
    value: usize,
    identity: usize,
    r_prime: usize,
    step_reads: usize,
    step_r_prime: usize,
    lines: Vec<String>,
}

impl Tally {
    fn failures(&self) -> usize {
        self.parted + self.value + self.identity + self.r_prime + self.step_r_prime
    }

    fn line(&self, name: &str, runs: usize) -> String {
        format!(
            "{name} runs {runs}: checkpoints {}, parted {}, value other than the rule's {}, \
             identity {}, r' {}; reads after every step {}, r' {}",
            self.checkpoints,
            self.parted,
            self.value,
            self.identity,
            self.r_prime,
            self.step_reads,
            self.step_r_prime
        )
    }

    /// Both reads of every replica after a step: R' holds on each.
    fn look(&mut self, reps: &[Rep; 3], label: &str) {
        for (seat, replica) in reps.iter().enumerate() {
            let default = default_read(replica);
            let identified = identified_read(replica);
            self.step_reads += 1;
            if strip(&identified, OPAQUE) != default {
                self.step_r_prime += 1;
                self.lines.push(format!(
                    "{label}: after a step, replica {seat}'s identified read stripped is not \
                     its default read: {identified} | {default}"
                ));
            }
        }
    }

    /// A checkpoint: every replica has delivered every operation sent. One
    /// default and one identified read, `want`, R', and the identities:
    /// exactly the `kept` elements served, each under the identity `known`
    /// holds for it if it holds one, and no identity `known` holds for a
    /// `left_out` role served anywhere. Answers the identities replica 0
    /// serves for the kept roles.
    fn checkpoint(
        &mut self,
        reps: &[Rep; 3],
        label: &str,
        want: &Value,
        (kept, left_out): (&[Role], &[Role]),
        known: &BTreeMap<Role, String>,
    ) -> BTreeMap<Role, String> {
        self.checkpoints += 1;
        let defaults: Vec<Value> = reps.iter().map(default_read).collect();
        let ids: Vec<Value> = reps.iter().map(identified_read).collect();
        if defaults.windows(2).any(|p| p[0] != p[1]) || ids.windows(2).any(|p| p[0] != p[1]) {
            self.parted += 1;
            self.lines.push(format!(
                "{label}: the replicas part: default reads {} | identified reads {}",
                defaults
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join(" / "),
                ids.iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join(" / ")
            ));
        }
        let mut value = false;
        let mut r_prime = false;
        let mut identity = false;
        for seat in 0..3 {
            if defaults[seat] != *want {
                value = true;
                self.lines.push(format!(
                    "{label}: replica {seat} serves {}, not {want}",
                    defaults[seat]
                ));
            }
            if strip(&ids[seat], OPAQUE) != defaults[seat] {
                r_prime = true;
                self.lines.push(format!(
                    "{label}: replica {seat}'s identified read stripped is not its default \
                     read: {} | {}",
                    ids[seat], defaults[seat]
                ));
            }
            match element_ids(&ids[seat]) {
                Err(why) => {
                    identity = true;
                    self.lines.push(format!("{label}: replica {seat}: {why}"));
                }
                Ok(served) if served.len() != kept.len() => {
                    identity = true;
                    self.lines.push(format!(
                        "{label}: replica {seat} serves {} elements where {} are kept ({kept:?}): \
                         {}",
                        served.len(),
                        kept.len(),
                        ids[seat]
                    ));
                }
                Ok(_) => {}
            }
            for &role in kept {
                match (id_at(&ids[seat], role), known.get(&role)) {
                    (None, _) => {
                        identity = true;
                        self.lines.push(format!(
                            "{label}: replica {seat} serves no {role:?} under an identity: {}",
                            ids[seat]
                        ));
                    }
                    (Some(id), Some(was)) if id != *was => {
                        identity = true;
                        self.lines.push(format!(
                            "{label}: replica {seat} serves {role:?} under {id}, which was {was}"
                        ));
                    }
                    _ => {}
                }
            }
            let served = all_ids(&ids[seat]);
            for role in left_out {
                if let Some(was) = known.get(role)
                    && served.contains(was)
                {
                    identity = true;
                    self.lines.push(format!(
                        "{label}: replica {seat} still serves {was}, the identity of the {role:?} \
                         left out"
                    ));
                }
            }
        }
        self.value += usize::from(value);
        self.r_prime += usize::from(r_prime);
        self.identity += usize::from(identity);
        kept.iter()
            .filter_map(|&role| id_at(&ids[0], role).map(|id| (role, id)))
            .collect()
    }
}

/// Three replicas of one model, the tally every step reads into, and the
/// run's label.
struct Run<'a> {
    reps: [Rep; 3],
    tally: &'a mut Tally,
    label: String,
}

impl Run<'_> {
    fn send(&mut self, seat: usize, op: ModelOp) -> EventMessage<ModelOp> {
        let event = self.reps[seat]
            .send(op.clone())
            .unwrap_or_else(|why| panic!("set-up: replica {seat} refused {op:?}: {why:?}"));
        self.tally.look(&self.reps, &self.label);
        event
    }

    fn deliver(&mut self, seat: usize, event: &EventMessage<ModelOp>) {
        self.reps[seat].receive(event.clone());
        self.tally.look(&self.reps, &self.label);
    }

    /// `op` sent on `seat` and delivered to the two others.
    fn broadcast(&mut self, seat: usize, op: ModelOp) {
        let event = self.send(seat, op);
        for other in 0..3 {
            if other != seat {
                self.deliver(other, &event);
            }
        }
    }

    /// Two operations written concurrently on seats `p` and `q`, each
    /// delivered to the other writer, then to the third seat, `op_p` first
    /// when `first_p`.
    fn race(&mut self, (p, q): (usize, usize), op_p: ModelOp, op_q: ModelOp, first_p: bool) {
        let r = 3 - p - q;
        let ep = self.send(p, op_p);
        let eq = self.send(q, op_q);
        self.deliver(p, &eq);
        self.deliver(q, &ep);
        if first_p {
            self.deliver(r, &ep);
            self.deliver(r, &eq);
        } else {
            self.deliver(r, &eq);
            self.deliver(r, &ep);
        }
    }

    fn checkpoint(
        &mut self,
        at: &str,
        want: &Value,
        roles: (&[Role], &[Role]),
        known: &BTreeMap<Role, String>,
    ) -> BTreeMap<Role, String> {
        let label = format!("{}, {at}", self.label);
        self.tally
            .checkpoint(&self.reps, &label, want, roles, known)
    }

    /// K8's and K9's formation on the cycles' metamodel: T, N1 and N2, each
    /// from replica 0 and delivered to the others, each followed by a
    /// checkpoint. Answers the identities of the five formed elements.
    fn form(&mut self, ops: &Ops) -> BTreeMap<Role, String> {
        self.broadcast(0, ops.tick());
        let root = self.checkpoint(
            "after T",
            &document(None),
            (&[Role::Root], &[]),
            &BTreeMap::new(),
        );
        self.broadcast(0, ops.inner(1));
        let four = self.checkpoint(
            "after N1",
            &after_inner(),
            (&[Role::Root, Role::BoxK, Role::MidJ, Role::MidI], &[]),
            &root,
        );
        self.broadcast(0, ops.back(1));
        self.checkpoint("after N2", &formed(), (&ALL, &[]), &four)
    }
}

/// Every assignment of U and W to two of the three replicas.
const SEATS: [(usize, usize); 6] = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)];

fn report(name: &str, runs: usize, failing_runs: usize, tally: &Tally) {
    println!("{}, failing runs {failing_runs}", tally.line(name, runs));
    assert!(
        tally.failures() == 0,
        "{}, failing runs {failing_runs}; first failures:\n{}",
        tally.line(name, runs),
        tally
            .lines
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ---------------------------------------------------------------------------
// K7 to K9
// ---------------------------------------------------------------------------

/// K7, the round-2 probe as it was, on its metamodel (`Mid.sub` of `Mid`):
/// replica 0 increments `tick`, then writes `k` +1 into the `Mid` at `i` of
/// the `Mid` at `j` of the `Box` at `k`, each delivered to the others;
/// replica 1 removes `k` from `bag`, delivered to the others. Every replica
/// is read after every step, and at each of the three checkpoints serves the
/// document the rule gives.
#[test]
fn st05_k7_the_round_2_probe_reads_a_class_whose_keyed_collection_holds_its_own_class() {
    let desc = probe_descriptor();
    let ops = Ops::new(&desc);
    let mut tally = Tally::default();
    let (a, b, c) = three(&desc);
    let mut run = Run {
        reps: [a, b, c],
        tally: &mut tally,
        label: "probe".to_string(),
    };
    run.broadcast(0, ops.tick());
    let root = run.checkpoint(
        "after T",
        &document(None),
        (&[Role::Root], &[]),
        &BTreeMap::new(),
    );
    run.broadcast(0, ops.inner(1));
    let written = run.checkpoint(
        "after the write",
        &probe_written(),
        (&[Role::Root, Role::BoxK, Role::MidJ, Role::MidI], &[]),
        &root,
    );
    run.broadcast(1, ops.removal(Place::Top));
    run.checkpoint(
        "after the removal",
        &document(None),
        (&[Role::Root], &[Role::BoxK, Role::MidJ, Role::MidI]),
        &written,
    );
    let failing = usize::from(tally.failures() > 0);
    report("ST05-K7", 1, failing, &tally);
}

/// K8, a removal alone on the cycles' metamodel: the formation, then U at
/// each place (`Top`, `Direct`, `Indirect`) from each replica, delivered to
/// the others (9 runs, 4 checkpoints each).
#[test]
fn st05_k8_a_removal_alone_in_classes_that_reach_themselves_through_keyed_collections() {
    let desc = cycles_descriptor();
    let ops = Ops::new(&desc);
    let mut tally = Tally::default();
    let mut runs = 0;
    let mut failing = 0;
    for place in [Place::Top, Place::Direct, Place::Indirect] {
        for u in 0..3 {
            let before = tally.failures();
            let (a, b, c) = three(&desc);
            let mut run = Run {
                reps: [a, b, c],
                tally: &mut tally,
                label: format!("{place:?}: U alone on {u}"),
            };
            let formed_ids = run.form(&ops);
            run.broadcast(u, ops.removal(place));
            run.checkpoint("after U", &alone(place), alone_roles(place), &formed_ids);
            runs += 1;
            failing += usize::from(tally.failures() > before);
        }
    }
    report("ST05-K8", runs, failing, &tally);
}

/// K9, a removal raced by a write inside either cycle, on the cycles'
/// metamodel: the formation, then U (key `k` of `bag`) on one replica and W
/// (+10 on the `Mid` at `j`, the `Mid` at `i` or the `Box` at `d`) on
/// another, concurrently; each writer delivers the other's, and the third
/// replica delivers both in either order (6 seatings x 2 orders x 3 targets,
/// 36 schedules, 4 checkpoints each).
#[test]
fn st05_k9_a_removal_raced_by_a_write_inside_either_cycle_converges_on_the_rule() {
    let desc = cycles_descriptor();
    let ops = Ops::new(&desc);
    let mut tally = Tally::default();
    let mut runs = 0;
    let mut failing = 0;
    for target in [Target::Joint, Target::Inner, Target::Back] {
        for (u, w) in SEATS {
            for u_first in [true, false] {
                let before = tally.failures();
                let (a, b, c) = three(&desc);
                let mut run = Run {
                    reps: [a, b, c],
                    tally: &mut tally,
                    label: format!("U on {u} W({target:?}) on {w}, third U first {u_first}"),
                };
                let formed_ids = run.form(&ops);
                run.race((u, w), ops.removal(Place::Top), ops.write(target), u_first);
                run.checkpoint(
                    "after U and W",
                    &raced(target),
                    raced_roles(target),
                    &formed_ids,
                );
                runs += 1;
                failing += usize::from(tally.failures() > before);
            }
        }
    }
    report("ST05-K9", runs, failing, &tally);
}
