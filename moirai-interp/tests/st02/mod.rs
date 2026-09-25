//! ST.02 oracle, the part its two test files share: the metamodel, the
//! operations of the race, the schedules, and the document the rule fixes
//! for each schedule.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.02.md`. The claim (A): an
//! optional containment of a concrete class, `Cell`, that a removal above it
//! reaches while a concurrent write to a sibling keeps its owner alive,
//! converges, and every replica serves the value ST.01's rule gives: an
//! object the removal emptied, which nothing wrote after it, is dropped; an
//! object a concurrent write reached survives, holding that write.
//!
//! The metamodel is `st02.ecore` (in `arachne-model-plane`,
//! `arachne-codegen/tests/generated/st02/`), whose descriptor, as
//! `arachne describe` writes it, is `../fixtures/st02.metamodel.json`:
//!
//! ```text
//! Holder   root; tick: counter, inner: optional Box, bag: keyed Entry
//! Box      k: counter, note: optional counter, cell: optional Cell,
//!          mid: optional Mid
//! Mid      k: counter, cell: optional Cell
//! Cell     n: counter
//! Entry    key: text, value: Box
//! ```
//!
//! `note` serves the spec's observation OB1 only; nothing here writes it.
//!
//! Nothing here reads the code under test to decide what it expects: the
//! expected documents are written by hand from the schedule and the rule
//! ([`expected`], [`formed_doc`], [`unset_alone`], [`again`], [`rebuilt`]),
//! and the identities from which operation created each element and whether
//! each later writer had delivered the removal ([`Formed`]).
#![allow(dead_code)]

use std::collections::BTreeSet;

use moirai_interp::leaf::Scalar;
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::StateSnapshot;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Map, Value, json};

use super::si01::{ID, Lang, OPAQUE, Rep, all_ids, default_read, elements, inc_int, strip, three};

/// `st02.ecore`'s descriptor, the bytes `arachne describe st02.ecore` writes.
pub const ST02: &str = include_str!("../fixtures/st02.metamodel.json");

pub fn descriptor() -> Value {
    serde_json::from_str(ST02).expect("the fixture is JSON")
}

// ---------------------------------------------------------------------------
// Where the emptied optional sits, and the operations that reach it
// ---------------------------------------------------------------------------

/// Where the optional containment of `Cell` sits, and what removes it.
///
/// - `Flat`: `Box.cell` of the `Box` in `Holder.inner`; U unsets
///   `Holder.inner`, the optional holding the `Box`, one level above.
/// - `Deep`: `Mid.cell` of the `Mid` in `Box.mid` of the `Box` in
///   `Holder.inner`; U unsets `Holder.inner`, two levels above.
/// - `Keyed`: `Box.cell` of the `Box` at key `k` of the keyed `Holder.bag`;
///   U removes the key, one level above. A removed key keeps its entry (the
///   keyed collection's own rule), so the `Box` there is compared once every
///   replica has delivered every operation and not in between (spec OB2).
/// - `Direct`: `Box.cell` as in `Flat`, and U unsets `Box.cell` itself (a
///   write of the `Box` that unsets its cell). The guard: no removal above
///   the optional, both runtimes agree today and must keep agreeing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    Flat,
    Deep,
    Keyed,
    Direct,
}

impl Place {
    /// The three places of the claim.
    pub const CLAIM: [Place; 3] = [Place::Flat, Place::Deep, Place::Keyed];
    /// The places of the removal-alone guard: an optional place, where U
    /// alone leaves nothing, and `Direct`. A removed key alone is outside
    /// the claim (spec OB2).
    pub const ALONE: [Place; 3] = [Place::Flat, Place::Deep, Place::Direct];

    /// Whether a `Mid` stands between the `Box` and the `Cell`.
    pub fn deep(self) -> bool {
        self == Place::Deep
    }

    /// What W can write, in this place.
    pub fn targets(self) -> &'static [Target] {
        match self {
            Place::Deep => &[Target::Outer, Target::Sibling, Target::Object],
            Place::Flat | Place::Keyed | Place::Direct => &[Target::Sibling, Target::Object],
        }
    }
}

/// What W, the write concurrent with U, increments by 10.
///
/// - `Sibling`: the counter `k` of the `Cell`'s owner (`Box.k` in `Flat`,
///   `Keyed` and `Direct`, `Mid.k` in `Deep`): the owner is kept alive, the `Cell` is not
///   reached.
/// - `Object`: the `Cell`'s own `n`: the `Cell` is reached.
/// - `Outer` (`Deep` only): `Box.k`: the `Box` is kept alive, and neither the
///   `Mid` nor its `Cell` is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Outer,
    Sibling,
    Object,
}

/// Operations on `st02.ecore`'s model.
pub struct Tray(pub Lang);

impl Tray {
    pub fn new() -> Self {
        Tray(Lang::parse(&descriptor()))
    }

    /// T: an increment of `Holder.tick`, which creates the root.
    pub fn tick(&self) -> ModelOp {
        let l = &self.0;
        l.is("Holder", l.at("Holder", "tick", inc_int(1)))
            .into_model_op()
    }

    /// `op` on the `Box` in `Holder.inner`, as a write of that `Box`.
    fn in_box(&self, op: InstanceOp) -> ModelOp {
        self.in_place_box(Place::Flat, op)
    }

    /// `op` on the place's `Box`: the one in `Holder.inner`, or the one at
    /// key `k` of `Holder.bag`.
    fn in_place_box(&self, place: Place, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        let held = l.is("Box", op);
        let at = match place {
            Place::Keyed => l.at("Holder", "bag", InstanceOp::entry(Scalar::text("k"), held)),
            _ => l.at("Holder", "inner", InstanceOp::set(held)),
        };
        l.is("Holder", at).into_model_op()
    }

    /// An increment of `Cell.n` by `by`, as the value an optional of `Cell`
    /// is set to.
    fn cell(&self, by: i64) -> InstanceOp {
        let l = &self.0;
        InstanceOp::set(l.is("Cell", l.at("Cell", "n", inc_int(by))))
    }

    /// `op` on the `Mid` in `Box.mid`, as a write of that `Mid`.
    fn in_mid(&self, op: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.at("Box", "mid", InstanceOp::set(l.is("Mid", op)))
    }

    /// A write at the place that increments `target` by `by`: it reaches the
    /// object that holds the counter, and every object above it, or makes
    /// them. X (the `Cell`), Y (the owner's sibling) and W are writes, and so
    /// are the later writes.
    pub fn write(&self, place: Place, target: Target, by: i64) -> ModelOp {
        let l = &self.0;
        match (place, target) {
            (Place::Deep, Target::Object) => {
                self.in_box(self.in_mid(l.at("Mid", "cell", self.cell(by))))
            }
            (Place::Deep, Target::Sibling) => {
                self.in_box(self.in_mid(l.at("Mid", "k", inc_int(by))))
            }
            (_, Target::Object) => self.in_place_box(place, l.at("Box", "cell", self.cell(by))),
            (_, Target::Sibling) | (Place::Deep, Target::Outer) => {
                self.in_place_box(place, l.at("Box", "k", inc_int(by)))
            }
            (_, Target::Outer) => panic!("set-up: Outer is a target of the Deep place only"),
        }
    }

    /// U, the removal: `Holder.inner` unset (`Flat`, `Deep`), key `k` of
    /// `Holder.bag` removed (`Keyed`), or `Box.cell` unset by a write of the
    /// `Box` (`Direct`).
    pub fn removal(&self, place: Place) -> ModelOp {
        let l = &self.0;
        match place {
            Place::Flat | Place::Deep => l
                .is("Holder", l.at("Holder", "inner", InstanceOp::unset()))
                .into_model_op(),
            Place::Keyed => l
                .is(
                    "Holder",
                    l.at("Holder", "bag", InstanceOp::remove(Scalar::text("k"))),
                )
                .into_model_op(),
            Place::Direct => self.in_box(l.at("Box", "cell", InstanceOp::unset())),
        }
    }
}

// ---------------------------------------------------------------------------
// The schedules
// ---------------------------------------------------------------------------

/// Every assignment of two concurrent writers to two of the three replicas.
pub const SEATS: [(usize, usize); 6] = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)];

/// One schedule of the claim.
///
/// T: replica 0 increments `tick`, delivered everywhere. X: the replica
/// `xy.0` writes the `Cell` at the place, `n` +1 (which makes the `Box`, and
/// in `Deep` the `Mid`, if the replica has none); Y: the replica `xy.1`,
/// concurrently, increments the owner's `k` by 2 (`Box.k`, in `Deep`
/// `Mid.k`). Each writer delivers the other's operation, and the third
/// replica delivers both, X first when `x_first`. Every replica then holds
/// the owner with `k` 2 and its `Cell` with `n` 1. U: the replica `uw.0`
/// removes; W: the replica `uw.1`, concurrently, increments `target` by 10.
/// Each writer delivers the other's, and the third delivers both, U first
/// when `u_first`. After that every replica has delivered T, X, Y, U and W.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    pub place: Place,
    pub xy: (usize, usize),
    pub x_first: bool,
    pub uw: (usize, usize),
    pub u_first: bool,
    pub target: Target,
}

impl Schedule {
    pub fn label(&self) -> String {
        format!(
            "{:?}: X on {} Y on {}, third X first {}; U on {} W({:?}) on {}, third U first {}",
            self.place,
            self.xy.0,
            self.xy.1,
            self.x_first,
            self.uw.0,
            self.target,
            self.uw.1,
            self.u_first
        )
    }

    /// Whether the race drops the `Cell`: W did not reach it.
    pub fn drops_cell(&self) -> bool {
        self.target != Target::Object
    }

    /// Whether the race drops the `Mid` (`Deep`, W reached neither it nor
    /// its `Cell`).
    pub fn drops_mid(&self) -> bool {
        self.place.deep() && self.target == Target::Outer
    }
}

/// The schedules of one place: 6 seatings of X and Y, times 2 orders at the
/// third replica, times 6 seatings of U and W, times 2 orders at the third
/// replica, times the targets of the place (288 in `Flat`, `Keyed` and
/// `Direct`, 432 in `Deep`).
pub fn schedules(place: Place) -> Vec<Schedule> {
    let mut out = Vec::new();
    for xy in SEATS {
        for x_first in [true, false] {
            for uw in SEATS {
                for u_first in [true, false] {
                    for &target in place.targets() {
                        out.push(Schedule {
                            place,
                            xy,
                            x_first,
                            uw,
                            u_first,
                            target,
                        });
                    }
                }
            }
        }
    }
    out
}

/// A schedule played on three in-process replicas.
pub struct Played {
    pub reps: [Rep; 3],
    /// Every operation sent, in the order it was sent: T, X, Y, U, W.
    pub sent: Vec<EventMessage<ModelOp>>,
    /// The identified read replica 0 served once X and Y had been delivered
    /// everywhere, before U and W.
    pub formed: Value,
}

fn send_on(
    reps: &mut [Rep; 3],
    seat: usize,
    op: ModelOp,
    sent: &mut Vec<EventMessage<ModelOp>>,
    step: &mut impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) -> EventMessage<ModelOp> {
    let event = reps[seat]
        .send(op.clone())
        .unwrap_or_else(|why| panic!("set-up: replica {seat} refused {op:?}: {why:?}"));
    sent.push(event.clone());
    step(reps, sent);
    event
}

fn deliver_to(
    reps: &mut [Rep; 3],
    seat: usize,
    event: &EventMessage<ModelOp>,
    sent: &[EventMessage<ModelOp>],
    step: &mut impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) {
    reps[seat].receive(event.clone());
    step(reps, sent);
}

/// Two operations written concurrently on seats `p` and `q`, each delivered
/// to the other writer, and to the third seat in the order `first_p`.
fn race(
    reps: &mut [Rep; 3],
    (p, q): (usize, usize),
    op_p: ModelOp,
    op_q: ModelOp,
    first_p: bool,
    sent: &mut Vec<EventMessage<ModelOp>>,
    step: &mut impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) {
    let r = 3 - p - q;
    let ep = send_on(reps, p, op_p, sent, step);
    let eq = send_on(reps, q, op_q, sent, step);
    deliver_to(reps, p, &eq, sent, step);
    deliver_to(reps, q, &ep, sent, step);
    if first_p {
        deliver_to(reps, r, &ep, sent, step);
        deliver_to(reps, r, &eq, sent, step);
    } else {
        deliver_to(reps, r, &eq, sent, step);
        deliver_to(reps, r, &ep, sent, step);
    }
}

/// T, then X and Y raced; checks, as set-up, that every replica serves one
/// identified read and the formed document.
fn form(
    tray: &Tray,
    desc: &Value,
    place: Place,
    xy: (usize, usize),
    x_first: bool,
    label: &str,
    sent: &mut Vec<EventMessage<ModelOp>>,
    step: &mut impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) -> ([Rep; 3], Value) {
    let (a, b, c) = three(desc);
    let mut reps = [a, b, c];
    let t = send_on(&mut reps, 0, tray.tick(), sent, step);
    deliver_to(&mut reps, 1, &t, sent, step);
    deliver_to(&mut reps, 2, &t, sent, step);
    race(
        &mut reps,
        xy,
        tray.write(place, Target::Object, 1),
        tray.write(place, Target::Sibling, 2),
        x_first,
        sent,
        step,
    );
    let formed = identified_read(&reps[0]);
    for (seat, replica) in reps.iter().enumerate() {
        assert_eq!(
            identified_read(replica),
            formed,
            "set-up: {label}: replicas 0 and {seat} serve different documents once X and Y \
             were delivered, before U and W"
        );
        assert_eq!(
            default_read(replica),
            formed_doc(place),
            "set-up: {label}: replica {seat} does not serve the formed document"
        );
    }
    (reps, formed)
}

/// Play `schedule` on three fresh replicas of `st02.ecore`'s model, calling
/// `step` after every send and every delivery (15 of them) with the replicas
/// and what has been sent so far.
pub fn play(
    tray: &Tray,
    desc: &Value,
    schedule: &Schedule,
    mut step: impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) -> Played {
    let mut sent = Vec::new();
    let label = schedule.label();
    let (mut reps, formed) = form(
        tray,
        desc,
        schedule.place,
        schedule.xy,
        schedule.x_first,
        &label,
        &mut sent,
        &mut step,
    );
    race(
        &mut reps,
        schedule.uw,
        tray.removal(schedule.place),
        tray.write(schedule.place, schedule.target, 10),
        schedule.u_first,
        &mut sent,
        &mut step,
    );
    Played { reps, sent, formed }
}

/// U alone: X and Y as in [`play`], then `u` removes and every other replica
/// delivers it.
pub fn play_unset_alone(
    tray: &Tray,
    desc: &Value,
    place: Place,
    xy: (usize, usize),
    x_first: bool,
    u: usize,
) -> Played {
    let mut sent = Vec::new();
    let mut step = |_: &[Rep; 3], _: &[EventMessage<ModelOp>]| {};
    let label = format!("{place:?}: X on {} Y on {}, U alone on {u}", xy.0, xy.1);
    let (mut reps, formed) = form(tray, desc, place, xy, x_first, &label, &mut sent, &mut step);
    let removal = send_on(&mut reps, u, tray.removal(place), &mut sent, &mut step);
    for seat in 0..3 {
        if seat != u {
            deliver_to(&mut reps, seat, &removal, &sent, &mut step);
        }
    }
    Played { reps, sent, formed }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// The identified read, `QueryableLog::query_identified_state_json`.
pub fn identified_read(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

/// The place's `Box` in a served document, default or identified: the one
/// in `Holder.inner`, or the one at key `k` of `Holder.bag`.
pub fn boxed_of(place: Place, doc: &Value) -> Option<&Value> {
    match place {
        Place::Keyed => doc.get("bag").and_then(|bag| bag.get("k")),
        _ => doc.get("inner"),
    }
}

/// The `Mid` in a served document.
pub fn mid_of(place: Place, doc: &Value) -> Option<&Value> {
    boxed_of(place, doc).and_then(|boxed| boxed.get("mid"))
}

/// The `Cell` of the place in a served document.
pub fn cell_of(place: Place, doc: &Value) -> Option<&Value> {
    if place.deep() {
        mid_of(place, doc).and_then(|mid| mid.get("cell"))
    } else {
        boxed_of(place, doc).and_then(|boxed| boxed.get("cell"))
    }
}

fn id_in(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| value.get(ID))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The identities every replica served once X and Y had formed the model:
/// the root's, the `Box`'s, the `Mid`'s in `Deep`, and the `Cell`'s.
#[derive(Clone, Debug)]
pub struct Formed {
    pub root: String,
    pub boxed: String,
    pub mid: Option<String>,
    pub cell: String,
}

impl Formed {
    /// Read off the identified document served once formed; a failure here
    /// is a failure of the set-up, not of the claim.
    pub fn read(place: Place, formed: &Value, who: &str) -> Formed {
        let need = |value: Option<String>, what: &str| -> String {
            value.unwrap_or_else(|| panic!("set-up: {who}: no identity for {what} in {formed}"))
        };
        Formed {
            root: need(id_in(Some(formed)), "the root"),
            boxed: need(id_in(boxed_of(place, formed)), "the Box"),
            mid: place
                .deep()
                .then(|| need(id_in(mid_of(place, formed)), "the Mid")),
            cell: need(id_in(cell_of(place, formed)), "the Cell"),
        }
    }

    /// Every identity served once formed.
    pub fn all(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> =
            [self.root.clone(), self.boxed.clone(), self.cell.clone()].into();
        out.extend(self.mid.clone());
        out
    }
}

// ---------------------------------------------------------------------------
// The expected documents, by hand
// ---------------------------------------------------------------------------

/// A `Cell` holding `n`.
pub fn cell(n: i64) -> Value {
    json!({"eClass": "Cell", "n": n})
}

/// A `Mid` holding `k`, and a `Cell` when there is one.
pub fn mid(k: i64, held: Option<Value>) -> Value {
    let mut out = Map::new();
    out.insert("eClass".into(), json!("Mid"));
    out.insert("k".into(), json!(k));
    if let Some(held) = held {
        out.insert("cell".into(), held);
    }
    Value::Object(out)
}

/// A `Box` holding `k`, and a `Cell` and a `Mid` when there are. `note`, an
/// optional attribute nothing writes here, is an absent key.
pub fn boxed(k: i64, held: Option<Value>, below: Option<Value>) -> Value {
    let mut out = Map::new();
    out.insert("eClass".into(), json!("Box"));
    out.insert("k".into(), json!(k));
    if let Some(held) = held {
        out.insert("cell".into(), held);
    }
    if let Some(below) = below {
        out.insert("mid".into(), below);
    }
    Value::Object(out)
}

/// The model's whole default document, with the place's `Box` holding
/// `content`, or no `Box` at all: `inner` absent (and `bag` empty), or, in
/// `Keyed`, `bag` holding `content` at key `k`. `tick` reads 1 (T).
pub fn document_at(place: Place, content: Option<Value>) -> Value {
    let mut doc = json!({"bag": {}, "eClass": "Holder", "tick": 1});
    if let Some(content) = content {
        match place {
            Place::Keyed => doc["bag"] = json!({"k": content}),
            _ => doc["inner"] = content,
        }
    }
    doc
}

/// [`document_at`] for a place under `Holder.inner`.
pub fn document(content: Option<Value>) -> Value {
    document_at(Place::Flat, content)
}

/// What every replica serves once X and Y are delivered: the owner holding
/// Y's 2 and the `Cell` holding X's 1.
pub fn formed_doc(place: Place) -> Value {
    match place {
        Place::Deep => document(Some(boxed(0, None, Some(mid(2, Some(cell(1))))))),
        Place::Flat | Place::Keyed | Place::Direct => {
            document_at(place, Some(boxed(2, Some(cell(1)), None)))
        }
    }
}

/// The value the rule fixes, once every replica has delivered T, X, Y, U and
/// W (spec section 2). A removal above the optional (`Flat`, `Deep`,
/// `Keyed`) resets
/// what it had seen, X's 1 and Y's 2, and keeps W's 10 (update-wins). The
/// object W reached is served holding 10; every object the removal emptied
/// and W did not reach is dropped, the `Cell` and, in `Deep` with W on the
/// `Box`, the `Mid` too. In `Direct` U resets the `Cell` alone and the
/// `Box` keeps Y's 2 beside W's 10.
pub fn expected(schedule: &Schedule) -> Value {
    let content = match (schedule.place, schedule.target) {
        (Place::Flat | Place::Keyed, Target::Sibling) => boxed(10, None, None),
        (Place::Flat | Place::Keyed, Target::Object) => boxed(0, Some(cell(10)), None),
        (Place::Deep, Target::Outer) => boxed(10, None, None),
        (Place::Deep, Target::Sibling) => boxed(0, None, Some(mid(10, None))),
        (Place::Deep, Target::Object) => boxed(0, None, Some(mid(0, Some(cell(10))))),
        (Place::Direct, Target::Sibling) => boxed(12, None, None),
        (Place::Direct, Target::Object) => boxed(2, Some(cell(10)), None),
        (place, target) => panic!("set-up: no {target:?} in {place:?}"),
    };
    document_at(schedule.place, Some(content))
}

/// What U alone leaves: `Holder.inner` unset (`Flat`, `Deep`); the `Box`
/// holding Y's 2 and no `Cell` (`Direct`).
pub fn unset_alone(place: Place) -> Value {
    match place {
        Place::Flat | Place::Deep => document(None),
        Place::Direct => document(Some(boxed(2, None, None))),
        Place::Keyed => panic!("set-up: a removed key alone is outside the claim (OB2)"),
    }
}

// ---------------------------------------------------------------------------
// Judging one replica's reads
// ---------------------------------------------------------------------------

/// SI.01's contract R': the default read is the identified read with every
/// identity stripped. `Err` names the replica.
pub fn r_prime(replica: &Rep, who: &str) -> Result<(), String> {
    let doc = identified_read(replica);
    let plain = default_read(replica);
    if strip(&doc, OPAQUE) == plain {
        Ok(())
    } else {
        Err(format!(
            "{who}: the default read is not the identified read stripped: default {plain}, \
             identified {doc}"
        ))
    }
}

/// C3, on one identified document once the race is delivered (SI.01's C2 to
/// C5): the root and the `Box` keep the identities served once formed (the
/// `Box` is alive, W reached it); the `Mid` keeps its identity where W
/// reached it and its identity is served nowhere where it was dropped; the
/// `Cell` keeps its identity where W reached it and its identity is served
/// nowhere where it was dropped; no two elements share an identity.
pub fn identities(
    schedule: &Schedule,
    formed: &Formed,
    doc: &Value,
    who: &str,
) -> Result<(), String> {
    let mut why = Vec::new();
    let place = schedule.place;
    if id_in(Some(doc)).as_deref() != Some(formed.root.as_str()) {
        why.push(format!("the root is not served under {}", formed.root));
    }
    let served_box = id_in(boxed_of(place, doc));
    if served_box.as_deref() != Some(formed.boxed.as_str()) {
        why.push(format!(
            "the Box is served under {served_box:?}, not {}, the identity served once formed",
            formed.boxed
        ));
    }
    let ids = all_ids(doc);
    if let Some(formed_mid) = &formed.mid {
        if schedule.drops_mid() {
            if ids.contains(formed_mid) {
                why.push(format!(
                    "the emptied Mid's identity {formed_mid} is still served"
                ));
            }
        } else {
            let served = id_in(mid_of(place, doc));
            if served.as_deref() != Some(formed_mid.as_str()) {
                why.push(format!(
                    "the Mid W kept alive is served under {served:?}, not {formed_mid}"
                ));
            }
        }
    }
    if schedule.drops_cell() {
        if ids.contains(&formed.cell) {
            why.push(format!(
                "the emptied Cell's identity {} is still served",
                formed.cell
            ));
        }
    } else {
        let served = id_in(cell_of(place, doc));
        if served.as_deref() != Some(formed.cell.as_str()) {
            why.push(format!(
                "the Cell W reached is served under {served:?}, not {}, the identity its creator \
                 served once formed",
                formed.cell
            ));
        }
    }
    let count = elements(doc).len();
    if ids.len() != count {
        why.push(format!("{count} elements share {} identities", ids.len()));
    }
    if why.is_empty() {
        Ok(())
    } else {
        Err(format!("{who}: {}; served {doc}", why.join("; ")))
    }
}

// ---------------------------------------------------------------------------
// The writes after the race, and what they must leave
// ---------------------------------------------------------------------------

/// One replica's two reads, named.
pub struct Reads {
    pub who: String,
    pub identified: Value,
    pub default: Value,
}

/// Both reads of each replica, named.
pub fn reads_of(reps: &[&Rep], names: &[&str]) -> Vec<Reads> {
    reps.iter()
        .zip(names)
        .map(|(replica, who)| Reads {
            who: who.to_string(),
            identified: identified_read(replica),
            default: default_read(replica),
        })
        .collect()
}

/// The first later write: W's target again, +1, by a writer who has
/// delivered U and W. It reaches an object every writer sees alive and
/// moves no identity (SI.01's C4).
pub fn again_op(tray: &Tray, schedule: &Schedule) -> ModelOp {
    tray.write(schedule.place, schedule.target, 1)
}

/// What every replica serves after [`again_op`]: [`expected`] with W's
/// counter at 11.
pub fn again(schedule: &Schedule) -> Value {
    let content = match (schedule.place, schedule.target) {
        (Place::Flat | Place::Keyed, Target::Sibling) => boxed(11, None, None),
        (Place::Flat | Place::Keyed, Target::Object) => boxed(0, Some(cell(11)), None),
        (Place::Deep, Target::Outer) => boxed(11, None, None),
        (Place::Deep, Target::Sibling) => boxed(0, None, Some(mid(11, None))),
        (Place::Deep, Target::Object) => boxed(0, None, Some(mid(0, Some(cell(11))))),
        (Place::Direct, Target::Sibling) => boxed(13, None, None),
        (Place::Direct, Target::Object) => boxed(2, Some(cell(11)), None),
        (place, target) => panic!("set-up: no {target:?} in {place:?}"),
    };
    document_at(schedule.place, Some(content))
}

/// The second later write, only where the race dropped the `Cell`: the
/// `Cell` written again, `n` +5, by a writer who has delivered U. The `Cell`
/// (and in `Deep` with W on the `Box`, the `Mid`) was removed by U, so the
/// write creates it again (SI.01's C5).
pub fn rebuild_op(tray: &Tray, schedule: &Schedule) -> Option<ModelOp> {
    schedule
        .drops_cell()
        .then(|| tray.write(schedule.place, Target::Object, 5))
}

/// What every replica serves after [`again_op`] and [`rebuild_op`].
pub fn rebuilt(schedule: &Schedule) -> Value {
    let content = match (schedule.place, schedule.target) {
        (Place::Flat | Place::Keyed, Target::Sibling) => boxed(11, Some(cell(5)), None),
        (Place::Deep, Target::Outer) => boxed(11, None, Some(mid(0, Some(cell(5))))),
        (Place::Deep, Target::Sibling) => boxed(0, None, Some(mid(11, Some(cell(5))))),
        (Place::Direct, Target::Sibling) => boxed(13, Some(cell(5)), None),
        (place, target) => panic!("set-up: nothing is rebuilt for {target:?} in {place:?}"),
    };
    document_at(schedule.place, Some(content))
}

/// Every replica serves one identified read, and each one's default read is
/// `expected` and is its identified read stripped.
pub fn agreed_on(reads: &[Reads], expected: &Value) -> Vec<String> {
    let mut why = Vec::new();
    if reads
        .windows(2)
        .any(|pair| pair[0].identified != pair[1].identified)
    {
        why.push(format!(
            "the replicas serve different identified reads: {}",
            reads
                .iter()
                .map(|read| format!("{} {}", read.who, read.identified))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    for read in reads {
        if read.default != *expected {
            why.push(format!(
                "{} serves {}, not {expected}",
                read.who, read.default
            ));
        }
        if strip(&read.identified, OPAQUE) != read.default {
            why.push(format!(
                "{}'s default read is not its identified read stripped: {}",
                read.who, read.identified
            ));
        }
    }
    why
}

/// After [`again_op`], delivered everywhere: [`again`] on every replica, and
/// every identity of [`identities`] where it was (nothing moved).
pub fn check_again(schedule: &Schedule, formed: &Formed, reads: &[Reads]) -> Vec<String> {
    let mut why = agreed_on(reads, &again(schedule));
    if let Err(line) = identities(schedule, formed, &reads[0].identified, &reads[0].who) {
        why.push(format!("after the write W's target again: {line}"));
    }
    why
}

/// After [`rebuild_op`], delivered everywhere: [`rebuilt`] on every replica;
/// the root and the `Box` keep their identities; the `Mid` keeps its
/// identity where it was kept and is new where it was dropped; the `Cell` is
/// served under an identity outside `seen` (every identity served before),
/// and its old identity is served nowhere (SI.01's C5).
pub fn check_rebuilt(
    schedule: &Schedule,
    formed: &Formed,
    seen: &BTreeSet<String>,
    reads: &[Reads],
) -> Vec<String> {
    let place = schedule.place;
    let mut why = agreed_on(reads, &rebuilt(schedule));
    let doc = &reads[0].identified;
    if id_in(Some(doc)).as_deref() != Some(formed.root.as_str()) {
        why.push(format!("the root's identity moved: {doc}"));
    }
    if id_in(boxed_of(place, doc)).as_deref() != Some(formed.boxed.as_str()) {
        why.push(format!("the Box's identity moved: {doc}"));
    }
    if let Some(formed_mid) = &formed.mid {
        let served = id_in(mid_of(place, doc));
        if schedule.drops_mid() {
            match &served {
                Some(id) if !seen.contains(id) => {}
                other => why.push(format!(
                    "the Mid made again after U is served under {other:?}, which is not an \
                     identity nothing carried before"
                )),
            }
        } else if served.as_deref() != Some(formed_mid.as_str()) {
            why.push(format!(
                "the Mid W kept alive is served under {served:?}, not {formed_mid}"
            ));
        }
    }
    match id_in(cell_of(place, doc)) {
        Some(id) if !seen.contains(&id) => {}
        found => why.push(format!(
            "the Cell written again after U is served under {found:?}, which is not an \
             identity nothing carried before"
        )),
    }
    let ids = all_ids(doc);
    if ids.contains(&formed.cell) {
        why.push(format!(
            "the removed Cell's identity {} is served again: {doc}",
            formed.cell
        ));
    }
    if schedule.drops_mid()
        && let Some(formed_mid) = &formed.mid
        && ids.contains(formed_mid)
    {
        why.push(format!(
            "the removed Mid's identity {formed_mid} is served again: {doc}"
        ));
    }
    why
}

// ---------------------------------------------------------------------------
// A joiner by state transfer, in process
// ---------------------------------------------------------------------------

/// A fourth replica, `d`, joining by state transfer from `donor`: it is
/// bootstrapped knowing only itself on the donor's log, and adopts the
/// donor's causal snapshot and its log, each through a serde round trip, as
/// `GenericNode::adopt_state` installs what a donor sent over the wire.
pub fn joiner(donor: &Rep) -> Rep {
    let log_wire = serde_json::to_string(donor.log()).expect("set-up: the donor's log serializes");
    let log: ModelLog =
        serde_json::from_str(&log_wire).expect("set-up: the donor's log comes back");
    let snapshot_wire =
        serde_json::to_string(&donor.snapshot()).expect("set-up: the donor's snapshot serializes");
    let snapshot: StateSnapshot<ModelOp> =
        serde_json::from_str(&snapshot_wire).expect("set-up: the donor's snapshot comes back");
    let mut d: Rep =
        Replica::bootstrap_with_log_id("d".to_string(), &["d"], donor.log_id().clone());
    d.adopt(snapshot, log);
    d
}
