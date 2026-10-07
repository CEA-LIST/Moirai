//! ST.05 oracle, the part its two test files share: the metamodel, the
//! operations, the schedules, and the document the rule fixes for each.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.05.md`. The claim: a keyed
//! collection whose values are objects serves one document on every
//! replica, and the same on both runtimes, once a key removal (alone, or
//! raced by a write into the removed key's object) has been delivered
//! everywhere; and that document is the rule's. The rule is the keyed
//! collection's own (`uw_map.rs`'s read): a removed key stays, and the read
//! leaves out an entry that reads as a freshly minted value of the
//! collection would. At a key of a class with no subclass, which the
//! generated runtime composes as a `record!`, a freshly minted value is the
//! object at its default, so an object the removal emptied and nothing wrote
//! after is left out, one level deeper as well. At a key of a class with
//! subclasses, a `union!`, a freshly minted value holds no object; under
//! ST.10's rule (`cea-cdrt-knowledge/spec/items/ST.10.md`, B2) an object
//! every write to which the removal had seen is absent, so such a key whose
//! every object the removal emptied is left out too, and an object a
//! concurrent write reached stays alone. ST.10 re-pinned the `Union`,
//! `UnionOne`, `UnionDeep` and `Family` values below, which kept the emptied
//! objects at the key.
//!
//! The metamodel is `st05.ecore` (in `arachne-model-plane`,
//! `arachne-codegen/tests/generated/st05/`), whose descriptor, as
//! `arachne describe` writes it, is `../fixtures/st05.metamodel.json`:
//!
//! ```text
//! Holder   root; tick: counter, inner: optional Box, bag: keyed Box,
//!          shapes: keyed Shape, bins: keyed Bin
//! Box      k: counter, note: optional counter, mid: optional Mid,
//!          sub: keyed Mid
//! Mid      k: counter
//! Shape    abstract; n: counter, parts: keyed Mid; A, B concrete
//! Bin      k: counter; BigBin, a concrete subclass
//! ```
//!
//! Nothing here reads the code under test to decide what it expects: the
//! expected documents are written by hand from the schedule and the rule
//! ([`formed_doc`], [`expected`], [`alone`], [`again`], [`rebuilt`],
//! [`again_alone`]), and the identities from which operation created each
//! element and whether each later writer had delivered the removal
//! ([`Formed`], [`race_roles`], [`alone_roles`]).
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::leaf::Scalar;
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::StateSnapshot;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Map, Value, json};

use super::si01::{ID, Lang, OPAQUE, Rep, all_ids, default_read, elements, inc_int, strip, three};

/// `st05.ecore`'s descriptor, the bytes `arachne describe st05.ecore` writes.
pub const ST05: &str = include_str!("../fixtures/st05.metamodel.json");

pub fn descriptor() -> Value {
    serde_json::from_str(ST05).expect("the fixture is JSON")
}

// ---------------------------------------------------------------------------
// Where the removed key sits, and the operations that reach it
// ---------------------------------------------------------------------------

/// Which keyed collection a key is removed from, and what the removed key's
/// object holds.
///
/// - `Record`: key `k` of `Holder.bag`, a `Box` (no subclass). X writes
///   `Box.k`, Y the optional attribute `Box.note`.
/// - `RecordDeep`: key `k` of `Holder.bag`, a `Box` holding one level deeper
///   the `Mid` at key `j` of its own keyed `Box.sub` (X) and the `Mid` of its
///   optional `Box.mid` (Y).
/// - `RecordInner`: the keyed collection itself one level deeper: key `j` of
///   `Box.sub` of the `Box` in `Holder.inner`, a `Mid` (X); Y writes that
///   `Box`'s `k`. U removes key `j` by a write of the `Box`.
/// - `Union`: key `k` of `Holder.shapes` (the abstract `Shape`), the conflict
///   of an `A` (X) and a `B` (Y), ST.01's keyed guard.
/// - `UnionOne`: key `k` of `Holder.shapes`, one `A`, written by X and Y.
/// - `UnionDeep`: key `k` of `Holder.shapes`, an `A` holding the `Mid` at key
///   `j` of its keyed `Shape.parts` (X); Y writes the `A`'s `n`.
/// - `Family`: key `k` of `Holder.bins`, a `Bin`, a concrete class with a
///   subclass, written by X and Y.
///
/// Every removal but `RecordInner`'s removes key `k` of the collection at the
/// root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    Record,
    RecordDeep,
    RecordInner,
    Union,
    UnionOne,
    UnionDeep,
    Family,
}

impl Place {
    pub const ALL: [Place; 7] = [
        Place::Record,
        Place::RecordDeep,
        Place::RecordInner,
        Place::Union,
        Place::UnionOne,
        Place::UnionDeep,
        Place::Family,
    ];

    /// What W can write, in this place.
    pub fn targets(self) -> &'static [Target] {
        match self {
            Place::RecordDeep => &[Target::Member, Target::Sub, Target::Mid],
            Place::Union => &[Target::Member, Target::Other],
            Place::UnionDeep => &[Target::Member, Target::Sub],
            _ => &[Target::Member],
        }
    }

    /// X and Y, the two concurrent writes that form the entry.
    pub fn formation(self) -> (Target, Target) {
        match self {
            Place::Record => (Target::Member, Target::Note),
            Place::RecordDeep => (Target::Sub, Target::Mid),
            Place::RecordInner => (Target::Member, Target::Owner),
            Place::Union => (Target::Member, Target::Other),
            Place::UnionOne | Place::Family => (Target::Member, Target::Member),
            Place::UnionDeep => (Target::Sub, Target::Member),
        }
    }

    /// The object at the removed key: the role written again after a
    /// removal alone.
    pub fn member(self) -> Role {
        match self {
            Place::Record | Place::RecordDeep => Role::Box,
            Place::RecordInner => Role::Sub,
            Place::Union | Place::UnionOne | Place::UnionDeep => Role::A,
            Place::Family => Role::Bin,
        }
    }

    /// Every object the formation makes, by role.
    pub fn roles(self) -> &'static [Role] {
        match self {
            Place::Record => &[Role::Box],
            Place::RecordDeep => &[Role::Box, Role::MidOpt, Role::Sub],
            Place::RecordInner => &[Role::Box, Role::Sub],
            Place::Union => &[Role::A, Role::B],
            Place::UnionOne => &[Role::A],
            Place::UnionDeep => &[Role::A, Role::Sub],
            Place::Family => &[Role::Bin],
        }
    }
}

/// What a write increments, in a place.
///
/// - `Member`: the removed key's own object: `Box.k` (`Record`,
///   `RecordDeep`), the `Mid`'s `k` at key `j` (`RecordInner`), `A.n`
///   (`Union`, `UnionOne`, `UnionDeep`), `Bin.k` (`Family`).
/// - `Sub`: one level deeper, the `Mid`'s `k` at key `j` of `Box.sub`
///   (`RecordDeep`) or of `A.parts` (`UnionDeep`).
/// - `Mid`: one level deeper, the `k` of the `Mid` in the optional `Box.mid`
///   (`RecordDeep`).
/// - `Other`: `B.n` (`Union`).
/// - `Note`: the optional attribute `Box.note` (`Record`, Y only).
/// - `Owner`: the `k` of the `Box` in `Holder.inner` that holds the keyed
///   collection (`RecordInner`, Y only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Member,
    Sub,
    Mid,
    Other,
    Note,
    Owner,
}

/// Operations on `st05.ecore`'s model.
pub struct Tray(pub Lang);

impl Tray {
    pub fn new() -> Self {
        Tray(Lang::parse(&descriptor()))
    }

    fn holder(&self, op: InstanceOp) -> ModelOp {
        self.0.is("Holder", op).into_model_op()
    }

    /// T: an increment of `Holder.tick`, which creates the root.
    pub fn tick(&self) -> ModelOp {
        self.holder(self.0.at("Holder", "tick", inc_int(1)))
    }

    /// A `Mid` whose `k` is incremented by `by`.
    fn mid_k(&self, by: i64) -> InstanceOp {
        let l = &self.0;
        l.is("Mid", l.at("Mid", "k", inc_int(by)))
    }

    /// `op` on the `Box` at key `k` of `Holder.bag`.
    fn in_bag(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        self.holder(l.at(
            "Holder",
            "bag",
            InstanceOp::entry(Scalar::text("k"), l.is("Box", op)),
        ))
    }

    /// `op` on the `Box` in `Holder.inner`, as a write of that `Box`.
    fn in_inner(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        self.holder(l.at("Holder", "inner", InstanceOp::set(l.is("Box", op))))
    }

    /// `op` on the `class` member at key `k` of `Holder.shapes`.
    fn in_shapes(&self, class: &str, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        self.holder(l.at(
            "Holder",
            "shapes",
            InstanceOp::entry(Scalar::text("k"), l.is(class, op)),
        ))
    }

    /// `op` on the `Bin` at key `k` of `Holder.bins`.
    fn in_bins(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        self.holder(l.at(
            "Holder",
            "bins",
            InstanceOp::entry(Scalar::text("k"), l.is("Bin", op)),
        ))
    }

    /// A write at the place that increments `target` by `by`: it reaches the
    /// object that holds the counter, and every object above it, or makes
    /// them. X, Y, W and the later writes are all writes.
    pub fn write(&self, place: Place, target: Target, by: i64) -> ModelOp {
        let l = &self.0;
        let j = || Scalar::text("j");
        match (place, target) {
            (Place::Record | Place::RecordDeep, Target::Member) => {
                self.in_bag(l.at("Box", "k", inc_int(by)))
            }
            (Place::Record, Target::Note) => {
                self.in_bag(l.at("Box", "note", InstanceOp::set(inc_int(by))))
            }
            (Place::RecordDeep, Target::Sub) => {
                self.in_bag(l.at("Box", "sub", InstanceOp::entry(j(), self.mid_k(by))))
            }
            (Place::RecordDeep, Target::Mid) => {
                self.in_bag(l.at("Box", "mid", InstanceOp::set(self.mid_k(by))))
            }
            (Place::RecordInner, Target::Member) => {
                self.in_inner(l.at("Box", "sub", InstanceOp::entry(j(), self.mid_k(by))))
            }
            (Place::RecordInner, Target::Owner) => self.in_inner(l.at("Box", "k", inc_int(by))),
            (Place::Union | Place::UnionOne | Place::UnionDeep, Target::Member) => {
                self.in_shapes("A", l.at("A", "n", inc_int(by)))
            }
            (Place::Union, Target::Other) => self.in_shapes("B", l.at("B", "n", inc_int(by))),
            (Place::UnionDeep, Target::Sub) => self.in_shapes(
                "A",
                l.at("A", "parts", InstanceOp::entry(j(), self.mid_k(by))),
            ),
            (Place::Family, Target::Member) => self.in_bins(l.at("Bin", "k", inc_int(by))),
            (place, target) => panic!("set-up: no {target:?} in {place:?}"),
        }
    }

    /// U, the key removal: key `k` of the place's collection at the root,
    /// or, in `RecordInner`, key `j` of `Box.sub` by a write of the `Box`.
    pub fn removal(&self, place: Place) -> ModelOp {
        let l = &self.0;
        let k = InstanceOp::remove(Scalar::text("k"));
        match place {
            Place::Record | Place::RecordDeep => self.holder(l.at("Holder", "bag", k)),
            Place::RecordInner => {
                self.in_inner(l.at("Box", "sub", InstanceOp::remove(Scalar::text("j"))))
            }
            Place::Union | Place::UnionOne | Place::UnionDeep => {
                self.holder(l.at("Holder", "shapes", k))
            }
            Place::Family => self.holder(l.at("Holder", "bins", k)),
        }
    }
}

// ---------------------------------------------------------------------------
// The schedules
// ---------------------------------------------------------------------------

/// Every assignment of two concurrent writers to two of the three replicas.
pub const SEATS: [(usize, usize); 6] = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)];

/// One schedule of the race.
///
/// T: replica 0 increments `tick`, delivered everywhere. X: the replica
/// `xy.0` makes the first write of the place's formation (+1); Y: the
/// replica `xy.1`, concurrently, the second (+2). Each writer delivers the
/// other's operation, and the third replica delivers both, X first when
/// `x_first`. Every replica then serves [`formed_doc`]. U: the replica
/// `uw.0` removes the key; W: the replica `uw.1`, concurrently, increments
/// `target` by 10. Each writer delivers the other's, and the third delivers
/// both, U first when `u_first`. After that every replica has delivered T, X,
/// Y, U and W.
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

    /// Whether the race leaves out the `Mid` at key `j` (W reached neither
    /// it nor anything that keeps it).
    pub fn drops_sub(&self) -> bool {
        race_roles(self).1.contains(&Role::Sub)
    }
}

/// The schedules of one place: 6 seatings of X and Y, times 2 orders at the
/// third replica, times 6 seatings of U and W, times 2 orders at the third
/// replica, times the targets of the place (144 per target).
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
    /// Every operation sent, in the order it was sent.
    pub sent: Vec<EventMessage<ModelOp>>,
    /// The identified read replica 0 served once X and Y had been delivered
    /// everywhere, before the removal.
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
    let (x, y) = place.formation();
    race(
        &mut reps,
        xy,
        tray.write(place, x, 1),
        tray.write(place, y, 2),
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
             were delivered, before the removal"
        );
        assert_eq!(
            default_read(replica),
            formed_doc(place),
            "set-up: {label}: replica {seat} does not serve the formed document"
        );
    }
    (reps, formed)
}

/// Play `schedule` on three fresh replicas of `st05.ecore`'s model, calling
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

/// The removal alone: X and Y as in [`play`], then `u` removes the key and
/// every other replica delivers it; `step` after every send and every
/// delivery (12 of them).
pub fn play_alone(
    tray: &Tray,
    desc: &Value,
    place: Place,
    xy: (usize, usize),
    x_first: bool,
    u: usize,
    mut step: impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) -> Played {
    let mut sent = Vec::new();
    let label = alone_label(place, xy, x_first, u);
    let (mut reps, formed) = form(tray, desc, place, xy, x_first, &label, &mut sent, &mut step);
    let removal = send_on(&mut reps, u, tray.removal(place), &mut sent, &mut step);
    for seat in 0..3 {
        if seat != u {
            deliver_to(&mut reps, seat, &removal, &sent, &mut step);
        }
    }
    Played { reps, sent, formed }
}

pub fn alone_label(place: Place, xy: (usize, usize), x_first: bool, u: usize) -> String {
    format!(
        "{place:?}: X on {} Y on {}, third X first {x_first}; U alone on {u}",
        xy.0, xy.1
    )
}

// ---------------------------------------------------------------------------
// Reads, and the objects in them
// ---------------------------------------------------------------------------

/// The identified read, `QueryableLog::query_identified_state_json`.
pub fn identified_read(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

/// An object the formation makes.
///
/// - `Box`: the `Box` at key `k` of `bag`, or in `Holder.inner`
///   (`RecordInner`).
/// - `MidOpt`: the `Mid` in that `Box`'s optional `mid`.
/// - `Sub`: the `Mid` at key `j` of that `Box`'s `sub`, or of the `A`'s
///   `parts` (`UnionDeep`).
/// - `A`, `B`: the members at key `k` of `shapes`.
/// - `Bin`: the `Bin` at key `k` of `bins`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Box,
    MidOpt,
    Sub,
    A,
    B,
    Bin,
}

/// The member of class `class` in what a key serves: the object itself, or
/// one of a conflict's.
fn member_of<'a>(held: &'a Value, class: &str) -> Option<&'a Value> {
    match held.get("__conflict").and_then(Value::as_array) {
        Some(members) => members.iter().find(|member| member["eClass"] == class),
        None => (held["eClass"] == class).then_some(held),
    }
}

/// Where `role` sits in a served document, default or identified.
pub fn role_in<'a>(place: Place, role: Role, doc: &'a Value) -> Option<&'a Value> {
    match role {
        Role::Box => match place {
            Place::RecordInner => doc.get("inner"),
            _ => doc.get("bag")?.get("k"),
        },
        Role::MidOpt => role_in(place, Role::Box, doc)?.get("mid"),
        Role::Sub => match place {
            Place::UnionDeep => role_in(place, Role::A, doc)?.get("parts")?.get("j"),
            _ => role_in(place, Role::Box, doc)?.get("sub")?.get("j"),
        },
        Role::A => member_of(doc.get("shapes")?.get("k")?, "A"),
        Role::B => member_of(doc.get("shapes")?.get("k")?, "B"),
        Role::Bin => doc.get("bins")?.get("k"),
    }
}

fn id_in(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| value.get(ID))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The identities every replica served once X and Y had formed the model:
/// the root's, and each object's of [`Place::roles`].
#[derive(Clone, Debug)]
pub struct Formed {
    pub root: String,
    pub ids: BTreeMap<Role, String>,
}

impl Formed {
    /// Read off the identified document served once formed; a failure here
    /// is a failure of the set-up, not of the claim.
    pub fn read(place: Place, formed: &Value, who: &str) -> Formed {
        let root = id_in(Some(formed))
            .unwrap_or_else(|| panic!("set-up: {who}: no identity for the root in {formed}"));
        let ids = place
            .roles()
            .iter()
            .map(|&role| {
                let id = id_in(role_in(place, role, formed)).unwrap_or_else(|| {
                    panic!("set-up: {who}: no identity for {role:?} in {formed}")
                });
                (role, id)
            })
            .collect();
        Formed { root, ids }
    }

    /// Every identity served once formed.
    pub fn all(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = self.ids.values().cloned().collect();
        out.insert(self.root.clone());
        out
    }

    pub fn of(&self, role: Role) -> &str {
        self.ids
            .get(&role)
            .map(String::as_str)
            .unwrap_or_else(|| panic!("set-up: no formed identity for {role:?}"))
    }
}

// ---------------------------------------------------------------------------
// The expected documents, by hand
// ---------------------------------------------------------------------------

/// A `Mid` holding `k`.
pub fn mid(k: i64) -> Value {
    json!({"eClass": "Mid", "k": k})
}

/// A keyed collection of `Mid`s, as the canonical form spells it.
fn keyed(entries: &[(&str, Value)]) -> Value {
    Value::Object(
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect(),
    )
}

/// A `Box` holding `k`, `note` when there is one, the `Mid` in `mid` when
/// there is one, and `sub`.
pub fn boxed(k: i64, note: Option<i64>, held: Option<Value>, sub: &[(&str, Value)]) -> Value {
    let mut out = Map::new();
    out.insert("eClass".into(), json!("Box"));
    out.insert("k".into(), json!(k));
    if let Some(note) = note {
        out.insert("note".into(), json!(note));
    }
    if let Some(held) = held {
        out.insert("mid".into(), held);
    }
    out.insert("sub".into(), keyed(sub));
    Value::Object(out)
}

/// An `A` or a `B` holding `n` and `parts`.
pub fn shape(class: &str, n: i64, parts: &[(&str, Value)]) -> Value {
    json!({"eClass": class, "n": n, "parts": keyed(parts)})
}

/// The conflict of two members, ordered by class name.
pub fn conflict(first: Value, second: Value) -> Value {
    let mut members = vec![first, second];
    members.sort_by(|l, r| l["eClass"].as_str().cmp(&r["eClass"].as_str()));
    json!({"__conflict": members})
}

/// A `Bin` holding `k`.
pub fn bin(k: i64) -> Value {
    json!({"eClass": "Bin", "k": k})
}

/// The model's whole default document, the place's object holding
/// `content`, or absent: at key `k` of `bag`, `shapes` or `bins`, or, in
/// `RecordInner`, as `Holder.inner`. `tick` reads 1 (T).
pub fn document(place: Place, content: Option<Value>) -> Value {
    let mut doc = json!({"bag": {}, "bins": {}, "eClass": "Holder", "shapes": {}, "tick": 1});
    if let Some(content) = content {
        match place {
            Place::Record | Place::RecordDeep => doc["bag"] = json!({"k": content}),
            Place::RecordInner => doc["inner"] = content,
            Place::Union | Place::UnionOne | Place::UnionDeep => {
                doc["shapes"] = json!({"k": content})
            }
            Place::Family => doc["bins"] = json!({"k": content}),
        }
    }
    doc
}

/// What every replica serves once X and Y are delivered.
pub fn formed_doc(place: Place) -> Value {
    let content = match place {
        Place::Record => boxed(1, Some(2), None, &[]),
        Place::RecordDeep => boxed(0, None, Some(mid(2)), &[("j", mid(1))]),
        Place::RecordInner => boxed(2, None, None, &[("j", mid(1))]),
        Place::Union => conflict(shape("A", 1, &[]), shape("B", 2, &[])),
        Place::UnionOne => shape("A", 3, &[]),
        Place::UnionDeep => shape("A", 2, &[("j", mid(1))]),
        Place::Family => bin(3),
    };
    document(place, Some(content))
}

/// The race's value, `w` being what W's target holds: the removal resets
/// what it had seen (X's and Y's writes) and keeps W's (update-wins); an
/// object the removal emptied that W did not reach is left out at a key of
/// a class with no subclass (the `Mid` at key `j`) and by an optional (the
/// `Mid` in `Box.mid`, ST.02; `Box.note`, ST.04), and, at a key of a class
/// with subclasses, is absent (the other member of `Union`, ST.10).
fn race_value(schedule: &Schedule, w: i64) -> Value {
    let content = match (schedule.place, schedule.target) {
        (Place::Record, Target::Member) => boxed(w, None, None, &[]),
        (Place::RecordDeep, Target::Member) => boxed(w, None, None, &[]),
        (Place::RecordDeep, Target::Sub) => boxed(0, None, None, &[("j", mid(w))]),
        (Place::RecordDeep, Target::Mid) => boxed(0, None, Some(mid(w)), &[]),
        (Place::RecordInner, Target::Member) => boxed(2, None, None, &[("j", mid(w))]),
        (Place::Union, Target::Member) => shape("A", w, &[]),
        (Place::Union, Target::Other) => shape("B", w, &[]),
        (Place::UnionOne, Target::Member) => shape("A", w, &[]),
        (Place::UnionDeep, Target::Member) => shape("A", w, &[]),
        (Place::UnionDeep, Target::Sub) => shape("A", 0, &[("j", mid(w))]),
        (Place::Family, Target::Member) => bin(w),
        (place, target) => panic!("set-up: no {target:?} in {place:?}"),
    };
    document(schedule.place, Some(content))
}

/// The value the rule fixes once every replica has delivered T, X, Y, U and
/// W: W's target holds 10.
pub fn expected(schedule: &Schedule) -> Value {
    race_value(schedule, 10)
}

/// What the removal alone leaves: at a key of a class with no subclass,
/// nothing (`Record`, `RecordDeep`: the whole `Box` emptied; `RecordInner`:
/// the `Mid` at key `j`, beside the `Box`'s own `k` 2, which U did not
/// remove); at a key of a class with subclasses, nothing either, since the
/// removal had seen every write to every object there (ST.10: `Union`,
/// `UnionOne`, `UnionDeep`, `Family`).
pub fn alone(place: Place) -> Value {
    let content = match place {
        Place::Record | Place::RecordDeep => None,
        Place::RecordInner => Some(boxed(2, None, None, &[])),
        Place::Union | Place::UnionOne | Place::UnionDeep | Place::Family => None,
    };
    document(place, content)
}

/// Which objects of the formation the race keeps (served under the
/// identity served once formed) and which it leaves out (their identity
/// served nowhere).
pub fn race_roles(schedule: &Schedule) -> (&'static [Role], &'static [Role]) {
    match (schedule.place, schedule.target) {
        (Place::Record, _) => (&[Role::Box], &[]),
        (Place::RecordDeep, Target::Member) => (&[Role::Box], &[Role::MidOpt, Role::Sub]),
        (Place::RecordDeep, Target::Sub) => (&[Role::Box, Role::Sub], &[Role::MidOpt]),
        (Place::RecordDeep, Target::Mid) => (&[Role::Box, Role::MidOpt], &[Role::Sub]),
        (Place::RecordInner, _) => (&[Role::Box, Role::Sub], &[]),
        (Place::Union, Target::Member) => (&[Role::A], &[Role::B]),
        (Place::Union, _) => (&[Role::B], &[Role::A]),
        (Place::UnionOne, _) => (&[Role::A], &[]),
        (Place::UnionDeep, Target::Member) => (&[Role::A], &[Role::Sub]),
        (Place::UnionDeep, _) => (&[Role::A, Role::Sub], &[]),
        (Place::Family, _) => (&[Role::Bin], &[]),
        (place, target) => panic!("set-up: no {target:?} in {place:?}"),
    }
}

/// Which objects the removal alone keeps and which it leaves out.
pub fn alone_roles(place: Place) -> (&'static [Role], &'static [Role]) {
    match place {
        Place::Record => (&[], &[Role::Box]),
        Place::RecordDeep => (&[], &[Role::Box, Role::MidOpt, Role::Sub]),
        Place::RecordInner => (&[Role::Box], &[Role::Sub]),
        Place::Union => (&[], &[Role::A, Role::B]),
        Place::UnionOne => (&[], &[Role::A]),
        Place::UnionDeep => (&[], &[Role::A, Role::Sub]),
        Place::Family => (&[], &[Role::Bin]),
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

/// SI.01's C2 to C5 on one identified document: the root keeps its
/// identity; each role of `kept` is served under the identity served once
/// formed; the identity of each role of `left_out` is served nowhere; no two
/// elements share an identity.
pub fn identities(
    place: Place,
    formed: &Formed,
    kept: &[Role],
    left_out: &[Role],
    doc: &Value,
    who: &str,
) -> Result<(), String> {
    let mut why = Vec::new();
    if id_in(Some(doc)).as_deref() != Some(formed.root.as_str()) {
        why.push(format!("the root is not served under {}", formed.root));
    }
    for &role in kept {
        let served = id_in(role_in(place, role, doc));
        if served.as_deref() != Some(formed.of(role)) {
            why.push(format!(
                "{role:?} is served under {served:?}, not {}, the identity served once formed",
                formed.of(role)
            ));
        }
    }
    let ids = all_ids(doc);
    for &role in left_out {
        if ids.contains(formed.of(role)) {
            why.push(format!(
                "the identity {} of {role:?}, which the removal emptied, is still served",
                formed.of(role)
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
// The writes after the race or the removal, and what they must leave
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

/// After the race, W's target again, +1, by a writer who has delivered U
/// and W: an update of an object every writer sees alive, which moves no
/// identity (SI.01's C4).
pub fn again_op(tray: &Tray, schedule: &Schedule) -> ModelOp {
    tray.write(schedule.place, schedule.target, 1)
}

/// What every replica serves after [`again_op`]: [`expected`] with W's
/// target at 11.
pub fn again(schedule: &Schedule) -> Value {
    race_value(schedule, 11)
}

/// After [`again_op`], delivered everywhere: [`again`] on every replica, and
/// the identities of the race where they were (nothing moved).
pub fn check_again(schedule: &Schedule, formed: &Formed, reads: &[Reads]) -> Vec<String> {
    let mut why = agreed_on(reads, &again(schedule));
    let (kept, left_out) = race_roles(schedule);
    if let Err(line) = identities(
        schedule.place,
        formed,
        kept,
        left_out,
        &reads[0].identified,
        &reads[0].who,
    ) {
        why.push(format!("after the write W's target again: {line}"));
    }
    why
}

/// The second later write, only where the race left out the `Mid` at key
/// `j`: that `Mid` written again, `k` +5, by a writer who has delivered U.
/// The removal emptied it and nothing kept it, so the write creates it
/// again (SI.01's C5).
pub fn rebuild_op(tray: &Tray, schedule: &Schedule) -> Option<ModelOp> {
    schedule
        .drops_sub()
        .then(|| tray.write(schedule.place, Target::Sub, 5))
}

/// What every replica serves after [`again_op`] and [`rebuild_op`].
pub fn rebuilt(schedule: &Schedule) -> Value {
    let content = match (schedule.place, schedule.target) {
        (Place::RecordDeep, Target::Member) => boxed(11, None, None, &[("j", mid(5))]),
        (Place::RecordDeep, Target::Mid) => boxed(0, None, Some(mid(11)), &[("j", mid(5))]),
        (Place::UnionDeep, Target::Member) => shape("A", 11, &[("j", mid(5))]),
        (place, target) => panic!("set-up: nothing is rebuilt for {target:?} in {place:?}"),
    };
    document(schedule.place, Some(content))
}

/// After [`rebuild_op`], delivered everywhere: [`rebuilt`] on every replica;
/// every kept object keeps its identity; the `Mid` at key `j` is served under
/// an identity outside `seen` (every identity served before), and its old
/// identity is served nowhere (SI.01's C5).
pub fn check_rebuilt(
    schedule: &Schedule,
    formed: &Formed,
    seen: &BTreeSet<String>,
    reads: &[Reads],
) -> Vec<String> {
    let place = schedule.place;
    let mut why = agreed_on(reads, &rebuilt(schedule));
    let doc = &reads[0].identified;
    let (kept, left_out) = race_roles(schedule);
    let others: Vec<Role> = left_out
        .iter()
        .copied()
        .filter(|role| *role != Role::Sub)
        .collect();
    if let Err(line) = identities(place, formed, kept, &others, doc, &reads[0].who) {
        why.push(format!("after the Mid was written again: {line}"));
    }
    match id_in(role_in(place, Role::Sub, doc)) {
        Some(id) if !seen.contains(&id) => {}
        found => why.push(format!(
            "the Mid written again after U is served under {found:?}, which is not an \
             identity nothing carried before"
        )),
    }
    if all_ids(doc).contains(formed.of(Role::Sub)) {
        why.push(format!(
            "the removed Mid's identity {} is served again: {doc}",
            formed.of(Role::Sub)
        ));
    }
    why
}

/// After a removal alone, the removed key's object written again, +5, by a
/// writer who has delivered U.
pub fn again_alone_op(tray: &Tray, place: Place) -> ModelOp {
    tray.write(place, Target::Member, 5)
}

/// And once more, +1, by another writer.
pub fn once_more_op(tray: &Tray, place: Place) -> ModelOp {
    tray.write(place, Target::Member, 1)
}

/// What every replica serves after [`again_alone_op`] (`m` 5) and after
/// [`once_more_op`] (`m` 6): the removed key's object holding that write
/// alone, and at `Union` nothing else at the key (ST.10: the `B` the removal
/// emptied is absent).
pub fn again_alone(place: Place, m: i64) -> Value {
    let content = match place {
        Place::Record | Place::RecordDeep => boxed(m, None, None, &[]),
        Place::RecordInner => boxed(2, None, None, &[("j", mid(m))]),
        Place::Union | Place::UnionOne | Place::UnionDeep => shape("A", m, &[]),
        Place::Family => bin(m),
    };
    document(place, Some(content))
}

/// After [`again_alone_op`]: [`again_alone`] on every replica; the removed
/// key's object served under an identity outside `seen`, its old identity
/// nowhere (SI.01's C5), every other kept object under its formed identity,
/// every other left-out object's identity nowhere. Answers the new identity.
pub fn check_again_alone(
    place: Place,
    formed: &Formed,
    seen: &BTreeSet<String>,
    reads: &[Reads],
) -> (Vec<String>, Option<String>) {
    let mut why = agreed_on(reads, &again_alone(place, 5));
    let doc = &reads[0].identified;
    let member = place.member();
    let (kept, left_out) = alone_roles(place);
    let kept: Vec<Role> = kept.iter().copied().filter(|r| *r != member).collect();
    let left_out: Vec<Role> = left_out.iter().copied().filter(|r| *r != member).collect();
    if let Err(line) = identities(place, formed, &kept, &left_out, doc, &reads[0].who) {
        why.push(format!("after the object was written again: {line}"));
    }
    let served = id_in(role_in(place, member, doc));
    match &served {
        Some(id) if !seen.contains(id) => {}
        found => why.push(format!(
            "{member:?} written again after the removal is served under {found:?}, which is \
             not an identity nothing carried before"
        )),
    }
    if all_ids(doc).contains(formed.of(member)) {
        why.push(format!(
            "the removed {member:?}'s identity {} is served again: {doc}",
            formed.of(member)
        ));
    }
    (why, served)
}

/// After [`once_more_op`]: [`again_alone`] at 6 on every replica, and the
/// object written again keeps the identity it was given (SI.01's C4).
pub fn check_once_more(place: Place, given: Option<&str>, reads: &[Reads]) -> Vec<String> {
    let mut why = agreed_on(reads, &again_alone(place, 6));
    let served = id_in(role_in(place, place.member(), &reads[0].identified));
    if served.as_deref() != given {
        why.push(format!(
            "the object written again moved from {given:?} to {served:?} on a later write"
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
