//! ST.01 oracle, the part its two test files share: the metamodel, the
//! operations of the race, the schedules, and the document the decided value
//! fixes for each schedule.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.01.md`. The claim, as the
//! spec's "Decisions" section fixes it (D1 = V1): an optional containment
//! holding the conflict of an `A` and a `B`, unset (U) concurrently with a
//! write (W) that increments the `n` of one member, serves, once every
//! replica has delivered every operation, the member W reached, alone,
//! holding W's increment and nothing of its first writer's, under the
//! identity its creator's replica served for it once the conflict formed;
//! the other member is served nowhere, and neither is its identity.
//!
//! The metamodel is `st01.ecore` (in `arachne-model-plane`,
//! `arachne-codegen/tests/generated/st01/`), whose descriptor, as
//! `arachne describe` writes it, is `../fixtures/st01.metamodel.json`:
//!
//! ```text
//! Holder   root; tick: counter, slot: optional Shape, inner: optional Box,
//!          bag: keyed Entry (key -> value)
//! Box      slot: optional Shape
//! Entry    key: text, value: Shape
//! Shape    abstract; n: counter
//! A, B     : Shape
//! ```
//!
//! Nothing here reads the code under test to decide what it expects: the
//! expected documents are written by hand from the schedule and the decided
//! value ([`v1`], [`keyed`], [`document`]), and the identities from which
//! operation created each element ([`Formed`]).
#![allow(dead_code)]

use std::collections::BTreeSet;

use moirai_interp::leaf::Scalar;
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::StateSnapshot;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Value, json};

use super::si01::{ID, Lang, OPAQUE, Rep, all_ids, default_read, elements, inc_int, strip, three};

/// `st01.ecore`'s descriptor, the bytes `arachne describe st01.ecore` writes.
pub const ST01: &str = include_str!("../fixtures/st01.metamodel.json");

pub fn descriptor() -> Value {
    serde_json::from_str(ST01).expect("the fixture is JSON")
}

// ---------------------------------------------------------------------------
// Where the conflict sits, and the operations that reach it
// ---------------------------------------------------------------------------

/// Where the conflict is made and raced.
///
/// - `Flat`: `Holder.slot`, and U unsets it.
/// - `Inner`: `Box.slot` of the `Box` in `Holder.inner`, and U unsets
///   `Box.slot` (a write of the `Box` that unsets its slot).
/// - `Outer`: the same nesting, and U unsets `Holder.inner`, the optional
///   that holds the `Box`.
/// - `Keyed`: key `k` of `Holder.bag`, and U removes `k`. Outside ST.01's
///   claim; under ST.10's rule (a member every write to which a removal has
///   seen is absent) it serves what the optional places serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    Flat,
    Inner,
    Outer,
    Keyed,
}

impl Place {
    /// The three places of the claim.
    pub const OPTIONAL: [Place; 3] = [Place::Flat, Place::Inner, Place::Outer];
    pub const ALL: [Place; 4] = [Place::Flat, Place::Inner, Place::Outer, Place::Keyed];

    /// Whether a `Box` stands between the root and the conflict.
    pub fn boxed(self) -> bool {
        matches!(self, Place::Inner | Place::Outer)
    }
}

/// Operations on `st01.ecore`'s model.
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

    /// An object of `class` with its `n` incremented by `by`.
    fn object(&self, class: &str, by: i64) -> InstanceOp {
        let l = &self.0;
        l.is(class, l.at(class, "n", inc_int(by)))
    }

    /// A write of `class` at the place, incrementing its `n` by `by`: it
    /// reaches the object of that class the place holds, or makes one. X, Y
    /// and W are writes, and so are the later writes of O2 and O3.
    pub fn write(&self, place: Place, class: &str, by: i64) -> ModelOp {
        let l = &self.0;
        let object = self.object(class, by);
        let op = match place {
            Place::Flat => l.at("Holder", "slot", InstanceOp::set(object)),
            Place::Inner | Place::Outer => l.at(
                "Holder",
                "inner",
                InstanceOp::set(l.is("Box", l.at("Box", "slot", InstanceOp::set(object)))),
            ),
            Place::Keyed => l.at(
                "Holder",
                "bag",
                InstanceOp::entry(Scalar::text("k"), object),
            ),
        };
        l.is("Holder", op).into_model_op()
    }

    /// U, the removal of the place.
    pub fn removal(&self, place: Place) -> ModelOp {
        let l = &self.0;
        let op = match place {
            Place::Flat => l.at("Holder", "slot", InstanceOp::unset()),
            Place::Inner => l.at(
                "Holder",
                "inner",
                InstanceOp::set(l.is("Box", l.at("Box", "slot", InstanceOp::unset()))),
            ),
            Place::Outer => l.at("Holder", "inner", InstanceOp::unset()),
            Place::Keyed => l.at("Holder", "bag", InstanceOp::remove(Scalar::text("k"))),
        };
        l.is("Holder", op).into_model_op()
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
/// `xy.0` writes an `A` with `n` +1; Y: the replica `xy.1`, concurrently, a
/// `B` with `n` +2. Each writer delivers the other's operation, and the third
/// replica delivers both, X first when `x_first`. The conflict `[A, B]` has
/// then formed everywhere. U: the replica `uw.0` removes the place; W: the
/// replica `uw.1`, concurrently, writes `member` with `n` +10. Each writer
/// delivers the other's, and the third delivers both, U first when
/// `u_first`. After that every replica has delivered T, X, Y, U and W.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    pub place: Place,
    pub xy: (usize, usize),
    pub x_first: bool,
    pub uw: (usize, usize),
    pub u_first: bool,
    pub member: &'static str,
}

impl Schedule {
    /// The class W did not reach.
    pub fn other(&self) -> &'static str {
        if self.member == "A" { "B" } else { "A" }
    }

    pub fn label(&self) -> String {
        format!(
            "{:?}: X on {} Y on {}, third X first {}; U on {} W({}) on {}, third U first {}",
            self.place,
            self.xy.0,
            self.xy.1,
            self.x_first,
            self.uw.0,
            self.member,
            self.uw.1,
            self.u_first
        )
    }
}

/// The 288 schedules of one place: 6 seatings of X and Y, times 2 orders at
/// the third replica, times 6 seatings of U and W, times 2 orders at the
/// third replica, times the member W reaches.
pub fn schedules(place: Place) -> Vec<Schedule> {
    let mut out = Vec::new();
    for xy in SEATS {
        for x_first in [true, false] {
            for uw in SEATS {
                for u_first in [true, false] {
                    for member in ["A", "B"] {
                        out.push(Schedule {
                            place,
                            xy,
                            x_first,
                            uw,
                            u_first,
                            member,
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
    /// The identified read replica 0 served once the conflict had formed on
    /// every replica, before U and W.
    pub formed: Value,
}

/// Send `op` on `seat`, record it, and report the step.
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

/// Deliver `event` to `seat`, and report the step.
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

/// Play `schedule` on three fresh replicas of `st01.ecore`'s model, calling
/// `step` after every send and every delivery (15 of them) with the replicas
/// and what has been sent so far.
pub fn play(
    tray: &Tray,
    desc: &Value,
    schedule: &Schedule,
    mut step: impl FnMut(&[Rep; 3], &[EventMessage<ModelOp>]),
) -> Played {
    let (a, b, c) = three(desc);
    let mut reps = [a, b, c];
    let mut sent = Vec::new();
    let place = schedule.place;

    let t = send_on(&mut reps, 0, tray.tick(), &mut sent, &mut step);
    deliver_to(&mut reps, 1, &t, &sent, &mut step);
    deliver_to(&mut reps, 2, &t, &sent, &mut step);

    race(
        &mut reps,
        schedule.xy,
        tray.write(place, "A", 1),
        tray.write(place, "B", 2),
        schedule.x_first,
        &mut sent,
        &mut step,
    );
    let formed = identified_read(&reps[0]);
    for (seat, replica) in reps.iter().enumerate().skip(1) {
        assert_eq!(
            identified_read(replica),
            formed,
            "set-up: {}: replicas 0 and {seat} serve different documents once the conflict has \
             formed, before U and W",
            schedule.label()
        );
    }

    race(
        &mut reps,
        schedule.uw,
        tray.removal(place),
        tray.write(place, schedule.member, 10),
        schedule.u_first,
        &mut sent,
        &mut step,
    );
    Played { reps, sent, formed }
}

/// U alone: the conflict formed as in [`play`], then `u` removes the place
/// and every other replica delivers it.
pub fn play_unset_alone(
    tray: &Tray,
    desc: &Value,
    place: Place,
    xy: (usize, usize),
    x_first: bool,
    u: usize,
) -> Played {
    let (a, b, c) = three(desc);
    let mut reps = [a, b, c];
    let mut sent = Vec::new();
    let mut step = |_: &[Rep; 3], _: &[EventMessage<ModelOp>]| {};
    let t = send_on(&mut reps, 0, tray.tick(), &mut sent, &mut step);
    deliver_to(&mut reps, 1, &t, &sent, &mut step);
    deliver_to(&mut reps, 2, &t, &sent, &mut step);
    race(
        &mut reps,
        xy,
        tray.write(place, "A", 1),
        tray.write(place, "B", 2),
        x_first,
        &mut sent,
        &mut step,
    );
    let formed = identified_read(&reps[0]);
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

/// The default read, `QueryableLog::query_state_json`.
pub fn default_of(replica: &Rep) -> Value {
    default_read(replica)
}

/// Where the place's content sits in a served document, default or
/// identified: the slot's value, the `Box`'s slot's value, or key `k`'s.
pub fn content(place: Place, doc: &Value) -> Option<&Value> {
    match place {
        Place::Flat => doc.get("slot"),
        Place::Inner | Place::Outer => doc.get("inner").and_then(|boxed| boxed.get("slot")),
        Place::Keyed => doc.get("bag").and_then(|bag| bag.get("k")),
    }
}

/// The members a content holds: itself when it is one object, or the
/// members of its conflict.
pub fn members(content: &Value) -> Vec<&Value> {
    match content.get("__conflict").and_then(Value::as_array) {
        Some(held) => held.iter().collect(),
        None => vec![content],
    }
}

/// The identity of the member of `class` in `content`, if one is served.
pub fn member_id(content: &Value, class: &str) -> Option<String> {
    members(content)
        .into_iter()
        .find(|member| member.get("eClass") == Some(&json!(class)))
        .and_then(|member| member.get(ID).and_then(Value::as_str).map(str::to_string))
}

/// The identities every replica served once the conflict had formed: the
/// root's, the `Box`'s where there is one, and each member's.
#[derive(Clone, Debug)]
pub struct Formed {
    pub root: String,
    pub boxed: Option<String>,
    pub a: String,
    pub b: String,
}

impl Formed {
    /// Read off the identified document served once the conflict formed;
    /// a failure here is a failure of the set-up, not of the claim.
    pub fn read(place: Place, formed: &Value, who: &str) -> Formed {
        let id = |value: Option<&Value>, what: &str| -> String {
            value
                .and_then(|value| value.get(ID))
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("set-up: {who}: no identity for {what} in {formed}"))
                .to_string()
        };
        let held = content(place, formed)
            .unwrap_or_else(|| panic!("set-up: {who}: nothing at the place once formed: {formed}"));
        assert_eq!(
            members(held).len(),
            2,
            "set-up: {who}: the conflict [A, B] has not formed: {formed}"
        );
        Formed {
            root: id(Some(formed), "the root"),
            boxed: place.boxed().then(|| id(formed.get("inner"), "the Box")),
            a: member_id(held, "A")
                .unwrap_or_else(|| panic!("set-up: {who}: no `A` once formed: {formed}")),
            b: member_id(held, "B")
                .unwrap_or_else(|| panic!("set-up: {who}: no `B` once formed: {formed}")),
        }
    }

    pub fn of(&self, class: &str) -> &str {
        if class == "A" { &self.a } else { &self.b }
    }

    /// Every identity served once the conflict formed.
    pub fn all(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = [self.root.clone(), self.a.clone(), self.b.clone()].into();
        out.extend(self.boxed.clone());
        out
    }
}

// ---------------------------------------------------------------------------
// The expected documents, by hand
// ---------------------------------------------------------------------------

/// One member as the canonical form spells it.
pub fn shape(class: &str, n: i64) -> Value {
    json!({"eClass": class, "n": n})
}

/// The conflict of an `A` holding `a` and a `B` holding `b`, ordered by
/// class name as the canonical form orders a conflict.
pub fn conflict(a: i64, b: i64) -> Value {
    json!({"__conflict": [shape("A", a), shape("B", b)]})
}

/// The model's whole default document with `content` at the place, or
/// nothing there. `tick` reads 1 (T); `bag` is an empty keyed collection
/// unless the place is `Keyed`; an unset optional is an absent key, and a
/// `Box` emptied of its slot is not served either (it reads as an unset
/// `inner`, which is what the store served for U alone before this item).
pub fn document(place: Place, content: Option<Value>) -> Value {
    let mut doc = json!({"bag": {}, "eClass": "Holder", "tick": 1});
    if let Some(content) = content {
        match place {
            Place::Flat => doc["slot"] = content,
            Place::Inner | Place::Outer => {
                doc["inner"] = json!({"eClass": "Box", "slot": content});
            }
            Place::Keyed => doc["bag"] = json!({"k": content}),
        }
    }
    doc
}

/// V1, the value D1 fixes: the member W reached, alone, holding W's +10 and
/// nothing of X's +1 or Y's +2 (U had seen them).
pub fn v1(schedule: &Schedule) -> Value {
    document(schedule.place, Some(shape(schedule.member, 10)))
}

/// The keyed place's value under ST.10's rule (`cea-cdrt-knowledge/spec/
/// items/ST.10.md`, B2): the member W reached, alone at the removed key,
/// holding W's +10; U had seen every write to the other member, which is
/// absent. The same document V1 fixes for an optional place.
pub fn keyed(schedule: &Schedule) -> Value {
    document(Place::Keyed, Some(shape(schedule.member, 10)))
}

/// What U alone leaves: an optional place reads unset, and a removed key
/// whose every write U had seen is absent (ST.10).
pub fn unset_alone(place: Place) -> Value {
    document(place, None)
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

/// C3 under V1, on one identified document of an optional place: the root
/// and the `Box` keep their identities; the member W reached is served alone
/// under the identity its creator's replica served once the conflict formed;
/// the other member's identity is served nowhere; no two elements share an
/// identity.
pub fn v1_identities(
    schedule: &Schedule,
    formed: &Formed,
    doc: &Value,
    who: &str,
) -> Result<(), String> {
    let mut why = Vec::new();
    if doc.get(ID).and_then(Value::as_str) != Some(formed.root.as_str()) {
        why.push(format!("the root is not served under {}", formed.root));
    }
    if let Some(boxed) = &formed.boxed {
        let served = doc
            .get("inner")
            .and_then(|inner| inner.get(ID))
            .and_then(Value::as_str);
        if served != Some(boxed.as_str()) {
            why.push(format!("the Box is served under {served:?}, not {boxed}"));
        }
    }
    match content(schedule.place, doc) {
        None => why.push("nothing is served at the place".to_string()),
        Some(held) => {
            let found = members(held);
            let reached = formed.of(schedule.member);
            if found.len() != 1 {
                why.push(format!(
                    "{} members are served where V1 serves the reached one alone",
                    found.len()
                ));
            }
            match member_id(held, schedule.member) {
                Some(id) if id == reached => {}
                other => why.push(format!(
                    "the reached `{}` is served under {other:?}, not {reached}, the identity its \
                     creator served once the conflict formed",
                    schedule.member
                )),
            }
        }
    }
    let ids = all_ids(doc);
    let gone = formed.of(schedule.other());
    if ids.contains(gone) {
        why.push(format!(
            "the emptied `{}`'s identity {gone} is still served",
            schedule.other()
        ));
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

/// Both reads of each replica, named by seat.
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

/// The first of the later writes (O2, O3, O4): the reached member's class,
/// +1, by a writer who has delivered U and W.
pub fn reached_write(tray: &Tray, schedule: &Schedule) -> ModelOp {
    tray.write(schedule.place, schedule.member, 1)
}

/// The second and third: the place removed again, and the emptied member's
/// class written +5. A write of the emptied class straight into a place
/// holding the reached member alone is refused locally by the interpreter
/// (`VariantTaken`), so the class is written again the way SI.01's O1k
/// writes one: into the place emptied by a removal.
pub fn written_again(tray: &Tray, schedule: &Schedule) -> [ModelOp; 2] {
    [
        tray.removal(schedule.place),
        tray.write(schedule.place, schedule.other(), 5),
    ]
}

/// Every replica serves one identified read, and each one's default read is
/// `expected` and is its identified read stripped.
fn agreed_on(reads: &[Reads], expected: &Value) -> Vec<String> {
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

/// After [`reached_write`], delivered everywhere: the reached member holds
/// 11 under its identity (a write into an element every writer sees alive
/// moves nothing, SI.01's C4); the root and the `Box` keep theirs.
pub fn check_reached_write(schedule: &Schedule, formed: &Formed, reads: &[Reads]) -> Vec<String> {
    let expected = document(schedule.place, Some(shape(schedule.member, 11)));
    let mut why = agreed_on(reads, &expected);
    let doc = &reads[0].identified;
    if doc.get(ID).and_then(Value::as_str) != Some(formed.root.as_str()) {
        why.push(format!("the root's identity moved: {doc}"));
    }
    if let Some(boxed) = &formed.boxed {
        let served = doc
            .get("inner")
            .and_then(|inner| inner.get(ID))
            .and_then(Value::as_str);
        if served != Some(boxed.as_str()) {
            why.push(format!("the Box is served under {served:?}, not {boxed}"));
        }
    }
    let reached = content(schedule.place, doc).and_then(|held| member_id(held, schedule.member));
    if reached.as_deref() != Some(formed.of(schedule.member)) {
        why.push(format!(
            "the reached `{}` is served under {reached:?}, not {}: a write moved it",
            schedule.member,
            formed.of(schedule.member)
        ));
    }
    why
}

/// After [`written_again`], delivered everywhere: the emptied member's class
/// is served alone, holding +5 and nothing of X's or Y's, under an identity
/// outside `seen` (every identity served before); neither its old identity
/// nor the reached member's is served (SI.01's C5; only the class written
/// comes back, as in SI.01's O1k). The root keeps its identity. The `Box`
/// keeps its identity where the removal was a write into it (`Inner`), and
/// is a new element where the removal removed it (`Outer`).
pub fn check_written_again(
    schedule: &Schedule,
    formed: &Formed,
    seen: &BTreeSet<String>,
    reads: &[Reads],
) -> Vec<String> {
    let (member, other) = (schedule.member, schedule.other());
    let expected = document(schedule.place, Some(shape(other, 5)));
    let mut why = agreed_on(reads, &expected);
    let doc = &reads[0].identified;
    if doc.get(ID).and_then(Value::as_str) != Some(formed.root.as_str()) {
        why.push(format!("the root's identity moved: {doc}"));
    }
    let boxed = doc
        .get("inner")
        .and_then(|inner| inner.get(ID))
        .and_then(Value::as_str);
    match schedule.place {
        Place::Inner => {
            if boxed != formed.boxed.as_deref() {
                why.push(format!(
                    "the Box, written and never removed, is served under {boxed:?}, not {:?}",
                    formed.boxed
                ));
            }
        }
        Place::Outer => match boxed {
            Some(id) if !seen.contains(id) => {}
            other => why.push(format!(
                "the Box removed and written again is served under {other:?}, which is not an \
                 identity nothing carried before"
            )),
        },
        Place::Flat | Place::Keyed => {}
    }
    match content(schedule.place, doc).and_then(|held| member_id(held, other)) {
        Some(id) if !seen.contains(&id) => {}
        found => why.push(format!(
            "the `{other}` written again after U is served under {found:?}, which is not an \
             identity nothing carried before"
        )),
    }
    let ids = all_ids(doc);
    for (what, gone) in [
        ("emptied", formed.of(other)),
        ("reached, then removed,", formed.of(member)),
    ] {
        if ids.contains(gone) {
            why.push(format!(
                "the {what} member's identity {gone} is still served: {doc}"
            ));
        }
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
