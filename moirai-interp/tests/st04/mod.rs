//! ST.04 oracle, the part its two test files share: the metamodel, the leaf
//! kinds, the operations of the race, the schedules, and the document the
//! rule fixes for each schedule.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.04.md`. The claim: an
//! optional attribute (a leaf) of an object that a removal above reaches,
//! while a concurrent write to a sibling feature keeps the object alive,
//! converges, and every replica serves the value the store's rules give: the
//! removal does to the optional what the optional's own unset does. It resets
//! the leaf against the removal's version, and when the leaf is then at its
//! default (its log holds nothing) the optional is unset, an absent key; a
//! leaf that still holds something (a concurrent write, or a text, whose
//! reset records the deletion) stays and reads what it holds.
//!
//! The metamodel is `st04.ecore` (in `arachne-model-plane`,
//! `arachne-codegen/tests/generated/st04/`), whose descriptor, as
//! `arachne describe` writes it, is `../fixtures/st04.metamodel.json`:
//!
//! ```text
//! Holder   root; tick: counter, inner: optional Box
//! Box      k: counter, mid: optional Mid, and one optional attribute per
//!          leaf kind (Kind::ALL)
//! Mid      k: counter, and the same optional attributes
//! Color    enum: red, green, blue
//! ```
//!
//! Nothing here reads the code under test to decide what it expects: the
//! expected documents are written by hand from the schedule and the rule
//! ([`expected`], [`formed_doc`], [`unset_alone`], [`again`], [`rebuilt`]),
//! and the identities from which operation created each element and whether
//! each later writer had delivered the removal ([`Formed`]).
#![allow(dead_code)]

use std::collections::BTreeSet;

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::StateSnapshot;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Map, Value, json};

use super::si01::{ID, Lang, OPAQUE, Rep, all_ids, default_read, elements, inc_int, strip, three};

/// `st04.ecore`'s descriptor, the bytes `arachne describe st04.ecore` writes.
pub const ST04: &str = include_str!("../fixtures/st04.metamodel.json");

pub fn descriptor() -> Value {
    serde_json::from_str(ST04).expect("the fixture is JSON")
}

// ---------------------------------------------------------------------------
// The leaf kinds
// ---------------------------------------------------------------------------

/// Every leaf the derivation can hold under an optional attribute
/// (`moirai-semantics` `LeafRule` under `Shape::Optional`), one optional
/// attribute of `Box` and of `Mid` each, named by [`Kind::feature`]:
///
/// - `Text`: `EString`, `GraphLog<List<char>>`;
/// - `Byte` to `Double`: the resettable counter at its six widths (`EByte`,
///   `EShort`, `EInt`, `ELong`, `EFloat`, `EDouble`), `VecLog<Counter<_>>`;
///   the counter that does not reset is an arm no spelling reaches;
/// - `Ew`, `Dw`: `EBoolean`, enable-wins and (`dw-flag`) disable-wins;
/// - `Ch`: `EChar`, a multi-value register over a character;
/// - `Mv`, `Lww`, `Fair`, `Po`, `To`: the five registers over `EString`;
/// - `Emv` to `Eto`: the five registers over the enum `Color`.
///
/// A set, a bag, an ordered set and a sequence are shapes of their own and
/// never sit under an optional; the register over an optional value is the
/// value of a keyed collection only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Text,
    Byte,
    Short,
    Int,
    Long,
    Float,
    Double,
    Ew,
    Dw,
    Ch,
    Mv,
    Lww,
    Fair,
    Po,
    To,
    Emv,
    Elww,
    Efair,
    Epo,
    Eto,
}

/// How a kind's values are written and read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    Text,
    Int,
    Float,
    Flag,
    Char,
    Str,
    Enum,
}

/// Which write to a leaf: X (the formation's), W (the race's, when W reaches
/// the leaf), `Again` (W's target written again after the race, when that is
/// the leaf), `Rebuild` (the leaf written again after the race, where W did
/// not reach it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Which {
    X,
    W,
    Again,
    Rebuild,
}

impl Kind {
    pub const ALL: [Kind; 20] = [
        Kind::Text,
        Kind::Byte,
        Kind::Short,
        Kind::Int,
        Kind::Long,
        Kind::Float,
        Kind::Double,
        Kind::Ew,
        Kind::Dw,
        Kind::Ch,
        Kind::Mv,
        Kind::Lww,
        Kind::Fair,
        Kind::Po,
        Kind::To,
        Kind::Emv,
        Kind::Elww,
        Kind::Efair,
        Kind::Epo,
        Kind::Eto,
    ];

    /// The optional attribute of `Box` and `Mid` that holds this kind.
    pub fn feature(self) -> &'static str {
        match self {
            Kind::Text => "text",
            Kind::Byte => "byte",
            Kind::Short => "short",
            Kind::Int => "int",
            Kind::Long => "long",
            Kind::Float => "float",
            Kind::Double => "double",
            Kind::Ew => "ew",
            Kind::Dw => "dw",
            Kind::Ch => "ch",
            Kind::Mv => "mv",
            Kind::Lww => "lww",
            Kind::Fair => "fair",
            Kind::Po => "po",
            Kind::To => "to",
            Kind::Emv => "emv",
            Kind::Elww => "elww",
            Kind::Efair => "efair",
            Kind::Epo => "epo",
            Kind::Eto => "eto",
        }
    }

    pub fn family(self) -> Family {
        match self {
            Kind::Text => Family::Text,
            Kind::Byte | Kind::Short | Kind::Int | Kind::Long => Family::Int,
            Kind::Float | Kind::Double => Family::Float,
            Kind::Ew | Kind::Dw => Family::Flag,
            Kind::Ch => Family::Char,
            Kind::Mv | Kind::Lww | Kind::Fair | Kind::Po | Kind::To => Family::Str,
            Kind::Emv | Kind::Elww | Kind::Efair | Kind::Epo | Kind::Eto => Family::Enum,
        }
    }

    /// Whether a removal that had seen every write to this leaf leaves its
    /// log at its default. Every kind but text: a reset prunes what the
    /// removal had seen. A text's reset records the deletion (the list's
    /// causal reset injects a `DeleteRange` at the removal's version), so a
    /// text that was written is never at its default again, after its own
    /// unset as after a removal above.
    pub fn empties(self) -> bool {
        self != Kind::Text
    }

    /// What the leaf reads after a removal that had seen X and nothing
    /// concurrent reached it: nothing (the optional is unset) for a kind
    /// that empties, the empty string for a text.
    pub fn kept(self) -> Option<Value> {
        (!self.empties()).then(|| json!(""))
    }

    /// The leaf operation of `which`. `color` is the enum's slot.
    pub fn op(self, which: Which, color: u16) -> LeafOp {
        let (ch, text, int, float, lit) = match which {
            Which::X => ('a', "x", 1, 1.0, 1),
            Which::W => ('w', "w", 10, 10.0, 2),
            Which::Again => ('v', "v", 1, 1.0, 0),
            Which::Rebuild => ('r', "r", 5, 5.0, 0),
        };
        match self.family() {
            Family::Text => LeafOp::InsertChar { pos: 0, ch },
            Family::Int => LeafOp::Inc(Scalar::Int(int)),
            Family::Float => LeafOp::Inc(Scalar::float(float)),
            // W on the disable-wins flag disables: the leaf then holds W's
            // operation and reads false, its default value, and must stay (its
            // log is not empty). An enable-wins flag keeps no disable (a
            // disable with nothing to disable leaves its log empty), so W
            // enables it.
            Family::Flag => match (self, which) {
                (Kind::Dw, Which::W) => LeafOp::Disable,
                _ => LeafOp::Enable,
            },
            Family::Char => LeafOp::Write(Scalar::Char(ch)),
            Family::Str => LeafOp::Write(Scalar::text(text)),
            Family::Enum => LeafOp::Write(Scalar::Enum(color, lit)),
        }
    }

    /// The canonical value the leaf reads right after `which`, in the
    /// schedule it is written in: X on a fresh leaf; W on the leaf X wrote,
    /// concurrently with a removal that had seen X; `Again` after W;
    /// `Rebuild` on a leaf the removal emptied (or a text it left empty).
    pub fn value(self, which: Which) -> Value {
        match (self.family(), which) {
            (Family::Text, Which::X) => json!("a"),
            (Family::Text, Which::W) => json!("w"),
            (Family::Text, Which::Again) => json!("vw"),
            (Family::Text, Which::Rebuild) => json!("r"),
            (Family::Int, Which::X) => json!(1),
            (Family::Int, Which::W) => json!(10),
            (Family::Int, Which::Again) => json!(11),
            (Family::Int, Which::Rebuild) => json!(5),
            (Family::Float, Which::X) => json!(1.0),
            (Family::Float, Which::W) => json!(10.0),
            (Family::Float, Which::Again) => json!(11.0),
            (Family::Float, Which::Rebuild) => json!(5.0),
            (Family::Flag, Which::W) => json!(self != Kind::Dw),
            (Family::Flag, _) => json!(true),
            (Family::Char | Family::Str, Which::X) => {
                json!(if self.family() == Family::Char {
                    "a"
                } else {
                    "x"
                })
            }
            (Family::Char | Family::Str, Which::W) => json!("w"),
            (Family::Char | Family::Str, Which::Again) => json!("v"),
            (Family::Char | Family::Str, Which::Rebuild) => json!("r"),
            (Family::Enum, Which::X) => json!("green"),
            (Family::Enum, Which::W) => json!("blue"),
            (Family::Enum, Which::Again | Which::Rebuild) => json!("red"),
        }
    }
}

// ---------------------------------------------------------------------------
// Where the leaf sits, and the operations that reach it
// ---------------------------------------------------------------------------

/// Where the optional attribute sits, and what removes it.
///
/// - `Flat`: `Box.<leaf>` of the `Box` in `Holder.inner`; U unsets
///   `Holder.inner`, the optional holding the `Box`, one level above.
/// - `Deep`: `Mid.<leaf>` of the `Mid` in `Box.mid` of the `Box` in
///   `Holder.inner`; U unsets `Holder.inner`, two levels above.
/// - `Direct`: `Box.<leaf>` as in `Flat`, and U unsets `Box.<leaf>` itself
///   (a write of the `Box` that unsets it). The guard: no removal above the
///   optional; what the rule reads a removal above against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    Flat,
    Deep,
    Direct,
}

impl Place {
    /// The two places of the claim.
    pub const CLAIM: [Place; 2] = [Place::Flat, Place::Deep];
    /// The places of the removal-alone guard.
    pub const ALONE: [Place; 3] = [Place::Flat, Place::Deep, Place::Direct];

    pub fn deep(self) -> bool {
        self == Place::Deep
    }

    /// The class that holds the leaf.
    pub fn owner(self) -> &'static str {
        if self.deep() { "Mid" } else { "Box" }
    }

    /// What W can write, in this place.
    pub fn targets(self) -> &'static [Target] {
        match self {
            Place::Deep => &[Target::Outer, Target::Sibling, Target::Leaf],
            Place::Flat | Place::Direct => &[Target::Sibling, Target::Leaf],
        }
    }
}

/// What W, the write concurrent with U, reaches.
///
/// - `Sibling`: the counter `k` of the leaf's owner (`Box.k` in `Flat` and
///   `Direct`, `Mid.k` in `Deep`), +10: the owner is kept alive, the leaf is
///   not reached. The claim's case.
/// - `Leaf`: the leaf itself, with [`Which::W`]: the leaf is reached.
/// - `Outer` (`Deep` only): `Box.k`, +10: the `Box` is kept alive, and
///   neither the `Mid` nor its leaf is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Outer,
    Sibling,
    Leaf,
}

/// Operations on `st04.ecore`'s model.
pub struct Tray {
    pub lang: Lang,
    /// `Color`'s slot in the table's enums.
    pub color: u16,
}

impl Tray {
    pub fn new() -> Self {
        let lang = Lang::parse(&descriptor());
        let color = lang
            .sem
            .enums
            .iter()
            .position(|entry| &*entry.name == "Color")
            .expect("set-up: st04.ecore declares Color") as u16;
        Tray { lang, color }
    }

    /// T: an increment of `Holder.tick`, which creates the root.
    pub fn tick(&self) -> ModelOp {
        let l = &self.lang;
        l.is("Holder", l.at("Holder", "tick", inc_int(1)))
            .into_model_op()
    }

    /// `op` on the `Box` in `Holder.inner`, as a write of that `Box`.
    fn in_box(&self, op: InstanceOp) -> ModelOp {
        let l = &self.lang;
        l.is(
            "Holder",
            l.at("Holder", "inner", InstanceOp::set(l.is("Box", op))),
        )
        .into_model_op()
    }

    /// `op` on the `Mid` in `Box.mid`, as a write of that `Mid`.
    fn in_mid(&self, op: InstanceOp) -> InstanceOp {
        let l = &self.lang;
        l.at("Box", "mid", InstanceOp::set(l.is("Mid", op)))
    }

    /// `op` on the owner of the leaf at `place`.
    fn on_owner(&self, place: Place, op: InstanceOp) -> ModelOp {
        if place.deep() {
            self.in_box(self.in_mid(op))
        } else {
            self.in_box(op)
        }
    }

    /// A write of `which` into the leaf of `kind` at `place` (setting the
    /// optional attribute): it reaches the leaf's owner and every object
    /// above it, or makes them.
    pub fn write_leaf(&self, place: Place, kind: Kind, which: Which) -> ModelOp {
        let l = &self.lang;
        let leaf = InstanceOp::set(InstanceOp::Leaf(kind.op(which, self.color)));
        self.on_owner(place, l.at(place.owner(), kind.feature(), leaf))
    }

    /// An increment of `k` by `by`: the owner's (`Sibling`) or the `Box`'s
    /// (`Outer`).
    pub fn inc(&self, place: Place, target: Target, by: i64) -> ModelOp {
        let l = &self.lang;
        match target {
            Target::Sibling => self.on_owner(place, l.at(place.owner(), "k", inc_int(by))),
            Target::Outer => self.in_box(l.at("Box", "k", inc_int(by))),
            Target::Leaf => panic!("set-up: the leaf is not a counter k"),
        }
    }

    /// X: the leaf written with [`Which::X`].
    pub fn x(&self, place: Place, kind: Kind) -> ModelOp {
        self.write_leaf(place, kind, Which::X)
    }

    /// Y: the owner's `k` +2.
    pub fn y(&self, place: Place) -> ModelOp {
        self.inc(place, Target::Sibling, 2)
    }

    /// W: `target` reached; +10 on a counter `k`, [`Which::W`] on the leaf.
    pub fn w(&self, place: Place, kind: Kind, target: Target) -> ModelOp {
        match target {
            Target::Leaf => self.write_leaf(place, kind, Which::W),
            _ => self.inc(place, target, 10),
        }
    }

    /// U, the removal: `Holder.inner` unset (`Flat`, `Deep`), or the leaf of
    /// `kind` unset by a write of the `Box` (`Direct`).
    pub fn removal(&self, place: Place, kind: Kind) -> ModelOp {
        let l = &self.lang;
        match place {
            Place::Flat | Place::Deep => l
                .is("Holder", l.at("Holder", "inner", InstanceOp::unset()))
                .into_model_op(),
            Place::Direct => self.in_box(l.at("Box", kind.feature(), InstanceOp::unset())),
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
/// `xy.0` writes the leaf of `kind` at the place (which makes the `Box`, and
/// in `Deep` the `Mid`, if the replica has none); Y: the replica `xy.1`,
/// concurrently, increments the owner's `k` by 2. Each writer delivers the
/// other's operation, and the third replica delivers both, X first when
/// `x_first`. Every replica then holds the owner with `k` 2 and the leaf
/// holding X. U: the replica `uw.0` removes; W: the replica `uw.1`,
/// concurrently, writes `target`. Each writer delivers the other's, and the
/// third delivers both, U first when `u_first`. After that every replica has
/// delivered T, X, Y, U and W: 15 steps (5 sends, 10 deliveries).
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    pub kind: Kind,
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
            "{:?} {:?}: X on {} Y on {}, third X first {}; U on {} W({:?}) on {}, third U \
             first {}",
            self.kind,
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

    /// Whether the race drops the `Mid` (`Deep`, W on `Box.k`, a leaf that
    /// empties): an object the removal emptied.
    pub fn drops_mid(&self) -> bool {
        self.place.deep() && self.target == Target::Outer && self.kind.empties()
    }

    /// Whether the rule fixes the `Mid`'s identity after the race: it does
    /// where W reached the `Mid` (kept) and where the removal emptied it
    /// (dropped). A `Mid` the removal reached and that a text keeps
    /// (`Deep`, W on `Box.k`, `Text`) is outside the identity claims
    /// (spec, OB2).
    pub fn mid_identity_fixed(&self) -> bool {
        self.place.deep() && !(self.target == Target::Outer && !self.kind.empties())
    }
}

/// The schedules of one kind at one place: 6 seatings of X and Y, times 2
/// orders at the third replica, times 6 seatings of U and W, times 2 orders
/// at the third replica, times the targets of the place (288 in `Flat` and
/// `Direct`, 432 in `Deep`).
pub fn schedules(kind: Kind, place: Place) -> Vec<Schedule> {
    let mut out = Vec::new();
    for xy in SEATS {
        for x_first in [true, false] {
            for uw in SEATS {
                for u_first in [true, false] {
                    for &target in place.targets() {
                        out.push(Schedule {
                            kind,
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
#[allow(clippy::too_many_arguments)]
fn form(
    tray: &Tray,
    desc: &Value,
    kind: Kind,
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
        tray.x(place, kind),
        tray.y(place),
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
            formed_doc(kind, place),
            "set-up: {label}: replica {seat} does not serve the formed document"
        );
    }
    (reps, formed)
}

/// Play `schedule` on three fresh replicas of `st04.ecore`'s model, calling
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
        schedule.kind,
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
        tray.removal(schedule.place, schedule.kind),
        tray.w(schedule.place, schedule.kind, schedule.target),
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
    kind: Kind,
    place: Place,
    xy: (usize, usize),
    x_first: bool,
    u: usize,
) -> Played {
    let mut sent = Vec::new();
    let mut step = |_: &[Rep; 3], _: &[EventMessage<ModelOp>]| {};
    let label = format!(
        "{kind:?} {place:?}: X on {} Y on {}, U alone on {u}",
        xy.0, xy.1
    );
    let (mut reps, formed) = form(
        tray, desc, kind, place, xy, x_first, &label, &mut sent, &mut step,
    );
    let removal = send_on(
        &mut reps,
        u,
        tray.removal(place, kind),
        &mut sent,
        &mut step,
    );
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

/// The `Box` in a served document, default or identified.
pub fn boxed_of(doc: &Value) -> Option<&Value> {
    doc.get("inner")
}

/// The `Mid` in a served document.
pub fn mid_of(doc: &Value) -> Option<&Value> {
    boxed_of(doc).and_then(|boxed| boxed.get("mid"))
}

fn id_in(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| value.get(ID))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The identities every replica served once X and Y had formed the model:
/// the root's, the `Box`'s, and the `Mid`'s in `Deep`.
#[derive(Clone, Debug)]
pub struct Formed {
    pub root: String,
    pub boxed: String,
    pub mid: Option<String>,
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
            boxed: need(id_in(boxed_of(formed)), "the Box"),
            mid: place.deep().then(|| need(id_in(mid_of(formed)), "the Mid")),
        }
    }

    /// Every identity served once formed.
    pub fn all(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = [self.root.clone(), self.boxed.clone()].into();
        out.extend(self.mid.clone());
        out
    }
}

// ---------------------------------------------------------------------------
// The expected documents, by hand
// ---------------------------------------------------------------------------

/// A `Mid` holding `k`, and the leaf of `kind` when it holds `leaf`.
pub fn mid(k: i64, kind: Kind, leaf: Option<Value>) -> Value {
    let mut out = Map::new();
    out.insert("eClass".into(), json!("Mid"));
    out.insert("k".into(), json!(k));
    if let Some(leaf) = leaf {
        out.insert(kind.feature().into(), leaf);
    }
    Value::Object(out)
}

/// A `Box` holding `k`, the leaf of `kind` when it holds `leaf`, and a `Mid`
/// when there is one. Every other optional attribute, which nothing writes,
/// is an absent key.
pub fn boxed(k: i64, kind: Kind, leaf: Option<Value>, below: Option<Value>) -> Value {
    let mut out = Map::new();
    out.insert("eClass".into(), json!("Box"));
    out.insert("k".into(), json!(k));
    if let Some(leaf) = leaf {
        out.insert(kind.feature().into(), leaf);
    }
    if let Some(below) = below {
        out.insert("mid".into(), below);
    }
    Value::Object(out)
}

/// The model's whole default document, with `Holder.inner` holding
/// `content`, or absent. `tick` reads 1 (T).
pub fn document(content: Option<Value>) -> Value {
    let mut doc = json!({"eClass": "Holder", "tick": 1});
    if let Some(content) = content {
        doc["inner"] = content;
    }
    doc
}

/// What every replica serves once X and Y are delivered: the owner holding
/// Y's 2 and the leaf holding X.
pub fn formed_doc(kind: Kind, place: Place) -> Value {
    let x = Some(kind.value(Which::X));
    match place {
        Place::Deep => document(Some(boxed(0, kind, None, Some(mid(2, kind, x))))),
        Place::Flat | Place::Direct => document(Some(boxed(2, kind, x, None))),
    }
}

/// The value the rule fixes, once every replica has delivered T, X, Y, U and
/// W (spec section 2). The removal (U) resets what it had seen, X and Y's 2,
/// and keeps W (update-wins). A leaf W reached holds W; a leaf W did not
/// reach is what the removal left of it, [`Kind::kept`]: absent for every
/// kind but text, the empty string for a text. In `Deep` with W on `Box.k`,
/// the `Mid` the removal emptied is dropped (ST.02's rule), unless a text
/// keeps it. In `Direct` U unsets the leaf alone and the `Box` keeps Y's 2
/// beside W's 10.
pub fn expected(schedule: &Schedule) -> Value {
    let kind = schedule.kind;
    let kept = kind.kept();
    let w = Some(kind.value(Which::W));
    let content = match (schedule.place, schedule.target) {
        (Place::Flat, Target::Sibling) => boxed(10, kind, kept, None),
        (Place::Flat, Target::Leaf) => boxed(0, kind, w, None),
        (Place::Deep, Target::Outer) => {
            boxed(10, kind, None, kept.map(|leaf| mid(0, kind, Some(leaf))))
        }
        (Place::Deep, Target::Sibling) => boxed(0, kind, None, Some(mid(10, kind, kept))),
        (Place::Deep, Target::Leaf) => boxed(0, kind, None, Some(mid(0, kind, w))),
        (Place::Direct, Target::Sibling) => boxed(12, kind, kept, None),
        (Place::Direct, Target::Leaf) => boxed(2, kind, w, None),
        (place, target) => panic!("set-up: no {target:?} in {place:?}"),
    };
    document(Some(content))
}

/// What U alone leaves: `Holder.inner` unset (`Flat`, `Deep`), unless a
/// text keeps the `Box` (and the `Mid`) holding an empty string; the `Box`
/// holding Y's 2 and what its own unset left of the leaf (`Direct`).
pub fn unset_alone(kind: Kind, place: Place) -> Value {
    let kept = kind.kept();
    match place {
        Place::Flat => document(kept.map(|leaf| boxed(0, kind, Some(leaf), None))),
        Place::Deep => {
            document(kept.map(|leaf| boxed(0, kind, None, Some(mid(0, kind, Some(leaf))))))
        }
        Place::Direct => document(Some(boxed(2, kind, kept, None))),
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

/// On one identified document once the race is delivered (SI.01's C2 to
/// C5): the root and the `Box` keep the identities served once formed (the
/// `Box` is alive, W reached it); the `Mid` keeps its identity where W
/// reached it, and its identity is served nowhere where the removal emptied
/// and dropped it; no two elements share an identity.
pub fn identities(
    schedule: &Schedule,
    formed: &Formed,
    doc: &Value,
    who: &str,
) -> Result<(), String> {
    let mut why = Vec::new();
    if id_in(Some(doc)).as_deref() != Some(formed.root.as_str()) {
        why.push(format!("the root is not served under {}", formed.root));
    }
    let served_box = id_in(boxed_of(doc));
    if served_box.as_deref() != Some(formed.boxed.as_str()) {
        why.push(format!(
            "the Box is served under {served_box:?}, not {}, the identity served once formed",
            formed.boxed
        ));
    }
    let ids = all_ids(doc);
    if let Some(formed_mid) = &formed.mid
        && schedule.mid_identity_fixed()
    {
        if schedule.drops_mid() {
            if ids.contains(formed_mid) {
                why.push(format!(
                    "the emptied Mid's identity {formed_mid} is still served"
                ));
            }
        } else {
            let served = id_in(mid_of(doc));
            if served.as_deref() != Some(formed_mid.as_str()) {
                why.push(format!(
                    "the Mid W kept alive is served under {served:?}, not {formed_mid}"
                ));
            }
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

/// The first later write: W's target again, by a writer who has delivered U
/// and W: `k` +1, or the leaf with [`Which::Again`]. It reaches objects every
/// writer sees alive and moves no identity (SI.01's C4).
pub fn again_op(tray: &Tray, schedule: &Schedule) -> ModelOp {
    match schedule.target {
        Target::Leaf => tray.write_leaf(schedule.place, schedule.kind, Which::Again),
        target => tray.inc(schedule.place, target, 1),
    }
}

/// What every replica serves after [`again_op`]: [`expected`] with W's
/// counter at 11, or the leaf holding [`Which::Again`].
pub fn again(schedule: &Schedule) -> Value {
    let kind = schedule.kind;
    let kept = kind.kept();
    let a = Some(kind.value(Which::Again));
    let content = match (schedule.place, schedule.target) {
        (Place::Flat, Target::Sibling) => boxed(11, kind, kept, None),
        (Place::Flat, Target::Leaf) => boxed(0, kind, a, None),
        (Place::Deep, Target::Outer) => {
            boxed(11, kind, None, kept.map(|leaf| mid(0, kind, Some(leaf))))
        }
        (Place::Deep, Target::Sibling) => boxed(0, kind, None, Some(mid(11, kind, kept))),
        (Place::Deep, Target::Leaf) => boxed(0, kind, None, Some(mid(0, kind, a))),
        (Place::Direct, Target::Sibling) => boxed(13, kind, kept, None),
        (Place::Direct, Target::Leaf) => boxed(2, kind, a, None),
        (place, target) => panic!("set-up: no {target:?} in {place:?}"),
    };
    document(Some(content))
}

/// The second later write, only where W did not reach the leaf: the leaf
/// written again with [`Which::Rebuild`] by a writer who has delivered U. It
/// holds that write alone, nothing of X; in `Deep` with W on `Box.k` it
/// makes the `Mid` again (SI.01's C5).
pub fn rebuild_op(tray: &Tray, schedule: &Schedule) -> Option<ModelOp> {
    (schedule.target != Target::Leaf)
        .then(|| tray.write_leaf(schedule.place, schedule.kind, Which::Rebuild))
}

/// What every replica serves after [`again_op`] and [`rebuild_op`].
pub fn rebuilt(schedule: &Schedule) -> Value {
    let kind = schedule.kind;
    let r = Some(kind.value(Which::Rebuild));
    let content = match (schedule.place, schedule.target) {
        (Place::Flat, Target::Sibling) => boxed(11, kind, r, None),
        (Place::Deep, Target::Outer) => boxed(11, kind, None, Some(mid(0, kind, r))),
        (Place::Deep, Target::Sibling) => boxed(0, kind, None, Some(mid(11, kind, r))),
        (Place::Direct, Target::Sibling) => boxed(13, kind, r, None),
        (place, target) => panic!("set-up: nothing is rebuilt for {target:?} in {place:?}"),
    };
    document(Some(content))
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
/// identity where W kept it alive, and where the removal emptied and dropped
/// it, it is served under an identity outside `seen` (every identity served
/// before) and its old identity is served nowhere (SI.01's C5).
pub fn check_rebuilt(
    schedule: &Schedule,
    formed: &Formed,
    seen: &BTreeSet<String>,
    reads: &[Reads],
) -> Vec<String> {
    let mut why = agreed_on(reads, &rebuilt(schedule));
    let doc = &reads[0].identified;
    if id_in(Some(doc)).as_deref() != Some(formed.root.as_str()) {
        why.push(format!("the root's identity moved: {doc}"));
    }
    if id_in(boxed_of(doc)).as_deref() != Some(formed.boxed.as_str()) {
        why.push(format!("the Box's identity moved: {doc}"));
    }
    if let Some(formed_mid) = &formed.mid
        && schedule.mid_identity_fixed()
    {
        let served = id_in(mid_of(doc));
        if schedule.drops_mid() {
            match &served {
                Some(id) if !seen.contains(id) => {}
                other => why.push(format!(
                    "the Mid made again after U is served under {other:?}, which is not an \
                     identity nothing carried before"
                )),
            }
            if all_ids(doc).contains(formed_mid) {
                why.push(format!(
                    "the removed Mid's identity {formed_mid} is served again: {doc}"
                ));
            }
        } else if served.as_deref() != Some(formed_mid.as_str()) {
            why.push(format!(
                "the Mid W kept alive is served under {served:?}, not {formed_mid}"
            ));
        }
    }
    let ids = all_ids(doc);
    let count = elements(doc).len();
    if ids.len() != count {
        why.push(format!("{count} elements share {} identities", ids.len()));
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

// ---------------------------------------------------------------------------
// Running the kinds on two threads
// ---------------------------------------------------------------------------

/// `f` for every kind of `kinds`, on two threads (kinds alternate between
/// them), the results in the order of `kinds`. Each replica lives and dies
/// on the thread that made it; nothing is shared but the table and the
/// descriptor, read-only.
pub fn per_kind<T: Send>(kinds: &[Kind], f: impl Fn(Kind) -> T + Sync) -> Vec<(Kind, T)> {
    const WORKERS: usize = 2;
    let f = &f;
    let mut out: Vec<(usize, Kind, T)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..WORKERS)
            .map(|worker| {
                scope.spawn(move || {
                    kinds
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| index % WORKERS == worker)
                        .map(|(index, &kind)| (index, kind, f(kind)))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("a worker thread panicked"))
            .collect()
    });
    out.sort_by_key(|(index, _, _)| *index);
    out.into_iter()
        .map(|(_, kind, value)| (kind, value))
        .collect()
}
