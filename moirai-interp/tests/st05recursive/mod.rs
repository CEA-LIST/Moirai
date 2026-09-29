//! ST.05 round 4 oracle, the part its two test files share: round 3's two
//! metamodels, the operations, the schedules (K7 to K9's again, and the
//! `MapOp::Clear` scenario), the document and the identities the rule fixes
//! at every checkpoint, and the in-process replicas and joiner.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.05.md`, its section "Round
//! 4, written before any rerun". Round 3 (K7 to K9,
//! `st05_recursive_keyed_class.rs`, left as it is) holds a record class that
//! reaches itself through keyed collections at L2 and in process only. This
//! module serves the three scenarios round 4 adds for such a class: the same
//! schedules served by `model_node` processes over the wire (K12), a joiner
//! by state transfer (K11, and K12 over the wire), and a key removed by
//! `MapOp::Clear` (K10, and K12 over the wire).
//!
//! The metamodels are K7 to K9's, built in code from `st05.ecore`'s
//! descriptor exactly as there: the probe's gives `Mid` the keyed `sub` of
//! `Mid`, the cycles' gives it `sub` and `boxes`, a keyed collection of
//! `Box`, so that `Mid` reaches itself directly and `Box` through `Box.sub`
//! and `Mid.boxes`.
//!
//! The rule is the keyed collection's own (spec section 2): a removal, and a
//! `Clear`, which is the same removal at every key the collection holds when
//! it is delivered (`UWMap::Clear`, `uw_map.rs:135-142`, and the interpreter's
//! `MapOp::Clear` arm, `node.rs:1756-1763`), resets what it had seen and
//! keeps what it had not, and the entry stays; the read leaves out an entry
//! that reads as a freshly minted value would, which at a key of a class with
//! no subclass is the object at its default, read after the entries below it
//! have applied the same rule. Nothing here reads the code under test to
//! decide what it expects: every document is written by hand below, and
//! every identity from which operation created each element and whether each
//! writer had delivered the removal.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::leaf::Scalar;
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::StateSnapshot;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Map, Value, json};

use super::si01::{ID, Lang, OPAQUE, Rep, all_ids, default_read, elements, inc_int, strip};

/// `st05.ecore`'s descriptor, the bytes `arachne describe st05.ecore` writes.
pub const ST05: &str = include_str!("../fixtures/st05.metamodel.json");

// ---------------------------------------------------------------------------
// The metamodels, as K7 to K9 build them
// ---------------------------------------------------------------------------

/// The round-2 probe's metamodel: `st05.ecore`'s descriptor with `Box.sub`,
/// a keyed containment of `Mid`, copied onto `Mid`, so `Mid.sub` holds
/// `Mid`s.
pub fn probe_descriptor() -> Value {
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
pub fn cycles_descriptor() -> Value {
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

/// Which of the two metamodels a scenario runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Meta {
    Probe,
    Cycles,
}

impl Meta {
    pub fn descriptor(self) -> Value {
        match self {
            Meta::Probe => probe_descriptor(),
            Meta::Cycles => cycles_descriptor(),
        }
    }
}

// ---------------------------------------------------------------------------
// The operations
// ---------------------------------------------------------------------------

fn key(k: &str) -> Scalar {
    Scalar::text(k)
}

/// The operations of one metamodel. On the probe's, only those that name no
/// `Mid.boxes` may be built: `tick`, `inner`, `box_k` and the removal at
/// `Top`.
pub struct Ops(Lang);

impl Ops {
    pub fn new(meta: Meta) -> Self {
        Ops(Lang::parse(&meta.descriptor()))
    }

    fn holder(&self, inner: InstanceOp) -> ModelOp {
        self.0.is("Holder", inner).into_model_op()
    }

    /// T: `Holder.tick` +1, the first write, which touches no keyed
    /// collection.
    pub fn tick(&self) -> ModelOp {
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

    /// `k` +`by` on the `Box` at key `k`: K10's formation (K), and the
    /// object a removal at `Top` emptied, written again.
    pub fn box_k(&self, by: i64) -> ModelOp {
        self.in_box(self.0.at("Box", "k", inc_int(by)))
    }

    /// `k` +`by` on the `Mid` at key `e` of that `Box`'s `sub`: K10's
    /// formation (E), a second key of the collection `Clear` empties.
    pub fn mid_e(&self, by: i64) -> ModelOp {
        let l = &self.0;
        self.in_box(l.at(
            "Box",
            "sub",
            InstanceOp::entry(key("e"), l.is("Mid", l.at("Mid", "k", inc_int(by)))),
        ))
    }

    /// `k` +`by` on the `Mid` at key `j`: W on `Joint`.
    pub fn joint(&self, by: i64) -> ModelOp {
        self.in_mid(self.0.at("Mid", "k", inc_int(by)))
    }

    /// `k` +`by` on the `Mid` at key `i` of that `Mid`'s own `sub`: N1, and
    /// W on `Inner`.
    pub fn inner(&self, by: i64) -> ModelOp {
        let l = &self.0;
        self.in_mid(l.at(
            "Mid",
            "sub",
            InstanceOp::entry(key("i"), l.is("Mid", l.at("Mid", "k", inc_int(by)))),
        ))
    }

    /// `k` +`by` on the `Box` at key `d` of that `Mid`'s `boxes`: N2, and W
    /// on `Back`.
    pub fn back(&self, by: i64) -> ModelOp {
        let l = &self.0;
        self.in_mid(l.at(
            "Mid",
            "boxes",
            InstanceOp::entry(key("d"), l.is("Box", l.at("Box", "k", inc_int(by)))),
        ))
    }

    /// `k` +`by` on the `Mid` at key `n` of that `Mid`'s own `sub`, a key
    /// nothing wrote before: W on `Fresh`, which creates that `Mid`.
    pub fn fresh(&self, by: i64) -> ModelOp {
        let l = &self.0;
        self.in_mid(l.at(
            "Mid",
            "sub",
            InstanceOp::entry(key("n"), l.is("Mid", l.at("Mid", "k", inc_int(by)))),
        ))
    }

    /// U at a place.
    pub fn removal(&self, place: Place) -> ModelOp {
        let l = &self.0;
        match place {
            Place::Top => self.holder(l.at("Holder", "bag", InstanceOp::remove(key("k")))),
            Place::Direct => self.in_mid(l.at("Mid", "sub", InstanceOp::remove(key("i")))),
            Place::Indirect => self.in_mid(l.at("Mid", "boxes", InstanceOp::remove(key("d")))),
        }
    }

    /// K10's U: `MapOp::Clear` of the `sub` of the `Box` at `k`, the keyed
    /// collection of `Mid` that holds the keys `e` and `j` once formed.
    pub fn clear(&self) -> ModelOp {
        self.in_box(self.0.at("Box", "sub", InstanceOp::clear()))
    }

    /// W's target +`by`.
    pub fn write(&self, target: Target, by: i64) -> ModelOp {
        match target {
            Target::Joint => self.joint(by),
            Target::Inner => self.inner(by),
            Target::Back => self.back(by),
            Target::Fresh => self.fresh(by),
        }
    }
}

/// Where a removal alone takes a key out (K8): `Top`, key `k` of
/// `Holder.bag`; `Direct`, key `i` of the `Mid` at `j`'s own `sub`;
/// `Indirect`, key `d` of that `Mid`'s `boxes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    Top,
    Direct,
    Indirect,
}

/// What W increments by 10: the `Mid` at `j` (`Joint`), the `Mid` at `i`
/// in its `sub` (`Inner`), the `Box` at `d` in its `boxes` (`Back`), or a
/// `Mid` at `n` in its `sub` that W creates (`Fresh`). K9 races the removal
/// at `Top` with the first three; K10 races `Clear` with the last three, two
/// keyed levels below the collection it empties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Joint,
    Inner,
    Back,
    Fresh,
}

// ---------------------------------------------------------------------------
// The documents the rule gives, by hand
// ---------------------------------------------------------------------------

/// A `Box` as the default read serves it: `note` and `mid` never written, so
/// absent.
pub fn boxed(k: i64, sub: Value) -> Value {
    json!({"eClass": "Box", "k": k, "sub": sub})
}

/// A `Mid` of the probe's metamodel.
pub fn probe_mid(k: i64, sub: Value) -> Value {
    json!({"eClass": "Mid", "k": k, "sub": sub})
}

/// A `Mid` of the cycles' metamodel.
pub fn mid(k: i64, boxes: Value, sub: Value) -> Value {
    json!({"boxes": boxes, "eClass": "Mid", "k": k, "sub": sub})
}

/// The whole document: `tick` 1, `bag` holding `held` at key `k` or
/// nothing, `inner` never written, `shapes` and `bins` empty.
pub fn document(held: Option<Value>) -> Value {
    let bag = match held {
        Some(held) => json!({ "k": held }),
        None => json!({}),
    };
    json!({"bag": bag, "bins": {}, "eClass": "Holder", "shapes": {}, "tick": 1})
}

/// The `Mid` at `j` of the cycles' metamodel as served, every entry given by
/// hand: its own `k`, the `Box` at `d` in its `boxes` (that `Box`'s `k`, or
/// no entry), the `Mid`s at `i` and at `n` in its `sub` (their `k`, or no
/// entry). Each entry given is a `Box` or `Mid` whose own keyed collections
/// serve nothing.
pub fn mid_j(k: i64, d: Option<i64>, i: Option<i64>, n: Option<i64>) -> Value {
    let mut boxes = Map::new();
    if let Some(d) = d {
        boxes.insert("d".to_string(), boxed(d, json!({})));
    }
    let mut sub = Map::new();
    if let Some(i) = i {
        sub.insert("i".to_string(), mid(i, json!({}), json!({})));
    }
    if let Some(n) = n {
        sub.insert("n".to_string(), mid(n, json!({}), json!({})));
    }
    mid(k, Value::Object(boxes), Value::Object(sub))
}

/// The document of the cycles' metamodel with the `Box` at `k` served, every
/// entry given by hand: its own `k`, the `Mid` at `e` in its `sub` (that
/// `Mid`'s `k`, or no entry) and the `Mid` at `j` (a [`mid_j`], or no
/// entry).
pub fn with_box(k: i64, e: Option<i64>, j: Option<Value>) -> Value {
    let mut sub = Map::new();
    if let Some(e) = e {
        sub.insert("e".to_string(), mid(e, json!({}), json!({})));
    }
    if let Some(j) = j {
        sub.insert("j".to_string(), j);
    }
    document(Some(boxed(k, Value::Object(sub))))
}

/// K7, once the probe's write is delivered everywhere: round 3's R3.2.
pub fn probe_written() -> Value {
    document(Some(boxed(
        0,
        json!({"j": probe_mid(0, json!({"i": probe_mid(1, json!({}))}))}),
    )))
}

/// K8 and K9, once N1 is delivered everywhere: round 3's R3.2.
pub fn after_inner() -> Value {
    document(Some(boxed(
        0,
        json!({"j": mid(0, json!({}), json!({"i": mid(1, json!({}), json!({}))}))}),
    )))
}

/// K8 and K9, once N1 and N2 are delivered everywhere, formed: round 3's
/// R3.2.
pub fn formed() -> Value {
    document(Some(boxed(
        0,
        json!({"j": mid(
            0,
            json!({"d": boxed(1, json!({}))}),
            json!({"i": mid(1, json!({}), json!({}))}),
        )}),
    )))
}

/// K8, once U alone is delivered everywhere: round 3's R3.2.
pub fn alone(place: Place) -> Value {
    match place {
        Place::Top => document(None),
        Place::Direct => document(Some(boxed(
            0,
            json!({"j": mid(0, json!({"d": boxed(1, json!({}))}), json!({}))}),
        ))),
        Place::Indirect => after_inner(),
    }
}

/// K9, once U and W are delivered everywhere: round 3's R3.2.
pub fn raced(target: Target) -> Value {
    let j = match target {
        Target::Joint => mid(10, json!({}), json!({})),
        Target::Inner => mid(0, json!({}), json!({"i": mid(10, json!({}), json!({}))})),
        Target::Back => mid(0, json!({"d": boxed(10, json!({}))}), json!({})),
        Target::Fresh => panic!("set-up: K9 races no write at a fresh key"),
    };
    document(Some(boxed(0, json!({ "j": j }))))
}

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

/// An element the operations create: the root `Holder`, the `Box` at `k`
/// (`BoxK`), the `Mid` at `j` in its `sub` (`MidJ`), the `Mid` at `i` in
/// that `Mid`'s `sub` (`MidI`), the `Box` at `d` in that `Mid`'s `boxes`
/// (`BoxD`), the `Mid` at `e` in the `Box`'s `sub` (`MidE`), the `Mid` at
/// `n` in `MidJ`'s `sub` (`MidN`, created by W on `Fresh`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Root,
    BoxK,
    MidJ,
    MidI,
    BoxD,
    MidE,
    MidN,
}

use Role::{BoxD, BoxK, MidE, MidI, MidJ, MidN, Root};

fn path(role: Role) -> &'static [&'static str] {
    match role {
        Root => &[],
        BoxK => &["bag", "k"],
        MidJ => &["bag", "k", "sub", "j"],
        MidI => &["bag", "k", "sub", "j", "sub", "i"],
        BoxD => &["bag", "k", "sub", "j", "boxes", "d"],
        MidE => &["bag", "k", "sub", "e"],
        MidN => &["bag", "k", "sub", "j", "sub", "n"],
    }
}

/// The identity the element of `role` carries in an identified read, if it
/// is served.
pub fn id_at(doc: &Value, role: Role) -> Option<String> {
    let mut at = doc;
    for step in path(role) {
        at = at.get(*step)?;
    }
    at.get(ID).and_then(Value::as_str).map(str::to_string)
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
// What a checkpoint expects, and the verdict
// ---------------------------------------------------------------------------

/// What every replica must serve at a checkpoint, once each has delivered
/// every operation sent: `want`, in both forms; exactly the elements of
/// `kept` and `renewed`; each `kept` element under the identity it was
/// served under before; each `renewed` element (one an operation of this
/// step created, or wrote again after a removal had emptied it) under an
/// identity no checkpoint of the run served before; and the identity served
/// before for each element of `left_out` served nowhere.
#[derive(Clone, Debug)]
pub struct Expect {
    pub want: Value,
    pub kept: Vec<Role>,
    pub renewed: Vec<Role>,
    pub left_out: Vec<Role>,
}

impl Expect {
    fn new(want: Value, kept: &[Role], renewed: &[Role], left_out: &[Role]) -> Self {
        Expect {
            want,
            kept: kept.to_vec(),
            renewed: renewed.to_vec(),
            left_out: left_out.to_vec(),
        }
    }

    /// The same checkpoint read again later, by a replica that joined: what
    /// was renewed there is kept now, under the identity served there.
    pub fn settled(&self) -> Expect {
        let mut kept = self.kept.clone();
        kept.extend(self.renewed.iter().copied());
        kept.sort();
        Expect {
            want: self.want.clone(),
            kept,
            renewed: Vec::new(),
            left_out: self.left_out.clone(),
        }
    }
}

/// One replica's two reads, named.
pub struct Reads {
    pub who: String,
    pub default: Value,
    pub identified: Value,
}

/// What one checkpoint found. A checkpoint counts once per kind it fails.
#[derive(Default)]
pub struct Verdict {
    pub parted: bool,
    pub value: bool,
    pub r_prime: bool,
    pub identity: bool,
    pub lines: Vec<String>,
    /// The identities the first reader serves for the `kept` and `renewed`
    /// roles.
    pub ids: BTreeMap<Role, String>,
    /// Every identity any reader serves.
    pub served: BTreeSet<String>,
}

/// Judge the reads of every replica at a checkpoint: one default and one
/// identified read, `expect.want`, R' (the identified read stripped is the
/// default read), and the identities, `known` holding the identity each
/// role was last served under and `seen` every identity served at an
/// earlier checkpoint of the run.
pub fn judge(
    label: &str,
    reads: &[Reads],
    expect: &Expect,
    known: &BTreeMap<Role, String>,
    seen: &BTreeSet<String>,
) -> Verdict {
    let mut v = Verdict::default();
    if reads.windows(2).any(|p| p[0].default != p[1].default)
        || reads.windows(2).any(|p| p[0].identified != p[1].identified)
    {
        v.parted = true;
        v.lines.push(format!(
            "{label}: the replicas part: {}",
            reads
                .iter()
                .map(|r| format!("{} serves {} identified {}", r.who, r.default, r.identified))
                .collect::<Vec<_>>()
                .join(" / ")
        ));
    }
    for r in reads {
        let who = &r.who;
        if r.default != expect.want {
            v.value = true;
            v.lines.push(format!(
                "{label}: {who} serves {}, not {}",
                r.default, expect.want
            ));
        }
        if strip(&r.identified, OPAQUE) != r.default {
            v.r_prime = true;
            v.lines.push(format!(
                "{label}: {who}'s identified read stripped is not its default read: {} | {}",
                r.identified, r.default
            ));
        }
        match element_ids(&r.identified) {
            Err(why) => {
                v.identity = true;
                v.lines.push(format!("{label}: {who}: {why}"));
            }
            Ok(ids) if ids.len() != expect.kept.len() + expect.renewed.len() => {
                v.identity = true;
                v.lines.push(format!(
                    "{label}: {who} serves {} elements where {:?} and {:?} are served: {}",
                    ids.len(),
                    expect.kept,
                    expect.renewed,
                    r.identified
                ));
            }
            Ok(_) => {}
        }
        for &role in &expect.kept {
            match (id_at(&r.identified, role), known.get(&role)) {
                (None, _) => {
                    v.identity = true;
                    v.lines.push(format!(
                        "{label}: {who} serves no {role:?} under an identity: {}",
                        r.identified
                    ));
                }
                (Some(id), Some(was)) if id != *was => {
                    v.identity = true;
                    v.lines.push(format!(
                        "{label}: {who} serves {role:?} under {id}, which was {was}"
                    ));
                }
                _ => {}
            }
        }
        for &role in &expect.renewed {
            match id_at(&r.identified, role) {
                None => {
                    v.identity = true;
                    v.lines.push(format!(
                        "{label}: {who} serves no {role:?} under an identity: {}",
                        r.identified
                    ));
                }
                Some(id) if seen.contains(&id) => {
                    v.identity = true;
                    v.lines.push(format!(
                        "{label}: {who} serves {role:?} under {id}, an identity served before, \
                         where it is created or written again after a removal"
                    ));
                }
                Some(_) => {}
            }
        }
        let served = all_ids(&r.identified);
        for role in &expect.left_out {
            if let Some(was) = known.get(role)
                && served.contains(was)
            {
                v.identity = true;
                v.lines.push(format!(
                    "{label}: {who} still serves {was}, the identity of the {role:?} left out"
                ));
            }
        }
        v.served.extend(served);
    }
    if let Some(first) = reads.first() {
        for &role in expect.kept.iter().chain(&expect.renewed) {
            if let Some(id) = id_at(&first.identified, role) {
                v.ids.insert(role, id);
            }
        }
    }
    v
}

/// Every checkpoint judged and every read made, by kind of failure.
#[derive(Default)]
pub struct Tally {
    pub checkpoints: usize,
    pub parted: usize,
    pub value: usize,
    pub identity: usize,
    pub r_prime: usize,
    pub step_reads: usize,
    pub step_r_prime: usize,
    /// Over the wire: node reads made at checkpoints (both forms each).
    pub reads: usize,
    /// Over the wire: reads a node did not answer.
    pub unanswered: usize,
    /// Over the wire: joiners that did not adopt a transferred state.
    pub not_adopted: usize,
    pub lines: Vec<String>,
}

impl Tally {
    pub fn failures(&self) -> usize {
        self.parted
            + self.value
            + self.identity
            + self.r_prime
            + self.step_r_prime
            + self.unanswered
            + self.not_adopted
    }

    pub fn add(&mut self, v: Verdict) {
        self.checkpoints += 1;
        self.parted += usize::from(v.parted);
        self.value += usize::from(v.value);
        self.r_prime += usize::from(v.r_prime);
        self.identity += usize::from(v.identity);
        self.lines.extend(v.lines);
    }

    /// The in-process report line, K7 to K9's.
    pub fn line(&self, name: &str, runs: usize) -> String {
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
}

/// Print the report line and hold the tally to no failure.
pub fn report(line: &str, tally: &Tally) {
    println!("{line}");
    for failure in tally.lines.iter().take(12) {
        println!("  {failure}");
    }
    assert!(
        tally.failures() == 0,
        "{line}; first failures:\n{}",
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
// The schedules
// ---------------------------------------------------------------------------

/// Every assignment of U and W to two of the three replicas.
pub const SEATS: [(usize, usize); 6] = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)];

/// One schedule. Seats are replica indices in process and node indices over
/// the wire (0 alice, 1 bob, 2 carol). The formation is written by seat 0,
/// each write delivered everywhere before the next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scenario {
    /// K7, the round-2 probe, on its metamodel: T and N1 on 0, then U at
    /// `Top` on 1.
    Probe,
    /// K8 on the cycles' metamodel: T, N1 and N2, then U alone at `place` on
    /// `u`.
    Alone { place: Place, u: usize },
    /// K9: T, N1 and N2, then U at `Top` on `uw.0` and W (+10 on `target`)
    /// on `uw.1`, concurrently; each writer delivers the other's, and the
    /// third seat delivers U first when `u_first`.
    Race {
        target: Target,
        uw: (usize, usize),
        u_first: bool,
    },
    /// K10: T, N1, N2, E and K, then `Clear` of the `Box` at `k`'s `sub`,
    /// alone, on `u`.
    ClearAlone { u: usize },
    /// K10: the same formation, then that `Clear` on `uw.0` and W (+10 on
    /// `target`, two keyed levels below it) on `uw.1`, as K9 races.
    ClearRace {
        target: Target,
        uw: (usize, usize),
        u_first: bool,
    },
}

/// How a schedule ends.
pub enum Ending {
    /// One operation on one seat, delivered everywhere.
    Alone { seat: usize, op: ModelOp },
    /// U on `uw.0` and W on `uw.1`, concurrently.
    Race {
        uw: (usize, usize),
        u: ModelOp,
        w: ModelOp,
        u_first: bool,
    },
}

impl Scenario {
    pub fn meta(self) -> Meta {
        match self {
            Scenario::Probe => Meta::Probe,
            _ => Meta::Cycles,
        }
    }

    pub fn is_race(self) -> bool {
        matches!(self, Scenario::Race { .. } | Scenario::ClearRace { .. })
    }

    /// The writes of the formation, each from seat 0, with the checkpoint
    /// that follows it.
    pub fn formation(self, ops: &Ops) -> Vec<(&'static str, ModelOp, Expect)> {
        let mut steps = vec![(
            "after T",
            ops.tick(),
            Expect::new(document(None), &[], &[Root], &[]),
        )];
        if self == Scenario::Probe {
            steps.push((
                "after the write",
                ops.inner(1),
                Expect::new(probe_written(), &[Root], &[BoxK, MidJ, MidI], &[]),
            ));
            return steps;
        }
        steps.push((
            "after N1",
            ops.inner(1),
            Expect::new(after_inner(), &[Root], &[BoxK, MidJ, MidI], &[]),
        ));
        steps.push((
            "after N2",
            ops.back(1),
            Expect::new(formed(), &[Root, BoxK, MidJ, MidI], &[BoxD], &[]),
        ));
        if matches!(
            self,
            Scenario::ClearAlone { .. } | Scenario::ClearRace { .. }
        ) {
            steps.push((
                "after E",
                ops.mid_e(1),
                Expect::new(
                    with_box(0, Some(1), Some(mid_j(0, Some(1), Some(1), None))),
                    &[Root, BoxK, MidJ, MidI, BoxD],
                    &[MidE],
                    &[],
                ),
            ));
            steps.push((
                "after K",
                ops.box_k(1),
                Expect::new(
                    with_box(1, Some(1), Some(mid_j(0, Some(1), Some(1), None))),
                    &[Root, BoxK, MidJ, MidI, BoxD, MidE],
                    &[],
                    &[],
                ),
            ));
        }
        steps
    }

    /// How the schedule ends.
    pub fn ending(self, ops: &Ops) -> Ending {
        match self {
            Scenario::Probe => Ending::Alone {
                seat: 1,
                op: ops.removal(Place::Top),
            },
            Scenario::Alone { place, u } => Ending::Alone {
                seat: u,
                op: ops.removal(place),
            },
            Scenario::Race {
                target,
                uw,
                u_first,
            } => Ending::Race {
                uw,
                u: ops.removal(Place::Top),
                w: ops.write(target, 10),
                u_first,
            },
            Scenario::ClearAlone { u } => Ending::Alone {
                seat: u,
                op: ops.clear(),
            },
            Scenario::ClearRace {
                target,
                uw,
                u_first,
            } => Ending::Race {
                uw,
                u: ops.clear(),
                w: ops.write(target, 10),
                u_first,
            },
        }
    }

    /// The checkpoint once the ending is delivered everywhere.
    pub fn ended(self) -> Expect {
        match self {
            Scenario::Probe => Expect::new(document(None), &[Root], &[], &[BoxK, MidJ, MidI]),
            Scenario::Alone { place, .. } => match place {
                Place::Top => Expect::new(alone(place), &[Root], &[], &[BoxK, MidJ, MidI, BoxD]),
                Place::Direct => Expect::new(alone(place), &[Root, BoxK, MidJ, BoxD], &[], &[MidI]),
                Place::Indirect => {
                    Expect::new(alone(place), &[Root, BoxK, MidJ, MidI], &[], &[BoxD])
                }
            },
            Scenario::Race { target, .. } => match target {
                Target::Joint => {
                    Expect::new(raced(target), &[Root, BoxK, MidJ], &[], &[MidI, BoxD])
                }
                Target::Inner => {
                    Expect::new(raced(target), &[Root, BoxK, MidJ, MidI], &[], &[BoxD])
                }
                Target::Back => Expect::new(raced(target), &[Root, BoxK, MidJ, BoxD], &[], &[MidI]),
                Target::Fresh => panic!("set-up: K9 races no write at a fresh key"),
            },
            Scenario::ClearAlone { .. } => Expect::new(
                with_box(1, None, None),
                &[Root, BoxK],
                &[],
                &[MidJ, MidI, BoxD, MidE],
            ),
            Scenario::ClearRace { target, .. } => match target {
                Target::Inner => Expect::new(
                    with_box(1, None, Some(mid_j(0, None, Some(10), None))),
                    &[Root, BoxK, MidJ, MidI],
                    &[],
                    &[BoxD, MidE],
                ),
                Target::Back => Expect::new(
                    with_box(1, None, Some(mid_j(0, Some(10), None, None))),
                    &[Root, BoxK, MidJ, BoxD],
                    &[],
                    &[MidI, MidE],
                ),
                Target::Fresh => Expect::new(
                    with_box(1, None, Some(mid_j(0, None, None, Some(10)))),
                    &[Root, BoxK, MidJ],
                    &[MidN],
                    &[MidI, BoxD, MidE],
                ),
                Target::Joint => panic!("set-up: K10 races no write on the Mid at j itself"),
            },
        }
    }

    /// Two writes after the ending, each delivered everywhere, with the
    /// checkpoint after each: the first by a writer `w`, the second by
    /// another, `n`. After a removal alone or a `Clear` alone, the object
    /// emptied at the removed key written again (+5), under an identity
    /// nothing carried before, then once more (+1), which moves nothing.
    /// After a race, W's target again (+1), which moves nothing, then an
    /// object the race left emptied written again (+5), under a new
    /// identity.
    pub fn later(self, ops: &Ops) -> [(ModelOp, Expect); 2] {
        match self {
            Scenario::Probe => [
                (
                    ops.box_k(5),
                    Expect::new(
                        document(Some(boxed(5, json!({})))),
                        &[Root],
                        &[BoxK],
                        &[MidJ, MidI],
                    ),
                ),
                (
                    ops.box_k(1),
                    Expect::new(
                        document(Some(boxed(6, json!({})))),
                        &[Root, BoxK],
                        &[],
                        &[MidJ, MidI],
                    ),
                ),
            ],
            Scenario::Alone { place, .. } => match place {
                Place::Top => [
                    (
                        ops.box_k(5),
                        Expect::new(
                            with_box(5, None, None),
                            &[Root],
                            &[BoxK],
                            &[MidJ, MidI, BoxD],
                        ),
                    ),
                    (
                        ops.box_k(1),
                        Expect::new(
                            with_box(6, None, None),
                            &[Root, BoxK],
                            &[],
                            &[MidJ, MidI, BoxD],
                        ),
                    ),
                ],
                Place::Direct => [
                    (
                        ops.inner(5),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(1), Some(5), None))),
                            &[Root, BoxK, MidJ, BoxD],
                            &[MidI],
                            &[],
                        ),
                    ),
                    (
                        ops.inner(1),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(1), Some(6), None))),
                            &[Root, BoxK, MidJ, MidI, BoxD],
                            &[],
                            &[],
                        ),
                    ),
                ],
                Place::Indirect => [
                    (
                        ops.back(5),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(5), Some(1), None))),
                            &[Root, BoxK, MidJ, MidI],
                            &[BoxD],
                            &[],
                        ),
                    ),
                    (
                        ops.back(1),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(6), Some(1), None))),
                            &[Root, BoxK, MidJ, MidI, BoxD],
                            &[],
                            &[],
                        ),
                    ),
                ],
            },
            Scenario::Race { target, .. } => match target {
                Target::Joint => [
                    (
                        ops.joint(1),
                        Expect::new(
                            with_box(0, None, Some(mid_j(11, None, None, None))),
                            &[Root, BoxK, MidJ],
                            &[],
                            &[MidI, BoxD],
                        ),
                    ),
                    (
                        ops.inner(5),
                        Expect::new(
                            with_box(0, None, Some(mid_j(11, None, Some(5), None))),
                            &[Root, BoxK, MidJ],
                            &[MidI],
                            &[BoxD],
                        ),
                    ),
                ],
                Target::Inner => [
                    (
                        ops.inner(1),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, None, Some(11), None))),
                            &[Root, BoxK, MidJ, MidI],
                            &[],
                            &[BoxD],
                        ),
                    ),
                    (
                        ops.back(5),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(5), Some(11), None))),
                            &[Root, BoxK, MidJ, MidI],
                            &[BoxD],
                            &[],
                        ),
                    ),
                ],
                Target::Back => [
                    (
                        ops.back(1),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(11), None, None))),
                            &[Root, BoxK, MidJ, BoxD],
                            &[],
                            &[MidI],
                        ),
                    ),
                    (
                        ops.inner(5),
                        Expect::new(
                            with_box(0, None, Some(mid_j(0, Some(11), Some(5), None))),
                            &[Root, BoxK, MidJ, BoxD],
                            &[MidI],
                            &[],
                        ),
                    ),
                ],
                Target::Fresh => panic!("set-up: K9 races no write at a fresh key"),
            },
            Scenario::ClearAlone { .. } => [
                (
                    ops.mid_e(5),
                    Expect::new(
                        with_box(1, Some(5), None),
                        &[Root, BoxK],
                        &[MidE],
                        &[MidJ, MidI, BoxD],
                    ),
                ),
                (
                    ops.mid_e(1),
                    Expect::new(
                        with_box(1, Some(6), None),
                        &[Root, BoxK, MidE],
                        &[],
                        &[MidJ, MidI, BoxD],
                    ),
                ),
            ],
            Scenario::ClearRace { target, .. } => match target {
                Target::Inner => [
                    (
                        ops.inner(1),
                        Expect::new(
                            with_box(1, None, Some(mid_j(0, None, Some(11), None))),
                            &[Root, BoxK, MidJ, MidI],
                            &[],
                            &[BoxD, MidE],
                        ),
                    ),
                    (
                        ops.mid_e(5),
                        Expect::new(
                            with_box(1, Some(5), Some(mid_j(0, None, Some(11), None))),
                            &[Root, BoxK, MidJ, MidI],
                            &[MidE],
                            &[BoxD],
                        ),
                    ),
                ],
                Target::Back => [
                    (
                        ops.back(1),
                        Expect::new(
                            with_box(1, None, Some(mid_j(0, Some(11), None, None))),
                            &[Root, BoxK, MidJ, BoxD],
                            &[],
                            &[MidI, MidE],
                        ),
                    ),
                    (
                        ops.mid_e(5),
                        Expect::new(
                            with_box(1, Some(5), Some(mid_j(0, Some(11), None, None))),
                            &[Root, BoxK, MidJ, BoxD],
                            &[MidE],
                            &[MidI],
                        ),
                    ),
                ],
                Target::Fresh => [
                    (
                        ops.fresh(1),
                        Expect::new(
                            with_box(1, None, Some(mid_j(0, None, None, Some(11)))),
                            &[Root, BoxK, MidJ, MidN],
                            &[],
                            &[MidI, BoxD, MidE],
                        ),
                    ),
                    (
                        ops.mid_e(5),
                        Expect::new(
                            with_box(1, Some(5), Some(mid_j(0, None, None, Some(11)))),
                            &[Root, BoxK, MidJ, MidN],
                            &[MidE],
                            &[MidI, BoxD],
                        ),
                    ),
                ],
                Target::Joint => panic!("set-up: K10 races no write on the Mid at j itself"),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// In process: replicas, steps and checkpoints
// ---------------------------------------------------------------------------

/// The identified read, `QueryableLog::query_identified_state_json`.
pub fn identified_read(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

/// A fourth replica, `d`, joining by state transfer from `donor`: it is
/// bootstrapped knowing only itself on the donor's log, and adopts the
/// donor's causal snapshot and its log, each through a serde round trip, as
/// `GenericNode::adopt_state` installs what a donor sent over the wire. The
/// same helper as `st05/mod.rs::joiner` (K4's), written here so that this
/// module reads no file of K1 to K6.
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

/// The replicas of one run, the tally its judged steps read into, and what
/// the run has served so far.
pub struct Run<'a> {
    pub reps: Vec<Rep>,
    pub tally: &'a mut Tally,
    pub label: String,
    /// Whether steps and checkpoints are judged and counted. When not, a
    /// checkpoint only reads the identities served, so that later
    /// checkpoints can be judged against them.
    pub judge: bool,
    /// Every event sent, in the order sent.
    pub sent: Vec<EventMessage<ModelOp>>,
    pub steps: usize,
    /// A joiner taken after this step from this seat.
    pub join: Option<(usize, usize)>,
    pub joined: Option<Rep>,
    /// The identity each role was last served under.
    pub known: BTreeMap<Role, String>,
    /// Every identity served at a checkpoint of this run.
    pub seen: BTreeSet<String>,
}

impl<'a> Run<'a> {
    pub fn new(reps: Vec<Rep>, tally: &'a mut Tally, label: String, judge: bool) -> Self {
        Run {
            reps,
            tally,
            label,
            judge,
            sent: Vec::new(),
            steps: 0,
            join: None,
            joined: None,
            known: BTreeMap::new(),
            seen: BTreeSet::new(),
        }
    }

    fn names(&self) -> Vec<String> {
        (0..self.reps.len())
            .map(|seat| format!("replica {seat}"))
            .collect()
    }

    /// After a send or a delivery: R' on both reads of every replica, when
    /// judged; and the joiner, when this is its step.
    fn stepped(&mut self) {
        self.steps += 1;
        if self.judge {
            for (seat, replica) in self.reps.iter().enumerate() {
                let default = default_read(replica);
                let identified = identified_read(replica);
                self.tally.step_reads += 1;
                if strip(&identified, OPAQUE) != default {
                    self.tally.step_r_prime += 1;
                    self.tally.lines.push(format!(
                        "{}: after a step, replica {seat}'s identified read stripped is not its \
                         default read: {identified} | {default}",
                        self.label
                    ));
                }
            }
        }
        if let Some((at, donor)) = self.join
            && at == self.steps
        {
            self.joined = Some(joiner(&self.reps[donor]));
        }
    }

    pub fn send(&mut self, seat: usize, op: ModelOp) -> EventMessage<ModelOp> {
        let event = self.reps[seat]
            .send(op.clone())
            .unwrap_or_else(|why| panic!("set-up: replica {seat} refused {op:?}: {why:?}"));
        self.sent.push(event.clone());
        self.stepped();
        event
    }

    pub fn deliver(&mut self, seat: usize, event: &EventMessage<ModelOp>) {
        self.reps[seat].receive(event.clone());
        self.stepped();
    }

    /// `op` sent on `seat` and delivered to every other replica, in seat
    /// order.
    pub fn broadcast(&mut self, seat: usize, op: ModelOp) {
        let event = self.send(seat, op);
        for other in 0..self.reps.len() {
            if other != seat {
                self.deliver(other, &event);
            }
        }
    }

    /// Two operations written concurrently on seats `p` and `q` of three,
    /// each delivered to the other writer, then to the third seat, `op_p`
    /// first when `first_p`.
    pub fn race(&mut self, (p, q): (usize, usize), op_p: ModelOp, op_q: ModelOp, first_p: bool) {
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

    /// A checkpoint: every replica has delivered every operation sent.
    pub fn checkpoint(&mut self, at: &str, expect: &Expect) {
        let label = format!("{}, {at}", self.label);
        let names = self.names();
        let reads: Vec<Reads> = self
            .reps
            .iter()
            .zip(names)
            .map(|(replica, who)| Reads {
                who,
                default: default_read(replica),
                identified: identified_read(replica),
            })
            .collect();
        let mut verdict = judge(&label, &reads, expect, &self.known, &self.seen);
        self.known.extend(std::mem::take(&mut verdict.ids));
        self.seen.extend(std::mem::take(&mut verdict.served));
        if self.judge {
            self.tally.add(verdict);
        }
    }

    /// The schedule: its formation, each write from seat 0 and delivered
    /// everywhere, each followed by its checkpoint; then its ending and the
    /// checkpoint after it.
    pub fn play(&mut self, scenario: Scenario, ops: &Ops) {
        for (at, op, expect) in scenario.formation(ops) {
            self.broadcast(0, op);
            self.checkpoint(at, &expect);
        }
        match scenario.ending(ops) {
            Ending::Alone { seat, op } => self.broadcast(seat, op),
            Ending::Race { uw, u, w, u_first } => self.race(uw, u, w, u_first),
        }
        self.checkpoint("after the ending", &scenario.ended());
    }

    /// The two later writes of [`Scenario::later`], by `w` then by `n`.
    pub fn later(&mut self, scenario: Scenario, ops: &Ops, w: usize, n: usize) {
        let [(first, after_first), (second, after_second)] = scenario.later(ops);
        self.broadcast(w, first);
        self.checkpoint("after the first later write", &after_first);
        self.broadcast(n, second);
        self.checkpoint("after the second later write", &after_second);
    }
}
