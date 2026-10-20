//! SI.14 oracle, the part its test files share: the contract the tests read,
//! the five new revisions of SimpleUML, the expected documents computed by
//! hand from SI.12's running example, and the migration status as the
//! contract fixes it.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.14.md`; the design it
//! follows is `cea-cdrt-knowledge/spec/Migration strategies, copy and
//! rebase.md` (Cam accepted Q1 to Q9). SI.12's contract (M1 to M7, restated
//! in `si12/mod.rs`) and SI.13's (M8 to M14) stand. SI.14 adds rebase, a
//! second strategy on the one migrate route, restated here so that a reader
//! of the tests does not need the spec open:
//!
//! - M15, the route. `POST /api/model/{m}/migrate` reads `strategy`
//!   (`"copy"` or `"rebase"`; absent means copy, SI.12's and SI.13's
//!   contract unchanged) and `drops` (an array of names, absent meaning
//!   none). A rebase is answered 2xx with `model_id` equal to `m` and
//!   `strategy` equal to `"rebase"`, only once the answering node has
//!   delivered its own rebase, so that its status names the rebase from the
//!   first read after the answer; a refused one is answered 422 with the
//!   reason (M19).
//! - M16, the action in process. `moirai_interp::migrate::rebase(source:
//!   &ModelLog, descriptor: &str, drops: &[String])` answers `Ok(op)`, the
//!   one `ModelOp` the migrating replica sends on `m`'s own log with
//!   `Replica::send` (which accepts it), or `Err(why)` with `why: Display`,
//!   in which case nothing is sent.
//! - M17, the model after a rebase (RA). On a replica that keeps a rebase
//!   to `D2` (M22): `header().model_id` is unchanged; `header().metamodel_id`
//!   and `semantics().digest` are `D2`'s digest; `descriptor()` is `D2`'s
//!   text; every object keeps the identity it had; both reads serve the
//!   expected document under `D2`; `unresolved()` stays 0; the local intake
//!   checks every new operation against `D2` (a `D2` operation is accepted,
//!   one spelled by `D1`'s slots and rules is refused). Over HTTP the node
//!   lists `m` under `D2`'s digest and serves `D2`'s text on
//!   `GET /api/model/{m}/metamodel`.
//! - M18, translation (RB, RC). The result on every replica is the state of
//!   `m` at the rebase's version mapped to `D2`, then every other delivered
//!   operation translated (H1 of the design note): slots relabelled by name;
//!   a character edit of a text that `D2` makes a fair or last-writer-wins
//!   register becomes the write of the text read at the edit's own version
//!   with the edit applied, tagged with the edit's own id and Lamport stamp;
//!   the base register holds the translation of the text's greatest event by
//!   the register's own rule. Whatever the delivery order.
//! - M19, refusals (RE). `rebase` answers `Err`, and nothing is written, for
//!   a revision that lacks a class or feature of `D1` that `drops` does not
//!   list (the reason names it as copy's refusal names it, `<Class>` or
//!   `<Class>.<feature>` of the declaring class), for a change of merge rule
//!   other than text to fair register and text to last-writer-wins register
//!   (the reason names `<Class>.<feature>`, and holds the word `copy` when
//!   strategy copy accepts the revision), and for a change of a feature's
//!   kind (the reason names it). Afterwards the model is under `D1`, names
//!   no migration, and accepts an edit.
//! - M20, removal (RD). With `drops` listing every class and feature `D2`
//!   lacks: no read serves a removed feature or an object of a removed class
//!   or under a removed containment; `rebase.hidden` lists the identities of
//!   the objects the `D1` read at the rebase's version served that the `D2`
//!   read does not; an operation concurrent with the rebase, written under
//!   `D1`, whose path reaches a removed feature or class is applied as
//!   nothing and listed under `rebase.dropped` as `{origin, seq, feature}`,
//!   `feature` the first removed name on its path from the root, spelled as
//!   in `drops`; a `D1` operation's sequence positions are resolved against
//!   its own version on a sequence that still holds the hidden entries, and
//!   a `D2` operation's positions count only what `D2` serves.
//! - M21, `D2` travels (RF). The rebase carries `D2`'s text: a replica that
//!   held no copy of `D2` ends with `descriptor()` equal to it, and a node
//!   that serves `D1` alone lists `m` under `D2`'s digest and serves `D2` on
//!   `GET /api/model/{m}/metamodel`.
//! - M22, one order (RG). Of the migration operations a replica delivered,
//!   cuts and rebases alike, it keeps the greatest by SI.13's M11 rule (the
//!   greatest Lamport stamp, the sum of the operation's version; then the
//!   least origin id compared as bytes; then the least sequence number). A
//!   kept cut is SI.12 and SI.13 (frozen documents, `refused`, read-only;
//!   the table `D1`). A kept rebase puts `m` under its revision, lists every
//!   other delivered rebase under `superseded_rebases` and every delivered
//!   cut's successor under `superseded`, names no successor, refuses
//!   nothing, and treats an instance operation whose causal past holds a
//!   migration operation other than the kept rebase, and not the kept rebase,
//!   as written under a superseded migration: applied as nothing and listed
//!   under `rebase.discarded`.
//! - M23, the window (RH). `rebase.d1_held` is true from the rebase's
//!   delivery until it is causally stable on the replica, and false after;
//!   `rebase.waiting_for` lists the members whose delivery of the rebase the
//!   replica has not heard of (no event of theirs it delivered has the
//!   rebase in its causal past), `[]` once stable. No timer drops `D1`.
//!
//! The status, `ModelLog::migration()` and `GET /api/model/{m}/migration`,
//! keeps SI.12's and SI.13's keys (`migrated_to`, `refused`, `superseded`)
//! and adds `strategy` (`"rebase"` when a rebase is kept), `rebase` (`null`
//! unless a rebase is kept; then `{origin, seq, to, hidden, dropped,
//! discarded, d1_held, waiting_for}`, `to` the revision's digest) and
//! `superseded_rebases` (`[{origin, seq}]`). Other keys may be there. Before
//! any migration a missing `strategy`, `rebase` or `superseded_rebases` reads
//! as `null`, `null` and `[]`; [`status14`] spells the rest.
//!
//! The five new revisions are what Arachne `7dc783c` (`arachne describe`,
//! built in scratch from a git archive) writes for the five `.ecore` files
//! committed beside them, each `simpleuml.d1.ecore` (SI.12's, Arachne's own
//! `examples/SimpleUML.ecore`) with one change, but `D2_LWW`, which is
//! SI.12's `simpleuml.d2.ecore` with one change:
//!
//! - `D2_DROP`: `ModelElement.taggedValue` and class `TaggedValue` removed;
//! - `D2_NOASSOC`: class `Association` removed;
//! - `D2_FLAG`: `Class.abstract` annotated `dw-flag` (enable-wins to
//!   disable-wins);
//! - `D2_KIND`: `Packageable.owner` and `Package.ownedElements` no longer
//!   opposites, so `Packageable.owner` turns from a reference by text into a
//!   reference by identity;
//! - `D2_LWW`: `ModelElement.name` annotated `lww-register` instead of
//!   `fair-register`.
#![allow(dead_code)]

#[path = "../si12/mod.rs"]
pub mod si12;

#[path = "../nw02/launch.rs"]
pub mod launch;

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::ModelOp;
use moirai_interp::leaf::LeafOp;
use serde_json::{Map, Value, json};

use si12::{At, CAR, D1, D2, DOOR, HAS_DOORS, ID, SEDAN, STRING, Uml, expected_d1, expected_d2};

/// `ModelElement.taggedValue` and class `TaggedValue` removed.
pub const D2_DROP: &str = include_str!("../fixtures/si14/simpleuml.d2-drop.metamodel.json");
/// Class `Association` removed.
pub const D2_NOASSOC: &str = include_str!("../fixtures/si14/simpleuml.d2-noassoc.metamodel.json");
/// `Class.abstract` turned from an enable-wins into a disable-wins flag.
pub const D2_FLAG: &str = include_str!("../fixtures/si14/simpleuml.d2-flag.metamodel.json");
/// `Packageable.owner` turned from a reference by text into one by identity.
pub const D2_KIND: &str = include_str!("../fixtures/si14/simpleuml.d2-kind.metamodel.json");
/// SI.12's `D2` with `ModelElement.name` a last-writer-wins register.
pub const D2_LWW: &str = include_str!("../fixtures/si14/simpleuml.d2-lww.metamodel.json");

/// What `drops` names for `D2_DROP`: every entry copy's refusal would list.
pub const DROPS_TAGGED: [&str; 2] = ["ModelElement.taggedValue", "TaggedValue"];
/// What `drops` names for `D2_NOASSOC`.
pub const DROPS_ASSOC: [&str; 1] = ["Association"];

/// The model id every in-process test opens `m` under.
pub const MODEL: &str = "si14-m";

/// `drops` as the in-process action takes it.
pub fn drops(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// The revisions a model of these tests can be under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rev {
    D1,
    D2,
    Drop,
    Noassoc,
    Lww,
}

impl Rev {
    pub fn text(self) -> &'static str {
        match self {
            Rev::D1 => D1,
            Rev::D2 => D2,
            Rev::Drop => D2_DROP,
            Rev::Noassoc => D2_NOASSOC,
            Rev::Lww => D2_LWW,
        }
    }

    pub fn digest(self) -> String {
        si12::digest(self.text())
    }

    /// The running example as it reads under this revision, projected: SI.12's
    /// `expected_d1` mapped by hand.
    pub fn running_example(self) -> Value {
        match self {
            Rev::D1 => expected_d1(),
            Rev::D2 | Rev::Lww => expected_d2(),
            Rev::Drop => expected_drop(),
            Rev::Noassoc => expected_noassoc(),
        }
    }

    pub fn uml(self) -> Uml {
        Uml::new(self.text())
    }
}

// ---------------------------------------------------------------------------
// Documents, projected through labels as SI.12 projects them
// ---------------------------------------------------------------------------

/// `doc` projected through `base`, the labels of the running example's
/// objects as one replica served them before any migration, and every
/// identity `base` does not hold labelled `+<name>` (`+` alone for an object
/// with no name). An object that kept its identity reads as `#<label>`; one
/// served under a new identity reads as `#+<name>`, so a projection equal to
/// a document of `#<label>`s is also the check that no identity moved (M17).
pub fn project_keep(doc: &Value, base: &BTreeMap<String, String>) -> Value {
    si12::project_with(doc, &labels_keep(doc, base))
}

/// The labels [`project_keep`] projects `doc` through: `base`, and `+<name>`
/// for every identity `doc` serves that `base` does not hold. Also what a
/// default read (which carries no identities, only references to them) is
/// projected through, taken from the identified read beside it.
pub fn labels_keep(doc: &Value, base: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    fn walk(value: &Value, base: &BTreeMap<String, String>, out: &mut BTreeMap<String, String>) {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(id)) = map.get(ID)
                    && !base.contains_key(id)
                {
                    let name = map.get("name").and_then(Value::as_str).unwrap_or("");
                    out.insert(id.clone(), format!("+{name}"));
                }
                map.values().for_each(|item| walk(item, base, out));
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, base, out)),
            _ => {}
        }
    }
    let mut labels = base.clone();
    let mut fresh = BTreeMap::new();
    walk(doc, base, &mut fresh);
    labels.extend(fresh);
    labels
}

/// The object labelled `label` (`__id` `#<label>`) in a projected document.
pub fn object_mut<'a>(doc: &'a mut Value, label: &str) -> &'a mut Map<String, Value> {
    fn find<'a>(value: &'a mut Value, id: &str) -> Option<&'a mut Map<String, Value>> {
        match value {
            Value::Object(map) => {
                if map.get(ID).and_then(Value::as_str) == Some(id) {
                    return Some(map);
                }
                for item in map.values_mut() {
                    if let Some(found) = find(item, id) {
                        return Some(found);
                    }
                }
                None
            }
            Value::Array(items) => {
                for item in items.iter_mut() {
                    if let Some(found) = find(item, id) {
                        return Some(found);
                    }
                }
                None
            }
            _ => None,
        }
    }
    let id = format!("#{label}");
    let shown = doc.to_string();
    find(doc, &id).unwrap_or_else(|| panic!("oracle: no object `{id}` in {shown}"))
}

/// `doc` with `key` of the object labelled `label` set to `value`.
pub fn set(doc: &mut Value, label: &str, key: &str, value: Value) {
    object_mut(doc, label).insert(key.to_string(), value);
}

/// `doc` with the set `key` of the object labelled `label` holding `items`
/// too, read back sorted as the read-out sorts a set.
pub fn add_to_set(doc: &mut Value, label: &str, key: &str, items: &[&str]) {
    let object = object_mut(doc, label);
    let mut all: BTreeSet<String> = object
        .get(key)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    all.extend(items.iter().map(|item| item.to_string()));
    object.insert(key.to_string(), json!(all.into_iter().collect::<Vec<_>>()));
}

/// `doc` with the key `key` taken out of every object.
pub fn without_key(doc: &Value, key: &str) -> Value {
    match doc {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(k, _)| k.as_str() != key)
                .map(|(k, v)| (k.clone(), without_key(v, key)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(|v| without_key(v, key)).collect()),
        other => other.clone(),
    }
}

/// `doc` with the object labelled `label` taken out of the array holding it.
pub fn without_object(doc: &Value, label: &str) -> Value {
    let id = format!("#{label}");
    match doc {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), without_object(v, label)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .filter(|item| item.get(ID).and_then(Value::as_str) != Some(id.as_str()))
                .map(|item| without_object(item, label))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `D2_DROP`'s read of the running example: SI.12's `expected_d1` with no
/// `taggedValue` on any object (it was `ModelElement`'s), and so no
/// `TaggedValue` object (`wheels`) either. Nothing else changes: the
/// revision moves no slot a value is read by name through.
pub fn expected_drop() -> Value {
    without_key(&expected_d1(), "taggedValue")
}

/// `D2_NOASSOC`'s read of the running example: `expected_d1` without the
/// `Association` `hasDoors`, `ownedElements[4]`, the last entry, and with it
/// its two links.
pub fn expected_noassoc() -> Value {
    without_object(&expected_d1(), "hasDoors")
}

/// The class `c` puts at `ownedElements[5]` in RD's second schedule, as the
/// read-out serves an unwritten `Class` (as `Sedan` reads in `expected_d1`
/// but with no generalization), named as far as its name was typed, and
/// carrying the stereotypes given.
pub fn wheel(typed: &str, stereotypes: &[&str]) -> Value {
    let mut entry = json!({
        "__id": format!("#+{typed}"), "eClass": "Class",
        "attributes": [], "generalizations": [], "owner": null,
        "stereotype": stereotypes, "taggedValue": []
    });
    if !typed.is_empty() {
        entry["name"] = json!(typed);
    }
    entry
}

/// Append `entry` to the root's `ownedElements`.
pub fn push_owned(doc: &mut Value, entry: Value) {
    doc["ownedElements"]
        .as_array_mut()
        .expect("oracle: the root has ownedElements")
        .push(entry);
}

// ---------------------------------------------------------------------------
// The running example built directly under a revision (the premises' ground
// truth, as SI.12's F3 builds it under D2)
// ---------------------------------------------------------------------------

/// SI.12's `Uml::structure`, with the tagged value left out when the table
/// has no `taggedValue`, and the association left out when it has no
/// `Association`: the same operations in the same order otherwise.
pub fn structure_under(uml: &Uml, tagged: bool, assoc: bool) -> Vec<ModelOp> {
    let mut ops = vec![uml.root()];
    ops.extend(uml.name(At::Root, "Vehicle"));
    let mut owned: Vec<(At, &str)> = vec![
        (CAR, "Car"),
        (DOOR, "Door"),
        (SEDAN, "Sedan"),
        (STRING, "String"),
    ];
    if assoc {
        owned.push((HAS_DOORS, "hasDoors"));
    }
    for (at, name) in owned {
        let At::Owned(pos, class) = at else {
            unreachable!()
        };
        ops.push(uml.insert(At::Root, "ownedElements", pos, class));
        ops.extend(uml.name(at, name));
    }
    ops.push(uml.leaf(CAR, "abstract", LeafOp::Enable));
    ops.push(uml.leaf(
        CAR,
        "stereotype",
        LeafOp::Add(moirai_interp::leaf::Scalar::text("entity")),
    ));
    ops.push(uml.insert(CAR, "attributes", 0, "Property"));
    ops.extend(uml.name(si12::CAR_DOORS, "doors"));
    if tagged {
        ops.push(uml.insert(CAR, "taggedValue", 0, "TaggedValue"));
        ops.extend(uml.type_text(si12::CAR_WHEELS, "name", "wheels"));
        ops.extend(uml.type_text(si12::CAR_WHEELS, "value", "4"));
    }
    ops.push(uml.leaf(
        DOOR,
        "owner",
        LeafOp::Write(moirai_interp::leaf::Scalar::text("Vehicle")),
    ));
    ops.push(uml.insert(DOOR, "attributes", 0, "Property"));
    ops.extend(uml.name(si12::DOOR_COLOR, "color"));
    ops.push(uml.insert(SEDAN, "generalizations", 0, "Generalization"));
    ops.push(uml.leaf(si12::SEDAN_GEN, "isSubstitutable", LeafOp::Enable));
    ops
}

/// SI.12's `Uml::links`, without the association's two when the table has
/// no `Association`.
pub fn links_under(uml: &Uml, ids: &BTreeMap<String, String>, assoc: bool) -> Vec<ModelOp> {
    if assoc {
        return uml.links(ids);
    }
    let id = |label: &str| {
        ids.get(label)
            .unwrap_or_else(|| panic!("oracle: no served object labelled `{label}` in {ids:?}"))
            .clone()
    };
    vec![
        uml.leaf(si12::CAR_DOORS, "ttype", LeafOp::Link(id("Door"))),
        uml.leaf(si12::DOOR_COLOR, "ttype", LeafOp::Link(id("String"))),
        uml.leaf(si12::SEDAN_GEN, "general", LeafOp::Link(id("Car"))),
    ]
}

// ---------------------------------------------------------------------------
// The migration status
// ---------------------------------------------------------------------------

fn pairs(value: Option<&Value>) -> Vec<Value> {
    let mut out: Vec<(String, u64)> = value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    (
                        item.get("origin")
                            .and_then(Value::as_str)
                            .unwrap_or("<no origin>")
                            .to_string(),
                        item.get("seq").and_then(Value::as_u64).unwrap_or(u64::MAX),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out.into_iter()
        .map(|(origin, seq)| json!({"origin": origin, "seq": seq}))
        .collect()
}

fn strings(value: Option<&Value>) -> Value {
    match value.and_then(Value::as_array) {
        Some(items) => {
            let mut out: Vec<String> = items
                .iter()
                .map(|item| match item.as_str() {
                    Some(text) => text.to_string(),
                    None => item.to_string(),
                })
                .collect();
            out.sort();
            json!(out)
        }
        None => json!("<missing or not an array>"),
    }
}

/// The keys of a migration status SI.14 reads, normalized: lists sorted,
/// `refused` and the `{origin, seq}` lists as SI.12 spells them. Before any
/// migration (no successor named, no rebase) a missing `strategy`, `rebase`,
/// `superseded` or `superseded_rebases` reads as `null`, `null`, `[]` and
/// `[]`. Once a cut is kept the `strategy` key is not read (copy is SI.12's
/// and SI.13's, which name none) and reads `"<not read>"`. Inside a present
/// `rebase` object every key is read, a missing one as `"<no ... key>"`.
pub fn status14(status: &Value) -> Value {
    let migrated_to = status.get("migrated_to").cloned().unwrap_or(Value::Null);
    let rebase = match status.get("rebase") {
        None | Some(Value::Null) => Value::Null,
        Some(rebase) => {
            let key = |name: &str| {
                rebase
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| json!(format!("<no {name} key>")))
            };
            let mut dropped: Vec<(String, u64, String)> = rebase
                .get("dropped")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .map(|item| {
                            (
                                item.get("origin")
                                    .and_then(Value::as_str)
                                    .unwrap_or("<no origin>")
                                    .to_string(),
                                item.get("seq").and_then(Value::as_u64).unwrap_or(u64::MAX),
                                item.get("feature")
                                    .and_then(Value::as_str)
                                    .unwrap_or("<no feature>")
                                    .to_string(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            dropped.sort();
            json!({
                "origin": key("origin"),
                "seq": key("seq"),
                "to": key("to"),
                "hidden": strings(rebase.get("hidden")),
                "dropped": match rebase.get("dropped") {
                    Some(Value::Array(_)) => json!(dropped
                        .into_iter()
                        .map(|(origin, seq, feature)| json!({"origin": origin, "seq": seq, "feature": feature}))
                        .collect::<Vec<_>>()),
                    _ => json!("<missing or not an array>"),
                },
                "discarded": match rebase.get("discarded") {
                    Some(Value::Array(_)) => json!(pairs(rebase.get("discarded"))),
                    _ => json!("<missing or not an array>"),
                },
                "d1_held": key("d1_held"),
                "waiting_for": strings(rebase.get("waiting_for")),
            })
        }
    };
    let quiet = migrated_to.is_null() && rebase.is_null();
    let superseded = match status.get("superseded") {
        Some(list @ Value::Array(_)) => strings(Some(list)),
        None if quiet => json!([]),
        _ => json!("<missing or not an array>"),
    };
    let superseded_rebases = match status.get("superseded_rebases") {
        Some(list @ Value::Array(_)) => json!(pairs(Some(list))),
        None if quiet => json!([]),
        _ => json!("<missing or not an array>"),
    };
    let strategy = if migrated_to.is_string() {
        json!("<not read>")
    } else {
        status.get("strategy").cloned().unwrap_or(Value::Null)
    };
    json!({
        "migrated_to": migrated_to,
        "refused": pairs(status.get("refused")),
        "superseded": superseded,
        "strategy": strategy,
        "superseded_rebases": superseded_rebases,
        "rebase": rebase,
    })
}

/// What [`status14`] reads before any migration.
pub fn quiet() -> Value {
    json!({
        "migrated_to": null, "refused": [], "superseded": [], "strategy": null,
        "superseded_rebases": [], "rebase": null
    })
}

/// One `{origin, seq}` of a status list.
pub fn ev(origin: &str, seq: u64) -> (String, u64) {
    (origin.to_string(), seq)
}

fn sorted_pairs(items: &[(String, u64)]) -> Vec<Value> {
    let mut items = items.to_vec();
    items.sort();
    items
        .into_iter()
        .map(|(origin, seq)| json!({"origin": origin, "seq": seq}))
        .collect()
}

/// What [`status14`] reads when a rebase is kept.
#[derive(Clone, Debug, Default)]
pub struct RebaseStatus {
    pub origin: String,
    pub seq: u64,
    pub to: String,
    pub hidden: Vec<String>,
    pub dropped: Vec<(String, u64, String)>,
    pub discarded: Vec<(String, u64)>,
    pub waiting_for: Vec<String>,
    pub superseded: Vec<String>,
    pub superseded_rebases: Vec<(String, u64)>,
}

impl RebaseStatus {
    pub fn render(&self) -> Value {
        let mut hidden = self.hidden.clone();
        hidden.sort();
        let mut dropped = self.dropped.clone();
        dropped.sort();
        let mut waiting = self.waiting_for.clone();
        waiting.sort();
        let mut superseded = self.superseded.clone();
        superseded.sort();
        json!({
            "migrated_to": null, "refused": [], "superseded": superseded,
            "strategy": "rebase",
            "superseded_rebases": sorted_pairs(&self.superseded_rebases),
            "rebase": {
                "origin": self.origin, "seq": self.seq, "to": self.to,
                "hidden": hidden,
                "dropped": dropped
                    .into_iter()
                    .map(|(origin, seq, feature)| json!({"origin": origin, "seq": seq, "feature": feature}))
                    .collect::<Vec<_>>(),
                "discarded": sorted_pairs(&self.discarded),
                "d1_held": !self.waiting_for.is_empty(),
                "waiting_for": waiting,
            }
        })
    }
}

/// What [`status14`] reads when a cut is kept (SI.12, SI.13).
pub fn cut_status(
    to: &str,
    refused: &[(String, u64)],
    superseded: &[String],
    superseded_rebases: &[(String, u64)],
) -> Value {
    let mut superseded = superseded.to_vec();
    superseded.sort();
    json!({
        "migrated_to": to, "refused": sorted_pairs(refused), "superseded": superseded,
        "strategy": "<not read>",
        "superseded_rebases": sorted_pairs(superseded_rebases),
        "rebase": null,
    })
}

/// The digest a listing entry names, given as a string or as `{digest}`.
pub fn listed_digest(value: &Value) -> Option<String> {
    match value {
        Value::String(digest) => Some(digest.clone()),
        Value::Object(map) => map
            .get("digest")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}
