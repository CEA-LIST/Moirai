//! SI.12 oracle, the part its test files share: the four SimpleUML
//! descriptors, the model every test builds, the operations a writer sends on
//! it, and the projection a served document is compared through.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.12.md`. The contract the
//! tests read is its section 3, restated here so that a reader of the tests
//! does not need the spec open:
//!
//! - M1, the action. In process, `moirai_interp::migrate::migrate(source:
//!   &ModelLog, model_id: &str, descriptor: &str)` answers `Ok(Migration)`
//!   or `Err(why)` with `why: Display`. A `Migration` has two public fields:
//!   `cut`, the one operation the migrating replica sends on the source log,
//!   and `ops`, the operations that open and fill the new log, the first of
//!   them `ModelOp::Install { model_id, metamodel_id: <digest of descriptor>,
//!   descriptor }` and no other `Install` among them. Over HTTP,
//!   `POST /api/model/{m}/migrate` with `{"metamodel_id": "<digest>"}`, the
//!   digest of a descriptor the node serves, answers 2xx with the new
//!   model's id under `model_id`, or 409 or 422 with the reason under
//!   `error`.
//! - M2, the cut. Once a replica has delivered the cut,
//!   `ModelLog::migration()` (a `serde_json::Value`) and
//!   `GET /api/model/{m}/migration` answer
//!   `{"migrated_to": "<m'>", "refused": [{"origin": "<replica>", "seq": <n>}, ...]}`;
//!   before, `{"migrated_to": null, "refused": []}`. Other keys may be there.
//! - M3, read-only. A replica that has delivered the cut refuses every local
//!   operation on `m` at its intake, with a reason naming `m'`, and a second
//!   migration of `m` too.
//! - M4, frozen. A replica that has delivered the cut serves, in the
//!   identified and in the default read, the document the migrating replica
//!   served when it cut, whatever else it has delivered or delivers later.
//! - M5, reported. `refused` lists every event of `m` the replica has
//!   delivered that the cut had not seen, by its origin and its sequence
//!   number, in any order, and nothing else.
//! - M6, the copy. The new model, read on any replica that delivered `ops`,
//!   is the cut's document mapped to the revised descriptor: shared features
//!   keep their values (an identity reference names the copy of its target),
//!   added features and classes read their defaults, a feature whose rule
//!   changed holds one value equal to what the cut's document read.
//! - M7, refused revisions. A revision that renames or removes a feature of
//!   the source descriptor is refused, naming the feature as
//!   `<Class>.<feature>`, and nothing is written.
//!
//! The descriptors are what Arachne `7dc783c` (`arachne describe`) writes for
//! `simpleuml.d1.ecore` (Arachne's own `examples/SimpleUML.ecore`, byte for
//! byte), `simpleuml.d2.ecore` (`ModelElement.name` annotated
//! `fair-register`, `Class.isLeaf : EBoolean [1..1]` and
//! `ModelElement.ownedComments : Comment [0..*]` added, class `Comment`
//! added), `simpleuml.d2-rename.ecore` (`Class.abstract` renamed
//! `isAbstract`) and `simpleuml.d2-remove.ecore` (`EnumerationLiteral.name`
//! removed).
//!
//! The model is the paper's running example: model `Vehicle` holding classes
//! `Car` and `Door`, and around them what exercises every kind of feature the
//! claim names:
//!
//! ```text
//! Model "Vehicle"
//!   ownedElements[0] Class "Car"    abstract, stereotype {entity},
//!                                   attributes[0] Property "doors" ttype -> Door,
//!                                   taggedValue[0] TaggedValue name "wheels" value "4"
//!   ownedElements[1] Class "Door"   owner "Vehicle" (a reference by text),
//!                                   attributes[0] Property "color" ttype -> String
//!   ownedElements[2] Class "Sedan"  generalizations[0] Generalization
//!                                   general -> Car, isSubstitutable
//!   ownedElements[3] PrimitiveType "String"
//!   ownedElements[4] Association "hasDoors" source -> Car, target -> Door
//! ```
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{class_slot, feature_slot, table};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{ClassSlot, FeatureSlot, LeafRule, MergeRule, MetamodelSemantics, Shape};
use serde_json::{Map, Value, json};

/// The source descriptor: SimpleUML as Arachne `7dc783c` describes it.
pub const D1: &str = include_str!("../fixtures/si12/simpleuml.d1.metamodel.json");
/// The revision: `ModelElement.name` a fair register, `Class.isLeaf`,
/// `ModelElement.ownedComments` and class `Comment` added.
pub const D2: &str = include_str!("../fixtures/si12/simpleuml.d2.metamodel.json");
/// A revision that renames `Class.abstract` to `Class.isAbstract`.
pub const D2_RENAME: &str = include_str!("../fixtures/si12/simpleuml.d2-rename.metamodel.json");
/// A revision that removes `EnumerationLiteral.name`.
pub const D2_REMOVE: &str = include_str!("../fixtures/si12/simpleuml.d2-remove.metamodel.json");

/// The served-identity key.
pub const ID: &str = "__id";

pub fn parsed(text: &str) -> Value {
    serde_json::from_str(text).expect("an SI.12 fixture is JSON")
}

/// The digest a node keys a descriptor by.
pub fn digest(text: &str) -> String {
    moirai_semantics::metamodel_digest(&parsed(text))
}

/// The opening operation of a model on `text`, as a node writes it.
pub fn install(model_id: &str, text: &str) -> ModelOp {
    ModelOp::Install {
        model_id: model_id.to_string(),
        metamodel_id: digest(text),
        descriptor: text.to_string(),
    }
}

/// Where an object of the running example sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
    /// The model root, a `Model`.
    Root,
    /// `ownedElements[i]`, of the class named.
    Owned(usize, &'static str),
    /// `ownedElements[i].attributes[j]`, a `Property`; the owner a `Class`.
    Attr(usize, usize),
    /// `ownedElements[i].generalizations[j]`, a `Generalization`.
    Gen(usize, usize),
    /// `ownedElements[i].taggedValue[j]`, a `TaggedValue`.
    Tag(usize, usize),
}

impl At {
    pub fn class(self) -> &'static str {
        match self {
            At::Root => "Model",
            At::Owned(_, class) => class,
            At::Attr(..) => "Property",
            At::Gen(..) => "Generalization",
            At::Tag(..) => "TaggedValue",
        }
    }
}

pub const CAR: At = At::Owned(0, "Class");
pub const DOOR: At = At::Owned(1, "Class");
pub const SEDAN: At = At::Owned(2, "Class");
pub const STRING: At = At::Owned(3, "PrimitiveType");
pub const HAS_DOORS: At = At::Owned(4, "Association");
pub const CAR_DOORS: At = At::Attr(0, 0);
pub const DOOR_COLOR: At = At::Attr(1, 0);
pub const SEDAN_GEN: At = At::Gen(2, 0);
pub const CAR_WHEELS: At = At::Tag(0, 0);

/// The running example's operations under one SimpleUML table, every slot
/// read from that table by name.
pub struct Uml {
    pub sem: Arc<MetamodelSemantics>,
}

impl Uml {
    pub fn new(text: &str) -> Self {
        Uml {
            sem: table(&parsed(text)),
        }
    }

    pub fn class(&self, name: &str) -> ClassSlot {
        class_slot(&self.sem, name)
    }

    /// The visible slot of `feature` on `class`.
    pub fn slot(&self, class: &str, feature: &str) -> FeatureSlot {
        feature_slot(&self.sem, self.class(class), feature)
    }

    /// The rule of `feature` as `class` sees it.
    pub fn rule(&self, class: &str, feature: &str) -> MergeRule {
        let holder = &self.sem.classes[self.class(class).index()];
        let slot = self.slot(class, feature);
        let (_, declaring, declared) = &holder.visible[slot.index()];
        *self
            .sem
            .rule(*declaring, *declared)
            .unwrap_or_else(|| panic!("no rule for `{class}.{feature}`"))
    }

    /// `inner` addressed to the object at `at`.
    pub fn on(&self, at: At, inner: InstanceOp) -> ModelOp {
        let owned = |i: usize, class: &str, inner: InstanceOp| {
            InstanceOp::field(
                self.slot("Model", "ownedElements"),
                InstanceOp::at(i, InstanceOp::variant(self.class(class), inner)),
            )
        };
        let nested = |i: usize, feature: &str, j: usize, class: &str, inner: InstanceOp| {
            owned(
                i,
                "Class",
                InstanceOp::field(
                    self.slot("Class", feature),
                    InstanceOp::at(j, InstanceOp::variant(self.class(class), inner)),
                ),
            )
        };
        let path = match at {
            At::Root => inner,
            At::Owned(i, class) => owned(i, class, inner),
            At::Attr(i, j) => nested(i, "attributes", j, "Property", inner),
            At::Gen(i, j) => nested(i, "generalizations", j, "Generalization", inner),
            At::Tag(i, j) => nested(i, "taggedValue", j, "TaggedValue", inner),
        };
        ModelOp::Instance(InstanceOp::variant(self.class("Model"), path))
    }

    /// Mint the root `Model`.
    pub fn root(&self) -> ModelOp {
        ModelOp::Instance(InstanceOp::variant(self.class("Model"), InstanceOp::New))
    }

    /// Insert a new `class` at `feature[pos]` of the object at `at`.
    pub fn insert(&self, at: At, feature: &str, pos: usize, class: &str) -> ModelOp {
        self.on(
            at,
            InstanceOp::field(
                self.slot(at.class(), feature),
                InstanceOp::insert(pos, InstanceOp::variant(self.class(class), InstanceOp::New)),
            ),
        )
    }

    /// Take `ownedElements[pos]` out.
    pub fn delete_owned(&self, pos: usize) -> ModelOp {
        self.on(
            At::Root,
            InstanceOp::field(self.slot("Model", "ownedElements"), InstanceOp::delete(pos)),
        )
    }

    /// `leaf` at `feature` of the object at `at`, inside the optional when the
    /// feature is one.
    pub fn leaf(&self, at: At, feature: &str, leaf: LeafOp) -> ModelOp {
        let inner = match self.rule(at.class(), feature) {
            MergeRule::Attribute {
                shape: Shape::Optional,
                ..
            } => InstanceOp::set(InstanceOp::Leaf(leaf)),
            _ => InstanceOp::Leaf(leaf),
        };
        self.on(at, InstanceOp::field(self.slot(at.class(), feature), inner))
    }

    /// `text` typed into the empty text attribute `feature` at `at`.
    pub fn type_text(&self, at: At, feature: &str, text: &str) -> Vec<ModelOp> {
        assert!(
            matches!(
                self.rule(at.class(), feature),
                MergeRule::Attribute {
                    leaf: LeafRule::Text,
                    ..
                }
            ),
            "`{}.{feature}` is not a text attribute in this table",
            at.class()
        );
        text.chars()
            .enumerate()
            .map(|(pos, ch)| self.leaf(at, feature, LeafOp::InsertChar { pos, ch }))
            .collect()
    }

    /// Give the object at `at` its name, as this table's rule for `name`
    /// writes one: characters into a text, one write into a register.
    pub fn name(&self, at: At, name: &str) -> Vec<ModelOp> {
        match self.rule(at.class(), "name") {
            MergeRule::Attribute {
                leaf: LeafRule::Text,
                ..
            } => self.type_text(at, "name", name),
            MergeRule::Attribute {
                leaf: LeafRule::Register { .. },
                ..
            } => vec![self.leaf(at, "name", LeafOp::Write(Scalar::text(name)))],
            other => panic!("`{}.name` has the rule {other:?}", at.class()),
        }
    }

    /// One character into the name text of the object at `at`.
    pub fn name_char(&self, at: At, pos: usize, ch: char) -> ModelOp {
        self.leaf(at, "name", LeafOp::InsertChar { pos, ch })
    }

    /// The structure and every attribute of the running example, from an
    /// empty model: everything but the identity references.
    pub fn structure(&self) -> Vec<ModelOp> {
        let mut ops = vec![self.root()];
        ops.extend(self.name(At::Root, "Vehicle"));
        for (at, name) in [
            (CAR, "Car"),
            (DOOR, "Door"),
            (SEDAN, "Sedan"),
            (STRING, "String"),
            (HAS_DOORS, "hasDoors"),
        ] {
            let At::Owned(pos, class) = at else {
                unreachable!()
            };
            ops.push(self.insert(At::Root, "ownedElements", pos, class));
            ops.extend(self.name(at, name));
        }
        ops.push(self.leaf(CAR, "abstract", LeafOp::Enable));
        ops.push(self.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("entity"))));
        ops.push(self.insert(CAR, "attributes", 0, "Property"));
        ops.extend(self.name(CAR_DOORS, "doors"));
        ops.push(self.insert(CAR, "taggedValue", 0, "TaggedValue"));
        ops.extend(self.type_text(CAR_WHEELS, "name", "wheels"));
        ops.extend(self.type_text(CAR_WHEELS, "value", "4"));
        ops.push(self.leaf(DOOR, "owner", LeafOp::Write(Scalar::text("Vehicle"))));
        ops.push(self.insert(DOOR, "attributes", 0, "Property"));
        ops.extend(self.name(DOOR_COLOR, "color"));
        ops.push(self.insert(SEDAN, "generalizations", 0, "Generalization"));
        ops.push(self.leaf(SEDAN_GEN, "isSubstitutable", LeafOp::Enable));
        ops
    }

    /// The five identity references, to the identities `ids` names by label.
    pub fn links(&self, ids: &BTreeMap<String, String>) -> Vec<ModelOp> {
        let id = |label: &str| {
            ids.get(label)
                .unwrap_or_else(|| panic!("no served object labelled `{label}` in {ids:?}"))
                .clone()
        };
        vec![
            self.leaf(CAR_DOORS, "ttype", LeafOp::Link(id("Door"))),
            self.leaf(DOOR_COLOR, "ttype", LeafOp::Link(id("String"))),
            self.leaf(SEDAN_GEN, "general", LeafOp::Link(id("Car"))),
            self.leaf(HAS_DOORS, "source", LeafOp::Link(id("Car"))),
            self.leaf(HAS_DOORS, "target", LeafOp::Link(id("Door"))),
        ]
    }

    /// `b`'s two edits concurrent with the cut: `Door` renamed `Doors`, and
    /// `Door` made abstract.
    pub fn b_edits(&self) -> Vec<ModelOp> {
        vec![
            self.name_char(DOOR, 4, 's'),
            self.leaf(DOOR, "abstract", LeafOp::Enable),
        ]
    }

    /// `c`'s edit concurrent with the cut: stereotype `persistent` on `Car`.
    pub fn c_edit(&self) -> ModelOp {
        self.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent")))
    }

    /// Edits of every kind a writer of `m` may send, each accepted by a
    /// replica that has not delivered the cut and refused by one that has:
    /// a character, a set element, a flag, a link, a creation, a deletion, a
    /// reference by text.
    pub fn local_edits(&self, ids: &BTreeMap<String, String>) -> Vec<(&'static str, ModelOp)> {
        vec![
            ("a character into Car's name", self.name_char(CAR, 3, 's')),
            (
                "a stereotype on Sedan",
                self.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("final"))),
            ),
            (
                "Sedan made abstract",
                self.leaf(SEDAN, "abstract", LeafOp::Enable),
            ),
            (
                "a link from Door.color's type to Car",
                self.leaf(
                    DOOR_COLOR,
                    "ttype",
                    LeafOp::Link(ids.get("Car").expect("Car is served").clone()),
                ),
            ),
            (
                "a new class at ownedElements[5]",
                self.insert(At::Root, "ownedElements", 5, "Class"),
            ),
            ("hasDoors deleted", self.delete_owned(4)),
            (
                "Door's owner written",
                self.leaf(DOOR, "owner", LeafOp::Write(Scalar::text("Garage"))),
            ),
        ]
    }

    /// A rename of `Door` to `name` in the revised model, by `D2`'s rule.
    pub fn rename_door(&self, name: &str) -> ModelOp {
        assert!(
            matches!(
                self.rule("Class", "name"),
                MergeRule::Attribute {
                    leaf: LeafRule::Register { .. },
                    ..
                }
            ),
            "premise: rename_door writes a register"
        );
        self.leaf(DOOR, "name", LeafOp::Write(Scalar::text(name)))
    }
}

// ---------------------------------------------------------------------------
// The projection
// ---------------------------------------------------------------------------

/// Every served object of an identified document, by its identity: its
/// `name` when it has one as a string, else `<owner label>.<feature>[<i>]`.
pub fn labels(doc: &Value) -> BTreeMap<String, String> {
    fn walk(value: &Value, owner: &str, key: &str, out: &mut BTreeMap<String, String>) {
        match value {
            Value::Object(map) => {
                let mut here = owner.to_string();
                if let Some(Value::String(id)) = map.get(ID) {
                    here = match map.get("name") {
                        Some(Value::String(name)) => name.clone(),
                        _ => format!("{owner}.{key}"),
                    };
                    if let Some(previous) = out.insert(id.clone(), here.clone()) {
                        panic!("the identity {id} is served twice ({previous}, {here})");
                    }
                }
                for (name, item) in map {
                    walk(item, &here, name, out);
                }
            }
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    walk(item, owner, &format!("{key}[{index}]"), out);
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeMap::new();
    walk(doc, "", "", &mut out);
    let distinct: BTreeSet<&String> = out.values().collect();
    assert_eq!(
        distinct.len(),
        out.len(),
        "two served objects carry one label: {out:?}"
    );
    out
}

/// The label of every served object, by label: what a writer needs to link.
pub fn ids_by_label(doc: &Value) -> BTreeMap<String, String> {
    labels(doc)
        .into_iter()
        .map(|(id, label)| (label, id))
        .collect()
}

/// `doc` with every identity replaced by the label of the object it names in
/// `labels`: under `__id` as `#<label>`, as a reference value as
/// `@<label>`. A string that is no served identity is kept as it is.
pub fn project_with(doc: &Value, labels: &BTreeMap<String, String>) -> Value {
    match doc {
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, value) in map {
                let value = match (key.as_str(), value) {
                    (ID, Value::String(id)) => Value::String(format!(
                        "#{}",
                        labels.get(id).cloned().unwrap_or_else(|| id.clone())
                    )),
                    _ => project_with(value, labels),
                };
                out.insert(key.clone(), value);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| project_with(item, labels))
                .collect(),
        ),
        Value::String(text) => match labels.get(text) {
            Some(label) => Value::String(format!("@{label}")),
            None => doc.clone(),
        },
        other => other.clone(),
    }
}

/// An identified document, projected through its own labels.
pub fn project(doc: &Value) -> Value {
    project_with(doc, &labels(doc))
}

/// `doc` with every `__id` key taken out: what the default read is, by
/// SI.01's contract R', for a metamodel with no transparent class.
pub fn strip_ids(doc: &Value) -> Value {
    match doc {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(key, _)| key.as_str() != ID)
                .map(|(key, value)| (key.clone(), strip_ids(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(strip_ids).collect()),
        other => other.clone(),
    }
}

/// The running example under `D1`, projected: computed by hand from the
/// operations of [`Uml::structure`] and [`Uml::links`] and the read-out's
/// rules (an object's visible features, an unset optional absent, an empty
/// sequence or set `[]`, a register nobody wrote `null` (`Packageable.owner`
/// and `Property.owner`, references by text), an identity reference read as
/// its target's identity).
pub fn expected_d1() -> Value {
    json!({
        "__id": "#Vehicle", "eClass": "Model", "name": "Vehicle",
        "ownedElements": [
            {
                "__id": "#Car", "eClass": "Class", "name": "Car",
                "abstract": true,
                "attributes": [
                    {"__id": "#doors", "eClass": "Property", "name": "doors", "owner": null,
                     "stereotype": [], "taggedValue": [], "ttype": "@Door"}
                ],
                "generalizations": [],
                "owner": null,
                "stereotype": ["entity"],
                "taggedValue": [
                    {"__id": "#wheels", "eClass": "TaggedValue", "name": "wheels", "value": "4"}
                ]
            },
            {
                "__id": "#Door", "eClass": "Class", "name": "Door",
                "attributes": [
                    {"__id": "#color", "eClass": "Property", "name": "color", "owner": null,
                     "stereotype": [], "taggedValue": [], "ttype": "@String"}
                ],
                "generalizations": [],
                "owner": "Vehicle",
                "stereotype": [],
                "taggedValue": []
            },
            {
                "__id": "#Sedan", "eClass": "Class", "name": "Sedan",
                "attributes": [],
                "generalizations": [
                    {"__id": "#Sedan.generalizations[0]", "eClass": "Generalization",
                     "general": "@Car", "isSubstitutable": true}
                ],
                "owner": null,
                "stereotype": [],
                "taggedValue": []
            },
            {
                "__id": "#String", "eClass": "PrimitiveType", "name": "String",
                "owner": null, "stereotype": [], "taggedValue": []
            },
            {
                "__id": "#hasDoors", "eClass": "Association", "name": "hasDoors",
                "owner": null, "source": "@Car", "stereotype": [], "taggedValue": [],
                "target": "@Door"
            }
        ],
        "owner": null,
        "stereotype": [],
        "taggedValue": []
    })
}

/// The running example under `D2`, projected: [`expected_d1`] with the
/// revision's additions at their defaults, and nothing else changed. Every
/// `ModelElement` (the model, the three classes, the primitive type, the
/// association, the two properties) gains `ownedComments: []`; every
/// `Class` gains `isLeaf: false`, a single flag nobody wrote. The names read
/// the same strings, now held by a register. `Generalization` and
/// `TaggedValue` are no `ModelElement` and gain nothing.
pub fn expected_d2() -> Value {
    let mut doc = expected_d1();
    fn add(value: &mut Value) {
        if let Value::Object(map) = value {
            let class = map
                .get("eClass")
                .and_then(Value::as_str)
                .map(str::to_string);
            if let Some(class) = class {
                if matches!(
                    class.as_str(),
                    "Model" | "Class" | "PrimitiveType" | "Association" | "Property"
                ) {
                    map.insert("ownedComments".to_string(), json!([]));
                }
                if class == "Class" {
                    map.insert("isLeaf".to_string(), json!(false));
                }
            }
            for item in map.values_mut() {
                add(item);
            }
        } else if let Value::Array(items) = value {
            items.iter_mut().for_each(add);
        }
    }
    add(&mut doc);
    doc
}

/// What the migration status reads before any cut.
pub fn not_migrated() -> Value {
    json!({"migrated_to": null, "refused": []})
}

/// The two keys of a migration status the contract fixes, `refused` sorted
/// by origin and sequence number so that two equal sets compare equal.
pub fn status_keys(status: &Value) -> Value {
    let mut refused: Vec<(String, u64)> = status
        .get("refused")
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
    refused.sort();
    json!({
        "migrated_to": status.get("migrated_to").cloned().unwrap_or(Value::String("<no migrated_to key>".into())),
        "refused": refused
            .into_iter()
            .map(|(origin, seq)| json!({"origin": origin, "seq": seq}))
            .collect::<Vec<_>>(),
    })
}

/// The status a replica that delivered the cut to `to`, and the edits
/// `refused` (origin, sequence number), must read.
pub fn migrated(to: &str, refused: &[(&str, u64)]) -> Value {
    let mut refused: Vec<(String, u64)> = refused
        .iter()
        .map(|(origin, seq)| (origin.to_string(), *seq))
        .collect();
    refused.sort();
    json!({
        "migrated_to": to,
        "refused": refused
            .into_iter()
            .map(|(origin, seq)| json!({"origin": origin, "seq": seq}))
            .collect::<Vec<_>>(),
    })
}

/// The name of `ownedElements[1]` (`Door`) in a projected or raw document.
pub fn door_name(doc: &Value) -> Value {
    doc["ownedElements"][1]["name"].clone()
}

/// Every ordering of streams of the given lengths: each ordering lists, step
/// by step, which stream's next message is delivered.
pub fn interleavings(lengths: &[usize]) -> Vec<Vec<usize>> {
    fn go(left: &mut Vec<usize>, prefix: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if left.iter().all(|n| *n == 0) {
            out.push(prefix.clone());
            return;
        }
        for stream in 0..left.len() {
            if left[stream] > 0 {
                left[stream] -= 1;
                prefix.push(stream);
                go(left, prefix, out);
                prefix.pop();
                left[stream] += 1;
            }
        }
    }
    let mut out = Vec::new();
    go(&mut lengths.to_vec(), &mut Vec::new(), &mut out);
    out
}
