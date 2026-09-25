//! SI.01 oracle, the part both of its test files share: how a served
//! document is read, and how operations are built from a metamodel's table.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`. What the tests read
//! is contract R' there, the identified read, which stands beside the default
//! state read and leaves it as it is:
//!
//! - `GET /api/model/{id}/state` and `QueryableLog::query_state_json` are the
//!   default read and serve exactly what they served before SI.01;
//! - `GET /api/model/{id}/state/identified` and
//!   `QueryableLog::query_identified_state_json` are the identified read: the
//!   same document, in which every element (every instance of a class the
//!   model holds) is a JSON record carrying `eClass` and, under the reserved
//!   key `__id`, a non-empty string, its store identity;
//! - in the identified read a non-transparent element is its canonical record
//!   with `__id` added, and a transparent element (`json.ecore`'s `Object`,
//!   `Array`, `String`, `Number`, `Boolean`) is the record
//!   `{"eClass": <class>, <its one feature>: <value>, "__id": <identity>}`,
//!   which is the explicit form `model-editor/src/crdt/explicit.ts` builds;
//! - the default read is the identified read with every `__id` removed and
//!   every transparent record given back as the bare value of its one feature
//!   ([`strip`]).
#![allow(dead_code, unused_macros)]

use std::collections::BTreeSet;

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{
    JSON_DESCRIPTOR, bench_descriptor, class_slot, feature_slot, install,
};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::{MetamodelSemantics, from_descriptor};
use serde_json::{Map, Value};

/// The reserved key an element's store identity is served under.
pub const ID: &str = "__id";

pub const UML: &str = include_str!("../fixtures/uml.metamodel.json");

/// `json.ecore`'s transparent classes and the one feature each is served by.
pub const JSON_TRANSPARENT: &[(&str, &str)] = &[
    ("Object", "entry"),
    ("Array", "items"),
    ("String", "value"),
    ("Number", "value"),
    ("Boolean", "value"),
];

/// A metamodel with no transparent class.
pub const OPAQUE: &[(&str, &str)] = &[];

// ---------------------------------------------------------------------------
// Reading the served form
// ---------------------------------------------------------------------------

/// Every element of a served document, depth first in document order: every
/// JSON object that carries `eClass`.
pub fn elements(doc: &Value) -> Vec<&Map<String, Value>> {
    fn walk<'a>(value: &'a Value, out: &mut Vec<&'a Map<String, Value>>) {
        match value {
            Value::Object(fields) => {
                if fields.get("eClass").and_then(Value::as_str).is_some() {
                    out.push(fields);
                }
                for (key, held) in fields {
                    if key != ID {
                        walk(held, out);
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(doc, &mut out);
    out
}

/// The identities of an identified document, held to the claim: exactly
/// `count` elements, each served with a non-empty string under `__id`, no two
/// alike.
pub fn identified(doc: &Value, count: usize, who: &str) -> Vec<String> {
    let found = elements(doc);
    assert_eq!(
        found.len(),
        count,
        "{who}: the identified read holds {} element records where {count} elements were \
         created; contract R' serves every element, transparent ones included, as a record \
         carrying `eClass` and `{ID}`. Served: {doc}",
        found.len()
    );
    let mut ids = Vec::new();
    for element in found {
        let class = element.get("eClass").and_then(Value::as_str).unwrap_or("?");
        match element.get(ID).and_then(Value::as_str) {
            Some(id) if !id.is_empty() => ids.push(id.to_string()),
            other => panic!(
                "{who}: a `{class}` is served with no store identity under `{ID}` (found \
                 {other:?}): {}",
                Value::Object(element.clone())
            ),
        }
    }
    let distinct: BTreeSet<&String> = ids.iter().collect();
    assert_eq!(
        distinct.len(),
        ids.len(),
        "{who}: two elements of one model share a store identity: {ids:?}"
    );
    ids
}

/// The store identity one served element carries.
pub fn id_of(element: &Value) -> String {
    element
        .get(ID)
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no `{ID}` on {element}"))
}

/// `Model.ownedElements`, as served.
pub fn owned(doc: &Value) -> &Vec<Value> {
    doc["ownedElements"]
        .as_array()
        .unwrap_or_else(|| panic!("no ownedElements array in {doc}"))
}

/// A UML element's name; an unwritten optional name is an absent key.
pub fn name_of(element: &Value) -> &str {
    element.get("name").and_then(Value::as_str).unwrap_or("")
}

/// Where the element with this identity sits in `Model.ownedElements`.
pub fn pos_of(doc: &Value, id: &str) -> usize {
    owned(doc)
        .iter()
        .position(|element| element.get(ID).and_then(Value::as_str) == Some(id))
        .unwrap_or_else(|| panic!("no element {id} in {doc}"))
}

/// The one owned element carrying this name.
pub fn named<'a>(doc: &'a Value, name: &str) -> &'a Value {
    let hits: Vec<&Value> = owned(doc)
        .iter()
        .filter(|element| name_of(element) == name)
        .collect();
    assert_eq!(hits.len(), 1, "one element named {name:?} in {doc}");
    hits[0]
}

/// A member of an identified JSON object, which contract R' serves as
/// `{"eClass": "Object", "entry": {<key>: <member>}, "__id": ...}`.
pub fn member<'a>(object: &'a Value, key: &str) -> &'a Value {
    let found = &object["entry"][key];
    assert!(!found.is_null(), "no member {key:?} in {object}");
    found
}

/// The items of an identified JSON array, `{"eClass": "Array", "items": [...]}`.
pub fn items(array: &Value) -> &Vec<Value> {
    array["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items array in {array}"))
}

/// Every identity anywhere in a served document.
pub fn all_ids(doc: &Value) -> BTreeSet<String> {
    elements(doc)
        .into_iter()
        .filter_map(|element| element.get(ID).and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

/// What the default read must be, given the identified read: every element's
/// `__id` removed, and every transparent element (a class of `transparent`)
/// given back as the bare value of its one feature. A key named `__id` that is
/// not on an element (a member of a JSON object, say) is data and is kept.
pub fn strip(doc: &Value, transparent: &[(&str, &str)]) -> Value {
    match doc {
        Value::Array(held) => {
            Value::Array(held.iter().map(|item| strip(item, transparent)).collect())
        }
        Value::Object(fields) => {
            let class = fields.get("eClass").and_then(Value::as_str);
            match class.and_then(|class| transparent.iter().find(|(name, _)| *name == class)) {
                Some((class, feature)) => {
                    let others: Vec<&String> = fields
                        .keys()
                        .filter(|key| !matches!(key.as_str(), "eClass" | ID) && *key != feature)
                        .collect();
                    assert!(
                        others.is_empty(),
                        "a transparent `{class}` is served with `{feature}` alone beside \
                         `eClass` and `{ID}`, and this one has {others:?}: {doc}"
                    );
                    let value = fields.get(*feature).unwrap_or_else(|| {
                        panic!("a transparent `{class}` is served with no `{feature}`: {doc}")
                    });
                    strip(value, transparent)
                }
                None => Value::Object(
                    fields
                        .iter()
                        .filter(|(key, _)| class.is_none() || key.as_str() != ID)
                        .map(|(key, value)| (key.clone(), strip(value, transparent)))
                        .collect(),
                ),
            }
        }
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// Building operations from a table
// ---------------------------------------------------------------------------

/// One metamodel's table, and the two path steps every operation is built of.
pub struct Lang {
    pub sem: MetamodelSemantics,
}

impl Lang {
    pub fn parse(descriptor: &Value) -> Self {
        Lang {
            sem: from_descriptor(descriptor).expect("the fixture is a descriptor this crate reads"),
        }
    }

    /// `Variant(class, inner)`.
    pub fn is(&self, class: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::variant(class_slot(&self.sem, class), inner)
    }

    /// `Field(feature, inner)`, the feature named on the class that sees it.
    pub fn at(&self, class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::field(
            feature_slot(&self.sem, class_slot(&self.sem, class), feature),
            inner,
        )
    }
}

pub fn chr(pos: usize, ch: char) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })
}

pub fn inc_int(by: i64) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(by)))
}

pub fn inc_float(by: f64) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Inc(Scalar::float(by)))
}

/// SimpleUML: a `Model` at the root holding classes in `ownedElements`.
pub struct Uml(pub Lang);

impl Uml {
    pub fn new() -> Self {
        Uml(Lang::parse(
            &serde_json::from_str(UML).expect("the fixture is JSON"),
        ))
    }

    /// An operation on `Model.ownedElements`.
    pub fn owned(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        l.is("Model", l.at("Model", "ownedElements", op))
            .into_model_op()
    }

    /// A write into a class's optional `name`.
    pub fn class_name(&self, leaf: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Class", l.at("Class", "name", InstanceOp::set(leaf)))
    }

    /// A class named `name` inserted at `pos`: the insert carries the first
    /// character and every later one is an update of what it made. An empty
    /// name is a bare `New`.
    pub fn named_class(&self, pos: usize, name: &str) -> Vec<ModelOp> {
        if name.is_empty() {
            return vec![self.owned(InstanceOp::insert(pos, self.0.is("Class", InstanceOp::New)))];
        }
        name.chars()
            .enumerate()
            .map(|(index, ch)| {
                let write = self.class_name(chr(index, ch));
                if index == 0 {
                    self.owned(InstanceOp::insert(pos, write))
                } else {
                    self.owned(InstanceOp::at(pos, write))
                }
            })
            .collect()
    }

    /// Replace the whole name of the class at `pos`.
    pub fn rename(&self, pos: usize, old: &str, new: &str) -> Vec<ModelOp> {
        let mut ops = vec![self.owned(InstanceOp::at(
            pos,
            self.class_name(InstanceOp::Leaf(LeafOp::DeleteRange {
                start: 0,
                len: old.chars().count(),
            })),
        ))];
        for (index, ch) in new.chars().enumerate() {
            ops.push(self.owned(InstanceOp::at(pos, self.class_name(chr(index, ch)))));
        }
        ops
    }

    /// Append one character to the name of the class at `pos`.
    pub fn append(&self, pos: usize, at: usize, ch: char) -> ModelOp {
        self.owned(InstanceOp::at(pos, self.class_name(chr(at, ch))))
    }

    /// Set `Class.abstract`, an optional flag, on the class at `pos`.
    pub fn make_abstract(&self, pos: usize) -> ModelOp {
        let l = &self.0;
        self.owned(InstanceOp::at(
            pos,
            l.is(
                "Class",
                l.at(
                    "Class",
                    "abstract",
                    InstanceOp::set(InstanceOp::Leaf(LeafOp::Enable)),
                ),
            ),
        ))
    }

    /// Add a stereotype, a set of strings, to the class at `pos`.
    pub fn stereotype(&self, pos: usize, value: &str) -> ModelOp {
        let l = &self.0;
        self.owned(InstanceOp::at(
            pos,
            l.is(
                "Class",
                l.at(
                    "Class",
                    "stereotype",
                    InstanceOp::Leaf(LeafOp::Add(Scalar::text(value))),
                ),
            ),
        ))
    }

    pub fn delete(&self, pos: usize) -> ModelOp {
        self.owned(InstanceOp::delete(pos))
    }

    /// One character of the model's own name, which creates the root when
    /// nothing has yet.
    pub fn model_name(&self, pos: usize, ch: char) -> ModelOp {
        let l = &self.0;
        l.is(
            "Model",
            l.at("Model", "name", InstanceOp::set(chr(pos, ch))),
        )
        .into_model_op()
    }
}

/// `json.ecore`: every value class transparent, an object a keyed collection.
pub struct Json(pub Lang);

impl Json {
    pub fn new() -> Self {
        Json(Lang::parse(
            &serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON"),
        ))
    }

    /// An operation on the entries of the root object.
    pub fn root(&self, op: InstanceOp) -> ModelOp {
        self.object(op).into_model_op()
    }

    /// `Object.entry`, addressed.
    pub fn object(&self, op: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Object", l.at("Object", "entry", op))
    }

    pub fn string(&self, leaf: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("String", l.at("String", "value", leaf))
    }

    pub fn number(&self, leaf: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Number", l.at("Number", "value", leaf))
    }

    /// `Boolean.value`, a flag: enabled.
    pub fn yes(&self) -> InstanceOp {
        let l = &self.0;
        l.is(
            "Boolean",
            l.at("Boolean", "value", InstanceOp::Leaf(LeafOp::Enable)),
        )
    }

    pub fn array(&self, op: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Array", l.at("Array", "items", op))
    }

    /// Put `value` under `key` of the root object.
    pub fn put(&self, key: &str, value: InstanceOp) -> ModelOp {
        self.root(InstanceOp::entry(Scalar::text(key), value))
    }

    /// `value` under `key` of an object, for nesting.
    pub fn entry(&self, key: &str, value: InstanceOp) -> InstanceOp {
        self.object(InstanceOp::entry(Scalar::text(key), value))
    }

    /// A string member written character by character, the way the JSON
    /// editor writes one: no `New` before it.
    pub fn text(&self, key: &str, text: &str) -> Vec<ModelOp> {
        text.chars()
            .enumerate()
            .map(|(index, ch)| self.put(key, self.string(chr(index, ch))))
            .collect()
    }

    pub fn remove(&self, key: &str) -> ModelOp {
        self.root(InstanceOp::remove(Scalar::text(key)))
    }

    /// The root object on its own.
    pub fn empty_root(&self) -> ModelOp {
        self.0.is("Object", InstanceOp::New).into_model_op()
    }
}

/// `testing::bench_descriptor`: `Bench` at the root holds an `Item` in a
/// single containment (`one`), an optional one (`maybe`) and a sequence
/// (`many`).
pub struct Bench(pub Lang);

impl Bench {
    pub fn new() -> Self {
        Bench(Lang::parse(&bench_descriptor()))
    }

    pub fn root(&self, inner: InstanceOp) -> ModelOp {
        self.0.is("Bench", inner).into_model_op()
    }

    pub fn item(&self, inner: InstanceOp) -> InstanceOp {
        self.0.is("Item", inner)
    }

    pub fn item_text(&self, pos: usize, ch: char) -> InstanceOp {
        self.item(self.0.at("Item", "ID", chr(pos, ch)))
    }

    pub fn item_count(&self, by: i64) -> InstanceOp {
        self.item(self.0.at("Item", "count", inc_int(by)))
    }

    pub fn one(&self, item: InstanceOp) -> ModelOp {
        self.root(self.0.at("Bench", "one", item))
    }

    pub fn maybe(&self, op: InstanceOp) -> ModelOp {
        self.root(self.0.at("Bench", "maybe", op))
    }

    pub fn many(&self, op: InstanceOp) -> ModelOp {
        self.root(self.0.at("Bench", "many", op))
    }

    pub fn text(&self, ch: char) -> ModelOp {
        self.root(self.0.at("Bench", "text", chr(0, ch)))
    }

    pub fn tick(&self) -> ModelOp {
        self.root(self.0.at("Bench", "tick", inc_int(1)))
    }
}

// ---------------------------------------------------------------------------
// In-process replicas
// ---------------------------------------------------------------------------

pub type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// The default read, what `GET /api/model/{id}/state` answers for this
/// replica.
pub fn default_read(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

pub fn uml_descriptor() -> Value {
    serde_json::from_str(UML).expect("the fixture is JSON")
}

pub fn json_descriptor() -> Value {
    serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON")
}

/// Three replicas of one model, the table installed on all three.
pub fn three(descriptor: &Value) -> (Rep, Rep, Rep) {
    let (mut a, mut b, mut c) = triplet_log::<ModelLog>();
    let opening = a
        .send(install("m", descriptor))
        .expect("a fresh log takes its first `Install`");
    b.receive(opening.clone());
    c.receive(opening);
    (a, b, c)
}

/// Send locally and require the replica to accept.
macro_rules! send {
    ($replica:expr, $op:expr) => {{
        let op = $op;
        $replica
            .send(op.clone())
            .unwrap_or_else(|why| panic!("refused {op:?}: {why:?}"))
    }};
}

/// Send each operation of a list locally, then deliver them all to `to`.
pub fn send_all(from: &mut Rep, to: &mut [&mut Rep], ops: Vec<ModelOp>) {
    for op in ops {
        let event = send!(from, op);
        for replica in to.iter_mut() {
            replica.receive(event.clone());
        }
    }
}
