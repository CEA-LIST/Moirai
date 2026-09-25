//! SI.01, the store identity of an element, read where a client reads it: the
//! state a replica serves for a model.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`. The contract these
//! tests read, called contract R there:
//!
//! - every element of a model (every instance of a class the model holds) is
//!   served as a JSON record carrying `eClass` and, under the reserved key
//!   `__id`, a non-empty string: its store identity;
//! - a non-transparent element is its canonical record with `__id` added;
//! - a transparent element (`json.ecore`'s `Object`, `Array`, `String`,
//!   `Number`, `Boolean`) is served as the record
//!   `{"eClass": <class>, <its one feature>: <value>, "__id": <identity>}`,
//!   which is the explicit form `model-editor/src/crdt/explicit.ts` builds;
//! - everything else in the canonical form of `eval.rs` is as it was.
//!
//! What is served is read through `QueryableLog::query_state_json`, the one
//! function `GET /api/model/{id}/state` calls, in process, and through that
//! route itself on `model_node` processes on loopback.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si01_store_identity \
//!     -- --test-threads=1
//! ```
//!
//! The multi-replica scenario follows the skip rule of `e2e_interp.rs`: no
//! binary, print `E2E-SKIP <scenario>: <why>` and return. The in-process tests
//! need no binary.
#![cfg(feature = "network")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{
    JSON_DESCRIPTOR, bench_descriptor, class_slot, feature_slot, install, opened,
};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Map, Value};

/// The reserved key an element's store identity is served under.
const ID: &str = "__id";

const UML: &str = include_str!("fixtures/uml.metamodel.json");

// ---------------------------------------------------------------------------
// Reading the served form
// ---------------------------------------------------------------------------

/// Every element of a served document, depth first in document order: every
/// JSON object that carries `eClass`.
fn elements(doc: &Value) -> Vec<&Map<String, Value>> {
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

/// The identities of a served document, held to the claim: exactly `count`
/// elements, each served with a non-empty string under `__id`, no two alike.
fn identified(doc: &Value, count: usize, who: &str) -> Vec<String> {
    let found = elements(doc);
    assert_eq!(
        found.len(),
        count,
        "{who}: the served document holds {} element records where {count} elements were \
         created; contract R serves every element, transparent ones included, as a record \
         carrying `eClass` and `{ID}`. Served: {doc}",
        found.len()
    );
    let mut ids = Vec::new();
    for element in found {
        let class = element
            .get("eClass")
            .and_then(Value::as_str)
            .unwrap_or("?");
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
fn id_of(element: &Value) -> String {
    element
        .get(ID)
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no `{ID}` on {element}"))
}

/// `Model.ownedElements`, as served.
fn owned(doc: &Value) -> &Vec<Value> {
    doc["ownedElements"]
        .as_array()
        .unwrap_or_else(|| panic!("no ownedElements array in {doc}"))
}

/// A UML element's name; an unwritten optional name is an absent key.
fn name_of(element: &Value) -> &str {
    element.get("name").and_then(Value::as_str).unwrap_or("")
}

/// Where the element with this identity sits in `Model.ownedElements`.
fn pos_of(doc: &Value, id: &str) -> usize {
    owned(doc)
        .iter()
        .position(|element| element.get(ID).and_then(Value::as_str) == Some(id))
        .unwrap_or_else(|| panic!("no element {id} in {doc}"))
}

/// The one owned element carrying this name.
fn named<'a>(doc: &'a Value, name: &str) -> &'a Value {
    let hits: Vec<&Value> = owned(doc)
        .iter()
        .filter(|element| name_of(element) == name)
        .collect();
    assert_eq!(hits.len(), 1, "one element named {name:?} in {doc}");
    hits[0]
}

/// A member of a served JSON object, which contract R serves as
/// `{"eClass": "Object", "entry": {<key>: <member>}, "__id": ...}`.
fn member<'a>(object: &'a Value, key: &str) -> &'a Value {
    let found = &object["entry"][key];
    assert!(!found.is_null(), "no member {key:?} in {object}");
    found
}

/// The items of a served JSON array, `{"eClass": "Array", "items": [...]}`.
fn items(array: &Value) -> &Vec<Value> {
    array["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items array in {array}"))
}

/// Every identity anywhere in a served document.
fn all_ids(doc: &Value) -> BTreeSet<String> {
    elements(doc)
        .into_iter()
        .filter_map(|element| element.get(ID).and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// Building operations from a table
// ---------------------------------------------------------------------------

/// One metamodel's table, and the two path steps every operation is built of.
struct Lang {
    sem: MetamodelSemantics,
}

impl Lang {
    fn parse(descriptor: &Value) -> Self {
        Lang {
            sem: from_descriptor(descriptor).expect("the fixture is a descriptor this crate reads"),
        }
    }

    /// `Variant(class, inner)`.
    fn is(&self, class: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::variant(class_slot(&self.sem, class), inner)
    }

    /// `Field(feature, inner)`, the feature named on the class that sees it.
    fn at(&self, class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::field(
            feature_slot(&self.sem, class_slot(&self.sem, class), feature),
            inner,
        )
    }
}

fn chr(pos: usize, ch: char) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })
}

fn inc_int(by: i64) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(by)))
}

fn inc_float(by: f64) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Inc(Scalar::float(by)))
}

/// SimpleUML: a `Model` at the root holding classes in `ownedElements`.
struct Uml(Lang);

impl Uml {
    fn new() -> Self {
        Uml(Lang::parse(
            &serde_json::from_str(UML).expect("the fixture is JSON"),
        ))
    }

    /// An operation on `Model.ownedElements`.
    fn owned(&self, op: InstanceOp) -> ModelOp {
        let l = &self.0;
        l.is("Model", l.at("Model", "ownedElements", op))
            .into_model_op()
    }

    /// A write into a class's optional `name`.
    fn class_name(&self, leaf: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Class", l.at("Class", "name", InstanceOp::set(leaf)))
    }

    /// A class named `name` inserted at `pos`: the insert carries the first
    /// character and every later one is an update of what it made. An empty
    /// name is a bare `New`.
    fn named_class(&self, pos: usize, name: &str) -> Vec<ModelOp> {
        if name.is_empty() {
            return vec![self.owned(InstanceOp::insert(
                pos,
                self.0.is("Class", InstanceOp::New),
            ))];
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
    fn rename(&self, pos: usize, old: &str, new: &str) -> Vec<ModelOp> {
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
    fn append(&self, pos: usize, at: usize, ch: char) -> ModelOp {
        self.owned(InstanceOp::at(pos, self.class_name(chr(at, ch))))
    }

    /// Set `Class.abstract`, an optional flag, on the class at `pos`.
    fn make_abstract(&self, pos: usize) -> ModelOp {
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
    fn stereotype(&self, pos: usize, value: &str) -> ModelOp {
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

    fn delete(&self, pos: usize) -> ModelOp {
        self.owned(InstanceOp::delete(pos))
    }

    /// One character of the model's own name, which creates the root when
    /// nothing has yet.
    fn model_name(&self, pos: usize, ch: char) -> ModelOp {
        let l = &self.0;
        l.is("Model", l.at("Model", "name", InstanceOp::set(chr(pos, ch))))
            .into_model_op()
    }
}

/// `json.ecore`: every value class transparent, an object a keyed collection.
struct Json(Lang);

impl Json {
    fn new() -> Self {
        Json(Lang::parse(
            &serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON"),
        ))
    }

    /// An operation on the entries of the root object.
    fn root(&self, op: InstanceOp) -> ModelOp {
        self.object(op).into_model_op()
    }

    /// `Object.entry`, addressed.
    fn object(&self, op: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Object", l.at("Object", "entry", op))
    }

    fn string(&self, leaf: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("String", l.at("String", "value", leaf))
    }

    fn number(&self, leaf: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Number", l.at("Number", "value", leaf))
    }

    fn array(&self, op: InstanceOp) -> InstanceOp {
        let l = &self.0;
        l.is("Array", l.at("Array", "items", op))
    }

    /// Put `value` under `key` of the root object.
    fn put(&self, key: &str, value: InstanceOp) -> ModelOp {
        self.root(InstanceOp::entry(Scalar::text(key), value))
    }

    /// A string member written character by character, the way the JSON
    /// editor writes one: no `New` before it.
    fn text(&self, key: &str, text: &str) -> Vec<ModelOp> {
        text.chars()
            .enumerate()
            .map(|(index, ch)| self.put(key, self.string(chr(index, ch))))
            .collect()
    }

    fn remove(&self, key: &str) -> ModelOp {
        self.root(InstanceOp::remove(Scalar::text(key)))
    }

    /// The root object on its own.
    fn empty_root(&self) -> ModelOp {
        self.0.is("Object", InstanceOp::New).into_model_op()
    }
}

/// `testing::bench_descriptor`: `Bench` at the root holds an `Item` in a
/// single containment (`one`), an optional one (`maybe`) and a sequence
/// (`many`).
struct Bench(Lang);

impl Bench {
    fn new() -> Self {
        Bench(Lang::parse(&bench_descriptor()))
    }

    fn root(&self, inner: InstanceOp) -> ModelOp {
        self.0.is("Bench", inner).into_model_op()
    }

    fn item(&self, inner: InstanceOp) -> InstanceOp {
        self.0.is("Item", inner)
    }

    fn item_text(&self, pos: usize, ch: char) -> InstanceOp {
        self.item(self.0.at("Item", "ID", chr(pos, ch)))
    }

    fn item_count(&self, by: i64) -> InstanceOp {
        self.item(self.0.at("Item", "count", inc_int(by)))
    }

    fn one(&self, item: InstanceOp) -> ModelOp {
        self.root(self.0.at("Bench", "one", item))
    }

    fn maybe(&self, op: InstanceOp) -> ModelOp {
        self.root(self.0.at("Bench", "maybe", op))
    }

    fn many(&self, op: InstanceOp) -> ModelOp {
        self.root(self.0.at("Bench", "many", op))
    }

    fn text(&self, ch: char) -> ModelOp {
        self.root(self.0.at("Bench", "text", chr(0, ch)))
    }

    fn tick(&self) -> ModelOp {
        self.root(self.0.at("Bench", "tick", inc_int(1)))
    }
}

// ---------------------------------------------------------------------------
// In-process replicas
// ---------------------------------------------------------------------------

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// What `GET /api/model/{id}/state` would answer for this replica.
fn served(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

fn uml_descriptor() -> Value {
    serde_json::from_str(UML).expect("the fixture is JSON")
}

fn json_descriptor() -> Value {
    serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON")
}

/// Three replicas of one model, the table installed on all three.
fn three(descriptor: &Value) -> (Rep, Rep, Rep) {
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
fn send_all(from: &mut Rep, to: &mut [&mut Rep], ops: Vec<ModelOp>) {
    for op in ops {
        let event = send!(from, op);
        for replica in to.iter_mut() {
            replica.receive(event.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// L2: the served form, in process
// ---------------------------------------------------------------------------

/// Every kind of place an element can sit in is served with an identity, one
/// operation that creates two elements gives them two, and nothing written
/// into an element afterwards moves its identity: not a text edit, not a
/// counter, not a sibling inserted before it, not a peer's concurrent write.
#[test]
fn si01_every_element_is_served_with_an_identity_that_edits_do_not_move() {
    // Bench: the root, a single containment, an optional one and a sequence.
    let bench = Bench::new();
    let (mut a, mut b) = opened("m", &bench_descriptor());
    // One operation creates the root and the first item of `many`.
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            bench.many(InstanceOp::insert(0, bench.item_text(0, 'x'))),
            bench.one(bench.item_text(0, 'o')),
            bench.maybe(InstanceOp::set(bench.item_count(1))),
            bench.many(InstanceOp::insert(1, bench.item_text(0, 'y'))),
        ],
    );
    let before = served(&a);
    assert_eq!(before, served(&b), "the twins serve one document");
    identified(&before, 5, "bench, created");
    let root = id_of(&before);
    let one = id_of(&before["one"]);
    let maybe = id_of(&before["maybe"]);
    let x = id_of(&before["many"][0]);
    let y = id_of(&before["many"][1]);

    // Concurrent edits from both sides, and a sibling put in front of `x`.
    let from_a = vec![
        send!(a, bench.many(InstanceOp::at(0, bench.item_text(1, 'z')))),
        send!(a, bench.many(InstanceOp::insert(0, bench.item_text(0, 'w')))),
        send!(a, bench.one(bench.item_count(3))),
    ];
    let from_b = vec![
        send!(b, bench.one(bench.item_text(1, 'p'))),
        send!(b, bench.maybe(InstanceOp::set(bench.item_count(2)))),
        send!(b, bench.many(InstanceOp::at(1, bench.item_count(4)))),
    ];
    for event in from_b {
        a.receive(event);
    }
    for event in from_a {
        b.receive(event);
    }
    let after = served(&a);
    assert_eq!(after, served(&b), "the twins serve one document after the edits");
    identified(&after, 6, "bench, edited");
    assert_eq!(id_of(&after), root, "the root kept its identity");
    assert_eq!(id_of(&after["one"]), one, "the single containment's element kept it");
    assert_eq!(id_of(&after["maybe"]), maybe, "the optional's element kept it");
    assert_eq!(id_of(&after["many"][1]), x, "x kept it with a sibling put before it");
    assert_eq!(id_of(&after["many"][2]), y, "y kept it");

    // JSON: every value is transparent and is served as a record.
    let js = Json::new();
    let (mut a, mut b) = opened("j", &json_descriptor());
    let mut ops = js.text("title", "x");
    ops.push(js.put(
        "tags",
        js.array(InstanceOp::insert(0, js.string(chr(0, 'p')))),
    ));
    ops.push(js.put("n", js.number(inc_float(3.0))));
    send_all(&mut a, &mut [&mut b], ops);
    let before = served(&a);
    assert_eq!(before, served(&b), "the JSON twins serve one document");
    // The root object, title, tags, tags[0] and n.
    identified(&before, 5, "json, created");
    let jroot = id_of(&before);
    let title = id_of(member(&before, "title"));
    let tags = id_of(member(&before, "tags"));
    let p = id_of(&items(member(&before, "tags"))[0]);
    let n = id_of(member(&before, "n"));

    let from_a = vec![
        send!(a, js.put("title", js.string(chr(1, 'y')))),
        send!(
            a,
            js.put(
                "tags",
                js.array(InstanceOp::insert(0, js.string(chr(0, 'q'))))
            )
        ),
    ];
    let from_b = vec![send!(b, js.put("n", js.number(inc_float(1.5))))];
    for event in from_b {
        a.receive(event);
    }
    for event in from_a {
        b.receive(event);
    }
    let after = served(&a);
    assert_eq!(after, served(&b), "the JSON twins serve one document after the edits");
    identified(&after, 6, "json, edited");
    assert_eq!(id_of(&after), jroot);
    assert_eq!(id_of(member(&after, "title")), title, "a text edit kept it");
    assert_eq!(id_of(member(&after, "tags")), tags);
    assert_eq!(
        id_of(&items(member(&after, "tags"))[1]),
        p,
        "an item kept it with another put before it"
    );
    assert_eq!(id_of(member(&after, "n")), n, "a peer's increment kept it");
}

/// Concurrent creations of one class in one place are one element on every
/// replica, and that element has one identity on every replica, whatever
/// order each of three replicas delivers the other two creations in. The
/// same holds where the places are distinct: three concurrent inserts are
/// three elements with three identities.
#[test]
fn si01_concurrent_creations_in_one_place_converge_on_one_identity_in_every_delivery_order() {
    let bench = Bench::new();
    let js = Json::new();
    let uml = Uml::new();

    type Setup = Box<dyn Fn() -> (Rep, Rep, Rep)>;
    let bench_fresh: Setup = Box::new(|| three(&bench_descriptor()));
    let bench_rooted: Setup = Box::new(move || {
        let (mut a, mut b, mut c) = three(&bench_descriptor());
        send_all(&mut a, &mut [&mut b, &mut c], vec![Bench::new().tick()]);
        (a, b, c)
    });
    let json_rooted: Setup = Box::new(move || {
        let (mut a, mut b, mut c) = three(&json_descriptor());
        send_all(&mut a, &mut [&mut b, &mut c], vec![Json::new().empty_root()]);
        (a, b, c)
    });
    let uml_fresh: Setup = Box::new(|| three(&uml_descriptor()));

    let places: Vec<(&str, &Setup, [ModelOp; 3], usize)> = vec![
        (
            "the root, created by all three",
            &bench_fresh,
            [bench.text('a'), bench.text('b'), bench.text('c')],
            1,
        ),
        (
            "a single-valued containment",
            &bench_rooted,
            [
                bench.one(bench.item_count(1)),
                bench.one(bench.item_count(2)),
                bench.one(bench.item_count(3)),
            ],
            2,
        ),
        (
            "an optional containment",
            &bench_rooted,
            [
                bench.maybe(InstanceOp::set(bench.item_count(1))),
                bench.maybe(InstanceOp::set(bench.item_count(2))),
                bench.maybe(InstanceOp::set(bench.item_count(3))),
            ],
            2,
        ),
        (
            "one key of a JSON object",
            &json_rooted,
            [
                js.put("k", js.string(chr(0, 'a'))),
                js.put("k", js.string(chr(0, 'b'))),
                js.put("k", js.string(chr(0, 'c'))),
            ],
            2,
        ),
        (
            "a UML root, each creator inserting a class of its own in the same operation",
            &uml_fresh,
            [
                uml.owned(InstanceOp::insert(0, uml.class_name(chr(0, 'A')))),
                uml.owned(InstanceOp::insert(0, uml.class_name(chr(0, 'B')))),
                uml.owned(InstanceOp::insert(0, uml.class_name(chr(0, 'C')))),
            ],
            4,
        ),
    ];

    for (place, setup, concurrent, count) in places {
        // Bit i of `order` says which of the other two replica i delivers
        // first.
        for order in 0u8..8 {
            let (mut a, mut b, mut c) = setup();
            let [op_a, op_b, op_c] = concurrent.clone();
            let ea = send!(a, op_a);
            let eb = send!(b, op_b);
            let ec = send!(c, op_c);
            let pair = |first, second, flip: bool| {
                if flip { [second, first] } else { [first, second] }
            };
            for event in pair(eb.clone(), ec.clone(), order & 1 != 0) {
                a.receive(event);
            }
            for event in pair(ea.clone(), ec.clone(), order & 2 != 0) {
                b.receive(event);
            }
            for event in pair(ea.clone(), eb.clone(), order & 4 != 0) {
                c.receive(event);
            }
            let who = format!("{place}, delivery order {order:03b}");
            let (sa, sb, sc) = (served(&a), served(&b), served(&c));
            identified(&sa, count, &format!("{who}, on a"));
            assert_eq!(sa, sb, "{who}: a and b serve different documents");
            assert_eq!(sa, sc, "{who}: a and c serve different documents");
        }
    }
}

/// An element deleted and then created again in the same place is a new
/// element: a new identity, and the old one appears nowhere.
#[test]
fn si01_an_element_deleted_and_created_again_gets_a_new_identity() {
    // A sequence: delete the item, insert one in the same place.
    let bench = Bench::new();
    let (mut a, mut b) = opened("m", &bench_descriptor());
    send_all(
        &mut a,
        &mut [&mut b],
        vec![bench.many(InstanceOp::insert(0, bench.item_text(0, 'x')))],
    );
    let first = id_of(&served(&a)["many"][0]);
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            bench.many(InstanceOp::delete(0)),
            bench.many(InstanceOp::insert(0, bench.item_text(0, 'x'))),
        ],
    );
    let now = served(&a);
    assert_eq!(now, served(&b));
    identified(&now, 2, "sequence, recreated");
    assert_ne!(id_of(&now["many"][0]), first, "a sequence item created again is new");
    assert!(!all_ids(&now).contains(&first), "the deleted item's identity is gone");

    // An optional containment: unset, then set again.
    send_all(
        &mut a,
        &mut [&mut b],
        vec![bench.maybe(InstanceOp::set(bench.item_count(1)))],
    );
    let first = id_of(&served(&a)["maybe"]);
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            bench.maybe(InstanceOp::unset()),
            bench.maybe(InstanceOp::set(bench.item_count(2))),
        ],
    );
    let now = served(&a);
    assert_eq!(now, served(&b));
    assert_ne!(id_of(&now["maybe"]), first, "an optional element set again is new");
    assert!(!all_ids(&now).contains(&first), "the unset element's identity is gone");

    // A JSON key: removed, then put again, with and without the same class's
    // `New`, which is how the JSON editor writes a value (in one go).
    let js = Json::new();
    let (mut a, mut b) = opened("j", &json_descriptor());
    let mut ops = js.text("s", "ab");
    ops.push(js.put("n", js.number(inc_float(3.0))));
    send_all(&mut a, &mut [&mut b], ops);
    let before = served(&a);
    let (s, n) = (id_of(member(&before, "s")), id_of(member(&before, "n")));
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            js.remove("s"),
            js.put("s", js.string(chr(0, 'c'))),
            js.remove("n"),
            js.put("n", js.number(inc_float(5.0))),
        ],
    );
    let now = served(&a);
    assert_eq!(now, served(&b));
    identified(&now, 3, "json, recreated");
    assert_ne!(id_of(member(&now, "s")), s, "a string put again under its key is new");
    assert_ne!(id_of(member(&now, "n")), n, "a number put again under its key is new");
    let ids = all_ids(&now);
    assert!(!ids.contains(&s) && !ids.contains(&n), "the removed values' identities are gone");
}

// ---------------------------------------------------------------------------
// L3: model_node processes on loopback
// ---------------------------------------------------------------------------

const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

fn node_binary() -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var("MOIRAI_E2E_INTERP_NODE_BIN") {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!("MOIRAI_E2E_INTERP_NODE_BIN points at `{raw}`, which is not a file"))
        };
    }
    let built =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/examples/model_node");
    if built.is_file() {
        Ok(built)
    } else {
        Err(format!(
            "no `model_node` at {}; build it with `cargo build -p moirai-interp --features \
             network --example model_node`",
            built.display()
        ))
    }
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("an ephemeral port")
        .local_addr()
        .expect("a bound address")
        .port()
}

struct Node {
    id: String,
    http: u16,
    child: Child,
    log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn tail(&self) -> String {
        self.log_text()
            .lines()
            .rev()
            .take(25)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `n` replicas of one binary, meshed by a static `PEERS` list, each serving
/// the descriptors given.
struct Cluster {
    nodes: Vec<Node>,
    _scratch: PathBuf,
    http: reqwest::blocking::Client,
}

impl Cluster {
    fn start(binary: &PathBuf, ids: &[&str], descriptors: &[(&str, &str)]) -> Result<Self, String> {
        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch =
            std::env::temp_dir().join(format!("moirai-si01-{}-{run}", std::process::id()));
        let metamodels = scratch.join("metamodels");
        fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
        for (name, text) in descriptors {
            fs::write(metamodels.join(format!("{name}.json")), text)
                .map_err(|err| err.to_string())?;
        }
        let ports: Vec<(u16, u16)> = ids.iter().map(|_| (free_port(), free_port())).collect();
        let mut nodes = Vec::new();
        for (index, id) in ids.iter().enumerate() {
            let peers: Vec<String> = ids
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(other, peer)| format!("{peer}:127.0.0.1:{}", ports[other].0))
                .collect();
            let log = scratch.join(format!("{id}.log"));
            let out = fs::File::create(&log).map_err(|err| err.to_string())?;
            let err_out = out.try_clone().map_err(|err| err.to_string())?;
            let child = Command::new(binary)
                .env("REPLICA_ID", id)
                .env("LISTEN_PORT", ports[index].0.to_string())
                .env("HTTP_PORT", ports[index].1.to_string())
                .env("PEERS", peers.join(","))
                .env("LOG_ID", "5101de7a5101de7a5101de7a5101de7a")
                .env("METAMODEL_DIR", &metamodels)
                .env_remove("BOOTNODE_URL")
                .env_remove("DASHBOARD_URL")
                .env_remove("METAMODEL_PATH")
                .stdout(Stdio::from(out))
                .stderr(Stdio::from(err_out))
                .spawn()
                .map_err(|err| format!("spawning {id}: {err}"))?;
            nodes.push(Node {
                id: id.to_string(),
                http: ports[index].1,
                child,
                log,
            });
        }
        let cluster = Cluster {
            nodes,
            _scratch: scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|err| err.to_string())?,
        };
        cluster.await_health()?;
        cluster.await_mesh()?;
        Ok(cluster)
    }

    fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .http
            .get(self.nodes[node].url(path))
            .send()
            .map_err(|err| format!("GET {path} on {}: {err}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((status, serde_json::from_str(&body).unwrap_or(Value::String(body))))
    }

    fn post(&self, node: usize, path: &str, body: String) -> Result<(u16, Value), String> {
        let response = self
            .http
            .post(self.nodes[node].url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| format!("POST {path} on {}: {err}", self.nodes[node].id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((status, serde_json::from_str(&body).unwrap_or(Value::String(body))))
    }

    fn await_health(&self) -> Result<(), String> {
        for (index, node) in self.nodes.iter().enumerate() {
            let deadline = Instant::now() + HEALTH_TIMEOUT;
            while !matches!(self.get(index, "/api/health"), Ok((200, _))) {
                if Instant::now() > deadline {
                    return Err(format!("{} never answered /api/health:\n{}", node.id, node.tail()));
                }
                std::thread::sleep(POLL);
            }
        }
        Ok(())
    }

    fn await_mesh(&self) -> Result<(), String> {
        let deadline = Instant::now() + MESH_TIMEOUT;
        loop {
            let ready = (0..self.nodes.len()).all(|index| {
                matches!(self.get(index, "/api/peers"), Ok((200, peers))
                    if connected(&peers) + 1 >= self.nodes.len())
            });
            if ready {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("the mesh never formed:\n{}", self.nodes[0].tail()));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Create (`model` none) or join a model; answers its id.
    fn register(&self, node: usize, model: Option<&str>, key: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => serde_json::json!({ "model_id": id, "metamodel_id": key }),
            None => serde_json::json!({ "metamodel_id": key }),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!("registering on {} answered {status}: {answer}", self.nodes[node].id));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    fn submit(&self, node: usize, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!(
                "{} refused {op:?} ({status}): {answer}",
                self.nodes[node].id
            ));
        }
        Ok(())
    }

    fn submit_all(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        ops.iter().try_for_each(|op| self.submit(node, model, op))
    }

    fn state(&self, node: usize, model: &str) -> Result<Value, String> {
        match self.get(node, &format!("/api/model/{model}/state"))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("state answered {status}: {body}")),
        }
    }

    /// Poll until every node in `on` serves the same document for `model`,
    /// and answer with it.
    fn converged_on(&self, model: &str, on: &[usize]) -> Result<Value, String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let states: Vec<Result<Value, String>> =
                on.iter().map(|index| self.state(*index, model)).collect();
            if let Some(Ok(first)) = states.first()
                && !first.is_null()
                && states.iter().all(|state| state.as_ref().ok() == Some(first))
            {
                return Ok(first.clone());
            }
            if Instant::now() > deadline {
                let last: BTreeMap<String, String> = on
                    .iter()
                    .zip(states.iter())
                    .map(|(index, state)| {
                        (
                            self.nodes[*index].id.clone(),
                            match state {
                                Ok(value) => value.to_string(),
                                Err(why) => why.clone(),
                            },
                        )
                    })
                    .collect();
                return Err(format!("model {model} never converged; states were {last:?}"));
            }
            std::thread::sleep(POLL);
        }
    }

    /// Poll until `node` holds the model's table (its `Install`).
    fn until_table(&self, node: usize, model: &str) -> Result<(), String> {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Ok((200, metrics)) = self.get(node, &format!("/api/model/{model}/metrics"))
                && metrics["delivered_ops"].as_u64().unwrap_or(0) >= 1
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("{} never received the table of {model}", self.nodes[node].id));
            }
            std::thread::sleep(POLL);
        }
    }

    fn pause_all(&self, node: usize) -> Result<(), String> {
        self.control(node, "/api/pause-all")
    }

    fn resume_all(&self, node: usize) -> Result<(), String> {
        self.control(node, "/api/resume-all")
    }

    fn control(&self, node: usize, path: &str) -> Result<(), String> {
        let (status, body) = self.post(node, path, String::new())?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!("{path} on {} answered {status}: {body}", self.nodes[node].id))
        }
    }
}

fn connected(peers: &Value) -> usize {
    peers["peers"]
        .as_array()
        .map(|list| {
            list.iter()
                .filter(|peer| peer["status"] == Value::String("Connected".into()))
                .count()
        })
        .unwrap_or(0)
}

fn digest(text: &str) -> String {
    metamodel_digest(&serde_json::from_str::<Value>(text).expect("the fixture is JSON"))
}

/// Whether `node`'s log says it adopted a transferred state for `model`.
fn adopted(node: &Node, model: &str) -> bool {
    node.log_text()
        .lines()
        .any(|line| line.contains("adopted state from") && line.contains(model))
}

/// Four `model_node` processes: `a` and `b` write, partitioned twice; `c`
/// joins the SimpleUML model and `d` the JSON model afterwards, each by state
/// transfer. The three failure cases of the notation are on the UML model (an
/// empty name, two elements with one name, a rename), and a JSON document is
/// the fourth.
#[test]
fn si01_l3_two_writers_partitioned_and_two_joiners_by_state_transfer_agree_on_every_identity() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si01-l3: {why}");
            return;
        }
    };
    if let Err(why) = l3(&binary) {
        panic!("si01-l3: {why}");
    }
}

fn l3(binary: &PathBuf) -> Result<(), String> {
    let (a, b, c, d) = (0, 1, 2, 3);
    let cluster = Cluster::start(
        binary,
        &["a", "b", "c", "d"],
        &[("uml", UML), ("json", JSON_DESCRIPTOR)],
    )?;
    let uml = Uml::new();
    let js = Json::new();

    let m = cluster.register(a, None, &digest(UML))?;
    let j = cluster.register(a, None, &digest(JSON_DESCRIPTOR))?;
    cluster.register(b, Some(&m), &digest(UML))?;
    cluster.register(b, Some(&j), &digest(JSON_DESCRIPTOR))?;
    cluster.until_table(b, &m)?;
    cluster.until_table(b, &j)?;

    // ---- partition 1: both create the root of each model concurrently ----
    cluster.pause_all(a)?;
    cluster.pause_all(b)?;

    // a: the root and Door in ONE operation, then an unnamed class.
    cluster.submit_all(a, &m, &uml.named_class(0, "Door"))?;
    cluster.submit_all(a, &m, &uml.named_class(1, ""))?;
    // b: the root again, concurrently, by naming the model; two classes
    // sharing one name.
    cluster.submit(b, &m, &uml.model_name(0, 'N'))?;
    cluster.submit_all(b, &m, &uml.named_class(0, "Dup"))?;
    cluster.submit_all(b, &m, &uml.named_class(1, "Dup"))?;

    // a: the root object and `title` in one operation, `n`, and an array
    // made with its first item in one operation.
    cluster.submit_all(a, &j, &js.text("title", "x"))?;
    cluster.submit(a, &j, &js.put("n", js.number(inc_float(3.0))))?;
    cluster.submit(
        a,
        &j,
        &js.put("tags", js.array(InstanceOp::insert(0, js.string(chr(0, 'p'))))),
    )?;
    cluster.submit(a, &j, &js.put("shared", js.string(chr(0, 'A'))))?;
    // b: the root object again, concurrently, with the same key `shared`.
    cluster.submit(b, &j, &js.put("shared", js.string(chr(0, 'B'))))?;

    cluster.resume_all(a)?;
    cluster.resume_all(b)?;

    let doc = cluster.converged_on(&m, &[a, b])?;
    identified(&doc, 5, "uml after partition 1 (root, Door, unnamed, Dup, Dup)");
    let root = id_of(&doc);
    let door = id_of(named(&doc, "Door"));
    let unnamed = id_of(named(&doc, ""));
    let dups: Vec<String> = owned(&doc)
        .iter()
        .filter(|element| name_of(element) == "Dup")
        .map(id_of)
        .collect();
    assert_eq!(dups.len(), 2, "two classes named Dup in {doc}");
    let (dup1, dup2) = (dups[0].clone(), dups[1].clone());

    let jdoc = cluster.converged_on(&j, &[a, b])?;
    identified(&jdoc, 6, "json after partition 1 (root, title, n, tags, tags[0], shared)");
    let jroot = id_of(&jdoc);
    let title = id_of(member(&jdoc, "title"));
    let n = id_of(member(&jdoc, "n"));
    let tags = id_of(member(&jdoc, "tags"));
    let p = id_of(&items(member(&jdoc, "tags"))[0]);
    let shared = id_of(member(&jdoc, "shared"));

    // ---- partition 2: renames, edits, a sibling, a delete and a re-create ----
    cluster.pause_all(a)?;
    cluster.pause_all(b)?;

    let on_a = cluster.state(a, &m)?;
    cluster.submit_all(a, &m, &uml.rename(pos_of(&on_a, &door), "Door", "Hatch"))?;
    cluster.submit_all(a, &m, &uml.named_class(0, "Aa"))?;

    let on_b = cluster.state(b, &m)?;
    cluster.submit(b, &m, &uml.make_abstract(pos_of(&on_b, &dup1)))?;
    cluster.submit(b, &m, &uml.stereotype(pos_of(&on_b, &unnamed), "s"))?;
    let gone = pos_of(&on_b, &dup2);
    cluster.submit(b, &m, &uml.delete(gone))?;
    cluster.submit_all(b, &m, &uml.named_class(gone, "Dup"))?;

    cluster.submit(a, &j, &js.remove("n"))?;
    cluster.submit(a, &j, &js.put("n", js.number(inc_float(5.0))))?;
    cluster.submit(
        a,
        &j,
        &js.put("tags", js.array(InstanceOp::insert(0, js.string(chr(0, 'q'))))),
    )?;
    cluster.submit(b, &j, &js.put("title", js.string(chr(1, 'y'))))?;

    cluster.resume_all(a)?;
    cluster.resume_all(b)?;

    let doc = cluster.converged_on(&m, &[a, b])?;
    let ids = identified(&doc, 6, "uml after partition 2");
    assert_eq!(id_of(&doc), root, "the root kept its identity");
    assert_eq!(id_of(named(&doc, "Hatch")), door, "the rename kept Door's identity");
    let stereotyped: Vec<&Value> = owned(&doc)
        .iter()
        .filter(|element| element["stereotype"] == serde_json::json!(["s"]))
        .collect();
    assert_eq!(stereotyped.len(), 1, "one stereotyped class in {doc}");
    assert_eq!(id_of(stereotyped[0]), unnamed, "the unnamed class kept its identity");
    let dups: Vec<&Value> = owned(&doc)
        .iter()
        .filter(|element| name_of(element) == "Dup")
        .collect();
    assert_eq!(dups.len(), 2, "two classes named Dup again in {doc}");
    let flagged: Vec<&&Value> = dups
        .iter()
        .filter(|element| element["abstract"] == Value::Bool(true))
        .collect();
    assert_eq!(flagged.len(), 1, "one abstract Dup in {doc}");
    assert_eq!(id_of(flagged[0]), dup1, "the edited Dup kept its identity");
    let recreated = dups
        .iter()
        .find(|element| element["abstract"] != Value::Bool(true))
        .map(|element| id_of(element))
        .expect("the other Dup");
    let before: BTreeSet<&String> = [&root, &door, &unnamed, &dup1, &dup2].into_iter().collect();
    assert!(
        !before.contains(&recreated),
        "the Dup deleted and created again carries an old identity: {recreated}"
    );
    assert!(!ids.contains(&dup2), "the deleted Dup's identity is still served");
    assert!(
        !before.contains(&id_of(named(&doc, "Aa"))),
        "the new class carries an old identity"
    );

    let jdoc = cluster.converged_on(&j, &[a, b])?;
    let jids = identified(&jdoc, 7, "json after partition 2");
    assert_eq!(id_of(&jdoc), jroot);
    assert_eq!(id_of(member(&jdoc, "title")), title, "a text edit kept the identity");
    assert_eq!(id_of(member(&jdoc, "tags")), tags);
    assert_eq!(
        id_of(&items(member(&jdoc, "tags"))[1]),
        p,
        "an item kept its identity with another put before it"
    );
    assert_eq!(id_of(member(&jdoc, "shared")), shared);
    assert_ne!(
        id_of(member(&jdoc, "n")),
        n,
        "a number removed and put again under its key is a new element"
    );
    assert!(!jids.contains(&n), "the removed number's identity is still served");

    // ---- the joiners, by state transfer ----
    cluster.register(c, Some(&m), &digest(UML))?;
    cluster.register(d, Some(&j), &digest(JSON_DESCRIPTOR))?;
    let joined = cluster.converged_on(&m, &[a, b, c])?;
    assert_eq!(joined, doc, "the joiner serves the document the writers agreed on");
    let jjoined = cluster.converged_on(&j, &[a, b, d])?;
    assert_eq!(jjoined, jdoc);
    for (node, model) in [(c, &m), (d, &j)] {
        assert!(
            adopted(&cluster.nodes[node], model),
            "{} did not join {model} by state transfer; its log:\n{}",
            cluster.nodes[node].id,
            cluster.nodes[node].tail()
        );
    }

    // A joiner's own edit leaves the identity where it was, everywhere.
    let on_c = cluster.state(c, &m)?;
    cluster.submit(c, &m, &uml.append(pos_of(&on_c, &door), 5, '2'))?;
    let doc = cluster.converged_on(&m, &[a, b, c])?;
    assert_eq!(id_of(named(&doc, "Hatch2")), door);
    cluster.submit(d, &j, &js.put("title", js.string(chr(2, 'z'))))?;
    let jdoc = cluster.converged_on(&j, &[a, b, d])?;
    assert_eq!(id_of(member(&jdoc, "title")), title);
    Ok(())
}
