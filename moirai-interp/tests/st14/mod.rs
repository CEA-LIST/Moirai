//! What ST.14's two oracle files share: a counting allocator, the `Seq`
//! descriptor, its operations and its readers, and a stable digest.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.14.md`.
//!
//! # Why allocations
//!
//! The claim is that a positional operation on a list whose whole history the
//! operation has seen no longer replays that list. A replay of a list's
//! eg-walker graph allocates at least once per event it replays (the
//! direct-predecessor list it reads for each event, the items it rebuilds, the
//! life dots it records), so the number of heap allocations one operation
//! makes grows with the history of every list on its path when it replays, and
//! stays flat when it does not. The count is deterministic for one build and
//! one operation stream, it is read by the test binary's own global
//! allocator, and it needs no hook in the code under test.
//!
//! The counter is per thread, so the test harness running tests side by side
//! does not mix their counts; every count is taken around one call made on the
//! counting thread.

#![allow(dead_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use moirai_interp::testing::{
    attribute, class, class_slot, containment, descriptor, feature_slot, install, table,
};
use moirai_interp::{InstanceOp, LeafOp, ModelLog, ModelOp, Node, Scalar, SlotNode};
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    crdt::query::Read,
    event::id::EventId,
    replica::{IsReplica, Replica},
};
use moirai_semantics::MetamodelSemantics;
use serde_json::{Value, json};
use std::sync::{Arc, LazyLock};

// ===========================================================================
// The counting allocator
// ===========================================================================

/// The system allocator, counting every allocation and reallocation the
/// calling thread asks it for.
pub struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn bump() {
    // `try_with`: a thread being torn down still allocates and must not panic.
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
}

// SAFETY: every call is forwarded to `System` unchanged; the only addition is
// a counter in a const-initialised thread-local that holds a `Cell<u64>`,
// which neither allocates nor registers a destructor.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Run `f` and answer what it returned and how many allocations the calling
/// thread made while it ran.
pub fn counted<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = ALLOCATIONS.with(Cell::get);
    let out = f();
    let after = ALLOCATIONS.with(Cell::get);
    (out, after - before)
}

/// The mean of a slice of counts.
pub fn mean(counts: &[u64]) -> f64 {
    if counts.is_empty() {
        return 0.0;
    }
    counts.iter().sum::<u64>() as f64 / counts.len() as f64
}

// ===========================================================================
// The `Seq` descriptor
// ===========================================================================

/// ```text
/// Root   items: sequence of Item
/// Item   n: counter (i64), parts: sequence of Part
/// Part   v: counter (i64)
/// ```
///
/// Two lists, one inside each element of the other, and counters as the only
/// leaves: a counter's local check and effect do not read its history, so
/// what an operation's cost does with history is the lists' doing.
pub fn seq_descriptor() -> Value {
    let counter = || json!({"kind": "counter", "num": "i64", "resettable": false});
    descriptor(
        &["Root"],
        vec![
            (
                "Root",
                class(false, &[], vec![containment("items", "Item", "sequence")]),
            ),
            (
                "Item",
                class(
                    false,
                    &[],
                    vec![
                        attribute("n", json!({"kind": "single"}), counter()),
                        containment("parts", "Part", "sequence"),
                    ],
                ),
            ),
            (
                "Part",
                class(
                    false,
                    &[],
                    vec![attribute("v", json!({"kind": "single"}), counter())],
                ),
            ),
        ],
        json!({}),
    )
}

pub static SEQ: LazyLock<Arc<MetamodelSemantics>> = LazyLock::new(|| table(&seq_descriptor()));

pub type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// The `Install` every replica of a `Seq` model starts from.
pub fn seq_install() -> ModelOp {
    install("st14", &seq_descriptor())
}

fn root_class() -> moirai_semantics::ClassSlot {
    class_slot(&SEQ, "Root")
}

fn item_class() -> moirai_semantics::ClassSlot {
    class_slot(&SEQ, "Item")
}

fn part_class() -> moirai_semantics::ClassSlot {
    class_slot(&SEQ, "Part")
}

fn on_items(inner: InstanceOp) -> ModelOp {
    let root = root_class();
    ModelOp::Instance(InstanceOp::variant(
        root,
        InstanceOp::field(feature_slot(&SEQ, root, "items"), inner),
    ))
}

fn inc(by: i64) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(by)))
}

fn item_n(by: i64) -> InstanceOp {
    let item = item_class();
    InstanceOp::variant(
        item,
        InstanceOp::field(feature_slot(&SEQ, item, "n"), inc(by)),
    )
}

fn item_parts(inner: InstanceOp) -> InstanceOp {
    let item = item_class();
    InstanceOp::variant(
        item,
        InstanceOp::field(feature_slot(&SEQ, item, "parts"), inner),
    )
}

fn part_v(by: i64) -> InstanceOp {
    let part = part_class();
    InstanceOp::variant(
        part,
        InstanceOp::field(feature_slot(&SEQ, part, "v"), inc(by)),
    )
}

/// `Root.items`: a new item at `pos`, its counter at `n`.
pub fn insert_item(pos: usize, n: i64) -> ModelOp {
    on_items(InstanceOp::insert(pos, item_n(n)))
}

/// `Root.items[pos].n += by`: a `SeqOp::Update` on `items`.
pub fn bump_item(pos: usize, by: i64) -> ModelOp {
    on_items(InstanceOp::at(pos, item_n(by)))
}

/// `Root.items`: the item at `pos` out.
pub fn delete_item(pos: usize) -> ModelOp {
    on_items(InstanceOp::delete(pos))
}

/// `Root.items[item].parts`: a new part at `pos`, its counter at `v`.
pub fn insert_part(item: usize, pos: usize, v: i64) -> ModelOp {
    on_items(InstanceOp::at(
        item,
        item_parts(InstanceOp::insert(pos, part_v(v))),
    ))
}

/// `Root.items[item].parts[pos].v += by`.
pub fn bump_part(item: usize, pos: usize, by: i64) -> ModelOp {
    on_items(InstanceOp::at(
        item,
        item_parts(InstanceOp::at(pos, part_v(by))),
    ))
}

/// `Root.items[item].parts`: the part at `pos` out.
pub fn delete_part(item: usize, pos: usize) -> ModelOp {
    on_items(InstanceOp::at(item, item_parts(InstanceOp::delete(pos))))
}

/// Send locally and require the replica to accept.
pub fn send(replica: &mut Rep, op: ModelOp) -> EventMessage<ModelOp> {
    let description = format!("{op:?}");
    replica
        .send(op)
        .unwrap_or_else(|why| panic!("{} refused {description}: {why}", replica.id()))
}

/// What `GET /api/state` serves for this replica's model, as bytes.
pub fn served(replica: &Rep) -> String {
    let value: Value = replica.query(&Read::<Value>::new());
    serde_json::to_string(&value).expect("a read serializes")
}

/// What a `Seq` model serves, projected to `[(n, [v, ...]), ...]`, the items
/// in order with their parts in order. Absent counters read as 0.
pub fn projection(replica: &Rep) -> Vec<(i64, Vec<i64>)> {
    let value: Value = replica.query(&Read::<Value>::new());
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            let n = item.get("n").and_then(Value::as_i64).unwrap_or(0);
            let parts = item
                .get("parts")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .map(|part| part.get("v").and_then(Value::as_i64).unwrap_or(0))
                        .collect()
                })
                .unwrap_or_default();
            (n, parts)
        })
        .collect()
}

// ===========================================================================
// Readers a writer picks its positions with
// ===========================================================================

fn root_object(replica: &Rep) -> Option<&moirai_interp::ObjectNode> {
    match replica.state().root() {
        Node::Slot(SlotNode::Value(object)) => Some(object.as_ref()),
        _ => None,
    }
}

fn seq_of<'a>(
    object: &'a moirai_interp::ObjectNode,
    class: moirai_semantics::ClassSlot,
    name: &str,
) -> Option<&'a moirai_interp::SeqNode> {
    match object.fields().get(&feature_slot(&SEQ, class, name)) {
        Some(Node::Seq(seq)) => Some(seq),
        _ => None,
    }
}

/// The ids of the operations that inserted the items this replica serves, in
/// order: `SeqNode::order` of `Root.items`.
pub fn item_order(replica: &Rep) -> Vec<EventId> {
    root_object(replica)
        .and_then(|root| seq_of(root, root_class(), "items"))
        .map(|seq| seq.order())
        .unwrap_or_default()
}

/// The ids of the operations that inserted the parts of the item at `item`,
/// in order.
pub fn part_order(replica: &Rep, item: usize) -> Vec<EventId> {
    let Some(items) = root_object(replica).and_then(|root| seq_of(root, root_class(), "items"))
    else {
        return Vec::new();
    };
    let Some(id) = items.order().get(item).cloned() else {
        return Vec::new();
    };
    match items.children().get(&id) {
        Some(Node::Slot(SlotNode::Value(object))) => seq_of(object, item_class(), "parts")
            .map(|seq| seq.order())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// An event id as a name both sides of a replica boundary agree on.
pub fn key(id: &EventId) -> (String, usize) {
    (id.origin_id().to_string(), id.seq())
}

// ===========================================================================
// A stable digest
// ===========================================================================

/// The SHA-256 of what this replica serves, in hex: `metamodel_digest` hashes
/// `value.to_string()`, which is the served bytes.
pub fn served_digest(replica: &Rep) -> String {
    let value: Value = replica.query(&Read::<Value>::new());
    moirai_semantics::metamodel_digest(&value)
}

/// Where a test writes what it observed when a fixture is missing or does not
/// match, so a reader can diff it: `target/tmp/st14/<name>`.
pub fn observed_path(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("st14");
    std::fs::create_dir_all(&dir).expect("the target's tmp directory is writable");
    dir.join(name)
}

/// A fixture of this item, by file name, from `tests/st14/`.
pub fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("st14")
        .join(name)
}
