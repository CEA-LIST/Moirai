//! ST.13, part (1): the interpreted log's deep size no longer reads low.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.13.md`. Part (2), the
//! optional register's absent value, has its own oracle in
//! `st13_optional_null.rs`.
//!
//! `st13_c1`: a removal above an object lists itself in each touch of that
//! object it covers (`Touch::covered_by`, `node.rs:688-695`, filled by
//! `ObjectNode::retire`, `node.rs:1304-1315`), so the node under the removed
//! key and the whole log grow by at least one `EventId` per covered touch.
//! `st13_c2`: an installed log counts the table its descriptor was parsed to
//! (the `Arc<MetamodelSemantics>` of `log.rs:65`) once: at least what a hand
//! count of the table's public fields finds, and less than what `deepsize`'s
//! rules give for it plus its inline size, at every replica. `st13_c2b`: a
//! table two logs share by `Arc` is counted by each log alone and once in one
//! walk over both, which is `deepsize`'s `Arc` rule and `moirai-fuzz`'s
//! stated reading of a sample (`memory.rs:3-4`). `st13_c3`: an object's
//! fields and a keyed collection's entries are charged per entry on one
//! basis, the inline size of one entry, `size_of::<(FeatureSlot, Node)>()`
//! and `size_of::<(Scalar, Node)>()`, on top of what the key and the value
//! hold. `st13_c4`: a sampled fuzz run over `json.ecore`, whose keyed objects
//! are removed and so carry covered touches, runs to its end.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 --offline -p moirai-interp --features fuzz --test st13_deep_size
//! ```

#![cfg(feature = "fuzz")]

use std::{
    collections::HashSet,
    mem::{size_of, size_of_val},
    sync::{Arc, LazyLock},
};

use deepsize::DeepSizeOf;
use moirai_fuzz::{
    config::{FuzzerConfig, Predicate, RunConfig},
    fuzzer::fuzzer,
};
use moirai_interp::fuzz::set_descriptor;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{
    BT_DESCRIPTOR, JSON_DESCRIPTOR, attribute, bench_descriptor, class, class_slot, descriptor,
    feature_slot, objects, opened, table,
};
use moirai_interp::{InstanceOp, ModelLog, ModelOp, Node, ObjectNode};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    crdt::query::Read,
    event::id::EventId,
    replica::{IsReplica, Replica},
};
use moirai_semantics::{
    ClassSemantics, ClassSlot, EnumSemantics, FeatureSemantics, FeatureSlot, MetamodelSemantics,
};
use serde_json::{Value, json};

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// What a value holds on the heap, as `deepsize` counts it: its deep size
/// less its own inline size.
fn heap_of<T: DeepSizeOf>(value: &T) -> usize {
    value.deep_size_of() - size_of_val(value)
}

fn send(replica: &mut Rep, op: InstanceOp) {
    let description = format!("{op:?}");
    replica
        .send(ModelOp::Instance(op))
        .unwrap_or_else(|refusal| {
            panic!("the scenario's own {description} was refused: {refusal}")
        });
}

/// A seed a reader can reproduce, as ST.12 and `ip12` write it.
fn seed(run: u8) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = run;
    seed[31] = !run;
    seed
}

// ===========================================================================
// C1: a removal counts the touches it covers
// ===========================================================================

fn json_descriptor() -> Value {
    serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON")
}

static JSON: LazyLock<Arc<MetamodelSemantics>> = LazyLock::new(|| table(&json_descriptor()));

fn on_root(inner: InstanceOp) -> InstanceOp {
    let object = class_slot(&JSON, "Object");
    InstanceOp::variant(
        object,
        InstanceOp::field(feature_slot(&JSON, object, "entry"), inner),
    )
}

/// The node the root object holds at `key`.
fn at_key<'a>(log: &'a ModelLog, key: &str) -> &'a Node {
    let root = objects(log.root());
    assert_eq!(root.len(), 1, "one root object, not {}", root.len());
    let entry = feature_slot(&JSON, class_slot(&JSON, "Object"), "entry");
    let Some(Node::Map(map)) = root[0].fields().get(&entry) else {
        panic!("the root object holds no map at `entry`");
    };
    map.children()
        .get(&Scalar::text(key))
        .unwrap_or_else(|| panic!("the root object holds no node at `{key}`"))
}

/// How many operations reach the object the removal then covers.
const COVERED: usize = 30;

/// C1: `COVERED` operations reach one object (the first creates it, each
/// later one adds a touch, `node.rs:955-971`), then a removal of its key
/// covers every one of those touches (`node.rs:1304-1315`). Each touch's
/// `covered_by` list goes from empty to holding the removal's id, so the
/// node at the key and the log grow by at least `COVERED` ids. Nothing else
/// under the key changes: the object holds no field, so there is no leaf for
/// the removal to empty. False at `a1e8a55`: `Touch` counts nothing beyond
/// its inline size (`fuzz.rs:532-538`).
#[test]
fn st13_c1_a_removal_counts_the_touches_it_covers() {
    let (mut a, _b) = opened("st13", &json_descriptor());
    let string = class_slot(&JSON, "String");
    for _ in 0..COVERED {
        send(
            &mut a,
            on_root(InstanceOp::entry(
                Scalar::text("k"),
                InstanceOp::variant(string, InstanceOp::New),
            )),
        );
    }
    let node_before = heap_of(at_key(a.state(), "k"));
    let log_before = heap_of(a.state());
    send(&mut a, on_root(InstanceOp::remove(Scalar::text("k"))));
    let node_after = heap_of(at_key(a.state(), "k"));
    let log_after = heap_of(a.state());

    let floor = COVERED * size_of::<EventId>();
    let mut wrong = Vec::new();
    if node_after < node_before + floor {
        wrong.push(format!(
            "the node at the removed key went from {node_before} to {node_after} bytes, {} \
             more, under {COVERED} ids' {floor}",
            node_after as isize - node_before as isize
        ));
    }
    if log_after < log_before + floor {
        wrong.push(format!(
            "the log went from {log_before} to {log_after} bytes, {} more, under {COVERED} \
             ids' {floor}",
            log_after as isize - log_before as isize
        ));
    }
    assert!(
        wrong.is_empty(),
        "C1: a removal covering {COVERED} touches is not counted:\n{}",
        wrong.join("\n")
    );
}

// ===========================================================================
// C2: the parsed table is counted, once per walk
// ===========================================================================

/// A hand count of what a table holds on the heap, on `deepsize`'s terms: a
/// `Vec` at its capacity (`exact`) or its length (the floor), a `String` at
/// its capacity or length, an `Arc<str>` or an `Arc<[ClassSlot]>` by the
/// bytes it points to, once per allocation, a `SmallVec` at its capacity
/// when it has spilled and nothing when it has not. The `Arc`s' counts are
/// left out, as `deepsize` leaves them out. Every field of the table is
/// public (`moirai-semantics/src/table.rs:393-477`), and every other field
/// (slots, flags, rules, provenance, `transparent`) is plain data with
/// nothing on the heap.
struct TableCount {
    exact: bool,
    seen: HashSet<usize>,
    bytes: usize,
}

impl TableCount {
    fn arc_str(&mut self, text: &Arc<str>) {
        if self.seen.insert(Arc::as_ptr(text) as *const u8 as usize) {
            self.bytes += text.len();
        }
    }

    fn arc_slots(&mut self, slots: &Arc<[ClassSlot]>) {
        if self.seen.insert(Arc::as_ptr(slots) as *const u8 as usize) {
            self.bytes += size_of_val::<[ClassSlot]>(slots);
        }
    }

    fn vec<T>(&mut self, items: &Vec<T>) {
        let slots = if self.exact {
            items.capacity()
        } else {
            items.len()
        };
        self.bytes += slots * size_of::<T>();
    }

    fn class(&mut self, class: &ClassSemantics) {
        self.arc_str(&class.name);
        if class.supers.spilled() {
            let slots = if self.exact {
                class.supers.capacity()
            } else {
                class.supers.len()
            };
            self.bytes += slots * size_of::<ClassSlot>();
        }
        self.vec::<FeatureSemantics>(&class.declared);
        for feature in &class.declared {
            self.arc_str(&feature.name);
        }
        self.vec(&class.visible);
        for (name, _, _) in &class.visible {
            self.arc_str(name);
        }
        self.arc_slots(&class.concrete);
    }

    fn enumeration(&mut self, entry: &EnumSemantics) {
        self.arc_str(&entry.name);
        self.vec(&entry.literals);
        for literal in &entry.literals {
            self.arc_str(literal);
        }
    }

    fn of(sem: &MetamodelSemantics, exact: bool) -> usize {
        let mut count = TableCount {
            exact,
            seen: HashSet::new(),
            bytes: 0,
        };
        count.arc_str(&sem.ns_uri);
        count.arc_str(&sem.package);
        count.bytes += if exact {
            sem.digest.capacity()
        } else {
            sem.digest.len()
        };
        count.vec::<ClassSemantics>(&sem.classes);
        for class in &sem.classes {
            count.class(class);
        }
        count.vec::<EnumSemantics>(&sem.enums);
        for entry in &sem.enums {
            count.enumeration(entry);
        }
        count.vec(&sem.roots);
        count.bytes
    }
}

/// The table's share of a log's heap, at least: the `Arc`'s pointee, which a
/// count through the `Arc` charges as its inline size, and the floor.
fn table_floor(sem: &MetamodelSemantics) -> usize {
    size_of::<MetamodelSemantics>() + TableCount::of(sem, false)
}

/// The table's share as `deepsize` counts it through the `Arc`, by hand.
fn table_exact(sem: &MetamodelSemantics) -> usize {
    size_of::<MetamodelSemantics>() + TableCount::of(sem, true)
}

/// The bytes of a log's header strings and descriptor.
fn text_of(log: &ModelLog) -> usize {
    let header = log.header().expect("the log is opened");
    header.model_id.len()
        + header.metamodel_id.len()
        + log.descriptor().expect("the log is opened").len()
}

/// C2: an installed log, at each of two replicas and on three descriptors of
/// different sizes, counts at least its header, its descriptor's bytes and
/// the table those bytes were parsed to, and counts the table once: what it
/// counts beyond its text is less than the table's count by `deepsize`'s
/// rules plus the table's inline size: a second pass over the table through
/// its pointee adds at least that much (its inline size, or its vectors,
/// which come to more), while a second pass through the `Arc` adds nothing.
/// False at `a1e8a55`: the impl counts the header and the descriptor and
/// leaves the table out (`fuzz.rs:386-404`).
#[test]
fn st13_c2_an_installed_log_counts_its_parsed_table_once() {
    let bt: Value = serde_json::from_str(BT_DESCRIPTOR).expect("the fixture is JSON");
    let mut wrong = Vec::new();
    for (label, descriptor) in [
        ("json", json_descriptor()),
        ("bt", bt),
        ("bench", bench_descriptor()),
    ] {
        let (a, b) = opened("st13", &descriptor);
        for (who, replica) in [("a", &a), ("b", &b)] {
            let log = replica.state();
            let sem = log.semantics().expect("the log is installed");
            let text = text_of(log);
            let (floor, exact) = (table_floor(sem), table_exact(sem));
            let counted = heap_of(log);
            if counted < text + floor {
                wrong.push(format!(
                    "{label}, replica {who}: {counted} bytes counted, under the {text} bytes of \
                     its header and descriptor plus the table's {floor} at least ({} inline)",
                    size_of::<MetamodelSemantics>()
                ));
            }
            if counted >= text + exact + size_of::<MetamodelSemantics>() {
                wrong.push(format!(
                    "{label}, replica {who}: {counted} bytes counted, {} beyond its text's {text}, \
                     where the table once is {exact} by `deepsize`'s rules and a second pass over \
                     it adds at least its inline {}",
                    counted - text,
                    size_of::<MetamodelSemantics>()
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "C2: an installed log does not count its parsed table once:\n{}",
        wrong.join("\n")
    );
}

/// C2b: two logs sharing one table by `Arc` (a clone shares it). Each log
/// alone counts the table, since each replica's sample is its own walk; one
/// walk over both counts it once, so the walk over the pair comes to at
/// least the table's floor less than the two logs walked apart. A count that
/// reached the table through the `Arc`'s pointee and not through the `Arc`
/// would charge it twice in the pair.
#[test]
fn st13_c2b_a_shared_table_is_counted_by_each_log_and_once_in_one_walk() {
    let (a, _b) = opened("st13", &json_descriptor());
    let one = a.state().clone();
    let pair = vec![one.clone(), one.clone()];
    let (left, right) = (
        pair[0].semantics().expect("installed"),
        pair[1].semantics().expect("installed"),
    );
    assert!(
        Arc::ptr_eq(left, right),
        "premise: the two clones share one table"
    );
    let floor = table_floor(left);
    let alone = [heap_of(&pair[0]), heap_of(&pair[1])];
    let together = heap_of(&pair) - pair.capacity() * size_of::<ModelLog>();

    let mut wrong = Vec::new();
    for (index, counted) in alone.iter().enumerate() {
        let text = text_of(&pair[index]);
        if *counted < text + floor {
            wrong.push(format!(
                "log {index} alone: {counted} bytes, under its text's {text} plus the table's \
                 {floor}"
            ));
        }
    }
    if together + floor > alone[0] + alone[1] {
        wrong.push(format!(
            "both in one walk: {together} bytes, against {} and {} apart; one walk should count \
             the shared table once, so at least {floor} under their sum {}",
            alone[0],
            alone[1],
            alone[0] + alone[1]
        ));
    }
    assert!(
        wrong.is_empty(),
        "C2b: a table shared by `Arc` is not counted once per walk:\n{}",
        wrong.join("\n")
    );
}

// ===========================================================================
// C3: object fields and map entries, one basis
// ===========================================================================

/// `Doc`, the root: two counters `a` and `b`, and a keyed collection `m` of
/// counters.
fn ledger_descriptor() -> Value {
    let counter = json!({"kind": "counter", "num": "i64", "resettable": true});
    descriptor(
        &["Doc"],
        vec![(
            "Doc",
            class(
                false,
                &[],
                vec![
                    attribute("a", json!({"kind": "single"}), counter.clone()),
                    attribute("b", json!({"kind": "single"}), counter.clone()),
                    attribute(
                        "m",
                        json!({"kind": "keyed", "key": {"kind": "str"}}),
                        counter,
                    ),
                ],
            ),
        )],
        json!({}),
    )
}

static LEDGER: LazyLock<Arc<MetamodelSemantics>> = LazyLock::new(|| table(&ledger_descriptor()));

fn on_doc(feature: &str, inner: InstanceOp) -> InstanceOp {
    let doc = class_slot(&LEDGER, "Doc");
    InstanceOp::variant(
        doc,
        InstanceOp::field(feature_slot(&LEDGER, doc, feature), inner),
    )
}

fn inc() -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1)))
}

fn bump(feature: &str) -> InstanceOp {
    on_doc(feature, inc())
}

fn bump_at(key: &str) -> InstanceOp {
    on_doc("m", InstanceOp::entry(Scalar::text(key), inc()))
}

/// A fresh ledger on one replica (its twin delivers nothing, so nothing
/// stabilizes) after `ops`.
fn ledger_after(ops: Vec<InstanceOp>) -> (Rep, Rep) {
    let (mut a, b) = opened("st13", &ledger_descriptor());
    for op in ops {
        send(&mut a, op);
    }
    (a, b)
}

fn doc_of(replica: &Rep) -> &ObjectNode {
    let root = objects(replica.state().root());
    assert_eq!(root.len(), 1, "one `Doc`, not {}", root.len());
    root[0]
}

fn field_of<'a>(object: &'a ObjectNode, name: &str) -> &'a Node {
    object
        .fields()
        .get(&feature_slot(&LEDGER, object.class(), name))
        .unwrap_or_else(|| panic!("`{name}` is not minted"))
}

/// What the resolver every id of one replica shares (an `Arc`) adds to a
/// walk that meets it first: an id's own heap, since the rest of an id is
/// inline.
fn resolver_share(object: &ObjectNode) -> usize {
    heap_of(object.born())
}

/// C3: what one more entry costs an object's fields and a keyed collection,
/// each measured between two twins that took the same number of operations,
/// so that the object's own record of them (its creations and touches) is
/// the same on both and cancels.
///
/// Objects: `P` takes `a`, `a`; `Q` takes `a`, `b`. `Q`'s `Doc` has one field
/// more. Its extra heap, less what its fields hold on their own (each
/// measured alone, so each counted the shared resolver once, which one walk
/// over `Doc` counts once in all), is the charge for one more entry. Maps:
/// `P'` takes `m[k1]`, `m[k1]`; `Q'` takes `m[k1]`, `m[k2]`; the same, less the
/// key's own bytes too. The one basis: an entry is charged its inline size,
/// `size_of::<(FeatureSlot, Node)>()` for a field and
/// `size_of::<(Scalar, Node)>()` for a map entry. False at `a1e8a55`: the
/// fields are charged that way by hand (`fuzz.rs:494-501`), and the map's
/// entries go through `deepsize`'s `BTreeMap` impl, which charges
/// `len * size_of::<(usize, u16, u16, [(K, V); 5], [usize; 6])>() * 2 / 16`.
#[test]
fn st13_c3_object_fields_and_map_entries_are_charged_on_one_basis() {
    let (p, _p) = ledger_after(vec![bump("a"), bump("a")]);
    let (q, _q) = ledger_after(vec![bump("a"), bump("b")]);
    let (p_doc, q_doc) = (doc_of(&p), doc_of(&q));
    assert_eq!(
        (p_doc.fields().len(), q_doc.fields().len()),
        (1, 2),
        "premise: `P` minted one field and `Q` two"
    );
    let resolver = resolver_share(q_doc);
    assert_eq!(
        resolver,
        resolver_share(p_doc),
        "premise: both replicas' ids share resolvers of one size"
    );
    let object_entry = (heap_of(q_doc) + heap_of(field_of(p_doc, "a")) + resolver) as isize
        - (heap_of(p_doc) + heap_of(field_of(q_doc, "a")) + heap_of(field_of(q_doc, "b"))) as isize;
    let object_expected = size_of::<(FeatureSlot, Node)>() as isize;

    let (pm, _pm) = ledger_after(vec![bump_at("k1"), bump_at("k1")]);
    let (qm, _qm) = ledger_after(vec![bump_at("k1"), bump_at("k2")]);
    let map_of = |replica: &Rep| -> (usize, Vec<(usize, usize)>) {
        let node = field_of(doc_of(replica), "m");
        let Node::Map(map) = node else {
            panic!("`m` is not a map");
        };
        (
            heap_of(node),
            map.children()
                .iter()
                .map(|(key, child)| (heap_of(key), heap_of(child)))
                .collect(),
        )
    };
    let ((pm_heap, pm_entries), (qm_heap, qm_entries)) = (map_of(&pm), map_of(&qm));
    assert_eq!(
        (pm_entries.len(), qm_entries.len()),
        (1, 2),
        "premise: `P'` holds one key and `Q'` two"
    );
    let entries = |list: &[(usize, usize)]| -> usize {
        list.iter().map(|(key, child)| key + child).sum::<usize>()
    };
    let resolver_m = resolver_share(doc_of(&qm));
    let map_entry = (qm_heap + entries(&pm_entries) + resolver_m) as isize
        - (pm_heap + entries(&qm_entries)) as isize;
    let map_expected = size_of::<(Scalar, Node)>() as isize;

    let mut wrong = Vec::new();
    if object_entry != object_expected {
        wrong.push(format!(
            "an object's field is charged {object_entry} bytes, where one entry's inline size, \
             `size_of::<(FeatureSlot, Node)>()`, is {object_expected}"
        ));
    }
    if map_entry != map_expected {
        wrong.push(format!(
            "a map's entry is charged {map_entry} bytes, where one entry's inline size, \
             `size_of::<(Scalar, Node)>()`, is {map_expected}"
        ));
    }
    assert!(
        wrong.is_empty(),
        "C3: object fields and map entries are not charged on one basis:\n{}",
        wrong.join("\n")
    );
}

// ===========================================================================
// C4: a sampled run over keyed objects
// ===========================================================================

/// C4: a fuzz run over `json.ecore` that samples memory every 25 operations,
/// three replicas, 200 operations, churn 0.4, final merge, runs to its end.
/// Seed 14 because its run makes the root an `Object` and writes keyed
/// objects under it (its final state at spec time is an object whose keys
/// include `alpha`, `delta`, `door` and `gamma`), and the keyed branch
/// draws a removal one time in ten at a map that holds keys
/// (`fuzz.rs:213-216`), so covered touches are walked in a real sampled run.
#[test]
fn st13_c4_a_sampled_fuzz_run_over_json_completes() {
    set_descriptor(JSON_DESCRIPTOR);
    let runs = vec![
        RunConfig::new(0.4, 3, 200, None, Some(seed(14)), false, false).with_memory_sampling(25),
    ];
    let config = FuzzerConfig::<ModelLog, Read<Value>>::new(
        "interp-json-memory",
        runs,
        true,
        Predicate::new(Read::new(), |left, right| left == right),
        false,
    );
    fuzzer::<ModelLog, Read<Value>>(config);
}
