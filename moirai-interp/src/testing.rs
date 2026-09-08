//! Fixtures the tests of three modules share: descriptors small enough to
//! read, the slot lookups a test writes its operations with, and one `IsLog`
//! that drives a bare [`Node`] without the installation ceremony.
//!
//! Compiled under `cfg(test)` only. It is here rather than inside one
//! module's `mod tests` because the container tests, the log tests and the
//! read-out tests all write against the same two metamodels, and a fixture
//! copied three times is a fixture that drifts twice.

use std::cell::RefCell;
use std::sync::Arc;

use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    clock::version_vector::Version,
    event::Event,
    log_id::LogId,
    replica::{IsReplica, Replica},
    state::log::IsLog,
};
use moirai_semantics::{ClassSlot, FeatureSlot, MetamodelSemantics, from_descriptor};
use serde_json::{Value, json};

use crate::leaf::LeafLog;
use crate::node::{At, Ctx, Emit, Node, ObjectNode, SeqNode, Shaped, Site, apply, check};

#[cfg(feature = "sink")]
use moirai_protocol::state::{
    object_path::ObjectPath,
    sink::{SinkCollector, SinkOwnership},
};

/// A `provenance` object; the parser requires all four sources and this
/// crate reads none of them, so one honest filler serves every fixture.
pub fn provenance() -> Value {
    json!({
        "ordered": "declared",
        "unique": "declared",
        "leaf": "declared",
        "presence": "declared"
    })
}

/// One attribute entry, at any shape and any leaf.
pub fn attribute(name: &str, shape: Value, leaf: Value) -> Value {
    json!({
        "name": name,
        "merge": {"kind": "attribute", "shape": shape, "leaf": leaf},
        "provenance": provenance()
    })
}

/// A single-valued text attribute, which is what most of `bt.ecore` is.
pub fn text(name: &str) -> Value {
    attribute(name, json!({"kind": "single"}), json!({"kind": "text"}))
}

/// An optional text attribute, which is what `TreeNode.name` is.
pub fn optional_text(name: &str) -> Value {
    attribute(name, json!({"kind": "optional"}), json!({"kind": "text"}))
}

/// One containment entry.
pub fn containment(name: &str, target: &str, shape: &str) -> Value {
    json!({
        "name": name,
        "merge": {"kind": "containment", "shape": {"kind": shape}, "target": target},
        "provenance": provenance()
    })
}

/// One non-containment reference entry.
pub fn reference(name: &str, target: &str, many: bool) -> Value {
    json!({
        "name": name,
        "merge": {"kind": "reference", "many": many, "target": target},
        "provenance": provenance()
    })
}

/// One class entry, with its features split into the three arrays the
/// descriptor keeps them in.
pub fn class(is_abstract: bool, supers: &[&str], features: Vec<Value>) -> Value {
    let mut attributes = Vec::new();
    let mut containments = Vec::new();
    let mut references = Vec::new();
    for feature in features {
        match feature["merge"]["kind"].as_str() {
            Some("attribute") => attributes.push(feature),
            Some("containment") => containments.push(feature),
            _ => references.push(feature),
        }
    }
    json!({
        "abstract": is_abstract,
        "superTypes": supers,
        "attributes": attributes,
        "containments": containments,
        "references": references
    })
}

/// A whole descriptor at `formatVersion` 2.
pub fn descriptor(roots: &[&str], classes: Vec<(&str, Value)>, enums: Value) -> Value {
    let mut map = serde_json::Map::new();
    for (name, body) in classes {
        map.insert(name.to_string(), body);
    }
    json!({
        "formatVersion": 2,
        "package": "fixture",
        "nsURI": "http://example.org/fixture",
        "rootClasses": roots,
        "classes": Value::Object(map),
        "enums": enums
    })
}

/// The table a descriptor parses to, or the reason it did not.
pub fn table(descriptor: &Value) -> Arc<MetamodelSemantics> {
    Arc::new(from_descriptor(descriptor).expect("the fixture is a readable descriptor"))
}

/// A metamodel shaped like the behaviour tree's own containment structure,
/// small enough that a reader can hold its slots in their head.
///
/// ```text
/// Root          children: sequence of TreeNode, main: single TreeNode,
///               title: text, tag: optional text
/// TreeNode      abstract; ID: text, name: optional text
/// Sequence      : TreeNode; children: sequence of TreeNode
/// Fallback      : TreeNode
/// ```
pub fn mini_descriptor() -> Value {
    descriptor(
        &["Root"],
        vec![
            (
                "Root",
                class(
                    false,
                    &[],
                    vec![
                        containment("children", "TreeNode", "sequence"),
                        containment("main", "TreeNode", "single"),
                        text("title"),
                        optional_text("tag"),
                    ],
                ),
            ),
            (
                "TreeNode",
                class(true, &[], vec![text("ID"), optional_text("name")]),
            ),
            (
                "Sequence",
                class(
                    false,
                    &["TreeNode"],
                    vec![containment("children", "TreeNode", "sequence")],
                ),
            ),
            ("Fallback", class(false, &["TreeNode"], vec![])),
        ],
        json!({}),
    )
}

/// The same, parsed.
pub fn mini() -> Arc<MetamodelSemantics> {
    table(&mini_descriptor())
}

/// A class's slot, by name.
pub fn class_slot(sem: &MetamodelSemantics, name: &str) -> ClassSlot {
    sem.classes
        .iter()
        .find(|class| &*class.name == name)
        .unwrap_or_else(|| panic!("no class `{name}` in the fixture"))
        .slot
}

/// A feature's **visible** slot on one class, by name: the index an
/// `InstanceOp::Field` carries.
pub fn feature_slot(sem: &MetamodelSemantics, class: ClassSlot, name: &str) -> FeatureSlot {
    let holder = &sem.classes[class.index()];
    let index = holder
        .visible
        .iter()
        .position(|(feature, _, _)| &**feature == name)
        .unwrap_or_else(|| panic!("`{}` cannot see `{name}`", holder.name));
    FeatureSlot(index as u16)
}

thread_local! {
    /// What [`Harness::default`] builds. Set by the test before it asks for
    /// its replicas; every `#[test]` is its own thread.
    static FIXTURE: RefCell<Option<(Arc<MetamodelSemantics>, ClassSlot)>> =
        const { RefCell::new(None) };
}

/// One model tree under one table, as a log a `Replica` can host.
///
/// The container layer without the installation ceremony: no `Install`, no
/// header, no unresolved counter. `ModelLog` adds those and reuses the same
/// [`check`] and [`apply`], so what this drives is what a real log drives.
#[derive(Clone, Debug)]
pub struct Harness {
    /// The table every node below was minted from.
    pub sem: Arc<MetamodelSemantics>,
    /// The class the model root is declared as.
    pub root_class: ClassSlot,
    /// The model.
    pub root: Node,
}

impl Harness {
    /// The site the root sits at: one object of the root class's concrete
    /// closure.
    pub(crate) fn site(&self) -> Shaped {
        Shaped::Bare(Site::Object(self.root_class))
    }
}

impl Default for Harness {
    fn default() -> Self {
        let (sem, root_class) = FIXTURE
            .with_borrow(Clone::clone)
            .expect("a test sets the fixture before it asks for its replicas");
        Harness {
            root: Node::for_shaped(Shaped::Bare(Site::Object(root_class))),
            sem,
            root_class,
        }
    }
}

impl IsLog for Harness {
    type Value = Value;
    type Op = crate::op::InstanceOp;

    fn is_enabled(&self, op: &Self::Op) -> bool {
        check(&self.sem, Some(&self.root), self.site(), op, &At::root()).is_ok()
    }

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        #[cfg(feature = "sink")] path: ObjectPath,
        #[cfg(feature = "sink")] sink: &mut SinkCollector,
        #[cfg(feature = "sink")] _ownership: SinkOwnership,
    ) {
        let ctx = Ctx {
            id: event.id(),
            lamport: event.lamport(),
            version: event.version(),
        };
        let site = self.site();
        apply(
            &self.sem,
            &mut self.root,
            site,
            ctx,
            event.op().clone(),
            &At::root(),
            Emit::new(
                #[cfg(feature = "sink")]
                path,
                #[cfg(feature = "sink")]
                sink,
            ),
        )
        .expect("the harness only ever applies what it checked");
    }

    fn stabilize(&mut self, version: &Version) {
        self.root.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.root.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.root.is_default()
    }
}

/// Two replicas of one model under one table.
pub type HarnessTwins = (
    Replica<Harness, Tcsb<crate::op::InstanceOp>>,
    Replica<Harness, Tcsb<crate::op::InstanceOp>>,
);

/// Three of them.
pub type HarnessTriplet = (
    Replica<Harness, Tcsb<crate::op::InstanceOp>>,
    Replica<Harness, Tcsb<crate::op::InstanceOp>>,
    Replica<Harness, Tcsb<crate::op::InstanceOp>>,
);

fn install(sem: &Arc<MetamodelSemantics>, root: &str) {
    let root_class = class_slot(sem, root);
    FIXTURE.with_borrow_mut(|slot| *slot = Some((Arc::clone(sem), root_class)));
}

/// [`moirai_crdt::utils::membership::twins_log`] over a fixture.
pub fn twins(sem: &Arc<MetamodelSemantics>, root: &str) -> HarnessTwins {
    install(sem, root);
    let log_id = LogId::generate();
    (
        Replica::bootstrap_with_log_id("a".to_string(), &["a", "b"], log_id.clone()),
        Replica::bootstrap_with_log_id("b".to_string(), &["a", "b"], log_id),
    )
}

/// [`moirai_crdt::utils::membership::triplet_log`] over a fixture.
pub fn triplet(sem: &Arc<MetamodelSemantics>, root: &str) -> HarnessTriplet {
    install(sem, root);
    let log_id = LogId::generate();
    (
        Replica::bootstrap_with_log_id("a".to_string(), &["a", "b", "c"], log_id.clone()),
        Replica::bootstrap_with_log_id("b".to_string(), &["a", "b", "c"], log_id.clone()),
        Replica::bootstrap_with_log_id("c".to_string(), &["a", "b", "c"], log_id),
    )
}

/// The one object a slot holds, for a test that knows there is one.
pub fn object(node: &Node) -> &ObjectNode {
    match node {
        Node::Slot(slot) => {
            let objects = slot.objects();
            assert_eq!(objects.len(), 1, "one object here, not {}", objects.len());
            objects[0]
        }
        other => panic!("not a containment: {other:?}"),
    }
}

/// The objects a slot holds, in the order it holds them.
pub fn objects(node: &Node) -> Vec<&ObjectNode> {
    match node {
        Node::Slot(slot) => slot.objects(),
        other => panic!("not a containment: {other:?}"),
    }
}

/// The sequence a node is.
pub fn sequence(node: &Node) -> &SeqNode {
    match node {
        Node::Seq(seq) => seq,
        other => panic!("not a sequence: {other:?}"),
    }
}

/// One feature of one object, if it has been minted.
pub fn field<'a>(sem: &MetamodelSemantics, object: &'a ObjectNode, name: &str) -> Option<&'a Node> {
    object
        .fields()
        .get(&feature_slot(sem, object.class(), name))
}

/// The leaf a node is.
pub fn leaf(node: &Node) -> &LeafLog {
    match node {
        Node::Leaf(leaf) => leaf,
        Node::Opt(opt) => leaf(opt.child().expect("the optional is set")),
        other => panic!("not a leaf: {other:?}"),
    }
}

/// One text attribute of one object, read as a string; the empty string when
/// nothing has been written to it.
pub fn text_of(sem: &MetamodelSemantics, object: &ObjectNode, name: &str) -> String {
    match field(sem, object, name) {
        Some(node) => leaf(node)
            .read_json(Some(sem))
            .as_str()
            .expect("a text leaf reads as a string")
            .to_string(),
        None => String::new(),
    }
}

/// The children of a sequence, in read order.
pub fn ordered(seq: &SeqNode) -> Vec<&Node> {
    seq.order()
        .iter()
        .filter_map(|id| seq.children().get(id))
        .collect()
}

/// A metamodel with one feature per rule the table can produce, so a test can
/// drive every construction the generator can compile without inventing
/// twenty metamodels.
///
/// `Bench` is the root and holds every attribute shape and leaf family, the
/// three containment shapes over `Item`, and both reference forms. `Item`
/// carries a text and a counter, so a test that has to watch a PO-Log shrink
/// underneath a container has something under it that shrinks.
pub fn bench_descriptor() -> Value {
    let counter =
        |resettable: bool| json!({"kind": "counter", "num": "i32", "resettable": resettable});
    descriptor(
        &["Bench"],
        vec![
            (
                "Bench",
                class(
                    false,
                    &[],
                    vec![
                        text("text"),
                        attribute("counter", json!({"kind": "single"}), counter(true)),
                        attribute("simple", json!({"kind": "single"}), counter(false)),
                        attribute(
                            "flagEw",
                            json!({"kind": "single"}),
                            json!({"kind": "flag", "wins": "enable"}),
                        ),
                        attribute(
                            "flagDw",
                            json!({"kind": "single"}),
                            json!({"kind": "flag", "wins": "disable"}),
                        ),
                        attribute(
                            "regMv",
                            json!({"kind": "single"}),
                            json!({"kind": "register", "tie": "mv"}),
                        ),
                        attribute(
                            "regLww",
                            json!({"kind": "single"}),
                            json!({"kind": "register", "tie": "lww"}),
                        ),
                        attribute(
                            "regFair",
                            json!({"kind": "single"}),
                            json!({"kind": "register", "tie": "fair"}),
                        ),
                        attribute(
                            "regPo",
                            json!({"kind": "single"}),
                            json!({"kind": "register", "tie": "po"}),
                        ),
                        attribute(
                            "regTo",
                            json!({"kind": "single"}),
                            json!({"kind": "register", "tie": "to"}),
                        ),
                        attribute(
                            "enumReg",
                            json!({"kind": "single"}),
                            json!({"kind": "enum", "class": "Status", "tie": "mv"}),
                        ),
                        attribute(
                            "setAw",
                            json!({"kind": "set", "tie": "aw"}),
                            json!({"kind": "text"}),
                        ),
                        attribute(
                            "setRw",
                            json!({"kind": "set", "tie": "rw"}),
                            json!({"kind": "text"}),
                        ),
                        attribute("bag", json!({"kind": "bag"}), json!({"kind": "text"})),
                        attribute("optCounter", json!({"kind": "optional"}), counter(true)),
                        attribute("seqCounter", json!({"kind": "sequence"}), counter(true)),
                        attribute("tick", json!({"kind": "single"}), counter(true)),
                        containment("one", "Item", "single"),
                        containment("maybe", "Item", "optional"),
                        containment("many", "Item", "sequence"),
                        reference("refOne", "Item", false),
                        reference("refMany", "Item", true),
                    ],
                ),
            ),
            (
                "Item",
                class(
                    false,
                    &[],
                    vec![
                        text("ID"),
                        attribute("count", json!({"kind": "single"}), counter(true)),
                    ],
                ),
            ),
        ],
        json!({"Status": ["RUNNING", "SUCCESS", "FAILURE"]}),
    )
}

/// The same, parsed.
pub fn bench() -> Arc<MetamodelSemantics> {
    table(&bench_descriptor())
}
