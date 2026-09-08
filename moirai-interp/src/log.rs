//! The log: the table, the header, the model, and the one number that says
//! how often a peer sent something this table could not route.
//!
//! # The table arrives inside the log (decision D2)
//!
//! [`ModelOp::Install`] is the first operation of a model's life. Its
//! `effect` parses the descriptor it carries and stores the table;
//! [`ModelLog::is_enabled`] refuses a second one and refuses every instance
//! operation until one has landed. Three consequences, and they are why this
//! design touches neither `replica.rs` nor `generic.rs`:
//!
//! - causal delivery puts `Install` before every other operation its creator
//!   wrote, so a peer catching up by delta replays it first;
//! - the table is a field of this struct and serializes with it, so a joiner
//!   by state transfer gets it without replaying anything;
//! - one log has one table for its whole life, so two replicas of one log
//!   cannot disagree about the schema, which is why the design's `Bind`
//!   cross-check is not built in this phase (decision D3).
//!
//! # Two refusals, and only one of them is a refusal
//!
//! Criterion I-A9 has two halves and they pull in opposite directions. A
//! *local* operation the descriptor alone shows to be malformed is refused,
//! with a sentence naming the class or the feature, by
//! [`ModelLog::is_enabled`] — which is the interpreted form of the phase 4
//! node's `check_structure` guard, and it is why the interpreted node needs
//! no separate guard at all. A *remote* operation is never refused: it has
//! already happened somewhere, refusing it would fork the state, and the
//! only honest answers are to apply it or to count it. One that will not
//! route increments [`ModelLog::unresolved`] and changes nothing else, and
//! nothing here panics on one.

use std::sync::Arc;

use moirai_protocol::{
    clock::version_vector::Version,
    event::Event,
    state::log::IsLog,
    utils::intern_str::{InternalizeOp, Interner},
};
use moirai_semantics::{MetamodelSemantics, from_descriptor};
use serde_json::Value;

#[cfg(feature = "sink")]
use moirai_protocol::state::{
    object_path::ObjectPath,
    sink::{SinkCollector, SinkOwnership},
};

use crate::node::{At, Ctx, Emit, Mode, Node, Refusal, Shaped, Site, Target, apply, check};
use crate::op::ModelOp;

/// What a model says it is.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ModelHeader {
    /// The model's own id, as the node registered it.
    pub model_id: String,
    /// The metamodel's id: the digest of the descriptor that opened this log.
    pub metamodel_id: String,
}

/// One model, merged by the table its first operation installed.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ModelLog {
    /// Serialized, not skipped: a joiner by state transfer has no `Install`
    /// to replay and this is where it gets its table.
    sem: Option<Arc<MetamodelSemantics>>,
    header: Option<ModelHeader>,
    root: Node,
    unresolved: u64,
}

impl ModelLog {
    /// The table, once one is installed.
    pub fn semantics(&self) -> Option<&Arc<MetamodelSemantics>> {
        self.sem.as_ref()
    }

    /// What the model says it is, once it has been opened.
    pub fn header(&self) -> Option<&ModelHeader> {
        self.header.as_ref()
    }

    /// The model.
    pub fn root(&self) -> &Node {
        &self.root
    }

    /// How many remote operations this table could not route.
    ///
    /// A metric and never a panic. With one table per log it stays at zero;
    /// a peer that installed a different metamodel into the same log is the
    /// only way to move it, and this is what that looks like from here.
    pub fn unresolved(&self) -> u64 {
        self.unresolved
    }

    /// Where the model sits: one object of whichever class the descriptor
    /// names as a root.
    pub(crate) fn site() -> Shaped {
        Shaped::Bare(Site::Object(Target::Roots))
    }

    /// Would this operation be accepted from a local writer, and why not?
    ///
    /// The whole path is resolved against the table before this answers,
    /// which is what makes it the structural check: an operation naming a
    /// feature its class does not declare, or writing a leaf with the wrong
    /// kind of operation, comes back with a sentence naming the class or the
    /// feature.
    pub fn refusal(&self, op: &ModelOp) -> Result<(), Refusal> {
        match op {
            ModelOp::Install { descriptor, .. } => {
                if self.sem.is_some() {
                    return Err(Refusal::AlreadyInstalled);
                }
                parse(descriptor).map(|_| ())
            }
            ModelOp::Instance(op) => {
                let sem = self.sem.as_deref().ok_or(Refusal::NoTable)?;
                check(
                    sem,
                    Some(&self.root),
                    Self::site(),
                    op,
                    &At::root(),
                    Mode::Local,
                )
            }
        }
    }
}

/// A descriptor's text as a table, or the sentence that says why not.
fn parse(descriptor: &str) -> Result<MetamodelSemantics, Refusal> {
    let value: Value = serde_json::from_str(descriptor)
        .map_err(|err| Refusal::Descriptor(format!("it is not JSON: {err}")))?;
    from_descriptor(&value).map_err(|err| Refusal::Descriptor(err.to_string()))
}

impl IsLog for ModelLog {
    /// The canonical form of `02 Validation Plan` §2, built by
    /// [`crate::eval`].
    type Value = Value;
    type Op = ModelOp;

    fn is_enabled(&self, op: &Self::Op) -> bool {
        self.refusal(op).is_ok()
    }

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        #[cfg(feature = "sink")] path: ObjectPath,
        #[cfg(feature = "sink")] sink: &mut SinkCollector,
        #[cfg(feature = "sink")] _ownership: SinkOwnership,
    ) {
        match event.op() {
            ModelOp::Install {
                model_id,
                metamodel_id,
                descriptor,
            } => {
                if let Some(header) = &self.header {
                    // Two replicas opened one log at once. Identical
                    // installations are the same table twice and are simply
                    // the second one landing; a different one is a peer this
                    // log disagrees with, and that is what the counter is
                    // for.
                    if header.metamodel_id != *metamodel_id {
                        self.unresolved += 1;
                    }
                    return;
                }
                match parse(descriptor) {
                    Ok(sem) => {
                        self.sem = Some(Arc::new(sem));
                        self.header = Some(ModelHeader {
                            model_id: model_id.clone(),
                            metamodel_id: metamodel_id.clone(),
                        });
                        self.root = Node::for_shaped(Self::site());
                    }
                    // A descriptor that will not parse is a peer's mistake and
                    // not this replica's crash.
                    Err(_) => self.unresolved += 1,
                }
            }
            ModelOp::Instance(op) => {
                let Some(sem) = self.sem.clone() else {
                    self.unresolved += 1;
                    return;
                };
                let ctx = Ctx {
                    id: event.id(),
                    lamport: event.lamport(),
                    version: event.version(),
                };
                // Asked before anything is written, so that an operation this
                // table cannot route leaves the model exactly as it was.
                if check(
                    &sem,
                    Some(&self.root),
                    Self::site(),
                    op,
                    &At::root(),
                    Mode::Routing,
                )
                .is_err()
                {
                    self.unresolved += 1;
                    return;
                }
                let outcome = apply(
                    &sem,
                    &mut self.root,
                    Self::site(),
                    ctx,
                    op.clone(),
                    &At::root(),
                    Emit::new(
                        #[cfg(feature = "sink")]
                        path,
                        #[cfg(feature = "sink")]
                        sink,
                    ),
                );
                if outcome.is_err() {
                    self.unresolved += 1;
                }
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        self.root.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.root.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.sem.is_none() && self.root.is_default()
    }
}

/// A log's operation crosses replicas, so it internalises; the table it
/// carries is text and does not.
impl InternalizeOp for ModelLog {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    //! `ip16` and `ip17`, the two halves of criterion I-A9, plus the rules
    //! that make decision D2 hold: one table, arriving first, for the life of
    //! the log. `ip7`'s second half is here too, where an operation crosses a
    //! real replica boundary between two interners that disagree.

    use moirai_protocol::{
        broadcast::tcsb::Tcsb,
        log_id::LogId,
        replica::{IsReplica, Replica},
        state::log::IsLog,
    };
    use moirai_semantics::{ClassSlot, FeatureSlot};
    use serde_json::{Value, json};

    use super::{ModelLog, ModelOp};
    use crate::leaf::{LeafOp, Scalar};
    use crate::node::Refusal;
    use crate::op::InstanceOp;
    use crate::testing::{
        self, BT_DESCRIPTOR, bt, class_slot, deliver, feature_slot, field, install, mini,
        mini_descriptor, object, opened, ordered, sequence, text_of,
    };

    fn bt_descriptor() -> Value {
        serde_json::from_str(BT_DESCRIPTOR).expect("the fixture is JSON")
    }

    /// `Variant(Root, Field(feature, inner))`, the shape every operation on a
    /// behaviour tree starts with.
    fn on_root(feature: &str, inner: InstanceOp) -> InstanceOp {
        let sem = bt();
        let root = class_slot(&sem, "Root");
        InstanceOp::variant(
            root,
            InstanceOp::field(feature_slot(&sem, root, feature), inner),
        )
    }

    fn field_of(class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        let sem = bt();
        InstanceOp::field(feature_slot(&sem, class_slot(&sem, class), feature), inner)
    }

    fn class(name: &str) -> ClassSlot {
        class_slot(&bt(), name)
    }

    /// Put a behaviour tree in `Root.behaviortrees[0]` and give it an id.
    fn add_tree(id: char) -> InstanceOp {
        on_root(
            "behaviortrees",
            InstanceOp::insert(
                0,
                InstanceOp::variant(
                    class("BehaviorTree"),
                    field_of(
                        "BehaviorTree",
                        "ID",
                        InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: id }),
                    ),
                ),
            ),
        )
    }

    // ------------------------------------------------------ decision D2

    #[test]
    fn a_log_takes_one_install_and_refuses_a_second() {
        let descriptor = mini_descriptor();
        let (mut a, _b) = opened("m1", &descriptor);
        assert_eq!(
            a.state().header().map(|header| header.model_id.as_str()),
            Some("m1")
        );
        assert!(a.state().semantics().is_some());

        assert!(a.send(install("m2", &descriptor)).is_none());
        assert_eq!(
            a.state().refusal(&install("m2", &descriptor)),
            Err(Refusal::AlreadyInstalled)
        );
    }

    #[test]
    fn every_instance_operation_waits_for_the_table() {
        let log = ModelLog::default();
        let op = ModelOp::Instance(InstanceOp::variant(ClassSlot(0), InstanceOp::New));
        assert_eq!(log.refusal(&op), Err(Refusal::NoTable));
        assert!(!log.is_enabled(&op));
        assert!(log.is_default());

        // And the sentence says what to do about it.
        assert!(
            Refusal::NoTable
                .to_string()
                .contains("its first operation opens it")
        );
    }

    #[test]
    fn a_descriptor_that_will_not_parse_is_refused_locally_by_its_reason() {
        let log = ModelLog::default();
        let op = ModelOp::Install {
            model_id: "m1".to_string(),
            metamodel_id: "sha256:nothing".to_string(),
            descriptor: "{\"formatVersion\": 1}".to_string(),
        };
        match log.refusal(&op) {
            Err(Refusal::Descriptor(reason)) => {
                assert!(reason.contains("formatVersion"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        assert!(!log.is_enabled(&op));
    }

    #[test]
    fn the_checked_in_fixture_is_the_descriptor_arachne_emits() {
        let descriptor = bt_descriptor();
        assert_eq!(descriptor["formatVersion"], json!(2));
        assert_eq!(descriptor["package"], json!("behaviortree"));
        assert_eq!(descriptor["classes"].as_object().unwrap().len(), 21);
        let sem = bt();
        assert_eq!(sem.classes.len(), 21);
        assert_eq!(sem.roots.len(), 1);
        assert_eq!(&*sem.classes[sem.roots[0].index()].name, "Root");
        // `TreeNode` is abstract and its concrete closure is the eight
        // subclasses `02 Validation Plan` names.
        let tree_node = class_slot(&sem, "TreeNode");
        assert_eq!(sem.classes[tree_node.index()].concrete.len(), 8);
    }

    // ----------------------------------------------------------------- ip16

    /// Every way an operation can be malformed against `bt.metamodel.json`,
    /// with the sentence each one comes back with.
    fn malformed() -> Vec<(&'static str, InstanceOp, &'static str)> {
        vec![
            (
                "a feature the class does not declare",
                InstanceOp::variant(
                    class("Root"),
                    InstanceOp::field(FeatureSlot(99), InstanceOp::New),
                ),
                "`Root` declares no feature at slot 99",
            ),
            (
                "a class the containment cannot hold",
                on_root(
                    "behaviortrees",
                    InstanceOp::insert(
                        0,
                        InstanceOp::variant(class("Blackboard"), InstanceOp::New),
                    ),
                ),
                "holds no `Blackboard`",
            ),
            (
                "a leaf write into a containment",
                on_root(
                    "behaviortrees",
                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'x' }),
                ),
                "`Root.behaviortrees` is a sequence",
            ),
            (
                "a counter operation on a text attribute",
                on_root(
                    "behaviortrees",
                    InstanceOp::insert(
                        0,
                        InstanceOp::variant(
                            class("BehaviorTree"),
                            field_of(
                                "BehaviorTree",
                                "ID",
                                InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1))),
                            ),
                        ),
                    ),
                ),
                "`BehaviorTree.ID`",
            ),
            (
                "a class that is not a root, at the root",
                InstanceOp::variant(class("Blackboard"), InstanceOp::New),
                "`model.root` holds no `Blackboard`",
            ),
            (
                "an abstract class, which is in nobody's concrete closure",
                on_root(
                    "behaviortrees",
                    InstanceOp::insert(0, InstanceOp::variant(class("TreeNode"), InstanceOp::New)),
                ),
                "holds no `TreeNode`",
            ),
        ]
    }

    #[test]
    fn ip16_a_malformed_operation_is_refused_locally_with_a_reason_naming_the_feature() {
        let (mut a, mut b) = opened("m1", &bt_descriptor());

        // A model with something in it, so the refusals are not answered by
        // an empty tree.
        let event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();
        b.receive(event);
        let before = format!("{:?}", a.state().root());

        for (what, op, sentence) in malformed() {
            let op = ModelOp::Instance(op);
            assert!(a.send(op.clone()).is_none(), "{what} was accepted");

            let from_a = a.state().refusal(&op).unwrap_err();
            let from_b = b.state().refusal(&op).unwrap_err();
            assert_eq!(from_a, from_b, "{what}: two replicas, two verdicts");
            assert!(
                from_a.to_string().contains(sentence),
                "{what}: `{from_a}` does not name what it stopped at"
            );
        }

        assert_eq!(
            format!("{:?}", a.state().root()),
            before,
            "a refused operation changed the model"
        );
        assert_eq!(a.state().unresolved(), 0, "a local refusal is not a miss");
    }

    // ----------------------------------------------------------------- ip17

    #[test]
    fn ip17_a_remote_operation_with_no_table_is_counted_and_never_refused() {
        let (mut a, _b) = opened("m1", &bt_descriptor());
        let event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();

        // The same operation, arriving at a log that never saw the `Install`.
        // Causal delivery makes this impossible in a session; a peer that is
        // wrong must still not bring this one down.
        let mut fresh = ModelLog::default();
        deliver(&mut fresh, event.event().clone());
        assert_eq!(fresh.unresolved(), 1);
        assert!(fresh.is_default(), "nothing was applied");
    }

    #[test]
    fn ip17_a_remote_operation_this_table_cannot_route_is_counted_and_applies_nothing() {
        // Two logs, two different metamodels, one log id: the only way one
        // table meets another's operations, and what it has to survive.
        let (mut a, _b) = opened("m1", &bt_descriptor());
        let bt_event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();

        let mut other = ModelLog::default();
        let install_mini = install("m1", &mini_descriptor());
        let (mut c, _d) = opened("m1", &mini_descriptor());
        let install_event = c
            .send(ModelOp::Instance(InstanceOp::variant(
                class_slot(&mini(), "Root"),
                InstanceOp::field(
                    feature_slot(&mini(), class_slot(&mini(), "Root"), "title"),
                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'm' }),
                ),
            )))
            .unwrap();

        // Open `other` on the mini table by hand, then feed it both a valid
        // operation of its own and one written against `bt`.
        deliver(
            &mut other,
            moirai_protocol::event::Event::new(
                install_event.event().id().clone(),
                *install_event.event().lamport(),
                install_mini,
                install_event.event().version().clone(),
            ),
        );
        assert!(other.semantics().is_some());
        assert_eq!(other.unresolved(), 0);

        deliver(&mut other, install_event.event().clone());
        assert_eq!(other.unresolved(), 0, "its own operation routed");
        let after_its_own = format!("{:?}", other.root());

        deliver(&mut other, bt_event.event().clone());
        assert_eq!(other.unresolved(), 1, "the foreign operation was counted");
        assert_eq!(
            format!("{:?}", other.root()),
            after_its_own,
            "and it applied nothing"
        );
    }

    #[test]
    fn ip17_a_second_install_of_another_metamodel_is_counted_rather_than_applied() {
        let (a, _b) = opened("m1", &bt_descriptor());
        // `New` at the root names no class, so it is not an operation on an
        // object and the table says so before anything else does.
        let (c, _d) = opened("m1", &mini_descriptor());
        assert!(
            c.state()
                .refusal(&ModelOp::Instance(InstanceOp::New))
                .is_err()
        );

        // Take a well-formed `Install` of the other metamodel and deliver it
        // to a log that already holds one.
        let (mut e, _f) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
        let other_install = e.send(install("m1", &mini_descriptor())).unwrap();

        let mut log = a.state().clone();
        deliver(&mut log, other_install.event().clone());
        assert_eq!(log.unresolved(), 1);
        assert_eq!(
            log.header().map(|header| header.metamodel_id.clone()),
            a.state().header().map(|header| header.metamodel_id.clone()),
            "the table it already had is the table it kept"
        );
    }

    #[test]
    fn ip17_the_same_install_twice_is_not_a_miss() {
        let descriptor = mini_descriptor();
        let (a, _b) = opened("m1", &descriptor);
        let (c, _d) = opened("m1", &descriptor);
        let duplicate = c.state().clone();
        assert!(duplicate.semantics().is_some());

        // Two replicas opening one log at once is the only way this happens,
        // and both of them installed the same bytes.
        let (mut e, _f) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
        let event = e.send(install("m1", &descriptor)).unwrap();
        let mut log = a.state().clone();
        deliver(&mut log, event.event().clone());
        assert_eq!(log.unresolved(), 0);
    }

    // ------------------------------------------------------- ip7, in flight

    #[test]
    fn ip7_a_deep_operation_crosses_two_interners_that_disagree_about_their_members() {
        // The two replicas index their members in opposite orders, so every
        // event id that crosses between them is re-indexed on arrival. This is
        // the situation `InternalizeOp` exists for, and the one where a
        // recursion that skipped a level would land a list child on the wrong
        // object.
        let log_id = LogId::generate();
        let mut a: Replica<ModelLog, Tcsb<ModelOp>> =
            Replica::bootstrap_with_log_id("a".to_string(), &["a", "b"], log_id.clone());
        let mut b: Replica<ModelLog, Tcsb<ModelOp>> =
            Replica::bootstrap_with_log_id("b".to_string(), &["b", "a"], log_id);

        let event = a.send(install("m1", &bt_descriptor())).unwrap();
        b.receive(event);

        let sem = bt();
        let a1 = a.send(ModelOp::Instance(add_tree('a'))).unwrap();
        b.receive(a1);
        let b1 = b.send(ModelOp::Instance(add_tree('b'))).unwrap();
        a.receive(b1);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(replica.state().root());
            let trees = sequence(field(&sem, root, "behaviortrees").expect("minted"));
            let names: Vec<String> = ordered(trees)
                .iter()
                .map(|tree| text_of(&sem, object(tree), "ID"))
                .collect();
            assert_eq!(names, vec!["b".to_string(), "a".to_string()], "{who}");
        }
    }

    // ------------------------------------------- the real behaviour tree

    #[test]
    fn a_behaviour_tree_converges_under_the_descriptor_arachne_emits() {
        let sem = bt();
        let (mut a, mut b) = opened("m1", &bt_descriptor());

        let event = a.send(ModelOp::Instance(add_tree('t'))).unwrap();
        b.receive(event);

        // Each writer puts a different control node under the tree's `child`,
        // which is a single-valued containment of the abstract `TreeNode`.
        let put_child = |class_name: &str, id: char| {
            ModelOp::Instance(on_root(
                "behaviortrees",
                InstanceOp::at(
                    0,
                    InstanceOp::variant(
                        class("BehaviorTree"),
                        field_of(
                            "BehaviorTree",
                            "child",
                            InstanceOp::variant(
                                class(class_name),
                                field_of(
                                    "TreeNode",
                                    "ID",
                                    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: id }),
                                ),
                            ),
                        ),
                    ),
                ),
            ))
        };

        let from_a = a.send(put_child("Sequence", 's')).unwrap();
        let from_b = b.send(put_child("Fallback", 'f')).unwrap();
        a.receive(from_b);
        b.receive(from_a);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(replica.state().root());
            let trees = sequence(field(&sem, root, "behaviortrees").unwrap());
            let tree = object(ordered(trees)[0]);
            assert_eq!(text_of(&sem, tree, "ID"), "t", "{who}");
            let child = field(&sem, tree, "child").expect("both writers set it");
            let mut held: Vec<String> = crate::testing::objects(child)
                .iter()
                .map(|object| sem.classes[object.class().index()].name.to_string())
                .collect();
            held.sort();
            assert_eq!(held, vec!["Fallback", "Sequence"], "{who}");
            assert_eq!(replica.state().unresolved(), 0, "{who}");
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_log_and_its_table_survive_a_round_trip_through_serde() {
        // The half of decision D2 that a joiner by state transfer rests on:
        // no `Install` is replayed, so the table has to be in the bytes.
        let (mut a, _b) = opened("m1", &bt_descriptor());
        let _ = a.send(ModelOp::Instance(add_tree('t'))).unwrap();

        let bytes = serde_json::to_vec(a.state()).expect("a log serializes");
        let back: ModelLog = serde_json::from_slice(&bytes).expect("and comes back");

        assert_eq!(back.header(), a.state().header());
        assert_eq!(
            back.semantics().map(|sem| sem.digest.clone()),
            a.state().semantics().map(|sem| sem.digest.clone())
        );
        assert_eq!(
            format!("{:?}", back.root()),
            format!("{:?}", a.state().root())
        );

        let sem = bt();
        let root = object(back.root());
        let trees = sequence(field(&sem, root, "behaviortrees").unwrap());
        assert_eq!(text_of(&sem, object(ordered(trees)[0]), "ID"), "t");
    }

    #[test]
    fn the_fixture_module_builds_both_tables() {
        let _ = testing::bench();
        let _ = testing::bt();
    }
}
