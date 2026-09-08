//! The nodes: the three containers the interpreter owns, the object, and the
//! one constructor that mints them.
//!
//! # Why the containers are rewritten and the hard half is not
//!
//! A library container mints its children through `Default`:
//! `uw_map.rs:133`'s `entry(k).or_default()`, `option/mod.rs:97`'s
//! `get_or_insert_with(L::default)`, and `NestedListLog` through the
//! `UWMapLog<EventId, L>` it is built on. An interpreted child has no
//! `Default` to mint through — the rule is what decides what it is — so the
//! interpreter owns the mapping half of each container and mints every child
//! through [`Node::for_rule`].
//!
//! What it does *not* own is the ordering half. [`SeqNode`] holds
//! `EventGraph<List<EventId>>` verbatim, the same field `NestedListLog`
//! holds, and resolves a position exactly as `nested_list.rs:138-139` and
//! `:166-167` do: `ReadAt(event.version())`, the state the writer saw, and
//! never the current state. Resolving against the current state is the
//! divergence the copied code's own comment warns about, and it is invisible
//! until two writers touch one list at once.
//!
//! The removal rule is copied the same way: `uw_map.rs:150-152` routes a
//! `Remove` to `child.redundant_by_parent(version, true)` rather than
//! dropping the entry, which is update-wins — what is causally below the
//! removal goes and what is concurrent with it stays. [`SlotNode`] copies
//! `union.rs:180-200`: a second concrete class arriving on a set slot is
//! kept beside the first as a conflict rather than refused, because refusing
//! it is `is_enabled`'s local job and never `effect`'s.
//!
//! # Flat, and keyed by the visible slot
//!
//! An [`ObjectNode`] holds every feature its class can see, its own and its
//! inherited ones, in one map. There is no `_super` nesting: the generated
//! `SequenceLog` reaches `ID` through `control_node_super.tree_node_super.id`
//! and this one reaches it in one index. See [`crate::op`] for why the key is
//! the visible slot and not the declaring class's slot.

// A sequence keys its children by the id of the operation that inserted
// them, and an `EventId` carries a `Resolver` whose `FrozenVec` is interior
// mutability as far as the lint is concerned. It is not as far as the map is
// concerned: an id hashes on its origin's *name* and its sequence number
// (`event/id.rs:76-80`), neither of which the resolver can change. The
// library keys the same map the same way and silences the same lint per site
// (`nested_list.rs:74`, `uw_map.rs:236`).
#![allow(clippy::mutable_key_type)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use moirai_crdt::list::eg_walker::{List, ReadAt};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::query::Read,
    event::{Event, id::EventId, lamport::Lamport},
    state::{event_graph::EventGraph, log::IsLog},
};
use moirai_semantics::{
    ClassSlot, FeatureSlot, LeafRule, MergeRule, MetamodelSemantics, SetTie, Shape, TieBreak,
    UnsupportedReason,
};

#[cfg(feature = "sink")]
use moirai_protocol::state::{
    object_path::ObjectPath,
    sink::{Sink, SinkCollector},
};

use crate::leaf::{LeafLog, LeafMismatch};
use crate::op::{InstanceOp, OptOp, SeqOp};

/// Why an operation was not routed.
///
/// Every variant names the class or the feature it stopped at, because a
/// refusal a modeller cannot act on is a refusal that will be reported as a
/// bug in the editor (criterion I-A9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// No table is installed, so nothing can be routed yet.
    NoTable,
    /// A table is already installed, and a log has one for its whole life.
    AlreadyInstalled,
    /// The descriptor an `Install` carried is not one this crate can read.
    Descriptor(String),
    /// The path opened at a class the table does not list.
    UnknownClass {
        /// The slot as the operation spelled it.
        slot: u16,
    },
    /// The class does not declare, and does not inherit, a feature at this
    /// visible slot.
    UnknownFeature {
        /// The class the path had reached.
        class: Arc<str>,
        /// The slot as the operation spelled it.
        slot: u16,
        /// How many features that class can see.
        visible: usize,
    },
    /// The operation addresses the feature as something it is not: a
    /// sequence step on a single-valued feature, a leaf write into a
    /// containment.
    WrongShape {
        /// Declaring or reached class.
        class: Arc<str>,
        /// The feature.
        feature: Arc<str>,
        /// What the table says sits there.
        expected: &'static str,
        /// What the operation treated it as.
        got: &'static str,
    },
    /// The class offered is not one this containment may hold.
    ClassNotAllowed {
        /// The class the feature is declared on.
        class: Arc<str>,
        /// The feature.
        feature: Arc<str>,
        /// The class the operation offered.
        offered: Arc<str>,
        /// The classes the containment's target closes over.
        allowed: Vec<Arc<str>>,
    },
    /// A single-valued containment already holds another concrete class, and
    /// a *local* writer is told so rather than being allowed to open a
    /// conflict on purpose.
    VariantTaken {
        /// The class the feature is declared on.
        class: Arc<str>,
        /// The feature.
        feature: Arc<str>,
        /// What sits there.
        held: Arc<str>,
        /// What was offered.
        offered: Arc<str>,
    },
    /// A position outside the collection as the operation's own version saw
    /// it.
    OutOfBounds {
        /// The class the feature is declared on.
        class: Arc<str>,
        /// The feature.
        feature: Arc<str>,
        /// The position asked for.
        pos: usize,
        /// How many children there were.
        len: usize,
    },
    /// The leaf does not take an operation of that kind.
    Leaf {
        /// The class the feature is declared on.
        class: Arc<str>,
        /// The feature.
        feature: Arc<str>,
        /// Which kind, and which leaf.
        mismatch: LeafMismatch,
    },
    /// The feature carries a rule the interpreted path has no node for.
    Unsupported {
        /// The class the feature is declared on.
        class: Arc<str>,
        /// The feature.
        feature: Arc<str>,
        /// Which form.
        reason: UnsupportedReason,
    },
    /// `New` on an object that is not empty.
    NotNew {
        /// The class.
        class: Arc<str>,
    },
    /// The path reached an object and the operation is not one an object
    /// takes.
    NotAnObjectOp {
        /// The class the path had reached.
        class: Arc<str>,
        /// What the operation was.
        got: &'static str,
    },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refusal::NoTable => write!(
                f,
                "this log holds no metamodel yet: its first operation opens it and every other \
                 one waits for that"
            ),
            Refusal::AlreadyInstalled => write!(
                f,
                "this log already holds a metamodel, and holds one for its whole life"
            ),
            Refusal::Descriptor(reason) => {
                write!(f, "the descriptor cannot be read as a table: {reason}")
            }
            Refusal::UnknownClass { slot } => {
                write!(f, "no class sits at slot {slot} of this metamodel")
            }
            Refusal::UnknownFeature {
                class,
                slot,
                visible,
            } => write!(
                f,
                "`{class}` declares no feature at slot {slot}; it can see {visible}"
            ),
            Refusal::WrongShape {
                class,
                feature,
                expected,
                got,
            } => write!(
                f,
                "`{class}.{feature}` is {expected}, and the operation addresses it as {got}"
            ),
            Refusal::ClassNotAllowed {
                class,
                feature,
                offered,
                allowed,
            } => {
                let allowed: Vec<&str> = allowed.iter().map(AsRef::as_ref).collect();
                write!(
                    f,
                    "`{class}.{feature}` holds no `{offered}`; it holds one of [{}]",
                    allowed.join(", ")
                )
            }
            Refusal::VariantTaken {
                class,
                feature,
                held,
                offered,
            } => write!(
                f,
                "`{class}.{feature}` already holds a `{held}`, so a `{offered}` written here \
                 would open a conflict this replica can still avoid"
            ),
            Refusal::OutOfBounds {
                class,
                feature,
                pos,
                len,
            } => write!(
                f,
                "`{class}.{feature}` holds {len} children and the operation addresses {pos}"
            ),
            Refusal::Leaf {
                class,
                feature,
                mismatch,
            } => write!(f, "`{class}.{feature}`: {mismatch}"),
            Refusal::Unsupported {
                class,
                feature,
                reason,
            } => write!(
                f,
                "`{class}.{feature}` is `{}`, which the interpreted path does not merge \
                 (decision D6)",
                reason.as_str()
            ),
            Refusal::NotNew { class } => write!(
                f,
                "`{class}` is already there: `New` opens an object and does not reopen one"
            ),
            Refusal::NotAnObjectOp { class, got } => write!(
                f,
                "the path reached a `{class}`, and {got} is not an operation on an object"
            ),
        }
    }
}

impl std::error::Error for Refusal {}

/// How strictly an operation is held.
///
/// The two halves of criterion I-A9. [`Mode::Local`] is the structural check
/// a writer's own operation is refused by, state and all: a second concrete
/// class on a slot this replica has already set, a position past the end,
/// `New` on an object that is already there. [`Mode::Routing`] is what is
/// left when the operation has already happened somewhere else and the only
/// question is whether this table can route it at all — a feature its class
/// declares, a shape the operation matches, a class the containment may
/// hold, a leaf of the operation's own kind.
///
/// Holding a remote operation to [`Mode::Local`] is the failure the
/// implementation plan names: "a `SlotNode` that refuses a second variant on
/// delivery, which is `is_enabled`'s local job and not `effect`'s". It costs
/// exactly the retention the design keeps — two writers who set different
/// subtypes at once would each keep only their own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A local writer's operation.
    Local,
    /// A peer's.
    Routing,
}

impl Mode {
    const fn is_local(self) -> bool {
        matches!(self, Mode::Local)
    }
}

/// Where the walk is, in names, for the sentence a refusal is written in.
#[derive(Clone)]
pub(crate) struct At {
    /// The class the feature is declared on.
    pub class: Arc<str>,
    /// The feature.
    pub feature: Arc<str>,
}

impl At {
    /// The top of a model, which belongs to no feature of no class.
    pub(crate) fn root() -> Self {
        At {
            class: Arc::from("model"),
            feature: Arc::from("root"),
        }
    }
}

/// The scalar construction a leaf site is at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeafSite {
    /// One value at one leaf rule.
    Scalar(LeafRule),
    /// A whole set of scalars in one CRDT.
    Set(SetTie),
    /// A whole bag of scalars in one CRDT.
    Bag,
}

/// Which classes may sit in one containment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// The concrete closure of one declared class, which is what a
    /// containment's `target` names.
    Class(ClassSlot),
    /// The concrete closure of every class the descriptor names as a root,
    /// which is what may sit at the top of a model. A descriptor that names
    /// none has every instantiable class as a root, exactly as the phase 4
    /// `Schema` reads it.
    Roots,
}

impl Target {
    /// Whether an instance of this class may sit here.
    pub(crate) fn allows(self, sem: &MetamodelSemantics, class: ClassSlot) -> bool {
        match self {
            Target::Class(target) => sem
                .classes
                .get(target.index())
                .is_some_and(|target| target.concrete.contains(&class)),
            Target::Roots => sem.roots.iter().any(|root| {
                sem.classes
                    .get(root.index())
                    .is_some_and(|root| root.concrete.contains(&class))
            }),
        }
    }

    /// The classes that may, by slot.
    pub(crate) fn allowed_slots(self, sem: &MetamodelSemantics) -> Vec<ClassSlot> {
        let mut slots: Vec<ClassSlot> = match self {
            Target::Class(target) => sem
                .classes
                .get(target.index())
                .map(|target| target.concrete.to_vec())
                .unwrap_or_default(),
            Target::Roots => sem
                .roots
                .iter()
                .filter_map(|root| sem.classes.get(root.index()))
                .flat_map(|root| root.concrete.iter().copied())
                .collect(),
        };
        slots.sort_unstable();
        slots.dedup();
        slots
    }

    /// The classes that may, by name; the error path only.
    pub(crate) fn allowed(self, sem: &MetamodelSemantics) -> Vec<Arc<str>> {
        self.allowed_slots(sem)
            .into_iter()
            .filter_map(|slot| sem.classes.get(slot.index()))
            .map(|class| Arc::clone(&class.name))
            .collect()
    }
}

/// What sits at one point of the tree, once its collection is peeled off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Site {
    /// An object of one of the target's concrete classes.
    Object(Target),
    /// A leaf.
    Leaf(LeafSite),
}

/// A feature's rule split into its collection and what the collection holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shaped {
    /// One of these, always present.
    Bare(Site),
    /// Zero or one of these.
    Optional(Site),
    /// Many of these, ordered.
    Sequence(Site),
}

impl Shaped {
    /// The word a refusal sentence uses.
    pub(crate) const fn word(self) -> &'static str {
        match self {
            Shaped::Bare(Site::Object(_)) => "a single-valued containment",
            Shaped::Bare(Site::Leaf(LeafSite::Scalar(_))) => "a single-valued attribute",
            Shaped::Bare(Site::Leaf(LeafSite::Set(_))) => "a set",
            Shaped::Bare(Site::Leaf(LeafSite::Bag)) => "a bag",
            Shaped::Optional(_) => "optional",
            Shaped::Sequence(_) => "a sequence",
        }
    }
}

/// Split one feature's rule.
///
/// The one place the generator's thirteen outcomes become node shapes, and
/// the one place decision D6's refusals are turned into a value.
pub(crate) fn shaped(rule: &MergeRule) -> Result<Shaped, UnsupportedReason> {
    Ok(match *rule {
        MergeRule::Attribute { shape, leaf } => match shape.effective() {
            Shape::Single => Shaped::Bare(Site::Leaf(LeafSite::Scalar(leaf))),
            Shape::Optional => Shaped::Optional(Site::Leaf(LeafSite::Scalar(leaf))),
            Shape::Sequence => Shaped::Sequence(Site::Leaf(LeafSite::Scalar(leaf))),
            Shape::Set { tie } => Shaped::Bare(Site::Leaf(LeafSite::Set(tie))),
            Shape::Bag => Shaped::Bare(Site::Leaf(LeafSite::Bag)),
            // `Shape::effective` degrades the one shape the generator cannot
            // compile, so nothing reaches here.
            Shape::OrderedSet => unreachable!("`effective` degrades an ordered set to a sequence"),
        },
        MergeRule::Containment { shape, target } => match shape.effective() {
            Shape::Optional => Shaped::Optional(Site::Object(Target::Class(target))),
            Shape::Sequence => Shaped::Sequence(Site::Object(Target::Class(target))),
            // `containment.rs:172-196` compiles a multi-valued containment as
            // a `NestedListLog` whatever its facets say, so the set and bag
            // shapes are not reachable for one; a single is a bare slot.
            _ => Shaped::Bare(Site::Object(Target::Class(target))),
        },
        // Design §8: a non-containment reference is carried as a string. One
        // of them is a register, so concurrent retargetings stay visible;
        // many of them are an add-wins set of strings.
        MergeRule::Reference { many: false, .. } => {
            Shaped::Bare(Site::Leaf(LeafSite::Scalar(LeafRule::Register {
                tie: TieBreak::MultiValue,
            })))
        }
        MergeRule::Reference { many: true, .. } => {
            Shaped::Bare(Site::Leaf(LeafSite::Set(SetTie::AddWins)))
        }
        MergeRule::Unsupported { reason } => return Err(reason),
    })
}

/// Resolve one visible slot of one class to the rule behind it.
///
/// Three indexes and no name compared: the class, its `visible` entry, and
/// the declaring class's `declared` entry that entry points at.
pub(crate) fn visible(
    sem: &MetamodelSemantics,
    class: ClassSlot,
    slot: FeatureSlot,
) -> Option<(&Arc<str>, &MergeRule)> {
    let holder = sem.classes.get(class.index())?;
    let (name, owner, declared) = holder.visible.get(slot.index())?;
    Some((name, sem.rule(*owner, *declared)?))
}

/// One node of the model tree.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Node {
    /// Nothing has been minted here yet; the state of a log with no table.
    Unbound,
    /// A containment: zero, one or — after a concurrent disagreement —
    /// several objects.
    Slot(SlotNode),
    /// An ordered collection.
    Seq(SeqNode),
    /// Zero or one of whatever the feature holds.
    Opt(OptNode),
    /// A leaf.
    Leaf(LeafLog),
}

impl Default for Node {
    /// [`Node::Unbound`], which is not a child: a child is minted by
    /// [`Node::for_rule`] and never by this. The impl exists because
    /// `ModelLog` is `Default` before its table arrives, and for no other
    /// reason.
    fn default() -> Self {
        Node::Unbound
    }
}

/// An object: its class, and every feature its class can see.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ObjectNode {
    pub(crate) class: ClassSlot,
    /// Keyed by the *visible* slot; minted on first write, from the rule.
    pub(crate) fields: BTreeMap<FeatureSlot, Node>,
}

/// A containment, as `union!`'s three states.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SlotNode {
    /// Nothing here.
    #[default]
    Unset,
    /// One object.
    Value(Box<ObjectNode>),
    /// Two writers put different concrete classes here at once, and both are
    /// kept: `union.rs:180-200`'s retention, which the design keeps.
    Conflicts(Vec<ObjectNode>),
}

/// An ordered collection: `NestedListLog`'s ordering half verbatim, and a
/// mapping half that mints by rule.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(serialize = "", deserialize = ""))
)]
pub struct SeqNode {
    pub(crate) positions: EventGraph<List<EventId>>,
    #[cfg_attr(feature = "serde", serde(with = "children_serde"))]
    pub(crate) children: BTreeMap<EventId, Node>,
}

/// A sequence's children as a list of pairs rather than as a JSON object.
///
/// The key is an [`EventId`], which is a struct and not a string, and a JSON
/// object's keys are strings. The state transfer this log has to survive is
/// JSON (`state_transfer.rs`), so the map travels as the pairs it is —
/// ordered by key, which keeps two replicas' bytes identical for one state.
#[cfg(feature = "serde")]
mod children_serde {
    use super::{BTreeMap, EventId, Node};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        children: &BTreeMap<EventId, Node>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        children.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<EventId, Node>, D::Error> {
        Ok(Vec::<(EventId, Node)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

/// Zero or one child.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OptNode {
    pub(crate) child: Option<Box<Node>>,
}

/// The event, taken apart once at the top of the walk.
///
/// The library containers clone the whole event at every level they descend
/// (`nested_list.rs`'s `event.clone()` three times over). A path here is as
/// deep as the model is, so the event is destructured once and rebuilt only
/// where a real `IsLog` needs one: at a leaf, and at a sequence's ordering
/// graph.
#[derive(Clone, Copy)]
pub(crate) struct Ctx<'a> {
    pub id: &'a EventId,
    pub lamport: &'a Lamport,
    pub version: &'a Version,
}

impl Ctx<'_> {
    /// The event this operation would be, at this level.
    pub(crate) fn event<O>(&self, op: O) -> Event<O> {
        Event::new(self.id.clone(), *self.lamport, op, self.version.clone())
    }
}

impl ObjectNode {
    /// An empty instance of one class.
    pub(crate) fn new(class: ClassSlot) -> Self {
        ObjectNode {
            class,
            fields: BTreeMap::new(),
        }
    }

    /// The class this is an instance of.
    pub fn class(&self) -> ClassSlot {
        self.class
    }

    /// Its features, by visible slot.
    pub fn fields(&self) -> &BTreeMap<FeatureSlot, Node> {
        &self.fields
    }
}

impl SlotNode {
    /// The objects here, none, one or several.
    pub fn objects(&self) -> Vec<&ObjectNode> {
        match self {
            SlotNode::Unset => Vec::new(),
            SlotNode::Value(object) => vec![object.as_ref()],
            SlotNode::Conflicts(objects) => objects.iter().collect(),
        }
    }
}

impl SeqNode {
    /// The children in read order.
    pub fn order(&self) -> Vec<EventId> {
        self.positions.execute_query_read()
    }

    /// The children, by the id of the operation that inserted them.
    pub fn children(&self) -> &BTreeMap<EventId, Node> {
        &self.children
    }
}

impl OptNode {
    /// What is here, if anything.
    pub fn child(&self) -> Option<&Node> {
        self.child.as_deref()
    }
}

/// `positions.eval(Read::new())` without importing `EvalNested` everywhere.
trait ReadPositions {
    fn execute_query_read(&self) -> Vec<EventId>;
    fn execute_query_read_at(&self, version: &Version) -> Vec<EventId>;
}

impl ReadPositions for EventGraph<List<EventId>> {
    fn execute_query_read(&self) -> Vec<EventId> {
        self.eval(Read::new())
    }

    /// The order the writer of an operation saw, which is the only reading of
    /// a position two replicas can agree on (`nested_list.rs:138`).
    fn execute_query_read_at(&self, version: &Version) -> Vec<EventId> {
        self.eval(ReadAt::new(version))
    }
}

impl Node {
    /// The only constructor.
    ///
    /// The table is not a parameter: a [`MergeRule`] already names the whole
    /// construction, which is the point of `moirai-semantics` carrying the
    /// rule rather than the facets. What the table is needed for — the
    /// concrete closure of a containment, the name of a feature, the literals
    /// of an enum — is needed on the walk, where it is a borrow, and not at
    /// mint time.
    pub fn for_rule(rule: &MergeRule) -> Result<Node, UnsupportedReason> {
        Ok(Node::for_shaped(shaped(rule)?))
    }

    pub(crate) fn for_shaped(shaped: Shaped) -> Node {
        match shaped {
            Shaped::Bare(site) => Node::for_site(site),
            Shaped::Optional(_) => Node::Opt(OptNode::default()),
            Shaped::Sequence(_) => Node::Seq(SeqNode::default()),
        }
    }

    pub(crate) fn for_site(site: Site) -> Node {
        match site {
            Site::Object(_) => Node::Slot(SlotNode::Unset),
            Site::Leaf(LeafSite::Scalar(rule)) => Node::Leaf(LeafLog::for_rule(rule)),
            Site::Leaf(LeafSite::Set(tie)) => Node::Leaf(LeafLog::for_set(tie)),
            Site::Leaf(LeafSite::Bag) => Node::Leaf(LeafLog::for_bag()),
        }
    }

    /// Hand a stable version to every node below this one.
    ///
    /// The recursion that `union!` shipped without for months (moirai
    /// `f36134a`): a container that does not pass a stable version down
    /// severs the only path an operation has out of a PO-Log's unstable half,
    /// and a container at the root of a model severs it for the whole model.
    /// `ip11` is the test that this one does not.
    pub fn stabilize(&mut self, version: &Version) {
        match self {
            Node::Unbound => {}
            Node::Slot(slot) => slot.for_each_mut(|object| object.stabilize(version)),
            Node::Seq(seq) => {
                for child in seq.children.values_mut() {
                    child.stabilize(version);
                }
                seq.positions.stabilize(version);
            }
            Node::Opt(opt) => {
                if let Some(child) = opt.child.as_mut() {
                    child.stabilize(version);
                }
            }
            Node::Leaf(leaf) => leaf.stabilize(version),
        }
    }

    /// A parent removed this subtree: update-wins, all the way down.
    pub fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match self {
            Node::Unbound => {}
            Node::Slot(slot) => {
                slot.for_each_mut(|object| object.redundant_by_parent(version, conservative));
            }
            Node::Seq(seq) => {
                for child in seq.children.values_mut() {
                    child.redundant_by_parent(version, conservative);
                }
                seq.positions.redundant_by_parent(version, conservative);
            }
            Node::Opt(opt) => {
                if let Some(child) = opt.child.as_mut() {
                    child.redundant_by_parent(version, conservative);
                }
            }
            Node::Leaf(leaf) => leaf.redundant_by_parent(version, conservative),
        }
    }

    /// Whether nothing below this node holds anything.
    pub fn is_default(&self) -> bool {
        match self {
            Node::Unbound => true,
            Node::Slot(SlotNode::Unset) => true,
            Node::Slot(SlotNode::Value(object)) => object.is_default(),
            Node::Slot(SlotNode::Conflicts(objects)) => objects.iter().all(ObjectNode::is_default),
            Node::Seq(seq) => {
                seq.positions.is_default() && seq.children.values().all(Node::is_default)
            }
            Node::Opt(opt) => opt.child.as_ref().is_none_or(|child| child.is_default()),
            Node::Leaf(leaf) => leaf.is_default(),
        }
    }

    /// How many operations the leaves below this node still hold unstably.
    ///
    /// Leaves only: a sequence's ordering graph is an `EventGraph`, whose
    /// `stabilize` is a no-op by construction, so counting it would hide the
    /// fall `ip11` is looking for behind a number that never moves.
    #[cfg(feature = "test_utils")]
    pub fn polog_len(&self) -> usize {
        match self {
            Node::Unbound => 0,
            Node::Slot(slot) => slot.objects().iter().map(|object| object.polog_len()).sum(),
            Node::Seq(seq) => seq.children.values().map(Node::polog_len).sum(),
            Node::Opt(opt) => opt.child.as_ref().map_or(0, |child| child.polog_len()),
            Node::Leaf(leaf) => leaf.polog_len(),
        }
    }
}

impl SlotNode {
    fn for_each_mut(&mut self, mut f: impl FnMut(&mut ObjectNode)) {
        match self {
            SlotNode::Unset => {}
            SlotNode::Value(object) => f(object),
            SlotNode::Conflicts(objects) => objects.iter_mut().for_each(f),
        }
    }

    /// The object of this class here, if one is.
    fn find_mut(&mut self, class: ClassSlot) -> Option<&mut ObjectNode> {
        match self {
            SlotNode::Unset => None,
            SlotNode::Value(object) => (object.class == class).then_some(object.as_mut()),
            SlotNode::Conflicts(objects) => objects.iter_mut().find(|object| object.class == class),
        }
    }

    fn find(&self, class: ClassSlot) -> Option<&ObjectNode> {
        match self {
            SlotNode::Unset => None,
            SlotNode::Value(object) => (object.class == class).then_some(object.as_ref()),
            SlotNode::Conflicts(objects) => objects.iter().find(|object| object.class == class),
        }
    }
}

impl ObjectNode {
    fn stabilize(&mut self, version: &Version) {
        for field in self.fields.values_mut() {
            field.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for field in self.fields.values_mut() {
            field.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.fields.values().all(Node::is_default)
    }

    #[cfg(feature = "test_utils")]
    fn polog_len(&self) -> usize {
        self.fields.values().map(Node::polog_len).sum()
    }
}

/// What an operation is, for a refusal sentence.
pub(crate) const fn word(op: &InstanceOp) -> &'static str {
    match op {
        InstanceOp::Field(..) => "a feature step",
        InstanceOp::Variant(..) => "a class step",
        InstanceOp::Seq(_) => "a sequence step",
        InstanceOp::Opt(_) => "an optional step",
        InstanceOp::New => "`New`",
        InstanceOp::Leaf(_) => "a leaf write",
    }
}

/// The sink half of a descent, and nothing at all when the feature is off.
///
/// Decision D4: a feature and a class descend through
/// `PathSegment::MapEntry(String)`, which takes an owned string, rather than
/// `moirai-protocol` gaining an `Arc<str>` segment this phase would have to
/// justify. The dashboard renders a feature as `/name[m]` instead of
/// `/name[f]` and nothing else changes.
pub(crate) struct Emit<'a> {
    #[cfg(feature = "sink")]
    path: ObjectPath,
    #[cfg(feature = "sink")]
    sink: &'a mut SinkCollector,
    #[cfg(not(feature = "sink"))]
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Emit<'a> {
    /// Open a descent at the root of one model.
    pub(crate) fn new(
        #[cfg(feature = "sink")] path: ObjectPath,
        #[cfg(feature = "sink")] sink: &'a mut SinkCollector,
    ) -> Self {
        Emit {
            #[cfg(feature = "sink")]
            path,
            #[cfg(feature = "sink")]
            sink,
            #[cfg(not(feature = "sink"))]
            _marker: std::marker::PhantomData,
        }
    }

    /// Step into a feature.
    pub(crate) fn field(&mut self, name: &str) -> Emit<'_> {
        self.entry(name)
    }

    /// Step into the concrete class sitting in a containment.
    pub(crate) fn variant(&mut self, name: &str) -> Emit<'_> {
        self.entry(name)
    }

    #[allow(unused_variables)]
    fn entry(&mut self, name: &str) -> Emit<'_> {
        Emit {
            #[cfg(feature = "sink")]
            path: self.path.clone().map_entry(name.to_string()),
            #[cfg(feature = "sink")]
            sink: self.sink,
            #[cfg(not(feature = "sink"))]
            _marker: std::marker::PhantomData,
        }
    }

    /// Step into one child of a sequence, by the id of the operation that
    /// inserted it.
    #[allow(unused_variables)]
    pub(crate) fn list_element(&mut self, id: EventId) -> Emit<'_> {
        Emit {
            #[cfg(feature = "sink")]
            path: self.path.clone().list_element(id),
            #[cfg(feature = "sink")]
            sink: self.sink,
            #[cfg(not(feature = "sink"))]
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn create(&mut self) {
        #[cfg(feature = "sink")]
        self.sink.collect(Sink::create(self.path.clone()));
    }

    pub(crate) fn update(&mut self) {
        #[cfg(feature = "sink")]
        self.sink.collect(Sink::update(self.path.clone()));
    }

    pub(crate) fn delete(&mut self) {
        #[cfg(feature = "sink")]
        self.sink.collect(Sink::delete(self.path.clone()));
    }
}

/// The ordering half of a sequence takes its own event.
///
/// `EventGraph::effect` ignores the path, the collector and the ownership
/// outright (`event_graph.rs:66-77`), so they are filled in here rather than
/// threaded down a recursion that would carry them for nobody.
fn positions_effect(positions: &mut EventGraph<List<EventId>>, event: Event<List<EventId>>) {
    positions.effect(
        event,
        #[cfg(feature = "sink")]
        ObjectPath::new("positions"),
        #[cfg(feature = "sink")]
        &mut SinkCollector::new(),
        #[cfg(feature = "sink")]
        moirai_protocol::state::sink::SinkOwnership::Delegated,
    );
}

/// Would this operation route, and what would stop it?
///
/// Pure: the table and the current state are read and nothing is written, so
/// [`crate::log::ModelLog::effect`] can ask before it applies and keep its
/// promise that a remote operation it cannot route changes nothing.
///
/// `node` is `None` where the child has not been minted yet, which is the
/// same thing `record!` does when it asks `L::default().is_enabled(op)` of a
/// field nobody has written.
pub(crate) fn check(
    sem: &MetamodelSemantics,
    node: Option<&Node>,
    shape: Shaped,
    op: &InstanceOp,
    at: &At,
    mode: Mode,
) -> Result<(), Refusal> {
    match shape {
        Shaped::Sequence(site) => {
            let InstanceOp::Seq(seq_op) = op else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a sequence",
                    got: word(op),
                });
            };
            let seq = match node {
                Some(Node::Seq(seq)) => Some(seq),
                _ => None,
            };
            let order = seq.map(SeqNode::order).unwrap_or_default();
            // A position means what it meant in the state its writer saw, and
            // only a local writer's state is this one. A peer's position is
            // resolved against its own version when it is applied, and out of
            // bounds *there* is what `apply` answers.
            if mode.is_local() {
                let pos = seq_op.pos();
                let limit = match seq_op {
                    SeqOp::Insert { .. } => order.len(),
                    _ => order.len().saturating_sub(1),
                };
                if pos > limit || (order.is_empty() && !matches!(seq_op, SeqOp::Insert { .. })) {
                    return Err(Refusal::OutOfBounds {
                        class: at.class.clone(),
                        feature: at.feature.clone(),
                        pos,
                        len: order.len(),
                    });
                }
            }
            match seq_op {
                SeqOp::Insert { op, .. } => check(sem, None, Shaped::Bare(site), op, at, mode),
                SeqOp::Update { pos, op } => {
                    let child = order
                        .get(*pos)
                        .and_then(|target| seq.and_then(|seq| seq.children.get(target)));
                    check(sem, child, Shaped::Bare(site), op, at, mode)
                }
                SeqOp::Delete { .. } => Ok(()),
            }
        }
        Shaped::Optional(site) => {
            let InstanceOp::Opt(opt_op) = op else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "optional",
                    got: word(op),
                });
            };
            match opt_op {
                OptOp::Set(inner) => {
                    let child = match node {
                        Some(Node::Opt(opt)) => opt.child(),
                        _ => None,
                    };
                    check(sem, child, Shaped::Bare(site), inner, at, mode)
                }
                OptOp::Unset => Ok(()),
            }
        }
        Shaped::Bare(Site::Leaf(leaf_site)) => {
            let InstanceOp::Leaf(leaf_op) = op else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: Shaped::Bare(Site::Leaf(leaf_site)).word(),
                    got: word(op),
                });
            };
            let minted;
            let leaf = match node {
                Some(Node::Leaf(leaf)) => leaf,
                _ => {
                    minted = Node::for_site(Site::Leaf(leaf_site));
                    match &minted {
                        Node::Leaf(leaf) => leaf,
                        _ => unreachable!("a leaf site mints a leaf"),
                    }
                }
            };
            let outcome = if mode.is_local() {
                leaf.is_enabled(leaf_op)
            } else {
                leaf.accepts(leaf_op)
            };
            outcome.map_err(|mismatch| Refusal::Leaf {
                class: at.class.clone(),
                feature: at.feature.clone(),
                mismatch,
            })
        }
        Shaped::Bare(Site::Object(target)) => {
            let InstanceOp::Variant(class, inner) = op else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a containment",
                    got: word(op),
                });
            };
            if !target.allows(sem, *class) {
                let offered = sem
                    .classes
                    .get(class.index())
                    .map_or_else(|| Arc::from("?"), |class| Arc::clone(&class.name));
                return Err(Refusal::ClassNotAllowed {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    offered,
                    allowed: target.allowed(sem),
                });
            }
            let slot = match node {
                Some(Node::Slot(slot)) => Some(slot),
                _ => None,
            };
            // `union.rs:130-145`: a *local* writer is told that this slot is
            // taken, so a conflict is something concurrency opens and never
            // something one replica opens on its own. A peer's operation is
            // not held to it: refusing it here is what would lose the
            // retention `ip9` asserts.
            if let Some(slot) = slot.filter(|_| mode.is_local()) {
                let held = match slot {
                    SlotNode::Unset => None,
                    SlotNode::Value(object) => Some(object.class),
                    SlotNode::Conflicts(objects) => objects.first().map(|object| object.class),
                };
                if let Some(held) = held
                    && slot.find(*class).is_none()
                {
                    return Err(Refusal::VariantTaken {
                        class: at.class.clone(),
                        feature: at.feature.clone(),
                        held: sem
                            .classes
                            .get(held.index())
                            .map_or_else(|| Arc::from("?"), |class| Arc::clone(&class.name)),
                        offered: sem
                            .classes
                            .get(class.index())
                            .map_or_else(|| Arc::from("?"), |class| Arc::clone(&class.name)),
                    });
                }
            }
            check_object(
                sem,
                slot.and_then(|slot| slot.find(*class)),
                *class,
                inner,
                mode,
            )
        }
    }
}

/// The object half of [`check`].
pub(crate) fn check_object(
    sem: &MetamodelSemantics,
    node: Option<&ObjectNode>,
    class: ClassSlot,
    op: &InstanceOp,
    mode: Mode,
) -> Result<(), Refusal> {
    let holder = sem
        .classes
        .get(class.index())
        .ok_or(Refusal::UnknownClass { slot: class.0 })?;
    match op {
        // `record!`'s own rule: `New` is enabled on an empty object only —
        // for a local writer. A peer's `New` on an object that is already
        // there is a `New` that raced another write, and applying it is a
        // no-op.
        InstanceOp::New => {
            if !mode.is_local() || node.is_none_or(ObjectNode::is_default) {
                Ok(())
            } else {
                Err(Refusal::NotNew {
                    class: Arc::clone(&holder.name),
                })
            }
        }
        InstanceOp::Field(slot, inner) => {
            let (name, rule) =
                visible(sem, class, *slot).ok_or_else(|| Refusal::UnknownFeature {
                    class: Arc::clone(&holder.name),
                    slot: slot.0,
                    visible: holder.visible.len(),
                })?;
            let at = At {
                class: Arc::clone(&holder.name),
                feature: Arc::clone(name),
            };
            let shape = shaped(rule).map_err(|reason| Refusal::Unsupported {
                class: Arc::clone(&at.class),
                feature: Arc::clone(&at.feature),
                reason,
            })?;
            check(
                sem,
                node.and_then(|object| object.fields.get(slot)),
                shape,
                inner,
                &at,
                mode,
            )
        }
        other => Err(Refusal::NotAnObjectOp {
            class: Arc::clone(&holder.name),
            got: word(other),
        }),
    }
}

/// Apply one operation to a node that was minted from `shape`.
///
/// Every failure here is one [`check`] would have caught, and returning it
/// rather than panicking is what keeps criterion I-A9's second half true: a
/// remote operation is never refused and never brings the process down; it is
/// counted.
pub(crate) fn apply(
    sem: &MetamodelSemantics,
    node: &mut Node,
    shape: Shaped,
    ctx: Ctx<'_>,
    op: InstanceOp,
    at: &At,
    mut emit: Emit<'_>,
) -> Result<(), Refusal> {
    match shape {
        Shaped::Sequence(site) => {
            let (InstanceOp::Seq(seq_op), Node::Seq(seq)) = (op, node) else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a sequence",
                    got: "something else",
                });
            };
            match seq_op {
                SeqOp::Insert { pos, op } => {
                    // `nested_list.rs:104-112`: the new child is keyed by the
                    // id of the operation that inserted it, and that id is
                    // what the ordering graph carries as its content.
                    positions_effect(
                        &mut seq.positions,
                        ctx.event(List::Insert {
                            pos,
                            content: ctx.id.clone(),
                        }),
                    );
                    let mut emit = emit.list_element(ctx.id.clone());
                    emit.create();
                    let child = seq
                        .children
                        .entry(ctx.id.clone())
                        .or_insert_with(|| Node::for_site(site));
                    apply(sem, child, Shaped::Bare(site), ctx, *op, at, emit)
                }
                SeqOp::Update { pos, op } => {
                    let target = target_at(seq, ctx, pos, at)?;
                    positions_effect(&mut seq.positions, ctx.event(List::Update { pos }));
                    let mut emit = emit.list_element(target.clone());
                    emit.update();
                    let child = seq
                        .children
                        .entry(target)
                        .or_insert_with(|| Node::for_site(site));
                    apply(sem, child, Shaped::Bare(site), ctx, *op, at, emit)
                }
                SeqOp::Delete { pos } => {
                    let target = target_at(seq, ctx, pos, at)?;
                    emit.list_element(target.clone()).delete();
                    positions_effect(&mut seq.positions, ctx.event(List::Delete { pos }));
                    // `uw_map.rs:150-152`: the child is reset, not dropped,
                    // so a concurrent update to it survives its removal.
                    if let Some(child) = seq.children.get_mut(&target) {
                        child.redundant_by_parent(ctx.version, true);
                    }
                    Ok(())
                }
            }
        }
        Shaped::Optional(site) => {
            let (InstanceOp::Opt(opt_op), Node::Opt(opt)) = (op, node) else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "optional",
                    got: "something else",
                });
            };
            match opt_op {
                OptOp::Set(inner) => {
                    if opt.child.is_some() {
                        emit.update();
                    } else {
                        emit.create();
                    }
                    let child = opt
                        .child
                        .get_or_insert_with(|| Box::new(Node::for_site(site)));
                    let outcome = apply(sem, child, Shaped::Bare(site), ctx, *inner, at, emit);
                    // `option/mod.rs:110`: a write that left the child empty
                    // leaves the optional unset, which is what makes an unset
                    // optional an absent key rather than an empty object.
                    if opt.child.as_ref().is_some_and(|child| child.is_default()) {
                        opt.child = None;
                    }
                    outcome
                }
                OptOp::Unset => {
                    emit.delete();
                    if let Some(child) = opt.child.as_mut() {
                        child.redundant_by_parent(ctx.version, true);
                        if child.is_default() {
                            opt.child = None;
                        }
                    }
                    Ok(())
                }
            }
        }
        Shaped::Bare(Site::Leaf(_)) => {
            let (InstanceOp::Leaf(leaf_op), Node::Leaf(leaf)) = (op, node) else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a leaf",
                    got: "something else",
                });
            };
            emit.update();
            leaf.effect(ctx.event(leaf_op))
                .map_err(|mismatch| Refusal::Leaf {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    mismatch,
                })
        }
        Shaped::Bare(Site::Object(_)) => {
            let (InstanceOp::Variant(class, inner), Node::Slot(slot)) = (op, node) else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a containment",
                    got: "something else",
                });
            };
            let name = sem
                .classes
                .get(class.index())
                .map_or_else(|| Arc::from("?"), |class| Arc::clone(&class.name));
            let emit = emit.variant(&name);
            // `union.rs:180-200`: a class this slot does not hold yet is
            // added beside what it holds, never refused.
            ensure_variant(slot, class);
            let object = slot
                .find_mut(class)
                .expect("`ensure_variant` put one of this class here");
            apply_object(sem, object, ctx, *inner, emit)
        }
    }
}

/// The object half of [`apply`].
fn apply_object(
    sem: &MetamodelSemantics,
    object: &mut ObjectNode,
    ctx: Ctx<'_>,
    op: InstanceOp,
    mut emit: Emit<'_>,
) -> Result<(), Refusal> {
    let holder = sem
        .classes
        .get(object.class.index())
        .ok_or(Refusal::UnknownClass {
            slot: object.class.0,
        })?;
    match op {
        // `record!`'s `New` writes nothing and reports the object's arrival,
        // which is all an interpreted `New` has to do either: the fields it
        // would have laid out are minted by the first write to each.
        InstanceOp::New => {
            emit.create();
            Ok(())
        }
        InstanceOp::Field(slot, inner) => {
            let (name, rule) =
                visible(sem, object.class, slot).ok_or_else(|| Refusal::UnknownFeature {
                    class: Arc::clone(&holder.name),
                    slot: slot.0,
                    visible: holder.visible.len(),
                })?;
            let at = At {
                class: Arc::clone(&holder.name),
                feature: Arc::clone(name),
            };
            let shape = shaped(rule).map_err(|reason| Refusal::Unsupported {
                class: Arc::clone(&at.class),
                feature: Arc::clone(&at.feature),
                reason,
            })?;
            let mut emit = emit.field(name);
            emit.update();
            let child = object
                .fields
                .entry(slot)
                .or_insert_with(|| Node::for_shaped(shape));
            apply(sem, child, shape, ctx, *inner, &at, emit)
        }
        other => Err(Refusal::NotAnObjectOp {
            class: Arc::clone(&holder.name),
            got: word(&other),
        }),
    }
}

/// The child this position named, in the state the operation's own writer
/// saw.
fn target_at(seq: &SeqNode, ctx: Ctx<'_>, pos: usize, at: &At) -> Result<EventId, Refusal> {
    let order = seq.positions.execute_query_read_at(ctx.version);
    order.get(pos).cloned().ok_or_else(|| Refusal::OutOfBounds {
        class: at.class.clone(),
        feature: at.feature.clone(),
        pos,
        len: order.len(),
    })
}

/// Put an object of this class in the slot if there is not one already,
/// keeping whatever else is there.
fn ensure_variant(slot: &mut SlotNode, class: ClassSlot) {
    match slot {
        SlotNode::Unset => {
            *slot = SlotNode::Value(Box::new(ObjectNode::new(class)));
        }
        SlotNode::Value(object) => {
            if object.class != class {
                let SlotNode::Value(existing) = std::mem::replace(slot, SlotNode::Unset) else {
                    unreachable!("matched a moment ago");
                };
                *slot = SlotNode::Conflicts(vec![*existing, ObjectNode::new(class)]);
            }
        }
        SlotNode::Conflicts(objects) => {
            if !objects.iter().any(|object| object.class == class) {
                objects.push(ObjectNode::new(class));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The container claims: `ip8`, `ip9`, `ip10` and `ip11`.
    //!
    //! They are driven through [`crate::testing::Harness`], which is the
    //! container layer under a real pair of replicas with no installation
    //! ceremony in the way. The log-level claims — the table arriving, a
    //! remote operation that will not route, the reasons a refusal is written
    //! in — are in [`crate::log`], over the real `ModelLog`.

    use moirai_protocol::replica::IsReplica;
    use moirai_semantics::{
        FeatureSlot, LeafRule, MergeRule, MetamodelSemantics, NumKind, Shape, UnsupportedReason,
    };

    use super::{Mode, Node, Refusal, Site, Target, check, shaped};
    use crate::leaf::{LeafOp, Scalar};
    use crate::op::InstanceOp;
    use crate::testing::{
        self, Harness, bench, class_slot, feature_slot, field, mini, object, objects, ordered,
        sequence, text_of, twins,
    };

    /// `Variant(class, inner)` at the root, which every operation starts with.
    fn at_root(sem: &MetamodelSemantics, class: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::variant(class_slot(sem, class), inner)
    }

    /// `Field(feature, inner)` on a class named by name.
    fn on(sem: &MetamodelSemantics, class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::field(feature_slot(sem, class_slot(sem, class), feature), inner)
    }

    /// One character appended to a text leaf.
    fn append(ch: char, pos: usize) -> InstanceOp {
        InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })
    }

    // ------------------------------------------------------------------ ip8

    #[test]
    fn ip8_two_writers_insert_into_one_sequence_and_both_children_survive() {
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");

        // Each writer inserts a child of its own class at position 0 and names
        // it, seeing only its own insert while it does so.
        let insert = |class: &str| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "children",
                    InstanceOp::insert(
                        0,
                        InstanceOp::variant(class_slot(&sem, class), InstanceOp::New),
                    ),
                ),
            )
        };
        let name = |class: &str, ch: char| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "children",
                    InstanceOp::at(
                        0,
                        InstanceOp::variant(
                            class_slot(&sem, class),
                            on(&sem, "TreeNode", "ID", append(ch, 0)),
                        ),
                    ),
                ),
            )
        };

        let a1 = a.send(insert("Sequence")).unwrap();
        let a2 = a.send(name("Sequence", 'a')).unwrap();
        let b1 = b.send(insert("Fallback")).unwrap();
        let b2 = b.send(name("Fallback", 'b')).unwrap();

        a.receive(b1);
        a.receive(b2);
        b.receive(a1);
        b.receive(a2);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(&replica.state().root);
            let children = sequence(field(&sem, root, "children").expect("minted by the insert"));
            let order = ordered(children);
            assert_eq!(order.len(), 2, "{who} kept both children");
            let names: Vec<String> = order
                .iter()
                .map(|child| text_of(&sem, object(child), "ID"))
                .collect();
            assert!(
                names == ["a", "b"] || names == ["b", "a"],
                "{who} read {names:?}"
            );
        }

        // The same order on both, which is the half of `ip8` that concurrency
        // can break on its own.
        let order_of = |replica: &moirai_protocol::replica::Replica<
            Harness,
            moirai_protocol::broadcast::tcsb::Tcsb<InstanceOp>,
        >| {
            let root = object(&replica.state().root);
            let children = sequence(field(&sem, root, "children").unwrap());
            ordered(children)
                .iter()
                .map(|child| text_of(&sem, object(child), "ID"))
                .collect::<Vec<_>>()
        };
        assert_eq!(order_of(&a), order_of(&b));
    }

    #[test]
    fn ip8_a_position_is_resolved_against_the_version_that_wrote_it() {
        // The failure this guards: resolving a position against the *current*
        // state rather than `ReadAt(event.version())`. Replica b inserts ahead
        // of a's child and then a's later update to "its" position 0 arrives;
        // read against the current state it would land on b's child.
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");

        let insert_at = |pos: usize, class: &str| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "children",
                    InstanceOp::insert(
                        pos,
                        InstanceOp::variant(class_slot(&sem, class), InstanceOp::New),
                    ),
                ),
            )
        };
        let write_at = |pos: usize, class: &str, ch: char| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "children",
                    InstanceOp::at(
                        pos,
                        InstanceOp::variant(
                            class_slot(&sem, class),
                            on(&sem, "TreeNode", "ID", append(ch, 0)),
                        ),
                    ),
                ),
            )
        };

        let a1 = a.send(insert_at(0, "Sequence")).unwrap();
        b.receive(a1);

        // Concurrent: b puts a second child in front, a names the only child
        // it can see.
        let b1 = b.send(insert_at(0, "Fallback")).unwrap();
        let a2 = a.send(write_at(0, "Sequence", 'a')).unwrap();

        a.receive(b1);
        b.receive(a2);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(&replica.state().root);
            let children = sequence(field(&sem, root, "children").unwrap());
            let order = ordered(children);
            assert_eq!(order.len(), 2, "{who}");
            let named: Vec<String> = order
                .iter()
                .map(|child| text_of(&sem, object(child), "ID"))
                .collect();
            assert_eq!(
                named,
                vec![String::new(), "a".to_string()],
                "{who} landed the name on the child its writer meant"
            );
        }
    }

    // ------------------------------------------------------------------ ip9

    #[test]
    fn ip9_two_concrete_subtypes_in_one_containment_both_survive() {
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");

        let put = |class: &str, ch: char| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "main",
                    InstanceOp::variant(
                        class_slot(&sem, class),
                        on(&sem, "TreeNode", "ID", append(ch, 0)),
                    ),
                ),
            )
        };

        let a1 = a.send(put("Sequence", 's')).unwrap();
        let b1 = b.send(put("Fallback", 'f')).unwrap();

        // Locally, a second class on a slot this replica has already set is
        // refused: `union.rs:130-145`, and the reason names the feature.
        assert!(a.send(put("Fallback", 'x')).is_none());

        a.receive(b1);
        b.receive(a1);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(&replica.state().root);
            let held = objects(field(&sem, root, "main").expect("minted by the write"));
            assert_eq!(held.len(), 2, "{who} kept both classes");
            let mut named: Vec<(String, String)> = held
                .iter()
                .map(|object| {
                    (
                        sem.classes[object.class().index()].name.to_string(),
                        text_of(&sem, object, "ID"),
                    )
                })
                .collect();
            named.sort();
            assert_eq!(
                named,
                vec![
                    ("Fallback".to_string(), "f".to_string()),
                    ("Sequence".to_string(), "s".to_string())
                ],
                "{who}"
            );
        }
    }

    // ----------------------------------------------------------------- ip10

    #[test]
    fn ip10_an_update_concurrent_with_a_removal_survives_it() {
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");

        let insert = at_root(
            &sem,
            "Root",
            on(
                &sem,
                "Root",
                "children",
                InstanceOp::insert(
                    0,
                    InstanceOp::variant(
                        class_slot(&sem, "Sequence"),
                        on(&sem, "TreeNode", "ID", append('s', 0)),
                    ),
                ),
            ),
        );
        let event = a.send(insert).unwrap();
        b.receive(event);

        let remove = at_root(
            &sem,
            "Root",
            on(&sem, "Root", "children", InstanceOp::delete(0)),
        );
        let update = at_root(
            &sem,
            "Root",
            on(
                &sem,
                "Root",
                "children",
                InstanceOp::at(
                    0,
                    InstanceOp::variant(
                        class_slot(&sem, "Sequence"),
                        on(&sem, "TreeNode", "ID", append('x', 1)),
                    ),
                ),
            ),
        );

        let removal = a.send(remove).unwrap();
        let edit = b.send(update).unwrap();
        a.receive(edit);
        b.receive(removal);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(&replica.state().root);
            let children = sequence(field(&sem, root, "children").unwrap());
            let order = ordered(children);
            assert_eq!(order.len(), 1, "{who} kept the updated child");
            // The name written before the removal went with it; the one
            // written concurrently with it stayed. That is update-wins told
            // character by character.
            assert_eq!(text_of(&sem, object(order[0]), "ID"), "x", "{who}");
        }
    }

    #[test]
    fn ip10_a_removal_with_nothing_concurrent_takes_the_child_with_it() {
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");

        let insert = at_root(
            &sem,
            "Root",
            on(
                &sem,
                "Root",
                "children",
                InstanceOp::insert(
                    0,
                    InstanceOp::variant(
                        class_slot(&sem, "Sequence"),
                        on(&sem, "TreeNode", "ID", append('s', 0)),
                    ),
                ),
            ),
        );
        let event = a.send(insert).unwrap();
        b.receive(event);

        let remove = at_root(
            &sem,
            "Root",
            on(&sem, "Root", "children", InstanceOp::delete(0)),
        );
        let event = a.send(remove).unwrap();
        b.receive(event);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let root = object(&replica.state().root);
            let children = sequence(field(&sem, root, "children").unwrap());
            assert!(ordered(children).is_empty(), "{who}");
        }
    }

    // ----------------------------------------------------------------- ip11

    /// The unstable half of one feature's subtree.
    fn feature_len(
        sem: &MetamodelSemantics,
        replica: &moirai_protocol::replica::Replica<
            Harness,
            moirai_protocol::broadcast::tcsb::Tcsb<InstanceOp>,
        >,
        feature: &str,
    ) -> usize {
        let root = object(&replica.state().root);
        field(sem, root, feature).map_or(0, Node::polog_len)
    }

    /// Three writes to one feature, then the causality that makes them
    /// stable, and the PO-Log length on either side of it.
    fn compaction(feature: &str, ops: [InstanceOp; 3]) -> (usize, usize) {
        let sem = bench();
        let (mut a, mut b) = twins(&sem, "Bench");

        let mut events = Vec::new();
        for op in ops {
            events.push(
                a.send(at_root(&sem, "Bench", op))
                    .unwrap_or_else(|| panic!("`{feature}`'s workload is enabled")),
            );
        }
        let before = feature_len(&sem, &a, feature);

        // b sees all three and answers on a feature nobody is measuring; when
        // that answer lands, a knows b has them and the three become stable.
        for event in events {
            b.receive(event);
        }
        let tick = b
            .send(at_root(
                &sem,
                "Bench",
                on(
                    &sem,
                    "Bench",
                    "tick",
                    InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1))),
                ),
            ))
            .unwrap();
        a.receive(tick);

        (before, feature_len(&sem, &a, feature))
    }

    /// One attribute of `Bench`, written three times.
    fn on_bench(feature: &str, op: LeafOp) -> InstanceOp {
        let sem = bench();
        on(&sem, "Bench", feature, InstanceOp::Leaf(op))
    }

    #[test]
    fn ip11_every_rule_compacts_after_stability() {
        let sem = bench();
        let item = class_slot(&sem, "Item");
        let count = |n: i64| InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(n)));
        let item_count = |n: i64| InstanceOp::variant(item, on(&sem, "Item", "count", count(n)));

        let arms: Vec<(&str, [InstanceOp; 3])> = vec![
            (
                "counter",
                [
                    on_bench("counter", LeafOp::Inc(Scalar::Int(1))),
                    on_bench("counter", LeafOp::Inc(Scalar::Int(2))),
                    on_bench("counter", LeafOp::Inc(Scalar::Int(3))),
                ],
            ),
            (
                "simple",
                [
                    on_bench("simple", LeafOp::Inc(Scalar::Int(1))),
                    on_bench("simple", LeafOp::Inc(Scalar::Int(2))),
                    on_bench("simple", LeafOp::Inc(Scalar::Int(3))),
                ],
            ),
            (
                "flagEw",
                [
                    on_bench("flagEw", LeafOp::Enable),
                    on_bench("flagEw", LeafOp::Disable),
                    on_bench("flagEw", LeafOp::Enable),
                ],
            ),
            (
                "flagDw",
                [
                    on_bench("flagDw", LeafOp::Enable),
                    on_bench("flagDw", LeafOp::Disable),
                    on_bench("flagDw", LeafOp::Enable),
                ],
            ),
            (
                "regMv",
                [
                    on_bench("regMv", LeafOp::Write(Scalar::text("a"))),
                    on_bench("regMv", LeafOp::Write(Scalar::text("b"))),
                    on_bench("regMv", LeafOp::Write(Scalar::text("c"))),
                ],
            ),
            (
                "regLww",
                [
                    on_bench("regLww", LeafOp::Write(Scalar::text("a"))),
                    on_bench("regLww", LeafOp::Write(Scalar::text("b"))),
                    on_bench("regLww", LeafOp::Write(Scalar::text("c"))),
                ],
            ),
            (
                "regFair",
                [
                    on_bench("regFair", LeafOp::Write(Scalar::text("a"))),
                    on_bench("regFair", LeafOp::Write(Scalar::text("b"))),
                    on_bench("regFair", LeafOp::Write(Scalar::text("c"))),
                ],
            ),
            (
                "regPo",
                [
                    on_bench("regPo", LeafOp::Write(Scalar::Int(1))),
                    on_bench("regPo", LeafOp::Write(Scalar::Int(2))),
                    on_bench("regPo", LeafOp::Write(Scalar::Int(3))),
                ],
            ),
            (
                "regTo",
                [
                    on_bench("regTo", LeafOp::Write(Scalar::Int(1))),
                    on_bench("regTo", LeafOp::Write(Scalar::Int(2))),
                    on_bench("regTo", LeafOp::Write(Scalar::Int(3))),
                ],
            ),
            (
                "enumReg",
                [
                    on_bench("enumReg", LeafOp::Write(Scalar::Enum(0, 0))),
                    on_bench("enumReg", LeafOp::Write(Scalar::Enum(0, 1))),
                    on_bench("enumReg", LeafOp::Write(Scalar::Enum(0, 2))),
                ],
            ),
            (
                "setAw",
                [
                    on_bench("setAw", LeafOp::Add(Scalar::text("a"))),
                    on_bench("setAw", LeafOp::Add(Scalar::text("b"))),
                    on_bench("setAw", LeafOp::Add(Scalar::text("c"))),
                ],
            ),
            (
                "setRw",
                [
                    on_bench("setRw", LeafOp::Add(Scalar::text("a"))),
                    on_bench("setRw", LeafOp::Add(Scalar::text("b"))),
                    on_bench("setRw", LeafOp::Add(Scalar::text("c"))),
                ],
            ),
            (
                "bag",
                [
                    on_bench("bag", LeafOp::Add(Scalar::text("a"))),
                    on_bench("bag", LeafOp::Add(Scalar::text("a"))),
                    on_bench("bag", LeafOp::Add(Scalar::text("b"))),
                ],
            ),
            (
                "optCounter",
                [
                    on(&sem, "Bench", "optCounter", InstanceOp::set(count(1))),
                    on(&sem, "Bench", "optCounter", InstanceOp::set(count(2))),
                    on(&sem, "Bench", "optCounter", InstanceOp::set(count(3))),
                ],
            ),
            (
                "seqCounter",
                [
                    on(&sem, "Bench", "seqCounter", InstanceOp::insert(0, count(1))),
                    on(&sem, "Bench", "seqCounter", InstanceOp::at(0, count(2))),
                    on(&sem, "Bench", "seqCounter", InstanceOp::at(0, count(3))),
                ],
            ),
            (
                "one",
                [
                    on(&sem, "Bench", "one", item_count(1)),
                    on(&sem, "Bench", "one", item_count(2)),
                    on(&sem, "Bench", "one", item_count(3)),
                ],
            ),
            (
                "maybe",
                [
                    on(&sem, "Bench", "maybe", InstanceOp::set(item_count(1))),
                    on(&sem, "Bench", "maybe", InstanceOp::set(item_count(2))),
                    on(&sem, "Bench", "maybe", InstanceOp::set(item_count(3))),
                ],
            ),
            (
                "many",
                [
                    on(&sem, "Bench", "many", InstanceOp::insert(0, item_count(1))),
                    on(&sem, "Bench", "many", InstanceOp::at(0, item_count(2))),
                    on(&sem, "Bench", "many", InstanceOp::at(0, item_count(3))),
                ],
            ),
            (
                "refOne",
                [
                    on_bench("refOne", LeafOp::Write(Scalar::text("i1"))),
                    on_bench("refOne", LeafOp::Write(Scalar::text("i2"))),
                    on_bench("refOne", LeafOp::Write(Scalar::text("i3"))),
                ],
            ),
            (
                "refMany",
                [
                    on_bench("refMany", LeafOp::Add(Scalar::text("i1"))),
                    on_bench("refMany", LeafOp::Add(Scalar::text("i2"))),
                    on_bench("refMany", LeafOp::Add(Scalar::text("i3"))),
                ],
            ),
        ];

        for (feature, ops) in arms {
            let (before, after) = compaction(feature, ops);
            assert!(before > 0, "`{feature}` held nothing to compact");
            assert!(
                after < before,
                "`{feature}` did not compact: {before} operations before stability, {after} after"
            );
        }
    }

    #[test]
    fn ip11_text_is_the_one_rule_that_does_not_compact_and_neither_does_its_twin() {
        // `EventGraph` is `List`'s home and `List` sets `DISABLE_STABILIZE`
        // (`eg_walker/mod.rs:502`), so a text leaf holds its whole history on
        // *both* paths. `ip11`'s claim is "on a workload where the generated
        // path's equivalent shrinks", and this is the one where it does not,
        // named here so the exclusion is a statement and not an omission.
        let (before, after) = compaction(
            "text",
            [
                on_bench("text", LeafOp::InsertChar { pos: 0, ch: 'a' }),
                on_bench("text", LeafOp::InsertChar { pos: 1, ch: 'b' }),
                on_bench("text", LeafOp::InsertChar { pos: 2, ch: 'c' }),
            ],
        );
        assert_eq!(before, 3);
        assert_eq!(after, 3, "text keeps its history, here as in `bt_crdt`");
    }

    // ------------------------------------------------- the pure resolutions

    #[test]
    fn a_rule_the_table_cannot_run_is_refused_at_mint_time() {
        for reason in [
            UnsupportedReason::Keyed,
            UnsupportedReason::Transparent,
            UnsupportedReason::Derived,
            UnsupportedReason::Transient,
            UnsupportedReason::Volatile,
        ] {
            assert_eq!(
                Node::for_rule(&MergeRule::Unsupported { reason }).unwrap_err(),
                reason
            );
        }
    }

    #[test]
    fn an_ordered_set_is_minted_as_the_sequence_the_generator_emits() {
        // `attribute.rs:183-198` warns and emits a list; `Shape::effective`
        // is where that degradation is written down, and this is where it is
        // checked to reach the node.
        let rule = MergeRule::Attribute {
            shape: Shape::OrderedSet,
            leaf: LeafRule::Text,
        };
        assert!(matches!(shaped(&rule).unwrap(), super::Shaped::Sequence(_)));
        assert!(matches!(Node::for_rule(&rule).unwrap(), Node::Seq(_)));
    }

    #[test]
    fn a_reference_is_a_string_and_a_containment_is_a_slot() {
        let sem = mini();
        let tree = class_slot(&sem, "TreeNode");
        assert!(matches!(
            shaped(&MergeRule::Reference {
                many: false,
                target: tree
            })
            .unwrap(),
            super::Shaped::Bare(Site::Leaf(_))
        ));
        assert!(matches!(
            shaped(&MergeRule::Containment {
                shape: Shape::Single,
                target: tree
            })
            .unwrap(),
            super::Shaped::Bare(Site::Object(_))
        ));
    }

    #[test]
    fn a_counter_of_every_width_mints_its_own_arm() {
        for num in [
            NumKind::U8,
            NumKind::I16,
            NumKind::I32,
            NumKind::I64,
            NumKind::F32,
            NumKind::F64,
        ] {
            for resettable in [true, false] {
                let rule = MergeRule::Attribute {
                    shape: Shape::Single,
                    leaf: LeafRule::Counter { num, resettable },
                };
                assert!(matches!(Node::for_rule(&rule).unwrap(), Node::Leaf(_)));
            }
        }
    }

    #[test]
    fn a_feature_a_class_cannot_see_is_refused_by_name() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let refusal = check(
            &sem,
            None,
            super::Shaped::Bare(Site::Object(Target::Class(root))),
            &InstanceOp::variant(root, InstanceOp::field(FeatureSlot(9), InstanceOp::New)),
            &super::At::root(),
            Mode::Local,
        )
        .unwrap_err();
        match &refusal {
            Refusal::UnknownFeature { class, slot, .. } => {
                assert_eq!(&**class, "Root");
                assert_eq!(*slot, 9);
            }
            other => panic!("{other:?}"),
        }
        assert!(refusal.to_string().contains("`Root` declares no feature"));
    }

    #[test]
    fn a_class_a_containment_cannot_hold_is_refused_by_name() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        // `TreeNode` is abstract, so it is not in its own concrete closure.
        let refusal = check(
            &sem,
            None,
            super::Shaped::Bare(Site::Object(Target::Class(root))),
            &InstanceOp::variant(
                root,
                on(
                    &sem,
                    "Root",
                    "main",
                    InstanceOp::variant(class_slot(&sem, "TreeNode"), InstanceOp::New),
                ),
            ),
            &super::At::root(),
            Mode::Local,
        )
        .unwrap_err();
        match &refusal {
            Refusal::ClassNotAllowed {
                class,
                feature,
                offered,
                allowed,
            } => {
                assert_eq!(&**class, "Root");
                assert_eq!(&**feature, "main");
                assert_eq!(&**offered, "TreeNode");
                let allowed: Vec<&str> = allowed.iter().map(|name| &**name).collect();
                assert_eq!(allowed, ["Fallback", "Sequence"]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn addressing_a_feature_as_the_wrong_shape_is_refused_by_name() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        // `Root.children` is a sequence, and this writes a leaf into it.
        let refusal = check(
            &sem,
            None,
            super::Shaped::Bare(Site::Object(Target::Class(root))),
            &InstanceOp::variant(root, on(&sem, "Root", "children", append('x', 0))),
            &super::At::root(),
            Mode::Local,
        )
        .unwrap_err();
        match &refusal {
            Refusal::WrongShape {
                class,
                feature,
                expected,
                got,
            } => {
                assert_eq!(&**class, "Root");
                assert_eq!(&**feature, "children");
                assert_eq!(*expected, "a sequence");
                assert_eq!(*got, "a leaf write");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_leaf_that_does_not_take_the_operation_is_refused_by_name() {
        let sem = mini();
        let root = class_slot(&sem, "Root");
        let refusal = check(
            &sem,
            None,
            super::Shaped::Bare(Site::Object(Target::Class(root))),
            &InstanceOp::variant(
                root,
                on(
                    &sem,
                    "Root",
                    "title",
                    InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1))),
                ),
            ),
            &super::At::root(),
            Mode::Local,
        )
        .unwrap_err();
        assert!(refusal.to_string().contains("`Root.title`"), "{refusal}");
        assert!(refusal.to_string().contains("`text` leaf"), "{refusal}");
    }

    #[test]
    fn the_fixtures_number_their_visible_slots_the_way_the_operations_read_them() {
        // The trap `op.rs` names: `Sequence` sees `ID` and `name` declared by
        // `TreeNode` and `children` declared by itself, and the three visible
        // slots are distinct even though two of the declared slots collide.
        let sem = mini();
        let sequence = class_slot(&sem, "Sequence");
        let visible: Vec<&str> = sem.classes[sequence.index()]
            .visible
            .iter()
            .map(|(name, _, _)| &**name)
            .collect();
        assert_eq!(visible, ["ID", "children", "name"]);
        assert_eq!(feature_slot(&sem, sequence, "ID"), FeatureSlot(0));
        assert_eq!(feature_slot(&sem, sequence, "children"), FeatureSlot(1));

        // Both of these are declared slot 0 of their own class, which is
        // exactly the collision the visible numbering avoids.
        let (id_name, id_owner, id_declared) = &sem.classes[sequence.index()].visible[0];
        assert_eq!(&**id_name, "ID");
        assert_eq!(*id_owner, class_slot(&sem, "TreeNode"));
        assert_eq!(*id_declared, FeatureSlot(0));
        let (children_name, children_owner, children_declared) =
            &sem.classes[sequence.index()].visible[1];
        assert_eq!(&**children_name, "children");
        assert_eq!(*children_owner, sequence);
        assert_eq!(*children_declared, FeatureSlot(0));
    }

    #[test]
    fn testing_fixtures_are_readable_descriptors() {
        let _ = testing::mini();
        let _ = testing::bench();
    }
}
