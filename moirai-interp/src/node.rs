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
//! `GraphLog<List<EventId>>` verbatim, the same field `NestedListLog`
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

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use moirai_crdt::list::eg_walker::{List, ReadAt};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::query::Read,
    event::{
        Event,
        id::{EventId, ResolvedEventId},
        lamport::Lamport,
    },
    replica::ReplicaIdx,
    state::{effect_context::EffectContext, graph_log::GraphLog, log::IsLog},
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

use crate::leaf::{LeafLog, LeafMismatch, Scalar};
use crate::op::{InstanceOp, MapOp, OptOp, SeqOp};

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
                 (decision D6): {}",
                reason.as_str(),
                reason.why()
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
    /// A whole ordered set of scalars in one CRDT.
    OrderedSet,
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
    /// Many of these, each under a key of its own.
    Keyed(Site),
}

impl Shaped {
    /// The word a refusal sentence uses.
    pub(crate) const fn word(self) -> &'static str {
        match self {
            Shaped::Bare(Site::Object(_)) => "a single-valued containment",
            Shaped::Bare(Site::Leaf(LeafSite::Scalar(_))) => "a single-valued attribute",
            Shaped::Bare(Site::Leaf(LeafSite::Set(_))) => "a set",
            Shaped::Bare(Site::Leaf(LeafSite::Bag)) => "a bag",
            Shaped::Bare(Site::Leaf(LeafSite::OrderedSet)) => "an ordered set",
            Shaped::Optional(_) => "optional",
            Shaped::Sequence(_) => "a sequence",
            Shaped::Keyed(_) => "a keyed collection",
        }
    }
}

/// Split one feature's rule.
///
/// The one place the generator's thirteen outcomes become node shapes, and
/// the one place decision D6's refusals are turned into a value.
pub(crate) fn shaped(rule: &MergeRule) -> Result<Shaped, UnsupportedReason> {
    Ok(match *rule {
        MergeRule::Attribute { shape, leaf } => match shape {
            Shape::Single => Shaped::Bare(Site::Leaf(LeafSite::Scalar(leaf))),
            Shape::Optional => Shaped::Optional(Site::Leaf(LeafSite::Scalar(leaf))),
            Shape::Sequence => Shaped::Sequence(Site::Leaf(LeafSite::Scalar(leaf))),
            Shape::Set { tie } => Shaped::Bare(Site::Leaf(LeafSite::Set(tie))),
            Shape::Bag => Shaped::Bare(Site::Leaf(LeafSite::Bag)),
            // One CRDT holding the whole collection, exactly as a set and a
            // bag are: `GraphLog<List<V>>` and not a `NestedListLog` of leaf
            // logs, which is what tells it from `Shape::Sequence`.
            Shape::OrderedSet => Shaped::Bare(Site::Leaf(LeafSite::OrderedSet)),
            // A `uw-map` whose entry class's value feature is an attribute:
            // `containment.rs`'s `uw_map_value_log_type` compiles the leaf's
            // own log under the map, and the entry class disappears.
            Shape::Keyed { .. } => Shaped::Keyed(Site::Leaf(LeafSite::Scalar(leaf))),
        },
        MergeRule::Containment { shape, target } => match shape {
            Shape::Optional => Shaped::Optional(Site::Object(Target::Class(target))),
            Shape::Sequence => Shaped::Sequence(Site::Object(Target::Class(target))),
            // `target` is the *value* feature's class and never the entry
            // class the `.ecore` file names: the map holds what its value
            // feature holds, which is what `UWMapLog<String, JsonKindLog>`
            // says.
            Shape::Keyed { .. } => Shaped::Keyed(Site::Object(Target::Class(target))),
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
    /// A keyed collection.
    Map(MapNode),
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

/// An object: its class, the event that created it, and every feature its
/// class can see.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ObjectNode {
    pub(crate) class: ClassSlot,
    /// The id of the operation that brought this object into existence.
    ///
    /// [`SeqNode`] already keys its children by exactly this and mints it as
    /// `ctx.id.clone()` (`nested_list.rs:104-112`, copied in [`apply`]); this
    /// is the same rule extended to the other three container shapes, so that
    /// an object in a slot, an optional or a map is as addressable as one in
    /// a sequence.
    ///
    /// **It is an `EventId` and not an `Option<EventId>`, and there is no
    /// `Root` variant beside it.** Spec 11 §3.6 wrote
    /// `enum ObjectId { Root, Born(EventId) }` because it expected a document
    /// root the store creates for itself, and gave the reason a sentinel is
    /// not allowed there: a synthesised `EventId` resolves to a different
    /// string on every replica. This tree has none:
    /// `ModelLog::effect` mints the root as `Node::Slot(SlotNode::Unset)` on
    /// `Install` and the root *object* is put there by [`ensure_variant`], on
    /// the first `InstanceOp::Variant` that reaches it, with that operation's
    /// event in hand exactly as every deeper object is. Every object here is
    /// born of an operation, so nothing has to be invented for the one at the
    /// top.
    ///
    /// **It is stable and it is not yet convergent, and the second half needs
    /// a decision before anything keys on it.** An object created by one
    /// operation and delivered to a peer has one birth on both. Two writers
    /// who put the *same* concrete class in one place at once do not:
    /// [`ensure_variant`] collapses them into one object, which is
    /// `union.rs:180-200`'s rule and what `ip9` asserts for two *different*
    /// classes, and each replica keeps the id of whichever creating operation
    /// it delivered first. The same holds for an [`OptNode`]'s
    /// `get_or_insert_with` and a [`MapNode`]'s `entry(..).or_insert_with`;
    /// a [`SeqNode`] insert is exempt, because it keys its child by
    /// `ctx.id` and two concurrent inserts are therefore two children.
    /// `two_concurrent_creations_of_one_class_collapse_into_one_object_and_two_births`
    /// pins it. Nothing observes it today, for the reason below.
    ///
    /// **Nothing reads it yet.** It is deliberately absent from
    /// [`Node::is_default`], from [`ObjectNode::is_default`] and from
    /// `eval::read_object`: an object that has just been created still reads
    /// as its default, which is what keeps a freshly minted empty object
    /// pruned by `MapNode`'s rule and keeps the read-out comparable with the
    /// generated path, which has no birth identity at all. It is here first
    /// because it moves the state-transfer format, and a format move is
    /// cheapest before there are consumers and fixtures to move with it.
    ///
    /// The identity a client is served is not this but [`ObjectNode::identity`],
    /// derived beside it from [`ObjectNode::creators`], which converges where
    /// this does not.
    pub(crate) born: EventId,
    /// The creations this object is served by: the operations that created
    /// it, as their own writers saw it, less those a later creation saw.
    ///
    /// An operation is a creation when, in the state its writer saw, the
    /// object was not alive (see [`ObjectNode::touches`]): it did not exist
    /// yet, or a removal had taken away everything written into it. That is a
    /// question about the operation's own causal past and nothing else, so
    /// every replica answers it the same way whatever order it delivers the
    /// operations in. Two writers who put one class in one place at once are
    /// both creations, where `born` keeps whichever came first. A creation
    /// that saw an earlier one takes its place, since the earlier one had
    /// been removed; so the list holds creations concurrent with one another,
    /// and an identity it has stopped serving it never serves again.
    ///
    /// Kept in the order of [`Creator::key`], so that two replicas holding
    /// one state serialize it to the same bytes.
    pub(crate) creators: Vec<Creator>,
    /// The operations that reached this object, each with the removals that
    /// had seen it: what decides whether the object is alive for a writer.
    ///
    /// A writer at a version sees the object alive when some operation in
    /// that version reached it and no removal in that version had seen that
    /// operation. That is update-wins, the rule the containers themselves
    /// follow: a write concurrent with a removal survives it, and so does the
    /// object it wrote into, whose identity the next writer then keeps.
    ///
    /// Pruned as operations stabilize ([`ObjectNode::stabilize`]), and kept
    /// in the order of the resolved id for the same reason as `creators`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) touches: Vec<Touch>,
    /// Keyed by the *visible* slot; minted on first write, from the rule.
    pub(crate) fields: BTreeMap<FeatureSlot, Node>,
}

/// One operation that created an object.
///
/// `step` tells apart the objects one operation creates: an operation is one
/// path, and each class step on it reaches exactly one object, so the pair
/// `(id, step)` names one creation and never two.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Creator {
    id: EventId,
    step: u32,
}

impl Creator {
    fn new(ctx: Ctx<'_>) -> Self {
        Creator {
            id: ctx.id.clone(),
            step: ctx.step,
        }
    }

    /// The order identities are chosen in: the same on every replica, since
    /// it names the replica by its string id and not by a local index.
    fn key(&self) -> (ResolvedEventId, u32) {
        (ResolvedEventId::from(&self.id), self.step)
    }

    /// The served form: `<replica>@<seq>.<step>`, with the event's
    /// disambiguator after the sequence number when it has one. Read from the
    /// right it is one creation and no other.
    fn render(&self) -> String {
        match self.id.disambiguator() {
            Some(extra) => format!(
                "{}@{}:{}.{}",
                self.id.origin_id(),
                self.id.seq(),
                extra,
                self.step
            ),
            None => format!("{}@{}.{}", self.id.origin_id(), self.id.seq(), self.step),
        }
    }
}

/// One operation that reached an object, and the removals that had seen it.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Touch {
    id: EventId,
    /// A sequence delete, an optional unset, a map removal or clear above the
    /// object, that had this operation in its past. A removal whose own past
    /// already holds one listed here adds nothing, so the list holds only
    /// removals concurrent with one another, in resolved-id order.
    covered_by: Vec<EventId>,
}

impl Touch {
    /// Whether a writer at `version` saw this operation and no removal of it.
    fn alive_at(&self, version: &Version) -> bool {
        self.id.is_predecessor_of(version) && !covered_at(&self.covered_by, version)
    }
}

/// Whether one of `removals` is in `version`.
fn covered_at(removals: &[EventId], version: &Version) -> bool {
    removals
        .iter()
        .any(|removal| removal.is_predecessor_of(version))
}

/// Put `item` into a list kept in the resolved-id order of what `id` reads.
fn insert_resolved<T>(list: &mut Vec<T>, item: T, id: impl Fn(&T) -> &EventId) {
    let key = ResolvedEventId::from(id(&item));
    let at = list.partition_point(|other| ResolvedEventId::from(id(other)) < key);
    list.insert(at, item);
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
    pub(crate) positions: GraphLog<List<EventId>>,
    /// Keyed by the id of the operation that inserted the child, as
    /// `NestedListLog` keys its own children. A hash map and not a sorted one
    /// because Moirai v0.4 dropped `Ord` from [`EventId`]: an id orders by a
    /// *local* replica index, which is not the same order on two replicas.
    /// Read order comes from `positions`, never from this map; the one place
    /// that needs a fixed order is serialization, which sorts by the resolved
    /// id. See [`children_serde`].
    #[cfg_attr(feature = "serde", serde(with = "children_serde"))]
    pub(crate) children: HashMap<EventId, Node>,
}

/// A sequence's children as a list of pairs rather than as a JSON object.
///
/// The key is an [`EventId`], which is a struct and not a string, and a JSON
/// object's keys are strings. The state transfer this log has to survive is
/// JSON (`state_transfer.rs`), so the map travels as the pairs it is — sorted
/// by the *resolved* id, which names the replica by its string id and so
/// orders the same on both sides, keeping two replicas' bytes identical for
/// one state.
#[cfg(feature = "serde")]
mod children_serde {
    use moirai_protocol::event::id::ResolvedEventId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{EventId, HashMap, Node};

    pub(super) fn serialize<S: Serializer>(
        children: &HashMap<EventId, Node>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut pairs: Vec<(&EventId, &Node)> = children.iter().collect();
        pairs.sort_by_key(|(id, _)| ResolvedEventId::from(*id));
        pairs.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<EventId, Node>, D::Error> {
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
    /// The objects a write or an unset left empty, taken out of `child` for
    /// that reason and kept here, one per class, so that their creations and
    /// touches are not lost with them.
    ///
    /// Whether a child empties depends on what else a replica has delivered,
    /// so dropping its objects would make the identity of the next object of
    /// their class here depend on delivery order. Read by nothing. A write of
    /// a class the child does not hold takes that class's object back from
    /// here instead of minting one, which reads the same, since an object
    /// here is at its default. Every class here is one the child does not
    /// hold, and the list is kept in class order.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub(crate) ghost: Vec<ObjectNode>,
}

/// A keyed collection: `UWMapLog`'s mapping half, minting by rule.
///
/// `UWMapLog` holds a `HashMap<K, L>` and mints its children through
/// `Default` (`uw_map.rs:133`). An interpreted child has no `Default` to mint
/// through, so the map is owned here and every child comes from
/// [`Node::for_site`]. The container is a `BTreeMap` rather than a `HashMap`
/// because two replicas holding one state have to serialize to the same bytes
/// for state transfer, which is the same reason [`SeqNode`]'s children are
/// one.
///
/// **A removal does not remove.** `uw_map.rs:150-152` routes `Remove` to the
/// child's `redundant_by_parent(version, true)` and leaves the entry in the
/// map, which is what makes the map update-wins: an update concurrent with
/// the removal is not below its version and survives it. Copied verbatim,
/// `Clear` included, and the read-out drops an entry that reads as its
/// default, which is `UWMapLog::execute_query`'s own rule and therefore how
/// both paths spell "removed".
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(serialize = "", deserialize = ""))
)]
pub struct MapNode {
    #[cfg_attr(feature = "serde", serde(with = "entries_serde"))]
    pub(crate) children: BTreeMap<Scalar, Node>,
}

/// A map's children as a list of pairs, for the same reason a sequence's are:
/// the key is a [`Scalar`] and a JSON object's keys are strings.
#[cfg(feature = "serde")]
mod entries_serde {
    use super::{BTreeMap, Node, Scalar};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        children: &BTreeMap<Scalar, Node>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        children.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Scalar, Node>, D::Error> {
        Ok(Vec::<(Scalar, Node)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

impl MapNode {
    /// The entries, by key, in key order.
    pub fn children(&self) -> &BTreeMap<Scalar, Node> {
        &self.children
    }
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
    /// How many class steps of the operation lie above this level: `0` at
    /// the top of a model. See [`Creator`].
    pub step: u32,
}

impl<'a> Ctx<'a> {
    /// The context at the top of a model, for one event.
    pub(crate) fn top(id: &'a EventId, lamport: &'a Lamport, version: &'a Version) -> Self {
        Ctx {
            id,
            lamport,
            version,
            step: 0,
        }
    }

    /// The event this operation would be, at this level.
    pub(crate) fn event<O>(&self, op: O) -> Event<O> {
        Event::new(self.id.clone(), *self.lamport, op, self.version.clone())
    }

    /// The context below one more class step.
    fn below_class(self) -> Self {
        Ctx {
            step: self.step + 1,
            ..self
        }
    }
}

impl ObjectNode {
    /// An empty instance of one class, born of one operation.
    ///
    /// The `born` argument is not defaultable and not optional, which is the
    /// point: the only caller is [`ensure_variant`], the only caller of
    /// *that* is [`apply`], and `apply` holds a [`Ctx`]. An object that could
    /// be minted without an event in hand would have no identity to give, and
    /// the compiler is what says so.
    pub(crate) fn new(class: ClassSlot, ctx: Ctx<'_>) -> Self {
        ObjectNode {
            class,
            born: ctx.id.clone(),
            creators: vec![Creator::new(ctx)],
            touches: vec![Touch {
                id: ctx.id.clone(),
                covered_by: Vec::new(),
            }],
            fields: BTreeMap::new(),
        }
    }

    /// The store identity this object is served with: a non-empty string,
    /// the same on every replica that delivered the same operations.
    ///
    /// The least of its creations ([`ObjectNode::creators`]). A creation
    /// belongs to one object only, so no two objects of one model share an
    /// identity; and one created again after a removal carries a creation
    /// that saw every earlier one, so it serves an identity nothing carried
    /// before and none of the old ones again.
    pub fn identity(&self) -> String {
        match self.creators.iter().min_by_key(|creator| creator.key()) {
            Some(creator) => creator.render(),
            // Not reachable from an operation: every object is minted with
            // its creator. Named from `born` rather than left empty.
            None => format!("{}@{}", self.born.origin_id(), self.born.seq()),
        }
    }

    /// An operation reached this object, which was already here: if its
    /// writer saw the object not alive, the operation is one more creation,
    /// and the creations its writer saw give way to it.
    fn written_by(&mut self, ctx: Ctx<'_>) {
        if !self.touches.iter().any(|touch| touch.alive_at(ctx.version)) {
            self.creators
                .retain(|creator| !creator.id.is_predecessor_of(ctx.version));
            insert_resolved(&mut self.creators, Creator::new(ctx), |creator| &creator.id);
        }
        insert_resolved(
            &mut self.touches,
            Touch {
                id: ctx.id.clone(),
                covered_by: Vec::new(),
            },
            |touch| &touch.id,
        );
    }

    /// The class this is an instance of.
    pub fn class(&self) -> ClassSlot {
        self.class
    }

    /// The id of the operation that created this object.
    ///
    /// Stable for the object's whole life: [`apply_object`] writes fields and
    /// never this. Nothing in this crate reads it — see the field's own note
    /// for why it is here, and [`ObjectNode::identity`] for what is served.
    pub fn born(&self) -> &EventId {
        &self.born
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
    pub fn children(&self) -> &HashMap<EventId, Node> {
        &self.children
    }
}

impl OptNode {
    /// What is here, if anything.
    pub fn child(&self) -> Option<&Node> {
        self.child.as_deref()
    }
}

/// `positions.eval(&Read::new())` without importing `EvalNested` everywhere.
trait ReadPositions {
    fn execute_query_read(&self) -> Vec<EventId>;
    fn execute_query_read_at(&self, version: &Version) -> Vec<EventId>;
}

impl ReadPositions for GraphLog<List<EventId>> {
    fn execute_query_read(&self) -> Vec<EventId> {
        self.eval(&Read::new())
    }

    /// The order the writer of an operation saw, which is the only reading of
    /// a position two replicas can agree on (`nested_list.rs:138`).
    fn execute_query_read_at(&self, version: &Version) -> Vec<EventId> {
        self.eval(&ReadAt::new(version))
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
            Shaped::Keyed(_) => Node::Map(MapNode::default()),
        }
    }

    pub(crate) fn for_site(site: Site) -> Node {
        match site {
            Site::Object(_) => Node::Slot(SlotNode::Unset),
            Site::Leaf(LeafSite::Scalar(rule)) => Node::Leaf(LeafLog::for_rule(rule)),
            Site::Leaf(LeafSite::Set(tie)) => Node::Leaf(LeafLog::for_set(tie)),
            Site::Leaf(LeafSite::Bag) => Node::Leaf(LeafLog::for_bag()),
            Site::Leaf(LeafSite::OrderedSet) => Node::Leaf(LeafLog::for_ordered_set()),
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
                for object in &mut opt.ghost {
                    object.stabilize(version);
                }
            }
            Node::Map(map) => {
                for child in map.children.values_mut() {
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
                for object in &mut opt.ghost {
                    object.redundant_by_parent(version, conservative);
                }
                // A removal above the optional empties what it had seen here
                // as much as an unset of the optional itself does.
                shed(opt);
            }
            Node::Map(map) => {
                for child in map.children.values_mut() {
                    child.redundant_by_parent(version, conservative);
                }
            }
            Node::Leaf(leaf) => leaf.redundant_by_parent(version, conservative),
        }
    }

    /// The removal `removal`, at `version`, took away this subtree: every
    /// creation of an object below it that the removal had seen is marked
    /// removed by it. What was created concurrently with the removal is not
    /// below its version and stays alive.
    fn retire(&mut self, removal: &EventId, version: &Version) {
        match self {
            Node::Unbound | Node::Leaf(_) => {}
            Node::Slot(slot) => slot.for_each_mut(|object| object.retire(removal, version)),
            Node::Seq(seq) => {
                for child in seq.children.values_mut() {
                    child.retire(removal, version);
                }
            }
            Node::Opt(opt) => {
                if let Some(child) = opt.child.as_mut() {
                    child.retire(removal, version);
                }
                for object in &mut opt.ghost {
                    object.retire(removal, version);
                }
            }
            Node::Map(map) => {
                for child in map.children.values_mut() {
                    child.retire(removal, version);
                }
            }
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
            // `uw_map.rs:174`, verbatim: a map that holds a key is not at
            // its default, whatever that key now holds. It reads as `{}` once
            // every entry has been emptied — that is the read-out's rule, in
            // `eval::read_node` — but it is not *default*, and the two are
            // different questions. Answering `all(is_default)` here would
            // make an emptied map default on this path and not on the
            // generated one, and a conflict set drops its default members
            // (`union.rs:333-338`), so the two read-outs would part company
            // over a `json.ecore` object whose every entry had been removed.
            Node::Map(map) => map.children.is_empty(),
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
            Node::Map(map) => map.children.values().map(Node::polog_len).sum(),
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
        self.forget_stable_touches(version);
        for field in self.fields.values_mut() {
            field.stabilize(version);
        }
    }

    /// Drop the touches no writer still to come can tell apart.
    ///
    /// Every operation still to be delivered has `stable` in its past. So a
    /// stable touch that a stable removal had seen is dead for all of them,
    /// and of the stable touches from one replica the latest says all that
    /// the earlier ones do: whatever removal had seen the latest had seen
    /// them too.
    fn forget_stable_touches(&mut self, stable: &Version) {
        self.touches.retain(|touch| {
            !(touch.id.is_predecessor_of(stable) && covered_at(&touch.covered_by, stable))
        });
        let mut latest: HashMap<ReplicaIdx, usize> = HashMap::new();
        for touch in &self.touches {
            if touch.id.is_predecessor_of(stable) {
                let seq = latest.entry(touch.id.idx()).or_default();
                *seq = (*seq).max(touch.id.seq());
            }
        }
        self.touches.retain(|touch| {
            !touch.id.is_predecessor_of(stable)
                || latest.get(&touch.id.idx()) == Some(&touch.id.seq())
        });
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for field in self.fields.values_mut() {
            field.redundant_by_parent(version, conservative);
        }
    }

    fn retire(&mut self, removal: &EventId, version: &Version) {
        for touch in &mut self.touches {
            // A removal already in this one's past says the same thing to
            // every later writer, so it is not listed twice.
            if touch.id.is_predecessor_of(version) && !covered_at(&touch.covered_by, version) {
                insert_resolved(&mut touch.covered_by, removal.clone(), |id| id);
            }
        }
        for field in self.fields.values_mut() {
            field.retire(removal, version);
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
        InstanceOp::Map(_) => "a keyed step",
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

    /// Step into one entry of a keyed collection, by its key.
    ///
    /// `uw_map.rs:117` descends through `path.map_entry` too, so the
    /// dashboard sees a map entry where the generated path puts one.
    pub(crate) fn map_key(&mut self, key: &str) -> Emit<'_> {
        self.entry(key)
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
/// `GraphLog::effect` ignores its effect context outright
/// (`graph_log.rs`), so a silent one is passed here rather than threaded down
/// a recursion that would carry it for nobody.
fn positions_effect(positions: &mut GraphLog<List<EventId>>, event: Event<List<EventId>>) {
    positions.effect(event, &mut EffectContext::silent());
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
        Shaped::Keyed(site) => {
            let InstanceOp::Map(map_op) = op else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a keyed collection",
                    got: word(op),
                });
            };
            match map_op {
                // `uw_map.rs:177-180`: an `Update` is enabled when the child
                // it addresses enables it, and a key nobody has written yet
                // enables anything. A key is not a position, so there is no
                // bound to be out of and no local-only check to make.
                MapOp::Update { key, op } => {
                    let child = match node {
                        Some(Node::Map(map)) => map.children.get(key),
                        _ => None,
                    };
                    check(sem, child, Shaped::Bare(site), op, at, mode)
                }
                // `uw_map.rs:181`: both are always enabled, on a key the map
                // holds and on one it does not.
                MapOp::Remove { .. } | MapOp::Clear => Ok(()),
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
                        child.retire(ctx.id, ctx.version);
                    }
                    Ok(())
                }
            }
        }
        Shaped::Keyed(site) => {
            let (InstanceOp::Map(map_op), Node::Map(map)) = (op, node) else {
                return Err(Refusal::WrongShape {
                    class: at.class.clone(),
                    feature: at.feature.clone(),
                    expected: "a keyed collection",
                    got: "something else",
                });
            };
            match map_op {
                MapOp::Update { key, op } => {
                    let name = key.to_key(Some(sem));
                    let mut emit = emit.map_key(&name);
                    if map.children.contains_key(&key) {
                        emit.update();
                    } else {
                        emit.create();
                    }
                    let child = map
                        .children
                        .entry(key)
                        .or_insert_with(|| Node::for_site(site));
                    apply(sem, child, Shaped::Bare(site), ctx, *op, at, emit)
                }
                // `uw_map.rs:150-152`, verbatim: the entry stays and its
                // subtree is reset against the removal's own version, so an
                // update concurrent with the removal survives it and one
                // causally below it does not.
                MapOp::Remove { key } => {
                    emit.map_key(&key.to_key(Some(sem))).delete();
                    if let Some(child) = map.children.get_mut(&key) {
                        child.redundant_by_parent(ctx.version, true);
                        child.retire(ctx.id, ctx.version);
                    }
                    Ok(())
                }
                // `uw_map.rs:157-163`: the same to every entry at once.
                MapOp::Clear => {
                    emit.delete();
                    for child in map.children.values_mut() {
                        child.redundant_by_parent(ctx.version, true);
                        child.retire(ctx.id, ctx.version);
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
                    revive(&mut opt.ghost, child, &inner);
                    let outcome = apply(sem, child, Shaped::Bare(site), ctx, *inner, at, emit);
                    // `option/mod.rs:110`: a write that left the child empty
                    // leaves the optional unset, which is what makes an unset
                    // optional an absent key rather than an empty object.
                    if opt.child.as_ref().is_some_and(|child| child.is_default()) {
                        bury(&mut opt.ghost, opt.child.take());
                    }
                    outcome
                }
                OptOp::Unset => {
                    emit.delete();
                    if let Some(child) = opt.child.as_mut() {
                        child.redundant_by_parent(ctx.version, true);
                        child.retire(ctx.id, ctx.version);
                        if child.is_default() {
                            bury(&mut opt.ghost, opt.child.take());
                        }
                    }
                    // What the ghost holds was here as much as the child was,
                    // only empty: the unset removes it the same way, or a
                    // write concurrent with the unset would find it alive on
                    // a replica that delivered the unset first and removed on
                    // one that delivered it second.
                    for object in &mut opt.ghost {
                        object.redundant_by_parent(ctx.version, true);
                        object.retire(ctx.id, ctx.version);
                    }
                    // After the ghost's own reset, so that what joins it now
                    // is not reset twice.
                    shed(opt);
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
            // added beside what it holds, never refused. The operation doing
            // the adding is the one whose id the new object is born with; an
            // operation that finds its class already here mints nothing and
            // changes no `born`; it may still be one more creator, which
            // `written_by` decides from what its writer saw.
            let minted = ensure_variant(slot, class, ctx);
            let object = slot
                .find_mut(class)
                .expect("`ensure_variant` put one of this class here");
            if !minted {
                object.written_by(ctx);
            }
            apply_object(sem, object, ctx.below_class(), *inner, emit)
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
///
/// `born` is the id of the operation that is putting it there, and it is
/// taken by the three arms that mint and by nothing else: an operation that
/// finds its class already here has not created anything, so it leaves the
/// birth identity of what it found alone. That is what makes the second
/// writer of a conflict the parent of *its* object and not of the other one.
/// Answers whether it minted one.
fn ensure_variant(slot: &mut SlotNode, class: ClassSlot, ctx: Ctx<'_>) -> bool {
    match slot {
        SlotNode::Unset => {
            *slot = SlotNode::Value(Box::new(ObjectNode::new(class, ctx)));
            true
        }
        SlotNode::Value(object) => {
            if object.class == class {
                return false;
            }
            let SlotNode::Value(existing) = std::mem::replace(slot, SlotNode::Unset) else {
                unreachable!("matched a moment ago");
            };
            *slot = SlotNode::Conflicts(vec![*existing, ObjectNode::new(class, ctx)]);
            true
        }
        SlotNode::Conflicts(objects) => {
            if objects.iter().any(|object| object.class == class) {
                return false;
            }
            objects.push(ObjectNode::new(class, ctx));
            true
        }
    }
}

/// Keep the objects of a child an optional takes out, in its ghost.
///
/// A leaf child holds no object, so nothing of it is kept. The ghost holds no
/// class the child held (see [`revive`]), so each object goes in beside what
/// is there, in class order.
fn bury(ghost: &mut Vec<ObjectNode>, child: Option<Box<Node>>) {
    let Some(child) = child else {
        return;
    };
    let Node::Slot(slot) = *child else {
        return;
    };
    let objects = match slot {
        SlotNode::Unset => Vec::new(),
        SlotNode::Value(object) => vec![*object],
        SlotNode::Conflicts(objects) => objects,
    };
    for object in objects {
        lay(ghost, object);
    }
}

/// Put one object in the ghost, in class order.
fn lay(ghost: &mut Vec<ObjectNode>, object: ObjectNode) {
    let at = ghost.partition_point(|other| other.class < object.class);
    ghost.insert(at, object);
}

/// Take out of an optional's child every object a removal left empty, into
/// the ghost, and keep the objects that still hold something.
///
/// An emptied child leaves the optional ([`bury`]); this is the same rule one
/// object at a time, so a conflict whose other member a concurrent write kept
/// serves that member alone, whichever of the removal and the write a replica
/// delivered first: an unset that arrives after the write finds the emptied
/// member here, one that arrives before it has already taken the whole child
/// out, and the write brings back only its own class ([`revive`]).
/// `union!` drops the same members under `OptionLog`, so both runtimes serve
/// one document. A leaf child holds no object and is left to its optional.
fn shed(opt: &mut OptNode) {
    let Some(Node::Slot(slot)) = opt.child.as_deref_mut() else {
        return;
    };
    let objects = match std::mem::take(slot) {
        SlotNode::Unset => Vec::new(),
        SlotNode::Value(object) => vec![*object],
        SlotNode::Conflicts(objects) => objects,
    };
    let (emptied, mut kept): (Vec<ObjectNode>, Vec<ObjectNode>) =
        objects.into_iter().partition(ObjectNode::is_default);
    *slot = match kept.len() {
        0 => SlotNode::Unset,
        1 => SlotNode::Value(Box::new(kept.remove(0))),
        _ => SlotNode::Conflicts(kept),
    };
    for object in emptied {
        lay(&mut opt.ghost, object);
    }
}

/// Before a write of a class the child does not hold, take that class's
/// object out of the ghost and put it into the child, where the write would
/// otherwise have minted a fresh one.
///
/// Only the object of the class written: the ghost's other objects stay
/// where they are, since bringing them back would open a conflict no writer
/// asked for. It joins the child the way [`ensure_variant`] would have added
/// a fresh one, after what the child holds, so the child reads as it would
/// have: an object in the ghost is at its default.
fn revive(ghost: &mut Vec<ObjectNode>, child: &mut Node, inner: &InstanceOp) {
    let InstanceOp::Variant(class, _) = inner else {
        return;
    };
    let Node::Slot(slot) = child else {
        return;
    };
    if slot.find(*class).is_some() {
        return;
    }
    let Some(at) = ghost.iter().position(|object| object.class == *class) else {
        return;
    };
    let object = ghost.remove(at);
    match std::mem::take(slot) {
        SlotNode::Unset => *slot = SlotNode::Value(Box::new(object)),
        SlotNode::Value(existing) => *slot = SlotNode::Conflicts(vec![*existing, object]),
        SlotNode::Conflicts(mut objects) => {
            objects.push(object);
            *slot = SlotNode::Conflicts(objects);
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

    use moirai_protocol::{event::id::EventId, replica::IsReplica};
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

    /// Ecore's own `EAnnotation` as a descriptor describes it: an optional
    /// `source`, a `details` map from a key to a register over an optional
    /// text, and the two `EObject`-typed features recorded as unsupported.
    ///
    /// Rooted at the annotation itself, which is what lets a test drive it
    /// without a metamodel around it; the real one, reached through
    /// `EModelElement.eAnnotations`, is what the `annotated` crate's oracle
    /// drives.
    fn annotated() -> std::sync::Arc<MetamodelSemantics> {
        testing::table(&testing::descriptor(
            &["ecore::EAnnotation"],
            vec![(
                "ecore::EAnnotation",
                testing::class(
                    false,
                    &[],
                    vec![
                        testing::optional_text("source"),
                        testing::attribute(
                            "details",
                            serde_json::json!({"kind": "keyed", "key": {"kind": "str"}}),
                            serde_json::json!({"kind": "optionalRegister"}),
                        ),
                        testing::unsupported("contents", "eObjectContainment"),
                        testing::unsupported("references", "eObjectReference"),
                    ],
                ),
            )],
            serde_json::json!({}),
        ))
    }

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

    // ------------------------------------------------------ birth identity

    /// Every object here, in `a`'s replica.
    fn births(
        sem: &MetamodelSemantics,
        replica: &moirai_protocol::replica::Replica<
            Harness,
            moirai_protocol::broadcast::tcsb::Tcsb<InstanceOp>,
        >,
    ) -> (EventId, EventId) {
        let root = object(&replica.state().root);
        let main = object(field(sem, root, "main").expect("the create minted it"));
        (root.born().clone(), main.born().clone())
    }

    #[test]
    fn an_objects_birth_identity_outlives_everything_written_into_it() {
        // What `born` is *for*: it names the operation that created the
        // object and no later one, so an object stays the same object
        // through every edit, every arrival from a peer and every removal
        // that update-wins leaves standing.
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");

        let create = at_root(
            &sem,
            "Root",
            on(
                &sem,
                "Root",
                "main",
                InstanceOp::variant(class_slot(&sem, "Sequence"), InstanceOp::New),
            ),
        );
        let event = a.send(create).unwrap();
        b.receive(event);

        let (root_born, main_born) = births(&sem, &a);
        assert_eq!(
            births(&sem, &b),
            (root_born.clone(), main_born.clone()),
            "the create and its delivery name one birth"
        );

        // A leaf write into the object, a child inserted under it, a second
        // writer's concurrent write into the same object, and the child
        // removed again: every kind of operation that reaches an object that
        // is already there.
        let name = |ch: char| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "main",
                    InstanceOp::variant(
                        class_slot(&sem, "Sequence"),
                        on(&sem, "TreeNode", "ID", append(ch, 0)),
                    ),
                ),
            )
        };
        let under = |op: InstanceOp| {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "main",
                    InstanceOp::variant(
                        class_slot(&sem, "Sequence"),
                        on(&sem, "Sequence", "children", op),
                    ),
                ),
            )
        };

        let a1 = a.send(name('a')).unwrap();
        let a2 = a
            .send(under(InstanceOp::insert(
                0,
                InstanceOp::variant(class_slot(&sem, "Fallback"), InstanceOp::New),
            )))
            .unwrap();
        // Written before b has seen either, so it is concurrent with both.
        let b1 = b.send(name('b')).unwrap();

        b.receive(a1);
        b.receive(a2);
        a.receive(b1);

        let a3 = a.send(under(InstanceOp::delete(0))).unwrap();
        b.receive(a3);

        // Something was actually written, so the claim is not about an
        // untouched object.
        for (who, replica) in [("a", &a), ("b", &b)] {
            let main = object(field(&sem, object(&replica.state().root), "main").unwrap());
            assert_eq!(main.fields().len(), 2, "{who} wrote into the object");
        }

        for (who, replica) in [("a", &a), ("b", &b)] {
            assert_eq!(
                births(&sem, replica),
                (root_born.clone(), main_born.clone()),
                "{who} moved a birth identity that nothing creates"
            );
        }
    }

    #[test]
    fn two_concurrent_creations_of_one_class_collapse_into_one_object_and_two_births() {
        // Named so the limit is a statement and not an omission. Two writers
        // who put the *same* concrete class in one slot at once collapse into
        // one object, which is `ensure_variant`'s rule and what `ip9` asserts
        // for two *different* classes. `born` is minted by whichever creating
        // operation each replica delivered first, so the two replicas hold
        // one object under two birth identities and stay that way.
        //
        // Nothing observes it today: `born` reaches neither the read-out
        // (`eval::read_object` walks `class` and `fields`) nor `is_default`
        // nor any equality, so the four oracles cannot see it and the state
        // the two replicas *read* is identical. It is written down here
        // because the first consumer — an index keyed by identity — would
        // see it immediately, and because the answer is a merge decision
        // (a deterministic pick over the concurrent creators, two objects
        // instead of one, or an identity that is not the creating event) and
        // not a local fix.
        let sem = mini();
        let (mut a, mut b) = twins(&sem, "Root");
        let create = || {
            at_root(
                &sem,
                "Root",
                on(
                    &sem,
                    "Root",
                    "main",
                    InstanceOp::variant(class_slot(&sem, "Sequence"), InstanceOp::New),
                ),
            )
        };

        let a1 = a.send(create()).unwrap();
        let b1 = b.send(create()).unwrap();
        a.receive(b1);
        b.receive(a1);

        // One object, on both, which is the part that is not in question.
        for (who, replica) in [("a", &a), ("b", &b)] {
            let held = objects(field(&sem, object(&replica.state().root), "main").unwrap());
            assert_eq!(held.len(), 1, "{who} kept one object");
        }

        let (root_at_a, main_at_a) = births(&sem, &a);
        let (root_at_b, main_at_b) = births(&sem, &b);
        assert_eq!(root_at_a.origin_id(), "a");
        assert_eq!(root_at_b.origin_id(), "b");
        assert_ne!(root_at_a, root_at_b, "the root object's birth diverges");
        assert_ne!(main_at_a, main_at_b, "and so does the contained object's");
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
        assert!(a.send(put("Fallback", 'x')).is_err());

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
                    .unwrap_or_else(|why| panic!("`{feature}`'s workload is enabled: {why:?}")),
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
            UnsupportedReason::EObjectContainment,
            UnsupportedReason::EObjectReference,
        ] {
            assert_eq!(
                Node::for_rule(&MergeRule::Unsupported { reason }).unwrap_err(),
                reason
            );
        }
    }

    /// A feature typed by `EObject` is refused with the reason and the
    /// sentence that says why, and the sentence is the one a reader of the
    /// descriptor gets: neither path represents an object of any class, and a
    /// typed graph cannot point an arc at any vertex.
    #[test]
    fn an_eobject_feature_is_refused_with_a_sentence_that_says_why() {
        let sem = annotated();
        let annotation = class_slot(&sem, "ecore::EAnnotation");
        for (feature, reason) in [
            ("contents", UnsupportedReason::EObjectContainment),
            ("references", UnsupportedReason::EObjectReference),
        ] {
            let refusal = check(
                &sem,
                None,
                super::Shaped::Bare(Site::Object(Target::Class(annotation))),
                &InstanceOp::variant(
                    annotation,
                    InstanceOp::field(feature_slot(&sem, annotation, feature), InstanceOp::New),
                ),
                &super::At::root(),
                Mode::Local,
            )
            .unwrap_err();
            match &refusal {
                Refusal::Unsupported { reason: got, .. } => assert_eq!(*got, reason),
                other => panic!("{other:?}"),
            }
            let sentence = refusal.to_string();
            assert!(sentence.contains(feature), "{sentence}");
            assert!(sentence.contains("any class"), "{sentence}");
        }
    }

    /// The whole of what Ecore's `EAnnotation.details` needs, driven on two
    /// replicas: concurrent puts on one key keep both values, a key put with
    /// **no** value is still read back, and a removal loses to a concurrent
    /// put.
    ///
    /// The middle one is why the register over an optional value is its own
    /// leaf arm. A keyed collection drops an entry that reads as its default,
    /// so under a plain multi-value register — where `Scalar::Null` is both
    /// "nobody wrote" and "somebody wrote nothing" — the key would vanish,
    /// while the generated `HashSet<Option<String>>` holding `{None}` keeps
    /// it.
    #[test]
    fn a_details_map_keeps_a_key_whose_value_is_absent() {
        let sem = annotated();
        let (mut a, mut b) = twins(&sem, "ecore::EAnnotation");
        let put = |key: &str, value: Option<&str>| {
            at_root(
                &sem,
                "ecore::EAnnotation",
                on(
                    &sem,
                    "ecore::EAnnotation",
                    "details",
                    InstanceOp::entry(
                        Scalar::text(key),
                        InstanceOp::Leaf(LeafOp::Write(match value {
                            Some(value) => Scalar::text(value),
                            None => Scalar::Null,
                        })),
                    ),
                ),
            )
        };
        let remove = |key: &str| {
            at_root(
                &sem,
                "ecore::EAnnotation",
                on(
                    &sem,
                    "ecore::EAnnotation",
                    "details",
                    InstanceOp::remove(Scalar::text(key)),
                ),
            )
        };

        // A key with no value, delivered everywhere.
        let event = a.send(put("bare", None)).unwrap();
        b.receive(event);
        // A key that is removed against a concurrent put.
        let event = a.send(put("contested", Some("x"))).unwrap();
        b.receive(event);
        // A key that is removed with nothing concurrent.
        let event = a.send(put("gone", Some("x"))).unwrap();
        b.receive(event);
        let event = a.send(remove("gone")).unwrap();
        b.receive(event);

        let from_a = a.send(put("keyword", Some("left"))).unwrap();
        let from_b = b.send(put("keyword", Some("right"))).unwrap();
        let removed = a.send(remove("contested")).unwrap();
        let written = b.send(put("contested", Some("y"))).unwrap();
        a.receive(from_b);
        a.receive(written);
        b.receive(from_a);
        b.receive(removed);

        for (who, replica) in [("a", &a), ("b", &b)] {
            let read = crate::eval::read(&sem, &replica.state().root);
            assert_eq!(
                read["details"],
                serde_json::json!({
                    "bare": [null],
                    "contested": ["y"],
                    "keyword": ["left", "right"],
                }),
                "{who}"
            );
        }
    }

    #[test]
    fn an_ordered_set_is_minted_as_the_one_log_the_generator_emits() {
        // `attribute.rs:152-162` emits `GraphLog<List<V>>` for a unique
        // ordered attribute, which is one CRDT over the whole collection and
        // not a nested list of leaf logs. The node is the matching leaf, so a
        // `Shaped::Sequence` here would be the old degradation coming back.
        let rule = MergeRule::Attribute {
            shape: Shape::OrderedSet,
            leaf: LeafRule::Text,
        };
        assert!(matches!(
            shaped(&rule).unwrap(),
            super::Shaped::Bare(Site::Leaf(super::LeafSite::OrderedSet))
        ));
        assert!(matches!(
            Node::for_rule(&rule).unwrap(),
            Node::Leaf(crate::leaf::LeafLog::OrderedSet(_))
        ));
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
            NumKind::I8,
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
