//! An operation generator, so `moirai-fuzz` runs over the interpreted log
//! unchanged.
//!
//! `fuzzer::<ModelLog, _>` (`moirai-fuzz/src/fuzzer.rs:22-28`) asks a log for an
//! operation and then *sends* it, unwrapping the result: a generated
//! operation that `is_enabled` would refuse crashes the run rather than
//! failing it. So this generator produces only table-valid, state-valid
//! operations, which makes it the second implementation of the rules
//! [`crate::node::check`] holds a local writer to — and a disagreement
//! between the two shows up as a panic in the first hundred operations rather
//! than as a subtle bias.
//!
//! The descriptor an [`crate::op::ModelOp::Install`] carries is set by the
//! caller through [`set_descriptor`], because a log with no table has no way
//! to know which metamodel it is about to be opened on.

use std::cell::RefCell;

use deepsize::{Context, DeepSizeOf};
use moirai_fuzz::op_generator::CommandGenerator;
use moirai_semantics::{
    ClassSlot, FeatureSlot, LeafRule, MetamodelSemantics, TieBreak, metamodel_digest,
};
use rand::{Rng, RngExt};

use crate::leaf::{LeafLog, LeafOp, Scalar};
use crate::log::ModelLog;
use crate::node::{
    LeafSite, MapNode, Node, ObjectNode, OptNode, SeqNode, Shaped, Site, SlotNode, Target, shaped,
    visible,
};
use crate::op::{InstanceOp, MapOp, ModelOp, OptOp, SeqOp};
use crate::refs::RefNode;

thread_local! {
    static DESCRIPTOR: RefCell<Option<(String, String)>> = const { RefCell::new(None) };
}

/// The descriptor every generated [`crate::op::ModelOp::Install`] carries.
///
/// Thread-local because `moirai-fuzz`'s runner builds and drives all of a
/// run's replicas in the calling thread, exactly as its `set_disable_stability`
/// assumes.
pub fn set_descriptor(text: &str) {
    let digest = serde_json::from_str::<serde_json::Value>(text)
        .map(|value| metamodel_digest(&value))
        .unwrap_or_default();
    DESCRIPTOR.with_borrow_mut(|slot| *slot = Some((text.to_string(), digest)));
}

/// How deep one generated operation may reach.
///
/// Bounds the work per operation and, with it, how deep the model grows: an
/// insert below this depth is never generated, so a thousand operations build
/// a wide tree rather than a thousand-deep one.
const BUDGET: u32 = 6;

/// A small closed pool, so a run's values collide often enough to exercise
/// the merge rules rather than filling every set with singletons.
const WORDS: [&str; 6] = ["alpha", "beta", "gamma", "delta", "door", "room"];

impl CommandGenerator for ModelLog {
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        let Some(sem) = self.semantics() else {
            let (descriptor, digest) = DESCRIPTOR
                .with_borrow(Clone::clone)
                .expect("`set_descriptor` before fuzzing an interpreted log");
            return ModelOp::Install {
                model_id: "fuzz".to_string(),
                metamodel_id: digest,
                descriptor,
            };
        };
        let op = site_op(
            sem,
            Some(self.root()),
            Site::Object(Target::Roots),
            rng,
            BUDGET,
        );
        debug_assert!(
            self.refusal(&ModelOp::Instance(op.clone())).is_ok(),
            "the generator produced an operation the log refuses: {:?}",
            self.refusal(&ModelOp::Instance(op.clone()))
        );
        ModelOp::Instance(op)
    }
}

/// One operation at a site holding a single value.
fn site_op(
    sem: &MetamodelSemantics,
    node: Option<&Node>,
    site: Site,
    rng: &mut impl Rng,
    budget: u32,
) -> InstanceOp {
    match site {
        Site::Leaf(leaf) => InstanceOp::Leaf(leaf_op(sem, leaf_of(node), leaf, rng)),
        // An identity reference takes a link of any identity: one nobody
        // serves is held and never read.
        Site::Refs { .. } => InstanceOp::Leaf(LeafOp::Link(
            WORDS[rng.random_range(0..WORDS.len())].to_string(),
        )),
        Site::Object(target) => {
            let slot = match node {
                Some(Node::Slot(slot)) => Some(slot),
                _ => None,
            };
            // The objects `check` holds a local writer to: at a family slot
            // the present ones only, so a slot whose members are all absent
            // is an empty slot here as it is there.
            let held: Vec<&ObjectNode> = slot
                .map(|slot| slot.served(target.is_family(sem)))
                .unwrap_or_default();
            // A class this replica has already put here, or a new one when it
            // has put none: writing a second class locally is what
            // `union.rs:130-145` refuses, so the generator does not try.
            let (class, object) = if held.is_empty() {
                let allowed = target.allowed_slots(sem);
                let class = allowed[rng.random_range(0..allowed.len())];
                (class, None)
            } else {
                let object = held[rng.random_range(0..held.len())];
                (object.class(), Some(object))
            };
            InstanceOp::variant(class, object_op(sem, object, class, rng, budget))
        }
    }
}

/// One operation on one object.
fn object_op(
    sem: &MetamodelSemantics,
    object: Option<&ObjectNode>,
    class: ClassSlot,
    rng: &mut impl Rng,
    budget: u32,
) -> InstanceOp {
    let empty = object.is_none_or(|object| object.fields().values().all(Node::is_default));
    let Some(holder) = sem.classes.get(class.index()) else {
        return InstanceOp::New;
    };

    // Every feature this class can see that the interpreted path merges,
    // with the leaves first when the budget is spent.
    let mut features: Vec<(FeatureSlot, Shaped)> = Vec::new();
    for slot in 0..holder.visible.len() {
        let slot = FeatureSlot(slot as u16);
        if let Some((_, rule)) = visible(sem, class, slot)
            && let Ok(shape) = shaped(rule)
        {
            features.push((slot, shape));
        }
    }
    if features.is_empty() || (empty && rng.random_range(0..8) == 0) {
        // `New` is enabled on an empty object only, which is exactly when
        // this branch is taken.
        return if empty {
            InstanceOp::New
        } else {
            InstanceOp::Leaf(LeafOp::Clear)
        };
    }
    if budget == 0 {
        // Prefer something that ends here.
        if let Some((slot, shape)) = features
            .iter()
            .find(|(_, shape)| matches!(shape, Shaped::Bare(Site::Leaf(_))))
            .copied()
        {
            return InstanceOp::field(slot, shaped_op(sem, field_of(object, slot), shape, rng, 0));
        }
    }
    let (slot, shape) = features[rng.random_range(0..features.len())];
    InstanceOp::field(
        slot,
        shaped_op(sem, field_of(object, slot), shape, rng, budget),
    )
}

/// One operation on one feature, at whatever collection it is.
fn shaped_op(
    sem: &MetamodelSemantics,
    node: Option<&Node>,
    shape: Shaped,
    rng: &mut impl Rng,
    budget: u32,
) -> InstanceOp {
    let next = budget.saturating_sub(1);
    match shape {
        Shaped::Bare(site) => site_op(sem, node, site, rng, next),
        Shaped::Optional(site) => {
            let child = match node {
                Some(Node::Opt(opt)) => opt.child(),
                _ => None,
            };
            match child {
                Some(child) if budget == 0 || rng.random_range(0..5) == 0 => {
                    let _ = child;
                    InstanceOp::unset()
                }
                Some(child) => InstanceOp::set(site_op(sem, Some(child), site, rng, next)),
                None => InstanceOp::set(site_op(sem, None, site, rng, next)),
            }
        }
        Shaped::Keyed(site) => {
            let map = match node {
                Some(Node::Map(map)) => Some(map),
                _ => None,
            };
            let keys: Vec<Scalar> = map
                .map(|map| map.children().keys().cloned().collect())
                .unwrap_or_default();
            // A key drawn from a fixed alphabet, so two replicas writing
            // independently do collide on one entry, which is the whole
            // question a keyed container has to answer.
            let fresh = Scalar::text(WORDS[rng.random_range(0..WORDS.len())]);
            match rng.random_range(0..10) {
                0 if !keys.is_empty() => {
                    InstanceOp::remove(keys[rng.random_range(0..keys.len())].clone())
                }
                1 => InstanceOp::clear(),
                2..=4 if budget > 0 || keys.is_empty() => {
                    // The word may be a key the map holds: its value is then
                    // written as the existing-key arm writes one, against the
                    // child, so that a held key is offered only a class it
                    // serves and a non-empty object is never offered `New`.
                    let child = map.and_then(|map| map.children().get(&fresh));
                    InstanceOp::entry(fresh, site_op(sem, child, site, rng, next))
                }
                _ if keys.is_empty() => {
                    InstanceOp::entry(fresh, site_op(sem, None, site, rng, next))
                }
                _ => {
                    let key = keys[rng.random_range(0..keys.len())].clone();
                    let child = map.and_then(|map| map.children().get(&key));
                    InstanceOp::entry(key, site_op(sem, child, site, rng, next))
                }
            }
        }
        Shaped::Sequence(site) => {
            let seq = match node {
                Some(Node::Seq(seq)) => Some(seq),
                _ => None,
            };
            let order = seq.map(|seq| seq.order()).unwrap_or_default();
            if order.is_empty() {
                return InstanceOp::insert(0, site_op(sem, None, site, rng, next));
            }
            match rng.random_range(0..8) {
                0 => InstanceOp::delete(rng.random_range(0..order.len())),
                1..=2 if budget > 0 => InstanceOp::insert(
                    rng.random_range(0..=order.len()),
                    site_op(sem, None, site, rng, next),
                ),
                _ => {
                    let pos = rng.random_range(0..order.len());
                    let child = seq.and_then(|seq| seq.children().get(&order[pos]));
                    InstanceOp::at(pos, site_op(sem, child, site, rng, next))
                }
            }
        }
    }
}

/// The node behind one visible slot, if it has been minted.
fn field_of(object: Option<&ObjectNode>, slot: FeatureSlot) -> Option<&Node> {
    object.and_then(|object| object.fields().get(&slot))
}

/// The leaf a node is, if it is one.
fn leaf_of(node: Option<&Node>) -> Option<&LeafLog> {
    match node {
        Some(Node::Leaf(leaf)) => Some(leaf),
        _ => None,
    }
}

/// One write to one leaf, of a kind that leaf takes and at a position it
/// holds.
fn leaf_op(
    sem: &MetamodelSemantics,
    leaf: Option<&LeafLog>,
    site: LeafSite,
    rng: &mut impl Rng,
) -> LeafOp {
    let word = Scalar::text(WORDS[rng.random_range(0..WORDS.len())]);
    match site {
        LeafSite::Scalar(LeafRule::Text) => {
            let len = leaf
                .and_then(|leaf| {
                    leaf.read_json(None)
                        .as_str()
                        .map(|text| text.chars().count())
                })
                .unwrap_or(0);
            if len > 0 && rng.random_range(0..4) == 0 {
                LeafOp::DeleteChar {
                    pos: rng.random_range(0..len),
                }
            } else {
                LeafOp::InsertChar {
                    pos: rng.random_range(0..=len),
                    ch: char::from(b'a' + rng.random_range(0..26u8)),
                }
            }
        }
        // Every numeric width Ecore has is signed, `EByte` included since it
        // is `i8` and no longer `u8`, so a decrement below zero is in range at
        // all six and no width is held back from one.
        LeafSite::Scalar(LeafRule::Counter { resettable, .. }) => {
            let step = i64::from(rng.random_range(0..3u8));
            match rng.random_range(0..6) {
                0 if resettable => LeafOp::Reset,
                1..=2 => LeafOp::Dec(Scalar::Int(step)),
                _ => LeafOp::Inc(Scalar::Int(step)),
            }
        }
        LeafSite::Scalar(LeafRule::Flag { .. }) => match rng.random_range(0..5) {
            0 => LeafOp::Clear,
            1..=2 => LeafOp::Disable,
            _ => LeafOp::Enable,
        },
        LeafSite::Scalar(LeafRule::Register { tie }) => {
            // `LwwRegister` and `FairRegister` are `Register<V, P>`, which has
            // one operation and no `Clear`.
            let clearable = !matches!(tie, TieBreak::LastWriterWins | TieBreak::Fair);
            if clearable && rng.random_range(0..8) == 0 {
                LeafOp::Clear
            } else {
                LeafOp::Write(word)
            }
        }
        LeafSite::Scalar(LeafRule::Enum { class, tie }) => {
            let clearable = !matches!(tie, TieBreak::LastWriterWins | TieBreak::Fair);
            if clearable && rng.random_range(0..8) == 0 {
                LeafOp::Clear
            } else {
                LeafOp::Write(literal(sem, class, rng))
            }
        }
        LeafSite::Scalar(LeafRule::OptionalRegister { class }) => {
            // An `MVRegister`, which takes `Write` and `Clear` whatever its
            // value; a literal of its enum when the value feature has one.
            // Its value is `Option<T>`, so on one draw in eight it writes
            // `Scalar::Null`, the absent value, in place of the one drawn:
            // the draws are the same as when it writes the value.
            let draw = rng.random_range(0..8);
            if draw == 0 {
                LeafOp::Clear
            } else {
                let value = match class {
                    Some(class) => literal(sem, class, rng),
                    None => word,
                };
                LeafOp::Write(if draw == 1 { Scalar::Null } else { value })
            }
        }
        LeafSite::Set(_) | LeafSite::Bag => match rng.random_range(0..8) {
            0 => LeafOp::Clear,
            1..=2 => LeafOp::Remove(word),
            _ => LeafOp::Add(word),
        },
        LeafSite::OrderedSet => {
            let len = leaf
                .and_then(|leaf| {
                    leaf.read_json(None)
                        .as_array()
                        .map(|elements| elements.len())
                })
                .unwrap_or(0);
            if len > 0 && rng.random_range(0..4) == 0 {
                LeafOp::DeleteChar {
                    pos: rng.random_range(0..len),
                }
            } else {
                LeafOp::Insert {
                    pos: rng.random_range(0..=len),
                    value: word,
                }
            }
        }
    }
}

/// One literal of the enum at `class` (its slot in the table's `enums`),
/// drawn uniformly; literal 0 when the table does not know the enum or it
/// has no literal.
fn literal(sem: &MetamodelSemantics, class: ClassSlot, rng: &mut impl Rng) -> Scalar {
    let count = sem
        .enums
        .get(class.index())
        .map_or(1, |entry| entry.literals.len().max(1));
    Scalar::Enum(class.0, rng.random_range(0..count) as u16)
}

/// `fuzzer` requires a size estimate of the log and of its operations, which
/// it reads only when a run samples memory (`memory_sample_interval`).
///
/// Counted: the header's strings, the descriptor's bytes, the parsed table
/// and the model tree, every container and every leaf log in it, with each
/// object's creations and touches and the removals that cover them.
///
/// The table is counted through its `Arc`, never through the pointee, so a
/// walk counts it once however many logs it meets that share it: each
/// replica's own sample counts its table, and a walk over two clones of one
/// log counts their shared table once.
///
/// An object's fields and a map's entries are charged on one basis: each
/// entry its inline size, `size_of::<(K, V)>()`, plus what its key and its
/// value hold (see [`entries`]).
impl DeepSizeOf for ModelLog {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        let header = self.header().map_or(0, |header| {
            header.model_id.deep_size_of_children(context)
                + header.metamodel_id.deep_size_of_children(context)
        });
        let descriptor = self.descriptor().map_or(0, str::len);
        let table = self
            .semantics()
            .map_or(0, |sem| sem.deep_size_of_children(context));
        header + descriptor + table + self.root().deep_size_of_children(context)
    }
}

/// What the entries of a map hold, on one basis whatever the map's type:
/// each entry its inline size, `size_of::<(K, V)>()`, plus its key's and its
/// value's heap.
///
/// Not `deepsize`'s `BTreeMap` charge, which comes to about five eighths of
/// one entry per entry, below what a B-tree leaf holds for it.
fn entries<'a, K, V>(map: impl IntoIterator<Item = (&'a K, &'a V)>, context: &mut Context) -> usize
where
    K: DeepSizeOf + 'a,
    V: DeepSizeOf + 'a,
{
    map.into_iter()
        .map(|(key, value)| {
            std::mem::size_of::<(K, V)>()
                + key.deep_size_of_children(context)
                + value.deep_size_of_children(context)
        })
        .sum()
}

/// `Install`'s strings, and an instance operation's boxes and the values it
/// carries.
impl DeepSizeOf for ModelOp {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            ModelOp::Install {
                model_id,
                metamodel_id,
                descriptor,
            } => {
                model_id.deep_size_of_children(context)
                    + metamodel_id.deep_size_of_children(context)
                    + descriptor.deep_size_of_children(context)
            }
            ModelOp::Instance(op) => op.deep_size_of_children(context),
        }
    }
}

impl DeepSizeOf for InstanceOp {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            InstanceOp::Field(_, inner) | InstanceOp::Variant(_, inner) => {
                inner.deep_size_of_children(context)
            }
            InstanceOp::Seq(SeqOp::Insert { op, .. } | SeqOp::Update { op, .. })
            | InstanceOp::Opt(OptOp::Set(op)) => op.deep_size_of_children(context),
            InstanceOp::Map(MapOp::Update { key, op }) => {
                key.deep_size_of_children(context) + op.deep_size_of_children(context)
            }
            InstanceOp::Map(MapOp::Remove { key }) => key.deep_size_of_children(context),
            InstanceOp::Seq(SeqOp::Delete { .. })
            | InstanceOp::Opt(OptOp::Unset)
            | InstanceOp::Map(MapOp::Clear)
            | InstanceOp::New => 0,
            InstanceOp::Leaf(leaf) => leaf.deep_size_of_children(context),
        }
    }
}

impl DeepSizeOf for LeafOp {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            LeafOp::Insert { value, .. }
            | LeafOp::Inc(value)
            | LeafOp::Dec(value)
            | LeafOp::Write(value)
            | LeafOp::Add(value)
            | LeafOp::Remove(value) => value.deep_size_of_children(context),
            LeafOp::Link(target) | LeafOp::Unlink(target) => target.deep_size_of_children(context),
            LeafOp::InsertChar { .. }
            | LeafOp::DeleteChar { .. }
            | LeafOp::DeleteRange { .. }
            | LeafOp::Reset
            | LeafOp::Enable
            | LeafOp::Disable
            | LeafOp::Clear => 0,
        }
    }
}

impl DeepSizeOf for Node {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            Node::Unbound => 0,
            Node::Slot(slot) => slot.deep_size_of_children(context),
            Node::Seq(seq) => seq.deep_size_of_children(context),
            Node::Opt(opt) => opt.deep_size_of_children(context),
            Node::Map(map) => map.deep_size_of_children(context),
            Node::Leaf(leaf) => leaf.deep_size_of_children(context),
            Node::Refs(refs) => refs.deep_size_of_children(context),
        }
    }
}

impl DeepSizeOf for SlotNode {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            SlotNode::Unset => 0,
            SlotNode::Value(object) => object.deep_size_of_children(context),
            SlotNode::Conflicts(objects) => objects.deep_size_of_children(context),
        }
    }
}

impl DeepSizeOf for ObjectNode {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.born.deep_size_of_children(context)
            + self.creators.deep_size_of_children(context)
            + self.touches.deep_size_of_children(context)
            + entries(&self.fields, context)
    }
}

impl DeepSizeOf for SeqNode {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.positions.deep_size_of_children(context) + self.children.deep_size_of_children(context)
    }
}

impl DeepSizeOf for OptNode {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.child.deep_size_of_children(context) + self.ghost.deep_size_of_children(context)
    }
}

impl DeepSizeOf for MapNode {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        entries(&self.children, context)
    }
}

impl DeepSizeOf for RefNode {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.links
            .iter()
            .map(|link| {
                link.target.deep_size_of_children(context) + link.id.deep_size_of_children(context)
            })
            .sum::<usize>()
            + self.links.capacity() * std::mem::size_of::<crate::refs::Link>()
    }
}

impl DeepSizeOf for LeafLog {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            LeafLog::Text(log) => log.deep_size_of_children(context),
            LeafLog::OrderedSet(log) => log.deep_size_of_children(context),
            LeafLog::CounterI8(log) => log.deep_size_of_children(context),
            LeafLog::CounterI16(log) => log.deep_size_of_children(context),
            LeafLog::CounterI32(log) => log.deep_size_of_children(context),
            LeafLog::CounterI64(log) => log.deep_size_of_children(context),
            LeafLog::CounterF32(log) => log.deep_size_of_children(context),
            LeafLog::CounterF64(log) => log.deep_size_of_children(context),
            LeafLog::SimpleCounterI8(log) => log.deep_size_of_children(context),
            LeafLog::SimpleCounterI16(log) => log.deep_size_of_children(context),
            LeafLog::SimpleCounterI32(log) => log.deep_size_of_children(context),
            LeafLog::SimpleCounterI64(log) => log.deep_size_of_children(context),
            LeafLog::SimpleCounterF32(log) => log.deep_size_of_children(context),
            LeafLog::SimpleCounterF64(log) => log.deep_size_of_children(context),
            LeafLog::FlagEw(log) => log.deep_size_of_children(context),
            LeafLog::FlagDw(log) => log.deep_size_of_children(context),
            LeafLog::RegisterMv(log) | LeafLog::RegisterOptional(log) => {
                log.deep_size_of_children(context)
            }
            LeafLog::RegisterLww(log) => log.deep_size_of_children(context),
            LeafLog::RegisterFair(log) => log.deep_size_of_children(context),
            LeafLog::RegisterPo(log) => log.deep_size_of_children(context),
            LeafLog::RegisterTo(log) => log.deep_size_of_children(context),
            LeafLog::SetAw(log) => log.deep_size_of_children(context),
            LeafLog::SetRw(log) => log.deep_size_of_children(context),
            LeafLog::Bag(log) => log.deep_size_of_children(context),
        }
    }
}
