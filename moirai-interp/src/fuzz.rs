//! An operation generator, so `moirai-fuzz` runs over the interpreted log
//! unchanged.
//!
//! `fuzzer::<ModelLog>` (`moirai-fuzz/src/fuzzer.rs:22-25`) asks a log for an
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

use moirai_fuzz::{
    metrics::{FuzzMetrics, StructureMetrics},
    op_generator::OpGeneratorNested,
};
use moirai_semantics::{
    ClassSlot, FeatureSlot, LeafRule, MetamodelSemantics, NumKind, TieBreak, metamodel_digest,
};
use rand::{Rng, RngExt};

use crate::leaf::{LeafLog, LeafOp, Scalar};
use crate::log::ModelLog;
use crate::node::{LeafSite, Node, ObjectNode, Shaped, Site, Target, shaped, visible};
use crate::op::{InstanceOp, ModelOp};

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

impl OpGeneratorNested for ModelLog {
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
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
        Site::Object(target) => {
            let slot = match node {
                Some(Node::Slot(slot)) => Some(slot),
                _ => None,
            };
            let held: Vec<&ObjectNode> = slot.map(|slot| slot.objects()).unwrap_or_default();
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
        LeafSite::Scalar(LeafRule::Counter { num, resettable }) => {
            let step = i64::from(rng.random_range(0..3u8));
            match rng.random_range(0..6) {
                0 if resettable => LeafOp::Reset,
                // `EByte` is unsigned, so a decrement below zero is the panic
                // the generated path would take too; the fuzzer's business is
                // convergence and not that.
                1..=2 if !matches!(num, NumKind::U8) => LeafOp::Dec(Scalar::Int(step)),
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
            let literals = sem
                .enums
                .get(class.index())
                .map_or(1, |entry| entry.literals.len().max(1));
            let clearable = !matches!(tie, TieBreak::LastWriterWins | TieBreak::Fair);
            if clearable && rng.random_range(0..8) == 0 {
                LeafOp::Clear
            } else {
                LeafOp::Write(Scalar::Enum(class.0, rng.random_range(0..literals) as u16))
            }
        }
        LeafSite::Set(_) | LeafSite::Bag => match rng.random_range(0..8) {
            0 => LeafOp::Clear,
            1..=2 => LeafOp::Remove(word),
            _ => LeafOp::Add(word),
        },
    }
}

impl FuzzMetrics for ModelLog {
    fn structure_metrics(&self) -> StructureMetrics {
        node_metrics(self.root())
    }
}

fn node_metrics(node: &Node) -> StructureMetrics {
    match node {
        Node::Unbound => StructureMetrics::empty(),
        Node::Leaf(_) => StructureMetrics::scalar(),
        Node::Opt(opt) => opt
            .child()
            .map_or_else(StructureMetrics::empty, node_metrics),
        Node::Seq(seq) => StructureMetrics::object(seq.children().values().map(node_metrics)),
        Node::Slot(slot) => StructureMetrics::object(
            slot.objects()
                .into_iter()
                .map(|object| StructureMetrics::object(object.fields().values().map(node_metrics))),
        ),
    }
}

#[cfg(test)]
mod tests {
    //! `ip12`: three replicas, a thousand operations, ten seeds, over the
    //! behaviour tree's own table, every run converging.

    use moirai_fuzz::{
        config::{FuzzerConfig, RunConfig},
        fuzzer::fuzzer,
    };

    use super::set_descriptor;
    use crate::log::ModelLog;
    use crate::testing::BT_DESCRIPTOR;

    /// A seed a reader can reproduce: the run's number, written into the
    /// first byte and its complement into the last.
    fn seed(run: u8) -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = run;
        seed[31] = !run;
        seed
    }

    #[test]
    fn ip12_three_replicas_a_thousand_operations_ten_seeds_all_converge() {
        set_descriptor(BT_DESCRIPTOR);
        let runs: Vec<RunConfig> = (0..10)
            .map(|run| RunConfig::new(0.4, 3, 1_000, None, Some(seed(run)), false, false))
            .collect();
        // `fuzzer` panics on a run that does not converge, on a generated
        // operation the log refuses, and on a replica that ends with a
        // different number of events; there is nothing left for this test to
        // assert that it does not assert itself.
        let config = FuzzerConfig::<ModelLog>::new(
            "interp-bt",
            runs,
            true,
            |left, right| left == right,
            false,
            None,
        );
        fuzzer::<ModelLog>(config);
    }

    #[test]
    fn a_generated_operation_is_always_one_the_log_would_accept() {
        // The narrow version of the same claim, without the fuzzer's own
        // machinery in the way: the generator and `check`'s local mode agree
        // about every operation, over one replica and a thousand draws.
        use moirai_fuzz::op_generator::OpGeneratorNested;
        use moirai_protocol::{replica::IsReplica, state::log::IsLog};
        use rand::SeedableRng;

        set_descriptor(BT_DESCRIPTOR);
        let (mut a, _b) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
        let mut rng = rand_chacha::ChaCha8Rng::from_seed(seed(42));

        for step in 0..1_000 {
            let op = a.state().generate(&mut rng);
            assert!(
                a.state().is_enabled(&op),
                "step {step}: {:?}",
                a.state().refusal(&op)
            );
            a.send(op).expect("and the replica takes it");
        }
        assert_eq!(a.state().unresolved(), 0);
    }
}
