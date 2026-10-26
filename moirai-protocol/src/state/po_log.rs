use std::{cmp::Ordering, fmt::Debug};

use rustc_hash::FxHashMap;

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::{Eval, EvalNested},
        query::QueryOperation,
        redundancy::RedundancyRelation,
        replicated_data_type::{
            CausalReset, ReplicatedDataType, Stabilization, UsesUnstableService,
        },
    },
    event::{Event, id::EventId, lamport::Lamport, tagged_op::TaggedOp},
    state::{
        effect_context::EffectContext, event_graph::EventGraph, log::IsLog,
        stable_state::IsStableState, unstable_state::IsUnstablePrune,
    },
};

pub type VecLog<O> = POLog<O, Vec<TaggedOp<O>>>;
pub type MapLog<O> = POLog<O, FxHashMap<EventId, TaggedOp<O>>>;
pub type GraphLog<O> = POLog<O, EventGraph<O>>;

#[derive(Debug, Clone)]
pub struct POLog<O, U>
where
    O: ReplicatedDataType,
{
    pub(crate) stable: O::StableState,
    pub(crate) unstable: U,
}

impl<O, U> IsLog for POLog<O, U>
where
    O: ReplicatedDataType + Clone + UsesUnstableService<U>,
    U: IsUnstablePrune<O> + Default + Debug,
{
    type Command = O;
    type Op = O;
    type Rejection = O::Rejection;

    fn new() -> Self {
        Self {
            stable: O::StableState::default(),
            unstable: U::default(),
        }
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        command
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        <O as UsesUnstableService<U>>::is_enabled(op, &self.stable, &self.unstable)
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        let new_tagged_op = TaggedOp::from(&event);
        if O::redundant_itself(&new_tagged_op, &self.stable, self.unstable.iter()) {
            if !O::DISABLE_R_WHEN_R {
                self.prune_redundant_ops(
                    O::redundant_by_when_redundant,
                    &new_tagged_op,
                    event.version(),
                );
            }
        } else {
            if !O::DISABLE_R_WHEN_NOT_R {
                self.prune_redundant_ops(
                    O::redundant_by_when_not_redundant,
                    &new_tagged_op,
                    event.version(),
                );
            }
            self.unstable.append(event);
        }
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        match O::causal_stabilize(frontier, &self.stable, &self.unstable) {
            Stabilization::Regular => {
                // 1. select all ops in unstable that are predecessors of a version
                // 2. for each of them, call stabilize, which may modify stable and/or unstable
                // 3. if the operation is still in unstable, apply the op to stable and remove it from unstable

                let candidates = self
                    .unstable
                    .iter()
                    .filter(|tagged_op| frontier.contains(tagged_op.id()))
                    .cloned()
                    .collect::<Vec<_>>();

                for tagged_op in candidates {
                    O::on_stabilize(&tagged_op, &mut self.stable, &mut self.unstable);
                    if self.unstable.get(tagged_op.id()).is_some() {
                        self.stable.apply(tagged_op.op().clone());
                        self.unstable.remove(tagged_op.id());
                    }
                }
            }
            Stabilization::Critical {
                frontier: cut,
                stable,
            } => {
                assert!(
                    matches!(
                        cut.partial_cmp(frontier),
                        Some(Ordering::Less | Ordering::Equal)
                    ),
                    "Critical frontier exceeds the stable one"
                );
                self.unstable.prune_predecessors(&cut);
                self.stable = stable;
            }
            Stabilization::None => {}
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match <O as UsesUnstableService<U>>::causal_reset(
            version,
            conservative,
            &self.stable,
            &self.unstable,
        ) {
            CausalReset::Inject(ops) => {
                for op in ops {
                    let event_id = EventId::from(version);
                    let lamport = Lamport::from(version);
                    let event = Event::new(event_id, lamport, op, version.clone());
                    self.unstable.append(event);
                }
            }
            CausalReset::Prune => {
                self.stable.clear();
                if conservative {
                    self.unstable
                        .retain(|tagged_op| !tagged_op.id().is_predecessor_of(version))
                } else {
                    self.unstable.clear();
                }
            }
        }
    }

    // TODO: we must decide if this function is semantic or structural, i.e. if it returns true if the log is semantically equivalent to the default value,
    // TODO: or if it returns true if the log is empty.
    fn is_default(&self) -> bool {
        self.stable.is_default() && self.unstable.is_empty()
    }
}

impl<O, U> Default for POLog<O, U>
where
    O: ReplicatedDataType,
    U: Default,
{
    fn default() -> Self {
        Self {
            stable: Default::default(),
            unstable: Default::default(),
        }
    }
}

impl<O, U> POLog<O, U>
where
    O: ReplicatedDataType,
{
    /// Construct a log from a stable state and its remaining unstable events.
    pub fn from_parts(stable: O::StableState, unstable: U) -> Self {
        Self { stable, unstable }
    }

    /// Start from a stable state with no prior causal history.
    pub fn from_stable(stable: O::StableState) -> Self
    where
        U: Default,
    {
        Self::from_parts(stable, U::default())
    }

    pub fn stable(&self) -> &O::StableState {
        &self.stable
    }

    pub fn unstable(&self) -> &U {
        &self.unstable
    }
}

impl<O, U> POLog<O, U>
where
    O: ReplicatedDataType,
    U: IsUnstablePrune<O>,
{
    fn prune_redundant_ops(
        &mut self,
        rdnt: RedundancyRelation<O>,
        new_tagged_op: &TaggedOp<O>,
        version: &Version,
    ) {
        self.stable.prune_redundant_ops(rdnt, new_tagged_op);
        self.unstable.retain(|old_tagged_op| {
            // Note: the new operation is not in the log at this point.
            let is_conc = !old_tagged_op.id().is_predecessor_of(version);

            !rdnt(
                old_tagged_op.op(),
                Some(old_tagged_op.tag()),
                is_conc,
                new_tagged_op,
            )
        });
    }
}

impl<Q, O, U> EvalNested<Q> for POLog<O, U>
where
    Q: QueryOperation,
    O: ReplicatedDataType + Clone + Debug + Eval<Q, U> + UsesUnstableService<U>,
    U: IsUnstablePrune<O> + Default + Debug,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        O::execute_query(q, &self.stable, &self.unstable)
    }
}

#[cfg(feature = "test_utils")]
impl<O, U> deepsize::DeepSizeOf for POLog<O, U>
where
    O: ReplicatedDataType,
    O::StableState: deepsize::DeepSizeOf,
    U: deepsize::DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.stable.deep_size_of_children(context) + self.unstable.deep_size_of_children(context)
    }
}
