use std::fmt::{self, Debug};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::{Eval, EvalNested},
        query::QueryOperation,
        redundancy::RedundancyRelation,
        replicated_data_type::{
            CausalCompaction, CausalReset, CausalStabilize, Precondition, ReplicatedDataType,
            Reset, Sequential, Stabilization, Stable,
        },
    },
    event::{Event, id::EventId, lamport::Lamport, tagged_op::TaggedOp},
    state::{
        effect_context::EffectContext,
        log::IsLog,
        unstable_state::{IsUnstableCore, IsUnstablePrune},
    },
};

pub type VecLogLamport<R> = POLogLamport<R, Vec<TaggedOp<<R as ReplicatedDataType>::Update>>>;

pub struct POLogLamport<R, U>
where
    R: ReplicatedDataType,
{
    pub(crate) stable: Stable<R>,
    pub(crate) unstable: U,
    last_lamport: usize,
}

impl<R, U> Clone for POLogLamport<R, U>
where
    R: ReplicatedDataType,
    Stable<R>: Clone,
    U: Clone,
{
    fn clone(&self) -> Self {
        Self {
            stable: self.stable.clone(),
            unstable: self.unstable.clone(),
            last_lamport: self.last_lamport,
        }
    }
}

impl<R, U> Debug for POLogLamport<R, U>
where
    R: ReplicatedDataType,
    Stable<R>: Debug,
    U: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("POLogLamport")
            .field("stable", &self.stable)
            .field("unstable", &self.unstable)
            .finish()
    }
}

impl<R, U> IsLog for POLogLamport<R, U>
where
    R: ReplicatedDataType
        + Precondition<U>
        + CausalCompaction<U>
        + CausalStabilize<U>
        + CausalReset<U>,
    U: IsUnstableCore<R::Update> + IsUnstablePrune<R::Update> + Default + Debug,
{
    type Command = R::Update;
    type Update = R::Update;
    type Rejection = R::Rejection;

    fn new() -> Self {
        Self {
            stable: Stable::<R>::default(),
            unstable: U::default(),
            last_lamport: 0,
        }
    }

    fn prepare(&self, command: Self::Command) -> Self::Update {
        command
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        R::is_enabled(&self.stable, &self.unstable, op)
    }

    fn effect(&mut self, event: Event<Self::Update>, _ctx: &mut EffectContext<'_>) {
        let new_tagged_op = TaggedOp::from(&event);
        if R::redundant_itself(&new_tagged_op, &self.stable, self.unstable.iter()) {
            if !<R as CausalCompaction<U>>::DISABLE_R_WHEN_R {
                self.prune_redundant_ops(
                    R::redundant_by_when_redundant,
                    &new_tagged_op,
                    event.version(),
                );
            }
        } else {
            if !R::DISABLE_R_WHEN_NOT_R {
                self.prune_redundant_ops(
                    R::redundant_by_when_not_redundant,
                    &new_tagged_op,
                    event.version(),
                );
            }
            self.unstable.append(event);
        }
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {

        let lamport = frontier.sum();

        if self.last_lamport >= lamport {
            return
        }

        let new_frontier = match self.get_frontier_from_lamport(Lamport::new(lamport)) {
            Some(f) => f,
            None => return
        };

        self.last_lamport = lamport;

        match R::causal_stabilize(&new_frontier, &self.stable, &self.unstable) {
            Stabilization::Regular => {
                // 1. select all ops in unstable that are predecessors of a version
                // 2. for each of them, call stabilize, which may modify stable and/or unstable
                // 3. if the operation is still in unstable, apply the op to stable and remove it from unstable

                let candidates = self
                    .unstable
                    .iter()
                    .filter(|tagged_op| new_frontier.contains(tagged_op.id()))
                    .cloned()
                    .collect::<Vec<_>>();

                for tagged_op in candidates {
                    R::on_stabilize(&tagged_op, &mut self.stable, &mut self.unstable);
                    if self.unstable.get(tagged_op.id()).is_some() {
                        R::Stable::apply(&mut self.stable, tagged_op.op());
                        self.unstable.remove(tagged_op.id());
                    }
                }
            }
            Stabilization::Critical {
                frontier: cut,
                stable,
            } => {
                self.unstable.prune_predecessors(&cut);
                self.stable = stable;
            }
            Stabilization::None => {}
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match R::reset(version, conservative, &self.stable, &self.unstable) {
            Reset::Inject(ops) => {
                for op in ops {
                    let event_id = EventId::from(version);
                    let lamport = Lamport::from(version);
                    let event = Event::new(event_id, lamport, op, version.clone());
                    self.unstable.append(event);
                }
            }
            Reset::Prune => {
                self.stable = Stable::<R>::default();
                if conservative {
                    self.unstable
                        .retain(|tagged_op| !tagged_op.id().is_predecessor_of(version))
                } else {
                    self.unstable.clear();
                }
            }
        }
    }

    fn is_default(&self) -> bool {
        self.stable.is_default() && self.unstable.is_empty()
    }
}

impl<O, U> Default for POLogLamport<O, U>
where
    O: ReplicatedDataType,
    U: Default,
{
    fn default() -> Self {
        Self {
            stable: Default::default(),
            unstable: Default::default(),
            last_lamport: Default::default(),
        }
    }
}

impl<R, U> POLogLamport<R, U>
where
    R: ReplicatedDataType,
    U: IsUnstableCore<R::Update> + IsUnstablePrune<R::Update> + Default + Debug,
{
    fn get_frontier_from_lamport(&self, watermark: Lamport) -> Option<CausalFrontier> {
        let eligible_ids: Vec<_> = self
            .unstable
            .iter()
            .filter(|op| op.lamport().val() <= watermark.val())
            .map(|op| op.id().clone())
            .collect();

        let Some(first_id) = eligible_ids.first() else {
            return None
        };

        let mut frontier = CausalFrontier::new(first_id.resolver().clone());

        // The frontier component for each replica is its greatest eligible sequence.
        for id in &eligible_ids {
            let origin = id.idx();
            let seq = id.seq();

            if seq > frontier.seq_by_idx(origin) {
                frontier.set_by_idx(origin, seq);
            }
        }

        Some(frontier)
    }

    pub fn from_parts(stable: Stable<R>, unstable: U, last_lamport: usize) -> Self {
        Self { stable, unstable, last_lamport }
    }

    pub fn from_stable(stable: Stable<R>) -> Self
    where
        U: Default,
    {
        Self::from_parts(stable, U::default(), usize::default())
    }

    pub fn stable(&self) -> &Stable<R> {
        &self.stable
    }

    pub fn unstable(&self) -> &U {
        &self.unstable
    }
}

impl<R, U> POLogLamport<R, U>
where
    R: ReplicatedDataType + CausalCompaction<U>,
    U: IsUnstablePrune<R::Update> + IsUnstableCore<R::Update>,
{
    fn prune_redundant_ops(
        &mut self,
        rdnt: RedundancyRelation<R::Update>,
        new_tagged_op: &TaggedOp<R::Update>,
        version: &Version,
    ) {
        R::compact_stable(&mut self.stable, rdnt, new_tagged_op);
        self.unstable.retain(|old_tagged_op| {
            //* Note: the new operation is not in the log at this point.
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

impl<Q, R, U> EvalNested<Q> for POLogLamport<R, U>
where
    Q: QueryOperation,
    R: ReplicatedDataType + Eval<Stable<R>, U, Q>,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        R::execute_query(&self.stable, &self.unstable, q)
    }
}

#[cfg(feature = "test_utils")]
impl<O, U> DeepSizeOf for POLogLamport<O, U>
where
    O: ReplicatedDataType,
    Stable<O>: DeepSizeOf,
    U: DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.stable.deep_size_of_children(context) + self.unstable.deep_size_of_children(context)
    }
}
