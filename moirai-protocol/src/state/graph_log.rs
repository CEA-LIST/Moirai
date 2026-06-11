#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(feature = "test_utils")]
use crate::state::{log::IsLogTest, unstable_state::CausalReplay};
use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::{BorrowedRead, Eval, EvalNested},
        pure_crdt::{CausalReset, PureCRDT},
        query::{QueryOperation, Read},
    },
    event::{Event, id::EventId, lamport::Lamport},
    state::{
        cache::CacheCell,
        effect_context::EffectContext,
        log::IsLog,
        stable_state::IsStableState,
        unstable_state::{IsUnstableCore, IsUnstablePrune, event_graph::EventGraph},
    },
};

#[derive(Debug)]
pub struct GraphLog<O>
where
    O: PureCRDT,
{
    stable: <O as PureCRDT>::StableState,
    unstable: EventGraph<O>,
    read_cache: CacheCell<O::Value>,
}

impl<O> Clone for GraphLog<O>
where
    O: PureCRDT + Clone,
    O::StableState: Clone,
{
    fn clone(&self) -> Self {
        Self {
            stable: self.stable.clone(),
            unstable: self.unstable.clone(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<O> IsLog for GraphLog<O>
where
    O: PureCRDT + Clone,
{
    type Value = <O as PureCRDT>::Value;
    type Op = O;
    type Rejection = O::Rejection;

    fn new() -> Self {
        const {
            debug_assert!(O::DISABLE_R_WHEN_NOT_R && O::DISABLE_R_WHEN_R);
        }
        Self {
            stable: <O as PureCRDT>::StableState::default(),
            unstable: Default::default(),
            read_cache: CacheCell::new(),
        }
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        self.read_cache.invalidate();
        self.unstable.append(event);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.read_cache.invalidate();
        debug_assert!(self.unstable.graph().node_count() >= self.unstable.heads().len());
        match O::causal_reset(version, conservative, &self.stable, &self.unstable) {
            CausalReset::Inject(ops) => {
                for op in ops {
                    let event_id = EventId::from(version);
                    let lamport = Lamport::from(version);
                    let event = Event::new(event_id, lamport, op, version.clone());
                    self.unstable.append(event);
                }
            }
            CausalReset::Prune => {
                debug_assert!(!conservative);
                self.stable.clear();
                self.unstable.clear();
            }
        }
    }

    fn is_default(&self) -> bool {
        self.stable.is_default() && self.unstable.graph().node_count() == 0
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        O::is_enabled(op, &self.stable, &self.unstable)
    }

    fn stabilize(&mut self, version: &Version) {
        self.read_cache.invalidate();
        self.unstable.stabilize(version);
    }
}

impl<O> Default for GraphLog<O>
where
    O: PureCRDT,
{
    fn default() -> Self {
        Self {
            stable: <O as PureCRDT>::StableState::default(),
            unstable: Default::default(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<O> GraphLog<O>
where
    O: PureCRDT,
{
    pub fn from_stable(stable: <O as PureCRDT>::StableState) -> Self {
        Self {
            stable,
            unstable: Default::default(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<O> BorrowedRead for GraphLog<O>
where
    O: PureCRDT + Clone + Eval<Read<<O as PureCRDT>::Value>, EventGraph<O>>,
{
    fn read_ref(&self) -> &Self::Value {
        self.read_cache
            .get_or_compute(|| O::execute_query(Read::new(), &self.stable, &self.unstable))
    }
}

impl<O, Q> EvalNested<Q> for GraphLog<O>
where
    O: PureCRDT + Clone + Eval<Q, EventGraph<O>>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        O::execute_query(q, &self.stable, &self.unstable)
    }
}

#[cfg(feature = "test_utils")]
impl<O> IsLogTest for GraphLog<O>
where
    O: PureCRDT + Clone + DeepSizeOf,
{
    fn stable(&self) -> &<Self::Op as PureCRDT>::StableState {
        &self.stable
    }

    fn unstable(&self) -> &(impl CausalReplay<Self::Op> + DeepSizeOf) {
        &self.unstable
    }

    fn unstable_mut(&mut self) -> &mut (impl CausalReplay<Self::Op> + DeepSizeOf) {
        &mut self.unstable
    }
}
