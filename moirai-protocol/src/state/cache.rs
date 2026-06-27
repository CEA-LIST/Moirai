use std::{
    cell::OnceCell,
    fmt,
    fmt::Debug,
};

use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::{BorrowedRead, EvalNested},
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};

#[cfg(feature = "test_utils")]
use crate::{
    crdt::pure_crdt::PureCRDT,
    state::{
        log::IsLogTest,
        stable_state::IsStableState,
        unstable_state::CausalReplay,
    },
};
#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[derive(Default)]
pub struct CacheCell<V> {
    value: OnceCell<V>,
}

impl<V> CacheCell<V> {
    pub fn new() -> Self {
        Self {
            value: OnceCell::new(),
        }
    }

    pub fn get(&self) -> Option<&V> {
        self.value.get()
    }

    pub fn get_mut(&mut self) -> Option<&mut V> {
        self.value.get_mut()
    }

    pub fn get_or_compute(&self, f: impl FnOnce() -> V) -> &V {
        self.value.get_or_init(f)
    }

    pub fn invalidate(&mut self) {
        self.value.take();
    }

    pub fn replace(&mut self, value: V) {
        if let Some(slot) = self.value.get_mut() {
            *slot = value;
        } else {
            let _ = self.value.set(value);
        }
    }
}

impl<V> Debug for CacheCell<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheCell").finish_non_exhaustive()
    }
}

impl<V> Clone for CacheCell<V> {
    fn clone(&self) -> Self {
        // Cache contents are derived from the log and can be recomputed. Cloning a log starts
        // with an empty cache to avoid adding a `V: Clone` bound to log clones.
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CachedLog<L: IsLog> {
    inner: L,
    read_cache: CacheCell<L::Value>,
}

impl<L: IsLog> CachedLog<L> {
    pub fn inner(&self) -> &L {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut L {
        self.read_cache.invalidate();
        &mut self.inner
    }

    pub fn into_inner(self) -> L {
        self.inner
    }
}

impl<L: IsLog> Default for CachedLog<L> {
    fn default() -> Self {
        Self {
            inner: L::default(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<L: IsLog> IsLog for CachedLog<L> {
    type Value = L::Value;
    type Op = L::Op;
    type Rejection = L::Rejection;

    fn new() -> Self {
        Self {
            inner: L::new(),
            read_cache: CacheCell::new(),
        }
    }

    fn prepare(op: Self::Op) -> Self::Op {
        L::prepare(op)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.inner.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        self.read_cache.invalidate();
        self.inner.effect(event, ctx);
    }

    fn stabilize(&mut self, version: &Version) {
        self.read_cache.invalidate();
        self.inner.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.read_cache.invalidate();
        self.inner.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.inner.is_default()
    }
}

impl<Q, L> EvalNested<Q> for CachedLog<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        self.inner.execute_query(q)
    }
}

impl<L> BorrowedRead for CachedLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    fn read_ref(&self) -> &Self::Value {
        self.read_cache
            .get_or_compute(|| self.inner.execute_query(Read::new()))
    }
}

#[cfg(feature = "test_utils")]
impl<L> IsLogTest for CachedLog<L>
where
    L: IsLogTest,
    L::Op: PureCRDT + DeepSizeOf,
    <L::Op as PureCRDT>::StableState: IsStableState<L::Op>,
{
    fn stable(&self) -> &<Self::Op as PureCRDT>::StableState {
        self.inner.stable()
    }

    fn unstable(&self) -> &(impl CausalReplay<Self::Op> + DeepSizeOf) {
        self.inner.unstable()
    }

    fn unstable_mut(&mut self) -> &mut (impl CausalReplay<Self::Op> + DeepSizeOf) {
        self.read_cache.invalidate();
        self.inner.unstable_mut()
    }
}
