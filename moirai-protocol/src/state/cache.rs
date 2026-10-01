use std::{
    cell::OnceCell,
    fmt::{self, Debug},
};

use castaway::cast;

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};

pub trait IncrementalCache<O> {
    fn apply(&mut self, op: &O);
}

#[derive(Default)]
pub struct CacheCell<V> {
    value: OnceCell<V>,
}

#[cfg(feature = "test_utils")]
impl<V: deepsize::DeepSizeOf> deepsize::DeepSizeOf for CacheCell<V> {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // OnceCell stores V inline. Measure only its children and never populate
        // an empty cache as a side effect of measuring it.
        self.get()
            .map_or(0, |value| value.deep_size_of_children(context))
    }
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
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CachedLog<L, V> {
    inner: L,
    /// Join of the event versions represented by the inner log, when known.
    frontier: Option<CausalFrontier>,
    read_cache: CacheCell<V>,
}

impl<L, V> CachedLog<L, V> {
    pub fn from_inner(inner: L) -> Self {
        Self {
            inner,
            frontier: None,
            read_cache: CacheCell::new(),
        }
    }

    pub fn inner(&self) -> &L {
        &self.inner
    }
}

impl<L, V> Default for CachedLog<L, V>
where
    L: Default,
{
    fn default() -> Self {
        Self {
            inner: L::default(),
            frontier: None,
            read_cache: CacheCell::new(),
        }
    }
}

impl<L, V> IsLog for CachedLog<L, V>
where
    L: IsLog,
    V: Debug + IncrementalCache<L::Op>,
{
    type Command = L::Command;
    type Op = L::Op;
    type Rejection = L::Rejection;

    fn prepare(&self, command: Self::Command) -> Self::Op {
        self.inner.prepare(command)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.inner.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        if let Some(frontier) = &self.frontier
            && event.version().includes_frontier(frontier)
            && let Some(cache) = self.read_cache.get_mut()
        {
            cache.apply(event.op());
        } else {
            self.read_cache.invalidate();
        }

        match &mut self.frontier {
            Some(frontier) => frontier.join_version(event.version()),
            None => self.frontier = Some(CausalFrontier::from(event.version())),
        }

        self.inner.effect(event, ctx);
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.inner.stabilize(frontier);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.read_cache.invalidate();
        self.inner.redundant_by_parent(version, conservative);
        match &mut self.frontier {
            Some(frontier) => frontier.join_version(version),
            None => self.frontier = Some(CausalFrontier::from(version)),
        }
    }

    fn is_default(&self) -> bool {
        self.inner.is_default()
    }
}

// The 'static bound here does not mean queries live forever.
// It only means their types contain no borrowed references.

impl<Q, L, V> EvalNested<Q> for CachedLog<L, V>
where
    Q: QueryOperation + 'static,
    Q::Response: 'static,
    L: IsLog + EvalNested<Q> + EvalNested<Read<V>>,
    V: Debug + Clone + IncrementalCache<L::Op> + 'static,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        if cast!(q, &Read<V>).is_ok() {
            let value = self
                .read_cache
                .get_or_compute(|| {
                    <L as EvalNested<Read<V>>>::execute_query(&self.inner, &Read::new())
                })
                .clone();

            cast!(value, Q::Response)
                .ok()
                .expect("Read response type must match the log value type")
        } else {
            self.inner.execute_query(q)
        }
    }
}

impl<L, V> CachedLog<L, V>
where
    L: IsLog + EvalNested<Read<V>>,
{
    pub fn read_ref(&self) -> &V {
        self.read_cache
            .get_or_compute(|| self.inner.execute_query(&Read::new()))
    }
}
