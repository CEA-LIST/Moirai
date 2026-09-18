use std::{
    cell::OnceCell,
    fmt::{self, Debug},
};

use castaway::cast;

use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    replica::ReplicaIdx,
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

#[cfg(all(test, feature = "test_utils"))]
mod memory_tests {
    use super::CacheCell;
    use deepsize::DeepSizeOf;

    #[test]
    fn memory_tracks_cached_allocations_without_populating_cache() {
        let mut cache = CacheCell::<Vec<u8>>::new();
        let inline = std::mem::size_of_val(&cache);
        assert_eq!(cache.deep_size_of(), inline);
        assert!(cache.get().is_none());

        let value = Vec::with_capacity(128);
        let capacity = value.capacity();
        cache.replace(value);
        assert_eq!(cache.deep_size_of(), inline + capacity);

        cache.invalidate();
        assert_eq!(cache.deep_size_of(), inline);
    }

    #[test]
    fn cached_inline_value_is_not_counted_twice() {
        let mut cache = CacheCell::<u64>::new();
        cache.replace(42);
        assert_eq!(cache.deep_size_of(), std::mem::size_of_val(&cache));
    }

    #[test]
    fn cached_shared_allocations_use_one_traversal_context() {
        use std::rc::Rc;

        let value = Rc::new(vec![0u8; 128]);
        let mut shared = CacheCell::new();
        shared.replace((value.clone(), value));
        let mut distinct = CacheCell::new();
        distinct.replace((Rc::new(vec![0u8; 128]), Rc::new(vec![0u8; 128])));
        assert!(shared.deep_size_of() < distinct.deep_size_of());
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
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "L: serde::Serialize",
        deserialize = "L: serde::de::DeserializeOwned"
    ))
)]
pub struct CachedLog<L, V> {
    inner: L,
    /// The join of every version this log has taken in, and not the version of
    /// the last event alone.
    ///
    /// Replaying an operation onto the materialised value is sound only for an
    /// event that is after **everything the log holds**, because the index the
    /// operation carries is the sender's index into the sender's own read-out.
    /// Testing the incoming event against the event before it is a different
    /// and weaker claim: causal delivery guarantees each new event is
    /// greater-than-or-concurrent-with every earlier one, never greater than
    /// all of them. A frontier is what the invariant the replay already
    /// intended is actually stated over.
    ///
    /// Not carried across the wire: a cache is derived from the log, so a
    /// deserialized log starts with an empty one and recomputes on first read.
    #[cfg_attr(feature = "serde", serde(skip))]
    frontier: Option<Version>,
    #[cfg_attr(feature = "serde", serde(skip, default = "CacheCell::new"))]
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

    pub fn inner_mut(&mut self) -> &mut L {
        self.read_cache.invalidate();
        &mut self.inner
    }

    pub fn into_inner(self) -> L {
        self.inner
    }

    /// Whether `version` is after every version this log has taken in.
    ///
    /// The test is entry-wise and deliberately does not go through
    /// [`Version`]'s own `partial_cmp`. That comparison shortcuts two versions
    /// that share an origin to a comparison of the origin's own sequence
    /// numbers, without looking at what else either of them has seen, so the
    /// second of two events from one replica answers `Greater` even when the
    /// log has since taken in an event concurrent with both — which is the
    /// whole of the defect. A frontier has no meaningful origin of its own
    /// either, being a join, so the shortcut would fire on it by accident.
    fn is_after_everything(&self, version: &Version) -> bool {
        let Some(frontier) = &self.frontier else {
            // Nothing has been taken in, so the log is empty and holds nothing
            // the event could be concurrent with.
            return true;
        };
        let members = version.len().max(frontier.len());
        (0..members).all(|idx| {
            let idx = ReplicaIdx(idx);
            version.seq_by_idx(idx) >= frontier.seq_by_idx(idx)
        })
    }

    /// Join `version` into the frontier.
    fn observe(&mut self, version: &Version) {
        match &mut self.frontier {
            None => self.frontier = Some(version.clone()),
            Some(frontier) => {
                for (idx, seq) in version.iter() {
                    if frontier.seq_by_idx(idx) < seq {
                        frontier.set_by_idx(idx, seq);
                    }
                }
            }
        }
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
        // The event is after everything the log holds exactly when the log's
        // content is the event's causal past, and then the sender's read-out
        // was this read-out and the index the operation carries is an index
        // into the cached value. Anything else recomputes.
        if self.is_after_everything(event.version()) {
            if let Some(cache) = self.read_cache.get_mut() {
                cache.apply(event.op());
            }
        } else {
            self.read_cache.invalidate();
        }
        self.observe(event.version());
        self.inner.effect(event, ctx);
    }

    fn stabilize(&mut self, version: &Version) {
        // A read-out that is a function of which operations have become stable
        // would otherwise be served from a cell that predates the
        // stabilization: `redundant_by_parent` beneath already invalidates for
        // the same reason, and this used to be the one traversal that did not.
        //
        // Moving an operation from the unstable log into the stable state is
        // meant to leave the read-out alone — that is what makes compaction
        // sound — but a data type that gets it wrong is exactly the defect
        // class a remove-wins set and a disable-wins flag were found in, so the
        // cell is dropped rather than trusted. It is only kept when no
        // operation can move at all, which is what
        // [`IsLog::STABILIZE_MOVES_OPERATIONS`] says, because `stabilize` is
        // called on every delivery and invalidating unconditionally would cost
        // the whole cache: a receiver reading a 2,000-character slot after each
        // of 2,000 deliveries goes from 46 ms to 65 s.
        if L::STABILIZE_MOVES_OPERATIONS {
            self.read_cache.invalidate();
        }
        self.inner.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.read_cache.invalidate();
        // The parent's removal changed what this log reads as, so an event may
        // only be replayed onto the recomputed value once it has seen the
        // removal too.
        self.observe(version);
        self.inner.redundant_by_parent(version, conservative);
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
