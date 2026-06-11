use std::{convert::Infallible, fmt::Debug, hash::Hash};

use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::{BorrowedRead, EvalNested},
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{cache::CacheCell, effect_context::EffectContext, log::IsLog, po_log::VecLog},
    utils::intern_str::{InternalizeOp, Interner},
};

use crate::{
    HashSet,
    flag::ew_flag::EWFlag,
    map::uw_map::{UWMap, UWMapLog},
};

#[derive(Clone, Debug)]
pub enum EWFlagSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

#[derive(Clone, Debug)]
pub struct EWFlagSetLog<V: Clone + Hash + Debug + Eq> {
    inner: UWMapLog<V, VecLog<EWFlag>>,
    read_cache: CacheCell<HashSet<V>>,
}

impl<V> Default for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn default() -> Self {
        Self {
            inner: UWMapLog::default(),
            read_cache: CacheCell::new(),
        }
    }
}

impl<V> IsLog for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    type Value = HashSet<V>;
    type Op = EWFlagSet<V>;
    type Rejection = Infallible;

    fn new() -> Self {
        Self::default()
    }

    fn is_enabled(&self, _op: &Self::Op) -> Result<(), Self::Rejection> {
        Ok(())
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        self.read_cache.invalidate();
        let op = match event.op() {
            EWFlagSet::Add(k) => UWMap::Update(k.clone(), EWFlag::Enable),
            EWFlagSet::Remove(k) => UWMap::Update(k.clone(), EWFlag::Disable),
            EWFlagSet::Clear => UWMap::Clear,
        };
        let event = Event::unfold(event, op);
        // The EWFlagSetLog is semantically a leaf CRDT, so we ignore the path and sink for now
        let mut silent_ctx = EffectContext::silent();
        self.inner.effect(event, &mut silent_ctx);
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

impl<V> EvalNested<Read<HashSet<V>>> for EWFlagSetLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<HashSet<V>>,
    ) -> <Read<HashSet<V>> as QueryOperation>::Response {
        self.read_ref().clone()
    }
}

impl<V> BorrowedRead for EWFlagSetLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn read_ref(&self) -> &Self::Value {
        self.read_cache.get_or_compute(|| self.read_uncached())
    }
}

impl<V> EWFlagSetLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn read_uncached(&self) -> HashSet<V> {
        let mut set = HashSet::default();
        for (k, val) in self.inner.read_ref() {
            if *val {
                set.insert(k.clone());
            }
        }
        set
    }
}

impl<V> InternalizeOp for EWFlagSet<V> {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::replica::IsReplica;

    use super::*;
    use crate::utils::membership::twins_log;

    #[test]
    fn default_uw_map() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSetLog<&str>>();
        let event_a = replica_a.send(EWFlagSet::<&str>::Add("a")).unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(EWFlagSet::<&str>::Remove("a")).unwrap();
        replica_a.receive(event_b);

        assert_eq!(
            replica_a.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec![])
        );
        assert_eq!(
            replica_b.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec![])
        );
    }

    #[test]
    fn test_ewflag_set() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSetLog<&str>>();
        let event_a = replica_a.send(EWFlagSet::<&str>::Add("a")).unwrap();
        let event_b = replica_b.send(EWFlagSet::<&str>::Add("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(EWFlagSet::<&str>::Remove("a")).unwrap();
        let event_b = replica_b.send(EWFlagSet::<&str>::Add("c")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(
            replica_a.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec!["b", "c"])
        );
        assert_eq!(
            replica_b.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec!["b", "c"])
        );
    }
}
