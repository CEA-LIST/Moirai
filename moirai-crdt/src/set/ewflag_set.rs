use std::{convert::Infallible, fmt::Debug, hash::Hash};

use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog, po_log::VecLog},
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
}

impl<V> Default for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn default() -> Self {
        Self {
            inner: UWMapLog::default(),
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
        self.inner.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
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
        let mut set = HashSet::default();
        let values = self.inner.execute_query(Read::new());
        for (k, val) in values {
            if val {
                set.insert(k);
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
