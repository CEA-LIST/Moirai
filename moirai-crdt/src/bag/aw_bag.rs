use std::{fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use moirai_fuzz::metrics::FuzzMetrics;
#[cfg(feature = "fuzz")]
use moirai_fuzz::metrics::StructureMetrics;
#[cfg(feature = "sink")]
use moirai_protocol::state::object_path::ObjectPath;
#[cfg(feature = "sink")]
use moirai_protocol::state::sink::SinkCollector;
use moirai_protocol::utils::intern_str::InternalizeOp;
use moirai_protocol::utils::intern_str::Interner;
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{log::IsLog, po_log::VecLog},
};

use crate::{
    HashMap,
    counter::resettable_counter::Counter,
    map::uw_map::{UWMap, UWMapLog},
};

#[derive(Clone, Debug)]
pub enum AWBag<V> {
    Add(V),
    Remove(V),
    Clear,
}

#[derive(Clone, Debug)]
pub struct AWBagLog<V: Clone + Hash + Debug + Eq>(UWMapLog<V, VecLog<Counter<usize>>>);

impl<V> Default for AWBagLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn default() -> Self {
        Self(UWMapLog::default())
    }
}

impl<V> IsLog for AWBagLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    type Value = HashMap<V, usize>;
    type Op = AWBag<V>;

    fn new() -> Self {
        Self::default()
    }

    fn is_enabled(&self, _op: &Self::Op) -> bool {
        true
    }

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        #[cfg(feature = "sink")] path: ObjectPath,
        #[cfg(feature = "sink")] _sink: &mut SinkCollector,
    ) {
        let op = match event.op() {
            AWBag::Add(k) => UWMap::Update(k.clone(), Counter::Inc(1)),
            AWBag::Remove(k) => UWMap::Update(k.clone(), Counter::Dec(1)),
            AWBag::Clear => UWMap::Clear,
        };
        let event = Event::unfold(event, op);
        // While the Bag contains a map, it is semantically a leaf CRDT, so we ignore the path and sink.
        #[cfg(feature = "sink")]
        let mut sink = SinkCollector::new();
        self.0.effect(
            event,
            #[cfg(feature = "sink")]
            path,
            #[cfg(feature = "sink")]
            &mut sink,
        );
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

impl<V> EvalNested<Read<HashMap<V, usize>>> for AWBagLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<HashMap<V, usize>>,
    ) -> <Read<HashMap<V, usize>> as QueryOperation>::Response {
        self.0.execute_query(Read::new())
    }
}

impl<V> InternalizeOp for AWBag<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[cfg(feature = "fuzz")]
impl<V> FuzzMetrics for AWBagLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn structure_metrics(&self) -> StructureMetrics {
        StructureMetrics::collection()
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::replica::IsReplica;

    use super::*;
    use crate::utils::membership::twins_log;

    #[test]
    fn simple_bag() {
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<&str>>();

        let event_a = replica_a.send(AWBag::Add("a")).unwrap();
        let event_b = replica_b.send(AWBag::Add("b")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(AWBag::Remove("a")).unwrap();
        replica_b.receive(event_a);

        let mut result = HashMap::default();
        result.insert("b", 1);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn concurrent_bag() {
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<&str>>();

        let event_a = replica_a.send(AWBag::Add("a")).unwrap();
        let event_b = replica_b.send(AWBag::Add("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut result = HashMap::default();
        result.insert("a", 2);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }
}
