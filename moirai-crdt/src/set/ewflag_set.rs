use std::{fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use moirai_fuzz::metrics::{FuzzMetrics, StructureMetrics};
use moirai_fuzz::{op_generator::OpGeneratorNested, value_generator::ValueGenerator};
#[cfg(feature = "sink")]
use moirai_protocol::state::{object_path::ObjectPath, sink::SinkCollector, sink::SinkOwnership};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{Contains, QueryOperation, Read},
    },
    event::Event,
    state::{log::IsLog, po_log::VecLog},
    utils::intern_str::{InternalizeOp, Interner},
};
use rand::distr::weighted::WeightedIndex;

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
pub struct EWFlagSetLog<V: Clone + Hash + Debug + Eq>(UWMapLog<V, VecLog<EWFlag>>);

impl<V> Default for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn default() -> Self {
        Self(UWMapLog::default())
    }
}

impl<V> IsLog for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    type Value = HashSet<V>;
    type Op = EWFlagSet<V>;

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
        #[cfg(feature = "sink")] _ownership: SinkOwnership,
    ) {
        let op = match event.op() {
            EWFlagSet::Add(k) => UWMap::Update(k.clone(), EWFlag::Enable),
            EWFlagSet::Remove(k) => UWMap::Update(k.clone(), EWFlag::Disable),
            EWFlagSet::Clear => UWMap::Clear,
        };
        let event = Event::unfold(event, op);
        // The EWFlagSetLog is a semantically a leaf CRDT, so we ignore the path and sink for now
        #[cfg(feature = "sink")]
        let mut sink = SinkCollector::new();
        self.0.effect(
            event,
            #[cfg(feature = "sink")]
            path,
            #[cfg(feature = "sink")]
            &mut sink,
            #[cfg(feature = "sink")]
            SinkOwnership::Delegated,
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

impl<V> EvalNested<Read<HashSet<V>>> for EWFlagSetLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<HashSet<V>>,
    ) -> <Read<HashSet<V>> as QueryOperation>::Response {
        let mut set = HashSet::default();
        for (k, v) in self.0.children() {
            let val = v.execute_query(Read::new());
            if val {
                set.insert(k.clone());
            }
        }
        set
    }
}

impl<V> EvalNested<Contains<V>> for EWFlagSetLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(&self, q: Contains<V>) -> <Contains<V> as QueryOperation>::Response {
        let v = self.0.children().get(&q.0);
        match v {
            Some(v) => v.execute_query(Read::new()),
            None => false,
        }
    }
}

impl<V> InternalizeOp for EWFlagSet<V> {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGeneratorNested for EWFlagSetLog<V>
where
    V: ValueGenerator + Clone + Hash + Debug + Eq,
{
    fn generate(&self, rng: &mut impl rand::Rng) -> Self::Op {
        use rand::distr::Distribution;

        #[derive(Debug)]
        enum Choice {
            Add,
            Remove,
            Clear,
        }

        let dist = WeightedIndex::new([1, 0, 0]).unwrap();

        let choice = &[Choice::Add, Choice::Remove, Choice::Clear][dist.sample(rng)];
        let value = V::generate(rng, &<V as ValueGenerator>::Config::default());
        match choice {
            Choice::Add => EWFlagSet::Add(value),
            Choice::Remove => EWFlagSet::Remove(value),
            Choice::Clear => EWFlagSet::Clear,
        }
    }
}

#[cfg(feature = "fuzz")]
impl<V> FuzzMetrics for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn structure_metrics(&self) -> StructureMetrics {
        if self.is_default() {
            StructureMetrics::empty()
        } else {
            StructureMetrics::scalar()
        }
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

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_ewflag_set() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run_1 = RunConfig::new(0.7, 16, 1_000, None, None, false, true);
        let run_2 = RunConfig::new(0.7, 16, 5_000, None, None, false, true);
        let run_3 = RunConfig::new(0.7, 16, 10_000, None, None, false, true);
        let run_4 = RunConfig::new(0.7, 16, 50_000, None, None, false, true);
        let run_5 = RunConfig::new(0.7, 16, 100_000, None, None, false, true);
        let runs = vec![run_1, run_2, run_3, run_4, run_5];

        let config = FuzzerConfig::<EWFlagSetLog<String>>::new(
            "ewflag_set",
            runs,
            true,
            |a, b| a == b,
            true,
            None,
        );

        fuzzer::<EWFlagSetLog<String>>(config);
    }
}
