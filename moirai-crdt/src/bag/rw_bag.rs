use std::{convert::Infallible, fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::{command_generator::CommandGenerator, value_generator::ValueGenerator},
    observers::footprint::{FootprintNode, LogFootprint},
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog, po_log::VecLog},
};
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::{
    FxHashMap,
    bag::update::BagUpdate,
    counter::{Counter, update::CounterUpdate},
    map::{rw_map::RWMapLog, update::MapUpdate},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct RWBagLog<V: Clone + Hash + Debug + Eq>(RWMapLog<V, VecLog<Counter<usize>>>);

impl<V> Default for RWBagLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    fn default() -> Self {
        Self(RWMapLog::default())
    }
}

impl<V> IsLog for RWBagLog<V>
where
    V: Clone + Hash + Debug + Eq,
{
    type Command = BagUpdate<V>;
    type Update = BagUpdate<V>;
    type Rejection = Infallible;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Update {
        cmd
    }

    fn effect(&mut self, event: Event<Self::Update>, _ctx: &mut EffectContext<'_>) {
        let op = match event.op() {
            BagUpdate::Add(k) => MapUpdate::Update(k.clone(), CounterUpdate::Inc(1)),
            BagUpdate::Remove(k) => MapUpdate::Update(k.clone(), CounterUpdate::Dec(1)),
            BagUpdate::Clear => MapUpdate::Clear,
        };
        let event = Event::unfold(event, op);
        // While the Bag contains a map, it is semantically a leaf CRDT, so we ignore the path and sink.
        let mut silent_ctx = EffectContext::silent();
        self.0.effect(event, &mut silent_ctx);
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.0.stabilize(frontier);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

impl<V> EvalNested<Read<FxHashMap<V, usize>>> for RWBagLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: &Read<FxHashMap<V, usize>>,
    ) -> <Read<FxHashMap<V, usize>> as QueryOperation>::Response {
        self.0.execute_query(&Read::new())
    }
}

#[cfg(feature = "fuzz")]
impl<V> CommandGenerator for RWBagLog<V>
where
    V: ValueGenerator + Clone + Hash + Debug + Eq,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Add,
            Remove,
            Clear,
        }
        let dist = WeightedIndex::new([5, 2, 1]).unwrap();

        let choice = &[Choice::Add, Choice::Remove, Choice::Clear][dist.sample(rng)];
        let value = V::generate(rng, &<V as ValueGenerator>::Config::default());
        match choice {
            Choice::Add => BagUpdate::Add(value),
            Choice::Remove => BagUpdate::Remove(value),
            Choice::Clear => BagUpdate::Clear,
        }
    }
}

#[cfg(feature = "fuzz")]
impl<V> LogFootprint for RWBagLog<V>
where
    V: Clone + Hash + Debug + Eq + DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        self.0.footprint()
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::replica::IsReplica;

    use super::*;
    use crate::utils::bootstrap::twins_log;

    #[test]
    fn simple_bag() {
        let (mut replica_a, mut replica_b) = twins_log::<RWBagLog<&str>>();

        let event_a = replica_a.send(BagUpdate::Add("a")).unwrap();
        let event_b = replica_b.send(BagUpdate::Add("b")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(BagUpdate::Remove("a")).unwrap();
        replica_b.receive(event_a);

        let mut result = FxHashMap::default();
        result.insert("b", 1);

        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    fn concurrent_bag() {
        let (mut replica_a, mut replica_b) = twins_log::<RWBagLog<&str>>();

        let event_a = replica_a.send(BagUpdate::Add("a")).unwrap();
        let event_b = replica_b.send(BagUpdate::Add("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut result = FxHashMap::default();
        result.insert("a", 2);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_rw_bag() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<RWBagLog<usize>, Read<FxHashMap<usize, usize>>>::new(
            "rw_bag",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<RWBagLog<usize>, Read<FxHashMap<usize, usize>>>(config);
    }
}
