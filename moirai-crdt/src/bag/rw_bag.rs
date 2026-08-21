use std::{convert::Infallible, fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::CommandGenerator, value_generator::ValueGenerator};
use moirai_protocol::{
    clock::version_vector::Version,
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
    HashMap,
    counter::resettable_counter::Counter,
    map::rw_map::{RWMap, RWMapLog},
};

#[derive(Clone, Debug)]
pub enum RWBag<V> {
    Add(V),
    Remove(V),
    Clear,
}

#[derive(Clone, Debug)]
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
    type Value = HashMap<V, usize>;
    type Command = RWBag<V>;
    type Op = RWBag<V>;
    type Rejection = Infallible;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Op {
        cmd
    }

    fn is_enabled(&self, _op: &Self::Op) -> Result<(), Self::Rejection> {
        Ok(())
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        let op = match event.op() {
            RWBag::Add(k) => RWMap::Update(k.clone(), Counter::Inc(1)),
            RWBag::Remove(k) => RWMap::Update(k.clone(), Counter::Dec(1)),
            RWBag::Clear => RWMap::Clear,
        };
        let event = Event::unfold(event, op);
        // While the Bag contains a map, it is semantically a leaf CRDT, so we ignore the path and sink.
        let mut silent_ctx = EffectContext::silent();
        self.0.effect(event, &mut silent_ctx);
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

impl<V> EvalNested<Read<HashMap<V, usize>>> for RWBagLog<V>
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
            Choice::Add => RWBag::Add(value),
            Choice::Remove => RWBag::Remove(value),
            Choice::Clear => RWBag::Clear,
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::replica::IsReplica;

    use super::*;
    use crate::utils::membership::twins_log;

    #[test]
    fn simple_bag() {
        let (mut replica_a, mut replica_b) = twins_log::<RWBagLog<&str>>();

        let event_a = replica_a.send(RWBag::Add("a")).unwrap();
        let event_b = replica_b.send(RWBag::Add("b")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(RWBag::Remove("a")).unwrap();
        replica_b.receive(event_a);

        let mut result = HashMap::default();
        result.insert("b", 1);

        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn concurrent_bag() {
        let (mut replica_a, mut replica_b) = twins_log::<RWBagLog<&str>>();

        let event_a = replica_a.send(RWBag::Add("a")).unwrap();
        let event_b = replica_b.send(RWBag::Add("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut result = HashMap::default();
        result.insert("a", 2);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_rw_bag() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config =
            FuzzerConfig::<RWBagLog<usize>>::new("rw_bag", runs, true, |a, b| a == b, false);

        fuzzer::<RWBagLog<usize>>(config);
    }
}
