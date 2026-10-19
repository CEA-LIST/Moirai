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
use rand::{Rng, RngExt};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{
    HashSet,
    flag::ew_flag::EWFlag,
    map::uw_map::{UWMap, UWMapLog},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum EWFlagSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
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
    type Command = EWFlagSet<V>;
    type Op = EWFlagSet<V>;
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
            EWFlagSet::Add(k) => UWMap::Update(k.clone(), EWFlag::Enable),
            EWFlagSet::Remove(k) => UWMap::Update(k.clone(), EWFlag::Disable),
            EWFlagSet::Clear => UWMap::Clear,
        };
        let event = Event::unfold(event, op);
        // The EWFlagSetLog is semantically a leaf CRDT, so we ignore the path and sink for now
        let mut silent_ctx = EffectContext::silent();
        self.inner.effect(event, &mut silent_ctx);
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.inner.stabilize(frontier);
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
        _q: &Read<HashSet<V>>,
    ) -> <Read<HashSet<V>> as QueryOperation>::Response {
        let mut set = HashSet::default();
        let values = self.inner.execute_query(&Read::new());
        for (k, val) in values {
            if val {
                set.insert(k);
            }
        }
        set
    }
}

#[cfg(feature = "fuzz")]
impl<V> CommandGenerator for EWFlagSetLog<V>
where
    V: ValueGenerator + Clone + Hash + Debug + Eq,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        let value = <V as ValueGenerator>::generate(rng, &V::Config::default());
        match rng.random_range(0..8) {
            0 => EWFlagSet::Clear,
            1 | 2 => EWFlagSet::Remove(value),
            _ => EWFlagSet::Add(value),
        }
    }
}

#[cfg(feature = "fuzz")]
impl<V> LogFootprint for EWFlagSetLog<V>
where
    V: Clone + Hash + Debug + Eq + DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        todo!()
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
            replica_a.query(&Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec![])
        );
        assert_eq!(
            replica_b.query(&Read::<HashSet<&str>>::new()),
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
            replica_a.query(&Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec!["b", "c"])
        );
        assert_eq!(
            replica_b.query(&Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec!["b", "c"])
        );
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_ewflag_set() {
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
        let config = FuzzerConfig::<EWFlagSetLog<usize>, Read<HashSet<usize>>>::new(
            "ewflag_set",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<EWFlagSetLog<usize>, Read<HashSet<usize>>>(config);
    }
}
