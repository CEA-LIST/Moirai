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
    state::{effect_context::EffectContext, log::IsLog},
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{
    HashMap,
    bag::{Count, CountLog, counts},
    counter::resettable_counter::Counter,
    map::uw_map::{UWMap, UWMapLog},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub enum AWBag<V> {
    Add(V),
    Remove(V),
    Clear,
}

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "V: serde::Serialize",
        deserialize = "V: serde::de::DeserializeOwned"
    ))
)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct AWBagLog<V: Clone + Hash + Debug + Eq>(UWMapLog<V, CountLog>);

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
    type Command = AWBag<V>;
    type Op = AWBag<V>;
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
            AWBag::Add(k) => UWMap::Update(k.clone(), Counter::Inc(1)),
            AWBag::Remove(k) => UWMap::Update(k.clone(), Counter::Dec(1)),
            AWBag::Clear => UWMap::Clear,
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

impl<V> EvalNested<Read<HashMap<V, usize>>> for AWBagLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: &Read<HashMap<V, usize>>,
    ) -> <Read<HashMap<V, usize>> as QueryOperation>::Response {
        let nets: HashMap<V, Count> = self.0.execute_query(&Read::new());
        counts(nets)
    }
}

#[cfg(feature = "fuzz")]
impl<V> CommandGenerator for AWBagLog<V>
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
            Choice::Add => AWBag::Add(value),
            Choice::Remove => AWBag::Remove(value),
            Choice::Clear => AWBag::Clear,
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
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<&str>>();

        let event_a = replica_a.send(AWBag::Add("a")).unwrap();
        let event_b = replica_b.send(AWBag::Add("b")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(AWBag::Remove("a")).unwrap();
        replica_b.receive(event_a);

        let mut result = HashMap::default();
        result.insert("b", 1);

        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
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
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    /// Both replicas see the one copy of `7` and remove it concurrently; both
    /// removes are enabled. `Remove` is `Dec(1)` on the value's counter, so the
    /// net is `1 - 1 - 1`. The count is signed now, so the fold reaches `-1`
    /// instead of panicking with `attempt to subtract with overflow`, and the
    /// read-out drops the value: a remove that would take a count below zero
    /// does nothing.
    ///
    /// This is `aw_bag_concurrent_removes_of_one_copy_read_without_underflow`
    /// of `repro/aw-bag-underflow` (`d64340e`), no longer ignored.
    #[test]
    fn aw_bag_concurrent_removes_of_one_copy_read_without_underflow() {
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<usize>>();

        let add = replica_a.send(AWBag::Add(7)).unwrap();
        replica_b.receive(add);

        let remove_a = replica_a.send(AWBag::Remove(7)).unwrap();
        let remove_b = replica_b.send(AWBag::Remove(7)).unwrap();
        replica_a.receive(remove_b);
        replica_b.receive(remove_a);

        let result: HashMap<usize, usize> = HashMap::default();
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    /// The same two concurrent removes, then a write on another value from each
    /// replica, which makes the two removes causally stable. Folding them into
    /// `CounterStable` used to underflow inside `receive`; the signed count
    /// holds the net instead.
    ///
    /// This is
    /// `aw_bag_concurrent_removes_of_one_copy_stabilize_without_underflow` of
    /// `repro/aw-bag-underflow`, no longer ignored.
    #[test]
    fn aw_bag_concurrent_removes_of_one_copy_stabilize_without_underflow() {
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<usize>>();

        let add = replica_a.send(AWBag::Add(7)).unwrap();
        replica_b.receive(add);

        let remove_a = replica_a.send(AWBag::Remove(7)).unwrap();
        let remove_b = replica_b.send(AWBag::Remove(7)).unwrap();
        replica_a.receive(remove_b);
        replica_b.receive(remove_a);

        let spacer_a = replica_a.send(AWBag::Add(8)).unwrap();
        let spacer_b = replica_b.send(AWBag::Add(9)).unwrap();
        replica_a.receive(spacer_b);
        replica_b.receive(spacer_a);

        let mut result = HashMap::default();
        result.insert(8, 1);
        result.insert(9, 1);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    /// Removing a value the bag does not hold is enabled and reads as the
    /// empty bag.
    ///
    /// This is `aw_bag_remove_of_an_absent_value_reads_without_underflow` of
    /// `repro/aw-bag-underflow`, no longer ignored.
    #[test]
    fn aw_bag_remove_of_an_absent_value_reads_without_underflow() {
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<usize>>();

        let remove = replica_a.send(AWBag::Remove(7)).unwrap();
        replica_b.receive(remove);

        let result: HashMap<usize, usize> = HashMap::default();
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    /// The clamp is an aggregate rule and not a per-operation one, and this is
    /// what that buys: a remove concurrent with the add it does not observe
    /// reads the same on both replicas, which is the whole point of keeping the
    /// net exact.
    ///
    /// `a` removes a value it does not hold while `b` adds it, so the two
    /// replicas fold the same two operations in opposite orders. A counter that
    /// clamped at each subtraction would read `1` on `a` (`0 - 1` clamped, then
    /// `+ 1`) and `0` on `b` (`0 + 1`, then `- 1`), and the replicas would
    /// diverge; the exact net is `0` on both.
    #[test]
    fn aw_bag_a_remove_concurrent_with_the_add_it_does_not_observe_converges() {
        let (mut replica_a, mut replica_b) = twins_log::<AWBagLog<usize>>();

        let remove_a = replica_a.send(AWBag::Remove(7)).unwrap();
        let add_b = replica_b.send(AWBag::Add(7)).unwrap();
        replica_a.receive(add_b);
        replica_b.receive(remove_a);

        let result: HashMap<usize, usize> = HashMap::default();
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_aw_bag() {
        use moirai_fuzz::{
            config::{FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<AWBagLog<usize>, Read<HashMap<usize, usize>>>::new(
            "aw_bag",
            runs,
            true,
            Predicate::new(Read::new(), |a, b| a == b),
            false,
        );

        fuzzer::<AWBagLog<usize>, Read<HashMap<usize, usize>>>(config);
    }
}
