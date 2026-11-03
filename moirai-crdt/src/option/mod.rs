pub mod update;

use std::convert::Infallible;

#[cfg(feature = "fuzz")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::command_generator::CommandGenerator,
    observers::footprint::{FootprintNode, LogFootprint},
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};
#[cfg(feature = "fuzz")]
use rand::RngExt;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::option::update::OptionUpdate;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OptionLog<L>
where
    L: IsLog,
{
    child: Option<L>,
}

impl<L> IsLog for OptionLog<L>
where
    L: IsLog,
{
    type Command = OptionUpdate<L::Update>;
    type Update = OptionUpdate<L::Update>;
    type Rejection = Infallible;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Update {
        cmd
    }

    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            OptionUpdate::Set(o) => {
                if self.child.is_some() {
                    ctx.update();
                } else {
                    ctx.create();
                }
                let child_op = Event::unfold(event, o);
                ctx.with_delegated(|ctx| {
                    self.child
                        .get_or_insert_with(L::default)
                        .effect(child_op, ctx);
                });
                self.child = self.child.take().filter(|c| !c.is_default());
            }
            OptionUpdate::Unset => {
                if ctx.is_owned() {
                    ctx.delete();
                }
                if let Some(child) = self.child.as_mut() {
                    child.redundant_by_parent(event.version(), true);
                    if child.is_default() {
                        self.child = None;
                    }
                }
            }
        }
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        if let Some(ref mut child) = self.child {
            child.stabilize(frontier);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        if let Some(ref mut child) = self.child {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.child.as_ref().is_none_or(|child| child.is_default())
    }
}

impl<L, V> EvalNested<Read<Option<V>>> for OptionLog<L>
where
    L: IsLog + EvalNested<Read<V>>,
    V: Clone + Default + PartialEq,
{
    fn execute_query(&self, _q: &Read<Option<V>>) -> <Read<Option<V>> as QueryOperation>::Response {
        self.child.as_ref().map_or_else(Default::default, |child| {
            Some(child.execute_query(&Read::new()))
        })
    }
}

impl<L> OptionLog<L>
where
    L: IsLog,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub const fn child(&self) -> Option<&L> {
        self.child.as_ref()
    }
}

impl<L> Default for OptionLog<L>
where
    L: IsLog,
{
    fn default() -> Self {
        Self { child: None }
    }
}

#[cfg(feature = "fuzz")]
impl<L> CommandGenerator for OptionLog<L>
where
    L: CommandGenerator + IsLog<Command = <L as IsLog>::Update>,
{
    fn generate_command(&self, rng: &mut impl rand::Rng) -> Self::Command {
        match &self.child {
            Some(child) => {
                if rng.random_bool(1.0 / 5.0) {
                    OptionUpdate::Unset
                } else {
                    OptionUpdate::Set(child.generate_command(rng))
                }
            }
            None => OptionUpdate::Set(<L as CommandGenerator>::generate_command(
                &L::default(),
                rng,
            )),
        }
    }
}

#[cfg(feature = "fuzz")]
impl<L> LogFootprint for OptionLog<L>
where
    L: LogFootprint + IsLog + DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog};

    use crate::{
        counter::{Counter, update::CounterUpdate},
        option::{OptionLog, OptionUpdate},
        utils::bootstrap::twins_log,
    };

    #[test]
    fn simple_optional() {
        let (mut replica_a, mut replica_b) = twins_log::<OptionLog<VecLog<Counter<i32>>>>();

        let event = replica_a
            .send(OptionUpdate::Set(CounterUpdate::Inc(2)))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a.send(OptionUpdate::Unset).unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(OptionUpdate::Set(CounterUpdate::Inc(5)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), Some(5));
        assert_eq!(replica_b.query(&Read::new()), Some(5));
    }

    #[test]
    fn concurrent_optional() {
        let (mut replica_a, mut replica_b) = twins_log::<OptionLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(OptionUpdate::Set(CounterUpdate::Inc(2)))
            .unwrap();
        let event_b = replica_b
            .send(OptionUpdate::Set(CounterUpdate::Inc(5)))
            .unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()), Some(7));
        assert_eq!(replica_b.query(&Read::new()), Some(7));
    }

    #[test]
    fn concurrent_unset_optional() {
        let (mut replica_a, mut replica_b) = twins_log::<OptionLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(OptionUpdate::Set(CounterUpdate::Inc(10)))
            .unwrap();
        replica_b.receive(event_a);

        let event_a = replica_a
            .send(OptionUpdate::Set(CounterUpdate::Inc(2)))
            .unwrap();
        let event_b = replica_b.send(OptionUpdate::Unset).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()), Some(2));
        assert_eq!(replica_b.query(&Read::new()), Some(2));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_optional_counter() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        type OptionalCounter = OptionLog<VecLog<Counter<i32>>>;

        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<OptionalCounter, Read<Option<i32>>>::new(
            "optional_counter",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<OptionalCounter, Read<Option<i32>>>(config);
    }
}
