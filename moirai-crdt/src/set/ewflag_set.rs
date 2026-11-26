/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//  
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
// 
//  SPDX-License-Identifier: Apache-2.0
//  
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

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
    data_type::query::{QueryOperation, Read},
    event::Event,
    state::{
        effect_context::EffectContext,
        log::{EvalNested, IsLog},
        po_log::VecLog,
    },
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

use crate::{
    FxHashSet,
    flag::{ew_flag::EWFlag, update::FlagUpdate},
    map::{update::MapUpdate, uw_map::UWMapLog},
    set::update::SetUpdate,
};

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
    type Command = SetUpdate<V>;
    type Update = SetUpdate<V>;
    type Rejection = Infallible;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Update {
        cmd
    }

    fn effect(&mut self, event: Event<Self::Update>, _ctx: &mut EffectContext<'_>) {
        let op = match event.op() {
            SetUpdate::Add(k) => MapUpdate::Update(k.clone(), FlagUpdate::Enable),
            SetUpdate::Remove(k) => MapUpdate::Update(k.clone(), FlagUpdate::Disable),
            SetUpdate::Clear => MapUpdate::Clear,
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

impl<V> EvalNested<Read<FxHashSet<V>>> for EWFlagSetLog<V>
where
    V: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: &Read<FxHashSet<V>>,
    ) -> <Read<FxHashSet<V>> as QueryOperation>::Response {
        let mut set = FxHashSet::default();
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
            0 => SetUpdate::Clear,
            1 | 2 => SetUpdate::Remove(value),
            _ => SetUpdate::Add(value),
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
    use crate::utils::bootstrap::twins_log;

    #[test]
    fn default_uw_map() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSetLog<&str>>();
        let event_a = replica_a.send(SetUpdate::<&str>::Add("a")).unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(SetUpdate::<&str>::Remove("a")).unwrap();
        replica_a.receive(event_b);

        assert_eq!(
            replica_a.query(&Read::<FxHashSet<&str>>::new()),
            FxHashSet::from_iter(vec![])
        );
        assert_eq!(
            replica_b.query(&Read::<FxHashSet<&str>>::new()),
            FxHashSet::from_iter(vec![])
        );
    }

    #[test]
    fn test_ewflag_set() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSetLog<&str>>();
        let event_a = replica_a.send(SetUpdate::<&str>::Add("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::<&str>::Add("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(SetUpdate::<&str>::Remove("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::<&str>::Add("c")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(
            replica_a.query(&Read::<FxHashSet<&str>>::new()),
            FxHashSet::from_iter(vec!["b", "c"])
        );
        assert_eq!(
            replica_b.query(&Read::<FxHashSet<&str>>::new()),
            FxHashSet::from_iter(vec!["b", "c"])
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
        let config = FuzzerConfig::<EWFlagSetLog<usize>, Read<FxHashSet<usize>>>::new(
            "ewflag_set",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<EWFlagSetLog<usize>, Read<FxHashSet<usize>>>(config);
    }
}
