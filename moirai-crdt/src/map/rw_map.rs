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

use std::{
    fmt::{Debug, Display, Formatter},
    hash::Hash,
};

#[cfg(feature = "fuzz")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::{command_generator::CommandGenerator, value_generator::ValueGenerator},
    observers::footprint::{FootprintNode, LogFootprint},
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    data_type::query::{Contains, Get, QueryOperation, Read},
    event::Event,
    state::{
        effect_context::EffectContext,
        log::{EvalNested, IsLog},
        po_log::VecLog,
    },
};
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::{
    FxHashMap,
    map::update::MapUpdate,
    set::{rw_set::RWSet, update::SetUpdate},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct RWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
{
    set: VecLog<RWSet<K>>,
    children: FxHashMap<K, L>,
}

impl<K, L> Default for RWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
{
    fn default() -> Self {
        Self {
            set: Default::default(),
            children: Default::default(),
        }
    }
}

impl<K, L> RWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
    L: IsLog,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub const fn children(&self) -> &FxHashMap<K, L> {
        &self.children
    }

    pub const fn set(&self) -> &VecLog<RWSet<K>> {
        &self.set
    }

    pub fn get_child(&self, key: &K) -> Option<&L> {
        self.children.get(key)
    }
}

impl<K, L> IsLog for RWMapLog<K, L>
where
    L: IsLog,
    K: Clone + Debug + Hash + Eq,
{
    type Command = MapUpdate<K, L::Update>;
    type Update = MapUpdate<K, L::Update>;
    type Rejection = L::Rejection;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Update {
        cmd
    }

    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            MapUpdate::Update(k, v) => {
                // Query the RWSet
                let was_live = self.set.execute_query(&Contains(k.clone()));
                let set_op = Event::unfold(event.clone(), SetUpdate::Add(k.clone()));

                // Effect of the RWSet will determine whether the key is live or not, which in turn determines how we effect the child log
                let mut silent_ctx = EffectContext::silent();
                self.set.effect(set_op, &mut silent_ctx);

                // If the child op is not redundant
                if self.set.execute_query(&Contains(k.clone())) {
                    let child_op = Event::unfold(event, v);

                    if ctx.is_owned() {
                        ctx.with_map_entry(
                            || format!("{:?}", k),
                            |ctx| {
                                if was_live {
                                    ctx.update();
                                } else {
                                    ctx.create();
                                }

                                self.children
                                    .entry(k.clone())
                                    .or_default()
                                    .effect(child_op, ctx);
                            },
                        );
                    } else {
                        ctx.with_owned(|ctx| {
                            self.children
                                .entry(k.clone())
                                .or_default()
                                .effect(child_op, ctx);
                        });
                    }
                }
            }
            MapUpdate::Remove(k) => {
                let set_op = Event::unfold(event.clone(), SetUpdate::Remove(k.clone()));
                let mut silent_ctx = EffectContext::silent();
                self.set.effect(set_op, &mut silent_ctx);

                if ctx.is_owned() {
                    ctx.with_map_entry(|| format!("{:?}", k), |ctx| ctx.delete());
                }

                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), false);
                }
            }
            MapUpdate::Clear => {
                let set_op = Event::unfold(event.clone(), SetUpdate::Clear);
                let mut silent_ctx = EffectContext::silent();
                self.set.effect(set_op, &mut silent_ctx);

                if ctx.is_owned() {
                    ctx.delete();
                }
                for child in self.children.values_mut() {
                    child.redundant_by_parent(event.version(), true);
                }
            }
        }
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.set.stabilize(frontier);
        for child in self.children.values_mut() {
            child.stabilize(frontier);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.set.redundant_by_parent(version, conservative);
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.set.is_default() && self.children.values().all(IsLog::is_default)
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        match op {
            MapUpdate::Update(k, v) => {
                if self.set.execute_query(&Contains(k.clone())) {
                    self.children
                        .get(k)
                        .map_or_else(|| L::default().is_enabled(v), |child| child.is_enabled(v))
                } else {
                    L::default().is_enabled(v)
                }
            }
            MapUpdate::Remove(_) | MapUpdate::Clear => Ok(()),
        }
    }
}

impl<K, L, V> EvalNested<Read<FxHashMap<K, V>>> for RWMapLog<K, L>
where
    L: IsLog + EvalNested<Read<V>>,
    K: Clone + Debug + Hash + Eq + PartialEq,
    V: Default + PartialEq,
{
    fn execute_query(
        &self,
        _q: &Read<FxHashMap<K, V>>,
    ) -> <Read<FxHashMap<K, V>> as QueryOperation>::Response {
        let mut map: FxHashMap<K, V> = FxHashMap::default();
        for (k, v) in &self.children {
            if self.set.execute_query(&Contains(k.clone())) {
                let val = v.execute_query(&Read::new());
                if val != V::default() {
                    map.insert(k.clone(), val);
                }
            }
        }
        map
    }
}

impl<K, Q, L> EvalNested<Get<K, Q>> for RWMapLog<K, L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
    K: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(&self, q: &Get<K, Q>) -> <Get<K, Q> as QueryOperation>::Response {
        if !self.set.execute_query(&Contains(q.key.clone())) {
            return None;
        }

        self.children
            .get(&q.key)
            .map_or_else(|| None, |child| Some(child.execute_query(&q.nested_query)))
    }
}

#[cfg(feature = "fuzz")]
impl<K, L> CommandGenerator for RWMapLog<K, L>
where
    L: IsLog<Command = <L as IsLog>::Update> + CommandGenerator,
    K: Clone + Debug + Hash + Eq + PartialEq + ValueGenerator,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        use moirai_fuzz::generator::value_generator::ValueGenerator;
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Update,
            Remove,
            Clear,
        }
        let dist = WeightedIndex::new([5, 2, 1]).unwrap();

        let choice = &[Choice::Update, Choice::Remove, Choice::Clear][dist.sample(rng)];
        let key = K::generate(rng, &<K as ValueGenerator>::Config::default());
        match choice {
            Choice::Update => {
                let child_op = if let Some(child) = self.children.get(&key) {
                    child.generate_command(rng)
                } else {
                    L::new().generate_command(rng)
                };
                MapUpdate::Update(key, child_op)
            }
            Choice::Remove => MapUpdate::Remove(key),
            Choice::Clear => MapUpdate::Clear,
        }
    }
}

impl<K, L> Display for RWMapLog<K, L>
where
    K: Display + Debug + Clone + PartialEq + Eq + Hash,
    L: IsLog + Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (k, v) in &self.children {
            write!(f, "{} => {}", k, v)?;
        }
        Ok(())
    }
}

#[cfg(feature = "fuzz")]
impl<K, L> LogFootprint for RWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash + DeepSizeOf,
    L: IsLog + LogFootprint + DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        let children = self
            .children()
            .values()
            .map(|child| child.footprint())
            .collect();
        FootprintNode::parent(self, children)
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        data_type::query::{Get, Read},
        replica::IsReplica,
        state::po_log::{GraphLog, VecLog},
    };

    use crate::{
        FxHashMap,
        counter::{Counter, update::CounterUpdate},
        list::{eg_walker::List, update::ListUpdate},
        map::{rw_map::RWMapLog, update::MapUpdate},
        utils::bootstrap::{triplet_log, twins_log},
    };

    type CounterMap = RWMapLog<String, VecLog<Counter<i32>>>;
    type ListMap = RWMapLog<String, GraphLog<List<char>>>;

    #[test]
    fn concurrent_remove_wins_over_update() {
        let (mut replica_a, mut replica_b) = twins_log::<CounterMap>();
        let key = "a".to_string();

        let remove = replica_a.send(MapUpdate::Remove(key.clone())).unwrap();
        let update = replica_b
            .send(MapUpdate::Update(key.clone(), CounterUpdate::Inc(10)))
            .unwrap();

        replica_a.receive(update);
        replica_b.receive(remove);

        let expected: FxHashMap<String, i32> = FxHashMap::default();
        assert_eq!(replica_a.query(&Read::new()), expected);
        assert_eq!(replica_b.query(&Read::new()), expected);
        assert_eq!(replica_a.query(&Get::new(&key, Read::new())), None);
        assert_eq!(replica_b.query(&Get::new(&key, Read::new())), None);
    }

    #[test]
    fn update_after_remove_recreates_key() {
        let (mut replica_a, mut replica_b) = twins_log::<CounterMap>();
        let key = "a".to_string();

        let remove = replica_a.send(MapUpdate::Remove(key.clone())).unwrap();
        replica_b.receive(remove);

        let update = replica_b
            .send(MapUpdate::Update(key.clone(), CounterUpdate::Inc(7)))
            .unwrap();
        replica_a.receive(update);

        let mut expected = FxHashMap::default();
        expected.insert(key.clone(), 7);

        assert_eq!(replica_a.query(&Read::new()), expected);
        assert_eq!(replica_b.query(&Read::new()), expected);
        assert_eq!(replica_a.query(&Get::new(&key, Read::new())), Some(7));
        assert_eq!(replica_b.query(&Get::new(&key, Read::new())), Some(7));
    }

    #[test]
    fn map_nested_eg_walker() {
        let (mut replica_a, mut replica_b) = twins_log::<ListMap>();

        let event_a = replica_a
            .send(MapUpdate::Update(
                "doc".to_string(),
                ListUpdate::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(MapUpdate::Remove("doc".to_string()))
            .unwrap();
        replica_a.receive(event_b);

        let result: FxHashMap<String, Vec<char>> = FxHashMap::default();
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    fn map_nested_eg_walker_2() {
        let (mut replica_a, _) = twins_log::<ListMap>();

        let _ = replica_a
            .send(MapUpdate::Update(
                "doc".to_string(),
                ListUpdate::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        let _ = replica_a
            .send(MapUpdate::Remove("patate".to_string()))
            .unwrap();
        let _ = replica_a.send(MapUpdate::Clear).unwrap();

        assert_eq!(
            replica_a.query(&Read::<FxHashMap<String, Vec<char>>>::new()),
            FxHashMap::default()
        );
    }

    /// Execution trace:
    /// replica_a: MapUpdate::Clear@v1
    /// replica_b: MapUpdate::Update("doc", ListUpdate::Insert { content: 'A', pos: 0 })@v2
    /// replica_c: MapUpdate::Clear@v3
    /// replica_c receives v1, v2
    /// replica_a receives v2, v3
    /// replica_b receives v1, v3
    /// v1 || v2, v1 || v3, v2 || v3
    #[test]
    fn map_nested_eg_walker_3() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<ListMap>();

        let event_a = replica_a.send(MapUpdate::Clear).unwrap();
        let event_b = replica_b
            .send(MapUpdate::Update(
                "doc".to_string(),
                ListUpdate::Insert {
                    content: 'A',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_c = replica_c.send(MapUpdate::Clear).unwrap();

        replica_c.receive(event_a.clone());
        replica_c.receive(event_b.clone());

        replica_a.receive(event_b);
        replica_a.receive(event_c.clone());

        replica_b.receive(event_a);
        replica_b.receive(event_c);

        let mut result = FxHashMap::default();
        result.insert("doc".to_string(), vec!['A']);

        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert_eq!(replica_c.query(&Read::new()), result);
    }

    #[test]
    fn map_nested_eg_walker_4() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<ListMap>();

        let event_a_1 = replica_a
            .send(MapUpdate::Update(
                "foo".to_string(),
                ListUpdate::Insert {
                    content: 'a',
                    pos: 0,
                },
            ))
            .unwrap();

        let event_a_2 = replica_a
            .send(MapUpdate::Update(
                "bar".to_string(),
                ListUpdate::Insert {
                    content: 'b',
                    pos: 0,
                },
            ))
            .unwrap();

        replica_c.receive(event_a_1.clone());
        replica_c.receive(event_a_2.clone());
        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);

        let event_c_1 = replica_c
            .send(MapUpdate::Remove("alice".to_string()))
            .unwrap();
        let event_c_2 = replica_c.send(MapUpdate::Clear).unwrap();

        replica_a.receive(event_c_1.clone());
        replica_a.receive(event_c_2.clone());
        replica_b.receive(event_c_1);
        replica_b.receive(event_c_2);

        let result: FxHashMap<String, Vec<char>> = FxHashMap::default();

        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_c.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    /// Execution trace:
    /// digraph {
    ///     0 [ label="[Remove('ym')@(0:1)]"]
    ///     1 [ label="[Clear@(0:2)]"]
    ///     2 [ label="[Remove('hwxt')@(0:3)]"]
    ///     3 [ label="[Update('ym', Insert { content: 'r', pos: 0 })@(1:1)]"]
    ///     4 [ label="[Update('mza', Insert { content: 's', pos: 0 })@(0:4)]"]
    ///     5 [ label="[Update('xz', Insert { content: 'K', pos: 0 })@(0:5)]"]
    ///     6 [ label="[Remove('hcl')@(0:6)]"]
    ///     7 [ label="[Update('snif', Insert { content: 'R', pos: 0 })@(1:2)]"]
    ///     0 -> 1 [ ]  1 -> 2 [ ]  2 -> 4 [ ]  4 -> 5 [ ]  5 -> 6 [ ]  3 -> 7 [ ]
    /// }
    #[test]
    fn map_nested_eg_walker_5() {
        let (mut replica_a, mut replica_b) = twins_log::<ListMap>();

        let event_a_1 = replica_a.send(MapUpdate::Remove("ym".to_string())).unwrap();
        let event_a_2 = replica_a.send(MapUpdate::Clear).unwrap();
        let event_a_3 = replica_a
            .send(MapUpdate::Remove("hwxt".to_string()))
            .unwrap();
        let event_a_4 = replica_a
            .send(MapUpdate::Update(
                "mza".to_string(),
                ListUpdate::Insert {
                    content: 's',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_a_5 = replica_a
            .send(MapUpdate::Update(
                "xz".to_string(),
                ListUpdate::Insert {
                    content: 'K',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_a_6 = replica_a
            .send(MapUpdate::Remove("hcl".to_string()))
            .unwrap();

        let event_b_1 = replica_b
            .send(MapUpdate::Update(
                "ym".to_string(),
                ListUpdate::Insert {
                    content: 'r',
                    pos: 0,
                },
            ))
            .unwrap();
        let event_b_2 = replica_b
            .send(MapUpdate::Update(
                "snif".to_string(),
                ListUpdate::Insert {
                    content: 'R',
                    pos: 0,
                },
            ))
            .unwrap();

        replica_a.receive(event_b_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);
        replica_b.receive(event_a_4);
        replica_b.receive(event_a_5);
        replica_b.receive(event_a_6);

        assert_eq!(
            replica_a.query(&Read::<FxHashMap<String, Vec<char>>>::new()),
            replica_b.query(&Read::<FxHashMap<String, Vec<char>>>::new())
        );
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_rw_map() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        let run =
            RunConfig::new(ChurnConfig::new(0.6, 0.4), 2, 8, None, None).with_execution_graph();
        let runs = vec![run; 10_000];

        let config = FuzzerConfig::<ListMap, Read<FxHashMap<String, String>>>::new(
            "rw_map",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<ListMap, Read<FxHashMap<String, String>>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_rw_map_aw_set() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        use crate::{FxHashSet, set::aw_set::AWSet};

        type RWMapNested = RWMapLog<String, VecLog<AWSet<i32>>>;
        type Value = Read<FxHashMap<String, FxHashSet<i32>>>;

        let run = RunConfig::new(ChurnConfig::new(0.4, 0.6), 8, 50, None, None);
        let runs = vec![run; 1_000];

        let config = FuzzerConfig::<RWMapNested, Value>::new(
            "rw_map_aw_set",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<RWMapNested, Value>(config);
    }
}
