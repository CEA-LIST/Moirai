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

use std::fmt::{Debug, Display};

#[cfg(feature = "fuzz")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::command_generator::CommandGenerator,
    observers::footprint::{FootprintNode, LogFootprint},
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    data_type::query::{QueryOperation, Read},
    event::{Event, id::EventId},
    state::{
        cache::CachedLog,
        effect_context::EffectContext,
        log::{EvalNested, IsLog},
        po_log::GraphLog,
    },
};
#[cfg(feature = "fuzz")]
use rand::RngExt;

use crate::{
    list::{
        eg_walker::{List as SimpleList, ReadAt},
        update::{ListUpdate, NestedListUpdate},
    },
    map::{update::MapUpdate, uw_map::UWMapLog},
};

/// Internal state of a nested list CRDT
///
/// Maintains both the logical ordering of children (via EgWalker) and the
/// actual child CRDT instances.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct NestedListLog<L>
where
    L: IsLog,
{
    /// EgWalker list tracking the logical positions of children
    positions: CachedLog<GraphLog<SimpleList<EventId>>, Vec<EventId>>,
    /// Map from EventId to child CRDT instance
    children: UWMapLog<EventId, L>,
}

impl<L> NestedListLog<L>
where
    L: IsLog,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn positions(&self) -> &GraphLog<SimpleList<EventId>> {
        self.positions.inner()
    }

    #[allow(clippy::mutable_key_type)]
    pub const fn children(&self) -> &UWMapLog<EventId, L> {
        &self.children
    }
}

#[derive(Debug)]
pub enum NestedListRejection<E> {
    /// Rejection from the child CRDT
    ChildError { pos: usize, error: E },
    /// The position specified is out of bounds
    InvalidPosition { pos: usize, len: usize },
}

impl<E> Display for NestedListRejection<E>
where
    E: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChildError { pos, error } => {
                write!(f, "Child error at position {}: {}", pos, error)
            }
            Self::InvalidPosition { pos, len } => {
                write!(f, "Invalid position {}: list length is {}", pos, len)
            }
        }
    }
}

impl<L> IsLog for NestedListLog<L>
where
    L: IsLog,
{
    type Command = NestedListUpdate<L::Update>;
    type Update = NestedListUpdate<L::Update>;
    type Rejection = NestedListRejection<Box<L::Rejection>>;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Update {
        cmd
    }

    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            NestedListUpdate::Insert { pos, op } => {
                let list_event = Event::new(
                    event.id().clone(),
                    *event.lamport(),
                    ListUpdate::Insert {
                        pos,
                        content: event.id().clone(),
                    },
                    event.version().clone(),
                );
                let child_event =
                    Event::unfold(event.clone(), MapUpdate::Update(event.id().clone(), op));
                ctx.with_list_element(
                    || event.id().clone(),
                    |ctx| {
                        ctx.create();
                        ctx.with_delegated(|ctx| self.positions.effect(list_event, ctx));
                        ctx.with_delegated(|ctx| self.children.effect(child_event, ctx));
                    },
                );
            }
            NestedListUpdate::Delete { pos } => {
                let positions_at_version =
                    self.positions.eval(&ReadAt::new(event.version())).unwrap();
                let target = positions_at_version[pos].clone();
                let list_event = Event::unfold(event.clone(), ListUpdate::Delete { pos });
                let map_event = Event::unfold(event.clone(), MapUpdate::Remove(target.clone()));
                ctx.with_list_element(
                    || target.clone(),
                    |ctx| {
                        ctx.delete();
                        ctx.with_delegated(|ctx| self.positions.effect(list_event, ctx));
                        ctx.with_delegated(|ctx| self.children.effect(map_event, ctx));
                    },
                );
            }
            NestedListUpdate::Update { pos, op } => {
                let positions_at_version =
                    self.positions.eval(&ReadAt::new(event.version())).unwrap();
                let target = positions_at_version[pos].clone();
                let list_event = Event::unfold(event.clone(), ListUpdate::Update { pos });
                let map_event = Event::unfold(event.clone(), MapUpdate::Update(target.clone(), op));
                ctx.with_list_element(
                    || target.clone(),
                    |ctx| {
                        ctx.update();
                        ctx.with_delegated(|ctx| self.positions.effect(list_event, ctx));
                        ctx.with_delegated(|ctx| self.children.effect(map_event, ctx));
                    },
                );
            }
        }
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.children.stabilize(frontier);
        self.positions.stabilize(frontier);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.children.redundant_by_parent(version, conservative);
        self.positions.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.positions.is_default() && self.children.is_default()
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        let positions = self.positions.read_ref();
        match op {
            NestedListUpdate::Insert { pos, op } => {
                if *pos > positions.len() {
                    return Err(NestedListRejection::InvalidPosition {
                        pos: *pos,
                        len: positions.len(),
                    });
                }
                L::default()
                    .is_enabled(op)
                    .map_err(|error| NestedListRejection::ChildError {
                        pos: *pos,
                        error: Box::new(error),
                    })
            }
            NestedListUpdate::Update { pos, op } => {
                if *pos >= positions.len() {
                    return Err(NestedListRejection::InvalidPosition {
                        pos: *pos,
                        len: positions.len(),
                    });
                }
                let target = positions[*pos].clone();
                let map_op = MapUpdate::Update(target, op.clone());
                self.children
                    .is_enabled(&map_op)
                    .map_err(|error| NestedListRejection::ChildError { pos: *pos, error })
            }
            NestedListUpdate::Delete { pos } => {
                if *pos < positions.len() {
                    Ok(())
                } else {
                    Err(NestedListRejection::InvalidPosition {
                        pos: *pos,
                        len: positions.len(),
                    })
                }
            }
        }
    }
}

impl<L, V> EvalNested<Read<Vec<V>>> for NestedListLog<L>
where
    L: IsLog + EvalNested<Read<V>>,
    V: Clone + Default + PartialEq,
{
    fn execute_query(&self, _q: &Read<Vec<V>>) -> <Read<Vec<V>> as QueryOperation>::Response {
        let mut list = Vec::new();
        let positions = self.positions.read_ref();
        #[allow(clippy::mutable_key_type)]
        let map = self.children.execute_query(&Read::new());
        for eid in positions {
            if let Some(child) = map.get(eid) {
                list.push(child.clone());
            }
        }
        list
    }
}

#[cfg(feature = "fuzz")]
impl<L> CommandGenerator for NestedListLog<L>
where
    L: CommandGenerator + IsLog<Command = <L as IsLog>::Update>,
{
    fn generate_command(&self, rng: &mut impl rand::Rng) -> Self::Command {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Insert,
            Update,
            Delete,
        }
        let dist = WeightedIndex::new([2, 2, 1]).unwrap();

        let positions = self.positions.read_ref();
        let choice = if positions.is_empty() {
            &Choice::Insert
        } else {
            &[Choice::Insert, Choice::Update, Choice::Delete][dist.sample(rng)]
        };

        let op = match choice {
            Choice::Insert => {
                let pos = rng.random_range(0..=positions.len());
                let default_child = L::new();
                let op = <L as CommandGenerator>::generate_command(&default_child, rng);
                NestedListUpdate::Insert { pos, op }
            }
            Choice::Update => {
                let pos = rng.random_range(0..positions.len());
                let target_id = &positions[pos];
                let child = self.children.get_child(target_id);
                let op = if let Some(c) = child {
                    <L as CommandGenerator>::generate_command(c, rng)
                } else {
                    let default_child = L::new();
                    <L as CommandGenerator>::generate_command(&default_child, rng)
                };
                NestedListUpdate::Update { pos, op }
            }
            Choice::Delete => {
                let pos = rng.random_range(0..positions.len());
                NestedListUpdate::Delete { pos }
            }
        };
        assert!(self.is_enabled(&op).is_ok());
        op
    }
}

#[cfg(feature = "fuzz")]
impl<L> LogFootprint for NestedListLog<L>
where
    L: IsLog + LogFootprint + DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::parent(
            &self,
            vec![self.positions().footprint(), self.children().footprint()],
        )
    }
}

impl<L> Default for NestedListLog<L>
where
    L: IsLog,
{
    fn default() -> Self {
        Self {
            positions: CachedLog::from_inner(GraphLog::default()),
            children: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_macros::record;
    use moirai_protocol::{data_type::query::Read, replica::IsReplica, state::po_log::VecLog};
    #[cfg(feature = "test_utils")]
    use moirai_protocol::{state::log::EvalNested, state::po_log::GraphLog};

    #[cfg(feature = "test_utils")]
    use crate::list::eg_walker::List;
    #[cfg(feature = "test_utils")]
    use crate::list::update::ListUpdate;
    use crate::{
        FxHashMap,
        counter::{Counter, update::CounterUpdate},
        list::{nested_list::NestedListLog, update::NestedListUpdate},
        map::{update::MapUpdate, uw_map::UWMapLog},
        utils::bootstrap::{triplet_log, twins_log},
    };

    #[cfg(feature = "test_utils")]
    #[test]
    fn concurrent_parent_delete_and_nested_insert_after_cache_read() {
        type Inner = NestedListLog<GraphLog<List<char>>>;
        let (mut a, mut b) = twins_log::<NestedListLog<Inner>>();
        let initial = a
            .send(NestedListUpdate::insert(
                0,
                NestedListUpdate::insert(0, ListUpdate::insert('w', 0)),
            ))
            .unwrap();
        b.receive(initial);

        let delete = a.send(NestedListUpdate::delete(0)).unwrap();
        for child in a.state().children().children().values() {
            let value: Vec<Vec<char>> = child.execute_query(&Read::new());
            assert!(value.is_empty());
        }

        let update = b
            .send(NestedListUpdate::update(
                0,
                NestedListUpdate::insert(1, ListUpdate::insert('x', 0)),
            ))
            .unwrap();
        a.receive(update);
        b.receive(delete);
        assert_eq!(
            a.query(&Read::<Vec<Vec<Vec<char>>>>::new()),
            b.query(&Read::<Vec<Vec<Vec<char>>>>::new())
        );
    }

    #[test]
    fn simple_nested_list() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(10)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), vec![10]);
        assert_eq!(replica_b.query(&Read::new()), vec![10]);

        let event = replica_b
            .send(NestedListUpdate::update(0, CounterUpdate::Dec(5)))
            .unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(&Read::new()), vec![5]);
        assert_eq!(replica_b.query(&Read::new()), vec![5]);

        let event = replica_a
            .send(NestedListUpdate::insert(1, CounterUpdate::Inc(10)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), vec![5, 10]);
        assert_eq!(replica_b.query(&Read::new()), vec![5, 10]);

        let event = replica_a
            .send(NestedListUpdate::update(0, CounterUpdate::Inc(1)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), vec![6, 10]);
        assert_eq!(replica_b.query(&Read::new()), vec![6, 10]);

        let event = replica_a.send(NestedListUpdate::delete(0)).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), vec![10]);
        assert_eq!(replica_b.query(&Read::new()), vec![10]);

        let event_a = replica_a
            .send(NestedListUpdate::insert(1, CounterUpdate::Inc(21)))
            .unwrap();
        let event_b = replica_b.send(NestedListUpdate::delete(0)).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(&Read::new()), vec![21]);
        assert_eq!(replica_a.query(&Read::new()), vec![21]);
    }

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(10)))
            .unwrap();
        let event_b = replica_b
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(20)))
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()), vec![10, 20]);
        assert_eq!(replica_b.query(&Read::new()), vec![10, 20]);
    }

    #[test]
    fn insert_then_delete() {
        record!(Duet {
            first: VecLog<Counter<i32>>,
            second: VecLog<Counter<i32>>,
        });

        let (mut replica_a, _) = twins_log::<NestedListLog<DuetLog>>();

        let _ = replica_a
            .send(NestedListUpdate::insert(
                0,
                Duet::First(CounterUpdate::Inc(10)),
            ))
            .unwrap();
        let _ = replica_a.send(NestedListUpdate::delete(0)).unwrap();

        let list = replica_a.query(&Read::new());
        assert_eq!(list, Vec::<DuetValue<i32, i32>>::new());
    }

    #[test]
    fn concurrent_update_delete() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(10)))
            .unwrap();
        replica_b.receive(event_a);

        let event_a = replica_a
            .send(NestedListUpdate::update(0, CounterUpdate::Inc(5)))
            .unwrap();

        let event_b = replica_b.send(NestedListUpdate::delete(0)).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(&Read::new()), vec![5]);
        assert_eq!(replica_a.query(&Read::new()), vec![5]);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn concurrent_update_delete_insert() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let initial_insert = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(10)))
            .unwrap();
        replica_b.receive(initial_insert.clone());
        replica_c.receive(initial_insert);

        let event_a = replica_a
            .send(NestedListUpdate::update(0, CounterUpdate::Inc(5)))
            .unwrap();

        let event_b = replica_b.send(NestedListUpdate::delete(0)).unwrap();
        let event_c = replica_c
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(15)))
            .unwrap();
        replica_a.receive(event_b.clone());
        replica_a.receive(event_c.clone());
        replica_b.receive(event_a.clone());
        replica_b.receive(event_c);
        replica_c.receive(event_a);
        replica_c.receive(event_b);

        assert_eq!(replica_a.query(&Read::new()), vec![15, 5]);
        assert_eq!(replica_b.query(&Read::new()), vec![15, 5]);
        assert_eq!(replica_c.query(&Read::new()), vec![15, 5]);
    }

    #[test]
    fn concurrent_update_delete_insert_2() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_b_1 = replica_b
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(1)))
            .unwrap();
        replica_a.receive(event_b_1.clone());
        replica_c.receive(event_b_1);

        let event_b_2 = replica_b.send(NestedListUpdate::delete(0)).unwrap();

        let event_a_1 = replica_a
            .send(NestedListUpdate::update(0, CounterUpdate::Inc(1)))
            .unwrap();

        let event_c_1 = replica_c
            .send(NestedListUpdate::insert(0, CounterUpdate::Reset))
            .unwrap();

        let event_c_2 = replica_c.send(NestedListUpdate::delete(1)).unwrap();

        replica_a.receive(event_b_2.clone());
        replica_a.receive(event_c_1.clone());
        replica_a.receive(event_c_2.clone());

        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_c_1);
        replica_b.receive(event_c_2);

        replica_c.receive(event_a_1);
        replica_c.receive(event_b_2);

        assert_eq!(replica_a.query(&Read::new()), vec![1]);
        assert_eq!(replica_b.query(&Read::new()), vec![1]);
        assert_eq!(replica_c.query(&Read::new()), vec![1]);
    }

    #[test]
    fn scenario_1() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Reset))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(NestedListUpdate::insert(0, CounterUpdate::Dec(64)))
            .unwrap();
        let event_a = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Dec(23)))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let event_b = replica_b.send(NestedListUpdate::delete(1)).unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn scenario_2() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Dec(22)))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(NestedListUpdate::update(0, CounterUpdate::Reset))
            .unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a
            .send(NestedListUpdate::update(0, CounterUpdate::Inc(40)))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(NestedListUpdate::insert(0, CounterUpdate::Inc(47)))
            .unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn scenario_3() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedListUpdate::insert(0, CounterUpdate::Dec(22)))
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(NestedListUpdate::insert(1, CounterUpdate::Inc(30)))
            .unwrap();
        replica_a.receive(event_b);

        let event_b = replica_b.send(NestedListUpdate::delete(0)).unwrap();
        let event_a = replica_a
            .send(NestedListUpdate::update(1, CounterUpdate::Inc(40)))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn map_of_list() {
        let (mut replica_a, mut replica_b) =
            twins_log::<UWMapLog<&str, NestedListLog<VecLog<Counter<i32>>>>>();

        let event_a = replica_a
            .send(MapUpdate::Update(
                "a",
                NestedListUpdate::insert(0, CounterUpdate::Inc(10)),
            ))
            .unwrap();
        let event_a_2 = replica_a
            .send(MapUpdate::Update(
                "a",
                NestedListUpdate::insert(1, CounterUpdate::Inc(5)),
            ))
            .unwrap();
        let event_a_3 = replica_a
            .send(MapUpdate::Update(
                "a",
                NestedListUpdate::update(0, CounterUpdate::Inc(1)),
            ))
            .unwrap();

        let mut result = FxHashMap::default();
        result.insert("a", vec![11, 5]);
        assert_eq!(replica_a.query(&Read::new()), result);

        replica_b.receive(event_a);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);

        assert_eq!(replica_b.query(&Read::new()), result);

        let event_b = replica_b.send(MapUpdate::Remove("a")).unwrap();
        let event_a = replica_a
            .send(MapUpdate::Update(
                "a",
                NestedListUpdate::insert(0, CounterUpdate::Inc(100)),
            ))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let mut result = FxHashMap::default();
        result.insert("a", vec![100]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_nested_list_counter() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        let run =
            RunConfig::new(ChurnConfig::new(0.6, 0.4), 6, 20, None, None).with_execution_graph();
        let runs = vec![run; 10];

        let config = FuzzerConfig::<NestedListLog<VecLog<Counter<i32>>>, Read<Vec<i32>>>::new(
            "nested_list_counter",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<NestedListLog<VecLog<Counter<i32>>>, Read<Vec<i32>>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_nested_list_string() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        use crate::list::eg_walker::List;

        let run =
            RunConfig::new(ChurnConfig::new(0.6, 0.4), 4, 25, None, None).with_execution_graph();
        let runs = vec![run; 10_000];

        let config = FuzzerConfig::<NestedListLog<GraphLog<List<char>>>, Read<Vec<Vec<char>>>>::new(
            "nested_list_string",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<NestedListLog<GraphLog<List<char>>>, Read<Vec<Vec<char>>>>(config);
    }
}
