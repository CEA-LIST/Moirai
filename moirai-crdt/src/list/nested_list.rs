use std::fmt::Debug;

#[cfg(feature = "fuzz")]
use moirai_fuzz::metrics::{FuzzMetrics, StructureMetrics};
#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGeneratorNested;
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{IsSemanticallyEmpty, QueryOperation, Read},
    },
    event::{Event, id::EventId},
    replica::ReplicaIdx,
    state::{
        event_graph::EventGraph,
        log::IsLog,
        sink::{DefaultSinkExpansion, IsLogSink, ObjectPath, Sink, SinkCollector},
    },
    utils::{boxer::Boxer, intern_str::Interner, translate_ids::TranslateIds},
};
#[cfg(feature = "fuzz")]
use rand::RngExt;

use crate::{
    HashMap,
    list::eg_walker::{List as SimpleList, ReadAt},
};

#[derive(Clone, Debug)]
pub enum NestedList<O> {
    /// Insert a new child CRDT at the given position
    Insert { pos: usize, value: O },
    /// Update the child at the given position
    Update { pos: usize, value: O },
    /// Delete the child at the given position
    Delete { pos: usize },
}

impl<O> NestedList<O> {
    pub fn insert(pos: usize, value: O) -> Self {
        Self::Insert { pos, value }
    }

    pub fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    pub fn update(pos: usize, value: O) -> Self {
        Self::Update { pos, value }
    }
}

impl<O> TranslateIds for NestedList<O>
where
    O: TranslateIds + Clone,
{
    fn translate_ids(&self, from: ReplicaIdx, interner: &Interner) -> Self {
        match self {
            NestedList::Insert { pos, value } => NestedList::Insert {
                pos: *pos,
                value: value.translate_ids(from, interner),
            },
            NestedList::Update { pos, value } => NestedList::Update {
                pos: *pos,
                value: value.translate_ids(from, interner),
            },
            NestedList::Delete { pos } => NestedList::Delete { pos: *pos },
        }
    }
}

/// Internal state of a nested list CRDT
///
/// Maintains both the logical ordering of children (via EgWalker) and the
/// actual child CRDT instances.
#[derive(Debug, Clone)]
pub struct NestedListLog<L> {
    /// EgWalker list tracking the logical positions of children
    positions: EventGraph<SimpleList<EventId>>,
    /// Map from EventId to child CRDT instance
    children: HashMap<EventId, L>,
    /// Last deleted position for a child. If the child later receives concurrent
    /// nested updates and becomes non-default again, we can surface it back in
    /// the list view at its deleted position.
    deleted_positions: HashMap<EventId, usize>,
}

impl<L> Default for NestedListLog<L> {
    fn default() -> Self {
        Self {
            positions: EventGraph::default(),
            children: Default::default(),
            deleted_positions: Default::default(),
        }
    }
}

impl<L> NestedListLog<L> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn positions(&self) -> &EventGraph<SimpleList<EventId>> {
        &self.positions
    }

    #[allow(clippy::mutable_key_type)]
    pub fn children(&self) -> &HashMap<EventId, L> {
        &self.children
    }

    fn resolved_positions(&self) -> Vec<EventId>
    where
        L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
        <L as IsLog>::Value: IsSemanticallyEmpty,
    {
        let mut positions = self.positions.execute_query(Read::new());
        let mut hidden_children = self
            .deleted_positions
            .iter()
            .filter_map(|(id, pos)| {
                self.children
                    .get(id)
                    .filter(|child| {
                        let value = child.execute_query(Read::new());
                        !value.is_semantically_empty() && !positions.contains(id)
                    })
                    .map(|_| (id.clone(), *pos))
            })
            .collect::<Vec<_>>();

        hidden_children.sort_by(|(left_id, left_pos), (right_id, right_pos)| {
            left_pos
                .cmp(right_pos)
                .then_with(|| left_id.to_string().cmp(&right_id.to_string()))
        });

        for (offset, (id, pos)) in hidden_children.into_iter().enumerate() {
            positions.insert((pos + offset).min(positions.len()), id);
        }

        positions
    }
}

impl<L> IsLog for NestedListLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: IsSemanticallyEmpty,
{
    type Op = NestedList<L::Op>;
    type Value = Vec<L::Value>;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            NestedList::Insert { pos, value } => {
                let list_event = Event::new(
                    event.id().clone(),
                    event.lamport().clone(),
                    SimpleList::Insert {
                        pos,
                        content: event.id().clone(),
                    },
                    event.version().clone(),
                );
                self.positions.effect(list_event);
                let child_event = Event::unfold(event.clone(), value);
                self.children
                    .entry(event.id().clone())
                    .or_default()
                    .effect(child_event);
                self.deleted_positions.remove(event.id());
            }
            NestedList::Delete { pos } => {
                let positions_at_version = self.positions.eval(ReadAt::new(event.version()));
                let target = positions_at_version[pos].clone();
                let list_event = Event::unfold(event.clone(), SimpleList::Delete { pos });
                self.positions.effect(list_event);
                if let Some(child) = self.children.get_mut(&target) {
                    child.redundant_by_parent(event.version(), true);
                }
                self.deleted_positions
                    .entry(target)
                    .and_modify(|deleted_pos| *deleted_pos = (*deleted_pos).max(pos))
                    .or_insert(pos);
            }
            NestedList::Update { pos, value } => {
                let positions_at_version = self.positions.eval(ReadAt::new(event.version()));
                let target = positions_at_version[pos].clone();
                let list_event = Event::unfold(event.clone(), SimpleList::Update { pos });
                self.positions.effect(list_event);
                let child_event = Event::unfold(event, value);
                self.children
                    .entry(target.clone())
                    .or_default()
                    .effect(child_event);
                if self.positions.execute_query(Read::new()).contains(&target) {
                    self.deleted_positions.remove(&target);
                }
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        for child in self.children.values_mut() {
            child.stabilize(version);
        }
        // TODO: Check this works
        self.positions.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
        // TODO: Check this works
        self.positions.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.positions.is_default() && self.children.is_empty() && self.deleted_positions.is_empty()
    }

    fn prepare(op: Self::Op) -> Self::Op {
        op
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        let positions = self.positions.eval(Read::new());
        match op {
            NestedList::Insert { pos, value } => {
                *pos <= positions.len() && L::default().is_enabled(value)
            }
            NestedList::Update { pos, value } => {
                *pos < positions.len()
                    && self.children.get(&positions[*pos]).map_or_else(
                        || L::default().is_enabled(value),
                        |child| child.is_enabled(value),
                    )
            }
            NestedList::Delete { pos } => *pos < positions.len(),
        }
    }
}

impl<L> IsLogSink for NestedListLog<L>
where
    L: IsLogSink + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: IsSemanticallyEmpty,
{
    fn effect_with_sink(
        &mut self,
        event: Event<Self::Op>,
        path: ObjectPath,
        sink: &mut SinkCollector,
    ) {
        match event.op().clone() {
            NestedList::Insert { pos, value } => {
                let path = path.list_element(event.id().clone());
                let list_event = Event::new(
                    event.id().clone(),
                    event.lamport().clone(),
                    SimpleList::Insert {
                        pos,
                        content: event.id().clone(),
                    },
                    event.version().clone(),
                );
                sink.collect(Sink::create(path.clone()));
                self.positions
                    .effect_with_sink(list_event, path.clone(), sink);
                let child_event = Event::unfold(event.clone(), value);
                self.children
                    .entry(event.id().clone())
                    .or_default()
                    .effect_with_sink(child_event, path, sink);
                self.deleted_positions.remove(event.id());
            }
            NestedList::Delete { pos } => {
                let positions_at_version = self.positions.eval(ReadAt::new(event.version()));
                let target = positions_at_version[pos].clone();
                let path = path.list_element(target.clone());
                let list_event = Event::unfold(event.clone(), SimpleList::Delete { pos });
                sink.collect(Sink::delete(path.clone()));
                self.positions.effect_with_sink(list_event, path, sink);
                if let Some(child) = self.children.get_mut(&target) {
                    child.redundant_by_parent(event.version(), true);
                }
                self.deleted_positions
                    .entry(target)
                    .and_modify(|deleted_pos| *deleted_pos = (*deleted_pos).max(pos))
                    .or_insert(pos);
            }
            NestedList::Update { pos, value } => {
                let positions_at_version = self.positions.eval(ReadAt::new(event.version()));
                let target = positions_at_version[pos].clone();
                let path = path.list_element(target.clone());
                let list_event = Event::unfold(event.clone(), SimpleList::Update { pos });
                sink.collect(Sink::update(path.clone()));
                self.positions
                    .effect_with_sink(list_event, path.clone(), sink);
                let child_event = Event::unfold(event, value);
                self.children
                    .entry(target.clone())
                    .or_default()
                    .effect_with_sink(child_event, path, sink);
                if self.positions.execute_query(Read::new()).contains(&target) {
                    self.deleted_positions.remove(&target);
                }
            }
        }
    }
}

impl<L> DefaultSinkExpansion for NestedListLog<L>
where
    L: IsLogSink + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: IsSemanticallyEmpty,
{
}

impl<L> EvalNested<Read<<Self as IsLog>::Value>> for NestedListLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: IsSemanticallyEmpty,
{
    fn execute_query(
        &self,
        _q: Read<<Self as IsLog>::Value>,
    ) -> <Read<<Self as IsLog>::Value> as QueryOperation>::Response {
        let mut list = Vec::new();
        let positions = self.resolved_positions();
        for id in positions.iter() {
            let child = self.children.get(id).unwrap();
            list.push(child.execute_query(Read::new()));
        }
        list
    }
}

#[cfg(feature = "fuzz")]
impl<L> FuzzMetrics for NestedListLog<L>
where
    L: IsLog + FuzzMetrics + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: IsSemanticallyEmpty,
{
    fn structure_metrics(&self) -> StructureMetrics {
        StructureMetrics::nested_collection(
            self.resolved_positions()
                .into_iter()
                .filter_map(|id| self.children.get(&id))
                .map(FuzzMetrics::structure_metrics),
        )
    }
}

#[cfg(feature = "fuzz")]
impl<L> OpGeneratorNested for NestedListLog<L>
where
    L: OpGeneratorNested + IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: IsSemanticallyEmpty,
{
    fn generate(&self, rng: &mut impl rand::Rng) -> Self::Op {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Insert,
            Update,
            Delete,
        }
        let dist = WeightedIndex::new([2, 2, 1]).unwrap();

        let positions = self.positions.eval(Read::new());
        let choice = if positions.is_empty() {
            &Choice::Insert
        } else {
            &[Choice::Insert, Choice::Update, Choice::Delete][dist.sample(rng)]
        };

        let op = match choice {
            Choice::Insert => {
                let pos = rng.random_range(0..=positions.len());
                let default_child = L::new();
                let value = <L as OpGeneratorNested>::generate(&default_child, rng);
                NestedList::Insert { pos, value }
            }
            Choice::Update => {
                let pos = rng.random_range(0..positions.len());
                let target_id = &positions[pos];
                let value = if let Some(child) = self.children.get(target_id) {
                    <L as OpGeneratorNested>::generate(child, rng)
                } else {
                    let default_child = L::new();
                    <L as OpGeneratorNested>::generate(&default_child, rng)
                };
                NestedList::Update { pos, value }
            }
            Choice::Delete => {
                let pos = rng.random_range(0..positions.len());
                NestedList::Delete { pos }
            }
        };
        assert!(self.is_enabled(&op));
        op
    }
}

#[cfg(test)]
mod tests {
    use moirai_macros::record;
    use moirai_protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog};

    use crate::{
        HashMap,
        counter::resettable_counter::Counter,
        list::nested_list::{NestedList, NestedListLog},
        map::uw_map::{UWMap, UWMapLog},
        utils::membership::{triplet_log, twins_log},
    };

    #[test]
    fn simple_nested_list() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event = replica_a
            .send(NestedList::insert(0, Counter::Inc(10)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![10]);
        assert_eq!(replica_b.query(Read::new()), vec![10]);

        let event = replica_b
            .send(NestedList::update(0, Counter::Dec(5)))
            .unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![5]);
        assert_eq!(replica_b.query(Read::new()), vec![5]);

        let event = replica_a
            .send(NestedList::insert(1, Counter::Inc(10)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![5, 10]);
        assert_eq!(replica_b.query(Read::new()), vec![5, 10]);

        let event = replica_a
            .send(NestedList::update(0, Counter::Inc(1)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![6, 10]);
        assert_eq!(replica_b.query(Read::new()), vec![6, 10]);

        let event = replica_a.send(NestedList::delete(0)).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![10]);
        assert_eq!(replica_b.query(Read::new()), vec![10]);

        let event_a = replica_a
            .send(NestedList::insert(1, Counter::Inc(21)))
            .unwrap();
        let event_b = replica_b.send(NestedList::delete(0)).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), vec![21]);
        assert_eq!(replica_b.query(Read::new()), vec![21]);
    }

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedList::insert(0, Counter::Inc(10)))
            .unwrap();
        let event_b = replica_b
            .send(NestedList::insert(0, Counter::Inc(20)))
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), vec![10, 20]);
        assert_eq!(replica_b.query(Read::new()), vec![10, 20]);
    }

    #[test]
    fn insert_then_delete() {
        record!(Duet {
            first: VecLog<Counter<i32>>,
            second: VecLog<Counter<i32>>,
        });

        let (mut replica_a, _) = twins_log::<NestedListLog<DuetLog>>();

        let _ = replica_a
            .send(NestedList::insert(0, Duet::First(Counter::Inc(10))))
            .unwrap();
        let _ = replica_a.send(NestedList::delete(0)).unwrap();

        let list = replica_a.query(Read::new());
        assert_eq!(list, Vec::<DuetValue>::new());
    }

    #[test]
    fn concurrent_update_delete() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedList::insert(0, Counter::Inc(10)))
            .unwrap();
        replica_b.receive(event_a);

        let event_a = replica_a
            .send(NestedList::update(0, Counter::Inc(5)))
            .unwrap();

        let event_b = replica_b.send(NestedList::delete(0)).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), vec![5]);
        assert_eq!(replica_b.query(Read::new()), vec![5]);
        assert_eq!(replica_b.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_update_delete_insert() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let initial_insert = replica_a
            .send(NestedList::insert(0, Counter::Inc(10)))
            .unwrap();
        replica_b.receive(initial_insert.clone());
        replica_c.receive(initial_insert);

        let event_a = replica_a
            .send(NestedList::update(0, Counter::Inc(5)))
            .unwrap();

        let event_b = replica_b.send(NestedList::delete(0)).unwrap();
        let event_c = replica_c
            .send(NestedList::insert(0, Counter::Inc(15)))
            .unwrap();
        replica_a.receive(event_b.clone());
        replica_a.receive(event_c.clone());
        replica_b.receive(event_a.clone());
        replica_b.receive(event_c.clone());
        replica_c.receive(event_a);
        replica_c.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), vec![5, 15]);
        assert_eq!(replica_b.query(Read::new()), vec![5, 15]);
        assert_eq!(replica_c.query(Read::new()), vec![5, 15]);
    }

    #[test]
    fn concurrent_update_delete_insert_2() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_b_1 = replica_b
            .send(NestedList::insert(0, Counter::Inc(1)))
            .unwrap();
        replica_a.receive(event_b_1.clone());
        replica_c.receive(event_b_1);

        let event_b_2 = replica_b.send(NestedList::delete(0)).unwrap();

        let event_a_1 = replica_a
            .send(NestedList::update(0, Counter::Inc(1)))
            .unwrap();

        let event_c_1 = replica_c
            .send(NestedList::insert(0, Counter::Reset))
            .unwrap();

        let event_c_2 = replica_c.send(NestedList::delete(1)).unwrap();

        replica_a.receive(event_b_2.clone());
        replica_a.receive(event_c_1.clone());
        replica_a.receive(event_c_2.clone());

        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_c_2.clone());

        replica_c.receive(event_a_1);
        replica_c.receive(event_b_2);

        assert_eq!(replica_a.query(Read::new()), vec![0, 1]);
        assert_eq!(replica_b.query(Read::new()), vec![0, 1]);
        assert_eq!(replica_c.query(Read::new()), vec![0, 1]);
    }

    #[test]
    fn scenario_1() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedList::insert(0, Counter::Reset))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(NestedList::insert(0, Counter::Dec(64)))
            .unwrap();
        let event_a = replica_a
            .send(NestedList::insert(0, Counter::Dec(23)))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let event_b = replica_b.send(NestedList::delete(1)).unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn scenario_2() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedList::insert(0, Counter::Dec(22)))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(NestedList::update(0, Counter::Reset))
            .unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a
            .send(NestedList::update(0, Counter::Inc(40)))
            .unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b
            .send(NestedList::insert(0, Counter::Inc(47)))
            .unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn scenario_3() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a
            .send(NestedList::insert(0, Counter::Dec(22)))
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(NestedList::insert(1, Counter::Inc(30)))
            .unwrap();
        replica_a.receive(event_b);

        let event_b = replica_b.send(NestedList::delete(0)).unwrap();
        let event_a = replica_a
            .send(NestedList::update(1, Counter::Inc(40)))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn map_of_list() {
        let (mut replica_a, mut replica_b) =
            twins_log::<UWMapLog<&str, NestedListLog<VecLog<Counter<i32>>>>>();

        let event_a = replica_a
            .send(UWMap::Update("a", NestedList::insert(0, Counter::Inc(10))))
            .unwrap();
        let event_a_2 = replica_a
            .send(UWMap::Update("a", NestedList::insert(1, Counter::Inc(5))))
            .unwrap();
        let event_a_3 = replica_a
            .send(UWMap::Update("a", NestedList::update(0, Counter::Inc(1))))
            .unwrap();

        let mut result = HashMap::default();
        result.insert("a", vec![11, 5]);
        assert_eq!(replica_a.query(Read::new()), result);

        replica_b.receive(event_a);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);

        assert_eq!(replica_b.query(Read::new()), result);

        let event_b = replica_b.send(UWMap::Remove("a")).unwrap();
        let event_a = replica_a
            .send(UWMap::Update("a", NestedList::insert(0, Counter::Inc(100))))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let mut result = HashMap::default();
        result.insert("a", vec![100]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_nested_list_counter() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run = RunConfig::new(0.6, 6, 20, None, None, true, false);
        let runs = vec![run.clone(); 10];

        let config = FuzzerConfig::<NestedListLog<VecLog<Counter<i32>>>>::new(
            "nested_list_counter",
            runs,
            true,
            |a, b| a == b,
            false,
        );

        fuzzer::<NestedListLog<VecLog<Counter<i32>>>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_nested_list_string() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::event_graph::EventGraph;

        use crate::list::eg_walker::List;

        let run = RunConfig::new(0.7, 4, 100, None, None, false, false);
        let runs = vec![run.clone(); 1_000];

        let config = FuzzerConfig::<NestedListLog<EventGraph<List<char>>>>::new(
            "nested_list_string",
            runs,
            true,
            |a, b| a == b,
            false,
        );

        fuzzer::<NestedListLog<EventGraph<List<char>>>>(config);
    }
}

impl<O> Boxer<NestedList<O>> for NestedList<Box<O>> {
    fn boxer(self) -> NestedList<O> {
        match self {
            NestedList::Insert { pos, value } => NestedList::Insert { pos, value: *value },
            NestedList::Update { pos, value } => NestedList::Update { pos, value: *value },
            NestedList::Delete { pos } => NestedList::Delete { pos },
        }
    }
}

impl<O> Boxer<NestedList<Box<O>>> for NestedList<O> {
    fn boxer(self) -> NestedList<Box<O>> {
        match self {
            NestedList::Insert { pos, value } => NestedList::Insert {
                pos,
                value: Box::new(value),
            },
            NestedList::Update { pos, value } => NestedList::Update {
                pos,
                value: Box::new(value),
            },
            NestedList::Delete { pos } => NestedList::Delete { pos },
        }
    }
}
