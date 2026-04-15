use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::metrics::{FuzzMetrics, StructureMetrics};
#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGeneratorNested;
#[cfg(feature = "sink")]
use moirai_protocol::state::sink::{Sink, SinkCollector};
#[cfg(feature = "sink")]
use moirai_protocol::state::{object_path::ObjectPath, sink::SinkOwnership};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::{Event, id::EventId},
    state::{event_graph::EventGraph, log::IsLog},
    utils::{
        boxer::Boxer,
        intern_str::{InternalizeOp, Interner},
    },
};
#[cfg(feature = "fuzz")]
use rand::RngExt;

use crate::{
    list::eg_walker::{List as SimpleList, ReadAt},
    map::uw_map::{UWMap, UWMapLog},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum NestedList<O> {
    /// Insert a new child CRDT at the given position
    Insert { pos: usize, op: O },
    /// Update the child at the given position
    Update { pos: usize, op: O },
    /// Delete the child at the given position
    Delete { pos: usize },
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
    children: UWMapLog<EventId, L>,
}

impl<L> NestedListLog<L> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn positions(&self) -> &EventGraph<SimpleList<EventId>> {
        &self.positions
    }

    #[allow(clippy::mutable_key_type)]
    pub fn children(&self) -> &UWMapLog<EventId, L> {
        &self.children
    }
}

impl<L> IsLog for NestedListLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    type Op = NestedList<L::Op>;
    type Value = Vec<L::Value>;

    fn new() -> Self {
        Self::default()
    }

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        #[cfg(feature = "sink")] path: ObjectPath,
        #[cfg(feature = "sink")] sink: &mut SinkCollector,
        #[cfg(feature = "sink")] _ownership: SinkOwnership,
    ) {
        match event.op().clone() {
            NestedList::Insert { pos, op } => {
                #[cfg(feature = "sink")]
                let path = path.list_element(event.id().clone());
                #[cfg(feature = "sink")]
                sink.collect(Sink::create(path.clone()));
                let list_event = Event::new(
                    event.id().clone(),
                    *event.lamport(),
                    SimpleList::Insert {
                        pos,
                        content: event.id().clone(),
                    },
                    event.version().clone(),
                );
                self.positions.effect(
                    list_event,
                    #[cfg(feature = "sink")]
                    path.clone(),
                    #[cfg(feature = "sink")]
                    sink,
                    #[cfg(feature = "sink")]
                    SinkOwnership::Delegated,
                );
                let child_event =
                    Event::unfold(event.clone(), UWMap::Update(event.id().clone(), op));
                self.children.effect(
                    child_event,
                    #[cfg(feature = "sink")]
                    path.clone(),
                    #[cfg(feature = "sink")]
                    sink,
                    #[cfg(feature = "sink")]
                    SinkOwnership::Delegated,
                );
            }
            NestedList::Delete { pos } => {
                let positions_at_version = self.positions.eval(ReadAt::new(event.version()));
                let target = positions_at_version[pos].clone();
                #[cfg(feature = "sink")]
                let path = path.list_element(target.clone());
                #[cfg(feature = "sink")]
                sink.collect(Sink::delete(path.clone()));
                let list_event = Event::unfold(event.clone(), SimpleList::Delete { pos });
                self.positions.effect(
                    list_event,
                    #[cfg(feature = "sink")]
                    path.clone(),
                    #[cfg(feature = "sink")]
                    sink,
                    #[cfg(feature = "sink")]
                    SinkOwnership::Delegated,
                );
                let map_event = Event::unfold(event.clone(), UWMap::Remove(target));
                self.children.effect(
                    map_event,
                    #[cfg(feature = "sink")]
                    path.clone(),
                    #[cfg(feature = "sink")]
                    sink,
                    #[cfg(feature = "sink")]
                    SinkOwnership::Delegated,
                );
            }
            NestedList::Update { pos, op } => {
                let positions_at_version = self.positions.eval(ReadAt::new(event.version()));
                let target = positions_at_version[pos].clone();
                #[cfg(feature = "sink")]
                let path = path.list_element(target.clone());
                #[cfg(feature = "sink")]
                sink.collect(Sink::update(path.clone()));
                let list_event = Event::unfold(event.clone(), SimpleList::Update { pos });
                let map_event = Event::unfold(event.clone(), UWMap::Update(target.clone(), op));
                self.positions.effect(
                    list_event,
                    #[cfg(feature = "sink")]
                    path.clone(),
                    #[cfg(feature = "sink")]
                    sink,
                    #[cfg(feature = "sink")]
                    SinkOwnership::Delegated,
                );
                self.children.effect(
                    map_event,
                    #[cfg(feature = "sink")]
                    path.clone(),
                    #[cfg(feature = "sink")]
                    sink,
                    #[cfg(feature = "sink")]
                    SinkOwnership::Delegated,
                );
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        self.children.stabilize(version);
        self.positions.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.children.redundant_by_parent(version, conservative);
        self.positions.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.positions.is_default() && self.children.is_default()
    }

    fn prepare(op: Self::Op) -> Self::Op {
        op
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        let positions = self.positions.eval(Read::new());
        match op {
            NestedList::Insert { pos, op } => {
                *pos <= positions.len() && L::default().is_enabled(op)
            }
            NestedList::Update { pos, op } => {
                let out_of_bound = *pos >= positions.len();
                if out_of_bound {
                    return false;
                }
                let target = positions[*pos].clone();
                let map_op = UWMap::Update(target, op.clone());
                self.children.is_enabled(&map_op)
            }
            NestedList::Delete { pos } => *pos < positions.len(),
        }
    }
}

impl<L> EvalNested<Read<<Self as IsLog>::Value>> for NestedListLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
    <L as IsLog>::Value: PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<<Self as IsLog>::Value>,
    ) -> <Read<<Self as IsLog>::Value> as QueryOperation>::Response {
        let mut list = Vec::new();
        let positions = self.positions.execute_query(Read::new());
        // println!(
        //     "Positions: {}",
        //     positions
        //         .iter()
        //         .map(|id| format!("{}", id))
        //         .collect::<Vec<_>>()
        //         .join(", ")
        // );
        #[allow(clippy::mutable_key_type)]
        let mut map = self.children.execute_query(Read::new());
        // println!(
        //     "Children: {}",
        //     map.iter()
        //         .map(|(id, child)| format!("{}: {:?}", id, child))
        //         .collect::<Vec<_>>()
        //         .join(", ")
        // );
        for eid in positions {
            if let Some(child) = map.remove(&eid) {
                list.push(child);
            }
        }
        list
    }
}

#[cfg(feature = "fuzz")]
impl<L> FuzzMetrics for NestedListLog<L>
where
    L: IsLog + FuzzMetrics + EvalNested<Read<<L as IsLog>::Value>>,
{
    fn structure_metrics(&self) -> StructureMetrics {
        // StructureMetrics::nested_collection(
        //     self.resolved_positions()
        //         .into_iter()
        //         .filter_map(|id| self.children.get(&id))
        //         .map(FuzzMetrics::structure_metrics),
        // )
        // TODO
        StructureMetrics::empty()
    }
}

#[cfg(feature = "fuzz")]
impl<L> OpGeneratorNested for NestedListLog<L>
where
    L: OpGeneratorNested + IsLog + EvalNested<Read<<L as IsLog>::Value>>,
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
                let op = <L as OpGeneratorNested>::generate(&default_child, rng);
                NestedList::Insert { pos, op }
            }
            Choice::Update => {
                let pos = rng.random_range(0..positions.len());
                let target_id = &positions[pos];
                let child = self.children.get_child(target_id);
                let op = if let Some(c) = child {
                    <L as OpGeneratorNested>::generate(c, rng)
                } else {
                    let default_child = L::new();
                    <L as OpGeneratorNested>::generate(&default_child, rng)
                };
                NestedList::Update { pos, op }
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

impl<O> InternalizeOp for NestedList<O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            NestedList::Insert { pos, op } => NestedList::Insert {
                pos,
                op: op.internalize(interner),
            },
            NestedList::Update { pos, op } => NestedList::Update {
                pos,
                op: op.internalize(interner),
            },
            NestedList::Delete { pos } => NestedList::Delete { pos },
        }
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

        assert_eq!(replica_b.query(Read::new()), vec![21]);
        assert_eq!(replica_a.query(Read::new()), vec![21]);
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

        assert_eq!(replica_b.query(Read::new()), vec![5]);
        assert_eq!(replica_a.query(Read::new()), vec![5]);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
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

        assert_eq!(replica_a.query(Read::new()), vec![15, 5]);
        assert_eq!(replica_b.query(Read::new()), vec![15, 5]);
        assert_eq!(replica_c.query(Read::new()), vec![15, 5]);
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

        assert_eq!(replica_a.query(Read::new()), vec![1]);
        assert_eq!(replica_b.query(Read::new()), vec![1]);
        assert_eq!(replica_c.query(Read::new()), vec![1]);
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
    #[ignore]
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
            None,
        );

        fuzzer::<NestedListLog<VecLog<Counter<i32>>>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_nested_list_string() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::event_graph::EventGraph;

        use crate::list::eg_walker::List;

        let run = RunConfig::new(0.6, 4, 25, None, None, true, false);
        let runs = vec![run.clone(); 10_000];

        let config = FuzzerConfig::<NestedListLog<EventGraph<List<char>>>>::new(
            "nested_list_string",
            runs,
            true,
            |a, b| a == b,
            false,
            None,
        );

        fuzzer::<NestedListLog<EventGraph<List<char>>>>(config);
    }
}

impl<O> Boxer<NestedList<O>> for NestedList<Box<O>> {
    fn boxer(self) -> NestedList<O> {
        match self {
            NestedList::Insert { pos, op } => NestedList::Insert { pos, op: *op },
            NestedList::Update { pos, op } => NestedList::Update { pos, op: *op },
            NestedList::Delete { pos } => NestedList::Delete { pos },
        }
    }
}

impl<O> Boxer<NestedList<Box<O>>> for NestedList<O> {
    fn boxer(self) -> NestedList<Box<O>> {
        match self {
            NestedList::Insert { pos, op } => NestedList::Insert {
                pos,
                op: Box::new(op),
            },
            NestedList::Update { pos, op } => NestedList::Update {
                pos,
                op: Box::new(op),
            },
            NestedList::Delete { pos } => NestedList::Delete { pos },
        }
    }
}

impl<L> Default for NestedListLog<L> {
    fn default() -> Self {
        Self {
            positions: EventGraph::default(),
            children: Default::default(),
        }
    }
}

impl<O> NestedList<O> {
    pub fn insert(pos: usize, op: O) -> Self {
        Self::Insert { pos, op }
    }

    pub fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    pub fn update(pos: usize, op: O) -> Self {
        Self::Update { pos, op }
    }
}
