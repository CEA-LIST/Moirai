use std::fmt::Debug;

#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGeneratorNested;
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::{Event, id::EventId},
    state::{event_graph::EventGraph, log::IsLog},
    utils::boxer::Boxer,
};

use crate::{
    HashMap,
    list::eg_walker::{List as SimpleList, MutationTarget},
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
}

impl<L> Default for NestedListLog<L> {
    fn default() -> Self {
        Self {
            positions: EventGraph::default(),
            children: Default::default(),
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

    pub fn children(&self) -> &HashMap<EventId, L> {
        &self.children
    }
}

impl<L> IsLog for NestedListLog<L>
where
    L: IsLog,
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
            }
            NestedList::Delete { pos } => {
                let list_event = Event::unfold(event, SimpleList::Delete { pos });
                self.positions.effect(list_event);
            }
            NestedList::Update { pos, value } => {
                let list_event = Event::unfold(event.clone(), SimpleList::Update { pos });
                self.positions.effect(list_event);
                let target = self.positions.eval(MutationTarget::new(event.id().clone()));
                let child_event = Event::unfold(event, value);
                self.children.get_mut(&target).unwrap().effect(child_event);
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
        self.positions.is_default() && self.children.is_empty()
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
                    && self
                        .children
                        .get(&positions[*pos])
                        .is_some_and(|c| c.is_enabled(value))
            }
            NestedList::Delete { pos } => *pos < positions.len(),
        }
    }
}

impl<L> EvalNested<Read<<Self as IsLog>::Value>> for NestedListLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    fn execute_query(
        &self,
        _q: Read<<Self as IsLog>::Value>,
    ) -> <Read<<Self as IsLog>::Value> as QueryOperation>::Response {
        let mut list = Vec::new();
        let positions = self.positions.execute_query(Read::new());
        for id in positions.iter() {
            let child = self.children.get(id).unwrap();
            list.push(child.execute_query(Read::new()));
        }
        list
    }
}

#[cfg(feature = "fuzz")]
impl<L> OpGeneratorNested for ListLog<L>
where
    L: OpGeneratorNested,
{
    fn generate(&self, rng: &mut impl rand::RngCore) -> Self::Op {
        use rand::{
            Rng,
            distr::{Distribution, weighted::WeightedIndex},
        };

        enum Choice {
            Insert,
            Update,
            Delete,
        }
        let dist = WeightedIndex::new([2, 2, 1]).unwrap();

        let positions = self.position.eval(Read::new());
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
                List::Insert { pos, value }
            }
            Choice::Update => {
                let pos = rng.random_range(0..positions.len());
                let target_id = &positions[pos];
                let child = self.children.get(target_id).unwrap();
                let value = <L as OpGeneratorNested>::generate(child, rng);
                List::Update { pos, value }
            }
            Choice::Delete => {
                let pos = rng.random_range(0..positions.len());
                List::Delete { pos }
            }
        };
        assert!(self.is_enabled(&op));
        op
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog};

    use crate::{
        HashMap,
        counter::resettable_counter::Counter,
        list::nested_list::{NestedList, NestedListLog},
        map::uw_map::{UWMap, UWMapLog},
        utils::membership::twins_log,
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
    fn fuzz_nested_list() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run = RunConfig::new(0.8, 8, 10, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<ListLog<VecLog<Counter<i32>>>>::new(
            "nested_list",
            runs,
            true,
            |a, b| a == b,
            false,
        );

        fuzzer::<ListLog<VecLog<Counter<i32>>>>(config);
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
