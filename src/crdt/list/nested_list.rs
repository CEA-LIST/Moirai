use std::fmt::Debug;

#[cfg(feature = "fuzz")]
use crate::fuzz::config::OpGeneratorNested;
use crate::{
    crdt::list::eg_walker::{List as SimpleList, MutationTarget},
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::EvalNested,
            query::{QueryOperation, Read},
        },
        event::{id::EventId, Event},
        state::{event_graph::EventGraph, log::IsLog},
    },
    utils::unboxer::Unboxer,
    HashMap,
};

#[derive(Clone, Debug)]
pub enum List<O> {
    /// Insert a new child CRDT at the given position
    Insert { pos: usize, value: O },
    /// Update the child at the given position
    Update { pos: usize, value: O },
    /// Delete the child at the given position
    Delete { pos: usize },
}

// impl<K, O> Unboxer<UWMap<K, O>> for UWMap<K, Box<O>> {
//     fn unbox(self) -> UWMap<K, O> {
//         match self {
//             UWMap::Update(k, v) => UWMap::Update(k, *v),
//             UWMap::Remove(k) => UWMap::Remove(k),
//             UWMap::Clear => UWMap::Clear,
//         }
//     }
// }

// impl<K, O> Unboxer<UWMap<K, Box<O>>> for UWMap<K, O> {
//     fn unbox(self) -> UWMap<K, Box<O>> {
//         match self {
//             UWMap::Update(k, v) => UWMap::Update(k, Box::new(v)),
//             UWMap::Remove(k) => UWMap::Remove(k),
//             UWMap::Clear => UWMap::Clear,
//         }
//     }
// }

impl<O> Unboxer<List<O>> for List<Box<O>> {
    fn unbox(self) -> List<O> {
        match self {
            List::Insert { pos, value } => List::Insert { pos, value: *value },
            List::Update { pos, value } => List::Update { pos, value: *value },
            List::Delete { pos } => List::Delete { pos },
        }
    }
}

impl<O> Unboxer<List<Box<O>>> for List<O> {
    fn unbox(self) -> List<Box<O>> {
        match self {
            List::Insert { pos, value } => List::Insert {
                pos,
                value: Box::new(value),
            },
            List::Update { pos, value } => List::Update {
                pos,
                value: Box::new(value),
            },
            List::Delete { pos } => List::Delete { pos },
        }
    }
}

impl<O> List<O> {
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
pub struct ListLog<L> {
    /// EgWalker list tracking the logical positions of children
    position: EventGraph<SimpleList<EventId>>,
    /// Map from EventId to child CRDT instance
    children: HashMap<EventId, L>,
}

impl<L> Default for ListLog<L> {
    fn default() -> Self {
        Self {
            position: EventGraph::default(),
            children: Default::default(),
        }
    }
}

impl<L> IsLog for ListLog<L>
where
    L: IsLog,
{
    type Op = List<L::Op>;
    type Value = Vec<L::Value>;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            List::Insert { pos, value } => {
                let list_event = Event::new(
                    event.id().clone(),
                    event.lamport().clone(),
                    SimpleList::Insert {
                        pos,
                        content: event.id().clone(),
                    },
                    event.version().clone(),
                );
                self.position.effect(list_event);
                let child_event = Event::unfold(event.clone(), value);
                self.children
                    .entry(event.id().clone())
                    .or_default()
                    .effect(child_event);
            }
            List::Delete { pos } => {
                let list_event = Event::unfold(event, SimpleList::Delete { pos });
                self.position.effect(list_event);
            }
            List::Update { pos, value } => {
                let list_event = Event::unfold(event.clone(), SimpleList::Update { pos });
                self.position.effect(list_event);
                let target = self.position.eval(MutationTarget::new(event.id().clone()));
                let child_event = Event::unfold(event, value);
                self.children.get_mut(&target).unwrap().effect(child_event);
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        for child in self.children.values_mut() {
            child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.position.is_default() && self.children.is_empty()
    }

    fn prepare(op: Self::Op) -> Self::Op {
        op
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        let positions = self.position.eval(Read::new());
        match op {
            List::Insert { pos, .. } => *pos <= positions.len(),
            List::Update { pos, .. } => *pos < positions.len(),
            List::Delete { pos } => *pos < positions.len(),
        }
    }
}

impl<L> EvalNested<Read<<Self as IsLog>::Value>> for ListLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    fn execute_query(
        &self,
        _q: Read<<Self as IsLog>::Value>,
    ) -> <Read<<Self as IsLog>::Value> as QueryOperation>::Response {
        let mut list = Vec::new();
        let positions = self.position.execute_query(Read::new());
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
        use rand::Rng;

        enum Choice {
            Insert,
            Update,
            Delete,
        }
        let positions = self.position.eval(Read::new());
        let choice = if positions.is_empty() {
            &Choice::Insert
        } else {
            rand::seq::IteratorRandom::choose(
                [Choice::Insert, Choice::Update, Choice::Delete].iter(),
                rng,
            )
            .unwrap()
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
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            list::nested_list::{List, ListLog},
            test_util::twins_log,
        },
        protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog},
    };

    #[test]
    fn simple_nested_list() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<VecLog<Counter<i32>>>>();

        let event = replica_a.send(List::insert(0, Counter::Inc(10))).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![10]);
        assert_eq!(replica_b.query(Read::new()), vec![10]);

        let event = replica_b.send(List::update(0, Counter::Dec(5))).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![5]);
        assert_eq!(replica_b.query(Read::new()), vec![5]);

        let event = replica_a.send(List::insert(1, Counter::Inc(10))).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![5, 10]);
        assert_eq!(replica_b.query(Read::new()), vec![5, 10]);

        let event = replica_a.send(List::update(0, Counter::Inc(1))).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![6, 10]);
        assert_eq!(replica_b.query(Read::new()), vec![6, 10]);

        let event = replica_a.send(List::delete(0)).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![10]);
        assert_eq!(replica_b.query(Read::new()), vec![10]);

        let event_a = replica_a.send(List::insert(1, Counter::Inc(21))).unwrap();
        let event_b = replica_b.send(List::delete(0)).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), vec![21]);
        assert_eq!(replica_b.query(Read::new()), vec![21]);
    }

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(List::insert(0, Counter::Inc(10))).unwrap();
        let event_b = replica_b.send(List::insert(0, Counter::Inc(20))).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()), vec![10, 20]);
        assert_eq!(replica_b.query(Read::new()), vec![10, 20]);
    }

    #[test]
    fn scenario_1() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(List::insert(0, Counter::Reset)).unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(List::insert(0, Counter::Dec(64))).unwrap();
        let event_a = replica_a.send(List::insert(0, Counter::Dec(23))).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let event_b = replica_b.send(List::delete(1)).unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn scenario_2() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(List::insert(0, Counter::Dec(22))).unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(List::update(0, Counter::Reset)).unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a.send(List::update(0, Counter::Inc(40))).unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(List::insert(0, Counter::Inc(47))).unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn scenario_3() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<VecLog<Counter<i32>>>>();

        let event_a = replica_a.send(List::insert(0, Counter::Dec(22))).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(List::insert(1, Counter::Inc(30))).unwrap();
        replica_a.receive(event_b);

        let event_b = replica_b.send(List::delete(0)).unwrap();
        let event_a = replica_a.send(List::update(1, Counter::Inc(40))).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_nested_list() {
        use crate::fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer,
        };

        let run = RunConfig::new(0.8, 8, 10, None, None, true);
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
