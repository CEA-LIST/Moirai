use std::fmt::Debug;

use crate::{
    crdt::list::eg_walker::List as SimpleList,
    protocol::{
        clock::version_vector::Version,
        crdt::pure_crdt::{QueryOperation, Read},
        event::{id::EventId, Event},
        state::{
            event_graph::EventGraph,
            log::{EvalNested, IsLog},
        },
    },
    HashMap,
};

#[derive(Clone, Debug)]
pub enum List<O> {
    Insert { pos: usize, value: O },
    Set { pos: usize, value: O },
    Delete { pos: usize },
}

impl<O> List<Box<O>> {
    pub fn boxed(op: List<O>) -> List<Box<O>> {
        match op {
            List::Insert { pos, value } => List::Insert {
                pos,
                value: Box::new(value),
            },
            List::Set { pos, value } => List::Set {
                pos,
                value: Box::new(value),
            },
            List::Delete { pos } => List::Delete { pos },
        }
    }

    pub fn unboxed(self) -> List<O> {
        match self {
            List::Insert { pos, value } => List::Insert { pos, value: *value },
            List::Set { pos, value } => List::Set { pos, value: *value },
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

    pub fn set(pos: usize, value: O) -> Self {
        Self::Set { pos, value }
    }
}

#[derive(Debug, Clone)]
pub struct ListLog<L> {
    position: EventGraph<SimpleList<EventId>>,
    children: HashMap<EventId, L>,
}

impl<L> ListLog<L>
where
    L: IsLog,
{
    #[allow(dead_code)]
    pub(crate) fn incorporate(&mut self, event: Event<L::Op>, log: L) {
        let id = event.id().clone();
        let event = Event::unfold(
            event,
            SimpleList::Insert {
                content: id.clone(),
                pos: 0,
            },
        );
        self.position.effect(event);
        self.children.insert(id, log);
    }
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
            List::Set { pos, value } => {
                // TODO: this precondition check should not appear here
                let positions = self.position.eval(Read::new());
                if pos >= positions.len() {
                    panic!(
                        "Set position {} out of bounds (len={})",
                        pos,
                        positions.len()
                    );
                }
                let target_id = &positions[pos];
                let child_event = Event::unfold(event.clone(), value);
                self.children
                    .get_mut(target_id)
                    .unwrap()
                    .effect(child_event);
            }
            List::Delete { pos } => {
                let list_event = Event::unfold(event, SimpleList::Delete { pos });
                self.position.effect(list_event);
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

    fn len(&self) -> usize {
        self.children.values().map(|c| c.len()).sum()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn prepare(op: Self::Op) -> Self::Op {
        op
    }

    fn is_enabled(&self, _op: &Self::Op) -> bool {
        true
    }

    fn eval<Q>(&self, q: Q) -> Q::Response
    where
        Q: QueryOperation,
        Self: EvalNested<Q>,
    {
        Self::execute_query(self, q)
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

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            list::nested_list::{List, ListLog},
            test_util::twins_log,
        },
        protocol::{crdt::pure_crdt::Read, replica::IsReplica, state::po_log::VecLog},
    };

    #[test]
    fn simple_nested_list() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<VecLog<Counter<i32>>>>();

        let event = replica_a.send(List::insert(0, Counter::Inc(10))).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![10]);
        assert_eq!(replica_b.query(Read::new()), vec![10]);

        let event = replica_b.send(List::set(0, Counter::Dec(5))).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![5]);
        assert_eq!(replica_b.query(Read::new()), vec![5]);

        let event = replica_a.send(List::insert(1, Counter::Inc(10))).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), vec![5, 10]);
        assert_eq!(replica_b.query(Read::new()), vec![5, 10]);

        let event = replica_a.send(List::set(0, Counter::Inc(1))).unwrap();
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
}
