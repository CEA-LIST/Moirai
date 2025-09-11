use std::collections::HashMap;
use std::fmt::Debug;

use crate::crdt::list::list::List as SimpleList;
use crate::protocol::clock::version_vector::Version;
use crate::protocol::event::id::EventId;
use crate::protocol::event::Event;
use crate::protocol::state::event_graph::EventGraph;
use crate::protocol::state::log::IsLog;

#[derive(Clone, Debug)]
pub enum List<O> {
    Insert { pos: usize, value: O },
    Set { pos: usize, value: O },
    Delete { pos: usize },
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

#[derive(Debug)]
pub struct ListLog<L> {
    position: EventGraph<SimpleList<EventId>>,
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
            List::Set { pos, value } => {
                let positions = self.position.eval();
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
                let list_event = Event::new(
                    event.id().clone(),
                    event.lamport().clone(),
                    SimpleList::Delete { pos },
                    event.version().clone(),
                );
                self.position.effect(list_event);
            }
        }
    }

    fn eval(&self) -> Self::Value {
        let mut list = Self::Value::new();
        let positions = self.position.eval();
        for id in positions.iter() {
            let child = self.children.get(id).unwrap();
            list.push(child.eval());
        }
        list
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
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            list::nested_list::{List, ListLog},
            test_util::twins_log,
        },
        protocol::{event::tagged_op::TaggedOp, replica::IsReplica, state::po_log::POLog},
    };

    #[test]
    fn simple_nested_list() {
        let (mut replica_a, mut replica_b) =
            twins_log::<ListLog<POLog<Counter<i32>, Vec<TaggedOp<Counter<i32>>>>>>();

        let event = replica_a.send(List::insert(0, Counter::Inc(10)));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), vec![10]);
        assert_eq!(replica_b.query(), vec![10]);

        let event = replica_b.send(List::set(0, Counter::Dec(5)));
        replica_a.receive(event);

        assert_eq!(replica_a.query(), vec![5]);
        assert_eq!(replica_b.query(), vec![5]);

        let event = replica_a.send(List::insert(1, Counter::Inc(10)));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), vec![5, 10]);
        assert_eq!(replica_b.query(), vec![5, 10]);

        let event = replica_a.send(List::set(0, Counter::Inc(1)));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), vec![6, 10]);
        assert_eq!(replica_b.query(), vec![6, 10]);

        let event = replica_a.send(List::delete(0));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), vec![10]);
        assert_eq!(replica_b.query(), vec![10]);

        let event_a = replica_a.send(List::insert(1, Counter::Inc(21)));
        let event_b = replica_b.send(List::delete(0));

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(), vec![21]);
        assert_eq!(replica_b.query(), vec![21]);
    }
}
