use moirai_protocol::{
    clock::version_vector::Version,
    crdt::query::Read,
    event::Event,
    state::{event_graph::EventGraph, log::IsLog, unstable_state::IsUnstableState},
};
use std::fmt::Debug;

use crate::list::eg_walker::List as SimpleList;

#[derive(Clone, Debug)]
pub enum UniqueList<V> {
    Insert { content: V, pos: usize },
    Delete { pos: usize },
}

#[derive(Debug, Clone)]
pub struct UniqueListLog<V>(EventGraph<SimpleList<V>>);

impl<V> Default for UniqueListLog<V> {
    fn default() -> Self {
        Self(EventGraph::<SimpleList<V>>::default())
    }
}

impl<V> IsLog for UniqueListLog<V>
where
    V: Clone + Debug + PartialEq,
{
    type Op = UniqueList<V>;
    type Value = Vec<V>;

    fn new() -> Self {
        Self::default()
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        let list = self.0.eval(Read::new());
        match op {
            UniqueList::Insert { pos, content } => *pos <= list.len() && !list.contains(content),
            UniqueList::Delete { pos } => *pos < list.len(),
        }
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        let op = match event.op() {
            UniqueList::Insert { content, pos } => SimpleList::Insert {
                content: content.clone(),
                pos: *pos,
            },
            UniqueList::Delete { pos } => SimpleList::Delete { pos: *pos },
        };
        let event = Event::unfold(event, op);
        // match event.op() {
        //     SimpleList::Insert { content, pos } => {
        //         let list = self.0.eval(Read::new());
        //         self.0.effect(event);

        //         if list.contains(content) {
        //         }
        //     }
        //     SimpleList::Delete { .. } => self.0.effect(event),
        //     _ => unreachable!(),
        // }
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}
