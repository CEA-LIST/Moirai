use std::{collections::HashSet, fmt::Debug};

use bimap::BiMap;
use petgraph::{
    graph::NodeIndex,
    prelude::StableDiGraph,
    visit::{Dfs, Visitable},
    Direction,
};

use crate::protocol::{
    clock::version_vector::Version,
    crdt::pure_crdt::PureCRDT,
    event::{id::EventId, tagged_op::TaggedOp, Event},
    state::{log::IsLog, unstable_state::IsUnstableState},
};

#[derive(Debug)]
pub struct EventGraph<O> {
    // TODO: stable or not?
    graph: StableDiGraph<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
    _heads: HashSet<EventId>,
}

impl<O> IsLog for EventGraph<O>
where
    O: PureCRDT + Clone,
{
    type Op = O;
    type Value = O::Value;

    fn new() -> Self {
        assert!(O::DISABLE_R_WHEN_NOT_R && O::DISABLE_R_WHEN_R && O::DISABLE_STABILIZE);
        Default::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        IsUnstableState::append(self, event);
    }

    fn eval(&self) -> Self::Value {
        O::eval(&O::StableState::default(), self)
    }

    fn stabilize(&mut self, _version: &Version) {}

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        if conservative {
            let to_remove = self.collect(version);
            for nx in to_remove {
                self.graph.remove_node(nx);
            }
        } else {
            *self = Self::default();
        }
    }

    fn len(&self) -> usize {
        IsUnstableState::len(self)
    }

    fn is_empty(&self) -> bool {
        IsUnstableState::is_empty(self)
    }
}

impl<O> IsUnstableState<O> for EventGraph<O>
where
    O: Clone + Debug,
{
    fn append(&mut self, event: Event<O>) {
        let new_tagged_op = TaggedOp::from(&event);
        let child_idx = self.graph.add_node(new_tagged_op);
        self.map.insert(child_idx, event.id().clone());
        for event_id in event
            .version()
            .iter()
            .filter(|id| *id != *event.id() && id.seq() > 0)
        {
            let parent_idx = self.map.get_by_right(&event_id).unwrap();
            self.graph.add_edge(child_idx, *parent_idx, ());
        }
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.map
            .get_by_right(event_id)
            .and_then(|idx| self.graph.node_weight(*idx))
    }

    fn remove(&mut self, event_id: &EventId) {
        if let Some(idx) = self.map.get_by_right(event_id) {
            self.graph.remove_node(*idx);
            self.map.remove_by_right(event_id);
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.graph.node_weights()
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        let to_remove: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|nx| {
                let tagged_op = self.graph.node_weight(*nx).unwrap();
                !predicate(tagged_op)
            })
            .collect();

        for nx in to_remove {
            let event_id = self.map.get_by_left(&nx).unwrap().clone();
            self.graph.remove_node(nx);
            self.map.remove_by_right(&event_id);
        }
    }

    fn len(&self) -> usize {
        self.graph.node_count()
    }

    fn is_empty(&self) -> bool {
        IsUnstableState::len(self) == 0
    }

    fn clear(&mut self) {
        self.graph.clear();
        self.map.clear();
        self._heads.clear();
    }

    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>> {
        self.collect(version)
            .iter()
            .filter_map(|nx| self.graph.node_weight(*nx).cloned())
            .collect()
    }

    fn parents(&self, event_id: &EventId) -> Vec<EventId> {
        let node_idx = self.map.get_by_right(event_id);
        match node_idx {
            Some(idx) => self
                .graph
                .neighbors_directed(*idx, Direction::Outgoing)
                .filter_map(|nx| self.map.get_by_left(&nx).cloned())
                .collect(),
            None => vec![],
        }
    }

    fn delivery_order(&self, event_id: &EventId) -> usize {
        let node_idx = self.map.get_by_right(event_id).unwrap();
        node_idx.index()
    }
}

impl<O> Default for EventGraph<O> {
    fn default() -> Self {
        Self {
            graph: StableDiGraph::new(),
            _heads: HashSet::new(),
            map: BiMap::new(),
        }
    }
}

impl<O> EventGraph<O> {
    /// Collect all the node indices that correspond to an event lower or equal to the given version.
    fn collect(&self, version: &Version) -> Vec<NodeIndex> {
        let start_nodes: Vec<NodeIndex> = version
            .iter()
            .map(|id| *self.map.get_by_right(&id).unwrap())
            .collect();

        let mut collected = Vec::new();
        let discovered = self.graph.visit_map();
        let mut dfs = Dfs::from_parts(start_nodes, discovered);

        while let Some(nx) = dfs.next(&self.graph) {
            collected.push(nx);
        }

        collected
    }
}
