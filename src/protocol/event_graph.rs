use std::{cmp::Ordering, collections::HashSet, fmt::Debug};

use bimap::BiMap;
use log::{debug, error};
use petgraph::{
    algo::has_path_connecting, graph::NodeIndex, prelude::StableDiGraph, visit::EdgeRef, Direction,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::{event::Event, log::Log, pulling::Since, pure_crdt::PureCRDT};
use crate::clocks::{clock::Clock, dependency_clock::DependencyClock, dot::Dot};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct EventGraph<Op> {
    pub stable: Vec<Op>,
    pub unstable: StableDiGraph<Op, ()>,
    pub(crate) index_map: BiMap<Dot, NodeIndex>,
}

impl<Op> EventGraph<Op>
where
    Op: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            stable: Vec::new(),
            unstable: StableDiGraph::new(),
            index_map: BiMap::new(),
        }
    }

    pub fn new_event(&mut self, event: &Event<Op>) {
        let dot = Dot::from(&event.metadata);
        if self.index_map.contains_left(&dot) {
            error!(
                "Event with metadata {} already present in the graph",
                event.metadata
            );
            panic!();
        }
        let from_idx = self.unstable.add_node(event.op.clone());
        self.index_map.insert(Dot::from(&event.metadata), from_idx);
        for (origin, cnt) in event.metadata.clock.iter() {
            if *cnt == 0 {
                continue;
            }
            let to_dot = if origin == &event.metadata.origin.expect("Origin not set") {
                Dot::new(*origin, *cnt - 1, &event.metadata.view)
            } else {
                Dot::new(*origin, *cnt, &event.metadata.view)
            };
            let to_idx = self.index_map.get_by_left(&to_dot);
            // `to_idx` may be None because the dot has been moved to the stable part.
            if let Some(to_idx) = to_idx {
                self.unstable.add_edge(from_idx, *to_idx, ());
            }
        }
        assert_eq!(self.index_map.len(), self.unstable.node_count());
    }

    pub fn remove_dot(&mut self, dot: &Dot) -> Option<Op> {
        let node_idx = self
            .index_map
            .get_by_left(dot)
            .expect("Dot not found in the graph.");
        let op = self.unstable.remove_node(*node_idx);
        self.index_map.remove_by_left(dot);
        op
    }

    pub fn get_op(&self, dot: &Dot) -> Option<Op> {
        let node_idx = self.index_map.get_by_left(dot)?;
        self.unstable.node_weight(*node_idx).cloned()
    }

    pub fn partial_cmp(&self, first: &Dot, second: &Dot) -> Option<Ordering> {
        let first_idx = self
            .index_map
            .get_by_left(first)
            .expect("Dot not found in the graph.");
        let second_idx = self
            .index_map
            .get_by_left(second)
            .unwrap_or_else(|| panic!("Dot {} not found in the graph.", second));

        let first_to_second = has_path_connecting(&self.unstable, *second_idx, *first_idx, None);
        let second_to_first = has_path_connecting(&self.unstable, *first_idx, *second_idx, None);

        if !first_to_second && !second_to_first && first.origin() == second.origin() {
            panic!(
                "Both dots are not connected but have the same origin: {} and {}. Graph: {:?}",
                first,
                second,
                petgraph::dot::Dot::with_config(
                    &self.unstable,
                    &[petgraph::dot::Config::EdgeNoLabel]
                )
            );
        }

        match (first_to_second, second_to_first) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => None,
        }
    }

    /// Reconstruct the event from the node index
    /// Reconstruct the dependency clock from the event graph
    fn event_from_idx(&self, node_idx: &NodeIndex) -> Event<Op> {
        let dot = self.index_map.get_by_right(node_idx).unwrap();
        let mut dependency_clock = DependencyClock::new(&dot.view(), dot.origin());
        dependency_clock.set(dot.origin(), dot.val());

        let op = self.unstable.node_weight(*node_idx).unwrap();
        let neighbors = self
            .unstable
            .neighbors_directed(*node_idx, Direction::Outgoing);
        for neighbor in neighbors {
            let neighbor_dot = self.index_map.get_by_right(&neighbor).unwrap();
            if neighbor_dot.origin() == dot.origin() {
                continue;
            }
            dependency_clock.set(neighbor_dot.origin(), neighbor_dot.val());
        }
        Event::new(op.clone(), dependency_clock)
    }
}

impl<O> Log for EventGraph<O>
where
    O: PureCRDT,
{
    type Op = O;
    type Value = O::Value;

    fn new_event(&mut self, event: &Event<Self::Op>) {
        self.new_event(event);
    }

    /// `is_r` is true if the operation is already redundant (will never be stored in the event graph)
    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r: bool) {
        // Keep only the operations that are not made redundant by the new operation
        if is_r {
            match O::R_ZERO {
                Some(true) => {
                    self.stable.clear();
                    self.unstable.clear();
                    self.index_map.clear();
                }
                Some(false) => {}
                None => {
                    self.stable
                        .retain(|o| !(Self::Op::r_zero(o, Some(Ordering::Less), &event.op)));
                    // TODO: shrink if capacity > 2*len
                    self.stable.shrink_to_fit();
                    prune_unstable(self, event, true);
                }
            }
        } else {
            match O::R_ONE {
                Some(true) => {
                    self.stable.clear();
                    self.unstable.clear();
                    self.index_map.clear();
                }
                Some(false) => {}
                None => {
                    self.stable
                        .retain(|o| !(Self::Op::r_one(o, Some(Ordering::Less), &event.op)));
                    self.stable.shrink_to_fit();
                    prune_unstable(self, event, false);
                }
            }
        }

        fn prune_unstable<O: PureCRDT>(graph: &mut EventGraph<O>, event: &Event<O>, is_r: bool) {
            graph.new_event(event);
            let new_dot = Dot::from(&event.metadata);

            let to_remove: Vec<NodeIndex> = graph
                .unstable
                .node_indices()
                .filter(|&node_idx| {
                    let other_dot = graph.index_map.get_by_right(&node_idx).unwrap();
                    if *other_dot == new_dot {
                        return true;
                    }
                    let op = graph.unstable.node_weight(node_idx).unwrap();
                    let ordering = graph.partial_cmp(other_dot, &new_dot);
                    if is_r {
                        O::r_zero(op, ordering, &event.op)
                    } else {
                        O::r_one(op, ordering, &event.op)
                    }
                })
                .collect();

            for node_idx in to_remove {
                graph.unstable.remove_node(node_idx);
                graph.index_map.remove_by_right(&node_idx);
            }
        }
    }

    /// Returns a list of events that are in the past of the given metadata
    fn collect_events(&self, upper_bound: &DependencyClock) -> Vec<Event<Self::Op>> {
        let start_nodes = upper_bound.clock.iter().filter_map(|(origin, cnt)| {
            let dot = Dot::new(*origin, *cnt, &upper_bound.view);
            self.index_map.get_by_left(&dot).cloned()
        });

        let mut events = Vec::new();
        let mut visited = HashSet::new();

        // TODO: stack = start_nodes. Attention visited
        for start in start_nodes {
            let mut stack = Vec::new();
            stack.push(start);
            while let Some(node_idx) = stack.pop() {
                if visited.insert(node_idx) {
                    let event = self.event_from_idx(&node_idx);
                    events.push(event);
                    for edge in self.unstable.edges(node_idx) {
                        let target = edge.target();
                        stack.push(target);
                    }
                }
            }
        }

        // remove duplicate events
        events.dedup_by(|a, b| a.metadata == b.metadata);
        events
    }

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
        let mut events = self.collect_events(&since.clock);
        events.retain(|event| since.exclude.contains(&Dot::from(&event.metadata)));

        events
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        Self::Op::r(&event.op)
    }

    fn r_n(&mut self, metadata: &DependencyClock, conservative: bool) {
        self.stable.clear();
        let to_remove = self
            .unstable
            .node_indices()
            .filter(|&node_idx| {
                let dot = self.index_map.get_by_right(&node_idx).unwrap();
                let ordering = self.partial_cmp(dot, &Dot::from(metadata));
                if conservative {
                    !matches!(ordering, Some(Ordering::Less) | Some(Ordering::Equal))
                } else {
                    !matches!(ordering, Some(Ordering::Greater))
                }
            })
            .collect::<Vec<_>>();
        for node_idx in to_remove {
            self.unstable.remove_node(node_idx);
            self.index_map.remove_by_right(&node_idx);
        }
    }

    fn eval(&self) -> Self::Value {
        let mut ops: Vec<O> = self.stable.clone();
        ops.extend(self.unstable.node_weights().cloned());
        assert_eq!(self.size(), ops.len());
        O::eval(&ops)
    }

    fn stabilize(&mut self, metadata: &DependencyClock) {
        O::stabilize(metadata, self);
    }

    fn purge_stable_metadata(&mut self, metadata: &DependencyClock) {
        // The dot may have been removed in the `stabilize` function
        let dot = Dot::from(metadata);
        let node_idx = self.index_map.get_by_left(&dot);

        if let Some(node_idx) = node_idx {
            let op = self.unstable.remove_node(*node_idx);
            self.index_map.remove_by_left(&dot);
            if let Some(op) = op {
                self.stable.push(op);
            }
            debug!("Dot {} has been moved to the stable part", dot);
        } else {
            debug!("Dot {} not found in the graph", dot);
        }
    }

    fn is_empty(&self) -> bool {
        self.stable.is_empty() && self.unstable.node_count() == 0
    }

    fn size(&self) -> usize {
        self.stable.len() + self.unstable.node_count()
    }

    fn reset(&mut self) {
        let mut new_graph = Self::new();
        for node_idx in self.unstable.node_indices() {
            let event = self.event_from_idx(&node_idx);
            new_graph.new_event(&event);
        }
        self.stable
            .iter()
            .for_each(|op| new_graph.stable.push(op.clone()));
        *self = new_graph;
    }
}

impl<Op> Default for EventGraph<Op>
where
    Op: Clone + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}
