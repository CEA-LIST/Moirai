// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
use log::{debug, error};
use ordermap::OrderSet;
use petgraph::{
    graph::NodeIndex,
    prelude::StableDiGraph,
    visit::{Dfs, EdgeRef},
    Direction,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    rc::Rc,
};
#[cfg(feature = "serde")]
use tsify::Tsify;

use super::{
    dot_index_map::DotIndexMap, event::Event, log::Log, membership::ViewData, pulling::Since,
    pure_crdt::PureCRDT,
};
use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
    },
    protocol::stable::Stable,
};

#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
// #[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct EventGraph<Op>
where
    Op: PureCRDT,
{
    pub stable: Op::Stable,
    pub unstable: StableDiGraph<Op, ()>,
    pub dot_index_map: DotIndexMap,
    pub non_tombstones: OrderSet<NodeIndex>,
    // Contains the heads of the graph
    // The heads are the nodes that have no incoming edges
    // It includes the nodes that are not in the graph anymore (that are in the stable part)
    pub heads: HashSet<Dot>,
}

impl<Op> EventGraph<Op>
where
    Op: Clone + PureCRDT,
{
    pub fn new() -> Self {
        Self {
            stable: Default::default(),
            unstable: StableDiGraph::new(),
            dot_index_map: DotIndexMap::new(),
            non_tombstones: OrderSet::new(),
            heads: HashSet::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event<Op>) {
        let dot = Dot::from(event.metadata());
        self.heads.insert(dot.clone());
        if self.dot_index_map.contains_left(&dot) {
            error!(
                "Event with metadata {} already present in the graph",
                event.metadata()
            );
            panic!();
        }
        let idx = self.unstable.add_node(event.op.clone());

        #[cfg(debug_assertions)]
        for dot in self.heads.iter() {
            let node_idx = self.dot_index_map.get_by_left(dot);
            if let Some(node_idx) = node_idx {
                assert_eq!(
                    self.unstable
                        .neighbors_directed(*node_idx, Direction::Incoming)
                        .count(),
                    0
                );
            }
        }

        self.dot_index_map.insert(Dot::from(event.metadata()), idx);
        for (origin, cnt) in event.metadata().clock.iter() {
            if *cnt == 0 {
                continue;
            }
            let to_dot = if origin == &event.metadata().origin.expect("Origin not set") {
                Dot::new(*origin, *cnt - 1, &event.metadata().view)
            } else {
                Dot::new(*origin, *cnt, &event.metadata().view)
            };
            let to_idx = self.dot_index_map.get_by_left(&to_dot);
            // `to_idx` may be None because the dot has been moved to the stable part.
            if let Some(to_idx) = to_idx {
                self.unstable.add_edge(idx, *to_idx, ());
                let to_dot = self.dot_index_map.get_by_right(to_idx).unwrap();
                if self.heads.contains(to_dot) {
                    self.heads.remove(to_dot);
                }
            }
        }
        self.non_tombstones.insert(idx);
        assert_eq!(self.dot_index_map.len(), self.unstable.node_count());
    }

    /// Used in `stabilize` to remove the dot from the graph
    pub fn remove_dot(&mut self, dot: &Dot) -> Option<Op> {
        let node_idx = self
            .dot_index_map
            .get_by_left(dot)
            .expect("Dot not found in the graph.");

        let op = self.unstable.remove_node(*node_idx);
        self.non_tombstones.remove(node_idx);
        self.dot_index_map.remove_by_left(dot);
        op
    }

    pub fn get_op(&self, dot: &Dot) -> Option<Op> {
        let node_idx = self.dot_index_map.get_by_left(dot)?;
        self.unstable.node_weight(*node_idx).cloned()
    }

    pub fn causal_predecessors(&self, dot: &Dot) -> HashSet<NodeIndex> {
        if dot.val() == 0 {
            // If the dot is a zero value, it has no predecessors
            return HashSet::new();
        }

        let node_idx = self
            .dot_index_map
            .get_by_left(dot)
            .unwrap_or_else(|| panic!("Dot {} not found in the graph.", dot));

        let mut predecessors = HashSet::new();
        let mut dfs = Dfs::new(&self.unstable, *node_idx);

        while let Some(node) = dfs.next(&self.unstable) {
            predecessors.insert(node);
        }

        predecessors
    }

    /// Reconstruct the event from the node index
    /// Reconstruct the dependency clock from the event graph
    fn event_from_idx(&self, node_idx: &NodeIndex) -> Event<Op> {
        let dot = self.dot_index_map.get_by_right(node_idx).unwrap();
        let mut dependency_clock = Clock::<Partial>::new(&dot.view(), dot.origin());
        dependency_clock.set(dot.origin(), dot.val());

        let op = self.unstable.node_weight(*node_idx).unwrap();
        let neighbors = self
            .unstable
            .neighbors_directed(*node_idx, Direction::Outgoing);
        for neighbor in neighbors {
            let neighbor_dot = self.dot_index_map.get_by_right(&neighbor).unwrap();
            if neighbor_dot.origin() == dot.origin() {
                continue;
            }
            dependency_clock.set(neighbor_dot.origin(), neighbor_dot.val());
        }
        Event::new(op.clone(), dependency_clock)
    }
}

impl<O> Default for EventGraph<O>
where
    O: PureCRDT,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<O> Log for EventGraph<O>
where
    O: PureCRDT,
{
    type Op = O;
    type Value = O::Value;

    fn new() -> Self {
        Self::new()
    }

    fn new_event(&mut self, event: &Event<Self::Op>) {
        self.add_event(event);
    }

    /// `is_r` is true if the operation is already redundant (will never be stored in the event graph)
    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r: bool) {
        // Keep only the operations that are not made redundant by the new operation
        if is_r {
            match O::R_ZERO {
                Some(true) => {
                    self.stable.clear();
                    self.unstable.clear();
                    self.dot_index_map.clear();
                }
                Some(false) => {}
                None => {
                    // If the operation is redundant, we make its effect on the stable part
                    // and prune the unstable part
                    self.stable
                        .apply_redundant(Self::Op::redundant_by_when_redundant, &event.op);
                    prune_unstable(self, event, true);
                }
            }
        } else {
            match O::R_ONE {
                Some(true) => {
                    self.stable.clear();
                    self.unstable.clear();
                    self.dot_index_map.clear();
                }
                Some(false) => {}
                None => {
                    // If the operation is not redundant, we make its effect on the unstable part
                    // but not on the stable part: what it makes redundant will be 'shadowed' by it anyway
                    // So we don't need to apply it to the stable part. Future refactore include making the
                    // effect on the stable part more generic
                    self.stable
                        .apply_redundant(Self::Op::redundant_by_when_not_redundant, &event.op);
                    prune_unstable(self, event, false);
                }
            }
        }

        fn prune_unstable<O: PureCRDT>(graph: &mut EventGraph<O>, event: &Event<O>, is_r: bool) {
            let new_dot = Dot::from(event.metadata());
            let predecessors = graph.causal_predecessors(&new_dot);

            for node_idx in graph.unstable.node_indices() {
                let other_dot = graph.dot_index_map.get_by_right(&node_idx).unwrap();
                if *other_dot == new_dot {
                    // We don't want to compare the event with itself
                    continue;
                }

                let op = graph.unstable.node_weight(node_idx).unwrap();
                let is_conc = !predecessors.contains(&node_idx);

                if is_r {
                    if O::redundant_by_when_redundant(op, is_conc, &event.op) {
                        graph.non_tombstones.remove(&node_idx);
                    }
                } else if O::redundant_by_when_not_redundant(op, is_conc, &event.op) {
                    graph.non_tombstones.remove(&node_idx);
                }
            }

            if is_r {
                graph
                    .non_tombstones
                    .remove(graph.dot_index_map.get_by_left(&new_dot).unwrap());
            }
        }
    }

    /// Returns a list of events that are in the past of the given metadata
    fn collect_events(
        &self,
        upper_bound: &Clock<Full>,
        lower_bound: &Clock<Full>,
    ) -> Vec<Event<Self::Op>> {
        let start_nodes: Vec<NodeIndex> = upper_bound
            .clock
            .iter()
            .filter_map(|(origin, cnt)| {
                if *cnt == 0 {
                    return None;
                }
                let dot = Dot::new(*origin, *cnt, &upper_bound.view);
                self.dot_index_map.get_by_left(&dot).cloned()
            })
            .collect();

        let end_nodes: HashSet<NodeIndex> = lower_bound
            .clock
            .iter()
            .filter_map(|(origin, cnt)| {
                if *cnt == 0 {
                    return None;
                }
                let dot = Dot::new(*origin, *cnt, &lower_bound.view);
                self.dot_index_map.get_by_left(&dot).cloned()
            })
            .collect();

        let mut events = Vec::new();
        let mut visited = HashSet::new();

        for start in start_nodes {
            let mut stack = Vec::new();
            stack.push(start);
            while let Some(node_idx) = stack.pop() {
                if visited.insert(node_idx) {
                    if end_nodes.contains(&node_idx) {
                        continue;
                    }
                    let event = self.event_from_idx(&node_idx);
                    events.push(event);
                    for edge in self.unstable.edges(node_idx) {
                        let target = edge.target();
                        stack.push(target);
                    }
                }
            }
        }

        events.dedup_by(|a, b| a.metadata == b.metadata);
        events
    }

    /// Collect events since the given metadata.
    /// Exclude the events that are in the `since.exclude` list.
    /// Technically, this does the inverse of `collect_events`.
    /// `collect_events` returns the events that are in the past of the given metadata
    /// and `collect_events_since` returns the events that are in the future/concurrent of the given metadata.
    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
        let idxs: Vec<NodeIndex> = self
            .unstable
            .node_indices()
            .filter(|&node| {
                self.unstable
                    .neighbors_directed(node, Direction::Incoming)
                    .next()
                    .is_none()
            })
            .collect();

        let dots = idxs
            .iter()
            .filter_map(|&node| self.dot_index_map.get_by_right(&node))
            .cloned()
            .collect::<Vec<_>>();
        let mut upper_bound = Clock::<Full>::new(&Rc::clone(&since.clock.view), None);
        for dot in dots {
            upper_bound.set(dot.origin(), dot.val());
        }

        let mut events = self.collect_events(&upper_bound, &since.clock);
        events.retain(|event| {
            !since.exclude.contains(&Dot::from(event.metadata()))
                && event.origin() != since.clock.origin()
        });

        events
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        Self::Op::redundant_itself(&event.op)
    }

    fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
        self.stable.clear();
        if conservative {
            // reverse DFS from the roots to the metadata
            let roots = self.unstable.externals(Direction::Outgoing);
            let mut completed = HashSet::new();
            let mut visited = HashSet::new();

            for n in roots {
                let mut stack = Vec::new();
                stack.push(n);
                while let Some(node_idx) = stack.pop() {
                    if visited.insert(node_idx) {
                        let dot = self.dot_index_map.get_by_right(&node_idx).unwrap();
                        if dot.val() <= metadata.get(dot.origin()).unwrap() {
                            // If conservative, we remove the event if it is less than or equal to the metadata
                            self.non_tombstones.remove(&node_idx);
                        } else {
                            completed.insert(dot.origin());
                            if completed.len() == metadata.clock.len() {
                                // If we have visited all the origins in the metadata, we can stop
                                break;
                            }
                        }

                        for edge in self.unstable.edges(node_idx) {
                            let target = edge.source();
                            stack.push(target);
                        }
                    }
                }
            }
        } else {
            self.non_tombstones.clear();
        }
    }

    fn eval(&self) -> Self::Value {
        let unstable: Vec<Self::Op> = self
            .non_tombstones
            .iter()
            .map(|&node_idx| self.unstable.node_weight(node_idx).unwrap().clone())
            .collect();
        O::eval(&self.stable, &unstable)
    }

    fn stabilize(&mut self, metadata: &Clock<Partial>) {
        O::stabilize(metadata, self);
    }

    /// Move the op to the stable part of the graph
    fn purge_stable_metadata(&mut self, metadata: &Clock<Partial>) {
        // The dot may have been removed in the `stabilize` function
        let dot = Dot::from(metadata);
        let node_idx = self.dot_index_map.get_by_left(&dot);

        if let Some(node_idx) = node_idx {
            // If the remove was successful, then the op was not a tombstone
            let was_not_tombstone = self.non_tombstones.remove(node_idx);
            let op = self.unstable.remove_node(*node_idx);
            self.dot_index_map.remove_by_left(&dot);
            if let Some(op) = op {
                if was_not_tombstone {
                    self.stable.apply(op);
                }
            }
        } else {
            debug!("Dot {} not found in the graph", dot);
        }
    }

    fn is_empty(&self) -> bool {
        self.stable.is_default()
            && (self.unstable.node_count() == 0 || self.non_tombstones.is_empty())
    }

    // TODO: remove this method (not needed)
    fn size(&self) -> usize {
        self.unstable.node_count()
    }

    fn deps(
        &self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        _op: &Self::Op,
    ) {
        let mut new_clock = Clock::<Partial>::new(&Rc::clone(view), dot.origin());
        for dot in self.heads.iter() {
            if dot.view().id != view.id {
                // If the dot is not from the current view, skip it
                // After a view change, the heads may contain dots from the previous view
                // TODO: clean the heads after a view change
                continue;
            }
            new_clock.set(dot.origin(), dot.val());
        }
        new_clock.set(dot.origin(), dot.val());
        clocks.push_back(new_clock);
    }
}
