use log::{debug, error};
use petgraph::{
    graph::NodeIndex,
    prelude::StableDiGraph,
    visit::{Dfs, Reversed, Visitable},
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
        matrix_clock::MatrixClock,
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
    pub unstable: StableDiGraph<(Op, Clock<Partial>), ()>,
    pub dot_index_map: DotIndexMap,
    pub non_tombstones: HashSet<NodeIndex>,
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
            non_tombstones: HashSet::new(),
            heads: HashSet::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event<Op>) {
        let dot = Dot::from(event.metadata());

        if self.dot_index_map.contains_left(&dot) {
            error!(
                "Event with metadata {} already present in the graph",
                event.metadata()
            );
            panic!();
        }
        let idx = self.unstable.add_node((
            event.op.clone(),
            Clock::<Partial>::new(&event.metadata().view, event.metadata().origin()),
        ));

        self.dot_index_map.insert(Dot::from(event.metadata()), idx);
        for (origin, cnt) in event.metadata().clock.iter() {
            // or origin and cnt equals 1
            if *cnt == 0 {
                continue;
            }
            // TODO: store the dependencies and the dot separately
            let to_dot = if origin == &event.metadata().origin.expect("Origin not set") {
                Dot::new(*origin, *cnt - 1, &event.metadata().view)
            } else {
                Dot::new(*origin, *cnt, &event.metadata().view)
            };
            let to_idx = self.dot_index_map.get_by_left(&to_dot);
            // `to_idx` may be None because the dot has been moved to the stable part.
            if let Some(to_idx) = to_idx {
                // Direction is important here: we add an edge from the current node to the target node
                // DAG direction is from child to parent
                assert_ne!(*to_idx, idx, "Cannot add self-loop in the event graph");
                self.unstable.add_edge(idx, *to_idx, ());
                let other_dot = self.dot_index_map.get_by_right(to_idx).unwrap();
                if self.heads.contains(other_dot) {
                    self.heads.remove(other_dot);
                }
            } else {
                let node = self.unstable.node_weight_mut(idx).unwrap();
                node.1.set_by_idx(*origin, *cnt);
                // if the dot is in heads, we remove it
                let other_dot = if origin == &event.metadata().origin.expect("Origin not set") {
                    Dot::new(*origin, *cnt - 1, &event.metadata().view)
                } else {
                    Dot::new(*origin, *cnt, &event.metadata().view)
                };
                if self.heads.contains(&other_dot) {
                    self.heads.remove(&other_dot);
                }
            }
        }

        self.heads.insert(dot.clone());

        self.heads
            .retain(|h| h.origin() != dot.origin() || h.val() >= dot.val());

        debug_assert!(
            !self
                .heads
                .iter()
                .any(|h| h.origin() == dot.origin() && h.val() != dot.val()),
            "There is already a head with the same origin and different value: old value -> {}, new value -> {}",
            self.heads
                .iter()
                .find(|h| h.origin() == dot.origin() && h.val() != dot.val())
                .unwrap(),
            dot
        );

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
        op.map(|op| op.0)
    }

    pub fn get_op(&self, dot: &Dot) -> Option<Op> {
        let node_idx = self.dot_index_map.get_by_left(dot)?;
        self.unstable.node_weight(*node_idx).cloned().map(|op| op.0)
    }

    /// Complexity: O(n + m), where n is the number of nodes and m is the number of edges in the graph.
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
    /// TODO: miss dependencies that have been stabilized
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
        dependency_clock.merge(&op.1);
        Event::new(op.0.clone(), dependency_clock)
    }

    fn node_indices_from_clock(&self, clock: &Clock<Full>) -> Vec<NodeIndex> {
        clock
            .clock
            .iter()
            .filter_map(|(origin, cnt)| {
                if *cnt == 0 {
                    None
                } else {
                    let dot = Dot::new(*origin, *cnt, &clock.view);
                    self.dot_index_map.get_by_left(&dot).cloned()
                }
            })
            .collect()
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
    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r: bool, ltm: &MatrixClock) {
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
                    let clock = ltm.get_by_idx(event.metadata().origin.unwrap()).unwrap();
                    // If the operation is redundant, we make its effect on the stable part
                    // and prune the unstable part
                    self.stable
                        .apply_redundant(Self::Op::redundant_by_when_redundant, &event.op);
                    prune_unstable(self, event, true, clock);
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
                    let clock = ltm.get_by_idx(event.metadata().origin.unwrap()).unwrap();
                    // If the operation is not redundant, we make its effect on the unstable part
                    // but not on the stable part: what it makes redundant will be 'shadowed' by it anyway
                    // So we don't need to apply it to the stable part. Future refactore include making the
                    // effect on the stable part more generic
                    self.stable
                        .apply_redundant(Self::Op::redundant_by_when_not_redundant, &event.op);
                    prune_unstable(self, event, false, clock);
                }
            }
        }

        fn prune_unstable<O: PureCRDT>(
            graph: &mut EventGraph<O>,
            event: &Event<O>,
            is_r: bool,
            clock: &Clock<Full>,
        ) {
            let new_dot = Dot::from(event.metadata());
            assert_ne!(new_dot.val(), 0, "Dot value cannot be 0");

            let mut to_remove = Vec::new();
            for node_idx in graph.non_tombstones.iter() {
                let other_dot = graph.dot_index_map.get_by_right(node_idx).unwrap();
                if *other_dot == new_dot {
                    // We don't want to compare the event with itself
                    continue;
                }

                let op = graph.unstable.node_weight(*node_idx).unwrap();
                let is_conc = !clock.is_predecessor(other_dot);
                // Create a total order for the operations
                // true if old_op > new_op, false otherwise
                // if conc, we compare on the lexicographic order of process ids
                let order = if is_conc {
                    other_dot.origin() > event.metadata().origin()
                } else {
                    false
                };

                if is_r {
                    if O::redundant_by_when_redundant(&op.0, is_conc, order, &event.op) {
                        to_remove.push(*node_idx);
                    }
                } else if O::redundant_by_when_not_redundant(&op.0, is_conc, order, &event.op) {
                    to_remove.push(*node_idx);
                }
            }
            for node_idx in to_remove {
                graph.non_tombstones.remove(&node_idx);
            }

            if is_r {
                graph
                    .non_tombstones
                    .remove(graph.dot_index_map.get_by_left(&new_dot).unwrap());
            }
        }
    }

    /// Collect events since the given metadata.
    /// Exclude the events that are in the `since.exclude` list.
    /// Technically, this does the inverse of `collect_events`.
    /// `collect_events` returns the events that are in the past of the given metadata
    /// and `collect_events_since` returns the events that are in the future/concurrent of the given metadata.
    fn collect_events_since(&self, since: &Since, ltm: &MatrixClock) -> Vec<Event<Self::Op>> {
        let mut events = Vec::new();

        for (o, c) in ltm.origin_clock().iter() {
            let val = since.clock.clock.get(o).unwrap();
            if val >= c {
                // If the value in the clock is greater than or equal to the value in the since clock,
                // we skip this origin
                continue;
            }
            for i in (*val + 1)..=*c {
                let dot = Dot::new(*o, i, &since.clock.view);
                if since.exclude.contains(&dot) {
                    // If the dot is in the exclude list, we skip it
                    continue;
                }
                if let Some(node_idx) = self.dot_index_map.get_by_left(&dot) {
                    // If the node index exists in the graph, we add the event
                    let e = self.event_from_idx(node_idx);
                    events.push(e)
                }
            }
        }

        events
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        Self::Op::redundant_itself(&event.op)
    }

    fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
        self.stable.clear();
        if conservative {
            // reverse DFS from the roots to the metadata
            let start_nodes = self.unstable.externals(Direction::Outgoing).collect();

            let discovered = self.unstable.visit_map();
            let mut dfs = Dfs::from_parts(start_nodes, discovered);

            // TODO: stop the DFS when we reach the metadata
            while let Some(nx) = dfs.next(Reversed(&self.unstable)) {
                let dot = self.dot_index_map.get_by_right(&nx).unwrap();
                if dot.val() <= metadata.get(dot.origin()).unwrap() {
                    // If conservative, we remove the event if it is less than or equal to the metadata
                    self.non_tombstones.remove(&nx);
                }
            }
        } else {
            // Every ops become a tombstone
            self.non_tombstones.clear();
        }
    }

    fn eval(&self) -> Self::Value {
        let unstable: Vec<Self::Op> = self
            .non_tombstones
            .iter()
            .map(|&node_idx| self.unstable.node_weight(node_idx).unwrap().0.clone())
            .collect();
        O::eval(&self.stable, &unstable)
    }

    fn stabilize(&mut self, dot: &Dot) {
        O::stabilize(dot, self);
    }

    fn clock_from_event(&self, event: &Event<Self::Op>) -> Clock<Full> {
        let mut vector_clock = Clock::<Full>::new(&event.metadata().view, Some(event.origin()));

        for (origin, cnt) in event.metadata().clock.iter() {
            vector_clock.set_by_idx(*origin, *cnt);
        }

        if event.metadata().clock.len() == event.metadata().view.members.len() {
            // If the vector clock is complete, we return it
            return vector_clock;
        }

        let start_nodes: Vec<NodeIndex> = event
            .metadata()
            .iter()
            .filter_map(|(origin, cnt)| {
                if origin == &event.metadata().origin.unwrap() {
                    // If the origin is the same as the event origin, we skip it
                    None
                } else {
                    let dot = Dot::new(*origin, *cnt, &event.metadata().view);
                    self.dot_index_map.get_by_left(&dot).cloned()
                }
            })
            .collect();

        let discovered = self.unstable.visit_map();
        let mut dfs = Dfs::from_parts(start_nodes, discovered);

        while let Some(nx) = dfs.next(&self.unstable) {
            let dot = self.dot_index_map.get_by_right(&nx).unwrap();
            if dot.val() > vector_clock.get(dot.origin()).unwrap() {
                vector_clock.set(dot.origin(), dot.val());
            }
        }
        vector_clock
    }

    /// Move the op to the stable part of the graph
    fn purge_stable_metadata(&mut self, dot: &Dot) {
        // The dot may have been removed in the `stabilize` function
        let node_idx = self.dot_index_map.get_by_left(dot);

        if let Some(node_idx) = node_idx {
            // If the remove was successful, then the op was not a tombstone
            let was_not_tombstone = self.non_tombstones.remove(node_idx);
            let op = self.unstable.remove_node(*node_idx);
            self.dot_index_map.remove_by_left(dot);
            if let Some(op) = op {
                if was_not_tombstone {
                    self.stable.apply(op.0);
                }
            }
        } else {
            debug!("Dot {} not found in the graph", dot);
        }
    }

    fn stable_by_clock(&mut self, clock: &Clock<Full>) {
        let start_nodes = self.node_indices_from_clock(clock);

        let discovered = self.unstable.visit_map();
        let mut dfs = Dfs::from_parts(start_nodes, discovered);

        let mut stable_nx = HashSet::new();

        while let Some(nx) = dfs.next(&self.unstable) {
            stable_nx.insert(nx);
        }

        for nx in &stable_nx {
            let dot = self.dot_index_map.get_by_right(nx).unwrap().clone();
            // We cannot stabilize a node if there is an unstable node that has an incoming edge to it
            let can_be_stabilized = self
                .unstable
                .neighbors_directed(*nx, Direction::Incoming)
                .all(|n| stable_nx.contains(&n));
            if can_be_stabilized {
                self.stable(&dot);
            } else {
                debug!(
                    "Dot {} cannot be stabilized because it has incoming edges from unstable nodes",
                    dot
                );
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.stable.is_default()
            && (self.unstable.node_count() == 0 || self.non_tombstones.is_empty())
    }

    fn deps(
        &mut self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        _op: &Self::Op,
    ) {
        // TODO: this is more than the transitive reduction because it includes also the dot of the current event
        // which is not a dependency
        let mut new_clock = Clock::<Partial>::new(&Rc::clone(view), dot.origin());
        let mut to_remove: Vec<Dot> = Vec::new();
        for dot in self.heads.iter() {
            if dot.view().id != view.id {
                // If the dot is not from the current view, skip it
                // After a view change, the heads may contain dots from the previous view
                to_remove.push(dot.clone());
                continue;
            }
            new_clock.set(dot.origin(), dot.val());
        }
        for dot in to_remove {
            self.heads.remove(&dot);
        }
        new_clock.set(dot.origin(), dot.val());
        clocks.push_back(new_clock);
    }
}
