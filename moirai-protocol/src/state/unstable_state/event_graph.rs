#![allow(clippy::mutable_key_type)]

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::{Debug, Display},
    ops::Bound::{Excluded, Unbounded},
    range::Range,
};

use bimap::BiMap;
#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use petgraph::{Direction, graph::NodeIndex, prelude::StableDiGraph};
use rustc_hash::FxHashSet;

use crate::{
    clock::{
        causal_frontier::CausalFrontier,
        version_vector::{Seq, Version},
    },
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::{
        IsUnstableCausal, IsUnstableCore, IsUnstableDelivery, IsUnstablePrune,
    },
};

#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, Clone)]
pub struct EventGraph<O> {
    /// Directed Acyclic Graph (DAG) of update events.
    /// - Vertices are update operations tagged with their event id.
    /// - Arcs are direct causal predecessor relationships between events.
    ///   Edges point from a child to its direct causal parents.
    graph: StableDiGraph<TaggedOp<O>, ()>,
    /// Bidirectional map from graph's node index to event id
    map: BiMap<NodeIndex, EventId>,
    /// Maximal events in the graph.
    /// Set of event ids that are heads of the graph (no children).
    heads: FxHashSet<EventId>,
    /// Latest critical frontier flushed into the stable state.
    roots: Option<CausalFrontier>,
    /// Contains EventIds retained for parent discovery, sorted by process and sequence number.
    cutter: Cutter,
    /// Lengths of critical prefixes in dense topological replay order.
    /// Each append pushes one cut and permanently pops the invalidated cuts.
    critical_versions: Vec<usize>,
}

impl<O> IsUnstableCore<O> for EventGraph<O>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
        let first_missing = self
            .cutter
            .first_after(|origin| event.version().seq_by_idx(origin))
            .unwrap_or(self.graph.node_count());
        let new_tagged_op = TaggedOp::from(&event);
        // Find the immediate predecessors
        let immediate_parents = self.find_immediate_predecessors(event.version());

        // Add the event to the graph
        let child_idx = self.graph.add_node(new_tagged_op);
        // Add to the cutter
        self.cutter.insert(event.id(), child_idx);

        // Record the node index of the event in the map
        self.map.insert(child_idx, event.id().clone());

        // For each predecessor found, add an edge from the new event to it
        for parent_idx in immediate_parents {
            self.graph.add_edge(child_idx, parent_idx, ());
            let parent_id = self.map.get_by_left(&parent_idx).unwrap();
            // If the parent was a head, it is not anymore since it has a child now
            if self.heads.contains(parent_id) {
                self.heads.remove(parent_id);
            }
        }
        // A new event is always a head of the graph since it has no children (yet)
        self.heads.insert(event.id().clone());
        // A new event invalidates precisely the cuts beyond its first unseen event.
        while self
            .critical_versions
            .last()
            .is_some_and(|&cut| cut > first_missing)
        {
            self.critical_versions.pop();
        }
        self.critical_versions.push(self.graph.node_count());

        //* DEBUGGING */
        self.is_graph_correct();
    }

    /// # Complexity
    /// `O(1)`
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.map
            .get_by_right(event_id)
            .and_then(|idx| self.graph.node_weight(*idx))
    }

    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>>
    where
        O: Clone,
    {
        self.iter()
            .filter(|event| event.id().is_predecessor_of(version))
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.graph.node_weights()
    }

    fn replica_events<'a>(
        &'a self,
        replica_idx: ReplicaIdx,
        range: Range<Seq>,
    ) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.cutter
            .0
            .get(&replica_idx)
            .into_iter()
            .flat_map(move |seq_map| {
                seq_map
                    .range(range)
                    .flat_map(move |(&seq, &(first, last))| {
                        (first.index()..=last.index())
                            .filter_map(|index| self.graph.node_weight(NodeIndex::new(index)))
                            .filter(move |event| {
                                event.id().idx() == replica_idx && event.id().seq() == seq
                            })
                    })
            })
    }

    fn len(&self) -> usize {
        self.graph.node_count()
    }

    fn is_empty(&self) -> bool {
        self.graph.node_count() == 0
    }
}

impl<O> IsUnstableCausal<O> for EventGraph<O>
where
    O: Debug + Clone,
{
    fn roots(&self) -> Option<&CausalFrontier> {
        self.roots.as_ref()
    }

    fn greatest_critical_frontier(&self, upper_bound: &CausalFrontier) -> Option<CausalFrontier> {
        if let Some(base) = &self.roots
            && !matches!(
                upper_bound.partial_cmp(base),
                Some(Ordering::Equal | Ordering::Greater)
            )
        {
            return None;
        }

        // Every event before this position belongs to upper_bound.
        let limit = self
            .cutter
            .first_after(|origin| upper_bound.seq_by_idx(origin))
            .unwrap_or(self.graph.node_count());

        // Select the largest critical prefix entirely inside that range.
        let end = self.critical_versions.partition_point(|&len| len <= limit);
        let &len = self.critical_versions.get(end.checked_sub(1)?)?;

        Some(self.frontier_for_prefix(len, upper_bound))
    }

    fn is_critical(&self, frontier: &CausalFrontier) -> bool {
        use std::cmp::Ordering::Equal;

        // The existing checkpoint, or the initial empty cut, is already critical.
        let at_base = match &self.roots {
            Some(base) => base.partial_cmp(frontier) == Some(Equal),
            None => frontier.iter().all(|(_, seq)| seq == 0),
        };

        at_base
            || self
                .greatest_critical_frontier(frontier)
                .is_some_and(|cut| cut.partial_cmp(frontier) == Some(Equal))
    }

    fn direct_predecessors(&self, event_id: &EventId) -> Vec<EventId> {
        let maybe_idx = self.map.get_by_right(event_id);
        if let Some(idx) = maybe_idx {
            self.graph
                .neighbors_directed(*idx, Direction::Outgoing)
                .filter_map(|nx| self.map.get_by_left(&nx).cloned())
                .collect()
        } else {
            vec![]
        }
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        self.heads
            .iter()
            .filter_map(|id| self.get(id).cloned())
            .collect()
    }

    fn previous(&self, _event_id: &EventId, _r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        todo!()
    }

    fn next(&self, _event_id: &EventId, _r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        todo!()
    }

    fn newly_observed_by(&self, _observer: &EventId) -> Vec<&TaggedOp<O>> {
        todo!()
    }

    fn version(&self, _event_id: &EventId) -> Option<&Version> {
        todo!()
    }
}

impl<O> IsUnstableDelivery<O> for EventGraph<O>
where
    O: Debug + Clone,
{
    fn delivery_order(&self, event_id: &EventId) -> Option<usize> {
        self.map.get_by_right(event_id).map(|idx| idx.index())
    }
}

impl<O> IsUnstablePrune<O> for EventGraph<O>
where
    O: Debug + Clone,
{
    /// Flush a critical prefix after computing its new stable state.
    fn prune_predecessors(&mut self, frontier: &CausalFrontier) {
        assert!(
            self.is_critical(frontier),
            "Only a known critical prefix can be flushed"
        );
        self.retain(|event| !frontier.contains(event.id()));
        self.roots = Some(frontier.clone());
    }

    fn remove(&mut self, event_id: &EventId) {
        if self.map.contains_right(event_id) {
            self.retain(|event| event.id() != event_id);
        }
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        // Evaluate each event once before changing the graph, so a panic leaves it intact.
        let keep: Vec<_> = self.graph.node_weights().map(predicate).collect();
        if keep.iter().all(|&keep| keep) {
            return;
        }
        // Removing a critical prefix preserves the remaining cuts. Other removals
        // may create new critical cuts, which must be recomputed.
        let removed_prefix = keep.iter().take_while(|&&keep| !keep).count();
        let prefix_only = keep[removed_prefix..].iter().all(|&keep| keep);
        let shift_cuts = prefix_only
            && self
                .critical_versions
                .binary_search(&removed_prefix)
                .is_ok();
        // Structural removal keeps the existing stable frontier.
        let mut retained = Self {
            roots: self.roots.clone(),
            ..Self::default()
        };
        // Translate old indexes to dense new indexes; removed vertices stay None.
        // Replay order guarantees that retained parents are rebuilt before children.
        let mut mapping = vec![None; keep.len()];
        for old in self.graph.node_indices().filter(|old| keep[old.index()]) {
            let parents: Vec<_> = if prefix_only {
                // A removed prefix cannot lie between two retained vertices.
                self.graph
                    .neighbors_directed(old, Direction::Outgoing)
                    .filter_map(|parent| mapping[parent.index()])
                    .collect()
            } else {
                // Follow deleted parents until reaching the nearest retained ancestors.
                let mut stack: Vec<_> = self
                    .graph
                    .neighbors_directed(old, Direction::Outgoing)
                    .collect();
                let mut visited = FxHashSet::default();
                let mut parents = FxHashSet::default();
                while let Some(parent) = stack.pop() {
                    if !visited.insert(parent) {
                        continue;
                    }
                    if let Some(mapped) = mapping[parent.index()] {
                        parents.insert(mapped);
                    } else {
                        // Bypass removed vertices to preserve causal reachability.
                        stack.extend(self.graph.neighbors_directed(parent, Direction::Outgoing));
                    }
                }
                parents.into_iter().collect()
            };
            let parents = if shift_cuts {
                parents
            } else {
                // Remove redundant parent edges and collect the vertex's full causal past.
                let (parents, mut ancestors) = retained.maximal_nodes(parents);
                ancestors.extend(parents.iter().copied());
                let first_missing = (0..retained.graph.node_count())
                    .find(|&index| !ancestors.contains(&NodeIndex::new(index)))
                    .unwrap_or(retained.graph.node_count());
                // Any cut containing an unobserved event is no longer critical.
                while retained
                    .critical_versions
                    .last()
                    .is_some_and(|&cut| cut > first_missing)
                {
                    retained.critical_versions.pop();
                }
                // The entire rebuilt prefix is critical until a later vertex invalidates it.
                retained
                    .critical_versions
                    .push(retained.graph.node_count() + 1);
                parents
            };
            let event = self.graph[old].clone();
            let id = event.id().clone();
            let child = retained.graph.add_node(event);
            // The new vertex becomes a head; its parents cease to be heads.
            for parent in parents {
                retained.graph.add_edge(child, parent, ());
                retained
                    .heads
                    .remove(retained.map.get_by_left(&parent).unwrap());
            }
            // Rebuild ID and per-origin sequence lookups with the new indexes.
            retained.map.insert(child, id.clone());
            retained.cutter.insert(&id, child);
            retained.heads.insert(id);
            mapping[old.index()] = Some(child);
        }
        if shift_cuts {
            // Drop removed cuts and subtract the removed prefix from surviving lengths.
            retained.critical_versions = self
                .critical_versions
                .iter()
                .filter(|&&cut| cut > removed_prefix)
                .map(|cut| cut - removed_prefix)
                .collect();
        }
        *self = retained;
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl<O> Default for EventGraph<O> {
    fn default() -> Self {
        Self {
            graph: StableDiGraph::new(),
            map: BiMap::new(),
            heads: FxHashSet::default(),
            roots: None,
            cutter: Cutter::new(),
            critical_versions: Vec::new(),
        }
    }
}

impl<O> EventGraph<O> {
    pub fn from_checkpoint(frontier: CausalFrontier) -> Self {
        Self {
            roots: Some(frontier),
            ..Self::default()
        }
    }
}

impl<O> EventGraph<O>
where
    O: Debug + Clone,
{
    /// Find the immediate predecessors of the given version in the DAG.
    /// An immediate predecessor is a node that is a predecessor of the given version,
    /// and has no other predecessors that are also predecessors of the given version.
    /// This is used to find the minimal set of events to attach a new event to.
    ///
    /// # Complexity
    /// O(v + e)
    ///
    /// # Algorithm
    /// 1. Start from the highest retained event per replica that is lower or equal to the version.
    /// 2. Keep a frontier of maximal starts.
    /// 3. Walk parent edges once per uncovered slice. Every reached ancestor is dominated by the
    ///    current start, any previous frontier node reached by this walk is removed.
    fn find_immediate_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        self.maximal_nodes(self.cutter.below(version)).0
    }

    fn maximal_nodes(
        &self,
        starts: impl IntoIterator<Item = NodeIndex>,
    ) -> (Vec<NodeIndex>, FxHashSet<NodeIndex>) {
        let mut frontier = FxHashSet::default();
        let mut dominated = FxHashSet::default();

        for start_idx in starts {
            if dominated.contains(&start_idx) || frontier.contains(&start_idx) {
                continue;
            }

            frontier.insert(start_idx);

            let mut stack: Vec<NodeIndex> = self
                .graph
                .neighbors_directed(start_idx, Direction::Outgoing)
                .collect();

            while let Some(node_idx) = stack.pop() {
                let was_frontier = frontier.remove(&node_idx);
                let newly_dominated = dominated.insert(node_idx);
                if !newly_dominated && !was_frontier {
                    continue;
                }

                for parent_idx in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                    stack.push(parent_idx);
                }
            }
        }

        let mut parents: Vec<NodeIndex> = frontier.into_iter().collect();
        parents.sort_by_key(|node_idx| node_idx.index());
        (parents, dominated)
    }

    pub fn graph(&self) -> &StableDiGraph<TaggedOp<O>, ()> {
        &self.graph
    }

    pub fn heads(&self) -> &FxHashSet<EventId> {
        &self.heads
    }

    fn is_graph_correct(&self) {
        debug_assert!(self.heads.iter().all(|h| {
            let nx = self.map.get_by_right(h).unwrap();
            let neighbors_count = self
                .graph
                .neighbors_directed(*nx, Direction::Incoming)
                .count();
            neighbors_count == 0
        }));
        debug_assert!(self.max_one_head_per_id());
        debug_assert!(self.graph.node_count() >= self.heads.len());
    }

    fn max_one_head_per_id(&self) -> bool {
        let mut seen = FxHashSet::default();
        for p in &self.heads {
            if !seen.insert(p.origin_id()) {
                return false; // duplicate name found
            }
        }
        true
    }

    fn frontier_for_prefix(&self, len: usize, context: &CausalFrontier) -> CausalFrontier {
        let mut frontier = self
            .roots
            .clone()
            .unwrap_or_else(|| CausalFrontier::new(context.resolver().clone()));

        for event in self.iter().take(len) {
            let id = event.id();
            frontier.set_by_idx(id.idx(), frontier.seq_by_idx(id.idx()).max(id.seq()));
        }

        frontier
    }
}

#[cfg(feature = "test_utils")]
impl<O> EventGraph<O>
where
    O: Debug,
{
    pub fn to_dot(&self) -> String {
        use petgraph::dot::{Config, Dot};
        format!(
            "{:?}",
            Dot::with_attr_getters(
                &self.graph,
                &[Config::EdgeNoLabel, Config::NodeNoLabel],
                &|_, _| String::new(),
                &|_, n| format!("label=\"{}\"", n.1),
            )
        )
    }
}

impl<O> Display for EventGraph<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Head:")?;
        for head in self.heads.iter() {
            write!(f, "{}", head)?;
        }
        writeln!(f, "\nEvents:")?;
        for (node_idx, tagged_op) in self.graph.node_indices().zip(self.graph.node_weights()) {
            write!(f, "\t{} -> ", tagged_op)?;
            let predecessors: Vec<String> = self
                .graph
                .neighbors_directed(node_idx, Direction::Outgoing)
                .filter_map(|pred_idx| self.map.get_by_left(&pred_idx).map(|id| id.to_string()))
                .collect();
            writeln!(f, "{:?}", predecessors)?;
        }
        Ok(())
    }
}

/// Contains EventIds retained for parent discovery, sorted by process and sequence number.
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, Clone)]
struct Cutter(BTreeMap<ReplicaIdx, BTreeMap<Seq, (NodeIndex, NodeIndex)>>);

impl Cutter {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Insert an event id and its corresponding node index in the cutter.
    fn insert(&mut self, event_id: &EventId, node_idx: NodeIndex) {
        self.0
            .entry(event_id.idx())
            .or_default()
            .entry(event_id.seq())
            .and_modify(|(_, last)| *last = node_idx)
            .or_insert((node_idx, node_idx));
    }

    /// For a given version, for each entry in the version,
    /// return the node index of the event with the same replica id and
    /// the highest sequence number that is lower or equal to the sequence number in the version.
    fn below(&self, version: &Version) -> Vec<NodeIndex> {
        version
            .iter()
            .filter_map(|(idx, seq)| {
                self.0.get(&idx).and_then(|seq_map| {
                    seq_map
                        .range(..=seq)
                        .next_back()
                        .map(|(_, (_, last))| *last)
                })
            })
            .collect()
    }

    /// First retained event beyond a vector's component, in replay order.
    fn first_after(&self, sequence: impl Fn(ReplicaIdx) -> Seq) -> Option<usize> {
        self.0
            .iter()
            .filter_map(|(&origin, events)| {
                events
                    .range((Excluded(sequence(origin)), Unbounded))
                    .next()
                    // A source event may expand into several disambiguated vertices.
                    .map(|(_, (first, _))| first.index())
            })
            .min()
    }
}

impl Display for Cutter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (replica_idx, seq_map) in &self.0 {
            write!(f, "{} -> ", replica_idx.0)?;
            for seq in seq_map.keys() {
                write!(f, "{{ {}, ", seq)?;
            }
            write!(f, "}}")?;
        }
        Ok(())
    }
}

impl Default for Cutter {
    fn default() -> Self {
        Self::new()
    }
}
