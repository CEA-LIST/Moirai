use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use bimap::BiMap;
use petgraph::{
    Direction,
    graph::NodeIndex,
    prelude::StableDiGraph,
    visit::{Dfs, Visitable},
};

use crate::{
    HashSet,
    clock::version_vector::Version,
    crdt::{
        eval::{Eval, EvalNested},
        pure_crdt::{CausalReset, PureCRDT},
        query::QueryOperation,
    },
    event::{Event, id::EventId, lamport::Lamport, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::{
        effect_context::EffectContext,
        log::IsLog,
        unstable_state::{IsUnstableCausal, IsUnstableCore, IsUnstableDelivery},
    },
};

#[derive(Debug, Clone)]
pub struct EventGraph<O> {
    graph: StableDiGraph<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
    heads: HashSet<EventId>,
    /// Contains EventIds retained for parent discovery, sorted by process and sequence number.
    cutter: Cutter,
}

impl<O> IsLog for EventGraph<O>
where
    O: PureCRDT + Clone,
{
    type Value = <O as PureCRDT>::Value;
    type Op = O;

    fn new() -> Self {
        const {
            debug_assert!(O::DISABLE_R_WHEN_NOT_R && O::DISABLE_R_WHEN_R && O::DISABLE_STABILIZE);
        }
        Default::default()
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        self.append(event);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        debug_assert!(self.graph.node_count() >= self.heads.len());
        match O::causal_reset(
            version,
            conservative,
            &<O as PureCRDT>::StableState::default(),
            self,
        ) {
            CausalReset::Inject(ops) => {
                for op in ops {
                    let event_id = EventId::from(version);
                    let lamport = Lamport::from(version);
                    let event = Event::new(event_id, lamport, op, version.clone());
                    self.append(event);
                }
            }
            CausalReset::Prune => {
                panic!("EventGraph requires CRDT to provide a reset op via causal_reset_plan()");
            }
        }
    }

    fn is_default(&self) -> bool {
        self.graph.node_count() == 0
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        O::is_enabled(op, &<O as PureCRDT>::StableState::default(), self)
    }

    fn stabilize(&mut self, version: &Version) {
        self.cutter.remove(version);
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

impl<O> IsUnstableCore<O> for EventGraph<O>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
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

        //* Debugging */
        #[allow(clippy::mutable_key_type)] // false positive
        fn max_one_per_id(set: &HashSet<EventId>) -> bool {
            let mut seen = HashSet::default();
            for p in set {
                if !seen.insert(p.origin_id()) {
                    return false; // duplicate name found
                }
            }
            true
        }

        debug_assert!(self.heads.iter().all(|h| {
            let nx = self.map.get_by_right(h).unwrap();
            let neighbors_count = self
                .graph
                .neighbors_directed(*nx, Direction::Incoming)
                .count();
            neighbors_count == 0
        }));
        debug_assert!(max_one_per_id(&self.heads));
        debug_assert!(self.graph.node_count() >= self.heads.len());
    }

    /// # Complexity
    /// O(1)
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.map
            .get_by_right(event_id)
            .and_then(|idx| self.graph.node_weight(*idx))
    }

    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>>
    where
        O: Clone,
    {
        self.collect_predecessors(version)
            .iter()
            .filter_map(|nx| self.graph.node_weight(*nx).cloned())
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.graph.node_weights()
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

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        self.heads
            .iter()
            .filter_map(|id| self.get(id).cloned())
            .collect()
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

// impl<O> UnstableKeyed<O> for EventGraph<O>
// where
//     O: Clone + Debug,
// {
//     type Key = EventId;

//     fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key {
//         tagged_op.id().clone()
//     }

//     /// # Complexity
//     /// O(e')
//     fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>> {
//         self.get(key)
//     }
// }

impl<O> Default for EventGraph<O> {
    fn default() -> Self {
        Self {
            graph: StableDiGraph::new(),
            map: BiMap::new(),
            heads: HashSet::default(),
            cutter: Cutter::new(),
        }
    }
}

impl<O> EventGraph<O>
where
    O: Debug,
{
    /// Collect all the node indices that correspond to an event lower or equal to the given version.
    /// # Complexity
    /// O(v + e)
    // TODO: use the cutter to reduce the number of visited nodes and edges
    fn collect_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        let start_nodes: Vec<NodeIndex> = self
            .heads
            .iter()
            .map(|id| *self.map.get_by_right(id).unwrap())
            .collect();

        let mut collected = Vec::new();
        let discovered = self.graph.visit_map();
        let mut dfs = Dfs::from_parts(start_nodes, discovered);

        debug_assert!(self.graph.node_count() >= self.heads.len());

        while let Some(nx) = dfs.next(&self.graph) {
            let event_id = self.map.get_by_left(&nx).unwrap();
            if event_id.is_predecessor_of(version) {
                collected.push(nx);
            }
        }

        // TODO: are node indices topologically sorted?

        collected
    }

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
    ///    current start; any previous frontier node reached by this walk is removed.
    fn find_immediate_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        let mut frontier = HashSet::default();
        let mut dominated = HashSet::default();

        for start_idx in self.cutter.below(version) {
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
        parents
    }
}

impl<O, Q> EvalNested<Q> for EventGraph<O>
where
    O: PureCRDT + Clone + Eval<Q, EventGraph<O>>,
    Q: QueryOperation,
    EventGraph<O>: IsLog,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        O::execute_query(q, &O::StableState::default(), self)
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

/// Contains EventIds retained for parent discovery, sorted by process and sequence number.
#[derive(Debug, Clone)]
struct Cutter(BTreeMap<ReplicaIdx, BTreeMap<usize, NodeIndex>>);

impl Cutter {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Insert an event id and its corresponding node index in the cutter.
    fn insert(&mut self, event_id: &EventId, node_idx: NodeIndex) {
        self.0
            .entry(event_id.idx())
            .or_default()
            .insert(event_id.seq(), node_idx);
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
                        .map(|(_, node_idx)| *node_idx)
                })
            })
            .collect()
    }

    /// Remove all entries in the cutter that are lower or equal to the given version.
    fn remove(&mut self, version: &Version) {
        for (idx, seq) in version.iter() {
            if let Some(seq_map) = self.0.get_mut(&idx) {
                let keys_to_remove: Vec<usize> =
                    seq_map.range(..seq).map(|(seq, _)| *seq).collect();
                for key in keys_to_remove {
                    seq_map.remove(&key);
                }
                if seq_map.is_empty() {
                    self.0.remove(&idx);
                }
            }
        }
    }
}
