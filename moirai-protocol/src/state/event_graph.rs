use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use bimap::BiMap;
use petgraph::{
    Direction,
    algo::has_path_connecting,
    graph::NodeIndex,
    prelude::StableDiGraph,
    visit::{Dfs, VisitMap, Visitable},
};

#[cfg(feature = "sink")]
use crate::state::{object_path::ObjectPath, sink::SinkCollector, sink::SinkOwnership};
use crate::{
    HashMap, HashSet,
    clock::version_vector::{Seq, Version},
    crdt::{
        eval::{Eval, EvalNested},
        pure_crdt::{CausalReset, PureCRDT},
        query::QueryOperation,
    },
    event::{Event, id::EventId, lamport::Lamport, tagged_op::TaggedOp},
    state::{log::IsLog, unstable_state::IsUnstableState},
};

// TODO: use Daggy?
#[derive(Debug, Clone)]
pub struct EventGraph<O> {
    // TODO: use the stability vector to know where to stop when performing find_immediate_predecessors, and to avoid visiting the whole graph when collecting predecessors.
    graph: StableDiGraph<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
    heads: HashSet<EventId>,
    by_replica: Vec<BTreeMap<Seq, NodeIndex>>,
    /// For each node, stores the maximum reachable sequence number per replica,
    /// including the node itself. Used as a fast negative filter before exact
    /// reachability checks.
    summaries: HashMap<NodeIndex, Vec<Seq>>,
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

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        #[cfg(feature = "sink")] _path: ObjectPath,
        #[cfg(feature = "sink")] _sink: &mut SinkCollector,
        #[cfg(feature = "sink")] _ownership: SinkOwnership,
    ) {
        IsUnstableState::append(self, event);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        debug_assert!(self.graph.node_count() >= self.heads.len());
        match O::causal_reset(
            version,
            conservative,
            &<O as PureCRDT>::StableState::default(),
            self,
        ) {
            // Note: inject with its own event id!
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

    fn stabilize(&mut self, _version: &Version) {}
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

// TODO: The Event Graph should never remove any operation from its graph.
// However, we must preserve the effect of operations that remove other operations.
// We need to ensure that querys do not read removed operations.

impl<O> IsUnstableState<O> for EventGraph<O>
where
    O: Clone + Debug,
{
    type Key = EventId;

    fn append(&mut self, event: Event<O>) {
        let new_tagged_op = TaggedOp::from(&event);
        let child_idx = self.graph.add_node(new_tagged_op);
        self.map.insert(child_idx, event.id().clone());
        let immediate_parents = self.find_immediate_predecessors(event.version());
        for parent_idx in &immediate_parents {
            self.graph.add_edge(child_idx, *parent_idx, ());
            let parent_id = self.map.get_by_left(parent_idx).unwrap();
            if self.heads.contains(parent_id) {
                self.heads.remove(parent_id);
            }
        }
        self.heads.insert(event.id().clone());
        if self.by_replica.len() <= event.id().idx().0 {
            self.by_replica
                .resize_with(event.id().idx().0 + 1, BTreeMap::new);
        }
        self.by_replica[event.id().idx().0].insert(event.id().seq(), child_idx);

        let mut summary = vec![0; event.id().resolver().len()];
        summary[event.id().idx().0] = event.id().seq();
        for parent_idx in &immediate_parents {
            if let Some(parent_summary) = self.summaries.get(parent_idx) {
                if summary.len() < parent_summary.len() {
                    summary.resize(parent_summary.len(), 0);
                }
                for (idx, seq) in parent_summary.iter().enumerate() {
                    if summary[idx] < *seq {
                        summary[idx] = *seq;
                    }
                }
            }
        }
        self.summaries.insert(child_idx, summary);

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

    fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key {
        tagged_op.id().clone()
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.map
            .get_by_right(event_id)
            .and_then(|idx| self.graph.node_weight(*idx))
    }

    /// # Complexity
    /// $O(e')$
    fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>> {
        self.get(key)
    }

    fn remove(&mut self, _event_id: &EventId) {
        // TODO: update heads, re-attach if needed
        unimplemented!("EventGraph::remove is not implemented");
    }

    fn remove_by_key(&mut self, key: &Self::Key) {
        self.remove(key)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.graph.node_weights()
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, _predicate: T) {
        // TODO: update heads, re-attach if needed
        unimplemented!("EventGraph::retain is not implemented");
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
        self.heads.clear();
        self.by_replica.clear();
        self.summaries.clear();
    }

    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>> {
        self.collect_predecessors(version)
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

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        self.heads
            .iter()
            .filter_map(|id| self.get(id).cloned())
            .collect()
    }

    /// # Complexity
    /// $O(1)$
    fn delivery_order(&self, event_id: &EventId) -> usize {
        let node_idx = self.map.get_by_right(event_id).unwrap();
        node_idx.index()
    }
}

impl<O> Default for EventGraph<O> {
    fn default() -> Self {
        Self {
            graph: StableDiGraph::new(),
            heads: HashSet::default(),
            map: BiMap::new(),
            by_replica: Vec::new(),
            summaries: HashMap::default(),
        }
    }
}

impl<O> EventGraph<O>
where
    O: Debug,
{
    fn may_reach(&self, from: NodeIndex, to: NodeIndex) -> bool {
        let Some(to_id) = self.map.get_by_left(&to) else {
            return true;
        };
        let Some(summary) = self.summaries.get(&from) else {
            return true;
        };
        summary
            .get(to_id.idx().0)
            .is_none_or(|max_seq| *max_seq >= to_id.seq())
    }

    fn start_nodes_for_version(&self, version: &Version) -> Vec<NodeIndex> {
        let mut start_nodes = Vec::new();
        let mut seen = HashSet::default();

        for (replica_idx, seq) in version.iter() {
            if seq == 0 {
                continue;
            }
            let Some(events) = self.by_replica.get(replica_idx.0) else {
                continue;
            };
            if let Some((_, node_idx)) = events.range(..=seq).next_back()
                && seen.insert(*node_idx)
            {
                start_nodes.push(*node_idx);
            }
        }

        if start_nodes.is_empty() {
            self.heads
                .iter()
                .filter_map(|id| self.map.get_by_right(id).copied())
                .collect()
        } else {
            start_nodes
        }
    }

    /// Collect all the node indices that correspond to an event lower or equal to the given version.
    /// # Complexity
    /// O(n)
    fn collect_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        let start_nodes = self.start_nodes_for_version(version);

        // TODO: does it visit in the right direction?
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
    /// O(n^2)
    ///
    /// # Algorithm
    /// 1. DFS from the heads of the graph.
    /// 2. For each node N, check if it is an predecessor of any node in the given version.
    /// 3. If it is, check that there is no other predecessor in the list with the same origin id and a higher sequence number.
    ///    3.1 If there exist a node N' in the list that has N as an ancestor, remove N from the list.
    ///    3.2 If not, add it to the list of immediate predecessors.
    ///    3.3 If there exist a node N' in the list with the same origin id and a lower sequence number, remove N' from the list.
    fn find_immediate_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        #[allow(clippy::mutable_key_type)]
        fn reaches_cached<O>(
            graph: &EventGraph<O>,
            cache: &mut HashMap<(NodeIndex, NodeIndex), bool>,
            from: NodeIndex,
            to: NodeIndex,
        ) -> bool
        where
            O: Debug,
        {
            if !graph.may_reach(from, to) {
                return false;
            }
            if let Some(reaches) = cache.get(&(from, to)) {
                return *reaches;
            }
            let reaches = has_path_connecting(&graph.graph, from, to, None);
            cache.insert((from, to), reaches);
            reaches
        }

        let start_nodes = self.start_nodes_for_version(version);

        #[allow(clippy::mutable_key_type)]
        let mut collected = Vec::<(EventId, NodeIndex)>::new();
        #[allow(clippy::mutable_key_type)]
        let mut reachability_cache: HashMap<(NodeIndex, NodeIndex), bool> = HashMap::default();
        let mut discovered = self.graph.visit_map();
        let mut stack = Vec::with_capacity(start_nodes.len());
        for node_idx in start_nodes {
            if discovered.visit(node_idx) {
                stack.push(node_idx);
            }
        }

        while let Some(node_idx) = stack.pop() {
            let event_id = self.map.get_by_left(&node_idx).unwrap();
            let dominated = collected.iter().any(|(id, nx)| {
                (event_id.origin_id() == id.origin_id() && id.seq() > event_id.seq())
                    || reaches_cached(self, &mut reachability_cache, *nx, node_idx)
            });

            if dominated {
                continue;
            }

            // The event is a predecessor of the version
            if event_id.is_predecessor_of(version) {
                // There is no event_id in the list with the same origin id and a higher sequence number
                // ...and there is no event_id in the list that has the new event_id as predecessor
                collected.retain(|(_, nx)| {
                    !reaches_cached(self, &mut reachability_cache, node_idx, *nx)
                });

                collected.push((event_id.clone(), node_idx));
                continue;
            }

            for parent_idx in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                if discovered.visit(parent_idx) {
                    stack.push(parent_idx);
                }
            }
        }

        collected.into_iter().map(|(_, nx)| nx).collect()
    }

    // TODO: very inefficient, improve it
    /// Perform transitive reduction on the event graph to remove redundant edges.
    /// # Complexity
    /// $O(∣V∣ * (∣V∣+∣E∣))$
    pub fn transitive_reduction(&mut self) {
        let edges: Vec<(NodeIndex, NodeIndex)> = self
            .graph
            .edge_indices()
            .map(|eidx| {
                let (u, v) = self.graph.edge_endpoints(eidx).unwrap();
                (u, v)
            })
            .collect();

        for (u, v) in edges {
            if !self.graph.contains_edge(u, v) {
                continue;
            }

            let mut redundant = false;
            let mut stack = Vec::new();
            let mut visited = self.graph.visit_map();

            for w in self
                .graph
                .neighbors_directed(u, Direction::Outgoing)
                .filter(|&nx| nx != v)
            {
                stack.push(w);
                visited.visit(w);
            }

            while let Some(nx) = stack.pop() {
                if nx == v {
                    redundant = true;
                    break;
                }
                for succ in self.graph.neighbors_directed(nx, Direction::Outgoing) {
                    if visited.visit(succ) {
                        stack.push(succ);
                    }
                }
            }

            if redundant && let Some(eidx) = self.graph.find_edge(u, v) {
                self.graph.remove_edge(eidx);
            }
        }
    }
}

impl<O, Q> EvalNested<Q> for EventGraph<O>
where
    O: PureCRDT + Clone + Eval<Q>,
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
