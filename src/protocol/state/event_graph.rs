use std::fmt::Debug;

use bimap::BiMap;
use petgraph::{
    algo::has_path_connecting,
    graph::NodeIndex,
    prelude::StableDiGraph,
    visit::{Dfs, VisitMap, Visitable},
    Direction,
};
#[cfg(feature = "fuzz")]
use rand::RngCore;

#[cfg(feature = "fuzz")]
use crate::fuzz::config::{OpGenerator, OpGeneratorNested};
use crate::{
    crdt::list::eg_walker::{List, ReadAt},
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::{Eval, EvalNested},
            pure_crdt::PureCRDT,
            query::QueryOperation,
        },
        event::{id::EventId, lamport::Lamport, tagged_op::TaggedOp, Event},
        state::{log::IsLog, unstable_state::IsUnstableState},
    },
    HashMap, HashSet,
};

#[derive(Debug, Clone)]
pub struct EventGraph<O> {
    // TODO: stable or not?
    graph: StableDiGraph<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
    heads: HashSet<EventId>,
}

impl<V> IsLog for EventGraph<List<V>>
where
    V: Debug + Clone,
{
    type Value = <List<V> as PureCRDT>::Value;
    type Op = List<V>;

    fn new() -> Self {
        const {
            debug_assert!(
                List::<V>::DISABLE_R_WHEN_NOT_R
                    && List::<V>::DISABLE_R_WHEN_R
                    && List::<V>::DISABLE_STABILIZE
            );
        }
        Default::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        IsUnstableState::append(self, event);
    }

    fn stabilize(&mut self, _version: &Version) {}

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        debug_assert!(self.graph.node_count() >= self.heads.len());
        if conservative {
            let state = self.execute_query(ReadAt::new(version));
            if state.is_empty() {
                // println!(
                //     "Redundant by parent: state is already empty, no need to add delete range"
                // );
                return;
            }
            let event_id = EventId::from(version);
            let lamport = Lamport::from(version);
            let event = Event::new(
                event_id,
                lamport,
                List::DeleteRange {
                    start: 0,
                    len: state.len(),
                },
                version.clone(),
            );
            // debug_assert!(self.is_enabled(&event.op()));
            // println!("Is enabled? {}", self.is_enabled(event.op()));
            self.append(event);
        } else {
            panic!("EventGraph::redundant_by_parent non-conservative is not implemented");
        }
    }

    fn is_default(&self) -> bool {
        self.graph.node_count() == 0
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        List::<V>::is_enabled(op, &<List<V> as PureCRDT>::StableState::default(), self)
    }
}

// TODO: The Event Graph should never remove any operation from its graph.
// However, we must preserve the effect of operations that remove other operations.
// We need to ensure that querys do not read removed operations.

impl<O> IsUnstableState<O> for EventGraph<O>
where
    O: Clone + Debug,
{
    fn append(&mut self, event: Event<O>) {
        let new_tagged_op = TaggedOp::from(&event);
        let child_idx = self.graph.add_node(new_tagged_op);
        self.map.insert(child_idx, event.id().clone());
        let immediate_parents = self.find_immediate_predecessors(event.version());
        for parent_idx in immediate_parents {
            self.graph.add_edge(child_idx, parent_idx, ());
            let parent_id = self.map.get_by_left(&parent_idx).unwrap();
            if self.heads.contains(parent_id) {
                self.heads.remove(parent_id);
            }
        }
        self.heads.insert(event.id().clone());

        // self.transitive_reduction();

        #[allow(clippy::mutable_key_type)] // false positive
        fn max_one_per_id(set: &HashSet<EventId>) -> bool {
            let mut seen = HashSet::default();
            // println!(
            //     "Checking max_one_per_id for set: {}",
            //     set.iter()
            //         .map(|e| e.to_string())
            //         .collect::<Vec<_>>()
            //         .join(", ")
            // );
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

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.map
            .get_by_right(event_id)
            .and_then(|idx| self.graph.node_weight(*idx))
    }

    /// # Complexity
    /// $O(e')$
    fn remove(&mut self, _event_id: &EventId) {
        // if let Some(idx) = self.map.get_by_right(event_id) {
        //     if self.heads.contains(event_id) {
        //         self.heads.remove(event_id);

        //     }

        //     self.graph.remove_node(*idx);
        //     self.map.remove_by_right(event_id);
        // }
        // TODO: update heads, re-attach if needed
        panic!("EventGraph::remove is not implemented");
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.graph.node_weights()
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, _predicate: T) {
        // let to_remove: Vec<NodeIndex> = self
        //     .graph
        //     .node_indices()
        //     .filter(|nx| {
        //         let tagged_op = self.graph.node_weight(*nx).unwrap();
        //         !predicate(tagged_op)
        //     })
        //     .collect();

        // for nx in to_remove {
        //     let event_id = self.map.get_by_left(&nx).unwrap().clone();
        //     self.graph.remove_node(nx);
        //     self.map.remove_by_right(&event_id);
        // }
        // TODO: update heads, re-attach if needed
        panic!("EventGraph::retain is not implemented");
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
        }
    }
}

impl<O> EventGraph<O>
where
    O: Debug,
{
    /// Collect all the node indices that correspond to an event lower or equal to the given version.
    /// # Complexity
    /// O(n)
    fn collect_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        let start_nodes: Vec<NodeIndex> = self
            .heads
            .iter()
            .map(|id| *self.map.get_by_right(id).unwrap())
            .collect();

        // TODO: does it visit in the right direction?
        let mut collected = Vec::new();
        let discovered = self.graph.visit_map();
        let mut dfs = Dfs::from_parts(start_nodes, discovered);

        debug_assert!(self.graph.node_count() >= self.heads.len());

        // println!("Collecting predecessors for version: {}", version);
        // println!(
        //     "Heads: {}",
        //     self.heads
        //         .iter()
        //         .map(|h| h.to_string())
        //         .collect::<Vec<_>>()
        //         .join(", ")
        // );
        // println!("Graph nodes: {}", self.graph.node_count());
        // println!("Dot representation:\n{}", self.to_dot());

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
    /// An immediate predecessor is a node that is an predecessor of the given version,
    /// and has no other predecessors that are also predecessors of the given version.
    /// This is used to find the minimal set of events that need to be considered
    /// when determining the state of the system at the given version.
    ///
    /// # Complexity
    /// O(n)
    ///
    /// # Algorithm
    /// 1. DFS from the heads of the graph.
    /// 2. For each node N, check if it is an predecessor of any node in the given version.
    /// 3. If it is, check that there is no other predecessor in the list with the same origin id and a higher sequence number.
    ///    3.1 If there exist a node N' in the list that has N as an ancestor, remove N from the list.
    ///    3.2 If not, add it to the list of immediate predecessors.
    ///    3.3 If there exist a node N' in the list with the same origin id and a lower sequence number, remove N' from the list.
    fn find_immediate_predecessors(&self, version: &Version) -> Vec<NodeIndex> {
        let start_nodes: Vec<NodeIndex> = self
            .heads
            .iter()
            .map(|id| *self.map.get_by_right(id).unwrap())
            .collect();

        #[allow(clippy::mutable_key_type)]
        let mut collected = HashMap::<EventId, NodeIndex>::default();
        let discovered = self.graph.visit_map();
        let mut dfs = Dfs::from_parts(start_nodes, discovered);

        while let Some(node_idx) = dfs.next(&self.graph) {
            let event_id = self.map.get_by_left(&node_idx).unwrap();
            // The event is a predecessor of the version
            if event_id.is_predecessor_of(version) {
                // There is no event_id in the list with the same origin id and a higher sequence number
                // ...and there is no event_id in the list that has the new event_id as predecessor
                if !collected.iter().any(|(id, nx)| {
                    (event_id.origin_id() == id.origin_id() && id.seq() > event_id.seq())
                        || has_path_connecting(&self.graph, *nx, node_idx, None)
                }) {
                    let to_remove: Vec<EventId> = collected
                        .iter()
                        .filter(|(_, nx)| has_path_connecting(&self.graph, node_idx, **nx, None))
                        .map(|(id, _)| id.clone())
                        .collect();
                    for id in to_remove {
                        collected.remove(&id);
                    }
                    collected.insert(event_id.clone(), node_idx);
                }
            }
        }

        collected.values().cloned().collect()
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

            if redundant {
                if let Some(eidx) = self.graph.find_edge(u, v) {
                    self.graph.remove_edge(eidx);
                }
            }
        }
    }
}

#[cfg(any(feature = "test_utils", feature = "fuzz"))]
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

#[cfg(feature = "fuzz")]
impl<O> OpGeneratorNested for EventGraph<O>
where
    O: PureCRDT + Clone + OpGenerator,
    EventGraph<O>: IsLog<Op = O>,
{
    fn generate(&self, rng: &mut impl RngCore) -> <EventGraph<O> as IsLog>::Op {
        O::generate(rng, &O::Config::default(), &O::StableState::default(), self)
    }
}
