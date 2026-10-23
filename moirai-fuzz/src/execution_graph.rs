use std::{cmp::Reverse, fmt::Debug};

use bimap::BiMap;
use daggy::{Dag, NodeIndex};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use moirai_protocol::{
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaId,
};
use petgraph::{
    Direction,
    dot::{Config, Dot},
    visit::IntoNeighborsDirected,
};
use rustc_hash::FxHashMap;

/// A sparse execution DAG and streaming inter-replica pair counts.
///
/// Every successful send must be appended once, in causal order, starting at
/// sequence one for each origin. Captured edges connect each event to the latest
/// dependency from each origin; DOT export removes redundant dependency edges.
#[derive(Debug)]
pub struct ExecutionGraph<O> {
    graph: Dag<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
    replica_event_counts: FxHashMap<ReplicaId, u128>,
    comparable_pairs: u128,
    reduced: bool,
}

impl<O> Default for ExecutionGraph<O>
where
    O: Debug + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<O> ExecutionGraph<O>
where
    O: Debug + Clone,
{
    pub fn new() -> Self {
        Self {
            graph: Dag::new(),
            map: BiMap::new(),
            replica_event_counts: FxHashMap::default(),
            comparable_pairs: 0,
            reduced: true,
        }
    }

    pub fn append(&mut self, event: &Event<O>) {
        self.reduced = false;
        let origin_count = self
            .replica_event_counts
            .entry(event.id().origin_id().clone())
            .or_default();
        assert_eq!(
            event.id().seq() as u128,
            *origin_count + 1,
            "execution graph requires every send exactly once, starting at sequence one"
        );
        *origin_count += 1;

        // Each cross-origin causal predecessor is one comparable pair. Counting
        // it at the later event counts each pair exactly once. Sum in u128 so
        // neither a frontier's sum nor the accumulated pair count uses usize.
        self.comparable_pairs += event
            .version()
            .iter()
            .filter(|(idx, _)| *idx != event.version().origin_idx())
            .map(|(_, seq)| seq as u128)
            .sum::<u128>();

        let tagged_op = TaggedOp::from(event);
        let node = self.graph.add_node(tagged_op);
        self.map.insert(node, event.id().clone());
        for dep in event.version().dependencies().filter(|dep| dep.seq() > 0) {
            let dep_node = *self
                .map
                .get_by_right(&dep)
                .expect("execution graph requires causal dependencies to be appended first");
            self.graph.add_edge(dep_node, node, ()).unwrap();
        }
    }

    pub fn to_dot(&self) -> String {
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

    /// Fraction of cross-origin event pairs that are causally incomparable.
    ///
    /// For origin counts n_r, total pairs are sum_{r < s}(n_r * n_s).
    /// Comparable pairs were accumulated from version vectors during append.
    /// Across N events and R replicas, counting takes O(N * R) time and O(R)
    /// additional memory; this accessor takes O(R) time.
    pub fn inter_replica_concurrency_ratio(&self) -> f64 {
        let (concurrent_pairs, total_pairs) = self.pair_counts();
        if total_pairs == 0 {
            return 0.0;
        }
        concurrent_pairs as f64 / total_pairs as f64
    }

    /// Mean number of concurrent partners per update over the whole execution.
    /// Each unordered concurrent pair contributes to both endpoints, including
    /// partners issued later. Empty executions have no average.
    pub fn avg_concurrent_updates(&self) -> Option<f64> {
        let count = self.graph.node_count();
        (count > 0).then(|| 2.0 * self.pair_counts().0 as f64 / count as f64)
    }

    fn pair_counts(&self) -> (u128, u128) {
        let mut previous_events = 0_u128;
        let mut total_pairs = 0_u128;
        for &count in self.replica_event_counts.values() {
            total_pairs += previous_events * count;
            previous_events += count;
        }

        let concurrent_pairs = total_pairs
            .checked_sub(self.comparable_pairs)
            .expect("causal pair count exceeds the complete execution's inter-replica pairs");
        (concurrent_pairs, total_pairs)
    }

    /// Remove redundant incoming edges using per-origin causal clocks.
    /// Nodes are insertion-ordered topologically, and each origin is a chain.
    /// For N updates and R origins, temporary memory is O(N * R), and time is
    /// O(N * R^2) for this DAG's at most R incoming edges per update.
    pub fn reduce(&mut self, display: bool) {
        if self.reduced {
            return;
        }

        let node_count = self.graph.node_count();
        let origins: FxHashMap<_, _> = self
            .replica_event_counts
            .keys()
            .cloned()
            .enumerate()
            .map(|(idx, id)| (id, idx))
            .collect();
        let origin_count = origins.len();
        let mut clocks = vec![
            0;
            node_count
                .checked_mul(origin_count)
                .expect("execution DAG clock storage is too large")
        ];
        let mut frontier = vec![0; origin_count];
        let mut retained_edges = Vec::with_capacity(self.graph.edge_count());

        let progress = ProgressBar::new(node_count as u64);
        if !display || node_count < 1_000 {
            progress.set_draw_target(ProgressDrawTarget::hidden());
        }
        progress.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} updates {msg} ETA: {eta_precise}")
            .unwrap());
        progress.set_message("Reducing execution DAG...");

        // Nodes are never removed, so their indices are dense and topological.
        for idx in 0..node_count {
            let node = NodeIndex::new(idx);
            let mut parents: Vec<_> = self
                .graph
                .neighbors_directed(node, Direction::Incoming)
                .collect();
            parents.sort_unstable_by_key(|parent| Reverse(parent.index()));
            frontier.fill(0);

            // Later parents are processed first. An earlier parent is redundant
            // iff a retained parent's causal past already contains its event.
            // A maximum across origin prefixes is sufficient to test this.
            for parent in parents {
                let id = self.graph[parent].id();
                let origin = origins[id.origin_id()];
                if frontier[origin] < id.seq() {
                    retained_edges.push((parent, node, ()));
                    let start = parent.index() * origin_count;
                    for (known, &seq) in frontier
                        .iter_mut()
                        .zip(&clocks[start..start + origin_count])
                    {
                        *known = (*known).max(seq);
                    }
                }
            }

            let id = self.graph[node].id();
            frontier[origins[id.origin_id()]] = id.seq();
            let start = idx * origin_count;
            clocks[start..start + origin_count].copy_from_slice(&frontier);
            progress.inc(1);
        }

        if retained_edges.len() != self.graph.edge_count() {
            progress.set_message("Applying transitive reduction...");
            // Removing individual edges scans adjacency lists and can become
            // quadratic for high-fan-out nodes. Rebuild once instead, moving
            // operation payloads and preserving every node index.
            drop(clocks);
            let original = std::mem::replace(
                &mut self.graph,
                Dag::with_capacity(node_count, retained_edges.len()),
            );
            let (nodes, _) = original.into_graph().into_nodes_edges();
            for node in nodes {
                self.graph.add_node(node.weight);
            }
            self.graph.add_edges(retained_edges).unwrap();
        }

        self.reduced = true;
        progress.finish_with_message("Execution DAG reduced ✓");
    }
}
