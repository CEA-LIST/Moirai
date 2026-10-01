use std::{collections::HashMap, fmt::Debug};

use bimap::BiMap;
use daggy::{Dag, NodeIndex};
use moirai_protocol::{
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaId,
};
use petgraph::dot::{Config, Dot};

/// A sparse execution DAG and streaming inter-replica pair counts.
///
/// Every successful send must be appended once, in causal order, starting at
/// sequence one for each origin. DOT edges connect each event to the latest
/// dependency from each origin; redundant dependency edges are retained.
#[derive(Debug)]
pub struct ExecutionGraph<O> {
    graph: Dag<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
    replica_event_counts: HashMap<ReplicaId, u128>,
    comparable_pairs: u128,
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
            replica_event_counts: HashMap::new(),
            comparable_pairs: 0,
        }
    }

    pub fn append(&mut self, event: &Event<O>) {
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
        let mut previous_events = 0_u128;
        let mut total_pairs = 0_u128;
        for &count in self.replica_event_counts.values() {
            total_pairs += previous_events * count;
            previous_events += count;
        }

        if total_pairs == 0 {
            return 0.0;
        }

        let concurrent_pairs = total_pairs
            .checked_sub(self.comparable_pairs)
            .expect("causal pair count exceeds the complete execution's inter-replica pairs");
        concurrent_pairs as f64 / total_pairs as f64
    }

    // TODO: Add transitive reduction for better visualization
    pub fn _reduce(&mut self) {
        todo!("transitive reduction of the execution graph for better visualization");
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        broadcast::internalizer::Interner, clock::version_vector::Version, event::lamport::Lamport,
    };
    use petgraph::algo::has_path_connecting;
    use rand::{RngExt, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    use super::*;

    fn interners(replica_count: usize) -> Vec<Interner> {
        (0..replica_count)
            .map(|origin| {
                let mut interner = Interner::new();
                // Each origin has a different index-to-ID mapping, as in bootstrap.
                for offset in 0..replica_count {
                    interner.intern(&ReplicaId(((origin + offset) % replica_count).to_string()));
                }
                interner
            })
            .collect()
    }

    fn event(origin: usize, clock: &[usize], interner: &Interner) -> Event<()> {
        let origin_id = ReplicaId(origin.to_string());
        let origin_idx = interner.get(&origin_id).unwrap();
        let mut version = Version::new(origin_idx, interner.resolver().clone());
        for (replica, &seq) in clock.iter().enumerate() {
            let idx = interner.get(&ReplicaId(replica.to_string())).unwrap();
            version.set_by_idx(idx, seq);
        }
        let id = EventId::new(origin_idx, clock[origin], interner.resolver().clone());
        Event::new(id, Lamport::from(&version), (), version)
    }

    fn graph(schedule: &[(usize, [usize; 2])]) -> ExecutionGraph<()> {
        let interners = interners(2);
        let mut graph = ExecutionGraph::new();
        for (origin, clock) in schedule {
            graph.append(&event(*origin, clock, &interners[*origin]));
        }
        graph
    }

    fn brute_force_ratio(graph: &ExecutionGraph<()>) -> f64 {
        let nodes: Vec<_> = graph.graph.graph().node_indices().collect();
        let mut total_pairs = 0_u128;
        let mut concurrent_pairs = 0_u128;
        for (index, &left) in nodes.iter().enumerate() {
            for &right in &nodes[index + 1..] {
                if graph.graph[left].id().origin_id() == graph.graph[right].id().origin_id() {
                    continue;
                }
                total_pairs += 1;
                if !has_path_connecting(&graph.graph, left, right, None)
                    && !has_path_connecting(&graph.graph, right, left, None)
                {
                    concurrent_pairs += 1;
                }
            }
        }
        if total_pairs == 0 {
            0.0
        } else {
            concurrent_pairs as f64 / total_pairs as f64
        }
    }

    #[test]
    fn concurrency_ratio_handles_empty_and_single_origin_executions() {
        assert_eq!(
            ExecutionGraph::<()>::new().inter_replica_concurrency_ratio(),
            0.0
        );
        let graph = graph(&[(0, [1, 0]), (0, [2, 0])]);
        assert_eq!(graph.inter_replica_concurrency_ratio(), 0.0);
    }

    #[test]
    fn concurrency_ratio_handles_independent_chained_and_mixed_events() {
        let independent = graph(&[
            (0, [1, 0]),
            (1, [0, 1]),
            (0, [2, 0]),
            (1, [0, 2]),
            (1, [0, 3]),
        ]);
        assert_eq!(independent.inter_replica_concurrency_ratio(), 1.0);

        let chained = graph(&[(0, [1, 0]), (1, [1, 1]), (0, [2, 1]), (1, [2, 2])]);
        assert_eq!(chained.inter_replica_concurrency_ratio(), 0.0);

        let mixed = graph(&[(0, [1, 0]), (1, [0, 1]), (0, [2, 1]), (1, [0, 2])]);
        assert_eq!(mixed.inter_replica_concurrency_ratio(), 0.75);
    }

    #[test]
    fn vector_clock_counts_match_graph_reachability_across_random_schedules() {
        const REPLICAS: usize = 4;
        let interners = interners(REPLICAS);
        for seed in 0..24 {
            let mut rng = ChaCha8Rng::from_seed([seed; 32]);
            let mut clocks = [[0; REPLICAS]; REPLICAS];
            let mut graph = ExecutionGraph::new();
            for _ in 0..80 {
                let origin = rng.random_range(0..REPLICAS);
                for other in 0..REPLICAS {
                    if rng.random_bool(0.3) {
                        let remote = clocks[other];
                        for (local, remote) in clocks[origin].iter_mut().zip(remote) {
                            *local = (*local).max(remote);
                        }
                    }
                }
                clocks[origin][origin] += 1;
                graph.append(&event(origin, &clocks[origin], &interners[origin]));
            }
            assert_eq!(
                graph.inter_replica_concurrency_ratio(),
                brute_force_ratio(&graph),
                "seed {seed}"
            );
        }
    }

    #[test]
    fn concurrency_counting_handles_a_hundred_thousand_events() {
        let interners = interners(2);
        let mut graph = ExecutionGraph::new();
        for sequence in 1..=50_000 {
            graph.append(&event(0, &[sequence, 0], &interners[0]));
            graph.append(&event(1, &[0, sequence], &interners[1]));
        }
        assert_eq!(graph.inter_replica_concurrency_ratio(), 1.0);
        assert_eq!(graph.graph.edge_count(), 99_998);
    }
}
