use bimap::BiMap;
use daggy::{Dag, NodeIndex};
use fixedbitset::FixedBitSet;
use moirai_protocol::event::{Event, id::EventId, tagged_op::TaggedOp};
use petgraph::visit::{IntoNeighborsDirected, VisitMap, Visitable};
use petgraph::{
    Direction,
    algo::toposort,
    dot::{Config, Dot},
};
use std::{collections::HashMap, fmt::Debug};

pub struct ExecutionGraph<O> {
    graph: Dag<TaggedOp<O>, ()>,
    map: BiMap<NodeIndex, EventId>,
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
        }
    }

    pub fn append(&mut self, event: &Event<O>) {
        let tagged_op = TaggedOp::from(event);
        let node = self.graph.add_node(tagged_op);
        self.map.insert(node, event.id().clone());
        // Add edges from all causally prior events
        let deps: Vec<EventId> = event.version().dependencies().collect();
        for dep in deps {
            if let Some(&dep_node) = self.map.get_by_right(&dep) {
                self.graph.add_edge(dep_node, node, ()).unwrap();
            }
        }
    }

    pub fn reduce(&mut self) {
        transitive_reduce_dag(&mut self.graph);
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

    pub fn inter_replica_concurrency_ratio(&self) -> f64 {
        let node_count = self.graph.node_count();
        if node_count < 2 {
            return 0.0;
        }

        let topo = toposort(&self.graph, None).expect("graph must be acyclic");

        // Map NodeIndex -> dense integer position [0..n)
        let mut pos_of = HashMap::with_capacity(node_count);
        for (pos, node) in topo.iter().copied().enumerate() {
            pos_of.insert(node, pos);
        }

        // reachable[pos_u][pos_v] == true iff v reachable from u
        let mut reachable: Vec<FixedBitSet> = (0..node_count)
            .map(|_| FixedBitSet::with_capacity(node_count))
            .collect();

        // Process in reverse topological order
        for &u in topo.iter().rev() {
            let pu = pos_of[&u];

            for v in self.graph.neighbors_directed(u, Direction::Outgoing) {
                let pv = pos_of[&v];
                reachable[pu].insert(pv);
                let tmp = reachable[pv].clone();
                reachable[pu].union_with(&tmp);
            }
        }

        let mut concurrent_pairs = 0usize;
        let mut total_inter_replica_pairs = 0usize;

        for i in 0..node_count {
            let u = topo[i];
            let u_rep = self.graph[u].id().origin_id();

            for j in (i + 1)..node_count {
                let v = topo[j];
                let v_rep = self.graph[v].id().origin_id();

                if u_rep == v_rep {
                    continue;
                }

                total_inter_replica_pairs += 1;

                if !reachable[i][j] && !reachable[j][i] {
                    concurrent_pairs += 1;
                }
            }
        }

        if total_inter_replica_pairs == 0 {
            0.0
        } else {
            concurrent_pairs as f64 / total_inter_replica_pairs as f64
        }
    }

    pub fn internal(&self) -> &Dag<TaggedOp<O>, ()> {
        &self.graph
    }
}

fn transitive_reduce_dag<N>(graph: &mut Dag<N, ()>) {
    let edges: Vec<(NodeIndex, NodeIndex)> = graph
        .graph()
        .edge_indices()
        .filter_map(|eidx| graph.edge_endpoints(eidx))
        .collect();

    for (u, v) in edges {
        if graph.find_edge(u, v).is_none() {
            continue;
        }

        let mut redundant = false;
        let mut stack = Vec::new();
        let mut visited = graph.visit_map();

        for w in graph
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

            for succ in graph.neighbors_directed(nx, Direction::Outgoing) {
                if visited.visit(succ) {
                    stack.push(succ);
                }
            }
        }

        if redundant && let Some(eidx) = graph.find_edge(u, v) {
            graph.remove_edge(eidx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::transitive_reduce_dag;
    use daggy::Dag;

    #[test]
    fn transitive_reduction_removes_direct_shortcut() {
        let mut dag = Dag::<&str, ()>::new();

        let a = dag.add_node("a");
        let b = dag.add_node("b");
        let c = dag.add_node("c");

        dag.add_edge(a, b, ()).unwrap();
        dag.add_edge(b, c, ()).unwrap();
        dag.add_edge(a, c, ()).unwrap();

        transitive_reduce_dag(&mut dag);

        assert!(dag.find_edge(a, b).is_some());
        assert!(dag.find_edge(b, c).is_some());
        assert!(dag.find_edge(a, c).is_none());
    }

    #[test]
    fn transitive_reduction_handles_multiple_roots() {
        let mut dag = Dag::<&str, ()>::new();

        let a = dag.add_node("a");
        let b = dag.add_node("b");
        let c = dag.add_node("c");
        let d = dag.add_node("d");

        dag.add_edge(a, c, ()).unwrap();
        dag.add_edge(b, c, ()).unwrap();
        dag.add_edge(c, d, ()).unwrap();
        dag.add_edge(a, d, ()).unwrap();

        transitive_reduce_dag(&mut dag);

        assert!(dag.find_edge(a, c).is_some());
        assert!(dag.find_edge(b, c).is_some());
        assert!(dag.find_edge(c, d).is_some());
        assert!(dag.find_edge(a, d).is_none());
    }
}
