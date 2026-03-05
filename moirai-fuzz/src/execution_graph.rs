use std::fmt::Debug;

use bimap::BiMap;
use daggy::{Dag, NodeIndex};
use moirai_protocol::event::{Event, id::EventId, tagged_op::TaggedOp};
use petgraph::dot::{Config, Dot};

pub struct ExecutionGraph<O> {
    graph: Dag<TaggedOp<O>, ()>,
    roots: Vec<NodeIndex>,
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
            roots: Vec::new(),
        }
    }

    pub fn append(&mut self, event: &Event<O>) {
        let tagged_op = TaggedOp::from(event);
        let node = self.graph.add_node(tagged_op);
        self.map.insert(node, event.id().clone());
        // Add edges from all causally prior events
        let deps: Vec<EventId> = event.version().dependencies().collect();
        if deps.is_empty() {
            self.roots.push(node);
        }
        for dep in deps {
            if let Some(&dep_node) = self.map.get_by_right(&dep) {
                self.graph.add_edge(dep_node, node, ()).unwrap();
            }
        }
    }

    pub fn reduce(&mut self) {
        let roots = self.roots.clone();
        self.graph.transitive_reduce(roots);
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
}
