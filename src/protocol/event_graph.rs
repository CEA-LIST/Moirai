use crate::clocks::dependency_clock::DependencyClock;
use crate::clocks::dot::Dot;

use super::event::Event;
use super::log::Log;
use super::pulling::Since;
use super::pure_crdt::PureCRDT;
use crate::clocks::clock::Clock;
use log::error;
use petgraph::graph::NodeIndex;
use petgraph::prelude::StableDiGraph;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct EventGraph<Op> {
    stable: Vec<Op>,
    graph: StableDiGraph<Op, ()>,
    index_map: HashMap<Dot, NodeIndex>,
}

impl<Op> EventGraph<Op>
where
    Op: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            stable: vec![],
            graph: StableDiGraph::new(),
            index_map: HashMap::new(),
        }
    }

    pub fn new_event(&mut self, event: &Event<Op>) {
        let dot = Dot::from(&event.metadata);
        if self.index_map.contains_key(&dot) {
            error!(
                "Event with metadata {:?} already present in the graph",
                event.metadata
            );
            return;
        }
        let from_idx = self.graph.add_node(event.op.clone());
        self.index_map.insert(Dot::from(&event.metadata), from_idx);
        for (origin, cnt) in event.metadata.clock.iter() {
            if origin == &event.metadata.origin {
                continue;
            }
            let to_dot = Dot::new(*origin, *cnt, &event.metadata.view);
            let to_idx = self
                .index_map
                .get(&to_dot)
                .expect("Causal delivery failed.");
            self.graph.add_edge(*to_idx, from_idx, ());
        }
    }

    pub fn partial_cmp(&self, first: Dot, second: Dot) -> Option<Ordering> {
        let first_idx = self
            .index_map
            .get(&first)
            .expect("Dot not found in the graph.");
        let second_idx = self
            .index_map
            .get(&second)
            .expect("Dot not found in the graph.");

        let first_to_second = petgraph::algo::has_path_connecting(
            &self.graph,
            *first_idx,
            *second_idx,
            None,
        );
        let second_to_first = petgraph::algo::has_path_connecting(
            &self.graph,
            *second_idx,
            *first_idx,
            None,
        );

        match (first_to_second, second_to_first) {
            (true, true) => None,
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => Some(Ordering::Equal),
        }
    }
}

impl<O> Log for EventGraph<O>
where
    O: PureCRDT,
{
    type Op = O;

    type Value = O::Value;

    fn new_event(&mut self, event: &Event<Self::Op>) {
        self.new_event(event);
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
        // Keep only the operations that are not made redundant by the new operation
        self.stable.retain(|o| {
            if is_r_0 {
                !(Self::Op::r_zero(&o, Some(Ordering::Less), &event.op))
            } else {
                !(Self::Op::r_one(&o, Some(Ordering::Less), &event.op))
            }
        });
        self.graph.retain_nodes(|_, node_idx| {
            let 
            // let old_event: Event<O> =
            //     Event::new(o.clone(), DependencyClock::bot(&event.metadata.view));
            // if is_r_0 {
            //     !(Self::Op::r_zero(&old_event, event))
            // } else {
            //     !(Self::Op::r_one(&old_event, event))
            // }
        });
        // self.graph.retain(|m, o| {
        //     let old_event: Event<O> = Event::new(o.clone(), m.clone());
        //     if is_r_0 {
        //         !(Self::Op::r_zero(&old_event, event))
        //     } else {
        //         !(Self::Op::r_one(&old_event, event))
        //     }
        // });
    }

    fn collect_events(&self, upper_bound: &DependencyClock) -> Vec<Event<Self::Op>> {
        todo!()
    }

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
        todo!()
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        todo!()
    }

    fn r_n(&mut self, metadata: &DependencyClock, conservative: bool) {
        todo!()
    }

    fn eval(&self) -> Self::Value {
        todo!()
    }

    fn stabilize(&mut self, metadata: &DependencyClock) {
        todo!()
    }

    fn purge_stable_metadata(&mut self, metadata: &DependencyClock) {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn lowest_view_id(&self) -> usize {
        todo!()
    }
}

impl<Op> Default for EventGraph<Op>
where
    Op: Clone + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}
