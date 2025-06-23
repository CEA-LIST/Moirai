use std::{
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
    rc::Rc,
};

use petgraph::{graph::DiGraph, visit::EdgeRef};

use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
        matrix_clock::MatrixClock,
    },
    crdt::aw_multigraph::AWGraph,
    protocol::{
        event::Event, event_graph::EventGraph, log::Log, membership::ViewData, pulling::Since,
    },
};

#[derive(Clone, Debug)]
pub enum UWGraph<V, E, No, Lo> {
    UpdateVertex(V, No),
    RemoveVertex(V),
    UpdateArc(V, V, E, Lo),
    RemoveArc(V, V, E),
}

#[derive(Clone, Debug)]
pub struct UWGraphLog<V, E, Nl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    graph: EventGraph<AWGraph<V, E>>,
    arc_content: HashMap<(V, V, E), El>,
    vertex_content: HashMap<V, Nl>,
}

impl<V, E, Nl, El> Default for UWGraphLog<V, E, Nl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
    Nl: Log,
    El: Log,
{
    fn default() -> Self {
        Self {
            graph: EventGraph::new(),
            arc_content: HashMap::new(),
            vertex_content: HashMap::new(),
        }
    }
}

impl<V, E, Nl, El> Log for UWGraphLog<V, E, Nl, El>
where
    Nl: Log,
    El: Log,
    V: Clone + Debug + Ord + PartialOrd + Hash + Eq + Default + Display,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    type Op = UWGraph<V, E, Nl::Op, El::Op>;
    type Value = DiGraph<Nl::Value, El::Value>;

    fn new() -> Self {
        Self {
            graph: EventGraph::new(),
            arc_content: HashMap::new(),
            vertex_content: HashMap::new(),
        }
    }

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match &event.op {
            UWGraph::UpdateVertex(v, no) => {
                let aw_graph_event = Event::new(
                    AWGraph::AddVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.new_event(&aw_graph_event);

                let mut nested_clocks = event.metadata.clone();
                nested_clocks.pop_front();

                assert!(
                    !nested_clocks.is_empty(),
                    "UWGraphLog: metadata should not be empty after popping the first element"
                );

                let log_event = Event::new_nested(no.clone(), nested_clocks, event.lamport());
                self.vertex_content
                    .entry(v.clone())
                    .or_default()
                    .new_event(&log_event);
            }
            UWGraph::RemoveVertex(v) => {
                let aw_graph_event = Event::new(
                    AWGraph::RemoveVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.new_event(&aw_graph_event);
            }
            UWGraph::UpdateArc(v1, v2, e_id, eo) => {
                let aw_graph_event = Event::new(
                    AWGraph::AddArc(v1.clone(), v2.clone(), e_id.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.new_event(&aw_graph_event);

                let mut nested_clocks = event.metadata.clone();
                nested_clocks.pop_front();

                assert!(
                    !nested_clocks.is_empty(),
                    "UWGraphLog: metadata should not be empty after popping the first element"
                );

                let log_event = Event::new_nested(eo.clone(), nested_clocks, event.lamport());
                self.arc_content
                    .entry((v1.clone(), v2.clone(), e_id.clone()))
                    .or_default()
                    .new_event(&log_event);
            }
            UWGraph::RemoveArc(v1, v2, e1) => {
                let aw_graph_event = Event::new(
                    AWGraph::RemoveArc(v1.clone(), v2.clone(), e1.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.new_event(&aw_graph_event);
            }
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool, ltm: &MatrixClock) {
        match &event.op {
            UWGraph::UpdateVertex(v, vo) => {
                let aw_graph_event = Event::new(
                    AWGraph::AddVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph
                    .prune_redundant_events(&aw_graph_event, is_r_0, ltm);

                let log_metadata = if let Some(m) = event.metadata.get(1) {
                    m.clone()
                } else {
                    let mut clock = Clock::<Partial>::new(&event.metadata().view, event.origin());
                    clock.set_by_idx(
                        event.metadata().origin.unwrap(),
                        event
                            .metadata()
                            .get_by_idx(event.metadata().origin.unwrap())
                            .unwrap(),
                    );
                    clock
                };

                let log_event = Event::new(vo.clone(), log_metadata, event.lamport());
                self.vertex_content
                    .entry(v.clone())
                    .or_default()
                    .prune_redundant_events(&log_event, is_r_0, ltm);
            }
            UWGraph::RemoveVertex(v) => {
                let event = Event::new(
                    AWGraph::RemoveVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.prune_redundant_events(&event, is_r_0, ltm);

                if let Some(v_content) = self.vertex_content.get_mut(v) {
                    // If the vertex content is not empty, we need to prune the events
                    if !v_content.is_empty() {
                        // compute the vector clock of the remove operation
                        let vector_clock =
                            ltm.get_by_idx(event.metadata().origin.unwrap()).unwrap();

                        // The `true` here is what makes the map a Update-Wins Map
                        v_content.r_n(vector_clock, true);
                    }
                }
            }
            UWGraph::UpdateArc(v1, v2, e1, lo) => {
                let aw_graph_event = Event::new(
                    AWGraph::AddArc(v1.clone(), v2.clone(), e1.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph
                    .prune_redundant_events(&aw_graph_event, is_r_0, ltm);

                let log_metadata = if let Some(m) = event.metadata.get(1) {
                    m.clone()
                } else {
                    let mut clock = Clock::<Partial>::new(&event.metadata().view, event.origin());
                    clock.set_by_idx(
                        event.metadata().origin.unwrap(),
                        event
                            .metadata()
                            .get_by_idx(event.metadata().origin.unwrap())
                            .unwrap(),
                    );
                    clock
                };

                let log_event = Event::new(lo.clone(), log_metadata, event.lamport());
                self.arc_content
                    .entry((v1.clone(), v2.clone(), e1.clone()))
                    .or_default()
                    .prune_redundant_events(&log_event, is_r_0, ltm);
            }
            UWGraph::RemoveArc(v1, v2, e1) => {
                let event = Event::new(
                    AWGraph::RemoveArc(v1.clone(), v2.clone(), e1.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.prune_redundant_events(&event, is_r_0, ltm);

                if let Some(arc_content) =
                    self.arc_content
                        .get_mut(&(v1.clone(), v2.clone(), e1.clone()))
                {
                    // If the arc content is not empty, we need to prune the events
                    if !arc_content.is_empty() {
                        // compute the vector clock of the remove operation
                        let vector_clock =
                            ltm.get_by_idx(event.metadata().origin.unwrap()).unwrap();

                        // The `true` here is what makes the map a Update-Wins Map
                        arc_content.r_n(vector_clock, true);
                    }
                }
            }
        }
    }

    fn collect_events_since(&self, since: &Since, ltm: &MatrixClock) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        let nested_vertex: HashMap<V, Vec<Event<Nl::Op>>> = self
            .vertex_content
            .iter()
            .map(|(k, log)| (k.clone(), log.collect_events_since(since, ltm)))
            .collect();
        let nested_arc: HashMap<(V, V, E), Vec<Event<El::Op>>> = self
            .arc_content
            .iter()
            .map(|((v1, v2, e), log)| {
                (
                    (v1.clone(), v2.clone(), e.clone()),
                    log.collect_events_since(since, ltm),
                )
            })
            .collect();

        for event in self.graph.collect_events_since(since, ltm) {
            match &event.op {
                AWGraph::AddVertex(v) => {
                    let mut event_found = nested_vertex
                        .get(v)
                        .unwrap()
                        .iter()
                        .find(|e| Dot::from(*e) == Dot::from(&event))
                        .unwrap()
                        .clone();
                    event_found.metadata.push_front(event.metadata().clone());
                    events.push(Event::new_nested(
                        UWGraph::UpdateVertex(v.clone(), event_found.op.clone()),
                        event_found.metadata.clone(),
                        event_found.lamport(),
                    ));
                }
                AWGraph::RemoveVertex(v) => {
                    events.push(Event::new(
                        UWGraph::RemoveVertex(v.clone()),
                        event.metadata().clone(),
                        event.lamport(),
                    ));
                }
                AWGraph::AddArc(v1, v2, e) => {
                    let mut event_found = nested_arc
                        .get(&(v1.clone(), v2.clone(), e.clone()))
                        .unwrap()
                        .iter()
                        .find(|e| Dot::from(*e) == Dot::from(&event))
                        .unwrap()
                        .clone();
                    event_found.metadata.push_front(event.metadata().clone());
                    events.push(Event::new_nested(
                        UWGraph::UpdateArc(
                            v1.clone(),
                            v2.clone(),
                            e.clone(),
                            event_found.op.clone(),
                        ),
                        event_found.metadata.clone(),
                        event_found.lamport(),
                    ));
                }
                AWGraph::RemoveArc(v1, v2, e1) => {
                    events.push(Event::new(
                        UWGraph::RemoveArc(v1.clone(), v2.clone(), e1.clone()),
                        event.metadata().clone(),
                        event.lamport(),
                    ));
                }
            }
        }
        events
    }

    fn redundant_itself(&self, event: &Event<Self::Op>) -> bool {
        let event = match &event.op {
            UWGraph::UpdateVertex(v, _) => Event::new(
                AWGraph::AddVertex(v.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWGraph::UpdateArc(v1, v2, e1, _) => Event::new(
                AWGraph::AddArc(v1.clone(), v2.clone(), e1.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWGraph::RemoveVertex(v) => Event::new(
                AWGraph::RemoveVertex(v.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWGraph::RemoveArc(v1, v2, e) => Event::new(
                AWGraph::RemoveArc(v1.clone(), v2.clone(), e.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
        };
        self.graph.redundant_itself(&event)
    }

    fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
        self.graph.r_n(metadata, conservative);

        for v in self.arc_content.values_mut() {
            if v.is_empty() {
                continue;
            }
            v.r_n(metadata, conservative);
        }

        for v in self.vertex_content.values_mut() {
            if v.is_empty() {
                continue;
            }
            v.r_n(metadata, conservative);
        }
    }

    fn eval(&self) -> Self::Value {
        let mut graph = Self::Value::new();
        let aux = self.graph.eval();
        let mut node_index = HashMap::new();
        for v in aux.node_weights() {
            let idx = graph.add_node(self.vertex_content.get(v).unwrap().eval());
            node_index.insert(v.clone(), idx);
        }
        for e in aux.edge_references() {
            let source = e.source();
            let target = e.target();
            let v1 = aux.node_weight(source).unwrap();
            let v2 = aux.node_weight(target).unwrap();
            let nx1 = node_index.get(v1).unwrap();
            let nx2 = node_index.get(v2).unwrap();
            let weight = self
                .arc_content
                .get(&(v1.clone(), v2.clone(), e.weight().clone()))
                .unwrap()
                .eval();
            graph.add_edge(*nx1, *nx2, weight);
        }
        graph
    }

    fn stabilize(&mut self, _dot: &Dot) {}

    fn purge_stable_metadata(&mut self, dot: &Dot) {
        self.graph.purge_stable_metadata(dot);
        self.vertex_content
            .iter_mut()
            .for_each(|(_, v)| v.purge_stable_metadata(dot));
        self.arc_content
            .iter_mut()
            .for_each(|(_, v)| v.purge_stable_metadata(dot));
    }

    fn stable_by_clock(&mut self, clock: &Clock<Full>) {
        self.graph.stable_by_clock(clock);
        for log in self.vertex_content.values_mut() {
            log.stable_by_clock(clock);
        }
        for log in self.arc_content.values_mut() {
            log.stable_by_clock(clock);
        }
    }

    fn clock_from_event(&self, event: &Event<Self::Op>) -> Clock<Full> {
        match &event.op {
            UWGraph::UpdateVertex(v, _) => {
                let aw_graph_event = Event::new(
                    AWGraph::AddVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
            UWGraph::RemoveVertex(v) => {
                let aw_graph_event = Event::new(
                    AWGraph::RemoveVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
            UWGraph::UpdateArc(v1, v2, e1, _) => {
                let aw_graph_event = Event::new(
                    AWGraph::AddArc(v1.clone(), v2.clone(), e1.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
            UWGraph::RemoveArc(v1, v2, e) => {
                let aw_graph_event = Event::new(
                    AWGraph::RemoveArc(v1.clone(), v2.clone(), e.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
        }
    }

    fn deps(
        &mut self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        op: &Self::Op,
    ) {
        match op {
            UWGraph::UpdateVertex(v, vo) => {
                self.graph
                    .deps(clocks, view, dot, &AWGraph::AddVertex(v.clone()));
                let log = self.vertex_content.entry(v.clone()).or_default();
                log.deps(clocks, view, dot, vo);
            }
            UWGraph::RemoveVertex(v) => {
                self.graph
                    .deps(clocks, view, dot, &AWGraph::RemoveVertex(v.clone()));
            }
            UWGraph::UpdateArc(v1, v2, e1, eo) => {
                self.graph.deps(
                    clocks,
                    view,
                    dot,
                    &AWGraph::AddArc(v1.clone(), v2.clone(), e1.clone()),
                );
                let log = self
                    .arc_content
                    .entry((v1.clone(), v2.clone(), e1.clone()))
                    .or_default();
                log.deps(clocks, view, dot, eo);
            }
            UWGraph::RemoveArc(v1, v2, e) => {
                self.graph.deps(
                    clocks,
                    view,
                    dot,
                    &AWGraph::RemoveArc(v1.clone(), v2.clone(), e.clone()),
                );
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            lww_register::LWWRegister,
            resettable_counter::Counter,
            test_util::twins,
            uw_multigraph::{UWGraph, UWGraphLog},
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn nested_graph() {
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWGraphLog<&str, u8, EventGraph<LWWRegister<i32>>, EventGraph<Counter<i32>>>>();

        let event = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(2)));
        tcsb_a.try_deliver(event);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(5)));
        let event_b_2 = tcsb_b.tc_bcast(UWGraph::UpdateArc("A", "B", 2, Counter::Dec(9)));

        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);
        tcsb_a.try_deliver(event_b_2);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(5)));
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(10)));
        let event_b_2 = tcsb_b.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(8)));

        tcsb_b.try_deliver(event_a);

        let event_b_3 = tcsb_b.tc_bcast(UWGraph::RemoveArc("A", "B", 1));
        tcsb_a.try_deliver(event_b);
        tcsb_a.try_deliver(event_b_2);
        tcsb_a.try_deliver(event_b_3);

        let event = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(3)));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(4)));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWGraph::UpdateArc("B", "A", 1, Counter::Inc(3)));
        tcsb_b.try_deliver(event);

        println!(
            "Eval A: {:?}",
            petgraph::dot::Dot::with_config(&tcsb_a.eval(), &[])
        );
        println!(
            "Eval B: {:?}",
            petgraph::dot::Dot::with_config(&tcsb_b.eval(), &[])
        );

        assert!(petgraph::algo::is_isomorphic(
            &tcsb_a.eval(),
            &tcsb_b.eval()
        ));
    }
}
