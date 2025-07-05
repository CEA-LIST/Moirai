use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
    rc::Rc,
};

use petgraph::graph::DiGraph;

use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
        matrix_clock::MatrixClock,
    },
    crdt::multidigraph::Graph,
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
    // TODO: the graph is not necessary, the CRDT can be recomputed from the arc_content and vertex_content
    graph: EventGraph<Graph<V, E>>,
    arc_content: HashMap<(V, V, E), El>,
    vertex_content: HashMap<V, Nl>,
    // For fast lookup of arcs by vertex
    vertex_index: HashMap<V, HashSet<(V, V, E)>>,
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
            vertex_index: HashMap::new(),
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
            vertex_index: HashMap::new(),
        }
    }

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match &event.op {
            UWGraph::UpdateVertex(v, no) => {
                let aw_graph_event = Event::new(
                    Graph::AddVertex(v.clone()),
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
                    Graph::RemoveVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.new_event(&aw_graph_event);
            }
            UWGraph::UpdateArc(v1, v2, e_id, eo) => {
                let aw_graph_event = Event::new(
                    Graph::AddArc(v1.clone(), v2.clone(), e_id.clone()),
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
                    Graph::RemoveArc(v1.clone(), v2.clone(), e1.clone()),
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
                    Graph::AddVertex(v.clone()),
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
                    Graph::RemoveVertex(v.clone()),
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

                        // The `true` here is what makes the map a Update-Wins graph
                        v_content.r_n(vector_clock, true);
                    }
                }
                if let Some(vidx) = self.vertex_index.get(v) {
                    for (v1, v2, e1) in vidx.iter() {
                        if let Some(arc_content) =
                            self.arc_content
                                .get_mut(&(v1.clone(), v2.clone(), e1.clone()))
                        {
                            // If the arc content is not empty, we need to prune the events
                            if !arc_content.is_empty() {
                                // compute the vector clock of the remove operation
                                let vector_clock =
                                    ltm.get_by_idx(event.metadata().origin.unwrap()).unwrap();

                                // The `true` here is what makes the map a Update-Wins graph
                                arc_content.r_n(vector_clock, true);
                            }
                        }
                    }
                }
                // TODO: remove the vertex from the vertex index
            }
            UWGraph::UpdateArc(v1, v2, e1, lo) => {
                let aw_graph_event = Event::new(
                    Graph::AddArc(v1.clone(), v2.clone(), e1.clone()),
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

                self.vertex_index.entry(v1.clone()).or_default().insert((
                    v1.clone(),
                    v2.clone(),
                    e1.clone(),
                ));
                self.vertex_index.entry(v2.clone()).or_default().insert((
                    v1.clone(),
                    v2.clone(),
                    e1.clone(),
                ));
            }
            UWGraph::RemoveArc(v1, v2, e1) => {
                let event = Event::new(
                    Graph::RemoveArc(v1.clone(), v2.clone(), e1.clone()),
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

                        // The `true` here is what makes the map a Update-Wins graph
                        arc_content.r_n(vector_clock, true);
                    }
                }

                self.vertex_index.entry(v1.clone()).or_default().remove(&(
                    v1.clone(),
                    v2.clone(),
                    e1.clone(),
                ));
            }
        }
    }

    fn collect_events_since(&self, since: &Since, ltm: &MatrixClock) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        type NestedVertexes<V, O> = HashMap<V, Vec<Event<O>>>;
        let nested_vertexes: NestedVertexes<V, Nl::Op> = self
            .vertex_content
            .iter()
            .map(|(k, log)| (k.clone(), log.collect_events_since(since, ltm)))
            .collect();
        type NestedArcs<V, E, O> = HashMap<(V, V, E), Vec<Event<O>>>;
        let nested_arcs: NestedArcs<V, E, El::Op> = self
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
                Graph::AddVertex(v) => {
                    let mut event_found = nested_vertexes
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
                Graph::RemoveVertex(v) => {
                    events.push(Event::new(
                        UWGraph::RemoveVertex(v.clone()),
                        event.metadata().clone(),
                        event.lamport(),
                    ));
                }
                Graph::AddArc(v1, v2, e) => {
                    let mut event_found = nested_arcs
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
                Graph::RemoveArc(v1, v2, e1) => {
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
                Graph::AddVertex(v.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWGraph::UpdateArc(v1, v2, e1, _) => Event::new(
                Graph::AddArc(v1.clone(), v2.clone(), e1.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWGraph::RemoveVertex(v) => Event::new(
                Graph::RemoveVertex(v.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWGraph::RemoveArc(v1, v2, e) => Event::new(
                Graph::RemoveArc(v1.clone(), v2.clone(), e.clone()),
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
        let mut node_idx = HashMap::new();
        for (v, log) in self.vertex_content.iter() {
            if log.is_empty() {
                continue; // Skip empty vertices
            }
            let idx = graph.add_node(log.eval());
            node_idx.insert(v.clone(), idx);
        }
        for ((v1, v2, _), log) in self.arc_content.iter() {
            if log.is_empty() {
                continue; // Skip empty edges
            }
            let idx1 = node_idx.get(v1);
            let idx2 = node_idx.get(v2);
            match (idx1, idx2) {
                (Some(i1), Some(i2)) => {
                    graph.add_edge(*i1, *i2, log.eval());
                }
                _ => {
                    continue;
                }
            }
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
                    Graph::AddVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
            UWGraph::RemoveVertex(v) => {
                let aw_graph_event = Event::new(
                    Graph::RemoveVertex(v.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
            UWGraph::UpdateArc(v1, v2, e1, _) => {
                let aw_graph_event = Event::new(
                    Graph::AddArc(v1.clone(), v2.clone(), e1.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.graph.clock_from_event(&aw_graph_event)
            }
            UWGraph::RemoveArc(v1, v2, e) => {
                let aw_graph_event = Event::new(
                    Graph::RemoveArc(v1.clone(), v2.clone(), e.clone()),
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
                    .deps(clocks, view, dot, &Graph::AddVertex(v.clone()));
                let log = self.vertex_content.entry(v.clone()).or_default();
                log.deps(clocks, view, dot, vo);
            }
            UWGraph::RemoveVertex(v) => {
                self.graph
                    .deps(clocks, view, dot, &Graph::RemoveVertex(v.clone()));
            }
            UWGraph::UpdateArc(v1, v2, e1, eo) => {
                self.graph.deps(
                    clocks,
                    view,
                    dot,
                    &Graph::AddArc(v1.clone(), v2.clone(), e1.clone()),
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
                    &Graph::RemoveArc(v1.clone(), v2.clone(), e.clone()),
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
    use petgraph::{algo::is_isomorphic, graph::DiGraph};

    use crate::{
        crdt::{
            lww_register::LWWRegister,
            resettable_counter::Counter,
            test_util::{triplet, twins},
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

    #[test_log::test]
    fn simple_graph() {
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWGraphLog<&str, u8, EventGraph<LWWRegister<i32>>, EventGraph<Counter<i32>>>>();

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(2)));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let mut graph: DiGraph<i32, i32> = DiGraph::new();
        graph.add_node(2);

        assert!(petgraph::algo::is_isomorphic(&tcsb_a.eval(), &graph));
        assert!(petgraph::algo::is_isomorphic(
            &tcsb_a.eval(),
            &tcsb_b.eval()
        ));
    }

    #[test_log::test]
    fn remove_vertex() {
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWGraphLog<&str, u8, EventGraph<LWWRegister<i32>>, EventGraph<Counter<i32>>>>();

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(UWGraph::RemoveVertex("A"));
        tcsb_a.try_deliver(event_b);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test_log::test]
    fn revive_arc() {
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWGraphLog<&str, u8, EventGraph<LWWRegister<i32>>, EventGraph<Counter<i32>>>>();

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(2)));
        tcsb_a.try_deliver(event_b);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        let event_b = tcsb_b.tc_bcast(UWGraph::RemoveVertex("B"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));

        assert_eq!(tcsb_a.eval().node_count(), 1);
        assert_eq!(tcsb_a.eval().edge_count(), 0);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(3)));
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval().node_count(), 2);
        assert_eq!(tcsb_a.eval().edge_count(), 1);

        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_a.eval(), &[]));
        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_b.eval(), &[]));

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test_log::test]
    fn revive_arc_2() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<
            UWGraphLog<&str, u8, EventGraph<LWWRegister<i32>>, EventGraph<Counter<i32>>>,
        >();

        let event_b_1 = tcsb_b.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        tcsb_c.try_deliver(event_b_1.clone());
        let event_c_1 = tcsb_c.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(1)));
        let event_c_2 = tcsb_c.tc_bcast(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        tcsb_b.try_deliver(event_c_1.clone());
        tcsb_b.try_deliver(event_c_2.clone());

        assert!(petgraph::algo::is_isomorphic(
            &tcsb_b.eval(),
            &tcsb_c.eval()
        ));

        let event_a_1 = tcsb_a.tc_bcast(UWGraph::RemoveVertex("B"));
        let event_a_2 = tcsb_a.tc_bcast(UWGraph::RemoveArc("A", "B", 1));
        tcsb_b.try_deliver(event_a_1.clone());
        tcsb_b.try_deliver(event_a_2.clone());

        tcsb_c.try_deliver(event_a_1);
        tcsb_c.try_deliver(event_a_2);

        tcsb_a.try_deliver(event_b_1);
        tcsb_a.try_deliver(event_c_1);
        tcsb_a.try_deliver(event_c_2);

        assert_eq!(tcsb_a.eval().node_count(), 2);
        assert_eq!(tcsb_a.eval().edge_count(), 1);

        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_a.eval(), &[]));

        assert!(petgraph::algo::is_isomorphic(
            &tcsb_a.eval(),
            &tcsb_b.eval()
        ));
        assert!(petgraph::algo::is_isomorphic(
            &tcsb_a.eval(),
            &tcsb_c.eval()
        ));
        assert!(petgraph::algo::is_isomorphic(
            &tcsb_b.eval(),
            &tcsb_c.eval()
        ));
    }

    #[test_log::test]
    fn revive_arc_3() {
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWGraphLog<&str, u8, EventGraph<LWWRegister<i32>>, EventGraph<Counter<i32>>>>();

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(2)));
        tcsb_a.try_deliver(event_b);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        tcsb_b.try_deliver(event_a);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateArc("B", "A", 1, Counter::Inc(8)));
        let event_b = tcsb_b.tc_bcast(UWGraph::RemoveVertex("B"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));

        assert_eq!(tcsb_a.eval().node_count(), 1);
        assert_eq!(tcsb_a.eval().edge_count(), 0);

        let event_a = tcsb_a.tc_bcast(UWGraph::UpdateVertex("B", LWWRegister::Write(3)));
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval().node_count(), 2);
        assert_eq!(tcsb_a.eval().edge_count(), 1);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }
}
