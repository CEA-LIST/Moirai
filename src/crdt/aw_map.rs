use std::{
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
    rc::Rc,
};

use super::aw_set::AWSet;
use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
        matrix_clock::MatrixClock,
    },
    protocol::{
        event::Event, event_graph::EventGraph, log::Log, membership::ViewData, pulling::Since,
    },
};

#[derive(Clone, Debug)]
pub enum AWMap<K, O> {
    Update(K, O),
    Remove(K),
}

#[derive(Clone, Debug)]
pub struct AWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
{
    keys: EventGraph<AWSet<K>>,
    values: HashMap<K, L>,
}

impl<K: Clone + Debug + Eq + Hash, L> Default for AWMapLog<K, L> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            values: Default::default(),
        }
    }
}

impl<K, L> Log for AWMapLog<K, L>
where
    L: Log,
    K: Clone + Debug + Ord + PartialOrd + Hash + Eq + Default + Display,
{
    type Op = AWMap<K, L::Op>;
    type Value = HashMap<K, L::Value>;

    fn new() -> Self {
        Self {
            keys: EventGraph::new(),
            values: HashMap::new(),
        }
    }

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match &event.op {
            AWMap::Update(k, v) => {
                let aw_set_event = Event::new(AWSet::Add(k.clone()), event.metadata().clone());
                self.keys.new_event(&aw_set_event);

                let mut nested_clocks = event.metadata.clone();
                nested_clocks.pop_front();

                assert!(
                    !nested_clocks.is_empty(),
                    "AWMapLog: metadata should not be empty after popping the first element"
                );

                let log_event = Event::new_nested(v.clone(), nested_clocks);
                self.values
                    .entry(k.clone())
                    .or_default()
                    .new_event(&log_event);
            }
            AWMap::Remove(k) => {
                let event = Event::new(AWSet::Remove(k.clone()), event.metadata().clone());
                self.keys.new_event(&event);
            }
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool, ltm: &MatrixClock) {
        match &event.op {
            AWMap::Update(k, v) => {
                let aw_set_event = Event::new(AWSet::Add(k.clone()), event.metadata().clone());
                self.keys.prune_redundant_events(&aw_set_event, is_r_0, ltm);

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

                let log_event = Event::new(v.clone(), log_metadata);
                self.values
                    .entry(k.clone())
                    .or_default()
                    .prune_redundant_events(&log_event, is_r_0, ltm);
            }
            AWMap::Remove(k) => {
                let event = Event::new(AWSet::Remove(k.clone()), event.metadata().clone());
                self.keys.prune_redundant_events(&event, is_r_0, ltm);

                if let Some(v) = self.values.get_mut(k) {
                    if !event.metadata().is_empty() {
                        // compute the vector clock of the remove operation

                        let vector_clock =
                            ltm.get_by_idx(event.metadata().origin.unwrap()).unwrap();

                        // The `true` here is what makes the map a Update-Wins Map
                        v.r_n(vector_clock, true);
                    }
                }
            }
        }
    }

    fn stable_by_clock(&mut self, clock: &Clock<Full>) {
        self.keys.stable_by_clock(clock);

        for v in self.values.values_mut() {
            v.stable_by_clock(clock);
        }
    }

    fn clock_from_event(&self, event: &Event<Self::Op>) -> Clock<Full> {
        match &event.op {
            AWMap::Update(k, _) => {
                let aw_set_event = Event::new(AWSet::Add(k.clone()), event.metadata().clone());
                self.keys.clock_from_event(&aw_set_event)
            }
            AWMap::Remove(k) => {
                let event = Event::new(AWSet::Remove(k.clone()), event.metadata().clone());
                self.keys.clock_from_event(&event)
            }
        }
    }

    fn collect_events_since(&self, since: &Since, ltm: &MatrixClock) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        let nested_ops: HashMap<K, Vec<Event<L::Op>>> = self
            .values
            .iter()
            .map(|(k, log)| (k.clone(), log.collect_events_since(since, ltm)))
            .collect();

        for event in self.keys.collect_events_since(since, ltm) {
            match &event.op {
                AWSet::Add(k) => {
                    let mut corresponding_op = nested_ops
                        .get(k)
                        .unwrap()
                        .iter()
                        .find(|e| Dot::from(e.metadata()) == Dot::from(event.metadata()))
                        .unwrap()
                        .clone();
                    corresponding_op
                        .metadata
                        .push_front(event.metadata().clone());
                    events.push(Event::new_nested(
                        AWMap::Update(k.clone(), corresponding_op.op.clone()),
                        corresponding_op.metadata.clone(),
                    ));
                }
                AWSet::Remove(k) => {
                    events.push(Event::new(
                        AWMap::Remove(k.clone()),
                        event.metadata().clone(),
                    ));
                }
                AWSet::Clear => {
                    panic!("AWMapLog: Clear operation is not supported");
                }
            }
        }
        events
    }

    /// A vector clock is used to avoid issues with direct predecessors.
    fn r_n(&mut self, vector_clock: &Clock<Full>, conservative: bool) {
        self.keys.r_n(vector_clock, conservative);

        for v in self.values.values_mut() {
            if v.is_empty() {
                continue;
            }
            v.r_n(vector_clock, conservative);
        }
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        let event = match &event.op {
            AWMap::Update(k, _) => Event::new(AWSet::Add(k.clone()), event.metadata().clone()),
            AWMap::Remove(k) => Event::new(AWSet::Remove(k.clone()), event.metadata().clone()),
        };
        self.keys.any_r(&event)
    }

    fn eval(&self) -> Self::Value {
        let mut map = HashMap::new();
        let set = self.keys.eval();
        for (k, v) in &self.values {
            let val = v.eval();
            if set.contains(k) {
                map.insert(k.clone(), val);
            }
        }
        map
    }

    fn stabilize(&mut self, _: &Dot) {}

    fn purge_stable_metadata(&mut self, dot: &Dot) {
        self.keys.purge_stable_metadata(dot);
        self.values
            .iter_mut()
            .for_each(|(_, v)| v.purge_stable_metadata(dot));
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn deps(
        &mut self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        op: &Self::Op,
    ) {
        match op {
            AWMap::Update(k, v) => {
                self.keys.deps(clocks, view, dot, &AWSet::Add(k.clone()));
                let log = self.values.entry(k.clone()).or_default();
                log.deps(clocks, view, dot, v);
            }
            AWMap::Remove(k) => {
                self.keys.deps(clocks, view, dot, &AWSet::Remove(k.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        crdt::{
            aw_map::{AWMap, AWMapLog},
            counter::Counter,
            duet::{Duet, DuetLog},
            test_util::{triplet, twins},
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn simple_aw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWMapLog<String, EventGraph<Counter<i32>>>>();

        let event = tcsb_a.tc_bcast(AWMap::Update("a".to_string(), Counter::Dec(5)));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update("b".to_string(), Counter::Inc(5)));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update("a".to_string(), Counter::Inc(15)));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), 10);
        map.insert(String::from("b"), 5);
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_aw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWMapLog<String, EventGraph<Counter<i32>>>>();

        let event_a = tcsb_a.tc_bcast(AWMap::Remove("a".to_string()));
        let event_b = tcsb_b.tc_bcast(AWMap::Update("a".to_string(), Counter::Inc(10)));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let mut map = HashMap::new();
        map.insert(String::from("a"), 10);
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<
            AWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
        >();

        let event = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update(
            "b".to_string(),
            Duet::Second(Counter::Dec(7)),
        ));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (25, 0));
        map.insert(String::from("b"), (5, -7));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_concurrent_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<
            AWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
        >();

        let event = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (15, 0));
        map.insert(String::from("b"), (5, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());

        let event_a = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        let event_b = tcsb_b.tc_bcast(AWMap::Remove("a".to_string()));
        tcsb_b.try_deliver(event_a);
        // bug here ->
        tcsb_a.try_deliver(event_b);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (10, 0));
        map.insert(String::from("b"), (5, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());

        let event = tcsb_a.tc_bcast(AWMap::Update(
            "b".to_string(),
            Duet::Second(Counter::Dec(7)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Remove("b".to_string()));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (10, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_deeply_nested() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<
            AWMapLog<String, AWMapLog<i32, AWMapLog<String, EventGraph<Counter<i32>>>>>,
        >();

        let event_a_1 = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            AWMap::Update(1, AWMap::Update("z".to_string(), Counter::Inc(2))),
        ));

        let event_a_2 = tcsb_a.tc_bcast(AWMap::Update(
            "b".to_string(),
            AWMap::Update(2, AWMap::Update("f".to_string(), Counter::Dec(20))),
        ));

        let event_a_3 = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            AWMap::Update(1, AWMap::Update("z".to_string(), Counter::Inc(8))),
        ));

        let event_b_1 = tcsb_b.tc_bcast(AWMap::Update(
            "a".to_string(),
            AWMap::Update(1, AWMap::Update("z".to_string(), Counter::Inc(8))),
        ));

        let event_b_2 = tcsb_b.tc_bcast(AWMap::Update(
            "a".to_string(),
            AWMap::Update(2, AWMap::Remove("f".to_string())),
        ));

        let event_c_1 = tcsb_c.tc_bcast(AWMap::Update(
            "a".to_string(),
            AWMap::Update(1, AWMap::Remove("z".to_string())),
        ));

        tcsb_a.try_deliver(event_b_1.clone());
        tcsb_a.try_deliver(event_b_2.clone());
        tcsb_a.try_deliver(event_c_1.clone());

        tcsb_b.try_deliver(event_a_1.clone());
        tcsb_b.try_deliver(event_c_1.clone());
        tcsb_b.try_deliver(event_a_2.clone());
        tcsb_b.try_deliver(event_a_3.clone());

        tcsb_c.try_deliver(event_b_2.clone());
        tcsb_c.try_deliver(event_a_3.clone());
        tcsb_c.try_deliver(event_b_1.clone());
        tcsb_c.try_deliver(event_a_2.clone());
        tcsb_c.try_deliver(event_a_1.clone());

        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
        assert_eq!(tcsb_c.eval(), tcsb_b.eval());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::utils::convergence_checker::convergence_checker;

        let mut result = HashMap::new();
        result.insert("a".to_string(), 5);
        result.insert("b".to_string(), -5);
        convergence_checker::<AWMapLog<String, EventGraph<Counter<i32>>>>(
            &[
                AWMap::Update("a".to_string(), Counter::Inc(5)),
                AWMap::Update("b".to_string(), Counter::Dec(5)),
                AWMap::Remove("a".to_string()),
                AWMap::Remove("b".to_string()),
            ],
            result,
        );
    }
}
