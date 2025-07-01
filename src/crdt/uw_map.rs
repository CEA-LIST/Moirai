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
pub enum UWMap<K, O> {
    Update(K, O),
    Remove(K),
    // TODO: add clear
}

#[derive(Clone, Debug)]
pub struct UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
{
    // TODO: No need to store the keys in a separate EventGraph, we can use the values directly.
    // TODO: Must change `clock_from_event` to use the values directly.
    keys: EventGraph<AWSet<K>>,
    values: HashMap<K, L>,
}

impl<K: Clone + Debug + Eq + Hash, L> Default for UWMapLog<K, L> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            values: Default::default(),
        }
    }
}

impl<K, L> Log for UWMapLog<K, L>
where
    L: Log,
    K: Clone + Debug + Ord + PartialOrd + Hash + Eq + Default + Display,
    <L as Log>::Value: Default + PartialEq,
{
    type Op = UWMap<K, L::Op>;
    type Value = HashMap<K, L::Value>;

    fn new() -> Self {
        Self {
            keys: EventGraph::new(),
            values: HashMap::new(),
        }
    }

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match &event.op {
            UWMap::Update(k, v) => {
                let aw_set_event = Event::new(
                    AWSet::Add(k.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.keys.new_event(&aw_set_event);

                let mut nested_clocks = event.metadata.clone();
                nested_clocks.pop_front();

                assert!(
                    !nested_clocks.is_empty(),
                    "AWMapLog: metadata should not be empty after popping the first element. Event: {}",
                    event
                );

                let log_event = Event::new_nested(v.clone(), nested_clocks, event.lamport());
                self.values
                    .entry(k.clone())
                    .or_default()
                    .new_event(&log_event);
            }
            UWMap::Remove(k) => {
                let event = Event::new(
                    AWSet::Remove(k.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.keys.new_event(&event);
            }
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool, ltm: &MatrixClock) {
        match &event.op {
            UWMap::Update(k, v) => {
                let aw_set_event = Event::new(
                    AWSet::Add(k.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
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

                let log_event = Event::new(v.clone(), log_metadata, event.lamport());
                self.values
                    .entry(k.clone())
                    .or_default()
                    .prune_redundant_events(&log_event, is_r_0, ltm);
            }
            UWMap::Remove(k) => {
                let event = Event::new(
                    AWSet::Remove(k.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
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
            UWMap::Update(k, _) => {
                let aw_set_event = Event::new(
                    AWSet::Add(k.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
                self.keys.clock_from_event(&aw_set_event)
            }
            UWMap::Remove(k) => {
                let event = Event::new(
                    AWSet::Remove(k.clone()),
                    event.metadata().clone(),
                    event.lamport(),
                );
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
                    let mut event_found = nested_ops
                        .get(k)
                        .unwrap()
                        .iter()
                        .find(|e| Dot::from(*e) == Dot::from(&event))
                        .unwrap()
                        .clone();
                    event_found.metadata.push_front(event.metadata().clone());
                    events.push(Event::new_nested(
                        UWMap::Update(k.clone(), event_found.op.clone()),
                        event_found.metadata.clone(),
                        event_found.lamport(),
                    ));
                }
                AWSet::Remove(k) => {
                    events.push(Event::new(
                        UWMap::Remove(k.clone()),
                        event.metadata().clone(),
                        event.lamport(),
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

    fn redundant_itself(&self, event: &Event<Self::Op>) -> bool {
        let event = match &event.op {
            UWMap::Update(k, _) => Event::new(
                AWSet::Add(k.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
            UWMap::Remove(k) => Event::new(
                AWSet::Remove(k.clone()),
                event.metadata().clone(),
                event.lamport(),
            ),
        };
        self.keys.redundant_itself(&event)
    }

    fn eval(&self) -> Self::Value {
        let mut map = HashMap::new();
        // let set = self.keys.eval();
        for (k, v) in &self.values {
            if v.eval() == <L as Log>::Value::default() {
                // If the value is empty, we don't need to add it to the map
                continue;
            }
            let val = v.eval();
            map.insert(k.clone(), val);
        }
        // for (k, v) in self.values.iter() {
        //     assert!(map.contains_key(k) || v.is_empty());
        // }
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

    // TODO: deps has access to the LTM and return the full vector clock for a remove op.
    fn deps(
        &mut self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        op: &Self::Op,
    ) {
        match op {
            UWMap::Update(k, v) => {
                self.keys.deps(clocks, view, dot, &AWSet::Add(k.clone()));
                let log = self.values.entry(k.clone()).or_default();
                log.deps(clocks, view, dot, v);
            }
            UWMap::Remove(k) => {
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
            counter::Counter,
            duet::{Duet, DuetLog},
            test_util::{triplet, twins},
            uw_map::{UWMap, UWMapLog},
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn simple_uw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, EventGraph<Counter<i32>>>>();

        let event = tcsb_a.tc_bcast(UWMap::Update("a".to_string(), Counter::Dec(5)));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b".to_string(), Counter::Inc(5)));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("a".to_string(), Counter::Inc(15)));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), 10);
        map.insert(String::from("b"), 5);
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_uw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, EventGraph<Counter<i32>>>>();

        let event_a = tcsb_a.tc_bcast(UWMap::Remove("a".to_string()));
        let event_b = tcsb_b.tc_bcast(UWMap::Update("a".to_string(), Counter::Inc(10)));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let mut map = HashMap::new();
        map.insert(String::from("a"), 10);
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn uw_map_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<
            UWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
        >();

        let event = tcsb_a.tc_bcast(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update(
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
    fn uw_map_concurrent_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<
            UWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
        >();

        let event = tcsb_a.tc_bcast(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (15, 0));
        map.insert(String::from("b"), (5, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());

        let event_a = tcsb_a.tc_bcast(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        let event_b = tcsb_b.tc_bcast(UWMap::Remove("a".to_string()));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (10, 0));
        map.insert(String::from("b"), (5, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());

        let event = tcsb_a.tc_bcast(UWMap::Update(
            "b".to_string(),
            Duet::Second(Counter::Dec(7)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Remove("b".to_string()));
        tcsb_b.try_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (10, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn uw_map_deeply_nested() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<
            UWMapLog<String, UWMapLog<i32, UWMapLog<String, EventGraph<Counter<i32>>>>>,
        >();

        let event_a_1 = tcsb_a.tc_bcast(UWMap::Update(
            "a".to_string(),
            UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(2))),
        ));

        let event_a_2 = tcsb_a.tc_bcast(UWMap::Update(
            "b".to_string(),
            UWMap::Update(2, UWMap::Update("f".to_string(), Counter::Dec(20))),
        ));

        let event_a_3 = tcsb_a.tc_bcast(UWMap::Update(
            "a".to_string(),
            UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(8))),
        ));

        let event_b_1 = tcsb_b.tc_bcast(UWMap::Update(
            "a".to_string(),
            UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(8))),
        ));

        let event_b_2 = tcsb_b.tc_bcast(UWMap::Update(
            "a".to_string(),
            UWMap::Update(2, UWMap::Remove("f".to_string())),
        ));

        let event_c_1 = tcsb_c.tc_bcast(UWMap::Update(
            "a".to_string(),
            UWMap::Update(1, UWMap::Remove("z".to_string())),
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

        tcsb_a.eval();

        // assert_eq!(tcsb_a.eval(), tcsb_b.eval());
        // assert_eq!(tcsb_c.eval(), tcsb_b.eval());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::utils::convergence_checker::convergence_checker;

        let mut result = HashMap::new();
        result.insert("a".to_string(), 5);
        result.insert("b".to_string(), -5);
        convergence_checker::<UWMapLog<String, EventGraph<Counter<i32>>>>(
            &[
                UWMap::Update("a".to_string(), Counter::Inc(5)),
                UWMap::Update("b".to_string(), Counter::Dec(5)),
                UWMap::Remove("a".to_string()),
                UWMap::Remove("b".to_string()),
            ],
            result,
        );
    }
}
