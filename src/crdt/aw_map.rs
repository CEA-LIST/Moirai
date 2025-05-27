use std::{
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
    rc::Rc,
};

use petgraph::visit::Dfs;

use super::aw_set::AWSet;
use crate::{
    clocks::{
        clock::{Clock, Full},
        dot::Dot,
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
pub struct UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
{
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

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
        match &event.op {
            AWMap::Update(k, v) => {
                let aw_set_event = Event::new(AWSet::Add(k.clone()), event.metadata().clone());
                self.keys.prune_redundant_events(&aw_set_event, is_r_0);

                let log_metadata = if let Some(m) = event.metadata.get(1) {
                    m.clone()
                } else {
                    Clock::new(&event.metadata().view, event.metadata().origin())
                };

                let log_event = Event::new(v.clone(), log_metadata);
                self.values
                    .entry(k.clone())
                    .or_default()
                    .prune_redundant_events(&log_event, is_r_0);
            }
            AWMap::Remove(k) => {
                let event = Event::new(AWSet::Remove(k.clone()), event.metadata().clone());
                self.keys.prune_redundant_events(&event, is_r_0);

                if let Some(v) = self.values.get_mut(k) {
                    // compute the vector clock of the remove operation
                    let mut vector_clock =
                        Clock::new_full(&event.metadata().view, Some(event.metadata().origin()));

                    let mut dfs = Dfs::new(
                        &self.keys.unstable,
                        *self
                            .keys
                            .dot_index_map
                            .get_by_left(&Dot::from(event.metadata()))
                            .unwrap(),
                    );

                    while let Some(nx) = dfs.next(&self.keys.unstable) {
                        let dot = self.keys.dot_index_map.get_by_right(&nx).unwrap();
                        if dot.val() > vector_clock.get(dot.origin()).unwrap() {
                            vector_clock.set(dot.origin(), dot.val());
                        }
                    }

                    // The `true` here is what makes the map a Update-Wins Map
                    v.r_n(&vector_clock, true);
                }
            }
        }
    }

    fn collect_events(&self, upper_bound: &Clock, lower_bound: &Clock) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        for (k, v) in &self.values {
            events.extend(
                v.collect_events(upper_bound, lower_bound)
                    .into_iter()
                    .map(|e| {
                        Event::new(AWMap::Update(k.clone(), e.op.clone()), e.metadata().clone())
                    }),
            );
        }
        events
    }

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        for (k, v) in &self.values {
            events.extend(
                v.collect_events_since(since).into_iter().map(|e| {
                    Event::new(AWMap::Update(k.clone(), e.op.clone()), e.metadata().clone())
                }),
            );
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
        for k in &self.keys.eval() {
            map.insert(k.clone(), self.values.get(k).unwrap().eval());
        }
        map
    }

    fn stabilize(&mut self, _: &Clock) {}

    fn purge_stable_metadata(&mut self, metadata: &Clock) {
        self.keys.purge_stable_metadata(metadata);
        self.values
            .iter_mut()
            .for_each(|(_, v)| v.purge_stable_metadata(metadata));
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn size(&self) -> usize {
        self.keys.size()
    }

    fn deps(&self, clocks: &mut VecDeque<Clock>, view: &Rc<ViewData>, dot: &Dot, op: &Self::Op) {
        match op {
            AWMap::Update(k, v) => {
                self.keys.deps(clocks, view, dot, &AWSet::Add(k.clone()));
                if let Some(log) = self.values.get(k) {
                    log.deps(clocks, view, dot, v);
                } else {
                    // If the key does not exist, we still need to add the dependency
                    // for the update operation.
                    let mut new_clock = Clock::new(view, dot.origin());
                    new_clock.set(dot.origin(), dot.val());
                    clocks.push_back(new_clock);
                }
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
            aw_map::{AWMap, UWMapLog},
            counter::Counter,
            duet::{Duet, DuetLog},
            test_util::twins,
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn simple_aw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, EventGraph<Counter<i32>>>>();

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
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, EventGraph<Counter<i32>>>>();

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
            UWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
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
            UWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
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

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::utils::convergence_checker::convergence_checker;

        let mut result = HashMap::new();
        result.insert("a".to_string(), 5);
        result.insert("b".to_string(), -5);
        convergence_checker::<UWMapLog<String, EventGraph<Counter<i32>>>>(
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
