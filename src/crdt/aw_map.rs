use crate::protocol::metadata::Metadata;
use crate::protocol::pulling::Since;
use crate::protocol::{event::Event, log::Log, po_log::POLog, utils::Keyable};
use std::collections::HashMap;
use std::fmt::Debug;

use super::aw_set::AWSet;

#[derive(Clone, Debug)]
pub enum AWMap<K, O> {
    Update(K, O),
    Remove(K),
}

#[derive(Clone, Debug)]
pub struct UWMapLog<K, L> {
    keys: POLog<AWSet<K>>,
    values: HashMap<K, L>,
}

impl<K: Clone + Debug, L> Default for UWMapLog<K, L> {
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
    K: Keyable + Clone + Debug,
{
    type Op = AWMap<K, L::Op>;
    type Value = HashMap<K, L::Value>;

    fn new_event(&mut self, event: &Event<Self::Op>) {
        match &event.op {
            AWMap::Update(k, v) => {
                let event = Event::new(v.clone(), event.metadata.clone());
                self.values.entry(k.clone()).or_default().new_event(&event);
                self.keys
                    .new_event(&Event::new(AWSet::Add(k.clone()), event.metadata.clone()));
            }
            _ => unreachable!("Remove operation"),
        }
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
        match &event.op {
            AWMap::Update(k, v) => {
                let event = Event::new(AWSet::Add(k.clone()), event.metadata.clone());
                self.keys.prune_redundant_events(&event, is_r_0);

                let event = Event::new(v.clone(), event.metadata.clone());
                self.values
                    .entry(k.clone())
                    .or_default()
                    .prune_redundant_events(&event, is_r_0);
            }
            AWMap::Remove(k) => {
                let event = Event::new(AWSet::Remove(k.clone()), event.metadata.clone());
                self.keys.prune_redundant_events(&event, is_r_0);

                self.values.entry(k.clone()).and_modify(|v| {
                    v.r_n(&event.metadata, true);
                });

                if let Some(v) = self.values.get(k) {
                    if v.is_empty() {
                        self.values.remove(k);
                    }
                }
            }
        }
    }

    fn collect_events(&self, upper_bound: &Metadata) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        for (k, v) in &self.values {
            events.extend(
                v.collect_events(upper_bound)
                    .into_iter()
                    .map(|e| Event::new(AWMap::Update(k.clone(), e.op), e.metadata)),
            );
        }
        events
    }

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
        let mut events = vec![];
        for (k, v) in &self.values {
            events.extend(
                v.collect_events_since(since)
                    .into_iter()
                    .map(|e| Event::new(AWMap::Update(k.clone(), e.op), e.metadata)),
            );
        }
        events
    }

    fn r_n(&mut self, metadata: &Metadata, conservative: bool) {
        self.keys.r_n(metadata, conservative);
        self.values.retain(|_, v| {
            v.r_n(metadata, conservative);
            !v.is_empty()
        });
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        let event = match &event.op {
            AWMap::Update(k, _) => Event::new(AWSet::Add(k.clone()), event.metadata.clone()),
            AWMap::Remove(k) => Event::new(AWSet::Remove(k.clone()), event.metadata.clone()),
        };
        self.keys.any_r(&event)
    }

    fn eval(&self) -> Self::Value {
        let mut map = HashMap::new();
        for (k, v) in &self.values {
            map.insert(k.clone(), v.eval());
        }
        map
    }

    fn stabilize(&mut self, _: &Metadata) {}

    fn purge_stable_metadata(&mut self, metadata: &Metadata) {
        self.keys.purge_stable_metadata(metadata);
        self.values
            .iter_mut()
            .for_each(|(_, v)| v.purge_stable_metadata(metadata));
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
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
        protocol::po_log::POLog,
    };

    #[test_log::test]
    fn simple_aw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, POLog<Counter<i32>>>>();

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
    fn aw_map_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWMapLog<String, DuetLog<POLog<Counter<i32>>, POLog<Counter<i32>>>>>();

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
        let (mut tcsb_a, mut tcsb_b) =
            twins::<UWMapLog<String, DuetLog<POLog<Counter<i32>>, POLog<Counter<i32>>>>>();

        let event = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.try_deliver(event);

        let event_a = tcsb_a.tc_bcast(AWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        let event_b = tcsb_b.tc_bcast(AWMap::Remove("a".to_string()));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

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
}
