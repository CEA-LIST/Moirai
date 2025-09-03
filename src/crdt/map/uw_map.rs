use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;

use crate::protocol::clock::version_vector::Version;
use crate::protocol::event::Event;
use crate::protocol::state::log::IsLog;

#[derive(Clone, Debug)]
pub enum UWMap<K, O> {
    Update(K, O),
    Remove(K),
    Clear,
}

#[derive(Clone, Debug)]
pub struct UWMapLog<K, L>
where
    K: Clone + Debug + Eq + Hash,
{
    children: HashMap<K, L>,
}

impl<K: Clone + Debug + Eq + Hash, L> Default for UWMapLog<K, L> {
    fn default() -> Self {
        Self {
            children: Default::default(),
        }
    }
}

impl<K, L> IsLog for UWMapLog<K, L>
where
    L: IsLog,
    K: Clone + Debug + Ord + PartialOrd + Hash + Eq + Default + Display,
    <L as IsLog>::Value: Default + PartialEq,
{
    type Op = UWMap<K, L::Op>;
    type Value = HashMap<K, L::Value>;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            UWMap::Update(k, v) => {
                let child_op = Event::unfold(event, v);
                self.children.entry(k.clone()).or_default().effect(child_op);
            }
            UWMap::Remove(k) => {
                if let Some(child) = self.children.get_mut(&k) {
                    child.redundant_by_parent(event.version(), true);
                }
            }
            UWMap::Clear => {
                for child in self.children.values_mut() {
                    child.redundant_by_parent(event.version(), true);
                }
            }
        }
    }

    fn eval(&self) -> Self::Value {
        let mut map = Self::Value::default();
        for (k, v) in &self.children {
            let val = v.eval();
            if val != <L as IsLog>::Value::default() {
                map.insert(k.clone(), val);
            }
        }
        map
    }

    fn stabilize(&mut self, version: &Version) {
        for child in self.children.values_mut() {
            child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for child in self.children.values_mut() {
            child.redundant_by_parent(version, conservative);
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use crate::{
//         crdt::{
//             counter::resettable_counter::Counter,
//             map::uw_map::{UWMap, UWMapLog},
//             object::duet::{Duet, DuetLog},
//             test_util::{triplet, twins},
//         },
//         protocol::event_graph::EventGraph,
//     };

//     #[test_log::test]
//     fn simple_uw_map() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, EventGraph<Counter<i32>>>>();

//         let event = tcsb_a.tc_bcast(UWMap::Update("a".to_string(), Counter::Dec(5)));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Update("b".to_string(), Counter::Inc(5)));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Update("a".to_string(), Counter::Inc(15)));
//         tcsb_b.try_deliver(event);

//         let mut map = HashMap::new();
//         map.insert(String::from("a"), 10);
//         map.insert(String::from("b"), 5);
//         assert_eq!(map, tcsb_a.eval());
//         assert_eq!(map, tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn concurrent_uw_map() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<UWMapLog<String, EventGraph<Counter<i32>>>>();

//         let event_a = tcsb_a.tc_bcast(UWMap::Remove("a".to_string()));
//         let event_b = tcsb_b.tc_bcast(UWMap::Update("a".to_string(), Counter::Inc(10)));

//         tcsb_a.try_deliver(event_b);
//         tcsb_b.try_deliver(event_a);

//         let mut map = HashMap::new();
//         map.insert(String::from("a"), 10);
//         assert_eq!(map, tcsb_a.eval());
//         assert_eq!(map, tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn uw_map_duet_counter() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<
//             UWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
//         >();

//         let event = tcsb_a.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             Duet::First(Counter::Inc(15)),
//         ));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             Duet::First(Counter::Inc(10)),
//         ));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Update(
//             "b".to_string(),
//             Duet::Second(Counter::Dec(7)),
//         ));
//         tcsb_b.try_deliver(event);

//         let mut map = HashMap::new();
//         map.insert(String::from("a"), (25, 0));
//         map.insert(String::from("b"), (5, -7));
//         assert_eq!(map, tcsb_a.eval());
//         assert_eq!(map, tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn uw_map_concurrent_duet_counter() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<
//             UWMapLog<String, DuetLog<EventGraph<Counter<i32>>, EventGraph<Counter<i32>>>>,
//         >();

//         let event = tcsb_a.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             Duet::First(Counter::Inc(15)),
//         ));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
//         tcsb_b.try_deliver(event);

//         let mut map = HashMap::new();
//         map.insert(String::from("a"), (15, 0));
//         map.insert(String::from("b"), (5, 0));
//         assert_eq!(map, tcsb_a.eval());
//         assert_eq!(map, tcsb_b.eval());

//         let event_a = tcsb_a.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             Duet::First(Counter::Inc(10)),
//         ));
//         let event_b = tcsb_b.tc_bcast(UWMap::Remove("a".to_string()));
//         tcsb_b.try_deliver(event_a);
//         tcsb_a.try_deliver(event_b);

//         let mut map = HashMap::new();
//         map.insert(String::from("a"), (10, 0));
//         map.insert(String::from("b"), (5, 0));
//         assert_eq!(map, tcsb_a.eval());
//         assert_eq!(map, tcsb_b.eval());

//         let event = tcsb_a.tc_bcast(UWMap::Update(
//             "b".to_string(),
//             Duet::Second(Counter::Dec(7)),
//         ));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(UWMap::Remove("b".to_string()));
//         tcsb_b.try_deliver(event);

//         let mut map = HashMap::new();
//         map.insert(String::from("a"), (10, 0));
//         assert_eq!(map, tcsb_a.eval());
//         assert_eq!(map, tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn uw_map_deeply_nested() {
//         let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<
//             UWMapLog<String, UWMapLog<i32, UWMapLog<String, EventGraph<Counter<i32>>>>>,
//         >();

//         let event_a_1 = tcsb_a.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(2))),
//         ));

//         let event_a_2 = tcsb_a.tc_bcast(UWMap::Update(
//             "b".to_string(),
//             UWMap::Update(2, UWMap::Update("f".to_string(), Counter::Dec(20))),
//         ));

//         let event_a_3 = tcsb_a.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(8))),
//         ));

//         let event_b_1 = tcsb_b.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             UWMap::Update(1, UWMap::Update("z".to_string(), Counter::Inc(8))),
//         ));

//         let event_b_2 = tcsb_b.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             UWMap::Update(2, UWMap::Remove("f".to_string())),
//         ));

//         let event_c_1 = tcsb_c.tc_bcast(UWMap::Update(
//             "a".to_string(),
//             UWMap::Update(1, UWMap::Remove("z".to_string())),
//         ));

//         tcsb_a.try_deliver(event_b_1.clone());
//         tcsb_a.try_deliver(event_b_2.clone());
//         tcsb_a.try_deliver(event_c_1.clone());

//         tcsb_b.try_deliver(event_a_1.clone());
//         tcsb_b.try_deliver(event_c_1.clone());
//         tcsb_b.try_deliver(event_a_2.clone());
//         tcsb_b.try_deliver(event_a_3.clone());

//         tcsb_c.try_deliver(event_b_2.clone());
//         tcsb_c.try_deliver(event_a_3.clone());
//         tcsb_c.try_deliver(event_b_1.clone());
//         tcsb_c.try_deliver(event_a_2.clone());
//         tcsb_c.try_deliver(event_a_1.clone());

//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//         assert_eq!(tcsb_c.eval(), tcsb_b.eval());
//     }

//     #[cfg(feature = "utils")]
//     #[test_log::test]
//     fn convergence_check() {
//         use crate::utils::convergence_checker::convergence_checker;

//         let mut result = HashMap::new();
//         result.insert("a".to_string(), 5);
//         result.insert("b".to_string(), -5);
//         convergence_checker::<UWMapLog<String, EventGraph<Counter<i32>>>>(
//             &[
//                 UWMap::Update("a".to_string(), Counter::Inc(5)),
//                 UWMap::Update("b".to_string(), Counter::Dec(5)),
//                 UWMap::Remove("a".to_string()),
//                 UWMap::Remove("b".to_string()),
//             ],
//             result,
//             HashMap::eq,
//         );
//     }

//     #[cfg(feature = "op_weaver")]
//     #[test_log::test]
//     fn op_weaver_deeply_nested_map() {
//         use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

//         let ops = vec![
//             UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Inc(2))),
//             UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Dec(3))),
//             UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Reset)),
//             UWMap::Update("a".to_string(), UWMap::Remove(1)),
//             UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Inc(5))),
//             UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Dec(1))),
//             UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Reset)),
//             UWMap::Update("b".to_string(), UWMap::Remove(2)),
//             UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Inc(10))),
//             UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Dec(2))),
//             UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Reset)),
//             UWMap::Update("c".to_string(), UWMap::Remove(3)),
//             UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Inc(7))),
//             UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Dec(4))),
//             UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Reset)),
//             UWMap::Update("d".to_string(), UWMap::Remove(4)),
//             UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Inc(3))),
//             UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Dec(1))),
//             UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Reset)),
//             UWMap::Update("e".to_string(), UWMap::Remove(5)),
//             UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Inc(2))),
//             UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Dec(2))),
//             UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Reset)),
//             UWMap::Update("a".to_string(), UWMap::Remove(6)),
//         ];

//         type MapValue = HashMap<String, HashMap<i32, i32>>;

//         let config = EventGraphConfig {
//             name: "uw_map_2_nested",
//             num_replicas: 8,
//             num_operations: 10_000,
//             operations: &ops,
//             final_sync: true,
//             churn_rate: 0.3,
//             reachability: None,
//             compare: |a: &MapValue, b: &MapValue| a == b,
//             record_results: true,
//             seed: None,
//             witness_graph: false,
//             concurrency_score: false,
//         };

//         op_weaver::<UWMapLog<String, UWMapLog<i32, EventGraph<Counter<i32>>>>>(config);
//     }
// }
