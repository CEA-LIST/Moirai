use serde_json::Value;

#[cfg(feature = "fuzz")]
use crate::{
    crdt::{
        counter::resettable_counter::Counter,
        flag::ew_flag::EWFlag,
        list::{
            eg_walker::List,
            nested_list::{List as NestedList, ListLog as NestedListLog},
        },
        map::uw_map::{UWMap, UWMapLog},
    },
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::EvalNested,
            query::{QueryOperation, Read},
        },
        event::Event,
        state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
    },
};

/// Operations that can be performed on the JSON CRDT.
#[derive(Debug, Clone, Default)]
pub enum Json {
    #[default]
    Null,
    Bool(EWFlag),
    Number(Counter<f64>),
    Object(UWMap<String, Box<Json>>),
    String(List<char>),
    Array(NestedList<Box<Json>>),
}

/// Different types of logs for each JSON value type.
#[derive(Debug, Clone)]
pub enum JsonLog {
    Null,
    Bool(VecLog<EWFlag>),
    Number(VecLog<Counter<f64>>),
    Object(UWMapLog<String, JsonLogTree>),
    String(EventGraph<List<char>>),
    Array(NestedListLog<JsonLogTree>),
}

#[derive(Debug, Default, Clone)]
pub struct JsonLogTree {
    node: Option<JsonLogNode>,
}

impl JsonLogTree {
    pub fn new(value: JsonLog, first_event: Option<Event<Json>>) -> Self {
        Self {
            node: Some(JsonLogNode::new(value, first_event)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsonLogNode {
    value: JsonLog,
    /// If a conflict occurs, the first event will be used to transform the current value into a list.
    first_event: Option<Event<Json>>,
}

impl JsonLogNode {
    pub fn new(value: JsonLog, first_event: Option<Event<Json>>) -> Self {
        Self { value, first_event }
    }
}

impl IsLog for JsonLogTree {
    type Op = Json;
    type Value = Value;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match &mut self.node {
            Some(node) => match event.op().clone() {
                Json::Null => match &mut node.value {
                    JsonLog::Null => {}
                    _ => {
                        assert!(!node
                            .first_event
                            .unwrap()
                            .id()
                            .is_predecessor_of(event.version()));
                        let mut list_log = NestedListLog::<JsonLogTree>::new();
                        list_log.incorporate(
                            node.first_event.clone(),
                            JsonLogTree::new(node.value.clone(), None),
                        );
                        list_log.incorporate(event.clone(), JsonLogTree::new(JsonLog::Null, None));
                        self.node = Some(JsonLogNode::new(
                            JsonLog::Array(list_log),
                            node.first_event.clone(),
                        ));
                    }
                },
                Json::Bool(ewflag) => match &mut node.value {
                    JsonLog::Bool(log) => {
                        let child_op = Event::unfold(event, ewflag);
                        log.effect(child_op);
                    }
                    _ => {
                        println!("Conflict on Bool");
                        let mut list_log = NestedListLog::<JsonLogTree>::new();
                        let child_op = Event::unfold(event.clone(), ewflag);
                        let mut flag_log = VecLog::<EWFlag>::new();
                        flag_log.effect(child_op);
                        list_log.incorporate(
                            node.first_event.clone(),
                            JsonLogTree::new(node.value.clone(), node.first_event.clone()),
                        );
                        list_log.incorporate(
                            event.clone(),
                            JsonLogTree::new(JsonLog::Bool(flag_log), event),
                        );
                        self.node = Some(JsonLogNode::new(
                            JsonLog::Array(list_log),
                            node.first_event.clone(),
                        ));
                    }
                },
                Json::Number(counter) => match &mut node.value {
                    JsonLog::Number(log) => {
                        let child_op = Event::unfold(event.clone(), counter);
                        log.effect(child_op);
                    }
                    _ => {
                        let mut list_log = NestedListLog::<JsonLogTree>::new();
                        let child_op = Event::unfold(event.clone(), counter);
                        let mut counter_log = VecLog::<Counter<f64>>::new();
                        counter_log.effect(child_op);
                        list_log.incorporate(
                            node.first_event.clone(),
                            JsonLogTree::new(node.value.clone(), node.first_event.clone()),
                        );
                        list_log.incorporate(
                            event.clone(),
                            JsonLogTree::new(JsonLog::Number(counter_log), event),
                        );
                    }
                },
                Json::Object(uwmap) => match &mut node.value {
                    JsonLog::Object(log) => {
                        let child_op = Event::unfold(event.clone(), uwmap.unboxed());
                        log.effect(child_op);
                    }
                    _ => {
                        let mut list_log = NestedListLog::<JsonLogTree>::new();
                        let child_op = Event::unfold(event.clone(), uwmap.unboxed());
                        let mut map_log = UWMapLog::<String, JsonLogTree>::new();
                        map_log.effect(child_op);
                        list_log.incorporate(
                            node.first_event.clone(),
                            JsonLogTree::new(node.value.clone(), node.first_event.clone()),
                        );
                        list_log.incorporate(
                            event.clone(),
                            JsonLogTree::new(JsonLog::Object(map_log), event),
                        );
                    }
                },
                Json::String(list) => match &mut node.value {
                    JsonLog::String(log) => {
                        let child_op = Event::unfold(event.clone(), list);
                        log.effect(child_op);
                    }
                    _ => {
                        let mut list_log = NestedListLog::<JsonLogTree>::new();
                        let child_op = Event::unfold(event.clone(), list);
                        let mut string_log = EventGraph::<List<char>>::new();
                        string_log.effect(child_op);
                        list_log.incorporate(
                            node.first_event.clone(),
                            JsonLogTree::new(node.value.clone(), node.first_event.clone()),
                        );
                        list_log.incorporate(
                            event.clone(),
                            JsonLogTree::new(JsonLog::String(string_log), event),
                        );
                    }
                },
                Json::Array(list) => match &mut node.value {
                    JsonLog::Array(log) => {
                        let child_op = Event::unfold(event.clone(), list.unboxed());
                        log.effect(child_op);
                    }
                    _ => {
                        let mut list_log = NestedListLog::<JsonLogTree>::new();
                        let child_op = Event::unfold(event.clone(), list.unboxed());
                        let mut array_log = NestedListLog::<JsonLogTree>::new();
                        array_log.effect(child_op);
                        list_log.incorporate(
                            node.first_event.clone(),
                            JsonLogTree::new(node.value.clone(), node.first_event.clone()),
                        );
                        list_log.incorporate(
                            event.clone(),
                            JsonLogTree::new(JsonLog::Array(array_log), event),
                        );
                    }
                },
            },
            None => match event.op().clone() {
                Json::Null => {
                    self.node = Some(JsonLogNode::new(JsonLog::Null, event));
                }
                Json::Bool(ewflag) => {
                    let mut log = VecLog::<EWFlag>::new();
                    let child_op = Event::unfold(event.clone(), ewflag);
                    log.effect(child_op);
                    self.node = Some(JsonLogNode::new(JsonLog::Bool(log), event));
                }
                Json::Number(counter) => {
                    let mut log = VecLog::<Counter<f64>>::new();
                    let child_op = Event::unfold(event.clone(), counter);
                    log.effect(child_op);
                    self.node = Some(JsonLogNode::new(JsonLog::Number(log), event));
                }
                Json::Object(uwmap) => {
                    let mut log = UWMapLog::<String, JsonLogTree>::new();
                    let child_op = Event::unfold(event.clone(), uwmap.unboxed());
                    log.effect(child_op);
                    self.node = Some(JsonLogNode::new(JsonLog::Object(log), event));
                }
                Json::String(list) => {
                    let mut log = EventGraph::<List<char>>::new();
                    let child_op = Event::unfold(event.clone(), list);
                    log.effect(child_op);
                    self.node = Some(JsonLogNode::new(JsonLog::String(log), event));
                }
                Json::Array(list) => {
                    let mut log = NestedListLog::<JsonLogTree>::new();
                    let child_op = Event::unfold(event.clone(), list.unboxed());
                    log.effect(child_op);
                    self.node = Some(JsonLogNode::new(JsonLog::Array(log), event));
                }
            },
        }

        // match event.op().clone() {
        //     Json::Bool(op) => match &mut self.value {
        //         JsonLog::Bool(log) => {
        //             let child_op = Event::unfold(event.clone(), op);
        //             log.effect(child_op);
        //         }
        //         _ => {
        //             println!("Conflict on Bool: {:?}", self);

        //             let mut list_log = NestedListLog::<JsonLogNode>::new();
        //             let child_op = Event::unfold(event.clone(), op);
        //             let mut flag_log = VecLog::<EWFlag>::new();
        //             flag_log.effect(child_op);
        //             list_log.incorporate(
        //                 self.first_event.clone().unwrap(),
        //                 JsonLogNode {
        //                     value: self.value.clone(),
        //                     first_event: None,
        //                 },
        //             );
        //             list_log.incorporate(
        //                 event,
        //                 JsonLogNode {
        //                     value: JsonLog::Bool(flag_log),
        //                     first_event: None,
        //                 },
        //             );
        //             self.value = JsonLog::Array(list_log);
        //         }
        //     },
        //     Json::Number(op) => match &mut self.value {
        //         JsonLog::Number(log) => {
        //             let child_op = Event::unfold(event.clone(), op);
        //             log.effect(child_op);
        //         }
        //         _ => {
        //             println!("Conflict on Number: {:?}", self);

        //             let mut list_log = NestedListLog::<JsonLogNode>::new();
        //             let child_op = Event::unfold(event.clone(), op);
        //             let mut counter_log = VecLog::<Counter<f64>>::new();
        //             counter_log.effect(child_op);
        //             list_log.incorporate(
        //                 self.first_event.clone().unwrap(),
        //                 JsonLogNode {
        //                     value: self.value.clone(),
        //                     first_event: None,
        //                 },
        //             );
        //             list_log.incorporate(
        //                 event,
        //                 JsonLogNode {
        //                     value: JsonLog::Number(counter_log),
        //                     first_event: None,
        //                 },
        //             );
        //             self.value = JsonLog::Array(list_log);
        //         } // None => {
        //           //     panic!("NONE NONE NONE ON NUMBER");
        //           //     let mut log = VecLog::<Counter<f64>>::new();
        //           //     let child_op = Event::unfold(event.clone(), op);
        //           //     log.effect(child_op);
        //           //     self.value = JsonLog::Number(log));
        //           //     self.first_event = Some(event);
        //           // }
        //     },
        //     Json::Object(op) => match &mut self.value {
        //         JsonLog::Object(log) => {
        //             let child_op = Event::unfold(event.clone(), op.unboxed());
        //             log.effect(child_op);
        //         }
        //         _ => {
        //             println!("Conflict on Object: {:?}", self);

        //             let mut list_log = NestedListLog::<JsonLogNode>::new();
        //             let child_op = Event::unfold(event.clone(), op.unboxed());
        //             let mut map_log = UWMapLog::<String, JsonLogNode>::new();
        //             map_log.effect(child_op);
        //             list_log.incorporate(
        //                 self.first_event.clone().unwrap(),
        //                 JsonLogNode {
        //                     value: self.value.clone(),
        //                     first_event: None,
        //                 },
        //             );
        //             list_log.incorporate(
        //                 event,
        //                 JsonLogNode {
        //                     value: JsonLog::Object(map_log),
        //                     first_event: None,
        //                 },
        //             );
        //             self.value = JsonLog::Array(list_log);
        //         } // None => {
        //           //     panic!("NONE NONE NONE ON OBJECT");
        //           //     let mut log = UWMapLog::<String, JsonLogNode>::new();
        //           //     let child_op = Event::unfold(event.clone(), op.unboxed());
        //           //     log.effect(child_op);
        //           //     self.value = JsonLog::Object(log);
        //           //     self.first_event = Some(event);
        //           // }
        //     },
        //     Json::Array(op) => match &mut self.value {
        //         JsonLog::Array(log) => {
        //             let child_op = Event::unfold(event.clone(), op.unboxed());
        //             log.effect(child_op);
        //         }
        //         _ => {
        //             println!("Conflict on Array: {:?}", self);

        //             let mut list_log = NestedListLog::<JsonLogNode>::new();
        //             let child_op = Event::unfold(event.clone(), op.unboxed());
        //             let mut array_log = NestedListLog::<JsonLogNode>::new();
        //             array_log.effect(child_op);
        //             list_log.incorporate(
        //                 self.first_event.clone().unwrap(),
        //                 JsonLogNode {
        //                     value: self.value.clone(),
        //                     first_event: None,
        //                 },
        //             );
        //             list_log.incorporate(
        //                 event,
        //                 JsonLogNode {
        //                     value: JsonLog::Array(array_log),
        //                     first_event: None,
        //                 },
        //             );
        //             self.value = JsonLog::Array(list_log);
        //         } // None => {
        //           //     panic!("NONE NONE NONE ON ARRAY");
        //           //     let mut log = NestedListLog::<JsonLogNode>::new();
        //           //     let child_op = Event::unfold(event.clone(), op.unboxed());
        //           //     log.effect(child_op);
        //           //     self.value = JsonLog::Array(log));
        //           //     self.first_event = Some(event);
        //           // }
        //     },
        //     Json::String(op) => match &mut self.value {
        //         JsonLog::String(log) => {
        //             let child_op = Event::unfold(event.clone(), op);
        //             log.effect(child_op);
        //         }
        //         _ => {
        //             println!("Conflict on String: {:?}", self);

        //             let mut list_log = NestedListLog::<JsonLogNode>::new();
        //             let child_op = Event::unfold(event.clone(), op);
        //             let mut string_log = EventGraph::<List<char>>::new();
        //             string_log.effect(child_op);
        //             list_log.incorporate(
        //                 self.first_event.clone().unwrap(),
        //                 JsonLogNode {
        //                     value: self.value.clone(),
        //                     first_event: None,
        //                 },
        //             );
        //             list_log.incorporate(
        //                 event,
        //                 JsonLogNode {
        //                     value: JsonLog::String(string_log),
        //                     first_event: None,
        //                 },
        //             );
        //             self.value = JsonLog::Array(list_log);
        //         } // None => {
        //           //     panic!("NONE NONE NONE ON STRING");
        //           //     let mut log = EventGraph::<List<char>>::new();
        //           //     let child_op = Event::unfold(event.clone(), op);
        //           //     log.effect(child_op);
        //           //     self.value = JsonLog::String(log));
        //           //     self.first_event = Some(event);
        //           // }
        //     },
        //     Json::Null => match &mut self.value {
        //         _ => {
        //             println!("Conflict on Null: {:?}", self);

        //             let mut list_log = NestedListLog::<JsonLogNode>::new();
        //             list_log.incorporate(
        //                 self.first_event.clone().unwrap(),
        //                 JsonLogNode {
        //                     value: self.value.clone(),
        //                     first_event: None,
        //                 },
        //             );
        //             list_log.incorporate(
        //                 event,
        //                 JsonLogNode {
        //                     value: JsonLog::Null,
        //                     first_event: None,
        //                 },
        //             );
        //             self.value = JsonLog::Array(list_log);
        //         } // None => {
        //           //     panic!("NONE NONE NONE ON NULL");
        //           //     self.value = JsonLog::Null;
        //           //     self.first_event = Some(event);
        //           // }
        //     },
        // }
    }

    fn stabilize(&mut self, _version: &Version) {}

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match &mut self.node {
            Some(node) => match &mut node.value {
                JsonLog::Bool(log) => log.redundant_by_parent(version, conservative),
                JsonLog::Number(log) => log.redundant_by_parent(version, conservative),
                JsonLog::Object(log) => log.redundant_by_parent(version, conservative),
                JsonLog::String(log) => log.redundant_by_parent(version, conservative),
                JsonLog::Array(log) => log.redundant_by_parent(version, conservative),
                JsonLog::Null => {}
            },
            None => {}
        }
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        match &self.node {
            Some(node) => match (op, &node.value) {
                (Json::Null, JsonLog::Null) => true,
                (Json::Bool(flag), JsonLog::Bool(log)) => log.is_enabled(flag),
                (Json::Number(counter), JsonLog::Number(log)) => log.is_enabled(counter),
                (Json::Object(uwmap), JsonLog::Object(log)) => {
                    log.is_enabled(&uwmap.clone().unboxed())
                }
                (Json::String(list), JsonLog::String(log)) => log.is_enabled(list),
                (Json::Array(list), JsonLog::Array(log)) => log.is_enabled(&list.clone().unboxed()),
                _ => false,
            },
            None => true,
        }
    }

    fn is_default(&self) -> bool {
        match &self.node {
            Some(node) => match &node.value {
                JsonLog::Bool(log) => log.is_default(),
                JsonLog::Number(log) => log.is_default(),
                JsonLog::Object(log) => log.is_default(),
                JsonLog::String(log) => log.is_default(),
                JsonLog::Array(log) => log.is_default(),
                JsonLog::Null => false,
            },
            None => true,
        }
    }
}

impl EvalNested<Read<<Self as IsLog>::Value>> for JsonLogTree {
    fn execute_query(
        &self,
        _q: Read<Self::Value>,
    ) -> <Read<Self::Value> as QueryOperation>::Response {
        match &self.node {
            Some(node) => match &node.value {
                JsonLog::Bool(log) => Value::Bool(log.execute_query(Read::new())),
                JsonLog::Number(log) => Value::Number(
                    serde_json::Number::from_f64(log.execute_query(Read::new()) as f64).unwrap(),
                ),
                JsonLog::Object(log) => {
                    let evaluated = log.execute_query(Read::new());
                    let mut map = serde_json::Map::new();
                    for (k, v) in evaluated {
                        map.insert(k, v);
                    }
                    Value::Object(map)
                }
                JsonLog::String(log) => {
                    let chars: String = log.execute_query(Read::new()).into_iter().collect();
                    Value::String(chars)
                }
                JsonLog::Array(log) => Value::Array(log.execute_query(Read::new())),
                JsonLog::Null => Value::Null,
            },
            None => Value::Null,
        }
    }
}

// #[cfg(feature = "fuzz")]
// impl OpGeneratorNested for JsonLogNode {
//     fn generate(&self, rng: &mut impl rand::RngCore) -> Self::Op {
//         use rand::{seq::IteratorRandom, Rng};

//         let current_value = self.eval(Read::new());

//         fn generate_leaf_op(rng: &mut impl rand::RngCore, value: &Value) -> Json {
//             use crate::{fuzz::config::OpGenerator, protocol::crdt::pure_crdt::PureCRDT};

//             match value {
//                 Value::Null => Json::Null,
//                 Value::Bool(_) => Json::Bool(<EWFlag as OpGenerator>::generate(
//                     rng,
//                     &<EWFlag as OpGenerator>::Config::default(),
//                     &<EWFlag as PureCRDT>::StableState::default(),
//                     &Vec::new(),
//                 )),
//                 Value::Number(_) => Json::Number(<Counter<f64> as OpGenerator>::generate(
//                     rng,
//                     &<Counter<f64> as OpGenerator>::Config::default(),
//                     &<Counter<f64> as PureCRDT>::StableState::default(),
//                     &Vec::new(),
//                 )),
//                 Value::String(s) => {}
//                 Value::Array(arr) => {
//                     if arr.is_empty() || rng.random_bool(0.7) {
//                         // Insert
//                         let pos = rng.random_range(0..=arr.len());
//                         Json::Array(NestedList::Insert {
//                             pos,
//                             value: Box::new(Json::Null),
//                         })
//                     } else {
//                         // Delete ou Set
//                         let pos = rng.random_range(0..arr.len());
//                         if rng.random_bool(0.5) {
//                             Json::Array(NestedList::Delete { pos })
//                         } else {
//                             Json::Array(NestedList::Update {
//                                 pos,
//                                 value: Box::new(Json::Null),
//                             })
//                         }
//                     }
//                 }
//                 Value::Object(map) => {
//                     if map.is_empty() || rng.random_bool(0.7) {
//                         // Update/Insert
//                         let key = if map.is_empty() || rng.random_bool(0.5) {
//                             format!("key{}", rng.random_range(0..10))
//                         } else {
//                             map.keys().choose(rng).unwrap().clone()
//                         };
//                         Json::Object(UWMap::Update(key, Box::new(Json::Null)))
//                     } else {
//                         // Remove
//                         let key = map.keys().choose(rng).unwrap().clone();
//                         Json::Object(UWMap::Remove(key))
//                     }
//                 }
//             }
//         }

//         // 2. Parcourt le JSON et décide où faire l'opération
//         fn traverse_and_generate(
//             rng: &mut impl rand::RngCore,
//             value: &Value,
//             log: &impl IsLog,
//             stop_probability: f64,
//         ) -> Json {
//             match value {
//                 // Feuilles : toujours faire une opération
//                 Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
//                     generate_leaf_op(rng, value)
//                 }

//                 // Nœuds : décider si on s'arrête ou si on continue
//                 Value::Array(arr) => {
//                     // Probabilité de s'arrêter et faire une opération sur l'array lui-même
//                     if arr.is_empty() || rng.random_bool(stop_probability) {
//                         generate_leaf_op(rng, value)
//                     } else {
//                         // Continuer la traversée : choisir un index et descendre
//                         let index = rng.random_range(0..arr.len());
//                         let nested_op = traverse_and_generate(
//                             rng,
//                             &arr[index],
//                             stop_probability * 0.8, // Diminuer la probabilité en descendant
//                         );
//                         Json::Array(NestedList::Update {
//                             pos: index,
//                             value: Box::new(nested_op),
//                         })
//                     }
//                 }

//                 Value::Object(map) => {
//                     // Probabilité de s'arrêter et faire une opération sur l'object lui-même
//                     if map.is_empty() || rng.random_bool(stop_probability) {
//                         generate_leaf_op(rng, value)
//                     } else {
//                         // Continuer la traversée : choisir une clé et descendre
//                         let key = map.keys().choose(rng).unwrap().clone();
//                         let nested_op = traverse_and_generate(
//                             rng,
//                             &map[&key],
//                             stop_probability * 0.8, // Diminuer la probabilité en descendant
//                         );
//                         Json::Object(UWMap::Update(key, Box::new(nested_op)))
//                     }
//                 }
//             }
//         }

//         // Commencer la traversée avec une probabilité initiale de s'arrêter
//         traverse_and_generate(rng, &current_value, 0.3, self.)
//     }
// }

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        crdt::{counter::resettable_counter::Counter, test_util::twins_log},
        protocol::replica::IsReplica,
    };

    #[test]
    fn concurrent_root() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogTree>();
        let event_a = replica_a.send(Json::Bool(EWFlag::Enable)).unwrap();
        let event_b = replica_b
            .send(Json::Object(UWMap::Update(
                "obj".to_string(),
                Box::new(Json::Number(Counter::Inc(5.0))),
            )))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        println!("Replica A: {:?}", replica_a.query(Read::new()));
        println!("Replica B: {:?}", replica_b.query(Read::new()));

        let result = json!([
            true,
            {
                "obj": 5.0
            }
        ]);
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    #[test]
    fn simple_object() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogTree>();
        let event_a = replica_a
            .send(Json::Object(UWMap::Update(
                "obj".to_string(),
                Box::new(Json::Number(Counter::Inc(5.0))),
            )))
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(Json::Object(UWMap::Update(
                "patate".to_string(),
                Box::new(Json::String(List::Insert {
                    content: 'o',
                    pos: 0,
                })),
            )))
            .unwrap();
        replica_a.receive(event_b);

        let result = json!({
            "obj": 5.0,
            "patate": "o"
        });
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    #[test]
    fn nested_conflict() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogTree>();
        let event_a = replica_a
            .send(Json::Object(UWMap::Update(
                "obj".to_string(),
                Box::new(Json::Number(Counter::Inc(5.0))),
            )))
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(Json::Object(UWMap::Update(
                "test".to_string(),
                Box::new(Json::String(List::Insert {
                    content: 'o',
                    pos: 0,
                })),
            )))
            .unwrap();
        let event_a = replica_a
            .send(Json::Object(UWMap::Update(
                "patate".to_string(),
                Box::new(Json::Bool(EWFlag::Enable)),
            )))
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = json!({
            "obj": 5.0,
            "test": "o",
            "patate": true
        });
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    #[test]
    fn nested_conflict_2() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogTree>();
        let event_a = replica_a
            .send(Json::Object(UWMap::Update(
                "obj".to_string(),
                Box::new(Json::Object(UWMap::Update(
                    "nested".to_string(),
                    Box::new(Json::Number(Counter::Inc(5.0))),
                ))),
            )))
            .unwrap();
        let event_b = replica_b
            .send(Json::Object(UWMap::Update(
                "obj".to_string(),
                Box::new(Json::Array(NestedList::Insert {
                    pos: 0,
                    value: Box::new(Json::Number(Counter::Inc(3.0))),
                })),
            )))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = json!({
            "obj": [
                {
                    "nested": 5.0
                },
                [3.0],
            ]
        });
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    #[test]
    fn empty_root() {
        let (replica_a, replica_b) = twins_log::<JsonLogTree>();

        let result = json!(null);
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    // #[cfg(feature = "fuzz")]
    // #[test]
    // fn fuzz_json() {
    //     use crate::fuzz::{
    //         config::{FuzzerConfig, RunConfig},
    //         fuzzer,
    //     };

    //     let run = RunConfig::new(
    //         0.4,
    //         2,
    //         4,
    //         None,
    //         Some([
    //             196, 141, 242, 5, 233, 202, 250, 93, 244, 51, 153, 66, 233, 235, 35, 232, 110, 134,
    //             158, 78, 41, 212, 67, 35, 161, 118, 154, 206, 252, 250, 225, 161,
    //         ]),
    //         true,
    //     );
    //     let runs = vec![run.clone(); 1];

    //     let config = FuzzerConfig::<JsonLogNode>::new("json", runs, true, |a, b| a == b, true);

    //     fuzzer::<JsonLogNode>(config);
    // }
}
