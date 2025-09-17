use serde_json::Value;

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
        event::{id::EventId, tag::Tag, Event},
        state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
    },
};

pub type Document = NestedListLog<JsonLogContainer>;

// impl Vec<Value> {
//     pub fn to_json_value(&self) -> Value {}
// }

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
#[derive(Debug, Default, Clone)]
pub enum JsonLog {
    #[default]
    Null,
    Bool(VecLog<EWFlag>),
    Number(VecLog<Counter<f64>>),
    Object(UWMapLog<String, JsonLogContainer>),
    String(EventGraph<List<char>>),
    Array(NestedListLog<JsonLogContainer>),
}

#[derive(Debug, Default, Clone)]
pub struct JsonLogContainer {
    value: Option<JsonLog>,
    first_event: Option<Event<Json>>,
}

impl IsLog for JsonLogContainer {
    type Op = Json;
    type Value = Value;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            Json::Bool(op) => match &mut self.value {
                Some(JsonLog::Bool(log)) => {
                    let child_op = Event::unfold(event.clone(), op);
                    log.effect(child_op);
                }
                Some(_) => {
                    let mut list_log = NestedListLog::<JsonLogContainer>::new();
                    let child_op = Event::unfold(event.clone(), op);
                    let mut flag_log = VecLog::<EWFlag>::new();
                    flag_log.effect(child_op);
                    list_log.incorporate(
                        self.first_event.clone().unwrap(),
                        JsonLogContainer {
                            value: self.value.clone(),
                            first_event: None,
                        },
                    );
                    list_log.incorporate(
                        event,
                        JsonLogContainer {
                            value: Some(JsonLog::Bool(flag_log)),
                            first_event: None,
                        },
                    );
                    self.value = Some(JsonLog::Array(list_log));
                    self.first_event = None;
                }
                None => {
                    let mut log = VecLog::<EWFlag>::new();
                    let child_op = Event::unfold(event.clone(), op);
                    log.effect(child_op);
                    self.value = Some(JsonLog::Bool(log));
                    self.first_event = Some(event);
                }
            },
            Json::Number(op) => match &mut self.value {
                Some(JsonLog::Number(log)) => {
                    let child_op = Event::unfold(event.clone(), op);
                    log.effect(child_op);
                }
                Some(_) => {
                    let mut list_log = NestedListLog::<JsonLogContainer>::new();
                    let child_op = Event::unfold(event.clone(), op);
                    let mut counter_log = VecLog::<Counter<f64>>::new();
                    counter_log.effect(child_op);
                    list_log.incorporate(
                        self.first_event.clone().unwrap(),
                        JsonLogContainer {
                            value: self.value.clone(),
                            first_event: None,
                        },
                    );
                    list_log.incorporate(
                        event,
                        JsonLogContainer {
                            value: Some(JsonLog::Number(counter_log)),
                            first_event: None,
                        },
                    );
                    self.value = Some(JsonLog::Array(list_log));
                    self.first_event = None;
                }
                None => {
                    let mut log = VecLog::<Counter<f64>>::new();
                    let child_op = Event::unfold(event.clone(), op);
                    log.effect(child_op);
                    self.value = Some(JsonLog::Number(log));
                    self.first_event = Some(event);
                }
            },
            Json::Object(uwmap) => {
                let op: UWMap<String, Json> = match uwmap {
                    UWMap::Update(k, v) => UWMap::Update(k, *v),
                    UWMap::Remove(k) => UWMap::Remove(k),
                    UWMap::Clear => UWMap::Clear,
                };
                match &mut self.value {
                    Some(JsonLog::Object(log)) => {
                        let child_op = Event::unfold(event.clone(), op);
                        log.effect(child_op);
                    }
                    Some(test) => {
                        println!("{:?}", test);
                        let mut list_log = NestedListLog::<JsonLogContainer>::new();
                        let child_op = Event::unfold(event.clone(), op);
                        let mut map_log = UWMapLog::<String, JsonLogContainer>::new();
                        map_log.effect(child_op);
                        list_log.incorporate(
                            self.first_event.clone().unwrap(),
                            JsonLogContainer {
                                value: self.value.clone(),
                                first_event: None,
                            },
                        );
                        list_log.incorporate(
                            event,
                            JsonLogContainer {
                                value: Some(JsonLog::Object(map_log)),
                                first_event: None,
                            },
                        );
                        self.value = Some(JsonLog::Array(list_log));
                        self.first_event = None;
                    }
                    None => {
                        let mut log = UWMapLog::<String, JsonLogContainer>::new();
                        let child_op = Event::unfold(event.clone(), op);
                        log.effect(child_op);
                        self.value = Some(JsonLog::Object(log));
                        self.first_event = Some(event);
                    }
                }
            }
            Json::Array(op) => {
                let op = match op {
                    NestedList::Insert { pos, value } => NestedList::Insert { pos, value: *value },
                    NestedList::Set { pos, value } => NestedList::Set { pos, value: *value },
                    NestedList::Delete { pos } => NestedList::Delete { pos },
                };
                match &mut self.value {
                    Some(JsonLog::Array(log)) => {
                        let child_op = Event::unfold(event.clone(), op);
                        log.effect(child_op);
                    }
                    Some(_) => {
                        let mut list_log = NestedListLog::<JsonLogContainer>::new();
                        let child_op = Event::unfold(event.clone(), op);
                        let mut array_log = NestedListLog::<JsonLogContainer>::new();
                        array_log.effect(child_op);
                        list_log.incorporate(
                            self.first_event.clone().unwrap(),
                            JsonLogContainer {
                                value: self.value.clone(),
                                first_event: None,
                            },
                        );
                        list_log.incorporate(
                            event,
                            JsonLogContainer {
                                value: Some(JsonLog::Array(array_log)),
                                first_event: None,
                            },
                        );
                        self.value = Some(JsonLog::Array(list_log));
                        self.first_event = None;
                    }
                    None => {
                        let mut log = NestedListLog::<JsonLogContainer>::new();
                        let child_op = Event::unfold(event.clone(), op);
                        log.effect(child_op);
                        self.value = Some(JsonLog::Array(log));
                        self.first_event = Some(event);
                    }
                }
            }
            Json::String(op) => match &mut self.value {
                Some(JsonLog::String(log)) => {
                    let child_op = Event::unfold(event.clone(), op);
                    log.effect(child_op);
                }
                Some(_) => {
                    let mut list_log = NestedListLog::<JsonLogContainer>::new();
                    let child_op = Event::unfold(event.clone(), op);
                    let mut string_log = EventGraph::<List<char>>::new();
                    string_log.effect(child_op);
                    list_log.incorporate(
                        self.first_event.clone().unwrap(),
                        JsonLogContainer {
                            value: self.value.clone(),
                            first_event: None,
                        },
                    );
                    list_log.incorporate(
                        event,
                        JsonLogContainer {
                            value: Some(JsonLog::String(string_log)),
                            first_event: None,
                        },
                    );
                    self.value = Some(JsonLog::Array(list_log));
                    self.first_event = None;
                }
                None => {
                    let mut log = EventGraph::<List<char>>::new();
                    let child_op = Event::unfold(event.clone(), op);
                    log.effect(child_op);
                    self.value = Some(JsonLog::String(log));
                    self.first_event = Some(event);
                }
            },
            Json::Null => match &mut self.value {
                Some(_) => {
                    let mut list_log = NestedListLog::<JsonLogContainer>::new();
                    list_log.incorporate(
                        self.first_event.clone().unwrap(),
                        JsonLogContainer {
                            value: self.value.clone(),
                            first_event: None,
                        },
                    );
                    list_log.incorporate(
                        event,
                        JsonLogContainer {
                            value: Some(JsonLog::Null),
                            first_event: None,
                        },
                    );
                    self.value = Some(JsonLog::Array(list_log));
                    self.first_event = None;
                }
                None => {
                    self.value = Some(JsonLog::Null);
                    self.first_event = Some(event);
                }
            },
        }
    }

    fn eval(&self) -> Self::Value {
        match &self.value {
            Some(JsonLog::Bool(log)) => Value::Bool(log.eval()),
            Some(JsonLog::Number(log)) => {
                Value::Number(serde_json::Number::from_f64(log.eval()).unwrap())
            }
            Some(JsonLog::Object(log)) => {
                let evaluated = log.eval();
                let mut map = serde_json::Map::new();
                for (k, v) in evaluated {
                    map.insert(k, v);
                }
                Value::Object(map)
            }
            Some(JsonLog::String(log)) => {
                let chars: String = log.eval().into_iter().collect();
                Value::String(chars)
            }
            Some(JsonLog::Array(log)) => Value::Array(log.eval()),
            None | Some(JsonLog::Null) => Value::Null,
        }
    }

    fn stabilize(&mut self, _version: &Version) {}

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match &mut self.value {
            Some(JsonLog::Bool(log)) => log.redundant_by_parent(version, conservative),
            Some(JsonLog::Number(log)) => log.redundant_by_parent(version, conservative),
            Some(JsonLog::Object(log)) => log.redundant_by_parent(version, conservative),
            Some(JsonLog::String(log)) => log.redundant_by_parent(version, conservative),
            Some(JsonLog::Array(log)) => log.redundant_by_parent(version, conservative),
            None | Some(JsonLog::Null) => {}
        }
    }

    fn len(&self) -> usize {
        // TODO
        0
    }

    fn is_empty(&self) -> bool {
        Self::len(self) == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crdt::{counter::resettable_counter::Counter, test_util::twins_log},
        protocol::replica::IsReplica,
    };

    #[test]
    fn concurrent_root() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogContainer>();
        let event_a = replica_a.send(Json::Bool(EWFlag::Enable));
        let event_b = replica_b.send(Json::Object(UWMap::Update(
            "obj".to_string(),
            Box::new(Json::Number(Counter::Inc(5.0))),
        )));
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        println!("{}", replica_a.query());
        println!("{}", replica_b.query());
    }

    #[test]
    fn simple_object() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogContainer>();
        let event_a = replica_a.send(Json::Object(UWMap::Update(
            "obj".to_string(),
            Box::new(Json::Number(Counter::Inc(5.0))),
        )));
        replica_b.receive(event_a);
        let event_b = replica_b.send(Json::Object(UWMap::Update(
            "patate".to_string(),
            Box::new(Json::String(List::Insert {
                content: 'o',
                pos: 0,
            })),
        )));
        replica_a.receive(event_b);

        println!("{}", replica_a.query());
        println!("{}", replica_b.query());
    }

    #[test]
    fn nested_conflict() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLogContainer>();
        let event_a = replica_a.send(Json::Object(UWMap::Update(
            "obj".to_string(),
            Box::new(Json::Number(Counter::Inc(5.0))),
        )));
        replica_b.receive(event_a);
        let event_b = replica_b.send(Json::Object(UWMap::Update(
            "obj".to_string(),
            Box::new(Json::String(List::Insert {
                content: 'o',
                pos: 0,
            })),
        )));
        let event_a = replica_a.send(Json::Bool(EWFlag::Enable));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        println!("{}", replica_a.query());
        println!("{}", replica_b.query());
    }

    #[test]
    fn empty_root() {
        let (replica_a, replica_b) = twins_log::<JsonLogContainer>();

        assert_eq!(Value::Null, replica_a.query());
        assert_eq!(Value::Null, replica_b.query());
    }
}
