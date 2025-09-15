use std::collections::HashMap;

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
        event::Event,
        state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
    },
};

#[derive(Debug, Clone, Default)]
pub enum JsonOps {
    #[default]
    Null,
    Bool(EWFlag),
    Number(Counter<f64>),
    Object(UWMap<String, Box<JsonOps>>),
    String(List<char>),
    Array(NestedList<Box<JsonOps>>),
}

#[derive(Debug, Default)]
pub enum JsonLog {
    #[default]
    Null,
    Bool(VecLog<EWFlag>),
    Number(VecLog<Counter<f64>>),
    Object(UWMapLog<String, JsonLogContainer>),
    String(EventGraph<List<char>>),
    Array(NestedListLog<JsonLogContainer>),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum JsonValue {
    #[default]
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Object(HashMap<String, JsonValue>),
    Array(Vec<JsonValue>),
}

#[derive(Debug, Default)]
pub struct JsonLogContainer {
    value: JsonLog,
}

impl IsLog for JsonLogContainer {
    type Op = JsonOps;
    type Value = JsonValue;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            JsonOps::Bool(op) => {
                if let JsonLog::Bool(log) = &mut self.value {
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                } else {
                    let mut log = VecLog::<EWFlag>::new();
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                    self.value = JsonLog::Bool(log);
                }
            }
            JsonOps::Number(op) => {
                if let JsonLog::Number(log) = &mut self.value {
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                } else {
                    let mut log = VecLog::<Counter<f64>>::new();
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                    self.value = JsonLog::Number(log);
                }
            }
            JsonOps::Object(uwmap) => {
                let op: UWMap<String, JsonOps> = match uwmap {
                    UWMap::Update(k, v) => UWMap::Update(k, *v),
                    UWMap::Remove(k) => UWMap::Remove(k),
                    UWMap::Clear => UWMap::Clear,
                };
                if let JsonLog::Object(log) = &mut self.value {
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                } else {
                    let mut log = UWMapLog::<String, JsonLogContainer>::new();
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                    self.value = JsonLog::Object(log);
                }
            }
            JsonOps::Null => {
                self.value = JsonLog::Null;
            }
            JsonOps::String(list) => {
                if let JsonLog::String(log) = &mut self.value {
                    let child_op = Event::unfold(event, list);
                    log.effect(child_op);
                } else {
                    let mut log = EventGraph::<List<char>>::new();
                    let child_op = Event::unfold(event, list);
                    log.effect(child_op);
                    self.value = JsonLog::String(log);
                }
            }
            JsonOps::Array(list) => {
                let op = match list {
                    NestedList::Insert { pos, value } => NestedList::Insert { pos, value: *value },
                    NestedList::Set { pos, value } => NestedList::Set { pos, value: *value },
                    NestedList::Delete { pos } => NestedList::Delete { pos },
                };
                if let JsonLog::Array(log) = &mut self.value {
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                } else {
                    let mut log = NestedListLog::<JsonLogContainer>::new();
                    let child_op = Event::unfold(event, op);
                    log.effect(child_op);
                    self.value = JsonLog::Array(log);
                }
            }
        }
    }

    fn eval(&self) -> Self::Value {
        match &self.value {
            JsonLog::Bool(log) => JsonValue::Bool(log.eval()),
            JsonLog::Number(log) => JsonValue::Number(log.eval()),
            JsonLog::Null => JsonValue::Null,
            JsonLog::Object(log) => JsonValue::Object(log.eval()),
            JsonLog::String(log) => {
                let chars: String = log.eval().into_iter().collect();
                JsonValue::String(chars)
            }
            JsonLog::Array(log) => JsonValue::Array(log.eval()),
        }
    }

    fn stabilize(&mut self, version: &Version) {}

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match &mut self.value {
            JsonLog::Bool(log) => log.redundant_by_parent(version, conservative),
            JsonLog::Number(log) => log.redundant_by_parent(version, conservative),
            JsonLog::Object(log) => log.redundant_by_parent(version, conservative),
            JsonLog::String(log) => log.redundant_by_parent(version, conservative),
            JsonLog::Null => {}
            JsonLog::Array(log) => log.redundant_by_parent(version, conservative),
        }
    }

    fn len(&self) -> usize {
        0
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{counter::resettable_counter::Counter, test_util::twins_log},
        protocol::replica::IsReplica,
    };

    use super::*;

    #[test]
    fn json_crdt() {
        let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<JsonLogContainer>>();
        let event_a = replica_a.send(NestedList::Insert {
            pos: 0,
            value: JsonOps::Bool(EWFlag::Enable),
        });
        let event_b = replica_b.send(NestedList::Insert {
            pos: 0,
            value: JsonOps::Object(UWMap::Update(
                "obj".to_string(),
                Box::new(JsonOps::Number(Counter::Inc(5.0))),
            )),
        });
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        // let event_a = replica_a.send(List::set(1, JsonOps::Bool(EWFlag::Disable)));
        // let event_b = replica_b.send(List::insert(
        //     2,
        //     JsonOps::Object(UWMap::Update(
        //         "obj".to_string(),
        //         Box::new(JsonOps::Number(Counter::Inc(5.0))),
        //     )),
        // ));
        // replica_b.receive(event_a);
        // replica_a.receive(event_b);

        println!("{:?}", replica_a.query());
        println!("{:?}", replica_b.query());
    }
}
