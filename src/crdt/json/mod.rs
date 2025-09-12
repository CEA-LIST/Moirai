use std::collections::HashMap;

use crate::{
    crdt::{
        counter::resettable_counter::Counter, flag::ew_flag::EWFlag, map::uw_map::UWMapLog,
        register::mv_register::MVRegister,
    },
    protocol::{
        clock::version_vector::Version,
        event::Event,
        state::{log::IsLog, po_log::VecLog},
    },
    record,
};

#[derive(Debug, Clone)]
pub enum Json {
    Null,
    Bool(EWFlag),
    Number(Counter<f64>),
}

#[derive(Debug, Clone)]
pub enum JsonLog {
    Null,
    Bool(VecLog<EWFlag>),
    Number(VecLog<Counter<f64>>),
}

impl Default for JsonLog {
    fn default() -> Self {
        JsonLog::Null
    }
}

#[derive(Debug, Clone)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
}

#[derive(Debug, Clone, Default)]
pub struct JsonLogContainer {
    value: JsonLog,
}

impl Default for Json {
    fn default() -> Self {
        Json::Null
    }
}

impl IsLog for JsonLogContainer {
    type Op = Json;
    type Value = JsonValue;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            Json::Bool(op) => {
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
            Json::Number(op) => {
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
            _ => {}
        }
    }

    fn eval(&self) -> Self::Value {
        match &self.value {
            JsonLog::Bool(log) => JsonValue::Bool(log.eval()),
            JsonLog::Number(log) => JsonValue::Number(log.eval()),
            _ => JsonValue::Null,
        }
    }

    fn stabilize(&mut self, version: &Version) {}

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        match &mut self.value {
            JsonLog::Bool(log) => log.redundant_by_parent(version, conservative),
            JsonLog::Number(log) => log.redundant_by_parent(version, conservative),
            _ => {}
        }
    }

    fn len(&self) -> usize {
        0
    }

    fn is_empty(&self) -> bool {
        false
    }
}

// pub mod json {
//     use crate::{
//         crdt::{map::uw_map::UWMapLog, register::mv_register::MVRegister},
//         protocol::state::log::IsLog,
//         record,
//     };
//     use std::collections::HashMap;

//     #[derive(Debug, Clone)]
//     pub enum Json {
//         Array(<UWMapLog<usize, JsonLog> as crate::protocol::state::log::IsLog>::Op),
//     }

//     #[derive(Debug, Clone, Default)]
//     pub struct JsonLog {
//         pub array: UWMapLog<usize, JsonLog>,
//     }

//     #[derive(Debug, Clone, Default, PartialEq)]
//     pub struct JsonValue {
//         pub array: <UWMapLog<usize, JsonLog> as crate::protocol::state::log::IsLog>::Value,
//     }

//     impl crate::protocol::state::log::IsLog for JsonLog {
//         type Op = Json;
//         type Value = JsonValue;
//         fn new() -> Self {
//             Self {
//                 array: <UWMapLog<usize, JsonLog> as crate::protocol::state::log::IsLog>::new(),
//             }
//         }
//         fn effect(&mut self, event: crate::protocol::event::Event<Self::Op>) {
//             match event.op().clone() {
//                 Json::Array(op) => {
//                     let child_op = crate::protocol::event::Event::unfold(event, op);
//                     self.array.effect(child_op);
//                 }
//                 _ => {}
//             }
//         }
//         fn eval(&self) -> Self::Value {
//             JsonValue {
//                 array: self.array.eval(),
//             }
//         }
//         fn stabilize(&mut self, version: &crate::protocol::clock::version_vector::Version) {
//             self.array.stabilize(version);
//         }
//         fn redundant_by_parent(
//             &mut self,
//             version: &crate::protocol::clock::version_vector::Version,
//             conservative: bool,
//         ) {
//             self.array.redundant_by_parent(version, conservative);
//         }
//         fn len(&self) -> usize {
//             0 + self.array.len()
//         }
//         fn is_empty(&self) -> bool {
//             true && self.array.is_empty()
//         }
//     }
// }

// record!(Json {
//     null: MVRegister<()>,
//     bool: MVRegister<bool>,
//     number: MVRegister<f64>,
//     string: MVRegister<String>,
//     array: UWMapLog<usize, JsonLog>,
//     object: UWMapLog<String, JsonLog>,
// });

// enum JsonValue {
//     Null,
//     Bool(bool),
//     Number(f64),
//     String(String),
//     Array(Vec<JsonValue>),
//     Object(HashMap<String, JsonValue>),
// }

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            list::nested_list::{List, ListLog},
            test_util::twins_log,
        },
        protocol::{
            event::tagged_op::TaggedOp,
            replica::IsReplica,
            state::po_log::{POLog, VecLog},
        },
    };

    use super::*;

    #[test]
    fn json_crdt() {
        let (mut replica_a, mut replica_b) = twins_log::<ListLog<JsonLogContainer>>();
        let test = replica_a.send(List::insert(0, Json::Number(Counter::Inc(10.0))));
        let test_2 = replica_b.send(List::insert(0, Json::Bool(EWFlag::Enable)));
        replica_b.receive(test);
        replica_a.receive(test_2);

        println!("{:?}", replica_a.query());
        println!("{:?}", replica_b.query());
    }
}
