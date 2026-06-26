use moirai_protocol::crdt::{
    eval::EvalNested,
    query::{QueryOperation, Read},
};
use serde_json::{Map, Number, Value};

use crate::{
    json::{JsonChildValue, JsonLog, JsonValue},
    list::nested_list::NestedListLog,
    map::uw_map::UWMapLog,
};

#[derive(Debug)]
pub struct ReadAsJson;

impl QueryOperation for ReadAsJson {
    type Response = Value;
}

impl ReadAsJson {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ReadAsJson {
    fn default() -> Self {
        Self::new()
    }
}

impl EvalNested<ReadAsJson> for NestedListLog<JsonLog> {
    fn execute_query(&self, _q: ReadAsJson) -> <ReadAsJson as QueryOperation>::Response {
        // let mut list = Vec::new();
        // let positions = self.positions().execute_query(Read::new());
        // for id in positions.iter() {
        //     let child = self.children().get(id).unwrap();
        //     list.push(child.execute_query(ReadAsJson::new()));
        // }
        // Value::Array(list)
        todo!()
    }
}

impl EvalNested<ReadAsJson> for UWMapLog<String, JsonLog> {
    fn execute_query(&self, _q: ReadAsJson) -> <ReadAsJson as QueryOperation>::Response {
        let mut map: Map<String, Value> = Map::new();
        for (k, l) in self.children() {
            let val = l.execute_query(ReadAsJson::new());
            map.insert(k.clone(), val);
        }
        Value::Object(map)
    }
}

fn variant_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

impl EvalNested<ReadAsJson> for JsonLog {
    fn execute_query(&self, _q: ReadAsJson) -> <ReadAsJson as QueryOperation>::Response {
        fn eval_child(child: &JsonChildValue) -> Value {
            match child {
                JsonChildValue::Number(value) => {
                    Value::Number(Number::from_f64(*value).unwrap())
                }
                JsonChildValue::Boolean(value) => Value::Bool(*value),
                JsonChildValue::String(value) => Value::String(value.iter().collect()),
                JsonChildValue::Object(map) => {
                    let mut object = Map::new();
                    for (key, value) in map {
                        object.insert(key.clone(), eval_value(value));
                    }
                    Value::Object(object)
                }
                JsonChildValue::Array(list) => {
                    Value::Array(list.iter().map(eval_value).collect())
                }
            }
        }

        fn eval_value(value: &JsonValue) -> Value {
            match value {
                JsonValue::Unset => Value::Null,
                JsonValue::Value(child) => eval_child(child),
                JsonValue::Conflict(children) => {
                    let mut evaluated = children.iter().map(eval_child).collect::<Vec<Value>>();
                    evaluated.sort_by_key(variant_rank);
                    Value::Array(evaluated)
                }
            }
        }

        let value = <JsonLog as EvalNested<Read<JsonValue>>>::execute_query(self, Read::new());
        eval_value(&value)
    }
}
