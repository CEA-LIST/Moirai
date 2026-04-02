use moirai_protocol::crdt::{
    eval::EvalNested,
    query::{QueryOperation, Read},
};
use serde_json::{Map, Number, Value};

use crate::{
    json::{JsonChild, JsonContainer, JsonLog},
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
        fn eval_child(child: &JsonChild) -> Value {
            match child {
                JsonChild::Number(log) => {
                    Value::Number(Number::from_f64(log.execute_query(Read::new())).unwrap())
                }
                JsonChild::Boolean(log) => Value::Bool(log.execute_query(Read::new())),
                JsonChild::String(log) => {
                    let chars: String = log.execute_query(Read::<String>::new());
                    Value::String(chars)
                }
                JsonChild::Object(log) => log.execute_query(ReadAsJson::new()),
                JsonChild::Array(log) => log.execute_query(ReadAsJson::new()),
            }
        }

        match &self.child {
            JsonContainer::Value(child) => eval_child(child),
            JsonContainer::Conflicts(children) => {
                let mut evaluated = children.iter().map(eval_child).collect::<Vec<Value>>();
                evaluated.sort_by_key(variant_rank);
                Value::Array(evaluated)
            }
            JsonContainer::Unset => Value::Null,
        }
    }
}
