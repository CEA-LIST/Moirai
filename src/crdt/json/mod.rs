use serde_json::{Map, Value};
#[cfg(feature = "fuzz")]
use {
    crate::{
        fuzz::config::{OpGenerator, OpGeneratorNested},
        protocol::state::log::IsLogTest,
        utils::boxer::Boxer,
    },
    rand::RngCore,
};

use crate::{
    crdt::{
        counter::resettable_counter::Counter,
        flag::ew_flag::EWFlag,
        list::{
            eg_walker::List,
            nested_list::{List as NestedList, ListLog, ListLog as NestedListLog},
        },
        map::uw_map::{UWMap, UWMapLog},
    },
    protocol::{
        crdt::{
            eval::EvalNested,
            query::{QueryOperation, Read},
        },
        state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
    },
};

crate::make_union! {
    Json = Number(Counter<isize>, VecLog::<Counter<isize>>)
        | Boolean(EWFlag, VecLog::<EWFlag>)
        | String(List<char>, EventGraph::<List<char>>)
        | Object(UWMap<String, Box<Json>>, UWMapLog::<String, JsonLog>)
        | Array(NestedList<Box<Json>>, NestedListLog::<JsonLog>)
}

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

impl EvalNested<ReadAsJson> for ListLog<JsonLog> {
    fn execute_query(&self, _q: ReadAsJson) -> <ReadAsJson as QueryOperation>::Response {
        let mut list = Vec::new();
        let positions = self.position.execute_query(Read::new());
        for id in positions.iter() {
            let child = self.children.get(id).unwrap();
            list.push(child.execute_query(ReadAsJson::new()));
        }
        Value::Array(list)
    }
}

impl EvalNested<ReadAsJson> for UWMapLog<String, JsonLog> {
    fn execute_query(&self, _q: ReadAsJson) -> <ReadAsJson as QueryOperation>::Response {
        let mut map: Map<String, Value> = Map::new();
        for (k, l) in &self.children {
            let val = l.execute_query(ReadAsJson::new());
            map.insert(k.clone(), val);
        }
        Value::Object(map)
    }
}

impl EvalNested<ReadAsJson> for JsonLog {
    fn execute_query(&self, _q: ReadAsJson) -> <ReadAsJson as QueryOperation>::Response {
        fn eval_child(child: &JsonChild) -> Value {
            match child {
                JsonChild::Number(log) => Value::Number(log.execute_query(Read::new()).into()),
                JsonChild::Boolean(log) => Value::Bool(log.execute_query(Read::new())),
                JsonChild::String(log) => {
                    let chars: String = log.execute_query(Read::new()).into_iter().collect();
                    Value::String(chars)
                }
                JsonChild::Object(log) => log.execute_query(ReadAsJson::new()),
                JsonChild::Array(log) => log.execute_query(ReadAsJson::new()),
            }
        }

        match &self.child {
            JsonContainer::Value(child) => eval_child(child),
            JsonContainer::Conflicts(children) => {
                Value::Array(children.iter().map(eval_child).collect())
            }
            JsonContainer::Unset => Value::Null,
        }
    }
}

#[cfg(feature = "fuzz")]
impl OpGeneratorNested for JsonLog {
    fn generate(&self, rng: &mut impl RngCore) -> Self::Op {
        use rand::distr::{weighted::WeightedIndex, Distribution};

        use crate::protocol::crdt::query::Read;

        enum Choice {
            Number,
            Boolean,
            String,
            Object,
            Array,
        }
        let dist = WeightedIndex::new([2, 2, 2, 3, 3]).unwrap();

        fn generate_number(log: &VecLog<Counter<isize>>, rng: &mut impl RngCore) -> Json {
            let counter_op = <Counter<isize> as OpGenerator>::generate(
                rng,
                &<Counter<isize> as OpGenerator>::Config::default(),
                log.stable(),
                log.unstable(),
            );
            Json::Number(counter_op)
        }

        fn generate_boolean(log: &VecLog<EWFlag>, rng: &mut impl RngCore) -> Json {
            let flag_op = <EWFlag as OpGenerator>::generate(
                rng,
                &<EWFlag as OpGenerator>::Config::default(),
                log.stable(),
                log.unstable(),
            );
            Json::Boolean(flag_op)
        }

        fn generate_object(log: &UWMapLog<String, JsonLog>, rng: &mut impl RngCore) -> Json {
            let map_op = <UWMapLog<String, JsonLog> as OpGeneratorNested>::generate(log, rng);
            let o = Boxer::<UWMap<String, Box<Json>>>::boxer(map_op);
            Json::Object(o)
        }

        fn generate_string(log: &EventGraph<List<char>>, rng: &mut impl RngCore) -> Json {
            let list_op = <EventGraph<List<char>> as OpGeneratorNested>::generate(log, rng);
            Json::String(list_op)
        }

        fn generate_array(log: &NestedListLog<JsonLog>, rng: &mut impl RngCore) -> Json {
            let list_op = <NestedListLog<JsonLog> as OpGeneratorNested>::generate(log, rng);
            let o = Boxer::<NestedList<Box<Json>>>::boxer(list_op);
            Json::Array(o)
        }

        fn generate_value(val: &JsonChildValue, log: &JsonChild, rng: &mut impl RngCore) -> Json {
            match (val, log) {
                (JsonChildValue::Number(_), JsonChild::Number(l)) => generate_number(l, rng),
                (JsonChildValue::Boolean(_), JsonChild::Boolean(l)) => generate_boolean(l, rng),
                (JsonChildValue::String(_), JsonChild::String(l)) => generate_string(l, rng),
                (JsonChildValue::Object(_), JsonChild::Object(l)) => generate_object(l, rng),
                (JsonChildValue::Array(_), JsonChild::Array(l)) => generate_array(l, rng),
                _ => unreachable!(),
            }
        }

        let value = self.eval(Read::new());
        match value {
            Some(val) => match (val, &self.child) {
                (JsonValue::Value(v), JsonContainer::Value(child_log)) => {
                    generate_value(&v, child_log, rng)
                }
                (JsonValue::Conflict(v), JsonContainer::Conflicts(child_logs)) => {
                    let choice = rand::seq::IteratorRandom::choose(v.iter(), rng).unwrap();
                    let log = child_logs
                        .iter()
                        .find(|log| {
                            matches!(
                                (choice, log),
                                (JsonChildValue::Number(_), JsonChild::Number(_))
                                    | (JsonChildValue::Boolean(_), JsonChild::Boolean(_))
                                    | (JsonChildValue::Object(_), JsonChild::Object(_))
                                    | (JsonChildValue::String(_), JsonChild::String(_))
                                    | (JsonChildValue::Array(_), JsonChild::Array(_))
                            )
                        })
                        .unwrap();
                    generate_value(choice, log, rng)
                }
                _ => unreachable!(),
            },
            None => {
                let choice = &[
                    Choice::Number,
                    Choice::String,
                    Choice::Boolean,
                    Choice::Object,
                    Choice::Array,
                ][dist.sample(rng)];
                match choice {
                    Choice::Number => generate_number(&VecLog::<Counter<isize>>::new(), rng),
                    Choice::Boolean => generate_boolean(&VecLog::<EWFlag>::new(), rng),
                    Choice::Object => generate_object(&UWMapLog::<String, JsonLog>::new(), rng),
                    Choice::String => generate_string(&EventGraph::<List<char>>::new(), rng),
                    Choice::Array => generate_array(&NestedListLog::<JsonLog>::new(), rng),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            flag::ew_flag::EWFlag,
            json::{Json, JsonLog, ReadAsJson},
            map::uw_map::UWMap,
            test_util::{triplet_log, twins_log},
        },
        protocol::replica::IsReplica,
    };

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Boolean(EWFlag::Enable)).unwrap();
        let event_b = replica_b.send(Json::Number(Counter::Inc(5))).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Value::Array(vec![Value::Bool(true), Value::Number(5.into())]);

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[test]
    fn sequential_same_variant() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        replica_a.send(Json::Number(Counter::Inc(3))).unwrap();

        let result = Value::Number(8.into());
        assert_eq!(result, replica_a.query(ReadAsJson::new()));
    }

    #[test]
    fn root() {
        let (replica_a, _) = twins_log::<JsonLog>();

        let result = Value::Null;
        assert_eq!(result, replica_a.query(ReadAsJson::new()));
    }

    #[test]
    fn sequential_different_variant_fail() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        let op = replica_a.send(Json::Boolean(EWFlag::Enable));
        assert!(op.is_none());

        let result = Value::Number(5.into());
        assert_eq!(result, replica_a.query(ReadAsJson::new()));
    }

    #[test]
    fn concurrent_same_variant() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        let event_b = replica_b.send(Json::Number(Counter::Inc(3))).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Value::Number(8.into());
        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[test]
    fn conflict_resolution_then_operation() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a1 = replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        let event_b1 = replica_b.send(Json::Boolean(EWFlag::Enable)).unwrap();

        replica_b.receive(event_a1.clone());
        replica_a.receive(event_b1.clone());

        let conflicts = Value::Array(vec![Value::Bool(true), Value::Number(5.into())]);

        assert_eq!(conflicts, replica_a.query(ReadAsJson::new()));

        let event_a2 = replica_a.send(Json::Number(Counter::Inc(2))).unwrap();
        let event_b2 = replica_b.send(Json::Boolean(EWFlag::Disable)).unwrap();

        replica_b.receive(event_a2);
        replica_a.receive(event_b2);

        let result = Value::Array(vec![Value::Bool(false), Value::Number(7.into())]);

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[test]
    fn triple_conflict() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(1))).unwrap();
        let event_b = replica_b.send(Json::Boolean(EWFlag::Enable)).unwrap();

        let map_op = UWMap::Update("key".to_string(), Box::new(Json::Number(Counter::Inc(0))));
        let event_c = replica_c.send(Json::Object(map_op)).unwrap();

        replica_a.receive(event_b.clone());
        replica_a.receive(event_c.clone());

        replica_b.receive(event_a.clone());
        replica_b.receive(event_c.clone());

        replica_c.receive(event_a.clone());
        replica_c.receive(event_b.clone());

        let result = json!([true, 1, {"key": 0}]);

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
        assert_eq!(result, replica_c.query(ReadAsJson::new()));
    }

    #[test]
    fn nested_conflicts() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(1))).unwrap();
        let event_b = replica_b
            .send(Json::Object(UWMap::Update(
                "foo".to_string(),
                Box::new(Json::Number(Counter::Inc(0))),
            )))
            .unwrap();

        replica_a.receive(event_b.clone());
        replica_b.receive(event_a.clone());

        let event_c = replica_c
            .send(Json::Object(UWMap::Update(
                "foo".to_string(),
                Box::new(Json::Object(UWMap::Update(
                    "bar".to_string(),
                    Box::new(Json::Boolean(EWFlag::Enable)),
                ))),
            )))
            .unwrap();

        replica_a.receive(event_c.clone());
        replica_b.receive(event_c);
        replica_c.receive(event_a);
        replica_c.receive(event_b);

        assert_eq!(
            replica_a.query(ReadAsJson::new()),
            replica_b.query(ReadAsJson::new())
        );
        assert_eq!(
            replica_a.query(ReadAsJson::new()),
            replica_c.query(ReadAsJson::new())
        );
    }

    #[test]
    fn map_recursion_same_variant() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let op_a = UWMap::Update("k1".to_string(), Box::new(Json::Number(Counter::Inc(1))));
        let event_a = replica_a.send(Json::Object(op_a)).unwrap();

        let op_b = UWMap::Update("k2".to_string(), Box::new(Json::Boolean(EWFlag::Enable)));
        let event_b = replica_b.send(Json::Object(op_b)).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = json!({
            "k1": 1,
            "k2": true
        });

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_union() {
        use crate::fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer,
        };

        let run = RunConfig::new(0.4, 4, 1_000, None, None, false);
        let runs = vec![run.clone(); 10];

        let config = FuzzerConfig::<JsonLog>::new("json", runs, true, |a, b| a == b, true);

        fuzzer::<JsonLog>(config);
    }
}
