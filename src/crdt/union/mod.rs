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
    protocol::state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
};

#[cfg(feature = "fuzz")]
use {
    crate::{
        fuzz::config::{OpGenerator, OpGeneratorNested},
        protocol::state::log::IsLogTest,
        utils::unboxer::Unboxer,
    },
    rand::RngCore,
};

crate::make_union! {
    Json = Number(Counter<isize>, VecLog::<Counter<isize>>)
        | Boolean(EWFlag, VecLog::<EWFlag>)
        | String(List<char>, EventGraph::<List<char>>)
        | Object(UWMap<String, Box<Json>>, UWMapLog::<String, JsonLog>)
        | Array(NestedList<Box<Json>>, NestedListLog::<JsonLog>)
}

pub fn to_json(value: Option<JsonValue>) -> serde_json::Value {
    match value {
        Some(v) => match v {
            JsonValue::Value(json_child_value) => match json_child_value {
                JsonChildValue::Number(n) => Value::Number(n.into()),
                JsonChildValue::Boolean(b) => Value::Bool(b),
                JsonChildValue::String(s) => {
                    let chars: String = s.into_iter().collect();
                    Value::String(chars)
                }
                JsonChildValue::Object(o) => {
                    let mut map = serde_json::Map::new();
                    for (k, v) in o {
                        map.insert(k, to_json(v));
                    }
                    Value::Object(map)
                }
                JsonChildValue::Array(a) => Value::Array(a.into_iter().map(to_json).collect()),
            },
            JsonValue::Conflict(json_child_values) => Value::Array(
                json_child_values
                    .into_iter()
                    .map(|c| to_json(Some(JsonValue::Value(c))))
                    .collect(),
            ),
        },
        None => Value::Null,
    }
}

#[cfg(feature = "fuzz")]
impl OpGeneratorNested for JsonLog {
    fn generate(&self, rng: &mut impl RngCore) -> Self::Op {
        use crate::protocol::crdt::query::Read;

        enum Choice {
            Number,
            Boolean,
            Object,
            String,
            Array,
        }

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
            let o = Unboxer::<UWMap<String, Box<Json>>>::unbox(map_op);
            Json::Object(o)
        }

        fn generate_string(log: &EventGraph<List<char>>, rng: &mut impl RngCore) -> Json {
            let list_op = <EventGraph<List<char>> as OpGeneratorNested>::generate(log, rng);
            Json::String(list_op)
        }

        fn generate_array(log: &NestedListLog<JsonLog>, rng: &mut impl RngCore) -> Json {
            let list_op = <NestedListLog<JsonLog> as OpGeneratorNested>::generate(log, rng);
            let o = Unboxer::<NestedList<Box<Json>>>::unbox(list_op);
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
                let choice = rand::seq::IteratorRandom::choose(
                    [
                        Choice::Number,
                        Choice::String,
                        Choice::Boolean,
                        Choice::Object,
                        Choice::Array,
                    ]
                    .iter(),
                    rng,
                )
                .unwrap();
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
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            flag::ew_flag::EWFlag,
            map::uw_map::UWMap,
            test_util::{triplet_log, twins_log},
            union::{to_json, Json, JsonChildValue, JsonLog, JsonValue},
        },
        protocol::{crdt::query::Read, replica::IsReplica},
    };

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Boolean(EWFlag::Enable)).unwrap();
        let event_b = replica_b.send(Json::Number(Counter::Inc(5))).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result: Option<JsonValue> = Some(JsonValue::Conflict(vec![
            JsonChildValue::Boolean(true),
            JsonChildValue::Number(5),
        ]));
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    #[test]
    fn sequential_same_variant() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        replica_a.send(Json::Number(Counter::Inc(3))).unwrap();

        let result = Some(JsonValue::Value(JsonChildValue::Number(8)));
        assert_eq!(result, replica_a.query(Read::new()));
    }

    #[test]
    fn root() {
        let (replica_a, _) = twins_log::<JsonLog>();

        let result: Option<JsonValue> = None;
        assert_eq!(result, replica_a.query(Read::new()));
        println!("Replica A: {:?}", to_json(replica_a.query(Read::new())));
    }

    #[test]
    fn sequential_different_variant_fail() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        let op = replica_a.send(Json::Boolean(EWFlag::Enable));
        assert!(op.is_none());

        let result = Some(JsonValue::Value(JsonChildValue::Number(5)));
        assert_eq!(result, replica_a.query(Read::new()));
    }

    #[test]
    fn concurrent_same_variant() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        let event_b = replica_b.send(Json::Number(Counter::Inc(3))).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Some(JsonValue::Value(JsonChildValue::Number(8)));
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
    }

    #[test]
    fn conflict_resolution_then_operation() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a1 = replica_a.send(Json::Number(Counter::Inc(5))).unwrap();
        let event_b1 = replica_b.send(Json::Boolean(EWFlag::Enable)).unwrap();

        replica_b.receive(event_a1.clone());
        replica_a.receive(event_b1.clone());

        let conflict = Some(JsonValue::Conflict(vec![
            JsonChildValue::Boolean(true),
            JsonChildValue::Number(5),
        ]));
        assert_eq!(conflict, replica_a.query(Read::new()));

        let event_a2 = replica_a.send(Json::Number(Counter::Inc(2))).unwrap();
        let event_b2 = replica_b.send(Json::Boolean(EWFlag::Disable)).unwrap();

        replica_b.receive(event_a2);
        replica_a.receive(event_b2);

        let result = Some(JsonValue::Conflict(vec![
            JsonChildValue::Boolean(false),
            JsonChildValue::Number(7),
        ]));
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
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

        let mut map_val = crate::HashMap::default();
        map_val.insert(
            "key".to_string(),
            Some(JsonValue::Value(JsonChildValue::Number(0))),
        );

        let result = Some(JsonValue::Conflict(vec![
            JsonChildValue::Boolean(true),
            JsonChildValue::Number(1),
            JsonChildValue::Object(map_val),
        ]));
        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));
        assert_eq!(result, replica_c.query(Read::new()));
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

        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
        assert_eq!(replica_a.query(Read::new()), replica_c.query(Read::new()));
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

        let mut expected_map = crate::HashMap::default();
        expected_map.insert(
            "k1".to_string(),
            Some(JsonValue::Value(JsonChildValue::Number(1))),
        );
        expected_map.insert(
            "k2".to_string(),
            Some(JsonValue::Value(JsonChildValue::Boolean(true))),
        );

        let result = Some(JsonValue::Value(JsonChildValue::Object(expected_map)));

        assert_eq!(result, replica_a.query(Read::new()));
        assert_eq!(result, replica_b.query(Read::new()));

        println!("Replica A: {:?}", to_json(replica_a.query(Read::new())));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_union() {
        use crate::fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer,
        };

        let run = RunConfig::new(0.4, 8, 25, None, None, true);
        let runs = vec![run.clone(); 100];

        let config = FuzzerConfig::<JsonLog>::new("union", runs, true, |a, b| a == b, false);

        fuzzer::<JsonLog>(config);
    }
}
