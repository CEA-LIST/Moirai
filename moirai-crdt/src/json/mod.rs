#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGeneratorNested;
use moirai_macros::union;
use moirai_protocol::state::{event_graph::EventGraph, po_log::VecLog};
#[cfg(feature = "fuzz")]
use moirai_protocol::{crdt::query::Read, utils::boxer::Boxer};
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::{
    counter::resettable_counter::Counter,
    flag::ew_flag::EWFlag,
    list::{
        eg_walker::List,
        nested_list::{NestedList, NestedListLog},
    },
    map::uw_map::{UWMap, UWMapLog},
};

union! {
    Json = Number(Counter<f64>, VecLog::<Counter<f64>>)
        | Boolean(EWFlag, VecLog::<EWFlag>)
        | String(List<char>, EventGraph::<List<char>>)
        | Object(UWMap<String, Box<Json>>, UWMapLog::<String, JsonLog>)
        | Array(NestedList<Box<Json>>, NestedListLog::<JsonLog>)
}

// TODO: the code must be factorized
#[cfg(feature = "fuzz")]
impl OpGeneratorNested for JsonLog {
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        use moirai_protocol::state::log::IsLog;
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Number,
            Boolean,
            String,
            Object,
            Array,
        }
        let dist = WeightedIndex::new([2, 2, 2, 3, 3]).unwrap();

        fn generate_number(log: &VecLog<Counter<f64>>, rng: &mut impl Rng) -> Json {
            Json::Number(<VecLog<Counter<f64>> as OpGeneratorNested>::generate(
                log, rng,
            ))
        }

        fn generate_boolean(log: &VecLog<EWFlag>, rng: &mut impl Rng) -> Json {
            Json::Boolean(<VecLog<EWFlag> as OpGeneratorNested>::generate(log, rng))
        }

        fn generate_string(log: &EventGraph<List<char>>, rng: &mut impl Rng) -> Json {
            Json::String(<EventGraph<List<char>> as OpGeneratorNested>::generate(
                log, rng,
            ))
        }

        fn generate_object(log: &UWMapLog<String, JsonLog>, rng: &mut impl Rng) -> Json {
            let op = <UWMapLog<String, JsonLog> as OpGeneratorNested>::generate(log, rng);
            Json::Object(Boxer::<UWMap<String, Box<Json>>>::boxer(op))
        }

        fn generate_array(log: &NestedListLog<JsonLog>, rng: &mut impl Rng) -> Json {
            use moirai_protocol::utils::boxer::Boxer;

            let list_op = <NestedListLog<JsonLog> as OpGeneratorNested>::generate(log, rng);
            let o = Boxer::<NestedList<Box<Json>>>::boxer(list_op);
            Json::Array(o)
        }

        fn generate_value(val: &JsonChildValue, log: &JsonChild, rng: &mut impl Rng) -> Json {
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
            JsonValue::Unset => {
                use moirai_protocol::state::log::IsLog;

                let available_choices: Vec<Choice> = match &self.child {
                    JsonContainer::Unset => vec![
                        Choice::Number,
                        Choice::String,
                        Choice::Boolean,
                        Choice::Object,
                        Choice::Array,
                    ],
                    JsonContainer::Value(child) => match child.as_ref() {
                        JsonChild::Number(_) => vec![Choice::Number],
                        JsonChild::Boolean(_) => vec![Choice::Boolean],
                        JsonChild::String(_) => vec![Choice::String],
                        JsonChild::Object(_) => vec![Choice::Object],
                        JsonChild::Array(_) => vec![Choice::Array],
                    },
                    JsonContainer::Conflicts(children) => children
                        .iter()
                        .map(|child| match child {
                            JsonChild::Number(_) => Choice::Number,
                            JsonChild::Boolean(_) => Choice::Boolean,
                            JsonChild::String(_) => Choice::String,
                            JsonChild::Object(_) => Choice::Object,
                            JsonChild::Array(_) => Choice::Array,
                        })
                        .collect(),
                };

                let choice = if available_choices.len() == 5 {
                    &available_choices[dist.sample(rng)]
                } else {
                    rand::seq::IteratorRandom::choose(available_choices.iter(), rng).unwrap()
                };
                match choice {
                    Choice::Number => generate_number(&VecLog::<Counter<f64>>::new(), rng),
                    Choice::Boolean => generate_boolean(&VecLog::<EWFlag>::new(), rng),
                    Choice::Object => generate_object(&UWMapLog::<String, JsonLog>::new(), rng),
                    Choice::String => generate_string(&EventGraph::<List<char>>::new(), rng),
                    Choice::Array => generate_array(&NestedListLog::<JsonLog>::new(), rng),
                }
            }
            JsonValue::Value(v) => match &self.child {
                JsonContainer::Value(child) => generate_value(&v, child.as_ref(), rng),
                JsonContainer::Conflicts(child_logs) => {
                    let log = child_logs
                        .iter()
                        .find(|log| {
                            matches!(
                                (v.as_ref(), log),
                                (JsonChildValue::Number(_), JsonChild::Number(_))
                                    | (JsonChildValue::Boolean(_), JsonChild::Boolean(_))
                                    | (JsonChildValue::Object(_), JsonChild::Object(_))
                                    | (JsonChildValue::String(_), JsonChild::String(_))
                                    | (JsonChildValue::Array(_), JsonChild::Array(_))
                            )
                        })
                        .unwrap();
                    generate_value(&v, log, rng)
                }
                JsonContainer::Unset => unreachable!(),
            },
            JsonValue::Conflict(json_child_values) => match &self.child {
                JsonContainer::Conflicts(child_logs) => {
                    let choice =
                        rand::seq::IteratorRandom::choose(json_child_values.iter(), rng).unwrap();
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
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};
    use serde_json::{Number, Value, json};

    use crate::{
        counter::resettable_counter::Counter,
        flag::ew_flag::EWFlag,
        json::{Json, JsonLog},
        list::{eg_walker::List, nested_list::NestedList},
        map::uw_map::UWMap,
        query::read_as_json::ReadAsJson,
        utils::membership::{triplet_log, twins_log},
    };

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Boolean(EWFlag::Enable)).unwrap();
        let event_b = replica_b.send(Json::Number(Counter::Inc(5.0))).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Value::Array(vec![
            Value::Bool(true),
            Value::Number(Number::from_f64(5.0).unwrap()),
        ]);

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[test]
    fn sequential_same_variant() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a.send(Json::Number(Counter::Inc(5.0))).unwrap();
        replica_a.send(Json::Number(Counter::Inc(3.0))).unwrap();

        let result = Value::Number(Number::from_f64(8.0).unwrap());
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

        replica_a.send(Json::Number(Counter::Inc(5.0))).unwrap();
        let op = replica_a.send(Json::Boolean(EWFlag::Enable));
        assert!(op.is_none());

        let result = Value::Number(Number::from_f64(5.0).unwrap());
        assert_eq!(result, replica_a.query(ReadAsJson::new()));
    }

    #[test]
    fn concurrent_same_variant() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(5.0))).unwrap();
        let event_b = replica_b.send(Json::Number(Counter::Inc(3.0))).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Value::Number(Number::from_f64(8.0).unwrap());
        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[test]
    fn conflict_resolution_then_operation() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a1 = replica_a.send(Json::Number(Counter::Inc(5.0))).unwrap();
        let event_b1 = replica_b.send(Json::Boolean(EWFlag::Enable)).unwrap();

        replica_b.receive(event_a1.clone());
        replica_a.receive(event_b1.clone());

        let conflicts = Value::Array(vec![
            Value::Bool(true),
            Value::Number(Number::from_f64(5.0).unwrap()),
        ]);

        assert_eq!(conflicts, replica_a.query(ReadAsJson::new()));

        let event_a2 = replica_a.send(Json::Number(Counter::Inc(2.0))).unwrap();
        let event_b2 = replica_b.send(Json::Boolean(EWFlag::Disable)).unwrap();

        replica_b.receive(event_a2);
        replica_a.receive(event_b2);

        let result = Value::Array(vec![
            Value::Bool(false),
            Value::Number(Number::from_f64(7.0).unwrap()),
        ]);

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    #[test]
    fn triple_conflict() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(1.0))).unwrap();
        let event_b = replica_b.send(Json::Boolean(EWFlag::Enable)).unwrap();

        let map_op = UWMap::Update("key".to_string(), Box::new(Json::Number(Counter::Inc(0.0))));
        let event_c = replica_c.send(Json::Object(map_op)).unwrap();

        replica_a.receive(event_b.clone());
        replica_a.receive(event_c.clone());

        replica_b.receive(event_a.clone());
        replica_b.receive(event_c.clone());

        replica_c.receive(event_a.clone());
        replica_c.receive(event_b.clone());

        let result = json!([true, 1.0, {"key": 0.0}]);

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
        assert_eq!(result, replica_c.query(ReadAsJson::new()));
    }

    #[test]
    fn nested_conflicts() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<JsonLog>();

        let event_a = replica_a.send(Json::Number(Counter::Inc(1.0))).unwrap();
        let event_b = replica_b
            .send(Json::Object(UWMap::Update(
                "foo".to_string(),
                Box::new(Json::Number(Counter::Inc(0.0))),
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

        let op_a = UWMap::Update("k1".to_string(), Box::new(Json::Number(Counter::Inc(1.0))));
        let event_a = replica_a.send(Json::Object(op_a)).unwrap();

        let op_b = UWMap::Update("k2".to_string(), Box::new(Json::Boolean(EWFlag::Enable)));
        let event_b = replica_b.send(Json::Object(op_b)).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = json!({
            "k1": 1.0,
            "k2": true
        });

        assert_eq!(result, replica_a.query(ReadAsJson::new()));
        assert_eq!(result, replica_b.query(ReadAsJson::new()));
    }

    /// digraph {
    ///     0       [label="[Array(Insert { pos: 0, value: Object(Update('sj', String(Insert { content: 'w', pos: 0 }))) })@(0:1)]"];
    ///     1       [label="[Array(Update { pos: 0, value: Object(Update('ok', Number(Inc(652798)))) })@(0:2)]"];
    ///     0 -> 1;
    ///     2       [label="[Array(Update { pos: 0, value: Object(Update('nsd', Object(Clear))) })@(1:1)]"];
    ///     0 -> 2;
    ///     4       [label="[Array(Delete { pos: 0 })@(0:3)]"];
    ///     1 -> 4;
    ///     3       [label="[Array(Update { pos: 0, value: Object(Clear) })@(1:2)]"];
    ///     2 -> 3;
    ///     5       [label="[Array(Insert { pos: 1, value: Boolean(Enable) })@(1:3)]"];
    ///     3 -> 5;
    ///     7       [label="[Array(Delete { pos: 0 })@(0:4)]"];
    ///     4 -> 7;
    ///     6       [label="[Array(Update { pos: 0, value: Object(Update('zw', Number(Reset))) })@(1:4)]"];
    ///     5 -> 6;
    ///     6 -> 7;
    /// }
    #[test]
    fn error_1() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let b1 = replica_b
            .send(Json::Array(NestedList::insert(
                0,
                Box::new(Json::Object(UWMap::Update(
                    "s".into(),
                    Box::new(Json::String(List::insert('w', 0))),
                ))),
            )))
            .unwrap();

        replica_a.receive(b1);

        let b2 = replica_b
            .send(Json::Array(NestedList::update(
                0,
                Box::new(Json::Object(UWMap::Update(
                    "ok".into(),
                    Box::new(Json::Number(Counter::Inc(5.0))),
                ))),
            )))
            .unwrap();

        let b3 = replica_b.send(Json::Array(NestedList::delete(0))).unwrap();

        let a1 = replica_a
            .send(Json::Array(NestedList::update(
                0,
                Box::new(Json::Object(UWMap::Update(
                    "nsd".into(),
                    Box::new(Json::Object(UWMap::Clear)),
                ))),
            )))
            .unwrap();
        let a2 = replica_a
            .send(Json::Array(NestedList::update(
                0,
                Box::new(Json::Object(UWMap::Clear)),
            )))
            .unwrap();
        let a3 = replica_a
            .send(Json::Array(NestedList::insert(
                1,
                Box::new(Json::Boolean(EWFlag::Enable)),
            )))
            .unwrap();
        let a4 = replica_a
            .send(Json::Array(NestedList::update(
                0,
                Box::new(Json::Object(UWMap::Update(
                    "zw".into(),
                    Box::new(Json::Number(Counter::Reset)),
                ))),
            )))
            .unwrap();

        replica_b.receive(a1);
        replica_b.receive(a2);
        replica_b.receive(a3);
        replica_b.receive(a4);

        let b4 = replica_b.send(Json::Array(NestedList::delete(0))).unwrap();

        replica_a.receive(b2);
        replica_a.receive(b3);
        replica_a.receive(b4);

        assert_eq!(replica_b.query(Read::new()), replica_a.query(Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_json() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run = RunConfig::new(0.6, 4, 100, None, None, true, false);
        let runs = vec![run.clone(); 1_000];

        let config =
            FuzzerConfig::<JsonLog>::new("json", runs, true, |a, b| a == b, false, None);

        fuzzer::<JsonLog>(config);
    }
}
