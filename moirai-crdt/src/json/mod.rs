#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::command_generator::CommandGenerator,
    observers::footprint::{FootprintNode, LogFootprint},
};
use moirai_macros::union;
use moirai_protocol::state::po_log::{GraphLog, VecLog};
#[cfg(feature = "fuzz")]
use moirai_protocol::utils::boxer::Boxer;
#[cfg(feature = "fuzz")]
use rand::Rng;

use crate::{
    FxHashMap,
    counter::{Counter, update::CounterUpdate},
    flag::{ew_flag::EWFlag, update::FlagUpdate},
    list::{
        eg_walker::List,
        nested_list::NestedListLog,
        update::{ListUpdate, NestedListUpdate},
    },
    map::{update::MapUpdate, uw_map::UWMapLog},
};

union! {
    Json = Number(CounterUpdate<f64>, VecLog::<Counter<f64>> => f64)
        | Boolean(FlagUpdate, VecLog::<EWFlag> => bool)
        | String(ListUpdate<char>, GraphLog::<List<char>> => String)
        | Object(MapUpdate<String, Box<Json>>, UWMapLog::<String, JsonLog> => FxHashMap<String, JsonValue>)
        | Array(NestedListUpdate<Box<Json>>, NestedListLog::<JsonLog> => Vec<JsonValue>)
}

#[cfg(feature = "fuzz")]
impl CommandGenerator for JsonLog {
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        use moirai_protocol::state::log::IsLog;
        use rand::{
            distr::{Distribution, weighted::WeightedIndex},
            seq::IteratorRandom,
        };

        fn generate_value(log: &JsonChild, rng: &mut impl Rng) -> Json {
            match log {
                JsonChild::Number(log) => Json::Number(log.generate_command(rng)),
                JsonChild::Boolean(log) => Json::Boolean(log.generate_command(rng)),
                JsonChild::String(log) => Json::String(log.generate_command(rng)),
                JsonChild::Object(log) => Json::Object(
                    Boxer::<MapUpdate<String, Box<Json>>>::boxer(log.generate_command(rng)),
                ),
                JsonChild::Array(log) => Json::Array(Boxer::<NestedListUpdate<Box<Json>>>::boxer(
                    log.generate_command(rng),
                )),
            }
        }

        let children: Vec<_> = self.active_children().collect();
        match children.as_slice() {
            [] => {
                let dist = WeightedIndex::new([2, 2, 2, 3, 3]).unwrap();
                let new_child = match dist.sample(rng) {
                    0 => JsonChild::Number(VecLog::new()),
                    1 => JsonChild::String(GraphLog::new()),
                    2 => JsonChild::Boolean(VecLog::new()),
                    3 => JsonChild::Object(UWMapLog::new()),
                    _ => JsonChild::Array(NestedListLog::new()),
                };
                generate_value(&new_child, rng)
            }
            [child] => generate_value(child, rng),
            _ => {
                let choose = WeightedIndex::new([1, 3]).unwrap().sample(rng) == 0;
                let child = children.into_iter().choose(rng).unwrap();
                if choose {
                    Json::Choose(child.variant())
                } else {
                    generate_value(child, rng)
                }
            }
        }
    }
}

#[cfg(feature = "fuzz")]
impl LogFootprint for JsonLog {
    fn footprint(&self) -> FootprintNode {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{data_type::query::Read, replica::IsReplica};
    use serde_json::{Number, Value, json};

    use crate::{
        counter::update::CounterUpdate,
        flag::update::FlagUpdate,
        json::{Json, JsonLog, JsonValue, JsonVariant},
        list::update::{ListUpdate, NestedListUpdate},
        map::update::MapUpdate,
        query::read_as_json::ReadAsJson,
        utils::bootstrap::{triplet_log, twins_log},
    };

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Boolean(FlagUpdate::Enable)).unwrap();
        let event_b = replica_b
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Value::Array(vec![
            Value::Bool(true),
            Value::Number(Number::from_f64(5.0).unwrap()),
        ]);

        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
        assert_eq!(result, replica_b.query(&ReadAsJson::new()));
    }

    #[test]
    fn choose_absent_variant_is_disabled() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        assert!(replica_a.send(Json::Choose(JsonVariant::Number)).is_err());

        replica_a
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();
        assert!(replica_a.send(Json::Choose(JsonVariant::Number)).is_err());
        assert!(replica_a.send(Json::Choose(JsonVariant::Boolean)).is_err());
    }

    #[test]
    fn choose_resolves_observed_conflict() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a.send(Json::Boolean(FlagUpdate::Enable)).unwrap();
        let event_b = replica_b
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let choose = replica_a.send(Json::Choose(JsonVariant::Number)).unwrap();
        replica_b.receive(choose);

        let result = Value::Number(Number::from_f64(5.0).unwrap());
        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
        assert_eq!(result, replica_b.query(&ReadAsJson::new()));
    }

    #[test]
    fn choose_preserves_concurrent_boolean_clear() {
        let (mut a, mut b) = twins_log::<JsonLog>();
        let boolean = a.send(Json::Boolean(FlagUpdate::Enable)).unwrap();
        let number = b.send(Json::Number(CounterUpdate::Inc(5.0))).unwrap();
        a.receive(number);
        b.receive(boolean);

        let choose = a.send(Json::Choose(JsonVariant::Number)).unwrap();
        let clear = b.send(Json::Boolean(FlagUpdate::Clear)).unwrap();
        a.receive(clear);
        b.receive(choose);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&ReadAsJson::new()), json!([false, 5.0]));
        }
    }

    #[test]
    fn choose_removes_observed_boolean_clear() {
        let (mut a, mut b) = twins_log::<JsonLog>();
        let boolean = a.send(Json::Boolean(FlagUpdate::Enable)).unwrap();
        let number = b.send(Json::Number(CounterUpdate::Inc(5.0))).unwrap();
        a.receive(number);
        b.receive(boolean);

        let clear = b.send(Json::Boolean(FlagUpdate::Clear)).unwrap();
        a.receive(clear);
        assert_eq!(a.query(&ReadAsJson::new()), json!([false, 5.0]));
        b.receive(a.send(Json::Choose(JsonVariant::Number)).unwrap());

        for replica in [&a, &b] {
            assert_eq!(replica.query(&ReadAsJson::new()), json!(5.0));
        }
    }

    #[test]
    fn concurrent_choices_can_unset_the_union() {
        let (mut a, mut b) = twins_log::<JsonLog>();
        let boolean = a.send(Json::Boolean(FlagUpdate::Enable)).unwrap();
        let number = b.send(Json::Number(CounterUpdate::Inc(5.0))).unwrap();
        a.receive(number);
        b.receive(boolean);

        let choose_number = a.send(Json::Choose(JsonVariant::Number)).unwrap();
        let choose_boolean = b.send(Json::Choose(JsonVariant::Boolean)).unwrap();
        a.receive(choose_boolean);
        b.receive(choose_number);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), JsonValue::Unset);
        }
    }

    #[test]
    fn hidden_string_keeps_history_for_concurrent_insert() {
        let (mut a, mut b) = twins_log::<JsonLog>();
        let number = a.send(Json::Number(CounterUpdate::Inc(5.0))).unwrap();
        let string = b.send(Json::String(ListUpdate::insert('x', 0))).unwrap();
        a.receive(string);
        b.receive(number);

        let choose = a.send(Json::Choose(JsonVariant::Number)).unwrap();
        assert_eq!(a.query(&ReadAsJson::new()), json!(5.0));
        let insert = b.send(Json::String(ListUpdate::insert('y', 1))).unwrap();
        a.receive(insert);
        b.receive(choose);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&ReadAsJson::new()), json!([5.0, "y"]));
        }
    }

    #[test]
    fn parent_reset_removes_observed_variant_presence() {
        let (mut a, mut b) = twins_log::<JsonLog>();
        let key = String::from("flag");
        let set_flag = |update| {
            Json::Object(MapUpdate::Update(
                key.clone(),
                Box::new(Json::Boolean(update)),
            ))
        };
        let remove_flag = || Json::Object(MapUpdate::Remove(key.clone()));

        b.receive(a.send(set_flag(FlagUpdate::Enable)).unwrap());
        let remove = a.send(remove_flag()).unwrap();
        let clear = b.send(set_flag(FlagUpdate::Clear)).unwrap();
        a.receive(clear);
        b.receive(remove);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&ReadAsJson::new()), json!({"flag": false}));
        }

        b.receive(a.send(remove_flag()).unwrap());
        for replica in [&a, &b] {
            assert_eq!(replica.query(&ReadAsJson::new()), json!({}));
        }
    }

    #[test]
    fn sequential_same_variant() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();
        replica_a
            .send(Json::Number(CounterUpdate::Inc(3.0)))
            .unwrap();

        let result = Value::Number(Number::from_f64(8.0).unwrap());
        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
    }

    #[test]
    fn root() {
        let (replica_a, _) = twins_log::<JsonLog>();

        assert!(JsonLog::default().is_unset());
        let result = Value::Null;
        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
    }

    #[test]
    fn sequential_different_variant_fail() {
        let (mut replica_a, _) = twins_log::<JsonLog>();

        replica_a
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();
        let op = replica_a.send(Json::Boolean(FlagUpdate::Enable));
        assert!(op.is_err());

        let result = Value::Number(Number::from_f64(5.0).unwrap());
        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
    }

    #[test]
    fn concurrent_same_variant() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a = replica_a
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();
        let event_b = replica_b
            .send(Json::Number(CounterUpdate::Inc(3.0)))
            .unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Value::Number(Number::from_f64(8.0).unwrap());
        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
        assert_eq!(result, replica_b.query(&ReadAsJson::new()));
    }

    #[test]
    fn conflict_resolution_then_operation() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let event_a1 = replica_a
            .send(Json::Number(CounterUpdate::Inc(5.0)))
            .unwrap();
        let event_b1 = replica_b.send(Json::Boolean(FlagUpdate::Enable)).unwrap();

        replica_b.receive(event_a1);
        replica_a.receive(event_b1);

        let conflicts = Value::Array(vec![
            Value::Bool(true),
            Value::Number(Number::from_f64(5.0).unwrap()),
        ]);

        assert_eq!(conflicts, replica_a.query(&ReadAsJson::new()));

        let event_a2 = replica_a
            .send(Json::Number(CounterUpdate::Inc(2.0)))
            .unwrap();
        let event_b2 = replica_b.send(Json::Boolean(FlagUpdate::Disable)).unwrap();

        replica_b.receive(event_a2);
        replica_a.receive(event_b2);

        let result = Value::Array(vec![
            Value::Bool(false),
            Value::Number(Number::from_f64(7.0).unwrap()),
        ]);

        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
        assert_eq!(result, replica_b.query(&ReadAsJson::new()));
    }

    #[test]
    fn triple_conflict() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<JsonLog>();

        let event_a = replica_a
            .send(Json::Number(CounterUpdate::Inc(1.0)))
            .unwrap();
        let event_b = replica_b.send(Json::Boolean(FlagUpdate::Enable)).unwrap();

        let map_op = MapUpdate::Update(
            "key".to_string(),
            Box::new(Json::Number(CounterUpdate::Inc(0.0))),
        );
        let event_c = replica_c.send(Json::Object(map_op)).unwrap();

        replica_a.receive(event_b.clone());
        replica_a.receive(event_c.clone());

        replica_b.receive(event_a.clone());
        replica_b.receive(event_c);

        replica_c.receive(event_a);
        replica_c.receive(event_b);

        let result = json!([true, 1.0, {"key": 0.0}]);

        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
        assert_eq!(result, replica_b.query(&ReadAsJson::new()));
        assert_eq!(result, replica_c.query(&ReadAsJson::new()));
    }

    #[test]
    fn nested_conflicts() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<JsonLog>();

        let event_a = replica_a
            .send(Json::Number(CounterUpdate::Inc(1.0)))
            .unwrap();
        let event_b = replica_b
            .send(Json::Object(MapUpdate::Update(
                "foo".to_string(),
                Box::new(Json::Number(CounterUpdate::Inc(0.0))),
            )))
            .unwrap();

        replica_a.receive(event_b.clone());
        replica_b.receive(event_a.clone());

        let event_c = replica_c
            .send(Json::Object(MapUpdate::Update(
                "foo".to_string(),
                Box::new(Json::Object(MapUpdate::Update(
                    "bar".to_string(),
                    Box::new(Json::Boolean(FlagUpdate::Enable)),
                ))),
            )))
            .unwrap();

        replica_a.receive(event_c.clone());
        replica_b.receive(event_c);
        replica_c.receive(event_a);
        replica_c.receive(event_b);

        assert_eq!(
            replica_a.query(&ReadAsJson::new()),
            replica_b.query(&ReadAsJson::new())
        );
        assert_eq!(
            replica_a.query(&ReadAsJson::new()),
            replica_c.query(&ReadAsJson::new())
        );
    }

    #[test]
    fn map_recursion_same_variant() {
        let (mut replica_a, mut replica_b) = twins_log::<JsonLog>();

        let op_a = MapUpdate::Update(
            "k1".to_string(),
            Box::new(Json::Number(CounterUpdate::Inc(1.0))),
        );
        let event_a = replica_a.send(Json::Object(op_a)).unwrap();

        let op_b = MapUpdate::Update(
            "k2".to_string(),
            Box::new(Json::Boolean(FlagUpdate::Enable)),
        );
        let event_b = replica_b.send(Json::Object(op_b)).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = json!({
            "k1": 1.0,
            "k2": true
        });

        assert_eq!(result, replica_a.query(&ReadAsJson::new()));
        assert_eq!(result, replica_b.query(&ReadAsJson::new()));
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
            .send(Json::Array(NestedListUpdate::insert(
                0,
                Box::new(Json::Object(MapUpdate::Update(
                    "s".into(),
                    Box::new(Json::String(ListUpdate::insert('w', 0))),
                ))),
            )))
            .unwrap();

        replica_a.receive(b1);

        let b2 = replica_b
            .send(Json::Array(NestedListUpdate::update(
                0,
                Box::new(Json::Object(MapUpdate::Update(
                    "ok".into(),
                    Box::new(Json::Number(CounterUpdate::Inc(5.0))),
                ))),
            )))
            .unwrap();

        let b3 = replica_b
            .send(Json::Array(NestedListUpdate::delete(0)))
            .unwrap();

        let a1 = replica_a
            .send(Json::Array(NestedListUpdate::update(
                0,
                Box::new(Json::Object(MapUpdate::Update(
                    "nsd".into(),
                    Box::new(Json::Object(MapUpdate::Clear)),
                ))),
            )))
            .unwrap();
        let a2 = replica_a
            .send(Json::Array(NestedListUpdate::update(
                0,
                Box::new(Json::Object(MapUpdate::Clear)),
            )))
            .unwrap();
        let a3 = replica_a
            .send(Json::Array(NestedListUpdate::insert(
                1,
                Box::new(Json::Boolean(FlagUpdate::Enable)),
            )))
            .unwrap();
        let a4 = replica_a
            .send(Json::Array(NestedListUpdate::update(
                0,
                Box::new(Json::Object(MapUpdate::Update(
                    "zw".into(),
                    Box::new(Json::Number(CounterUpdate::Reset)),
                ))),
            )))
            .unwrap();

        replica_b.receive(a1);
        replica_b.receive(a2);
        replica_b.receive(a3);
        replica_b.receive(a4);

        let b4 = replica_b
            .send(Json::Array(NestedListUpdate::delete(0)))
            .unwrap();

        replica_a.receive(b2);
        replica_a.receive(b3);
        replica_a.receive(b4);

        assert_eq!(replica_b.query(&Read::new()), replica_a.query(&Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_json() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };

        let run =
            RunConfig::new(ChurnConfig::new(0.6, 0.4), 4, 100, None, None).with_execution_graph();
        let runs = vec![run; 1_000];

        let config = FuzzerConfig::<JsonLog, Read<JsonValue>>::new(
            "json",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<JsonLog, Read<JsonValue>>(config);
    }
}
