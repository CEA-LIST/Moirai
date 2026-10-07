//! Removing a key from a JSON object.
//!
//! 1. A key whose every update has been seen by a remove is absent from the read.
//! 2. An update concurrent with the remove keeps the key, holding that update's effect.
//! 3. A key set to an empty value by an update that no remove covers stays.
//! 4. After a remove, a put of another kind reads that kind alone.
//! 5. Every replica reads the same document whatever the delivery order.
//!
//! Each scenario is a script of sends and deliveries between replicas, with a
//! hand-computed expected document. It is checked in two configurations:
//! (a) the scripted replicas are the only members, and each replica receives the
//! events it has not seen yet in every possible order;
//! (b) an extra member, the observer, receives all the events in every possible order.
//! Every read must equal the expected document, and all reads must be equal.

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, catch_unwind},
};

use moirai_crdt::{
    counter::resettable_counter::Counter,
    flag::ew_flag::EWFlag,
    json::{Json, JsonLog, JsonValue},
    list::{eg_walker::List, nested_list::NestedList},
    map::uw_map::UWMap,
    query::read_as_json::ReadAsJson,
};
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    crdt::query::Read,
    log_id::LogId,
    replica::{IsReplica, Replica},
};
use serde_json::{Value, json};

type JsonReplica = Replica<JsonLog, Tcsb<Json>>;

const OBSERVER: &str = "o";
const MAX_REPORTED: usize = 12;

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// Update `key` of an object with `value`.
fn put(key: &str, value: Json) -> Json {
    Json::Object(UWMap::Update(key.to_string(), Box::new(value)))
}

/// Remove `key` from an object.
fn remove(key: &str) -> Json {
    Json::Object(UWMap::Remove(key.to_string()))
}

/// Remove `inner` from the object held at `key`.
fn remove_nested(key: &str, inner: &str) -> Json {
    put(key, remove(inner))
}

fn ins(content: char, pos: usize) -> Json {
    Json::String(List::insert(content, pos))
}

fn del(pos: usize) -> Json {
    Json::String(List::delete(pos))
}

fn inc(value: f64) -> Json {
    Json::Number(Counter::Inc(value))
}

fn dec(value: f64) -> Json {
    Json::Number(Counter::Dec(value))
}

fn reset() -> Json {
    Json::Number(Counter::Reset)
}

fn enable() -> Json {
    Json::Boolean(EWFlag::Enable)
}

fn disable() -> Json {
    Json::Boolean(EWFlag::Disable)
}

fn clear_object() -> Json {
    Json::Object(UWMap::Clear)
}

fn arr_insert(pos: usize, value: Json) -> Json {
    Json::Array(NestedList::insert(pos, Box::new(value)))
}

fn arr_update(pos: usize, value: Json) -> Json {
    Json::Array(NestedList::update(pos, Box::new(value)))
}

fn arr_delete(pos: usize) -> Json {
    Json::Array(NestedList::delete(pos))
}

/// The five kinds of JSON values, each with the updates that fill a key with
/// a non-empty value and the value they read as.
#[derive(Clone, Copy, Debug)]
enum Kind {
    String,
    Number,
    Boolean,
    Object,
    Array,
}

const KINDS: [Kind; 5] = [
    Kind::String,
    Kind::Number,
    Kind::Boolean,
    Kind::Object,
    Kind::Array,
];

impl Kind {
    fn fill(self) -> Vec<Json> {
        match self {
            Kind::String => vec![ins('a', 0), ins('b', 1)],
            Kind::Number => vec![inc(5.0), inc(3.0)],
            Kind::Boolean => vec![enable()],
            Kind::Object => vec![put("x", inc(1.0)), put("y", ins('s', 0))],
            Kind::Array => vec![arr_insert(0, inc(1.0)), arr_insert(1, ins('t', 0))],
        }
    }

    fn filled(self) -> Value {
        match self {
            Kind::String => json!("ab"),
            Kind::Number => json!(8.0),
            Kind::Boolean => json!(true),
            Kind::Object => json!({"x": 1.0, "y": "s"}),
            Kind::Array => json!([1.0, "t"]),
        }
    }

    /// A fresh value of this kind, put on a key after it was removed.
    fn fresh(self) -> (Json, Value) {
        match self {
            Kind::String => (ins('n', 0), json!("n")),
            Kind::Number => (inc(7.0), json!(7.0)),
            Kind::Boolean => (enable(), json!(true)),
            Kind::Object => (put("z", inc(1.0)), json!({"z": 1.0})),
            Kind::Array => (arr_insert(0, ins('q', 0)), json!(["q"])),
        }
    }

    fn next(self) -> Kind {
        match self {
            Kind::String => Kind::Number,
            Kind::Number => Kind::Boolean,
            Kind::Boolean => Kind::Object,
            Kind::Object => Kind::Array,
            Kind::Array => Kind::String,
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Run {
    ids: Vec<String>,
    replicas: Vec<JsonReplica>,
    events: Vec<EventMessage<Json>>,
    /// Events each replica has sent or received, by index in `events`.
    seen: Vec<BTreeSet<usize>>,
    /// Checkpoints of the script that did not read as expected.
    failures: Vec<String>,
}

impl Run {
    fn new(ids: &[&str]) -> Self {
        // One log, hosted by every replica of the run: a replica bootstrapped
        // with a log id of its own refuses the events of every other one.
        let log_id = LogId::from_bytes([0x5a; 16]);
        Self {
            ids: ids.iter().map(|id| id.to_string()).collect(),
            replicas: ids
                .iter()
                .map(|id| JsonReplica::bootstrap_with_log_id((*id).into(), ids, log_id.clone()))
                .collect(),
            events: Vec::new(),
            seen: vec![BTreeSet::new(); ids.len()],
            failures: Vec::new(),
        }
    }

    /// Replica `r` issues `op`; returns the event's index.
    fn send(&mut self, r: usize, op: Json) -> usize {
        let description = format!("{op:?}");
        let event = match self.replicas[r].send(op) {
            Ok(event) => event,
            Err(rejection) => panic!(
                "replica {} rejected {description}: {rejection}",
                self.ids[r]
            ),
        };
        self.events.push(event);
        let index = self.events.len() - 1;
        self.seen[r].insert(index);
        index
    }

    /// Replica `r` receives the event of index `e`.
    fn deliver(&mut self, r: usize, e: usize) {
        assert!(self.seen[r].insert(e), "script delivers event {e} twice");
        self.replicas[r].receive(self.events[e].clone());
    }

    fn deliver_all(&mut self, r: usize, events: &[usize]) {
        for e in events {
            self.deliver(r, *e);
        }
    }

    fn read(&self, r: usize) -> Value {
        self.replicas[r].query(&ReadAsJson::new())
    }

    fn read_value(&self, r: usize) -> JsonValue {
        self.replicas[r].query(&Read::<JsonValue>::new())
    }

    /// Checkpoint inside a script: replica `r` must read `expected` now.
    fn expect(&mut self, r: usize, expected: Value) {
        let got = self.read(r);
        if got != expected {
            self.failures.push(format!(
                "checkpoint on {}: expected {expected}, read {got}",
                self.ids[r]
            ));
        }
    }

    fn missing(&self, r: usize) -> Vec<usize> {
        (0..self.events.len())
            .filter(|e| !self.seen[r].contains(e))
            .collect()
    }
}

struct Scenario {
    name: String,
    replicas: Vec<&'static str>,
    script: Box<dyn Fn(&mut Run)>,
    expected: Value,
}

impl Scenario {
    fn new(
        name: impl Into<String>,
        replicas: &[&'static str],
        expected: Value,
        script: impl Fn(&mut Run) + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            replicas: replicas.to_vec(),
            script: Box::new(script),
            expected,
        }
    }
}

fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
    if items.is_empty() {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, head);
            out.push(tail);
        }
    }
    out
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic".to_string()
    }
}

/// One full run: the script, then replica `r` receives `order`.
fn run_once(
    scenario: &Scenario,
    members: &[&str],
    r: usize,
    order: &[usize],
) -> Result<(Value, JsonValue, Vec<String>), String> {
    catch_unwind(AssertUnwindSafe(|| {
        let mut run = Run::new(members);
        (scenario.script)(&mut run);
        run.deliver_all(r, order);
        (run.read(r), run.read_value(r), run.failures)
    }))
    .map_err(panic_message)
}

/// Check one scenario in both configurations; returns the failures.
fn check(scenario: &Scenario) -> Vec<String> {
    let mut failures = BTreeSet::new();
    let mut reads: Vec<(String, JsonValue)> = Vec::new();

    let generators = scenario.replicas.clone();
    let mut with_observer = generators.clone();
    with_observer.push(OBSERVER);

    // The script alone, to learn which events each replica misses.
    let script_only = |members: &[&str]| {
        catch_unwind(AssertUnwindSafe(|| {
            let mut run = Run::new(members);
            (scenario.script)(&mut run);
            (0..members.len())
                .map(|r| run.missing(r))
                .collect::<Vec<_>>()
        }))
        .map_err(panic_message)
    };

    for (config, members, targets) in [
        (
            "members only",
            &generators,
            (0..generators.len()).collect::<Vec<_>>(),
        ),
        ("with observer", &with_observer, vec![generators.len()]),
    ] {
        let missing = match script_only(members) {
            Ok(missing) => missing,
            Err(message) => {
                failures.insert(format!("[{config}] script panicked: {message}"));
                continue;
            }
        };
        for r in targets {
            for order in permutations(&missing[r]) {
                let label = format!("[{config}] {} after {order:?}", members[r]);
                match run_once(scenario, members, r, &order) {
                    Ok((got, value, checkpoints)) => {
                        for checkpoint in checkpoints {
                            failures.insert(format!("[{config}] {checkpoint}"));
                        }
                        if got != scenario.expected {
                            failures.insert(format!(
                                "{label}: expected {}, read {got}",
                                scenario.expected
                            ));
                        }
                        reads.push((label, value));
                    }
                    Err(message) => {
                        failures.insert(format!("{label}: panicked: {message}"));
                        break;
                    }
                }
            }
        }
    }

    if let Some((first_label, first)) = reads.first() {
        for (label, value) in &reads[1..] {
            if value != first {
                failures.insert(format!(
                    "diverged: {label} reads {value:?} but {first_label} reads {first:?}"
                ));
            }
        }
    }

    failures
        .into_iter()
        .map(|failure| format!("{}: {failure}", scenario.name))
        .collect()
}

fn assert_scenarios(scenarios: Vec<Scenario>) {
    let mut failures = Vec::new();
    for scenario in &scenarios {
        failures.extend(check(scenario));
    }
    if !failures.is_empty() {
        let total = failures.len();
        failures.truncate(MAX_REPORTED);
        panic!(
            "{total} failure(s) over {} scenario(s), first {}:\n  {}",
            scenarios.len(),
            failures.len(),
            failures.join("\n  ")
        );
    }
}

// ---------------------------------------------------------------------------
// 1. A key removed after every update to it is absent
// ---------------------------------------------------------------------------

/// The writer removes the key itself.
fn removed_by_writer(kind: Kind) -> Scenario {
    Scenario::new(
        format!("{kind:?} removed by its writer"),
        &["a", "b"],
        json!({"keep": 1.0}),
        move |run| {
            run.send(0, put("keep", inc(1.0)));
            for op in kind.fill() {
                run.send(0, put("k", op));
            }
            run.expect(0, json!({"keep": 1.0, "k": kind.filled()}));
            run.send(0, remove("k"));
            run.expect(0, json!({"keep": 1.0}));
        },
    )
}

/// The other replica removes the key after the updates became stable.
fn removed_by_other_after_stability(kind: Kind) -> Scenario {
    Scenario::new(
        format!("{kind:?} removed by the other replica after stability"),
        &["a", "b"],
        json!({"keep": 2.0}),
        move |run| {
            let mut writes = vec![run.send(0, put("keep", inc(1.0)))];
            for op in kind.fill() {
                writes.push(run.send(0, put("k", op)));
            }
            run.deliver_all(1, &writes);
            // b acknowledges, so that a learns the writes are stable.
            let ack = run.send(1, put("keep", inc(1.0)));
            run.deliver(0, ack);
            run.expect(0, json!({"keep": 2.0, "k": kind.filled()}));
            run.expect(1, json!({"keep": 2.0, "k": kind.filled()}));
            run.send(1, remove("k"));
            run.expect(1, json!({"keep": 2.0}));
        },
    )
}

#[test]
fn removed_string_key_is_absent() {
    assert_scenarios(vec![
        removed_by_writer(Kind::String),
        removed_by_other_after_stability(Kind::String),
    ]);
}

#[test]
fn removed_number_key_is_absent() {
    assert_scenarios(vec![
        removed_by_writer(Kind::Number),
        removed_by_other_after_stability(Kind::Number),
    ]);
}

#[test]
fn removed_boolean_key_is_absent() {
    assert_scenarios(vec![
        removed_by_writer(Kind::Boolean),
        removed_by_other_after_stability(Kind::Boolean),
    ]);
}

#[test]
fn removed_object_key_is_absent() {
    assert_scenarios(vec![
        removed_by_writer(Kind::Object),
        removed_by_other_after_stability(Kind::Object),
    ]);
}

#[test]
fn removed_array_key_is_absent() {
    assert_scenarios(vec![
        removed_by_writer(Kind::Array),
        removed_by_other_after_stability(Kind::Array),
    ]);
}

#[test]
fn removed_nested_key_is_absent() {
    assert_scenarios(vec![
        Scenario::new(
            "nested key removed beside a kept one",
            &["a", "b"],
            json!({"o": {"keep": 1.0}}),
            |run| {
                let w1 = run.send(0, put("o", put("k", ins('a', 0))));
                let w2 = run.send(0, put("o", put("k", ins('b', 1))));
                let w3 = run.send(0, put("o", put("keep", inc(1.0))));
                run.deliver_all(1, &[w1, w2, w3]);
                run.expect(1, json!({"o": {"k": "ab", "keep": 1.0}}));
                run.send(1, remove_nested("o", "k"));
                run.expect(1, json!({"o": {"keep": 1.0}}));
            },
        ),
        // Removing the last key of a nested object is an update to the outer
        // key, which no remove covers: the outer key stays, as {}.
        Scenario::new(
            "last nested key removed by the other replica",
            &["a", "b"],
            json!({"o": {}}),
            |run| {
                let w = run.send(0, put("o", put("k", inc(5.0))));
                run.deliver(1, w);
                run.send(1, remove_nested("o", "k"));
                run.expect(1, json!({"o": {}}));
            },
        ),
        Scenario::new(
            "last nested key removed by its writer",
            &["a", "b"],
            json!({"o": {}}),
            |run| {
                run.send(0, put("o", put("k", inc(1.0))));
                run.send(0, remove_nested("o", "k"));
                run.expect(0, json!({"o": {}}));
            },
        ),
    ]);
}

#[test]
fn removed_key_holding_a_conflict_of_kinds_is_absent() {
    // a puts a string and c concurrently a number; b has seen both and removes k.
    assert_scenarios(vec![Scenario::new(
        "conflict of kinds, both seen by the remove",
        &["a", "b", "c"],
        json!({}),
        |run| {
            let u1 = run.send(0, put("k", ins('a', 0)));
            let u2 = run.send(2, put("k", inc(2.0)));
            run.deliver(1, u1);
            run.deliver(1, u2);
            run.expect(1, json!({"k": [2.0, "a"]}));
            run.send(1, remove("k"));
            run.expect(1, json!({}));
        },
    )]);
}

// ---------------------------------------------------------------------------
// 2. An update concurrent with the remove keeps the key (update-wins)
// ---------------------------------------------------------------------------

/// a fills `k` and b receives it; then a updates `k` with `update` while b
/// concurrently removes `k`.
fn concurrent_update(name: &str, fill: Vec<Json>, update: Json, expected: Value) -> Scenario {
    Scenario::new(name, &["a", "b"], expected, move |run| {
        let mut writes = Vec::new();
        for op in fill.clone() {
            writes.push(run.send(0, put("k", op)));
        }
        run.deliver_all(1, &writes);
        run.send(0, put("k", update.clone()));
        run.send(1, remove("k"));
    })
}

#[test]
fn concurrent_update_keeps_the_key_with_its_effect() {
    assert_scenarios(vec![
        concurrent_update(
            "string insert concurrent with remove",
            vec![ins('a', 0), ins('b', 1)],
            ins('c', 2),
            json!({"k": "c"}),
        ),
        concurrent_update(
            "number increment concurrent with remove",
            vec![inc(5.0)],
            inc(3.0),
            json!({"k": 3.0}),
        ),
        concurrent_update(
            "boolean enable concurrent with remove",
            vec![enable()],
            enable(),
            json!({"k": true}),
        ),
        concurrent_update(
            "boolean disable concurrent with remove",
            vec![enable()],
            disable(),
            json!({"k": false}),
        ),
        concurrent_update(
            "array insert concurrent with remove",
            vec![arr_insert(0, inc(1.0))],
            arr_insert(1, inc(2.0)),
            json!({"k": [2.0]}),
        ),
        concurrent_update(
            "array element update concurrent with remove",
            vec![arr_insert(0, inc(5.0))],
            arr_update(0, inc(2.0)),
            json!({"k": [2.0]}),
        ),
    ]);
}

#[test]
fn concurrent_update_keeps_only_the_fields_the_remove_has_not_seen() {
    assert_scenarios(vec![
        // The remove has seen every update to k.x, so k.x is gone; k holds
        // only the field the concurrent update wrote.
        concurrent_update(
            "object field update concurrent with remove",
            vec![put("x", inc(1.0))],
            put("y", inc(2.0)),
            json!({"k": {"y": 2.0}}),
        ),
        // An empty object put after the remove: k stays, without k.x.
        Scenario::new(
            "object put empty after remove",
            &["a", "b"],
            json!({"k": {}}),
            |run| {
                let w = run.send(0, put("k", put("x", inc(1.0))));
                run.deliver(1, w);
                let r = run.send(1, remove("k"));
                run.deliver(0, r);
                run.send(0, put("k", clear_object()));
                run.expect(0, json!({"k": {}}));
            },
        ),
    ]);
}

#[test]
fn concurrent_inserts_into_removed_text_keep_their_order() {
    // c inserts into "abc" while b removes the key: the remove deletes the
    // characters it saw, the concurrent ones stay, in c's order.
    assert_scenarios(vec![Scenario::new(
        "inserts into removed text",
        &["a", "b", "c"],
        json!({"k": "YX"}),
        |run| {
            let w = vec![
                run.send(0, put("k", ins('a', 0))),
                run.send(0, put("k", ins('b', 1))),
                run.send(0, put("k", ins('c', 2))),
            ];
            run.deliver_all(1, &w);
            run.deliver_all(2, &w);
            run.send(2, put("k", ins('X', 3)));
            run.send(2, put("k", ins('Y', 1)));
            run.expect(2, json!({"k": "aYbcX"}));
            run.send(1, remove("k"));
        },
    )]);
}

#[test]
fn remove_covers_only_the_increment_it_has_seen() {
    // u1 (a) and u2 (c) are concurrent; b's remove has seen u1 only.
    assert_scenarios(vec![Scenario::new(
        "two concurrent increments, one seen by the remove",
        &["a", "b", "c"],
        json!({"k": 2.0}),
        |run| {
            let u1 = run.send(0, put("k", inc(1.0)));
            run.send(2, put("k", inc(2.0)));
            run.deliver(1, u1);
            run.send(1, remove("k"));
        },
    )]);
}

#[test]
fn remove_resolves_a_conflict_of_kinds_it_has_partly_seen() {
    // a puts a string and c concurrently a number; b's remove has seen the
    // string only, so the number stays alone.
    assert_scenarios(vec![Scenario::new(
        "conflict of kinds, one seen by the remove",
        &["a", "b", "c"],
        json!({"k": 2.0}),
        |run| {
            let u1 = run.send(0, put("k", ins('a', 0)));
            run.send(2, put("k", inc(2.0)));
            run.deliver(1, u1);
            run.expect(1, json!({"k": "a"}));
            run.send(1, remove("k"));
            run.expect(1, json!({}));
        },
    )]);
}

// ---------------------------------------------------------------------------
// 3. An empty value set by an update that no remove covers stays
// ---------------------------------------------------------------------------

#[test]
fn empty_values_set_by_uncovered_updates_stay() {
    let cases: Vec<(&str, Vec<Json>, Value)> = vec![
        (
            "string emptied by delete",
            vec![ins('a', 0), del(0)],
            json!(""),
        ),
        ("number incremented by zero", vec![inc(0.0)], json!(0.0)),
        ("number reset", vec![reset()], json!(0.0)),
        ("number back to zero", vec![inc(5.0), dec(5.0)], json!(0.0)),
        ("boolean disabled", vec![disable()], json!(false)),
        (
            "boolean enabled then disabled",
            vec![enable(), disable()],
            json!(false),
        ),
        ("object cleared", vec![clear_object()], json!({})),
        (
            "array element deleted",
            vec![arr_insert(0, inc(1.0)), arr_delete(0)],
            json!([]),
        ),
    ];
    let scenarios = cases
        .into_iter()
        .map(|(name, ops, value)| {
            let expected = json!({ "k": value });
            let local = expected.clone();
            Scenario::new(name, &["a", "b"], expected, move |run| {
                for op in ops.clone() {
                    run.send(0, put("k", op));
                }
                run.expect(0, local.clone());
            })
        })
        .collect();
    assert_scenarios(scenarios);
}

#[test]
fn empty_value_put_after_a_remove_stays() {
    let cases: Vec<(&str, Json, Json, Value)> = vec![
        ("number", inc(5.0), inc(0.0), json!(0.0)),
        ("boolean", enable(), disable(), json!(false)),
    ];
    let scenarios = cases
        .into_iter()
        .map(|(name, fill, empty, value)| {
            Scenario::new(
                format!("{name} put empty after remove"),
                &["a", "b"],
                json!({ "k": value }),
                move |run| {
                    let w = run.send(0, put("k", fill.clone()));
                    run.deliver(1, w);
                    let r = run.send(1, remove("k"));
                    run.deliver(0, r);
                    run.send(0, put("k", empty.clone()));
                },
            )
        })
        .collect();
    assert_scenarios(scenarios);
}

// ---------------------------------------------------------------------------
// 4. After a remove, a put of another kind reads that kind alone
// ---------------------------------------------------------------------------

#[test]
fn put_of_another_kind_after_remove_reads_alone() {
    let mut scenarios = Vec::new();
    for kind in KINDS {
        let next = kind.next();
        let (fresh, fresh_value) = next.fresh();
        // b removes, a puts the new kind after receiving the remove.
        let op = fresh.clone();
        let value = fresh_value.clone();
        scenarios.push(Scenario::new(
            format!("{kind:?} removed by b, then {next:?} put by a"),
            &["a", "b"],
            json!({ "k": fresh_value.clone() }),
            move |run| {
                let mut writes = Vec::new();
                for fill in kind.fill() {
                    writes.push(run.send(0, put("k", fill)));
                }
                run.deliver_all(1, &writes);
                let r = run.send(1, remove("k"));
                run.deliver(0, r);
                run.send(0, put("k", op.clone()));
                run.expect(0, json!({ "k": value.clone() }));
            },
        ));
        // The writer removes and puts the new kind itself.
        let op = fresh;
        let value = fresh_value.clone();
        scenarios.push(Scenario::new(
            format!("{kind:?} removed, then {next:?} put, by a"),
            &["a", "b"],
            json!({ "k": fresh_value }),
            move |run| {
                for fill in kind.fill() {
                    run.send(0, put("k", fill));
                }
                run.send(0, remove("k"));
                run.send(0, put("k", op.clone()));
                run.expect(0, json!({ "k": value.clone() }));
            },
        ));
    }
    assert_scenarios(scenarios);
}

#[test]
fn put_of_the_same_kind_after_remove_starts_empty() {
    assert_scenarios(vec![
        Scenario::new("string", &["a", "b"], json!({"k": "x"}), |run| {
            run.send(0, put("k", ins('a', 0)));
            run.send(0, put("k", ins('b', 1)));
            run.send(0, remove("k"));
            run.send(0, put("k", ins('x', 0)));
        }),
        Scenario::new("number", &["a", "b"], json!({"k": 2.0}), |run| {
            run.send(0, put("k", inc(8.0)));
            run.send(0, remove("k"));
            run.send(0, put("k", inc(2.0)));
        }),
    ]);
}

#[test]
fn new_kind_after_remove_conflicts_with_a_concurrent_old_kind_update() {
    // b removes the string and puts a number; c concurrently inserts into the
    // string. The insert was not seen by the remove, so the string stays,
    // holding only c's character, in conflict with the number.
    assert_scenarios(vec![Scenario::new(
        "number after remove vs concurrent string insert",
        &["a", "b", "c"],
        json!({"k": [7.0, "b"]}),
        |run| {
            let w = run.send(0, put("k", ins('a', 0)));
            run.deliver(1, w);
            run.deliver(2, w);
            run.send(1, remove("k"));
            run.send(1, put("k", inc(7.0)));
            run.expect(1, json!({"k": 7.0}));
            run.send(2, put("k", ins('b', 1)));
            run.expect(2, json!({"k": "ab"}));
        },
    )]);
}
