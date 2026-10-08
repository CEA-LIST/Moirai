//! ST.12: the interpreted fuzz generator after ST.11, parts (1) to (3).
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.12.md`. Part (4), a comment
//! of `moirai-network/tests/e2e_convergence.rs`, has its own oracle in
//! `moirai-network/tests/st12_e2e_comment.rs`.
//!
//! Part (1), `st12_k*`: the keyed branch of `fuzz.rs` writes an existing key
//! with an operation of the class that key holds. On `json.ecore` every JSON
//! object is a keyed collection whose values sit at a family slot (target
//! `Json`, abstract), so a write to a held key that names another class is
//! refused by `check` with `VariantTaken`, and one that names the held class
//! but treats a non-empty object as new is refused with `NotNew`. The ground
//! truth is `check`, read through `ModelLog::refusal`, at two hand-built
//! states (K1a, K1b), over one replica sending what it generates (K2), and
//! the fuzzer's own convergence check over three replicas (K3).
//!
//! Part (2), `st12_o*`: the `LeafRule::OptionalRegister` arm of `leaf_op` is
//! run, on a descriptor built here (no fixture in the tree reaches one from a
//! root the generator starts at): its `Write` and `Clear` are accepted, a
//! register over an enum is written only literals of its own enum, all of
//! them, and replicas converge (the fuzzer, and two partitioned replicas
//! merged). The enum literal-count lookup is written once in `fuzz.rs` (a
//! text check), and the `Enum` arm draws what it drew at `3036102` for the
//! same seeds (a golden captured from the generator at that commit).
//!
//! Part (3), `st12_d*`: the `DeepSizeOf` impls of `ModelLog` and `ModelOp`
//! count what they hold: a log at least its descriptor's bytes, more as
//! operations add keys, and at least the bytes of the values its leaves
//! hold; an operation at least its boxes and its text. A sampled fuzz run
//! completes.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features fuzz --test st12_fuzz_followups
//! ```

#![cfg(feature = "fuzz")]

use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, LazyLock},
};

use deepsize::DeepSizeOf;
use moirai_fuzz::{
    config::{FuzzerConfig, Predicate, RunConfig},
    fuzzer::fuzzer,
    op_generator::CommandGenerator,
};
use moirai_interp::fuzz::set_descriptor;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{
    BT_DESCRIPTOR, JSON_DESCRIPTOR, attribute, bench_descriptor, class, class_slot, descriptor,
    feature_slot, install, objects, opened, table,
};
use moirai_interp::{InstanceOp, MapOp, ModelLog, ModelOp, Node};
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    crdt::query::Read,
    replica::{IsReplica, Replica},
};
use moirai_semantics::{ClassSlot, MetamodelSemantics};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// `fuzz.rs`'s `WORDS`, the alphabet its keyed branch draws a key from. K1a
/// and K1b hold every one of them, so that a draw of a "fresh" key always
/// lands on a held one; both tests check that no other key is ever drawn, so
/// a change to the alphabet shows up as a failed premise and not as a pass.
const WORDS: [&str; 6] = ["alpha", "beta", "gamma", "delta", "door", "room"];

/// The five classes `Json` allows, by name.
const JSON_CLASSES: [&str; 5] = ["Array", "Boolean", "Number", "Object", "String"];

/// Draws per seed and seeds per fixed state, as ST.11's G1 to G3.
const DRAWS: usize = 500;
const SEEDS: u8 = 4;

/// The fewest draws that must reach each key of a fixed state. About 80% of
/// the draws update some key and there are six, so about 260 reach each;
/// this is a floor well under that.
const MIN_PER_KEY: usize = 60;

/// A seed a reader can reproduce: the run's number in the first byte and its
/// complement in the last, as `ip12` writes it.
fn seed(run: u8) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = run;
    seed[31] = !run;
    seed
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a panic without a message".to_string()
    }
}

/// One operation from the generator, a panic inside it (its own
/// `debug_assert!` that `check` accepts what it made) caught as an error.
fn generated(log: &ModelLog, rng: &mut ChaCha8Rng) -> Result<ModelOp, String> {
    catch_unwind(AssertUnwindSafe(|| log.generate_command(rng)))
        .map_err(|payload| format!("the generator panicked: {}", panic_message(payload)))
}

fn send(replica: &mut Rep, op: InstanceOp) -> EventMessage<ModelOp> {
    let description = format!("{op:?}");
    replica
        .send(ModelOp::Instance(op))
        .unwrap_or_else(|refusal| panic!("the scenario's own {description} was refused: {refusal}"))
}

fn read(replica: &Rep) -> Value {
    replica.query(&Read::<Value>::new())
}

// ===========================================================================
// Part (1): json.ecore
// ===========================================================================

fn json_descriptor() -> Value {
    serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON")
}

static JSON: LazyLock<Arc<MetamodelSemantics>> = LazyLock::new(|| table(&json_descriptor()));

fn on(class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
    let class = class_slot(&JSON, class);
    InstanceOp::variant(
        class,
        InstanceOp::field(feature_slot(&JSON, class, feature), inner),
    )
}

/// `entry(key, value)` on the root object.
fn put(key: &str, value: InstanceOp) -> InstanceOp {
    on(
        "Object",
        "entry",
        InstanceOp::entry(Scalar::text(key), value),
    )
}

fn remove(key: &str) -> InstanceOp {
    on("Object", "entry", InstanceOp::remove(Scalar::text(key)))
}

fn ins(content: char, pos: usize) -> InstanceOp {
    on(
        "String",
        "value",
        InstanceOp::Leaf(LeafOp::InsertChar { pos, ch: content }),
    )
}

fn inc(value: f64) -> InstanceOp {
    on(
        "Number",
        "value",
        InstanceOp::Leaf(LeafOp::Inc(Scalar::float(value))),
    )
}

fn enable() -> InstanceOp {
    on("Boolean", "value", InstanceOp::Leaf(LeafOp::Enable))
}

/// An array holding one number.
fn array_of_one() -> InstanceOp {
    on("Array", "items", InstanceOp::insert(0, inc(1.0)))
}

/// An object holding one entry, at a key that is not one of `WORDS`.
fn object_of_one() -> InstanceOp {
    on(
        "Object",
        "entry",
        InstanceOp::entry(Scalar::text("k"), inc(1.0)),
    )
}

fn json_name(class: ClassSlot) -> String {
    JSON.classes[class.index()].name.to_string()
}

/// The classes of the objects held at one key of the root object, absent
/// ones included, by name and sorted.
fn held_at(log: &ModelLog, key: &str) -> Vec<String> {
    let root = objects(log.root());
    assert_eq!(root.len(), 1, "one root object, not {}", root.len());
    let entry = feature_slot(&JSON, class_slot(&JSON, "Object"), "entry");
    let Some(Node::Map(map)) = root[0].fields().get(&entry) else {
        panic!("the root object's `entry` is not a keyed collection");
    };
    let Some(slot) = map.children().get(&Scalar::text(key)) else {
        panic!("the root object holds no entry `{key}`");
    };
    let mut names: Vec<String> = objects(slot)
        .iter()
        .map(|object| json_name(object.class()))
        .collect();
    names.sort();
    names
}

/// The key and the class a generated operation names, if it updates a key of
/// the root object with an operation on an object.
fn key_and_class(op: &ModelOp) -> Option<(String, ClassSlot)> {
    let object = class_slot(&JSON, "Object");
    let entry = feature_slot(&JSON, object, "entry");
    let ModelOp::Instance(InstanceOp::Variant(class, inner)) = op else {
        return None;
    };
    if *class != object {
        return None;
    }
    let InstanceOp::Field(feature, inner) = inner.as_ref() else {
        return None;
    };
    if *feature != entry {
        return None;
    }
    let InstanceOp::Map(MapOp::Update { key, op }) = inner.as_ref() else {
        return None;
    };
    let Scalar::Str(key) = key else {
        return None;
    };
    match op.as_ref() {
        InstanceOp::Variant(class, _) => Some((key.clone(), *class)),
        _ => None,
    }
}

/// What the draws against one fixed state came to.
struct Draws {
    /// Classes named at each key, one entry per draw that reached it.
    at: BTreeMap<String, Vec<ClassSlot>>,
    /// The first refused (or generator-panicking) draw of each seed.
    failures: Vec<String>,
}

/// `SEEDS * DRAWS` operations from the generator against `log`, which is not
/// changed. A seed stops at its first failure.
fn draw(label: &str, log: &ModelLog) -> Draws {
    let mut at: BTreeMap<String, Vec<ClassSlot>> = BTreeMap::new();
    let mut failures = Vec::new();
    for run in 0..SEEDS {
        let mut rng = ChaCha8Rng::from_seed(seed(run));
        for step in 0..DRAWS {
            let op = match generated(log, &mut rng) {
                Ok(op) => op,
                Err(why) => {
                    failures.push(format!("{label}, seed {run}, draw {step}: {why}"));
                    break;
                }
            };
            if let Err(refusal) = log.refusal(&op) {
                failures.push(format!(
                    "{label}, seed {run}, draw {step}: the log refuses {op:?}: {refusal}"
                ));
                break;
            }
            if let Some((key, class)) = key_and_class(&op) {
                at.entry(key).or_default().push(class);
            }
        }
    }
    Draws { at, failures }
}

/// The per-key report the K1 tests assert on: for each key the generator
/// reached, how many draws and which classes, against what was expected.
fn report(draws: &Draws, expected: &BTreeMap<&str, BTreeSet<String>>) -> Vec<String> {
    let mut wrong = Vec::new();
    for key in draws.at.keys() {
        if !expected.contains_key(key.as_str()) {
            wrong.push(format!(
                "`{key}`: a key the state does not hold and `WORDS` does not name was drawn \
                 (the oracle's premise is that `fuzz.rs` draws keys from `WORDS`)"
            ));
        }
    }
    for (key, classes) in expected {
        let drawn = draws.at.get(*key).map_or(&[][..], Vec::as_slice);
        let names: BTreeSet<String> = drawn.iter().copied().map(json_name).collect();
        if drawn.len() < MIN_PER_KEY || &names != classes {
            wrong.push(format!(
                "`{key}`: {} draws (at least {MIN_PER_KEY} expected) naming {names:?}, \
                 where {classes:?} is expected",
                drawn.len()
            ));
        }
    }
    wrong
}

fn one(class: &str) -> BTreeSet<String> {
    BTreeSet::from([class.to_string()])
}

fn every_json_class() -> BTreeSet<String> {
    JSON_CLASSES.iter().map(|name| name.to_string()).collect()
}

/// K1a: every word of `WORDS` is a key of the root object holding one
/// present object, of a different class at each key but `room`, which holds
/// an absent string beside a present number (a retype after a remove, as
/// ST.11's G1). Every draw must be accepted, and every draw that reaches a
/// key must name the class present there and nothing else. False at
/// `3036102`: a draw of a "fresh" word builds its value as if the key were
/// new (`fuzz.rs:214-215`) and offers any of the five classes, which `check`
/// refuses with `VariantTaken`, or `New` on a non-empty object, refused with
/// `NotNew`.
#[test]
fn st12_k1a_a_held_key_is_written_with_the_class_it_holds() {
    let (mut a, _b) = opened("st12", &json_descriptor());
    send(&mut a, put("alpha", inc(2.0)));
    send(&mut a, put("beta", ins('a', 0)));
    send(&mut a, put("beta", ins('b', 1)));
    send(&mut a, put("gamma", array_of_one()));
    send(&mut a, put("delta", enable()));
    send(&mut a, put("door", object_of_one()));
    send(&mut a, put("room", ins('r', 0)));
    send(&mut a, remove("room"));
    send(&mut a, put("room", inc(5.0)));

    // The state is the one described, before any draw.
    assert_eq!(
        read(&a),
        json!({
            "alpha": 2.0, "beta": "ab", "gamma": [1.0], "delta": true,
            "door": {"k": 1.0}, "room": 5.0
        }),
        "K1a: the read"
    );
    for (key, class) in [
        ("alpha", "Number"),
        ("beta", "String"),
        ("gamma", "Array"),
        ("delta", "Boolean"),
        ("door", "Object"),
    ] {
        assert_eq!(
            held_at(a.state(), key),
            vec![class.to_string()],
            "K1a: `{key}`"
        );
    }
    assert_eq!(
        held_at(a.state(), "room"),
        vec!["Number".to_string(), "String".to_string()],
        "K1a: `room` holds the absent string beside the number"
    );

    let draws = draw("K1a on a", a.state());
    let expected: BTreeMap<&str, BTreeSet<String>> = BTreeMap::from([
        ("alpha", one("Number")),
        ("beta", one("String")),
        ("gamma", one("Array")),
        ("delta", one("Boolean")),
        ("door", one("Object")),
        ("room", one("Number")),
    ]);
    let wrong = report(&draws, &expected);
    assert!(
        draws.failures.is_empty() && wrong.is_empty(),
        "K1a: {} refused or panicking seed(s), {} key(s) drawn wrongly\n{}\n{}",
        draws.failures.len(),
        wrong.len(),
        draws.failures.join("\n"),
        wrong.join("\n")
    );
}

/// K1b: every word is a key of the root object whose one object has been
/// removed, so the slot at each key serves nothing. Every draw must be
/// accepted and the draws at each key must name all five classes: a held key
/// is written with the class it *serves*, and one that serves none is as
/// open as a new key. Passes at `3036102`; it is here so that a fix which
/// reads the class from an absent member is not taken for the claim's.
#[test]
fn st12_k1b_a_key_whose_object_was_removed_is_open_to_every_class() {
    let (mut a, _b) = opened("st12", &json_descriptor());
    send(&mut a, put("alpha", inc(2.0)));
    send(&mut a, put("beta", ins('a', 0)));
    send(&mut a, put("gamma", array_of_one()));
    send(&mut a, put("delta", enable()));
    send(&mut a, put("door", object_of_one()));
    send(&mut a, put("room", ins('r', 0)));
    for word in WORDS {
        send(&mut a, remove(word));
    }
    assert_eq!(read(&a), json!({}), "K1b: the read");
    for word in WORDS {
        assert_eq!(
            held_at(a.state(), word).len(),
            1,
            "K1b: `{word}` holds one object"
        );
    }

    let draws = draw("K1b on a", a.state());
    let expected: BTreeMap<&str, BTreeSet<String>> = WORDS
        .iter()
        .map(|word| (*word, every_json_class()))
        .collect();
    let wrong = report(&draws, &expected);
    assert!(
        draws.failures.is_empty() && wrong.is_empty(),
        "K1b: {} refused or panicking seed(s), {} key(s) drawn wrongly\n{}\n{}",
        draws.failures.len(),
        wrong.len(),
        draws.failures.join("\n"),
        wrong.join("\n")
    );
}

/// Seeds and steps of K2.
const K2_SEEDS: u8 = 8;
const K2_STEPS: usize = 400;

/// K2: one replica on `json.ecore` sends what the generator gives it, from
/// an empty model, `K2_SEEDS` seeds of `K2_STEPS` steps; every operation must
/// be one `check` accepts and the replica takes. ST.11's rehearsal found the
/// first refusal at seed 3, step 28 (held `Number`, offered `String`).
#[test]
fn st12_k2_one_replica_sending_what_it_generates_on_json_is_never_refused() {
    let mut failures = Vec::new();
    for run in 0..K2_SEEDS {
        let (mut a, _b) = opened("st12", &json_descriptor());
        let mut rng = ChaCha8Rng::from_seed(seed(run));
        for step in 0..K2_STEPS {
            let op = match generated(a.state(), &mut rng) {
                Ok(op) => op,
                Err(why) => {
                    failures.push(format!("seed {run}, step {step}: {why}"));
                    break;
                }
            };
            if let Err(refusal) = a.state().refusal(&op) {
                failures.push(format!(
                    "seed {run}, step {step}: the log refuses {op:?}: {refusal}"
                ));
                break;
            }
            if let Err(refusal) = a.send(op) {
                failures.push(format!(
                    "seed {run}, step {step}: the replica refuses: {refusal}"
                ));
                break;
            }
        }
        assert_eq!(a.state().unresolved(), 0, "K2: seed {run}");
    }
    assert!(
        failures.is_empty(),
        "K2: {} of {K2_SEEDS} seeds refused:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// K3: the fuzzer over `json.ecore`, three replicas, 300 operations, four
/// seeds, churn 0.4, final merge. `fuzzer` panics on a generated operation a
/// replica refuses, on replicas that do not converge under `Read<Value>`
/// equality, and on replicas ending with different event counts.
#[test]
fn st12_k3_the_fuzzer_over_json_converges() {
    set_descriptor(JSON_DESCRIPTOR);
    let runs: Vec<RunConfig> = (0..4)
        .map(|run| RunConfig::new(0.4, 3, 300, None, Some(seed(run)), false, false))
        .collect();
    let config = FuzzerConfig::<ModelLog, Read<Value>>::new(
        "interp-json",
        runs,
        true,
        Predicate::new(Read::new(), |left, right| left == right),
        false,
    );
    fuzzer::<ModelLog, Read<Value>>(config);
}

// ===========================================================================
// Part (2): the optional register, and the enum literal count
// ===========================================================================

fn keyed_optional_register(name: &str, enumeration: Option<&str>) -> Value {
    let leaf = match enumeration {
        Some(class) => json!({"kind": "optionalRegister", "class": class}),
        None => json!({"kind": "optionalRegister"}),
    };
    attribute(name, json!({"kind": "keyed", "key": {"kind": "str"}}), leaf)
}

/// `Doc`, the root, holds three keyed collections whose values are registers
/// over an optional value (`EAnnotation.details`'s construction): `details`
/// over text, `levels` over the enum `Level` (three literals) and `tones`
/// over `Tone` (five), so that a literal drawn from the wrong enum's count is
/// told apart.
fn optreg_descriptor() -> Value {
    descriptor(
        &["Doc"],
        vec![(
            "Doc",
            class(
                false,
                &[],
                vec![
                    keyed_optional_register("details", None),
                    keyed_optional_register("levels", Some("Level")),
                    keyed_optional_register("tones", Some("Tone")),
                ],
            ),
        )],
        json!({"Level": ["LOW", "MID", "HIGH"], "Tone": ["C", "D", "E", "F", "G"]}),
    )
}

static OPTREG: LazyLock<Arc<MetamodelSemantics>> = LazyLock::new(|| table(&optreg_descriptor()));

fn enum_slot(sem: &MetamodelSemantics, name: &str) -> u16 {
    sem.enums
        .iter()
        .find(|entry| &*entry.name == name)
        .unwrap_or_else(|| panic!("no enum `{name}`"))
        .slot
        .0
}

/// The feature and the map operation of a generated operation on `Doc`, if
/// it addresses one of its keyed collections.
fn doc_map(op: &ModelOp) -> Option<(String, &MapOp<Box<InstanceOp>>)> {
    let doc = class_slot(&OPTREG, "Doc");
    let ModelOp::Instance(InstanceOp::Variant(class, inner)) = op else {
        return None;
    };
    if *class != doc {
        return None;
    }
    let InstanceOp::Field(feature, inner) = inner.as_ref() else {
        return None;
    };
    let InstanceOp::Map(map) = inner.as_ref() else {
        return None;
    };
    let name = OPTREG.classes[doc.index()].visible[feature.index()]
        .0
        .to_string();
    Some((name, map))
}

/// The feature and the leaf operation of a generated operation on `Doc`, if
/// it writes the value at a key.
fn doc_leaf(op: &ModelOp) -> Option<(String, LeafOp)> {
    let (name, MapOp::Update { op, .. }) = doc_map(op)? else {
        return None;
    };
    let InstanceOp::Leaf(leaf) = op.as_ref() else {
        return None;
    };
    Some((name, leaf.clone()))
}

/// Keeps `nulls`, the keys (feature, key as the read names it) whose last
/// operation wrote `Scalar::Null`, up to date with one more operation: a
/// write of `Scalar::Null` adds its key, any other operation at a key takes
/// it out, and a clear of a collection takes out every key of it.
fn track_nulls(nulls: &mut BTreeSet<(String, String)>, op: &ModelOp) {
    let sem: &MetamodelSemantics = &OPTREG;
    match doc_map(op) {
        Some((feature, MapOp::Update { key, op })) => {
            let at = (feature, key.to_key(Some(sem)));
            if matches!(op.as_ref(), InstanceOp::Leaf(LeafOp::Write(Scalar::Null))) {
                nulls.insert(at);
            } else {
                nulls.remove(&at);
            }
        }
        Some((feature, MapOp::Remove { key })) => {
            nulls.remove(&(feature, key.to_key(Some(sem))));
        }
        Some((feature, MapOp::Clear)) => nulls.retain(|(at, _)| *at != feature),
        None => {}
    }
}

/// O1: one replica on the optional-register descriptor sends what the
/// generator gives it, four seeds of 400 steps. Every operation is accepted;
/// at each feature the arm writes and clears; `details` is written text
/// only; `levels` and `tones` are written literals of their own enum only,
/// every literal of it; and the read renders every value under `levels` and
/// `tones` as a literal name.
///
/// Since ST.13 (ruling R1, Cam, 2026-10-08) the arm also writes
/// `Scalar::Null`, the absent value. Such a write is classed nowhere, and the
/// read may render `null` only at a key whose last operation in the run wrote
/// `Scalar::Null`: one replica sends every operation in order, so a key reads
/// what the last operation at it left. Every other value under `levels` and
/// `tones` is still a literal name. A key whose last operation was a `Clear`,
/// a removal of the key or a clear of its collection reads `[]`, which the
/// keyed read leaves out, so it renders nothing. What a write of
/// `Scalar::Null` reads back as is held in `st13_optional_null.rs`'s N1.
#[test]
fn st12_o1_the_optional_register_arm_writes_clears_and_names_its_own_literals() {
    let level = enum_slot(&OPTREG, "Level");
    let tone = enum_slot(&OPTREG, "Tone");
    let mut failures = Vec::new();
    let mut writes: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut clears: BTreeMap<String, usize> = BTreeMap::new();
    let mut ops: BTreeMap<String, usize> = BTreeMap::new();
    let mut literals: BTreeMap<String, BTreeSet<u16>> = BTreeMap::new();
    // Each value the read renders, as its JSON text (`"LOW"`, or `7` for a
    // literal the enum does not have), but for a `null` at a key whose last
    // operation wrote `Scalar::Null`.
    let mut rendered: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for run in 0..4u8 {
        let (mut a, _b) = opened("st12", &optreg_descriptor());
        let mut rng = ChaCha8Rng::from_seed(seed(run));
        // The keys whose last operation in this run wrote `Scalar::Null`.
        let mut nulls: BTreeSet<(String, String)> = BTreeSet::new();
        for step in 0..400 {
            let op = match generated(a.state(), &mut rng) {
                Ok(op) => op,
                Err(why) => {
                    failures.push(format!("seed {run}, step {step}: {why}"));
                    break;
                }
            };
            if let Err(refusal) = a.state().refusal(&op) {
                failures.push(format!(
                    "seed {run}, step {step}: the log refuses {op:?}: {refusal}"
                ));
                break;
            }
            track_nulls(&mut nulls, &op);
            if let Some((feature, leaf)) = doc_leaf(&op) {
                *ops.entry(feature.clone()).or_default() += 1;
                match leaf {
                    LeafOp::Clear => *clears.entry(feature).or_default() += 1,
                    LeafOp::Write(Scalar::Null) => {}
                    LeafOp::Write(value) => {
                        let kind = match (&value, feature.as_str()) {
                            (Scalar::Str(_), "details") => "text".to_string(),
                            (Scalar::Enum(class, literal), "levels") if *class == level => {
                                literals
                                    .entry(feature.clone())
                                    .or_default()
                                    .insert(*literal);
                                "own enum".to_string()
                            }
                            (Scalar::Enum(class, literal), "tones") if *class == tone => {
                                literals
                                    .entry(feature.clone())
                                    .or_default()
                                    .insert(*literal);
                                "own enum".to_string()
                            }
                            (other, _) => format!("unexpected {other:?}"),
                        };
                        writes.entry(feature).or_default().insert(kind);
                    }
                    other => {
                        writes
                            .entry(feature)
                            .or_default()
                            .insert(format!("not a register operation: {other:?}"));
                    }
                }
            }
            if let Err(refusal) = a.send(op) {
                failures.push(format!(
                    "seed {run}, step {step}: the replica refuses: {refusal}"
                ));
                break;
            }
        }
        let state = read(&a);
        for feature in ["levels", "tones"] {
            if let Some(map) = state.get(feature).and_then(Value::as_object) {
                for (key, values) in map {
                    let null_written = nulls.contains(&(feature.to_string(), key.clone()));
                    for value in values.as_array().into_iter().flatten() {
                        if value.is_null() && null_written {
                            continue;
                        }
                        rendered
                            .entry(feature.to_string())
                            .or_default()
                            .insert(value.to_string());
                    }
                }
            }
        }
    }

    let mut wrong = Vec::new();
    let expect = |feature: &str, kind: &str, count: u16| {
        let mut wrong = Vec::new();
        let n = ops.get(feature).copied().unwrap_or(0);
        if n < 40 {
            wrong.push(format!(
                "`{feature}`: {n} register operations (at least 40 expected)"
            ));
        }
        if clears.get(feature).copied().unwrap_or(0) == 0 {
            wrong.push(format!("`{feature}`: never cleared"));
        }
        let kinds = writes.get(feature).cloned().unwrap_or_default();
        if kinds != BTreeSet::from([kind.to_string()]) {
            wrong.push(format!(
                "`{feature}`: writes of {kinds:?}, where only {kind:?} is expected"
            ));
        }
        if count > 0 {
            let drawn = literals.get(feature).cloned().unwrap_or_default();
            let all: BTreeSet<u16> = (0..count).collect();
            if drawn != all {
                wrong.push(format!(
                    "`{feature}`: literals {drawn:?} drawn, where every one of {all:?} is expected"
                ));
            }
        }
        wrong
    };
    wrong.extend(expect("details", "text", 0));
    wrong.extend(expect("levels", "own enum", 3));
    wrong.extend(expect("tones", "own enum", 5));
    for (feature, names) in [
        ("levels", &["LOW", "MID", "HIGH"][..]),
        ("tones", &["C", "D", "E", "F", "G"][..]),
    ] {
        let allowed: BTreeSet<String> = names.iter().map(|name| json!(name).to_string()).collect();
        let got = rendered.get(feature).cloned().unwrap_or_default();
        if got.is_empty() || !got.is_subset(&allowed) {
            wrong.push(format!(
                "`{feature}`: the read renders {got:?} (a `null` at a key whose last operation \
                 wrote `Scalar::Null` left out), where a non-empty subset of {allowed:?} is \
                 expected"
            ));
        }
    }
    assert!(
        failures.is_empty() && wrong.is_empty(),
        "O1: {} refused or panicking seed(s), {} wrong\n{}\n{}",
        failures.len(),
        wrong.len(),
        failures.join("\n"),
        wrong.join("\n")
    );
}

/// O2: the fuzzer over the optional-register descriptor, three replicas,
/// 300 operations, three seeds, churn 0.4, final merge.
#[test]
fn st12_o2_the_fuzzer_over_optional_registers_converges() {
    set_descriptor(&optreg_descriptor().to_string());
    let runs: Vec<RunConfig> = (0..3)
        .map(|run| RunConfig::new(0.4, 3, 300, None, Some(seed(run)), false, false))
        .collect();
    let config = FuzzerConfig::<ModelLog, Read<Value>>::new(
        "interp-optreg",
        runs,
        true,
        Predicate::new(Read::new(), |left, right| left == right),
        false,
    );
    fuzzer::<ModelLog, Read<Value>>(config);
}

/// O3: two replicas, partitioned, each sends 150 operations of its own seed
/// with nothing delivered, then each delivers everything the other sent. The
/// reads agree, and at least one key holds two values or more: two writes
/// the replicas made concurrently, both kept by the register.
#[test]
fn st12_o3_two_partitioned_replicas_writing_optional_registers_converge() {
    let (mut a, mut b) = opened("st12", &optreg_descriptor());
    let mut from_a = Vec::new();
    let mut from_b = Vec::new();
    let mut rng_a = ChaCha8Rng::from_seed(seed(10));
    let mut rng_b = ChaCha8Rng::from_seed(seed(11));
    for step in 0..150 {
        let op = generated(a.state(), &mut rng_a).unwrap_or_else(|why| panic!("a, {step}: {why}"));
        from_a.push(
            a.send(op)
                .unwrap_or_else(|why| panic!("a, {step}: refused: {why}")),
        );
        let op = generated(b.state(), &mut rng_b).unwrap_or_else(|why| panic!("b, {step}: {why}"));
        from_b.push(
            b.send(op)
                .unwrap_or_else(|why| panic!("b, {step}: refused: {why}")),
        );
    }
    for message in from_a {
        b.receive(message);
    }
    for message in from_b {
        a.receive(message);
    }
    let (left, right) = (read(&a), read(&b));
    assert_eq!(left, right, "O3: the replicas agree after the exchange");
    let concurrent = ["details", "levels", "tones"].iter().any(|feature| {
        left.get(*feature)
            .and_then(Value::as_object)
            .is_some_and(|map| {
                map.values()
                    .any(|values| values.as_array().is_some_and(|values| values.len() >= 2))
            })
    });
    assert!(
        concurrent,
        "O3: no key holds two concurrent values, so the schedule exercised no conflict: {left}"
    );
}

/// The `Enum` arm's own descriptor: `Panel` with three enum attributes, one
/// per clearable tie and one not, over enums of three, five and two literals.
fn enums_descriptor() -> Value {
    descriptor(
        &["Panel"],
        vec![(
            "Panel",
            class(
                false,
                &[],
                vec![
                    attribute(
                        "status",
                        json!({"kind": "single"}),
                        json!({"kind": "enum", "class": "Status", "tie": "mv"}),
                    ),
                    attribute(
                        "mode",
                        json!({"kind": "single"}),
                        json!({"kind": "enum", "class": "Mode", "tie": "lww"}),
                    ),
                    attribute(
                        "phase",
                        json!({"kind": "single"}),
                        json!({"kind": "enum", "class": "Phase", "tie": "po"}),
                    ),
                ],
            ),
        )],
        json!({
            "Status": ["RUNNING", "SUCCESS", "FAILURE"],
            "Mode": ["A", "B", "C", "D", "E"],
            "Phase": ["ON", "OFF"]
        }),
    )
}

/// One draw on `Panel`, as a token: `N` for `New`, else the feature's first
/// letter and `W<enum>.<literal>` or `C` for a clear.
fn enum_token(sem: &MetamodelSemantics, op: &ModelOp) -> String {
    let ModelOp::Instance(InstanceOp::Variant(class, inner)) = op else {
        return format!("?{op:?}");
    };
    match inner.as_ref() {
        InstanceOp::New => "N".to_string(),
        InstanceOp::Field(feature, inner) => {
            let name = &sem.classes[class.index()].visible[feature.index()].0;
            let letter = name.chars().next().unwrap_or('?');
            match inner.as_ref() {
                InstanceOp::Leaf(LeafOp::Write(Scalar::Enum(class, literal))) => {
                    format!("{letter}W{class}.{literal}")
                }
                InstanceOp::Leaf(LeafOp::Clear) => format!("{letter}C"),
                other => format!("{letter}?{other:?}"),
            }
        }
        other => format!("?{other:?}"),
    }
}

/// What the generator drew at `3036102`, 40 draws for each of seeds 0 to 3,
/// against `enums_descriptor`'s log freshly installed, through
/// `enum_tokens`. Captured from the generator at that commit (spec section
/// 3); the refactor of part (2) must not move a single draw.
const ENUM_GOLDEN: [&str; 4] = [
    "mW0.2 sW2.2 mW0.1 N N sW2.2 sW2.1 N N mW0.3 pW1.1 pW1.0 sW2.1 pW1.0 mW0.1 mW0.3 pW1.0 N pW1.0 mW0.2 \
     mW0.2 sW2.0 pW1.1 sC mW0.0 sW2.1 sC sW2.0 mW0.2 sW2.1 pC sW2.0 pW1.1 N pC pW1.1 sW2.1 sW2.0 mW0.3 sW2.0",
    "pW1.1 sC sW2.0 mW0.2 sW2.0 mW0.3 mW0.1 mW0.1 sW2.1 pW1.1 sW2.2 N N mW0.0 mW0.4 pW1.1 pW1.0 mW0.4 \
     pW1.1 N mW0.0 sC sW2.1 pW1.1 N mW0.1 pW1.0 pW1.1 sW2.2 pW1.0 pW1.0 mW0.0 mW0.4 sW2.0 mW0.3 pW1.0 \
     pW1.1 mW0.0 mW0.2 mW0.2",
    "mW0.4 sW2.0 N mW0.4 sW2.2 sC mW0.3 sW2.1 pW1.0 mW0.2 mW0.4 N sW2.1 pW1.1 N pC pW1.0 pW1.1 sW2.2 N \
     pW1.1 sC mW0.1 sW2.2 sW2.2 sW2.1 mW0.3 sC pW1.0 mW0.4 mW0.4 pW1.1 N mW0.2 mW0.3 sW2.1 sW2.0 pC N \
     sW2.2",
    "N mW0.4 pW1.0 N sW2.1 mW0.4 sW2.1 sW2.2 mW0.1 pW1.1 pW1.0 mW0.2 sW2.0 N N mW0.0 sW2.1 mW0.0 pC \
     mW0.0 mW0.4 pW1.0 sW2.0 pW1.1 mW0.2 pW1.0 sC sW2.1 sW2.2 sW2.1 pW1.1 sW2.0 pW1.1 pW1.1 pW1.0 pC \
     pW1.0 mW0.2 N pW1.0",
];

fn enum_tokens() -> Vec<String> {
    let sem = table(&enums_descriptor());
    let (a, _b) = opened("st12", &enums_descriptor());
    (0..4u8)
        .map(|run| {
            let mut rng = ChaCha8Rng::from_seed(seed(run));
            (0..40)
                .map(|step| {
                    let op = generated(a.state(), &mut rng)
                        .unwrap_or_else(|why| panic!("seed {run}, draw {step}: {why}"));
                    a.state()
                        .refusal(&op)
                        .unwrap_or_else(|why| panic!("seed {run}, draw {step}: refused: {why}"));
                    enum_token(&sem, &op)
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

/// O4: the `Enum` arm draws, for the same seeds, what it drew before the
/// literal count moved into a helper.
#[test]
fn st12_o4_the_enum_arm_draws_what_it_drew_for_the_same_seeds() {
    let tokens = enum_tokens();
    let moved: Vec<String> = tokens
        .iter()
        .zip(ENUM_GOLDEN)
        .enumerate()
        .filter(|(_, (got, want))| got != want)
        .map(|(run, (got, want))| format!("seed {run}:\n  drew {got:?}\n  was  {want:?}"))
        .collect();
    assert!(
        moved.is_empty(),
        "O4: the `Enum` arm's draws moved at {} of 4 seeds\n{}",
        moved.len(),
        moved.join("\n")
    );
}

/// `fuzz.rs` with its `//` comments dropped and all whitespace removed.
fn fuzz_rs_code() -> String {
    include_str!("../src/fuzz.rs")
        .lines()
        .map(|line| line.find("//").map_or(line, |at| &line[..at]))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// How many times `code` reads the field `name`: `.name` not preceded by
/// another `.` (so `0..literals`, a range over a local, is not one) and not
/// followed by an identifier character.
fn field_reads(code: &str, name: &str) -> usize {
    let needle = format!(".{name}");
    code.match_indices(&needle)
        .filter(|(at, _)| !code[..*at].ends_with('.'))
        .filter(|(at, _)| {
            code[at + needle.len()..]
                .chars()
                .next()
                .is_none_or(|next| !(next.is_alphanumeric() || next == '_'))
        })
        .count()
}

/// O5: the enum literal-count lookup (`sem.enums.get(..)` then
/// `.literals.len()`) is written once in `fuzz.rs`, not once per arm. False
/// at `3036102`: the `Enum` arm (`fuzz.rs:321-324`) and the
/// `OptionalRegister` arm (`fuzz.rs:338-341`) each spell it.
#[test]
fn st12_o5_the_enum_literal_count_is_looked_up_in_one_place() {
    let code = fuzz_rs_code();
    let literals = field_reads(&code, "literals");
    let enums = field_reads(&code, "enums");
    assert!(
        literals == 1 && enums <= 1,
        "O5: fuzz.rs reads `.literals` {literals} time(s) and `.enums` {enums} time(s); \
         one shared lookup reads `.literals` once and `.enums` at most once"
    );
}

// ===========================================================================
// Part (3): the deep sizes
// ===========================================================================

/// What a value holds on the heap, as `deepsize` counts it: its deep size
/// less its own inline size.
fn heap_of<T: DeepSizeOf>(value: &T) -> usize {
    value.deep_size_of() - size_of::<T>()
}

/// How many long values D1 adds to a set, and how long each is.
const LONG_VALUES: usize = 10;
const LONG_BYTES: usize = 1_000;

/// D1: a log's deep size counts what it holds, its leaves' contents
/// included.
///
/// On `json.ecore`: once installed the log holds its descriptor verbatim, so
/// at least that many bytes; 64 new keys add at least one `Node` each. On
/// the bench descriptor: `LONG_VALUES` distinct strings of `LONG_BYTES`
/// bytes added to the add-wins set `setAw` are held by that leaf, so they
/// add at least their bytes. The second check is the one only a count of
/// the leaves meets: what the tree around a leaf keeps per operation (its
/// touches) is a few dozen bytes, not a thousand. False at `3036102`: the
/// impl returns 0 children.
#[test]
fn st12_d1_a_logs_deep_size_counts_what_it_holds() {
    let (mut a, _b) = opened("st12", &json_descriptor());
    let installed = heap_of(a.state());
    let descriptor = a.state().descriptor().expect("installed").len();
    for key in 0..64 {
        send(&mut a, put(&format!("key{key:02}"), ins('x', 0)));
    }
    let keyed = heap_of(a.state());

    let bench = table(&bench_descriptor());
    let root = class_slot(&bench, "Bench");
    let add = |value: Scalar| {
        InstanceOp::variant(
            root,
            InstanceOp::field(
                feature_slot(&bench, root, "setAw"),
                InstanceOp::Leaf(LeafOp::Add(value)),
            ),
        )
    };
    let (mut b, _c) = opened("st12", &bench_descriptor());
    send(&mut b, add(Scalar::text("short")));
    let before = heap_of(b.state());
    for value in 0..LONG_VALUES {
        let long = format!("{value:04}{}", "x".repeat(LONG_BYTES - 4));
        send(&mut b, add(Scalar::text(long)));
    }
    let after = heap_of(b.state());

    let mut wrong = Vec::new();
    if installed < descriptor {
        wrong.push(format!(
            "installed: {installed} bytes counted, under the {descriptor} bytes of the \
             descriptor the log holds"
        ));
    }
    let keys = 64 * size_of::<Node>();
    if keyed < installed + keys {
        wrong.push(format!(
            "64 keys added {} bytes, under 64 nodes' {keys}",
            keyed.saturating_sub(installed)
        ));
    }
    let values = LONG_VALUES * LONG_BYTES;
    if after < before + values {
        wrong.push(format!(
            "{LONG_VALUES} values of {LONG_BYTES} bytes added to a set added {} bytes, \
             under their {values}",
            after.saturating_sub(before)
        ));
    }
    assert!(
        wrong.is_empty(),
        "D1: the log's deep size (heap: json {installed} installed, {keyed} with 64 keys; \
         bench {before} before the long values, {after} after) does not count what the log \
         holds:\n{}",
        wrong.join("\n")
    );
}

/// D2: an operation's deep size counts its boxes and its text. `Install`
/// holds three strings. `put("k", inc(1.0))` is five boxed `InstanceOp`s
/// (`Variant`, `Field`, the map's `Update`, then `Variant` and `Field` of the
/// number) and the one byte of `"k"`; nesting it one object deeper adds three
/// boxes (`Variant`, `Field`, `Update`) and the byte of the inner `"k"`.
/// False at `3036102`: the impl returns 0 children.
#[test]
fn st12_d2_an_operations_deep_size_counts_its_boxes_and_text() {
    let mut wrong = Vec::new();

    let opening = install("st12", &json_descriptor());
    let ModelOp::Install {
        model_id,
        metamodel_id,
        descriptor,
    } = &opening
    else {
        unreachable!("`install` builds an `Install`");
    };
    let text = model_id.len() + metamodel_id.len() + descriptor.len();
    if heap_of(&opening) < text {
        wrong.push(format!(
            "Install: {} bytes counted, under its strings' {text}",
            heap_of(&opening)
        ));
    }

    let boxed = size_of::<InstanceOp>();
    let shallow = ModelOp::Instance(put("k", inc(1.0)));
    let deep = ModelOp::Instance(put(
        "k",
        on(
            "Object",
            "entry",
            InstanceOp::entry(Scalar::text("k"), inc(1.0)),
        ),
    ));
    if heap_of(&shallow) < 5 * boxed + 1 {
        wrong.push(format!(
            "put(k, inc): {} bytes counted, under five boxes and one byte, {}",
            heap_of(&shallow),
            5 * boxed + 1
        ));
    }
    if heap_of(&deep) < heap_of(&shallow) + 3 * boxed + 1 {
        wrong.push(format!(
            "one object deeper: {} bytes counted, under the shallow op's {} plus three \
             boxes and one byte, {}",
            heap_of(&deep),
            heap_of(&shallow),
            heap_of(&shallow) + 3 * boxed + 1
        ));
    }
    assert!(
        wrong.is_empty(),
        "D2: an operation's deep size does not count what it holds:\n{}",
        wrong.join("\n")
    );
}

/// D3: a fuzz run that samples memory runs to its end over the interpreted
/// log, the only path on which `moirai-fuzz` reads these impls.
#[test]
fn st12_d3_a_fuzz_run_that_samples_memory_completes() {
    set_descriptor(BT_DESCRIPTOR);
    let runs = vec![
        RunConfig::new(0.4, 3, 200, None, Some(seed(7)), false, false).with_memory_sampling(25),
    ];
    let config = FuzzerConfig::<ModelLog, Read<Value>>::new(
        "interp-bt-memory",
        runs,
        true,
        Predicate::new(Read::new(), |left, right| left == right),
        false,
    );
    fuzzer::<ModelLog, Read<Value>>(config);
}
