//! ST.13, part (2): the optional register arm writes the absent value.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.13.md`. Part (1), the deep
//! size, has its own oracle in `st13_deep_size.rs`.
//!
//! A register over an optional value (`LeafRule::OptionalRegister`,
//! `EAnnotation.details`'s construction) holds `Option<T>`, and
//! `Scalar::Null` is this vocabulary's `None` (`leaf.rs:70-83`, `:406-424`,
//! `:515-521`): a key put with no value reads back as `[null]`, which is the
//! case the arm exists for. `check` takes it at every such register, text or
//! enum: a leaf's `probe` (`leaf.rs:597-664`, through `mv_register_op`,
//! `:1050-1056`) asks whether the operation is of the leaf's kind and whether
//! the inner log enables it, never what type the written scalar is.
//!
//! `st13_n0`: the ground truth, written by hand on two replicas. `st13_n1`:
//! the generator writes `Scalar::Null` at every optional register, `check`
//! accepts each draw, and the read renders the key as `[null]` right after.
//! `st13_n2`: partitioned replicas writing what the generator gives them
//! converge, with a key holding `null` beside a value they wrote
//! concurrently. `st13_n3`: no other arm draws `Scalar::Null`.
//!
//! ST.12's `O1` (`st12_fuzz_followups.rs`) holds the same arm to writing
//! text only at `details` and literal names only in the read of `levels` and
//! `tones`; part (2) contradicts both, and the spec says how (section 1).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 --offline -p moirai-interp --features fuzz --test st13_optional_null
//! ```

#![cfg(feature = "fuzz")]

use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, LazyLock},
};

use moirai_fuzz::op_generator::CommandGenerator;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{
    BT_DESCRIPTOR, JSON_DESCRIPTOR, attribute, bench_descriptor, class, class_slot, descriptor,
    feature_slot, opened, table,
};
use moirai_interp::{InstanceOp, MapOp, ModelLog, ModelOp, OptOp, SeqOp};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    crdt::query::Read,
    replica::{IsReplica, Replica},
};
use moirai_semantics::MetamodelSemantics;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// A seed a reader can reproduce, as ST.12 and `ip12` write it.
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

fn read(replica: &Rep) -> Value {
    replica.query(&Read::<Value>::new())
}

// ===========================================================================
// The descriptor: ST.12's, three keyed optional registers
// ===========================================================================

fn keyed_optional_register(name: &str, enumeration: Option<&str>) -> Value {
    let leaf = match enumeration {
        Some(class) => json!({"kind": "optionalRegister", "class": class}),
        None => json!({"kind": "optionalRegister"}),
    };
    attribute(name, json!({"kind": "keyed", "key": {"kind": "str"}}), leaf)
}

/// ST.12's `optreg_descriptor`, copied: `Doc`, the root, holds three keyed
/// collections whose values are registers over an optional value: `details`
/// over text, `levels` over the enum `Level` (three literals) and `tones`
/// over `Tone` (five).
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

const FEATURES: [&str; 3] = ["details", "levels", "tones"];

fn enum_slot(sem: &MetamodelSemantics, name: &str) -> u16 {
    sem.enums
        .iter()
        .find(|entry| &*entry.name == name)
        .unwrap_or_else(|| panic!("no enum `{name}`"))
        .slot
        .0
}

/// `put(feature, key, value)` on `Doc`, by hand.
fn put(feature: &str, key: &str, value: Scalar) -> InstanceOp {
    let doc = class_slot(&OPTREG, "Doc");
    InstanceOp::variant(
        doc,
        InstanceOp::field(
            feature_slot(&OPTREG, doc, feature),
            InstanceOp::entry(Scalar::text(key), InstanceOp::Leaf(LeafOp::Write(value))),
        ),
    )
}

/// The feature, the key and the leaf operation of a generated operation on
/// `Doc`, if it writes the value at a key.
fn doc_leaf(op: &ModelOp) -> Option<(String, String, LeafOp)> {
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
    let InstanceOp::Map(MapOp::Update { key, op }) = inner.as_ref() else {
        return None;
    };
    let InstanceOp::Leaf(leaf) = op.as_ref() else {
        return None;
    };
    let Scalar::Str(key) = key else {
        return None;
    };
    let name = OPTREG.classes[doc.index()].visible[feature.index()]
        .0
        .to_string();
    Some((name, key.clone(), leaf.clone()))
}

/// The values at one key of one feature, as the read renders them.
fn rendered<'a>(state: &'a Value, feature: &str, key: &str) -> Option<&'a Value> {
    state.get(feature).and_then(|map| map.get(key))
}

// ===========================================================================
// N0: the ground truth
// ===========================================================================

/// N0: the read of a key put with no value, written by hand on two replicas.
/// A key put with `Scalar::Null` and delivered everywhere reads `[null]`; a
/// key one replica puts with no value while the other puts a value reads
/// `[null, <value>]` on both, `null` first, at a text register and at an
/// enum register alike (`leaf.rs:799-803`: every value the register holds,
/// sorted, `Scalar::Null` least; `Scalar::to_json` renders it as JSON
/// `null`, `leaf.rs:114`). True at `a1e8a55`; the reading every other test
/// here holds the generator's draws to.
#[test]
fn st13_n0_ground_truth_a_key_put_with_no_value_reads_as_null() {
    let level = enum_slot(&OPTREG, "Level");
    let (mut a, mut b) = opened("st13", &optreg_descriptor());
    let bare = a
        .send(ModelOp::Instance(put("details", "bare", Scalar::Null)))
        .expect("a put with no value is accepted");
    b.receive(bare);
    let mut from_a = Vec::new();
    let mut from_b = Vec::new();
    from_a.push(
        a.send(ModelOp::Instance(put("details", "both", Scalar::Null)))
            .expect("accepted"),
    );
    from_b.push(
        b.send(ModelOp::Instance(put("details", "both", Scalar::text("x"))))
            .expect("accepted"),
    );
    from_a.push(
        a.send(ModelOp::Instance(put("levels", "both", Scalar::Null)))
            .expect("accepted"),
    );
    from_b.push(
        b.send(ModelOp::Instance(put(
            "levels",
            "both",
            Scalar::Enum(level, 1),
        )))
        .expect("accepted"),
    );
    for message in from_b {
        a.receive(message);
    }
    for message in from_a {
        b.receive(message);
    }
    for (who, replica) in [("a", &a), ("b", &b)] {
        let state = read(replica);
        assert_eq!(
            (
                rendered(&state, "details", "bare"),
                rendered(&state, "details", "both"),
                rendered(&state, "levels", "both"),
            ),
            (
                Some(&json!([null])),
                Some(&json!([null, "x"])),
                Some(&json!([null, "MID"])),
            ),
            "N0, replica {who}: {state}"
        );
    }
    assert_eq!(read(&a), read(&b), "N0: the replicas agree");
}

// ===========================================================================
// N1: the generator draws the absent value, check accepts it, the read
// renders it
// ===========================================================================

const N1_SEEDS: u8 = 4;
const N1_STEPS: usize = 400;

/// N1: one replica on the optional-register descriptor sends what the
/// generator gives it, four seeds of 400 steps (ST.12's O1 schedule). Every
/// draw is accepted by `check` and by the replica. At each of the three
/// features the arm writes `Scalar::Null` at least once and still writes a
/// value of the register's own kind (text at `details`, a literal of its own
/// enum at `levels` and `tones`); it writes nothing else. Right after each
/// write of `Scalar::Null` at a key, the read renders that key as `[null]`,
/// since one replica writing in sequence leaves the register holding that
/// write alone. False at `a1e8a55`: the arm never draws `Scalar::Null`
/// (`fuzz.rs:337-347`).
#[test]
fn st13_n1_the_optional_register_arm_writes_the_absent_value() {
    let level = enum_slot(&OPTREG, "Level");
    let tone = enum_slot(&OPTREG, "Tone");
    let mut failures = Vec::new();
    let mut nulls: BTreeMap<String, usize> = BTreeMap::new();
    let mut values: BTreeMap<String, usize> = BTreeMap::new();
    let mut other = Vec::new();
    let mut misread = Vec::new();
    for run in 0..N1_SEEDS {
        let (mut a, _b) = opened("st13", &optreg_descriptor());
        let mut rng = ChaCha8Rng::from_seed(seed(run));
        for step in 0..N1_STEPS {
            let op = match generated(a.state(), &mut rng) {
                Ok(op) => op,
                Err(why) => {
                    failures.push(format!("seed {run}, step {step}: {why}"));
                    break;
                }
            };
            if let Err(refusal) = a.state().refusal(&op) {
                failures.push(format!(
                    "seed {run}, step {step}: `check` refuses {op:?}: {refusal}"
                ));
                break;
            }
            let null_at = match doc_leaf(&op) {
                Some((feature, key, LeafOp::Write(value))) => match (&value, feature.as_str()) {
                    (Scalar::Null, _) => {
                        *nulls.entry(feature.clone()).or_default() += 1;
                        Some((feature, key))
                    }
                    (Scalar::Str(_), "details") => {
                        *values.entry(feature).or_default() += 1;
                        None
                    }
                    (Scalar::Enum(class, _), "levels") if *class == level => {
                        *values.entry(feature).or_default() += 1;
                        None
                    }
                    (Scalar::Enum(class, _), "tones") if *class == tone => {
                        *values.entry(feature).or_default() += 1;
                        None
                    }
                    (value, _) => {
                        other.push(format!(
                            "seed {run}, step {step}: `{feature}` written {value:?}"
                        ));
                        None
                    }
                },
                Some((_, _, LeafOp::Clear)) | None => None,
                Some((feature, _, leaf)) => {
                    other.push(format!(
                        "seed {run}, step {step}: `{feature}` given {leaf:?}"
                    ));
                    None
                }
            };
            if let Err(refusal) = a.send(op) {
                failures.push(format!(
                    "seed {run}, step {step}: the replica refuses: {refusal}"
                ));
                break;
            }
            if let Some((feature, key)) = null_at {
                let state = read(&a);
                let got = rendered(&state, &feature, &key);
                if got != Some(&json!([null])) {
                    misread.push(format!(
                        "seed {run}, step {step}: `{feature}`[{key:?}] put with no value reads \
                         {got:?}, not [null]"
                    ));
                }
            }
        }
    }
    let mut wrong = Vec::new();
    for feature in FEATURES {
        let drawn = nulls.get(feature).copied().unwrap_or(0);
        if drawn == 0 {
            wrong.push(format!("`{feature}`: no write of `Scalar::Null` drawn"));
        }
        if values.get(feature).copied().unwrap_or(0) == 0 {
            wrong.push(format!(
                "`{feature}`: no write of a value of its own kind drawn"
            ));
        }
    }
    wrong.extend(other);
    wrong.extend(misread);
    assert!(
        failures.is_empty() && wrong.is_empty(),
        "N1: {} refused or panicking seed(s), {} wrong (writes of `Scalar::Null` drawn: \
         {nulls:?}; of a value: {values:?})\n{}\n{}",
        failures.len(),
        wrong.len(),
        failures.join("\n"),
        wrong.join("\n")
    );
}

// ===========================================================================
// N2: replicas converge on it, concurrent writes included
// ===========================================================================

const N2_PAIRS: u8 = 32;
const N2_STEPS: usize = 150;

/// N2: `N2_PAIRS` pairs of replicas, each pair partitioned: each replica
/// sends `N2_STEPS` operations of its own seed with nothing delivered, then
/// each delivers everything the other sent. Every pair's reads are equal;
/// and across the pairs at least one key holds `null` beside a value, a key
/// one replica put with no value while the other put one there, both kept by
/// the register. Thirty-two pairs because such a key needs both replicas'
/// last writes at one key to be one `null` and one value, and the rehearsal
/// of spec section 11 found 4 of them in the first 8 pairs: 32 pairs keep a
/// draw of none unlikely for any arm that writes `null` about one time in
/// eight. False at `a1e8a55`: no `null` is ever written, so no key holds one.
#[test]
fn st13_n2_partitioned_replicas_converge_with_concurrent_absent_and_present_values() {
    let mut failures = Vec::new();
    let mut mixed = Vec::new();
    for pair in 0..N2_PAIRS {
        let (mut a, mut b) = opened("st13", &optreg_descriptor());
        let mut rng_a = ChaCha8Rng::from_seed(seed(40 + 2 * pair));
        let mut rng_b = ChaCha8Rng::from_seed(seed(41 + 2 * pair));
        let mut from_a = Vec::new();
        let mut from_b = Vec::new();
        let mut broken = None;
        for step in 0..N2_STEPS {
            for (who, replica, rng, sent) in [
                ("a", &mut a, &mut rng_a, &mut from_a),
                ("b", &mut b, &mut rng_b, &mut from_b),
            ] {
                match generated(replica.state(), rng) {
                    Ok(op) => match replica.send(op) {
                        Ok(message) => sent.push(message),
                        Err(refusal) => {
                            broken = Some(format!(
                                "pair {pair}, {who}, step {step}: refused: {refusal}"
                            ));
                        }
                    },
                    Err(why) => broken = Some(format!("pair {pair}, {who}, step {step}: {why}")),
                }
            }
            if broken.is_some() {
                break;
            }
        }
        if let Some(why) = broken {
            failures.push(why);
            continue;
        }
        for message in from_a {
            b.receive(message);
        }
        for message in from_b {
            a.receive(message);
        }
        let (left, right) = (read(&a), read(&b));
        if left != right {
            failures.push(format!(
                "pair {pair}: the reads differ\n  a {left}\n  b {right}"
            ));
            continue;
        }
        for feature in FEATURES {
            if let Some(map) = left.get(feature).and_then(Value::as_object) {
                for (key, held) in map {
                    let held = held.as_array().cloned().unwrap_or_default();
                    if held.contains(&Value::Null) && held.iter().any(|value| !value.is_null()) {
                        mixed.push(format!("pair {pair}: `{feature}`[{key:?}] = {held:?}"));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty() && !mixed.is_empty(),
        "N2: {} pair(s) refused or diverged; {} key(s) hold `null` beside a value (at least \
         one expected)\n{}",
        failures.len(),
        mixed.len(),
        failures.join("\n")
    );
}

// ===========================================================================
// N3: no other arm draws it
// ===========================================================================

/// Whether an operation carries `Scalar::Null` anywhere: as a key, or as the
/// value of a leaf write.
fn carries_null(op: &InstanceOp) -> bool {
    match op {
        InstanceOp::Field(_, inner)
        | InstanceOp::Variant(_, inner)
        | InstanceOp::Opt(OptOp::Set(inner))
        | InstanceOp::Seq(SeqOp::Insert { op: inner, .. } | SeqOp::Update { op: inner, .. }) => {
            carries_null(inner)
        }
        InstanceOp::Map(MapOp::Update { key, op }) => *key == Scalar::Null || carries_null(op),
        InstanceOp::Map(MapOp::Remove { key }) => *key == Scalar::Null,
        InstanceOp::Leaf(
            LeafOp::Insert { value, .. }
            | LeafOp::Inc(value)
            | LeafOp::Dec(value)
            | LeafOp::Write(value)
            | LeafOp::Add(value)
            | LeafOp::Remove(value),
        ) => *value == Scalar::Null,
        _ => false,
    }
}

/// N3: on three descriptors with no optional register and every other leaf
/// kind between them (`bench`: text, counters, both flags, a register of
/// every tie, an enum, both sets, a bag; `bt.ecore`; `json.ecore`), one
/// replica sending what the generator gives it, two seeds of 300 steps: no
/// drawn operation carries `Scalar::Null`, and every one is accepted. A
/// plain register's value space has no absent value (its generated twin is a
/// register over `T`, not `Option<T>`), so `Scalar::Null` belongs to the
/// optional register's arm alone. True at `a1e8a55`; the guard on where part
/// (2) puts it.
#[test]
fn st13_n3_no_other_arm_draws_the_absent_value() {
    let bt: Value = serde_json::from_str(BT_DESCRIPTOR).expect("the fixture is JSON");
    let json: Value = serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON");
    let mut wrong = Vec::new();
    for (label, descriptor) in [("bench", bench_descriptor()), ("bt", bt), ("json", json)] {
        for run in 0..2u8 {
            let (mut a, _b) = opened("st13", &descriptor);
            let mut rng = ChaCha8Rng::from_seed(seed(60 + run));
            for step in 0..300 {
                let op = match generated(a.state(), &mut rng) {
                    Ok(op) => op,
                    Err(why) => {
                        wrong.push(format!("{label}, seed {run}, step {step}: {why}"));
                        break;
                    }
                };
                if let ModelOp::Instance(instance) = &op
                    && carries_null(instance)
                {
                    wrong.push(format!(
                        "{label}, seed {run}, step {step}: draws `Scalar::Null`: {instance:?}"
                    ));
                }
                if let Err(refusal) = a.send(op) {
                    wrong.push(format!(
                        "{label}, seed {run}, step {step}: refused: {refusal}"
                    ));
                    break;
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "N3: {} draw(s) outside an optional register carry `Scalar::Null` or are refused\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}
