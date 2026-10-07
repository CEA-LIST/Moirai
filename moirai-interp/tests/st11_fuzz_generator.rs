//! ST.11 part (1): the fuzz generator draws a held object at a slot only from
//! the objects that slot serves.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.11.md`. Since ST.10 a
//! local writer at a family slot (the roots, and a containment whose target
//! class is abstract or has a subclass) is held to the *present* objects
//! only (`SlotNode::served`, read by `check`), while a removed member stays
//! in the slot, absent. The generator (`moirai-interp/src/fuzz.rs`) must see
//! the slot the way `check` does: a member it draws must be one the slot
//! serves, and a slot whose members are all absent is an empty slot for it.
//!
//! Three states put an absent member in a family slot of `json.ecore` (the
//! value of key `k` of the root object, whose target class `Json` is
//! abstract):
//!
//! - G1: one replica puts a string at `k`, removes `k`, and puts a number
//!   there: the string is absent beside the present number.
//! - G2: one replica puts a string at `k` and removes `k`: the string is
//!   absent and alone.
//! - G3: ST.10's `remove_resolves_a_conflict_of_kinds_it_has_partly_seen`:
//!   `a` puts a string, `c` concurrently a number, `b` removes `k` having
//!   seen the string only; after every replica has delivered everything,
//!   each holds the string absent beside the number.
//!
//! In each, operations are drawn from the generator against the fixed state
//! (nothing is sent), with fixed seeds, and every one must be an operation
//! the log accepts (`ModelLog::refusal`). G1 and G3 add that every draw that
//! reaches `k` names `Number`, the one class served there; G2 that the draws
//! reaching `k` name every class `Json` allows, since the slot serves none.
//! Each also checks that it is not vacuous: the state is the one described
//! (two objects held at `k` in G1 and G3, one in G2, and the read), and at
//! least `MIN_AT_K` draws reached `k`.
//!
//! The last two tests replace `fuzz.rs`'s inline `ip12` and
//! `a_generated_operation_is_always_one_the_log_would_accept`, ported to
//! `moirai-fuzz`'s current API (`CommandGenerator`, `Predicate`), with their
//! descriptor, seeds, counts and assertions unchanged (spec section 5).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features fuzz --test st11_fuzz_generator
//! ```

#![cfg(feature = "fuzz")]

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, LazyLock},
};

use moirai_fuzz::{
    config::{FuzzerConfig, Predicate, RunConfig},
    fuzzer::fuzzer,
    op_generator::CommandGenerator,
};
use moirai_interp::fuzz::set_descriptor;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{
    BT_DESCRIPTOR, JSON_DESCRIPTOR, class_slot, feature_slot, install, objects, table,
};
use moirai_interp::{InstanceOp, MapOp, ModelLog, ModelOp, Node};
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    crdt::query::Read,
    log_id::LogId,
    replica::{IsReplica, Replica},
    state::log::IsLog,
};
use moirai_semantics::{ClassSlot, MetamodelSemantics};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// The key every scenario writes. Not one of the generator's own words
/// (`fuzz.rs`'s `WORDS`), so no generated `entry(fresh, ..)` lands on it and
/// every draw that reaches it went through the slot the scenario built.
const K: &str = "k";

/// Draws per seed, and seeds per state.
const DRAWS: usize = 500;
const SEEDS: u8 = 4;

/// The fewest draws that must reach `k` for a state to count as exercised.
/// About half the draws do (the root object's keyed feature picks an
/// existing key five times in ten); this is a floor well under that.
const MIN_AT_K: usize = 100;

fn json_descriptor() -> Value {
    serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON")
}

static SEM: LazyLock<Arc<MetamodelSemantics>> = LazyLock::new(|| table(&json_descriptor()));

/// A seed a reader can reproduce: the run's number in the first byte and its
/// complement in the last, as `fuzz.rs`'s `ip12` writes it.
fn seed(run: u8) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = run;
    seed[31] = !run;
    seed
}

// ---------------------------------------------------------------------------
// Operations on `json.ecore`, as ST.10's interpreter oracle writes them
// ---------------------------------------------------------------------------

fn on(class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
    let class = class_slot(&SEM, class);
    InstanceOp::variant(
        class,
        InstanceOp::field(feature_slot(&SEM, class, feature), inner),
    )
}

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

// ---------------------------------------------------------------------------
// Replicas
// ---------------------------------------------------------------------------

/// The replicas of one log, every one of which has delivered the
/// descriptor's `Install`, sent by the first.
fn opened(ids: &[&str]) -> Vec<Rep> {
    let log_id = LogId::from_bytes([0x5c; 16]);
    let mut replicas: Vec<Rep> = ids
        .iter()
        .map(|id| Rep::bootstrap_with_log_id((*id).into(), ids, log_id.clone()))
        .collect();
    let opening = replicas[0]
        .send(install("st11", &json_descriptor()))
        .unwrap_or_else(|refusal| panic!("a fresh log refused its Install: {refusal}"));
    for replica in replicas.iter_mut().skip(1) {
        replica.receive(opening.clone());
    }
    replicas
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

/// The classes of the objects the slot at key `k` of the root object holds,
/// absent ones included, in the order the slot holds them.
fn held_at_k(log: &ModelLog) -> Vec<ClassSlot> {
    let root = objects(log.root());
    assert_eq!(root.len(), 1, "one root object, not {}", root.len());
    let entry = feature_slot(&SEM, class_slot(&SEM, "Object"), "entry");
    let Some(Node::Map(map)) = root[0].fields().get(&entry) else {
        panic!("the root object's `entry` is not a keyed collection");
    };
    let Some(slot) = map.children().get(&Scalar::text(K)) else {
        panic!("the root object holds no entry `{K}`");
    };
    objects(slot).iter().map(|object| object.class()).collect()
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------

/// The class a generated operation names at key `k` of the root object, if
/// it is an update of `k`.
fn class_at_k(op: &ModelOp) -> Option<ClassSlot> {
    let object = class_slot(&SEM, "Object");
    let entry = feature_slot(&SEM, object, "entry");
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
    if *key != Scalar::text(K) {
        return None;
    }
    match op.as_ref() {
        InstanceOp::Variant(class, _) => Some(*class),
        _ => None,
    }
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

/// What the draws against one fixed state came to.
struct Draws {
    /// Classes named by the draws that reached `k`, one entry per draw.
    at_k: Vec<ClassSlot>,
    /// The first refused (or generator-panicking) draw of each seed.
    failures: Vec<String>,
}

/// `SEEDS * DRAWS` operations from the generator against `log`, which is not
/// changed. A seed stops at its first failure, so a broken generator is
/// reported once per seed rather than hundreds of times.
fn draw(label: &str, log: &ModelLog) -> Draws {
    let mut at_k = Vec::new();
    let mut failures = Vec::new();
    for run in 0..SEEDS {
        let mut rng = ChaCha8Rng::from_seed(seed(run));
        for step in 0..DRAWS {
            let op = match catch_unwind(AssertUnwindSafe(|| log.generate_command(&mut rng))) {
                Ok(op) => op,
                Err(payload) => {
                    failures.push(format!(
                        "{label}, seed {run}, draw {step}: the generator panicked: {}",
                        panic_message(payload)
                    ));
                    break;
                }
            };
            if let Err(refusal) = log.refusal(&op) {
                failures.push(format!(
                    "{label}, seed {run}, draw {step}: the log refuses {op:?}: {refusal}"
                ));
                break;
            }
            if let Some(class) = class_at_k(&op) {
                at_k.push(class);
            }
        }
    }
    Draws { at_k, failures }
}

fn names(classes: impl IntoIterator<Item = ClassSlot>) -> Vec<String> {
    classes
        .into_iter()
        .map(|class| SEM.classes[class.index()].name.to_string())
        .collect()
}

/// G1 and G3's assertion on one replica holding the string absent beside the
/// present number at `k`.
fn assert_draws_only_the_served_member(label: &str, replica: &Rep) {
    let string = class_slot(&SEM, "String");
    let number = class_slot(&SEM, "Number");
    // The state is the one the claim is about: both members held, the
    // number alone served.
    let mut held = held_at_k(replica.state());
    held.sort_unstable();
    let mut expected = vec![string, number];
    expected.sort_unstable();
    assert_eq!(
        held,
        expected,
        "{label}: the slot at `{K}` must hold the string and the number, and holds {:?}",
        names(held.clone())
    );
    assert_eq!(read(replica), json!({K: 2.0}), "{label}: the read");

    let draws = draw(label, replica.state());
    let wrong: BTreeSet<String> = names(draws.at_k.iter().copied().filter(|c| *c != number))
        .into_iter()
        .collect();
    assert!(
        draws.failures.is_empty() && wrong.is_empty() && draws.at_k.len() >= MIN_AT_K,
        "{label}: {} draws reached `{K}` (at least {MIN_AT_K} expected); \
         classes other than Number drawn there: {wrong:?}; failures:\n{}",
        draws.at_k.len(),
        draws.failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// G1 to G3
// ---------------------------------------------------------------------------

#[test]
fn st11_g1_a_retype_after_a_remove_draws_the_present_member_only() {
    let mut replicas = opened(&["a", "b"]);
    let a = &mut replicas[0];
    send(a, put(K, ins('a', 0)));
    send(a, remove(K));
    send(a, put(K, inc(2.0)));
    assert_draws_only_the_served_member("G1 on a", a);
}

#[test]
fn st11_g2_an_absent_member_alone_leaves_the_slot_empty_to_the_generator() {
    let mut replicas = opened(&["a", "b"]);
    let a = &mut replicas[0];
    send(a, put(K, ins('a', 0)));
    send(a, put(K, ins('b', 1)));
    send(a, remove(K));
    let string = class_slot(&SEM, "String");
    assert_eq!(
        held_at_k(a.state()),
        vec![string],
        "G2: the slot at `{K}` must hold the removed string alone"
    );
    assert_eq!(read(a), json!({}), "G2: the read");

    let draws = draw("G2 on a", a.state());
    let allowed: BTreeSet<String> = ["Array", "Boolean", "Number", "Object", "String"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let drawn: BTreeSet<String> = names(draws.at_k.iter().copied()).into_iter().collect();
    assert!(
        draws.failures.is_empty() && drawn == allowed && draws.at_k.len() >= MIN_AT_K,
        "G2: {} draws reached `{K}` (at least {MIN_AT_K} expected), naming {drawn:?} \
         where every class `Json` allows ({allowed:?}) is expected, since the slot \
         serves none; failures:\n{}",
        draws.at_k.len(),
        draws.failures.join("\n")
    );
}

#[test]
fn st11_g3_a_remove_that_saw_one_kind_of_a_conflict_draws_the_other_only() {
    let mut replicas = opened(&["a", "b", "c"]);
    let u1 = send(&mut replicas[0], put(K, ins('a', 0)));
    let u2 = send(&mut replicas[2], put(K, inc(2.0)));
    replicas[1].receive(u1.clone());
    assert_eq!(
        read(&replicas[1]),
        json!({K: "a"}),
        "G3: b before its remove"
    );
    let u3 = send(&mut replicas[1], remove(K));
    assert_eq!(read(&replicas[1]), json!({}), "G3: b after its remove");
    replicas[1].receive(u2.clone());
    replicas[0].receive(u2);
    replicas[0].receive(u3.clone());
    replicas[2].receive(u1);
    replicas[2].receive(u3);
    for (replica, id) in replicas.iter().zip(["a", "b", "c"]) {
        assert_draws_only_the_served_member(&format!("G3 on {id}"), replica);
    }
}

// ---------------------------------------------------------------------------
// The inline tests of `fuzz.rs`, ported (spec section 5)
// ---------------------------------------------------------------------------

/// `ip12`: three replicas, a thousand operations, ten seeds, over the
/// behaviour tree's own table, every run converging.
#[test]
fn ip12_three_replicas_a_thousand_operations_ten_seeds_all_converge() {
    set_descriptor(BT_DESCRIPTOR);
    let runs: Vec<RunConfig> = (0..10)
        .map(|run| RunConfig::new(0.4, 3, 1_000, None, Some(seed(run)), false, false))
        .collect();
    // `fuzzer` panics on a run that does not converge, on a generated
    // operation the log refuses, and on a replica that ends with a different
    // number of events; there is nothing left for this test to assert that
    // it does not assert itself.
    let config = FuzzerConfig::<ModelLog, Read<Value>>::new(
        "interp-bt",
        runs,
        true,
        Predicate::new(Read::new(), |left, right| left == right),
        false,
    );
    fuzzer::<ModelLog, Read<Value>>(config);
}

#[test]
fn a_generated_operation_is_always_one_the_log_would_accept() {
    // The narrow version of the same claim, without the fuzzer's own
    // machinery in the way: the generator and `check`'s local mode agree
    // about every operation, over one replica and a thousand draws.
    set_descriptor(BT_DESCRIPTOR);
    let (mut a, _b) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
    let mut rng = ChaCha8Rng::from_seed(seed(42));

    for step in 0..1_000 {
        let op = a.state().generate_command(&mut rng);
        assert!(
            a.state().is_enabled(&op).is_ok(),
            "step {step}: {:?}",
            a.state().refusal(&op)
        );
        a.send(op).expect("and the replica takes it");
    }
    assert_eq!(a.state().unresolved(), 0);
}
