//! The operations the `json_crdt` rig drives, and a seeded generator over them.
//!
//! Two consumers, one implementation: the randomised end-to-end scenario (R1)
//! and the dashboard's `--random` driver. They must generate the *same*
//! sequence for a given seed, because a run the dashboard showed is only worth
//! anything if the suite can replay it — so this lives in the library rather
//! than being written twice.
//!
//! # Why this is model-specific and the rest of the crate is not
//!
//! `GenericNode` is generic over the log type on purpose. This module is not:
//! it hard-codes the wire shapes of the Arachne-generated `json_crdt`
//! operations, which is what the whole rig — image, Compose file, e2e suite,
//! dashboard — already speaks. Every builder below has been executed against a
//! live node; none is guessed.
//!
//! The shapes follow serde's default externally-tagged enum representation of
//! the generated types:
//!
//! - `Json::JsonKind(JsonKind)` — `{"JsonKind": <kind>}`
//! - `JsonKind::{Object,String,Number,Boolean,Array}(..)` — `{"Object": <op>}`
//! - `UWMap::Update(K, O)` — `{"Update": [<key>, <op>]}`
//! - `UWMap::Remove(K)` — `{"Remove": <key>}`
//! - `List::Insert { content, pos }` — `{"Insert": {"content": .., "pos": ..}}`
//! - `Counter::Inc(V)` — `{"Inc": <n>}`
//! - `EWFlag::Enable` (a unit variant) — `"Enable"`

use serde_json::{json, Value};

/// Builders for the wire format of `json_crdt`'s operation type.
pub mod ops {
    use serde_json::{json, Value};

    /// `Json::JsonKind(JsonKind::Object(UWMap::Update(key, inner)))`.
    pub fn object_update(key: &str, inner: Value) -> Value {
        json!({ "JsonKind": { "Object": { "Update": [key, inner] } } })
    }

    /// `Json::JsonKind(JsonKind::Object(UWMap::Remove(key)))`.
    ///
    /// Note the observed semantics: `Remove` resets the child CRDT to its
    /// default value rather than dropping the key from the rendered state, so
    /// a removed string reads back as `[]`, not as an absent key.
    pub fn object_remove(key: &str) -> Value {
        json!({ "JsonKind": { "Object": { "Remove": key } } })
    }

    /// `JsonKind::String(List::Insert { content, pos })` — a sequence CRDT
    /// insert of a single character.
    pub fn string_insert(content: char, pos: usize) -> Value {
        json!({ "String": { "Insert": { "content": content.to_string(), "pos": pos } } })
    }

    /// `JsonKind::Number(Counter::Inc(by))`.
    pub fn number_inc(by: f64) -> Value {
        json!({ "Number": { "Inc": by } })
    }

    /// `JsonKind::Boolean(EWFlag::Enable)` — an enable-wins flag.
    pub fn bool_enable() -> Value {
        json!({ "Boolean": "Enable" })
    }

    /// `JsonKind::Array(NestedList::Insert { pos, op })`.
    pub fn array_insert(pos: usize, inner: Value) -> Value {
        json!({ "Array": { "Insert": { "pos": pos, "op": inner } } })
    }
}

/// SplitMix64. Deterministic, seedable, and thirty lines shorter than taking a
/// dependency on `rand` for a workload generator that does not need statistical
/// quality — only reproducibility, which is the whole point of printing the
/// seed on failure.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`. `n` must not be zero.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// What one generated step asks of the cluster: which replica applies it, and
/// what it applies.
#[derive(Debug, Clone)]
pub struct Step {
    /// Index into the caller's replica list. The generator does not know the
    /// replica ids, so that a run is reproducible across differently-named
    /// clusters of the same size.
    pub replica: usize,
    /// A human-readable summary, for the failure report and the feed.
    pub label: String,
    pub op: Value,
}

/// What a key holds. Fixed per key for the life of a session — see
/// [`Workload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Text,
    Counter,
    Flag,
}

/// A seeded stream of operations across `replicas` replicas.
///
/// # Every key has one kind, for ever
///
/// Measured against a live node, not assumed: the generated `JsonKind` union
/// binds a key to a variant on its first write, and every later operation of a
/// different variant is refused — `{"success":false,"message":"Operation not
/// enabled"}`. A `Remove` does **not** release the binding: after removing a
/// counter-valued key, an increment is still accepted and a string insert is
/// still refused. So the generator assigns a kind per key up front and never
/// crosses them.
///
/// This matters because a rejected operation is a test failure, not a condition
/// to handle. A generator that mixed kinds would produce a run that fails for a
/// reason having nothing to do with replication.
///
/// # Why every insert is at position 0
///
/// Same rule. An insert at position 0 is always enabled; an insert at position
/// `k` is enabled only if the replica's copy of that string is already at least
/// `k` long, which the generator cannot know without modelling every replica's
/// divergent state — exactly the thing under test. Inserting at 0 also
/// maximises the interesting case: every concurrent insert collides at the same
/// position, which is C3 happening continuously rather than once.
#[derive(Debug, Clone)]
pub struct Workload {
    rng: Rng,
    replicas: usize,
    seed: u64,
}

impl Workload {
    /// Keys the generated operations touch, and what each one holds. Few, so
    /// that concurrent operations collide on the same target often rather than
    /// almost never.
    const KEYS: [(&'static str, Kind); 5] = [
        ("alpha", Kind::Text),
        ("beta", Kind::Text),
        ("gamma", Kind::Counter),
        ("delta", Kind::Counter),
        ("epsilon", Kind::Flag),
    ];

    /// The key names, for a caller that wants to assert on the final state.
    pub fn keys() -> impl Iterator<Item = &'static str> {
        Self::KEYS.iter().map(|(name, _)| *name)
    }

    pub fn new(seed: u64, replicas: usize) -> Self {
        assert!(replicas > 0, "a workload needs at least one replica");
        Self {
            rng: Rng::new(seed),
            replicas,
            seed,
        }
    }

    /// The seed this workload was built from — print it on failure.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The next operation, and who applies it.
    pub fn next(&mut self) -> Step {
        let replica = self.rng.below(self.replicas);
        let (key, kind) = Self::KEYS[self.rng.below(Self::KEYS.len())];
        // One roll in ten is a remove. Rare on purpose: a remove landing
        // between every other operation makes the final state trivially empty
        // and the run proves nothing about resolution.
        let removing = self.rng.below(10) == 0;
        let (label, op) = if removing {
            (format!("remove {key}"), ops::object_remove(key))
        } else {
            match kind {
                Kind::Text => {
                    let ch = (b'a' + (self.rng.below(26) as u8)) as char;
                    (
                        format!("insert '{ch}' at 0 of {key}"),
                        ops::object_update(key, ops::string_insert(ch, 0)),
                    )
                }
                Kind::Counter => {
                    let by = 1 + self.rng.below(5);
                    (
                        format!("increment {key} by {by}"),
                        ops::object_update(key, ops::number_inc(by as f64)),
                    )
                }
                Kind::Flag => (
                    format!("enable {key}"),
                    ops::object_update(key, ops::bool_enable()),
                ),
            }
        };
        Step { replica, label, op }
    }

    /// `count` steps, in order.
    pub fn take(&mut self, count: usize) -> Vec<Step> {
        (0..count).map(|_| self.next()).collect()
    }
}

/// A digest of a rendered state, computed identically on every replica so that
/// "do these two agree?" is one string comparison.
///
/// FNV-1a over the canonical JSON encoding. `serde_json::Map` is a `BTreeMap`
/// unless the `preserve_order` feature is on — it is not, here — so
/// `Value::to_string` is key-sorted and therefore canonical, and two replicas
/// that agree structurally produce the same bytes. Not a cryptographic hash and
/// not used as one: it answers "same or different", which is all a divergence
/// marker needs.
pub fn state_digest(state: &Value) -> String {
    let mut hash: u64 = 0xCBF2_9CE4_8422_2325;
    for byte in state.to_string().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01B3);
    }
    format!("{hash:016x}")
}

/// The `{"json": ...}` envelope a replica with no state at all reports.
pub fn is_unset(state: &Value) -> bool {
    state.pointer("/json") == Some(&json!("Unset"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_gives_the_same_run() {
        let a = Workload::new(42, 3).take(50);
        let b = Workload::new(42, 3).take(50);
        assert_eq!(
            a.iter().map(|s| s.label.clone()).collect::<Vec<_>>(),
            b.iter().map(|s| s.label.clone()).collect::<Vec<_>>()
        );
        assert_eq!(
            a.iter().map(|s| s.replica).collect::<Vec<_>>(),
            b.iter().map(|s| s.replica).collect::<Vec<_>>()
        );
    }

    #[test]
    fn different_seeds_give_different_runs() {
        let a = Workload::new(1, 3).take(50);
        let b = Workload::new(2, 3).take(50);
        assert_ne!(
            a.iter().map(|s| s.label.clone()).collect::<Vec<_>>(),
            b.iter().map(|s| s.label.clone()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn every_step_names_a_replica_that_exists() {
        let mut w = Workload::new(7, 4);
        for _ in 0..500 {
            assert!(w.next().replica < 4);
        }
    }

    #[test]
    fn all_four_replicas_get_work() {
        let mut w = Workload::new(7, 4);
        let mut seen = [false; 4];
        for _ in 0..200 {
            seen[w.next().replica] = true;
        }
        assert!(seen.iter().all(|s| *s), "a replica was never scheduled");
    }

    #[test]
    fn digests_agree_across_key_ordering() {
        // The same state built in two key orders must digest identically, or
        // the divergence marker would fire on nothing.
        let a: Value = serde_json::from_str(r#"{"json":{"a":1,"b":2}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"json":{"b":2,"a":1}}"#).unwrap();
        assert_eq!(state_digest(&a), state_digest(&b));
    }

    #[test]
    fn digests_differ_when_the_state_does() {
        let a: Value = serde_json::from_str(r#"{"json":{"a":1}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"json":{"a":2}}"#).unwrap();
        assert_ne!(state_digest(&a), state_digest(&b));
    }
}
