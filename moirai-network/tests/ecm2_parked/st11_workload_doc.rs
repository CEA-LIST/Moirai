//! ST.11 part (2): what `src/workload.rs` says about a removed key is what
//! the library does after ST.10.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.11.md`. Two passages of
//! `workload.rs` describe the removed-key read and the kind binding of the
//! generated `json_crdt` runtime as they were before ST.10's presence rule
//! (`42ee5ab`, the cherry-pick of ST.09's `12e16d9` into `moirai-macros`'
//! `union!`): `ops::object_remove`'s note ("a removed string reads back as
//! `[]`, not as an absent key") and `Workload`'s "Every key has one kind, for
//! ever" ("A `Remove` does not release the binding").
//!
//! Two kinds of test, kept apart:
//!
//! - The `ground_truth_*` tests say what is true, on `moirai_crdt::json`,
//!   whose `Json` is the same `union!` (from this workspace's
//!   `moirai-macros`, by path) that the generated `json_crdt`'s `JsonKind`
//!   expands, over the same kinds the workload writes: a text key, a counter
//!   key. They pass before and after the change; they are the oracle's
//!   ground truth, not its red half. `moirai-crdt/tests/json_remove.rs`
//!   holds the rest of it (every kind, every delivery order).
//! - `workload_rs_no_longer_states_the_pre_st10_read` is the red half: the
//!   four sentences that ground truth contradicts must be gone from
//!   `workload.rs`'s comments. What the new comments must say instead is the
//!   spec's checklist (F1 to F5), which the review holds them to.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-network --test st11_workload_doc
//! ```

use moirai_crdt::{
    counter::resettable_counter::Counter, json::Json, list::eg_walker::List, map::uw_map::UWMap,
    query::read_as_json::ReadAsJson,
};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    log_id::LogId,
    replica::{IsReplica, Replica},
    state::log::IsLog,
};
use serde_json::{json, Value};

type JsonReplica = Replica<moirai_crdt::json::JsonLog, Tcsb<Json>>;

// ---------------------------------------------------------------------------
// The red half: the comments
// ---------------------------------------------------------------------------

/// `workload.rs`'s comment text, markers stripped, lower-cased, with
/// backticks and asterisks dropped and whitespace collapsed, so a sentence is
/// found however it is wrapped or emphasised.
fn comment_text() -> String {
    let source = include_str!("../src/workload.rs");
    let mut text = String::new();
    for line in source.lines() {
        let line = line.trim_start();
        let comment = line
            .strip_prefix("//!")
            .or_else(|| line.strip_prefix("///"))
            .or_else(|| line.strip_prefix("//"));
        if let Some(comment) = comment {
            text.push(' ');
            text.push_str(comment);
        }
    }
    let text: String = text
        .to_lowercase()
        .chars()
        .filter(|c| *c != '`' && *c != '*')
        .collect();
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn workload_rs_no_longer_states_the_pre_st10_read() {
    let text = comment_text();
    // Each is a sentence of `workload.rs` at the spec commit, normalised as
    // `comment_text` normalises, and each is false after ST.10 (spec
    // section 6, F1 and F4).
    let stale = [
        "remove resets the child crdt to its default value rather than dropping the key from the rendered state",
        "a removed string reads back as [], not as an absent key",
        "a remove does not release the binding",
        "after removing a counter-valued key, an increment is still accepted and a string insert is still refused",
    ];
    let found: Vec<&str> = stale
        .iter()
        .copied()
        .filter(|sentence| text.contains(sentence))
        .collect();
    assert!(
        found.is_empty(),
        "workload.rs still states the pre-ST.10 read in {} place(s):\n{}",
        found.len(),
        found.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Ground truth, on the union the generated runtime expands
// ---------------------------------------------------------------------------

fn put(key: &str, value: Json) -> Json {
    Json::Object(UWMap::Update(key.to_string(), Box::new(value)))
}

fn remove(key: &str) -> Json {
    Json::Object(UWMap::Remove(key.to_string()))
}

/// `ops::string_insert`'s operation: one character at position 0.
fn ins(content: char) -> Json {
    Json::String(List::insert(content, 0))
}

/// `ops::number_inc`'s operation.
fn inc(by: f64) -> Json {
    Json::Number(Counter::Inc(by))
}

fn pair() -> (JsonReplica, JsonReplica) {
    let log_id = LogId::from_bytes([0x5d; 16]);
    (
        JsonReplica::bootstrap_with_log_id("a".into(), &["a", "b"], log_id.clone()),
        JsonReplica::bootstrap_with_log_id("b".into(), &["a", "b"], log_id),
    )
}

fn read(replica: &JsonReplica) -> Value {
    replica.query(&ReadAsJson::new())
}

fn accepts(replica: &JsonReplica, op: &Json) -> bool {
    replica.state().is_enabled(op).is_ok()
}

/// F1: a removed text key whose every update the remove has seen is absent
/// from the read, at the remover and at a peer that delivered everything;
/// it does not read as an empty value.
#[test]
fn ground_truth_a_removed_text_key_is_absent() {
    let (mut a, mut b) = pair();
    let w1 = a
        .send(put("alpha", ins('x')))
        .expect("a fresh key takes a string");
    let w2 = a
        .send(put("alpha", ins('y')))
        .expect("and another character");
    assert_eq!(read(&a), json!({"alpha": "yx"}));
    let r = a.send(remove("alpha")).expect("a remove is always enabled");
    assert_eq!(read(&a), json!({}), "the remover");
    b.receive(w1);
    b.receive(w2);
    b.receive(r);
    assert_eq!(read(&b), json!({}), "a peer that delivered everything");
}

/// F3: with no remove, a key bound to a counter refuses a string insert, at
/// the writer and at a peer that delivered the increment.
#[test]
fn ground_truth_without_a_remove_another_kind_is_refused() {
    let (mut a, mut b) = pair();
    let w = a
        .send(put("gamma", inc(1.0)))
        .expect("a fresh key takes a counter");
    b.receive(w);
    assert!(!accepts(&a, &put("gamma", ins('x'))), "the writer");
    assert!(!accepts(&b, &put("gamma", ins('x'))), "the peer");
    assert!(
        accepts(&a, &put("gamma", inc(1.0))),
        "its own kind still is"
    );
}

/// F4: at the replica that removed a counter key (the remove having seen
/// every increment), the binding is released: an increment and a string
/// insert are both accepted, and the string reads alone.
#[test]
fn ground_truth_a_remove_releases_the_binding_where_it_has_been_delivered() {
    let (mut a, mut b) = pair();
    let w = a
        .send(put("gamma", inc(3.0)))
        .expect("a fresh key takes a counter");
    b.receive(w);
    let r = b.send(remove("gamma")).expect("a remove is always enabled");
    assert!(
        accepts(&b, &put("gamma", inc(1.0))),
        "an increment, at the remover"
    );
    assert!(
        accepts(&b, &put("gamma", ins('x'))),
        "a string insert, at the remover"
    );
    a.receive(r);
    assert!(
        accepts(&a, &put("gamma", ins('x'))),
        "a string insert, at the writer once it has the remove"
    );
    b.send(put("gamma", ins('x')))
        .expect("accepted, as checked");
    assert_eq!(read(&b), json!({"gamma": "x"}), "the string reads alone");
}

/// F5: at a replica the remove has not reached, the binding holds.
#[test]
fn ground_truth_the_binding_holds_where_the_remove_has_not_arrived() {
    let (mut a, mut b) = pair();
    let w = a
        .send(put("gamma", inc(3.0)))
        .expect("a fresh key takes a counter");
    b.receive(w);
    let _r = b.send(remove("gamma")).expect("a remove is always enabled");
    assert!(
        accepts(&b, &put("gamma", ins('x'))),
        "released at the remover"
    );
    assert!(
        !accepts(&a, &put("gamma", ins('x'))),
        "still bound at a replica that has not delivered the remove"
    );
}
