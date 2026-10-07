//! ST.12 part (4): what `tests/e2e_convergence.rs` says a `remove` does to
//! the rendered state is what the library does after ST.10.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.12.md`. The comment that
//! opens the NW.08 scenarios of `e2e_convergence.rs` gave two examples of an
//! operation that changes nothing a replica renders: a second `enable` of a
//! flag, and a `remove` of a key already at its default. The second was true
//! before ST.10's presence rule (`42ee5ab`, `union!` in `moirai-macros`),
//! when a remove reset the key's value and left the key in the render. Since
//! ST.10 a remove that no concurrent update keeps makes the key absent, so a
//! remove of a key at its default (an emptied string, a counter reset to
//! zero) takes the key out of the render: it is not an operation that renders
//! nothing. The validator corrected the comment in the oracle commit, since
//! `e2e_convergence.rs` is an oracle path; this file keeps it corrected.
//!
//! Two kinds of test, kept apart, as in `st11_workload_doc.rs`:
//!
//! - The `ground_truth_*` tests say what is true, on `moirai_crdt::json`,
//!   whose `Json` is the same `union!` (this workspace's `moirai-macros`, by
//!   path) that the Arachne-generated `json_crdt`'s `JsonKind` expands, the
//!   runtime `e2e_convergence.rs`'s `network_node` hosts.
//! - `e2e_convergence_no_longer_counts_a_remove_at_default_among_the_operations_that_render_nothing`
//!   reads the comment text: the stale example is gone and the corrected
//!   sentence is there.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-network --test st12_e2e_comment
//! ```

use moirai_crdt::{
    counter::resettable_counter::Counter, json::Json, list::eg_walker::List, map::uw_map::UWMap,
    query::read_as_json::ReadAsJson,
};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    log_id::LogId,
    replica::{IsReplica, Replica},
};
use serde_json::{json, Value};

type JsonReplica = Replica<moirai_crdt::json::JsonLog, Tcsb<Json>>;

// ---------------------------------------------------------------------------
// The comment
// ---------------------------------------------------------------------------

/// `e2e_convergence.rs`'s comment text, markers stripped, lower-cased, with
/// backticks and asterisks dropped and whitespace collapsed, so a sentence is
/// found however it is wrapped or emphasised (`st11_workload_doc.rs`'s
/// normalisation).
fn comment_text() -> String {
    let source = include_str!("e2e_convergence.rs");
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

/// The parenthesis of `e2e_convergence.rs:3712-3713` at `3036102`, as
/// `comment_text` normalises it: a remove of a key at its default listed
/// among the operations that change nothing a replica renders.
const STALE: &str = "(a second enable of a flag, a remove of a key already at its default)";

/// The sentence the corrected comment carries, normalised.
const CORRECTED: &str = "since st.10 a remove that no concurrent update keeps makes the key \
                         absent, which changes the render";

#[test]
fn e2e_convergence_no_longer_counts_a_remove_at_default_among_the_operations_that_render_nothing() {
    let text = comment_text();
    let stale = text.contains(STALE);
    let corrected = text.contains(CORRECTED);
    assert!(
        !stale && corrected,
        "e2e_convergence.rs: the pre-ST.10 example {} ({STALE:?}); the corrected \
         sentence {} ({CORRECTED:?})",
        if stale { "is still there" } else { "is gone" },
        if corrected { "is there" } else { "is missing" },
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

fn ins(content: char, pos: usize) -> Json {
    Json::String(List::insert(content, pos))
}

fn del(pos: usize) -> Json {
    Json::String(List::delete(pos))
}

fn inc(by: f64) -> Json {
    Json::Number(Counter::Inc(by))
}

fn reset() -> Json {
    Json::Number(Counter::Reset)
}

fn pair() -> (JsonReplica, JsonReplica) {
    let log_id = LogId::from_bytes([0x5e; 16]);
    (
        JsonReplica::bootstrap_with_log_id("a".into(), &["a", "b"], log_id.clone()),
        JsonReplica::bootstrap_with_log_id("b".into(), &["a", "b"], log_id),
    )
}

fn read(replica: &JsonReplica) -> Value {
    replica.query(&ReadAsJson::new())
}

/// A text key emptied by a delete is at its default and is rendered, as `""`;
/// a remove that saw every update to it takes it out of the render, at the
/// remover and at a peer that delivered everything. Before the peer delivers
/// the remove, the two renders differ: the remove changed what is rendered.
#[test]
fn ground_truth_a_remove_of_an_emptied_text_key_changes_the_render() {
    let (mut a, mut b) = pair();
    let w = a.send(put("alpha", ins('x', 0))).expect("a fresh key");
    let d = a.send(put("alpha", del(0))).expect("its one character out");
    b.receive(w);
    b.receive(d);
    assert_eq!(read(&a), json!({"alpha": ""}), "the emptied key, at a");
    assert_eq!(read(&b), json!({"alpha": ""}), "the emptied key, at b");
    let r = a.send(remove("alpha")).expect("a remove is always enabled");
    assert_eq!(read(&a), json!({}), "the remover");
    assert_ne!(
        read(&a),
        read(&b),
        "before b delivers the remove, the renders differ"
    );
    b.receive(r);
    assert_eq!(read(&b), json!({}), "a peer that delivered the remove");
}

/// The same for a counter key reset to zero.
#[test]
fn ground_truth_a_remove_of_a_counter_reset_to_zero_changes_the_render() {
    let (mut a, mut b) = pair();
    let w = a.send(put("gamma", inc(3.0))).expect("a fresh key");
    let z = a.send(put("gamma", reset())).expect("back to zero");
    b.receive(w);
    b.receive(z);
    assert_eq!(read(&b), json!({"gamma": 0.0}), "the key at zero, at b");
    let r = b.send(remove("gamma")).expect("a remove is always enabled");
    assert_eq!(read(&b), json!({}), "the remover");
    a.receive(r);
    assert_eq!(read(&a), json!({}), "a peer that delivered the remove");
}

/// The qualifier of the corrected sentence: an update concurrent with the
/// remove keeps the key, with that update's effect alone.
#[test]
fn ground_truth_an_update_concurrent_with_the_remove_keeps_the_key() {
    let (mut a, mut b) = pair();
    let w = a.send(put("alpha", ins('x', 0))).expect("a fresh key");
    b.receive(w);
    let r = a.send(remove("alpha")).expect("a remove is always enabled");
    let u = b
        .send(put("alpha", ins('y', 1)))
        .expect("b appends, not having seen the remove");
    a.receive(u);
    b.receive(r);
    assert_eq!(read(&a), read(&b), "the replicas agree");
    assert_eq!(
        read(&a),
        json!({"alpha": "y"}),
        "the key stays, with the concurrent update's effect alone"
    );
}
