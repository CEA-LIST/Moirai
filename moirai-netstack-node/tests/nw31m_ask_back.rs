//! NW.31m oracle on the toy (vault spec/items/NW.31m.md): the ask-back on
//! SyncRequest, the cases of tests/nw31m/cases.rs on ToyFactory.
//!
//! Sockets on 127.0.0.1:0; run under nodes.lock (tests/nw31m/gate.sh).

#[path = "../../tests/nw31m/cases.rs"]
mod cases;

use moirai_netstack_testkit::ToyFactory;
use serde_json::{Value, json};

fn ops() -> Vec<Value> {
    ["red", "green", "blue", "amber", "teal", "plum"]
        .map(|s| json!(s))
        .to_vec()
}

#[test]
fn nw31m_toy_asks_back_once_per_unchanged_lead() {
    cases::asks_back_once_per_unchanged_lead(ToyFactory::default, &ops());
}

#[test]
fn nw31m_toy_no_ask_back_when_the_frontier_is_covered() {
    cases::no_ask_back_when_the_frontier_is_covered(ToyFactory::default, &ops());
}

#[test]
fn nw31m_toy_a_log_with_no_history_does_not_ask_back() {
    cases::a_log_with_no_history_does_not_ask_back(ToyFactory::default, &ops());
}

#[test]
fn nw31m_toy_one_sided_pause_then_resume() {
    cases::one_sided_pause_then_resume(ToyFactory::default, &ops());
}
