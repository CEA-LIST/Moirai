//! NW.31m oracle on upstream Moirai replicas through moirai-netstack-crdt
//! (vault spec/items/NW.31m.md): the cases of tests/nw31m/cases.rs on the
//! add-wins set of strings.
//!
//! Sockets on 127.0.0.1:0; run under nodes.lock (tests/nw31m/gate.sh).

#[path = "../../../tests/nw31m/cases.rs"]
mod cases;

use moirai_netstack_crdt::awset;
use serde_json::{Value, json};

fn ops() -> Vec<Value> {
    ["red", "green", "blue", "amber", "teal", "plum"]
        .map(|s| json!({ "Add": s }))
        .to_vec()
}

#[test]
fn nw31m_upstream_asks_back_once_per_unchanged_lead() {
    cases::asks_back_once_per_unchanged_lead(awset::factory, &ops());
}

#[test]
fn nw31m_upstream_no_ask_back_when_the_frontier_is_covered() {
    cases::no_ask_back_when_the_frontier_is_covered(awset::factory, &ops());
}

#[test]
fn nw31m_upstream_a_log_with_no_history_does_not_ask_back() {
    cases::a_log_with_no_history_does_not_ask_back(awset::factory, &ops());
}

#[test]
fn nw31m_upstream_one_sided_pause_then_resume() {
    cases::one_sided_pause_then_resume(awset::factory, &ops());
}
