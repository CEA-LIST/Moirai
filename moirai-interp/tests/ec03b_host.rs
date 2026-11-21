//! EC.03b: the log knows which replica hosts it, whatever that replica's
//! index in its member table.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03b.md`. A rebase from `a`
//! that `b` has delivered and `c` has not: every replica but `c` reads that
//! the rebase still waits for `c` only, never for itself. At `748a185` the
//! log is told its host (`IsLog::hosted_by`); Léo's API makes no promise
//! about where a replica sits in its table, so here the replicas are built
//! with the table `[a, b, c]` (and `[a, b, c, j]` for the joiner), which puts
//! `b`, `c` and `j` away from index 0.

#[path = "ec03/side.rs"]
mod side;

use moirai_interp::migrate::rebase;
use moirai_interp::testing::install;
use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{broadcast::tcsb::Tcsb, replica::IsReplica, replica::Replica};
use serde_json::{Value, json};

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

const D1: &str = include_str!("fixtures/si12/simpleuml.d1.metamodel.json");
const D2: &str = include_str!("fixtures/si12/simpleuml.d2.metamodel.json");
const TABLE: [&str; 3] = ["a", "b", "c"];

fn waiting_for(replica: &Rep) -> Value {
    replica.state().migration()["rebase"]["waiting_for"].clone()
}

/// `a` installs D1, everyone delivers it, `a` rebases to D2, `b` delivers
/// the rebase and `c` does not.
fn held() -> [Rep; 3] {
    let mut r = TABLE.map(|id| side::bootstrap_in_order::<ModelLog>(id, &TABLE));
    let d1: Value = serde_json::from_str(D1).expect("D1 is JSON");
    let open = r[0]
        .send(install("ec03b", &d1))
        .expect("a fresh log takes its Install");
    r[1].receive(open.clone());
    r[2].receive(open);
    let op = rebase(r[0].state(), D2, &[]).expect("D1 rebases to D2");
    let moved = r[0].send(op).expect("the rebase is enabled");
    r[1].receive(moved);
    r
}

#[test]
fn ec03b_h1_a_replica_away_from_index_0_does_not_wait_for_itself() {
    let r = held();
    assert_eq!(waiting_for(&r[0]), json!(["b", "c"]), "a, at index 0");
    assert_eq!(waiting_for(&r[1]), json!(["c"]), "b, at index 1");
}

#[test]
fn ec03b_h2_a_joiner_away_from_index_0_does_not_wait_for_itself() {
    use moirai_protocol::broadcast::tcsb::StateSnapshot;
    let r = held();
    let snapshot: StateSnapshot<ModelOp> =
        serde_json::from_value(serde_json::to_value(r[1].snapshot()).unwrap()).unwrap();
    let log: ModelLog =
        serde_json::from_value(serde_json::to_value(r[1].state()).unwrap()).unwrap();
    let mut j = side::bootstrap_in_order::<ModelLog>("j", &["a", "b", "c", "j"]);
    j.adopt(snapshot, log);
    assert_eq!(
        waiting_for(&j),
        json!(["c"]),
        "j, at index 3, adopted from b"
    );
}
