//! NW.31f oracle: the node-level contract cases of moirai-netstack-testkit
//! (vault spec/items/NW.31f.md, NW.31 design row S5b).
//!
//! Two parts. (A) every case passes on the toy, whose replicas offer no state
//! transfer (the two transfer cases report it not offered), and the two
//! transfer cases pass on the oracle's offering toy, reporting it offered.
//! (B) each case can fail: a wrapper around the replica changes one rule and
//! the case must panic. The foreign-log case has no replica rule to bend (the
//! filter is the node's); its red is the MUT arm the spec names.
//!
//! Sockets on 127.0.0.1:0; run under nodes.lock (tests/nw31f/gate.sh).

#[path = "../../tests/nw31f/common.rs"]
mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};

use common::OfferingFactory;
use moirai_netstack::{Opaque, Progress, ReplicaFactory, ReplicaPort};
use moirai_netstack_testkit::ToyFactory;
use moirai_netstack_testkit::contract::Inputs;
use moirai_netstack_testkit::contract_node::{self, Transfer};
use serde_json::{Value, json};

fn inputs() -> Inputs {
    Inputs {
        ops: ["red", "green", "blue", "amber", "teal", "plum"]
            .map(|s| json!(s))
            .to_vec(),
        refused_op: json!(42),
        malformed_remote: json!(42),
    }
}

// ---- (A) the cases pass ------------------------------------------------------

#[test]
fn toy_passes_three_nodes_converge() {
    contract_node::three_nodes_converge(ToyFactory::default, &inputs());
}
#[test]
fn toy_passes_catch_up_after_pause() {
    contract_node::catch_up_after_pause(ToyFactory::default, &inputs());
}
#[test]
fn toy_passes_late_joiner_by_delta() {
    assert_eq!(
        contract_node::late_joiner(ToyFactory::default, &inputs()),
        Transfer::NotOffered
    );
}
#[test]
fn toy_passes_foreign_log_filtered() {
    contract_node::foreign_log_filtered(ToyFactory::default, &inputs());
}
#[test]
fn toy_passes_returning_member_refused() {
    assert_eq!(
        contract_node::returning_member_refused(ToyFactory::default, &inputs()),
        Transfer::NotOffered
    );
}
#[test]
fn toy_passes_batch_stale_refusal() {
    contract_node::batch_stale_refusal(ToyFactory::default, &inputs());
}
#[test]
fn offering_toy_passes_late_joiner_by_transfer() {
    assert_eq!(
        contract_node::late_joiner(OfferingFactory::default, &inputs()),
        Transfer::Offered
    );
}
#[test]
fn offering_toy_passes_returning_member_refused() {
    assert_eq!(
        contract_node::returning_member_refused(OfferingFactory::default, &inputs()),
        Transfer::Offered
    );
}

// ---- (B) each case can fail ----------------------------------------------------

/// One rule changed around a correct replica.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Bend {
    /// `apply_remote` answers Ok and applies nothing.
    ApplyDropped,
    /// `ops_since` answers the replica's own frontier: a delta carries nothing.
    OpsSinceEmpty,
    /// Every peer counts as one this replica has history from.
    HistoryAlways,
    /// No peer counts as one this replica has history from.
    HistoryNever,
    /// `delivered` stays 0, so the version never moves.
    ProgressZero,
}

struct Bent<R> {
    inner: R,
    bend: Bend,
}

impl<R: ReplicaPort> ReplicaPort for Bent<R> {
    fn replica_id(&self) -> &str {
        self.inner.replica_id()
    }
    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        self.inner.submit(op)
    }
    fn apply_remote(&mut self, from: &str, ops: Opaque) -> Result<(), String> {
        if self.bend == Bend::ApplyDropped {
            return Ok(());
        }
        self.inner.apply_remote(from, ops)
    }
    fn frontier(&self) -> Opaque {
        self.inner.frontier()
    }
    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
        if self.bend == Bend::OpsSinceEmpty {
            let own = self.inner.frontier();
            return self.inner.ops_since(own);
        }
        self.inner.ops_since(frontier)
    }
    fn progress(&self) -> Progress {
        if self.bend == Bend::ProgressZero {
            return Progress::default();
        }
        self.inner.progress()
    }
    fn members(&self) -> Vec<String> {
        self.inner.members()
    }
    fn has_history_from(&self, peer: &str) -> bool {
        match self.bend {
            Bend::HistoryAlways => true,
            Bend::HistoryNever => false,
            _ => self.inner.has_history_from(peer),
        }
    }
    fn read_state(&self) -> Value {
        self.inner.read_state()
    }
    fn snapshot_for(&mut self, requester: &str) -> Option<Result<Opaque, String>> {
        self.inner.snapshot_for(requester)
    }
    fn submit_batch(&mut self, ops: Vec<Value>) -> (usize, Option<String>, Vec<Opaque>) {
        self.inner.submit_batch(ops)
    }
}

struct BentFactory<F> {
    inner: F,
    bend: Bend,
}

impl<F: ReplicaFactory> ReplicaFactory for BentFactory<F> {
    type Replica = Bent<F::Replica>;
    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Bent<F::Replica> {
        Bent {
            inner: self.inner.open(log, self_id, members),
            bend: self.bend,
        }
    }
}

macro_rules! fails {
    ($($name:ident: $case:ident on $base:ident, $bend:ident;)*) => {$(
        #[test]
        fn $name() {
            let make = || BentFactory { inner: $base::default(), bend: Bend::$bend };
            let r = catch_unwind(AssertUnwindSafe(|| {
                let _ = contract_node::$case(make, &inputs());
            }));
            assert!(
                r.is_err(),
                "{} passed on a {} replica bent by {:?}",
                stringify!($case),
                stringify!($base),
                Bend::$bend
            );
        }
    )*};
}

fails! {
    three_nodes_fails_when_apply_is_dropped: three_nodes_converge on ToyFactory, ApplyDropped;
    catch_up_fails_when_ops_since_sends_nothing: catch_up_after_pause on ToyFactory, OpsSinceEmpty;
    late_joiner_fails_when_ops_since_sends_nothing: late_joiner on ToyFactory, OpsSinceEmpty;
    late_joiner_fails_when_every_donor_holds_history_from_the_joiner: late_joiner on OfferingFactory, HistoryAlways;
    returning_member_fails_when_history_is_never_held: returning_member_refused on ToyFactory, HistoryNever;
    returning_member_fails_when_history_is_never_held_and_a_transfer_is_offered: returning_member_refused on OfferingFactory, HistoryNever;
    batch_stale_fails_when_progress_is_always_zero: batch_stale_refusal on ToyFactory, ProgressZero;
}
