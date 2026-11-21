//! EC.03: the replica layer a test needs, on the reference `748a185`
//! (`akira/moirai-modelstore`). `ec03/run.sh` copies this file over
//! `ec03/side.rs` in the reference tree only.
#![allow(dead_code, unused_imports)]

use moirai_protocol::{broadcast::tcsb::Tcsb, log_id::LogId, replica::Replica, state::log::IsLog};

pub use moirai_crdt::utils::membership::{triplet_log, twins_log};
use moirai_protocol::replica::IsReplica;

/// The operation type a log stores.
pub type Update<L> = <L as IsLog>::Op;

/// One replica of a log shared by `members`. Every replica a test makes this
/// way names one fixed log, so replicas of one model accept each other's
/// events, as every replica does on Léo's API, which has no log id.
pub fn bootstrap<L: IsLog>(id: &str, members: &[&str]) -> Replica<L, Tcsb<L::Op>> {
    let log_id = LogId::parse("ec03ec03ec03ec03ec03ec03ec03ec03").expect("a log id");
    Replica::bootstrap_with_log_id(id.to_string(), members, log_id)
}

/// The same: the reference's log is told which replica hosts it
/// (`IsLog::hosted_by`), whatever its index. EC.03b.
pub fn bootstrap_in_order<L: IsLog>(id: &str, members: &[&str]) -> Replica<L, Tcsb<L::Op>> {
    bootstrap(id, members)
}

/// A donor takes a joiner into its member table before a state transfer, as
/// `model_node` does (`st14_sequence_order_cache.rs`'s `transfer` at
/// `748a185`). EC.03b.
pub fn admit<L: IsLog>(donor: &mut Replica<L, Tcsb<L::Op>>, joiner: &Replica<L, Tcsb<L::Op>>) {
    use moirai_protocol::broadcast::{internalizer::lock_interner, tcsb::IsTcsbTest};
    lock_interner(donor.tcsb().interner()).intern(joiner.id());
}

/// A fresh replica that will join `donor`'s log by state transfer: the line of
/// `st14_sequence_order_cache.rs`'s `transfer` at `748a185`. EC.03b.
pub fn joiner<L: IsLog>(donor: &Replica<L, Tcsb<L::Op>>, id: &str) -> Replica<L, Tcsb<L::Op>> {
    Replica::bootstrap_with_log_id(id.to_string(), &[id], donor.log_id().clone())
}

/// How many events the replica has delivered. EC.03b.
pub fn delivered<L: IsLog>(replica: &Replica<L, Tcsb<L::Op>>) -> usize {
    replica.num_delivered_events()
}

/// What the replica knows to be causally stable. EC.03c2b.
pub fn stability<L: IsLog>(
    replica: &Replica<L, Tcsb<L::Op>>,
) -> moirai_protocol::broadcast::tcsb::StabilitySnapshot {
    replica.stability()
}

/// The log a replica's frames name. EC.03c2b.
pub fn log_id<L: IsLog>(replica: &Replica<L, Tcsb<L::Op>>) -> LogId {
    replica.log_id().clone()
}
