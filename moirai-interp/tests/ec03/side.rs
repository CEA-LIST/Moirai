//! EC.03: the replica layer a test needs, on Léo's Moirai `5be63b1` API.
//!
//! The reference copy of the interpreter (`748a185`) is driven through the
//! same test sources with `side_748a185.rs` in this file's place
//! (`ec03/run.sh` swaps it in). Nothing else differs between the two builds
//! of a test.
#![allow(dead_code, unused_imports)]

use moirai_protocol::{broadcast::tcsb::Tcsb, replica::Replica, state::log::IsLog};

pub use moirai_crdt::utils::bootstrap::{triplet_log, twins_log};

/// The operation type a log stores.
pub type Update<L> = <L as IsLog>::Update;

/// One replica of a log shared by `members`.
pub fn bootstrap<L: IsLog>(id: &str, members: &[&str]) -> Replica<L, Tcsb<L::Update>> {
    Replica::bootstrap(id.into(), members)
}

/// The same, with the member table in the order given, so the replica need
/// not sit at index 0 (`Replica::bootstrap` puts it there). EC.03b.
pub fn bootstrap_in_order<L: IsLog>(id: &str, members: &[&str]) -> Replica<L, Tcsb<L::Update>> {
    use moirai_protocol::broadcast::{internalizer::Interner, tcsb::IsTcsb};
    let mut interner = Interner::new();
    for member in members {
        interner.intern(&(*member).into());
    }
    let idx = interner.get(&id.into()).expect("the replica is a member");
    Replica::new(id.into(), Tcsb::new(idx, interner), L::default())
}

/// A donor takes a joiner into its member table before a state transfer, as
/// it does when the joiner first asks it what it misses. EC.03b.
pub fn admit<L: IsLog>(
    donor: &mut Replica<L, Tcsb<L::Update>>,
    joiner: &Replica<L, Tcsb<L::Update>>,
) {
    use moirai_protocol::replica::IsReplica;
    let _ = donor.pull(joiner.since());
}

/// A fresh replica that will join `donor`'s log by state transfer. EC.03b.
pub fn joiner<L: IsLog>(
    _donor: &Replica<L, Tcsb<L::Update>>,
    id: &str,
) -> Replica<L, Tcsb<L::Update>> {
    Replica::bootstrap(id.into(), &[id])
}

/// How many events the replica has delivered. EC.03b.
pub fn delivered<L: IsLog>(replica: &Replica<L, Tcsb<L::Update>>) -> usize {
    replica.delivered_count()
}

/// What the replica knows to be causally stable, `748a185`'s
/// `Replica::stability`; on Léo's line it is the `Tcsb`'s. EC.03c2b.
pub fn stability<L: IsLog>(
    replica: &Replica<L, Tcsb<L::Update>>,
) -> moirai_protocol::broadcast::tcsb::StabilitySnapshot {
    replica.tcsb().stability()
}

/// The log a replica's frames name. Léo's replica has none, so every replica
/// a test makes names the one fixed log `side_748a185.rs`'s `bootstrap` gives
/// them. EC.03c2b.
#[cfg(feature = "network")]
pub fn log_id<L: IsLog>(_replica: &Replica<L, Tcsb<L::Update>>) -> moirai_network::log_id::LogId {
    moirai_network::log_id::LogId::parse("ec03ec03ec03ec03ec03ec03ec03ec03").expect("a log id")
}
