use std::{fmt::Debug, hash::Hash};

use moirai_protocol::state::po_log::VecLog;

use crate::{FxHashMap, counter::Counter};

pub mod aw_bag;
pub mod rw_bag;
pub mod update;

/// The width one value's copy count is kept in.
///
/// It is signed: two replicas that each observe the single copy of a value and
/// each remove it both issue a valid remove, so the net is `1 - 1 - 1`, which a
/// `usize` cannot hold. Clamping each subtraction would not converge (the
/// unstable fold visits operations in delivery order), so the net is kept exact
/// and clamped once, at the read-out ([`counts`]).
pub type Count = isize;

/// The log one value's copy count lives in.
pub type CountLog = VecLog<Counter<Count>>;

/// The copy counts a bag reads out, from the signed nets its map holds: a value
/// whose net is zero or below holds no copy.
pub fn counts<V>(nets: FxHashMap<V, Count>) -> FxHashMap<V, usize>
where
    V: Clone + Debug + Hash + Eq,
{
    nets.into_iter()
        .filter(|(_, net)| *net > 0)
        .map(|(value, net)| (value, net as usize))
        .collect()
}
