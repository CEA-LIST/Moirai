use std::{fmt::Debug, hash::Hash};

use moirai_protocol::state::po_log::VecLog;

use crate::{HashMap, counter::resettable_counter::Counter};

pub mod aw_bag;
pub mod rw_bag;

/// The width one value's copy count is kept in.
///
/// It is signed, and that is the whole of the bag's underflow fix. A bag is a
/// map of counters with `Add` spelled `Inc(1)` and `Remove` spelled `Dec(1)`,
/// and two replicas that each observe the single copy of a value and each
/// remove it both issue a valid remove: no workload can avoid the pair. The
/// net is then `1 - 1 - 1`, which a `usize` cannot hold, so the fold panicked
/// with `attempt to subtract with overflow` — on the next read in
/// `counter/resettable_counter.rs`, or on receipt in `counter/stable.rs` when
/// the two removes stabilized first.
///
/// Clamping each subtraction where it happens would not do: the unstable fold
/// visits operations in delivery order, which differs between replicas, and
/// `Inc(1), Dec(1), Dec(1)` clamps to `0` while `Dec(1), Dec(1), Inc(1)`
/// clamps to `1`. A saturating counter does not converge. Keeping the net
/// exact in a width that can hold it does, because addition commutes, and the
/// clamp then happens once, at the read-out, where [`counts`] states it.
pub type Count = isize;

/// The log one value's copy count lives in, on both paths.
///
/// `AWBagLog` and `RWBagLog` compose it here; `moirai-interp`'s `LeafLog::Bag`
/// spells the same composition out over the same library types, so naming the
/// width once is what makes the two paths agree on it.
pub type CountLog = VecLog<Counter<Count>>;

/// The copy counts a bag reads out, from the signed nets its map holds.
///
/// A value whose net has been driven to zero or below holds no copy, and a bag
/// holds no negative number of anything: **a remove that would take a value's
/// count below zero does nothing**, which is what an observed-remove bag means
/// — you cannot remove what you do not observe. The map's own read-out already
/// drops a child at its default, so this drops the ones the double remove took
/// past it.
///
/// It is an aggregate rule and not a per-operation one, so a value driven to a
/// net of `-1` and then added to once more reads as absent rather than as one
/// copy: the removes that crossed are remembered as a debt against that value
/// until a `Clear` or a map `Remove` empties the child. Spending that debt
/// instead would mean giving every copy an identity and letting a remove name
/// the copies it observed, which is a change to the operation vocabulary and
/// Léo Olivier's call, not a repair.
pub fn counts<V>(nets: HashMap<V, Count>) -> HashMap<V, usize>
where
    V: Clone + Debug + Hash + Eq,
{
    let mut counts: HashMap<V, usize> = HashMap::default();
    for (value, net) in nets {
        if net > 0 {
            counts.insert(value, net as usize);
        }
    }
    counts
}
