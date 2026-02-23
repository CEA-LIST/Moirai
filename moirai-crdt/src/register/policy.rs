use std::cmp::Ordering;
use std::fmt::Debug;

use moirai_protocol::event::tag::Tag;

pub trait Policy: Ord + Clone + Debug {
    fn compare(a: &Tag, b: &Tag) -> Ordering
    where
        Self: Sized;
}

/// # Last-Writer-Wins (LWW)
///
/// a -> b => Lamport(a) < Lamport(b)
/// Lamport(a) < Lamport(b) => a -> b || a conc b
/// Because of the causal broadcast, new_op can only be concurrent or causally after old_op.
/// The new op is redundant if there is an old op that is concurrent to it and has a higher origin identifier.
/// i.e. (t, o) R s = \exists (t', o') \in s : t ≮ t' \land t.id < t'.id
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwwPolicy;

impl Policy for LwwPolicy {
    fn compare(a: &Tag, b: &Tag) -> Ordering {
        // First, compare Lamport timestamps
        match a.lamport().cmp(b.lamport()) {
            Ordering::Equal => {
                // Tie-break using origin id
                a.id().origin_id().cmp(b.id().origin_id())
            }
            other_order => other_order,
        }
    }
}

impl Ord for LwwPolicy {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl PartialOrd for LwwPolicy {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// Wrapper with reference for convenient comparison
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lww<'a>(pub &'a Tag);

impl Ord for Lww<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        LwwPolicy::compare(self.0, other.0)
    }
}

impl PartialOrd for Lww<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// First-Writer-Wins
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FwwPolicy;

impl Policy for FwwPolicy {
    fn compare(a: &Tag, b: &Tag) -> Ordering {
        // First, compare Lamport timestamps
        match a.lamport().cmp(b.lamport()) {
            Ordering::Equal => {
                // Tie-break using origin id
                b.id().origin_id().cmp(a.id().origin_id())
            }
            other_order => other_order,
        }
    }
}

impl Ord for FwwPolicy {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl PartialOrd for FwwPolicy {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// Wrapper with reference for convenient comparison
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fww<'a>(pub &'a Tag);

impl Ord for Fww<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        FwwPolicy::compare(self.0, other.0)
    }
}

impl PartialOrd for Fww<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// Fair (https://amturing.acm.org/p558-lamport.pdf)
/// Use a round-robin policy to break ties.
/// For example, if C_i(a) = C_j(b) and j < i then we can let a -> b
/// if j < C_i(a) mod N <= i, and b -> a otherwise; where N is the total number of processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FairPolicy;

impl Policy for FairPolicy {
    fn compare(a: &Tag, b: &Tag) -> Ordering {
        match a.lamport().cmp(b.lamport()) {
            Ordering::Equal => {
                let val = a.lamport().val();
                let mut members = a.id().resolver().into_vec();
                members.sort_unstable();
                let n = members.len();
                let round_leader = val % n;
                let self_idx = members
                    .iter()
                    .position(|r| *r == *a.id().origin_id())
                    .unwrap();
                let other_idx = members
                    .iter()
                    .position(|r| *r == *b.id().origin_id())
                    .unwrap();

                if other_idx < self_idx && other_idx < round_leader && round_leader <= self_idx {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            other_order => other_order,
        }
    }
}

impl Ord for FairPolicy {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl PartialOrd for FairPolicy {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// Wrapper with reference for convenient comparison
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fair<'a>(pub &'a Tag);

impl Ord for Fair<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        FairPolicy::compare(self.0, other.0)
    }
}

impl PartialOrd for Fair<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        event::{id::EventId, lamport::Lamport, tag::Tag},
        replica::ReplicaIdx,
        utils::intern_str::Interner,
    };

    use crate::register::policy::Fair;

    #[test]
    fn test_fair() {
        let mut interner = Interner::new();
        interner.intern("A");
        interner.intern("B");
        interner.intern("C");

        let resolver = interner.resolver();

        let a = Tag::new(
            EventId::new(ReplicaIdx(0), 1, resolver.clone()),
            Lamport::new(1),
        );

        let b = Tag::new(
            EventId::new(ReplicaIdx(1), 1, resolver.clone()),
            Lamport::new(1),
        );

        let ab = Fair(&a).cmp(&Fair(&b));
        let ba = Fair(&b).cmp(&Fair(&a));

        assert_eq!(ab, ba.reverse(), "antisymmetry violated: Ord is invalid");
    }

    #[test]
    fn test_fair_2() {
        let mut interner = Interner::new();
        interner.intern("A");
        interner.intern("B");
        interner.intern("C");

        let resolver = interner.resolver();

        let a = Tag::new(
            EventId::new(ReplicaIdx(0), 1, resolver.clone()),
            Lamport::new(1),
        );

        let b = Tag::new(
            EventId::new(ReplicaIdx(1), 1, resolver.clone()),
            Lamport::new(1),
        );

        let c = Tag::new(
            EventId::new(ReplicaIdx(2), 1, resolver.clone()),
            Lamport::new(1),
        );

        let ab = Fair(&a).cmp(&Fair(&b));
        let bc = Fair(&b).cmp(&Fair(&c));
        let ac = Fair(&a).cmp(&Fair(&c));

        assert_eq!(ab, bc);
        assert_eq!(ab, ac);
    }
}
