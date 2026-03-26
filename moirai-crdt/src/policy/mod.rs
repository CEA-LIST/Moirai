use std::{cmp::Ordering, fmt::Debug};

use moirai_protocol::{crdt::policy::Policy, event::tag::Tag};

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
                if a.id() == b.id() {
                    return Ordering::Equal;
                }

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

                let self_rank = (self_idx + n - round_leader) % n;
                let other_rank = (other_idx + n - round_leader) % n;

                self_rank.cmp(&other_rank)
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
    use std::cmp::Ordering;

    use moirai_protocol::{
        event::{id::EventId, lamport::Lamport, tag::Tag},
        replica::ReplicaIdx,
        utils::intern_str::Interner,
    };

    use crate::policy::Fair;

    fn fair_cmp(num_members: usize, lamport: usize, left: usize, right: usize) -> Ordering {
        let mut interner = Interner::new();
        for idx in 0..num_members {
            interner.intern(&format!("R{idx}"));
        }

        let resolver = interner.resolver();
        let left = Tag::new(
            EventId::new(ReplicaIdx(left), 1, resolver.clone()),
            Lamport::new(lamport),
        );
        let right = Tag::new(
            EventId::new(ReplicaIdx(right), 1, resolver.clone()),
            Lamport::new(lamport),
        );

        Fair(&left).cmp(&Fair(&right))
    }

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

        assert_eq!(ab, Ordering::Greater);
        assert_eq!(bc, Ordering::Less);
        assert_eq!(ac, Ordering::Greater);
    }

    #[test]
    fn test_fair_reflexive() {
        let mut interner = Interner::new();
        interner.intern("A");
        interner.intern("B");
        interner.intern("C");

        let resolver = interner.resolver();
        let a = Tag::new(
            EventId::new(ReplicaIdx(0), 1, resolver.clone()),
            Lamport::new(1),
        );

        assert_eq!(Fair(&a).cmp(&Fair(&a)), Ordering::Equal);
    }

    #[test]
    fn test_fair_antisymmetric_for_all_pairs_and_rounds_with_four_members() {
        for lamport in 0..8 {
            for left in 0..4 {
                for right in 0..4 {
                    let lr = fair_cmp(4, lamport, left, right);
                    let rl = fair_cmp(4, lamport, right, left);

                    assert_eq!(
                        lr,
                        rl.reverse(),
                        "lamport={lamport}, left={left}, right={right}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_fair_round_robin_order_with_four_members() {
        let expected = [2, 3, 0, 1];
        let mut interner = Interner::new();
        for idx in 0..4 {
            interner.intern(&format!("R{idx}"));
        }

        let resolver = interner.resolver();
        let tags: Vec<_> = (0..4)
            .map(|idx| {
                Tag::new(
                    EventId::new(ReplicaIdx(idx), 1, resolver.clone()),
                    Lamport::new(2),
                )
            })
            .collect();

        let mut ordered: Vec<_> = (0..4).collect();
        ordered.sort_by(|left, right| Fair(&tags[*left]).cmp(&Fair(&tags[*right])));

        assert_eq!(ordered, expected);
    }
}
