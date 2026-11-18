use std::{convert::Infallible, fmt::Debug, hash::Hash, marker::PhantomData};

use crate::{
    clock::causal_frontier::CausalFrontier,
    crdt::{
        eval::Eval,
        policy::{LwwPolicy, Policy},
        query::{Contains, QueryOperation, Read},
        replicated_data_type::{
            CausalCompaction, CausalReset, CausalStabilize, Precondition, Reconciled,
            Reconciliation, ReplicatedDataType, Sequential, SequentialEval, Stabilization, Stable,
        },
    },
    event::tagged_op::TaggedOp,
    state::unstable_state::IsUnstableCore,
};
#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use rustc_hash::FxHashSet;

/// Sequential operations carried by the commitment set example.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SetUpdate<V> {
    Add(V),
    Remove(V),
    Clear,
    /// Leave the set unchanged.
    NoOp,
}

/// Reconciles a log by sorting borrowed events with `P`.
///
/// `P` must provide a deterministic order compatible with causality. For example,
/// `Sorted<LwwPolicy>` orders by increasing timestamp, then replica ID.
pub struct Sorted<P>(PhantomData<fn() -> P>);

impl<O, U, P> Reconciliation<O, U> for Sorted<P>
where
    U: IsUnstableCore<O>,
    P: Policy,
{
    fn linearize<'a>(log: &'a U) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        let mut events: Vec<_> = log.iter().collect();
        events.sort_unstable_by(|a, b| {
            P::compare(a.tag(), b.tag())
                // Derived events can share a timestamp. Break ties independently
                // of delivery order and each replica's local index assignment.
                .then_with(|| a.id().origin_id().cmp(b.id().origin_id()))
                .then_with(|| a.id().seq().cmp(&b.id().seq()))
                .then_with(|| a.id().disambiguator().cmp(&b.id().disambiguator()))
        });
        events.into_iter()
    }
}

/// An update-consistent set
pub struct CommitSet<V>(PhantomData<V>);

impl<V: Debug + Clone + Eq + Hash> ReplicatedDataType for CommitSet<V> {
    type Update = SetUpdate<V>;
    type Stable = SequentialSet<V>;
    type Rejection = Infallible;
}

impl<V: Debug + Clone + Eq + Hash, U> Precondition<U> for CommitSet<V> {
    fn is_enabled(_: &SequentialSet<V>, _: &U, _: &SetUpdate<V>) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<V, U> CausalCompaction<U> for CommitSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
}

impl<V: Debug + Clone + Eq + Hash, U> CausalStabilize<U> for CommitSet<V>
where
    U: IsUnstableCore<SetUpdate<V>>,
{
    const DISABLE_STABILIZE: bool = false;

    fn causal_stabilize(
        frontier: &CausalFrontier,
        stable: &Stable<Self>,
        unstable: &U,
    ) -> Stabilization<Stable<Self>> {
        let mut new_stable = stable.clone();

        for tagged_op in Sorted::<LwwPolicy>::linearize(unstable)
            .filter(|tagged_op| frontier.contains(tagged_op.id()))
        {
            new_stable.apply(tagged_op.op());
        }

        Stabilization::Critical {
            frontier: frontier.clone(),
            stable: new_stable,
        }
    }
}

impl<V: Debug + Clone + Eq + Hash, U> CausalReset<U> for CommitSet<V> {}

impl<V, U, Q> Eval<SequentialSet<V>, U, Q> for CommitSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
    Q: QueryOperation,
    SequentialSet<V>: SequentialEval<Q, Update = SetUpdate<V>>,
{
    fn execute_query(stable: &SequentialSet<V>, unstable: &U, query: &Q) -> Q::Response {
        let state = Reconciled::<SequentialSet<V>, Sorted<LwwPolicy>>::fold(stable, unstable);
        SequentialSet::eval(&state, query)
    }
}

/// Ordinary sequential set semantics, independent of the replication order.
#[derive(Clone, Debug)]
pub struct SequentialSet<V>(FxHashSet<V>);

impl<V> Default for SequentialSet<V> {
    fn default() -> Self {
        Self(FxHashSet::default())
    }
}

impl<V: Debug + Clone + Eq + Hash> Sequential for SequentialSet<V> {
    type Update = SetUpdate<V>;

    fn apply(&mut self, update: &Self::Update) {
        match update {
            SetUpdate::Add(value) => {
                self.0.insert(value.clone());
            }
            SetUpdate::Remove(value) => {
                self.0.remove(value);
            }
            SetUpdate::Clear => self.0.clear(),
            SetUpdate::NoOp => {},
        }
    }

    fn is_default(&self) -> bool {
        self.0.is_empty()
    }
}

impl<V: Debug + Clone + Eq + Hash> SequentialEval<Read<FxHashSet<V>>> for SequentialSet<V> {
    fn eval(state: &Self, _: &Read<FxHashSet<V>>) -> FxHashSet<V> {
        state.0.clone()
    }
}

impl<V: Debug + Clone + Eq + Hash> SequentialEval<Contains<V>> for SequentialSet<V> {
    fn eval(state: &Self, query: &Contains<V>) -> bool {
        state.0.contains(&query.0)
    }
}

#[cfg(feature = "test_utils")]
impl<V> DeepSizeOf for SequentialSet<V>
where
    V: DeepSizeOf + Eq + Hash,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::query::{Contains, Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };

    use super::{CommitSet, SetUpdate};
    use crate::commitment::examples::tools::builders::{commit_replicas, lamport_commit_replicas};
    use rustc_hash::FxHashSet;

    #[test]
    fn commit_set_converges_with_commit_log() {
        let [mut a, mut b] = commit_replicas::<VecLog<CommitSet<&str>>>(2)
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        // Both updates have timestamp 1. B's removal wins, even though it is
        // initially ineffective and B receives A's insertion afterwards.
        let add = a.send(SetUpdate::Add("x")).unwrap();
        assert!(a.query(&Contains("x")));

        let remove = b.send(SetUpdate::Remove("x")).unwrap();

        a.receive(remove);
        b.receive(add);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::<&str>::default());
            assert!(!replica.query(&Contains("x")));
        }

        // Reverse the operations: B's concurrent insertion now wins.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        let add = b.send(SetUpdate::Add("x")).unwrap();
        a.receive(add);
        b.receive(remove);
        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::from_iter(["x"]));
        }

        // A later timestamp takes precedence over the replica-ID tie-break.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        b.receive(remove);
        assert!(!a.query(&Contains("x")));
        assert!(!b.query(&Contains("x")));
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn commit_set_converges_after_repeated_commitment() {
        use crate::{crdt::query::Read, replica::IsReplica, state::unstable_state::IsUnstableCore};

        let [a, b] = commit_replicas::<VecLog<CommitSet<u8>>>(2)
            .try_into()
            .unwrap_or_else(|_| unreachable!());
        let (mut a, mut b) = (a.into_inner(), b.into_inner());

        let mut stable_progress_rounds_a = 0;
        let mut stable_progress_rounds_b = 0;
        let mut last_stable_a = 0;
        let mut last_stable_b = 0;

        for round in 0..64 {
            let value = (round % 4) as u8;
            let next_value = ((round + 1) % 4) as u8;

            let (op_a, op_b) = match round % 4 {
                0 => (SetUpdate::Add(value), SetUpdate::Remove(value)),
                1 => (SetUpdate::Remove(value), SetUpdate::Add(value)),
                2 => (SetUpdate::Add(next_value), SetUpdate::Clear),
                _ => (SetUpdate::Clear, SetUpdate::Add(value)),
            };

            // Both operations are issued before either replica receives the other.
            let msg_a = a.send(op_a).unwrap();
            let msg_b = b.send(op_b).unwrap();

            a.receive(msg_b);
            b.receive(msg_a);

            assert_eq!(a.delivered_count(), b.delivered_count());

            let stable_a = a.delivered_count() - a.state().child().unstable().iter().count();
            let stable_b = b.delivered_count() - b.state().child().unstable().iter().count();

            if stable_a > last_stable_a {
                stable_progress_rounds_a += 1;
                last_stable_a = stable_a;
            }
            if stable_b > last_stable_b {
                stable_progress_rounds_b += 1;
                last_stable_b = stable_b;
            }
        }

        assert!(
            stable_progress_rounds_a >= 2,
            "replica a did not show stable-log progress in multiple rounds"
        );
        assert!(
            stable_progress_rounds_b >= 2,
            "replica b did not show stable-log progress in multiple rounds"
        );

        assert_eq!(a.query(&Read::new()), b.query(&Read::new()));
    }

    #[test]
    fn commit_set_converges_with_lamport_commit_log() {
        let [mut a, mut b] = lamport_commit_replicas::<VecLog<CommitSet<&str>>>(2)
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        // Both updates have timestamp 1. B's removal wins, even though it is
        // initially ineffective and B receives A's insertion afterwards.
        let add = a.send(SetUpdate::Add("x")).unwrap();
        assert!(a.query(&Contains("x")));

        let remove = b.send(SetUpdate::Remove("x")).unwrap();

        a.receive(remove);
        b.receive(add);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::<&str>::default());
            assert!(!replica.query(&Contains("x")));
        }

        // Reverse the operations: B's concurrent insertion now wins.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        let add = b.send(SetUpdate::Add("x")).unwrap();
        a.receive(add);
        b.receive(remove);
        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::from_iter(["x"]));
        }

        // A later timestamp takes precedence over the replica-ID tie-break.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        b.receive(remove);
        assert!(!a.query(&Contains("x")));
        assert!(!b.query(&Contains("x")));
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn commit_set_converges_after_repeated_lamport_commitment() {
        use crate::{crdt::query::Read, replica::IsReplica, state::unstable_state::IsUnstableCore};

        let [a, b] = lamport_commit_replicas::<VecLog<CommitSet<u8>>>(2)
            .try_into()
            .unwrap_or_else(|_| unreachable!());
        let (mut a, mut b) = (a.into_inner(), b.into_inner());

        let mut stable_progress_rounds_a = 0;
        let mut stable_progress_rounds_b = 0;
        let mut last_stable_a = 0;
        let mut last_stable_b = 0;

        for round in 0..64 {
            let value = (round % 4) as u8;
            let next_value = ((round + 1) % 4) as u8;

            let (op_a, op_b) = match round % 4 {
                0 => (SetUpdate::Add(value), SetUpdate::Remove(value)),
                1 => (SetUpdate::Remove(value), SetUpdate::Add(value)),
                2 => (SetUpdate::Add(next_value), SetUpdate::Clear),
                _ => (SetUpdate::Clear, SetUpdate::Add(value)),
            };

            // Both operations are issued before either replica receives the other.
            let msg_a = a.send(op_a).unwrap();
            let msg_b = b.send(op_b).unwrap();

            a.receive(msg_b);
            b.receive(msg_a);

            assert_eq!(a.delivered_count(), b.delivered_count());

            let stable_a = a.delivered_count() - a.state().child().unstable().iter().count();
            let stable_b = b.delivered_count() - b.state().child().unstable().iter().count();

            if stable_a > last_stable_a {
                stable_progress_rounds_a += 1;
                last_stable_a = stable_a;
            }
            if stable_b > last_stable_b {
                stable_progress_rounds_b += 1;
                last_stable_b = stable_b;
            }
        }

        assert!(
            stable_progress_rounds_a >= 2,
            "replica a did not show stable-log progress in multiple rounds"
        );
        assert!(
            stable_progress_rounds_b >= 2,
            "replica b did not show stable-log progress in multiple rounds"
        );

        assert_eq!(a.query(&Read::new()), b.query(&Read::new()));
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn lamport_commitment_waits_for_every_member() {
        let [a, b, c] = lamport_commit_replicas::<VecLog<CommitSet<u8>>>(3)
            .try_into()
            .unwrap_or_else(|_| unreachable!());
        let (mut a, mut b, mut c) = (a.into_inner(), b.into_inner(), c.into_inner());
        let add = a.send(SetUpdate::Add(1)).unwrap();
        b.receive(add.clone());
        let remove = b.send(SetUpdate::Remove(1)).unwrap();
        a.receive(remove.clone());
        assert!(a.state().committed_frontier().is_none());
        assert!(b.state().committed_frontier().is_none());
        // C's delayed concurrent insertion must precede B's removal.
        let concurrent = c.send(SetUpdate::Add(1)).unwrap();
        c.receive(add);
        c.receive(remove);
        a.receive(concurrent.clone());
        b.receive(concurrent);
        let advance = c.send(SetUpdate::Add(2)).unwrap();
        a.receive(advance.clone());
        b.receive(advance);
        assert!(!a.query(&Contains(1)));
        assert_eq!(a.query(&Read::new()), b.query(&Read::new()));
        assert_eq!(a.query(&Read::new()), c.query(&Read::new()));
        assert!(a.state().committed_frontier().is_some());
    }
    type EventLog<L> = crate::commitment::commit_log::CommitmentLog<
        L,
        crate::commitment::commit_protocols::omega::omega_commit::OmegaCommit<
            crate::commitment::commit_protocols::omega::oracle::EventOracle,
        >,
    >;

    fn event_commit_replicas<L: crate::state::log::IsLog>(
        n: usize,
    ) -> Vec<
        crate::replica::Replica<
            EventLog<L>,
            crate::broadcast::tcsb::Tcsb<
                crate::commitment::commit_protocols::omega::support_op::SupportOp<L::Update>,
            >,
        >,
    > {
        use crate::{
            commitment::{
                commit_protocols::omega::{omega_commit::OmegaCommit, oracle::EventOracle},
                wire_id::StableReplicaId,
            },
            replica::{Replica, ReplicaId},
        };
        let members: Vec<_> = (0..n)
            .map(|i| (StableReplicaId(i as u16), ReplicaId(format!("replica-{i}"))))
            .collect();
        let ids: Vec<_> = members.iter().map(|(_, id)| id.clone()).collect();
        (0..n)
            .map(|i| {
                let oracle =
                    EventOracle::from_parts(StableReplicaId(0), 60_000, Default::default(), None);
                let protocol =
                    OmegaCommit::from_parts(oracle, members.clone(), StableReplicaId(i as u16));
                Replica::bootstrap_with_state(
                    ids[i].clone(),
                    &ids,
                    EventLog::new(L::default(), protocol),
                )
            })
            .collect()
    }

    #[test]
    fn commit_set_converges_with_event_oracle() {
        let [mut a, mut b] = event_commit_replicas::<VecLog<CommitSet<&str>>>(2)
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        // Both updates have timestamp 1. B's removal wins, even though it is
        // initially ineffective and B receives A's insertion afterwards.
        let add = a.send(SetUpdate::Add("x")).unwrap();
        assert!(a.query(&Contains("x")));

        let remove = b.send(SetUpdate::Remove("x")).unwrap();

        a.receive(remove);
        b.receive(add);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::<&str>::default());
            assert!(!replica.query(&Contains("x")));
        }

        // Reverse the operations: B's concurrent insertion now wins.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        let add = b.send(SetUpdate::Add("x")).unwrap();
        a.receive(add);
        b.receive(remove);
        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::from_iter(["x"]));
        }

        // A later timestamp takes precedence over the replica-ID tie-break.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        b.receive(remove);
        assert!(!a.query(&Contains("x")));
        assert!(!b.query(&Contains("x")));
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn commit_set_event_oracle_makes_commitment_progress() {
        use crate::{crdt::query::Read, replica::IsReplica, state::unstable_state::IsUnstableCore};

        let [a, b] = event_commit_replicas::<VecLog<CommitSet<u8>>>(2)
            .try_into()
            .unwrap_or_else(|_| unreachable!());
        let (mut a, mut b) = (a, b);

        let mut stable_progress_rounds_a = 0;
        let mut stable_progress_rounds_b = 0;
        let mut last_stable_a = 0;
        let mut last_stable_b = 0;

        for round in 0..64 {
            let value = (round % 4) as u8;
            let next_value = ((round + 1) % 4) as u8;

            let (op_a, op_b) = match round % 4 {
                0 => (SetUpdate::Add(value), SetUpdate::Remove(value)),
                1 => (SetUpdate::Remove(value), SetUpdate::Add(value)),
                2 => (SetUpdate::Add(next_value), SetUpdate::Clear),
                _ => (SetUpdate::Clear, SetUpdate::Add(value)),
            };

            // Both operations are issued before either replica receives the other.
            let msg_a = a.send(op_a).unwrap();
            let msg_b = b.send(op_b).unwrap();

            a.receive(msg_b);
            b.receive(msg_a);

            assert_eq!(a.delivered_count(), b.delivered_count());

            let stable_a = a.delivered_count() - a.state().child().unstable().iter().count();
            let stable_b = b.delivered_count() - b.state().child().unstable().iter().count();

            if stable_a > last_stable_a {
                stable_progress_rounds_a += 1;
                last_stable_a = stable_a;
            }
            if stable_b > last_stable_b {
                stable_progress_rounds_b += 1;
                last_stable_b = stable_b;
            }
        }

        assert!(
            stable_progress_rounds_a >= 2,
            "replica a did not show stable-log progress in multiple rounds"
        );
        assert!(
            stable_progress_rounds_b >= 2,
            "replica b did not show stable-log progress in multiple rounds"
        );

        assert_eq!(a.query(&Read::new()), b.query(&Read::new()));
    }
}
