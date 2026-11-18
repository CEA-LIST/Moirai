use std::convert::Infallible;

use crate::{
    broadcast::internalizer::{Interner, Resolver},
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{commit_log::CommitmentLog, wire_id::StableReplicaId},
    crdt::{
        eval::EvalNested,
        query::QueryOperation,
        replicated_data_type::{
            CausalCompaction, CausalReset, CausalStabilize, Precondition, ReplicatedDataType,
            Sequential, SequentialEval,
        },
    },
    event::{Event, id::EventId, lamport::Lamport, tagged_op::TaggedOp},
    replica::{ReplicaId, ReplicaIdx},
    state::{effect_context::EffectContext, log::IsLog},
};

use crate::commitment::{
    commit_protocols::omega::{
        omega_commit::OmegaCommit, oracle::Omega, support_op::SupportOp,
        versioned_vote::VersionedVote, vote_log::VoteLog,
    },
    examples::{builders::CommitReplica, commit_set::Sorted},
};
use crate::{
    crdt::{
        eval::Eval,
        policy::LwwPolicy,
        replicated_data_type::{Reconciliation, Stabilization},
    },
    state::po_log::VecLog,
};

fn resolver(names: &[&str]) -> Resolver {
    let mut interner = Interner::new();
    for name in names {
        interner.intern(&ReplicaId::from(*name));
    }
    interner.resolver().clone()
}

fn version(r: &Resolver, origin: usize, entries: &[usize]) -> Version {
    let mut v = Version::new(ReplicaIdx(origin), r.clone());
    for (i, seq) in entries.iter().enumerate() {
        v.set_by_idx(ReplicaIdx(i), *seq);
    }
    v
}

fn vote(
    r: &Resolver,
    origin: usize,
    entries: &[usize],
    leader: usize,
    partial: bool,
) -> VersionedVote {
    VersionedVote::new(
        ReplicaIdx(leader),
        version(r, origin, entries),
        partial,
        None,
    )
}

fn members(n: usize) -> Vec<ReplicaIdx> {
    (0..n).map(ReplicaIdx).collect()
}

#[test]
fn disagreeing_successor_cannot_close_support() {
    let r = resolver(&["A", "B", "C"]);
    let mut log = VoteLog::default();
    for v in [
        vote(&r, 0, &[1, 0, 0], 0, false),
        vote(&r, 1, &[0, 1, 0], 0, false),
    ] {
        log.insert(v);
    }
    let candidate = vote(&r, 0, &[2, 1, 0], 0, true);
    log.insert(candidate.clone());
    log.insert(vote(&r, 0, &[3, 1, 0], 0, false));
    log.insert(vote(&r, 1, &[2, 2, 0], 1, false));
    assert!(!log.get_leaders(&members(3)).contains(&candidate));
    assert!(!log.partial_leaders[&candidate].contains_key(&ReplicaIdx(1)));
}

#[test]
fn first_vertex_has_no_own_predecessor_support() {
    let r = resolver(&["A", "B", "C"]);
    let mut log = VoteLog::default();
    log.insert(vote(&r, 1, &[0, 1, 0], 0, false));
    log.insert(vote(&r, 2, &[0, 0, 1], 0, false));
    let candidate = vote(&r, 0, &[1, 1, 1], 0, true);
    log.insert(candidate.clone());
    log.insert(vote(&r, 0, &[2, 1, 1], 0, false));
    log.insert(vote(&r, 1, &[1, 2, 1], 0, false));
    assert!(!log.get_leaders(&members(3)).contains(&candidate));
    assert!(!log.partial_leaders[&candidate].contains_key(&ReplicaIdx(0)));
}

#[test]
fn closed_support_is_immutable() {
    let r = resolver(&["A", "B"]);
    let mut log = VoteLog::default();
    log.insert(vote(&r, 0, &[1, 0], 0, false));
    log.insert(vote(&r, 1, &[0, 1], 0, false));
    let candidate = vote(&r, 0, &[2, 1], 0, true);
    log.insert(candidate.clone());
    log.insert(vote(&r, 1, &[2, 2], 0, false));
    log.insert(vote(&r, 1, &[2, 3], 1, false));
    assert_eq!(
        log.partial_leaders[&candidate].get(&ReplicaIdx(1)),
        Some(&true)
    );
}

#[test]
fn delayed_concurrent_candidate_does_not_use_pruned_votes() {
    let r = resolver(&["A", "B", "C"]);
    let mut log = VoteLog::default();
    for v in [
        vote(&r, 0, &[1, 0, 0], 0, false),
        vote(&r, 1, &[0, 1, 0], 2, false),
        vote(&r, 2, &[0, 0, 1], 2, false),
        vote(&r, 1, &[0, 2, 0], 2, false),
        vote(&r, 0, &[2, 2, 1], 0, false),
        vote(&r, 0, &[3, 2, 1], 0, false),
        vote(&r, 0, &[4, 2, 1], 0, false),
        vote(&r, 1, &[4, 3, 1], 0, false),
    ] {
        log.insert(v);
    }
    let checkpoint = vote(&r, 0, &[5, 3, 1], 0, true);
    log.insert(checkpoint.clone());
    log.insert(vote(&r, 0, &[6, 3, 1], 0, false));
    log.insert(vote(&r, 1, &[5, 4, 1], 0, false));
    assert!(log.get_leaders(&members(3)).contains(&checkpoint));
    let metadata = log.get_causal_frontier(checkpoint.version());
    log.set_committed(checkpoint.version());
    log.prune(&metadata);
    assert!(!log.causal_votes[&ReplicaIdx(1)].contains_key(&2));
    let delayed = vote(&r, 2, &[2, 2, 2], 2, true);
    log.insert(delayed.clone());
    assert!(!log.partial_leaders.contains_key(&delayed));
    assert_eq!(
        log.causal_votes[&ReplicaIdx(2)].get(&2),
        Some(&ReplicaIdx(2))
    );
    assert!(log.ready_to_commit(&members(3)).is_empty());
}

#[test]
fn predecessor_is_deliberately_selected_before_the_outgoing_vote() {
    let r = resolver(&["A", "B", "C"]);
    let mut log = VoteLog::default();
    for v in [
        vote(&r, 0, &[1, 0, 0], 0, false),
        vote(&r, 1, &[0, 1, 0], 0, false),
        vote(&r, 0, &[2, 1, 0], 0, true),
        vote(&r, 0, &[3, 1, 0], 0, true),
        vote(&r, 0, &[4, 1, 0], 1, true),
        vote(&r, 2, &[0, 0, 1], 1, false),
    ] {
        log.insert(v);
    }
    let (partial, previous) = log.is_potential(ReplicaIdx(1), &members(3));
    assert!(partial);
    assert_eq!(
        previous.as_ref().map(|p| (p.id(), p.seq())),
        Some((ReplicaIdx(0), 2))
    );
    let b2 = VersionedVote::new(ReplicaIdx(1), version(&r, 1, &[4, 2, 1]), partial, previous);
    log.insert(b2.clone());
    log.insert(vote(&r, 0, &[5, 2, 1], 1, false));
    log.insert(vote(&r, 2, &[4, 2, 2], 1, false));
    let checkpoints: Vec<_> = log
        .ready_to_commit(&members(3))
        .into_iter()
        .map(|v| (v.origin_idx(), v.origin_seq()))
        .collect();
    assert_eq!(checkpoints, vec![(ReplicaIdx(0), 2), (ReplicaIdx(1), 2)]);
}

#[test]
fn retention_boundary_saturates_and_keeps_the_checkpoint() {
    let r = resolver(&["A", "B", "C"]);
    let v = vote(&r, 0, &[1, 0, 0], 0, true);
    let f = VoteLog::default().get_causal_frontier(v.version());
    assert_eq!(
        f.iter().map(|(_, seq)| seq).collect::<Vec<_>>(),
        vec![0, 0, 0]
    );
}

#[test]
fn ordering_and_equality_use_stable_event_identity() {
    let ab = resolver(&["A", "B"]);
    let ba = resolver(&["B", "A"]);
    let a = vote(&ab, 0, &[2, 1], 0, true);
    let same_a = vote(&ba, 1, &[1, 2], 1, false);
    assert_eq!(a.cmp(&same_a), std::cmp::Ordering::Equal);
    assert_eq!(a, same_a);
    let later = vote(&ba, 0, &[2, 2], 1, true);
    assert!(a < later);
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
struct Sequence(Vec<u64>);
impl Sequential for Sequence {
    type Update = u64;
    fn apply(&mut self, op: &u64) {
        self.0.push(*op);
    }
    fn is_default(&self) -> bool {
        self.0.is_empty()
    }
}
struct ReadSequence;
impl QueryOperation for ReadSequence {
    type Response = Vec<u64>;
}
impl SequentialEval<ReadSequence> for Sequence {
    fn eval(state: &Self, _: &ReadSequence) -> Vec<u64> {
        state.0.clone()
    }
}
struct Object;
type Unstable = Vec<TaggedOp<u64>>;
type Log = CommitmentLog<VecLog<Object>, OmegaCommit<Omega>>;
impl ReplicatedDataType for Object {
    type Update = u64;
    type Stable = Sequence;
    type Rejection = Infallible;
}
impl Precondition<Unstable> for Object {
    fn is_enabled(_: &Sequence, _: &Unstable, _: &u64) -> Result<(), Infallible> {
        Ok(())
    }
}
impl CausalCompaction<Unstable> for Object {}
impl CausalReset<Unstable> for Object {}
impl CausalStabilize<Unstable> for Object {
    fn causal_stabilize(
        frontier: &CausalFrontier,
        stable: &Sequence,
        unstable: &Unstable,
    ) -> Stabilization<Sequence> {
        let mut stable = stable.clone();
        for event in
            Sorted::<LwwPolicy>::linearize(unstable).filter(|event| frontier.contains(event.id()))
        {
            stable.apply(event.op());
        }
        Stabilization::Critical {
            frontier: frontier.clone(),
            stable,
        }
    }
}
impl Eval<Sequence, Unstable, ReadSequence> for Object {
    fn execute_query(stable: &Sequence, unstable: &Unstable, _: &ReadSequence) -> Vec<u64> {
        let mut result = stable.clone();
        for event in Sorted::<LwwPolicy>::linearize(unstable) {
            result.apply(event.op());
        }
        result.0
    }
}

fn configured(own: &str, ids: &[&str]) -> Log {
    let system_members: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (StableReplicaId(i as u16), ReplicaId::from(*id)))
        .collect();
    let own_id = StableReplicaId(
        ids.iter()
            .position(|id| *id == own)
            .expect("own ID missing") as u16,
    );
    Log::new(
        VecLog::<Object>::default(),
        OmegaCommit::from_parts(
            Omega::with_leader(StableReplicaId(0)),
            system_members,
            own_id,
        ),
    )
}

#[test]
fn causal_stability_does_not_commit_and_queries_sort_the_tail() {
    let r = resolver(&["A", "B"]);
    let mut log = configured("A", &["A", "B"]);
    for (origin, clock, op) in [(1, vec![0, 1], 20), (0, vec![1, 0], 10)] {
        let v = version(&r, origin, &clock);
        let payload = SupportOp::new(op, StableReplicaId(0), false, None);
        let event = Event::new(EventId::from(&v), Lamport::from(&v), payload, v);
        log.effect(event, &mut EffectContext::silent());
    }
    log.stabilize(&CausalFrontier::from(version(&r, 0, &[1, 1])));
    assert!(log.child().stable().0.is_empty());
    assert_eq!(log.execute_query(&ReadSequence), vec![10, 20]);
}

#[test]
#[should_panic(expected = "from_stable/from_parts")]
fn unconfigured_log_cannot_prepare_votes() {
    Log::default().prepare(1);
}

#[test]
#[should_panic(expected = "Duplicate replica ID")]
fn duplicate_members_are_rejected() {
    CommitReplica::<VecLog<Object>>::new(
        StableReplicaId(0),
        vec![
            (StableReplicaId(0), ReplicaId::from("A")),
            (StableReplicaId(1), ReplicaId::from("A")),
        ],
        StableReplicaId(0),
    );
}

#[cfg(feature = "test_utils")]
#[test]
fn footprint_includes_retained_vote_metadata() {
    use deepsize::DeepSizeOf;
    let r = resolver(&["A", "B", "C"]);
    let mut log = configured("A", &["A", "B", "C"]);
    let before = log.protocol().deep_size_of();
    let v = version(&r, 0, &[1, 0, 0]);
    let event = Event::new(
        EventId::from(&v),
        Lamport::from(&v),
        SupportOp::new(1, StableReplicaId(0), false, None),
        v,
    );
    log.effect(event, &mut EffectContext::silent());
    assert!(log.protocol().deep_size_of() > before);
    assert!(
        !log.is_default(),
        "Retained protocol state is not an empty log"
    );
}

#[cfg(feature = "test_utils")]
#[test]
fn tcsb_translation_commitment_pruning_and_crash_progress() {
    use crate::{
        broadcast::tcsb::Tcsb,
        replica::{IsReplica, Replica},
    };
    type Node = Replica<Log, Tcsb<SupportOp<u64>>>;
    let ids = ["A", "B", "C"];
    let system_members: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (StableReplicaId(i as u16), ReplicaId::from(*id)))
        .collect();
    let mut nodes: Vec<Node> = (0..ids.len())
        .map(|i| {
            CommitReplica::<VecLog<Object>>::new(
                StableReplicaId(i as u16),
                system_members.clone(),
                StableReplicaId(0),
            )
            .into_inner()
        })
        .collect();
    let mut counter = 0;
    // The last operation from C must commit after C stops issuing vertices.
    counter += 1;
    let final_c = nodes[2].send(counter).unwrap();
    for node in nodes.iter_mut().take(2) {
        node.receive(final_c.clone());
    }
    for round in 0..10 {
        for origin in 0..2 {
            counter += 1;
            let message = nodes[origin].send(counter).unwrap();
            assert_eq!(message.event().op().vote, StableReplicaId(0));
            for receiver in 0..2 {
                if receiver != origin {
                    nodes[receiver].receive(message.clone());
                }
            }
            let a = &nodes[0].state().child().stable().0;
            let b = &nodes[1].state().child().stable().0;
            assert!(a.starts_with(b) || b.starts_with(a), "round {round}");
            assert!(nodes[0].query(&ReadSequence).starts_with(a));
            assert!(nodes[1].query(&ReadSequence).starts_with(b));
        }
    }
    for node in &nodes[..2] {
        assert!(node.state().child().stable().0.contains(&1));
        let unique: std::collections::BTreeSet<_> =
            node.state().child().stable().0.iter().collect();
        assert_eq!(
            unique.len(),
            node.state().child().stable().0.len(),
            "An operation committed twice"
        );
        assert!(
            !node
                .state()
                .child()
                .unstable()
                .iter()
                .any(|event| event.id() == final_c.event().id()),
            "C's final operation must leave the unstable log after commitment"
        );
        assert!(node.state().child().unstable().len() < counter as usize);
    }
    assert_eq!(nodes[0].query(&ReadSequence), nodes[1].query(&ReadSequence));
    // B's own index is zero, but the received A vote must resolve to A, not B.
    let b_version = nodes[1].tcsb().matrix_clock().origin_version();
    assert_eq!(
        b_version.resolver().get(&ReplicaId::from("B")),
        Some(ReplicaIdx(0))
    );
    assert_eq!(
        b_version.resolver().get(&ReplicaId::from("A")),
        Some(ReplicaIdx(1))
    );
    let prepared = nodes[1].prepare(counter + 1);
    assert_eq!(prepared.vote, StableReplicaId(0));
    assert!(!prepared.is_partial, "B has no majority voting for it");
    assert!(prepared.last_partial.is_none());
    let prepared = nodes[0].prepare(counter + 1);
    assert!(prepared.is_partial, "A has majority support");
    assert_eq!(
        prepared
            .last_partial
            .expect("A must have a supported checkpoint")
            .origin,
        StableReplicaId(0)
    );
}

#[cfg(feature = "serde")]
#[test]
fn payload_round_trip_preserves_global_vote_and_predecessor_ids() {
    use crate::commitment::wire_id::StableEventId;

    let payload = SupportOp::new(
        7u64,
        StableReplicaId(7),
        true,
        Some(StableEventId::new(StableReplicaId(42), 3)),
    );
    let bytes = serde_json::to_vec(&payload).unwrap();
    let decoded: SupportOp<u64> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.vote, StableReplicaId(7));
    assert_eq!(decoded.last_partial, payload.last_partial);
    assert_eq!(
        decoded.last_partial,
        Some(StableEventId::new(StableReplicaId(42), 3))
    );
    // The serialized identities are independent of each receiver's local index assignment.
}
