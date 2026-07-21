pub mod commit_log;
pub mod commit_op;
pub mod mixed_consistency_replica;
pub mod oracle;

// use std::{
//     collections::{BTreeMap, BTreeSet},
//     fmt::Debug,
//     marker::PhantomData,
// };

// #[cfg(feature = "test_utils")]
// use crate::broadcast::tcsb::IsTcsbTest;
// use crate::{
//     broadcast::{
//         internalizer::{InternalizeOp, Interner},
//         message::{BatchMessage, EventMessage, SinceMessage},
//         tcsb::{IsTcsb, Tcsb},
//     },
//     event::{id::EventId, tagged_op::TaggedOp},
//     replica::{Replica, ReplicaId, ReplicaIdOwned, ReplicaIdx},
//     state::{
//         commit_log::CommitLog,
//         log::IsLog,
//         unstable_state::{IsUnstableCore, event_graph::EventGraph},
//     },
// };

// pub type CommitPosition = EventId;
// pub type CommitEntry<U> = TaggedOp<CommitOp<U>>;
// pub type CommitTcsb<U, O> = OmegaTcsb<U, Tcsb<CommitOp<U>>, O>;
// pub type CommittedReplica<A, C, O> =
//     Replica<CommitLog<A, C>, CommitTcsb<<A as SequentialADT>::Update, O>>;

// /// Commitment metadata update performed when a new event is delivered.
// pub trait CommitmentProtocol<U>: Debug {
//     /// Record `delivered` and return commit positions that became newly pre-committed.
//     fn on_deliver(
//         &mut self,
//         delivered: &EventId,
//         log: &EventGraph<CommitOp<U>>,
//         n_members: usize,
//     ) -> Vec<EventId>
//     where
//         U: Clone + Debug;

//     fn latest_committed(&self) -> Option<&EventId>;
// }

// /// No-op commitment protocol, useful while wiring a sequential log without
// /// enabling the Omega majority algorithm.
// #[derive(Clone, Debug, Default)]
// pub struct NoCommitment;

// impl<U> CommitmentProtocol<U> for NoCommitment {
//     fn on_deliver(
//         &mut self,
//         _delivered: &EventId,
//         _log: &EventGraph<CommitOp<U>>,
//         _n_members: usize,
//     ) -> Vec<EventId>
//     where
//         U: Clone + Debug,
//     {
//         Vec::new()
//     }

//     fn latest_committed(&self) -> Option<&EventId> {
//         None
//     }
// }

// /// Majority/Omega commitment protocol.
// ///
// /// The structure mirrors the ad-hoc prototype:
// /// - a potential leader keeps one boolean vote per replica that may validate it;
// /// - `partial_leaders_hist` records the partial leaders visible when the vertex
// ///   was detected;
// /// - `pre_committed` is closed under that history relation.
// #[derive(Clone, Debug, Default)]
// pub struct MajorityOmegaCommitment {
//     potential_leaders: BTreeMap<EventId, BTreeMap<ReplicaIdOwned, bool>>,
//     partial_leaders_hist: BTreeMap<EventId, BTreeSet<EventId>>,
//     pre_committed: BTreeSet<EventId>,
//     latest_committed: Option<EventId>,
// }

// impl MajorityOmegaCommitment {
//     pub fn potential_leaders(&self) -> &BTreeMap<EventId, BTreeMap<ReplicaIdOwned, bool>> {
//         &self.potential_leaders
//     }

//     pub fn pre_committed(&self) -> &BTreeSet<EventId> {
//         &self.pre_committed
//     }

//     fn quorum_size(n_members: usize) -> usize {
//         (n_members / 2) + 1
//     }

//     fn partial_leaders(
//         &self,
//         quorum_size: usize,
//     ) -> impl Iterator<Item = (&EventId, &BTreeMap<ReplicaIdOwned, bool>)> {
//         self.potential_leaders
//             .iter()
//             .filter(move |(_, votes)| votes.len() >= quorum_size)
//     }

//     fn partial_leader_ids(&self, quorum_size: usize) -> BTreeSet<EventId> {
//         self.partial_leaders(quorum_size)
//             .map(|(event_id, _)| event_id.clone())
//             .collect()
//     }

//     fn is_potential_leader<U>(
//         &self,
//         position: &EventId,
//         log: &EventGraph<CommitOp<U>>,
//         quorum_size: usize,
//     ) -> Option<BTreeSet<ReplicaIdOwned>>
//     where
//         U: Clone + Debug,
//     {
//         let candidate = position.origin_id();
//         let votes = self.votes(candidate, log.ancestors(position), log);

//         (votes.len() >= quorum_size).then_some(votes)
//     }

//     fn votes<U>(
//         &self,
//         candidate: &ReplicaId,
//         positions: Vec<EventId>,
//         log: &EventGraph<CommitOp<U>>,
//     ) -> BTreeSet<ReplicaIdOwned>
//     where
//         U: Clone + Debug,
//     {
//         let mut replica_vote = BTreeMap::<ReplicaIdOwned, bool>::new();

//         for position in positions {
//             let Some(entry) = log.get(&position) else {
//                 continue;
//             };

//             let sender = entry.id().origin_id().to_string();
//             let vote = entry.op().leader == candidate;
//             replica_vote
//                 .entry(sender)
//                 .and_modify(|current| *current = *current && vote)
//                 .or_insert(vote);
//         }

//         replica_vote
//             .into_iter()
//             .filter_map(|(replica, voted)| voted.then_some(replica))
//             .collect()
//     }

//     fn update_leaders<U>(&mut self, delivered: &EventId, log: &EventGraph<CommitOp<U>>)
//     where
//         U: Clone + Debug,
//     {
//         let Some(entry) = log.get(delivered) else {
//             return;
//         };
//         let sender = delivered.origin_id().to_string();
//         let leader = entry.op().leader.clone();

//         for (candidate, votes) in &mut self.potential_leaders {
//             if *votes.get(&sender).unwrap_or(&true) {
//                 continue;
//             }

//             if leader != candidate.origin_id() {
//                 votes.remove(&sender);
//             } else if log.happens_before(candidate, delivered)
//                 && let Some(vote) = votes.get_mut(&sender)
//             {
//                 *vote = true;
//             }
//         }
//     }

//     fn check_pre_committed(&mut self, quorum_size: usize) {
//         let leaders: BTreeSet<EventId> = self
//             .partial_leaders(quorum_size)
//             .filter(|(_, votes)| votes.values().filter(|voted| **voted).count() >= quorum_size)
//             .map(|(event_id, _)| event_id.clone())
//             .collect();

//         self.pre_committed.extend(leaders);

//         let mut closure = self.pre_committed.clone();
//         loop {
//             let before_len = closure.len();
//             for pre_committed in closure.clone() {
//                 if let Some(history) = self.partial_leaders_hist.get(&pre_committed) {
//                     closure.extend(history.iter().cloned());
//                 }
//             }

//             if closure.len() == before_len {
//                 break;
//             }
//         }

//         self.pre_committed = closure;
//     }
// }

// impl<U> CommitmentProtocol<U> for MajorityOmegaCommitment {
//     fn on_deliver(
//         &mut self,
//         delivered: &EventId,
//         log: &EventGraph<CommitOp<U>>,
//         n_members: usize,
//     ) -> Vec<EventId>
//     where
//         U: Clone + Debug,
//     {
//         let quorum_size = Self::quorum_size(n_members);

//         if let Some(votes) = self.is_potential_leader(delivered, log, quorum_size) {
//             let partial_leaders = self.partial_leader_ids(quorum_size);
//             self.partial_leaders_hist
//                 .insert(delivered.clone(), partial_leaders);
//             self.potential_leaders.insert(
//                 delivered.clone(),
//                 votes.into_iter().map(|replica| (replica, false)).collect(),
//             );
//         }

//         let previous_pre_committed = self.pre_committed.clone();
//         self.update_leaders(delivered, log);
//         self.check_pre_committed(quorum_size);

//         let mut new_commits: Vec<EventId> = self
//             .pre_committed
//             .difference(&previous_pre_committed)
//             .cloned()
//             .collect();
//         new_commits.sort();

//         if let Some(latest) = new_commits.last().cloned() {
//             self.latest_committed = Some(latest);
//         }

//         new_commits
//     }

//     fn latest_committed(&self) -> Option<&EventId> {
//         self.latest_committed.as_ref()
//     }
// }
