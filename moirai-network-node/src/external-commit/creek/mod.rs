//! Creek-inspired conditional atomic broadcast of causally delivered vertex IDs.
//! See README.md for the protocol contract, crash model, and integration example.
mod paxos;
mod wire;
mod worker;

pub use worker::{Worker, WorkerConfig};

pub use moirai_protocol::commitment::commit_protocols::omega::oracle::IsOracle as IsOmega;

use crate::{Node, Transport};
use deepsize::DeepSizeOf;
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        commit_log::CommitmentLog, protocol::AtomicBroadcastProtocol, wire_id::StableReplicaId,
    },
    event::Event,
    replica::{ReplicaId, ReplicaIdx},
    state::{effect_context::EffectContext, log::IsLog},
};
use paxos::Snapshot;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    sync::{Arc, Mutex},
};

/// The complete consensus value: a stable origin and its root-event sequence.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DeepSizeOf,
)]
pub struct VertexId {
    pub origin: u16,
    pub seq: u64,
}

#[derive(Debug)]
struct Checkpoint {
    slot: u64,
    version: Version,
}

impl DeepSizeOf for Checkpoint {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.version.deep_size_of_children(context)
    }
}

/// A short handoff only: Paxos itself and its retained journal are worker-owned.
#[derive(Debug, Default)]
struct Shared {
    snapshot: Snapshot,
    learned: VecDeque<(u64, VertexId)>,
    started: bool,
    closed: bool,
    error: Option<String>,
}

/// The owner-thread commitment adapter. Paxos runs on an independent worker;
/// clocks, operation payloads, and the oracle never leave the replica thread.
#[derive(Debug)]
pub struct Creek<O: IsOmega> {
    oracle: O,
    members: Vec<(StableReplicaId, ReplicaId)>,
    indices: BTreeMap<u16, ReplicaIdx>,
    versions: BTreeMap<VertexId, Version>,
    heads: Vec<VertexId>,
    delivered: BTreeMap<u16, u64>,
    decisions: BTreeMap<u64, VertexId>,
    ready: VecDeque<Checkpoint>,
    applied_slot: u64,
    committed: Option<CausalFrontier>,
    shared: Arc<Mutex<Shared>>,
}

impl<O: IsOmega> Default for Creek<O> {
    fn default() -> Self {
        Self {
            oracle: O::default(),
            members: Vec::new(),
            indices: BTreeMap::new(),
            versions: BTreeMap::new(),
            heads: Vec::new(),
            delivered: BTreeMap::new(),
            decisions: BTreeMap::new(),
            ready: VecDeque::new(),
            applied_slot: 0,
            committed: None,
            shared: Arc::new(Mutex::new(Shared::default())),
        }
    }
}

impl<O: IsOmega> Creek<O> {
    /// Use the same membership order as Replica::bootstrap_with_state. Local
    /// indices are fixed at construction, with this replica at index zero.
    pub fn new(
        mut oracle: O,
        mut members: Vec<(StableReplicaId, ReplicaId)>,
        own: StableReplicaId,
    ) -> io::Result<Self> {
        let invalid = |reason| io::Error::new(io::ErrorKind::InvalidInput, reason);
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for (id, name) in &members {
            if !ids.insert(*id) || !names.insert(name) {
                return Err(invalid("duplicate member identity"));
            }
        }
        let own_position = members
            .iter()
            .position(|(id, _)| *id == own)
            .ok_or_else(|| invalid("own ID is not a member"))?;
        let own_member = members.remove(own_position);
        members.insert(0, own_member);
        oracle.configure_members(&members.iter().map(|(id, _)| *id).collect::<Vec<_>>());
        if !ids.contains(&oracle.query()) {
            return Err(invalid("Omega selected an unknown member"));
        }
        let indices = members
            .iter()
            .enumerate()
            .map(|(idx, (id, _))| (id.0, ReplicaIdx(idx)))
            .collect();
        let mut protocol = Self::default();
        protocol.members = members;
        protocol.indices = indices;
        protocol.oracle = oracle;
        protocol.publish()?;
        Ok(protocol)
    }

    pub fn oracle(&self) -> &O {
        &self.oracle
    }
    pub fn oracle_mut(&mut self) -> &mut O {
        &mut self.oracle
    }
    pub fn applied_slot(&self) -> u64 {
        self.applied_slot
    }

    fn covered(&self, cut: &CausalFrontier, vertex: VertexId) -> bool {
        cut.seq_by_idx(self.indices[&vertex.origin]) as u64 >= vertex.seq
    }

    fn candidate(&self) -> Option<VertexId> {
        self.heads
            .iter()
            .copied()
            .min_by_key(|id| (self.versions[id].sum(), *id))
    }

    fn publish(&mut self) -> io::Result<()> {
        let leader = self.oracle.query().0;
        if !self.indices.contains_key(&leader) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Omega selected an unknown member",
            ));
        }
        // Build the input outside the lock. The applied cursor and candidate
        // travel in one coherent snapshot to prevent stale next-slot proposals.
        let snapshot = Snapshot {
            leader,
            applied: self.applied_slot,
            candidate: self.candidate(),
            delivered: self.delivered.clone(),
        };
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| io::Error::other("Creek handoff poisoned"))?;
        shared.snapshot = snapshot;
        Ok(())
    }

    /// Drain chosen IDs, prepare causally available cuts, and publish fresh input.
    /// The caller then invokes CommitmentLog::commit_ready() on the owner thread.
    pub fn poll(&mut self) -> io::Result<()> {
        let learned = {
            let mut shared = self
                .shared
                .lock()
                .map_err(|_| io::Error::other("Creek handoff poisoned"))?;
            if let Some(error) = &shared.error {
                return Err(io::Error::other(error.clone()));
            }
            std::mem::take(&mut shared.learned)
        };
        for (slot, id) in learned {
            if slot <= self.applied_slot {
                continue;
            }
            if let Some(previous) = self.decisions.insert(slot, id)
                && previous != id
            {
                return Err(io::Error::other("conflicting Paxos decisions"));
            }
        }
        let mut next = self.applied_slot + self.ready.len() as u64 + 1;
        let mut cut = self
            .ready
            .back()
            .map(|c| CausalFrontier::from(&c.version))
            .or_else(|| self.committed.clone());
        while let Some(&id) = self.decisions.get(&next) {
            if let Some(version) = self.versions.get(&id) {
                if let Some(cut) = &mut cut {
                    cut.join_version(version);
                } else {
                    cut = Some(CausalFrontier::from(version));
                }
            } else if !cut.as_ref().is_some_and(|cut| self.covered(cut, id)) {
                // A chosen ID can arrive before the normal TCSB data. Do not
                // apply later decisions out of order or fetch payloads via Paxos.
                break;
            }
            let version = Version::from((cut.as_ref().unwrap().clone(), self.indices[&id.origin]));
            self.ready.push_back(Checkpoint {
                slot: next,
                version,
            });
            next = next
                .checked_add(1)
                .ok_or_else(|| io::Error::other("Paxos slot exhausted"))?;
        }
        self.publish()
    }
}

impl<L: IsLog, O: IsOmega> AtomicBroadcastProtocol<L> for Creek<O> {
    type Update = L::Update;
    fn ab_prepare(&self, op: L::Update) -> L::Update {
        assert!(
            !self.members.is_empty(),
            "Configure Creek before preparing operations"
        );
        op
    }
    fn child_op(op: &L::Update) -> &L::Update {
        op
    }
    fn is_default(&self) -> bool {
        self.versions.is_empty() && self.committed.is_none()
    }

    fn ab_handler(&mut self, event: Event<L::Update>, _: &mut EffectContext<'_>) {
        assert_eq!(
            event.version().iter().count(),
            self.members.len(),
            "Fixed membership changed"
        );
        let event = self.oracle.update(event);
        let version = event.version();
        let origin = self.members[version.origin_idx().0].0.0;
        let id = VertexId {
            origin,
            seq: u64::try_from(version.origin_seq()).expect("sequence exceeds u64"),
        };
        assert!(id.seq > 0, "Creek only accepts actual TCSB events");
        self.heads
            .retain(|head| (version.seq_by_idx(self.indices[&head.origin]) as u64) < head.seq);
        self.heads.push(id);
        self.versions.insert(id, version.clone());
        self.delivered
            .entry(origin)
            .and_modify(|seq| *seq = (*seq).max(id.seq))
            .or_insert(id.seq);
    }

    fn ab_ready_to_commit(&self) -> Vec<&Version> {
        self.ready
            .iter()
            .map(|checkpoint| &checkpoint.version)
            .collect()
    }
    fn ab_prune(&mut self, version: &Version) {
        self.versions
            .retain(|id, _| (version.seq_by_idx(self.indices[&id.origin]) as u64) < id.seq);
        self.heads.retain(|id| self.versions.contains_key(id));
    }
    fn ab_last_committed(&self) -> Option<&CausalFrontier> {
        self.committed.as_ref()
    }
    fn ab_set_last_committed(&mut self, version: &Version) {
        let checkpoint = self.ready.pop_front().expect("no ready Creek checkpoint");
        assert_eq!(
            &checkpoint.version, version,
            "Creek decisions must be applied in order"
        );
        self.applied_slot = checkpoint.slot;
        self.decisions.remove(&checkpoint.slot);
        self.committed = Some(CausalFrontier::from(version));
    }
}

// As in the original worker design, log-memory sampling measures owner-local
// metadata. It never locks or traverses the worker's independently owned journal.
impl<O: IsOmega + DeepSizeOf> DeepSizeOf for Creek<O> {
    fn deep_size_of_children(&self, ctx: &mut deepsize::Context) -> usize {
        self.oracle.deep_size_of_children(ctx)
            + self.members.deep_size_of_children(ctx)
            + self.indices.deep_size_of_children(ctx)
            + self.versions.deep_size_of_children(ctx)
            + self.heads.deep_size_of_children(ctx)
            + self.delivered.deep_size_of_children(ctx)
            + self.decisions.deep_size_of_children(ctx)
            + self.ready.deep_size_of_children(ctx)
            + self.committed.deep_size_of_children(ctx)
    }
}

impl<O: IsOmega> Drop for Creek<O> {
    fn drop(&mut self) {
        if let Ok(mut shared) = self.shared.lock() {
            shared.closed = true;
        }
    }
}

/// Pass Some(&mut creek::drive) to Node::run. This owner-thread hook only
/// publishes metadata and applies ready cuts; the worker runs Paxos in parallel.
pub fn drive<L, O, T>(node: &mut Node<CommitmentLog<L, Creek<O>>, T>) -> io::Result<()>
where
    L: IsLog + DeepSizeOf,
    L::Update: Serialize + DeserializeOwned,
    O: IsOmega + DeepSizeOf,
    T: Transport,
{
    let log = node.replica_mut().state_mut();
    log.protocol_mut().poll()?;
    log.commit_ready();
    Ok(())
}

#[cfg(test)]
mod tests;
