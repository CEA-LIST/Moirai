use std::{
    collections::BTreeSet,
    fmt::Debug,
    time::{Duration, Instant},
};

use crate::{commitment::wire_id::StableReplicaId, event::Event, replica::ReplicaIdx};

/// Supplies the current leader to the commitment protocol.
pub trait IsOracle: Debug + Clone + Default {
    fn query(&self) -> StableReplicaId;
    fn set_leader(&mut self, leader: impl Into<StableReplicaId>);
    fn update<T>(&mut self, event: Event<T>) -> Event<T>;

    /// Configure once, in the fixed local TCSB index order. Static oracles need no setup.
    fn configure_members(&mut self, _: &[StableReplicaId]) {}
}

/// Oracle output holder; configure a stable member ID before use.
/// An external oracle must provide eventual agreement and accuracy.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct Omega {
    leader: StableReplicaId,
}

impl Omega {
    pub fn new() -> Omega {
        Omega {
            leader: StableReplicaId(u16::MIN),
        }
    }

    pub fn with_leader(leader: impl Into<StableReplicaId>) -> Omega {
        Omega {
            leader: leader.into(),
        }
    }
}

impl Default for Omega {
    fn default() -> Self {
        Self::new()
    }
}

impl IsOracle for Omega {
    fn query(&self) -> StableReplicaId {
        self.leader.clone()
    }
    fn set_leader(&mut self, leader: impl Into<StableReplicaId>) {
        self.leader = leader.into();
    }
    fn update<T>(&mut self, event: Event<T>) -> Event<T> {
        event
    }
}

/// Selects the smallest stable ID observed within the timeout.
/// If no member is active, returns the configured fallback leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct EventOmega {
    leader: StableReplicaId,
    timeout: Duration,
    stable_ids: Vec<StableReplicaId>,
    latest: Vec<Option<Instant>>,
    checkpoints: BTreeSet<(Instant, StableReplicaId)>,
}

pub type EventOracle = EventOmega;

impl EventOmega {
    pub fn new() -> Self {
        Self::with_leader(StableReplicaId(0))
    }

    pub fn with_leader(leader: impl Into<StableReplicaId>) -> Self {
        Self::with_timeout(leader, Duration::from_secs(10))
    }

    /// Membership is configured once by OmegaCommit, before any event is delivered.
    pub fn with_timeout(leader: impl Into<StableReplicaId>, timeout: Duration) -> Self {
        Self {
            leader: leader.into(),
            timeout,
            stable_ids: Vec::new(),
            latest: Vec::new(),
            checkpoints: BTreeSet::new(),
        }
    }

    fn observe_at(&mut self, idx: ReplicaIdx, now: Instant) {
        let stable = self.stable_ids[idx.0];
        if let Some(previous) = self.latest[idx.0].replace(now) {
            self.checkpoints.remove(&(previous, stable));
        }
        self.checkpoints.insert((now, stable));
    }

    fn query_at(&self, now: Instant) -> StableReplicaId {
        self.checkpoints
            .iter()
            .rev()
            .take_while(|(instant, _)| now.saturating_duration_since(*instant) <= self.timeout)
            .map(|(_, id)| *id)
            .min()
            .unwrap_or(self.leader)
    }
}

impl Default for EventOmega {
    fn default() -> Self {
        Self::new()
    }
}

impl IsOracle for EventOmega {
    fn configure_members(&mut self, members: &[StableReplicaId]) {
        if !self.stable_ids.is_empty() {
            assert_eq!(self.stable_ids, members, "EventOracle membership is fixed");
            return;
        }
        assert!(
            !members.is_empty(),
            "EventOracle requires nonempty fixed membership"
        );
        assert_eq!(
            members.iter().collect::<BTreeSet<_>>().len(),
            members.len(),
            "Duplicate stable IDs"
        );
        self.stable_ids = members.to_vec();
        self.latest = vec![None; members.len()];
    }

    fn query(&self) -> StableReplicaId {
        self.query_at(Instant::now())
    }
    fn set_leader(&mut self, leader: impl Into<StableReplicaId>) {
        self.leader = leader.into();
    }

    fn update<T>(&mut self, event: Event<T>) -> Event<T> {
        self.observe_at(event.version().origin_idx(), Instant::now());
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broadcast::internalizer::Interner,
        clock::version_vector::Version,
        event::{id::EventId, lamport::Lamport},
        replica::ReplicaId,
    };

    fn event(names: &[&str], idx: usize) -> Event<u64> {
        let mut interner = Interner::new();
        for name in names {
            interner.intern(&ReplicaId::from(*name));
        }
        let mut version = Version::new(ReplicaIdx(idx), interner.resolver().clone());
        version.set_by_idx(ReplicaIdx(idx), 1);
        Event::new(
            EventId::from(&version),
            Lamport::from(&version),
            42,
            version,
        )
    }

    #[test]
    fn timeout_uses_latest_observation_and_stable_ids() {
        let now = Instant::now();
        let mut oracle = EventOmega::with_timeout(StableReplicaId(9), Duration::from_millis(10));
        oracle.configure_members(&[StableReplicaId(9), StableReplicaId(3)]);
        oracle.observe_at(ReplicaIdx(1), now - Duration::from_millis(20));
        oracle.observe_at(ReplicaIdx(1), now);
        oracle.observe_at(ReplicaIdx(0), now);
        assert_eq!(oracle.checkpoints.len(), 2);
        assert_eq!(oracle.query_at(now), StableReplicaId(3));
        assert_eq!(
            oracle.query_at(now + Duration::from_millis(10)),
            StableReplicaId(3)
        );
        assert_eq!(
            oracle.query_at(now + Duration::from_millis(11)),
            StableReplicaId(9)
        );
    }

    #[test]
    fn event_refresh_preserves_payload_and_replaces_the_old_checkpoint() {
        let mut oracle = EventOmega::with_leader(StableReplicaId(9));
        let ids = [StableReplicaId(9), StableReplicaId(3)];
        oracle.configure_members(&ids);
        assert_eq!(*oracle.update(event(&["A", "B"], 1)).op(), 42);
        oracle.update(event(&["A", "B"], 1));
        oracle.configure_members(&ids); // Identical setup does not reset activity.
        assert_eq!(oracle.checkpoints.len(), 1);
        assert_eq!(oracle.query(), StableReplicaId(3));
        oracle.update(event(&["A", "B"], 0));
        assert_eq!(oracle.checkpoints.len(), 2);
    }

    #[test]
    fn different_fixed_local_orders_elect_the_same_stable_id() {
        let mut a = EventOmega::with_leader(StableReplicaId(9));
        let mut b = EventOmega::with_leader(StableReplicaId(3));
        a.configure_members(&[StableReplicaId(9), StableReplicaId(3)]);
        b.configure_members(&[StableReplicaId(3), StableReplicaId(9)]);
        a.update(event(&["A", "B"], 1));
        b.update(event(&["B", "A"], 0));
        assert_eq!(a.query(), StableReplicaId(3));
        assert_eq!(b.query(), StableReplicaId(3));
    }

    #[test]
    #[should_panic(expected = "EventOracle membership is fixed")]
    fn membership_cannot_be_reordered_after_configuration() {
        let mut oracle = EventOmega::new();
        oracle.configure_members(&[StableReplicaId(0), StableReplicaId(1)]);
        oracle.configure_members(&[StableReplicaId(1), StableReplicaId(0)]);
    }
}
