use std::fmt::Debug;

use crate::{
    broadcast::internalizer::Resolver,
    commitment::wire_id::StableReplicaId,
    replica::{ReplicaId, ReplicaIdx},
};

use crate::event::Event;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// An oracle is a component that provides information about the current state of the system, such as the current leader.
pub trait IsOracle: Debug + Clone + Default {
    /// Query the oracle for the current leader.
    fn query(&self) -> StableReplicaId;

    fn set_leader(&mut self, leader: impl Into<StableReplicaId>);

    fn update<T>(&mut self, event: Event<T>) -> Event<T>;

    /// Supply the system-wide identity mapping; static oracles need no setup.
    fn configure_members(&mut self, _: &[(StableReplicaId, ReplicaId)]) {}
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

/// Selects the smallest stable ID observed within the timeout (in milliseconds).
/// If no member is active, returns the explicitly configured fallback leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct EventOmega {
    leader: StableReplicaId,
    timeout_ms: usize,
    checkpoints: BTreeSet<(Instant, ReplicaIdx)>,
    latest: BTreeMap<ReplicaIdx, Instant>,
    members: BTreeMap<ReplicaId, StableReplicaId>,
    stable_ids: Vec<StableReplicaId>,
    resolver: Option<Resolver>,
}

/// Descriptive alias for the event-driven Omega implementation.
pub type EventOracle = EventOmega;

impl EventOmega {
    pub fn new() -> Self {
        Self::with_leader(StableReplicaId(0))
    }

    pub fn with_leader(leader: impl Into<StableReplicaId>) -> Self {
        Self::from_parts(leader, 10_000, BTreeSet::new(), None)
    }

    pub fn from_parts(
        leader: impl Into<StableReplicaId>,
        timeout_ms: usize,
        checkpoints: BTreeSet<(Instant, ReplicaIdx)>,
        resolver: Option<Resolver>,
    ) -> Self {
        let mut latest = BTreeMap::new();
        for (instant, idx) in checkpoints {
            latest
                .entry(idx)
                .and_modify(|old: &mut Instant| *old = (*old).max(instant))
                .or_insert(instant);
        }
        let checkpoints = latest
            .iter()
            .map(|(&idx, &instant)| (instant, idx))
            .collect();
        Self {
            leader: leader.into(),
            timeout_ms,
            checkpoints,
            latest,
            members: BTreeMap::new(),
            stable_ids: Vec::new(),
            resolver,
        }
    }

    pub fn update_resolver(&mut self, resolver: Option<Resolver>) {
        if self.resolver == resolver {
            return;
        }
        if let (Some(old), Some(new)) = (&self.resolver, &resolver) {
            self.latest = self
                .latest
                .iter()
                .map(|(&idx, &instant)| {
                    let id = old.resolve(idx).expect("Unknown checkpoint origin");
                    (new.get(id).expect("Oracle membership changed"), instant)
                })
                .collect();
            self.checkpoints = self
                .latest
                .iter()
                .map(|(&idx, &instant)| (instant, idx))
                .collect();
        } else if resolver.is_none() {
            self.latest.clear();
            self.checkpoints.clear();
        }
        self.resolver = resolver;
        self.refresh_stable_ids();
    }

    fn refresh_stable_ids(&mut self) {
        self.stable_ids = self
            .resolver
            .as_ref()
            .map(|r| {
                (0..r.len())
                    .map(|i| {
                        *self
                            .members
                            .get(r.resolve(ReplicaIdx(i)).unwrap())
                            .expect("Oracle member missing from stable ID mapping")
                    })
                    .collect()
            })
            .unwrap_or_default();
    }

    fn query_at(&self, now: Instant) -> StableReplicaId {
        let timeout =
            Duration::from_millis(u64::try_from(self.timeout_ms).expect("Timeout exceeds u64"));
        self.checkpoints
            .iter()
            .rev()
            .take_while(|(instant, _)| now.saturating_duration_since(*instant) <= timeout)
            .map(|(_, idx)| {
                self.stable_ids
                    .get(idx.0)
                    .copied()
                    .expect("Configure oracle membership before querying events")
            })
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
    fn configure_members(&mut self, members: &[(StableReplicaId, ReplicaId)]) {
        self.members = members
            .iter()
            .map(|(stable, id)| (id.clone(), *stable))
            .collect();
        assert_eq!(self.members.len(), members.len(), "Duplicate replica IDs");
        assert_eq!(
            members
                .iter()
                .map(|(stable, _)| stable)
                .collect::<BTreeSet<_>>()
                .len(),
            members.len(),
            "Duplicate stable IDs"
        );
        self.refresh_stable_ids();
    }

    fn query(&self) -> StableReplicaId {
        self.query_at(Instant::now())
    }

    fn set_leader(&mut self, leader: impl Into<StableReplicaId>) {
        self.leader = leader.into();
    }

    fn update<T>(&mut self, event: Event<T>) -> Event<T> {
        self.update_resolver(Some(event.version().resolver().clone()));
        let instant = Instant::now();
        let idx = event.version().origin_idx();
        if let Some(previous) = self.latest.insert(idx, instant) {
            self.checkpoints.remove(&(previous, idx));
        }
        self.checkpoints.insert((instant, idx));
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
    };

    fn resolver(names: &[&str]) -> Resolver {
        let mut interner = Interner::new();
        for name in names {
            interner.intern(&ReplicaId::from(*name));
        }
        interner.resolver().clone()
    }
    fn members() -> Vec<(StableReplicaId, ReplicaId)> {
        vec![
            (StableReplicaId(9), ReplicaId::from("A")),
            (StableReplicaId(3), ReplicaId::from("B")),
        ]
    }
    fn event(r: &Resolver, idx: usize) -> Event<u64> {
        let mut version = Version::new(ReplicaIdx(idx), r.clone());
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
        let r = resolver(&["A", "B"]);
        let mut oracle = EventOmega::from_parts(
            StableReplicaId(9),
            10,
            BTreeSet::from([
                (now - Duration::from_millis(20), ReplicaIdx(1)),
                (now, ReplicaIdx(1)),
                (now, ReplicaIdx(0)),
            ]),
            Some(r),
        );
        oracle.configure_members(&members());
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
    fn event_refresh_and_resolver_reordering_preserve_identity() {
        let ab = resolver(&["A", "B"]);
        let ba = resolver(&["B", "A"]);
        let mut oracle = EventOmega::with_leader(StableReplicaId(9));
        oracle.configure_members(&members());
        assert_eq!(*oracle.update(event(&ab, 1)).op(), 42);
        oracle.update(event(&ab, 1));
        assert_eq!(oracle.checkpoints.len(), 1);
        oracle.update_resolver(Some(ba.clone()));
        oracle.update(event(&ba, 1));
        assert_eq!(oracle.query(), StableReplicaId(3));
        assert_eq!(oracle.checkpoints.len(), 2);
    }
}
