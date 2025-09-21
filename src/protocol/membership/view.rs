use crate::protocol::membership::{ReplicaId, ReplicaIdx};
use bimap::BiMap;
use std::fmt::Display;

// TODO: partialeq impl is origin_idx == other_idx && members.len() == other_members.len() (because of monotonicity)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    count: usize,
    members: BiMap<ReplicaIdx, ReplicaId>,
}

impl View {
    pub fn new(id: &ReplicaId) -> Self {
        let mut members = BiMap::new();
        members.insert(0, id.clone());
        Self { members, count: 0 }
    }

    pub fn is_known(&self, id: &ReplicaId) -> bool {
        self.members.get_by_right(id).is_some()
    }

    pub fn members(&self) -> impl Iterator<Item = (&ReplicaIdx, &ReplicaId)> {
        self.members.iter()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn get_idx(&self, id: &ReplicaId) -> Option<ReplicaIdx> {
        self.members.get_by_right(id).copied()
    }

    pub fn get_id(&self, idx: ReplicaIdx) -> Option<&ReplicaId> {
        self.members.get_by_left(&idx)
    }

    pub fn add(&mut self, id: &ReplicaId) {
        self.count += 1;
        self.members.insert(self.count, id.clone());
    }
}

impl Display for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ids: Vec<String> = self.members.right_values().cloned().collect();
        write!(f, "View({})", ids.join(", "))
    }
}
