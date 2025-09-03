use std::collections::HashMap;

use bimap::BiMap;

use crate::utils::mut_owner::{MutOwner, Reader};

pub type ReplicaId = String;
pub type ReplicaIdx = usize;

#[derive(Debug)]
pub struct Membership {
    mapping: HashMap<ReplicaId, MutOwner<View>>,
}

impl Membership {
    pub fn new(id: &ReplicaId) -> Self {
        let view = View::new(id);
        let mapping = HashMap::from([(id.clone(), MutOwner::new(view))]);
        Self { mapping }
    }

    pub fn get(&self, id: &ReplicaId) -> Option<&MutOwner<View>> {
        self.mapping.get(id)
    }

    pub fn get_reader(&self, id: &ReplicaId) -> Option<Reader<View>> {
        self.mapping.get(id).map(|v| v.as_reader())
    }
}

// TODO: partialeq impl is origin_idx == other_idx && members.len() == other_members.len() (because of monotonicity)
#[derive(Debug, PartialEq, Eq)]
pub struct View {
    members: BiMap<ReplicaIdx, ReplicaId>,
}

impl View {
    pub fn new(id: &ReplicaId) -> Self {
        let mut members = BiMap::new();
        members.insert(0, id.clone());
        Self { members }
    }

    pub fn is_known(&self, id: &ReplicaId) -> bool {
        self.members.get_by_right(id).is_some()
    }

    pub fn members(&self) -> impl Iterator<Item = (&ReplicaIdx, &ReplicaId)> {
        self.members.iter()
    }

    pub fn get_idx(&self, id: &ReplicaId) -> Option<ReplicaIdx> {
        self.members.get_by_right(id).copied()
    }

    pub fn get_id(&self, idx: ReplicaIdx) -> Option<&ReplicaId> {
        self.members.get_by_left(&idx)
    }
}

// # Invariants
// - The origin replica is always present in the membership.
// - The membership is never empty.
// - `map` and `removed` are monotonically increasing.
// - There exist a translation for each known replica in `map` and there exist a replica in `map` for each translation, i.e., `translation` length = `match` length.
// - There may be less entries in a particular `translation` than in `map`.
// #[derive(Debug)]
// pub struct Membership {
//     origin_idx: ReplicaIdx,
//     map: BiMap<ReplicaIdx, ReplicaId>,
//     removed: HashSet<ReplicaIdx>,
//     /// A mapping of the other replicas id -> idx mapping to the one of the local replica.
//     translation: HashMap<ReplicaIdx, HashMap<ReplicaIdx, ReplicaIdx>>,
// }

// impl Membership {
//     pub fn new(id: ReplicaId) -> Self {
//         let mut map = BiMap::new();
//         map.insert(0, id);
//         Self {
//             origin_idx: 0,
//             map,
//             removed: HashSet::new(),
//             translation: HashMap::new(),
//         }
//     }

//     pub fn origin_idx(&self) -> ReplicaIdx {
//         self.origin_idx
//     }

//     pub fn members(&self, origin: ReplicaIdx) -> &[ReplicaIdx] {
//         self.translation.get(&origin).map(|m| m.keys().collect::<Vec<_>>().as_slice()).unwrap_or_default()
//     }
// }
