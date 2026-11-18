use crate::{clock::causal_frontier::CausalFrontier, replica::ReplicaId};

pub trait UseOmega {
    fn set_leader(&mut self, leader: ReplicaId);

    fn committed_frontier(&self) -> Option<&CausalFrontier>;
}