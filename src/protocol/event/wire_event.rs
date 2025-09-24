use std::fmt::Debug;

use crate::{
    protocol::{clock::version_vector::Seq, event::lamport::Lamport, membership::ReplicaIdOwned},
    HashMap,
};

#[derive(Clone, Debug)]
pub struct WireEvent<O> {
    pub(crate) id: (ReplicaIdOwned, Seq),
    pub(crate) lamport: Lamport,
    pub(crate) op: O,
    pub(crate) version: HashMap<ReplicaIdOwned, Seq>,
}

impl<O> WireEvent<O> {
    pub fn new(
        id: (ReplicaIdOwned, Seq),
        lamport: Lamport,
        op: O,
        version: HashMap<ReplicaIdOwned, Seq>,
    ) -> Self {
        Self {
            id,
            lamport,
            op,
            version,
        }
    }
}
