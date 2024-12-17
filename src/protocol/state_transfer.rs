use super::{membership::Views, po_log::POLog, pure_crdt::PureCRDT, tcsb::Tcsb};
use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StateTransfer<O>
where
    O: PureCRDT,
{
    pub group_membership: Views,
    pub state: POLog<O>,
    pub lsv: VectorClock<String, usize>,
    pub ltm: MatrixClock<String, usize>,
}

impl<O> StateTransfer<O>
where
    O: PureCRDT,
{
    pub fn new(tcsb: &Tcsb<O>, to: &String) -> Self {
        assert!(&tcsb.id != to && tcsb.group_membership.members().contains(to));
        StateTransfer {
            group_membership: tcsb.group_membership.clone(),
            state: tcsb.state.clone(),
            lsv: tcsb.lsv.clone(),
            ltm: tcsb.ltm.clone(),
        }
    }
}

impl<O> Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub fn deliver_state(&mut self, state: StateTransfer<O>) {
        self.lsv = state.lsv;
        self.ltm = state.ltm;
        self.ltm.most_update(&self.id);
        self.state = state.state;
        self.group_membership = state.group_membership;
    }
}
