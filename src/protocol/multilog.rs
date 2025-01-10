use super::po_log::POLog;
use crate::crdt::{
    aw_set::AWSet,
    counter::{Counter, Number},
    graph::Graph,
    mv_register::MVRegister,
    rw_set::RWSet,
};
use radix_trie::Trie;
use std::{fmt::Debug, hash::Hash};

pub enum Logs<
    V: Debug + Clone + Hash + Eq = String,
    I: Debug + Number = usize,
    G: Debug + Clone + Hash + Eq = String,
    J: Debug + Clone + Hash + Eq = String,
    N: Debug + Clone + Hash + Eq = String,
> {
    AWSetLog(POLog<AWSet<V>>),
    RwSetLog(POLog<RWSet<N>>),
    CounterLog(POLog<Counter<I>>),
    GraphLog(POLog<Graph<G>>),
    MvRegisterLog(POLog<MVRegister<J>>),
}

pub struct MultiLog<
    V: Debug + Clone + Hash + Eq = String,
    I: Debug + Number = usize,
    G: Debug + Clone + Hash + Eq = String,
    J: Debug + Clone + Hash + Eq = String,
    N: Debug + Clone + Hash + Eq = String,
> {
    pub multilog: Trie<String, Logs<V, I, G, J, N>>,
}
