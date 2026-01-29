use std::fmt::Debug;

use crate::{
    crdt::{graph::aw_multidigraph::Graph, register::mv_register::MVRegister},
    protocol::state::po_log::VecLog,
    record,
};

#[derive(Clone, Debug)]
pub enum ReferenceManager<O> {
    Root(O),
}

record!(NamedElement {
    name: VecLog::<MVRegister::<String>>,
});

#[derive(Clone, Debug)]
pub struct ReferenceManagerLog<L> {
    pub child: L,
    pub references: Graph<String, String>,
}
