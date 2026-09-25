//! What a node has to know to host a [`ModelLog`]: how to read one out.
//!
//! # Why the impl is here and not there
//!
//! `QueryableLog` is `moirai-network`'s trait and [`ModelLog`] is this
//! crate's type, so the orphan rule allows the impl in either crate and in no
//! third one. Writing it there would mean `moirai-network` naming a model
//! log, which is exactly what criterion I-A10 forbids; writing it here costs
//! one optional dependency and no concept at all. `moirai-network` depends on
//! `moirai-crdt` and `moirai-protocol`, never on this crate, so the edge
//! added here closes no cycle.
//!
//! Behind the `network` feature, off by default: every other test in this
//! crate runs a log in process and has no use for a socket.
//!
//! # What `GET /api/state` answers with
//!
//! The canonical form of `02 Validation Plan` §2, which is what
//! [`crate::eval`] builds — the same document the in-process tests and the
//! equivalence oracle compare, so a scenario driving three replicas over HTTP
//! and a scenario driving two twins in memory are reading the same thing.
//! A model whose log has no table yet reads `null`.
//!
//! # And what `GET /api/model/{id}/state/identified` answers with
//!
//! The same document with every element's store identity under `__id`
//! ([`crate::eval::read_identified`]). An opt-in read beside the default one,
//! so a reader that wants a plain document, a JSON model's above all, keeps
//! getting exactly that.

use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::Value;

use crate::eval::read_identified;
use crate::log::ModelLog;
use crate::op::ModelOp;

impl QueryableLog for ModelLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<ModelOp>>) -> Value {
        replica.query(&Read::new())
    }

    /// The same document with every element's store identity in it, which is
    /// what `GET /api/model/{id}/state/identified` answers: see
    /// [`crate::eval::read_identified`].
    fn query_identified_state_json(replica: &Replica<Self, Tcsb<ModelOp>>) -> Value {
        let log = replica.state();
        match log.semantics() {
            Some(sem) => read_identified(sem, log.root()),
            None => Value::Null,
        }
    }
}
