//! EC.03: what one replica serves, as one line of the differential's trace.
//!
//! Four reads of a model (`GET /api/state`, the identified read of the tree,
//! the unresolved count, the migration status), through the crate's public
//! API, joined into one canonical JSON text and hashed. After a cut
//! `model_node` serves a frozen identified document, which only the crate
//! sees; the identified read here is always the tree's, and the plain read
//! is the served one. The same file is compiled into the reference and the
//! candidate, so two equal lines mean the replica read the same four things.
#![allow(dead_code)]

use std::hash::{DefaultHasher, Hash, Hasher};

use moirai_interp::eval::read_identified;
use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    crdt::query::Read,
    replica::{IsReplica, Replica},
};
use serde_json::{Value, json};

pub type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// The four reads, as one document.
pub fn reads(replica: &Rep) -> Value {
    let log = replica.state();
    let identified = match log.semantics() {
        Some(sem) => read_identified(sem, log.root()),
        None => Value::Null,
    };
    json!({
        "plain": replica.query(&Read::<Value>::new()),
        "identified": identified,
        "unresolved": log.unresolved(),
        "migration": log.migration(),
    })
}

/// A short stable name for a document. Both builds hash with the same
/// toolchain's `DefaultHasher`, so only equal texts give equal names.
pub fn digest(doc: &Value) -> String {
    let mut hasher = DefaultHasher::new();
    serde_json::to_string(doc)
        .expect("a read serializes")
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
