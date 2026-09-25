//! Query support for network-exposed CRDT state.
//!
//! This module provides traits for querying and serializing CRDT state
//! for network endpoints (e.g., HTTP API).

use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::Replica;
use moirai_protocol::state::log::IsLog;

/// Trait for logs whose state can be queried and serialized to JSON.
///
/// Implement this for any generated log type to enable the `GET /api/state`
/// endpoint in `GenericNode`.
///
/// The impl calls `replica.query(Read::new())` to evaluate the full CRDT
/// state via the `EvalNested<Read<Value>>` chain, then serializes the result.
pub trait QueryableLog: IsLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<Self::Op>>) -> serde_json::Value;

    /// The state with every element's identity in it, for
    /// `GET /api/model/{id}/state/identified`: an opt-in read beside
    /// [`QueryableLog::query_state_json`], which stays as it is.
    ///
    /// A log with no notion of element identity has nothing to add, and by
    /// default answers the default read.
    fn query_identified_state_json(replica: &Replica<Self, Tcsb<Self::Op>>) -> serde_json::Value {
        Self::query_state_json(replica)
    }
}
