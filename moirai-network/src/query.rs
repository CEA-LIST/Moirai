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
    fn query_state_json(
        replica: &Replica<Self, Tcsb<Self::Op>>,
    ) -> serde_json::Value;
}
