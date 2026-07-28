//! Moving a CRDT log to a replica that was not there when it was built.
//!
//! `SyncRequest` is served from the TCSB outbox, and `prune_outbox` drops
//! everything at or below the causally stable version. So a joiner can replay
//! the unstable suffix and nothing else: the operations behind the stable
//! frontier have been folded into the compacted state and no longer exist as
//! operations anywhere. A pure operation-based CRDT has no snapshot by
//! construction — the PO-Log *is* the state — so the compacted state itself is
//! what has to travel.
//!
//! [`TransferableLog`] is how it travels. It is deliberately a *capability*
//! rather than a bound on [`GenericNode`]: a hand-written log — the
//! `awset_node` example, say — need not be serialisable to keep working, it
//! simply cannot serve or accept a transfer. That mirrors [`QueryableLog`],
//! which makes `GET /api/state` opt-in the same way and for the same reason.
//!
//! [`GenericNode`]: crate::generic::GenericNode
//! [`QueryableLog`]: crate::query::QueryableLog

use serde::{de::DeserializeOwned, Serialize};

use moirai_protocol::state::log::IsLog;

/// A log that can be handed to a replica joining an existing session.
///
/// Blanket-implemented for every serialisable log, so a generated crate gets it
/// for free the moment its log type derives `Serialize` and `Deserialize`.
///
/// JSON, rather than a compact binary format, because that is what the rest of
/// this transport already speaks — `TransportMessage` is serialised with
/// `serde_json` and framed by newline. Size is measured, not assumed: see the
/// phase-2 experiment under `experiments/`.
pub trait TransferableLog: IsLog {
    /// Serialise the log for transfer. Not the rendered *value*: a value can be
    /// read but not replayed onto.
    fn export_log(&self) -> serde_json::Value;

    /// Rebuild a log from [`TransferableLog::export_log`]'s output.
    ///
    /// `None` on a payload this replica cannot parse — a peer built from a
    /// different model, most plausibly. The joiner then falls back to a plain
    /// `SyncRequest` rather than installing half a state.
    fn import_log(value: serde_json::Value) -> Option<Self>
    where
        Self: Sized;
}

impl<L> TransferableLog for L
where
    L: IsLog + Serialize + DeserializeOwned,
{
    fn export_log(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    fn import_log(value: serde_json::Value) -> Option<Self> {
        serde_json::from_value(value).ok()
    }
}
