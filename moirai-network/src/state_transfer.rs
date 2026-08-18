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

/// The exported log as it travels: compressed, unless compressing it does not
/// help.
///
/// # Why the field and not the frame
///
/// The relay copies a frame's `msg` through as a `RawValue` without ever
/// parsing it (`moirai-relay/src/routing.rs`), and that opacity is a stated
/// property rather than an implementation detail — it is what keeps the relay
/// independent of the operation type and what makes per-recipient encryption
/// additive later. Compressing the frame would end it. Compressing one field
/// inside `msg` leaves it exactly as it was.
///
/// # Why it is worth doing here in particular
///
/// This is the payload that *cannot* be made smaller by compacting. A sequence
/// CRDT keeps its whole causal history, so a model containing text grows the
/// exported log with session age whatever else is fixed — and that log is
/// unusually compressible, because most of it is the same member-id list and
/// the same operation shape repeated once per retained operation. Measured, on
/// a 800-operation three-replica string workload: **218 482 B of JSON becomes
/// 29 720 B, 7.4x**, deflate plus base64 included.
///
/// # Why base64, and why `Plain` is not dead code
///
/// A `TransportMessage` is one line of JSON, so a binary payload has to be
/// text; base64 gives back 4/3 of what deflate saved, which the ratio above
/// already accounts for. It also means the encoded form can be *larger* than
/// the input for something small or incompressible, so [`LogPayload::encode`]
/// keeps whichever is smaller. That makes the encoded size never exceed the
/// plain size, which is what lets `MAX_STATE_TRANSFER_BYTES` stay a bound on
/// the frame as well as on memory.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "encoding", content = "log")]
pub enum LogPayload {
    /// The JSON [`TransferableLog::export_log`] produced, verbatim.
    Plain(serde_json::Value),
    /// The same JSON, zlib-deflated and base64-encoded.
    Deflate(String),
}

/// Largest log this replica will *inflate* from a `StateResponse`, in bytes.
///
/// Compression cuts both ways: the relay bounds a frame at 1 MiB, and at the
/// ratios measured above 1 MiB of base64 deflate expands to something on the
/// order of a gigabyte. Bounding the reader rather than trusting the sender is
/// the same discipline the relay's `read_frame` and the bootnode's `read_body`
/// already apply, and for the same reason: the socket is unauthenticated.
///
/// Deliberately equal to the donor-side ceiling in `generic.rs`. A payload a
/// donor would refuse to send is one a joiner should refuse to accept.
const MAX_INFLATED_BYTES: u64 = 768 * 1024;

impl LogPayload {
    /// Encode an exported log, given the serialised form of it that the caller
    /// has already produced.
    ///
    /// Both arguments rather than one because the donor needs the serialised
    /// length anyway, to decide whether the transfer is within its ceiling at
    /// all, and serialising a large log twice is part of what this exists to
    /// avoid.
    pub fn encode(log: serde_json::Value, serialized: &[u8]) -> Self {
        use std::io::Write;

        use base64::Engine as _;

        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        let deflated = encoder
            .write_all(serialized)
            .and_then(|()| encoder.finish());
        let Ok(deflated) = deflated else {
            // Writing to a `Vec` does not fail for any reason worth a distinct
            // wire form; send it uncompressed and let the ceiling do its job.
            return LogPayload::Plain(log);
        };
        let encoded = base64::engine::general_purpose::STANDARD.encode(deflated);
        if encoded.len() < serialized.len() {
            LogPayload::Deflate(encoded)
        } else {
            LogPayload::Plain(log)
        }
    }

    /// Recover the exported log, or `None` if this payload cannot be read.
    ///
    /// `None` rather than an error type because the one caller already has a
    /// defined answer for a log it cannot parse — fall back to a delta sync —
    /// and a peer built from a different model is the likeliest cause either
    /// way.
    pub fn decode(self) -> Option<serde_json::Value> {
        use std::io::Read;

        use base64::Engine as _;

        match self {
            LogPayload::Plain(value) => Some(value),
            LogPayload::Deflate(text) => {
                let deflated = base64::engine::general_purpose::STANDARD
                    .decode(text)
                    .ok()?;
                let mut raw = Vec::new();
                // One byte past the limit, so an oversized payload is detected
                // rather than silently truncated into something that might
                // still parse — the same shape as `moirai-bootnode`'s
                // `read_body`.
                flate2::read::ZlibDecoder::new(&deflated[..])
                    .take(MAX_INFLATED_BYTES + 1)
                    .read_to_end(&mut raw)
                    .ok()?;
                if raw.len() as u64 > MAX_INFLATED_BYTES {
                    return None;
                }
                serde_json::from_slice(&raw).ok()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: serde_json::Value) -> (LogPayload, Option<serde_json::Value>) {
        let serialized = serde_json::to_vec(&value).unwrap();
        let payload = LogPayload::encode(value, &serialized);
        let wire = serde_json::to_string(&payload).unwrap();
        let back: LogPayload = serde_json::from_str(&wire).unwrap();
        (payload, back.decode())
    }

    #[test]
    fn a_repetitive_log_compresses_and_comes_back_unchanged() {
        // The shape a real exported log has: the same member-id list and the
        // same operation repeated once per retained operation.
        let entry = serde_json::json!({
            "op": {"Inc": 1.0},
            "tag": {"id": {"idx": 0, "resolver": ["049aa19e3a6a", "6bf2940fe957"], "seq": 1},
                    "lamport": 1}
        });
        let value = serde_json::json!({"unstable": vec![entry; 500]});
        let plain = serde_json::to_vec(&value).unwrap().len();
        let (payload, back) = round_trip(value.clone());

        assert!(matches!(payload, LogPayload::Deflate(_)));
        assert_eq!(back, Some(value));
        let encoded = serde_json::to_string(&payload).unwrap().len();
        assert!(
            encoded * 5 < plain,
            "expected better than 5x on a log of this shape, got {plain} -> {encoded}"
        );
    }

    #[test]
    fn a_small_log_is_left_alone() {
        let value = serde_json::json!({"stable": 0.0, "unstable": []});
        let (payload, back) = round_trip(value.clone());
        assert!(matches!(payload, LogPayload::Plain(_)));
        assert_eq!(back, Some(value));
    }

    #[test]
    fn an_oversized_inflation_is_refused_rather_than_allocated() {
        use std::io::Write;

        use base64::Engine as _;

        // A frame the relay would happily carry, holding far more than the
        // ceiling once inflated.
        let bomb = vec![b'x'; (MAX_INFLATED_BYTES + 1) as usize];
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&bomb).unwrap();
        let deflated = encoder.finish().unwrap();
        assert!(
            deflated.len() < 64 * 1024,
            "the point of this test is that a small frame inflates past the ceiling"
        );

        let payload =
            LogPayload::Deflate(base64::engine::general_purpose::STANDARD.encode(deflated));
        assert_eq!(payload.decode(), None);
    }
}
