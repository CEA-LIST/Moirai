//! The state transfer's `log` field, ported from 601c887's `state_transfer.rs`.

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
/// plain size, which is what lets `MAX_STATE_TRANSFER_LOG_BYTES` stay a bound
/// on the message as well as on memory.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "encoding", content = "log")]
pub enum LogPayload {
    /// The exported log's JSON, verbatim.
    Plain(serde_json::Value),
    /// The same JSON, zlib-deflated and base64-encoded.
    Deflate(String),
}

/// Largest log [`LogPayload::decode`] will *inflate*, in bytes, for a caller
/// that names no bound of its own.
///
/// Compression cuts both ways: at the ratios measured above a few megabytes
/// of base64 deflate expand to something on the order of a gigabyte.
/// Bounding the reader rather than trusting the sender is the same
/// discipline the relay's `read_frame` and the bootnode's `read_body`
/// already apply, and for the same reason: the socket is unauthenticated.
///
/// This default does not bound a node's state transfer. A node joining by
/// state transfer names its own bound to [`LogPayload::decode_within`], its
/// donor-side ceiling (`MAX_STATE_TRANSFER_LOG_BYTES` in `generic.rs`, 128
/// MiB): a payload a donor would refuse to send is one a joiner should
/// refuse to accept. That ceiling, not this one, is the one sized for a
/// 5,000-object model's logs (21 MB for the copy, 62 to 88 MB for the cut
/// source), which a relay session carries as chunk frames since SI.13 (D2).
/// The logs this default admits are smaller.
const MAX_INFLATED_BYTES: u64 = 48 * 1024 * 1024;

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

    /// Recover the exported log, or `None` if this payload cannot be read
    /// or would inflate past [`MAX_INFLATED_BYTES`].
    ///
    /// `None` rather than an error type because the one caller already has a
    /// defined answer for a log it cannot parse — fall back to a delta sync —
    /// and a peer built from a different model is the likeliest cause either
    /// way.
    pub fn decode(self) -> Option<serde_json::Value> {
        self.decode_within(MAX_INFLATED_BYTES)
    }

    /// [`Self::decode`], refusing a payload that would inflate past `limit`
    /// bytes rather than allocating it.
    pub fn decode_within(self, limit: u64) -> Option<serde_json::Value> {
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
                    .take(limit.saturating_add(1))
                    .read_to_end(&mut raw)
                    .ok()?;
                if raw.len() as u64 > limit {
                    return None;
                }
                serde_json::from_slice(&raw).ok()
            }
        }
    }
}
