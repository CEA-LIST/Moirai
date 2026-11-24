//! The state transfer's `log` field, ported from 601c887's `state_transfer.rs`.

/// The exported log as it travels: compressed, unless compressing it does not
/// help.
///
/// Only this field is compressed, never the frame, so whatever forwards a
/// frame can keep copying it without parsing it. The log is the payload that
/// grows with history and repeats the same shapes, so it compresses well.
/// A frame is one line of JSON, so the deflated bytes travel as base64, and
/// [`LogPayload::encode`] keeps whichever form is smaller: the encoded size
/// never exceeds the plain size.
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
/// A few megabytes of deflate can expand to gigabytes, and the sender is not
/// trusted, so the reader is bounded. A caller with its own ceiling passes it
/// to [`LogPayload::decode_within`].
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
    /// `None` rather than an error type because the caller already has a
    /// defined answer for a log it cannot read: fall back to a delta sync.
    pub fn decode(self) -> Option<serde_json::Value> {
        self.decode_within(MAX_INFLATED_BYTES)
    }

    /// [`Self::decode`], refusing a payload that would inflate past `limit`
    /// bytes rather than allocating it.
    pub fn decode_within(self, limit: u64) -> Option<serde_json::Value> {
        use std::io::Read;

        use base64::Engine as _;

        match self {
            LogPayload::Plain(value) => checked_value(value)
                .map_err(|e| eprintln!("Refusing an unreadable plain log: {e}"))
                .ok(),
            LogPayload::Deflate(text) => {
                let deflated = base64::engine::general_purpose::STANDARD
                    .decode(text)
                    .ok()?;
                let mut raw = Vec::new();
                // One byte past the limit, so an oversized payload is detected
                // rather than silently truncated into something that might
                // still parse.
                flate2::read::ZlibDecoder::new(&deflated[..])
                    .take(limit.saturating_add(1))
                    .read_to_end(&mut raw)
                    .ok()?;
                if raw.len() as u64 > limit {
                    return None;
                }
                crate::json::decode(&raw)
                    .map_err(|e| eprintln!("Refusing an unreadable inflated log: {e}"))
                    .ok()
            }
        }
    }
}

// Depth rule ported from 601c887's `json_decode.rs`, now shared with the frame
// decoder in `json.rs`: a log may nest up to 1024 deep, past serde_json's
// default 128, on a grown stack.

/// A plain log comes already parsed, so its depth is measured on the value.
fn checked_value(value: serde_json::Value) -> Result<serde_json::Value, serde_json::Error> {
    use serde_json::Value;

    let mut maximum = 0;
    let mut pending = vec![(&value, 0)];
    while let Some((node, parent_depth)) = pending.pop() {
        let depth = parent_depth + 1;
        match node {
            Value::Array(children) => {
                maximum = maximum.max(depth);
                pending.extend(children.iter().map(|child| (child, depth)));
            }
            Value::Object(children) => {
                maximum = maximum.max(depth);
                pending.extend(children.values().map(|child| (child, depth)));
            }
            _ => (),
        }
    }
    if let Err(error) = crate::json::check_depth(maximum) {
        // Even destruction must not recurse through an arbitrarily deep input.
        let mut pending = vec![value];
        while let Some(node) = pending.pop() {
            match node {
                Value::Array(children) => pending.extend(children),
                Value::Object(children) => pending.extend(children.into_values()),
                _ => (),
            }
        }
        return Err(error);
    }
    Ok(value)
}
