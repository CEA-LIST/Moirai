//! Wire v1: 601c887's frames, with the replica's payloads opaque.
//!
//! Tags and field names are 601c887's, so a frame encodes to the bytes a
//! 601c887 node writes. Key order inside opaque payloads is kept by
//! serde_json's `preserve_order`.

use std::fmt;

use serde::de::{DeserializeOwned, Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::{LogId, LogPayload, Opaque, json};

/// One frame. A frame of a type wire v1 does not name is `Unknown`, kept
/// whole; a frame of a named type that does not decode is an error.
#[derive(Debug, Clone)]
pub enum TransportMessage {
    Event {
        event: Opaque,
    },
    Batch {
        batch: Opaque,
    },
    SyncRequest {
        since: Opaque,
    },
    /// "I have nothing; send me everything", for the log `log_id`.
    StateRequest {
        id: String,
        log_id: LogId,
    },
    StateResponse {
        snapshot: Opaque,
        log: LogPayload,
        log_id: LogId,
    },
    /// The donor declined; `log_id` is the log the donor hosts.
    StateUnavailable {
        reason: String,
        log_id: LogId,
    },
    Hello {
        id: String,
        metadata: Option<String>,
    },
    Ack {
        from: String,
    },
    Goodbye {
        id: String,
    },
    /// The `index`th of `count` pieces of one message's JSON text.
    Chunk {
        id: u64,
        index: u32,
        count: u32,
        data: String,
    },
    /// A frame of any other type, passed on as it came.
    Unknown(Value),
}

/// The derive's home: a private mirror, so the generated codec is not a public
/// `TransportMessage::serialize`/`deserialize` that would bypass `Unknown`.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", remote = "TransportMessage")]
enum Wire {
    Event {
        event: Opaque,
    },
    Batch {
        batch: Opaque,
    },
    SyncRequest {
        since: Opaque,
    },
    StateRequest {
        #[serde(deserialize_with = "replica_id")]
        id: String,
        log_id: LogId,
    },
    StateResponse {
        snapshot: Opaque,
        log: LogPayload,
        log_id: LogId,
    },
    StateUnavailable {
        reason: String,
        log_id: LogId,
    },
    Hello {
        #[serde(deserialize_with = "replica_id")]
        id: String,
        metadata: Option<String>,
    },
    Ack {
        #[serde(deserialize_with = "replica_id")]
        from: String,
    },
    Goodbye {
        #[serde(deserialize_with = "replica_id")]
        id: String,
    },
    Chunk {
        id: u64,
        index: u32,
        count: u32,
        data: String,
    },
    #[serde(skip)]
    Unknown(Value),
}

const NAMED: [&str; 10] = [
    "Event",
    "Batch",
    "SyncRequest",
    "StateRequest",
    "StateResponse",
    "StateUnavailable",
    "Hello",
    "Ack",
    "Goodbye",
    "Chunk",
];

impl Serialize for TransportMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unknown(frame) => frame.serialize(serializer),
            named => Wire::serialize(named, serializer),
        }
    }
}

impl<'de> Deserialize<'de> for TransportMessage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let frame = Value::Object(deserializer.deserialize_map(FrameObject)?);
        let Some(tag) = frame.get("type").and_then(Value::as_str) else {
            return Err(D::Error::custom(
                "a frame is an object with a string type tag",
            ));
        };
        if NAMED.contains(&tag) {
            Wire::deserialize(frame).map_err(D::Error::custom)
        } else {
            Ok(Self::Unknown(frame))
        }
    }
}

/// A frame's top-level object, refusing a repeated key as 601c887's derive
/// does, where building a `Value` would keep only the last one.
struct FrameObject;

impl<'de> Visitor<'de> for FrameObject {
    type Value = Map<String, Value>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a frame object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut frame = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if frame.contains_key(&key) {
                return Err(A::Error::custom(format!("duplicate field `{key}`")));
            }
            let value = map.next_value()?;
            frame.insert(key, value);
        }
        Ok(frame)
    }
}

/// 601c887's replica-id rule: nonempty ASCII letters, digits, `.`, `_`, `-`.
fn replica_id<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    if !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        Ok(id)
    } else {
        Err(D::Error::custom(format!(
            "Invalid replica id {id:?}: expected nonempty ASCII letters, digits, '.', '_' or '-'"
        )))
    }
}

/// Decode one frame under 601c887's 1024-deep rule, on a grown stack, and
/// refuse trailing content. The transports use this instead of
/// `serde_json::from_slice`, whose 128-deep recursion limit refuses frames
/// 601c887 delivered.
pub fn decode_frame(input: &[u8]) -> Result<TransportMessage, serde_json::Error> {
    json::decode(input)
}

/// [`decode_frame`] for a value that wraps a frame, such as the relay's
/// envelope: the depth rule counts the envelope's own braces too. Hidden: it is
/// there for the relay transport's envelope, not as a general JSON decoder.
#[doc(hidden)]
pub fn decode_json<T: DeserializeOwned>(input: &[u8]) -> Result<T, serde_json::Error> {
    json::decode(input)
}
