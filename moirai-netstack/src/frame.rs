//! Wire v1: 601c887's frames, with the replica's payloads opaque.
//!
//! Tags and field names are 601c887's, so a frame encodes to the bytes a
//! 601c887 node writes. Key order inside opaque payloads is kept by
//! serde_json's `preserve_order`.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::Value;

use crate::{LogId, LogPayload, Opaque};

/// One frame. A frame of a type wire v1 does not name is `Unknown`, kept
/// whole; a frame of a named type that does not decode is an error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", remote = "Self")]
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
        #[serde(deserialize_with = "replica_id")]
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
    /// The `index`th of `count` pieces of one message's JSON text.
    Chunk {
        id: u64,
        index: u32,
        count: u32,
        data: String,
    },
    /// A frame of any other type, passed on as it came.
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
            named => Self::serialize(named, serializer),
        }
    }
}

impl<'de> Deserialize<'de> for TransportMessage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let frame = Value::deserialize(deserializer)?;
        let Some(tag) = frame.get("type").and_then(Value::as_str) else {
            return Err(D::Error::custom(
                "a frame is an object with a string type tag",
            ));
        };
        if NAMED.contains(&tag) {
            Self::deserialize(frame).map_err(D::Error::custom)
        } else {
            Ok(Self::Unknown(frame))
        }
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
