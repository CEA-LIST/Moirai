use std::marker::PhantomData;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{
    broadcast::{batch::Batch, since::Since},
    event::Event,
    log_id::LogId,
    utils::intern_str::Resolver,
};

pub mod kind {
    #[cfg(feature = "test_utils")]
    use deepsize::DeepSizeOf;

    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Any;
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Event;
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Batch;
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Since;
}

pub type EventMessage<O> = Message<O, kind::Event>;
pub type BatchMessage<O> = Message<O, kind::Batch>;
pub type SinceMessage = Message<(), kind::Since>;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Message<O, K = kind::Any> {
    payload: Payload<O>,
    resolver: Resolver,
    /// Which log this message belongs to.
    ///
    /// Hoisted beside the resolver rather than carried per event, for the same
    /// reason the resolver is: one message is one log's traffic, so one copy
    /// says everything a receiver needs in order to decide whether the payload
    /// is addressed to the log it hosts.
    log_id: LogId,
    #[cfg_attr(feature = "serde", serde(skip))]
    _kind: PhantomData<K>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum Payload<O> {
    Event(Event<O>),
    Batch(Batch<O>),
    Since(Since),
}

impl<O, K> Message<O, K> {
    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    /// The log this message came from. A receiver hosting a different log
    /// refuses it; see [`crate::broadcast::tcsb::Tcsb::receive`].
    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }
}

impl<O> Message<O> {
    pub fn new(payload: Payload<O>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload,
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn payload(&self) -> &Payload<O> {
        &self.payload
    }
}

impl<O> Message<O, kind::Event> {
    pub fn new(event: Event<O>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Event(event),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn event(&self) -> &Event<O> {
        match &self.payload {
            Payload::Event(event) => event,
            _ => unreachable!("EventMessage is expected to hold an event payload"),
        }
    }
}

impl<O> Message<O, kind::Batch> {
    pub fn new(batch: Batch<O>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Batch(batch),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn batch(&self) -> &Batch<O> {
        match &self.payload {
            Payload::Batch(batch) => batch,
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }

    pub fn into_batch(self) -> Batch<O> {
        match self.payload {
            Payload::Batch(batch) => batch,
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }

    pub fn into_parts(self) -> (Batch<O>, Resolver) {
        match self.payload {
            Payload::Batch(batch) => (batch, self.resolver),
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }
}

impl<O> Message<O, kind::Since> {
    pub fn new(since: Since, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Since(since),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn since(&self) -> &Since {
        match &self.payload {
            Payload::Since(since) => since,
            _ => unreachable!("SinceMessage is expected to hold a since payload"),
        }
    }
}

/// M-E1 of the model plane's validation plan: what `log_id` costs on the
/// wire, asserted rather than derived. A frame is the compact JSON of one
/// message (`moirai-network/src/transport.rs`, `write_frame`), so the field
/// serialises once per frame as `,"log_id":"<32 hex>"`: one comma, eight
/// characters of key, one colon and a 34-character quoted value, 44 bytes.
#[cfg(all(test, feature = "serde"))]
mod wire_overhead {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        broadcast::tcsb::{IsTcsb, Tcsb},
        log_id::LOG_ID_LEN,
        utils::intern_str::{InternalizeOp, Interner},
    };

    /// The bytes `log_id` adds to a compact frame.
    const LOG_ID_WIRE_BYTES: usize = 1 + "\"log_id\"".len() + 1 + (LOG_ID_LEN + 2);

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Op(u32);

    impl InternalizeOp for Op {
        fn internalize(self, _interner: &Interner) -> Self {
            self
        }
    }

    /// An event as a replica broadcasts it: minted by a `Tcsb`, so the
    /// resolver, the version and the lamport are what a real frame carries.
    fn event_message() -> EventMessage<Op> {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern("a");
        interner.intern("b");
        let mut tcsb = Tcsb::new(idx, interner.into_shared(), LogId::from_bytes([0x5a; 16]));
        tcsb.send(Op(7))
    }

    /// The compact JSON of `message` with its `log_id` member removed, which
    /// is the frame the pre-model-plane wire carried.
    fn frame_without_log_id(frame: &str) -> String {
        let mut value: serde_json::Value = serde_json::from_str(frame).expect("a frame is JSON");
        let removed = value
            .as_object_mut()
            .expect("a message is a JSON object")
            .remove("log_id")
            .expect("the frame carries a log_id member");
        assert_eq!(removed.as_str().map(str::len), Some(LOG_ID_LEN));
        value.to_string()
    }

    #[test]
    fn a_frame_carries_the_log_id_exactly_once() {
        let frame = serde_json::to_string(&event_message()).expect("serialize");

        assert_eq!(
            frame.matches("\"log_id\":\"").count(),
            1,
            "the field is hoisted beside the resolver, once per message: {frame}"
        );
    }

    #[test]
    fn the_log_id_costs_forty_four_bytes_per_frame() {
        let frame = serde_json::to_string(&event_message()).expect("serialize");
        let without = frame_without_log_id(&frame);

        assert_eq!(
            LOG_ID_WIRE_BYTES, 44,
            "the derived constant is the plan's number"
        );
        assert_eq!(
            frame.len() - without.len(),
            LOG_ID_WIRE_BYTES,
            "frame:\n{frame}\nwithout log_id:\n{without}"
        );
        // M-E1 reads this from the test output; the workload's median frame
        // is measured on the wire by `experiments/model-plane/m-e1/run.sh`.
        println!(
            "M-E1 event frame: {} bytes, of which log_id {} bytes ({:.2}%)",
            frame.len(),
            LOG_ID_WIRE_BYTES,
            100.0 * LOG_ID_WIRE_BYTES as f64 / frame.len() as f64
        );
    }

    #[test]
    fn the_member_removed_is_the_log_id_and_nothing_else() {
        let frame = serde_json::to_string(&event_message()).expect("serialize");
        let without = frame_without_log_id(&frame);

        // Compact JSON keeps field order, so the removed text is one
        // contiguous member, `,"log_id":"<id>"`: cutting exactly that text
        // out of the frame must give the same document as removing the key.
        let member = format!(",\"log_id\":\"{}\"", "5a".repeat(16));
        assert_eq!(frame.matches(&member).count(), 1, "{frame}");
        let cut: serde_json::Value =
            serde_json::from_str(&frame.replacen(&member, "", 1)).expect("still a frame");
        let parsed: serde_json::Value = serde_json::from_str(&without).expect("a frame");
        assert_eq!(cut, parsed);
    }
}
