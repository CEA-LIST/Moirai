//! EC.03c2b (spec/items/EC.03c2b.md, section 2): the inline tests of
//! `748a185`'s `moirai-protocol/src/log_id.rs` and
//! `moirai-protocol/src/broadcast/message.rs`, which EC.03c1 moved into this
//! crate (`log_id.rs`, `message.rs`) without their test modules. Each test
//! keeps its `748a185` name, body and expected values; the one adaptation is
//! registered in the spec (`event_message`, Léo's `Tcsb` mints the event and
//! this crate's `EventMessage::new` stamps the log id, as `LogReplica::send`
//! does). Left out, with the reason in the spec: `log_id`'s
//! `the_fallback_generator_is_also_distinct_per_call` (a private function) and
//! `state/trace.rs`'s three (crate-private functions of `moirai-protocol`).

/// `748a185` `moirai-protocol/src/log_id.rs` lines 300 to 363.
mod log_id {
    use moirai_network::log_id::{LogId, LogIdError, LOG_ID_LEN};

    #[test]
    fn a_generated_id_is_canonical_and_parses_back() {
        let id = LogId::generate();
        assert_eq!(id.as_str().len(), LOG_ID_LEN);
        assert!(id
            .as_str()
            .chars()
            .all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        assert_eq!(LogId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn two_generated_ids_differ() {
        // Not a statistical claim. This catches a generator wired to a
        // constant, which is the way this function actually breaks.
        let a = LogId::generate();
        let b = LogId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn bytes_render_low_nibble_last() {
        let id = LogId::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        assert_eq!(id.as_str(), "0123456789abcdeffedcba9876543210");
    }

    #[test]
    fn parse_rejects_the_wrong_length() {
        assert!(matches!(
            LogId::parse("abc"),
            Err(LogIdError::Length { found: 3, .. })
        ));
    }

    #[test]
    fn parse_rejects_uppercase_so_one_log_has_one_spelling() {
        let upper = "0123456789ABCDEFFEDCBA9876543210";
        assert!(matches!(
            LogId::parse(upper),
            Err(LogIdError::Character('A'))
        ));
    }

    #[test]
    fn parse_rejects_non_hex() {
        assert!(matches!(
            LogId::parse("0123456789abcdeffedcba987654321z"),
            Err(LogIdError::Character('z'))
        ));
    }
}

/// `748a185` `moirai-protocol/src/broadcast/message.rs` lines 157 to 253.
mod wire_overhead {
    use serde::{Deserialize, Serialize};

    use moirai_network::log_id::{LogId, LOG_ID_LEN};
    use moirai_network::message::EventMessage;
    use moirai_protocol::broadcast::internalizer::Interner;
    use moirai_protocol::broadcast::tcsb::{IsTcsb, Tcsb};

    /// The bytes `log_id` adds to a compact frame.
    const LOG_ID_WIRE_BYTES: usize = 1 + "\"log_id\"".len() + 1 + (LOG_ID_LEN + 2);

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Op(u32);

    /// An event as a replica broadcasts it: minted by a `Tcsb`, so the
    /// resolver, the version and the lamport are what a real frame carries.
    fn event_message() -> EventMessage<Op> {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&"a".into());
        interner.intern(&"b".into());
        let mut tcsb = Tcsb::new(idx, interner);
        let message = tcsb.send(Op(7));
        EventMessage::new(
            message.event().clone(),
            message.resolver().clone(),
            LogId::from_bytes([0x5a; 16]),
        )
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
        // is measured on the wire by `experiments/mp1-wire-overhead/run.sh`
        // in the vault.
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
