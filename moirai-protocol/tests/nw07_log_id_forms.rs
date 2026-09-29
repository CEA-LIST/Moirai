//! NW.07: a model id is `<32 lowercase hex>` or `<32 lowercase hex>_<name>`,
//! where the name is one to thirty-two ASCII letters and digits in the case
//! it was written in, and the whole string is the log's identity.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.07.md`. This file reads
//! only what exists before the change: `LogId::parse`, `as_str`, `Display`,
//! `Debug`, `Eq`, `Hash`, `Ord`, serde, and a `Tcsb` stamping and checking
//! the id on every frame. The minting half is `nw07_log_id_mint.rs`.
//!
//! The ground truth is written here by hand: every accepted and refused
//! spelling is a literal of this file, derived from the rule of the spec and
//! from nothing the implementation says.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-protocol --features serde --test nw07_log_id_forms -- --nocapture
//! ```

use std::collections::{BTreeSet, HashSet};

use moirai_protocol::log_id::{LOG_ID_LEN, LogId};

/// The hex part every fixture below is built on: bytes `01 23 45 67 89 ab cd
/// ef fe dc ba 98 76 54 32 10`, as `LogId::from_bytes` renders them.
const H: &str = "0123456789abcdeffedcba9876543210";

/// Thirty-two letters and digits, the longest name the rule allows.
const NAME_32: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef";

fn named(name: &str) -> String {
    format!("{H}_{name}")
}

/// Every spelling the rule accepts, with why.
fn accepted() -> Vec<(String, &'static str)> {
    vec![
        (H.to_string(), "the bare form, today's"),
        (named("SimpleUML"), "Cam's example, mixed case kept"),
        (
            named("simpleuml"),
            "SimpleUML's package as its descriptor declares it",
        ),
        (named("behaviortree"), "the behaviour tree's package"),
        (named("json"), "json's package"),
        (named("notation"), "notation's package"),
        (named("sysml"), "SysML's package"),
        (named("a"), "a one-character name"),
        (named("Z"), "an uppercase one-character name"),
        (named("7"), "a name that is one digit"),
        (named("2024Model"), "a name that starts with digits"),
        (named("SimpleUMLv20"), "letters and digits mixed"),
        (named(NAME_32), "a thirty-two character name, the longest"),
        (
            "ffffffffffffffffffffffffffffffff_Ff".to_string(),
            "another hex part",
        ),
        (
            "00000000000000000000000000000000_x".to_string(),
            "an all-zero hex part",
        ),
    ]
}

/// Every spelling the rule refuses, with why. The first six are refused
/// today and must stay refused.
fn refused() -> Vec<(String, &'static str)> {
    let upper_h = H.to_uppercase();
    vec![
        ("abc".to_string(), "too short, no name"),
        ("zz".to_string(), "too short, not hex"),
        (String::new(), "empty"),
        (upper_h.clone(), "uppercase hex, bare"),
        (
            format!("{}z", &H[..31]),
            "a non-hex character in the hex part",
        ),
        ("ab".repeat(32), "sixty-four hex characters"),
        (format!("{H}_"), "an empty name"),
        (format!("{H}_{NAME_32}g"), "a thirty-three character name"),
        (
            format!("{H}_{}", "a".repeat(64)),
            "a sixty-four character name",
        ),
        (format!("{upper_h}_x"), "uppercase hex with a name"),
        (
            format!("{}A_x", &H[..31]),
            "one uppercase hex digit with a name",
        ),
        (
            format!("{}_x", &H[..31]),
            "thirty-one hex characters with a name",
        ),
        (format!("{H}0_x"), "thirty-three hex characters with a name"),
        (format!("{H}-SimpleUML"), "a hyphen for the separator"),
        (format!("{H}SimpleUML"), "no separator"),
        (format!("{H}__x"), "two separators"),
        (format!("{H}_x_y"), "an underscore inside the name"),
        (format!("{H}_Simple-UML"), "a hyphen inside the name"),
        (format!("{H}_Simple UML"), "a space inside the name"),
        (format!("{H}_Simple.UML"), "a dot inside the name"),
        (format!("{H}_Simple/UML"), "a slash inside the name"),
        (format!("{H}_é"), "a non-ASCII letter"),
        (
            format!("{H}_Ünïcödé"),
            "non-ASCII letters around ASCII ones",
        ),
        (format!("{H}_日本"), "a name with no ASCII at all"),
        (format!("{H}_x "), "trailing whitespace"),
        (format!(" {H}_x"), "leading whitespace"),
        (format!("{H}_x\n"), "a trailing newline"),
        (format!("{H}_x\u{0}"), "a NUL after the name"),
        ("_SimpleUML".to_string(), "a name with no hex part"),
        ("SimpleUML".to_string(), "a name alone"),
        (format!("{H}_../x"), "a path inside the name"),
        (format!("{H}_x.json"), "a file name"),
    ]
}

#[test]
fn nw07_f1_parse_accepts_the_bare_form_and_the_named_form_and_keeps_every_character() {
    let mut failures = Vec::new();
    for (text, why) in accepted() {
        match LogId::parse(&text) {
            Ok(id) => {
                if id.as_str() != text {
                    failures.push(format!("{text:?} ({why}) parsed as {:?}", id.as_str()));
                }
                if id.to_string() != text {
                    failures.push(format!("{text:?} ({why}) displays as {:?}", id.to_string()));
                }
                let debug = format!("{id:?}");
                if !debug.contains(&text) {
                    failures.push(format!("{text:?} ({why}) debugs as {debug}, not in full"));
                }
            }
            Err(err) => failures.push(format!("{text:?} ({why}) refused: {err}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} spellings the rule accepts were not accepted as written:\n  {}",
        failures.len(),
        accepted().len(),
        failures.join("\n  ")
    );
}

#[test]
fn nw07_f2_parse_refuses_every_other_spelling() {
    let mut failures = Vec::new();
    for (text, why) in refused() {
        if let Ok(id) = LogId::parse(&text) {
            failures.push(format!("{text:?} ({why}) accepted as {:?}", id.as_str()));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} spellings the rule refuses were accepted:\n  {}",
        failures.len(),
        refused().len(),
        failures.join("\n  ")
    );
}

#[test]
fn nw07_f3_the_hex_part_is_thirty_two_characters_and_the_constant_says_so() {
    // `LOG_ID_LEN` is read by the wire-cost tests of `message.rs` and by the
    // I-E4 harness as the length of a bare id; it stays the hex part's length.
    assert_eq!(LOG_ID_LEN, 32);
    assert_eq!(H.len(), LOG_ID_LEN);
    let bare = LogId::from_bytes([
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ]);
    assert_eq!(bare.as_str(), H, "from_bytes still renders the bare form");
    assert_eq!(
        LogId::generate().as_str().len(),
        LOG_ID_LEN,
        "generate still mints the bare form; the named form is minted by name"
    );
}

#[test]
fn nw07_f4_the_whole_string_is_the_identity() {
    let spellings = [
        H.to_string(),
        named("SimpleUML"),
        named("simpleuml"),
        named("SIMPLEUML"),
        named("json"),
        named("Simpleuml"),
    ];
    let ids: Vec<LogId> = spellings
        .iter()
        .map(|text| {
            LogId::parse(text)
                .unwrap_or_else(|err| panic!("{text:?} must parse to compare it: {err}"))
        })
        .collect();

    let mut failures = Vec::new();
    for (i, a) in ids.iter().enumerate() {
        for (j, b) in ids.iter().enumerate() {
            if i != j && a == b {
                failures.push(format!("{:?} == {:?}", a.as_str(), b.as_str()));
            }
            if i != j && a.cmp(b) == std::cmp::Ordering::Equal {
                failures.push(format!("{:?} orders equal to {:?}", a.as_str(), b.as_str()));
            }
        }
    }
    let hashed: HashSet<LogId> = ids.iter().cloned().collect();
    let ordered: BTreeSet<LogId> = ids.iter().cloned().collect();
    if hashed.len() != ids.len() {
        failures.push(format!(
            "a HashSet holds {} of {} ids",
            hashed.len(),
            ids.len()
        ));
    }
    if ordered.len() != ids.len() {
        failures.push(format!(
            "a BTreeSet holds {} of {} ids",
            ordered.len(),
            ids.len()
        ));
    }
    // The same spelling twice is the same log.
    let again = LogId::parse(&named("SimpleUML")).expect("parsed above");
    if !hashed.contains(&again) || again != ids[1] {
        failures.push("the same named spelling parsed twice is not the same id".to_string());
    }
    assert!(
        failures.is_empty(),
        "two spellings that differ are two logs:\n  {}",
        failures.join("\n  ")
    );
}

#[cfg(not(feature = "serde"))]
#[test]
fn nw07_serde_feature_is_on() {
    panic!("NW.07's oracle runs with `--features serde`; without it the wire tests do not exist");
}

#[cfg(feature = "serde")]
mod wire {
    use moirai_protocol::broadcast::internalizer::Interner;
    use moirai_protocol::broadcast::message::EventMessage;
    use moirai_protocol::broadcast::tcsb::{IsTcsb, Tcsb};
    use moirai_protocol::log_id::LogId;
    use serde::{Deserialize, Serialize};

    use super::{H, NAME_32, named, refused};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Op(u32);

    fn id(text: &str) -> LogId {
        LogId::parse(text).unwrap_or_else(|err| panic!("{text:?} must parse: {err}"))
    }

    /// A log's causal bookkeeping for `me` among `members`, stamped with `log`.
    fn tcsb(me: &str, members: &[&str], log: LogId) -> Tcsb<Op> {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(me);
        for member in members {
            interner.intern(member);
        }
        Tcsb::new(idx, interner.into_shared(), log)
    }

    /// The compact JSON frame of one event from `a` on `log`.
    fn frame(log: LogId) -> String {
        let mut a = tcsb("a", &["a", "b"], log);
        serde_json::to_string(&a.send(Op(7))).expect("an event serializes")
    }

    /// The bytes the `log_id` member adds to a frame, read by removing it.
    fn log_id_bytes(frame: &str) -> (usize, String) {
        let mut value: serde_json::Value = serde_json::from_str(frame).expect("a frame is JSON");
        let removed = value
            .as_object_mut()
            .expect("a message is a JSON object")
            .remove("log_id")
            .expect("the frame carries a log_id member");
        let text = removed.as_str().expect("the id is a string").to_string();
        (frame.len() - value.to_string().len(), text)
    }

    #[test]
    fn nw07_w1_a_named_id_serializes_as_its_whole_text_and_reads_back() {
        let mut failures = Vec::new();
        for text in [
            H.to_string(),
            named("SimpleUML"),
            named("behaviortree"),
            named(NAME_32),
        ] {
            let parsed = id(&text);
            let json = serde_json::to_string(&parsed).expect("an id serializes");
            if json != format!("\"{text}\"") {
                failures.push(format!("{text:?} serialized as {json}"));
            }
            match serde_json::from_str::<LogId>(&json) {
                Ok(back) if back == parsed => {}
                Ok(back) => failures.push(format!("{text:?} read back as {:?}", back.as_str())),
                Err(err) => failures.push(format!("{text:?} did not read back: {err}")),
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn nw07_w2_a_frame_whose_id_breaks_the_rule_is_still_malformed() {
        let mut accepted = Vec::new();
        for (text, why) in refused() {
            let json = serde_json::to_string(&text).expect("a string serializes");
            if let Ok(back) = serde_json::from_str::<LogId>(&json) {
                accepted.push(format!("{text:?} ({why}) read as {:?}", back.as_str()));
            }
        }
        assert!(
            accepted.is_empty(),
            "a malformed id on the wire must be a malformed frame:\n  {}",
            accepted.join("\n  ")
        );
    }

    /// The wire cost of the named form, measured on real frames: the member
    /// is `,"log_id":"<id>"`, 12 bytes plus the id, so 44 bytes for a bare
    /// id and 44 + 1 + the name's length for a named one, whatever the
    /// payload. Prints `NW07-WIRE` lines for the record.
    #[test]
    fn nw07_w3_a_named_id_costs_its_name_and_one_separator_on_every_frame() {
        let cases = [
            ("bare", H.to_string(), 44usize),
            ("SimpleUML", named("simpleuml"), 44 + 1 + 9),
            ("behaviortree", named("behaviortree"), 44 + 1 + 12),
            ("json", named("json"), 44 + 1 + 4),
            ("notation", named("notation"), 44 + 1 + 8),
            ("sysml", named("sysml"), 44 + 1 + 5),
            ("longest", named(NAME_32), 44 + 1 + 32),
        ];
        let mut failures = Vec::new();
        for (label, text, want) in cases {
            let frame = frame(id(&text));
            if frame.matches("\"log_id\":\"").count() != 1 {
                failures.push(format!(
                    "{label}: the frame carries the id other than once: {frame}"
                ));
                continue;
            }
            let (cost, carried) = log_id_bytes(&frame);
            println!(
                "NW07-WIRE {label}: frame {} bytes, log_id member {cost} bytes (+{} over the bare 44)",
                frame.len(),
                cost as i64 - 44
            );
            if carried != text {
                failures.push(format!(
                    "{label}: the frame carries {carried:?}, not {text:?}"
                ));
            }
            if cost != want {
                failures.push(format!(
                    "{label}: the member costs {cost} bytes, not {want}"
                ));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// Identity on the wire: a replica of `<hex>_simpleuml` delivers that
    /// log's events and refuses, and counts, events stamped with the bare
    /// hex, another case of the name, or another name.
    #[test]
    fn nw07_w4_an_event_stamped_with_another_spelling_is_refused_as_another_log() {
        let ours = named("simpleuml");
        let mut failures = Vec::new();

        // The same log: delivered.
        let mut a = tcsb("a", &["a", "b"], id(&ours));
        let mut b = tcsb("b", &["a", "b"], id(&ours));
        let message: EventMessage<Op> = {
            let json = serde_json::to_string(&a.send(Op(1))).expect("serialize");
            serde_json::from_str(&json).expect("the frame reads back")
        };
        if message.log_id().as_str() != ours {
            failures.push(format!("the frame carries {:?}", message.log_id().as_str()));
        }
        b.receive(message);
        let mut delivered = 0;
        while b.next_causally_ready().is_some() {
            delivered += 1;
        }
        if delivered != 1 || b.foreign_log_refusals() != 0 {
            failures.push(format!(
                "{ours:?} to {ours:?}: delivered {delivered}, refusals {}",
                b.foreign_log_refusals()
            ));
        }

        // Another spelling: refused and counted, nothing delivered.
        for other in [H.to_string(), named("SimpleUML"), named("json")] {
            let mut sender = tcsb("a", &["a", "b"], id(&other));
            let mut receiver = tcsb("b", &["a", "b"], id(&ours));
            let before = receiver.stability();
            receiver.receive(sender.send(Op(2)));
            let arrived = receiver.next_causally_ready().is_some();
            if receiver.foreign_log_refusals() != 1 || arrived || receiver.stability() != before {
                failures.push(format!(
                    "{other:?} to {ours:?}: refusals {}, delivered {arrived}",
                    receiver.foreign_log_refusals()
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "the whole id is the log's identity on the wire:\n  {}",
            failures.join("\n  ")
        );
    }
}
