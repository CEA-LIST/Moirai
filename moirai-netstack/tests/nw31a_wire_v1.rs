//! NW.31a: wire v1 is 601c887's frames (NW.31 design, section 2.2).
//!
//! `fixtures/wire_v1/frames.jsonl` holds one frame per line, each the exact
//! text a 601c887 node writes for it (`serde_json::to_string` of its
//! `TransportMessage`), captured from moirai-model-plane 601c887; the
//! capture is described in `fixtures/wire_v1/README.md`. `refused.jsonl`
//! holds frames 601c887 refuses to decode, `unknown.jsonl` frames of a type
//! wire v1 does not name, and `deflated_log.json` the log the deflated state
//! response carries, as its donor exported it.

use moirai_netstack::{LogId, LogPayload, Opaque, TransportMessage};
use serde_json::Value;

const FRAMES: &str = include_str!("fixtures/wire_v1/frames.jsonl");
const REFUSED: &str = include_str!("fixtures/wire_v1/refused.jsonl");
const UNKNOWN: &str = include_str!("fixtures/wire_v1/unknown.jsonl");
const DEFLATED_LOG: &str = include_str!("fixtures/wire_v1/deflated_log.json");

/// The variant a frame decoded to, with the field types of design table 2.2:
/// the payloads the replica produces are opaque, the rest is typed.
fn kind(frame: &TransportMessage) -> &'static str {
    match frame {
        TransportMessage::Event { event } => {
            let _: &Opaque = event;
            "Event"
        }
        TransportMessage::Batch { batch } => {
            let _: &Opaque = batch;
            "Batch"
        }
        TransportMessage::SyncRequest { since } => {
            let _: &Opaque = since;
            "SyncRequest"
        }
        TransportMessage::StateRequest { id, log_id } => {
            let _: (&String, &LogId) = (id, log_id);
            "StateRequest"
        }
        TransportMessage::StateResponse {
            snapshot,
            log,
            log_id,
        } => {
            let _: (&Opaque, &LogPayload, &LogId) = (snapshot, log, log_id);
            "StateResponse"
        }
        TransportMessage::StateUnavailable { reason, log_id } => {
            let _: (&String, &LogId) = (reason, log_id);
            "StateUnavailable"
        }
        TransportMessage::Hello { id, metadata } => {
            let _: (&String, &Option<String>) = (id, metadata);
            "Hello"
        }
        TransportMessage::Ack { from } => {
            let _: &String = from;
            "Ack"
        }
        TransportMessage::Goodbye { id } => {
            let _: &String = id;
            "Goodbye"
        }
        TransportMessage::Chunk {
            id,
            index,
            count,
            data,
        } => {
            let _: (&u64, &u32, &u32, &String) = (id, index, count, data);
            "Chunk"
        }
        TransportMessage::Unknown(frame) => {
            let _: &Value = frame;
            "Unknown"
        }
    }
}

fn lines(text: &str) -> Vec<&str> {
    text.lines().filter(|line| !line.is_empty()).collect()
}

#[test]
fn every_601c887_frame_decodes_to_its_variant_and_re_encodes_byte_identical() {
    let frames = lines(FRAMES);
    let mut tags: Vec<String> = Vec::new();
    let mut failures = Vec::new();
    for (n, line) in frames.iter().enumerate() {
        let tag = serde_json::from_str::<Value>(line).expect("a fixture line is JSON")["type"]
            .as_str()
            .expect("a 601c887 frame has a type tag")
            .to_string();
        if !tags.contains(&tag) {
            tags.push(tag.clone());
        }
        match serde_json::from_str::<TransportMessage>(line) {
            Err(e) => failures.push(format!("frame {n} ({tag}) does not decode: {e}")),
            Ok(frame) => {
                if kind(&frame) != tag {
                    failures.push(format!("frame {n} ({tag}) decoded as {}", kind(&frame)));
                }
                let again = serde_json::to_string(&frame).expect("a frame encodes");
                if again != *line {
                    failures.push(format!(
                        "frame {n} ({tag}) re-encodes to other bytes:\n  601c887: {line}\n  now:     {again}"
                    ));
                }
            }
        }
    }
    tags.sort();
    assert_eq!(
        tags,
        [
            "Ack",
            "Batch",
            "Chunk",
            "Event",
            "Goodbye",
            "Hello",
            "StateRequest",
            "StateResponse",
            "StateUnavailable",
            "SyncRequest"
        ],
        "the fixture covers every frame type 601c887 sends"
    );
    assert!(
        failures.is_empty(),
        "{} of {} frames fail:\n{}",
        failures.len(),
        frames.len(),
        failures.join("\n")
    );
}

#[test]
fn every_frame_601c887_refuses_is_refused() {
    let accepted: Vec<&str> = lines(REFUSED)
        .into_iter()
        .filter(|line| serde_json::from_str::<TransportMessage>(line).is_ok())
        .collect();
    assert!(
        accepted.is_empty(),
        "frames with an invalid replica id or log id were accepted:\n{}",
        accepted.join("\n")
    );
}

#[test]
fn a_frame_of_a_type_wire_v1_does_not_name_passes_through_whole() {
    for line in lines(UNKNOWN) {
        let frame: TransportMessage = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("an unknown frame type is refused: {e}: {line}"));
        assert_eq!(kind(&frame), "Unknown", "{line}");
        assert_eq!(serde_json::to_string(&frame).unwrap(), line);
    }
}

#[test]
fn the_deflated_log_decodes_to_the_donors_export() {
    let line = lines(FRAMES)
        .into_iter()
        .find(|line| line.contains(r#""encoding":"Deflate""#))
        .expect("the fixture holds a deflated state response");
    let Ok(TransportMessage::StateResponse { log, .. }) = serde_json::from_str(line) else {
        panic!("the deflated state response does not decode as one");
    };
    let expected: Value = serde_json::from_str(DEFLATED_LOG).unwrap();
    assert_eq!(log.decode(), Some(expected));
}
