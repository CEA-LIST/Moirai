//! SI.13 oracle, the part its test files share: the contract the tests read,
//! the successor rule computed from a schedule, the migration status as the
//! contract fixes it, and, in two submodules, the relay cluster the node
//! tests start and SI.12's model generator.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.13.md`. SI.13 keeps SI.12's
//! contract (M1 to M7, restated in `si12/mod.rs`) and adds, restated here so
//! that a reader of the tests does not need the spec open:
//!
//! - M8, the migrate answer (OB6). `POST /api/model/{m}/migrate` answers
//!   within [`ANSWER_BOUND`] (6 s: `REPLY_TIMEOUT`, 5 s, plus one) of the
//!   request. A 2xx (201 or 202) names the successor `m'` under `model_id`
//!   and is sent only once the answering node has delivered its own cut: its
//!   migration status names `m'` (or, by M11, a cut that supersedes it) from
//!   the first read after the answer, and the node completes `m'` (M9)
//!   without a further request. A 504 is answered only for a migration the
//!   event loop never took, and the node then never cuts `m` nor opens a
//!   model for it. 409 and 422 stay SI.12's refusals.
//! - M9, progress. From its 2xx on, the answering node's
//!   `GET /api/model/{m}/migration` carries `"copy": {"applied": n, "of": N}`:
//!   `N >= 1` the operations of the successor's plan, `n` those this node has
//!   applied to `m'`, `0 <= n <= N`, never smaller than at the read before,
//!   reaching `N`; once `n == N` the node serves `m'` whole and its
//!   `GET /api/model/{m'}/metrics` reports `ops_applied` `N`. Every read of
//!   that status from the answer until `n == N` answers 200 within
//!   [`ANSWER_BOUND`]. A 201 says the copy is whole at the answer: the first
//!   read shows `n == N`. A node that did not plan the copy reads no `copy`
//!   key, or `null`.
//! - M10, frames. A relay reads no frame longer than its `MAX_FRAME_BYTES`
//!   (1 MiB): its log holds no "frame exceeds" and no "read error" line, and
//!   no node's relay session closes, while a node that reaches its peers only
//!   through it delivers the cut, the copy and every catch-up.
//! - M11, the successor rule (OB2). A replica that has delivered more than
//!   one cut of `m` keeps the cut with the greatest Lamport stamp (the sum of
//!   the cut event's version: the events of `m` in its causal past, itself
//!   included); on equal stamps, the cut whose origin replica id is least,
//!   compared as bytes; then the least sequence number. It names that cut's
//!   successor under `migrated_to`, serves that cut's documents (M4),
//!   refuses local operations and a second migration naming it (M3), lists
//!   under `refused` every event of `m` it has delivered that this cut had
//!   not seen (M5; the other cuts are among them), and lists under
//!   `superseded` the successors of every other cut it has delivered, in any
//!   order, `[]` with one cut. The rule reads the cuts alone, so replicas that
//!   delivered the same cuts name the same successor whatever the order.
//! - M12, joiners (OB3). A replica that joins `m` after the cut, by state
//!   transfer or by delta sync, serves within the convergence bound what a
//!   replica that delivered the cuts serves: the same successor and the same
//!   superseded copies, the frozen documents, and a refusal of a local
//!   operation and of a second migration naming the successor. A replica that
//!   joins `m'` once the others agree on it serves `m'` equal to theirs,
//!   through a relay included.
//! - M13, a busy node. While the event loop is busy for longer than
//!   `REPLY_TIMEOUT`, a command the HTTP API sends it that writes (a
//!   migration, a model registration, a descriptor, a pause) is answered 504
//!   only when the loop never carries it out; that is what `REPLY_TIMEOUT`'s
//!   doc comment says of every command but an operation or a batch.
#![allow(dead_code)]

#[path = "../si12/mod.rs"]
pub mod si12;

#[path = "../nw02/launch.rs"]
pub mod launch;

pub mod cluster;
pub mod generated;

use std::time::Duration;

use serde_json::{Value, json};

/// M8: `REPLY_TIMEOUT` (5 s, `moirai-network/src/http_api.rs`) plus one
/// second for the answer to reach the client.
pub const ANSWER_BOUND: Duration = Duration::from_secs(6);

/// A cut as the oracle knows it from the schedule it ran: who wrote it, its
/// sequence number, how many events of `m` its causal past holds (itself
/// included, which is its Lamport stamp), and the successor it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CutInfo {
    pub origin: String,
    pub seq: u64,
    pub stamp: u64,
    pub to: String,
}

/// M11, computed by the oracle from its own schedule: the greatest stamp,
/// then the least origin by bytes, then the least sequence number.
pub fn winner(cuts: &[CutInfo]) -> CutInfo {
    cuts.iter()
        .max_by(|x, y| {
            x.stamp
                .cmp(&y.stamp)
                .then_with(|| y.origin.as_bytes().cmp(x.origin.as_bytes()))
                .then_with(|| y.seq.cmp(&x.seq))
        })
        .cloned()
        .expect("the rule is asked about at least one cut")
}

/// The three keys of a migration status SI.13 reads: `migrated_to`,
/// `refused` sorted by origin and sequence number, and `superseded` sorted.
/// A status that names no successor may leave `superseded` out (read as
/// `[]`); one that names a successor must carry it.
pub fn status13(status: &Value) -> Value {
    let mut refused: Vec<(String, u64)> = status
        .get("refused")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    (
                        item.get("origin")
                            .and_then(Value::as_str)
                            .unwrap_or("<no origin>")
                            .to_string(),
                        item.get("seq").and_then(Value::as_u64).unwrap_or(u64::MAX),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    refused.sort();
    let migrated_to = status
        .get("migrated_to")
        .cloned()
        .unwrap_or(Value::String("<no migrated_to key>".into()));
    let superseded = match status.get("superseded") {
        Some(Value::Array(items)) => {
            let mut names: Vec<String> = items
                .iter()
                .map(|item| match item {
                    Value::String(name) => name.clone(),
                    other => format!("<not a model id: {other}>"),
                })
                .collect();
            names.sort();
            json!(names)
        }
        None if migrated_to.is_null() => json!([]),
        None => json!("<no superseded key>"),
        Some(other) => json!(format!("<superseded is not a list: {other}>")),
    };
    json!({
        "migrated_to": migrated_to,
        "refused": refused
            .into_iter()
            .map(|(origin, seq)| json!({"origin": origin, "seq": seq}))
            .collect::<Vec<_>>(),
        "superseded": superseded,
    })
}

/// The status M2, M5 and M11 fix for a replica that names `to` (or none),
/// lists `refused` and supersedes `superseded`.
pub fn expected13(to: Option<&str>, refused: &[(String, u64)], superseded: &[String]) -> Value {
    let mut refused = refused.to_vec();
    refused.sort();
    let mut superseded = superseded.to_vec();
    superseded.sort();
    json!({
        "migrated_to": to,
        "refused": refused
            .into_iter()
            .map(|(origin, seq)| json!({"origin": origin, "seq": seq}))
            .collect::<Vec<_>>(),
        "superseded": superseded,
    })
}

/// M9's progress key, read off a raw status: `(applied, of)` when it is a
/// well-formed `{"applied": n, "of": N}`, an error naming what was read
/// otherwise.
pub fn copy_progress(status: &Value) -> Result<(u64, u64), String> {
    match status.get("copy") {
        Some(Value::Object(copy)) => {
            let applied = copy.get("applied").and_then(Value::as_u64);
            let of = copy.get("of").and_then(Value::as_u64);
            match (applied, of) {
                (Some(applied), Some(of)) if of >= 1 && applied <= of => Ok((applied, of)),
                _ => Err(format!(
                    "the status's `copy` is not {{\"applied\": n, \"of\": N}} with 0 <= n <= N and \
                     N >= 1: {status}"
                )),
            }
        }
        Some(other) => Err(format!("the status's `copy` is {other}: {status}")),
        None => Err(format!(
            "the status carries no `copy` key reporting the copy's progress (M9): {status}"
        )),
    }
}

/// The node that did not plan the copy reads no progress (M9).
pub fn no_copy(status: &Value) -> Result<(), String> {
    match status.get("copy") {
        None | Some(Value::Null) => Ok(()),
        Some(other) => Err(format!(
            "a node that planned no copy reports `copy` {other}: {status}"
        )),
    }
}

/// Every event `(origin, seq)` an origin wrote, from 1 to `last`.
pub fn events(origin: &str, last: u64) -> Vec<(String, u64)> {
    (1..=last).map(|seq| (origin.to_string(), seq)).collect()
}

/// `delivered` less every event in `seen`.
pub fn unseen(delivered: &[(String, u64)], seen: &[(String, u64)]) -> Vec<(String, u64)> {
    let mut out: Vec<(String, u64)> = delivered
        .iter()
        .filter(|event| !seen.contains(event))
        .cloned()
        .collect();
    out.sort();
    out.dedup();
    out
}
