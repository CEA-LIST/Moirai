//! The node: hosts logs by id, routes each frame to the log it names, filters
//! frames for logs it does not host, and applies operations and batches
//! (NW.31 design row S5a). No operation type and no Moirai: the replica behind
//! every log is a [`ReplicaPort`](moirai_netstack::ReplicaPort).
//!
//! Catch-up, state transfer, a late joiner, a paused peer and a returning
//! member are S5b, not here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use moirai_netstack::{LogId, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_transport::transport::{CrdtTransport, PeerId};
use serde::Serialize;
use serde_json::{Value, json};

/// 601c887's bound on one frame: a batch larger than this goes out one `Event`
/// per operation, so a peer that reads one frame per step is never handed a
/// body it cannot take in one.
const MAX_STATE_TRANSFER_BYTES: usize = 768 * 1024;

/// The answer to one operation.
#[derive(Debug, Clone, Serialize)]
pub struct OpResult {
    pub success: bool,
    pub message: String,
}

/// What became of a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchOutcome {
    Applied,
    Stale,
    Refused,
    NotHosted,
}

/// The answer to one batch.
#[derive(Debug, Clone, Serialize)]
pub struct BatchResult {
    pub outcome: BatchOutcome,
    /// How many operations were applied, counted from the first.
    pub applied: usize,
    /// How many operations the batch held.
    pub of: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused_at: Option<usize>,
    pub message: String,
    /// The log's version once the intake step is over; `None` when not hosted.
    pub version: Option<String>,
}

/// Why a log could not be hosted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    AlreadyHosted(LogId),
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyHosted(log_id) => write!(f, "log {log_id} is already hosted here"),
        }
    }
}

impl std::error::Error for HostError {}

/// One hosted log: its replica and the count of remote input it refused.
struct Hosted<R> {
    replica: R,
    foreign_log_refusals: u64,
}

/// A node hosting logs by id over one transport.
pub struct Node<F: ReplicaFactory, T: CrdtTransport> {
    factory: F,
    replica_id: String,
    logs: BTreeMap<LogId, Hosted<F::Replica>>,
    default_log: LogId,
    transport: T,
    incarnation: String,
    frames_not_hosted: u64,
    seen_not_hosted: BTreeSet<LogId>,
}

impl<F: ReplicaFactory, T: CrdtTransport> Node<F, T> {
    /// Open a node whose default log is `default_log`, opened from `factory`.
    pub fn new(
        mut factory: F,
        replica_id: String,
        members: &[&str],
        transport: T,
        default_log: LogId,
    ) -> Self {
        let members: Vec<String> = members.iter().map(|m| m.to_string()).collect();
        let replica = factory.open(default_log.as_str(), &replica_id, &members);
        Self {
            factory,
            replica_id,
            logs: BTreeMap::from([(
                default_log.clone(),
                Hosted {
                    replica,
                    foreign_log_refusals: 0,
                },
            )]),
            default_log,
            transport,
            // Eight hex characters of a freshly minted id, as 601c887.
            incarnation: LogId::generate().as_str()[..8].to_string(),
            frames_not_hosted: 0,
            seen_not_hosted: BTreeSet::new(),
        }
    }

    /// Host one more log, with no history. A node hosts a log only through this
    /// call, never because a frame for the id arrived.
    pub fn host_log(&mut self, log_id: LogId) -> Result<(), HostError> {
        if self.logs.contains_key(&log_id) {
            return Err(HostError::AlreadyHosted(log_id));
        }
        let members = [self.replica_id.clone()];
        let replica = self
            .factory
            .open(log_id.as_str(), &self.replica_id, &members);
        self.seen_not_hosted.remove(&log_id);
        self.logs.insert(
            log_id,
            Hosted {
                replica,
                foreign_log_refusals: 0,
            },
        );
        Ok(())
    }

    pub fn hosts(&self, log_id: &LogId) -> bool {
        self.logs.contains_key(log_id)
    }

    /// The replica of the hosted log named `log_id`.
    pub fn hosted(&self, log_id: &LogId) -> Option<&F::Replica> {
        self.logs.get(log_id).map(|log| &log.replica)
    }

    /// The ids of every log this node hosts, in id order.
    pub fn hosted_logs(&self) -> impl Iterator<Item = &LogId> {
        self.logs.keys()
    }

    /// The default log: the one the unscoped calls name.
    pub fn log_id(&self) -> &LogId {
        &self.default_log
    }

    /// Frames dropped because no hosted log carried their id.
    pub fn frames_not_hosted(&self) -> u64 {
        self.frames_not_hosted
    }

    /// The ids of logs this node does not host but has seen traffic for.
    pub fn seen_not_hosted(&self) -> impl Iterator<Item = &LogId> {
        self.seen_not_hosted.iter()
    }

    /// The logs this node hosts and those it has seen, for `GET /api/models`.
    pub fn models(&self) -> Value {
        let seen: Vec<&str> = self.seen_not_hosted.iter().map(LogId::as_str).collect();
        json!({ "seen": seen })
    }

    /// The version of `log_id` as this node holds it now:
    /// `<incarnation>.<delivered>`, or `None` when it does not host the log.
    pub fn version_of(&self, log_id: &LogId) -> Option<String> {
        let log = self.logs.get(log_id)?;
        Some(format!(
            "{}.{}",
            self.incarnation,
            log.replica.progress().delivered
        ))
    }

    /// The node's counters. `foreign_log_refusals` is the default log's.
    pub fn metrics(&self) -> Value {
        json!({
            "frames_not_hosted": self.frames_not_hosted,
            "hosted_logs": self.logs.len(),
            "foreign_log_refusals": self
                .logs
                .get(&self.default_log)
                .map_or(0, |log| log.foreign_log_refusals),
        })
    }

    /// One hosted log's counters; `None` when it is not hosted.
    pub fn log_metrics(&self, log_id: &LogId) -> Option<Value> {
        let log = self.logs.get(log_id)?;
        Some(json!({
            "log_id": log_id.as_str(),
            "foreign_log_refusals": log.foreign_log_refusals,
        }))
    }

    pub fn apply_op(&mut self, op: Value) -> OpResult {
        let log_id = self.default_log.clone();
        self.apply_op_to(&log_id, op)
    }

    /// Apply one operation to `log_id` and broadcast the event it became.
    pub fn apply_op_to(&mut self, log_id: &LogId, op: Value) -> OpResult {
        match self.apply_local(log_id, op) {
            Ok(event) => {
                self.broadcast(TransportMessage::Event { event });
                OpResult {
                    success: true,
                    message: "Applied and broadcasted".to_string(),
                }
            }
            Err(message) => OpResult {
                success: false,
                message,
            },
        }
    }

    /// The adapter intake for one operation; `None` names the default log.
    pub fn submit_op(&mut self, log_id: Option<LogId>, op: Value) -> OpResult {
        let log_id = log_id.unwrap_or_else(|| self.default_log.clone());
        self.apply_op_to(&log_id, op)
    }

    /// Apply the operations one edit produced in one step, against `base`, the
    /// version the batch was computed at. `None` names the default log.
    pub fn submit_batch(
        &mut self,
        log_id: Option<LogId>,
        ops: Vec<Value>,
        base: Option<&str>,
    ) -> BatchResult {
        let log_id = log_id.unwrap_or_else(|| self.default_log.clone());
        let of = ops.len();
        let answer = |outcome, applied, refused_at, message: String, version| BatchResult {
            outcome,
            applied,
            of,
            refused_at,
            message,
            version,
        };
        let Some(version) = self.version_of(&log_id) else {
            return answer(
                BatchOutcome::NotHosted,
                0,
                None,
                format!("log {log_id} is not hosted here"),
                None,
            );
        };
        if let Some(base) = base
            && base != version
        {
            return answer(
                BatchOutcome::Stale,
                0,
                None,
                format!(
                    "the model moved since the read this batch was computed against: it \
                     was at {base} and is at {version}; nothing was applied"
                ),
                Some(version),
            );
        }
        let log = self
            .logs
            .get_mut(&log_id)
            .expect("version_of answered for this log");
        let (applied, refusal, events) = log.replica.submit_batch(ops);
        let events = events
            .into_iter()
            .map(|event| stamp(event, &log_id))
            .collect();
        self.broadcast_batch(&log_id, events);
        let version = self.version_of(&log_id);
        match refusal {
            None => answer(
                BatchOutcome::Applied,
                applied,
                None,
                format!("{of} operations applied in one step"),
                version,
            ),
            Some(reason) => answer(
                BatchOutcome::Refused,
                applied,
                Some(applied),
                format!(
                    "operation {} of {of} refused after {applied} applied: {reason}",
                    applied + 1
                ),
                version,
            ),
        }
    }

    /// Apply one operation to `log_id` and hand back the event it became, with
    /// the log id the library owns added to it; or the reason it was refused.
    fn apply_local(&mut self, log_id: &LogId, op: Value) -> Result<Value, String> {
        let Some(log) = self.logs.get_mut(log_id) else {
            return Err(format!("log {log_id} is not hosted here"));
        };
        let event = log.replica.submit(op)?;
        Ok(stamp(event, log_id))
    }

    /// Broadcast the events one batch became: one frame when it fits, one event
    /// per operation when it does not.
    fn broadcast_batch(&mut self, log_id: &LogId, mut events: Vec<Value>) {
        if events.is_empty() {
            return;
        }
        if events.len() == 1 {
            let event = events.pop().expect("one event");
            self.broadcast(TransportMessage::Event { event });
            return;
        }
        let frame = TransportMessage::Batch {
            batch: json!({ "log_id": log_id.as_str(), "events": events }),
        };
        let fits =
            serde_json::to_vec(&frame).is_ok_and(|bytes| bytes.len() <= MAX_STATE_TRANSFER_BYTES);
        if fits {
            self.broadcast(frame);
        } else if let TransportMessage::Batch { batch } = frame
            && let Some(Value::Array(events)) = batch.get("events").cloned()
        {
            for event in events {
                self.broadcast(TransportMessage::Event { event });
            }
        }
    }

    fn broadcast(&mut self, msg: TransportMessage) {
        if let Err(e) = self.transport.broadcast(msg) {
            eprintln!("[{}] Broadcast failed: {e}", self.replica_id);
        }
    }

    /// Hand an inbound frame to the log whose id it carries, or filter it.
    pub fn handle_transport_message(&mut self, from: PeerId, msg: TransportMessage) {
        match msg {
            TransportMessage::Event { event } => {
                if let Some(log_id) = body_log_id(&event) {
                    self.deliver(&from, &log_id, event);
                }
            }
            TransportMessage::Batch { batch } => {
                if let Some(log_id) = body_log_id(&batch) {
                    let events = batch.get("events").cloned().unwrap_or(Value::Null);
                    self.deliver(&from, &log_id, events);
                }
            }
            TransportMessage::SyncRequest { since } => {
                if let Some(log_id) = body_log_id(&since)
                    && !self.hosts(&log_id)
                {
                    self.note_filtered(&log_id);
                }
            }
            TransportMessage::StateRequest { id, log_id } => {
                if !self.hosts(&log_id) {
                    self.note_filtered(&log_id);
                    let answer = self.state_unavailable(&id, &log_id);
                    if let Err(e) = self.transport.send(&from, answer) {
                        eprintln!(
                            "[{}] Failed to answer the state request from {from}: {e}",
                            self.replica_id
                        );
                    }
                }
            }
            TransportMessage::StateResponse { log_id, .. } if !self.hosts(&log_id) => {
                self.note_filtered(&log_id);
            }
            _ => {}
        }
    }

    /// Apply one event or one batch from `from` to the log it names, or filter
    /// it and remember its id.
    fn deliver(&mut self, from: &str, log_id: &LogId, ops: Value) {
        match self.logs.get_mut(log_id) {
            Some(log) => {
                if log.replica.apply_remote(from, ops).is_err() {
                    log.foreign_log_refusals += 1;
                }
            }
            None => self.note_filtered(log_id),
        }
    }

    /// Count a frame for a log this node does not host, and remember its id.
    fn note_filtered(&mut self, log_id: &LogId) {
        self.frames_not_hosted += 1;
        self.seen_not_hosted.insert(log_id.clone());
    }

    /// The refusal a state request for a log this node does not host gets: the
    /// default log is the one a donor stamps, and the reason names both ids.
    fn state_unavailable(&self, requester: &str, log_id: &LogId) -> TransportMessage {
        let hosted: Vec<&str> = self.logs.keys().map(LogId::as_str).collect();
        TransportMessage::StateUnavailable {
            reason: format!(
                "`{requester}` asked for log {log_id}, but this replica does not host it \
                 (it hosts {}); a state transfer never crosses logs",
                hosted.join(", ")
            ),
            log_id: self.default_log.clone(),
        }
    }

    /// Accept new connections, then receive every waiting frame and handle it.
    pub fn poll(&mut self) -> usize {
        self.transport.accept_connections().ok();
        let mut handled = 0;
        while let Ok(Some((from, msg))) = self.transport.try_recv() {
            self.handle_transport_message(from, msg);
            handled += 1;
        }
        handled
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

/// The one key the library owns in an opaque body: the log the frame is for.
fn stamp(mut body: Value, log_id: &LogId) -> Value {
    if let Value::Object(fields) = &mut body {
        fields.insert("log_id".to_string(), json!(log_id.as_str()));
    }
    body
}

/// The log a frame names, read from the key the library owns in its body.
fn body_log_id(body: &Value) -> Option<LogId> {
    LogId::parse(body.get("log_id")?.as_str()?).ok()
}
