//! The node: hosts logs by id, routes each frame to the log it names, filters
//! frames for logs it does not host, and applies operations and batches
//! (NW.31 design row S5a). No operation type and no Moirai: the replica behind
//! every log is a [`ReplicaPort`](moirai_netstack::ReplicaPort).
//!
//! Catch-up and state transfer (row S5b) follow 601c887: a log with no
//! history asks one peer at a time for a state transfer, a log with history
//! asks for a delta on every new link and on resume.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, LogPayload, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_transport::transport::{CrdtTransport, PeerId, PeerStatus};
use serde::Serialize;
use serde_json::{Value, json};

/// 601c887's bound on one frame: a batch larger than this goes out one `Event`
/// per operation, so a peer that reads one frame per step is never handed a
/// body it cannot take in one.
const MAX_STATE_TRANSFER_BYTES: usize = 768 * 1024;

/// 601c887's wait on one donor before asking the next, and before a fresh
/// round once every connected peer has been asked.
const STATE_TRANSFER_RETRY: Duration = Duration::from_secs(2);

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

/// One hosted log: its replica, the remote input it refused and its side of
/// the state transfer.
struct Hosted<R> {
    replica: R,
    foreign_log_refusals: u64,
    /// The replica's reason for the last remote input it refused.
    last_refusal: Option<String>,
    transfer: TransferState,
    /// Per peer and replica id: the highest sequence already asked back for, so
    /// a lead that never arrives is not asked again.
    asked_back: BTreeMap<PeerId, BTreeMap<String, u64>>,
}

impl<R: ReplicaPort> Hosted<R> {
    fn new(replica: R) -> Self {
        Self {
            replica,
            foreign_log_refusals: 0,
            last_refusal: None,
            transfer: TransferState::default(),
            asked_back: BTreeMap::new(),
        }
    }

    fn has_no_history(&self) -> bool {
        self.replica.progress().delivered == 0
    }
}

/// One log's side of the state transfer, as 601c887's.
#[derive(Default)]
struct TransferState {
    /// When the last `StateRequest` went out.
    last_request: Option<Instant>,
    /// The peer asked and not yet answered.
    donor: Option<PeerId>,
    /// Peers asked in the current round.
    donors_tried: Vec<PeerId>,
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
    /// Event, Batch and SyncRequest frames with no usable log id.
    frames_malformed: u64,
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
            logs: BTreeMap::from([(default_log.clone(), Hosted::new(replica))]),
            default_log,
            transport,
            // Eight hex characters of a freshly minted id, as 601c887.
            incarnation: LogId::generate().as_str()[..8].to_string(),
            frames_not_hosted: 0,
            frames_malformed: 0,
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
        self.logs.insert(log_id, Hosted::new(replica));
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
            "frames_malformed": self.frames_malformed,
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
            "last_refusal": log.last_refusal,
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
            TransportMessage::Event { event } => match body_log_id(&event) {
                Some(log_id) => self.deliver(&from, &log_id, vec![event]),
                None => self.note_malformed(&from, "Event"),
            },
            TransportMessage::Batch { batch } => match body_log_id(&batch) {
                // A batch holds one replica payload per op (submit_batch) or one
                // ops_since answer; each goes to apply_remote as the replica made it.
                Some(log_id) => {
                    let payloads = match batch.get("events").cloned() {
                        Some(Value::Array(payloads)) => payloads,
                        other => vec![other.unwrap_or(Value::Null)],
                    };
                    self.deliver(&from, &log_id, payloads);
                }
                None => self.note_malformed(&from, "Batch"),
            },
            TransportMessage::SyncRequest { since } => match body_log_id(&since) {
                Some(log_id) => self.answer_sync(&from, &log_id, &since),
                None => self.note_malformed(&from, "SyncRequest"),
            },
            TransportMessage::StateRequest { id, log_id } => {
                let answer = if self.hosts(&log_id) {
                    self.state_answer(&id, &log_id)
                } else {
                    self.note_filtered(&log_id);
                    self.state_unavailable(&id, &log_id)
                };
                self.send(&from, answer);
            }
            TransportMessage::StateResponse {
                snapshot, log_id, ..
            } => {
                if self.hosts(&log_id) {
                    self.adopt(&from, &log_id, snapshot);
                } else {
                    self.note_filtered(&log_id);
                }
            }
            TransportMessage::StateUnavailable { reason, log_id } => {
                eprintln!(
                    "[{}] {from} will not serve a state transfer ({reason}); asking for a delta",
                    self.replica_id
                );
                // Stamped with the log the donor answered for; when this node
                // does not host it, every log waiting on `from` is freed.
                let waiting: Vec<LogId> = if self.hosts(&log_id) {
                    vec![log_id]
                } else {
                    self.logs
                        .iter()
                        .filter(|(_, log)| log.transfer.donor.as_ref() == Some(&from))
                        .map(|(id, _)| id.clone())
                        .collect()
                };
                for log_id in &waiting {
                    if let Some(log) = self.logs.get_mut(log_id)
                        && log.transfer.donor.as_ref() == Some(&from)
                    {
                        log.transfer.donor = None;
                    }
                    self.request_delta(log_id, &from);
                }
            }
            TransportMessage::Hello { id, .. } => self.request_sync(&id),
            _ => {}
        }
    }

    /// Apply the payloads of one frame from `from` in order to the log it
    /// names, stopping at the replica's first refusal; or filter it.
    fn deliver(&mut self, from: &str, log_id: &LogId, payloads: Vec<Value>) {
        let Some(log) = self.logs.get_mut(log_id) else {
            self.note_filtered(log_id);
            return;
        };
        for payload in payloads {
            if let Err(reason) = log.replica.apply_remote(from, payload) {
                refused(&self.replica_id, log_id, log, reason);
                return;
            }
        }
    }

    /// Answer a delta request with a Batch carrying the replica's `ops_since`,
    /// then ask the sender back for a lead its frontier shows and this replica
    /// lacks (388715b): the batch first so it catches up before the ask, once
    /// per lead, and never for a log with no history.
    fn answer_sync(&mut self, from: &PeerId, log_id: &LogId, since: &Value) {
        let Some(log) = self.logs.get_mut(log_id) else {
            self.note_filtered(log_id);
            return;
        };
        let frontier = since.get("frontier").cloned().unwrap_or(Value::Null);
        let ask_back = if log.has_no_history() {
            false
        } else {
            match log.replica.undelivered(&frontier) {
                Ok(leads) => {
                    let asked = log.asked_back.entry(from.clone()).or_default();
                    leads.into_iter().fold(false, |fresh, (id, seq)| {
                        let last = asked.entry(id).or_default();
                        if seq > *last {
                            *last = seq;
                            true
                        } else {
                            fresh
                        }
                    })
                }
                Err(reason) => {
                    eprintln!(
                        "[{}] log {log_id} cannot compare a frontier from {from}: {reason}",
                        self.replica_id
                    );
                    false
                }
            }
        };
        match log.replica.ops_since(frontier) {
            Ok(ops) => {
                let batch = json!({ "log_id": log_id.as_str(), "events": [ops] });
                self.send(from, TransportMessage::Batch { batch });
            }
            Err(reason) => refused(&self.replica_id, log_id, log, reason),
        }
        if ask_back {
            self.request_delta(log_id, from);
        }
    }

    /// The answer to a state request for a hosted log, in 601c887's order.
    fn state_answer(&mut self, requester: &str, log_id: &LogId) -> TransportMessage {
        let log = self
            .logs
            .get_mut(log_id)
            .expect("the caller checked the log is hosted");
        let unavailable = |reason: String| TransportMessage::StateUnavailable {
            reason,
            log_id: log_id.clone(),
        };
        if log.has_no_history() {
            return unavailable("this replica has no history to transfer".to_string());
        }
        // Adopting a snapshot replaces, so serving a returning member would
        // discard whatever it holds that the session never saw.
        if log.replica.has_history_from(requester) {
            return unavailable(format!(
                "`{requester}` is a returning member, not a fresh one; merging its \
                 history with a snapshot is not implemented"
            ));
        }
        match log.replica.snapshot_for(requester) {
            None => unavailable("a state transfer is not offered by this replica".to_string()),
            Some(Err(reason)) => {
                unavailable(format!("the replica refused a state transfer: {reason}"))
            }
            Some(Ok(snapshot)) => TransportMessage::StateResponse {
                snapshot,
                log: LogPayload::Plain(Value::Null),
                log_id: log_id.clone(),
            },
        }
    }

    /// Take a donor's snapshot while the log has no history, then ask the donor
    /// for a delta either way: a second donor's answer must not undo the first.
    fn adopt(&mut self, from: &PeerId, log_id: &LogId, snapshot: Value) {
        if let Some(log) = self.logs.get_mut(log_id)
            && log.has_no_history()
        {
            match log.replica.apply_remote(from, snapshot) {
                Ok(()) => eprintln!(
                    "[{}] adopted state from {from} for log {log_id}",
                    self.replica_id
                ),
                Err(reason) => refused(&self.replica_id, log_id, log, reason),
            }
        }
        self.request_delta(log_id, from);
    }

    /// Ask `peer` for a delta of every log that has history; a log with none
    /// waits for the transfer step.
    fn request_sync(&mut self, peer: &PeerId) {
        let wants_delta: Vec<LogId> = self
            .logs
            .iter()
            .filter(|(_, log)| !log.has_no_history())
            .map(|(id, _)| id.clone())
            .collect();
        for log_id in &wants_delta {
            self.request_delta(log_id, peer);
        }
    }

    /// Ask `peer` for the events of `log_id` above this replica's frontier.
    fn request_delta(&mut self, log_id: &LogId, peer: &PeerId) {
        let Some(log) = self.logs.get(log_id) else {
            return;
        };
        let since = json!({ "log_id": log_id.as_str(), "frontier": log.replica.frontier() });
        self.send(peer, TransportMessage::SyncRequest { since });
    }

    /// For every log with no history, ask one connected peer at a time for a
    /// state transfer, passing to the next after a refusal or
    /// [`STATE_TRANSFER_RETRY`] of silence (601c887's retry_state_transfer).
    fn retry_state_transfer(&mut self) {
        let mut peers: Option<Vec<PeerId>> = None;
        let now = Instant::now();
        let mut asks = Vec::new();
        for (log_id, log) in &mut self.logs {
            if !log.has_no_history() {
                log.transfer = TransferState::default();
                continue;
            }
            let peers = peers.get_or_insert_with(|| {
                self.transport
                    .peers()
                    .into_iter()
                    .filter(|p| p.status == PeerStatus::Connected)
                    .map(|p| p.id)
                    .collect()
            });
            if peers.is_empty() {
                break;
            }
            let transfer = &mut log.transfer;
            let waited = transfer
                .last_request
                .map_or(STATE_TRANSFER_RETRY, |last| now.duration_since(last));
            if transfer.donor.is_some() {
                if waited < STATE_TRANSFER_RETRY {
                    continue;
                }
                transfer.donor = None;
            }
            let next = match peers.iter().find(|p| !transfer.donors_tried.contains(p)) {
                Some(peer) => peer.clone(),
                None => {
                    if waited < STATE_TRANSFER_RETRY {
                        continue;
                    }
                    transfer.donors_tried.clear();
                    peers[0].clone()
                }
            };
            transfer.donors_tried.push(next.clone());
            transfer.donor = Some(next.clone());
            transfer.last_request = Some(now);
            asks.push((log_id.clone(), next));
        }
        for (log_id, peer) in asks {
            let id = self.replica_id.clone();
            self.send(&peer, TransportMessage::StateRequest { id, log_id });
        }
    }

    /// Count a frame for a log this node does not host, and remember its id.
    fn note_filtered(&mut self, log_id: &LogId) {
        self.frames_not_hosted += 1;
        self.seen_not_hosted.insert(log_id.clone());
    }

    /// Count a frame whose body names no usable log id.
    fn note_malformed(&mut self, from: &str, kind: &str) {
        self.frames_malformed += 1;
        eprintln!(
            "[{}] {kind} frame from {from} has no usable log_id; dropped",
            self.replica_id
        );
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage) {
        if let Err(e) = self.transport.send(peer, msg) {
            eprintln!("[{}] Failed to send to {peer}: {e}", self.replica_id);
        }
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

    /// Dial every known peer not yet connected, then ask each new link for what
    /// this node lacks.
    pub fn connect(&mut self) {
        match self.transport.connect_to_peers() {
            Ok(new_peers) => {
                for peer in new_peers {
                    self.request_sync(&peer);
                }
            }
            Err(e) => eprintln!("[{}] Some peer connections failed: {e}", self.replica_id),
        }
    }

    pub fn pause_peer(&mut self, peer: &str) -> OpResult {
        match self.transport.pause_peer(&peer.to_string()) {
            Ok(()) => OpResult {
                success: true,
                message: format!("Paused peer '{peer}'"),
            },
            Err(e) => OpResult {
                success: false,
                message: format!("Failed to pause '{peer}': {e}"),
            },
        }
    }

    /// Resume `peer`, handle what it buffered, then ask it for a delta.
    pub fn resume_peer(&mut self, peer: &str) -> OpResult {
        let peer = peer.to_string();
        if let Err(e) = self.transport.resume_peer(&peer) {
            return OpResult {
                success: false,
                message: format!("Failed to resume '{peer}': {e}"),
            };
        }
        let buffered = self.transport.drain_buffer(&peer);
        let count = buffered.len();
        for msg in buffered {
            self.handle_transport_message(peer.clone(), msg);
        }
        self.request_sync(&peer);
        OpResult {
            success: true,
            message: format!(
                "Resumed peer '{peer}', delivered {count} buffered msgs, requested sync"
            ),
        }
    }

    /// Accept new connections, receive every waiting frame and handle it, then
    /// run the transfer step. Dials nobody.
    pub fn poll(&mut self) -> usize {
        self.transport.accept_connections().ok();
        let mut handled = 0;
        while let Ok(Some((from, msg))) = self.transport.try_recv() {
            self.handle_transport_message(from, msg);
            handled += 1;
        }
        self.retry_state_transfer();
        handled
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

/// Count and keep the replica's refusal of remote input on `log`.
fn refused<R>(replica_id: &str, log_id: &LogId, log: &mut Hosted<R>, reason: String) {
    eprintln!("[{replica_id}] log {log_id} refused remote input: {reason}");
    log.foreign_log_refusals += 1;
    log.last_refusal = Some(reason);
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
