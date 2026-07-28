//! Generic network node for any Arachne-generated (or manually-written) CRDT.
//!
//! This module provides [`GenericNode`], which is parameterised with a single
//! log type `L` that implements [`IsLog`].
//!
//! The operation type `L::Op` must satisfy the transport bounds:
//! `Serialize + DeserializeOwned + Clone + Debug + Send + InternalizeOp + 'static`
//!

use std::fmt::{Debug, Display};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;
use moirai_protocol::utils::intern_str::InternalizeOp;

use crate::discovery::{Discovery, DiscoveryConfig};
use crate::query::QueryableLog;
use crate::state_transfer::TransferableLog;
use crate::tcp_transport::TcpTransport;
use crate::transport::{CrdtTransport, PeerId, TransportMessage};
use crate::HashMap;

/// Convenience alias
pub type TcpNode<L> = GenericNode<L, TcpTransport<<L as IsLog>::Op>>;

/// How often a replica that still has no history re-asks its peers for one.
///
/// Asking once, on connect, is not enough, and not because the network is
/// unreliable. A peer that has written nothing yet has nothing to transfer and
/// says so; the replicas of a session that starts together are all in that
/// state until the first operation is applied, and the one that arrives second
/// must be able to ask again afterwards. It also covers T5 — a donor that dies
/// mid-transfer — without any special handling: the next round simply reaches a
/// different peer.
///
/// Same reasoning as P1-D9, where the node re-dials on every roster rather than
/// only on a changed one: a first attempt can legitimately fail, and gating the
/// retry on an event that will not recur reintroduces ask-once.
const STATE_TRANSFER_RETRY: Duration = Duration::from_secs(2);

// Alias for these bounds, needed to transport operations over the network (e.g. via HTTP API).
pub trait NetworkOp:
    Serialize + DeserializeOwned + Clone + Debug + Send + InternalizeOp + 'static
{
}

impl<T> NetworkOp for T where
    T: Serialize + DeserializeOwned + Clone + Debug + Send + InternalizeOp + 'static
{
}

// =============================================================================
// GenericNode — one replica + transport + external adapters
// =============================================================================

/// A generic network node.
///
/// `L` is the CRDT **log** type (e.g. `BehaviortreeLog`).
/// `T` is the transport backend (e.g. [`TcpTransport`]
///
/// The [`TcpNode`] type alias is for TCP.
pub struct GenericNode<L: IsLog, T: CrdtTransport<Op = L::Op>>
where
    L::Op: NetworkOp,
{
    replica_id: String,
    replica: Replica<L, Tcsb<L::Op>>,
    transport: T,
    adapter_op_rx: Receiver<OpEnvelope<L::Op>>,
    adapter_op_tx: Sender<OpEnvelope<L::Op>>,
    ctrl_rx: Receiver<ControlCmd>,
    ctrl_tx: Sender<ControlCmd>,

    /// Optional callback to query the CRDT state as JSON.
    /// Set by `enable_state_query()` when `L: QueryableLog`.
    query_fn: Option<fn(&Replica<L, Tcsb<L::Op>>) -> serde_json::Value>,
    /// Log of all operations delivered to this replica (for operation log endpoint)
    operation_log: Vec<L::Op>,
    /// Operations *originated* here and accepted by the CRDT.
    ///
    /// Separate from `operation_log`, which counts deliveries and over-counts
    /// remote operations while both peers dial each other. This one is exact,
    /// so `/api/metrics` can be trusted as a test oracle.
    local_ops: usize,
    /// Bootnode poller, when `BOOTNODE_URL` was configured.
    ///
    /// `None` is the pre-phase-1 behaviour in full: peers come from `PEERS`,
    /// are dialled once, and nothing is ever discovered.
    discovery: Option<Discovery>,
    /// Serialise / rebuild the CRDT log, set by `enable_state_transfer()` when
    /// `L: TransferableLog`.
    ///
    /// `None` means this replica can neither serve nor accept a state
    /// transfer, and both paths degrade to `SyncRequest`. Same shape, and same
    /// reason, as `query_fn`: a hand-written log that is not serialisable must
    /// keep working.
    export_log: Option<fn(&L) -> serde_json::Value>,
    import_log: Option<fn(serde_json::Value) -> Option<L>>,
    /// When the last round of `StateRequest`s went out. See
    /// [`STATE_TRANSFER_RETRY`].
    last_state_request: Option<Instant>,
}

/// Envelope for ops submitted, with a oneshot reply channel.
pub struct OpEnvelope<O> {
    pub op: O,
    pub reply: Sender<OpResult>,
}

/// Control commands sent by external adapters (for example, HTTP).
pub(crate) enum ControlCmd {
    Pause {
        peer_id: String,
        reply: Sender<OpResult>,
    },
    Resume {
        peer_id: String,
        reply: Sender<OpResult>,
    },
    PauseAll {
        reply: Sender<OpResult>,
    },
    ResumeAll {
        reply: Sender<OpResult>,
    },
    Peers {
        reply: Sender<serde_json::Value>,
    },
    Query {
        reply: Sender<serde_json::Value>,
    },
    Operations {
        reply: Sender<serde_json::Value>,
    },
    Metrics {
        reply: Sender<serde_json::Value>,
    },
    Leave {
        reply: Sender<OpResult>,
    },
}

/// Result sent back to HTTP callers.
#[derive(Debug, Clone, Serialize)]
pub struct OpResult {
    pub success: bool,
    pub message: String,
}

impl<L: IsLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
where
    L::Op: NetworkOp,
{
    fn unit_result_to_op_result<E: Display>(
        result: Result<(), E>,
        success_message: String,
        error_prefix: String,
    ) -> OpResult {
        match result {
            Ok(()) => OpResult {
                success: true,
                message: success_message,
            },
            Err(e) => OpResult {
                success: false,
                message: format!("{}: {}", error_prefix, e),
            },
        }
    }

    /// Create a node with the given transport.
    /// Transport-agnostic creator
    pub fn with_transport(replica_id: String, members: &[&str], transport: T) -> Self {
        let replica: Replica<L, Tcsb<L::Op>> = IsReplica::bootstrap(replica_id.clone(), members);

        let (adapter_op_tx, adapter_op_rx) = mpsc::channel();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();

        Self {
            replica_id,
            replica,
            transport,
            adapter_op_rx,
            adapter_op_tx,
            ctrl_rx,
            ctrl_tx,
            query_fn: None,
            operation_log: Vec::new(),
            local_ops: 0,
            discovery: None,
            export_log: None,
            import_log: None,
            last_state_request: None,
        }
    }

    /// Start discovering peers through a bootnode.
    ///
    /// Purely additive: not calling this leaves the node exactly as it was
    /// before phase 1. `PEERS` keeps working either way and takes effect
    /// immediately, so a static list and a discovered one compose — the static
    /// entries are simply already in the address book when the first roster
    /// arrives.
    pub fn enable_discovery(&mut self, config: DiscoveryConfig) {
        eprintln!(
            "[{}] discovery on: {} session `{}` as {} every {:?}",
            self.replica_id,
            config.bootnode_url,
            config.session,
            config.advertise_addr,
            config.interval
        );
        self.discovery = Some(Discovery::spawn(config));
    }

    /// Get a sender that can be used to submit ops from other threads.
    pub fn op_sender(&self) -> Sender<OpEnvelope<L::Op>> {
        self.adapter_op_tx.clone()
    }

    /// Start the optional HTTP API on the given port.
    ///
    /// The HTTP implementation lives in `http_api.rs` and communicates with the
    /// generic node through channels.
    pub fn start_http(&self, port: u16) {
        crate::http_api::start_http_api::<L::Op>(
            port,
            self.replica_id.clone(),
            self.adapter_op_tx.clone(),
            self.ctrl_tx.clone(),
        );
    }

    /// Ask `peer` for everything this replica has not seen yet.
    ///
    /// The single place that turns "we have a link to `peer`" into a history
    /// pull. Every path that needs one — an accepted `Hello`, a resumed peer, a
    /// freshly dialled peer — funnels through here, so the four copies of
    /// `since()` -> `SyncRequest` -> `send()` that used to exist cannot drift
    /// apart.
    fn request_sync(&mut self, peer: &PeerId) {
        // A replica that has been in the session asks for the delta, which is
        // what its peers can actually answer from their outboxes. A replica
        // that has not needs the compacted state as well, and asking for a
        // delta would get it a correct answer to the wrong question: an empty
        // batch from a healthy peer, because everything it needs has already
        // been pruned.
        if self.import_log.is_some() && self.has_no_history() {
            self.request_state_transfer(peer);
        } else {
            self.request_delta_sync(peer);
        }
    }

    /// Ask `peer` for the events it holds above what this replica has already
    /// delivered.
    ///
    /// Answered out of the peer's outbox, which `prune_outbox` keeps to exactly
    /// the events above its stable frontier. That is the right question for a
    /// replica that has been in the session; it is the wrong one for a replica
    /// that has not, which is what `StateRequest` is for.
    fn request_delta_sync(&mut self, peer: &PeerId) {
        let since = self.replica.since();
        let msg = TransportMessage::SyncRequest { since };
        if let Err(e) = self.transport.send(peer, msg) {
            eprintln!(
                "[{}] Failed to request sync from {}: {}",
                self.replica_id, peer, e
            );
        }
    }

    /// Ask `peer` for everything, compacted state included.
    fn request_state_transfer(&mut self, peer: &PeerId) {
        let msg = TransportMessage::StateRequest {
            id: self.replica_id.clone(),
        };
        if let Err(e) = self.transport.send(peer, msg) {
            eprintln!(
                "[{}] Failed to request a state transfer from {}: {}",
                self.replica_id, peer, e
            );
        }
    }

    /// `true` while this replica has delivered nothing at all.
    ///
    /// The whole precondition for adopting a donor's state wholesale, in one
    /// place, because it is checked twice: once when deciding what to ask a peer
    /// for, and again when a response arrives — several donors can answer the
    /// same request, and the second answer must not undo the first.
    ///
    /// Deliberately *not* "and knows no other replica", which the plan proposed
    /// and which does not work: a peer's `SyncRequest` is internalised, so being
    /// asked for a delta adds the asker to the member set. A replica that has
    /// merely been spoken to would then look like one with history, and — as
    /// measured — a joiner would receive its donors' state transfers and
    /// silently discard every one of them. Knowing who the members are is not
    /// history. Having delivered something is.
    fn has_no_history(&self) -> bool {
        self.replica.stability().delivered == 0
    }

    /// Build the answer to a `StateRequest` from `id`.
    ///
    /// The refusal below is the whole reason returning-member merge stays out
    /// of this phase rather than half-happening by accident. A requester this
    /// replica already has operations from is *returning* — evicted, or long
    /// partitioned — and it may hold operations the session has never seen.
    /// Adopting a snapshot is a replace, not a merge, so serving one would
    /// discard them silently. Refusing costs the requester one round trip and
    /// gives phase 3 a defined starting point.
    fn state_response_for(&self, id: &str) -> TransportMessage<L::Op> {
        // Two replicas that start together are both empty and both ask. Serving
        // an empty snapshot would work, but it would make one of them adopt the
        // other's index ordering for nothing; saying there is nothing to give
        // lets both fall back to a delta sync, which is the right shape for
        // peers that are equals rather than donor and joiner.
        if self.replica.stability().delivered == 0 {
            return TransportMessage::StateUnavailable {
                reason: "this replica has no history to transfer".to_string(),
            };
        }
        if self.replica.has_history_for(id) {
            return TransportMessage::StateUnavailable {
                reason: format!(
                    "`{id}` is a returning member, not a fresh one; merging its \
                     history with a snapshot is not implemented"
                ),
            };
        }
        let Some(export) = self.export_log else {
            return TransportMessage::StateUnavailable {
                reason: "state transfer is not enabled on this replica".to_string(),
            };
        };
        eprintln!("[{}] serving a state transfer to {}", self.replica_id, id);
        TransportMessage::StateResponse {
            snapshot: self.replica.snapshot(),
            log: export(self.replica.log()),
        }
    }

    /// Install a donor's state, or explain why not.
    fn adopt_state(
        &mut self,
        from: &PeerId,
        snapshot: moirai_protocol::broadcast::tcsb::StateSnapshot<L::Op>,
        log: serde_json::Value,
    ) {
        let Some(import) = self.import_log else {
            eprintln!(
                "[{}] {} sent a state transfer but this replica cannot accept one",
                self.replica_id, from
            );
            return;
        };
        // A second donor's answer to the same request must not undo the first.
        // `adopt` replaces rather than merges, so re-adopting after delivering
        // anything would silently roll the replica back.
        if !self.has_no_history() {
            self.request_delta_sync(from);
            return;
        }
        match import(log) {
            Some(state) => {
                let members = snapshot.resolver().len();
                self.replica.adopt(snapshot, state);
                eprintln!(
                    "[{}] adopted state from {}: {} members, stable prefix {}, {} events above it",
                    self.replica_id,
                    from,
                    members,
                    self.replica.stability().stable_prefix,
                    self.replica.stability().retained,
                );
                // The snapshot is a point in time, and `adopt` discards
                // whatever this replica had buffered before it. A delta sync
                // closes both gaps in one round trip.
                self.request_delta_sync(from);
            }
            None => {
                eprintln!(
                    "[{}] could not decode the log {} sent; falling back to a delta sync",
                    self.replica_id, from
                );
                self.request_delta_sync(from);
            }
        }
    }

    /// Connect to known peers, then pull history from each new link.
    ///
    /// The pull is what makes a late joiner work. `Hello` travels dialer ->
    /// acceptor and the *acceptor* answers it with a `SyncRequest`, so without
    /// this the dialer pushes its history and receives none. A symmetric
    /// `PEERS` list hides that — both replicas are dialer and acceptor at once
    /// — but a node that starts after the others is only ever a dialer.
    pub fn connect(&mut self) {
        match self.transport.connect_to_peers() {
            Ok(new_peers) => {
                for peer in new_peers {
                    self.request_sync(&peer);
                }
            }
            Err(e) => {
                eprintln!("[{}] Some peer connections failed: {}", self.replica_id, e);
            }
        }
    }

    /// Apply an operation: send to the CRDT, then broadcast to peers.
    pub fn apply_op(&mut self, op: L::Op) -> OpResult {
        match self.replica.send(op.clone()) {
            Some(event_msg) => {
                // Record the operation
                self.operation_log.push(op);
                self.local_ops += 1;

                let transport_msg = TransportMessage::Event { event: event_msg };
                if let Err(e) = self.transport.broadcast(transport_msg) {
                    eprintln!("[{}] Broadcast failed: {}", self.replica_id, e);
                }
                OpResult {
                    success: true,
                    message: "Applied and broadcasted".to_string(),
                }
            }
            None => OpResult {
                success: false,
                message: "Operation not enabled".to_string(),
            },
        }
    }

    /// Handle an inbound transport message.
    fn handle_transport_message(&mut self, from: PeerId, msg: TransportMessage<L::Op>) {
        match msg {
            TransportMessage::Event { event } => {
                self.operation_log.push(event.event().op().clone());
                self.replica.receive(event);
            }
            TransportMessage::Batch { batch } => {
                for event in batch.batch().events() {
                    self.operation_log.push(event.op().clone());
                }
                self.replica.receive_batch(batch);
            }
            TransportMessage::SyncRequest { since } => {
                let batch = self.replica.pull(since);
                let response = TransportMessage::Batch { batch };
                if let Err(e) = self.transport.send(&from, response) {
                    eprintln!(
                        "[{}] Failed to send batch to {}: {}",
                        self.replica_id, from, e
                    );
                }
            }
            TransportMessage::StateRequest { id } => {
                let response = self.state_response_for(&id);
                if let Err(e) = self.transport.send(&from, response) {
                    eprintln!(
                        "[{}] Failed to answer the state request from {}: {}",
                        self.replica_id, from, e
                    );
                }
            }
            TransportMessage::StateResponse { snapshot, log } => {
                self.adopt_state(&from, snapshot, log);
            }
            TransportMessage::StateUnavailable { reason } => {
                eprintln!(
                    "[{}] {} will not serve a state transfer ({}); falling back to a delta sync",
                    self.replica_id, from, reason
                );
                self.request_delta_sync(&from);
            }
            TransportMessage::Hello { id, .. } => {
                eprintln!("[{}] Peer connected: {}", self.replica_id, id);
                self.request_sync(&id);
            }
            TransportMessage::Goodbye { id } => {
                eprintln!("[{}] Peer disconnected: {}", self.replica_id, id);
            }
            _ => {}
        }
    }

    /// Run the main event loop.
    pub fn run(&mut self) {
        eprintln!("[{}] Entering main event loop", self.replica_id);
        loop {
            // --- Adapter-submitted operations ---
            while let Ok(envelope) = self.adapter_op_rx.try_recv() {
                let result = self.apply_op(envelope.op);
                let _ = envelope.reply.send(result);
            }

            // --- Control commands (pause/resume/peers) ---
            while let Ok(cmd) = self.ctrl_rx.try_recv() {
                self.handle_control_cmd(cmd);
            }

            // --- Peers the bootnode has told us about since the last pass ---
            self.reconcile_discovered_peers();

            // --- Still nothing? Ask again. ---
            self.retry_state_transfer();

            // --- Accept new inbound TCP connections ---
            self.transport.accept_connections().ok();

            // --- Inbound network messages ---
            while let Ok(Some((from, msg))) = self.transport.try_recv() {
                self.handle_transport_message(from, msg);
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Re-ask every connected peer for a state transfer while this replica
    /// still has nothing.
    ///
    /// Asking *all* of them rather than one is deliberate. It costs a refusal
    /// per peer that has nothing to give, and it means a donor that dies
    /// mid-transfer does not strand the joiner — another peer answers on the
    /// next round. The second answer to arrive is discarded by the freshness
    /// re-check in `adopt_state`, so there is no race between two donors.
    ///
    /// Stops by itself: the moment anything is delivered — adopted, replayed or
    /// locally applied — `has_no_history` goes false and this becomes one
    /// comparison per loop iteration.
    fn retry_state_transfer(&mut self) {
        if self.import_log.is_none() || !self.has_no_history() {
            return;
        }
        let now = Instant::now();
        if self
            .last_state_request
            .is_some_and(|last| now.duration_since(last) < STATE_TRANSFER_RETRY)
        {
            return;
        }
        let peers: Vec<PeerId> = self
            .transport
            .peers()
            .into_iter()
            .filter(|p| p.status == crate::transport::PeerStatus::Connected)
            .map(|p| p.id)
            .collect();
        if peers.is_empty() {
            return;
        }
        self.last_state_request = Some(now);
        for peer in peers {
            self.request_state_transfer(&peer);
        }
    }

    /// Fold the newest bootnode roster into the transport's address book and
    /// dial whatever is new.
    ///
    /// Cheap when nothing arrived, which is every iteration but one per
    /// interval: the poll thread does the waiting, this only drains a channel.
    /// `connect()` skips peers already connected, so a roster that has not
    /// changed costs one `HashMap` lookup per member.
    fn reconcile_discovered_peers(&mut self) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(roster) = discovery.latest_roster() else {
            return;
        };

        for peer in roster {
            self.transport.add_peer(peer.id, peer.addr);
        }

        // Dial on *every* roster, not only when the roster changed. A first
        // dial can fail because the peer is not listening yet, and gating the
        // retry on a membership change would reintroduce dial-once for exactly
        // the peers that need retrying. `connect_to_peers()` skips anything
        // already connected, so a settled mesh costs one map lookup per member
        // per interval, and `connect()` carries the `SyncRequest` that gives a
        // newly dialled peer our history and asks for theirs.
        self.connect();
    }

    /// Process a control command from the HTTP thread.
    fn handle_control_cmd(&mut self, cmd: ControlCmd) {
        match cmd {
            ControlCmd::Pause { peer_id, reply } => {
                let result = Self::unit_result_to_op_result(
                    self.transport.pause_peer(&peer_id),
                    format!("Paused peer '{}'", peer_id),
                    format!("Failed to pause '{}'", peer_id),
                );
                let _ = reply.send(result);
            }
            ControlCmd::Resume { peer_id, reply } => {
                let result = match self.transport.resume_peer(&peer_id) {
                    Ok(()) => {
                        let buffered = self.transport.drain_buffer(&peer_id);
                        let count = buffered.len();
                        for msg in buffered {
                            self.handle_transport_message(peer_id.clone(), msg);
                        }
                        // Request delta sync from the peer
                        self.request_sync(&peer_id);
                        OpResult {
                            success: true,
                            message: format!(
                                "Resumed peer '{}', delivered {} buffered msgs, requested sync",
                                peer_id, count
                            ),
                        }
                    }
                    Err(e) => OpResult {
                        success: false,
                        message: format!("Failed to resume '{}': {}", peer_id, e),
                    },
                };
                let _ = reply.send(result);
            }
            ControlCmd::PauseAll { reply } => {
                let result = Self::unit_result_to_op_result(
                    self.transport.pause_all(),
                    "All peers paused".to_string(),
                    "Failed".to_string(),
                );
                let _ = reply.send(result);
            }
            ControlCmd::ResumeAll { reply } => {
                // Collect paused peer IDs first
                let paused: Vec<String> = self
                    .transport
                    .peers()
                    .into_iter()
                    .filter(|p| p.status == crate::transport::PeerStatus::Paused)
                    .map(|p| p.id)
                    .collect();
                let _ = self.transport.resume_all();
                // Drain buffers and sync for each
                let mut total_buffered = 0;
                for peer_id in &paused {
                    let buffered = self.transport.drain_buffer(peer_id);
                    total_buffered += buffered.len();
                    for msg in buffered {
                        self.handle_transport_message(peer_id.clone(), msg);
                    }
                    self.request_sync(peer_id);
                }
                let _ = reply.send(OpResult {
                    success: true,
                    message: format!(
                        "Resumed {} peers, delivered {} buffered msgs",
                        paused.len(),
                        total_buffered
                    ),
                });
            }
            ControlCmd::Peers { reply } => {
                let peers: Vec<serde_json::Value> = self
                    .transport
                    .peers()
                    .into_iter()
                    .map(|p| {
                        json!({
                            "id": p.id,
                            "status": format!("{:?}", p.status),
                            "buffered": self.transport.buffered_count(&p.id),
                        })
                    })
                    .collect();
                let _ = reply.send(json!({ "peers": peers }));
            }
            ControlCmd::Query { reply } => {
                let serialized = match &self.query_fn {
                    Some(f) => f(&self.replica),
                    None => json!({ "error": "state query not enabled — implement QueryableLog" }),
                };
                let _ = reply.send(serialized);
            }
            ControlCmd::Operations { reply } => {
                // Serialize all logged operations
                let operations: Vec<serde_json::Value> = self
                    .operation_log
                    .iter()
                    .filter_map(|op| serde_json::to_value(op).ok())
                    .collect();

                let _ = reply.send(json!({
                    "operations": operations,
                    "count": operations.len()
                }));
            }
            ControlCmd::Metrics { reply } => {
                let _ = reply.send(self.metrics());
            }
            ControlCmd::Leave { reply } => {
                // A replica has no shutdown path — `run()` never returns and
                // the process is killed from outside — so departure cannot be
                // announced on the way out. This is the announcement: stop
                // re-registering and tell the directory to drop us, while the
                // replica keeps running and keeps answering its peers.
                //
                // It is deliberately *only* a directory departure. Every peer
                // still has this replica in its matrix clock, so causal
                // stability keeps waiting for it exactly as it would for a
                // crash. Making a leave advance stability is phase 2.
                let result = match &self.discovery {
                    Some(discovery) => {
                        discovery.leave();
                        self.discovery = None;
                        OpResult {
                            success: true,
                            message: "deregistered from the bootnode session".to_string(),
                        }
                    }
                    None => OpResult {
                        success: false,
                        message: "discovery is not enabled; nothing to leave".to_string(),
                    },
                };
                let _ = reply.send(result);
            }
        }
    }

    /// Everything an observer needs to plot causal stability over time.
    ///
    /// Field notes, because the names are easy to misread:
    ///
    /// - `stable_prefix` only advances when *every* known replica has
    ///   acknowledged. One silent member freezes it, and that freeze is the
    ///   phenomenon the phase-1 rig exists to measure.
    /// - `retained_ops` is the replication buffer, pruned only as
    ///   `stable_prefix` advances. It is the growth curve, and the closest
    ///   observable proxy for PO-Log length — the Arachne-generated composite
    ///   logs do not expose their unstable length through `IsLog`.
    /// - `ops_applied` counts operations *originated* here. Use it, never
    ///   `/api/operations`, which double-counts remote deliveries.
    fn metrics(&self) -> serde_json::Value {
        let stability = self.replica.stability();
        let peers = self.transport.peers();
        let stable_version: serde_json::Map<String, serde_json::Value> = stability
            .stable_version
            .iter()
            .map(|(id, seq)| (id.clone(), json!(seq)))
            .collect();

        json!({
            "replica_id": self.replica_id,
            "stable_prefix": stability.stable_prefix,
            "stable_version": stable_version,
            "delivered_ops": stability.delivered,
            "retained_ops": stability.retained,
            "pending_ops": stability.pending,
            "known_replicas": stability.known_replicas,
            "ops_applied": self.local_ops,
            "peer_count": peers
                .iter()
                .filter(|p| p.status == crate::transport::PeerStatus::Connected)
                .count(),
            "peers_known": peers.len(),
        })
    }
}

// =============================================================================
// TCP convenience constructor
// =============================================================================

impl<L: IsLog> GenericNode<L, TcpTransport<L::Op>>
where
    L::Op: NetworkOp,
{
    /// Create a TCP-backed node (convenience wrapper around [`with_transport`]).
    ///
    /// * `replica_id` — unique identifier for this replica.
    /// * `members` — all replica IDs in the cluster (including self).
    /// * `listen_port` — TCP port for peer connections.
    /// * `peer_addresses` — map of `peer_id → "host:port"` for outbound connections.
    pub fn new(
        replica_id: String,
        members: &[&str],
        listen_port: u16,
        peer_addresses: HashMap<String, String>,
    ) -> Self {
        let transport = TcpTransport::new(replica_id.clone(), listen_port, peer_addresses)
            .expect("Failed to create TCP transport");
        Self::with_transport(replica_id, members, transport)
    }
}

impl<L: IsLog + QueryableLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
where
    L::Op: NetworkOp,
{
    /// Enable the `GET /api/state` endpoint by wiring up the query function.
    /// Call this after `new()` and before `run()`.
    pub fn enable_state_query(&mut self) {
        self.query_fn = Some(L::query_state_json);
    }
}

impl<L: IsLog + TransferableLog, T: CrdtTransport<Op = L::Op>> GenericNode<L, T>
where
    L::Op: NetworkOp,
{
    /// Allow this replica to serve and to accept a state transfer.
    ///
    /// Opt-in, like [`enable_state_query`], and for the same reason: a
    /// hand-written log that is not serialisable must keep working. Without it
    /// a joiner can still catch up — but only on the events its peers have not
    /// yet compacted away, which is the phase-1 behaviour and the gap this
    /// phase closes.
    ///
    /// [`enable_state_query`]: GenericNode::enable_state_query
    pub fn enable_state_transfer(&mut self) {
        self.export_log = Some(L::export_log);
        self.import_log = Some(L::import_log);
    }
}
