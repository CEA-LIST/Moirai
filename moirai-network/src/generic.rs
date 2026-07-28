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
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;
use moirai_protocol::utils::intern_str::InternalizeOp;

use crate::query::QueryableLog;
use crate::tcp_transport::TcpTransport;
use crate::transport::{CrdtTransport, PeerId, TransportMessage};
use crate::HashMap;

/// Convenience alias
pub type TcpNode<L> = GenericNode<L, TcpTransport<<L as IsLog>::Op>>;

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
        }
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
        let since = self.replica.since();
        let msg = TransportMessage::SyncRequest { since };
        if let Err(e) = self.transport.send(peer, msg) {
            eprintln!(
                "[{}] Failed to request sync from {}: {}",
                self.replica_id, peer, e
            );
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

            // --- Accept new inbound TCP connections ---
            self.transport.accept_connections().ok();

            // --- Inbound network messages ---
            while let Ok(Some((from, msg))) = self.transport.try_recv() {
                self.handle_transport_message(from, msg);
            }

            thread::sleep(Duration::from_millis(10));
        }
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
        }
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
