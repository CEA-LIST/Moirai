//! NW.31f oracle helpers (vault spec/items/NW.31f.md), shared by the node and
//! testkit tests through `#[path]`: a transport tap that records every frame a
//! node takes in, and the toy replica with a state transfer on offer.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use moirai_netstack::{Opaque, Progress, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_testkit::{ToyFactory, ToyReplica};
use moirai_netstack_transport::transport::{CrdtTransport, PeerId, PeerInfo, TransportResult};
use serde_json::{Value, json};

/// The wire tag of a frame.
pub fn kind(msg: &TransportMessage) -> &'static str {
    match msg {
        TransportMessage::Event { .. } => "Event",
        TransportMessage::Batch { .. } => "Batch",
        TransportMessage::SyncRequest { .. } => "SyncRequest",
        TransportMessage::StateRequest { .. } => "StateRequest",
        TransportMessage::StateResponse { .. } => "StateResponse",
        TransportMessage::StateUnavailable { .. } => "StateUnavailable",
        TransportMessage::Hello { .. } => "Hello",
        TransportMessage::Ack { .. } => "Ack",
        TransportMessage::Goodbye { .. } => "Goodbye",
        TransportMessage::Chunk { .. } => "Chunk",
        TransportMessage::Unknown(_) => "Unknown",
    }
}

/// A transport that records the sender and tag of every frame it hands the
/// node, whether received or drained from a paused peer's buffer.
pub struct Tap<T> {
    pub inner: T,
    pub received: Vec<(PeerId, &'static str)>,
}

impl<T> Tap<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            received: Vec::new(),
        }
    }

    /// The senders of every frame of tag `tag` taken in so far.
    pub fn senders_of(&self, tag: &str) -> Vec<PeerId> {
        self.received
            .iter()
            .filter(|(_, k)| *k == tag)
            .map(|(from, _)| from.clone())
            .collect()
    }
}

impl<T: CrdtTransport> CrdtTransport for Tap<T> {
    fn local_id(&self) -> &PeerId {
        self.inner.local_id()
    }
    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        self.inner.send(peer, msg)
    }
    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        self.inner.broadcast(msg)
    }
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        let got = self.inner.try_recv()?;
        if let Some((from, msg)) = &got {
            self.received.push((from.clone(), kind(msg)));
        }
        Ok(got)
    }
    fn peers(&self) -> Vec<PeerInfo> {
        self.inner.peers()
    }
    fn is_connected(&self, peer: &PeerId) -> bool {
        self.inner.is_connected(peer)
    }
    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        self.inner.pause_peer(peer)
    }
    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        self.inner.resume_peer(peer)
    }
    fn pause_all(&mut self) -> TransportResult<()> {
        self.inner.pause_all()
    }
    fn resume_all(&mut self) -> TransportResult<()> {
        self.inner.resume_all()
    }
    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.inner.buffered_count(peer)
    }
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        self.inner.accept_connections()
    }
    fn add_peer(&mut self, peer: PeerId, addr: String) -> bool {
        self.inner.add_peer(peer, addr)
    }
    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        self.inner.connect_to_peers()
    }
    fn routes(&self) -> Vec<(PeerId, &'static str)> {
        self.inner.routes()
    }
    fn set_relay(&mut self, session: &str, addr: &str) {
        self.inner.set_relay(session, addr)
    }
    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage> {
        let drained = self.inner.drain_buffer(peer);
        for msg in &drained {
            self.received.push((peer.clone(), kind(msg)));
        }
        drained
    }
}

/// Elements a delta no longer carries, shared by every replica of a factory:
/// 601c887's pruned outbox, made by hand.
pub type Pruned = Rc<RefCell<BTreeSet<String>>>;

/// Opens toy replicas that offer a state transfer.
#[derive(Default, Clone)]
pub struct OfferingFactory {
    pub pruned: Pruned,
}

/// The toy, plus a snapshot (every event it delivered, pruned or not, as the
/// toy's own `ops_since` array, which its `apply_remote` takes) and a delta
/// that leaves out the pruned elements.
pub struct Offering {
    inner: ToyReplica,
    pruned: Pruned,
}

impl ReplicaFactory for OfferingFactory {
    type Replica = Offering;

    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Offering {
        Offering {
            inner: ToyFactory::default().open(log, self_id, members),
            pruned: self.pruned.clone(),
        }
    }
}

impl ReplicaPort for Offering {
    fn replica_id(&self) -> &str {
        self.inner.replica_id()
    }
    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        self.inner.submit(op)
    }
    fn apply_remote(&mut self, from: &str, ops: Opaque) -> Result<(), String> {
        self.inner.apply_remote(from, ops)
    }
    fn frontier(&self) -> Opaque {
        self.inner.frontier()
    }
    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
        let answer = self.inner.ops_since(frontier)?;
        let pruned = self.pruned.borrow();
        let kept = answer
            .as_array()
            .expect("the toy answers an array")
            .iter()
            .filter(|e| {
                !e.get("op")
                    .and_then(Value::as_str)
                    .is_some_and(|op| pruned.contains(op))
            })
            .cloned()
            .collect();
        Ok(Value::Array(kept))
    }
    fn progress(&self) -> Progress {
        self.inner.progress()
    }
    fn members(&self) -> Vec<String> {
        self.inner.members()
    }
    fn has_history_from(&self, peer: &str) -> bool {
        self.inner.has_history_from(peer)
    }
    fn read_state(&self) -> Value {
        self.inner.read_state()
    }
    fn snapshot_for(&mut self, _requester: &str) -> Option<Result<Opaque, String>> {
        Some(self.inner.ops_since(json!([])))
    }
}
