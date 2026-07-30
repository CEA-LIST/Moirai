//! The transport a node is actually built with: one direct provider, later one
//! relay provider, and a per-peer routing decision between them.
//!
//! # Why a composite and not a branch inside the TCP transport
//!
//! A relayed peer has **no stream of its own** — its frames are multiplexed
//! over a single session shared with every other relayed peer — so the direct
//! transport's `HashMap<PeerId, TcpStream>` cannot describe it. The cheap way
//! to absorb that would be a `Channel::{Direct, Relayed}` enum inside
//! [`DirectTransport`], dispatched per peer. That was considered and rejected
//! (design C-D8): it modifies the *existing* provider instead of adding one,
//! which folds relaying into TCP so it can never be swapped, tested or replaced
//! on its own, and leaves the architecture's Transport extension point (EXT-4)
//! describing a component structure the code does not have.
//!
//! So: three realizations of one interface. [`DirectTransport`] dials and holds
//! a stream per peer. `RelayTransport` holds the one multiplexed relay session.
//! `CompositeTransport` owns both, keeps the per-peer routing table, and
//! delegates each call — and because it realizes [`CrdtTransport`] itself,
//! `GenericNode<L, T>` is unchanged and `Replica`, the TCSB and the PO-Log
//! never learn a relay exists.
//!
//! # What this file is at this point
//!
//! Only the `direct` half. Every [`CrdtTransport`] method delegates 1:1, so a
//! node built with a composite behaves exactly as one built with a
//! [`DirectTransport`] — which is the acceptance criterion for the refactor
//! that introduced it. The relay provider and the routing table land in the
//! steps that have something to route.
//!
//! [`DirectTransport`]: crate::direct_transport::DirectTransport

use serde::{de::DeserializeOwned, Serialize};

use crate::direct_transport::DirectTransport;
use crate::transport::{CrdtTransport, PeerId, PeerInfo, TransportMessage, TransportResult};
use crate::HashMap;

/// A transport that owns the providers a replica can reach a peer through.
pub struct CompositeTransport<O>
where
    O: Serialize + DeserializeOwned,
{
    direct: DirectTransport<O>,
}

impl<O> CompositeTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Build a composite over a fresh direct transport.
    ///
    /// The arguments are [`DirectTransport::new`]'s, unchanged: a replica that
    /// has no relay configured is constructed exactly as it was before the
    /// composite existed.
    pub fn new(
        local_id: PeerId,
        listen_port: u16,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        Ok(Self {
            direct: DirectTransport::new(local_id, listen_port, peer_addresses)?,
        })
    }

    /// Whether `peer` is currently paused. Delegated; see
    /// [`DirectTransport::is_paused`].
    pub fn is_paused(&self, peer: &PeerId) -> bool {
        self.direct.is_paused(peer)
    }
}

impl<O> CrdtTransport for CompositeTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    type Op = O;

    fn local_id(&self) -> &PeerId {
        self.direct.local_id()
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        self.direct.send(peer, msg)
    }

    fn broadcast(&mut self, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        self.direct.broadcast(msg)
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Self::Op>)>> {
        self.direct.try_recv()
    }

    fn peers(&self) -> Vec<PeerInfo> {
        self.direct.peers()
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        self.direct.is_connected(peer)
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        self.direct.pause_peer(peer)
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        self.direct.resume_peer(peer)
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        self.direct.pause_all()
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        self.direct.resume_all()
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.direct.buffered_count(peer)
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        self.direct.accept_connections()
    }

    fn add_peer(&mut self, peer: PeerId, addr: String) -> bool {
        self.direct.add_peer(peer, addr)
    }

    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        self.direct.connect_to_peers()
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<Self::Op>> {
        self.direct.drain_buffer(peer)
    }
}
