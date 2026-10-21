//! The transport a [`crate::generic::Node`] is built with: TCP or Zenoh,
//! chosen at run time by `MOIRAI_TRANSPORT` (NW.14 note, section 4).
//!
//! An enum rather than a type parameter, so that a caller that reaches the
//! transport only through `Node::<L>::new_with_log_id` (Arachne's generated
//! `network_node`) needs no change, and one binary serves both transports.

use std::net::TcpListener;

use serde::{de::DeserializeOwned, Serialize};

use crate::composite::CompositeTransport;
use crate::transport::{CrdtTransport, PeerId, PeerInfo, TransportMessage, TransportResult};
use crate::transport_config::{TransportConfig, TransportKind};
#[cfg(feature = "zenoh")]
use crate::zenoh_transport::ZenohTransport;
use crate::HashMap;

/// TCP or Zenoh, delegating every call of [`CrdtTransport`] to the one it is.
// One per node, built once and never moved around: the difference in size
// between the two arms costs nothing, and boxing one would change the types
// a caller builds the enum from.
#[allow(clippy::large_enum_variant)]
pub enum AnyTransport<O>
where
    O: Serialize + DeserializeOwned,
{
    Tcp(CompositeTransport<O>),
    #[cfg(feature = "zenoh")]
    Zenoh(ZenohTransport<O>),
}

/// Where a TCP transport listens: a port to bind, or a listener the caller
/// has already bound.
enum Listen {
    Port(u16),
    Listener(TcpListener),
}

impl<O> AnyTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// The transport `MOIRAI_TRANSPORT` names, listening on `listen_port` and
    /// knowing `peer_addresses` (`id → host:port`).
    pub fn from_env(
        local_id: PeerId,
        listen_port: u16,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        Self::build(local_id, Listen::Port(listen_port), peer_addresses)
    }

    /// [`from_env`], over a replication listener the caller has bound.
    ///
    /// Under Zenoh the listener is released and Zenoh binds its port itself;
    /// the port is the listener's.
    ///
    /// [`from_env`]: AnyTransport::from_env
    pub fn from_env_with_listener(
        local_id: PeerId,
        listener: TcpListener,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        Self::build(local_id, Listen::Listener(listener), peer_addresses)
    }

    fn build(
        local_id: PeerId,
        listen: Listen,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        let config = TransportConfig::from_env()
            .map_err(|e| crate::transport::TransportError::Other(e.to_string()))?;
        match config.kind() {
            TransportKind::Tcp => match listen {
                Listen::Port(port) => CompositeTransport::new(local_id, port, peer_addresses),
                Listen::Listener(listener) => {
                    CompositeTransport::with_listener(local_id, listener, peer_addresses)
                }
            }
            .map(AnyTransport::Tcp),
            TransportKind::Zenoh => Self::zenoh(&config, local_id, listen, peer_addresses),
        }
    }

    #[cfg(feature = "zenoh")]
    fn zenoh(
        config: &TransportConfig,
        local_id: PeerId,
        listen: Listen,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        let port = match listen {
            Listen::Port(port) => port,
            Listen::Listener(listener) => listener.local_addr()?.port(),
        };
        let zenoh_config = config
            .zenoh_config(port, &peer_addresses)
            .map_err(|e| crate::transport::TransportError::Other(e.to_string()))?;
        let known = peer_addresses.into_keys().collect();
        ZenohTransport::open(local_id, config.session_id(), known, zenoh_config)
            .map(AnyTransport::Zenoh)
    }

    #[cfg(not(feature = "zenoh"))]
    fn zenoh(
        _config: &TransportConfig,
        _local_id: PeerId,
        _listen: Listen,
        _peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        Err(crate::transport::TransportError::Other(
            "MOIRAI_TRANSPORT=zenoh, but moirai-network was built without its `zenoh` feature"
                .to_string(),
        ))
    }
}

/// Call `$call` on whichever transport `$self` is.
macro_rules! delegate {
    ($self:expr, $t:ident => $call:expr) => {
        match $self {
            AnyTransport::Tcp($t) => $call,
            #[cfg(feature = "zenoh")]
            AnyTransport::Zenoh($t) => $call,
        }
    };
}

impl<O> CrdtTransport for AnyTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    type Op = O;

    fn local_id(&self) -> &PeerId {
        delegate!(self, t => t.local_id())
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<O>) -> TransportResult<()> {
        delegate!(self, t => t.send(peer, msg))
    }

    fn broadcast(&mut self, msg: TransportMessage<O>) -> TransportResult<()> {
        delegate!(self, t => t.broadcast(msg))
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<O>)>> {
        delegate!(self, t => t.try_recv())
    }

    fn peers(&self) -> Vec<PeerInfo> {
        delegate!(self, t => t.peers())
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        delegate!(self, t => t.is_connected(peer))
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        delegate!(self, t => t.pause_peer(peer))
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        delegate!(self, t => t.resume_peer(peer))
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        delegate!(self, t => t.pause_all())
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        delegate!(self, t => t.resume_all())
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        delegate!(self, t => t.buffered_count(peer))
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        delegate!(self, t => t.accept_connections())
    }

    fn add_peer(&mut self, peer: PeerId, addr: String) -> bool {
        delegate!(self, t => t.add_peer(peer, addr))
    }

    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        delegate!(self, t => t.connect_to_peers())
    }

    fn routes(&self) -> Vec<(PeerId, &'static str)> {
        delegate!(self, t => t.routes())
    }

    fn set_relay(&mut self, session: &str, addr: &str) {
        delegate!(self, t => t.set_relay(session, addr))
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<O>> {
        delegate!(self, t => t.drain_buffer(peer))
    }
}
