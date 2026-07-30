//! Transport abstraction for Moirai CRDT.
//!
//! This module provides a transport-independent interface for sending and receiving
//! CRDT events between replicas. Different implementations can use TCP, WebRTC,
//! WebSocket, or any other communication channel.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Moirai CRDT Core                         │
//! └──────────────────────────┬──────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Message Format Layer                     │
//! │              (EventMessage, BatchMessage, SinceMessage)     │
//! └──────────────────────────┬──────────────────────────────────┘
//!                            │
//!           ┌────────────────┼────────────────┐
//!           ▼                ▼                ▼
//!    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
//!    │ TCP/Docker  │  │   WebRTC    │  │  WebSocket  │
//!    │  Transport  │  │  Transport  │  │  Transport  │
//!    └─────────────┘  └─────────────┘  └─────────────┘
//! ```

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{self, Write};
use std::net::TcpStream;

use moirai_protocol::broadcast::message::{BatchMessage, EventMessage, SinceMessage};
use moirai_protocol::broadcast::tcsb::StateSnapshot;

/// Peer identifier type
pub type PeerId = String;

/// Messages that can be sent over any transport
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    bound(serialize = "O: Serialize", deserialize = "O: DeserializeOwned")
)]
pub enum TransportMessage<O>
where
    O: Serialize + DeserializeOwned,
{
    /// Wraps moirai protocol events for transport-agnostic dispatch
    Event {
        event: EventMessage<O>,
    },
    Batch {
        batch: BatchMessage<O>,
    },
    SyncRequest {
        since: SinceMessage,
    },

    /// "I have nothing; send me everything."
    ///
    /// Sent instead of `SyncRequest` by a replica with no history at all,
    /// because a `SyncRequest` is answered out of the outbox and the outbox
    /// holds only what is not yet causally stable. Carries the requester's id
    /// so that the donor can tell a *fresh* joiner from a *returning* member
    /// and refuse the latter — wholesale adoption would discard whatever a
    /// returning member did while it was away. Merging the two is phase 3.
    StateRequest {
        id: PeerId,
    },
    /// The compacted state, and the causal bookkeeping that makes sense of it.
    ///
    /// The two travel together on purpose: a matrix clock claiming operations
    /// the log does not contain would make the joiner reject them as duplicates
    /// forever.
    StateResponse {
        snapshot: StateSnapshot<O>,
        /// The donor's log, via `TransferableLog::export_log`.
        log: serde_json::Value,
    },
    /// The donor declined. The requester falls back to `SyncRequest`, which is
    /// the correct behaviour for a returning member and a harmless one for a
    /// peer that simply cannot serve a transfer.
    StateUnavailable {
        reason: String,
    },

    /// Hello handshake - announces replica identity
    Hello {
        id: PeerId,
        metadata: Option<String>,
    },
    /// Acknowledgment
    Ack {
        from: PeerId,
    },
    /// Peer disconnecting gracefully
    Goodbye {
        id: PeerId,
    },
    /// Raw payload
    Raw {
        payload: O,
    },
}

/// Error type for transport operations
#[derive(Debug)]
pub enum TransportError {
    Io(io::Error),
    Serialization(String),
    PeerNotFound(PeerId),
    ConnectionClosed(PeerId),
    WouldBlock,
    Other(String),
}

impl From<io::Error> for TransportError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::WouldBlock {
            TransportError::WouldBlock
        } else {
            TransportError::Io(e)
        }
    }
}

impl From<serde_json::Error> for TransportError {
    fn from(e: serde_json::Error) -> Self {
        TransportError::Serialization(e.to_string())
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Io(e) => write!(f, "IO error: {}", e),
            TransportError::Serialization(s) => write!(f, "Serialization error: {}", s),
            TransportError::PeerNotFound(p) => write!(f, "Peer not found: {}", p),
            TransportError::ConnectionClosed(p) => write!(f, "Connection closed: {}", p),
            TransportError::WouldBlock => write!(f, "Would block"),
            TransportError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for TransportError {}

/// Result type for transport operations
pub type TransportResult<T> = Result<T, TransportError>;

/// Write one newline-delimited JSON frame.
///
/// The framing every transport in this crate uses, in one place. It is
/// deliberately generic over the value rather than fixed to
/// [`TransportMessage`]: the relay transport writes the same framing around an
/// *envelope* that carries a `TransportMessage` as one of its fields, so both
/// share the codec and neither owns it.
pub(crate) fn write_frame<T: Serialize>(stream: &mut TcpStream, value: &T) -> TransportResult<()> {
    let json = serde_json::to_string(value)?;
    writeln!(stream, "{}", json)?;
    stream.flush()?;
    Ok(())
}

/// Connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    Connected,
    Paused,
    Disconnected,
    Connecting,
}

/// Information about a peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: PeerId,
    pub address: String,
    pub status: PeerStatus,
    pub buffered_messages: usize,
}

/// Transport-agnostic interface for CRDT replication.
///

pub trait CrdtTransport {
    /// The operation type this transport handles
    type Op: Serialize + DeserializeOwned + Clone;

    /// Get this replica's ID
    fn local_id(&self) -> &PeerId;

    /// Send a message to a specific peer
    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Self::Op>) -> TransportResult<()>;

    /// Broadcast a message to all connected (non-paused) peers
    fn broadcast(&mut self, msg: TransportMessage<Self::Op>) -> TransportResult<()>;

    /// Try to receive a message (non-blocking)
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Self::Op>)>>;

    /// Get list of all peers
    fn peers(&self) -> Vec<PeerInfo>;

    /// Check if a specific peer is connected
    fn is_connected(&self, peer: &PeerId) -> bool;

    /// Pause communication with a peer (for simulation)
    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()>;

    /// Resume communication
    /// Buffered messages will be delivered
    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()>;

    /// Pause all peers
    fn pause_all(&mut self) -> TransportResult<()>;

    /// Resume all peers
    fn resume_all(&mut self) -> TransportResult<()>;

    /// Get number of buffered messages for a peer
    fn buffered_count(&self, peer: &PeerId) -> usize;

    /// Accept new incoming connections (if applicable)
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>>;

    /// Learn how to reach `peer`, so the next [`connect_to_peers`] can dial it.
    ///
    /// Returns `true` when this is new information — a peer that was unknown,
    /// or one whose address changed — so a caller can tell a discovery round
    /// that changed the roster from one that did not.
    ///
    /// The default is a no-op returning `false`, for transports that find
    /// their peers themselves and have no address book to update.
    ///
    /// [`connect_to_peers`]: CrdtTransport::connect_to_peers
    fn add_peer(&mut self, _peer: PeerId, _addr: String) -> bool {
        false
    }

    /// Connect to configured/known peers.
    ///
    /// Necessary for transports that require explicit connections (e.g. TCP)
    /// The default implementation is a no-op, for transports that discover peers automatically (e.g. gossip).
    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(vec![])
    }

    /// How each known peer is currently reached, as a short label.
    ///
    /// Empty by default, and that is the honest answer for a transport with one
    /// way of reaching a peer: there is no route to report because there is no
    /// choice. A composite over several providers overrides it, and that is what
    /// `/api/metrics` surfaces so an operator can see that one edge is direct
    /// and another relayed instead of inferring it from throughput.
    fn routes(&self) -> Vec<(PeerId, &'static str)> {
        Vec::new()
    }

    /// Learn where a relay is, so peers that cannot be dialled directly can
    /// still be reached.
    ///
    /// The endpoint is advertised by the bootnode rather than configured on the
    /// replica, so it arrives at run time with a roster and can change; hence a
    /// method rather than a constructor argument.
    ///
    /// Default is a no-op, exactly like [`add_peer`]: a transport with no
    /// notion of a relay ignores it, which is what keeps the whole relay path
    /// additive — a replica with no `BOOTNODE_URL` never hears of one and
    /// behaves as it did before.
    ///
    /// [`add_peer`]: CrdtTransport::add_peer
    fn set_relay(&mut self, _session: &str, _addr: &str) {}

    /// Get all messages buffered while a peer was paused.
    ///
    /// Called after [`resume_peer`] to deliver messages that arrived during the
    /// partition. The default implementation returns an empty vec.
    fn drain_buffer(&mut self, _peer: &PeerId) -> Vec<TransportMessage<Self::Op>> {
        vec![]
    }
}
