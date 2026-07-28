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
use std::io;

use moirai_protocol::broadcast::message::{BatchMessage, EventMessage, SinceMessage};

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

    /// Connect to configured/known peers.
    ///
    /// Necessary for transports that require explicit connections (e.g. TCP)
    /// The default implementation is a no-op, for transports that discover peers automatically (e.g. gossip).
    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(vec![])
    }

    /// Get all messages buffered while a peer was paused.
    ///
    /// Called after [`resume_peer`] to deliver messages that arrived during the
    /// partition. The default implementation returns an empty vec.
    fn drain_buffer(&mut self, _peer: &PeerId) -> Vec<TransportMessage<Self::Op>> {
        vec![]
    }
}
