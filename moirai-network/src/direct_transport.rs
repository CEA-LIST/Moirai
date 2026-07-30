//! One TCP stream per peer: the transport this project has always had.
//!
//! Named `DirectTransport` because it is now one of three realizations of
//! [`CrdtTransport`] rather than the only one. It dials a peer's advertised
//! address and holds a stream of its own for it — which is exactly what a
//! replica behind NAT cannot offer. `RelayTransport` is the realization for
//! those peers, and [`crate::composite`] is what routes
//! between the two. Nothing here knows any of that: this file is the
//! pre-relay `TcpTransport` renamed, and its behaviour is unchanged.
//!
//! `pub struct DirectTransport` is still keyed `HashMap<PeerId, TcpStream>`,
//! and that assumption — one stream per peer — is the reason a relayed peer
//! cannot live here: its frames are multiplexed over a session shared with
//! every other relayed peer.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;

use serde::{de::DeserializeOwned, Serialize};

use crate::transport::{
    dial, write_frame, CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportError,
    TransportMessage, TransportResult,
};
use crate::{HashMap, HashSet};

/// How long one direct dial may take.
///
/// There was no timeout at all until phase 3: `TcpStream::connect` inherits the
/// operating system's, which is minutes. That was invisible for as long as every
/// unreachable peer in the test suite was either *refused* or a DNS failure, both
/// of which return in milliseconds — and it is exactly wrong for the case the
/// relay exists to serve, where a NAT'd peer's advertised address **black-holes**
/// and the connect neither succeeds nor fails. One such peer in a roster would
/// freeze the replica's event loop for minutes per reconcile.
///
/// The honest remaining cost: a dial still blocks the event loop for up to this
/// long, so a replica with an unreachable peer pauses for two seconds every time
/// the composite's backoff decides to re-probe it. At the 30-second cap that is
/// under 7% of the loop, and moving the dial off the loop is a larger change than
/// this phase needs.
const DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// TCP-based transport for CRDT replication, one stream per peer.
pub struct DirectTransport<O>
where
    O: Serialize + DeserializeOwned,
{
    /// This replica's ID
    local_id: PeerId,
    /// Active TCP connections to peers
    connections: HashMap<PeerId, TcpStream>,
    /// Peer addresses for reconnection
    peer_addresses: HashMap<PeerId, String>,
    /// Paused peers (for simulation)
    paused_peers: HashSet<PeerId>,
    /// Buffered messages from paused peers
    message_buffer: HashMap<PeerId, VecDeque<TransportMessage<O>>>,
    /// Channel for receiving messages from connection handlers
    incoming_rx: Receiver<(PeerId, TransportMessage<O>)>,
    /// Sender for connection handler threads
    incoming_tx: Sender<(PeerId, TransportMessage<O>)>,
    /// TCP listener for accepting connections
    listener: Option<TcpListener>,
    /// Listen port
    listen_port: u16,
    /// Counter for generating unique temporary connection IDs
    temp_id_counter: AtomicU64,
}

impl<O> DirectTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Create a new TCP transport
    ///
    /// # Arguments
    /// * `local_id` - This replica's identifier
    /// * `listen_port` - Port to listen for incoming connections
    /// * `peer_addresses` - Map of peer IDs to their TCP addresses (host:port)
    pub fn new(
        local_id: PeerId,
        listen_port: u16,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        let (incoming_tx, incoming_rx) = mpsc::channel();

        // Create listener
        let listener = TcpListener::bind(format!("0.0.0.0:{}", listen_port))?;
        listener.set_nonblocking(true)?;

        eprintln!(
            "[{}] TCP transport listening on port {}",
            local_id, listen_port
        );

        Ok(Self {
            local_id,
            connections: HashMap::default(),
            peer_addresses,
            paused_peers: HashSet::default(),
            message_buffer: HashMap::default(),
            incoming_rx,
            incoming_tx,
            listener: Some(listener),
            listen_port,
            temp_id_counter: AtomicU64::new(0),
        })
    }

    /// Spawn a reader thread for a connection. The node is already connected
    fn spawn_reader_thread(&self, peer_id: PeerId, stream: TcpStream) {
        let tx = self.incoming_tx.clone();
        let local_id = self.local_id.clone();

        thread::spawn(move || {
            let reader = BufReader::new(stream);
            let mut current_peer_id = peer_id;

            for line in reader.lines() {
                match line {
                    Ok(data) => {
                        // A message that will not parse used to vanish without
                        // a trace, which turns a serialization defect into a
                        // silent stall: the sender logs a successful send and
                        // the receiver logs nothing at all. Say so instead.
                        let parsed = match serde_json::from_str::<TransportMessage<O>>(&data) {
                            Ok(msg) => Some(msg),
                            Err(e) => {
                                eprintln!(
                                    "[{}] Dropping an unparseable message from {} ({} bytes): {}",
                                    local_id,
                                    current_peer_id,
                                    data.len(),
                                    e
                                );
                                None
                            }
                        };
                        if let Some(msg) = parsed {
                            // A Hello reveals the real identity behind an
                            // inbound connection, which was registered under a
                            // temporary id. Forward it under the *temporary*
                            // id so `try_recv` can rekey the stored connection,
                            // then adopt the real id for every later message.
                            //
                            // Renaming before the send would hide the temporary
                            // id from `try_recv`, leaving the connection keyed
                            // under `temp-conn-N` forever: the replica could
                            // then receive from that peer but never send to it.
                            let from_id = if let TransportMessage::Hello { ref id, .. } = msg {
                                let previous = current_peer_id.clone();
                                current_peer_id = id.clone();
                                previous
                            } else {
                                current_peer_id.clone()
                            };

                            if tx.send((from_id, msg)).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[{}] Read error on the link with {}: {}",
                            local_id, current_peer_id, e
                        );
                        break;
                    }
                }
            }

            eprintln!("[{}] Connection closed from {}", local_id, current_peer_id);
        });
    }

    /// Register a new connection with the transport
    ///
    /// This method unifies connection handling for both incoming and outgoing connections.
    ///
    /// # Arguments
    /// * `peer_id` - The peer ID (may be temporary for incoming connections)
    /// * `stream` - The TCP stream for this connection
    /// * `send_hello` - Whether to send a Hello message immediately (for outgoing connections)
    fn register_connection(
        &mut self,
        peer_id: PeerId,
        stream: TcpStream,
        send_hello: bool,
    ) -> TransportResult<()> {
        // Send hello if requested (for outgoing connections)
        if send_hello {
            let mut stream_clone = stream.try_clone()?;
            write_frame(
                &mut stream_clone,
                &TransportMessage::Hello::<O> {
                    id: self.local_id.clone(),
                    metadata: Some(self.listen_port.to_string()),
                },
            )?;
        }

        // Start reader thread
        self.spawn_reader_thread(peer_id.clone(), stream.try_clone()?);

        // Store connection
        self.connections.insert(peer_id, stream);

        Ok(())
    }
}

impl<O> CrdtTransport for DirectTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    type Op = O;

    fn local_id(&self) -> &PeerId {
        &self.local_id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        // Sends only if not paused
        if self.paused_peers.contains(peer) {
            eprintln!("[{}] Skipping send to paused peer {}", self.local_id, peer);
            return Ok(());
        }

        let stream = self
            .connections
            .get_mut(peer)
            .ok_or_else(|| TransportError::PeerNotFound(peer.clone()))?;

        write_frame(stream, &msg)
    }

    fn broadcast(&mut self, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        let peer_ids: Vec<PeerId> = self.connections.keys().cloned().collect();

        for peer_id in peer_ids {
            if self.paused_peers.contains(&peer_id) {
                continue;
            }

            if let Some(stream) = self.connections.get_mut(&peer_id) {
                if let Err(e) = write_frame(stream, &msg) {
                    eprintln!("[{}] Failed to send to {}: {}", self.local_id, peer_id, e);
                }
            }
        }

        Ok(())
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Self::Op>)>> {
        match self.incoming_rx.try_recv() {
            Ok((from_id, msg)) => {
                // Check if this is a Hello message
                if let TransportMessage::Hello { ref id, .. } = msg {
                    if from_id != *id && self.connections.contains_key(&from_id) {
                        // Remap it to the real peer ID now
                        if let Some(stream) = self.connections.remove(&from_id) {
                            eprintln!(
                                "[{}] Remapping connection: {} -> {}",
                                self.local_id, from_id, id
                            );
                            self.connections.insert(id.clone(), stream);
                        }
                        // Update from_id to the real peer ID for the rest of processing
                        return Ok(Some((id.clone(), msg)));
                    }
                }

                // If peer is paused, buffer the message
                if self.paused_peers.contains(&from_id) {
                    eprintln!(
                        "[{}] Buffering message from paused peer {}",
                        self.local_id, from_id
                    );
                    self.message_buffer
                        .entry(from_id)
                        .or_default()
                        .push_back(msg);
                    return Ok(None);
                }

                Ok(Some((from_id, msg)))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err(TransportError::Other("Channel disconnected".to_string()))
            }
        }
    }

    fn peers(&self) -> Vec<PeerInfo> {
        let mut peers = Vec::new();

        for (id, addr) in &self.peer_addresses {
            let status = if self.paused_peers.contains(id) {
                PeerStatus::Paused
            } else if self.connections.contains_key(id) {
                PeerStatus::Connected
            } else {
                PeerStatus::Disconnected
            };

            let buffered = self.message_buffer.get(id).map(|b| b.len()).unwrap_or(0);

            peers.push(PeerInfo {
                id: id.clone(),
                address: addr.clone(),
                status,
                buffered_messages: buffered,
            });
        }

        peers
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        self.connections.contains_key(peer) && !self.paused_peers.contains(peer)
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.peer_addresses.contains_key(peer) {
            self.paused_peers.insert(peer.clone());
            eprintln!(
                "[{}] Paused peer {} (messages will be buffered)",
                self.local_id, peer
            );
            Ok(())
        } else {
            Err(TransportError::PeerNotFound(peer.clone()))
        }
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.paused_peers.remove(peer) {
            eprintln!("[{}] Resumed peer {}", self.local_id, peer);
            Ok(())
        } else {
            Err(TransportError::Other(format!(
                "Peer {} was not paused",
                peer
            )))
        }
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        for peer_id in self.peer_addresses.keys() {
            self.paused_peers.insert(peer_id.clone());
        }
        eprintln!(
            "[{}] Paused all {} peers",
            self.local_id,
            self.peer_addresses.len()
        );
        Ok(())
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        self.paused_peers.clear();
        eprintln!("[{}] Resumed all peers", self.local_id);
        Ok(())
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.message_buffer.get(peer).map(|b| b.len()).unwrap_or(0)
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        let mut new_peers = Vec::new();

        // Check if listener exists first to avoid borrowing issues
        if self.listener.is_none() {
            return Ok(new_peers);
        }

        loop {
            let listener = self.listener.as_ref().unwrap();
            match listener.accept() {
                Ok((stream, addr)) => {
                    eprintln!("[{}] Accepted connection from {}", self.local_id, addr);
                    // Unique temporary ID to avoid collisions if multiple connections arrive
                    let temp_id = format!(
                        "temp-conn-{}",
                        self.temp_id_counter.fetch_add(1, Ordering::SeqCst)
                    );

                    // Register the connection without sending Hello
                    // (the remote peer will send Hello to identify themselves)
                    self.register_connection(temp_id.clone(), stream, false)?;
                    new_peers.push(temp_id);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(new_peers)
    }

    fn add_peer(&mut self, peer: PeerId, addr: String) -> bool {
        // Never overwrite our own entry: a bootnode roster that echoes this
        // replica back would otherwise make it dial itself.
        if peer == self.local_id {
            return false;
        }
        match self.peer_addresses.get(&peer) {
            Some(known) if *known == addr => false,
            _ => {
                self.peer_addresses.insert(peer, addr);
                true
            }
        }
    }

    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        let mut connected = Vec::new();

        for peer_id in self.peer_addresses.keys().cloned().collect::<Vec<_>>() {
            if self.connections.contains_key(&peer_id) {
                continue;
            }
            match self.connect_to(&peer_id) {
                Ok(()) => connected.push(peer_id),
                Err(e) => {
                    eprintln!(
                        "[{}] Failed to connect to {}: {}",
                        self.local_id, peer_id, e
                    );
                }
            }
        }

        Ok(connected)
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<O>> {
        self.message_buffer
            .remove(peer)
            .map(|b| b.into_iter().collect())
            .unwrap_or_default()
    }
}

impl<O> DirectTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Check if peer is paused
    pub fn is_paused(&self, peer: &PeerId) -> bool {
        self.paused_peers.contains(peer)
    }

    /// Dial exactly one peer, giving up after two seconds — see `DIAL_TIMEOUT`.
    ///
    /// Split out of [`CrdtTransport::connect_to_peers`] so that a caller which
    /// knows *when* a peer should be retried can drive the dials itself. That
    /// caller is [`crate::composite`], which owns the per-peer backoff; this
    /// keeps "how to dial one peer" here and "which peers, when" there.
    ///
    /// `Ok(())` also covers a peer that was already connected, because the
    /// question a caller is asking is whether there is a link now.
    pub fn connect_to(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.connections.contains_key(peer) {
            return Ok(());
        }
        let addr = self
            .peer_addresses
            .get(peer)
            .cloned()
            .ok_or_else(|| TransportError::PeerNotFound(peer.clone()))?;

        let stream = dial(&addr, DIAL_TIMEOUT)?;
        eprintln!("[{}] Connected to {} at {}", self.local_id, peer, addr);
        // Register the connection and send Hello to identify ourselves
        self.register_connection(peer.clone(), stream, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_status() {
        assert_eq!(PeerStatus::Connected, PeerStatus::Connected);
        assert_ne!(PeerStatus::Paused, PeerStatus::Connected);
    }
}
