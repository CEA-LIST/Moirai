//! One multiplexed session to a relay, for peers that cannot be dialled.
//!
//! The second realization of [`CrdtTransport`]. Where [`DirectTransport`] holds
//! a stream per peer, this holds **one** stream — to the relay — and addresses
//! peers by id inside an envelope on it. Everything is outbound, so a replica
//! behind NAT needs no inbound port.
//!
//! ```text
//! -> {"hello":{"session_id":"demo","replica_id":"a"}}   once, on connect
//! -> {"to":"b","from":"a","msg":<TransportMessage>}     send
//! -> {"to":["b","c"],"from":"a","msg":<TransportMessage>}  broadcast, one upload
//! <- {"to":"a","from":"b","msg":<TransportMessage>}     inbound
//! ```
//!
//! # What is *not* here
//!
//! No `Hello` rekeying. An inbound direct connection arrives anonymous and is
//! registered under a temporary id until a `Hello` reveals who it is; a relayed
//! frame carries `from`, so identity arrives with the first frame. Unverified
//! identity, which is C-D1 and not an oversight: the relay believes any client
//! claiming an id, so this transport can only believe what the relay tells it.
//!
//! No reconnect logic either. The session is dialled by whoever owns this —
//! [`crate::composite`] does it on every reconcile — because that owner is
//! already looping and a second retry clock here would compete with it.
//!
//! # A message longer than a frame (SI.13 D2)
//!
//! `moirai-relay` reads at most `MAX_FRAME_BYTES` (1 MiB) as one frame and
//! ends the session of a sender that writes more. A message whose frame
//! would pass [`MAX_WHOLE_FRAME_BYTES`] (a migration's cut, a delta-sync
//! batch, a state transfer) is therefore written as consecutive
//! [`TransportMessage::Chunk`] frames, each well under the relay's bound, and
//! the reader thread of the receiving session joins them back into the
//! message before handing it on, so the node above never sees a chunk. The
//! relay forwards the chunks as it forwards any frame, unparsed and in order
//! per sender; a chunk it drops for a slow reader leaves an incomplete set,
//! which the reader drops when the sender's next set starts, and which the
//! CRDT's catch-up repairs as it repairs any lost frame.
//!
//! [`DirectTransport`]: crate::direct_transport::DirectTransport

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::transport::{
    CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportError, TransportMessage, TransportResult,
    dial, write_frame,
};
use crate::{HashMap, HashSet};

/// The longest frame this transport writes whole: under the relay's
/// `MAX_FRAME_BYTES` (1 MiB), with room for the envelope around the message.
/// A longer message goes as chunks (see the module header).
const MAX_WHOLE_FRAME_BYTES: usize = 768 * 1024;

/// The most JSON text one chunk carries. Escaped into the chunk's `data`
/// string a byte becomes at most two (`"` and `\\` are the only characters
/// JSON text holds that need escaping), so a chunk frame stays near half the
/// relay's bound.
const CHUNK_BYTES: usize = 256 * 1024;

/// The longest message the reader joins chunks into: a bound on what one
/// sender can make this replica hold. Above the state-transfer ceiling on
/// the log, whose payload travels compressed, because a state transfer also
/// carries the donor's unstable suffix uncompressed, and a delta-sync batch
/// carries the same events.
const MAX_JOINED_BYTES: usize = 256 * 1024 * 1024;

/// The most every chunked message being joined holds together, whichever
/// senders they claim to come from: two of the longest.
const MAX_JOINING_BYTES: usize = 2 * MAX_JOINED_BYTES;

/// The most chunked messages being joined at once, whichever senders they
/// claim to come from: far more than the replicas of one session.
const MAX_JOINING_SETS: usize = 256;

/// How long dialling the relay may take.
///
/// Short, and for the same reason the direct dial is: a relay that is down must
/// cost one reconcile interval, not stall the replica's event loop.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// The handshake, matching `moirai-relay`'s `HelloFrame`.
#[derive(Debug, Serialize)]
struct HelloFrame<'a> {
    hello: Hello<'a>,
}

#[derive(Debug, Serialize)]
struct Hello<'a> {
    session_id: &'a str,
    replica_id: &'a str,
}

/// `"b"` or `["b","c"]` — the recipient forms the relay understands.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum To<'a> {
    One(&'a str),
    Many(&'a [PeerId]),
}

/// One frame on the way in. `to` is ignored: the relay only ever writes frames
/// addressed to this replica, and believing our own id back would add nothing.
#[derive(Debug, Deserialize)]
struct Inbound {
    from: PeerId,
    msg: TransportMessage,
}

/// A transport that reaches peers through a relay session.
pub struct RelayTransport {
    local_id: PeerId,
    /// `host:port` of the relay, as the bootnode advertised it.
    relay_addr: String,
    /// The write half. The reader thread owns a clone of the same socket.
    stream: TcpStream,
    /// Cleared by the reader thread when the session ends, and by a failed
    /// write. The owner re-dials on the strength of this rather than by probing.
    alive: Arc<AtomicBool>,
    /// Peers reachable through this session.
    ///
    /// Two ways in: the owner routes a peer here because dialling it failed, or
    /// a frame arrives from a peer — a replica we have *heard from* over the
    /// relay is by construction reachable over it, and that is what lets the
    /// pair work when only one side's roster has caught up.
    routed: HashSet<PeerId>,
    paused_peers: HashSet<PeerId>,
    message_buffer: HashMap<PeerId, VecDeque<TransportMessage>>,
    incoming_rx: Receiver<(PeerId, TransportMessage)>,
    /// The id of the next chunked message this session writes.
    next_chunked: u64,
}

impl RelayTransport {
    /// Dial the relay and complete the handshake.
    ///
    /// Fails, rather than retrying, when the relay is unreachable: the caller is
    /// already on a reconcile loop and the next pass will try again.
    pub fn connect(local_id: PeerId, session: String, relay_addr: String) -> TransportResult<Self> {
        let mut stream = dial(&relay_addr, CONNECT_TIMEOUT)?;
        write_frame(
            &mut stream,
            &HelloFrame {
                hello: Hello {
                    session_id: &session,
                    replica_id: &local_id,
                },
            },
        )?;

        let (incoming_tx, incoming_rx) = mpsc::channel();
        let alive = Arc::new(AtomicBool::new(true));
        spawn_reader_thread(
            stream.try_clone()?,
            incoming_tx,
            local_id.clone(),
            relay_addr.clone(),
            Arc::clone(&alive),
        );

        eprintln!("[{local_id}] relay session open to {relay_addr} in `{session}`");

        Ok(Self {
            local_id,
            relay_addr,
            stream,
            alive,
            routed: HashSet::default(),
            paused_peers: HashSet::default(),
            message_buffer: HashMap::default(),
            incoming_rx,
            next_chunked: 0,
        })
    }

    /// Whether the session is still usable.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    /// The endpoint this session was dialled at.
    pub fn relay_addr(&self) -> &str {
        &self.relay_addr
    }

    /// Start routing `peer` through this session. `true` when that is new.
    pub fn route(&mut self, peer: &PeerId) -> bool {
        if *peer == self.local_id {
            return false;
        }
        self.routed.insert(peer.clone())
    }

    /// Stop routing `peer` here, because a direct path has appeared.
    ///
    /// Not required for correctness — the composite prefers the direct route
    /// whatever this session thinks — but a peer left routed here receives every
    /// broadcast twice, once per provider, and the duplicate costs an upload in
    /// the direction the recipient list exists to spare.
    pub fn unroute(&mut self, peer: &PeerId) -> bool {
        self.routed.remove(peer)
    }

    /// Whether `peer` is currently reached through this session.
    pub fn routes(&self, peer: &PeerId) -> bool {
        self.routed.contains(peer)
    }

    pub fn is_paused(&self, peer: &PeerId) -> bool {
        self.paused_peers.contains(peer)
    }

    /// One frame carrying `msg` to every recipient in `to`.
    ///
    /// The array form is why this exists as one call rather than a loop over
    /// [`CrdtTransport::send`]: N recipients cost one upload, which is the
    /// direction that is scarce on a domestic connection.
    ///
    /// A message whose frame would pass [`MAX_WHOLE_FRAME_BYTES`] goes as
    /// chunk frames to the same recipients, one after the other.
    fn write_to(&mut self, to: To<'_>, msg: &TransportMessage) -> TransportResult<()> {
        let text = serde_json::to_string(msg)?;
        let result = if text.len() <= MAX_WHOLE_FRAME_BYTES {
            self.write_raw(&to, &text)
        } else {
            self.write_chunks(&to, &text)
        };
        if result.is_err() {
            // The session is gone. Say so once, here, rather than making every
            // caller infer it from an error it is about to log anyway.
            self.alive.store(false, Ordering::Relaxed);
        }
        result
    }

    /// One frame whose `msg` is `text`, a message's JSON already written:
    /// `{"to": ..., "from": ..., "msg": ...}`, the envelope `moirai-relay`
    /// reads, without serializing the message a second time.
    fn write_raw(&mut self, to: &To<'_>, text: &str) -> TransportResult<()> {
        let to = serde_json::to_string(to)?;
        let from = serde_json::to_string(&self.local_id)?;
        writeln!(
            self.stream,
            "{{\"to\":{to},\"from\":{from},\"msg\":{text}}}"
        )?;
        self.stream.flush()?;
        Ok(())
    }

    /// `text` as consecutive chunk frames, each holding at most
    /// [`CHUNK_BYTES`] of it, cut on character boundaries.
    fn write_chunks(&mut self, to: &To<'_>, text: &str) -> TransportResult<()> {
        let pieces = split_on_chars(text, CHUNK_BYTES);
        let id = self.next_chunked;
        self.next_chunked += 1;
        let count = u32::try_from(pieces.len())
            .map_err(|_| TransportError::Other("a message too long to chunk".to_string()))?;
        for (index, piece) in pieces.into_iter().enumerate() {
            let chunk = TransportMessage::Chunk {
                id,
                index: index as u32,
                count,
                data: piece.to_string(),
            };
            let chunk = serde_json::to_string(&chunk)?;
            self.write_raw(to, &chunk)?;
        }
        Ok(())
    }
}

/// `text` cut into pieces of at most `most` bytes, each ending on a
/// character boundary (a character is at most four bytes, so `most` must be
/// at least four).
fn split_on_chars(text: &str, most: usize) -> Vec<&str> {
    let mut pieces = Vec::with_capacity(text.len() / most + 1);
    let mut rest = text;
    while !rest.is_empty() {
        let mut end = most.min(rest.len());
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let (piece, tail) = rest.split_at(end);
        pieces.push(piece);
        rest = tail;
    }
    pieces
}

/// One sender's chunked message as the reader joins it.
struct Joining {
    id: u64,
    count: u32,
    next: u32,
    text: String,
    /// When a chunk last joined this set, as the joiner's count of chunks
    /// taken: what the oldest open set is told by.
    touched: u64,
}

/// The chunked messages being joined, one per sender: the relay keeps one
/// sender's frames in order, so a sender has at most one set open.
///
/// The `from` of a frame is the sender's own claim, so one relay client can
/// open sets under many ids. What all open sets hold together is therefore
/// bounded too, by [`MAX_JOINING_BYTES`]: a chunk that would pass it first
/// drops the sets left longest without a chunk, and is dropped with its own
/// set if that is not enough. A set past [`MAX_JOINING_SETS`] likewise
/// drops the set left longest without a chunk.
#[derive(Default)]
struct Joiner {
    open: HashMap<PeerId, Joining>,
    /// The bytes every open set holds, together.
    held: usize,
    /// The chunks taken so far.
    taken: u64,
}

impl Joiner {
    /// Take one chunk from `from`; answer the whole message's text once its
    /// last chunk is in. A chunk out of turn drops the set it belongs to.
    fn take(
        &mut self,
        local_id: &str,
        from: &PeerId,
        id: u64,
        index: u32,
        count: u32,
        data: String,
    ) -> Option<String> {
        self.taken += 1;
        if index == 0 {
            if let Some(dropped) = self.drop_set(from) {
                eprintln!(
                    "[{local_id}] dropping an incomplete chunked message from {from} ({} of {} \
                     chunks)",
                    dropped.next, dropped.count
                );
            }
            while self.open.len() >= MAX_JOINING_SETS {
                self.drop_oldest(local_id, from);
            }
            self.open.insert(
                from.clone(),
                Joining {
                    id,
                    count,
                    next: 0,
                    text: String::new(),
                    touched: self.taken,
                },
            );
        }
        let joining = self.open.get(from)?;
        if joining.id != id || joining.next != index || joining.count != count {
            eprintln!(
                "[{local_id}] dropping an incomplete chunked message from {from}: chunk {index} \
                 of message {id} came when chunk {} of message {} was due",
                joining.next, joining.id
            );
            self.drop_set(from);
            return None;
        }
        if joining.text.len() + data.len() > MAX_JOINED_BYTES {
            eprintln!(
                "[{local_id}] dropping a chunked message from {from}: it passes {MAX_JOINED_BYTES} \
                 bytes"
            );
            self.drop_set(from);
            return None;
        }
        if !self.make_room(local_id, from, data.len()) {
            eprintln!(
                "[{local_id}] dropping a chunked message from {from}: the chunked messages \
                 being joined would hold more than {MAX_JOINING_BYTES} bytes"
            );
            self.drop_set(from);
            return None;
        }
        let joining = self.open.get_mut(from)?;
        joining.text.push_str(&data);
        joining.next += 1;
        joining.touched = self.taken;
        self.held += data.len();
        if joining.next < joining.count {
            return None;
        }
        self.drop_set(from).map(|joined| joined.text)
    }

    /// Drop the sets of other senders left longest without a chunk until
    /// `more` bytes fit under [`MAX_JOINING_BYTES`]; `false` when they do
    /// not fit even with every other set dropped.
    fn make_room(&mut self, local_id: &str, from: &PeerId, more: usize) -> bool {
        while self.held + more > MAX_JOINING_BYTES {
            if !self.drop_oldest(local_id, from) {
                return false;
            }
        }
        true
    }

    /// Drop the set of a sender other than `from` left longest without a
    /// chunk; `false` when there is none.
    fn drop_oldest(&mut self, local_id: &str, from: &PeerId) -> bool {
        let oldest = self
            .open
            .iter()
            .filter(|(sender, _)| *sender != from)
            .min_by_key(|(_, joining)| joining.touched)
            .map(|(sender, _)| sender.clone());
        let Some(oldest) = oldest else {
            return false;
        };
        if let Some(dropped) = self.drop_set(&oldest) {
            eprintln!(
                "[{local_id}] dropping an incomplete chunked message from {oldest} ({} of {} \
                 chunks) to make room for one from {from}",
                dropped.next, dropped.count
            );
        }
        true
    }

    /// Remove `from`'s open set, no longer counting what it held.
    fn drop_set(&mut self, from: &PeerId) -> Option<Joining> {
        let dropped = self.open.remove(from)?;
        self.held -= dropped.text.len();
        Some(dropped)
    }
}

/// Read the relay session until it ends, funnelling frames into `tx`.
///
/// Deliberately the same shape as `DirectTransport::spawn_reader_thread`,
/// including reporting an unparseable line instead of dropping it: a
/// serialisation defect that vanishes here looks like a stall, with the sender
/// logging a successful send and the receiver logging nothing at all.
fn spawn_reader_thread(
    stream: TcpStream,
    tx: Sender<(PeerId, TransportMessage)>,
    local_id: PeerId,
    relay_addr: String,
    alive: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        let mut joiner = Joiner::default();
        for line in reader.lines() {
            match line {
                Ok(data) => match moirai_netstack::decode_json::<Inbound>(data.as_bytes()) {
                    Ok(Inbound {
                        from,
                        msg:
                            TransportMessage::Chunk {
                                id,
                                index,
                                count,
                                data,
                            },
                    }) => {
                        let Some(text) = joiner.take(&local_id, &from, id, index, count, data)
                        else {
                            continue;
                        };
                        match moirai_netstack::decode_frame(text.as_bytes()) {
                            Ok(msg) => {
                                if tx.send((from, msg)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => eprintln!(
                                "[{}] Dropping a joined message from {} that will not parse ({} \
                                 bytes): {}",
                                local_id,
                                from,
                                text.len(),
                                e
                            ),
                        }
                    }
                    Ok(frame) => {
                        if tx.send((frame.from, frame.msg)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[{}] Dropping an unparseable relay frame ({} bytes): {}",
                            local_id,
                            data.len(),
                            e
                        );
                    }
                },
                Err(e) => {
                    eprintln!("[{}] Read error on the relay session: {}", local_id, e);
                    break;
                }
            }
        }
        alive.store(false, Ordering::Relaxed);
        eprintln!("[{}] Relay session to {} closed", local_id, relay_addr);
    });
}

impl CrdtTransport for RelayTransport {
    fn local_id(&self) -> &PeerId {
        &self.local_id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        if self.paused_peers.contains(peer) {
            return Ok(());
        }
        if !self.routed.contains(peer) {
            return Err(TransportError::PeerNotFound(peer.clone()));
        }
        self.write_to(To::One(peer), &msg)
    }

    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        // Sorted, so the frame is reproducible between runs and a log or a
        // capture can be compared. `HashSet` iteration order is not.
        let mut recipients: Vec<PeerId> = self
            .routed
            .iter()
            .filter(|peer| !self.paused_peers.contains(*peer))
            .cloned()
            .collect();
        if recipients.is_empty() {
            return Ok(());
        }
        recipients.sort_unstable();
        self.write_to(To::Many(&recipients), &msg)
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        match self.incoming_rx.try_recv() {
            Ok((from, msg)) => {
                // Hearing from a peer *is* the evidence that it is reachable
                // here, and it is the only evidence available when this
                // replica's roster has not caught up with the sender's.
                self.route(&from);

                if self.paused_peers.contains(&from) {
                    self.message_buffer.entry(from).or_default().push_back(msg);
                    return Ok(None);
                }
                Ok(Some((from, msg)))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(TransportError::Other(
                "the relay session's reader thread is gone".to_string(),
            )),
        }
    }

    fn peers(&self) -> Vec<PeerInfo> {
        let mut peers: Vec<PeerInfo> = self
            .routed
            .iter()
            .map(|id| PeerInfo {
                id: id.clone(),
                // Not an address anybody dials — it is where the frames go. Said
                // this way so that `/api/peers` shows a relayed peer as relayed
                // without a new field on `PeerInfo`.
                address: format!("relay:{}", self.relay_addr),
                status: if self.paused_peers.contains(id) {
                    PeerStatus::Paused
                } else if self.is_alive() {
                    // Connected on purpose, not a new `Relayed` status: the node
                    // and every scenario read `Connected` as "traffic flows",
                    // and it does. Which route it took is surfaced separately.
                    PeerStatus::Connected
                } else {
                    PeerStatus::Disconnected
                },
                buffered_messages: self.message_buffer.get(id).map(|b| b.len()).unwrap_or(0),
            })
            .collect();
        peers.sort_by(|a, b| a.id.cmp(&b.id));
        peers
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        self.routed.contains(peer) && !self.paused_peers.contains(peer) && self.is_alive()
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.routed.contains(peer) {
            self.paused_peers.insert(peer.clone());
            Ok(())
        } else {
            Err(TransportError::PeerNotFound(peer.clone()))
        }
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.paused_peers.remove(peer) {
            Ok(())
        } else {
            Err(TransportError::Other(format!(
                "Peer {} was not paused",
                peer
            )))
        }
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        for peer in &self.routed {
            self.paused_peers.insert(peer.clone());
        }
        Ok(())
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        self.paused_peers.clear();
        Ok(())
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.message_buffer.get(peer).map(|b| b.len()).unwrap_or(0)
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        // Nothing to accept: the one connection this transport has is outbound,
        // which is the entire reason it exists.
        Ok(Vec::new())
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage> {
        self.message_buffer
            .remove(peer)
            .map(|b| b.into_iter().collect())
            .unwrap_or_default()
    }
}
