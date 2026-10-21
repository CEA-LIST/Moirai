//! One Zenoh session per node: the transport of the NW.14 note (vault
//! `spec/Zenoh transport, peer-to-peer and router.md`, section 5).
//!
//! Keys, under `<session>` = `SESSION_ID`:
//!
//! | Key expression | Put by |
//! |---|---|
//! | `moirai/<session>/all/<sender>` | `broadcast` while no peer is paused |
//! | `moirai/<session>/to/<recipient>/<sender>` | `send`, and `broadcast` while a peer is paused, once per live unpaused peer |
//! | `moirai/<session>/node/<peer>` | one liveliness token per node |
//!
//! Frames are `TransportMessage` in JSON, carried as they are: nothing here
//! reads a frame's variant, except the two it produces from presence, `Hello`
//! when a peer's token appears and `Goodbye` when it goes, so the backend
//! compiles unchanged on every branch whatever variants it adds (note 3.1).
//!
//! What the backend relies on, and why:
//!
//! - Every put is `CongestionControl::Block`. Zenoh's default for a put,
//!   `Drop`, drops a frame on a congested link that stays up, and nothing in
//!   Moirai asks again until the next link event (note 5.4).
//! - Samples go from the subscribers' callbacks into one unbounded channel
//!   that `try_recv` reads. Zenoh's default handler is a FIFO of 256 whose
//!   producer blocks when full, holding one of the receive threads every
//!   session of the process shares (note 5.2).
//! - The subscribers are declared before the own token, so a peer that
//!   answers the token at once finds them there (note 5.2).
//! - Pause flags and buffers are this transport's bookkeeping, with
//!   `DirectTransport`'s semantics; a pause touches no Zenoh link (note 5.6).
//!   A broadcast during a pause is one put per live unpaused peer, never one
//!   put on `all`, which the paused peer would also receive.

use std::collections::{BTreeSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};

use serde::{de::DeserializeOwned, Serialize};
use zenoh::bytes::ZBytes;
use zenoh::liveliness::LivelinessToken;
use zenoh::pubsub::Subscriber;
use zenoh::qos::{CongestionControl, Priority};
use zenoh::sample::{Locality, Sample, SampleKind};
use zenoh::{Config, Session, Wait};

use crate::transport::{
    CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportError, TransportMessage, TransportResult,
};
use crate::HashMap;

/// What the callbacks hand to `try_recv`, in the order Zenoh delivered it.
enum Inbound<O>
where
    O: Serialize + DeserializeOwned,
{
    /// A frame, and the sender named by its key's last chunk.
    Frame(PeerId, TransportMessage<O>),
    /// A peer's token appeared.
    Up(PeerId),
    /// A peer's token went away.
    Down(PeerId),
}

/// A `CrdtTransport` over one Zenoh session.
pub struct ZenohTransport<O>
where
    O: Serialize + DeserializeOwned,
{
    local_id: PeerId,
    session_id: String,
    session: Session,
    /// Held so the declarations live as long as the transport.
    _subscribers: Vec<Subscriber<()>>,
    _token: LivelinessToken,
    incoming: Receiver<Inbound<O>>,
    /// The `PEERS` ids, and every id seen by token since.
    known: BTreeSet<PeerId>,
    /// The ids whose token is live, as `try_recv` has seen them so far.
    live: BTreeSet<PeerId>,
    paused: BTreeSet<PeerId>,
    buffers: HashMap<PeerId, VecDeque<TransportMessage<O>>>,
}

/// Whether `id` can stand as one chunk of a key expression: non-empty, and
/// none of `/`, `*`, `$`, `?`, `#` (note 5.1).
fn is_key_chunk(id: &str) -> bool {
    !id.is_empty() && !id.contains(['/', '*', '$', '?', '#'])
}

/// The last chunk of a key expression.
fn last_chunk(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or_default()
}

fn zenoh_error(what: &str, e: zenoh::Error) -> TransportError {
    TransportError::Other(format!("Zenoh: {what}: {e}"))
}

impl<O> ZenohTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Open a session over `config` as given, as `local_id` in the Moirai
    /// session `session_id`, knowing `known_peers` (the `PEERS` ids).
    ///
    /// Refuses an id that is not a key chunk, naming the variable it comes
    /// from, rather than encoding it.
    pub fn open(
        local_id: PeerId,
        session_id: &str,
        known_peers: Vec<PeerId>,
        config: Config,
    ) -> TransportResult<Self> {
        if !is_key_chunk(&local_id) {
            return Err(TransportError::Other(format!(
                "REPLICA_ID `{local_id}` cannot be a Zenoh key chunk: it must be non-empty and \
                 contain none of / * $ ? #"
            )));
        }
        if !is_key_chunk(session_id) {
            return Err(TransportError::Other(format!(
                "SESSION_ID `{session_id}` cannot be a Zenoh key chunk: it must be non-empty and \
                 contain none of / * $ ? #"
            )));
        }

        let session = zenoh::open(config)
            .wait()
            .map_err(|e| zenoh_error("open a session", e))?;
        let (tx, incoming) = mpsc::channel();

        let mut subscribers = Vec::new();
        for key in [
            format!("moirai/{session_id}/all/*"),
            format!("moirai/{session_id}/to/{local_id}/*"),
        ] {
            let tx: Sender<Inbound<O>> = tx.clone();
            let me = local_id.clone();
            let subscriber = session
                .declare_subscriber(&key)
                .allowed_origin(Locality::Remote)
                .callback(move |sample: Sample| {
                    let from = last_chunk(sample.key_expr().as_str()).to_string();
                    match serde_json::from_slice::<TransportMessage<O>>(
                        &sample.payload().to_bytes(),
                    ) {
                        Ok(frame) => {
                            let _ = tx.send(Inbound::Frame(from, frame));
                        }
                        Err(e) => eprintln!(
                            "[{me}] Dropping an unparseable frame on {} ({} bytes): {e}",
                            sample.key_expr(),
                            sample.payload().len()
                        ),
                    }
                })
                .wait()
                .map_err(|e| zenoh_error(&format!("subscribe to {key}"), e))?;
            subscribers.push(subscriber);
        }

        let presence = format!("moirai/{session_id}/node/*");
        let me = local_id.clone();
        let subscriber = session
            .liveliness()
            .declare_subscriber(&presence)
            .history(true)
            .callback(move |sample: Sample| {
                let peer = last_chunk(sample.key_expr().as_str()).to_string();
                if peer == me {
                    return;
                }
                let _ = tx.send(match sample.kind() {
                    SampleKind::Put => Inbound::Up(peer),
                    SampleKind::Delete => Inbound::Down(peer),
                });
            })
            .wait()
            .map_err(|e| zenoh_error(&format!("subscribe to the tokens {presence}"), e))?;
        subscribers.push(subscriber);

        // Last, so that a peer answering this token reaches the subscribers.
        let token = session
            .liveliness()
            .declare_token(format!("moirai/{session_id}/node/{local_id}"))
            .wait()
            .map_err(|e| zenoh_error("declare the token", e))?;

        let known = known_peers
            .into_iter()
            .filter(|peer| *peer != local_id)
            .collect();
        let transport = Self {
            local_id,
            session_id: session_id.to_string(),
            session,
            _subscribers: subscribers,
            _token: token,
            incoming,
            known,
            live: BTreeSet::new(),
            paused: BTreeSet::new(),
            buffers: HashMap::default(),
        };
        eprintln!(
            "[{}] Zenoh transport in session `{session_id}`, listening on {:?}",
            transport.local_id,
            transport.locators()
        );
        Ok(transport)
    }

    /// The session's listening locators as Zenoh reports them, with the real
    /// port after a bind to port 0, for example `tcp/127.0.0.1:41234`.
    pub fn locators(&self) -> Vec<String> {
        self.session
            .info()
            .locators()
            .wait()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Where a broadcast goes while no peer is paused.
    fn all_key(&self) -> String {
        format!("moirai/{}/all/{}", self.session_id, self.local_id)
    }

    /// Where a frame for `peer` alone goes.
    fn to_key(&self, peer: &str) -> String {
        format!("moirai/{}/to/{peer}/{}", self.session_id, self.local_id)
    }

    fn put(&self, key: String, payload: ZBytes) -> TransportResult<()> {
        self.session
            .put(&key, payload)
            .congestion_control(CongestionControl::Block)
            .priority(Priority::Data)
            .express(false)
            .wait()
            .map_err(|e| zenoh_error(&format!("put on {key}"), e))
    }

    fn encode(msg: &TransportMessage<O>) -> TransportResult<ZBytes> {
        Ok(ZBytes::from(serde_json::to_vec(msg)?))
    }

    /// Every peer this transport knows of, by `PEERS` or by token.
    fn all_peers(&self) -> BTreeSet<PeerId> {
        self.known.union(&self.live).cloned().collect()
    }
}

impl<O> CrdtTransport for ZenohTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    type Op = O;

    fn local_id(&self) -> &PeerId {
        &self.local_id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<O>) -> TransportResult<()> {
        if self.paused.contains(peer) {
            return Ok(());
        }
        if !self.live.contains(peer) {
            return Err(TransportError::PeerNotFound(peer.clone()));
        }
        self.put(self.to_key(peer), Self::encode(&msg)?)
    }

    fn broadcast(&mut self, msg: TransportMessage<O>) -> TransportResult<()> {
        let payload = Self::encode(&msg)?;
        if self.paused.is_empty() {
            return self.put(self.all_key(), payload);
        }
        for peer in self.live.difference(&self.paused) {
            if let Err(e) = self.put(self.to_key(peer), payload.clone()) {
                eprintln!("[{}] Failed to send to {peer}: {e}", self.local_id);
            }
        }
        Ok(())
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<O>)>> {
        loop {
            let inbound = match self.incoming.try_recv() {
                Ok(inbound) => inbound,
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => {
                    return Err(TransportError::Other("Channel disconnected".to_string()))
                }
            };
            match inbound {
                Inbound::Up(peer) => {
                    self.known.insert(peer.clone());
                    if self.live.insert(peer.clone()) {
                        let hello = TransportMessage::Hello {
                            id: peer.clone(),
                            metadata: None,
                        };
                        return Ok(Some((peer, hello)));
                    }
                }
                Inbound::Down(peer) => {
                    if self.live.remove(&peer) {
                        let goodbye = TransportMessage::Goodbye { id: peer.clone() };
                        return Ok(Some((peer, goodbye)));
                    }
                }
                Inbound::Frame(from, frame) => {
                    if self.paused.contains(&from) {
                        self.buffers.entry(from).or_default().push_back(frame);
                    } else {
                        return Ok(Some((from, frame)));
                    }
                }
            }
        }
    }

    fn peers(&self) -> Vec<PeerInfo> {
        self.all_peers()
            .into_iter()
            .map(|id| {
                let status = if self.paused.contains(&id) {
                    PeerStatus::Paused
                } else if self.live.contains(&id) {
                    PeerStatus::Connected
                } else {
                    PeerStatus::Disconnected
                };
                PeerInfo {
                    address: format!("moirai/{}/node/{id}", self.session_id),
                    buffered_messages: self.buffered_count(&id),
                    id,
                    status,
                }
            })
            .collect()
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        self.live.contains(peer) && !self.paused.contains(peer)
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.known.contains(peer) || self.live.contains(peer) {
            self.paused.insert(peer.clone());
            Ok(())
        } else {
            Err(TransportError::PeerNotFound(peer.clone()))
        }
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if self.paused.remove(peer) {
            Ok(())
        } else {
            Err(TransportError::Other(format!("Peer {peer} was not paused")))
        }
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        self.paused.extend(self.all_peers());
        Ok(())
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        self.paused.clear();
        Ok(())
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.buffers.get(peer).map_or(0, VecDeque::len)
    }

    /// Nothing to accept: Zenoh accepts its own links.
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(Vec::new())
    }

    /// `zenoh` for every connected peer, so `/api/metrics` keeps its shape.
    fn routes(&self) -> Vec<(PeerId, &'static str)> {
        self.live
            .iter()
            .filter(|peer| !self.paused.contains(*peer))
            .map(|peer| (peer.clone(), "zenoh"))
            .collect()
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<O>> {
        self.buffers.remove(peer).map(Vec::from).unwrap_or_default()
    }
}

impl<O> Drop for ZenohTransport<O>
where
    O: Serialize + DeserializeOwned,
{
    /// Close the session, so the token goes now. Nothing else holds the
    /// session: the callbacks hold only the channel's sender.
    fn drop(&mut self) {
        if let Err(e) = self.session.close().wait() {
            eprintln!("[{}] Closing the Zenoh session: {e}", self.local_id);
        }
    }
}
