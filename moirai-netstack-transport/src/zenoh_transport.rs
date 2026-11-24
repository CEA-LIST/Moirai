//! One Zenoh session per node: the transport of the NW.14 note (vault
//! `spec/Zenoh transport, peer-to-peer and router.md`, section 5). A node
//! that must hold a kernel's port before its peers are known adds a second,
//! listening-less session once it reads them (NW.30, `dial_peers`).
//!
//! Keys, under `<session>` = `SESSION_ID`:
//!
//! | Key expression | Put by |
//! |---|---|
//! | `moirai/<session>/all/<sender>` | `broadcast` while no peer is paused |
//! | `moirai/<session>/to/<recipient>/<sender>` | `send`, and `broadcast` while a peer is paused, once per live unpaused peer |
//! | `moirai/<session>/node/<peer>` | one liveliness token per node |
//! | `moirai/<session>/node/<peer>/<zid>` | one liveliness token per session of a node, `<zid>` its Zenoh id |
//!
//! Frames are [`TransportMessage`] in JSON, carried as they are: nothing here
//! reads a frame's variant, except the two it produces from presence, `Hello`
//! when a peer's token appears and `Goodbye` when it goes.
//!
//! What the backend relies on, and why:
//!
//! - Every put is `CongestionControl::Block`. Zenoh's default for a put,
//!   `Drop`, drops a frame on a congested link that stays up, and nothing in
//!   Moirai asks again until the next link event (note 5.4).
//! - Every put goes through a Publisher declared on its key, never through
//!   `Session::put`. A declared Publisher asks the router which subscribers
//!   it has for the key; once answered, a peer linked directly to the
//!   subscriber sends its frames there alone, where a `Session::put` also
//!   sends the router a copy (NW.16c Q5, NW.16d). The `all` Publisher is
//!   declared when the session opens; a peer's own one on the first frame
//!   for it, kept until the peer's last token goes.
//! - Samples go from the subscribers' callbacks into one unbounded channel
//!   that `try_recv` reads. Zenoh's default handler is a FIFO of 256 whose
//!   producer blocks when full, holding one of the receive threads every
//!   session of the process shares (note 5.2).
//! - The subscribers are declared before the own token, so a peer that
//!   answers the token at once finds them there (note 5.2).
//! - Pause flags and buffers are this transport's bookkeeping, with
//!   `DirectTransport`'s semantics; a pause touches no Zenoh link (note 5.6).
//!   A broadcast during a pause is one put per live unpaused peer, never one
//!   put on `all`, which the paused peer would also receive; a failed put
//!   fails the broadcast, as it fails a `send`.
//! - A peer's `Hello` stands for its arrival and is handled as a frame from
//!   it: kept in its buffer while it is paused.
//! - Zenoh reports a key declared by two sessions once, alive at the first
//!   declaration and gone at the last (note Z7), so a node restarted with
//!   the same id inside the old session's lease would go unseen on the node
//!   key alone. Each session also declares a token of its own, under its
//!   Zenoh id; one appearing beside another live one of the same peer is a
//!   successor, and yields a `Hello` again.

use std::collections::{BTreeSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};

use zenoh::bytes::ZBytes;
use zenoh::liveliness::LivelinessToken;
use zenoh::pubsub::{Publisher, Subscriber};
use zenoh::qos::{CongestionControl, Priority};
use zenoh::sample::{Locality, Sample, SampleKind};
use zenoh::{Config, Session, Wait};

use crate::HashMap;
use crate::transport::{
    CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportError, TransportMessage, TransportResult,
};

/// What the callbacks hand to `try_recv`, in the order Zenoh delivered it.
enum Inbound {
    /// A frame, and the sender named by its key's last chunk.
    Frame(PeerId, TransportMessage),
    /// A token of a peer appeared: its node token (`None`), or the token of
    /// one of its sessions (`Some(zid)`).
    Up(PeerId, Option<String>),
    /// A token of a peer went away.
    Down(PeerId, Option<String>),
}

/// A `CrdtTransport` over one Zenoh session, and a second listening-less one
/// once it dials the peers named after its first opened.
pub struct ZenohTransport {
    local_id: PeerId,
    session_id: String,
    session: Session,
    /// The listening-less session that dials the peers named after this one
    /// opened, when there is one (NW.30): it carries frames both ways, so a
    /// peer this session never dialled is reached and reaches back.
    dialer: Option<Session>,
    /// Held so the declarations live as long as the transport, on both
    /// sessions.
    _subscribers: Vec<Subscriber<()>>,
    _tokens: Vec<LivelinessToken>,
    /// Puts every broadcast made while no peer is paused, on
    /// `moirai/<session>/all/<local>`.
    all_publisher: Publisher<'static>,
    /// The same publisher on [`dialer`], once there is one: every put goes on
    /// both sessions, so the frame is seen at the port reported in
    /// `PORTS_FILE` and reaches a peer whose session this one never dialled.
    ///
    /// [`dialer`]: ZenohTransport::dialer
    dialed_all_publisher: Option<Publisher<'static>>,
    /// Puts every frame for one peer, on its `to_key`: declared on the first
    /// frame for it, dropped (undeclared) when its last token goes.
    publishers: HashMap<PeerId, Publisher<'static>>,
    /// The same, on [`dialer`].
    ///
    /// [`dialer`]: ZenohTransport::dialer
    dialed_publishers: HashMap<PeerId, Publisher<'static>>,
    /// The sender every subscriber of either session hands its sample to, so
    /// the [`dialer`] joins the one channel. Dropped once the dialer's
    /// subscribers are declared, and by [`close`], so the channel can
    /// disconnect.
    ///
    /// [`dialer`]: ZenohTransport::dialer
    /// [`close`]: ZenohTransport::close
    tx: Option<Sender<Inbound>>,
    incoming: Receiver<Inbound>,
    /// The `PEERS` ids, and every id seen by token since.
    known: BTreeSet<PeerId>,
    /// The ids with at least one live token, as `try_recv` has seen them so
    /// far.
    live: BTreeSet<PeerId>,
    /// The live tokens of each id in `live`: `None` for its node token,
    /// `Some(zid)` for a session's.
    tokens: HashMap<PeerId, BTreeSet<Option<String>>>,
    paused: BTreeSet<PeerId>,
    buffers: HashMap<PeerId, VecDeque<TransportMessage>>,
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

/// The peer, and the session if any, a token key names below `prefix`
/// (`moirai/<session>/node/`): `<peer>` or `<peer>/<zid>`.
fn token_owner(key: &str, prefix: &str) -> Option<(PeerId, Option<String>)> {
    let mut chunks = key.strip_prefix(prefix)?.split('/');
    let peer = chunks.next().filter(|peer| !peer.is_empty())?.to_string();
    match (chunks.next(), chunks.next()) {
        (None, _) => Some((peer, None)),
        (Some(zid), None) => Some((peer, Some(zid.to_string()))),
        (Some(_), Some(_)) => None,
    }
}

fn zenoh_error(what: &str, e: zenoh::Error) -> TransportError {
    TransportError::Other(format!("Zenoh: {what}: {e}"))
}

/// A Publisher on `key` with the settings of every put: `Block`, `Data`,
/// not express.
fn declare_publisher(session: &Session, key: String) -> TransportResult<Publisher<'static>> {
    session
        .declare_publisher(key.clone())
        .congestion_control(CongestionControl::Block)
        .priority(Priority::Data)
        .express(false)
        .wait()
        .map_err(|e| zenoh_error(&format!("declare a publisher on {key}"), e))
}

/// Put `payload` through `publisher`.
fn publish(publisher: &Publisher<'_>, payload: ZBytes) -> TransportResult<()> {
    publisher
        .put(payload)
        .wait()
        .map_err(|e| zenoh_error(&format!("put on {}", publisher.key_expr()), e))
}

/// The subscribers of one session that hand frames to `tx`: the broadcast key
/// and the frames addressed to `local_id`.
fn subscribe_frames(
    session: &Session,
    session_id: &str,
    local_id: &str,
    tx: &Sender<Inbound>,
) -> TransportResult<Vec<Subscriber<()>>> {
    let mut subscribers = Vec::new();
    for key in [
        format!("moirai/{session_id}/all/*"),
        format!("moirai/{session_id}/to/{local_id}/*"),
    ] {
        let tx: Sender<Inbound> = tx.clone();
        let me = local_id.to_string();
        let subscriber = session
            .declare_subscriber(&key)
            .allowed_origin(Locality::Remote)
            .callback(move |sample: Sample| {
                let from = last_chunk(sample.key_expr().as_str()).to_string();
                // A second session of the same node is remote to the first,
                // so its own frame would loop back and the replica would
                // refuse an event from its own replica.
                if from == me {
                    return;
                }
                match moirai_netstack::decode_frame(&sample.payload().to_bytes()) {
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
    Ok(subscribers)
}

/// The subscriber of one session that turns a peer token appearing or going
/// into an [`Inbound::Up`] or [`Inbound::Down`].
fn subscribe_presence(
    session: &Session,
    session_id: &str,
    local_id: &str,
    tx: &Sender<Inbound>,
) -> TransportResult<Subscriber<()>> {
    let node_prefix = format!("moirai/{session_id}/node/");
    let presence = format!("{node_prefix}**");
    let tx: Sender<Inbound> = tx.clone();
    let me = local_id.to_string();
    session
        .liveliness()
        .declare_subscriber(&presence)
        .history(true)
        .callback(move |sample: Sample| {
            let Some((peer, zid)) = token_owner(sample.key_expr().as_str(), &node_prefix) else {
                return;
            };
            if peer == me {
                return;
            }
            let _ = tx.send(match sample.kind() {
                SampleKind::Put => Inbound::Up(peer, zid),
                SampleKind::Delete => Inbound::Down(peer, zid),
            });
        })
        .wait()
        .map_err(|e| zenoh_error(&format!("subscribe to the tokens {presence}"), e))
}

/// The node's liveliness tokens on one session: this session's own, then the
/// node's, so that a peer answering them reaches the presence subscriber.
fn declare_tokens(
    session: &Session,
    session_id: &str,
    local_id: &str,
) -> TransportResult<Vec<LivelinessToken>> {
    let mut tokens = Vec::new();
    for key in [
        format!("moirai/{session_id}/node/{local_id}/{}", session.zid()),
        format!("moirai/{session_id}/node/{local_id}"),
    ] {
        let token = session
            .liveliness()
            .declare_token(&key)
            .wait()
            .map_err(|e| zenoh_error(&format!("declare the token {key}"), e))?;
        tokens.push(token);
    }
    Ok(tokens)
}

impl ZenohTransport {
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

        let mut subscribers = subscribe_frames(&session, session_id, &local_id, &tx)?;
        subscribers.push(subscribe_presence(&session, session_id, &local_id, &tx)?);

        let all_publisher =
            declare_publisher(&session, format!("moirai/{session_id}/all/{local_id}"))?;

        // Last, so that a peer answering these tokens reaches the
        // subscribers: this session's own, then the node's.
        let tokens = declare_tokens(&session, session_id, &local_id)?;

        let known = known_peers
            .into_iter()
            .filter(|peer| *peer != local_id)
            .collect();
        let transport = Self {
            local_id,
            session_id: session_id.to_string(),
            session,
            dialer: None,
            _subscribers: subscribers,
            _tokens: tokens,
            all_publisher,
            dialed_all_publisher: None,
            publishers: HashMap::default(),
            dialed_publishers: HashMap::default(),
            tx: Some(tx),
            incoming,
            known,
            live: BTreeSet::new(),
            tokens: HashMap::default(),
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

    /// Dial the peers named after [`open`] returned, in a second,
    /// listening-less session: Zenoh 1.10.1 dials no peer after a session
    /// opens, and the node may have to report the kernel's port before its
    /// peers are known (NW.30).
    ///
    /// Both sessions feed the one inbound channel and both put every frame, so
    /// a frame is seen at the reported port and reaches a peer whose session
    /// this one never dialled; both also declare the node's tokens, so a peer
    /// that only accepted this one knows the node. The receiver's replica
    /// discards the duplicate, as it does one from a relay.
    ///
    /// [`open`]: ZenohTransport::open
    pub fn dial_peers(&mut self, config: Config, known_peers: Vec<PeerId>) -> TransportResult<()> {
        if self.dialer.is_some() {
            return Err(TransportError::Other(
                "the peers were already dialled".to_string(),
            ));
        }
        let session = zenoh::open(config)
            .wait()
            .map_err(|e| zenoh_error("open the session that dials the peers", e))?;
        let tx = self.tx.clone().ok_or_else(|| {
            TransportError::Other(
                "the transport was closed before it dialled its peers".to_string(),
            )
        })?;
        let mut subscribers = subscribe_frames(&session, &self.session_id, &self.local_id, &tx)?;
        subscribers.push(subscribe_presence(
            &session,
            &self.session_id,
            &self.local_id,
            &tx,
        )?);
        let all_publisher = declare_publisher(
            &session,
            format!("moirai/{}/all/{}", self.session_id, self.local_id),
        )?;
        let tokens = declare_tokens(&session, &self.session_id, &self.local_id)?;
        self._subscribers.extend(subscribers);
        self._tokens.extend(tokens);
        let me = self.local_id.clone();
        self.known
            .extend(known_peers.into_iter().filter(|peer| *peer != me));
        self.dialed_all_publisher = Some(all_publisher);
        eprintln!(
            "[{}] Zenoh transport in session `{}`, dialling the peers it was told",
            self.local_id, self.session_id
        );
        self.dialer = Some(session);
        self.tx = None;
        Ok(())
    }

    /// Close the sessions now: their tokens go, and every later put fails
    /// with an error. The transport still answers every call, with an error
    /// where a put is needed; dropping it afterwards closes nothing more.
    /// The session that dialled the peers closes with it, so their tokens go
    /// too and a peer reads the node gone. The subscribers and the sender go
    /// with them, so the inbound channel disconnects.
    pub fn close(&mut self) -> TransportResult<()> {
        let main = self
            .session
            .close()
            .wait()
            .map_err(|e| zenoh_error("close the session", e));
        let dialer = self.dialer.as_ref().map(|dialer| {
            dialer
                .close()
                .wait()
                .map_err(|e| zenoh_error("close the session that dialled the peers", e))
        });
        self._subscribers.clear();
        self._tokens.clear();
        self.tx = None;
        main?;
        if let Some(dialer) = dialer {
            dialer?;
        }
        Ok(())
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

    /// The port the session listens on, read from its locators: the port the
    /// kernel chose after a bind to port 0. `None` when the session lists no
    /// locator, as it does on 0.0.0.0 in a namespace whose only address is
    /// loopback, which Zenoh leaves out.
    pub fn listen_port(&self) -> Option<u16> {
        self.session
            .info()
            .locators()
            .wait()
            .iter()
            .find_map(|locator| {
                locator
                    .address()
                    .as_str()
                    .parse::<std::net::SocketAddr>()
                    .ok()
                    .map(|addr| addr.port())
            })
    }

    /// Where a frame for `peer` alone goes.
    fn to_key(&self, peer: &str) -> String {
        format!("moirai/{}/to/{peer}/{}", self.session_id, self.local_id)
    }

    /// Put a frame for `peer` alone through its Publishers, declared now if
    /// this is the first frame for it since its arrival: on the first session
    /// and, when there is one, on the dialer.
    fn put_to(&mut self, peer: &PeerId, payload: ZBytes) -> TransportResult<()> {
        let key = self.to_key(peer);
        if !self.publishers.contains_key(peer) {
            let publisher = declare_publisher(&self.session, key.clone())?;
            self.publishers.insert(peer.clone(), publisher);
        }
        if !self.dialed_publishers.contains_key(peer)
            && let Some(session) = &self.dialer
        {
            let publisher = declare_publisher(session, key)?;
            self.dialed_publishers.insert(peer.clone(), publisher);
        }
        publish(&self.publishers[peer], payload.clone())?;
        if let Some(publisher) = self.dialed_publishers.get(peer) {
            publish(publisher, payload)?;
        }
        Ok(())
    }

    fn encode(msg: &TransportMessage) -> TransportResult<ZBytes> {
        Ok(ZBytes::from(serde_json::to_vec(msg)?))
    }

    /// Every peer this transport knows of, by `PEERS` or by token.
    fn all_peers(&self) -> BTreeSet<PeerId> {
        self.known.union(&self.live).cloned().collect()
    }

    /// Record a token of `peer` that appeared; answers whether it is an
    /// arrival: the peer's first live token, or a session of its own beside
    /// another live session of the peer (a successor with the same id).
    fn token_up(&mut self, peer: &PeerId, zid: Option<String>) -> bool {
        let tokens = self.tokens.entry(peer.clone()).or_default();
        let first = tokens.is_empty();
        let successor = zid.is_some() && tokens.iter().any(|t| t.is_some() && *t != zid);
        if !tokens.insert(zid) {
            return false;
        }
        self.known.insert(peer.clone());
        self.live.insert(peer.clone());
        first || successor
    }

    /// Record a token of `peer` that went; answers whether it was the
    /// peer's last, in which case its Publisher goes too, declared again on
    /// the first frame for it after it returns.
    fn token_down(&mut self, peer: &PeerId, zid: &Option<String>) -> bool {
        let Some(tokens) = self.tokens.get_mut(peer) else {
            return false;
        };
        if !tokens.remove(zid) || !tokens.is_empty() {
            return false;
        }
        self.tokens.remove(peer);
        self.publishers.remove(peer);
        self.dialed_publishers.remove(peer);
        self.live.remove(peer)
    }
}

impl CrdtTransport for ZenohTransport {
    fn local_id(&self) -> &PeerId {
        &self.local_id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        if self.paused.contains(peer) {
            return Ok(());
        }
        if !self.live.contains(peer) {
            return Err(TransportError::PeerNotFound(peer.clone()));
        }
        self.put_to(peer, Self::encode(&msg)?)
    }

    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        let payload = Self::encode(&msg)?;
        if self.paused.is_empty() {
            publish(&self.all_publisher, payload.clone())?;
            if let Some(publisher) = &self.dialed_all_publisher {
                publish(publisher, payload)?;
            }
            return Ok(());
        }
        // Every live unpaused peer is tried; the first failure is answered.
        let peers: Vec<PeerId> = self.live.difference(&self.paused).cloned().collect();
        let mut failed = None;
        for peer in &peers {
            if let Err(e) = self.put_to(peer, payload.clone()) {
                eprintln!("[{}] Failed to send to {peer}: {e}", self.local_id);
                failed.get_or_insert(e);
            }
        }
        failed.map_or(Ok(()), Err)
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        loop {
            let inbound = match self.incoming.try_recv() {
                Ok(inbound) => inbound,
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => {
                    return Err(TransportError::Other("Channel disconnected".to_string()));
                }
            };
            match inbound {
                Inbound::Up(peer, zid) => {
                    if self.token_up(&peer, zid) {
                        let hello = TransportMessage::Hello {
                            id: peer.clone(),
                            metadata: None,
                        };
                        if self.paused.contains(&peer) {
                            self.buffers.entry(peer).or_default().push_back(hello);
                        } else {
                            return Ok(Some((peer, hello)));
                        }
                    }
                }
                Inbound::Down(peer, zid) => {
                    if self.token_down(&peer, &zid) {
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

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage> {
        self.buffers.remove(peer).map(Vec::from).unwrap_or_default()
    }
}

impl Drop for ZenohTransport {
    /// Close both sessions, so the tokens go now, unless [`close`] already
    /// did. Nothing else holds a session: the callbacks hold only the
    /// channel's sender.
    ///
    /// [`close`]: ZenohTransport::close
    fn drop(&mut self) {
        if !self.session.is_closed()
            && let Err(e) = self.session.close().wait()
        {
            eprintln!("[{}] Closing the Zenoh session: {e}", self.local_id);
        }
        if let Some(dialer) = &self.dialer
            && !dialer.is_closed()
            && let Err(e) = dialer.close().wait()
        {
            eprintln!(
                "[{}] Closing the session that dialled the peers: {e}",
                self.local_id
            );
        }
    }
}
