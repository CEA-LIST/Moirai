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
//! # The shape is deliberately the direct transport's
//!
//! A reader thread per stream funnelling into an `mpsc::Receiver` that
//! `try_recv` drains, which is exactly [`DirectTransport`]'s pattern and not a
//! new one. There is one stream instead of N, so there is one thread instead of
//! N; nothing else about the shape changes, and the node above the seam cannot
//! tell the difference — which is the whole point of the seam.
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
//! [`DirectTransport`]: crate::direct_transport::DirectTransport

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::transport::{
    write_frame, CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportError, TransportMessage,
    TransportResult,
};
use crate::{HashMap, HashSet};

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

/// One frame on the way out.
#[derive(Debug, Serialize)]
#[serde(bound(serialize = "O: Serialize"))]
struct Outbound<'a, O>
where
    O: Serialize + DeserializeOwned,
{
    to: To<'a>,
    from: &'a str,
    msg: &'a TransportMessage<O>,
}

/// One frame on the way in. `to` is ignored: the relay only ever writes frames
/// addressed to this replica, and believing our own id back would add nothing.
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "O: DeserializeOwned"))]
struct Inbound<O>
where
    O: Serialize + DeserializeOwned,
{
    from: PeerId,
    msg: TransportMessage<O>,
}

/// A transport that reaches peers through a relay session.
pub struct RelayTransport<O>
where
    O: Serialize + DeserializeOwned,
{
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
    message_buffer: HashMap<PeerId, VecDeque<TransportMessage<O>>>,
    incoming_rx: Receiver<(PeerId, TransportMessage<O>)>,
}

impl<O> RelayTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Dial the relay and complete the handshake.
    ///
    /// Fails, rather than retrying, when the relay is unreachable: the caller is
    /// already on a reconcile loop and the next pass will try again.
    pub fn connect(local_id: PeerId, session: String, relay_addr: String) -> TransportResult<Self> {
        let target = resolve(&relay_addr)?;
        let mut stream = TcpStream::connect_timeout(&target, CONNECT_TIMEOUT)?;
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
    fn write_to(&mut self, to: To<'_>, msg: &TransportMessage<O>) -> TransportResult<()> {
        let frame = Outbound {
            to,
            from: &self.local_id,
            msg,
        };
        let result = write_frame(&mut self.stream, &frame);
        if result.is_err() {
            // The session is gone. Say so once, here, rather than making every
            // caller infer it from an error it is about to log anyway.
            self.alive.store(false, Ordering::Relaxed);
        }
        result
    }
}

/// Resolve `host:port` to one address.
///
/// [`TcpStream::connect_timeout`] takes a `SocketAddr`, not a `ToSocketAddrs`,
/// and the timeout is what keeps a dead relay from stalling the event loop — so
/// the resolution happens here instead of being done for us.
fn resolve(addr: &str) -> TransportResult<std::net::SocketAddr> {
    use std::net::ToSocketAddrs;
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| TransportError::Other(format!("`{addr}` resolved to no address")))
}

/// Read the relay session until it ends, funnelling frames into `tx`.
///
/// Deliberately the same shape as `DirectTransport::spawn_reader_thread`,
/// including reporting an unparseable line instead of dropping it: a
/// serialisation defect that vanishes here looks like a stall, with the sender
/// logging a successful send and the receiver logging nothing at all.
fn spawn_reader_thread<O>(
    stream: TcpStream,
    tx: Sender<(PeerId, TransportMessage<O>)>,
    local_id: PeerId,
    relay_addr: String,
    alive: Arc<AtomicBool>,
) where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        for line in reader.lines() {
            match line {
                Ok(data) => match serde_json::from_str::<Inbound<O>>(&data) {
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

impl<O> CrdtTransport for RelayTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    type Op = O;

    fn local_id(&self) -> &PeerId {
        &self.local_id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        if self.paused_peers.contains(peer) {
            return Ok(());
        }
        if !self.routed.contains(peer) {
            return Err(TransportError::PeerNotFound(peer.clone()));
        }
        self.write_to(To::One(peer), &msg)
    }

    fn broadcast(&mut self, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
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

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Self::Op>)>> {
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

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<O>> {
        self.message_buffer
            .remove(peer)
            .map(|b| b.into_iter().collect())
            .unwrap_or_default()
    }
}

/// A stand-in relay, for this module's tests and the composite's.
///
/// Accepts one session, remembers every line it was sent, and can write lines
/// back. Enough to pin the wire format and the reader thread without
/// `moirai-relay` in the loop — that crate has its own tests, and a test that
/// needed both would be testing the pair rather than either.
#[cfg(test)]
pub(crate) mod tests_support {
    use std::io::{BufRead, BufReader, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    pub(crate) struct FakeRelay {
        addr: String,
        lines: Receiver<String>,
        to_client: Sender<String>,
        /// The accepted session, so a test can hang up on purpose. Dropping this
        /// struct would not: the accept thread is blocked in a read and owns the
        /// socket, so nothing would close until a line arrived.
        session: Arc<Mutex<Option<TcpStream>>>,
    }

    impl FakeRelay {
        pub(crate) fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind a fake relay");
            let addr = listener.local_addr().expect("read the fake relay's port");
            let (line_tx, lines) = mpsc::channel();
            let (to_client, outbound) = mpsc::channel::<String>();
            let session: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
            let accepted = Arc::clone(&session);

            thread::spawn(move || {
                let (stream, _) = listener.accept().expect("accept the session");
                *accepted.lock().expect("the session slot") =
                    Some(stream.try_clone().expect("split the stream"));
                let mut write_half = stream.try_clone().expect("split the stream");
                thread::spawn(move || {
                    while let Ok(line) = outbound.recv() {
                        if writeln!(write_half, "{line}").is_err() || write_half.flush().is_err() {
                            break;
                        }
                    }
                });
                for line in BufReader::new(stream).lines() {
                    match line {
                        Ok(line) => {
                            if line_tx.send(line).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                addr: addr.to_string(),
                lines,
                to_client,
                session,
            }
        }

        pub(crate) fn addr(&self) -> String {
            self.addr.clone()
        }

        /// Close the session, as a relay dying does.
        pub(crate) fn hang_up(&self) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(stream) = self.session.lock().expect("the session slot").as_ref() {
                    let _ = stream.shutdown(Shutdown::Both);
                    return;
                }
                assert!(Instant::now() < deadline, "the session was never accepted");
                thread::sleep(Duration::from_millis(10));
            }
        }

        pub(crate) fn next_line(&self) -> String {
            self.lines
                .recv_timeout(Duration::from_secs(5))
                .expect("the transport wrote a line")
        }

        pub(crate) fn next_json(&self) -> serde_json::Value {
            serde_json::from_str(&self.next_line()).expect("the line is JSON")
        }

        /// Nothing was written within a short grace period. For asserting an
        /// absence, which needs a wait however short.
        pub(crate) fn quiet(&self) -> bool {
            self.lines.recv_timeout(Duration::from_millis(200)).is_err()
        }

        /// Push a line towards the replica.
        pub(crate) fn write(&self, line: String) {
            self.to_client.send(line).expect("the fake relay is alive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::FakeRelay;
    use super::*;

    fn transport(relay: &FakeRelay) -> RelayTransport<String> {
        RelayTransport::<String>::connect("a".to_string(), "s".to_string(), relay.addr())
            .expect("connect to the fake relay")
    }

    fn hello() -> TransportMessage<String> {
        TransportMessage::Hello {
            id: "a".to_string(),
            metadata: Some("9001".to_string()),
        }
    }

    #[test]
    fn the_session_opens_with_a_hello() {
        let relay = FakeRelay::start();
        let _t = transport(&relay);
        let frame = relay.next_json();
        assert_eq!(frame["hello"]["session_id"], "s");
        assert_eq!(frame["hello"]["replica_id"], "a");
    }

    #[test]
    fn a_send_is_wrapped_in_an_envelope_and_the_message_is_untouched() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json(); // the hello

        t.route(&"b".to_string());
        t.send(&"b".to_string(), hello()).expect("send over relay");

        let frame = relay.next_json();
        assert_eq!(frame["to"], "b");
        assert_eq!(frame["from"], "a");
        assert_eq!(
            frame["msg"],
            serde_json::to_value(hello()).unwrap(),
            "the message must cross the relay exactly as it would cross a direct link"
        );
    }

    #[test]
    fn a_broadcast_is_one_frame_with_every_recipient() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        for peer in ["c", "b", "d"] {
            t.route(&peer.to_string());
        }
        t.broadcast(hello()).expect("broadcast over relay");

        let frame = relay.next_json();
        assert_eq!(
            frame["to"],
            serde_json::json!(["b", "c", "d"]),
            "one upload, N downloads, and in a reproducible order"
        );
    }

    #[test]
    fn a_broadcast_with_nobody_routed_writes_nothing() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        t.broadcast(hello())
            .expect("a broadcast to nobody is not an error");
        assert!(
            relay.quiet(),
            "an empty recipient list must not become a frame"
        );
    }

    #[test]
    fn a_paused_peer_is_left_out_of_a_broadcast() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        t.route(&"b".to_string());
        t.route(&"c".to_string());
        t.pause_peer(&"c".to_string()).expect("pause a routed peer");
        t.broadcast(hello()).expect("broadcast");

        assert_eq!(relay.next_json()["to"], serde_json::json!(["b"]));
    }

    #[test]
    fn sending_to_an_unrouted_peer_is_an_error_rather_than_a_silent_drop() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        let sent = t.send(&"stranger".to_string(), hello());
        assert!(
            matches!(sent, Err(TransportError::PeerNotFound(id)) if id == "stranger"),
            "a caller that has not been told a peer is relayed must find out"
        );
    }

    #[test]
    fn an_inbound_frame_arrives_under_the_sender_it_names() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        let msg = serde_json::to_string(&hello()).unwrap();
        relay.write(format!(r#"{{"to":"a","from":"b","msg":{msg}}}"#));

        let (from, received) = poll(&mut t).expect("the frame arrived");
        assert_eq!(from, "b");
        assert!(matches!(received, TransportMessage::Hello { .. }));
    }

    #[test]
    fn hearing_from_a_peer_starts_routing_it() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        assert!(!t.routes(&"b".to_string()));
        let msg = serde_json::to_string(&hello()).unwrap();
        relay.write(format!(r#"{{"to":"a","from":"b","msg":{msg}}}"#));
        poll(&mut t).expect("the frame arrived");

        assert!(
            t.routes(&"b".to_string()),
            "a peer we have heard from over the relay is reachable over it, and \
             that is the only evidence available before our own roster catches up"
        );
        assert!(t.is_connected(&"b".to_string()));
    }

    #[test]
    fn an_unparseable_frame_does_not_kill_the_session() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        relay.write(r#"{"to":"a","from":"b","msg":{"type":"NotAMessage"}}"#.to_string());
        let msg = serde_json::to_string(&hello()).unwrap();
        relay.write(format!(r#"{{"to":"a","from":"c","msg":{msg}}}"#));

        let (from, _) = poll(&mut t).expect("the good frame still arrived");
        assert_eq!(from, "c");
        assert!(t.is_alive());
    }

    #[test]
    fn the_session_reports_itself_dead_once_the_relay_hangs_up() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();
        t.route(&"b".to_string());
        assert!(t.is_alive());

        relay.hang_up();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while t.is_alive() && std::time::Instant::now() < deadline {
            // A write is not needed to notice: the reader thread sees EOF.
            let _ = t.try_recv();
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !t.is_alive(),
            "a closed session must stop claiming to be one"
        );
        assert!(
            !t.is_connected(&"b".to_string()),
            "and its peers must stop reading as connected"
        );
    }

    #[test]
    fn a_paused_peers_frames_are_buffered_and_released_on_resume() {
        let relay = FakeRelay::start();
        let mut t = transport(&relay);
        relay.next_json();

        t.route(&"b".to_string());
        t.pause_peer(&"b".to_string()).expect("pause");

        let msg = serde_json::to_string(&hello()).unwrap();
        relay.write(format!(r#"{{"to":"a","from":"b","msg":{msg}}}"#));

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while t.buffered_count(&"b".to_string()) == 0 && std::time::Instant::now() < deadline {
            let _ = t.try_recv();
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            t.buffered_count(&"b".to_string()),
            1,
            "a simulated partition must hold over the relay too, or a scenario \
             that pauses a peer would silently keep talking to it"
        );

        t.resume_peer(&"b".to_string()).expect("resume");
        assert_eq!(t.drain_buffer(&"b".to_string()).len(), 1);
    }

    /// `try_recv` until something arrives or a deadline passes. The reader
    /// thread is a real thread, so nothing arrives synchronously.
    fn poll(t: &mut RelayTransport<String>) -> Option<(PeerId, TransportMessage<String>)> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Ok(Some(received)) = t.try_recv() {
                return Some(received);
            }
            thread::sleep(Duration::from_millis(10));
        }
        None
    }
}
