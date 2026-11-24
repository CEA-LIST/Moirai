//! The transport a node is actually built with: a direct provider, an optional
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
//! on its own.
//!
//! So: three realizations of one interface. [`DirectTransport`] dials and holds
//! a stream per peer. [`RelayTransport`] holds the one multiplexed relay
//! session. `CompositeTransport` owns both and delegates each call.
//!
//! # Direct-preferred, relay as the fallback
//!
//! [`CompositeTransport::connect_to_peers`] dials the peers that are due a direct
//! attempt and routes whatever is still unreachable through the relay, if there
//! is one. A peer's route is then *derived* rather than stored:
//!
//! ```text
//! paused                      -> nothing goes, whatever the route
//! directly connected          -> Direct
//! else, routed by the relay   -> Relayed
//! else                        -> unreachable, and nothing is sent
//! ```
//!
//! Deriving it is what makes the direct path win by itself: a stored route
//! would be a second copy of something two `is_connected` calls already
//! answer, and the copy is what would go stale.
//!
//! What *is* stored is `Dial` — when each peer was last attempted and how often
//! it has failed — because that is genuinely not derivable:
//!
//! ```text
//! absent from `dials`          -> Known, due now
//! present and not yet due      -> Relayed or Unreachable, waiting out its backoff
//! present and due              -> Dialing on the next reconcile
//! removed on success           -> Direct
//! ```
//!
//! The backoff is what pays for the dial timeout. Without one, a peer whose
//! address black-holes costs two seconds of the event loop *per reconcile*.
//!
//! # No relay, no change
//!
//! The relay endpoint arrives from a bootnode roster. A replica with no
//! `BOOTNODE_URL` never receives one, `relay` stays `None`, every method is the
//! direct transport's, and the whole pre-relay behaviour is intact.
//!
//! [`DirectTransport`]: crate::direct_transport::DirectTransport
//! [`RelayTransport`]: crate::relay_transport::RelayTransport

use std::net::TcpListener;
use std::time::{Duration, Instant};

use crate::HashMap;
use crate::direct_transport::DirectTransport;
use crate::relay_transport::RelayTransport;
use crate::transport::{CrdtTransport, PeerId, PeerInfo, TransportMessage, TransportResult};

/// Shortest gap between two direct attempts on the same peer.
///
/// Short, because a first dial legitimately fails when the peer is not listening
/// yet — that is P1-D9, and gating the retry on anything slower reintroduces
/// dial-once for exactly the peers that need retrying.
const MIN_REDIAL: Duration = Duration::from_secs(1);

/// Longest gap, and therefore how often a relayed peer is re-probed for a direct
/// path (design §7's `Relayed -> Dialing`).
///
/// One curve covers both cases the state machine distinguishes, which is why
/// there is no second interval: a peer that is merely slow to start is retried in
/// a second or two, and a peer that is genuinely unreachable settles here.
const MAX_REDIAL: Duration = Duration::from_secs(30);

/// Per-peer direct-dial state: design §7's `Known -> Dialing -> ...`, with the
/// backoff that keeps a doomed dial off the event loop.
///
/// Absent from the table means `Known` — never attempted, so due immediately.
#[derive(Debug)]
struct Dial {
    last_attempt: Instant,
    /// Consecutive failures. The only thing the backoff is computed from, so
    /// there is no separate state field to disagree with it.
    failures: u32,
}

impl Dial {
    /// 1 s, 2, 4, 8, 16, then [`MAX_REDIAL`] for ever.
    fn backoff(&self) -> Duration {
        MIN_REDIAL
            .saturating_mul(1u32 << self.failures.saturating_sub(1).min(5))
            .min(MAX_REDIAL)
    }

    fn due(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_attempt) >= self.backoff()
    }
}

/// How a peer is currently reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// A TCP stream of its own.
    Direct,
    /// Multiplexed over the relay session.
    Relayed,
    /// Known, and nothing can reach it: no direct connection and no relay.
    Unreachable,
}

impl Route {
    /// The string `/api/metrics` and the dashboard use.
    pub fn as_str(self) -> &'static str {
        match self {
            Route::Direct => "direct",
            Route::Relayed => "relayed",
            Route::Unreachable => "unreachable",
        }
    }
}

/// A transport that owns the providers a replica can reach a peer through.
pub struct CompositeTransport {
    direct: DirectTransport,
    /// `None` until a bootnode advertises a relay, which is the pre-relay
    /// behaviour in full.
    relay: Option<RelayTransport>,
    /// Where to dial the relay, and in which session. Held separately from
    /// `relay` because the endpoint is known before, and survives, any one
    /// session: a session that drops is re-dialled from this on the next
    /// reconcile.
    relay_endpoint: Option<(String, String)>,
    /// Direct-dial state per peer. Only ever holds peers whose last direct
    /// attempt *failed*: a success removes the entry, so an empty table means
    /// every known peer is directly connected.
    dials: HashMap<PeerId, Dial>,
}

impl CompositeTransport {
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
            relay: None,
            relay_endpoint: None,
            dials: HashMap::default(),
        })
    }

    /// [`new`], over a replication listener the caller has already bound:
    /// the arguments are [`DirectTransport::with_listener`]'s.
    ///
    /// [`new`]: CompositeTransport::new
    pub fn with_listener(
        local_id: PeerId,
        listener: TcpListener,
        peer_addresses: HashMap<PeerId, String>,
    ) -> TransportResult<Self> {
        Ok(Self {
            direct: DirectTransport::with_listener(local_id, listener, peer_addresses)?,
            relay: None,
            relay_endpoint: None,
            dials: HashMap::default(),
        })
    }

    /// Known peers that are neither connected nor paused — the ones a direct
    /// attempt could still help.
    fn dial_candidates(&self) -> Vec<PeerId> {
        self.direct
            .peers()
            .into_iter()
            .filter(|peer| !self.direct.is_connected(&peer.id) && !self.direct.is_paused(&peer.id))
            .map(|peer| peer.id)
            .collect()
    }

    /// How `peer` is reached right now.
    ///
    /// Derived, never stored — see the module header for why that is what makes
    /// the direct path win without any upgrade logic.
    pub fn route_of(&self, peer: &PeerId) -> Route {
        if self.direct.is_connected(peer) {
            return Route::Direct;
        }
        match &self.relay {
            Some(relay) if relay.is_connected(peer) => Route::Relayed,
            _ => Route::Unreachable,
        }
    }

    /// Every peer this replica knows how to reach, and how.
    ///
    /// Typed counterpart of [`CrdtTransport::routes`].
    pub fn routes_of_peers(&self) -> Vec<(PeerId, Route)> {
        let mut routes: Vec<(PeerId, Route)> = self
            .peers()
            .into_iter()
            .map(|peer| {
                let route = self.route_of(&peer.id);
                (peer.id, route)
            })
            .collect();
        routes.sort_by(|a, b| a.0.cmp(&b.0));
        routes
    }

    /// Whether `peer` is currently paused, on either route.
    pub fn is_paused(&self, peer: &PeerId) -> bool {
        self.direct.is_paused(peer)
            || self
                .relay
                .as_ref()
                .is_some_and(|relay| relay.is_paused(peer))
    }

    /// The relay session, if one is open. For tests and for `/health`-style
    /// reporting; the routing decisions are all above.
    pub fn relay(&self) -> Option<&RelayTransport> {
        self.relay.as_ref()
    }

    /// Open the relay session if the endpoint is known and there is no live one.
    ///
    /// Called from [`CrdtTransport::connect_to_peers`], so re-dialling rides on
    /// the reconcile the node already performs rather than on a clock of its own.
    fn ensure_relay_session(&mut self) {
        let Some((session, addr)) = self.relay_endpoint.clone() else {
            return;
        };
        if self.relay.as_ref().is_some_and(|relay| relay.is_alive()) {
            return;
        }

        // A session that died takes its routing with it. That is correct rather
        // than merely simple: which peers are reachable through a relay is a
        // property of the session, and the peers are re-routed by the next
        // failed dial or by the first frame that arrives from them.
        let previous = self.relay.take();
        match RelayTransport::connect(self.direct.local_id().clone(), session, addr.clone()) {
            Ok(mut relay) => {
                // Carry the previous session's peers over, so a replica whose
                // relay blinked does not have to wait for a dial to fail again
                // before it can talk to a peer it was already relaying to.
                if let Some(previous) = previous {
                    for peer in previous.peers() {
                        relay.route(&peer.id);
                    }
                }
                self.relay = Some(relay);
            }
            Err(e) => {
                eprintln!(
                    "[{}] relay at {} unreachable: {}",
                    self.direct.local_id(),
                    addr,
                    e
                );
            }
        }
    }
}

impl CrdtTransport for CompositeTransport {
    fn local_id(&self) -> &PeerId {
        self.direct.local_id()
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        match self.route_of(peer) {
            Route::Direct => self.direct.send(peer, msg),
            Route::Relayed => self
                .relay
                .as_mut()
                .expect("`Relayed` is only returned when a relay session exists")
                .send(peer, msg),
            // Deliberately the direct transport's error rather than a new one:
            // an unreachable peer is exactly the `PeerNotFound` every caller
            // already handles, and a paused peer is silently skipped, which is
            // what the pause flag means.
            Route::Unreachable => self.direct.send(peer, msg),
        }
    }

    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        // Both, and neither is a subset of the other: the direct transport
        // writes one frame per stream it holds, the relay writes one frame
        // naming every peer it routes. A peer cannot be in both, because
        // `route_of` prefers direct and the relay's own `is_connected` is what
        // the composite asks.
        let direct = self.direct.broadcast(msg.clone());
        if let Some(relay) = self.relay.as_mut()
            && let Err(e) = relay.broadcast(msg)
        {
            eprintln!("[{}] relay broadcast failed: {}", self.direct.local_id(), e);
        }
        direct
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        // Direct first, arbitrarily: both are non-blocking drains and the node
        // loops until both are empty. A relayed frame that waits one iteration
        // waits 10 ms, and the CRDT is order-insensitive anyway.
        if let Some(received) = self.direct.try_recv()? {
            return Ok(Some(received));
        }
        match self.relay.as_mut() {
            Some(relay) => relay.try_recv(),
            None => Ok(None),
        }
    }

    fn peers(&self) -> Vec<PeerInfo> {
        let mut peers = self.direct.peers();
        let Some(relay) = &self.relay else {
            return peers;
        };
        // One entry per peer. A peer the direct transport knows about but cannot
        // reach keeps its direct entry — and its `Disconnected` status would lie
        // about a peer the relay is carrying, so the relay's entry wins for it.
        for relayed in relay.peers() {
            match peers.iter_mut().find(|known| known.id == relayed.id) {
                Some(known) if !self.direct.is_connected(&relayed.id) => *known = relayed,
                Some(_) => {}
                None => peers.push(relayed),
            }
        }
        peers
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        self.route_of(peer) != Route::Unreachable
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        // Both providers, so a simulated partition holds whichever route the
        // peer is on. Pausing only the route it happens to be on today would
        // make a scenario that cuts a link keep talking over the other one —
        // the failure mode where a partition test passes while proving nothing.
        let direct = self.direct.pause_peer(peer);
        let relay = match self.relay.as_mut() {
            Some(relay) => relay.pause_peer(peer),
            None => Err(crate::transport::TransportError::PeerNotFound(peer.clone())),
        };
        direct.or(relay)
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        let direct = self.direct.resume_peer(peer);
        let relay = match self.relay.as_mut() {
            Some(relay) => relay.resume_peer(peer),
            None => Err(crate::transport::TransportError::Other(format!(
                "Peer {} was not paused",
                peer
            ))),
        };
        direct.or(relay)
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        let direct = self.direct.pause_all();
        if let Some(relay) = self.relay.as_mut() {
            relay.pause_all()?;
        }
        direct
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        let direct = self.direct.resume_all();
        if let Some(relay) = self.relay.as_mut() {
            relay.resume_all()?;
        }
        direct
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.direct.buffered_count(peer)
            + self
                .relay
                .as_ref()
                .map(|relay| relay.buffered_count(peer))
                .unwrap_or(0)
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        // The relay session is outbound and has nothing to accept.
        self.direct.accept_connections()
    }

    fn add_peer(&mut self, peer: PeerId, addr: String) -> bool {
        self.direct.add_peer(peer, addr)
    }

    fn routes(&self) -> Vec<(PeerId, &'static str)> {
        self.routes_of_peers()
            .into_iter()
            .map(|(peer, route)| (peer, route.as_str()))
            .collect()
    }

    fn set_relay(&mut self, session: &str, addr: &str) {
        let endpoint = (session.to_string(), addr.to_string());
        if self.relay_endpoint.as_ref() == Some(&endpoint) {
            return;
        }
        eprintln!(
            "[{}] relay advertised at {} for session `{}`",
            self.direct.local_id(),
            addr,
            session
        );
        self.relay_endpoint = Some(endpoint);
        // A moved endpoint invalidates the session; dropping it here means the
        // dial below happens against the new address.
        self.relay = None;
    }

    /// Dial the peers that are due a direct attempt, then relay whatever is
    /// still unreachable.
    ///
    /// The returned peers are the ones that became reachable in this pass,
    /// **relayed ones included**. That is not a detail: a node answers each of
    /// them with a `SyncRequest` or a `StateRequest`, and a relayed peer left
    /// out of this list would connect and then sit there receiving nothing
    /// that happened before it arrived.
    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        self.ensure_relay_session();

        let now = Instant::now();
        let mut reached = Vec::new();
        for peer in self.dial_candidates() {
            if !self.dials.get(&peer).is_none_or(|dial| dial.due(now)) {
                continue;
            }
            match self.direct.connect_to(&peer) {
                Ok(()) => {
                    self.dials.remove(&peer);
                    // A direct path has appeared, so stop paying the relay for
                    // this peer. Left routed, every broadcast would go out twice
                    // — harmless, since the receiver rejects the duplicate, and
                    // wasteful in exactly the direction the relay exists to
                    // spare.
                    if let Some(relay) = self.relay.as_mut() {
                        relay.unroute(&peer);
                    }
                    reached.push(peer);
                }
                Err(e) => {
                    let dial = self.dials.entry(peer.clone()).or_insert(Dial {
                        last_attempt: now,
                        failures: 0,
                    });
                    dial.last_attempt = now;
                    dial.failures = dial.failures.saturating_add(1);
                    eprintln!(
                        "[{}] direct dial to {} failed ({}); next attempt in {:?}",
                        self.direct.local_id(),
                        peer,
                        e,
                        dial.backoff()
                    );
                }
            }
        }

        let Some(relay) = self.relay.as_mut() else {
            return Ok(reached);
        };
        // Whatever is still not directly reachable. Every one of these has had a
        // failed dial at some point, so this is evidence rather than a guess.
        let unreachable: Vec<PeerId> = self
            .direct
            .peers()
            .into_iter()
            .filter(|peer| !self.direct.is_connected(&peer.id) && !self.direct.is_paused(&peer.id))
            .map(|peer| peer.id)
            .collect();

        for peer in unreachable {
            if relay.route(&peer) {
                eprintln!(
                    "[{}] {} is not directly reachable; routing through {}",
                    self.direct.local_id(),
                    peer,
                    relay.relay_addr()
                );
                // The relay does not tell a recipient that somebody started
                // routing to it, so say so: a `Hello` is what makes the far end
                // answer with a sync request of its own, which is the symmetry
                // the direct path gets from the TCP handshake for free.
                let hello = TransportMessage::Hello {
                    id: self.direct.local_id().clone(),
                    metadata: None,
                };
                if let Err(e) = relay.send(&peer, hello) {
                    eprintln!(
                        "[{}] could not greet {} over the relay: {}",
                        self.direct.local_id(),
                        peer,
                        e
                    );
                    continue;
                }
                reached.push(peer);
            }
        }

        Ok(reached)
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage> {
        let mut buffered = self.direct.drain_buffer(peer);
        if let Some(relay) = self.relay.as_mut() {
            buffered.extend(relay.drain_buffer(peer));
        }
        buffered
    }
}
