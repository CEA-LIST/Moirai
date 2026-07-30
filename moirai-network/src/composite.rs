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
//! on its own, and leaves the architecture's Transport extension point (EXT-4)
//! describing a component structure the code does not have.
//!
//! So: three realizations of one interface. [`DirectTransport`] dials and holds
//! a stream per peer. [`RelayTransport`] holds the one multiplexed relay
//! session. `CompositeTransport` owns both and delegates each call — and because
//! it realizes [`CrdtTransport`] itself, `GenericNode<L, T>` is unchanged and
//! `Replica`, the TCSB and the PO-Log never learn a relay exists.
//!
//! # Direct-preferred, relay as the fallback
//!
//! [`CompositeTransport::connect_to_peers`] dials every known peer it is not
//! already connected to — which is [`DirectTransport`]'s behaviour, untouched —
//! and then routes whatever is left through the relay, if there is one. A peer's
//! route is therefore *derived* rather than stored:
//!
//! ```text
//! paused                      -> nothing goes, whatever the route
//! directly connected          -> Direct
//! else, routed by the relay   -> Relayed
//! else                        -> unreachable, and nothing is sent
//! ```
//!
//! Deriving it is what makes the direct path win by itself. `connect_to_peers`
//! re-dials every peer it is not connected to on every reconcile (P1-D9), so a
//! peer that was only reachable through the relay is promoted to `Direct` the
//! moment a dial succeeds, with no upgrade logic to write and nothing to keep in
//! step. The design's per-peer state machine (§7) adds dial backoff and a
//! re-probe interval on top of this; it does not replace it.
//!
//! # No relay, no change
//!
//! The relay endpoint arrives from a bootnode roster. A replica with no
//! `BOOTNODE_URL` never receives one, `relay` stays `None`, every method is the
//! direct transport's, and the whole pre-relay behaviour is intact. That is the
//! same guard rail phase 1 used, and every scenario that runs without a
//! bootnode is the test of it.
//!
//! [`DirectTransport`]: crate::direct_transport::DirectTransport
//! [`RelayTransport`]: crate::relay_transport::RelayTransport

use serde::{de::DeserializeOwned, Serialize};

use crate::direct_transport::DirectTransport;
use crate::relay_transport::RelayTransport;
use crate::transport::{CrdtTransport, PeerId, PeerInfo, TransportMessage, TransportResult};
use crate::HashMap;

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
pub struct CompositeTransport<O>
where
    O: Serialize + DeserializeOwned,
{
    direct: DirectTransport<O>,
    /// `None` until a bootnode advertises a relay, which is the pre-relay
    /// behaviour in full.
    relay: Option<RelayTransport<O>>,
    /// Where to dial the relay, and in which session. Held separately from
    /// `relay` because the endpoint is known before, and survives, any one
    /// session: a session that drops is re-dialled from this on the next
    /// reconcile.
    relay_endpoint: Option<(String, String)>,
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
            relay: None,
            relay_endpoint: None,
        })
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
    /// Typed counterpart of [`CrdtTransport::routes`], which is what
    /// `/api/metrics` surfaces.
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
    pub fn relay(&self) -> Option<&RelayTransport<O>> {
        self.relay.as_ref()
    }

    /// Open the relay session if the endpoint is known and there is no live one.
    ///
    /// Called from [`CrdtTransport::connect_to_peers`], so re-dialling rides on
    /// the reconcile the node already performs rather than on a clock of its own.
    /// A relay that is down therefore costs one failed dial per reconcile and
    /// nothing else.
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

impl<O> CrdtTransport for CompositeTransport<O>
where
    O: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    type Op = O;

    fn local_id(&self) -> &PeerId {
        self.direct.local_id()
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
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

    fn broadcast(&mut self, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        // Both, and neither is a subset of the other: the direct transport
        // writes one frame per stream it holds, the relay writes one frame
        // naming every peer it routes. A peer cannot be in both, because
        // `route_of` prefers direct and the relay's own `is_connected` is what
        // the composite asks — but even if it were, the receiving replica
        // discards a duplicate on arrival.
        let direct = self.direct.broadcast(msg.clone());
        if let Some(relay) = self.relay.as_mut() {
            if let Err(e) = relay.broadcast(msg) {
                eprintln!("[{}] relay broadcast failed: {}", self.direct.local_id(), e);
            }
        }
        direct
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Self::Op>)>> {
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

    /// Dial every peer that is not connected, then relay whatever is left.
    ///
    /// The returned peers are the ones that became reachable in this pass,
    /// **relayed ones included**. That is not a detail: `GenericNode::connect`
    /// answers each of them with a `SyncRequest` or a `StateRequest`, which is
    /// phase 1's dialer-requests-sync chain and the only reason a replica that
    /// joins late catches up. A relayed peer left out of this list would connect
    /// and then sit there receiving nothing that happened before it arrived.
    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        self.ensure_relay_session();

        let mut reached = self.direct.connect_to_peers()?;

        let Some(relay) = self.relay.as_mut() else {
            return Ok(reached);
        };
        // Whatever the direct transport knows about and could not reach. It has
        // just tried every one of them, so this is a failed dial and not a
        // guess.
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

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<Self::Op>> {
        let mut buffered = self.direct.drain_buffer(peer);
        if let Some(relay) = self.relay.as_mut() {
            buffered.extend(relay.drain_buffer(peer));
        }
        buffered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn composite(port: u16, peers: &[(&str, &str)]) -> CompositeTransport<String> {
        let mut addresses = HashMap::default();
        for (id, addr) in peers {
            addresses.insert((*id).to_string(), (*addr).to_string());
        }
        CompositeTransport::new("a".to_string(), port, addresses).expect("bind the listener")
    }

    /// Any port the OS is willing to give away, so the tests do not collide.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    }

    #[test]
    fn with_no_relay_an_unreachable_peer_stays_unreachable() {
        // 127.0.0.1:1 refuses instantly, which is the cheap way to have a peer
        // that is known and cannot be dialled.
        let mut t = composite(free_port(), &[("b", "127.0.0.1:1")]);
        let reached = t.connect_to_peers().expect("dialling is not fatal");

        assert!(reached.is_empty());
        assert_eq!(t.route_of(&"b".to_string()), Route::Unreachable);
        assert!(!t.is_connected(&"b".to_string()));
        assert!(
            t.relay().is_none(),
            "no bootnode advertised a relay, so there must not be one"
        );
    }

    #[test]
    fn a_peer_that_cannot_be_dialled_is_routed_through_the_relay() {
        let relay = crate::relay_transport::tests_support::FakeRelay::start();
        let mut t = composite(free_port(), &[("b", "127.0.0.1:1")]);
        t.set_relay("s", &relay.addr());

        let reached = t.connect_to_peers().expect("connect");

        assert_eq!(
            reached,
            vec!["b".to_string()],
            "a newly relayed peer must be returned, or the node never asks it \
             for history and a late joiner over a relay catches up on nothing"
        );
        assert_eq!(t.route_of(&"b".to_string()), Route::Relayed);
        assert!(t.is_connected(&"b".to_string()));

        assert_eq!(relay.next_json()["hello"]["replica_id"], "a");
        let greeting = relay.next_json();
        assert_eq!(greeting["to"], "b");
        assert_eq!(
            greeting["msg"]["type"], "Hello",
            "the far end has to be told somebody is routing to it"
        );
    }

    #[test]
    fn a_relayed_peer_is_reported_once_and_as_connected() {
        let relay = crate::relay_transport::tests_support::FakeRelay::start();
        let mut t = composite(free_port(), &[("b", "127.0.0.1:1")]);
        t.set_relay("s", &relay.addr());
        t.connect_to_peers().expect("connect");

        let peers = t.peers();
        assert_eq!(
            peers.len(),
            1,
            "a peer known to both providers must not be counted twice — \
             `peer_count` in /api/metrics is an oracle"
        );
        assert_eq!(peers[0].id, "b");
        assert_eq!(peers[0].status, crate::transport::PeerStatus::Connected);
        assert!(peers[0].address.starts_with("relay:"));
        assert_eq!(t.routes_of_peers(), vec![("b".to_string(), Route::Relayed)]);
    }

    #[test]
    fn a_paused_peer_stays_paused_over_the_relay() {
        let relay = crate::relay_transport::tests_support::FakeRelay::start();
        let mut t = composite(free_port(), &[("b", "127.0.0.1:1")]);
        t.set_relay("s", &relay.addr());
        t.connect_to_peers().expect("connect");
        relay.next_json(); // hello
        relay.next_json(); // the greeting for b

        t.pause_peer(&"b".to_string())
            .expect("pause a relayed peer");
        assert!(t.is_paused(&"b".to_string()));

        t.broadcast(TransportMessage::Ack {
            from: "a".to_string(),
        })
        .expect("broadcast");
        assert!(
            relay.quiet(),
            "a paused peer must not be reachable over the other route — a \
             partition scenario would otherwise pass while proving nothing"
        );

        t.resume_peer(&"b".to_string()).expect("resume");
        t.broadcast(TransportMessage::Ack {
            from: "a".to_string(),
        })
        .expect("broadcast");
        assert_eq!(relay.next_json()["to"], serde_json::json!(["b"]));
    }

    #[test]
    fn a_direct_connection_wins_over_a_relayed_one() {
        let relay = crate::relay_transport::tests_support::FakeRelay::start();
        // A listener that accepts, so the dial succeeds.
        let peer = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a peer");
        let peer_addr = peer.local_addr().expect("addr").to_string();

        let mut t = composite(free_port(), &[("b", &peer_addr)]);
        t.set_relay("s", &relay.addr());
        t.connect_to_peers().expect("connect");

        assert_eq!(
            t.route_of(&"b".to_string()),
            Route::Direct,
            "the relay is the fallback, not the default"
        );
        // The relay session is still open — open question 1 leans on keeping it,
        // since it is one idle TCP connection and the next unreachable joiner
        // needs it — but nothing is routed through it.
        assert!(t.relay().is_some());
        assert!(!t.relay().expect("a session").routes(&"b".to_string()));
    }
}
