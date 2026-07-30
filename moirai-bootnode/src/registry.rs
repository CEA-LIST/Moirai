//! The session directory itself, with no HTTP and no clock of its own.
//!
//! Time is a parameter on every method rather than a call to `Instant::now()`
//! inside them. That is what lets TTL expiry be tested in microseconds instead
//! of by sleeping past a 30-second deadline, which is the kind of test the e2e
//! harness already bans.

use std::time::{Duration, Instant};

use serde::Serialize;

/// Opaque session name, taken from the URL.
pub type SessionId = String;
/// Replica id, the same string a replica uses in `REPLICA_ID`.
pub type PeerId = String;

/// One replica's entry in one session.
#[derive(Debug, Clone, Serialize)]
pub struct Peer {
    pub id: PeerId,
    /// `host:port` of the replication listener, as *other replicas* reach it.
    /// The bootnode never dials it and never validates it.
    pub addr: String,
}

#[derive(Debug, Clone)]
struct Entry {
    addr: String,
    last_seen: Instant,
}

#[derive(Debug, Default)]
struct Session {
    peers: Vec<(PeerId, Entry)>,
    /// Bumped whenever the *set* of ids changes: a join, a leave, an expiry.
    ///
    /// Nothing consumes it in phase 1. It exists from day one so that the
    /// field is already on the wire when signed membership cuts start using
    /// it in phase 2, rather than being a breaking addition later.
    epoch: u64,
}

impl Session {
    fn index_of(&self, id: &str) -> Option<usize> {
        self.peers.iter().position(|(peer_id, _)| peer_id == id)
    }
}

/// Every session this bootnode knows about.
///
/// A `Vec` per session rather than a map: a session is five to twenty
/// replicas, linear scans are free at that size, and insertion order makes the
/// roster stable between calls, which matters when a human is reading `curl`
/// output.
#[derive(Debug, Default)]
pub struct Registry {
    sessions: Vec<(SessionId, Session)>,
    ttl: Duration,
    /// `host:port` of the relay, from `RELAY_ADDR`. One relay, deliberately
    /// singular: C-D6 allows several and the design says implement one now.
    relay: Option<String>,
}

/// What a caller gets back from any of the three endpoints.
#[derive(Debug, Clone, Serialize)]
pub struct Roster {
    pub peers: Vec<Peer>,
    pub epoch: u64,
    /// Where the relay is, for pairs that cannot reach each other directly, or
    /// `null` when none is deployed.
    ///
    /// Advertised here rather than configured on each replica for one reason:
    /// relay deployment then changes in one place, and a replica that has only
    /// ever been told where a bootnode is can still be reached by a peer behind
    /// NAT. `null` is exactly the pre-relay behaviour.
    ///
    /// It does not make the bootnode a data path. This is a string it was given
    /// and repeats; it still never sees an operation, which is the property that
    /// makes hosting one acceptable and the reason the relay is a separate
    /// service (C-D3).
    pub relay: Option<String>,
}

impl Registry {
    pub fn new(ttl: Duration) -> Self {
        Self {
            sessions: Vec::new(),
            ttl,
            relay: None,
        }
    }

    /// Advertise `addr` as the relay in every roster this registry returns.
    pub fn with_relay(mut self, addr: Option<String>) -> Self {
        self.relay = addr;
        self
    }

    pub fn relay(&self) -> Option<&str> {
        self.relay.as_deref()
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Upsert `id` into `session` and return everyone *else* currently in it.
    ///
    /// Upsert-plus-read in one call, so a joining replica needs a single
    /// request to both announce itself and learn the roster. Re-registering is
    /// how a replica refreshes its TTL, so this is also the heartbeat.
    ///
    /// The caller is excluded from the returned roster: a replica dialling
    /// itself is the one thing the list is never useful for.
    pub fn register(&mut self, session: &str, id: &str, addr: &str, now: Instant) -> Roster {
        self.sweep(now);
        let entry = Entry {
            addr: addr.to_string(),
            last_seen: now,
        };
        // Cloned before the session is borrowed mutably below.
        let relay = self.relay.clone();

        let session = self.session_mut(session);
        match session.index_of(id) {
            Some(pos) => {
                // A refresh only bumps the epoch if the address moved: a
                // heartbeat is not a membership change.
                if session.peers[pos].1.addr != entry.addr {
                    session.epoch += 1;
                }
                session.peers[pos].1 = entry;
            }
            None => {
                session.peers.push((id.to_string(), entry));
                session.epoch += 1;
            }
        }

        Self::roster_of(session, Some(id), relay)
    }

    /// The current roster of `session`, including every member.
    ///
    /// An unknown session is not an error: it reads as empty at epoch 0. A
    /// poller that starts before the first replica registers would otherwise
    /// have to treat a 404 as "keep trying", which is the same behaviour with
    /// more code on both sides.
    pub fn peers(&mut self, session: &str, now: Instant) -> Roster {
        self.sweep(now);
        let relay = self.relay.clone();
        match self.session_ref(session) {
            Some(session) => Self::roster_of(session, None, relay),
            None => Roster {
                peers: Vec::new(),
                epoch: 0,
                relay,
            },
        }
    }

    /// Remove `id` from `session`. Returns the roster and whether anything was
    /// actually removed — a leave for an unknown id is a no-op, not an error,
    /// because a replica that already timed out is entitled to say goodbye.
    pub fn leave(&mut self, session: &str, id: &str, now: Instant) -> (Roster, bool) {
        self.sweep(now);
        let relay = self.relay.clone();
        let session = self.session_mut(session);
        let removed = match session.index_of(id) {
            Some(pos) => {
                session.peers.remove(pos);
                session.epoch += 1;
                true
            }
            None => false,
        };
        (Self::roster_of(session, None, relay), removed)
    }

    /// Drop every entry not refreshed within the TTL.
    ///
    /// Swept on access rather than by a timer thread. The only observer of an
    /// expiry is a caller, so a lazily swept registry and a continuously swept
    /// one are indistinguishable from outside — and the lazy one has no thread
    /// to shut down and no lock held on a timer.
    ///
    /// Expiry is a *directory* event and deliberately not a membership change
    /// for causal stability: a replica that stops registering disappears from
    /// here while remaining in every peer's matrix clock. Keeping those two
    /// notions apart is what makes the E4 measurement mean anything.
    fn sweep(&mut self, now: Instant) {
        let ttl = self.ttl;
        for (_, session) in self.sessions.iter_mut() {
            let before = session.peers.len();
            session
                .peers
                .retain(|(_, entry)| now.duration_since(entry.last_seen) < ttl);
            if session.peers.len() != before {
                session.epoch += 1;
            }
        }
    }

    fn roster_of(session: &Session, exclude: Option<&str>, relay: Option<String>) -> Roster {
        Roster {
            relay,
            peers: session
                .peers
                .iter()
                .filter(|(id, _)| Some(id.as_str()) != exclude)
                .map(|(id, entry)| Peer {
                    id: id.clone(),
                    addr: entry.addr.clone(),
                })
                .collect(),
            epoch: session.epoch,
        }
    }

    fn session_ref(&self, name: &str) -> Option<&Session> {
        self.sessions
            .iter()
            .find(|(id, _)| id == name)
            .map(|(_, session)| session)
    }

    fn session_mut(&mut self, name: &str) -> &mut Session {
        if let Some(pos) = self.sessions.iter().position(|(id, _)| id == name) {
            return &mut self.sessions[pos].1;
        }
        self.sessions.push((name.to_string(), Session::default()));
        &mut self.sessions.last_mut().unwrap().1
    }

    /// Sessions currently known, for `/health`.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    fn ids(roster: &Roster) -> Vec<&str> {
        roster.peers.iter().map(|p| p.id.as_str()).collect()
    }

    #[test]
    fn register_returns_every_other_member() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let now = t0();

        assert!(ids(&reg.register("s", "a", "a:9001", now)).is_empty());
        assert_eq!(ids(&reg.register("s", "b", "b:9001", now)), ["a"]);
        assert_eq!(ids(&reg.register("s", "c", "c:9001", now)), ["a", "b"]);

        // `peers` is the whole roster; `register` is the roster minus the caller.
        assert_eq!(ids(&reg.peers("s", now)), ["a", "b", "c"]);
    }

    #[test]
    fn the_relay_is_null_unless_one_was_configured() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let now = t0();
        assert!(
            reg.register("s", "a", "a:9001", now).relay.is_none(),
            "a bootnode with no RELAY_ADDR must advertise none — that `null` is \
             the whole guard rail: it is the behaviour before a relay existed"
        );
        assert!(reg.peers("s", now).relay.is_none());
        assert!(reg.leave("s", "a", now).0.relay.is_none());
    }

    #[test]
    fn a_configured_relay_rides_on_every_answer() {
        let mut reg =
            Registry::new(Duration::from_secs(30)).with_relay(Some("relay:7100".to_string()));
        let now = t0();
        // Every endpoint, because a replica learns the relay from whichever one
        // it happens to call: `register` on its heartbeat, `peers` from an
        // observer, `leave` on the way out.
        assert_eq!(
            reg.register("s", "a", "a:9001", now).relay.as_deref(),
            Some("relay:7100")
        );
        assert_eq!(reg.peers("s", now).relay.as_deref(), Some("relay:7100"));
        assert_eq!(
            reg.leave("s", "a", now).0.relay.as_deref(),
            Some("relay:7100")
        );
        // Including for a session nobody has ever registered in, so a replica
        // that polls before anyone joins still learns where the relay is.
        assert_eq!(
            reg.peers("never-seen", now).relay.as_deref(),
            Some("relay:7100")
        );
    }

    #[test]
    fn sessions_are_isolated() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let now = t0();
        reg.register("left", "a", "a:9001", now);
        reg.register("right", "b", "b:9001", now);

        assert_eq!(ids(&reg.peers("left", now)), ["a"]);
        assert_eq!(ids(&reg.peers("right", now)), ["b"]);
    }

    #[test]
    fn unknown_session_reads_as_empty_at_epoch_zero() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let roster = reg.peers("never-seen", t0());
        assert!(roster.peers.is_empty());
        assert_eq!(roster.epoch, 0);
        // Reading must not conjure the session into existence.
        assert_eq!(reg.session_count(), 0);
    }

    #[test]
    fn leave_of_an_unknown_id_is_a_no_op() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let now = t0();
        reg.register("s", "a", "a:9001", now);
        let epoch_before = reg.peers("s", now).epoch;

        let (roster, removed) = reg.leave("s", "ghost", now);
        assert!(!removed);
        assert_eq!(ids(&roster), ["a"]);
        assert_eq!(
            roster.epoch, epoch_before,
            "a no-op must not bump the epoch"
        );
    }

    #[test]
    fn leave_removes_and_bumps_the_epoch() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let now = t0();
        reg.register("s", "a", "a:9001", now);
        reg.register("s", "b", "b:9001", now);
        let epoch_before = reg.peers("s", now).epoch;

        let (roster, removed) = reg.leave("s", "a", now);
        assert!(removed);
        assert_eq!(ids(&roster), ["b"]);
        assert!(roster.epoch > epoch_before);
    }

    #[test]
    fn an_entry_expires_once_it_stops_refreshing() {
        let ttl = Duration::from_secs(30);
        let mut reg = Registry::new(ttl);
        let t = t0();

        reg.register("s", "a", "a:9001", t);
        reg.register("s", "b", "b:9001", t);

        // `b` keeps its registration alive; `a` goes quiet.
        let later = t + ttl - Duration::from_millis(1);
        reg.register("s", "b", "b:9001", later);
        assert_eq!(ids(&reg.peers("s", later)), ["a", "b"], "not expired yet");

        let past_ttl = t + ttl;
        assert_eq!(ids(&reg.peers("s", past_ttl)), ["b"]);
    }

    #[test]
    fn expiry_bumps_the_epoch_exactly_once() {
        let ttl = Duration::from_secs(30);
        let mut reg = Registry::new(ttl);
        let t = t0();
        reg.register("s", "a", "a:9001", t);
        reg.register("s", "b", "b:9001", t);

        let past = t + ttl;
        reg.register("s", "b", "b:9001", past);
        let after_expiry = reg.peers("s", past).epoch;
        // A second read with nothing changed must be idempotent.
        assert_eq!(reg.peers("s", past).epoch, after_expiry);
    }

    #[test]
    fn a_refresh_is_not_a_membership_change_but_a_move_is() {
        let mut reg = Registry::new(Duration::from_secs(30));
        let t = t0();
        reg.register("s", "a", "a:9001", t);
        let joined = reg.peers("s", t).epoch;

        reg.register("s", "a", "a:9001", t + Duration::from_secs(1));
        assert_eq!(
            reg.peers("s", t).epoch,
            joined,
            "heartbeat bumped the epoch"
        );

        reg.register("s", "a", "a:9002", t + Duration::from_secs(2));
        assert!(
            reg.peers("s", t).epoch > joined,
            "a moved address is a change"
        );
    }

    #[test]
    fn a_returning_replica_is_readmitted() {
        let ttl = Duration::from_secs(30);
        let mut reg = Registry::new(ttl);
        let t = t0();
        reg.register("s", "a", "a:9001", t);
        reg.register("s", "b", "b:9001", t);

        let past = t + ttl;
        assert_eq!(ids(&reg.peers("s", past)), Vec::<&str>::new());

        // Nothing about expiry is permanent — this is a directory, not a
        // membership authority. Phase 2's eviction is the one that is.
        assert!(ids(&reg.register("s", "a", "a:9001", past)).is_empty());
        assert_eq!(ids(&reg.register("s", "b", "b:9001", past)), ["a"]);
    }
}
