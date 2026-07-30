//! The routing table and the wire format, with no sockets in sight.
//!
//! Every socket the relay owns is in `main.rs`. What is here is the part with
//! decisions in it — which live session a recipient id names, what happens when
//! it names none, and when a session has been idle long enough to reclaim — so
//! that all of it is unit-testable without a listener, a port or a sleep. Time
//! is a parameter on every method that needs it, the same discipline
//! `moirai-bootnode`'s registry uses and for the same reason: idle expiry is
//! then tested in microseconds rather than by waiting out a five-minute
//! deadline.
//!
//! # The one property this file exists to protect
//!
//! [`Envelope::msg`] is a [`RawValue`]. The relay reads `to` and `from` and
//! copies `msg` through **without ever parsing it**, which is worth stating as
//! a property rather than an implementation detail (design §6.2):
//!
//! - it keeps the relay independent of the CRDT operation type, so the crate
//!   has no dependency on `moirai-protocol` and could not parse an operation
//!   even if someone tried;
//! - it is why phase 2's state transfer relays with no change at all —
//!   `StateRequest` and `StateResponse` are just `msg` payloads;
//! - it is what makes encryption additive later: `msg` becomes ciphertext
//!   sealed per recipient and this file does not change.
//!
//! Deserialising the envelope fully would work today and quietly cost all
//! three.

use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Session name, opaque here — the relay never interprets it.
pub type SessionId = String;
/// Replica id, the same string a replica uses in `REPLICA_ID`.
pub type ReplicaId = String;

/// Frames that may queue for one slow recipient before new ones are dropped.
///
/// Bounded and dropping rather than blocking, the same discipline as the
/// dashboard reporter, and here it is load-bearing: every relayed peer of a
/// replica shares one session, and all sessions share the accept loop. A queue
/// that applied back-pressure would let one slow consumer stall the writer
/// thread of the session feeding it — and, through the reader that feeds that
/// writer, every other recipient of the same sender.
const QUEUE_DEPTH: usize = 1024;

/// The first line of every relay session: `{"hello":{"session_id":..,"replica_id":..}}`.
///
/// Wrapped in a one-field object so that a frame can be told apart from a
/// `hello` by shape alone, without a discriminator field on every envelope.
#[derive(Debug, Deserialize)]
pub struct HelloFrame {
    pub hello: Hello,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hello {
    pub session_id: SessionId,
    pub replica_id: ReplicaId,
}

/// `"b"` or `["b","c","d"]`.
///
/// The array form is what makes a broadcast cost one upload instead of N
/// (design §6.3): the sender writes one frame and the relay does the fan-out.
/// Upstream is the scarce direction on a domestic connection, which is why this
/// is in the format from the start rather than retrofitted.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Recipients {
    One(ReplicaId),
    Many(Vec<ReplicaId>),
}

impl Recipients {
    /// Both forms as a slice. `One` borrows itself, so neither allocates.
    pub fn as_slice(&self) -> &[ReplicaId] {
        match self {
            Recipients::One(id) => std::slice::from_ref(id),
            Recipients::Many(ids) => ids,
        }
    }
}

/// One inbound frame.
///
/// Borrows from the line it was parsed out of, `msg` included — see the module
/// header for why that is the point and not an optimisation.
#[derive(Debug, Deserialize)]
pub struct Envelope<'a> {
    pub to: Recipients,
    pub from: ReplicaId,
    #[serde(borrow)]
    pub msg: &'a RawValue,
}

/// One outbound frame: `to` narrowed to the single recipient, `from` and `msg`
/// verbatim.
///
/// `from` is copied through unverified. That is C-D1 in one line — any client
/// claiming an id is believed — and §10 is where a signature over a
/// relay-issued challenge attaches. Rewriting `from` to the session's declared
/// id would be marginally harder to abuse and would still not be
/// authentication, so it is left as the design specifies.
#[derive(Debug, Serialize)]
struct Forwarded<'a> {
    to: &'a str,
    from: &'a str,
    msg: &'a RawValue,
}

/// Handle identifying one opened session, so a connection can close *its own*
/// session and not its successor's.
///
/// A replica that reconnects while its previous connection is still draining
/// produces exactly that race: newest wins, and then the old reader thread
/// notices its socket is gone and tries to clean up. Without this token it
/// would remove the session that just replaced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionToken(u64);

/// What became of one routed frame. Counts, not ids: a relay that allocated a
/// `Vec` of recipient names per frame would be paying for its own logging on
/// the hot path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Delivery {
    /// Frames written into a recipient's queue.
    pub delivered: usize,
    /// Recipients whose queue was full. The frame is gone; the CRDT's catch-up
    /// path is what repairs the gap.
    pub dropped: usize,
    /// Recipients with no live session in this session id. Silently dropped by
    /// design (§6.4): no queueing, no NACK.
    pub absent: usize,
}

/// Counters for `/health`, which is also how the fan-out claim is observed:
/// one broadcast to N relayed peers must show one `frames_in` and N
/// `frames_out`.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Stats {
    pub sessions: usize,
    pub frames_in: u64,
    pub frames_out: u64,
    pub frames_dropped: u64,
    pub frames_absent: u64,
    pub sessions_opened: u64,
    pub sessions_displaced: u64,
    pub sessions_expired: u64,
}

#[derive(Debug)]
struct Entry {
    session: SessionId,
    replica: ReplicaId,
    token: SessionToken,
    /// Bounded; see [`QUEUE_DEPTH`]. Dropping this is what closes the session's
    /// socket — the writer thread's `recv` fails and it shuts the stream down —
    /// which is how this type manages connections without holding one.
    tx: SyncSender<String>,
    /// Refreshed by traffic in *either* direction. A replica that only ever
    /// receives is not idle.
    last_seen: Instant,
}

/// Every live relay session, keyed by `(session id, replica id)`.
///
/// A `Vec` with linear scans, like the bootnode's registry: a session is five
/// to twenty replicas and a relay serves a handful of sessions, so the scan is
/// free at this size and insertion order keeps `/health` stable between reads.
#[derive(Debug)]
pub struct Sessions {
    entries: Vec<Entry>,
    idle_timeout: Duration,
    next_token: u64,
    stats: Stats,
}

impl Sessions {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            entries: Vec::new(),
            idle_timeout,
            next_token: 0,
            stats: Stats::default(),
        }
    }

    /// A queue for one session, sized by [`QUEUE_DEPTH`]. The caller keeps the
    /// receiver for its writer thread and hands the sender to [`Sessions::open`].
    pub fn queue() -> (SyncSender<String>, std::sync::mpsc::Receiver<String>) {
        std::sync::mpsc::sync_channel(QUEUE_DEPTH)
    }

    /// Register `replica` in `session`, displacing whatever session it had.
    ///
    /// **Newest wins.** A replica that reconnects — because its connection
    /// broke, or because it restarted — must be reachable at once, and the
    /// alternative (refuse the new session while the old one is still nominally
    /// alive) makes a replica unreachable for as long as a dead TCP connection
    /// takes to notice it is dead, which can be minutes.
    ///
    /// Returns the token this connection must present to [`Sessions::close`].
    pub fn open(
        &mut self,
        session: &str,
        replica: &str,
        tx: SyncSender<String>,
        now: Instant,
    ) -> SessionToken {
        let token = SessionToken(self.next_token);
        self.next_token += 1;
        self.stats.sessions_opened += 1;

        let entry = Entry {
            session: session.to_string(),
            replica: replica.to_string(),
            token,
            tx,
            last_seen: now,
        };
        match self.index_of(session, replica) {
            Some(pos) => {
                // Dropping the old entry drops its sender, which is the signal
                // its writer thread waits on.
                self.entries[pos] = entry;
                self.stats.sessions_displaced += 1;
            }
            None => self.entries.push(entry),
        }
        token
    }

    /// Forward one frame to every live recipient named in it.
    ///
    /// Recipients are resolved **within the sender's session**, so two runs
    /// sharing one relay cannot reach each other however they name themselves.
    pub fn route(&mut self, session: &str, envelope: &Envelope<'_>, now: Instant) -> Delivery {
        self.stats.frames_in += 1;
        let mut outcome = Delivery::default();

        for recipient in envelope.to.as_slice() {
            let Some(pos) = self.index_of(session, recipient) else {
                outcome.absent += 1;
                self.stats.frames_absent += 1;
                continue;
            };
            let line = match serde_json::to_string(&Forwarded {
                to: recipient,
                from: &envelope.from,
                msg: envelope.msg,
            }) {
                Ok(line) => line,
                Err(_) => {
                    // Unreachable in practice: the parts came out of a parsed
                    // frame. Counted as a drop rather than panicking the
                    // session, because one malformed frame must not take a
                    // replica's whole link with it.
                    outcome.dropped += 1;
                    self.stats.frames_dropped += 1;
                    continue;
                }
            };
            match self.entries[pos].tx.try_send(line) {
                Ok(()) => {
                    self.entries[pos].last_seen = now;
                    outcome.delivered += 1;
                    self.stats.frames_out += 1;
                }
                Err(TrySendError::Full(_)) => {
                    outcome.dropped += 1;
                    self.stats.frames_dropped += 1;
                }
                Err(TrySendError::Disconnected(_)) => {
                    // The writer thread is gone; the session is dead and has
                    // not been reaped yet.
                    self.entries.remove(pos);
                    outcome.absent += 1;
                    self.stats.frames_absent += 1;
                }
            }
        }

        // The sender is transmitting, so it is not idle either.
        if let Some(pos) = self.index_of(session, &envelope.from) {
            self.entries[pos].last_seen = now;
        }
        outcome
    }

    /// Close the session `token` identifies, if it is still the registered one.
    ///
    /// Called by a connection on its way out. Silently does nothing when the
    /// token is stale, which is the whole reason it exists.
    pub fn close(&mut self, session: &str, replica: &str, token: SessionToken) -> bool {
        match self.index_of(session, replica) {
            Some(pos) if self.entries[pos].token == token => {
                self.entries.remove(pos);
                true
            }
            _ => false,
        }
    }

    /// Drop every session with no traffic inside the idle timeout.
    ///
    /// Driven by a thread of its own, unlike the bootnode's registry which
    /// sweeps on access. The difference is deliberate: a bootnode entry's
    /// expiry is only ever observed by a caller, so a lazily swept registry and
    /// an eagerly swept one are indistinguishable from outside. A relay session
    /// holds a socket and a writer thread, so its expiry has an effect nobody
    /// asked for — and two silent replicas would keep both forever if only a
    /// caller could trigger the sweep.
    pub fn sweep(&mut self, now: Instant) -> usize {
        let idle_timeout = self.idle_timeout;
        let before = self.entries.len();
        self.entries.retain(|entry| {
            // `duration_since` saturates rather than panicking on a `last_seen`
            // in the future, which a test constructing instants can produce.
            now.saturating_duration_since(entry.last_seen) < idle_timeout
        });
        let expired = before - self.entries.len();
        self.stats.sessions_expired += expired as u64;
        expired
    }

    pub fn stats(&self) -> Stats {
        Stats {
            sessions: self.entries.len(),
            ..self.stats
        }
    }

    /// Live sessions as `(session, replica)`, for `/health`.
    pub fn members(&self) -> Vec<(SessionId, ReplicaId)> {
        self.entries
            .iter()
            .map(|e| (e.session.clone(), e.replica.clone()))
            .collect()
    }

    fn index_of(&self, session: &str, replica: &str) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.session == session && e.replica == replica)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::Receiver;

    fn t0() -> Instant {
        Instant::now()
    }

    /// Parse a frame the way the connection handler does.
    fn envelope(line: &str) -> Envelope<'_> {
        serde_json::from_str(line).expect("parseable envelope")
    }

    fn open(sessions: &mut Sessions, s: &str, id: &str, now: Instant) -> Receiver<String> {
        let (tx, rx) = Sessions::queue();
        sessions.open(s, id, tx, now);
        rx
    }

    #[test]
    fn a_frame_reaches_the_named_recipient_and_nobody_else() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let b = open(&mut sessions, "s", "b", now);
        let c = open(&mut sessions, "s", "c", now);

        let line = r#"{"to":"b","from":"a","msg":{"type":"Hello","id":"a"}}"#;
        let outcome = sessions.route("s", &envelope(line), now);

        assert_eq!(
            outcome,
            Delivery {
                delivered: 1,
                dropped: 0,
                absent: 0
            }
        );
        let forwarded: serde_json::Value =
            serde_json::from_str(&b.try_recv().expect("b received the frame")).unwrap();
        assert_eq!(forwarded["to"], "b");
        assert_eq!(forwarded["from"], "a", "`from` must survive the hop");
        assert_eq!(forwarded["msg"]["type"], "Hello");
        assert!(c.try_recv().is_err(), "c was not addressed");
    }

    #[test]
    fn two_sessions_sharing_a_relay_cannot_reach_each_other() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let left_b = open(&mut sessions, "left", "b", now);
        let right_b = open(&mut sessions, "right", "b", now);

        let line = r#"{"to":"b","from":"a","msg":1}"#;
        assert_eq!(sessions.route("left", &envelope(line), now).delivered, 1);

        assert!(left_b.try_recv().is_ok());
        assert!(
            right_b.try_recv().is_err(),
            "the same replica id in another session is a different replica"
        );
    }

    #[test]
    fn a_recipient_list_is_one_frame_in_and_n_out() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let b = open(&mut sessions, "s", "b", now);
        let c = open(&mut sessions, "s", "c", now);
        let d = open(&mut sessions, "s", "d", now);

        let line = r#"{"to":["b","c","d"],"from":"a","msg":{"n":7}}"#;
        let outcome = sessions.route("s", &envelope(line), now);

        assert_eq!(outcome.delivered, 3);
        let stats = sessions.stats();
        assert_eq!(
            (stats.frames_in, stats.frames_out),
            (1, 3),
            "the whole point of the array form: one upload, N downloads"
        );
        for (name, rx) in [("b", &b), ("c", &c), ("d", &d)] {
            let forwarded: serde_json::Value =
                serde_json::from_str(&rx.try_recv().expect("received")).unwrap();
            assert_eq!(
                forwarded["to"], name,
                "each copy is addressed to its own recipient, not to the list"
            );
            assert_eq!(forwarded["msg"]["n"], 7);
        }
    }

    #[test]
    fn an_absent_recipient_is_dropped_and_the_rest_still_go() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let b = open(&mut sessions, "s", "b", now);

        let line = r#"{"to":["b","ghost"],"from":"a","msg":null}"#;
        let outcome = sessions.route("s", &envelope(line), now);

        assert_eq!(
            outcome,
            Delivery {
                delivered: 1,
                dropped: 0,
                absent: 1
            }
        );
        assert!(b.try_recv().is_ok(), "one absent recipient is not an error");
        assert_eq!(sessions.stats().frames_absent, 1);
    }

    #[test]
    fn the_newest_session_for_a_replica_wins() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let stale = open(&mut sessions, "s", "b", now);
        let fresh = open(&mut sessions, "s", "b", now);

        let line = r#"{"to":"b","from":"a","msg":2}"#;
        assert_eq!(sessions.route("s", &envelope(line), now).delivered, 1);

        assert!(fresh.try_recv().is_ok());
        assert!(
            stale.try_recv().is_err(),
            "the displaced session must receive nothing more"
        );
        assert_eq!(sessions.stats().sessions, 1, "and must not be counted");
        assert_eq!(sessions.stats().sessions_displaced, 1);
    }

    #[test]
    fn a_stale_connection_cannot_close_its_successors_session() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let (stale_tx, _stale_rx) = Sessions::queue();
        let stale_token = sessions.open("s", "b", stale_tx, now);
        let fresh = open(&mut sessions, "s", "b", now);

        assert!(
            !sessions.close("s", "b", stale_token),
            "the token is stale, so there is nothing for it to close"
        );

        let line = r#"{"to":"b","from":"a","msg":3}"#;
        assert_eq!(sessions.route("s", &envelope(line), now).delivered, 1);
        assert!(fresh.try_recv().is_ok(), "the live session survived");
    }

    #[test]
    fn a_session_closes_its_own_entry() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let (tx, _rx) = Sessions::queue();
        let token = sessions.open("s", "b", tx, now);

        assert!(sessions.close("s", "b", token));
        assert_eq!(sessions.stats().sessions, 0);

        let line = r#"{"to":"b","from":"a","msg":4}"#;
        assert_eq!(sessions.route("s", &envelope(line), now).absent, 1);
    }

    #[test]
    fn a_session_expires_once_it_stops_carrying_traffic() {
        let idle = Duration::from_secs(300);
        let mut sessions = Sessions::new(idle);
        let t = t0();
        let _b = open(&mut sessions, "s", "b", t);
        let _c = open(&mut sessions, "s", "c", t);

        // `c` keeps its session alive by receiving; `b` goes quiet.
        let later = t + idle - Duration::from_millis(1);
        let line = r#"{"to":"c","from":"a","msg":5}"#;
        sessions.route("s", &envelope(line), later);
        assert_eq!(sessions.sweep(later), 0, "not idle yet");

        let past = t + idle;
        assert_eq!(
            sessions.sweep(past),
            1,
            "`b` has been silent for the timeout"
        );
        assert_eq!(sessions.members(), vec![("s".to_string(), "c".to_string())]);
    }

    #[test]
    fn receiving_alone_keeps_a_session_alive() {
        let idle = Duration::from_secs(300);
        let mut sessions = Sessions::new(idle);
        let t = t0();
        let _a = open(&mut sessions, "s", "a", t);
        let b = open(&mut sessions, "s", "b", t);

        // `b` never sends anything; `a` keeps writing to it.
        let mut now = t;
        for _ in 0..3 {
            now += idle - Duration::from_millis(1);
            let line = r#"{"to":"b","from":"a","msg":6}"#;
            sessions.route("s", &envelope(line), now);
            assert_eq!(sessions.sweep(now), 0);
        }
        assert_eq!(sessions.stats().sessions, 2);
        assert!(b.try_recv().is_ok());
    }

    #[test]
    fn a_full_queue_drops_rather_than_blocking() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        // Nothing reads this, so the queue fills at QUEUE_DEPTH.
        let _b = open(&mut sessions, "s", "b", now);

        let line = r#"{"to":"b","from":"a","msg":0}"#;
        let start = Instant::now();
        let mut dropped = 0;
        for _ in 0..(QUEUE_DEPTH + 16) {
            dropped += sessions.route("s", &envelope(line), now).dropped;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "routing blocked; a full queue must drop so one slow consumer \
             cannot stall the sender's other recipients"
        );
        assert_eq!(dropped, 16);
        assert_eq!(sessions.stats().frames_dropped, 16);
    }

    #[test]
    fn a_disconnected_writer_reads_as_an_absent_recipient() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let b = open(&mut sessions, "s", "b", now);
        drop(b);

        let line = r#"{"to":"b","from":"a","msg":7}"#;
        assert_eq!(sessions.route("s", &envelope(line), now).absent, 1);
        assert_eq!(
            sessions.stats().sessions,
            0,
            "and the dead session is reaped on the way"
        );
    }

    // --- the format ---

    #[test]
    fn msg_travels_verbatim_whatever_it_is() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let b = open(&mut sessions, "s", "b", now);

        // None of these is a `TransportMessage`. The relay must not care, which
        // is the property that lets `msg` become ciphertext later without this
        // crate changing at all.
        for payload in [
            "42",
            r#""a string""#,
            "[1,2,3]",
            r#"{"unknown":{"deeply":{"nested":true}}}"#,
            "null",
        ] {
            let line = format!(r#"{{"to":"b","from":"a","msg":{payload}}}"#);
            assert_eq!(sessions.route("s", &envelope(&line), now).delivered, 1);
            let forwarded: serde_json::Value =
                serde_json::from_str(&b.try_recv().expect("received")).unwrap();
            assert_eq!(
                forwarded["msg"],
                serde_json::from_str::<serde_json::Value>(payload).unwrap(),
                "payload `{payload}` did not survive the hop unchanged"
            );
        }
    }

    #[test]
    fn a_hello_is_recognised_and_an_envelope_is_not() {
        let hello: HelloFrame =
            serde_json::from_str(r#"{"hello":{"session_id":"s","replica_id":"a"}}"#).unwrap();
        assert_eq!(hello.hello.session_id, "s");
        assert_eq!(hello.hello.replica_id, "a");

        assert!(
            serde_json::from_str::<HelloFrame>(r#"{"to":"b","from":"a","msg":1}"#).is_err(),
            "an envelope must not deserialise as a hello, or a frame sent \
             before the handshake would open a session"
        );
        assert!(
            serde_json::from_str::<Envelope>(r#"{"hello":{"session_id":"s","replica_id":"a"}}"#)
                .is_err(),
            "and a hello must not deserialise as an envelope"
        );
    }

    #[test]
    fn both_recipient_forms_parse() {
        let one: Envelope = serde_json::from_str(r#"{"to":"b","from":"a","msg":1}"#).unwrap();
        assert_eq!(one.to.as_slice(), ["b".to_string()]);

        let many: Envelope =
            serde_json::from_str(r#"{"to":["b","c"],"from":"a","msg":1}"#).unwrap();
        assert_eq!(many.to.as_slice(), ["b".to_string(), "c".to_string()]);

        let empty: Envelope = serde_json::from_str(r#"{"to":[],"from":"a","msg":1}"#).unwrap();
        assert!(
            empty.to.as_slice().is_empty(),
            "an empty list is well-formed and reaches nobody"
        );
    }

    #[test]
    fn a_frame_addressed_to_nobody_costs_one_inbound_and_no_outbound() {
        let mut sessions = Sessions::new(Duration::from_secs(300));
        let now = t0();
        let line = r#"{"to":[],"from":"a","msg":1}"#;
        assert_eq!(
            sessions.route("s", &envelope(line), now),
            Delivery::default()
        );
        let stats = sessions.stats();
        assert_eq!((stats.frames_in, stats.frames_out), (1, 0));
    }
}
