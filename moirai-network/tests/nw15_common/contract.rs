//! NW.15, points Z1 and Z4 (vault `spec/items/NW.15.md`): the contract a
//! `CrdtTransport` keeps, written once and run with the same expectations
//! against `DirectTransport` (`nw15_contract_direct.rs`, green at d996427)
//! and `ZenohTransport` (`nw15_contract_zenoh.rs`).
//!
//! Every expectation is written here by hand from `DirectTransport` as it is
//! at d996427 (`direct_transport.rs`) and from section 5 of the NW.14 note
//! (vault `spec/Zenoh transport, peer-to-peer and router.md`), which says the
//! Zenoh backend keeps those semantics by construction (5.6):
//!
//! - a send names one peer and reaches it once; a broadcast reaches every
//!   unpaused peer once, and never comes back to its sender;
//! - a pause is one-sided: the pauser's sends and broadcasts skip the paused
//!   peer, and what the paused peer sends is buffered at the pauser, counted
//!   by `buffered_count` and handed over by `drain_buffer`, while the paused
//!   peer, which paused nobody, keeps hearing everyone else;
//! - `pause_all` pauses every known peer and `resume_all` lifts it without
//!   discarding the buffers;
//! - a send to a peer nobody knows is `PeerNotFound`, and so is pausing one;
//! - a receiver that does not read for a while loses nothing (the receive
//!   queue is unbounded, 5.2);
//! - a message of 128 MiB arrives whole (5.5, Z4).
//!
//! "Nothing else arrived" is checked by reading on for `SETTLE` after the
//! frames that must arrive have arrived, so a stray frame is caught unless it
//! is slower than every frame put after it on the same link.
#![allow(dead_code)]

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use moirai_network::transport::{
    CrdtTransport, PeerId, PeerStatus, TransportError, TransportMessage,
};

/// The frames the suite sends: `Raw` with a text payload, the one variant a
/// transport carries without the node giving it a meaning.
pub type Msg = TransportMessage<String>;

/// How long a frame that must arrive may take. Generous: these are loopback
/// links in one process, and a miss is a failure, never a retry.
pub const DELIVERY: Duration = Duration::from_secs(20);
/// How long the suite keeps reading after the expected frames, to catch one
/// that must not arrive.
pub const SETTLE: Duration = Duration::from_millis(500);

/// One way of building transports for the suite.
pub trait Rig {
    type T: CrdtTransport<Op = String> + Send + 'static;

    /// One transport per id, each knowing every other id as a peer, linked
    /// pairwise on loopback, returned once every one reports every other
    /// connected.
    fn mesh(&mut self, ids: &[&str]) -> Vec<Self::T>;
}

/// What one transport has handed over so far.
#[derive(Default, Debug)]
pub struct Seen {
    /// `(from, payload)` of every `Raw` frame, in the order received.
    pub raw: Vec<(PeerId, String)>,
    /// `(from, frame)` of every other frame (`Hello`, `Goodbye`, ...).
    pub other: Vec<(PeerId, Msg)>,
}

impl Seen {
    pub fn has(&self, from: &str, payload: &str) -> bool {
        self.raw.iter().any(|(f, p)| f == from && p == payload)
    }

    /// The payloads received from `from`, in order.
    pub fn from(&self, from: &str) -> Vec<String> {
        self.raw
            .iter()
            .filter(|(f, _)| f == from)
            .map(|(_, p)| p.clone())
            .collect()
    }
}

pub fn raw(text: &str) -> Msg {
    TransportMessage::Raw {
        payload: text.to_string(),
    }
}

pub fn id(text: &str) -> PeerId {
    text.to_string()
}

/// Take what `t` has for us now.
///
/// `try_recv` may answer `Ok(None)` after buffering a paused peer's frame
/// while more frames wait (`DirectTransport` does), so one call is one pass,
/// and every wait below calls it again until its condition holds.
pub fn collect<T: CrdtTransport<Op = String>>(t: &mut T, seen: &mut Seen) {
    let _ = t.accept_connections();
    for _ in 0..100_000 {
        match t.try_recv() {
            Ok(Some((from, TransportMessage::Raw { payload }))) => seen.raw.push((from, payload)),
            Ok(Some((from, frame))) => seen.other.push((from, frame)),
            Ok(None) => break,
            Err(e) => panic!("try_recv failed on {}: {e}", t.local_id()),
        }
    }
}

/// Pump every transport until `done` holds; `false` if the deadline passed.
pub fn pump_until<T, F>(ts: &mut [T], seen: &mut [Seen], timeout: Duration, mut done: F) -> bool
where
    T: CrdtTransport<Op = String>,
    F: FnMut(&[Seen], &[T]) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        for (t, s) in ts.iter_mut().zip(seen.iter_mut()) {
            collect(t, s);
        }
        if done(seen, ts) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

/// Pump every transport for `period`.
pub fn settle<T: CrdtTransport<Op = String>>(ts: &mut [T], seen: &mut [Seen], period: Duration) {
    pump_until(ts, seen, period, |_, _| false);
}

pub fn status<T: CrdtTransport>(t: &T, peer: &str) -> Option<PeerStatus> {
    t.peers()
        .into_iter()
        .find(|p| p.id == peer)
        .map(|p| p.status)
}

/// Pump until every transport reports every other connected.
pub fn await_mesh<T: CrdtTransport<Op = String>>(ts: &mut [T], ids: &[&str]) {
    let mut scratch: Vec<Seen> = ids.iter().map(|_| Seen::default()).collect();
    let formed = pump_until(ts, &mut scratch, Duration::from_secs(30), |_, ts| {
        ts.iter().enumerate().all(|(i, t)| {
            ids.iter()
                .enumerate()
                .all(|(j, other)| i == j || t.is_connected(&id(other)))
        })
    });
    if !formed {
        let views: Vec<String> = ts
            .iter()
            .map(|t| format!("{}: {:?}", t.local_id(), t.peers()))
            .collect();
        panic!("the mesh {ids:?} did not form within 30 s: {views:?}");
    }
    // Any data frame here would be a frame nobody sent.
    for s in &scratch {
        assert!(
            s.raw.is_empty(),
            "a data frame arrived before any was sent: {s:?}"
        );
    }
}

fn seen_for(n: usize) -> Vec<Seen> {
    (0..n).map(|_| Seen::default()).collect()
}

/// Z1: a send reaches the peer it names, once, and nobody else.
pub fn z1_send_reaches_the_named_peer_once<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b", "c"]);
    let mut s = seen_for(3);
    t[0].send(&id("b"), raw("a to b")).expect("a.send(b)");
    t[0].send(&id("c"), raw("a to c")).expect("a.send(c)");
    t[1].send(&id("a"), raw("b to a")).expect("b.send(a)");
    let arrived = pump_until(&mut t, &mut s, DELIVERY, |s, _| {
        s[1].has("a", "a to b") && s[2].has("a", "a to c") && s[0].has("b", "b to a")
    });
    assert!(arrived, "a sent frame did not arrive: {s:?}");
    settle(&mut t, &mut s, SETTLE);
    assert_eq!(
        s[0].raw,
        vec![(id("b"), "b to a".to_string())],
        "a received"
    );
    assert_eq!(
        s[1].raw,
        vec![(id("a"), "a to b".to_string())],
        "b received"
    );
    assert_eq!(
        s[2].raw,
        vec![(id("a"), "a to c".to_string())],
        "c received"
    );
}

/// Z1: a broadcast reaches every unpaused peer once and never its sender.
pub fn z1_broadcast_reaches_every_peer_once_and_not_its_sender<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b", "c"]);
    let mut s = seen_for(3);
    t[0].broadcast(raw("a to all")).expect("a.broadcast");
    let arrived = pump_until(&mut t, &mut s, DELIVERY, |s, _| {
        s[1].has("a", "a to all") && s[2].has("a", "a to all")
    });
    assert!(arrived, "the broadcast did not reach b and c: {s:?}");
    settle(&mut t, &mut s, SETTLE);
    assert!(
        s[0].raw.is_empty(),
        "a's own broadcast came back to a: {:?}",
        s[0].raw
    );
    assert_eq!(
        s[1].raw,
        vec![(id("a"), "a to all".to_string())],
        "b received"
    );
    assert_eq!(
        s[2].raw,
        vec![(id("a"), "a to all".to_string())],
        "c received"
    );
}

/// Z1: a pauses b, and b does not pause a. Nothing a sends or broadcasts
/// reaches b; everything b sends to a waits in a's buffer; c, paused by
/// nobody, hears both; after a resumes b the buffer is handed over and a's
/// next broadcast reaches b.
///
/// This is the one-sided partition of the note's 5.2: with one put on
/// `all/a` during the pause, b would receive `m1`.
pub fn z1_one_sided_pause_holds_both_directions_at_the_pauser<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b", "c"]);
    let mut s = seen_for(3);

    t[0].pause_peer(&id("b")).expect("a.pause_peer(b)");
    assert_eq!(
        status(&t[0], "b"),
        Some(PeerStatus::Paused),
        "b at a while paused"
    );
    assert_eq!(
        status(&t[0], "c"),
        Some(PeerStatus::Connected),
        "c at a while b is paused"
    );
    assert!(
        !t[0].is_connected(&id("b")),
        "a reports the paused b connected"
    );
    assert!(
        t[0].is_connected(&id("c")),
        "a reports c, never paused, not connected"
    );

    t[0].broadcast(raw("m1 a broadcast during the pause"))
        .expect("a.broadcast during the pause");
    t[0].send(&id("b"), raw("m2 a to the paused b"))
        .expect("a send to a paused peer is skipped and succeeds");
    t[1].send(&id("a"), raw("m3 b to a")).expect("b.send(a)");
    t[1].broadcast(raw("m4 b broadcast")).expect("b.broadcast");
    t[2].send(&id("a"), raw("m5 c to a")).expect("c.send(a)");

    let arrived = pump_until(&mut t, &mut s, DELIVERY, |s, t| {
        s[2].has("a", "m1 a broadcast during the pause")
            && s[2].has("b", "m4 b broadcast")
            && s[0].has("c", "m5 c to a")
            && t[0].buffered_count(&id("b")) == 2
    });
    assert!(
        arrived,
        "c did not get m1 and m4, a did not get m5, or a did not buffer b's two frames \
         (buffered {}): {s:?}",
        t[0].buffered_count(&id("b"))
    );
    settle(&mut t, &mut s, SETTLE);
    assert!(
        s[1].from("a").is_empty(),
        "b, paused by a, received from a: {:?}",
        s[1].from("a")
    );
    assert!(
        s[0].from("b").is_empty(),
        "a handed over frames of the paused b before resuming it: {:?}",
        s[0].from("b")
    );
    assert_eq!(
        t[0].buffered_count(&id("b")),
        2,
        "frames of b buffered at a"
    );

    t[0].resume_peer(&id("b")).expect("a.resume_peer(b)");
    assert_eq!(
        status(&t[0], "b"),
        Some(PeerStatus::Connected),
        "b at a once resumed"
    );
    assert!(
        t[0].is_connected(&id("b")),
        "a reports the resumed b not connected"
    );
    let mut drained: Vec<String> = t[0]
        .drain_buffer(&id("b"))
        .into_iter()
        .map(|frame| match frame {
            TransportMessage::Raw { payload } => payload,
            other => panic!("a buffered a frame nobody sent: {other:?}"),
        })
        .collect();
    drained.sort();
    assert_eq!(
        drained,
        vec!["m3 b to a", "m4 b broadcast"],
        "a's buffer for b"
    );
    assert_eq!(
        t[0].buffered_count(&id("b")),
        0,
        "the buffer after drain_buffer"
    );

    t[0].broadcast(raw("m6 a broadcast after resume"))
        .expect("a.broadcast after resume");
    let arrived = pump_until(&mut t, &mut s, DELIVERY, |s, _| {
        s[1].has("a", "m6 a broadcast after resume") && s[2].has("a", "m6 a broadcast after resume")
    });
    assert!(
        arrived,
        "a's broadcast after resume did not reach b and c: {s:?}"
    );
    settle(&mut t, &mut s, SETTLE);
    assert_eq!(
        s[1].from("a"),
        vec!["m6 a broadcast after resume"],
        "b's frames from a"
    );
    let mut at_c = s[2].from("a");
    at_c.sort();
    assert_eq!(
        at_c,
        vec![
            "m1 a broadcast during the pause",
            "m6 a broadcast after resume"
        ],
        "c's frames from a"
    );
}

/// Z1: `pause_all` pauses every known peer, `resume_all` lifts the pause and
/// leaves what was buffered for `drain_buffer`.
pub fn z1_pause_all_and_resume_all_keep_the_buffers<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b", "c"]);
    let mut s = seen_for(3);

    t[0].pause_all().expect("a.pause_all");
    assert_eq!(
        status(&t[0], "b"),
        Some(PeerStatus::Paused),
        "b at a after pause_all"
    );
    assert_eq!(
        status(&t[0], "c"),
        Some(PeerStatus::Paused),
        "c at a after pause_all"
    );

    t[0].broadcast(raw("p1 a broadcast, all paused"))
        .expect("a.broadcast");
    t[0].send(&id("c"), raw("p2 a to the paused c"))
        .expect("a.send(c) while paused");
    t[1].send(&id("a"), raw("q1 b to a")).expect("b.send(a)");
    t[2].broadcast(raw("q2 c broadcast")).expect("c.broadcast");

    let arrived = pump_until(&mut t, &mut s, DELIVERY, |s, t| {
        s[1].has("c", "q2 c broadcast")
            && t[0].buffered_count(&id("b")) == 1
            && t[0].buffered_count(&id("c")) == 1
    });
    assert!(
        arrived,
        "b did not get c's broadcast, or a did not buffer one frame each of b and c \
         (b {}, c {}): {s:?}",
        t[0].buffered_count(&id("b")),
        t[0].buffered_count(&id("c"))
    );
    settle(&mut t, &mut s, SETTLE);
    assert!(
        s[0].raw.is_empty(),
        "a handed over frames while all were paused: {:?}",
        s[0].raw
    );
    assert!(
        s[1].from("a").is_empty(),
        "b received from a: {:?}",
        s[1].from("a")
    );
    assert!(
        s[2].from("a").is_empty(),
        "c received from a: {:?}",
        s[2].from("a")
    );

    t[0].resume_all().expect("a.resume_all");
    assert_eq!(
        status(&t[0], "b"),
        Some(PeerStatus::Connected),
        "b at a after resume_all"
    );
    assert_eq!(
        status(&t[0], "c"),
        Some(PeerStatus::Connected),
        "c at a after resume_all"
    );
    assert_eq!(
        t[0].buffered_count(&id("b")),
        1,
        "resume_all discarded b's buffer"
    );
    assert_eq!(
        t[0].buffered_count(&id("c")),
        1,
        "resume_all discarded c's buffer"
    );
    let from_b: Vec<Msg> = t[0].drain_buffer(&id("b"));
    let from_c: Vec<Msg> = t[0].drain_buffer(&id("c"));
    assert!(
        matches!(from_b.as_slice(), [TransportMessage::Raw { payload }] if payload == "q1 b to a"),
        "a's buffer for b: {from_b:?}"
    );
    assert!(
        matches!(from_c.as_slice(), [TransportMessage::Raw { payload }] if payload == "q2 c broadcast"),
        "a's buffer for c: {from_c:?}"
    );

    t[0].broadcast(raw("p3 a broadcast after resume_all"))
        .expect("a.broadcast");
    let arrived = pump_until(&mut t, &mut s, DELIVERY, |s, _| {
        s[1].has("a", "p3 a broadcast after resume_all")
            && s[2].has("a", "p3 a broadcast after resume_all")
    });
    assert!(
        arrived,
        "a's broadcast after resume_all did not arrive: {s:?}"
    );
    settle(&mut t, &mut s, SETTLE);
    assert_eq!(
        s[1].from("a"),
        vec!["p3 a broadcast after resume_all"],
        "b's frames from a"
    );
    assert_eq!(
        s[2].from("a"),
        vec!["p3 a broadcast after resume_all"],
        "c's frames from a"
    );
}

/// Z1: a peer nobody knows is `PeerNotFound` to send to and to pause; a peer
/// that is not paused cannot be resumed; nothing is buffered for either.
pub fn z1_unknown_peers_are_peer_not_found<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b"]);
    match t[0].send(&id("nobody"), raw("x")) {
        Err(TransportError::PeerNotFound(peer)) => assert_eq!(peer, "nobody"),
        other => panic!("send to an unknown peer answered {other:?}, not PeerNotFound"),
    }
    match t[0].pause_peer(&id("nobody")) {
        Err(TransportError::PeerNotFound(_)) => {}
        other => panic!("pausing an unknown peer answered {other:?}, not PeerNotFound"),
    }
    assert!(
        t[0].resume_peer(&id("b")).is_err(),
        "resuming b, which was never paused, succeeded"
    );
    assert_eq!(
        t[0].buffered_count(&id("nobody")),
        0,
        "buffered for an unknown peer"
    );
    assert!(
        t[0].drain_buffer(&id("nobody")).is_empty(),
        "drained for an unknown peer"
    );
    assert!(
        t[0].peers().iter().all(|p| p.id != "nobody"),
        "peers() lists a peer nobody named"
    );
    assert!(
        !t[0].is_connected(&id("nobody")),
        "an unknown peer reads connected"
    );
}

/// How many frames the unread receiver is sent, and how large each is:
/// 3000 frames of 16 KiB, 48 MiB in all, far more than the 256 samples of
/// Zenoh's default handler and than loopback's socket buffers.
pub const UNREAD_FRAMES: usize = 3000;
pub const UNREAD_FRAME_BYTES: usize = 16 * 1024;
/// How long the receiver does not read: longer than the 5 s after which
/// Zenoh closes a link whose sender has been blocked (`wait_before_close`).
pub const UNREAD_HOLD: Duration = Duration::from_secs(8);

/// Z1: a receiver that does not call `try_recv` for `UNREAD_HOLD` loses
/// nothing, and its sender is not held up meanwhile.
///
/// The node loop is a receiver that sometimes does not read: one long apply
/// and the transport must hold everything that arrives meanwhile. With
/// Zenoh's default handler, a FIFO of 256 whose producer blocks when full
/// (the note's Z3), the sender blocks, Zenoh closes the link after 5 s, and
/// what was put meanwhile is gone.
pub fn z1_a_receiver_that_does_not_read_loses_nothing<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b"]);
    let mut b = t.pop().expect("b");
    let mut a = t.pop().expect("a");
    let filler = "q".repeat(UNREAD_FRAME_BYTES);
    let start = Instant::now();
    let (done_tx, done_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut failed = Vec::new();
        for i in 0..UNREAD_FRAMES {
            if let Err(e) = a.send(&id("b"), raw(&format!("{i:05}:{filler}"))) {
                failed.push(format!("{i}: {e}"));
            }
        }
        let _ = done_tx.send((failed, start.elapsed(), a));
    });
    let finished = done_rx.recv_timeout(UNREAD_HOLD);
    let left = UNREAD_HOLD.saturating_sub(start.elapsed());
    thread::sleep(left);
    let (failed, took, _a) = match finished {
        Ok(done) => done,
        Err(_) => panic!(
            "a's {UNREAD_FRAMES} sends were still blocked after {:?} while b did not read",
            UNREAD_HOLD
        ),
    };
    assert!(
        failed.is_empty(),
        "sends that failed while b did not read: {failed:?}"
    );
    println!("NW15 unread receiver: {UNREAD_FRAMES} sends returned after {took:?}");

    let mut seen = Seen::default();
    let deadline = Instant::now() + Duration::from_secs(60);
    while seen.raw.len() < UNREAD_FRAMES && Instant::now() < deadline {
        collect(&mut b, &mut seen);
        thread::sleep(Duration::from_millis(2));
    }
    settle(
        std::slice::from_mut(&mut b),
        std::slice::from_mut(&mut seen),
        SETTLE,
    );
    let mut numbers: Vec<usize> = seen
        .raw
        .iter()
        .map(|(from, payload)| {
            assert_eq!(from, "a", "a frame from someone else");
            payload[..5].parse().expect("a numbered frame")
        })
        .collect();
    let received = numbers.len();
    numbers.sort_unstable();
    numbers.dedup();
    assert_eq!(
        (received, numbers.len()),
        (UNREAD_FRAMES, UNREAD_FRAMES),
        "b received {received} frames, {} distinct, of {UNREAD_FRAMES} sent while it did not read",
        numbers.len()
    );
}

/// The size of Z4's payload: 128 MiB exactly, so the frame around it is more.
pub const LARGE_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

/// FNV-1a, 64 bits: enough to tell one 128 MiB payload from another without
/// holding two copies of it.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

fn high_water_mark() -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM"))
                .map(str::to_string)
        })
        .unwrap_or_else(|| "VmHWM unknown".to_string())
}

/// Z4: a message of 128 MiB arrives whole at the transport.
///
/// The payload is not uniform, so a lost or repeated fragment changes its
/// hash. Time and memory are printed as an L2 resource check (note 11), not
/// a threshold.
pub fn z4_a_128_mib_message_arrives_whole<R: Rig>(rig: &mut R) {
    let mut t = rig.mesh(&["a", "b"]);
    let mut payload = String::with_capacity(LARGE_PAYLOAD_BYTES);
    let mut line = 0u64;
    while payload.len() < LARGE_PAYLOAD_BYTES {
        let piece = format!("{line:016x}");
        let room = LARGE_PAYLOAD_BYTES - payload.len();
        payload.push_str(&piece[..piece.len().min(room)]);
        line += 1;
    }
    let expected = fnv1a(payload.as_bytes());
    let start = Instant::now();
    t[0].send(&id("b"), TransportMessage::Raw { payload })
        .expect("a.send(b) of 128 MiB");
    let sent = start.elapsed();
    let mut seen = Seen::default();
    let deadline = Instant::now() + Duration::from_secs(120);
    while seen.raw.is_empty() && Instant::now() < deadline {
        collect(&mut t[1], &mut seen);
        thread::sleep(Duration::from_millis(5));
    }
    let received = start.elapsed();
    assert_eq!(
        seen.raw.len(),
        1,
        "frames at b after the 128 MiB send: {}",
        seen.raw.len()
    );
    let (from, got) = &seen.raw[0];
    assert_eq!(from, "a");
    assert_eq!(
        got.len(),
        LARGE_PAYLOAD_BYTES,
        "the 128 MiB payload's length at b"
    );
    assert_eq!(
        fnv1a(got.as_bytes()),
        expected,
        "the 128 MiB payload's content at b"
    );
    println!(
        "NW15-RESOURCE z4: send returned after {sent:?}, frame whole at b after {received:?}, {}",
        high_water_mark()
    );
}
