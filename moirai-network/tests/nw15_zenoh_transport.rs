//! NW.15, what only a Zenoh backend has (vault `spec/items/NW.15.md`): the
//! key expressions and the JSON on them, presence by liveliness tokens, the
//! sender read from the key, `CongestionControl::Block`, the own token
//! declared after the subscribers, ids that are not key chunks, two Moirai
//! sessions on one link, and a closed session and its successor.
//!
//! The ground truth is section 5 of the NW.14 note (vault `spec/Zenoh
//! transport, peer-to-peer and router.md`), read from outside the backend:
//! a raw Zenoh session in this file subscribes, publishes and declares tokens
//! on the keys the note fixes, so a backend that used other keys, another
//! encoding or another order would be seen doing so.
//!
//! - `moirai/<session>/all/<sender>`: a broadcast while no peer is paused.
//! - `moirai/<session>/to/<recipient>/<sender>`: a send, and a broadcast
//!   while any peer is paused, one put per live unpaused peer.
//! - `moirai/<session>/node/<peer>`: one liveliness token per node.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_zenoh_transport -- --test-threads=1
//! ```

#[path = "nw15_common/contract.rs"]
mod contract;
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use contract::{collect, id, raw, status, Seen, DELIVERY, SETTLE};
use moirai_network::transport::{CrdtTransport, PeerStatus, TransportError, TransportMessage};
use moirai_network::zenoh_transport::ZenohTransport;
use zenoh::qos::CongestionControl;
use zenoh::sample::{Sample, SampleKind};
use zenoh::Wait;
use zenoh_rig::{fresh_session, last_chunk, loopback_peer, raw_locator, raw_session, tcp_locator};

type Transport = ZenohTransport<String>;

/// `(key, payload)` of every sample a raw observer received.
type Puts = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

fn poll(timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

/// Pump one transport until `done` holds.
fn pump(
    t: &mut Transport,
    seen: &mut Seen,
    timeout: Duration,
    mut done: impl FnMut(&Seen, &Transport) -> bool,
) -> bool {
    poll(timeout, || {
        collect(t, seen);
        done(seen, t)
    })
}

/// The `Raw` payload of a sample's bytes, if they are a `TransportMessage`
/// in JSON at all.
fn payload_text(bytes: &[u8]) -> Option<String> {
    match serde_json::from_slice::<TransportMessage<String>>(bytes) {
        Ok(TransportMessage::Raw { payload }) => Some(payload),
        _ => None,
    }
}

fn json_of(text: &str) -> Vec<u8> {
    serde_json::to_vec(&raw(text)).expect("a frame as JSON")
}

fn hello_from(seen: &Seen, peer: &str) -> bool {
    seen.other.iter().any(|(from, frame)| {
        from == peer && matches!(frame, TransportMessage::Hello { id, .. } if id == peer)
    })
}

fn goodbye_from(seen: &Seen, peer: &str) -> bool {
    seen.other
        .iter()
        .any(|(_, frame)| matches!(frame, TransportMessage::Goodbye { id } if id == peer))
}

/// Keys, JSON and presence, read by a raw observer `o`.
///
/// a knows `o` (live) and `b` (named in its peers, never started). A
/// broadcast with nobody paused is one put on `all/a`; a send to o is one put
/// on `to/o/a`; with b paused a broadcast is one put on `to/o/a` and none on
/// `all/a`; with o paused too nobody live is unpaused and nothing is put; a's
/// token is `node/a`; every payload is a `TransportMessage` in JSON; and a
/// never receives its own puts.
#[test]
fn keys_frames_and_presence_as_seen_from_outside() {
    let s = fresh_session("k");
    let observer = raw_session(&[]);
    let observer_at = raw_locator(&observer);
    let puts: Puts = Arc::default();
    let sink = Arc::clone(&puts);
    let _all = observer
        .declare_subscriber(format!("moirai/{s}/**"))
        .callback(move |sample: Sample| {
            sink.lock().unwrap().push((
                sample.key_expr().as_str().to_string(),
                sample.payload().to_bytes().to_vec(),
            ));
        })
        .wait()
        .expect("the observer's subscriber");
    let tokens: Arc<Mutex<Vec<String>>> = Arc::default();
    let token_sink = Arc::clone(&tokens);
    let _presence = observer
        .liveliness()
        .declare_subscriber(format!("moirai/{s}/node/*"))
        .history(true)
        .callback(move |sample: Sample| {
            if sample.kind() == SampleKind::Put {
                token_sink
                    .lock()
                    .unwrap()
                    .push(sample.key_expr().as_str().to_string());
            }
        })
        .wait()
        .expect("the observer's liveliness subscriber");
    let _o = observer
        .liveliness()
        .declare_token(format!("moirai/{s}/node/o"))
        .wait()
        .expect("o's token");

    let mut a = Transport::open(
        id("a"),
        &s,
        vec![id("o"), id("b")],
        loopback_peer(&[observer_at]),
    )
    .expect("open a");
    let mut seen = Seen::default();
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |seen, a| a
            .is_connected(&id("o"))
            && hello_from(seen, "o")),
        "a never reported o, whose token is live, connected with a Hello: {:?} {:?}",
        a.peers(),
        seen.other
    );
    assert_eq!(status(&a, "o"), Some(PeerStatus::Connected), "o at a");
    assert_eq!(
        status(&a, "b"),
        Some(PeerStatus::Disconnected),
        "b, named but never live, at a"
    );
    let a_token = format!("moirai/{s}/node/a");
    assert!(
        poll(DELIVERY, || tokens.lock().unwrap().contains(&a_token)),
        "the observer never saw a's token {a_token}: {:?}",
        tokens.lock().unwrap()
    );

    let keys_of = |text: &str| -> Vec<String> {
        puts.lock()
            .unwrap()
            .iter()
            .filter(|(_, bytes)| payload_text(bytes).as_deref() == Some(text))
            .map(|(key, _)| key.clone())
            .collect()
    };
    let all_a = format!("moirai/{s}/all/a");
    let to_o = format!("moirai/{s}/to/o/a");
    let expect_keys = |text: &str, expected: &str| {
        assert!(
            poll(DELIVERY, || !keys_of(text).is_empty()),
            "the observer never saw `{text}`"
        );
        thread::sleep(SETTLE);
        assert_eq!(
            keys_of(text),
            vec![expected.to_string()],
            "the puts of `{text}`"
        );
    };

    a.broadcast(raw("k1 broadcast, nobody paused"))
        .expect("broadcast k1");
    expect_keys("k1 broadcast, nobody paused", &all_a);

    a.send(&id("o"), raw("k2 send to o")).expect("send k2");
    expect_keys("k2 send to o", &to_o);

    a.pause_peer(&id("b")).expect("pause b");
    a.broadcast(raw("k3 broadcast while b is paused"))
        .expect("broadcast k3");
    expect_keys("k3 broadcast while b is paused", &to_o);

    a.pause_peer(&id("o")).expect("pause o");
    a.broadcast(raw("k4 broadcast while b and o are paused"))
        .expect("broadcast k4");
    a.send(&id("o"), raw("k5 send to the paused o"))
        .expect("a send to a paused peer succeeds");
    a.resume_peer(&id("o")).expect("resume o");
    a.send(&id("o"), raw("k6 send to o after resume"))
        .expect("send k6");
    expect_keys("k6 send to o after resume", &to_o);
    assert!(
        keys_of("k4 broadcast while b and o are paused").is_empty(),
        "a put a broadcast while every live peer was paused: {:?}",
        keys_of("k4 broadcast while b and o are paused")
    );
    assert!(
        keys_of("k5 send to the paused o").is_empty(),
        "a put a send to a paused peer"
    );

    a.resume_peer(&id("b")).expect("resume b");
    a.broadcast(raw("k7 broadcast after both resumed"))
        .expect("broadcast k7");
    expect_keys("k7 broadcast after both resumed", &all_a);

    let mut own = Seen::default();
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        collect(&mut a, &mut own);
        thread::sleep(Duration::from_millis(2));
    }
    assert!(own.raw.is_empty(), "a received its own puts: {:?}", own.raw);
    for (key, bytes) in puts.lock().unwrap().iter() {
        assert!(
            serde_json::from_slice::<TransportMessage<String>>(bytes).is_ok(),
            "a put on {key} is not a TransportMessage in JSON"
        );
    }
}

/// Frames put by a raw session `r` on the note's keys reach a under the
/// sender `r` read from the key; a frame for another recipient or another
/// Moirai session does not; r's token is a's `Hello`, and its going away a
/// `Goodbye`, after which r reads `Disconnected` and a send to it is
/// `PeerNotFound`.
#[test]
fn frames_from_outside_arrive_under_the_sender_named_by_the_key() {
    let s = fresh_session("r");
    let elsewhere = fresh_session("r");
    let mut a = Transport::open(id("a"), &s, vec![id("r")], loopback_peer(&[])).expect("open a");
    let a_at = tcp_locator(a.locators());
    let r = raw_session(&[a_at]);
    let token = r
        .liveliness()
        .declare_token(format!("moirai/{s}/node/r"))
        .wait()
        .expect("r's token");
    let mut seen = Seen::default();
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |seen, a| hello_from(seen, "r")
            && a.is_connected(&id("r"))),
        "a did not yield Hello for r's token: {:?} {:?}",
        seen.other,
        a.peers()
    );
    assert_eq!(status(&a, "r"), Some(PeerStatus::Connected));

    let put = |key: String, text: &str| {
        r.put(key, json_of(text))
            .congestion_control(CongestionControl::Block)
            .wait()
            .expect("a raw put");
    };
    put(format!("moirai/{s}/all/r"), "r1 on all");
    put(format!("moirai/{s}/to/a/r"), "r2 to a");
    put(format!("moirai/{s}/to/z/r"), "r3 to z, not a");
    put(
        format!("moirai/{elsewhere}/all/r"),
        "r4 another session's all",
    );
    put(
        format!("moirai/{elsewhere}/to/a/r"),
        "r5 another session's to a",
    );
    put(format!("moirai/{s}/to/a/r"), "r6 to a, last");
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |seen, _| seen
            .has("r", "r6 to a, last")),
        "a did not receive r's last frame: {:?}",
        seen.raw
    );
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        collect(&mut a, &mut seen);
        thread::sleep(Duration::from_millis(2));
    }
    let mut got: Vec<(String, String)> = seen.raw.clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            (id("r"), "r1 on all".to_string()),
            (id("r"), "r2 to a".to_string()),
            (id("r"), "r6 to a, last".to_string()),
        ],
        "what a received from r"
    );

    token.undeclare().wait().expect("undeclare r's token");
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |seen, _| goodbye_from(
            seen, "r"
        )),
        "a did not yield Goodbye when r's token went away: {:?}",
        seen.other
    );
    assert_eq!(
        status(&a, "r"),
        Some(PeerStatus::Disconnected),
        "r at a after its token went"
    );
    assert!(!a.is_connected(&id("r")));
    match a.send(&id("r"), raw("to r, gone")) {
        Err(TransportError::PeerNotFound(peer)) => assert_eq!(peer, "r"),
        other => panic!("a send to a peer with no live token answered {other:?}"),
    }
}

/// Frames the slow receiver is sent per phase, and the filler that makes
/// each one about 60 kB of JSON: 90 MB a phase, more than loopback's socket
/// buffers and Zenoh's queues hold.
const CONGESTED_FRAMES: usize = 1500;
const CONGESTED_FILLER: usize = 60_000;
/// What the slow receiver spends on each sample before taking the next.
const SLOW_RECEIVER_DELAY: Duration = Duration::from_millis(1);

fn slow_receiver(
    got: Arc<Mutex<Vec<(String, String)>>>,
) -> impl Fn(Sample) + Send + Sync + 'static {
    move |sample: Sample| {
        thread::sleep(SLOW_RECEIVER_DELAY);
        let key = sample.key_expr().as_str().to_string();
        let tag = payload_text(&sample.payload().to_bytes())
            .map(|p| p.splitn(3, '|').take(2).collect::<Vec<_>>().join("|"))
            .unwrap_or_else(|| "not a frame".to_string());
        got.lock().unwrap().push((key, tag));
    }
}

/// A congested live link loses no put (`CongestionControl::Block`, note
/// 5.4). A raw receiver `slow` takes 1 ms per sample, so a's queue to it
/// fills; under Zenoh's default for a put, `Drop`, the put waits 1 ms and is
/// dropped while the link stays up, and nothing in Moirai asks again. Three
/// phases, one per put a node makes: a send, a broadcast with nobody paused,
/// and a broadcast while another peer is paused.
#[test]
fn a_congested_link_loses_no_put() {
    let s = fresh_session("q");
    let slow = raw_session(&[]);
    let slow_at = raw_locator(&slow);
    let got: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let _to = slow
        .declare_subscriber(format!("moirai/{s}/to/slow/*"))
        .callback(slow_receiver(Arc::clone(&got)))
        .wait()
        .expect("slow's to subscriber");
    let _all = slow
        .declare_subscriber(format!("moirai/{s}/all/*"))
        .callback(slow_receiver(Arc::clone(&got)))
        .wait()
        .expect("slow's all subscriber");
    let _token = slow
        .liveliness()
        .declare_token(format!("moirai/{s}/node/slow"))
        .wait()
        .expect("slow's token");

    let mut a = Transport::open(
        id("a"),
        &s,
        vec![id("slow"), id("x")],
        loopback_peer(&[slow_at]),
    )
    .expect("open a");
    let mut seen = Seen::default();
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |_, a| a
            .is_connected(&id("slow"))),
        "a never saw slow's token"
    );
    let filler = "w".repeat(CONGESTED_FILLER);
    let to_slow = format!("moirai/{s}/to/slow/a");
    let all_a = format!("moirai/{s}/all/a");
    for (phase, key) in [
        ("send", &to_slow),
        ("broadcast", &all_a),
        ("paused-broadcast", &to_slow),
    ] {
        if phase == "paused-broadcast" {
            a.pause_peer(&id("x")).expect("pause x");
        }
        let start = Instant::now();
        for i in 0..CONGESTED_FRAMES {
            let frame = raw(&format!("{phase}|{i:05}|{filler}"));
            let sent = if phase == "send" {
                a.send(&id("slow"), frame)
            } else {
                a.broadcast(frame)
            };
            sent.unwrap_or_else(|e| panic!("{phase} {i} failed: {e}"));
        }
        let puts_took = start.elapsed();
        let prefix = format!("{phase}|");
        let count = || {
            got.lock()
                .unwrap()
                .iter()
                .filter(|(_, tag)| tag.starts_with(&prefix))
                .count()
        };
        let mut last = count();
        let mut moved = Instant::now();
        while last < CONGESTED_FRAMES && moved.elapsed() < Duration::from_secs(10) {
            thread::sleep(Duration::from_millis(50));
            let now = count();
            if now != last {
                last = now;
                moved = Instant::now();
            }
        }
        let frames: Vec<(String, String)> = got
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, tag)| tag.starts_with(&prefix))
            .cloned()
            .collect();
        let distinct: BTreeSet<&String> = frames.iter().map(|(_, tag)| tag).collect();
        println!(
            "NW15 congested {phase}: puts returned after {puts_took:?}, {} of {CONGESTED_FRAMES} \
             received after {:?}",
            frames.len(),
            start.elapsed()
        );
        assert_eq!(
            (frames.len(), distinct.len()),
            (CONGESTED_FRAMES, CONGESTED_FRAMES),
            "the slow receiver got {} frames, {} distinct, of {CONGESTED_FRAMES} in phase {phase}",
            frames.len(),
            distinct.len()
        );
        let keys: BTreeSet<&String> = frames.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, BTreeSet::from([key]), "the keys of phase {phase}");
    }
}

/// The own token is declared after the subscribers (note 5.2).
///
/// A raw observer `o`, already up, answers a node's token from inside its
/// liveliness callback with a put on `to/<node>/o`. Zenoh runs that
/// callback while it handles the token's declaration and before it handles
/// anything declared after the token, with its routing tables unlocked, so
/// the put reaches the node only if the node's subscriber on `to/<node>/*`
/// was declared before its token. That is the failure the order prevents: a
/// peer that answers a new token at once must find the subscriber there.
/// Rehearsed on raw sessions before this oracle was committed: subscribers
/// first, 20 of 20 answers arrived; token first, 0 of 20.
#[test]
fn the_own_token_is_declared_after_the_subscribers() {
    for round in 0..3 {
        let s = fresh_session("p");
        let observer = raw_session(&[]);
        let observer_at = raw_locator(&observer);
        let answering = observer.clone();
        let session = s.clone();
        let _presence = observer
            .liveliness()
            .declare_subscriber(format!("moirai/{s}/node/*"))
            .callback(move |sample: Sample| {
                let peer = last_chunk(sample.key_expr().as_str());
                if sample.kind() == SampleKind::Put && peer != "o" {
                    let _ = answering
                        .put(
                            format!("moirai/{session}/to/{peer}/o"),
                            json_of("answered from the token callback"),
                        )
                        .congestion_control(CongestionControl::Block)
                        .wait();
                }
            })
            .wait()
            .expect("the observer's liveliness subscriber");
        let _o = observer
            .liveliness()
            .declare_token(format!("moirai/{s}/node/o"))
            .wait()
            .expect("o's token");
        let mut a = Transport::open(id("a"), &s, vec![id("o")], loopback_peer(&[observer_at]))
            .expect("open a");
        let mut seen = Seen::default();
        assert!(
            pump(&mut a, &mut seen, Duration::from_secs(10), |seen, _| {
                seen.has("o", "answered from the token callback")
            }),
            "round {round}: the answer to a's token never reached a, so a's subscriber was not \
             there when its token was: {:?}",
            seen
        );
        drop(a);
        // The callback holds a handle on the observer, so it closes only when
        // told to.
        observer.close().wait().expect("close the observer");
    }
}

/// An id that is not a key chunk is refused at start, naming `REPLICA_ID`
/// (note 5.1); the ids in use today are accepted.
#[test]
fn ids_that_are_not_key_chunks_are_refused_naming_replica_id() {
    let s = fresh_session("i");
    for bad in ["", "a/b", "/a", "a/", "a*", "*", "**", "a$b", "a?b", "a#b"] {
        match Transport::open(bad.to_string(), &s, vec![], loopback_peer(&[])) {
            Err(e) => assert!(
                e.to_string().contains("REPLICA_ID"),
                "the refusal of {bad:?} does not name REPLICA_ID: {e}"
            ),
            Ok(_) => panic!("the id {bad:?} was accepted"),
        }
    }
    let principal = "0123456789abcdef".repeat(4);
    for good in ["a", "si14-c", "dt-alice", "x.y:z", principal.as_str()] {
        let opened = Transport::open(good.to_string(), &s, vec![], loopback_peer(&[]));
        assert!(
            opened.is_ok(),
            "the id {good:?} was refused: {:?}",
            opened.err().map(|e| e.to_string())
        );
    }
}

/// Two Moirai sessions on one Zenoh link do not see each other (`<session>`
/// is a key chunk): b, in another session, gets no frame and no token of a,
/// while c, in a's session on the same link, gets both.
#[test]
fn two_moirai_sessions_on_one_link_do_not_see_each_other() {
    let ours = fresh_session("s");
    let theirs = fresh_session("s");
    let mut a = Transport::open(id("a"), &ours, vec![id("b"), id("c")], loopback_peer(&[]))
        .expect("open a");
    let a_at = tcp_locator(a.locators());
    let mut b = Transport::open(
        id("b"),
        &theirs,
        vec![id("a")],
        loopback_peer(std::slice::from_ref(&a_at)),
    )
    .expect("open b");
    let mut c =
        Transport::open(id("c"), &ours, vec![id("a")], loopback_peer(&[a_at])).expect("open c");
    let (mut sa, mut sb, mut sc) = (Seen::default(), Seen::default(), Seen::default());
    assert!(
        poll(DELIVERY, || {
            collect(&mut a, &mut sa);
            collect(&mut b, &mut sb);
            collect(&mut c, &mut sc);
            a.is_connected(&id("c")) && c.is_connected(&id("a"))
        }),
        "a and c, in one session, never saw each other"
    );
    a.broadcast(raw("s1 a broadcast")).expect("a.broadcast");
    a.send(&id("c"), raw("s2 a to c")).expect("a.send(c)");
    b.broadcast(raw("s3 b broadcast")).expect("b.broadcast");
    assert!(
        poll(DELIVERY, || {
            collect(&mut a, &mut sa);
            collect(&mut b, &mut sb);
            collect(&mut c, &mut sc);
            sc.has("a", "s1 a broadcast") && sc.has("a", "s2 a to c")
        }),
        "c did not get a's frames: {:?}",
        sc.raw
    );
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        collect(&mut a, &mut sa);
        collect(&mut b, &mut sb);
        collect(&mut c, &mut sc);
        thread::sleep(Duration::from_millis(2));
    }
    assert!(
        sb.raw.is_empty(),
        "b, in another session, received: {:?}",
        sb.raw
    );
    assert!(
        !hello_from(&sb, "a"),
        "b, in another session, saw a's token"
    );
    assert!(
        !b.is_connected(&id("a")),
        "b reports a, of another session, connected"
    );
    assert!(
        !a.is_connected(&id("b")),
        "a reports b, of another session, connected"
    );
    assert_ne!(status(&a, "b"), Some(PeerStatus::Connected));
    assert!(
        sa.raw.is_empty() && sc.from("b").is_empty(),
        "b's broadcast crossed sessions"
    );
    match a.send(&id("b"), raw("s4 a to b")) {
        Err(TransportError::PeerNotFound(_)) => {}
        other => panic!("a send to a peer of another session answered {other:?}"),
    }
}

/// A closed session reads `Disconnected` (a `Goodbye`, then
/// `PeerNotFound`), and a successor session with the same id is found again
/// (a `Hello`) and reached.
#[test]
fn a_closed_session_reads_disconnected_and_its_successor_is_reached() {
    let s = fresh_session("x");
    let mut a = Transport::open(id("a"), &s, vec![id("b")], loopback_peer(&[])).expect("open a");
    let a_at = tcp_locator(a.locators());
    let b = Transport::open(
        id("b"),
        &s,
        vec![id("a")],
        loopback_peer(std::slice::from_ref(&a_at)),
    )
    .expect("open b");
    let mut seen = Seen::default();
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |_, a| a.is_connected(&id("b"))),
        "a never saw b"
    );
    drop(b);
    assert!(
        pump(&mut a, &mut seen, Duration::from_secs(15), |seen, _| {
            goodbye_from(seen, "b")
        }),
        "a did not yield Goodbye after b's session closed: {:?}",
        seen.other
    );
    assert_eq!(
        status(&a, "b"),
        Some(PeerStatus::Disconnected),
        "b at a once closed"
    );
    assert!(!a.is_connected(&id("b")));
    match a.send(&id("b"), raw("to the closed b")) {
        Err(TransportError::PeerNotFound(peer)) => assert_eq!(peer, "b"),
        other => panic!("a send to a closed session answered {other:?}"),
    }

    let hellos_before = seen
        .other
        .iter()
        .filter(|(_, f)| matches!(f, TransportMessage::Hello { id, .. } if id == "b"))
        .count();
    let mut successor = Transport::open(id("b"), &s, vec![id("a")], loopback_peer(&[a_at]))
        .expect("open b's successor");
    assert!(
        pump(&mut a, &mut seen, DELIVERY, |seen, a| {
            a.is_connected(&id("b"))
                && seen
                    .other
                    .iter()
                    .filter(|(_, f)| matches!(f, TransportMessage::Hello { id, .. } if id == "b"))
                    .count()
                    > hellos_before
        }),
        "a did not yield a new Hello for b's successor: {:?}",
        seen.other
    );
    a.send(&id("b"), raw("to the successor"))
        .expect("a.send(b) to the successor");
    let mut at_successor = Seen::default();
    assert!(
        pump(&mut successor, &mut at_successor, DELIVERY, |seen, _| {
            seen.has("a", "to the successor")
        }),
        "b's successor did not receive a's frame: {:?}",
        at_successor
    );
}
