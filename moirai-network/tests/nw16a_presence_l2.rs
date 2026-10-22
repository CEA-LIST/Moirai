//! NW.16a, C3 and P4 at the transport (vault `spec/items/NW.16a.md`): what
//! `ZenohTransport` hands the node when a peer's liveliness token appears.
//!
//! - C3: a token that appears for a paused peer is handled as any frame from
//!   a paused peer is (`DirectTransport::try_recv`, `direct_transport.rs`):
//!   kept in that peer's buffer, counted by `buffered_count`, handed over by
//!   `drain_buffer` after `resume_peer`, and never yielded by `try_recv`
//!   while the pause lasts. The frame a token stands for is the `Hello` the
//!   transport yields for it when nobody is paused (note 5.2).
//! - P4, its transport half: a node whose same-id successor declares its
//!   token while the old session still lives (inside the 10 s lease, or
//!   frozen) greets the successor, a `Hello` from that id once more, so the
//!   node's `Hello` arm asks it for a sync; once the old session goes, the
//!   id stays `Connected` and a send reaches the successor. Zenoh's
//!   liveliness reports one key declared by two sessions once, alive at the
//!   first declaration and gone at the last (note section 12, Z7), so a
//!   transport that reads presence from that key alone never sees the
//!   successor arrive.
//!
//! Every session here is a real `ZenohTransport`, so whatever mechanism the
//! transport uses to announce itself runs on both sides. Configurations are
//! NW.15's rig (`nw15_common/zenoh_rig.rs`, read only): peer mode on
//! `tcp/127.0.0.1:0`, scouting and gossip off.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16a_presence_l2 -- --test-threads=1
//! ```

#[path = "nw15_common/contract.rs"]
mod contract;
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use std::thread;
use std::time::{Duration, Instant};

use contract::{collect, id, raw, status, Seen, DELIVERY};
use moirai_network::transport::{CrdtTransport, PeerStatus, TransportMessage};
use moirai_network::zenoh_transport::ZenohTransport;
use zenoh_rig::{fresh_session, loopback_peer, tcp_locator};

type Transport = ZenohTransport<String>;

/// How long a pause is watched for a `Hello` that must not come out.
const WATCH: Duration = Duration::from_secs(3);

fn open(me: &str, session: &str, known: &[&str], connect: &[String]) -> Transport {
    Transport::open(
        id(me),
        session,
        known.iter().map(|k| id(k)).collect(),
        loopback_peer(connect),
    )
    .unwrap_or_else(|e| panic!("open {me}: {e}"))
}

fn hellos_from(seen: &Seen, peer: &str) -> usize {
    seen.other
        .iter()
        .filter(|(from, frame)| {
            from == peer && matches!(frame, TransportMessage::Hello { id, .. } if id == peer)
        })
        .count()
}

fn goodbyes_from(seen: &Seen, peer: &str) -> usize {
    seen.other
        .iter()
        .filter(|(from, frame)| {
            from == peer && matches!(frame, TransportMessage::Goodbye { id } if id == peer)
        })
        .count()
}

/// Read `t` until `done` holds or `timeout` passes.
fn read_until(
    t: &mut Transport,
    seen: &mut Seen,
    timeout: Duration,
    mut done: impl FnMut(&Seen, &Transport) -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        collect(t, seen);
        if done(seen, t) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

/// C3. `a` knows `b` from `PEERS` and pauses it before `b` exists; `b`
/// then opens, connected to `a`. Its token reaches `a` during the pause.
#[test]
fn c3_a_token_appearing_for_a_paused_peer_is_kept_in_its_buffer() {
    let s = fresh_session("c3");
    let mut a = open("a", &s, &["b"], &[]);
    let a_at = tcp_locator(a.locators());
    a.pause_peer(&id("b"))
        .expect("pause b, which a knows from PEERS (NW.15: a known id can be paused)");
    let mut b = open("b", &s, &["a"], &[a_at]);

    // b must see a, so the link is up and a has b's token to report.
    let mut b_seen = Seen::default();
    assert!(
        read_until(&mut b, &mut b_seen, DELIVERY, |seen, _| hellos_from(
            seen, "a"
        ) >= 1),
        "set-up: b never saw a's token, so nothing reached a either"
    );

    // While b is paused at a, nothing from b may come out of a's try_recv;
    // b's token must show up as one buffered frame.
    let mut a_seen = Seen::default();
    let buffered = read_until(&mut a, &mut a_seen, DELIVERY, |seen, t| {
        hellos_from(seen, "b") > 0 || t.buffered_count(&id("b")) >= 1
    });
    assert_eq!(
        hellos_from(&a_seen, "b"),
        0,
        "C3: b's token appeared while a had b paused, and a's try_recv yielded b's Hello at \
         once; a frame from a paused peer is kept in its buffer until the pause is lifted, as \
         DirectTransport keeps it (a saw: {a_seen:?})"
    );
    assert!(
        buffered,
        "C3: b's token never showed at a within {DELIVERY:?}: no Hello came out and \
         buffered_count(b) stayed 0"
    );
    // Watch the pause a while longer: still nothing out, still one frame in.
    read_until(&mut a, &mut a_seen, WATCH, |_, _| false);
    assert_eq!(
        hellos_from(&a_seen, "b"),
        0,
        "C3: a yielded b's Hello during the pause: {a_seen:?}"
    );
    assert_eq!(
        a.buffered_count(&id("b")),
        1,
        "C3: one token appeared for paused b, so one frame waits in b's buffer"
    );
    assert_eq!(
        status(&a, "b"),
        Some(PeerStatus::Paused),
        "C3: b reads Paused at a during the pause"
    );

    a.resume_peer(&id("b")).expect("resume b");
    let drained = a.drain_buffer(&id("b"));
    assert!(
        matches!(drained.as_slice(), [TransportMessage::Hello { id, .. }] if id == "b"),
        "C3: after the resume, b's buffer hands over exactly b's Hello, which the node's \
         resume handler then delivers; drained {drained:?}"
    );
    // And b is an ordinary live peer afterwards.
    a.send(&id("b"), raw("after-resume"))
        .expect("C3: a send to b after the resume");
    assert!(
        read_until(&mut b, &mut b_seen, DELIVERY, |seen, _| seen
            .has("a", "after-resume")),
        "C3: a's send after the resume never reached b"
    );
}

/// P4 at the transport. `b1` runs as `b`; its successor `b2`, also `b`,
/// opens while `b1` still lives; then `b1` goes.
#[test]
fn p4_a_same_id_successor_that_appears_while_the_old_session_lives_is_greeted() {
    let s = fresh_session("p4");
    let mut a = open("a", &s, &["b"], &[]);
    let a_at = tcp_locator(a.locators());
    let b1 = open("b", &s, &["a"], std::slice::from_ref(&a_at));

    let mut a_seen = Seen::default();
    assert!(
        read_until(&mut a, &mut a_seen, DELIVERY, |seen, _| hellos_from(
            seen, "b"
        ) >= 1),
        "set-up: a never saw b's first session"
    );
    let first = hellos_from(&a_seen, "b");

    // The successor, while b1's session and its token still live.
    let mut b2 = open("b", &s, &["a"], &[a_at]);
    let greeted = read_until(&mut a, &mut a_seen, DELIVERY, |seen, _| {
        hellos_from(seen, "b") > first
    });
    assert!(
        greeted,
        "P4: b's successor declared its token while b's first session still lived, and a \
         yielded no new Hello from b within {DELIVERY:?}, so a's node never asks the successor \
         for a sync (a saw: {a_seen:?})"
    );

    // The old session goes; the successor stays the live b.
    drop(b1);
    read_until(&mut a, &mut a_seen, Duration::from_secs(3), |_, _| false);
    let after = read_until(&mut a, &mut a_seen, DELIVERY, |_, t| {
        status(t, "b") == Some(PeerStatus::Connected)
    });
    assert!(
        after,
        "P4: once b's first session closed, a reads b {:?} though b's successor is live \
         (Goodbyes seen: {})",
        status(&a, "b"),
        goodbyes_from(&a_seen, "b")
    );
    a.send(&id("b"), raw("to-the-successor"))
        .expect("P4: a send to b after the first session closed");
    let mut b2_seen = Seen::default();
    assert!(
        read_until(&mut b2, &mut b2_seen, DELIVERY, |seen, _| seen
            .has("a", "to-the-successor")),
        "P4: a's send to b never reached b's successor"
    );
}
