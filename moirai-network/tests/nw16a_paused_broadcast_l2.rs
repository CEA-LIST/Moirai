//! NW.16a, C2 (vault `spec/items/NW.16a.md`): a broadcast while a peer is
//! paused reports a failed per-peer put as `send` reports a failed put, as
//! an error, never as a log line and `Ok`.
//!
//! While any peer is paused, `ZenohTransport::broadcast` makes one put per
//! live unpaused peer on `moirai/<s>/to/<peer>/<me>` (note 5.2), the key a
//! `send` to that peer puts on. A put fails once the session is closed
//! (Zenoh's `Session::resolve_put` answers an error when the session has no
//! primitives left), so the test closes `a`'s session under the transport
//! with `ZenohTransport::close` (section 1 of the spec) and compares the two
//! calls on the same peer:
//!
//! - `send(b)` answers an error, as it does at d28b6d7 (the reference);
//! - `broadcast` with `x` paused, whose one put goes to `b`, must answer an
//!   error too; at d28b6d7 it prints `Failed to send to b` and answers `Ok`.
//!
//! The TCP transport is not the reference here: `DirectTransport::broadcast`
//! prints a failed per-peer write and answers `Ok` (`direct_transport.rs`,
//! read at d28b6d7); C2 asks the Zenoh backend to answer as its own `send`
//! does, and changes nothing under TCP.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16a_paused_broadcast_l2 -- --test-threads=1
//! ```

#[path = "nw15_common/contract.rs"]
mod contract;
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use std::thread;
use std::time::{Duration, Instant};

use contract::{collect, id, raw, Seen, DELIVERY};
use moirai_network::transport::{CrdtTransport, TransportMessage};
use moirai_network::zenoh_transport::ZenohTransport;
use zenoh_rig::{fresh_session, loopback_peer, tcp_locator};

type Transport = ZenohTransport<String>;

fn hello_from(seen: &Seen, peer: &str) -> bool {
    seen.other
        .iter()
        .any(|(from, frame)| from == peer && matches!(frame, TransportMessage::Hello { .. }))
}

fn read_until(t: &mut Transport, seen: &mut Seen, mut done: impl FnMut(&Seen) -> bool) -> bool {
    let deadline = Instant::now() + DELIVERY;
    loop {
        collect(t, seen);
        if done(seen) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn c2_a_paused_broadcast_reports_a_failed_put_as_send_does() {
    let s = fresh_session("c2");
    let mut a =
        Transport::open(id("a"), &s, vec![id("b"), id("x")], loopback_peer(&[])).expect("open a");
    let a_at = tcp_locator(a.locators());
    let mut b =
        Transport::open(id("b"), &s, vec![id("a")], loopback_peer(&[a_at])).expect("open b");

    let mut a_seen = Seen::default();
    assert!(
        read_until(&mut a, &mut a_seen, |seen| hello_from(seen, "b")),
        "set-up: a never saw b"
    );
    a.pause_peer(&id("x"))
        .expect("pause x, known from PEERS and never live");

    // Before the close: the paused broadcast is one put, to b, and arrives.
    a.broadcast(raw("before"))
        .expect("set-up: a paused broadcast before the close");
    let mut b_seen = Seen::default();
    assert!(
        read_until(&mut b, &mut b_seen, |seen| seen.has("a", "before")),
        "set-up: a's paused broadcast never reached b before the close"
    );

    a.close().expect("close a's session under the transport");

    let sent = a.send(&id("b"), raw("send-after-close"));
    assert!(
        sent.is_err(),
        "reference: a send to b after a's session closed answered Ok, so no put fails here and \
         C2 cannot be read"
    );
    let broadcast = a.broadcast(raw("broadcast-after-close"));
    assert!(
        broadcast.is_err(),
        "C2: with x paused, a's broadcast is one put to b; after a's session closed that put \
         fails, and the broadcast answered Ok, where a send to b answered {sent:?}"
    );
}
