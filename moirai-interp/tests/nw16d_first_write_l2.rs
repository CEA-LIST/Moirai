//! NW.16d, D1 (vault `spec/items/NW.16d.md`): the first frame a transport
//! puts right after it reads the other node `Connected` arrives, a
//! broadcast and a send alike, in each layout a node runs in.
//!
//! The risk this guards: a declared Publisher may filter a put by what its
//! session knows of the remote subscribers, and right after a peer appears
//! that knowledge may lag its liveliness token. A lost frame here would not
//! be repaired by Moirai's own syncs, which this test does not run: two
//! `ZenohTransport`s in this process, pumped by the test, each putting one
//! broadcast and one send the moment `is_connected` first answers true for
//! the other, and each reading until it holds both of the other's.
//!
//! - `d1_direct_`: two peers, one dialling the other, no router.
//! - `d1_introduced_`: two peers that each dial moirai-router only, which
//!   introduces them (gossip one hop); they link directly.
//! - `d1_client_`: a peer and a client of moirai-router; the client holds
//!   one link, to the router.
//!
//! Each runs `ROUNDS` rounds on fresh Moirai session ids. The Zenoh
//! configurations are written in `nw16d/mod.rs`, as `transport_config.rs`
//! builds them for `ZENOH_ROUTER` on loopback, not taken from the code
//! under test. The router must have passed its L1 check
//! (`nw16b_router_config_l1`) before this file runs on the host.
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-router
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16d_first_write_l2 \
//!     -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
mod nw16d;

use std::thread;
use std::time::{Duration, Instant};

use moirai_network::transport::{CrdtTransport, TransportMessage};
use moirai_network::zenoh_transport::ZenohTransport;
use nw16b::Router;
use zenoh::Config;

type Transport = ZenohTransport<String>;

/// Rounds per layout.
const ROUNDS: usize = 10;
/// How long two transports may take to read each other `Connected`.
const MEET: Duration = Duration::from_secs(30);
/// How long a frame put right after `Connected` may take to arrive.
const DELIVERY: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum Layout {
    Direct,
    Introduced,
    Client,
}

/// What one transport has handed over: `(from, payload)` of every `Raw`.
#[derive(Default)]
struct Held(Vec<(String, String)>);

impl Held {
    fn has(&self, from: &str, payload: &str) -> bool {
        self.0.iter().any(|(f, p)| f == from && p == payload)
    }
}

fn take(t: &mut Transport, held: &mut Held) -> Result<(), String> {
    loop {
        match t.try_recv() {
            Ok(Some((from, TransportMessage::Raw { payload }))) => held.0.push((from, payload)),
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(e) => return Err(format!("try_recv on {}: {e}", t.local_id())),
        }
    }
}

fn raw(text: String) -> TransportMessage<String> {
    TransportMessage::Raw { payload: text }
}

fn open(id: &str, session: &str, config: Config) -> Result<Transport, String> {
    nw16d::multicast_off(&config)?;
    Transport::open(id.to_string(), session, Vec::new(), config)
        .map_err(|e| format!("set-up: opening {id}: {e}"))
}

/// The two transports of one round, `a` first.
fn pair(layout: Layout, session: &str, router: Option<&Router>) -> Result<[Transport; 2], String> {
    let at = |router: Option<&Router>| {
        router
            .map(|r| format!("tcp/127.0.0.1:{}", r.port))
            .ok_or_else(|| "set-up: this layout needs the router".to_string())
    };
    Ok(match layout {
        Layout::Direct => {
            let b = open("b", session, nw16d::peer_config(&[], false)?)?;
            let b_at = b
                .locators()
                .into_iter()
                .find(|l| l.starts_with("tcp/127.0.0.1:"))
                .ok_or_else(|| {
                    format!("set-up: b lists no loopback locator: {:?}", b.locators())
                })?;
            let a = open("a", session, nw16d::peer_config(&[b_at], false)?)?;
            [a, b]
        }
        Layout::Introduced => {
            let router = at(router)?;
            let a = open(
                "a",
                session,
                nw16d::peer_config(std::slice::from_ref(&router), true)?,
            )?;
            let b = open("b", session, nw16d::peer_config(&[router], true)?)?;
            [a, b]
        }
        Layout::Client => {
            let router = at(router)?;
            let a = open(
                "a",
                session,
                nw16d::peer_config(std::slice::from_ref(&router), true)?,
            )?;
            let b = open("b", session, nw16d::client_config(&router)?)?;
            [a, b]
        }
    })
}

/// One round: each side, the moment it first reads the other `Connected`,
/// puts a broadcast and a send to it; each must hold both of the other's.
fn round(layout: Layout, n: usize, router: Option<&Router>) -> Result<String, String> {
    let session = format!("nw16d{}{layout:?}{n}", std::process::id());
    let mut ts = pair(layout, &session, router)?;
    let ids = ["a", "b"];
    let mut held = [Held::default(), Held::default()];
    let mut fired: [Option<Instant>; 2] = [None, None];
    let opened = Instant::now();
    let frames = |i: usize| {
        [
            format!("{session} {} broadcast right after Connected", ids[i]),
            format!("{session} {} send right after Connected", ids[i]),
        ]
    };
    loop {
        for i in 0..2 {
            let other = ids[1 - i];
            take(&mut ts[i], &mut held[i])?;
            if fired[i].is_none() && ts[i].is_connected(&other.to_string()) {
                let [broadcast, send] = frames(i);
                ts[i].broadcast(raw(broadcast)).map_err(|e| {
                    format!(
                        "D1 {layout:?} round {n}: {}'s broadcast failed: {e}",
                        ids[i]
                    )
                })?;
                ts[i].send(&other.to_string(), raw(send)).map_err(|e| {
                    format!("D1 {layout:?} round {n}: {}'s send failed: {e}", ids[i])
                })?;
                fired[i] = Some(Instant::now());
            }
        }
        let arrived = |i: usize| {
            let from = ids[1 - i];
            frames(1 - i).map(|f| held[i].has(from, &f))
        };
        if fired.iter().all(Option::is_some)
            && arrived(0) == [true, true]
            && arrived(1) == [true, true]
        {
            return Ok(format!(
                "{layout:?} round {n}: a read b Connected {:?} after the opens, b read a {:?}; \
                 all four frames arrived {:?} after the opens",
                fired[0].map(|t| t - opened),
                fired[1].map(|t| t - opened),
                opened.elapsed()
            ));
        }
        let now = Instant::now();
        let never_met = fired.iter().any(Option::is_none) && now - opened > MEET;
        let lost = fired.iter().flatten().any(|at| now - *at > DELIVERY);
        if never_met {
            return Err(format!(
                "set-up: in layout {layout:?} round {n}, a and b did not read each other \
                 Connected within {MEET:?} (a read b at {:?}, b read a at {:?}); a: {:?}; b: {:?}",
                fired[0].map(|t| t - opened),
                fired[1].map(|t| t - opened),
                ts[0]
                    .peers()
                    .iter()
                    .map(|p| (&p.id, p.status))
                    .collect::<Vec<_>>(),
                ts[1]
                    .peers()
                    .iter()
                    .map(|p| (&p.id, p.status))
                    .collect::<Vec<_>>()
            ));
        }
        if lost {
            return Err(format!(
                "D1 {layout:?} round {n}: a frame put right after Connected did not arrive within \
                 {DELIVERY:?}: b holds a's [broadcast, send] = {:?} (a put them {:?} after the \
                 opens), a holds b's = {:?} (b put them {:?} after the opens)",
                arrived(1),
                fired[0].map(|t| t - opened),
                arrived(0),
                fired[1].map(|t| t - opened)
            ));
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn run(layout: Layout, router: Option<&Router>) {
    for n in 0..ROUNDS {
        match round(layout, n, router) {
            Ok(line) => println!("NW16D RECORD D1 {line}"),
            Err(why) => panic!("{why}"),
        }
    }
}

fn started_router(tag: &str) -> Router {
    let scratch = nw16a::scratch(tag).unwrap_or_else(|why| panic!("set-up: {why}"));
    let router = Router::start(&scratch, Some(0)).unwrap_or_else(|why| panic!("{why}"));
    router
        .check_listening()
        .unwrap_or_else(|why| panic!("{why}"));
    router
}

#[test]
fn d1_direct_the_first_broadcast_and_send_after_connected_arrive() {
    run(Layout::Direct, None);
}

#[test]
fn d1_introduced_the_first_broadcast_and_send_after_connected_arrive() {
    let router = started_router("nw16d-d1-introduced");
    run(Layout::Introduced, Some(&router));
}

#[test]
fn d1_client_the_first_broadcast_and_send_after_connected_arrive() {
    let router = started_router("nw16d-d1-client");
    run(Layout::Client, Some(&router));
}
