//! NW.16d, on loopback (vault `spec/items/NW.16d.md`): with moirai-router
//! present and peer-mode `model_node`s linked directly to each other, the
//! frames they exchange do not reach the router, and delivery is unchanged.
//!
//! Every node runs `MOIRAI_TRANSPORT=zenoh` with `ZENOH_ROUTER` naming the
//! router on loopback and `PEERS` empty (NW.16b's layout: the router
//! introduces the peers, gossip one hop, multicast off); the client of `k_`
//! adds `ZENOH_MODE=client`. Read from outside: the bytes each process's TCP
//! sockets receive in a window, from the kernel's counters (`nw16d/mod.rs`);
//! links between processes in the kernel's tables (`nw16b`); what each node
//! holds through its HTTP API (`nw16a::Cluster`).
//!
//! - `c1_`: two linked peers; while `p` writes `C1_CLASSES` classes that `q`
//!   receives, the router receives at most `ROUTER_RX_MAX` bytes.
//! - `c2_`: a third peer `r` joins a model of `C2_CLASSES` classes: it
//!   adopts a state transfer (a `send` from its donor) and serves every
//!   class (D2), and the router receives at most `ROUTER_RX_MAX` bytes.
//! - `c3_`: `p` pauses `r` (and `r` pauses `p`), so each broadcast of `p`
//!   is one put per live unpaused peer; while `p` writes `C3_CLASSES`
//!   classes that `q` receives, the router receives at most `ROUTER_RX_MAX`
//!   bytes, and `r` receives none of them; once both resume, `r` serves
//!   every class.
//! - `d3_`: `q` is killed, `p` reads it `Disconnected` (its token gone); a
//!   new process with the same id `q` joins: it adopts a state transfer from
//!   `p` and serves every class, the router receiving at most
//!   `ROUTER_RX_MAX` bytes meanwhile; a class `p` writes then reaches it, and
//!   one it writes reaches `p`.
//! - `k_`, the meter's control and D4: a client `h` and a peer `p` of one
//!   router, no link between them; while `h` writes `K_CLASSES` classes that
//!   `p` receives, and then `p` writes as many that `h` receives, the router
//!   receives at least half of what the receiving node receives, and at
//!   least `PAYLOAD_MIN` bytes, in each window.
//!
//! The router must have passed its L1 check (`moirai-router`'s
//! `nw16b_router_config_l1`) before this file runs on the host (NW.16b F3).
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo build -j 2 -p moirai-router
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16d_router_bytes_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
mod nw16d;
#[path = "nw16a/p5.rs"]
mod p5;

use std::thread;
use std::time::{Duration, Instant};

use nw16a::{Cluster, node_binary, scratch};
use nw16b::{LINK_TIMEOUT, REPORT_TIMEOUT, Router, Spec};
use nw16d::{Party, Reading, Window, name_set, names};

/// The most the router may receive in a claim window (registered in the
/// spec, section 7, from the rehearsal of section 9).
const ROUTER_RX_MAX: u64 = 4096;
/// The least the receiving node must receive in a window for the window to
/// have carried a payload at all.
const PAYLOAD_MIN: u64 = 20_000;
/// Classes written in each window: four operations each.
const C1_CLASSES: usize = 100;
const C2_CLASSES: usize = 100;
const C3_CLASSES: usize = 40;
const D3_CLASSES: usize = 60;
const K_CLASSES: usize = 40;
/// Left after the links form and before a window opens, so that the
/// greetings a new peer draws (`Hello`, the delta syncs it triggers) are
/// over.
const SETTLE: Duration = Duration::from_secs(3);
/// How long a killed peer's token may take to go at the other peer.
const TOKEN_GONE: Duration = Duration::from_secs(30);

/// The value, or a panic at the caller's line naming `point`.
#[track_caller]
fn ok<T>(point: &str, result: Result<T, String>) -> T {
    match result {
        Ok(value) => value,
        Err(why) => panic!("{point}: {why}"),
    }
}

/// The value, or a panic at the caller's line with the message as is: the
/// router's own checks name their point themselves.
#[track_caller]
fn must<T>(result: Result<T, String>) -> T {
    match result {
        Ok(value) => value,
        Err(why) => panic!("{why}"),
    }
}

/// A started router that passed NW.16b's R0 checks.
fn router(scratch: &std::path::Path) -> Router {
    let router = must(Router::start(scratch, Some(0)));
    must(router.check_listening());
    router
}

/// Peers `ids` introduced by `router`, healthy, witnessed on Zenoh, each
/// listing every other `Connected`, and a TCP link joining every pair.
fn linked_peers(
    binary: &std::path::Path,
    scratch: &std::path::Path,
    ids: &[&str],
    router: &Router,
) -> Cluster {
    let nodes = ok("set-up", nw16b::peers(binary, scratch, ids, router.port));
    let cluster = ok("set-up", nw16b::cluster(binary, scratch, nodes, true));
    let all: Vec<usize> = (0..ids.len()).collect();
    ok("set-up", cluster.await_mesh(&all));
    let pids = nw16b::pids(&cluster);
    ok(
        "set-up",
        nw16b::await_links(&pids, LINK_TIMEOUT).map(|_| ()),
    );
    cluster
}

/// Start one more peer `id` introduced by `router` into `cluster`; healthy,
/// witnessed, meshed with every node, and linked to each.
fn add_peer(
    cluster: &mut Cluster,
    binary: &std::path::Path,
    scratch: &std::path::Path,
    id: &str,
    router: &Router,
) -> usize {
    let mut nodes = ok("set-up", nw16b::peers(binary, scratch, &[id], router.port));
    cluster.nodes.push(nodes.remove(0));
    let index = cluster.nodes.len() - 1;
    ok("set-up", cluster.await_health(index));
    ok("set-up", nw16a::witness(&cluster.nodes[index]));
    let all: Vec<usize> = (0..cluster.nodes.len()).collect();
    ok("set-up", cluster.await_mesh(&all));
    let pids = nw16b::pids(cluster);
    ok(
        "set-up",
        nw16b::await_links(&pids, LINK_TIMEOUT).map(|_| ()),
    );
    index
}

/// The router and every node of `cluster` as parties of a window.
fn parties(router: &Router, cluster: &Cluster) -> Vec<Party> {
    let mut parties = vec![Party::new("router", router.pid, None)];
    for node in &cluster.nodes {
        parties.push(Party::new(&node.id, node.pid, Some(node.http)));
    }
    parties
}

fn write_all(point: &str, cluster: &Cluster, node: usize, model: &str, names: &[String]) {
    for name in names {
        ok(point, cluster.write_class(node, model, name));
    }
}

/// The claim of a window: the meter saw it whole, the receiving node got a
/// payload, and the router received at most `ROUTER_RX_MAX` bytes.
#[track_caller]
fn router_kept_out(point: &str, reading: &Reading, receiver: &str) {
    println!("NW16D RECORD {point}: {}", reading.line());
    must(reading.witness("router"));
    let payload = reading.of(receiver).received;
    assert!(
        payload >= PAYLOAD_MIN,
        "NW16D PAYLOAD WITNESS: in window {}, {receiver} received {payload} B, under \
         {PAYLOAD_MIN} B, so the window carried no payload to keep from the router: {}",
        reading.label,
        reading.line()
    );
    let router = reading.of("router").received;
    assert!(
        router <= ROUTER_RX_MAX,
        "{point}: the router received {router} B in window {} while {receiver} received \
         {payload} B; at most {ROUTER_RX_MAX} B may reach the router while the peers are \
         linked: {}",
        reading.label,
        reading.line()
    );
}

// ---------------------------------------------------------------------------
// C1: broadcasts between two linked peers
// ---------------------------------------------------------------------------

#[test]
fn c1_broadcasts_between_two_linked_peers_do_not_reach_the_router() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16d-c1"));
    let router = router(&scratch);
    let cluster = linked_peers(&binary, &scratch, &["p", "q"], &router);
    let model = ok("set-up", cluster.create_model(0));
    ok("set-up", cluster.join_model(1, &model));
    ok("set-up", cluster.await_table(1, &model));
    thread::sleep(SETTLE);

    // Recorded, not a gate: what reaches the router while nothing is
    // written, over as long as SETTLE.
    let idle = ok("set-up", Window::open("idle", &parties(&router, &cluster)));
    thread::sleep(SETTLE);
    let idle = ok("set-up", idle.close());
    println!("NW16D RECORD C1 idle: {}", idle.line());

    let written = names('c', C1_CLASSES);
    let window = ok("set-up", Window::open("C1", &parties(&router, &cluster)));
    write_all("C1", &cluster, 0, &model, &written);
    ok("C1", cluster.await_names(1, &model, &name_set(&written)));
    let reading = ok("set-up", window.close());
    router_kept_out("C1", &reading, "q");
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// C2 and D2: a state transfer to a late joiner
// ---------------------------------------------------------------------------

#[test]
fn c2_a_late_joiner_adopts_a_state_transfer_that_does_not_reach_the_router() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16d-c2"));
    let router = router(&scratch);
    let mut cluster = linked_peers(&binary, &scratch, &["p", "q"], &router);
    let model = ok("set-up", cluster.create_model(0));
    ok("set-up", cluster.join_model(1, &model));
    ok("set-up", cluster.await_table(1, &model));
    let written = names('s', C2_CLASSES);
    write_all("set-up", &cluster, 0, &model, &written);
    ok(
        "set-up",
        cluster.await_names(1, &model, &name_set(&written)),
    );

    let r = add_peer(&mut cluster, &binary, &scratch, "r", &router);
    thread::sleep(SETTLE);
    let window = ok("set-up", Window::open("C2", &parties(&router, &cluster)));
    ok("D2", cluster.join_model(r, &model));
    ok("D2", cluster.await_names(r, &model, &name_set(&written)));
    let reading = ok("set-up", window.close());
    assert!(
        cluster.nodes[r].adopted(&model),
        "D2: r serves the model but its log never says it adopted a state transfer for \
         {model}:\n{}",
        cluster.nodes[r].tail()
    );
    router_kept_out("C2", &reading, "r");
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// C3: a broadcast while a peer is paused (one put per unpaused peer)
// ---------------------------------------------------------------------------

#[test]
fn c3_a_paused_broadcast_to_the_unpaused_peer_does_not_reach_the_router() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16d-c3"));
    let router = router(&scratch);
    let cluster = linked_peers(&binary, &scratch, &["p", "q", "r"], &router);
    let model = ok("set-up", cluster.create_model(0));
    for node in [1, 2] {
        ok("set-up", cluster.join_model(node, &model));
        ok("set-up", cluster.await_table(node, &model));
    }
    // p pauses r, so p's broadcasts become one put per live unpaused peer,
    // q alone; r pauses p, so that r's resume makes r ask p for what it
    // missed (a resume makes the resumer pull).
    ok("set-up", cluster.control(0, "/api/pause/r"));
    ok("set-up", cluster.control(2, "/api/pause/p"));
    thread::sleep(SETTLE);

    let written = names('u', C3_CLASSES);
    let window = ok("set-up", Window::open("C3", &parties(&router, &cluster)));
    write_all("C3", &cluster, 0, &model, &written);
    ok("C3", cluster.await_names(1, &model, &name_set(&written)));
    let reading = ok("set-up", window.close());
    router_kept_out("C3", &reading, "q");
    // The paused peer got none of it.
    ok(
        "C3",
        cluster.holds_names(2, &model, &std::collections::BTreeSet::new()),
    );

    ok("C3", cluster.control(0, "/api/resume/r"));
    ok("C3", cluster.control(2, "/api/resume/p"));
    ok("C3", cluster.await_names(2, &model, &name_set(&written)));
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// D3: a peer that stops and returns with the same id receives again
// ---------------------------------------------------------------------------

#[test]
fn d3_a_peer_that_stops_and_returns_with_the_same_id_receives_again() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16d-d3"));
    let router = router(&scratch);
    let mut cluster = linked_peers(&binary, &scratch, &["p", "q"], &router);
    let model = ok("set-up", cluster.create_model(0));
    ok("set-up", cluster.join_model(1, &model));
    ok("set-up", cluster.await_table(1, &model));
    // q never writes, so its successor is a member p has no history from,
    // which p serves a state transfer to (`has_history_for`).
    let written = names('v', D3_CLASSES);
    write_all("set-up", &cluster, 0, &model, &written);
    ok(
        "set-up",
        cluster.await_names(1, &model, &name_set(&written)),
    );
    // A send to q has gone out before it stops: p answers q's join.
    let old_pid = cluster.nodes[1].pid;

    // q stops for good: SIGKILL, so the kernel closes its links and its
    // tokens go at p. p must read it Disconnected, its token gone.
    cluster.nodes[1].kill();
    let killed = Instant::now();
    let mut status = cluster.status_of(0, "q");
    while status.as_deref() != Some("Disconnected") && killed.elapsed() < TOKEN_GONE {
        thread::sleep(nw16a::POLL);
        status = cluster.status_of(0, "q");
    }
    assert_eq!(
        status.as_deref(),
        Some("Disconnected"),
        "set-up: p still reads q {status:?} {TOKEN_GONE:?} after q (pid {old_pid}) was killed, \
         so q's token never went at p and this run cannot show a return"
    );
    println!(
        "NW16D RECORD D3: p read q Disconnected {:?} after q was killed",
        killed.elapsed()
    );

    // A new process, the same id q, introduced by the router.
    let mut nodes = ok(
        "set-up",
        nw16b::peers(&binary, &scratch, &["q"], router.port),
    );
    cluster.nodes[1] = nodes.remove(0);
    ok("set-up", cluster.await_health(1));
    ok("set-up", nw16a::witness(&cluster.nodes[1]));
    ok("D3", cluster.await_mesh(&[0, 1]));
    let pids = nw16b::pids(&cluster);
    ok("D3", nw16b::await_links(&pids, LINK_TIMEOUT).map(|_| ()));
    thread::sleep(SETTLE);

    let window = ok("set-up", Window::open("D3", &parties(&router, &cluster)));
    ok("D3", cluster.join_model(1, &model));
    ok("D3", cluster.await_names(1, &model, &name_set(&written)));
    let reading = ok("set-up", window.close());
    assert!(
        cluster.nodes[1].adopted(&model),
        "D3: the returned q serves the model but its log never says it adopted a state \
         transfer for {model}:\n{}",
        cluster.nodes[1].tail()
    );
    router_kept_out("D3", &reading, "q");

    // Both directions after the return.
    let mut all = written.clone();
    ok("D3", cluster.write_class(0, &model, "pz"));
    all.push("pz".into());
    ok("D3", cluster.await_names(1, &model, &name_set(&all)));
    ok("D3", cluster.write_class(1, &model, "qz"));
    all.push("qz".into());
    for node in [0, 1] {
        ok("D3", cluster.await_names(node, &model, &name_set(&all)));
    }
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// K and D4: the meter's control, a client carried through the router
// ---------------------------------------------------------------------------

#[test]
fn k_a_client_and_a_peer_exchange_through_the_router_which_receives_the_payload() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16d-k"));
    let router = router(&scratch);
    let mut nodes = ok(
        "set-up",
        nw16b::peers(&binary, &scratch, &["p"], router.port),
    );
    let given = ok("set-up", p5::kernel_chosen_port());
    let mut h = ok(
        "set-up",
        nw16b::start(&binary, &scratch, &Spec::client("h", router.port, given)),
    );
    let report = ok("set-up", h.await_report(REPORT_TIMEOUT));
    nodes.push(h.into_node(report.http, 0));
    let cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, false));
    ok("set-up", nw16a::witness(&cluster.nodes[0]));
    ok(
        "set-up",
        nw16a::token_at(router.port, "h", nw16a::WITNESS_TIMEOUT)
            .map_err(|why| format!("client h is not seen on Zenoh through its router: {why}")),
    );
    ok("D4", cluster.await_mesh(&[0, 1]));
    let (p_pid, h_pid) = (cluster.nodes[0].pid, cluster.nodes[1].pid);
    let model = ok("set-up", cluster.create_model(0));
    ok("D4", cluster.join_model(1, &model));
    ok("D4", cluster.await_table(1, &model));
    thread::sleep(SETTLE);

    let mut all = Vec::new();
    for (point, writer, receiver, tag) in [("K1", 1, 0, 'h'), ("K2", 0, 1, 'k')] {
        let written = names(tag, K_CLASSES);
        all.extend(written.iter().cloned());
        let window = ok("set-up", Window::open(point, &parties(&router, &cluster)));
        write_all("D4", &cluster, writer, &model, &written);
        ok("D4", cluster.await_names(receiver, &model, &name_set(&all)));
        let reading = ok("set-up", window.close());
        println!("NW16D RECORD {point}: {}", reading.line());
        must(reading.witness("router"));
        let id = &cluster.nodes[receiver].id;
        let got = reading.of(id).received;
        let router_rx = reading.of("router").received;
        assert!(
            got >= PAYLOAD_MIN && 2 * router_rx >= got,
            "NW16D METER CONTROL {point}: {} wrote {K_CLASSES} classes that {id} received \
             through the router ({got} B), and the meter read {router_rx} B received by the \
             router; it must read at least half of what {id} received, and {id} at least \
             {PAYLOAD_MIN} B, or the meter cannot be trusted to see data reaching the router: {}",
            cluster.nodes[writer].id,
            reading.line()
        );
    }
    // D4: through the router alone.
    let links = nw16b::links_between(p_pid, h_pid);
    assert!(
        links.is_empty(),
        "NW16D METER CONTROL: a TCP link joins p and the client h ({links:?}), so their data \
         did not have to cross the router"
    );
    cluster_end(cluster);
}

/// Kill every node now, before the router goes with its handle.
fn cluster_end(mut cluster: Cluster) {
    cluster.nodes.clear();
}
