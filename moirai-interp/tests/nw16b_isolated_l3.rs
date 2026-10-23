//! NW.16b, L2 and R6 inside the spec's isolated container (vault
//! `spec/items/NW.16b.md`): every test here is ignored on the host and runs
//! only through `nw16b/container.sh`, which starts one container from
//! `busybox:latest` with `--network none`, so nothing here can reach the
//! host's network.
//!
//! - L2 (phase 1, `lo` alone with no address but 127.0.0.1, so a Zenoh
//!   session listening on 0.0.0.0 lists no locator): under
//!   `MOIRAI_TRANSPORT=zenoh` with `PEERS` given and `LISTEN_PORT=0`, a node
//!   without `PORTS_FILE` runs (alive after 10 s, healthy, its peer
//!   `Connected`), and the same node with `PORTS_FILE` exits non-zero within
//!   30 s.
//! - R6 (phase 2, `lo` given 10.77.0.1 and multicast): a peer opened with
//!   Ledgera's compiled scouting configuration (multicast on
//!   224.0.0.224:7446, TTL 1, autoconnect to routers and peers; fixture
//!   `nw16b_ledgera_scouting.json5`) is open from before the router starts
//!   until 15 s after three nodes the router introduced have converged, and
//!   in that time it never connects to the router or to any node; the router
//!   listens on 7447 by default, holds no UDP socket on 7446 or 7450, and
//!   every TCP link it holds leads to a node. Then the instrument's witness:
//!   a router on Zenoh's scouting defaults, opened beside it, is met by the
//!   Ledgera-configured peer within the same 15 s (`NW16B SCOUT WITNESS`).
//!
//! ```text
//! moirai-interp/tests/nw16b/container.sh <this test binary> <model_node> <moirai-router> <out-dir>
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
#[path = "nw16a/p5.rs"]
mod p5;

use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant};

use nw16a::{node_binary, scratch, set};
use nw16b::{LINK_TIMEOUT, REPORT_TIMEOUT, Router, Spec};
use zenoh::Wait;

/// How long a node without `PORTS_FILE` must keep running (L2).
const RUNS_FOR: Duration = Duration::from_secs(10);
/// How long a node with `PORTS_FILE` has to stop (L2).
const TO_EXIT: Duration = Duration::from_secs(30);
/// How long, once the Moirai nodes converged, the Ledgera-configured peer is
/// watched (R6), and how long the witness router has to be met.
const WINDOW: Duration = Duration::from_secs(15);

/// The value, or a panic at the caller's line naming `point`.
#[track_caller]
fn ok<T>(point: &str, result: Result<T, String>) -> T {
    match result {
        Ok(value) => value,
        Err(why) => panic!("{point}: {why}"),
    }
}

/// The value, or a panic at the caller's line with the message as is: the
/// router's own checks name their point (R0, R6) themselves.
#[track_caller]
fn must<T>(result: Result<T, String>) -> T {
    match result {
        Ok(value) => value,
        Err(why) => panic!("{why}"),
    }
}

/// `y`, a node on a fixed `LISTEN_PORT` with `PEERS` empty and `PORTS_FILE`,
/// reported and healthy: the peer the L2 nodes are given.
#[track_caller]
fn y(binary: &std::path::Path, scratch: &std::path::Path) -> (nw16a::Cluster, u16) {
    let port = ok("set-up", p5::kernel_chosen_port());
    let spec = Spec {
        id: "y",
        listen_port: port,
        http_port: 0,
        ports_file: true,
        peers: String::new(),
        router: None,
        client: false,
        preload: None,
    };
    let mut started = ok("L2", nw16b::start(binary, scratch, &spec));
    let report = ok(
        "L2",
        started
            .await_report(REPORT_TIMEOUT)
            .map_err(|why| format!("y, on a fixed LISTEN_PORT with PEERS given: {why}")),
    );
    let node = started.into_node(report.http, port);
    (
        ok("L2", nw16b::cluster(binary, scratch, vec![node], true)),
        port,
    )
}

// ---------------------------------------------------------------------------
// L2
// ---------------------------------------------------------------------------

#[test]
#[ignore = "inside nw16b/container.sh only"]
fn l2_without_ports_file_a_node_whose_session_lists_no_locator_runs() {
    ok("set-up", nw16b::isolated_without_addresses());
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-l2a"));
    let (mut cluster, y_port) = y(&binary, &scratch);
    let http = ok("set-up", p5::kernel_chosen_port());
    let spec = Spec {
        id: "x",
        listen_port: 0,
        http_port: http,
        ports_file: false,
        peers: format!("y:127.0.0.1:{y_port}"),
        router: None,
        client: false,
        preload: None,
    };
    let mut x = ok("L2", nw16b::start(&binary, &scratch, &spec));
    let since = Instant::now();
    while since.elapsed() < RUNS_FOR {
        ok(
            "L2",
            x.alive().map_err(|why| {
                format!(
                    "x, with LISTEN_PORT=0, PEERS given and no PORTS_FILE, in a namespace where its \
                     session lists no locator, stopped after {:?}; without PORTS_FILE it must run: \
                     {why}",
                    since.elapsed()
                )
            }),
        );
        thread::sleep(Duration::from_millis(100));
    }
    cluster.nodes.push(x.into_node(http, 0));
    ok("L2", cluster.await_health(1));
    ok("L2", cluster.await_mesh(&[0, 1]));
    cluster.nodes.clear();
}

#[test]
#[ignore = "inside nw16b/container.sh only"]
fn l2_with_ports_file_the_same_node_stops_non_zero() {
    ok("set-up", nw16b::isolated_without_addresses());
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-l2b"));
    let (mut cluster, y_port) = y(&binary, &scratch);
    let spec = Spec {
        id: "x2",
        listen_port: 0,
        http_port: 0,
        ports_file: true,
        peers: format!("y:127.0.0.1:{y_port}"),
        router: None,
        client: false,
        preload: None,
    };
    let mut x2 = ok("L2", nw16b::start(&binary, &scratch, &spec));
    match x2.exit_within(TO_EXIT) {
        None => panic!(
            "L2: x2, with LISTEN_PORT=0, PEERS given and PORTS_FILE, in a namespace where its \
             session lists no locator, still runs after {TO_EXIT:?}; with PORTS_FILE it must stop \
             (the port it would report is unknown); last log lines:\n{}",
            x2.tail()
        ),
        Some(status) => assert!(
            !status.success(),
            "L2: x2 stopped with {status}, a success, where it must stop non-zero; last log \
             lines:\n{}",
            x2.tail()
        ),
    }
    cluster.nodes.clear();
}

// ---------------------------------------------------------------------------
// R6
// ---------------------------------------------------------------------------

#[test]
#[ignore = "inside nw16b/container.sh only"]
fn r6_a_peer_on_ledgera_scouting_meets_neither_the_router_nor_any_node() {
    ok("set-up", nw16b::isolated_with_an_address());
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-r6"));
    let me = std::process::id();

    // The Ledgera-configured peer is open before anything Moirai starts.
    let ledgera = ok("set-up", nw16b::ledgera_peer());
    let ledgera_zid = ledgera.zid().to_string();
    let watch = nw16b::MetWatch::start(ledgera.clone());

    // R0 and R6: the router on its default port, no scouting socket.
    let mut router = must(Router::start(&scratch, None));
    assert_eq!(
        router.port, 7447,
        "R0: the router started with LISTEN_PORT unset reported port {}, not its default 7447",
        router.port
    );
    must(router.check_listening());

    // Three peers it introduces, converged (R1 inside the container).
    let nodes = ok(
        "R1",
        nw16b::peers(&binary, &scratch, &["a", "b", "c"], router.port),
    );
    let mut cluster = ok("R1", nw16b::cluster(&binary, &scratch, nodes, true));
    ok("R1", cluster.await_mesh(&[0, 1, 2]));
    let pids = nw16b::pids(&cluster);
    ok("R1", nw16b::await_links(&pids, LINK_TIMEOUT).map(|_| ()));
    let model = ok("R1", cluster.create_model(0));
    for node in [1, 2] {
        ok("R1", cluster.join_model(node, &model));
        ok("R1", cluster.await_table(node, &model));
    }
    for (node, name) in [(0, "a1"), (1, "b1"), (2, "c1")] {
        ok("R1", cluster.write_class(node, &model, name));
    }
    for node in 0..3 {
        ok(
            "R1",
            cluster.await_names(node, &model, &set(&["a1", "b1", "c1"])),
        );
    }
    // The witness sessions of `nw16b::peers` are closed; let their sockets
    // leave ESTABLISHED before the window's kernel reads.
    thread::sleep(Duration::from_secs(1));

    // R6: the window.
    let http_ports: BTreeSet<u16> = cluster.nodes.iter().map(|n| n.http).collect();
    let node_pids: BTreeSet<u32> = pids.iter().map(|(_, pid)| *pid).collect();
    let start = Instant::now();
    while start.elapsed() < WINDOW {
        let seen = watch.seen();
        assert!(
            seen.is_empty(),
            "R6: the Ledgera-configured peer ({ledgera_zid}) met {seen:?} (Zenoh id, kind, time \
             since it opened); the router is {} on port {}, the nodes {pids:?}",
            router.pid,
            router.port
        );
        // Every link of the test process (which holds the Ledgera-configured
        // peer) that leads to the router or a node, its HTTP clients' links
        // to a node's HTTP port aside.
        let mut mine = Vec::new();
        for conn in nw16b::established_of(me) {
            for (who, pid) in
                std::iter::once(("router".to_string(), router.pid)).chain(pids.iter().cloned())
            {
                for other in nw16b::established_of(pid) {
                    let mirrored = other.local == conn.remote && other.remote == conn.local;
                    let http = who != "router" && http_ports.contains(&other.local_port());
                    if mirrored && !http {
                        mine.push(format!(
                            "{}->{} with {who} (pid {pid})",
                            conn.local_port(),
                            conn.remote_port()
                        ));
                    }
                }
            }
        }
        assert!(
            mine.is_empty(),
            "R6: the test process, which holds the Ledgera-configured peer, holds TCP links to \
             the router or a node: {mine:?}"
        );
        for conn in nw16b::established_of(router.pid) {
            let to_node = node_pids.iter().any(|pid| {
                nw16b::established_of(*pid)
                    .iter()
                    .any(|c| c.local == conn.remote && c.remote == conn.local)
            });
            assert!(
                to_node,
                "R6: the router holds a TCP link {}->{} that leads to no Moirai node (nodes \
                 {pids:?}); the router's peers must be Moirai sessions only",
                conn.local_port(),
                conn.remote_port()
            );
        }
        must(router.refuse_scouting());
        thread::sleep(Duration::from_millis(200));
    }
    println!(
        "NW16B RECORD R6: the Ledgera-configured peer met nobody from its open until {WINDOW:?} \
         after the nodes converged; the router holds UDP ports {:?}",
        nw16a::udp_ports_of(router.pid)
    );

    // NW16B SCOUT WITNESS: a router on Zenoh's scouting defaults is met.
    let default_router = ok("set-up", nw16b::default_router());
    let default_zid = default_router.zid().to_string();
    let opened = Instant::now();
    let met = loop {
        if watch
            .seen()
            .iter()
            .any(|(zid, kind, _)| *zid == default_zid && *kind == "router")
        {
            break Some(opened.elapsed());
        }
        if opened.elapsed() > WINDOW {
            break None;
        }
        thread::sleep(Duration::from_millis(100));
    };
    match met {
        Some(after) => println!(
            "NW16B RECORD R6 witness: the Ledgera-configured peer met a router on Zenoh's \
             scouting defaults {after:?} after it opened"
        ),
        None => panic!(
            "NW16B SCOUT WITNESS: within {WINDOW:?} the Ledgera-configured peer never met a router \
             on Zenoh's scouting defaults ({default_zid}), so this run could not have seen a \
             router that answers scouts; R6's scouting half inconclusive (seen: {:?})",
            watch.seen()
        ),
    }
    drop(watch);
    let _ = default_router.close().wait();
    let _ = ledgera.close().wait();
    router.stop();
    cluster.nodes.clear();
}
