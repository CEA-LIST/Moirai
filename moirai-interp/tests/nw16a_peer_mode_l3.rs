//! NW.16a, P1 to P5 (vault `spec/items/NW.16a.md`): `model_node` processes
//! on loopback, started by NW.02's launcher with `LISTEN_PORT=0`,
//! `PORTS_FILE` and `PEERS_FILE`, run on the transport `MOIRAI_TRANSPORT`
//! names. Each point runs twice, once with `MOIRAI_TRANSPORT=zenoh` and once
//! with `MOIRAI_TRANSPORT=tcp`, the TCP twin, which passes at d28b6d7: a red
//! Zenoh test with a green twin is the transport's, never the scenario's.
//!
//! - P1: three nodes mesh (each lists the other two `Connected`) and
//!   converge on one model; under Zenoh each node is witnessed on Zenoh (its
//!   token seen by a raw session at the port it reported, `nw16a/mod.rs`).
//! - P2: `/api/pause-all` on `c` and `/api/pause/c` on `a` and `b` cut
//!   {a, b} | {c}; writes on both sides stay on their side; resumes by
//!   `/api/resume/<peer>` heal it and all three converge.
//! - P3: a fourth node, told the three as its peers, joins the model by
//!   state transfer (its log says it adopted a state) and converges.
//! - P4: `c`, which never wrote, is frozen (SIGSTOP) and a successor with
//!   the same `REPLICA_ID` starts while the old token still lives at `a`
//!   and `b`; under Zenoh a raw observer must see `a` and `b` each put a
//!   `SyncRequest` to `c` (the greeting: the node's `Hello` arm asks the
//!   newcomer for a sync); the old process is then killed (SIGKILL) and the
//!   successor converges, writes, and is read `Connected` by both.
//! - P5a (round 2, Cam's ruling A): a node that knows its peers at start
//!   (`PEERS` in its environment, empty included, on port 0 or a fixed
//!   port) is held where its `PORTS_FILE` appears, and the one socket
//!   listening on the reported port there is the socket it runs on, bound
//!   once, on 0.0.0.0, and it dialled every peer it was given; a node with a
//!   fixed `LISTEN_PORT` and `PEERS_FILE` binds nothing on that port before
//!   it is told its peers, and binds it once after. Under Zenoh the node's
//!   token is seen there. `nw16a/p5.rs` and `nw16a/bind_shim.c`.
//! - P5b (round 2): with `LISTEN_PORT=0` and `PEERS_FILE`, a second bind of
//!   the kernel-chosen port is made to fail as a port taken in the gap
//!   fails; under Zenoh the node stops, non-zero, naming the port, and never
//!   listens elsewhere; under TCP the same device changes nothing.
//! - B: a node given `BOOTNODE_URL` under Zenoh stops, non-zero, with a log
//!   that names `MOIRAI_TRANSPORT=tcp`; under TCP the same node runs (its
//!   discovery fails against an address nothing listens on).
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16a_peer_mode_l3 \
//!     -- --test-threads=1
//! ```
#![cfg(feature = "network")]

mod nw16a;

use std::time::{Duration, Instant};

use nw16a::{
    Cluster, Node, Observer, Transport, node_binary, node_env, scratch, set, spawn, the_listener,
    witness,
};

#[path = "nw16a/p5.rs"]
mod p5;

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::thread;

use nw16a::{Listening, Reported, listening_on, socket_inodes};

/// How long after the successor's token appears `a` and `b` have to greet
/// it.
const GREETING: Duration = Duration::from_secs(20);
/// The successor must be up within this time of the freeze for the old
/// token to be alive still at `a` and `b`: Zenoh's lease is 10 s and a
/// keep-alive leaves every 2.5 s (note Z7), so the old token lives at least
/// 7.5 s after the freeze.
const INSIDE_THE_LEASE: Duration = Duration::from_millis(7_000);

fn ok<T>(point: &str, transport: Transport, result: Result<T, String>) -> T {
    result.unwrap_or_else(|why| panic!("{point} on {transport:?}: {why}"))
}

/// Three nodes and one model the two others joined, with one class each
/// written by `writers`, read back by all three.
fn three_with_a_model(
    transport: Transport,
    tag: &str,
    point: &str,
    writers: &[(usize, &str)],
) -> (Cluster, String) {
    let cluster = ok(
        point,
        transport,
        Cluster::start(transport, tag, &["a", "b", "c"]),
    );
    let model = ok(point, transport, cluster.create_model(0));
    for node in [1, 2] {
        ok(point, transport, cluster.join_model(node, &model));
        ok(point, transport, cluster.await_table(node, &model));
    }
    for (node, name) in writers {
        ok(point, transport, cluster.write_class(*node, &model, name));
    }
    let names: Vec<&str> = writers.iter().map(|(_, name)| *name).collect();
    for node in 0..3 {
        ok(
            point,
            transport,
            cluster.await_names(node, &model, &set(&names)),
        );
    }
    (cluster, model)
}

// ---------------------------------------------------------------------------
// P1
// ---------------------------------------------------------------------------

fn p1(transport: Transport) {
    // `Cluster::start` is the claim's first half: three nodes on port 0,
    // meshed by PEERS_FILE, every one listing the two others Connected, and
    // under Zenoh every one witnessed on Zenoh.
    let _ = three_with_a_model(transport, "p1", "P1", &[(0, "a1"), (1, "b1"), (2, "c1")]);
}

#[test]
fn p1_three_nodes_on_port_0_mesh_and_converge_zenoh() {
    p1(Transport::Zenoh);
}

#[test]
fn p1_three_nodes_on_port_0_mesh_and_converge_tcp() {
    p1(Transport::Tcp);
}

// ---------------------------------------------------------------------------
// P2
// ---------------------------------------------------------------------------

fn p2(transport: Transport) {
    let (c, m) = three_with_a_model(transport, "p2", "P2", &[(0, "a1"), (1, "b1"), (2, "c1")]);
    // {a, b} | {c}: c pauses everyone, a and b pause c.
    ok("P2", transport, c.control(2, "/api/pause-all"));
    ok("P2", transport, c.control(0, "/api/pause/c"));
    ok("P2", transport, c.control(1, "/api/pause/c"));
    for (node, peer) in [(2, "a"), (2, "b"), (0, "c"), (1, "c")] {
        assert_eq!(
            c.status_of(node, peer).as_deref(),
            Some("Paused"),
            "P2 on {transport:?}: {} reads {peer} {:?} after the pause",
            c.nodes[node].id,
            c.status_of(node, peer)
        );
    }
    ok("P2", transport, c.write_class(0, &m, "a2"));
    ok("P2", transport, c.write_class(1, &m, "b2"));
    ok("P2", transport, c.write_class(2, &m, "c2"));
    let ab = set(&["a1", "b1", "c1", "a2", "b2"]);
    let cc = set(&["a1", "b1", "c1", "c2"]);
    ok("P2", transport, c.await_names(0, &m, &ab));
    ok("P2", transport, c.await_names(1, &m, &ab));
    ok("P2", transport, c.await_names(2, &m, &cc));
    // Nothing crosses the partition, either way.
    ok("P2", transport, c.holds_names(0, &m, &ab));
    ok("P2", transport, c.holds_names(1, &m, &ab));
    ok("P2", transport, c.holds_names(2, &m, &cc));
    // Heal, peer by peer.
    ok("P2", transport, c.control(2, "/api/resume/a"));
    ok("P2", transport, c.control(2, "/api/resume/b"));
    ok("P2", transport, c.control(0, "/api/resume/c"));
    ok("P2", transport, c.control(1, "/api/resume/c"));
    let all = set(&["a1", "b1", "c1", "a2", "b2", "c2"]);
    for node in 0..3 {
        ok("P2", transport, c.await_names(node, &m, &all));
    }
}

#[test]
fn p2_pause_all_and_resume_partition_and_heal_zenoh() {
    p2(Transport::Zenoh);
}

#[test]
fn p2_pause_all_and_resume_partition_and_heal_tcp() {
    p2(Transport::Tcp);
}

// ---------------------------------------------------------------------------
// P3
// ---------------------------------------------------------------------------

fn p3(transport: Transport) {
    let (mut c, m) = three_with_a_model(transport, "p3", "P3", &[(0, "a1"), (1, "b1"), (2, "c1")]);
    // d is told a, b and c; under Zenoh `add` witnesses it on Zenoh.
    let d = ok("P3", transport, c.add("d", &[0, 1, 2]));
    for peer in ["a", "b", "c"] {
        let deadline = Instant::now() + nw16a::MESH_TIMEOUT;
        while c.status_of(d, peer).as_deref() != Some("Connected") && Instant::now() < deadline {
            std::thread::sleep(nw16a::POLL);
        }
        assert_eq!(
            c.status_of(d, peer).as_deref(),
            Some("Connected"),
            "P3 on {transport:?}: d reads {peer} {:?}",
            c.status_of(d, peer)
        );
    }
    ok("P3", transport, c.join_model(d, &m));
    ok(
        "P3",
        transport,
        c.await_names(d, &m, &set(&["a1", "b1", "c1"])),
    );
    assert!(
        c.nodes[d].adopted(&m),
        "P3 on {transport:?}: d holds the model but its log never says it adopted a state \
         transfer for {m}:\n{}",
        c.nodes[d].tail()
    );
    ok("P3", transport, c.write_class(d, &m, "d1"));
    let all = set(&["a1", "b1", "c1", "d1"]);
    for node in 0..4 {
        ok("P3", transport, c.await_names(node, &m, &all));
    }
}

#[test]
fn p3_a_fourth_node_joins_by_state_transfer_zenoh() {
    p3(Transport::Zenoh);
}

#[test]
fn p3_a_fourth_node_joins_by_state_transfer_tcp() {
    p3(Transport::Tcp);
}

// ---------------------------------------------------------------------------
// P4
// ---------------------------------------------------------------------------

fn p4(transport: Transport) {
    // c never writes, so its successor is a fresh member the donors serve
    // a state transfer to (a donor refuses one to a member it has history
    // from, `has_history_for`).
    let (mut c, m) = three_with_a_model(transport, "p4", "P4", &[(0, "a1"), (1, "b1")]);
    let observer = match transport {
        Transport::Zenoh => Some(ok(
            "P4",
            transport,
            Observer::start(&[c.nodes[0].listen, c.nodes[1].listen], "moirai/*/to/c/*"),
        )),
        Transport::Tcp => None,
    };

    // The observer's own witness, before anything is claimed: a resume
    // makes a node ask the resumed peer for a sync (`request_sync`, the
    // same call a greeting makes), so pausing and resuming c at a and at b
    // must show the observer a SyncRequest naming the model from each. A
    // run where it does not cannot see a greeting either.
    if let Some(observer) = &observer {
        let since = Instant::now();
        for node in [0, 1] {
            ok("P4", transport, c.control(node, "/api/pause/c"));
            ok("P4", transport, c.control(node, "/api/resume/c"));
        }
        let seen_from = |peer: &str| {
            observer.frames().iter().any(|seen| {
                seen.at >= since
                    && seen.kind == "SyncRequest"
                    && seen.key.ends_with(&format!("/to/c/{peer}"))
                    && seen
                        .text
                        .as_deref()
                        .is_some_and(|text| text.contains(m.as_str()))
            })
        };
        let deadline = Instant::now() + GREETING;
        while !(seen_from("a") && seen_from("b")) && Instant::now() < deadline {
            std::thread::sleep(nw16a::POLL);
        }
        assert!(
            seen_from("a") && seen_from("b"),
            "NW16A OBSERVER WITNESS: a and b resumed c, which makes each ask c for a sync of \
             the model, and the observer saw no such SyncRequest from {}; this run cannot see \
             a greeting",
            if seen_from("a") { "b" } else { "a" }
        );
    }

    // The old c freezes: no FIN, no keep-alive, its token lives on at a and
    // b until their lease runs out.
    let frozen_at = Instant::now();
    ok("P4", transport, c.nodes[2].signal("STOP"));
    ok("P4", transport, c.write_class(0, &m, "a2"));
    ok("P4", transport, c.write_class(1, &m, "b2"));
    let ab = set(&["a1", "b1", "a2", "b2"]);
    ok("P4", transport, c.await_names(0, &m, &ab));
    ok("P4", transport, c.await_names(1, &m, &ab));

    // Its successor, same REPLICA_ID, told a and b; `add` returns once it
    // answers /api/health and, under Zenoh, once its token is seen.
    let successor = ok("P4", transport, c.add("c", &[0, 1]));
    let up_after = frozen_at.elapsed();
    println!("NW16A P4 {transport:?}: the successor was up {up_after:?} after the freeze");

    if let Some(observer) = &observer {
        assert!(
            up_after <= INSIDE_THE_LEASE,
            "NW16A LEASE WITNESS: the successor came up {up_after:?} after the freeze, later \
             than {INSIDE_THE_LEASE:?}, so the old token may have expired first and this run \
             cannot tell a greeting from an ordinary Hello"
        );
        // A greeting is the node's `Hello` arm, `request_sync`: a
        // `SyncRequest` for every log with history, here the model. The
        // default log, hosted empty everywhere, draws `SyncRequest`s of its
        // own from the state-transfer retry (`StateUnavailable` answered by a
        // delta sync), so only a request naming the model counts.
        let deadline = Instant::now() + GREETING;
        let greeted_by = |peer: &str| {
            observer.frames().iter().any(|seen| {
                seen.at >= frozen_at
                    && seen.kind == "SyncRequest"
                    && seen.key.ends_with(&format!("/to/c/{peer}"))
                    && seen
                        .text
                        .as_deref()
                        .is_some_and(|text| text.contains(m.as_str()))
            })
        };
        while !(greeted_by("a") && greeted_by("b")) && Instant::now() < deadline {
            std::thread::sleep(nw16a::POLL);
        }
        let frames: Vec<String> = observer
            .frames()
            .iter()
            .filter(|seen| seen.at >= frozen_at)
            .map(|seen| {
                let names_model = seen.text.as_deref().is_some_and(|t| t.contains(m.as_str()));
                format!("{} {} model={names_model}", seen.key, seen.kind)
            })
            .collect();
        println!("NW16A P4 Zenoh: frames put to c since the freeze: {frames:?}");
        for peer in ["a", "b"] {
            assert!(
                greeted_by(peer),
                "P4 on Zenoh: c's successor declared its token while the frozen c's token still \
                 lived, and {peer} put no SyncRequest to c within {GREETING:?} of the successor \
                 coming up, so {peer} never greeted it; frames put to c since the freeze: \
                 {frames:?}"
            );
        }
    }

    // The old process goes for good.
    c.nodes[2].kill();
    ok("P4", transport, c.join_model(successor, &m));
    ok("P4", transport, c.await_names(successor, &m, &ab));
    ok("P4", transport, c.write_class(successor, &m, "c1"));
    let all = set(&["a1", "b1", "a2", "b2", "c1"]);
    for node in [0, 1, successor] {
        ok("P4", transport, c.await_names(node, &m, &all));
    }
    std::thread::sleep(nw16a::QUIET);
    for node in [0, 1] {
        assert_eq!(
            c.status_of(node, "c").as_deref(),
            Some("Connected"),
            "P4 on {transport:?}: once the old c is gone, {} reads its successor {:?}",
            c.nodes[node].id,
            c.status_of(node, "c")
        );
    }
}

#[test]
fn p4_a_node_restarted_inside_its_tokens_lease_is_greeted_and_synced_zenoh() {
    p4(Transport::Zenoh);
}

#[test]
fn p4_a_node_restarted_inside_its_tokens_lease_is_greeted_and_synced_tcp() {
    p4(Transport::Tcp);
}

// ---------------------------------------------------------------------------
// P5a and P5b (round 2, Cam's ruling A, 2026-10-22)
// ---------------------------------------------------------------------------

/// P5a with the peers known at start: `a` on port 0 with `PEERS` empty, `b`
/// on port 0 with `PEERS` naming `a`, `c` on a fixed port (one the kernel
/// chose for the test) with `PEERS` naming `a` and `b`, each started once
/// the earlier ones reported, so each link is dialled by the later node.
/// Each node is held at the moment its `PORTS_FILE` appears and read there.
fn p5a_peers(transport: Transport) {
    let point = "P5a (PEERS)";
    let binary = ok(point, transport, node_binary());
    let scratch = ok(point, transport, scratch("p5a-peers"));
    let shim = ok(point, transport, p5::shim(&scratch));
    let env = node_env(transport, &scratch);
    let fixed = ok(point, transport, p5::kernel_chosen_port());
    let plan = [("a", 0u16), ("b", 0), ("c", fixed)];

    let mut started: Vec<p5::Started> = Vec::new();
    let mut at_report: Vec<(Reported, Listening)> = Vec::new();
    for (id, port) in plan {
        let peers = at_report
            .iter()
            .map(|(r, _)| format!("{}:127.0.0.1:{}", r.replica_id, r.listen_port))
            .collect::<Vec<_>>()
            .join(",");
        let device = p5::Device {
            hold: true,
            gap: false,
        };
        let mut node = ok(
            point,
            transport,
            p5::start(
                &binary,
                &scratch,
                &shim,
                id,
                transport,
                &env,
                port,
                p5::Peers::Env(peers),
                device,
            ),
        );
        let reported = ok(point, transport, node.await_held());
        let http = reported.http_port.expect("checked in await_ports");
        if port != 0 {
            assert_eq!(
                reported.listen_port, port,
                "{point} on {transport:?}: {id} was given LISTEN_PORT={port} and reported listen \
                 port {} in PORTS_FILE",
                reported.listen_port
            );
        }
        // The node stands still where its PORTS_FILE appeared.
        let listener = the_listener(reported.pid, reported.listen_port).unwrap_or_else(|why| {
            panic!(
                "{point} on {transport:?}: {id} reported listen port {} in PORTS_FILE and stands \
                 at its report, but {why}",
                reported.listen_port
            )
        });
        let held = p5::held_listen_ports(reported.pid);
        assert_eq!(
            held,
            BTreeSet::from([http, reported.listen_port]),
            "{point} on {transport:?}: at its report {id} holds LISTEN sockets on {held:?}, where \
             its HTTP port {http} and its reported port {} are the only ones it may hold",
            reported.listen_port
        );
        println!(
            "NW16A P5a {transport:?}: {id} (LISTEN_PORT={port}) reported {}, inode {} listening \
             there at the report; binds so far {:?}",
            reported.listen_port,
            listener.inode,
            node.binds().done
        );
        ok(point, transport, node.go());
        started.push(node);
        at_report.push((reported, listener));
    }

    let bind_logs: Vec<PathBuf> = started.iter().map(|s| s.bind_log.clone()).collect();
    let nodes: Vec<Node> = started
        .into_iter()
        .zip(&at_report)
        .map(|(s, (r, _))| s.into_node(r))
        .collect();
    let cluster = ok(
        point,
        transport,
        Cluster::from_nodes(transport, nodes, scratch, binary),
    );
    for index in 0..3 {
        ok(point, transport, cluster.await_health(index));
    }
    // Each node lists Connected every peer it was given. (A TCP node does
    // not list a peer that dialled it unannounced, so the earlier nodes are
    // not asked about the later ones.)
    for later in 1..3 {
        for earlier in 0..later {
            let peer = cluster.nodes[earlier].id.clone();
            let deadline = Instant::now() + nw16a::MESH_TIMEOUT;
            while cluster.status_of(later, &peer).as_deref() != Some("Connected")
                && Instant::now() < deadline
            {
                std::thread::sleep(nw16a::POLL);
            }
            assert_eq!(
                cluster.status_of(later, &peer).as_deref(),
                Some("Connected"),
                "{point} on {transport:?}: {} was given {peer} in PEERS and reads it {:?}",
                cluster.nodes[later].id,
                cluster.status_of(later, &peer)
            );
        }
    }

    for (index, node) in cluster.nodes.iter().enumerate() {
        let (reported, at) = &at_report[index];
        let http = reported.http_port.expect("checked in await_ports");
        let binds = p5::Binds::read(&bind_logs[index]);
        println!(
            "NW16A P5a {transport:?}: {} runs; binds {:?}",
            node.id, binds.done
        );
        let now = the_listener(node.pid, node.listen).unwrap_or_else(|why| {
            panic!(
                "{point} on {transport:?}: {} runs, but on the port it reported, {}, {why}",
                node.id, node.listen
            )
        });
        assert_eq!(
            now.inode, at.inode,
            "{point} on {transport:?}: {} reported port {} while socket inode {} listened there; \
             it runs with inode {} listening there instead, so the port was bound, freed and \
             bound again after the report",
            node.id, node.listen, at.inode, now.inode
        );
        assert_eq!(
            now.local, "00000000",
            "{point} on {transport:?}: {} listens on {} (hex), not on 0.0.0.0",
            node.id, now.local
        );
        let of_port = binds.of(node.listen);
        assert_eq!(
            of_port.len(),
            1,
            "{point} on {transport:?}: {} bound its reported port {} {} times ({of_port:?}); \
             the one socket it binds there is the one it runs on",
            node.id,
            node.listen,
            of_port.len()
        );
        assert_eq!(
            binds.ports(),
            BTreeSet::from([http, node.listen]),
            "{point} on {transport:?}: {} bound TCP sockets on {:?}; its HTTP port {http} and its \
             replication port {} are the only ones it may bind",
            node.id,
            binds.ports(),
            node.listen
        );
        let held = p5::held_listen_ports(node.pid);
        assert_eq!(
            held,
            BTreeSet::from([http, node.listen]),
            "{point} on {transport:?}: {} holds LISTEN sockets on {held:?}, where its HTTP port \
             {http} and its reported port {} are the only ones it may hold",
            node.id,
            node.listen
        );
        // Each link was dialled by the later node, from its PEERS.
        for earlier in &cluster.nodes[..index] {
            assert!(
                p5::dialed(node.pid, earlier.listen),
                "{point} on {transport:?}: {} was given {} in PEERS and holds no connection it \
                 dialled to {}'s port {}",
                node.id,
                earlier.id,
                earlier.id,
                earlier.listen
            );
        }
        if transport == Transport::Zenoh {
            ok(point, transport, witness(node));
        }
    }
}

#[test]
fn p5a_with_peers_known_at_start_the_reported_socket_is_the_one_the_node_runs_on_zenoh() {
    p5a_peers(Transport::Zenoh);
}

#[test]
fn p5a_with_peers_known_at_start_the_reported_socket_is_the_one_the_node_runs_on_tcp() {
    p5a_peers(Transport::Tcp);
}

/// P5a with a fixed `LISTEN_PORT` and `PEERS_FILE`: `x` and `y`, each given
/// a port the kernel chose for the test, report it, are held at the report
/// and then watched for [`p5::BEFORE_PEERS`] before they are told each
/// other. Under Zenoh nothing of theirs may listen on, or bind, that port
/// until their session opens, after `PEERS_FILE`; under TCP the listener is
/// bound before the report and kept, as at d28b6d7.
fn p5a_fixed(transport: Transport) {
    let point = "P5a (fixed LISTEN_PORT, PEERS_FILE)";
    let binary = ok(point, transport, node_binary());
    let scratch = ok(point, transport, scratch("p5a-fixed"));
    let shim = ok(point, transport, p5::shim(&scratch));
    let env = node_env(transport, &scratch);
    let ids = ["x", "y"];
    let first = ok(point, transport, p5::kernel_chosen_port());
    let mut second = ok(point, transport, p5::kernel_chosen_port());
    while second == first {
        second = ok(point, transport, p5::kernel_chosen_port());
    }
    let ports = [first, second];

    let mut started: Vec<p5::Started> = Vec::new();
    let mut reports: Vec<Reported> = Vec::new();
    let mut tcp_inodes: Vec<Option<u64>> = Vec::new();
    for (id, port) in ids.into_iter().zip(ports) {
        let device = p5::Device {
            hold: true,
            gap: false,
        };
        let mut node = ok(
            point,
            transport,
            p5::start(
                &binary,
                &scratch,
                &shim,
                id,
                transport,
                &env,
                port,
                p5::Peers::File,
                device,
            ),
        );
        let reported = ok(point, transport, node.await_held());
        let http = reported.http_port.expect("checked in await_ports");
        assert_eq!(
            reported.listen_port, port,
            "{point} on {transport:?}: {id} was given LISTEN_PORT={port} and reported listen port \
             {} in PORTS_FILE",
            reported.listen_port
        );
        println!(
            "NW16A P5a {transport:?}: {id} (LISTEN_PORT={port}, PEERS_FILE) at its report: \
             listening on the port {:?}, binds {:?}",
            listening_on(port),
            node.binds().done
        );
        match transport {
            Transport::Zenoh => {
                let mine = socket_inodes(reported.pid);
                let on_port = listening_on(port);
                assert!(
                    !on_port.iter().any(|l| mine.contains(&l.inode)),
                    "{point} on Zenoh: {id} reported its configured port {port} and, standing at \
                     its report before it was told its peers, already holds a socket listening \
                     there ({on_port:?}): it bound the port before its session could open"
                );
                assert!(
                    on_port.is_empty(),
                    "set-up: another process listens on {port}, the port the test chose for {id}: \
                     {on_port:?}"
                );
                let binds = node.binds();
                assert!(
                    binds.of(port).is_empty(),
                    "{point} on Zenoh: {id} bound its configured port {port} before it was told \
                     its peers (binds {:?}), so before its session could open",
                    binds.done
                );
                let held = p5::held_listen_ports(reported.pid);
                assert_eq!(
                    held,
                    BTreeSet::from([http]),
                    "{point} on Zenoh: at its report {id} holds LISTEN sockets on {held:?}, where \
                     its HTTP port {http} is the only one it may hold before its session opens"
                );
                tcp_inodes.push(None);
            }
            Transport::Tcp => {
                let listener = the_listener(reported.pid, port).unwrap_or_else(|why| {
                    panic!(
                        "{point} on Tcp: {id} reported its configured port {port}, where its \
                         listener is bound before the report as at d28b6d7, but {why}"
                    )
                });
                tcp_inodes.push(Some(listener.inode));
            }
        }
        ok(point, transport, node.go());
        started.push(node);
        reports.push(reported);
    }

    // Before PEERS_FILE: still nothing on the port under Zenoh, the same
    // listener under TCP.
    let until = Instant::now() + p5::BEFORE_PEERS;
    while Instant::now() < until {
        for (index, reported) in reports.iter().enumerate() {
            let port = reported.listen_port;
            match tcp_inodes[index] {
                None => {
                    let mine = socket_inodes(reported.pid);
                    let on_port = listening_on(port);
                    assert!(
                        !on_port.iter().any(|l| mine.contains(&l.inode)),
                        "{point} on Zenoh: {} listens on its configured port {port} \
                         ({on_port:?}) after its report and before it was told its peers, so \
                         before its session could open",
                        ids[index]
                    );
                }
                Some(inode) => {
                    let now = the_listener(reported.pid, port).map(|l| l.inode);
                    assert_eq!(
                        now,
                        Ok(inode),
                        "{point} on Tcp: {}'s listener on {port} changed before PEERS_FILE",
                        ids[index]
                    );
                }
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    if transport == Transport::Zenoh {
        for (index, node) in started.iter().enumerate() {
            let binds = node.binds();
            assert!(
                binds.of(ports[index]).is_empty(),
                "{point} on Zenoh: {} bound its configured port {} before it was told its peers \
                 (binds {:?})",
                ids[index],
                ports[index],
                binds.done
            );
        }
    }

    // Now the peers: x is told y, y is told x.
    let watches: Vec<p5::PortWatch> = ports.iter().map(|&p| p5::PortWatch::start(p)).collect();
    for (index, node) in started.iter().enumerate() {
        let other = 1 - index;
        ok(
            point,
            transport,
            node.pending().tell_peers(&[(ids[other], ports[other])]),
        );
    }
    let bind_logs: Vec<PathBuf> = started.iter().map(|s| s.bind_log.clone()).collect();
    let nodes: Vec<Node> = started
        .into_iter()
        .zip(&reports)
        .map(|(s, r)| s.into_node(r))
        .collect();
    let cluster = ok(
        point,
        transport,
        Cluster::from_nodes(transport, nodes, scratch, binary),
    );
    for index in 0..2 {
        ok(point, transport, cluster.await_health(index));
    }
    ok(point, transport, cluster.await_mesh(&[0, 1]));
    let seen: Vec<BTreeSet<u64>> = watches.into_iter().map(p5::PortWatch::finish).collect();

    for (index, node) in cluster.nodes.iter().enumerate() {
        let http = reports[index].http_port.expect("checked in await_ports");
        let binds = p5::Binds::read(&bind_logs[index]);
        println!(
            "NW16A P5a {transport:?}: {} runs; binds {:?}; inodes seen listening on {} since \
             PEERS_FILE {:?}",
            node.id, binds.done, node.listen, seen[index]
        );
        let now = the_listener(node.pid, node.listen).unwrap_or_else(|why| {
            panic!(
                "{point} on {transport:?}: {} runs, but on its configured port {}, {why}",
                node.id, node.listen
            )
        });
        if let Some(inode) = tcp_inodes[index] {
            assert_eq!(
                now.inode, inode,
                "{point} on Tcp: {}'s listener on {} was inode {inode} at the report and is {} \
                 now",
                node.id, node.listen, now.inode
            );
        }
        assert_eq!(
            seen[index],
            BTreeSet::from([now.inode]),
            "{point} on {transport:?}: between PEERS_FILE and the mesh, sockets {:?} listened on \
             {}'s port {}; the one it runs on, inode {}, must be the only one",
            seen[index],
            node.id,
            node.listen,
            now.inode
        );
        assert_eq!(
            now.local, "00000000",
            "{point} on {transport:?}: {} listens on {} (hex), not on 0.0.0.0",
            node.id, now.local
        );
        let of_port = binds.of(node.listen);
        assert_eq!(
            of_port.len(),
            1,
            "{point} on {transport:?}: {} bound its configured port {} {} times ({of_port:?})",
            node.id,
            node.listen,
            of_port.len()
        );
        assert_eq!(
            binds.ports(),
            BTreeSet::from([http, node.listen]),
            "{point} on {transport:?}: {} bound TCP sockets on {:?}; its HTTP port {http} and its \
             replication port {} are the only ones it may bind",
            node.id,
            binds.ports(),
            node.listen
        );
        let held = p5::held_listen_ports(node.pid);
        assert_eq!(
            held,
            BTreeSet::from([http, node.listen]),
            "{point} on {transport:?}: {} holds LISTEN sockets on {held:?}, where its HTTP port \
             {http} and its configured port {} are the only ones it may hold",
            node.id,
            node.listen
        );
        if transport == Transport::Zenoh {
            ok(point, transport, witness(node));
        }
    }
}

#[test]
fn p5a_with_a_fixed_port_and_peers_file_nothing_is_bound_before_the_session_zenoh() {
    p5a_fixed(Transport::Zenoh);
}

#[test]
fn p5a_with_a_fixed_port_and_peers_file_nothing_is_bound_before_the_session_tcp() {
    p5a_fixed(Transport::Tcp);
}

/// P5b: `LISTEN_PORT=0` with `PEERS_FILE`, the one path that keeps the gap.
/// The preloaded library makes the gap certain: once the kernel has chosen
/// the node's port, a second bind of that port fails as it fails when
/// another process took the port after the node let it go. Under Zenoh the
/// node must then stop, non-zero, naming the port, and never listen
/// anywhere else; under TCP, whose listener is bound once and kept, the same
/// library changes nothing (the device's own control).
fn p5b(transport: Transport) {
    let point = "P5b";
    let binary = ok(point, transport, node_binary());
    let scratch = ok(point, transport, scratch("p5b"));
    let shim = ok(point, transport, p5::shim(&scratch));
    let env = node_env(transport, &scratch);
    let device = p5::Device {
        hold: false,
        gap: true,
    };
    let mut node = ok(
        point,
        transport,
        p5::start(
            &binary,
            &scratch,
            &shim,
            "a",
            transport,
            &env,
            0,
            p5::Peers::File,
            device,
        ),
    );
    let reported = ok(point, transport, node.pending_mut().await_ports());
    let port = reported.listen_port;
    let http = reported.http_port.expect("checked in await_ports");
    let tcp_listener = (transport == Transport::Tcp).then(|| {
        the_listener(reported.pid, port).unwrap_or_else(|why| {
            panic!(
                "{point} on Tcp: a reported {port}, where its listener is bound before the report \
                 as at d28b6d7, but {why}"
            )
        })
    });
    let offset = fs::metadata(&node.pending().log)
        .map(|m| m.len() as usize)
        .unwrap_or(0);
    // A lone node, told no peers.
    ok(point, transport, node.pending().tell_peers(&[]));

    match transport {
        Transport::Zenoh => {
            let deadline = Instant::now() + p5::TO_EXIT;
            let mut listened: BTreeSet<u16> = BTreeSet::new();
            let mut reports: BTreeSet<u16> = BTreeSet::new();
            let exit = loop {
                listened.extend(p5::held_listen_ports(reported.pid));
                if let Some(Ok(r)) = nw16a::read_ports_file(&node.pending().ports) {
                    reports.insert(r.listen_port);
                }
                if let Ok(Some(status)) = node.pending_mut().child.try_wait() {
                    break Some(status);
                }
                if Instant::now() > deadline {
                    break None;
                }
                thread::sleep(Duration::from_millis(20));
            };
            let text = fs::read_to_string(&node.pending().log).unwrap_or_default();
            let after = text.get(offset..).unwrap_or("").to_string();
            let binds = node.binds();
            println!(
                "NW16A P5b Zenoh: a reported {port}; binds {:?}, refused {:?}; LISTEN ports held \
                 {listened:?}; exit {exit:?}; printed after PEERS_FILE:\n{after}",
                binds.done, binds.refused
            );
            let elsewhere: BTreeSet<u16> = listened
                .iter()
                .copied()
                .filter(|p| *p != http && *p != port)
                .collect();
            assert!(
                elsewhere.is_empty(),
                "{point} on Zenoh: a reported port {port}, its session could not bind it, and a \
                 listened on {elsewhere:?}: it ran on another port; log after PEERS_FILE:\n{after}"
            );
            assert!(
                reports.iter().all(|p| *p == port),
                "{point} on Zenoh: a reported port {port}, then rewrote PORTS_FILE to {reports:?}"
            );
            assert!(
                binds.refused.contains(&port),
                "NW16A GAP WITNESS: a never bound its reported port {port} a second time (binds \
                 {:?}), so this run did not exercise the gap; P5b inconclusive",
                binds.done
            );
            // A node still running is killed when `node` is dropped.
            let status = exit.unwrap_or_else(|| {
                panic!(
                    "{point} on Zenoh: a's session could not bind its reported port {port} \
                     (taken in the gap) and a still runs after {:?}; log after PEERS_FILE:\n{after}",
                    p5::TO_EXIT
                )
            });
            assert!(
                !status.success(),
                "{point} on Zenoh: a's session could not bind its reported port {port} and a \
                 exited with {status}, which is not a failure; log after PEERS_FILE:\n{after}"
            );
            assert!(
                p5::names_port(&after, port),
                "{point} on Zenoh: a's session could not bind its reported port {port} and a \
                 stopped ({status}) without naming {port} in what it printed after PEERS_FILE \
                 appeared:\n{after}"
            );
        }
        Transport::Tcp => {
            let bind_log = node.bind_log.clone();
            let node = node.into_node(&reported);
            let cluster = ok(
                point,
                transport,
                Cluster::from_nodes(transport, vec![node], scratch, binary),
            );
            ok(point, transport, cluster.await_health(0));
            std::thread::sleep(nw16a::QUIET);
            let binds = p5::Binds::read(&bind_log);
            println!(
                "NW16A P5b Tcp: a reported {port}; binds {:?}, refused {:?}",
                binds.done, binds.refused
            );
            assert!(
                binds.refused.is_empty(),
                "{point} on Tcp: the library refused binds {:?} of a node whose listener is bound \
                 once and kept",
                binds.refused
            );
            let now = the_listener(cluster.nodes[0].pid, port).unwrap_or_else(|why| {
                panic!("{point} on Tcp: a runs, but on the port it reported, {port}, {why}")
            });
            let at = tcp_listener.expect("read at the report under TCP");
            assert_eq!(
                now.inode, at.inode,
                "{point} on Tcp: a's listener on {port} was inode {} at the report and is {} now",
                at.inode, now.inode
            );
            let held = p5::held_listen_ports(cluster.nodes[0].pid);
            assert_eq!(
                held,
                BTreeSet::from([http, port]),
                "{point} on Tcp: a holds LISTEN sockets on {held:?}"
            );
            ok(point, transport, cluster.await_health(0));
        }
    }
}

#[test]
fn p5b_with_port_0_and_peers_file_a_lost_rebind_stops_the_node_naming_its_port_zenoh() {
    p5b(Transport::Zenoh);
}

#[test]
fn p5b_with_port_0_and_peers_file_a_lost_rebind_stops_the_node_naming_its_port_tcp() {
    p5b(Transport::Tcp);
}

// ---------------------------------------------------------------------------
// B: BOOTNODE_URL under Zenoh
// ---------------------------------------------------------------------------

/// A bootnode address nothing listens on: discovery under TCP fails there
/// and the node runs on; under Zenoh the variable alone must stop it.
const NO_BOOTNODE: &str = "http://127.0.0.1:9";

fn b(transport: Transport) {
    let binary = ok("B", transport, node_binary());
    let scratch = ok("B", transport, scratch("b"));
    let mut env = node_env(transport, &scratch);
    env.push(("BOOTNODE_URL".into(), NO_BOOTNODE.into()));
    let mut pending = ok(
        "B",
        transport,
        spawn(&binary, &scratch, "a", transport, &env),
    );
    // Whether the node refuses before or after it reports its ports, it is
    // told no peers as soon as it reports, as a lone node is.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut told = false;
    let mut reported = None;
    let exit = loop {
        if let Ok(Some(status)) = pending.child.try_wait() {
            break Some(status);
        }
        if !told && let Some(Ok(r)) = nw16a::read_ports_file(&pending.ports) {
            ok("B", transport, pending.tell_peers(&[]));
            told = true;
            reported = Some(r);
        }
        if Instant::now() > deadline {
            break None;
        }
        std::thread::sleep(nw16a::POLL);
    };
    let log = std::fs::read_to_string(&pending.log).unwrap_or_default();
    match transport {
        Transport::Zenoh => {
            let status = exit.unwrap_or_else(|| {
                panic!(
                    "B on Zenoh: a node given BOOTNODE_URL under MOIRAI_TRANSPORT=zenoh still runs \
                     after 30 s; log:\n{log}"
                )
            });
            assert!(
                !status.success(),
                "B on Zenoh: the node exited with {status}, which is not a refusal; log:\n{log}"
            );
            assert!(
                log.contains("MOIRAI_TRANSPORT=tcp"),
                "B on Zenoh: the node stopped ({status}) without naming MOIRAI_TRANSPORT=tcp; \
                 log:\n{log}"
            );
        }
        Transport::Tcp => {
            assert!(
                exit.is_none(),
                "B on TCP: a node given an unreachable BOOTNODE_URL exited ({exit:?}); log:\n{log}"
            );
            let reported = reported.expect("the TCP node reported its ports");
            let node = pending.into_node(&reported);
            let cluster = ok(
                "B",
                transport,
                Cluster::from_nodes(transport, vec![node], scratch, binary),
            );
            ok("B", transport, cluster.await_health(0));
        }
    }
}

#[test]
fn b_bootnode_url_is_refused_under_zenoh_naming_moirai_transport_tcp_zenoh() {
    b(Transport::Zenoh);
}

#[test]
fn b_bootnode_url_is_refused_under_zenoh_naming_moirai_transport_tcp_tcp() {
    b(Transport::Tcp);
}
