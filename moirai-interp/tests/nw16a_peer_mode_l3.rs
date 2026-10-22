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
//! - P5: the one socket listening on the port a node reported in
//!   `PORTS_FILE` when the file appeared is the socket listening there once
//!   the node runs (no bind, free and rebind), it is bound to 0.0.0.0, it is
//!   held by the node's process, and under Zenoh the node's token is seen
//!   there.
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
// P5
// ---------------------------------------------------------------------------

fn p5(transport: Transport) {
    let binary = ok("P5", transport, node_binary());
    let scratch = ok("P5", transport, scratch("p5"));
    let env = node_env(transport, &scratch);
    let ids = ["a", "b", "c"];
    let mut pending: Vec<_> = ids
        .iter()
        .map(|id| {
            ok(
                "P5",
                transport,
                spawn(&binary, &scratch, id, transport, &env),
            )
        })
        .collect();
    let reported: Vec<_> = pending
        .iter_mut()
        .map(|p| ok("P5", transport, p.await_ports()))
        .collect();

    // When PORTS_FILE has appeared, before the node is told its peers: the
    // one socket listening on the reported port, held by the node.
    let at_report: Vec<_> = reported
        .iter()
        .map(|r| {
            the_listener(r.pid, r.listen_port).unwrap_or_else(|why| {
                panic!(
                    "P5 on {transport:?}: {} reported listen port {} in PORTS_FILE, but at that \
                     moment {why}",
                    r.replica_id, r.listen_port
                )
            })
        })
        .collect();

    for (index, p) in pending.iter().enumerate() {
        let peers: Vec<(&str, u16)> = ids
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(other, id)| (*id, reported[other].listen_port))
            .collect();
        ok("P5", transport, p.tell_peers(&peers));
    }
    let nodes: Vec<Node> = pending
        .into_iter()
        .zip(&reported)
        .map(|(p, r)| p.into_node(r))
        .collect();
    let cluster = ok(
        "P5",
        transport,
        Cluster::from_nodes(transport, nodes, scratch, binary),
    );
    for index in 0..3 {
        ok("P5", transport, cluster.await_health(index));
    }
    ok("P5", transport, cluster.await_mesh(&[0, 1, 2]));

    for (index, node) in cluster.nodes.iter().enumerate() {
        let now = the_listener(node.pid, node.listen).unwrap_or_else(|why| {
            panic!(
                "P5 on {transport:?}: {} runs, but on the port it reported, {}, {why}",
                node.id, node.listen
            )
        });
        assert_eq!(
            now.inode, at_report[index].inode,
            "P5 on {transport:?}: {} reported port {} while socket inode {} listened there; it \
             runs with inode {} listening there instead, so the port was bound, freed and bound \
             again between the report and the node's start",
            node.id, node.listen, at_report[index].inode, now.inode
        );
        assert_eq!(
            now.local, "00000000",
            "P5 on {transport:?}: {} listens on {} (hex), not on 0.0.0.0",
            node.id, now.local
        );
        if transport == Transport::Zenoh {
            ok("P5", transport, witness(node));
        }
    }
}

#[test]
fn p5_ports_file_names_the_port_the_session_listens_on_zenoh() {
    p5(Transport::Zenoh);
}

#[test]
fn p5_ports_file_names_the_port_the_session_listens_on_tcp() {
    p5(Transport::Tcp);
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
