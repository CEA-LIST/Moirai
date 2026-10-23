//! NW.16b, R0 to R5 on loopback (vault `spec/items/NW.16b.md`): the in-tree
//! router `moirai-router` introduces peer-mode `model_node`s to each other,
//! as ROS 2's `rmw_zenohd` does (NW.14 note, 6.2), and client-mode nodes go
//! through it.
//!
//! Every node runs `MOIRAI_TRANSPORT=zenoh` with `ZENOH_ROUTER` naming the
//! router on loopback and `PEERS` empty in its environment (NW.15's
//! configuration: peer mode, gossip on, multicast off); clients add
//! `ZENOH_MODE=client`. Read from outside (`nw16b/mod.rs`): links between
//! processes in the kernel's tables, listeners and UDP sockets likewise,
//! peers and states through each node's HTTP API.
//!
//! - R0, in every test that starts a router: the router binary exists,
//!   reports its pid and port in `PORTS_FILE`, and is the one process
//!   listening on that port, on 0.0.0.0; it holds no UDP socket on 7446 or
//!   7450 (R6's host half), checked at every poll from its spawn on.
//! - R1 and R2 (`r1_r2_...`): three peers each list the two others
//!   `Connected`, a TCP link joins every pair of node processes, and they
//!   converge; the router is then killed, and the three converge on writes
//!   made after it, keep every link they had past Zenoh's 10 s lease, and
//!   converge again.
//! - R3 (`r3_...`): a node started before its router runs, then joins the
//!   two others and converges once the router is up.
//! - R4 (`r4_...`): with the router killed, a node started then meets
//!   nobody for 10 s; once the router returns on the same port, it joins,
//!   links to both others, and converges.
//! - R5 (`r5_...`): a client started while the router is down keeps running
//!   for 10 s; once the router is up it and two more clients list each
//!   other `Connected`, listen on no replication port, are linked to the
//!   router and to no other node, and converge through the router.
//!
//! The router must have passed its L1 check (`moirai-router`'s
//! `nw16b_router_config_l1`) and the container run (`nw16b/container.sh`)
//! before this file runs on the host (spec, section 9).
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo build -j 2 -p moirai-router
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16b_router_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
#[path = "nw16a/p5.rs"]
mod p5;

use std::thread;
use std::time::{Duration, Instant};

use nw16a::{Cluster, node_binary, scratch, set};
use nw16b::{LINK_TIMEOUT, REPORT_TIMEOUT, Router, Spec};

/// Past Zenoh's lease (10 s, note Z7): links that outlive it were not kept
/// alive by the router.
const PAST_THE_LEASE: Duration = Duration::from_secs(12);
/// How long a node started while its router is down is watched for anyone
/// it meets.
const OUTAGE: Duration = Duration::from_secs(10);
/// How long a node started before its router must keep running.
const BEFORE_THE_ROUTER: Duration = Duration::from_secs(10);

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

/// A model created on `creator`, joined by every other node of `cluster`.
#[track_caller]
fn model_on(point: &str, cluster: &Cluster, creator: usize) -> String {
    let model = ok(point, cluster.create_model(creator));
    for node in 0..cluster.nodes.len() {
        if node != creator {
            ok(point, cluster.join_model(node, &model));
            ok(point, cluster.await_table(node, &model));
        }
    }
    model
}

/// Every node of `cluster` serves exactly `names` for `model`.
#[track_caller]
fn converge(point: &str, cluster: &Cluster, model: &str, names: &[&str]) {
    for node in 0..cluster.nodes.len() {
        ok(point, cluster.await_names(node, model, &set(names)));
    }
}

fn ids(cluster: &Cluster) -> String {
    cluster
        .nodes
        .iter()
        .map(|n| format!("{} (pid {})", n.id, n.pid))
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// R1 and R2
// ---------------------------------------------------------------------------

#[test]
fn r1_r2_three_peers_introduced_by_the_router_link_directly_converge_and_keep_converging_without_it()
 {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-r1"));
    let mut router = must(Router::start(&scratch, Some(0)));
    must(router.check_listening());

    // R1: three peer-mode nodes, PEERS empty, introduced by the router.
    let nodes = ok(
        "R1",
        nw16b::peers(&binary, &scratch, &["a", "b", "c"], router.port),
    );
    let mut cluster = ok("R1", nw16b::cluster(&binary, &scratch, nodes, true));
    ok("R1", cluster.await_mesh(&[0, 1, 2]));
    let pids = nw16b::pids(&cluster);
    let links = ok("R1", nw16b::await_links(&pids, LINK_TIMEOUT));
    println!(
        "NW16B RECORD R1 links: {}",
        links
            .iter()
            .map(|((a, b), l)| format!("{a}-{b} {}", l.len()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let model = model_on("R1", &cluster, 0);
    for (node, name) in [(0, "a1"), (1, "b1"), (2, "c1")] {
        ok("R1", cluster.write_class(node, &model, name));
    }
    converge("R1", &cluster, &model, &["a1", "b1", "c1"]);
    must(router.refuse_scouting());
    println!(
        "NW16B RECORD router: pid {}, port {}, {} (three peers, converged)",
        router.pid,
        router.port,
        router.memory()
    );

    // R2: the router stops; the links stay and carry new writes.
    let before = nw16b::pair_links(&pids);
    router.stop();
    let stopped = Instant::now();
    assert!(
        nw16a::listening_on(router.port).is_empty(),
        "set-up: something still listens on the router's port {} after it was killed",
        router.port
    );
    for (node, name) in [(0, "a2"), (1, "b2"), (2, "c2")] {
        ok("R2", cluster.write_class(node, &model, name));
    }
    let six = ["a1", "b1", "c1", "a2", "b2", "c2"];
    converge("R2", &cluster, &model, &six);
    println!(
        "NW16B RECORD R2: with the router stopped, {} served the writes made after the stop \
         {:?} after it",
        ids(&cluster),
        stopped.elapsed()
    );
    let wait = PAST_THE_LEASE.saturating_sub(stopped.elapsed());
    thread::sleep(wait);
    for ((a, b), links) in &before {
        let now = nw16b::links_between(
            pids.iter().find(|(id, _)| id == a).unwrap().1,
            pids.iter().find(|(id, _)| id == b).unwrap().1,
        );
        assert!(
            links.iter().any(|link| now.contains(link)),
            "R2: the TCP links that joined {a} and {b} before the router stopped ({links:?}, \
             socket inode pairs) are gone {:?} after the stop (now {now:?}): the peers did not \
             keep their links",
            stopped.elapsed()
        );
    }
    for i in 0..3 {
        for j in 0..3 {
            if i != j {
                let status = cluster.status_of(i, &cluster.nodes[j].id);
                assert_eq!(
                    status.as_deref(),
                    Some("Connected"),
                    "R2: {:?} after the router stopped, {} reads {} {status:?}",
                    stopped.elapsed(),
                    cluster.nodes[i].id,
                    cluster.nodes[j].id
                );
            }
        }
    }
    for (node, name) in [(0, "a3"), (1, "b3"), (2, "c3")] {
        ok("R2", cluster.write_class(node, &model, name));
    }
    converge(
        "R2",
        &cluster,
        &model,
        &["a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3"],
    );
    println!(
        "NW16B RECORD R2: {} still linked pairwise and converged on a3, b3, c3 {:?} after the \
         router stopped",
        ids(&cluster),
        stopped.elapsed()
    );
    cluster.nodes.clear();
}

// ---------------------------------------------------------------------------
// R3
// ---------------------------------------------------------------------------

#[test]
fn r3_a_node_started_before_its_router_joins_and_converges_once_the_router_is_up() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-r3"));
    let port = ok("set-up", p5::kernel_chosen_port());

    // R3: `a` starts while no router listens on `port`, and keeps running.
    // It may report its ports before the router is up or only once it is;
    // either is allowed.
    let mut first = ok(
        "R3",
        nw16b::start(&binary, &scratch, &Spec::peer("a", port)),
    );
    let since = Instant::now();
    while since.elapsed() < BEFORE_THE_ROUTER {
        ok(
            "R3",
            first.alive().map_err(|why| {
                format!("a, started before its router, did not keep running: {why}")
            }),
        );
        thread::sleep(Duration::from_millis(100));
    }

    let router = must(Router::start(&scratch, Some(port)));
    must(router.check_listening());
    let up = Instant::now();
    let report = ok(
        "R3",
        first
            .await_report(REPORT_TIMEOUT)
            .map_err(|why| format!("a, started before its router: {why}")),
    );
    let listen = ok(
        "R3",
        report
            .listen
            .filter(|p| *p != 0)
            .ok_or_else(|| "a, a peer on LISTEN_PORT=0, reported no listen port".to_string()),
    );
    let a = first.into_node(report.http, listen);
    let mut nodes = vec![a];
    nodes.extend(ok(
        "R3",
        nw16b::peers(&binary, &scratch, &["b", "c"], router.port),
    ));
    let mut cluster = ok("R3", nw16b::cluster(&binary, &scratch, nodes, true));
    ok("R3", cluster.await_mesh(&[0, 1, 2]));
    println!(
        "NW16B RECORD R3: a listed b and c Connected, and they a, {:?} after its router came up",
        up.elapsed()
    );
    let pids = nw16b::pids(&cluster);
    ok("R3", nw16b::await_links(&pids, LINK_TIMEOUT).map(|_| ()));
    let model = model_on("R3", &cluster, 0);
    for (node, name) in [(0, "a1"), (1, "b1"), (2, "c1")] {
        ok("R3", cluster.write_class(node, &model, name));
    }
    converge("R3", &cluster, &model, &["a1", "b1", "c1"]);
    println!(
        "NW16B RECORD R3: converged {:?} after the router came up",
        up.elapsed()
    );
    cluster.nodes.clear();
}

// ---------------------------------------------------------------------------
// R4
// ---------------------------------------------------------------------------

#[test]
fn r4_a_node_started_while_the_router_is_down_meets_nobody_until_it_returns_then_joins() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-r4"));
    let mut router = must(Router::start(&scratch, Some(0)));
    must(router.check_listening());
    let port = router.port;

    let nodes = ok("R4", nw16b::peers(&binary, &scratch, &["a", "b"], port));
    let mut cluster = ok("R4", nw16b::cluster(&binary, &scratch, nodes, true));
    ok("R4", cluster.await_mesh(&[0, 1]));
    ok(
        "R4",
        nw16b::await_links(&nw16b::pids(&cluster), LINK_TIMEOUT).map(|_| ()),
    );
    let model = model_on("R4", &cluster, 0);
    for (node, name) in [(0, "a1"), (1, "b1")] {
        ok("R4", cluster.write_class(node, &model, name));
    }
    converge("R4", &cluster, &model, &["a1", "b1"]);

    router.stop();
    assert!(
        nw16a::listening_on(port).is_empty(),
        "set-up: something still listens on the router's port {port} after it was killed"
    );

    // R4: `c` starts while the router is down and meets nobody.
    let mut late = ok(
        "R4",
        nw16b::start(&binary, &scratch, &Spec::peer("c", port)),
    );
    let c_pid = late.pid();
    let (a_pid, b_pid) = (cluster.nodes[0].pid, cluster.nodes[1].pid);
    let mut report = None;
    let since = Instant::now();
    ok("R4", cluster.write_class(0, &model, "a2"));
    while since.elapsed() < OUTAGE {
        ok(
            "R4",
            late.alive().map_err(|why| {
                format!("c, started while its router was down, did not keep running: {why}")
            }),
        );
        for (other, pid) in [("a", a_pid), ("b", b_pid)] {
            let links = nw16b::links_between(c_pid, pid);
            assert!(
                links.is_empty(),
                "R4: {:?} after c started with its router down, a TCP link joins c and {other} \
                 ({links:?}): c met a node without its router",
                since.elapsed()
            );
        }
        for node in [0, 1] {
            let status = cluster.status_of(node, "c");
            assert_ne!(
                status.as_deref(),
                Some("Connected"),
                "R4: {:?} after c started with its router down, {} reads c Connected",
                since.elapsed(),
                cluster.nodes[node].id
            );
        }
        // A node may report before it has met anyone, or only once its
        // session is open; either is allowed here.
        if report.is_none()
            && let Ok(seen) = late.await_report(Duration::ZERO)
        {
            report = Some(seen);
        }
        if let Some(seen) = &report {
            let url = format!("http://127.0.0.1:{}/api/peers", seen.http);
            if let Ok(response) = reqwest::blocking::get(&url)
                && let Ok(body) = response.json::<serde_json::Value>()
            {
                let connected: Vec<String> = body["peers"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|p| p["status"] == "Connected")
                    .filter_map(|p| p["id"].as_str().map(str::to_string))
                    .collect();
                assert!(
                    connected.is_empty(),
                    "R4: {:?} after c started with its router down, c lists {connected:?} \
                     Connected",
                    since.elapsed()
                );
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
    converge("R4", &cluster, &model, &["a1", "b1", "a2"]);

    // R4: the router returns on the same port; c joins and converges.
    let router = ok(
        "R4",
        Router::start(&scratch, Some(port))
            .map_err(|why| format!("the router could not return on port {port}: {why}")),
    );
    must(router.check_listening());
    let back = Instant::now();
    let report = match report {
        Some(seen) => seen,
        None => ok(
            "R4",
            late.await_report(REPORT_TIMEOUT)
                .map_err(|why| format!("c, once its router returned: {why}")),
        ),
    };
    let listen = ok(
        "R4",
        report
            .listen
            .filter(|p| *p != 0)
            .ok_or_else(|| "c, a peer on LISTEN_PORT=0, reported no listen port".to_string()),
    );
    cluster.nodes.push(late.into_node(report.http, listen));
    ok("R4", cluster.await_health(2));
    ok("R4", nw16a::witness(&cluster.nodes[2]).map(|_| ()));
    ok("R4", cluster.await_mesh(&[0, 1, 2]));
    println!(
        "NW16B RECORD R4: c listed a and b Connected, and they c, {:?} after the router returned",
        back.elapsed()
    );
    ok(
        "R4",
        nw16b::await_links(&nw16b::pids(&cluster), LINK_TIMEOUT).map(|_| ()),
    );
    ok("R4", cluster.join_model(2, &model));
    ok(
        "R4",
        cluster.await_names(2, &model, &set(&["a1", "b1", "a2"])),
    );
    println!(
        "NW16B RECORD R4: c served a1, b1, a2 {:?} after the router returned",
        back.elapsed()
    );
    ok("R4", cluster.write_class(2, &model, "c1"));
    converge("R4", &cluster, &model, &["a1", "b1", "a2", "c1"]);
    drop(router);
    cluster.nodes.clear();
}

// ---------------------------------------------------------------------------
// R5
// ---------------------------------------------------------------------------

#[test]
fn r5_clients_listen_on_nothing_converge_through_the_router_and_one_started_first_waits_for_it() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-r5"));
    let port = ok("set-up", p5::kernel_chosen_port());
    let given: Vec<u16> = (0..3)
        .map(|_| ok("set-up", p5::kernel_chosen_port()))
        .collect();

    // R5: `g`, a client started while its router is down, keeps running.
    let mut first = ok(
        "R5",
        nw16b::start(&binary, &scratch, &Spec::client("g", port, given[0])),
    );
    let since = Instant::now();
    while since.elapsed() < BEFORE_THE_ROUTER {
        ok(
            "R5",
            first.alive().map_err(|why| {
                format!(
                    "g, a client started while its router was down, exited after {:?} where it \
                     must keep retrying: {why}",
                    since.elapsed()
                )
            }),
        );
        thread::sleep(Duration::from_millis(100));
    }

    let router = must(Router::start(&scratch, Some(port)));
    must(router.check_listening());
    let up = Instant::now();
    let mut clients = vec![("g", first)];
    for (id, listen) in [("e", given[1]), ("f", given[2])] {
        clients.push((
            id,
            ok(
                "R5",
                nw16b::start(&binary, &scratch, &Spec::client(id, port, listen)),
            ),
        ));
    }
    let mut nodes = Vec::new();
    for (id, mut started) in clients {
        let report = ok(
            "R5",
            started
                .await_report(REPORT_TIMEOUT)
                .map_err(|why| format!("client {id}: {why}")),
        );
        nodes.push(started.into_node(report.http, 0));
    }
    let mut cluster = ok("R5", nw16b::cluster(&binary, &scratch, nodes, false));
    for node in &cluster.nodes {
        ok(
            "R5",
            nw16a::token_at(router.port, &node.id, nw16a::WITNESS_TIMEOUT).map_err(|why| {
                format!(
                    "NW16B WITNESS: client {} is not seen on Zenoh through its router: {why}",
                    node.id
                )
            }),
        );
    }
    // R5: clients listen on no replication port: their HTTP port only.
    for (node, listen) in cluster.nodes.iter().zip(&given) {
        let held = nw16b::listen_ports_of(node.pid);
        assert_eq!(
            held,
            [node.http].into_iter().collect(),
            "R5: client {} (pid {}, given LISTEN_PORT={listen}) holds LISTEN sockets on \
             {held:?}, where a client listens on its HTTP port {} only",
            node.id,
            node.pid,
            node.http
        );
    }
    ok("R5", cluster.await_mesh(&[0, 1, 2]));
    println!(
        "NW16B RECORD R5: g, e, f listed each other Connected {:?} after the router came up",
        up.elapsed()
    );
    let model = model_on("R5", &cluster, 1);
    for (node, name) in [(1, "e1"), (2, "f1"), (0, "g1")] {
        ok("R5", cluster.write_class(node, &model, name));
    }
    converge("R5", &cluster, &model, &["e1", "f1", "g1"]);
    // R5: through the router: no link joins two clients, each is linked to
    // the router.
    let pids = nw16b::pids(&cluster);
    for ((a, b), links) in nw16b::pair_links(&pids) {
        assert!(
            links.is_empty(),
            "R5: a TCP link joins clients {a} and {b} ({links:?}): their data did not go through \
             the router alone"
        );
    }
    for (id, pid) in &pids {
        assert!(
            !nw16b::links_between(*pid, router.pid).is_empty(),
            "R5: client {id} (pid {pid}) holds no TCP link to the router (pid {})",
            router.pid
        );
    }
    println!(
        "NW16B RECORD R5: converged through the router {:?} after it came up",
        up.elapsed()
    );
    cluster.nodes.clear();
}
