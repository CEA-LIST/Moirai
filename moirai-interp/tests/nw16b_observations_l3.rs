//! NW.16b, observations, not gates (vault `spec/items/NW.16b.md`): each test
//! is ignored and run on its own, prints `NW16B O1` or `NW16B O2` lines,
//! and fails only at a set-up step or at its instrument's witness, never on
//! what it observes.
//!
//! - O1 (the NW.14 note's open point, 6.2): two peers `p` and `q`, each
//!   introduced by the router and each preloaded with `nw16b/connect_shim.c`
//!   refusing a connect to the other's listen port, so no direct link can
//!   form whichever dials. Recorded: whether each lists the other
//!   `Connected`, whether a model `p` creates reaches `q` and each one's
//!   class reaches the other within 30 s, that is, whether the router
//!   forwards data between two peers that cannot reach each other.
//!   `NW16B BLOCK WITNESS`: a TCP link joined `p` and `q` anyway, so the
//!   observation is void.
//! - O2: three peers linked pairwise; the bytes the router's sockets send
//!   and receive (the kernel's TCP counters, `ss -tin`) while `a` writes 60
//!   classes that `b` and `c` receive, beside the bytes the links between
//!   nodes carry, to see whether data crosses the router while the peers
//!   are linked.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16b_observations_l3 \
//!     -- --ignored --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
#[path = "nw16a/p5.rs"]
mod p5;

use std::thread;
use std::time::{Duration, Instant};

use nw16a::{node_binary, scratch, set};
use nw16b::{LINK_TIMEOUT, REPORT_TIMEOUT, Router, Spec};

/// How long O1 waits for each thing it observes.
const OBSERVE: Duration = Duration::from_secs(30);

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

#[test]
#[ignore = "an observation: run on its own"]
fn o1_two_peers_whose_direct_link_is_blocked_record_whether_the_router_forwards() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-o1"));
    let shim = ok("set-up", nw16b::connect_shim(&scratch));
    let router = must(Router::start(&scratch, Some(0)));
    must(router.check_listening());
    let p_port = ok("set-up", p5::kernel_chosen_port());
    let q_port = loop {
        let port = ok("set-up", p5::kernel_chosen_port());
        if port != p_port {
            break port;
        }
    };
    let mut nodes = Vec::new();
    let mut logs = Vec::new();
    for (id, mine, other) in [("p", p_port, q_port), ("q", q_port, p_port)] {
        let log = scratch.join(format!("{id}.connects"));
        let spec = Spec {
            id,
            listen_port: mine,
            http_port: 0,
            ports_file: true,
            peers: String::new(),
            router: Some(router.port),
            client: false,
            preload: Some((
                shim.clone(),
                vec![
                    ("NW16B_REFUSE_CONNECT".into(), other.to_string()),
                    (
                        "NW16B_CONNECT_LOG".into(),
                        log.to_string_lossy().into_owned(),
                    ),
                ],
            )),
        };
        let mut started = ok("set-up", nw16b::start(&binary, &scratch, &spec));
        let report = ok("set-up", started.await_report(REPORT_TIMEOUT));
        nodes.push(started.into_node(report.http, mine));
        logs.push(log);
    }
    let mut cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, true));
    let (p_pid, q_pid) = (cluster.nodes[0].pid, cluster.nodes[1].pid);
    let block = |when: &str| {
        let links = nw16b::links_between(p_pid, q_pid);
        assert!(
            links.is_empty(),
            "NW16B BLOCK WITNESS: {when}, a TCP link joins p and q ({links:?}) despite the device; \
             O1 not observed"
        );
    };

    // Introductions: does each try the other's port, and list it?
    let since = Instant::now();
    let mut statuses = (None, None);
    while since.elapsed() < OBSERVE {
        block("while they were introduced");
        statuses = (cluster.status_of(0, "q"), cluster.status_of(1, "p"));
        if statuses.0.as_deref() == Some("Connected") && statuses.1.as_deref() == Some("Connected")
        {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }
    let refused_p = nw16b::refused_connects(&logs[0]);
    let refused_q = nw16b::refused_connects(&logs[1]);
    println!(
        "NW16B O1 introductions: after {:?}, p reads q {:?} and q reads p {:?}; p's refused \
         connects {:?} (to q's port {q_port}), q's {:?} (to p's port {p_port})",
        since.elapsed(),
        statuses.0,
        statuses.1,
        refused_p,
        refused_q
    );

    // Data: does a model p creates, and a class each writes, cross?
    let model = ok("set-up", cluster.create_model(0));
    let joined = cluster.join_model(1, &model);
    let table = joined.clone().and_then(|()| {
        cluster
            .await_table(1, &model)
            .map_err(|_| "no table".to_string())
    });
    let p1 = cluster.write_class(0, &model, "p1");
    let q1 = match &table {
        Ok(()) => cluster.write_class(1, &model, "q1"),
        Err(why) => Err(format!("q never received the model's table: {why}")),
    };
    let wanted = match (&p1, &q1) {
        (Ok(()), Ok(())) => set(&["p1", "q1"]),
        _ => set(&["p1"]),
    };
    let since = Instant::now();
    let mut held = (Err(String::new()), Err(String::new()));
    while since.elapsed() < OBSERVE {
        block("while they wrote");
        held = (cluster.names(0, &model), cluster.names(1, &model));
        if held.0.as_ref() == Ok(&wanted) && held.1.as_ref() == Ok(&wanted) {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }
    let forwarded = held.0.as_ref() == Ok(&wanted) && held.1.as_ref() == Ok(&wanted);
    println!(
        "NW16B O1 data: router forwards between p and q = {forwarded}; q joined {joined:?}, q's \
         table {table:?}, p wrote p1 {p1:?}, q wrote q1 {q1:?}; after {:?} p serves {:?} and q \
         serves {:?}",
        since.elapsed(),
        held.0,
        held.1
    );
    block("at the end");
    cluster.nodes.clear();
}

#[test]
#[ignore = "an observation: run on its own"]
fn o2_bytes_through_the_router_while_three_peers_are_linked() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16b-o2"));
    let router = must(Router::start(&scratch, Some(0)));
    let nodes = ok(
        "set-up",
        nw16b::peers(&binary, &scratch, &["a", "b", "c"], router.port),
    );
    let mut cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, true));
    ok("set-up", cluster.await_mesh(&[0, 1, 2]));
    let pids = nw16b::pids(&cluster);
    ok(
        "set-up",
        nw16b::await_links(&pids, LINK_TIMEOUT).map(|_| ()),
    );
    let model = ok("set-up", cluster.create_model(0));
    for node in [1, 2] {
        ok("set-up", cluster.join_model(node, &model));
        ok("set-up", cluster.await_table(node, &model));
    }
    let names: Vec<String> = (0..60).map(|i| format!("o{i:03}")).collect();
    // The node ends of the links between nodes, as (local, remote) ports.
    let mut node_ends = std::collections::BTreeSet::new();
    for (_, x) in &pids {
        for (_, y) in &pids {
            if x == y {
                continue;
            }
            for a in nw16b::established_of(*x) {
                if nw16b::established_of(*y)
                    .iter()
                    .any(|b| b.local == a.remote && b.remote == a.local)
                {
                    node_ends.insert((a.local_port(), a.remote_port()));
                }
            }
        }
    }
    // (router sent, router received, node links received).
    let count = |bytes: &[(u16, u16, u64, u64)]| {
        let mut sums = (0u64, 0u64, 0u64);
        for (local, remote, sent, received) in bytes {
            if *local == router.port {
                sums.0 += sent;
                sums.1 += received;
            }
            if node_ends.contains(&(*local, *remote)) {
                sums.2 += received;
            }
        }
        sums
    };
    let before = count(&ok("set-up", nw16b::tcp_bytes()));
    let started = Instant::now();
    for name in &names {
        ok("set-up", cluster.write_class(0, &model, name));
    }
    let wanted: std::collections::BTreeSet<String> = names.iter().cloned().collect();
    for node in [1, 2] {
        ok("set-up", cluster.await_names(node, &model, &wanted));
    }
    let after = count(&ok("set-up", nw16b::tcp_bytes()));
    println!(
        "NW16B O2: while a wrote 60 classes (4 operations each) that b and c received in {:?}, \
         the router's sockets sent {} B and received {} B, and the node ends of the {} links \
         between nodes received {} B",
        started.elapsed(),
        after.0 - before.0,
        after.1 - before.1,
        node_ends.len(),
        after.2 - before.2
    );
    cluster.nodes.clear();
}
