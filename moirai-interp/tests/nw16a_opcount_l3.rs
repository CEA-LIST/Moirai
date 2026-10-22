//! NW.16a, an observation, not a pass gate (vault `spec/items/NW.16a.md`):
//! the claim of `opcount_double_delivery`
//! (`moirai-network/tests/e2e_convergence.rs`, ignored there), each remote
//! operation counted once in `/api/operations`, read on `model_node`
//! processes under each transport.
//!
//! The original runs Arachne's generated `network_node`, which this branch
//! does not build; the claim is ported here as the orchestrator allowed: two
//! nodes, the default log (the one `/api/operations` lists) opened by `a`
//! with SimpleUML's `Install`, three classes written on each (two
//! operations a class for a two-letter name), and every node's
//! `/api/operations` read once both have converged. Under TCP both peers
//! dial each other, so two streams exist between them and a remote
//! operation arrives twice; under Zenoh one
//! transport links two sessions whoever dials (note section 8), so the note
//! predicts once. Both tests are ignored; the spec's record keeps what each
//! prints and whether its assertion held.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16a_opcount_l3 \
//!     -- --ignored --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;

use nw16a::{Cluster, Transport, set, si01};

/// How many operations `/api/operations` lists on `node`: its default
/// log's delivery record.
fn listed(cluster: &Cluster, node: usize) -> usize {
    let (_, body) = cluster
        .get(node, "/api/operations")
        .unwrap_or_else(|e| panic!("{e}"));
    body["operations"].as_array().map_or(0, Vec::len)
}

fn opcount(transport: Transport) {
    let cluster =
        Cluster::start(transport, "opcount", &["a", "b"]).unwrap_or_else(|e| panic!("{e}"));
    cluster
        .submit_default(0, &Cluster::default_install())
        .unwrap_or_else(|e| panic!("{e}"));
    let names = ["a1", "a2", "a3", "b1", "b2", "b3"];
    for name in names {
        let node = usize::from(name.starts_with('b'));
        if node == 1 {
            // b writes once the table has reached it: its default log has
            // delivered the Install.
            let deadline = std::time::Instant::now() + nw16a::CONVERGE_TIMEOUT;
            while !matches!(cluster.get(1, "/api/metrics"), Ok((200, m))
                    if m["delivered_ops"].as_u64().unwrap_or(0) >= 1)
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(nw16a::POLL);
            }
        }
        for op in si01::Uml::new().named_class(0, name) {
            let mut sent = cluster.submit_default(node, &op);
            let deadline = std::time::Instant::now() + nw16a::CONVERGE_TIMEOUT;
            while sent.is_err() && std::time::Instant::now() < deadline {
                std::thread::sleep(nw16a::POLL);
                sent = cluster.submit_default(node, &op);
            }
            sent.unwrap_or_else(|e| panic!("{e}"));
        }
    }
    for node in 0..2 {
        cluster
            .await_default_names(node, &set(&names))
            .unwrap_or_else(|e| panic!("{e}"));
    }
    std::thread::sleep(nw16a::QUIET);
    // The Install and two operations per class, each written once.
    let written = 1 + names.len() * 2;
    let counts: Vec<usize> = (0..2).map(|node| listed(&cluster, node)).collect();
    for (node, count) in counts.iter().enumerate() {
        println!(
            "NW16A OPCOUNT {transport:?} {}: {count} operations listed, {written} written \
             cluster-wide",
            cluster.nodes[node].id
        );
    }
    for (node, count) in counts.iter().enumerate() {
        assert_eq!(
            *count, written,
            "opcount on {transport:?}: {} lists {count} operations where {written} were written \
             cluster-wide",
            cluster.nodes[node].id
        );
    }
}

#[test]
#[ignore = "NW.16a observation of opcount_double_delivery's claim: run with --ignored"]
fn opcount_each_remote_operation_is_counted_once_zenoh() {
    opcount(Transport::Zenoh);
}

#[test]
#[ignore = "NW.16a observation of opcount_double_delivery's claim: run with --ignored"]
fn opcount_each_remote_operation_is_counted_once_tcp() {
    opcount(Transport::Tcp);
}
