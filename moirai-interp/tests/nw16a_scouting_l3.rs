//! NW.16a, P7 (vault `spec/items/NW.16a.md`, note 6.1 and Q1): two clusters
//! that run at once on one host and host the same log in Moirai session
//! `default` stay apart on the default configuration, and merge when
//! multicast scouting is turned on, which is the risk the default avoids.
//!
//! Cluster X is `x1`, `x2`; cluster Y is `y1`, `y2`; each meshed by its own
//! `PEERS_FILE`, all four on `MOIRAI_TRANSPORT=zenoh`, all hosting the
//! default log `LOG_ID` (`nw16a::LOG_ID`), the fixed id two copies of one
//! L3 suite share (note 6.1, NW.02's concurrent runs). `x1` opens that log
//! with SimpleUML's `Install` and writes a class `x`; `y1` opens it with the
//! same `Install` and writes `y`. Two identical installations of one log
//! are one table (`ModelLog::effect`), so a merge reads `{x, y}`.
//!
//! - `p7_two_clusters_on_the_default_configuration_exchange_nothing`: after
//!   each cluster has converged on its own class, every node keeps reading
//!   its own cluster's class only, lists no node of the other cluster, and
//!   holds no UDP socket on a scouting port (7446, Zenoh's default and
//!   Ledgera's; 7450, Moirai's). Safe on the host: nothing scouts.
//! - `p7_two_clusters_with_multicast_scouting_merge`, ignored by default:
//!   the same with `ZENOH_SCOUTING=multicast`; every node reads `{x, y}`,
//!   lists the other cluster's nodes, and holds a UDP socket on Moirai's
//!   scouting port 7450 and none on 7446. It refuses to start a node unless
//!   it runs in a network namespace with no interface but `lo` and
//!   `NW16A_ISOLATED_NETNS=1` says the harness meant it: multicast must
//!   never leave the machine. The spec's container command runs it.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16a_scouting_l3 \
//!     -- --test-threads=1
//! ```
#![cfg(feature = "network")]

mod nw16a;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nw16a::{Cluster, Transport, set, si01, udp_ports_of};

/// Moirai's multicast scouting address, registered in the spec and read in
/// `moirai_network::transport_config::MOIRAI_SCOUTING_ADDRESS` at d28b6d7.
const MOIRAI_SCOUTING: (&str, u16) = ("224.0.0.224", 7450);
/// Zenoh 1.10.1's default scouting address, which Ledgera's compiled
/// configuration also uses (note 3.2 and Z10).
const ZENOH_DEFAULT_SCOUTING: (&str, u16) = ("224.0.0.224", 7446);
/// How long two clusters on the default configuration are watched for a
/// leak once each has converged.
const APART: Duration = Duration::from_secs(10);

fn ok<T>(what: &str, result: Result<T, String>) -> T {
    result.unwrap_or_else(|why| panic!("P7 {what}: {why}"))
}

/// Open the default log with SimpleUML and write one class `name`.
///
/// With `merged_may_have_opened_it`, an `Install` refused because the log
/// already holds the table is passed over: under multicast scouting the
/// other cluster's `Install` may arrive first, which is the merge itself.
/// On the default configuration the refusal is P7's leak, asserted in
/// [`refused_install`]: only the other cluster's `Install` could open it.
fn open_and_write(
    cluster: &Cluster,
    node: usize,
    name: &str,
    merged_may_have_opened_it: bool,
) -> Result<(), String> {
    match cluster.submit_default(node, &Cluster::default_install()) {
        Ok(()) => {}
        Err(why) if merged_may_have_opened_it && why.contains("already holds a metamodel") => {
            println!(
                "NW16A P7: {} found the log already opened: {why}",
                cluster.nodes[node].id
            );
        }
        Err(why) => return Err(refused_install(cluster, node, why)),
    }
    for op in si01::Uml::new().named_class(0, name) {
        cluster.submit_default(node, &op)?;
    }
    Ok(())
}

fn two_clusters(scouting: bool) -> (Cluster, Cluster) {
    let extra: Vec<(String, String)> = if scouting {
        vec![("ZENOH_SCOUTING".into(), "multicast".into())]
    } else {
        Vec::new()
    };
    let x = ok(
        "cluster X",
        Cluster::start_with(Transport::Zenoh, "p7x", &["x1", "x2"], &extra),
    );
    let y = ok(
        "cluster Y",
        Cluster::start_with(Transport::Zenoh, "p7y", &["y1", "y2"], &extra),
    );
    ok("x1 writes", open_and_write(&x, 0, "x", scouting));
    ok("y1 writes", open_and_write(&y, 0, "y", scouting));
    (x, y)
}

#[test]
fn p7_two_clusters_on_the_default_configuration_exchange_nothing() {
    let (x, y) = two_clusters(false);
    let only_x = set(&["x"]);
    let only_y = set(&["y"]);
    // Each cluster converged on its own class: the frames were put.
    for node in 0..2 {
        ok("X converges", x.await_default_names(node, &only_x));
        ok("Y converges", y.await_default_names(node, &only_y));
    }
    let until = Instant::now() + APART;
    while Instant::now() < until {
        for (cluster, own, other) in [(&x, &only_x, ["y1", "y2"]), (&y, &only_y, ["x1", "x2"])] {
            for node in 0..2 {
                let held = ok("read", cluster.default_names(node));
                assert_eq!(
                    &held, own,
                    "P7 (Q1): on the default configuration {} serves {held:?} on the shared \
                     default log, so the other cluster's writes reached it",
                    cluster.nodes[node].id
                );
                let listed: Vec<String> = cluster
                    .peers_of(node)
                    .into_iter()
                    .map(|(id, _)| id)
                    .filter(|id| other.contains(&id.as_str()))
                    .collect();
                assert!(
                    listed.is_empty(),
                    "P7 (Q1): {} lists {listed:?} of the other cluster as peers",
                    cluster.nodes[node].id
                );
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    for cluster in [&x, &y] {
        for node in &cluster.nodes {
            let udp = udp_ports_of(node.pid);
            for port in [ZENOH_DEFAULT_SCOUTING.1, MOIRAI_SCOUTING.1] {
                assert!(
                    !udp.contains(&port),
                    "P7 (Q1): {} holds a UDP socket on scouting port {port} on the default \
                     configuration (its UDP ports: {udp:?})",
                    node.id
                );
            }
        }
    }
}

#[test]
#[ignore = "multicast scouting: run only in the spec's isolated container (--network none)"]
fn p7_two_clusters_with_multicast_scouting_merge() {
    if let Err(why) = nw16a::isolated() {
        panic!(
            "P7: refusing to start a node with multicast scouting outside an isolated network \
             namespace ({why}); run the spec's container command"
        );
    }
    assert_ne!(
        MOIRAI_SCOUTING, ZENOH_DEFAULT_SCOUTING,
        "Moirai's scouting address must not be Zenoh's default, which Ledgera scouts on"
    );
    assert_eq!(
        moirai_network::transport_config::MOIRAI_SCOUTING_ADDRESS,
        format!("{}:{}", MOIRAI_SCOUTING.0, MOIRAI_SCOUTING.1),
        "P7: the address Moirai scouts on is not the one the spec registers"
    );
    let (x, y) = two_clusters(true);
    let both = set(&["x", "y"]);
    for (cluster, other) in [(&x, ["y1", "y2"]), (&y, ["x1", "x2"])] {
        for node in 0..2 {
            ok("merge", cluster.await_default_names(node, &both));
            let listed: BTreeSet<String> = cluster
                .peers_of(node)
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            for id in other {
                assert!(
                    listed.contains(id),
                    "P7: with multicast scouting {} merged the other cluster's writes but does \
                     not list {id} ({listed:?})",
                    cluster.nodes[node].id
                );
            }
        }
    }
    for cluster in [&x, &y] {
        for node in &cluster.nodes {
            let udp = udp_ports_of(node.pid);
            assert!(
                udp.contains(&MOIRAI_SCOUTING.1) && !udp.contains(&ZENOH_DEFAULT_SCOUTING.1),
                "P7: {} scouts on UDP ports {udp:?}; it must hold Moirai's {} and not Zenoh's \
                 default {}",
                node.id,
                MOIRAI_SCOUTING.1,
                ZENOH_DEFAULT_SCOUTING.1
            );
        }
    }
}

/// P7 (Q1), round 3 (Cam's ruling R3.1, vault `spec/items/NW.16a.md`):
/// `open_and_write` hands here every refusal of a node's own cluster's
/// `Install` but the one its first arm passes over under multicast scouting.
/// On the default configuration a refusal because the shared default log
/// already holds a metamodel means the other cluster's `Install` reached
/// that node, the leak P7 forbids, asserted at a line of its own. Any other
/// refusal is handed back as it was, a set-up failure. What the node serves
/// and lists as peers is read for the message only and cannot fail the test.
fn refused_install(cluster: &Cluster, node: usize, why: String) -> String {
    let id = &cluster.nodes[node].id;
    assert!(
        !why.contains("already holds a metamodel"),
        "P7 (Q1): on the default configuration {id} refused its own cluster's Install because \
         the shared default log already holds a metamodel, so the other cluster's Install \
         reached it; {id} then serves {:?} and lists as peers {:?}; the refusal: {why}",
        cluster.default_names(node),
        cluster.peers_of(node)
    );
    why
}
