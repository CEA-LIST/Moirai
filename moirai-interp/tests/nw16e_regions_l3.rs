//! NW.16e on loopback (vault `spec/items/NW.16e.md`): moirai-router started
//! with `ROUTER_REGIONS=core,site-h` and peer-mode `model_node`s introduced
//! by it, each claiming its region with `ZENOH_REGION` (Cam's topology, the
//! NW.16c note's Layout R): direct inside a region, through the router
//! between regions, and a catch-all for the rest.
//!
//! Every node runs `MOIRAI_TRANSPORT=zenoh` with `ZENOH_ROUTER` naming the
//! router on loopback and `PEERS` empty. Read from outside: whom each node
//! lists and what it holds, through its HTTP API (`nw16a::Cluster`); links
//! between processes in the kernel's tables (`nw16b`); the bytes each
//! process's TCP sockets receive in a window, from the kernel's counters
//! (`nw16d`).
//!
//! - `s1_`: `p` in `core` and `q` in `site-h`, each preloaded with NW.16b's
//!   connect shim refusing a connect to the other's port, so no direct link
//!   can form: each lists the other `Connected`, and a model `p` creates,
//!   with classes each writes, converges on both. `NW16E BLOCK WITNESS`: a
//!   TCP link joined them anyway; the run is void.
//! - `s2_`: `c1` and `c2` in `core` are introduced, link directly, converge,
//!   and while only `core` is present the router receives at most
//!   `ROUTER_RX_MAX` bytes while `c1` writes (W2a). Then `h1` in `site-h`
//!   joins (and, as the meter's control, writes classes that reach `c1`
//!   through the router: W2k), `c3` in `core` joins late and adopts a state
//!   transfer from `c1` (W2b), and `c1`, having paused `h1`, writes classes
//!   that `c2` and `c3` receive by per-peer puts (W2c): in W2b and W2c the
//!   router receives at most `ROUTER_RX_MAX` bytes, while `h1` subscribes
//!   to every broadcast through it.
//! - `s3_`: beside `c1` (`core`) and `h1` (`site-h`), `k1` claims `site-k`,
//!   which the router does not list, and `n1` claims no region; all four
//!   list each other `Connected` and converge.
//! - `s4_`: beside `c1` and `h1`, a client-mode node `cl` (`ZENOH_MODE=
//!   client`, no region) lists both `Connected` and converges with them.
//!
//! The router must have passed its L1 check (`moirai-router`'s
//! `nw16b_router_config_l1`) before this file runs on the host (NW.16b F3).
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo build -j 2 -p moirai-router
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16e_regions_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
mod nw16d;
mod nw16e;
#[path = "nw16a/p5.rs"]
mod p5;

use std::collections::BTreeSet;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use nw16a::{Cluster, node_binary, scratch};
use nw16b::{LINK_TIMEOUT, REPORT_TIMEOUT};
use nw16d::{Party, Reading, Window, name_set, names};
use nw16e::{NodeSpec, RegionRouter};

/// The router's list in every scenario: two regions, and the catch-all the
/// claim adds after them.
const REGIONS: &str = "core,site-h";
/// The most the router may receive in a window where it must carry nothing
/// (registered in the spec, section 7).
const ROUTER_RX_MAX: u64 = 4096;
/// The least the receiving node must receive in a window for the window to
/// have carried a payload at all.
const PAYLOAD_MIN: u64 = 20_000;
/// Classes written: four operations each.
const S1_CLASSES: usize = 20;
const W2A_CLASSES: usize = 100;
const W2K_CLASSES: usize = 40;
const W2C_CLASSES: usize = 40;
const S3_CLASSES: usize = 5;
/// Left after the links form and before a window opens, so that the
/// greetings a new peer draws are over.
const SETTLE: Duration = Duration::from_secs(3);

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

/// A router started with `ROUTER_REGIONS=core,site-h` that passed NW.16b's
/// R0 and R6 checks.
fn router(scratch: &Path) -> RegionRouter {
    let router = must(RegionRouter::start(scratch, Some(REGIONS)));
    must(router.check_listening());
    println!(
        "NW16E RECORD router pid {} port {} report {}",
        router.pid, router.port, router.report
    );
    router
}

/// The router and every node of `cluster` as parties of a window.
fn parties(router: &RegionRouter, cluster: &Cluster) -> Vec<Party> {
    let mut parties = vec![Party::new("router", router.pid, None)];
    for node in &cluster.nodes {
        parties.push(Party::new(&node.id, node.pid, Some(node.http)));
    }
    parties
}

#[track_caller]
fn write_all(point: &str, cluster: &Cluster, node: usize, model: &str, names: &[String]) {
    for name in names {
        ok(point, cluster.write_class(node, model, name));
    }
}

/// The links between every pair of `cluster`'s nodes and between each node
/// and the router, for the record.
fn links_record(router: &RegionRouter, cluster: &Cluster) -> String {
    let mut parts = Vec::new();
    for (i, a) in cluster.nodes.iter().enumerate() {
        parts.push(format!(
            "{}-router {}",
            a.id,
            nw16e::link_count(a.pid, router.pid)
        ));
        for b in &cluster.nodes[i + 1..] {
            parts.push(format!(
                "{}-{} {}",
                a.id,
                b.id,
                nw16e::link_count(a.pid, b.pid)
            ));
        }
    }
    parts.join(", ")
}

/// The claim of a window where the router must carry nothing: the meter
/// saw it whole, the receiving node got a payload, and the router received
/// at most `ROUTER_RX_MAX` bytes.
#[track_caller]
fn router_kept_out(point: &str, reading: &Reading, receiver: &str) {
    println!("NW16E RECORD {point}: {}", reading.line());
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
         {payload} B; at most {ROUTER_RX_MAX} B may reach the router while two nodes of one \
         region exchange frames over their own link: {}",
        reading.label,
        reading.line()
    );
}

/// Every node of `indices` serves exactly `expected` for `model`.
#[track_caller]
fn converge(
    point: &str,
    cluster: &Cluster,
    indices: &[usize],
    model: &str,
    expected: &BTreeSet<String>,
) {
    for &node in indices {
        ok(point, cluster.await_names(node, model, expected));
    }
}

/// The class written into the default log (`LOG_ID`) of S2's nodes.
const DEFAULT_CLASS: &str = "d0";

/// Give the default log a history, written on `node`. Every node hosts the
/// default log, and while it holds nothing, each node asks one connected
/// peer every 2 s for a state transfer of it, round by round, and falls back
/// to a delta sync (`retry_state_transfer`, `STATE_TRANSFER_RETRY`): per-peer
/// frames that cross the router whenever the peer asked is in another
/// region. Once its default log has delivered anything, a node stops asking.
fn default_history(cluster: &Cluster, node: usize) -> Result<(), String> {
    cluster.submit_default(node, &Cluster::default_install())?;
    for op in nw16a::si01::Uml::new().named_class(0, DEFAULT_CLASS) {
        cluster.submit_default(node, &op)?;
    }
    Ok(())
}

/// `node`'s default log holds the class [`default_history`] wrote.
fn default_held(cluster: &Cluster, node: usize) -> Result<(), String> {
    cluster.await_default_names(node, &name_set(&[DEFAULT_CLASS.to_string()]))
}

/// Kill every node now, before the router goes with its handle.
fn cluster_end(mut cluster: Cluster) {
    cluster.nodes.clear();
}

// ---------------------------------------------------------------------------
// S1: two regions, the direct link blocked, converge through the router
// ---------------------------------------------------------------------------

#[test]
fn s1_two_regions_whose_direct_link_is_blocked_converge_through_the_router() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16e-s1"));
    let shim = ok("set-up", nw16b::connect_shim(&scratch));
    let router = router(&scratch);
    let p_port = ok("set-up", p5::kernel_chosen_port());
    let q_port = loop {
        let port = ok("set-up", p5::kernel_chosen_port());
        if port != p_port {
            break port;
        }
    };
    let mut nodes = Vec::new();
    let mut logs = Vec::new();
    for (id, region, mine, other) in [
        ("p", "core", p_port, q_port),
        ("q", "site-h", q_port, p_port),
    ] {
        let log = scratch.join(format!("{id}.connects"));
        let spec = NodeSpec {
            listen_port: mine,
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
            ..NodeSpec::peer(id, router.port, Some(region))
        };
        let node = ok("set-up", nw16e::peer_node(&binary, &scratch, &spec));
        if node.listen != mine {
            panic!(
                "set-up: {id} was given LISTEN_PORT={mine} and reported {}, so the shim refuses \
                 the wrong port",
                node.listen
            );
        }
        nodes.push(node);
        logs.push(log);
    }
    let cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, true));
    let (p_pid, q_pid) = (cluster.nodes[0].pid, cluster.nodes[1].pid);
    let block = |when: &str| {
        let links = nw16b::links_between(p_pid, q_pid);
        assert!(
            links.is_empty(),
            "NW16E BLOCK WITNESS: {when}, a TCP link joins p and q ({links:?}) despite the \
             connect shim; S1 not observed"
        );
    };
    block("at the start");

    let since = Instant::now();
    ok("S1", cluster.await_mesh(&[0, 1]));
    let connected = since.elapsed();
    block("once they listed each other");
    let model = ok("set-up", cluster.create_model(0));
    ok("S1", cluster.join_model(1, &model));
    ok("S1", cluster.await_table(1, &model));

    let mut all = names('p', S1_CLASSES);
    let from_q = names('q', S1_CLASSES);
    let window = ok("set-up", Window::open("S1", &parties(&router, &cluster)));
    let wrote = Instant::now();
    write_all("S1", &cluster, 0, &model, &all);
    write_all("S1", &cluster, 1, &model, &from_q);
    all.extend(from_q);
    converge("S1", &cluster, &[0, 1], &model, &name_set(&all));
    let converged = wrote.elapsed();
    let reading = ok("set-up", window.close());
    block("at the end");
    println!(
        "NW16E RECORD S1: Connected after {connected:?}, converged {converged:?} after the first \
         write; refused connects p {:?} (q's port {q_port}), q {:?} (p's port {p_port}); links: {}",
        nw16b::refused_connects(&logs[0]),
        nw16b::refused_connects(&logs[1]),
        links_record(&router, &cluster)
    );
    println!("NW16E RECORD S1 window: {}", reading.line());
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// S2: two nodes of one region link directly and keep their frames off the
// router
// ---------------------------------------------------------------------------

#[test]
fn s2_two_nodes_of_one_region_link_directly_and_keep_their_frames_off_the_router() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16e-s2"));
    let router = router(&scratch);
    let nodes = vec![
        ok(
            "set-up",
            nw16e::peer(&binary, &scratch, "c1", router.port, Some("core")),
        ),
        ok(
            "set-up",
            nw16e::peer(&binary, &scratch, "c2", router.port, Some("core")),
        ),
    ];
    let mut cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, true));
    ok("S2", cluster.await_mesh(&[0, 1]));
    ok(
        "S2",
        nw16b::await_links(&nw16b::pids(&cluster), LINK_TIMEOUT).map(|_| ()),
    );
    ok("set-up", default_history(&cluster, 0));
    ok("set-up", default_held(&cluster, 1));
    let model = ok("set-up", cluster.create_model(0));
    ok("S2", cluster.join_model(1, &model));
    ok("S2", cluster.await_table(1, &model));
    thread::sleep(SETTLE);

    // W2a: only `core` is present; c1's frames reach c2 over their link.
    let mut all = names('a', W2A_CLASSES);
    let window = ok("set-up", Window::open("W2a", &parties(&router, &cluster)));
    write_all("S2", &cluster, 0, &model, &all);
    ok("S2", cluster.await_names(1, &model, &name_set(&all)));
    let reading = ok("set-up", window.close());
    router_kept_out("S2 W2a", &reading, "c2");

    // h1 of `site-h` joins: every broadcast now has a subscriber the router
    // must reach.
    cluster.nodes.push(ok(
        "set-up",
        nw16e::peer(&binary, &scratch, "h1", router.port, Some("site-h")),
    ));
    ok("set-up", cluster.await_health(2));
    ok("set-up", nw16a::witness(&cluster.nodes[2]));
    ok("S2", cluster.await_mesh(&[0, 1, 2]));
    ok("set-up", default_held(&cluster, 2));
    ok("S2", cluster.join_model(2, &model));
    ok("S2", cluster.await_names(2, &model, &name_set(&all)));
    thread::sleep(SETTLE);

    // W2k, the meter's control: h1's classes reach c1 through the router,
    // which must then be read receiving them.
    let from_h = names('k', W2K_CLASSES);
    all.extend(from_h.iter().cloned());
    let window = ok("set-up", Window::open("W2k", &parties(&router, &cluster)));
    write_all("S2", &cluster, 2, &model, &from_h);
    converge("S2", &cluster, &[0, 1], &model, &name_set(&all));
    let reading = ok("set-up", window.close());
    println!("NW16E RECORD S2 W2k: {}", reading.line());
    must(reading.witness("router"));
    let got = reading.of("c1").received;
    let router_rx = reading.of("router").received;
    assert!(
        got >= PAYLOAD_MIN && 2 * router_rx >= got,
        "NW16E METER CONTROL: h1 wrote {W2K_CLASSES} classes that c1 received through the router \
         ({got} B), and the meter read {router_rx} B received by the router; it must read at \
         least half of what c1 received, and c1 at least {PAYLOAD_MIN} B, or the meter cannot be \
         trusted to see data reaching the router: {}",
        reading.line()
    );

    // c3 of `core` joins late.
    cluster.nodes.push(ok(
        "set-up",
        nw16e::peer(&binary, &scratch, "c3", router.port, Some("core")),
    ));
    ok("set-up", cluster.await_health(3));
    ok("set-up", nw16a::witness(&cluster.nodes[3]));
    ok("S2", cluster.await_mesh(&[0, 1, 2, 3]));
    let core: Vec<(String, u32)> = nw16b::pids(&cluster)
        .into_iter()
        .filter(|(id, _)| id.starts_with('c'))
        .collect();
    ok("S2", nw16b::await_links(&core, LINK_TIMEOUT).map(|_| ()));
    ok("set-up", default_held(&cluster, 3));
    println!("NW16E RECORD S2 links: {}", links_record(&router, &cluster));
    thread::sleep(SETTLE);

    // Recorded, not a gate: what reaches the router while nothing is
    // written, over as long as SETTLE, with all four nodes up.
    let idle = ok("set-up", Window::open("idle", &parties(&router, &cluster)));
    thread::sleep(SETTLE);
    let idle = ok("set-up", idle.close());
    println!("NW16E RECORD S2 idle: {}", idle.line());

    // W2b: c3 joins the model; it asks its first connected peer by id, c1,
    // which answers with a state transfer, one per-peer frame.
    let window = ok("set-up", Window::open("W2b", &parties(&router, &cluster)));
    ok("S2", cluster.join_model(3, &model));
    ok("S2", cluster.await_names(3, &model, &name_set(&all)));
    let reading = ok("set-up", window.close());
    assert!(
        cluster.nodes[3].adopted(&model),
        "S2: c3 serves the model but its log never says it adopted a state transfer for \
         {model}:\n{}",
        cluster.nodes[3].tail()
    );
    router_kept_out("S2 W2b", &reading, "c3");

    // W2c: c1 pauses h1 (and h1 pauses c1), so each of c1's broadcasts is one
    // per-peer put to each of c2 and c3.
    ok("set-up", cluster.control(0, "/api/pause/h1"));
    ok("set-up", cluster.control(2, "/api/pause/c1"));
    thread::sleep(SETTLE);
    let before_c = name_set(&all);
    let from_c = names('b', W2C_CLASSES);
    all.extend(from_c.iter().cloned());
    let window = ok("set-up", Window::open("W2c", &parties(&router, &cluster)));
    write_all("S2", &cluster, 0, &model, &from_c);
    converge("S2", &cluster, &[1, 3], &model, &name_set(&all));
    let reading = ok("set-up", window.close());
    router_kept_out("S2 W2c", &reading, "c2");
    ok("S2", cluster.holds_names(2, &model, &before_c));
    ok("S2", cluster.control(0, "/api/resume/h1"));
    ok("S2", cluster.control(2, "/api/resume/c1"));
    converge("S2", &cluster, &[0, 1, 2, 3], &model, &name_set(&all));
    println!(
        "NW16E RECORD S2 links at the end: {}",
        links_record(&router, &cluster)
    );
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// S3: an unlisted region and no region, carried by the catch-all
// ---------------------------------------------------------------------------

#[test]
fn s3_a_node_of_an_unlisted_region_and_one_of_none_are_carried_by_the_catch_all() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16e-s3"));
    let router = router(&scratch);
    let layout: [(&str, Option<&str>); 4] = [
        ("c1", Some("core")),
        ("h1", Some("site-h")),
        ("k1", Some("site-k")),
        ("n1", None),
    ];
    let mut nodes = Vec::new();
    for (id, region) in layout {
        nodes.push(ok(
            "set-up",
            nw16e::peer(&binary, &scratch, id, router.port, region),
        ));
    }
    let cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, true));
    let since = Instant::now();
    ok("S3", cluster.await_mesh(&[0, 1, 2, 3]));
    let connected = since.elapsed();
    let model = ok("set-up", cluster.create_model(0));
    for node in 1..4 {
        ok("S3", cluster.join_model(node, &model));
        ok("S3", cluster.await_table(node, &model));
    }
    let mut all = Vec::new();
    let wrote = Instant::now();
    for (node, tag) in [(0, 'c'), (1, 'h'), (2, 'k'), (3, 'n')] {
        let written = names(tag, S3_CLASSES);
        write_all("S3", &cluster, node, &model, &written);
        all.extend(written);
    }
    converge("S3", &cluster, &[0, 1, 2, 3], &model, &name_set(&all));
    println!(
        "NW16E RECORD S3: Connected after {connected:?}, converged {:?} after the first write; \
         links: {}",
        wrote.elapsed(),
        links_record(&router, &cluster)
    );
    cluster_end(cluster);
}

// ---------------------------------------------------------------------------
// S4: a client beside the regions
// ---------------------------------------------------------------------------

#[test]
fn s4_a_client_beside_the_regions_converges_with_them() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16e-s4"));
    let router = router(&scratch);
    let given = ok("set-up", p5::kernel_chosen_port());
    let mut nodes = vec![
        ok(
            "set-up",
            nw16e::peer(&binary, &scratch, "c1", router.port, Some("core")),
        ),
        ok(
            "set-up",
            nw16e::peer(&binary, &scratch, "h1", router.port, Some("site-h")),
        ),
    ];
    // A client opens its session only once the router has taken it, and
    // reports its ports after: a client the router turns away never
    // reports, which is S4's failure, not the host's.
    let mut cl = ok(
        "set-up",
        nw16e::start_node(
            &binary,
            &scratch,
            &NodeSpec::client("cl", router.port, given),
        ),
    );
    let report = ok(
        "S4",
        cl.await_report(REPORT_TIMEOUT).map_err(|why| {
            format!("the client never opened its session through the router: {why}")
        }),
    );
    nodes.push(cl.into_node(report.http, 0));
    let cluster = ok("set-up", nw16b::cluster(&binary, &scratch, nodes, false));
    for node in [0, 1] {
        ok("set-up", nw16a::witness(&cluster.nodes[node]));
    }
    let since = Instant::now();
    ok("S4", cluster.await_mesh(&[0, 1, 2]));
    let connected = since.elapsed();
    let model = ok("set-up", cluster.create_model(0));
    for node in [1, 2] {
        ok("S4", cluster.join_model(node, &model));
        ok("S4", cluster.await_table(node, &model));
    }
    let mut all = Vec::new();
    let wrote = Instant::now();
    for (node, tag) in [(0, 'c'), (1, 'h'), (2, 'l')] {
        let written = names(tag, S3_CLASSES);
        write_all("S4", &cluster, node, &model, &written);
        all.extend(written);
    }
    converge("S4", &cluster, &[0, 1, 2], &model, &name_set(&all));
    println!(
        "NW16E RECORD S4: Connected after {connected:?}, converged {:?} after the first write; \
         links: {}",
        wrote.elapsed(),
        links_record(&router, &cluster)
    );
    cluster_end(cluster);
}
