//! NW.16e at L2 (vault `spec/items/NW.16e.md`): one real process at a time,
//! read from outside.
//!
//! - RE: `moirai-router` given an invalid `ROUTER_REGIONS` (an empty name, a
//!   name over 32 bytes, a duplicate, the variable set but empty) exits
//!   non-zero within `REFUSAL_TIMEOUT`, and what it prints names
//!   `ROUTER_REGIONS`.
//! - RF: given a valid list, it starts, listens on its one port at 0.0.0.0,
//!   holds no scouting socket, and writes the names it applied, in the
//!   order given, as the `regions` list of its `PORTS_FILE`, beside `pid`
//!   and `listen_port`; with `ROUTER_REGIONS` unset its report holds no
//!   list (`regions` absent or null).
//! - ND: `model_node` given `ZENOH_REGION` where it is refused (beside
//!   `PEERS`, beside `ZENOH_MODE=client`, beside `ZENOH_CONFIG`, without
//!   `ZENOH_ROUTER`, empty, over 32 bytes) exits non-zero within
//!   `REFUSAL_TIMEOUT`, and what it prints names `ZENOH_REGION` and the
//!   other variable; the same node given a valid `ZENOH_REGION` beside
//!   `ZENOH_ROUTER` is still running then (the control of the six).
//!
//! Every router here binds port 0, with multicast scouting off by its L1
//! check: `moirai-router`'s `nw16b_router_config_l1` must pass before this
//! file runs on the host (NW.16b F3). The nodes' `ZENOH_ROUTER` names a port
//! the kernel chose and nobody listens on: no router is needed to read a
//! refusal.
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo build -j 2 -p moirai-router
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw16e_refusals_l2 \
//!     -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;
mod nw16b;
mod nw16e;
#[path = "nw16a/p5.rs"]
mod p5;

use nw16a::{node_binary, scratch};
use nw16e::{
    Ended, NodeSpec, REFUSAL_TIMEOUT, ROUTER_REGIONS, RegionRouter, ZENOH_REGION, nodes_until,
    refused_routers,
};
use serde_json::{Value, json};

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

fn record(point: &str, ended: &[Ended]) {
    for e in ended {
        println!("NW16E RECORD {point} {}", e.line());
    }
}

// ---------------------------------------------------------------------------
// RE: the router refuses an invalid list
// ---------------------------------------------------------------------------

#[test]
fn re_a_router_given_an_invalid_region_list_stops_naming_router_regions() {
    let scratch = ok("set-up", scratch("nw16e-re"));
    let long_ascii = "x".repeat(33);
    let long_utf8 = "\u{20ac}".repeat(11); // 11 characters, 33 bytes
    let cases: Vec<(&str, &str)> = vec![
        ("set-but-empty", ""),
        ("empty-middle", "core,,site-h"),
        ("empty-first", ",core"),
        ("empty-last", "core,"),
        ("duplicate", "core,site-h,core"),
        ("long-33-ascii", &long_ascii),
        ("long-33-bytes-11-chars", &long_utf8),
    ];
    let ended = must(refused_routers(&scratch, &cases));
    record("RE", &ended);
    for e in &ended {
        assert!(
            e.scouting.is_empty(),
            "R6: the router of case {} held a UDP socket on {:?}, a scouting port, and was \
             killed",
            e.label,
            e.scouting
        );
    }
    let wrong: Vec<String> = ended
        .iter()
        .filter(|e| !(e.refused() && e.names_all(&[ROUTER_REGIONS])))
        .map(Ended::line)
        .collect();
    assert!(
        wrong.is_empty(),
        "RE: a router given an invalid ROUTER_REGIONS must exit non-zero within {REFUSAL_TIMEOUT:?} \
         with a message naming ROUTER_REGIONS; these did not:\n{}",
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// RF: the router reports the list it applied
// ---------------------------------------------------------------------------

#[test]
fn rf_the_router_reports_the_regions_it_applied_and_none_without_them() {
    let scratch = ok("set-up", scratch("nw16e-rf"));
    for (list, expected) in [
        ("core,site-h,site-k", json!(["core", "site-h", "site-k"])),
        ("a\"b,c\\d", json!(["a\"b", "c\\d"])),
    ] {
        let router = must(RegionRouter::start(&scratch, Some(list)));
        must(router.check_listening());
        println!(
            "NW16E RECORD RF ROUTER_REGIONS=`{list}`: pid {} port {} report {}",
            router.pid, router.port, router.report
        );
        assert_eq!(
            router.applied(),
            Some(&expected),
            "RF: the router started with ROUTER_REGIONS=`{list}` reported {}, whose `regions` \
             must be the names it applied, in the order given: {expected}",
            router.report
        );
    }
    let router = must(RegionRouter::start(&scratch, None));
    must(router.check_listening());
    println!(
        "NW16E RECORD RF {ROUTER_REGIONS} unset: pid {} port {} report {}",
        router.pid, router.port, router.report
    );
    assert!(
        matches!(router.applied(), None | Some(Value::Null)),
        "RF: the router started with ROUTER_REGIONS unset reported {}, which names a region \
         list; unset, it keeps Zenoh's auto preset and reports none",
        router.report
    );
}

// ---------------------------------------------------------------------------
// ND: the node refuses ZENOH_REGION where it does not apply
// ---------------------------------------------------------------------------

#[test]
fn nd_a_node_given_zenoh_region_where_it_is_refused_exits_naming_both_variables() {
    let binary = ok("set-up", node_binary());
    let scratch = ok("set-up", scratch("nw16e-nd"));
    // Ports the kernel chose and released: nobody listens on them.
    let dead_router = ok("set-up", p5::kernel_chosen_port());
    let dead_peer = ok("set-up", p5::kernel_chosen_port());
    let client_port = ok("set-up", p5::kernel_chosen_port());
    // A Zenoh configuration file that keeps every node on loopback with
    // scouting off, as NW.15's `ZENOH_CONFIG` tests write theirs.
    let file = scratch.join("nw16e_as_is.json5");
    ok(
        "set-up",
        std::fs::write(
            &file,
            r#"{
  mode: "peer",
  listen: { endpoints: ["tcp/127.0.0.1:0"] },
  connect: { endpoints: [] },
  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
}"#,
        )
        .map_err(|e| e.to_string()),
    );
    let in_core = |id: &str| NodeSpec::peer(id, dead_router, Some("core"));
    let peers = NodeSpec {
        peers: format!("x:127.0.0.1:{dead_peer}"),
        ..in_core("nd-peers")
    };
    let client = NodeSpec {
        region: Some("core".into()),
        ..NodeSpec::client("nd-client", dead_router, client_port)
    };
    let config = NodeSpec {
        router: None,
        extra: vec![("ZENOH_CONFIG".into(), file.to_string_lossy().into_owned())],
        ..in_core("nd-config")
    };
    let no_router = NodeSpec {
        router: None,
        ..in_core("nd-no-router")
    };
    let empty = NodeSpec {
        region: Some(String::new()),
        ..in_core("nd-empty")
    };
    let long = NodeSpec {
        region: Some("x".repeat(33)),
        ..in_core("nd-long")
    };
    let valid = in_core("nd-valid");
    let cases: Vec<(&str, NodeSpec)> = vec![
        ("beside-PEERS", peers),
        ("beside-ZENOH_MODE-client", client),
        ("beside-ZENOH_CONFIG", config),
        ("without-ZENOH_ROUTER", no_router),
        ("empty", empty),
        ("over-32-bytes", long),
        ("control-valid", valid),
    ];
    let ended = must(nodes_until(&binary, &scratch, &cases));
    record("ND", &ended);
    let must_name: [(&str, &[&str]); 6] = [
        ("beside-PEERS", &[ZENOH_REGION, "PEERS"]),
        ("beside-ZENOH_MODE-client", &[ZENOH_REGION, "ZENOH_MODE"]),
        ("beside-ZENOH_CONFIG", &[ZENOH_REGION, "ZENOH_CONFIG"]),
        ("without-ZENOH_ROUTER", &[ZENOH_REGION, "ZENOH_ROUTER"]),
        ("empty", &[ZENOH_REGION]),
        ("over-32-bytes", &[ZENOH_REGION]),
    ];
    let mut wrong = Vec::new();
    for (label, names) in must_name {
        let e = ended
            .iter()
            .find(|e| e.label == label)
            .expect("every case ended");
        if !(e.refused() && e.names_all(names)) {
            wrong.push(format!("must exit non-zero naming {names:?}: {}", e.line()));
        }
    }
    let control = ended
        .iter()
        .find(|e| e.label == "control-valid")
        .expect("the control ended");
    assert!(
        control.status.is_none(),
        "NW16E REFUSAL CONTROL: the node given a valid ZENOH_REGION beside ZENOH_ROUTER in peer \
         mode did not run for {REFUSAL_TIMEOUT:?}, so a refusal read here cannot be told from a \
         node that stops anyway: {}",
        control.line()
    );
    assert!(
        wrong.is_empty(),
        "ND: a node given ZENOH_REGION where it is refused must exit non-zero within \
         {REFUSAL_TIMEOUT:?} with a message naming the variables; these did not:\n{}",
        wrong.join("\n")
    );
}
