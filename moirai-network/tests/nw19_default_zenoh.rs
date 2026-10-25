//! NW.19, P1's library checks (vault `spec/items/NW.19.md`, section 1, P1):
//! with `MOIRAI_TRANSPORT` unset, a node built by any of the library's three
//! constructors runs on Zenoh, seen from outside.
//!
//! - NEW, LOGID, LISTENER: a node built by `Node::new`, `Node::new_with_log_id`
//!   and `Node::with_listener_and_log_id` in this process, with every
//!   transport variable removed, declares its liveliness token
//!   `moirai/default/node/<id>` (the session chunk is `SESSION_ID`'s default),
//!   and a raw Zenoh session this test opens on `127.0.0.1:<the node's port>`,
//!   scouting and gossip off, sees it within 10 s. A node on TCP holds no Zenoh
//!   session at that port, so it cannot pass.
//! - LISTENER-NO-LINE: a `Hello`, written as one line of JSON on a plain TCP
//!   stream to the `with_listener_and_log_id` node's port, gets no
//!   `SyncRequest` line back within 5 s: the newline-JSON wire is not what an
//!   unset transport speaks any more (NW.15's `nw15_default_tcp` keeps that
//!   claim for an explicit `MOIRAI_TRANSPORT=tcp`, NW.19 ruling R-2).
//!
//! Each node runs in a thread of this process until it exits. The variables
//! are process-wide, so every test removes the same ones and sets none.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw19_default_zenoh -- --test-threads=1
//! ```
#![cfg(feature = "zenoh")]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::generic::Node;
use moirai_network::HashMap;
use moirai_protocol::log_id::LogId;
use moirai_protocol::state::po_log::VecLog;
use serde_json::{json, Value};
use zenoh::sample::{Sample, SampleKind};
use zenoh::Wait;

#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

type TestNode = Node<VecLog<AWSet<String>>>;

/// How long a raw session may take to see a node's token.
const WITNESS: Duration = Duration::from_secs(10);
/// How long the `Hello` line waits for an answer.
const NO_LINE: Duration = Duration::from_secs(5);

/// Every variable that names or shapes a transport.
const CLEARED: [&str; 8] = [
    "MOIRAI_TRANSPORT",
    "BOOTNODE_URL",
    "ZENOH_ROUTER",
    "ZENOH_MODE",
    "ZENOH_CONFIG",
    "ZENOH_SCOUTING",
    "ZENOH_REGION",
    "SESSION_ID",
];

fn clear() {
    for var in CLEARED {
        std::env::remove_var(var);
    }
}

/// A loopback port the kernel hands out, released for the node to bind (the
/// constructors that take a port number need one up front).
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR free port: {e}"))
}

/// Builds the node in a thread of its own and runs it there; the thread
/// outlives the test.
fn spawn_node(build: impl FnOnce() -> TestNode + Send + 'static) {
    thread::spawn(move || {
        let mut node = build();
        node.run();
    });
}

/// The key of a liveliness token `moirai/*/node/<id>` that a raw Zenoh
/// session (NW.15's observer, `nw15_common/zenoh_rig.rs`: peer mode, scouting
/// and gossip off), dialling only `127.0.0.1:port`, sees within `timeout`.
fn token_at(port: u16, id: &str, timeout: Duration) -> Result<String, String> {
    let session = zenoh_rig::raw_session(&[format!("tcp/127.0.0.1:{port}")]);
    let seen: Arc<Mutex<Option<String>>> = Arc::default();
    let into = Arc::clone(&seen);
    let subscriber = session
        .liveliness()
        .declare_subscriber(format!("moirai/*/node/{id}"))
        .history(true)
        .callback(move |sample: Sample| {
            if sample.kind() == SampleKind::Put {
                *into.lock().unwrap() = Some(sample.key_expr().as_str().to_string());
            }
        })
        .wait()
        .map_err(|e| format!("NW19 HARNESS ERROR liveliness subscriber: {e}"))?;
    let deadline = Instant::now() + timeout;
    let found = loop {
        if let Some(key) = seen.lock().unwrap().clone() {
            break Ok(key);
        }
        if Instant::now() > deadline {
            break Err(format!(
                "no Zenoh session at 127.0.0.1:{port} declares a token moirai/*/node/{id} \
                 within {timeout:?}"
            ));
        }
        thread::sleep(Duration::from_millis(20));
    };
    drop(subscriber);
    let _ = session.close().wait();
    found
}

fn witnessed(case: &str, port: u16, id: &str) {
    match token_at(port, id, WITNESS) {
        Ok(key) => {
            assert_eq!(
                key,
                format!("moirai/default/node/{id}"),
                "NW19 {case}: the token is not under the default session"
            );
            println!("NW19 {case} PASS port {port} token {key}");
        }
        Err(why) => panic!(
            "NW19 {case} WITNESS: with MOIRAI_TRANSPORT unset the node runs no Zenoh session at \
             its port: {why}"
        ),
    }
}

#[test]
fn nw19_new_with_the_transport_unset_runs_on_zenoh() {
    clear();
    let port = free_port();
    let id = "nw19-new";
    spawn_node(move || TestNode::new(id.to_string(), &[id], port, HashMap::default()));
    witnessed("NEW", port, id);
}

#[test]
fn nw19_new_with_log_id_with_the_transport_unset_runs_on_zenoh() {
    clear();
    let port = free_port();
    let id = "nw19-logid";
    let log = LogId::generate();
    spawn_node(move || {
        TestNode::new_with_log_id(id.to_string(), &[id], port, HashMap::default(), log)
    });
    witnessed("LOGID", port, id);
}

#[test]
fn nw19_with_listener_with_the_transport_unset_runs_on_zenoh_and_answers_no_line() {
    clear();
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR bind 127.0.0.1:0: {e}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR local addr: {e}"))
        .port();
    let id = "nw19-listener";
    let log = LogId::generate();
    spawn_node(move || {
        TestNode::with_listener_and_log_id(id.to_string(), &[id], listener, HashMap::default(), log)
    });
    witnessed("LISTENER", port, id);

    // NW.15's probe, unchanged but for its verdict: a `Hello` line.
    let mut stream = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(stream) => stream,
        Err(e) => {
            println!("NW19 LISTENER-NO-LINE PASS the port refuses a plain stream: {e}");
            return;
        }
    };
    stream
        .set_read_timeout(Some(NO_LINE))
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR read timeout: {e}"));
    let hello = json!({ "type": "Hello", "id": "nw19-probe", "metadata": null });
    let _ = writeln!(stream, "{hello}");
    let mut reader = BufReader::new(stream);
    let started = Instant::now();
    while started.elapsed() < NO_LINE {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                if let Ok(frame) = serde_json::from_str::<Value>(line.trim_end()) {
                    assert_ne!(
                        frame["type"],
                        json!("SyncRequest"),
                        "NW19 LISTENER-NO-LINE: with MOIRAI_TRANSPORT unset the node answered a \
                         Hello line with a SyncRequest line, the newline-JSON wire of TCP: {frame}"
                    );
                }
            }
        }
    }
    println!("NW19 LISTENER-NO-LINE PASS no SyncRequest line within {NO_LINE:?}");
}
