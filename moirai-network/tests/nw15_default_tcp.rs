//! NW.15: with `MOIRAI_TRANSPORT` unset a node's constructor still builds
//! today's TCP transport (vault `spec/items/NW.15.md`). Green at d996427 and
//! after: TCP stays the default and speaks the same wire.
//!
//! A node built by `Node::with_listener_and_log_id` on a listener bound to
//! `127.0.0.1:0` is sent a `Hello` as one line of JSON over a plain TCP
//! stream, and answers on that stream with a `SyncRequest` line, which is
//! what a node does today for a peer that says hello (`generic.rs`, the
//! `Hello` arm, and `direct_transport.rs`, the remap of an inbound stream).
//!
//! One test in its own binary, because it clears process variables.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_default_tcp
//! ```

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::generic::Node;
use moirai_network::HashMap;
use moirai_protocol::log_id::LogId;
use moirai_protocol::state::po_log::VecLog;
use serde_json::{json, Value};

#[test]
fn with_moirai_transport_unset_a_node_speaks_newline_json_over_tcp() {
    for var in [
        "MOIRAI_TRANSPORT",
        "ZENOH_CONFIG",
        "ZENOH_ROUTER",
        "ZENOH_MODE",
        "ZENOH_SCOUTING",
        "BOOTNODE_URL",
    ] {
        std::env::remove_var(var);
    }
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let at = listener.local_addr().expect("local addr");
    let log = LogId::generate();
    thread::spawn(move || {
        let mut node = Node::<VecLog<AWSet<String>>>::with_listener_and_log_id(
            "t".to_string(),
            &["t"],
            listener,
            HashMap::default(),
            log,
        );
        node.run();
    });

    let mut stream = TcpStream::connect(at).expect("connect to the node's listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .expect("read timeout");
    let hello = json!({ "type": "Hello", "id": "probe", "metadata": null });
    writeln!(stream, "{hello}").expect("write a Hello line");
    let mut line = String::new();
    BufReader::new(stream.try_clone().expect("clone the stream"))
        .read_line(&mut line)
        .expect("a line back from the node within 15 s");
    let frame: Value = serde_json::from_str(line.trim_end())
        .unwrap_or_else(|e| panic!("the node's answer is not one JSON line: {e}: {line:?}"));
    assert_eq!(
        frame["type"],
        json!("SyncRequest"),
        "the node answered a Hello with {frame}"
    );
}
