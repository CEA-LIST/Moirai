//! NW.31f oracle: the node-level contract cases on upstream Moirai 7fe5ea2
//! replicas through moirai-netstack-upstream (vault spec/items/NW.31f.md, NW.31
//! design row S5b). Upstream replicas offer no state transfer, so the two
//! transfer cases must report it not offered and catch up by delta. One more
//! test drives three nodes with concurrent adds and removes and a two-sided
//! pause to a hand-computed add-wins state.
//!
//! Sockets on 127.0.0.1:0; run under nodes.lock (tests/nw31f/gate.sh).

use std::net::TcpListener;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, ReplicaPort};
use moirai_netstack_node::Node;
use moirai_netstack_testkit::contract::Inputs;
use moirai_netstack_testkit::contract_node::{self, Transfer};
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::transport::{CrdtTransport, PeerId};
use moirai_netstack_upstream::{Factory, awset};
use serde_json::{Value, json};

fn add(s: &str) -> Value {
    json!({ "Add": s })
}
fn remove(s: &str) -> Value {
    json!({ "Remove": s })
}

fn inputs() -> Inputs {
    Inputs {
        ops: ["red", "green", "blue", "amber", "teal", "plum"]
            .map(add)
            .to_vec(),
        refused_op: json!({ "Add": 7 }),
        malformed_remote: json!({ "nw31f": "not a payload" }),
    }
}

// ---- the six cases through the adapter ---------------------------------------

#[test]
fn upstream_passes_three_nodes_converge() {
    contract_node::three_nodes_converge(awset::factory, &inputs());
}
#[test]
fn upstream_passes_catch_up_after_pause() {
    contract_node::catch_up_after_pause(awset::factory, &inputs());
}
#[test]
fn upstream_passes_late_joiner_with_transfer_not_offered() {
    assert_eq!(
        contract_node::late_joiner(awset::factory, &inputs()),
        Transfer::NotOffered
    );
}
#[test]
fn upstream_passes_foreign_log_filtered() {
    contract_node::foreign_log_filtered(awset::factory, &inputs());
}
#[test]
fn upstream_passes_returning_member_refused_with_transfer_not_offered() {
    assert_eq!(
        contract_node::returning_member_refused(awset::factory, &inputs()),
        Transfer::NotOffered
    );
}
#[test]
fn upstream_passes_batch_stale_refusal() {
    contract_node::batch_stale_refusal(awset::factory, &inputs());
}

// ---- three upstream nodes, concurrent ops and a pause, add-wins by hand -----

type UpNode = Node<Factory<awset::Log>, DirectTransport>;

const WAIT: Duration = Duration::from_secs(30);
const HOLD: Duration = Duration::from_millis(400);

fn read(node: &UpNode) -> Value {
    node.hosted(node.log_id())
        .expect("the default log is hosted")
        .read_state()
}

fn op(node: &mut UpNode, op: Value) {
    let r = node.apply_op(op.clone());
    assert!(r.success, "{op}: {}", r.message);
}

fn pump(nodes: &mut [UpNode], within: Duration, done: impl Fn(&[UpNode]) -> bool) -> bool {
    let deadline = Instant::now() + within;
    loop {
        for node in nodes.iter_mut() {
            node.poll();
        }
        if done(nodes) {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn views(nodes: &[UpNode]) -> Vec<Value> {
    nodes.iter().map(read).collect()
}

/// ann, bob and cy, all members from the start, each knowing every address;
/// ann adds red and all converge. Then, with no frame moving in between, ann
/// removes red while bob adds it again (concurrent: add wins), cy adds teal and
/// bob green. Then ann and cy pause each other: ann removes teal (it saw cy's
/// add), cy adds plum; bob, paused by nobody, sees both, while ann and cy each
/// miss the other's write until they resume and ask for a delta.
#[test]
fn three_upstream_nodes_converge_add_wins_under_concurrent_ops_and_a_pause() {
    let ids = ["ann", "bob", "cy"];
    let log = LogId::parse("f7a8f7a8f7a8f7a8f7a8f7a8f7a8f7a8").unwrap();
    let listeners: Vec<TcpListener> = ids
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0"))
        .collect();
    let addrs: Vec<String> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();
    let mut nodes: Vec<UpNode> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let peers: HashMap<PeerId, String> = ids
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(j, other)| (other.to_string(), addrs[j].clone()))
                .collect();
            let transport = DirectTransport::with_listener(ids[i].to_string(), listener, peers)
                .expect("DirectTransport::with_listener");
            Node::new(
                awset::factory(),
                ids[i].to_string(),
                &ids,
                transport,
                log.clone(),
            )
        })
        .collect();
    for i in 0..ids.len() {
        for other in &ids[i + 1..] {
            nodes[i]
                .transport_mut()
                .connect_to(&other.to_string())
                .expect("dial a peer on loopback");
        }
    }
    let meshed = pump(&mut nodes, WAIT, |nodes| {
        nodes.iter().enumerate().all(|(i, n)| {
            ids.iter()
                .enumerate()
                .all(|(j, other)| i == j || n.transport().is_connected(&other.to_string()))
        })
    });
    assert!(meshed, "the mesh did not form within {WAIT:?}");
    let all = |nodes: &[UpNode], v: &Value| nodes.iter().all(|n| read(n) == *v);

    op(&mut nodes[0], add("red"));
    let p1 = json!(["red"]);
    assert!(
        pump(&mut nodes, WAIT, |n| all(n, &p1)),
        "P1: {:?}",
        views(&nodes)
    );

    op(&mut nodes[0], remove("red"));
    op(&mut nodes[1], add("red"));
    op(&mut nodes[2], add("teal"));
    op(&mut nodes[1], add("green"));
    let p2 = json!(["green", "red", "teal"]);
    assert!(
        pump(&mut nodes, WAIT, |n| all(n, &p2)),
        "P2, bob's concurrent add of red must win over ann's remove: {:?}",
        views(&nodes)
    );

    for (who, peer) in [(0, "cy"), (2, "ann")] {
        let r = nodes[who].pause_peer(peer);
        assert!(r.success, "pause {peer}: {}", r.message);
    }
    op(&mut nodes[0], remove("teal"));
    op(&mut nodes[2], add("plum"));
    let ann = json!(["green", "red"]);
    let bob = json!(["green", "plum", "red"]);
    let cy = json!(["green", "plum", "red", "teal"]);
    let split = |n: &[UpNode]| read(&n[0]) == ann && read(&n[1]) == bob && read(&n[2]) == cy;
    assert!(
        pump(&mut nodes, WAIT, split),
        "P3, during the pause: {:?}",
        views(&nodes)
    );
    pump(&mut nodes, HOLD, |_| false);
    assert!(split(&nodes), "P3, a moment later: {:?}", views(&nodes));

    for (who, peer) in [(0, "cy"), (2, "ann")] {
        let r = nodes[who].resume_peer(peer);
        assert!(r.success, "resume {peer}: {}", r.message);
    }
    assert!(
        pump(&mut nodes, WAIT, |n| all(n, &bob)),
        "P4, ann and cy catch up after the pause: {:?}",
        views(&nodes)
    );
}
