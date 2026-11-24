//! NW.31f oracle: moirai-netstack-node's catch-up and transfer (vault
//! spec/items/NW.31f.md, NW.31 design row S5b), on the toy replica.
//!
//! 601c887's generic.rs holds no catch-up or transfer test beyond the 17 that
//! S5a ported; 601c887's node-level ones live in moirai-network/tests, and four
//! are ported here by name: z2 and z3 (nw15_common/nodes.rs) and s6 and t6
//! (e2e_convergence.rs). The toy is a grow-only set, so z2's remove is dropped
//! and its hand-computed sets follow from that. z3's pruned outbox is made by
//! hand: the oracle's OfferingFactory stops carrying the pruned elements in a
//! delta, so only a state transfer can bring them. Four more tests pin the
//! donor's refusal order, adoption by a log with no history only, the count of
//! frames with no usable log id, and the replica's refusal reason kept per log.
//!
//! flock ~/.cache/cea-crdt-scratch/cargo.lock flock ~/.cache/cea-crdt-scratch/nodes.lock \
//!   nice -n 10 cargo test -j 2 -p moirai-netstack-node --locked --test nw31f_node

#[path = "../../tests/nw31f/common.rs"]
mod common;

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use common::{OfferingFactory, Tap};
use moirai_netstack::{LogId, LogPayload, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_node::Node;
use moirai_netstack_testkit::ToyFactory;
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::transport::{CrdtTransport, PeerId, PeerInfo, TransportResult};
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(30);
const HOLD: Duration = Duration::from_millis(400);

fn log() -> LogId {
    LogId::parse("e5f6e5f6e5f6e5f6e5f6e5f6e5f6e5f6").unwrap()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn union(a: &BTreeSet<String>, more: &[&str]) -> BTreeSet<String> {
    a.iter()
        .cloned()
        .chain(more.iter().map(|s| s.to_string()))
        .collect()
}

/// The elements of the node's default log, sorted.
fn state<F: ReplicaFactory, T: CrdtTransport>(node: &Node<F, T>) -> BTreeSet<String> {
    let read = node
        .hosted(node.log_id())
        .expect("the default log is hosted")
        .read_state();
    read.as_array()
        .unwrap_or_else(|| panic!("the toy reads out an array, got {read}"))
        .iter()
        .map(|e| e.as_str().expect("string elements").to_string())
        .collect()
}

fn add<F: ReplicaFactory, T: CrdtTransport>(node: &mut Node<F, T>, element: &str) {
    let r = node.apply_op(json!(element));
    assert!(r.success, "add {element}: {}", r.message);
}

// ---- in-process nodes with no network ---------------------------------------

/// Records everything sent and broadcast, and receives nothing.
struct RecordingTransport {
    id: PeerId,
    sent: Vec<(PeerId, TransportMessage)>,
    broadcast: Vec<TransportMessage>,
}

impl CrdtTransport for RecordingTransport {
    fn local_id(&self) -> &PeerId {
        &self.id
    }
    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        self.sent.push((peer.clone(), msg));
        Ok(())
    }
    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        self.broadcast.push(msg);
        Ok(())
    }
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        Ok(None)
    }
    fn peers(&self) -> Vec<PeerInfo> {
        Vec::new()
    }
    fn is_connected(&self, _peer: &PeerId) -> bool {
        false
    }
    fn pause_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        Ok(())
    }
    fn resume_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        Ok(())
    }
    fn pause_all(&mut self) -> TransportResult<()> {
        Ok(())
    }
    fn resume_all(&mut self) -> TransportResult<()> {
        Ok(())
    }
    fn buffered_count(&self, _peer: &PeerId) -> usize {
        0
    }
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(Vec::new())
    }
}

fn recorded<F: ReplicaFactory>(factory: F, me: &str) -> Node<F, RecordingTransport> {
    let transport = RecordingTransport {
        id: me.to_string(),
        sent: Vec::new(),
        broadcast: Vec::new(),
    };
    Node::new(factory, me.to_string(), &[me], transport, log())
}

/// Hand every frame `from` broadcast since the last call to `to`.
fn carry<F: ReplicaFactory, G: ReplicaFactory>(
    from: &mut Node<F, RecordingTransport>,
    to: &mut Node<G, RecordingTransport>,
) {
    let sender = from.transport().id.clone();
    for frame in std::mem::take(&mut from.transport_mut().broadcast) {
        to.handle_transport_message(sender.clone(), frame);
    }
}

/// The one frame `donor` sends in answer to a state request from `requester`.
fn answer_to<F: ReplicaFactory>(
    donor: &mut Node<F, RecordingTransport>,
    requester: &str,
) -> (PeerId, TransportMessage) {
    donor.transport_mut().sent.clear();
    donor.handle_transport_message(
        requester.to_string(),
        TransportMessage::StateRequest {
            id: requester.to_string(),
            log_id: log(),
        },
    );
    let mut sent = std::mem::take(&mut donor.transport_mut().sent);
    assert_eq!(sent.len(), 1, "one answer to one state request: {sent:?}");
    sent.pop().unwrap()
}

/// The answer is a refusal to `requester`, stamped with the log, naming `cause`.
fn expect_unavailable(answer: (PeerId, TransportMessage), requester: &str, cause: &str) -> String {
    let (to, msg) = answer;
    assert_eq!(to, requester, "the answer went to someone else");
    match msg {
        TransportMessage::StateUnavailable { reason, log_id } => {
            assert_eq!(
                log_id,
                log(),
                "the refusal is stamped with the log asked for"
            );
            assert!(
                reason.contains(cause),
                "expected a refusal naming `{cause}`, got: {reason}"
            );
            reason
        }
        other => panic!("expected StateUnavailable naming `{cause}`, got {other:?}"),
    }
}

/// Whether `node` sent `to` a SyncRequest for the log since the last call.
fn sent_sync_request<F: ReplicaFactory>(node: &mut Node<F, RecordingTransport>, to: &str) -> bool {
    std::mem::take(&mut node.transport_mut().sent)
        .iter()
        .any(|(peer, msg)| {
            peer == to
                && matches!(msg, TransportMessage::SyncRequest { since }
                    if since.get("log_id").and_then(Value::as_str) == Some(log().as_str()))
        })
}

/// A snapshot as the oracle's offering toy gives one, of a replica holding `elements`.
fn snapshot_of(elements: &[&str]) -> Value {
    let mut replica = OfferingFactory::default().open(log().as_str(), "dee", &["dee".to_string()]);
    for element in elements {
        replica.submit(json!(element)).expect("a string element");
    }
    replica
        .snapshot_for("joe")
        .expect("the offering toy offers a transfer")
        .expect("a snapshot")
}

fn state_response(snapshot: Value) -> TransportMessage {
    TransportMessage::StateResponse {
        snapshot,
        log: LogPayload::Plain(Value::Null),
        log_id: log(),
    }
}

// ---- nodes over DirectTransport on 127.0.0.1 -------------------------------

type Wired<F> = Node<F, Tap<DirectTransport>>;

fn bind() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local addr").to_string();
    (listener, addr)
}

fn wired<F: ReplicaFactory>(
    factory: F,
    me: &str,
    members: &[&str],
    listener: TcpListener,
    known: &[(&str, &str)],
) -> Wired<F> {
    let peers: HashMap<PeerId, String> = known
        .iter()
        .map(|(id, addr)| (id.to_string(), addr.to_string()))
        .collect();
    let transport = DirectTransport::with_listener(me.to_string(), listener, peers)
        .expect("DirectTransport::with_listener");
    Node::new(factory, me.to_string(), members, Tap::new(transport), log())
}

fn pump<F: ReplicaFactory>(
    nodes: &mut [Wired<F>],
    within: Duration,
    done: impl Fn(&[Wired<F>]) -> bool,
) -> bool {
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

fn views<F: ReplicaFactory>(nodes: &[Wired<F>]) -> Vec<BTreeSet<String>> {
    nodes.iter().map(state).collect()
}

/// Nodes for `ids`, each knowing every other's address, every pair dialled
/// once (the lower index dials), meshed. Also the addresses, by index.
fn mesh<F: ReplicaFactory>(
    mut make: impl FnMut() -> F,
    ids: &[&str],
) -> (Vec<Wired<F>>, Vec<String>) {
    let bound: Vec<(TcpListener, String)> = ids.iter().map(|_| bind()).collect();
    let addrs: Vec<String> = bound.iter().map(|(_, a)| a.clone()).collect();
    let mut nodes: Vec<Wired<F>> = bound
        .into_iter()
        .enumerate()
        .map(|(i, (listener, _))| {
            let known: Vec<(&str, &str)> = ids
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(j, other)| (*other, addrs[j].as_str()))
                .collect();
            wired(make(), ids[i], ids, listener, &known)
        })
        .collect();
    for i in 0..ids.len() {
        for other in &ids[i + 1..] {
            nodes[i]
                .transport_mut()
                .inner
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
    (nodes, addrs)
}

fn pause<F: ReplicaFactory>(node: &mut Wired<F>, peer: &str) {
    let r = node.pause_peer(peer);
    assert!(r.success, "pause {peer}: {}", r.message);
}

fn resume<F: ReplicaFactory>(node: &mut Wired<F>, peer: &str) {
    let r = node.resume_peer(peer);
    assert!(r.success, "resume {peer}: {}", r.message);
}

/// The count after the last dot of a version, `<incarnation>.<delivered>`.
fn count(version: &str) -> u64 {
    version.rsplit('.').next().unwrap().parse().unwrap()
}

// ---- 601c887's node-level catch-up and transfer tests, by name -------------

/// Z2 (601c887 nw15_common/nodes.rs): three nodes converge, are partitioned,
/// heal and converge, then a one-sided partition holds and heals. The toy has
/// no remove, so c's remove of b1 is dropped and b1 stays.
#[test]
fn z2_three_nodes_converge_partition_heal_converge() {
    let (mut nodes, _) = mesh(ToyFactory::default, &["a", "b", "c"]);
    let all =
        |nodes: &[Wired<ToyFactory>], s: &BTreeSet<String>| nodes.iter().all(|n| state(n) == *s);

    // P1. One add each.
    add(&mut nodes[0], "a1");
    add(&mut nodes[1], "b1");
    add(&mut nodes[2], "c1");
    let p1 = set(&["a1", "b1", "c1"]);
    assert!(
        pump(&mut nodes, WAIT, |n| all(n, &p1)),
        "Z2 P1, converge: {:?}",
        views(&nodes)
    );

    // P2. {a, b} | {c}, both sides pausing; each writes.
    pause(&mut nodes[0], "c");
    pause(&mut nodes[1], "c");
    pause(&mut nodes[2], "a");
    pause(&mut nodes[2], "b");
    add(&mut nodes[0], "a2");
    add(&mut nodes[1], "b2");
    add(&mut nodes[2], "c2");
    let side_ab = set(&["a1", "b1", "c1", "a2", "b2"]);
    let side_c = set(&["a1", "b1", "c1", "c2"]);
    let split = |n: &[Wired<ToyFactory>]| {
        state(&n[0]) == side_ab && state(&n[1]) == side_ab && state(&n[2]) == side_c
    };
    assert!(
        pump(&mut nodes, WAIT, split),
        "Z2 P2, during the partition: {:?}",
        views(&nodes)
    );
    pump(&mut nodes, HOLD, |_| false);
    assert!(split(&nodes), "Z2 P2, a moment later: {:?}", views(&nodes));

    // P3. Heal: the union everywhere.
    resume(&mut nodes[0], "c");
    resume(&mut nodes[1], "c");
    resume(&mut nodes[2], "a");
    resume(&mut nodes[2], "b");
    let p3 = set(&["a1", "a2", "b1", "b2", "c1", "c2"]);
    assert!(
        pump(&mut nodes, WAIT, |n| all(n, &p3)),
        "Z2 P3, converge after the heal: {:?}",
        views(&nodes)
    );

    // P4. a pauses b; b does not pause a. a's add must not reach b, b's add
    // waits in a's buffer, and c, paused by nobody, gets both.
    pause(&mut nodes[0], "b");
    add(&mut nodes[0], "a3");
    add(&mut nodes[1], "b3");
    let everything = union(&p3, &["a3", "b3"]);
    assert!(
        pump(&mut nodes, WAIT, |n| state(&n[2]) == everything),
        "Z2 P4, c during the one-sided partition: {:?}",
        views(&nodes)
    );
    pump(&mut nodes, HOLD, |_| false);
    assert_eq!(
        state(&nodes[0]),
        union(&p3, &["a3"]),
        "Z2 P4, a during the one-sided partition"
    );
    assert_eq!(
        state(&nodes[1]),
        union(&p3, &["b3"]),
        "Z2 P4, b during the one-sided partition"
    );
    assert!(
        nodes[0].transport().buffered_count(&"b".to_string()) >= 1,
        "Z2 P4: b3 is not waiting in a's buffer"
    );

    // P5. a resumes b and takes b3 from its buffer; b asks a for what it
    // missed by a pause and resume of a.
    resume(&mut nodes[0], "b");
    pause(&mut nodes[1], "a");
    resume(&mut nodes[1], "a");
    assert!(
        pump(&mut nodes, WAIT, |n| all(n, &everything)),
        "Z2 P5, converge after the one-sided partition: {:?}",
        views(&nodes)
    );
}

/// Z3 (601c887 nw15_common/nodes.rs): a and b write 21 adds in three rounds,
/// and the delta stops carrying them (pruned by hand); c then joins knowing a
/// and b, with no history. It must receive a StateResponse, adopt it and read
/// all 21; its own add then reaches a and b.
#[test]
fn z3_a_late_joiner_adopts_by_state_transfer_and_converges() {
    let factory = OfferingFactory::default();
    let (mut nodes, addrs) = mesh(|| factory.clone(), &["a", "b"]);

    let es: Vec<String> = (0..10).map(|i| format!("e{i}")).collect();
    let fs: Vec<String> = (0..10).map(|i| format!("f{i}")).collect();
    for e in &es {
        add(&mut nodes[0], e);
    }
    let round1: BTreeSet<String> = es.iter().cloned().collect();
    assert!(
        pump(&mut nodes, WAIT, |n| state(&n[1]) == round1),
        "Z3, b has a's ten adds"
    );
    for f in &fs {
        add(&mut nodes[1], f);
    }
    let round2: BTreeSet<String> = es.iter().chain(&fs).cloned().collect();
    assert!(
        pump(&mut nodes, WAIT, |n| state(&n[0]) == round2),
        "Z3, a has b's ten adds"
    );
    add(&mut nodes[0], "e10");
    let history = union(&round2, &["e10"]);
    assert!(
        pump(&mut nodes, WAIT, |n| state(&n[1]) == history),
        "Z3, b has e10"
    );
    // Precondition: a delta no longer carries any of them.
    factory.pruned.borrow_mut().extend(history.iter().cloned());

    let (listener, _) = bind();
    let mut c = wired(
        factory.clone(),
        "c",
        &["c", "a", "b"],
        listener,
        &[("a", addrs[0].as_str()), ("b", addrs[1].as_str())],
    );
    c.connect();
    nodes.push(c);
    let adopted = pump(&mut nodes, WAIT, |n| state(&n[2]) == history);
    assert!(
        adopted,
        "Z3, the joiner never adopted the donors' state: it reads {:?}",
        state(&nodes[2])
    );
    let responses = nodes[2].transport().senders_of("StateResponse");
    assert!(
        responses.iter().any(|from| from == "a" || from == "b"),
        "Z3: the joiner reached the history without a StateResponse from a or b: {:?}",
        nodes[2].transport().received
    );
    let version = nodes[2].version_of(&log()).expect("hosted");
    assert!(
        count(&version) >= 21,
        "Z3: the joiner counts fewer than 21 delivered: {version}"
    );

    add(&mut nodes[2], "g0");
    let after = union(&history, &["g0"]);
    assert!(
        pump(&mut nodes, WAIT, |n| n.iter().all(|x| state(x) == after)),
        "Z3, the joiner's add converges: {:?}",
        views(&nodes)
    );
}

/// S6 (601c887 e2e_convergence.rs): ann runs alone and writes; bob starts
/// afterwards knowing only ann, so it is only ever the dialer, and must catch
/// up from nothing. No transfer is on offer, so the delta brings it.
#[test]
fn s6_late_joiner_catches_up() {
    let (listener, addr) = bind();
    let ann = wired(ToyFactory::default(), "ann", &["ann", "bob"], listener, &[]);
    let mut nodes = vec![ann];
    for e in ["name-Bo", "age-30", "city-P"] {
        add(&mut nodes[0], e);
    }
    let written = set(&["name-Bo", "age-30", "city-P"]);

    let (listener, _) = bind();
    let mut bob = wired(
        ToyFactory::default(),
        "bob",
        &["ann", "bob"],
        listener,
        &[("ann", addr.as_str())],
    );
    bob.connect();
    nodes.push(bob);
    assert!(
        pump(&mut nodes, WAIT, |n| state(&n[1]) == written),
        "S6: the late joiner did not catch up: {:?}",
        views(&nodes)
    );
    add(&mut nodes[1], "late");
    let after = union(&written, &["late"]);
    assert!(
        pump(&mut nodes, WAIT, |n| n.iter().all(|x| state(x) == after)),
        "S6: the joiner's write did not reach ann: {:?}",
        views(&nodes)
    );
}

/// T6 (601c887 e2e_convergence.rs): a StateRequest from a replica the donor
/// already has history from is refused, while a fresh id is served by the same
/// donor; answering changes nothing at the donor.
#[test]
fn t6_a_returning_member_is_refused_a_state_transfer() {
    let mut donor = recorded(OfferingFactory::default(), "ann");
    let mut bob = recorded(OfferingFactory::default(), "bob");
    add(&mut donor, "name-B");
    add(&mut bob, "city-P");
    carry(&mut bob, &mut donor);
    assert_eq!(
        state(&donor),
        set(&["city-P", "name-B"]),
        "set-up: the donor has bob's write"
    );
    let before = (state(&donor), donor.version_of(&log()));

    let (to, fresh) = answer_to(&mut donor, "zz");
    assert_eq!(to, "zz");
    assert!(
        matches!(&fresh, TransportMessage::StateResponse { log_id, .. } if *log_id == log()),
        "the donor refused a genuinely fresh joiner, so the refusal below proves nothing: {fresh:?}"
    );
    let reason = expect_unavailable(answer_to(&mut donor, "bob"), "bob", "returning member");
    assert!(
        reason.contains("bob"),
        "the refusal does not name the returning member: {reason}"
    );
    assert_eq!(
        (state(&donor), donor.version_of(&log())),
        before,
        "answering a state request changed the donor"
    );
}

// ---- the donor's order, adoption, malformed frames, refusal reasons --------

/// 601c887's order: no history first, then a returning member, then whether
/// the replica offers a transfer; every refusal stamped with the log.
#[test]
fn a_donor_refuses_in_601c887s_order_and_stamps_its_log() {
    let mut empty = recorded(OfferingFactory::default(), "ann");
    expect_unavailable(answer_to(&mut empty, "zz"), "zz", "no history");

    let mut plain = recorded(ToyFactory::default(), "ann");
    add(&mut plain, "x");
    expect_unavailable(answer_to(&mut plain, "zz"), "zz", "not offered");

    let mut bob = recorded(ToyFactory::default(), "bob");
    add(&mut bob, "y");
    carry(&mut bob, &mut plain);
    let reason = expect_unavailable(answer_to(&mut plain, "bob"), "bob", "returning member");
    assert!(
        reason.contains("bob"),
        "the refusal does not name the returning member: {reason}"
    );
}

/// A log with no history adopts a StateResponse and then asks its donor for a
/// delta; a log with history never adopts one (a second donor's answer must
/// not undo the first) and asks for a delta instead.
#[test]
fn a_state_response_is_adopted_only_by_a_log_with_no_history() {
    let mut joiner = recorded(OfferingFactory::default(), "joe");
    joiner.handle_transport_message("dee".to_string(), state_response(snapshot_of(&["x", "y"])));
    assert_eq!(
        state(&joiner),
        set(&["x", "y"]),
        "the joiner did not adopt the snapshot"
    );
    assert!(
        sent_sync_request(&mut joiner, "dee"),
        "after adopting, the joiner did not ask its donor for a delta"
    );

    joiner.handle_transport_message(
        "eve".to_string(),
        state_response(snapshot_of(&["x", "y", "z"])),
    );
    assert_eq!(
        state(&joiner),
        set(&["x", "y"]),
        "a log that already has history adopted a second snapshot"
    );
    assert!(
        sent_sync_request(&mut joiner, "eve"),
        "a log with history did not answer a snapshot with a delta request"
    );
}

/// An Event, Batch or SyncRequest whose body has no log_id, or one that is not
/// a log id, is counted, not dropped silently, and changes nothing.
#[test]
fn inbound_frames_with_no_usable_log_id_are_counted_not_dropped_silently() {
    let mut node = recorded(ToyFactory::default(), "n");
    let frames = [
        TransportMessage::Event {
            event: json!({ "log": log().as_str(), "origin": "p", "op": "x" }),
        },
        TransportMessage::Batch {
            batch: json!({ "log_id": "not a log id", "events": [] }),
        },
        TransportMessage::SyncRequest { since: json!([]) },
    ];
    for frame in frames {
        node.handle_transport_message("p".to_string(), frame);
    }
    let metrics = node.metrics();
    assert_eq!(metrics["frames_malformed"].as_u64(), Some(3), "{metrics}");
    assert_eq!(
        metrics["frames_not_hosted"].as_u64(),
        Some(0),
        "a frame with no usable log id is not one for an unhosted log: {metrics}"
    );
    assert_eq!(state(&node), set(&[]), "a malformed frame was applied");
    assert!(
        node.transport().sent.is_empty(),
        "a malformed SyncRequest was answered"
    );

    let mut writer = recorded(ToyFactory::default(), "w");
    add(&mut writer, "y");
    carry(&mut writer, &mut node);
    assert_eq!(
        state(&node),
        set(&["y"]),
        "a well-formed event was not applied"
    );
    assert_eq!(
        node.metrics()["frames_malformed"].as_u64(),
        Some(3),
        "a well-formed event was counted"
    );
}

/// The replica's own reason for refusing remote input is kept on the log.
#[test]
fn the_replicas_refusal_reason_is_kept_on_the_log() {
    let mut node = recorded(ToyFactory::default(), "n");
    let fresh = node.log_metrics(&log()).expect("hosted");
    assert_eq!(
        fresh["last_refusal"],
        Value::Null,
        "a refusal before any input: {fresh}"
    );

    node.handle_transport_message(
        "p".to_string(),
        TransportMessage::Event {
            event: json!({ "log_id": log().as_str(), "origin": "p" }),
        },
    );
    let metrics = node.log_metrics(&log()).expect("hosted");
    assert_eq!(
        metrics["foreign_log_refusals"].as_u64(),
        Some(1),
        "{metrics}"
    );
    let reason = metrics["last_refusal"]
        .as_str()
        .unwrap_or_else(|| panic!("no last_refusal kept: {metrics}"));
    assert!(
        reason.contains("malformed event"),
        "the toy's own reason is not kept: {reason}"
    );
}
