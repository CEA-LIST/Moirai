//! The node-level contract cases (NW.31 design row S5b). Each case is generic
//! over a [`ReplicaFactory`], builds its nodes with `make()` over
//! DirectTransport on 127.0.0.1:0, bounds every wait at 30 s and panics on a
//! violation. It assumes `inputs.ops` commute, so a converged state equals one
//! fresh replica that submitted them all.

use std::net::TcpListener;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_node::{BatchOutcome, Node};
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::transport::{CrdtTransport, PeerId, PeerInfo, TransportResult};
use serde_json::Value;

use crate::contract::Inputs;

const WAIT: Duration = Duration::from_secs(30);
const SETTLE: Duration = Duration::from_millis(400);
const MEMBERS: [&str; 3] = ["ann", "bob", "cy"];

/// Whether the replicas under a case offered a state transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transfer {
    Offered,
    NotOffered,
}

fn shared() -> LogId {
    LogId::parse("c0de0c0de0c0de0c0de0c0de0c0de0c0").expect("a log id")
}

fn other() -> LogId {
    LogId::parse("0dd50dd50dd50dd50dd50dd50dd50dd5").expect("a log id")
}

fn members() -> Vec<String> {
    MEMBERS.map(String::from).to_vec()
}

/// DirectTransport, recording every frame it hands the node.
struct Tap {
    inner: DirectTransport,
    received: Vec<(PeerId, TransportMessage)>,
}

impl CrdtTransport for Tap {
    fn local_id(&self) -> &PeerId {
        self.inner.local_id()
    }
    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        self.inner.send(peer, msg)
    }
    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        self.inner.broadcast(msg)
    }
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        let got = self.inner.try_recv()?;
        if let Some(frame) = &got {
            self.received.push(frame.clone());
        }
        Ok(got)
    }
    fn peers(&self) -> Vec<PeerInfo> {
        self.inner.peers()
    }
    fn is_connected(&self, peer: &PeerId) -> bool {
        self.inner.is_connected(peer)
    }
    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        self.inner.pause_peer(peer)
    }
    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        self.inner.resume_peer(peer)
    }
    fn pause_all(&mut self) -> TransportResult<()> {
        self.inner.pause_all()
    }
    fn resume_all(&mut self) -> TransportResult<()> {
        self.inner.resume_all()
    }
    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.inner.buffered_count(peer)
    }
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        self.inner.accept_connections()
    }
    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        self.inner.connect_to_peers()
    }
    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage> {
        let drained = self.inner.drain_buffer(peer);
        self.received
            .extend(drained.iter().map(|msg| (peer.clone(), msg.clone())));
        drained
    }
}

type Wired<F> = Node<F, Tap>;

fn bind() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local addr").to_string();
    (listener, addr)
}

fn wired<F: ReplicaFactory>(
    factory: F,
    me: &str,
    listener: TcpListener,
    known: &[(&str, String)],
) -> Wired<F> {
    let peers: HashMap<PeerId, String> = known
        .iter()
        .map(|(id, addr)| (id.to_string(), addr.clone()))
        .collect();
    let inner = DirectTransport::with_listener(me.to_string(), listener, peers)
        .expect("DirectTransport::with_listener");
    let tap = Tap {
        inner,
        received: Vec::new(),
    };
    Node::new(factory, me.to_string(), &MEMBERS, tap, shared())
}

/// Poll every node until `done` holds or `within` passes; whether it held.
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

fn settle<F: ReplicaFactory>(nodes: &mut [Wired<F>]) {
    pump(nodes, SETTLE, |_| false);
}

/// Nodes for the first `n` members, every pair dialled once and meshed; also
/// their addresses.
fn mesh<F: ReplicaFactory>(make: &mut impl FnMut() -> F, n: usize) -> (Vec<Wired<F>>, Vec<String>) {
    let ids = &MEMBERS[..n];
    let bound: Vec<(TcpListener, String)> = ids.iter().map(|_| bind()).collect();
    let addrs: Vec<String> = bound.iter().map(|(_, a)| a.clone()).collect();
    let mut nodes: Vec<Wired<F>> = bound
        .into_iter()
        .enumerate()
        .map(|(i, (listener, _))| {
            let known: Vec<(&str, String)> = (0..n)
                .filter(|j| *j != i)
                .map(|j| (ids[j], addrs[j].clone()))
                .collect();
            wired(make(), ids[i], listener, &known)
        })
        .collect();
    for i in 0..n {
        for other in &ids[i + 1..] {
            nodes[i]
                .transport_mut()
                .inner
                .connect_to(&other.to_string())
                .expect("dial a peer on loopback");
        }
    }
    let meshed = pump(&mut nodes, WAIT, |nodes| {
        nodes.iter().enumerate().all(|(i, node)| {
            (0..n).all(|j| i == j || node.transport().is_connected(&ids[j].to_string()))
        })
    });
    assert!(meshed, "the mesh did not form within {WAIT:?}");
    (nodes, addrs)
}

fn read<F: ReplicaFactory, T: CrdtTransport>(node: &Node<F, T>) -> Value {
    node.hosted(&shared())
        .expect("the shared log is hosted")
        .read_state()
}

fn delivered<F: ReplicaFactory, T: CrdtTransport>(node: &Node<F, T>) -> u64 {
    node.hosted(&shared())
        .expect("the shared log is hosted")
        .progress()
        .delivered
}

fn views<F: ReplicaFactory>(nodes: &[Wired<F>]) -> Vec<Value> {
    nodes.iter().map(read).collect()
}

/// The state of one fresh replica that submitted `ops`.
fn reference<F: ReplicaFactory>(make: &mut impl FnMut() -> F, ops: &[Value]) -> Value {
    let mut replica = make().open(shared().as_str(), "ref", &members());
    for op in ops {
        replica.submit(op.clone()).expect("a valid op is accepted");
    }
    replica.read_state()
}

/// Whether a replica of the factory that holds `ops[0]` offers a transfer.
fn probe<F: ReplicaFactory>(make: &mut impl FnMut() -> F, inputs: &Inputs) -> Transfer {
    let mut replica = make().open(shared().as_str(), "ann", &members());
    replica
        .submit(inputs.ops[0].clone())
        .expect("a valid op is accepted");
    match replica.snapshot_for("probe") {
        Some(Ok(_)) => Transfer::Offered,
        _ => Transfer::NotOffered,
    }
}

fn op<F: ReplicaFactory, T: CrdtTransport>(node: &mut Node<F, T>, op: &Value) {
    let r = node.apply_op(op.clone());
    assert!(r.success, "{op}: {}", r.message);
}

fn converge<F: ReplicaFactory>(nodes: &mut [Wired<F>], want: &Value, what: &str) {
    let done = pump(nodes, WAIT, |nodes| nodes.iter().all(|n| read(n) == *want));
    assert!(
        done,
        "{what}: want {want}, the nodes read {:?}",
        views(nodes)
    );
}

/// Frames of tag `kind` the node took in since `mark`, with their senders.
fn taken<F: ReplicaFactory>(
    node: &Wired<F>,
    mark: usize,
    kind: fn(&TransportMessage) -> bool,
) -> Vec<(PeerId, TransportMessage)> {
    node.transport().received[mark..]
        .iter()
        .filter(|(_, msg)| kind(msg))
        .cloned()
        .collect()
}

fn is_state_request(msg: &TransportMessage) -> bool {
    matches!(msg, TransportMessage::StateRequest { .. })
}
fn is_state_response(msg: &TransportMessage) -> bool {
    matches!(msg, TransportMessage::StateResponse { .. })
}
fn is_state_unavailable(msg: &TransportMessage) -> bool {
    matches!(msg, TransportMessage::StateUnavailable { .. })
}
fn is_sync_request(msg: &TransportMessage) -> bool {
    matches!(msg, TransportMessage::SyncRequest { .. })
}
fn is_state_answer(msg: &TransportMessage) -> bool {
    is_state_response(msg) || is_state_unavailable(msg)
}

/// Three nodes meshed; with no frame moving in between ann and cy submit one
/// op each and bob a two-op batch; all end equal to the reference, with the
/// same delivered count.
pub fn three_nodes_converge<F: ReplicaFactory>(mut make: impl FnMut() -> F, inputs: &Inputs) {
    let (mut nodes, _) = mesh(&mut make, 3);
    op(&mut nodes[0], &inputs.ops[0]);
    let batch = nodes[1].submit_batch(None, inputs.ops[1..3].to_vec(), None);
    assert_eq!(batch.outcome, BatchOutcome::Applied, "{}", batch.message);
    op(&mut nodes[2], &inputs.ops[3]);
    let want = reference(&mut make, &inputs.ops[..4]);
    converge(&mut nodes, &want, "three nodes converge");
    settle(&mut nodes);
    let counts: Vec<u64> = nodes.iter().map(delivered).collect();
    assert!(
        counts.iter().all(|c| *c == counts[0]),
        "the nodes count different deliveries: {counts:?}"
    );
    assert!(
        views(&nodes).iter().all(|v| *v == want),
        "a node moved after converging"
    );
}

/// Three nodes converge on one op each; ann and bob pause each other, each
/// submits, both resume; all converge. After the pause began no node took in a
/// StateRequest, and ann and bob each took in a SyncRequest from the other.
pub fn catch_up_after_pause<F: ReplicaFactory>(mut make: impl FnMut() -> F, inputs: &Inputs) {
    let (mut nodes, _) = mesh(&mut make, 3);
    for (i, node) in nodes.iter_mut().enumerate() {
        op(node, &inputs.ops[i]);
    }
    let before = reference(&mut make, &inputs.ops[..3]);
    converge(&mut nodes, &before, "converge before the pause");
    settle(&mut nodes);
    let marks: Vec<usize> = nodes.iter().map(|n| n.transport().received.len()).collect();

    for (who, peer) in [(0, "bob"), (1, "ann")] {
        let r = nodes[who].pause_peer(peer);
        assert!(r.success, "pause {peer}: {}", r.message);
    }
    op(&mut nodes[0], &inputs.ops[3]);
    op(&mut nodes[1], &inputs.ops[4]);
    for (who, peer) in [(0, "bob"), (1, "ann")] {
        let r = nodes[who].resume_peer(peer);
        assert!(r.success, "resume {peer}: {}", r.message);
    }
    let want = reference(&mut make, &inputs.ops[..5]);
    converge(&mut nodes, &want, "catch up after the pause");

    for (node, mark) in nodes.iter().zip(&marks) {
        let requests = taken(node, *mark, is_state_request);
        assert!(
            requests.is_empty(),
            "a log with history asked for a state transfer after the pause: {requests:?}"
        );
    }
    for (who, peer) in [(0, "bob"), (1, "ann")] {
        let syncs = taken(&nodes[who], marks[who], is_sync_request);
        assert!(
            syncs.iter().any(|(from, _)| from == peer),
            "{} took in no SyncRequest from {peer} after the pause",
            MEMBERS[who]
        );
    }
}

/// ann and bob write and converge; cy starts afterwards knowing both, calls
/// `connect()`, reaches their state, and its own op reaches them. Offered: cy
/// took in a StateResponse; NotOffered: none, and at least one StateUnavailable.
pub fn late_joiner<F: ReplicaFactory>(mut make: impl FnMut() -> F, inputs: &Inputs) -> Transfer {
    let offered = probe(&mut make, inputs);
    let (mut nodes, addrs) = mesh(&mut make, 2);
    op(&mut nodes[0], &inputs.ops[0]);
    op(&mut nodes[1], &inputs.ops[1]);
    let written = reference(&mut make, &inputs.ops[..2]);
    converge(&mut nodes, &written, "ann and bob converge");

    let (listener, _) = bind();
    let known = [("ann", addrs[0].clone()), ("bob", addrs[1].clone())];
    let mut cy = wired(make(), "cy", listener, &known);
    cy.connect();
    nodes.push(cy);
    let joined = pump(&mut nodes, WAIT, |n| read(&n[2]) == written);
    assert!(
        joined,
        "the late joiner did not catch up: {:?}",
        views(&nodes)
    );

    op(&mut nodes[2], &inputs.ops[2]);
    let want = reference(&mut make, &inputs.ops[..3]);
    converge(&mut nodes, &want, "the joiner's op reaches ann and bob");

    let responses = taken(&nodes[2], 0, is_state_response);
    let refusals = taken(&nodes[2], 0, is_state_unavailable);
    match offered {
        Transfer::Offered => assert!(
            responses
                .iter()
                .any(|(from, _)| from == "ann" || from == "bob"),
            "a transfer is offered and the joiner took in no StateResponse: {refusals:?}"
        ),
        Transfer::NotOffered => {
            assert!(
                responses.is_empty(),
                "no transfer is offered and the joiner took in one"
            );
            assert!(
                !refusals.is_empty(),
                "the joiner never asked for a transfer"
            );
        }
    }
    offered
}

/// ann hosts a second log bob does not; ann's write to it is filtered at bob,
/// counted and remembered, and bob's shared log is unchanged.
pub fn foreign_log_filtered<F: ReplicaFactory>(mut make: impl FnMut() -> F, inputs: &Inputs) {
    let (mut nodes, _) = mesh(&mut make, 2);
    nodes[0].host_log(other()).expect("a fresh id");
    let before = (read(&nodes[1]), delivered(&nodes[1]));
    let filtered = nodes[1].frames_not_hosted();
    let r = nodes[0].apply_op_to(&other(), inputs.ops[0].clone());
    assert!(r.success, "{}", r.message);
    let grew = pump(&mut nodes, WAIT, |n| n[1].frames_not_hosted() > filtered);
    assert!(
        grew,
        "bob did not filter the write to a log it does not host"
    );
    settle(&mut nodes);
    let bob = &nodes[1];
    assert!(!bob.hosts(&other()), "a frame made bob host a log");
    assert!(
        bob.seen_not_hosted().any(|id| *id == other()),
        "bob does not remember the log it filtered"
    );
    assert_eq!(
        (read(bob), delivered(bob)),
        before,
        "the filtered write changed bob's shared log"
    );
}

/// ann and bob write and converge; a StateRequest naming bob, sent on bob's
/// link, is refused naming bob as a returning member and changes nothing. A
/// fresh id on the same link is served when a transfer is offered, and refused
/// for another cause when not.
pub fn returning_member_refused<F: ReplicaFactory>(
    mut make: impl FnMut() -> F,
    inputs: &Inputs,
) -> Transfer {
    let offered = probe(&mut make, inputs);
    let (mut nodes, _) = mesh(&mut make, 2);
    op(&mut nodes[0], &inputs.ops[0]);
    op(&mut nodes[1], &inputs.ops[1]);
    let written = reference(&mut make, &inputs.ops[..2]);
    converge(&mut nodes, &written, "ann and bob converge");
    settle(&mut nodes);
    let before: Vec<(Value, u64)> = nodes.iter().map(|n| (read(n), delivered(n))).collect();

    let ask = |nodes: &mut Vec<Wired<F>>, id: &str| -> TransportMessage {
        let mark = nodes[1].transport().received.len();
        let request = TransportMessage::StateRequest {
            id: id.to_string(),
            log_id: shared(),
        };
        nodes[1]
            .transport_mut()
            .send(&"ann".to_string(), request)
            .expect("send on bob's link");
        let answered = pump(nodes, WAIT, |n| {
            !taken(&n[1], mark, is_state_answer).is_empty()
        });
        assert!(answered, "ann did not answer the state request naming {id}");
        settle(nodes);
        let answers = taken(&nodes[1], mark, is_state_answer);
        assert_eq!(
            answers.len(),
            1,
            "one answer to one state request: {answers:?}"
        );
        answers[0].1.clone()
    };

    match ask(&mut nodes, "bob") {
        TransportMessage::StateUnavailable { reason, .. } => assert!(
            reason.contains("bob") && reason.contains("returning member"),
            "the refusal does not name bob as a returning member: {reason}"
        ),
        other => panic!("a returning member was answered {other:?}"),
    }
    let after: Vec<(Value, u64)> = nodes.iter().map(|n| (read(n), delivered(n))).collect();
    assert_eq!(after, before, "refusing a returning member changed a node");

    let fresh = ask(&mut nodes, "dee");
    match (offered, &fresh) {
        (Transfer::Offered, TransportMessage::StateResponse { .. }) => {}
        (Transfer::NotOffered, TransportMessage::StateUnavailable { reason, .. })
            if !reason.contains("returning member") => {}
        _ => panic!("a fresh id was answered {fresh:?} with the transfer {offered:?}"),
    }
    let after: Vec<(Value, u64)> = nodes.iter().map(|n| (read(n), delivered(n))).collect();
    assert_eq!(after, before, "answering a fresh id changed a node");
    offered
}

/// ann reads its version; bob's op reaches ann; ann's batch against the old
/// version is stale, applies nothing and sends nothing; against the fresh
/// version it applies and reaches bob.
pub fn batch_stale_refusal<F: ReplicaFactory>(mut make: impl FnMut() -> F, inputs: &Inputs) {
    let (mut nodes, _) = mesh(&mut make, 2);
    let read_at = nodes[0].version_of(&shared()).expect("hosted");
    op(&mut nodes[1], &inputs.ops[0]);
    let moved = reference(&mut make, &inputs.ops[..1]);
    converge(&mut nodes, &moved, "bob's op reaches ann");

    let batch = inputs.ops[1..3].to_vec();
    let stale = nodes[0].submit_batch(None, batch.clone(), Some(&read_at));
    assert_eq!(
        (stale.outcome, stale.applied),
        (BatchOutcome::Stale, 0),
        "{}",
        stale.message
    );
    settle(&mut nodes);
    assert!(
        views(&nodes).iter().all(|v| *v == moved),
        "a stale batch wrote: {:?}",
        views(&nodes)
    );

    let fresh = nodes[0].version_of(&shared()).expect("hosted");
    let applied = nodes[0].submit_batch(None, batch, Some(&fresh));
    assert_eq!(
        (applied.outcome, applied.applied),
        (BatchOutcome::Applied, 2),
        "{}",
        applied.message
    );
    let want = reference(&mut make, &inputs.ops[..3]);
    converge(&mut nodes, &want, "the fresh batch reaches bob");
}
