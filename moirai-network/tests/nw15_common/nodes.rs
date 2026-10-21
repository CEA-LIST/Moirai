//! NW.15, points Z2 and Z3 (vault `spec/items/NW.15.md`): real `GenericNode`s
//! in one process, each on its own thread running the node's own event loop
//! (`GenericNode::run`), driven and read through the node's own HTTP API on
//! `127.0.0.1:0`, exactly as a harness drives `model_node` processes.
//!
//! The scenarios are written once over a [`Net`], the transports the nodes
//! run on: `nw15_nodes_tcp.rs` runs them over today's `CompositeTransport`
//! (green at d996427, so every expected state below is shown to be the
//! node's behaviour today), `nw15_nodes_zenoh.rs` over `AnyTransport::Zenoh`.
//!
//! The log is an add-wins set of strings (`AWSet` in a `VecLog`, as the
//! `awset_node` example has it), and every expected state is computed by
//! hand from the operations each scenario applies, written beside them.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::generic::GenericNode;
use moirai_network::query::QueryableLog;
use moirai_network::transport::{
    CrdtTransport, PeerId, PeerInfo, TransportError, TransportMessage, TransportResult,
};
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::clock::version_vector::Version;
use moirai_protocol::crdt::eval::EvalNested;
use moirai_protocol::crdt::query::{QueryOperation, Read};
use moirai_protocol::event::Event;
use moirai_protocol::log_id::LogId;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::effect_context::EffectContext;
use moirai_protocol::state::log::IsLog;
use moirai_protocol::state::po_log::VecLog;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub type Op = AWSet<String>;

/// An add-wins set of strings: `VecLog<AWSet<String>>`, wrapped so this
/// crate may implement `QueryableLog` for it, and serialisable so that it
/// can be transferred to a late joiner.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SetLog(VecLog<AWSet<String>>);

impl IsLog for SetLog {
    type Command = <VecLog<AWSet<String>> as IsLog>::Command;
    type Op = <VecLog<AWSet<String>> as IsLog>::Op;
    type Rejection = <VecLog<AWSet<String>> as IsLog>::Rejection;

    const STABILIZE_MOVES_OPERATIONS: bool =
        <VecLog<AWSet<String>> as IsLog>::STABILIZE_MOVES_OPERATIONS;

    fn prepare(&self, command: Self::Command) -> Self::Op {
        self.0.prepare(command)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.0.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        self.0.effect(event, ctx)
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.stabilize(version)
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative)
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

impl<Q> EvalNested<Q> for SetLog
where
    VecLog<AWSet<String>>: EvalNested<Q>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

impl QueryableLog for SetLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<Self::Op>>) -> Value {
        let value = replica.query(&Read::new());
        serde_json::to_value(&value)
            .unwrap_or_else(|e| json!({ "error": format!("cannot serialize the set: {e}") }))
    }
}

/// The variant of a frame, by name. A wildcard arm, so the oracle compiles on
/// a branch whose enum has more variants.
pub fn kind<O: Serialize + DeserializeOwned>(frame: &TransportMessage<O>) -> &'static str {
    match frame {
        TransportMessage::Event { .. } => "Event",
        TransportMessage::Batch { .. } => "Batch",
        TransportMessage::SyncRequest { .. } => "SyncRequest",
        TransportMessage::StateRequest { .. } => "StateRequest",
        TransportMessage::StateResponse { .. } => "StateResponse",
        TransportMessage::StateUnavailable { .. } => "StateUnavailable",
        TransportMessage::Hello { .. } => "Hello",
        TransportMessage::Goodbye { .. } => "Goodbye",
        TransportMessage::Raw { .. } => "Raw",
        _ => "other",
    }
}

type Shared<T> = Arc<Mutex<Option<T>>>;
type Received = Arc<Mutex<Vec<(PeerId, &'static str)>>>;

/// A transport the test keeps a hand on while the node owns it: it records
/// the variant of every frame the node takes from it, and it can be closed
/// (the inner transport dropped) and replaced by a successor.
pub struct Tap<T> {
    id: PeerId,
    inner: Shared<T>,
    received: Received,
}

/// The test's side of a [`Tap`].
pub struct TapHandle<T> {
    inner: Shared<T>,
    received: Received,
}

pub fn tap<T: CrdtTransport>(transport: T) -> (Tap<T>, TapHandle<T>) {
    let inner = Arc::new(Mutex::new(Some(transport)));
    let received: Received = Arc::default();
    let id = inner.lock().unwrap().as_ref().unwrap().local_id().clone();
    (
        Tap {
            id,
            inner: Arc::clone(&inner),
            received: Arc::clone(&received),
        },
        TapHandle { inner, received },
    )
}

impl<T> TapHandle<T> {
    /// Drop the transport the node runs on, which closes its links; the node
    /// keeps running on nothing until [`TapHandle::replace`].
    pub fn close(&self) {
        let closing = self.inner.lock().unwrap().take();
        drop(closing);
    }

    pub fn replace(&self, successor: T) {
        let previous = self.inner.lock().unwrap().replace(successor);
        drop(previous);
    }

    /// `(from, variant)` of every frame the node has taken so far.
    pub fn received(&self) -> Vec<(PeerId, &'static str)> {
        self.received.lock().unwrap().clone()
    }
}

impl<T: CrdtTransport> CrdtTransport for Tap<T> {
    type Op = T::Op;

    fn local_id(&self) -> &PeerId {
        &self.id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.send(peer, msg),
            None => Err(TransportError::ConnectionClosed(peer.clone())),
        }
    }

    fn broadcast(&mut self, msg: TransportMessage<Self::Op>) -> TransportResult<()> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.broadcast(msg),
            None => Ok(()),
        }
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Self::Op>)>> {
        let got = match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.try_recv()?,
            None => None,
        };
        if let Some((from, frame)) = &got {
            self.received
                .lock()
                .unwrap()
                .push((from.clone(), kind(frame)));
        }
        Ok(got)
    }

    fn peers(&self) -> Vec<PeerInfo> {
        match self.inner.lock().unwrap().as_ref() {
            Some(t) => t.peers(),
            None => Vec::new(),
        }
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        match self.inner.lock().unwrap().as_ref() {
            Some(t) => t.is_connected(peer),
            None => false,
        }
    }

    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.pause_peer(peer),
            None => Err(TransportError::Other("the transport is closed".into())),
        }
    }

    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.resume_peer(peer),
            None => Err(TransportError::Other("the transport is closed".into())),
        }
    }

    fn pause_all(&mut self) -> TransportResult<()> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.pause_all(),
            None => Err(TransportError::Other("the transport is closed".into())),
        }
    }

    fn resume_all(&mut self) -> TransportResult<()> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.resume_all(),
            None => Err(TransportError::Other("the transport is closed".into())),
        }
    }

    fn buffered_count(&self, peer: &PeerId) -> usize {
        match self.inner.lock().unwrap().as_ref() {
            Some(t) => t.buffered_count(peer),
            None => 0,
        }
    }

    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.accept_connections(),
            None => Ok(Vec::new()),
        }
    }

    fn add_peer(&mut self, peer: PeerId, addr: String) -> bool {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.add_peer(peer, addr),
            None => false,
        }
    }

    fn connect_to_peers(&mut self) -> TransportResult<Vec<PeerId>> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.connect_to_peers(),
            None => Ok(Vec::new()),
        }
    }

    fn routes(&self) -> Vec<(PeerId, &'static str)> {
        match self.inner.lock().unwrap().as_ref() {
            Some(t) => t.routes(),
            None => Vec::new(),
        }
    }

    fn set_relay(&mut self, session: &str, addr: &str) {
        if let Some(t) = self.inner.lock().unwrap().as_mut() {
            t.set_relay(session, addr);
        }
    }

    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage<Self::Op>> {
        match self.inner.lock().unwrap().as_mut() {
            Some(t) => t.drain_buffer(peer),
            None => Vec::new(),
        }
    }
}

/// The transports a scenario's nodes run on.
pub trait Net {
    type T: CrdtTransport<Op = Op> + Send + 'static;

    /// One transport per id, the i-th knowing the ids in `known[i]` (opened
    /// in this call or an earlier one) as its peers.
    fn open(&mut self, ids: &[&str], known: &[&[&str]]) -> Vec<Self::T>;
}

/// A node running on its own thread, and the test's hand on its transport.
pub struct NodeUnderTest<T> {
    pub id: String,
    pub base: String,
    pub tap: TapHandle<T>,
}

/// Start a node: build it on its own thread (the node need not be `Send`),
/// give it the HTTP API on `127.0.0.1:0`, dial its peers once
/// (`GenericNode::connect`, a no-op for a transport that finds its peers
/// itself) and run its event loop, which never returns.
pub fn start_node<T>(
    me: &str,
    members: &[&str],
    transport: T,
    log_id: &LogId,
    state_transfer: bool,
) -> NodeUnderTest<T>
where
    T: CrdtTransport<Op = Op> + Send + 'static,
{
    let (tap, handle) = tap(transport);
    let http = TcpListener::bind("127.0.0.1:0").expect("bind the HTTP API on 127.0.0.1:0");
    let base = format!("http://{}", http.local_addr().expect("HTTP address"));
    let replica_id = me.to_string();
    let members: Vec<String> = members.iter().map(|m| m.to_string()).collect();
    let log_id = log_id.clone();
    thread::Builder::new()
        .name(format!("nw15-node-{me}"))
        .spawn(move || {
            let refs: Vec<&str> = members.iter().map(String::as_str).collect();
            let mut node: GenericNode<SetLog, Tap<T>> =
                GenericNode::with_transport_and_log_id(replica_id, &refs, tap, log_id);
            node.enable_state_query();
            if state_transfer {
                node.enable_state_transfer();
            }
            node.start_http_on(http);
            node.connect();
            node.run();
        })
        .expect("spawn a node thread");
    let health = format!("{base}/api/health");
    assert!(
        poll(Duration::from_secs(10), || http_client()
            .get(&health)
            .send()
            .map(|r| r.status().is_success())
            .unwrap_or(false)),
        "node {me} never answered {health}"
    );
    NodeUnderTest {
        id: me.to_string(),
        base,
        tap: handle,
    }
}

pub fn poll(timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// How long a state that must be reached may take, and how long a state that
/// must hold is watched before it is read again.
pub const CONVERGE: Duration = Duration::from_secs(30);
pub const HOLD: Duration = Duration::from_secs(1);

fn http_client() -> &'static reqwest::blocking::Client {
    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("an HTTP client")
    })
}

pub fn get_json(url: &str) -> Value {
    http_client()
        .get(url)
        .send()
        .and_then(|r| r.json())
        .unwrap_or_else(|e| panic!("GET {url}: {e}"))
}

fn answered(url: &str, reply: Value) {
    assert_eq!(reply["success"], json!(true), "{url} answered {reply}");
}

fn op<T>(node: &NodeUnderTest<T>, body: Value) {
    let url = format!("{}/api/op", node.base);
    let reply: Value = http_client()
        .post(&url)
        .json(&body)
        .send()
        .and_then(|r| r.json())
        .unwrap_or_else(|e| panic!("POST {url} {body}: {e}"));
    answered(&url, reply);
}

pub fn add<T>(node: &NodeUnderTest<T>, element: &str) {
    op(node, json!({ "Add": element }));
}

pub fn remove<T>(node: &NodeUnderTest<T>, element: &str) {
    op(node, json!({ "Remove": element }));
}

fn control<T>(node: &NodeUnderTest<T>, path: &str) {
    let url = format!("{}{path}", node.base);
    let reply: Value = http_client()
        .post(&url)
        .send()
        .and_then(|r| r.json())
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    answered(&url, reply);
}

pub fn pause<T>(node: &NodeUnderTest<T>, peer: &str) {
    control(node, &format!("/api/pause/{peer}"));
}

pub fn resume<T>(node: &NodeUnderTest<T>, peer: &str) {
    control(node, &format!("/api/resume/{peer}"));
}

pub fn state<T>(node: &NodeUnderTest<T>) -> BTreeSet<String> {
    let url = format!("{}/api/state", node.base);
    let value = get_json(&url);
    value
        .as_array()
        .unwrap_or_else(|| panic!("{url} is not a JSON array: {value}"))
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("{url} lists a non-string: {value}"))
                .to_string()
        })
        .collect()
}

/// `(status, buffered)` of `peer` in the node's `/api/peers`.
pub fn peer_entry<T>(node: &NodeUnderTest<T>, peer: &str) -> Option<(String, u64)> {
    let listing = get_json(&format!("{}/api/peers", node.base));
    listing["peers"].as_array().and_then(|peers| {
        peers.iter().find(|p| p["id"] == json!(peer)).map(|p| {
            (
                p["status"].as_str().unwrap_or_default().to_string(),
                p["buffered"].as_u64().unwrap_or_default(),
            )
        })
    })
}

pub fn metrics<T>(node: &NodeUnderTest<T>) -> Value {
    get_json(&format!("{}/api/metrics", node.base))
}

pub fn set(elements: &[&str]) -> BTreeSet<String> {
    elements.iter().map(|e| e.to_string()).collect()
}

pub fn union(a: &BTreeSet<String>, more: &[&str]) -> BTreeSet<String> {
    a.iter()
        .cloned()
        .chain(more.iter().map(|e| e.to_string()))
        .collect()
}

pub fn await_state<T>(node: &NodeUnderTest<T>, expected: &BTreeSet<String>, what: &str) {
    let mut last = BTreeSet::new();
    let reached = poll(CONVERGE, || {
        last = state(node);
        last == *expected
    });
    assert!(
        reached,
        "{what}: node {} never reached {expected:?}; it reads {last:?}",
        node.id
    );
}

pub fn assert_state<T>(node: &NodeUnderTest<T>, expected: &BTreeSet<String>, what: &str) {
    assert_eq!(state(node), *expected, "{what}: node {}", node.id);
}

/// Wait until every `(node, peer)` pair reads `status` in the node's
/// `/api/peers`.
pub fn await_status<T>(pairs: &[(&NodeUnderTest<T>, &str)], status: &str, what: &str) {
    for (node, peer) in pairs {
        let mut last = None;
        let reached = poll(CONVERGE, || {
            last = peer_entry(node, peer);
            last.as_ref().map(|(s, _)| s.as_str()) == Some(status)
        });
        assert!(
            reached,
            "{what}: {peer} never read {status} at {}; it reads {last:?}",
            node.id
        );
    }
}

/// Z2: three nodes converge, are partitioned, heal and converge, then a
/// one-sided partition holds and heals.
pub fn z2_three_nodes_converge_partition_heal_converge<N: Net>(net: &mut N) {
    let log = LogId::generate();
    let all = ["a", "b", "c"];
    let transports = net.open(&all, &[&["b", "c"], &["a", "c"], &["a", "b"]]);
    let nodes: Vec<NodeUnderTest<N::T>> = transports
        .into_iter()
        .zip(all)
        .map(|(t, me)| start_node(me, &all, t, &log, false))
        .collect();
    let (a, b, c) = (&nodes[0], &nodes[1], &nodes[2]);
    await_status(
        &[(a, "b"), (a, "c"), (b, "a"), (b, "c"), (c, "a"), (c, "b")],
        "Connected",
        "Z2 start",
    );

    // P1. One add each.
    add(a, "a1");
    add(b, "b1");
    add(c, "c1");
    let p1 = set(&["a1", "b1", "c1"]);
    for n in [a, b, c] {
        await_state(n, &p1, "Z2 P1, converge");
    }

    // P2. {a, b} | {c}, both sides pausing. Each writes; c removes b1, an add
    // it has seen. a and b see each other's writes and not c's; c sees only
    // its own.
    pause(a, "c");
    pause(b, "c");
    pause(c, "a");
    pause(c, "b");
    add(a, "a2");
    add(b, "b2");
    add(c, "c2");
    remove(c, "b1");
    let side_ab = set(&["a1", "b1", "c1", "a2", "b2"]);
    let side_c = set(&["a1", "c1", "c2"]);
    await_state(a, &side_ab, "Z2 P2, a during the partition");
    await_state(b, &side_ab, "Z2 P2, b during the partition");
    await_state(c, &side_c, "Z2 P2, c during the partition");
    thread::sleep(HOLD);
    assert_state(a, &side_ab, "Z2 P2, a a second later");
    assert_state(b, &side_ab, "Z2 P2, b a second later");
    assert_state(c, &side_c, "Z2 P2, c a second later");

    // P3. Heal. c's remove saw b1's only add, so b1 is gone everywhere; the
    // rest is the union.
    resume(a, "c");
    resume(b, "c");
    resume(c, "a");
    resume(c, "b");
    let p3 = set(&["a1", "a2", "b2", "c1", "c2"]);
    for n in [a, b, c] {
        await_state(n, &p3, "Z2 P3, converge after the heal");
    }

    // P4. a pauses b; b does not pause a. a's add must not reach b, b's add
    // waits in a's buffer, and c, paused by nobody, gets both.
    pause(a, "b");
    add(a, "a3");
    add(b, "b3");
    let everything = union(&p3, &["a3", "b3"]);
    await_state(c, &everything, "Z2 P4, c during the one-sided partition");
    thread::sleep(HOLD);
    assert_state(
        a,
        &union(&p3, &["a3"]),
        "Z2 P4, a during the one-sided partition",
    );
    assert_state(
        b,
        &union(&p3, &["b3"]),
        "Z2 P4, b during the one-sided partition",
    );
    let entry = peer_entry(a, "b");
    assert!(
        matches!(&entry, Some((status, buffered)) if status == "Paused" && *buffered >= 1),
        "Z2 P4: b at a reads {entry:?}, not Paused with b3 buffered"
    );

    // P5. a resumes b and takes b3 from its buffer; b asks a for what it
    // missed (a pause and resume of a, which is how a node asks today).
    resume(a, "b");
    pause(b, "a");
    resume(b, "a");
    for n in [a, b, c] {
        await_state(
            n,
            &everything,
            "Z2 P5, converge after the one-sided partition",
        );
    }

    for n in &nodes {
        n.tap.close();
    }
}

fn number(metrics: &Value, field: &str) -> u64 {
    metrics[field]
        .as_u64()
        .unwrap_or_else(|| panic!("metrics carry no number `{field}`: {metrics}"))
}

/// Z3: a late joiner by state transfer under d996427's 768 KiB ceiling.
///
/// a and b, members of each other only, write 21 adds in three rounds so
/// that the first 20 are causally stable at both (each round is seen by the
/// other before the next), and so pruned from their outboxes: a delta sync
/// can no longer deliver them. c then joins, knowing a and b, with no
/// history: it must receive a `StateResponse`, adopt it, and read all 21; its
/// own add then reaches a and b. The log is 21 short adds, a few kB, far
/// under the ceiling.
pub fn z3_a_late_joiner_adopts_by_state_transfer_and_converges<N: Net>(net: &mut N) {
    let log = LogId::generate();
    let pair = net.open(&["a", "b"], &[&["b"], &["a"]]);
    let mut started = pair
        .into_iter()
        .zip(["a", "b"])
        .map(|(t, me)| start_node(me, &["a", "b"], t, &log, true));
    let a = started.next().unwrap();
    let b = started.next().unwrap();
    await_status(&[(&a, "b"), (&b, "a")], "Connected", "Z3 start");

    let es: Vec<String> = (0..10).map(|i| format!("e{i}")).collect();
    let fs: Vec<String> = (0..10).map(|i| format!("f{i}")).collect();
    for e in &es {
        add(&a, e);
    }
    let round1: BTreeSet<String> = es.iter().cloned().collect();
    await_state(&b, &round1, "Z3, b has a's ten adds");
    for f in &fs {
        add(&b, f);
    }
    let round2: BTreeSet<String> = es.iter().chain(&fs).cloned().collect();
    await_state(&a, &round2, "Z3, a has b's ten adds");
    add(&a, "e10");
    let history = union(&round2, &["e10"]);
    await_state(&b, &history, "Z3, b has e10");

    // Precondition: the first 20 are stable at both donors and something was
    // pruned, or this would not test a state transfer.
    let mut last = (Value::Null, Value::Null);
    let stable = poll(CONVERGE, || {
        last = (metrics(&a), metrics(&b));
        number(&last.0, "stable_prefix") >= 20 && number(&last.1, "stable_prefix") >= 20
    });
    assert!(
        stable,
        "Z3 precondition: the first 20 adds never became stable: {last:?}"
    );
    for m in [&last.0, &last.1] {
        assert!(
            number(m, "retained_ops") < number(m, "delivered_ops"),
            "Z3 precondition: nothing was pruned: {m}"
        );
    }

    let joiner = net.open(&["c"], &[&["a", "b"]]);
    let c = start_node(
        "c",
        &["c", "a", "b"],
        joiner.into_iter().next().unwrap(),
        &log,
        true,
    );
    await_state(&c, &history, "Z3, the joiner adopts the donors' state");
    let responses: Vec<(PeerId, &'static str)> = c
        .tap
        .received()
        .into_iter()
        .filter(|(_, kind)| *kind == "StateResponse")
        .collect();
    assert!(
        responses.iter().any(|(from, _)| from == "a" || from == "b"),
        "Z3: the joiner reached the history without a StateResponse from a or b: {:?}",
        c.tap.received()
    );
    let joined = metrics(&c);
    assert!(
        number(&joined, "delivered_ops") >= 21,
        "Z3: the joiner counts fewer than 21 delivered: {joined}"
    );

    add(&c, "g0");
    let after = union(&history, &["g0"]);
    for n in [&a, &b, &c] {
        await_state(n, &after, "Z3, the joiner's add converges");
    }

    for n in [&a, &b, &c] {
        n.tap.close();
    }
}
