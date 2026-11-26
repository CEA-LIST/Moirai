//! NW.31m oracle (vault spec/items/NW.31m.md): a node asks a SyncRequest
//! sender back for what its frontier holds and this replica has not
//! delivered, once per unchanged lead, never for a log with no history.
//! Generic over a ReplicaFactory and included by `#[path]` from
//! moirai-netstack-node/tests/nw31m_ask_back.rs (the toy) and
//! adapters/crdt/tests/nw31m_upstream_ask_back.rs (upstream replicas).
//! `ops` are six distinct ops that commute, so a converged state equals one
//! fresh replica that submitted them all. Sockets on 127.0.0.1:0 only in
//! `one_sided_pause_then_resume`; run under nodes.lock (tests/nw31m/gate.sh).
#![allow(dead_code)]

use std::net::TcpListener;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_node::Node;
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::transport::{CrdtTransport, PeerId, PeerInfo, TransportResult};
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(30);
/// No frame handled for this long ends an exchange.
const QUIET: Duration = Duration::from_secs(1);
const QUIET_WITHIN: Duration = Duration::from_secs(10);
const MEMBERS: [&str; 3] = ["ann", "bob", "cy"];

fn shared() -> LogId {
    LogId::parse("a11ba11ba11ba11ba11ba11ba11ba11b").expect("a log id")
}

fn second() -> LogId {
    LogId::parse("5ec05ec05ec05ec05ec05ec05ec05ec0").expect("a log id")
}

fn members() -> Vec<String> {
    MEMBERS.map(String::from).to_vec()
}

fn op<F: ReplicaFactory, T: CrdtTransport>(node: &mut Node<F, T>, op: &Value) {
    let r = node.apply_op(op.clone());
    assert!(r.success, "{op}: {}", r.message);
}

fn read<F: ReplicaFactory, T: CrdtTransport>(node: &Node<F, T>) -> Value {
    node.hosted(&shared()).expect("hosted").read_state()
}

/// The state of one fresh replica that submitted `ops`.
fn reference<F: ReplicaFactory>(make: &mut impl FnMut() -> F, ops: &[Value]) -> Value {
    let mut replica = make().open(shared().as_str(), "ref", &members());
    for op in ops {
        replica.submit(op.clone()).expect("a valid op is accepted");
    }
    replica.read_state()
}

// ---- one node, frames by hand ------------------------------------------------

/// Records what the node sends and broadcasts; receives nothing; no peer is
/// connected.
struct Recorder {
    id: PeerId,
    sent: Vec<(PeerId, TransportMessage)>,
    broadcast: Vec<TransportMessage>,
}

impl CrdtTransport for Recorder {
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

type Lone<F> = Node<F, Recorder>;

/// ann, a member with bob and cy, alone with a recorder.
fn ann<F: ReplicaFactory>(make: &mut impl FnMut() -> F) -> Lone<F> {
    let recorder = Recorder {
        id: "ann".to_string(),
        sent: Vec::new(),
        broadcast: Vec::new(),
    };
    Node::new(make(), "ann".to_string(), &MEMBERS, recorder, shared())
}

/// A replica of `log` named `id`, from the same factory: the source of the
/// frontiers and events bob shows ann.
fn replica<F: ReplicaFactory>(make: &mut impl FnMut() -> F, log: &LogId, id: &str) -> F::Replica {
    make().open(log.as_str(), id, &members())
}

/// bob's SyncRequest for `log` carrying `frontier`, handed to ann: the tag of
/// every frame ann sent, in order, and the frames.
fn ask<F: ReplicaFactory>(
    ann: &mut Lone<F>,
    log: &LogId,
    frontier: Value,
) -> (Vec<&'static str>, Vec<TransportMessage>) {
    ann.transport_mut().sent.clear();
    let since = json!({ "log_id": log.as_str(), "frontier": frontier });
    ann.handle_transport_message("bob".to_string(), TransportMessage::SyncRequest { since });
    let sent = std::mem::take(&mut ann.transport_mut().sent);
    assert!(
        sent.iter().all(|(to, _)| to == "bob"),
        "ann sent a frame to someone other than bob: {sent:?}"
    );
    let frames: Vec<TransportMessage> = sent.into_iter().map(|(_, msg)| msg).collect();
    let tags = frames
        .iter()
        .map(|msg| match msg {
            TransportMessage::Batch { .. } => "Batch",
            TransportMessage::SyncRequest { .. } => "SyncRequest",
            _ => "other",
        })
        .collect();
    (tags, frames)
}

/// ann holds ops[0]; bob's frontier holds ops[1]. ann answers with a batch,
/// then asks bob back, naming the log and carrying ann's frontier. bob never
/// answers, so the lead cannot be delivered: the same request three more
/// times gets a batch each and no ask-back. A third party's op (cy's ops[2])
/// reaches bob: a new lead, asked back once more, and once only.
pub fn asks_back_once_per_unchanged_lead<F: ReplicaFactory>(
    mut make: impl FnMut() -> F,
    ops: &[Value],
) {
    let mut ann = ann(&mut make);
    op(&mut ann, &ops[0]);
    let mut bob = replica(&mut make, &shared(), "bob");
    bob.submit(ops[1].clone()).expect("a valid op is accepted");
    let lead = bob.frontier();

    let (tags, frames) = ask(&mut ann, &shared(), lead.clone());
    assert_eq!(
        tags,
        ["Batch", "SyncRequest"],
        "bob's frontier holds an op ann has not delivered: a batch, then an ask-back"
    );
    let TransportMessage::SyncRequest { since } = &frames[1] else {
        unreachable!()
    };
    assert_eq!(
        since.get("log_id"),
        Some(&json!(shared().as_str())),
        "the ask-back does not name the log: {since}"
    );
    let own = ann.hosted(&shared()).expect("hosted").frontier();
    assert_eq!(
        since.get("frontier"),
        Some(&own),
        "the ask-back does not carry ann's frontier"
    );

    for round in 1..=3 {
        let (tags, _) = ask(&mut ann, &shared(), lead.clone());
        assert_eq!(
            tags,
            ["Batch"],
            "repeat {round} of an unchanged, undelivered lead was asked back again"
        );
    }

    let mut cy = replica(&mut make, &shared(), "cy");
    let event = cy.submit(ops[2].clone()).expect("a valid op is accepted");
    bob.apply_remote("cy", event).expect("bob takes cy's op");
    let lead = bob.frontier();
    let (tags, _) = ask(&mut ann, &shared(), lead.clone());
    assert_eq!(
        tags,
        ["Batch", "SyncRequest"],
        "a new lead (a third party's op on bob) was not asked back"
    );
    let (tags, _) = ask(&mut ann, &shared(), lead);
    assert_eq!(tags, ["Batch"], "the new lead was asked back twice");
}

/// ann holds ops[0] and ops[1]. A SyncRequest whose frontier ann already
/// covers gets a batch and no ask-back: ann's own frontier, an empty
/// replica's, and that of bob holding ann's first op only (a lagging peer).
pub fn no_ask_back_when_the_frontier_is_covered<F: ReplicaFactory>(
    mut make: impl FnMut() -> F,
    ops: &[Value],
) {
    let mut ann = ann(&mut make);
    op(&mut ann, &ops[0]);
    let Some(TransportMessage::Event { event }) = ann.transport_mut().broadcast.pop() else {
        panic!("set-up: ann's op was not broadcast as one event");
    };
    op(&mut ann, &ops[1]);

    let own = ann.hosted(&shared()).expect("hosted").frontier();
    let empty = replica(&mut make, &shared(), "bob").frontier();
    let mut bob = replica(&mut make, &shared(), "bob");
    bob.apply_remote("ann", event)
        .expect("bob takes ann's first op");
    let lagging = bob.frontier();
    assert_eq!(
        bob.progress().delivered,
        1,
        "set-up: bob does not hold ann's first op"
    );

    for (what, frontier) in [
        ("ann's own", own),
        ("an empty", empty),
        ("a lagging", lagging),
    ] {
        let (tags, _) = ask(&mut ann, &shared(), frontier);
        assert_eq!(
            tags,
            ["Batch"],
            "{what} frontier holds nothing ann lacks, yet ann did not answer with one batch alone"
        );
    }
}

/// A log with no history leaves catching up to state transfer: bob's lead on
/// it gets a batch and no ask-back, on the default log of a fresh node and on
/// a second log hosted beside a default log with history. The rule is per
/// log: the same lead on the default log, once it has history, is asked back.
pub fn a_log_with_no_history_does_not_ask_back<F: ReplicaFactory>(
    mut make: impl FnMut() -> F,
    ops: &[Value],
) {
    let mut ann = ann(&mut make);
    let mut bob = replica(&mut make, &shared(), "bob");
    bob.submit(ops[1].clone()).expect("a valid op is accepted");
    let (tags, _) = ask(&mut ann, &shared(), bob.frontier());
    assert_eq!(
        tags,
        ["Batch"],
        "a fresh node asked back for a delta on a log with no history"
    );

    op(&mut ann, &ops[0]);
    ann.host_log(second()).expect("a fresh id");
    let mut other = replica(&mut make, &second(), "bob");
    other
        .submit(ops[2].clone())
        .expect("a valid op is accepted");
    let (tags, _) = ask(&mut ann, &second(), other.frontier());
    assert_eq!(
        tags,
        ["Batch"],
        "a second log with no history asked back for a delta"
    );

    let (tags, _) = ask(&mut ann, &shared(), bob.frontier());
    assert_eq!(
        tags,
        ["Batch", "SyncRequest"],
        "the default log, with history now, did not ask back for bob's lead"
    );
}

// ---- three nodes over DirectTransport on 127.0.0.1 ---------------------------

type Wired<F> = Node<F, DirectTransport>;

fn bind() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local addr").to_string();
    (listener, addr)
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

/// ann, bob and cy, each knowing the others, every pair dialled once, meshed.
fn mesh<F: ReplicaFactory>(make: &mut impl FnMut() -> F) -> Vec<Wired<F>> {
    let bound: Vec<(TcpListener, String)> = MEMBERS.iter().map(|_| bind()).collect();
    let addrs: Vec<String> = bound.iter().map(|(_, a)| a.clone()).collect();
    let mut nodes: Vec<Wired<F>> = bound
        .into_iter()
        .enumerate()
        .map(|(i, (listener, _))| {
            let peers: HashMap<PeerId, String> = (0..MEMBERS.len())
                .filter(|j| *j != i)
                .map(|j| (MEMBERS[j].to_string(), addrs[j].clone()))
                .collect();
            let transport = DirectTransport::with_listener(MEMBERS[i].to_string(), listener, peers)
                .expect("DirectTransport::with_listener");
            Node::new(
                make(),
                MEMBERS[i].to_string(),
                &MEMBERS,
                transport,
                shared(),
            )
        })
        .collect();
    for i in 0..MEMBERS.len() {
        for other in &MEMBERS[i + 1..] {
            nodes[i]
                .transport_mut()
                .connect_to(&other.to_string())
                .expect("dial a peer on loopback");
        }
    }
    let meshed = pump(&mut nodes, WAIT, |nodes| {
        nodes.iter().enumerate().all(|(i, n)| {
            (0..MEMBERS.len())
                .all(|j| i == j || n.transport().is_connected(&MEMBERS[j].to_string()))
        })
    });
    assert!(meshed, "set-up: the mesh did not form within {WAIT:?}");
    nodes
}

fn converge<F: ReplicaFactory>(nodes: &mut [Wired<F>], want: &Value, what: &str) {
    let done = pump(nodes, WAIT, |nodes| nodes.iter().all(|n| read(n) == *want));
    let views: Vec<Value> = nodes.iter().map(read).collect();
    assert!(
        done,
        "{what}: want {want} on every node, they read {views:?}"
    );
}

/// Poll until no node handles a frame for [`QUIET`]; panics when frames keep
/// moving for [`QUIET_WITHIN`]: an exchange that does not end.
fn quiet<F: ReplicaFactory>(nodes: &mut [Wired<F>], what: &str) {
    let start = Instant::now();
    let mut last = start;
    loop {
        let handled: usize = nodes.iter_mut().map(|n| n.poll()).sum();
        let now = Instant::now();
        if handled > 0 {
            last = now;
        }
        if now.duration_since(last) >= QUIET {
            return;
        }
        assert!(
            now.duration_since(start) < QUIET_WITHIN,
            "{what}: frames kept moving for {QUIET_WITHIN:?}; the exchange did not end"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// DT.76's situation. ann, bob and cy write one op each and converge. ann
/// pauses bob, and bob does not pause ann. ann writes (it reaches cy only),
/// bob writes (cy, and ann's buffer), cy writes (both). ann resumes bob: it
/// takes in bob's buffered op and asks bob for a delta. Every write ends on
/// every replica, and the exchange ends.
pub fn one_sided_pause_then_resume<F: ReplicaFactory>(mut make: impl FnMut() -> F, ops: &[Value]) {
    let mut nodes = mesh(&mut make);
    for (i, node) in nodes.iter_mut().enumerate() {
        op(node, &ops[i]);
    }
    let before = reference(&mut make, &ops[..3]);
    converge(&mut nodes, &before, "before the pause");
    quiet(&mut nodes, "before the pause");

    let r = nodes[0].pause_peer("bob");
    assert!(r.success, "ann pauses bob: {}", r.message);
    op(&mut nodes[0], &ops[3]);
    op(&mut nodes[1], &ops[4]);
    op(&mut nodes[2], &ops[5]);
    let want = reference(&mut make, &ops[..6]);
    let cy_has_all = pump(&mut nodes, WAIT, |n| read(&n[2]) == want);
    assert!(
        cy_has_all,
        "set-up: cy did not take in the three writes made during the pause"
    );

    let r = nodes[0].resume_peer("bob");
    assert!(r.success, "ann resumes bob: {}", r.message);
    converge(
        &mut nodes,
        &want,
        "after ann resumed bob, every write on every replica",
    );
    quiet(&mut nodes, "after the resume");
}
