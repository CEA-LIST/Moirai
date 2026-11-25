//! NW.31j oracle (vault spec/items/NW.31j.md): the two resume routes of 601c887's
//! http_api.rs, POST /api/resume/{peer} and POST /api/resume-all, which NW.31g
//! left for later. On 601c887 (generic.rs, ControlCmd::Resume and ResumeAll) a
//! resume lifts the pause, hands the frames the transport buffered to the node,
//! and asks each resumed peer for a delta (a SyncRequest carrying the frontier);
//! the answer is an OpResult with status 200. 601c887 has no route test for
//! them, so these are new, on the toy behind a real Node, with a transport that
//! buffers like moirai-netstack-transport's Zenoh one (resume of a peer that is
//! not paused is refused).
//!
//! flock ~/.cache/cea-crdt-scratch/cargo.lock flock ~/.cache/cea-crdt-scratch/nodes.lock \
//!   nice -n 10 cargo test -j 2 -p moirai-netstack-http --locked --test nw31j_resume

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use moirai_netstack::{LogId, ReplicaPort, TransportMessage};
use moirai_netstack_http::HttpApi;
use moirai_netstack_node::Node;
use moirai_netstack_testkit::ToyFactory;
use moirai_netstack_transport::transport::{
    CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportError, TransportResult,
};
use serde_json::{Value, json};

const LOG: &str = "00112233445566778899aabbccddeeff";

/// Knows bob, carol and dave; buffers frames per peer while it is paused, as
/// ZenohTransport does; records every frame sent.
struct Buffering {
    id: PeerId,
    peers: BTreeMap<PeerId, (PeerStatus, VecDeque<TransportMessage>)>,
    sent: Vec<(PeerId, TransportMessage)>,
}

impl Buffering {
    fn new() -> Self {
        let peers = ["bob", "carol", "dave"]
            .into_iter()
            .map(|p| (p.to_string(), (PeerStatus::Connected, VecDeque::new())))
            .collect();
        Self {
            id: "alice".to_string(),
            peers,
            sent: Vec::new(),
        }
    }

    /// Pause `peer` and hold one toy event from it, as if it arrived meanwhile.
    fn paused_with_one_event(&mut self, peer: &str, element: &str) {
        let (status, buffer) = self.peers.get_mut(peer).expect("a known peer");
        *status = PeerStatus::Paused;
        buffer.push_back(TransportMessage::Event {
            event: json!({ "log": LOG, "log_id": LOG, "origin": peer, "op": element }),
        });
    }

    fn status(&self, peer: &str) -> PeerStatus {
        self.peers[peer].0.clone()
    }

    /// The SyncRequests sent, as (peer, since).
    fn sync_requests(&self) -> Vec<(PeerId, Value)> {
        self.sent
            .iter()
            .filter_map(|(peer, msg)| match msg {
                TransportMessage::SyncRequest { since } => Some((peer.clone(), since.clone())),
                _ => None,
            })
            .collect()
    }
}

impl CrdtTransport for Buffering {
    fn local_id(&self) -> &PeerId {
        &self.id
    }
    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        self.sent.push((peer.clone(), msg));
        Ok(())
    }
    fn broadcast(&mut self, _msg: TransportMessage) -> TransportResult<()> {
        Ok(())
    }
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        Ok(None)
    }
    fn peers(&self) -> Vec<PeerInfo> {
        self.peers
            .iter()
            .map(|(id, (status, buffer))| PeerInfo {
                id: id.clone(),
                address: "127.0.0.1:1".to_string(),
                status: status.clone(),
                buffered_messages: buffer.len(),
            })
            .collect()
    }
    fn is_connected(&self, peer: &PeerId) -> bool {
        self.peers
            .get(peer)
            .is_some_and(|(status, _)| *status == PeerStatus::Connected)
    }
    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        match self.peers.get_mut(peer) {
            Some((status, _)) => {
                *status = PeerStatus::Paused;
                Ok(())
            }
            None => Err(TransportError::PeerNotFound(peer.clone())),
        }
    }
    fn resume_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        match self.peers.get_mut(peer) {
            Some((status, _)) if *status == PeerStatus::Paused => {
                *status = PeerStatus::Connected;
                Ok(())
            }
            _ => Err(TransportError::Other(format!("Peer {peer} was not paused"))),
        }
    }
    fn pause_all(&mut self) -> TransportResult<()> {
        for (status, _) in self.peers.values_mut() {
            *status = PeerStatus::Paused;
        }
        Ok(())
    }
    fn resume_all(&mut self) -> TransportResult<()> {
        for (status, _) in self.peers.values_mut() {
            *status = PeerStatus::Connected;
        }
        Ok(())
    }
    fn buffered_count(&self, peer: &PeerId) -> usize {
        self.peers.get(peer).map_or(0, |(_, buffer)| buffer.len())
    }
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(Vec::new())
    }
    fn drain_buffer(&mut self, peer: &PeerId) -> Vec<TransportMessage> {
        self.peers
            .get_mut(peer)
            .map(|(_, buffer)| buffer.drain(..).collect())
            .unwrap_or_default()
    }
}

/// A node "alice" on the toy whose log already holds "x", so it has history and
/// asks for a delta (never a state transfer) on resume.
fn node() -> Node<ToyFactory, Buffering> {
    let log = LogId::parse(LOG).expect("a fixed, valid log id");
    let mut node = Node::new(
        ToyFactory,
        "alice".to_string(),
        &["alice", "bob", "carol", "dave"],
        Buffering::new(),
        log,
    );
    assert!(
        node.submit_op(None, json!("x")).success,
        "set-up: the first op"
    );
    node.transport_mut().sent.clear();
    node
}

/// One POST with no body, while this thread plays the node's event loop.
fn post(node: &mut Node<ToyFactory, Buffering>, path: &str) -> (u16, Value) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("loopback bind");
    let port = listener.local_addr().expect("local addr").port();
    let api = HttpApi::start(
        listener,
        "alice",
        LogId::parse(LOG).expect("valid"),
        Duration::from_secs(5),
    );
    let raw = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    );
    let client = thread::spawn(move || {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.write_all(raw.as_bytes()).expect("request written");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("response read");
        response
    });
    while !client.is_finished() {
        api.serve(node, &mut ());
        thread::sleep(Duration::from_millis(2));
    }
    let response = client.join().expect("the client thread");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let body = response.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let body = serde_json::from_str(body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"));
    (status, body)
}

fn state(node: &Node<ToyFactory, Buffering>) -> Value {
    let log = LogId::parse(LOG).expect("valid");
    node.hosted(&log).expect("hosted").read_state()
}

/// The SyncRequest 601c887 sends on resume: this log and the frontier after the
/// buffered frames were handled.
fn delta_request(frontier: Value) -> Value {
    json!({ "log_id": LOG, "frontier": frontier })
}

#[test]
fn nw31j_resume_peer_hands_the_buffer_over_and_asks_the_peer_for_a_delta() {
    let mut node = node();
    node.transport_mut().paused_with_one_event("bob", "b1");
    node.transport_mut().paused_with_one_event("carol", "c1");
    assert_eq!(
        post(&mut node, "/api/resume/bob"),
        (
            200,
            json!({
                "success": true,
                "message": "Resumed peer 'bob', delivered 1 buffered msgs, requested sync"
            })
        )
    );
    let transport = node.transport();
    assert_eq!(transport.status("bob"), PeerStatus::Connected);
    assert_eq!(
        transport.status("carol"),
        PeerStatus::Paused,
        "only bob is resumed"
    );
    assert_eq!(transport.buffered_count(&"carol".to_string()), 1);
    assert_eq!(
        transport.sync_requests(),
        vec![(
            "bob".to_string(),
            delta_request(json!([["alice", "x"], ["bob", "b1"]]))
        )],
        "one SyncRequest, to bob, after bob's buffered event"
    );
    assert_eq!(state(&node), json!(["b1", "x"]));
}

#[test]
fn nw31j_resume_all_resumes_every_paused_peer_and_asks_each_for_a_delta() {
    let mut node = node();
    assert_eq!(
        post(&mut node, "/api/resume-all"),
        (
            200,
            json!({ "success": true, "message": "Resumed 0 peers, delivered 0 buffered msgs" })
        ),
        "nothing paused"
    );
    assert_eq!(node.transport().sync_requests(), Vec::new());

    node.transport_mut().paused_with_one_event("bob", "b1");
    node.transport_mut().paused_with_one_event("carol", "c1");
    assert_eq!(
        post(&mut node, "/api/resume-all"),
        (
            200,
            json!({ "success": true, "message": "Resumed 2 peers, delivered 2 buffered msgs" })
        )
    );
    let transport = node.transport();
    for peer in ["bob", "carol", "dave"] {
        assert_eq!(transport.status(peer), PeerStatus::Connected, "{peer}");
        assert_eq!(transport.buffered_count(&peer.to_string()), 0, "{peer}");
    }
    let mut asked: Vec<PeerId> = transport
        .sync_requests()
        .into_iter()
        .map(|(peer, since)| {
            assert_eq!(since["log_id"], json!(LOG), "{since}");
            peer
        })
        .collect();
    asked.sort();
    assert_eq!(
        asked,
        vec!["bob".to_string(), "carol".to_string()],
        "one SyncRequest to each resumed peer and none to dave, never paused"
    );
    assert_eq!(state(&node), json!(["b1", "c1", "x"]));
}

#[test]
fn nw31j_resume_of_a_peer_not_paused_answers_the_transport_refusal() {
    let mut node = node();
    let refusal = TransportError::Other("Peer dave was not paused".to_string());
    assert_eq!(
        post(&mut node, "/api/resume/dave"),
        (
            200,
            json!({ "success": false, "message": format!("Failed to resume 'dave': {refusal}") })
        )
    );
    assert_eq!(
        node.transport().sync_requests(),
        Vec::new(),
        "no sync after a refusal"
    );
}
