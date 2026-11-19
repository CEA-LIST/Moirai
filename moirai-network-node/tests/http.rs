use moirai_network_node::{Application, Config, Node, NodeHandle};
use moirai_protocol::{
    commitment::{
        commit_log::CommitmentLog,
        commit_protocols::lamport::{
            lamport_commit::LamportCommit, lamport_tracker::LamportTracker,
        },
        examples::commit_set::{CommitSet, SetUpdate},
    },
    crdt::query::Read,
    replica::IsReplica,
    state::po_log::VecLog,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io::{Read as _, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

fn request(port: u16, method: &str, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    write!(stream, "{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap())
}

fn start_node(timeout_ms: u64) -> (u16, NodeHandle<SetUpdate<String>>, thread::JoinHandle<()>) {
    start_node_with_limits(timeout_ms, 64, 64)
}

fn start_node_with_limits(
    timeout_ms: u64,
    optimistic: usize,
    consistent: usize,
) -> (u16, NodeHandle<SetUpdate<String>>, thread::JoinHandle<()>) {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel();
    let config: Config = serde_json::from_value(json!({"replica_id":1,"replica_name":"test","listen_port":0,"http_port":port,
        "peers":[{"replica_id":2,"address":"127.0.0.1:1"}],"noop_interval_ms":null,"memory_usage_enabled":false,"request_timeout_ms":timeout_ms,"http_optimistic_limit":optimistic,"http_consistent_limit":consistent})).unwrap();
    drop(reservation);
    let server = thread::spawn(move || {
        type Child = VecLog<CommitSet<String>>;
        let protocol = LamportCommit::from_parts(config.members(), LamportTracker::default());
        let log = CommitmentLog::new(Child::default(), protocol);
        let app: Application<CommitmentLog<Child, LamportCommit>> = Application {
            query: |replica| {
                let values: HashSet<String, _> = replica.query(&Read::new());
                let mut values: Vec<_> = values.into_iter().collect();
                values.sort();
                json!(values)
            },
            committed: |replica, id| {
                replica
                    .state()
                    .committed_frontier()
                    .is_some_and(|f| f.contains(id))
            },
            no_op: || SetUpdate::NoOp,
        };
        let mut node = Node::new(config, log, app).unwrap();
        tx.send(node.handle()).unwrap();
        node.run(None).unwrap();
    });
    let handle = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    (port, handle, server)
}

#[test]
fn minimal_http_api_keeps_weak_updates_and_reads_available_during_commitment_waits() {
    let (port, handle, server) = start_node(300);
    let strong = thread::spawn(move || {
        request(
            port,
            "POST",
            "/api/op?wait_for_commit=true",
            r#"{"Add":"strong"}"#,
        )
    });
    let (status, health) = request(port, "GET", "/api/health", "");
    assert_eq!(status, 200);
    assert_eq!(health["status"], "ok");
    let (status, result) = request(port, "POST", "/api/op", r#"{"Add":"weak"}"#);
    assert_eq!(status, 200);
    assert_eq!(result["success"], true);
    let (status, result) = strong.join().unwrap();
    assert_eq!(status, 504);
    assert_eq!(result["success"], false);
    assert_eq!(result["applied"], true);
    let (status, state) = request(port, "GET", "/api/state", "");
    assert_eq!(status, 200);
    assert_eq!(state["state"], json!(["strong", "weak"]));
    assert_eq!(request(port, "POST", "/api/op", "invalid").0, 400);
    for path in ["/api/operations", "/api/peers", "/api/pause-all"] {
        assert_eq!(request(port, "GET", path, "").0, 404);
    }
    handle.stop();
    server.join().unwrap();
}

#[test]
fn saturated_consistent_limit_preserves_optimistic_reads_and_health_capacity() {
    let (port, handle, server) = start_node_with_limits(5000, 1, 3);
    let clients: Vec<_> = (0..3)
        .map(|index| {
            thread::spawn(move || {
                request(
                    port,
                    "POST",
                    "/api/op?wait_for_commit=true",
                    &format!(r#"{{"Add":"item-{index}"}}"#),
                )
            })
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(2);
    // Observe acceptance through the client channel so polling does not itself
    // occupy an HTTP worker and cause one of the updates to be rejected.
    loop {
        let state = handle
            .state()
            .unwrap()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        if state["state"].as_array().unwrap().len() == 3 {
            break;
        }
        assert!(Instant::now() < deadline, "HTTP workers never saturated");
    }
    let (status, result) = request(port, "POST", "/api/op?wait_for_commit=true", "\"NoOp\"");
    assert_eq!(status, 503);
    assert_eq!(result["request_class"], "consistent");
    assert_eq!(request(port, "POST", "/api/op", r#"{"Add":"weak"}"#).0, 200);
    // Wait for the completed optimistic worker to release its single slot.
    let deadline = Instant::now() + Duration::from_secs(1);
    while request(port, "GET", "/api/state", "").0 != 200 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(request(port, "GET", "/api/health", "").0, 200);
    handle.stop();
    server.join().unwrap();
    for client in clients {
        assert!(matches!(client.join().unwrap().0, 503 | 504));
    }
}

#[test]
fn saturated_optimistic_limit_preserves_consistent_capacity() {
    let (port, handle, server) = start_node_with_limits(5000, 1, 2);
    // Hold the optimistic slot while its worker waits for a request body.
    let mut blocked = TcpStream::connect(("127.0.0.1", port)).unwrap();
    blocked
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    write!(
        blocked,
        "POST /api/op HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nContent-Length: 65536\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    // A 100 response is emitted when the admitted worker starts reading the
    // body. Wait for it before probing the limit, avoiding an admission race.
    let mut interim = Vec::new();
    while !interim.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        blocked.read_exact(&mut byte).unwrap();
        interim.push(byte[0]);
    }
    assert!(String::from_utf8(interim).unwrap().contains("100 Continue"));
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let (status, result) = request(port, "POST", "/api/op", "\"NoOp\"");
        if status == 503 {
            assert_eq!(result["request_class"], "optimistic");
            break;
        }
        assert!(Instant::now() < deadline);
    }
    let strong = thread::spawn(move || {
        request(
            port,
            "POST",
            "/api/op?wait_for_commit=true",
            r#"{"Add":"consistent"}"#,
        )
    });
    loop {
        let state = handle
            .state()
            .unwrap()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        if state["state"] == json!(["consistent"]) {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(request(port, "GET", "/api/health", "").0, 200);
    drop(blocked);
    handle.stop();
    server.join().unwrap();
    assert_eq!(strong.join().unwrap().0, 504);
}
