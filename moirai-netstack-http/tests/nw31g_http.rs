//! NW.31g oracle: moirai-netstack-http, 601c887's generic HTTP routes over a
//! moirai-netstack-node Node<F, T> (vault spec/items/NW.31g.md, NW.31 design row S6).
//!
//! The 13 N tests of 601c887's moirai-network/src/http_api.rs, ported by name onto
//! the toy replica of moirai-netstack-testkit: no op type and no Moirai. Where
//! 601c887 played the event loop with a thread answering channels, this thread
//! plays it here by calling HttpApi::serve on a real node, so a test that needs
//! the loop late simply calls it late. Two more tests: an application route
//! added through the extension point, and the generic routes the 13 do not reach,
//! with the routes left for later (S5b catch-up, a port call) answered 404.
//!
//! flock ~/.cache/cea-crdt-scratch/cargo.lock flock ~/.cache/cea-crdt-scratch/nodes.lock \
//!   nice -n 10 cargo test -j 2 -p moirai-netstack-http --locked --test nw31g_http

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, Opaque, Progress, ReplicaFactory, ReplicaPort, TransportMessage};
use moirai_netstack_http::{HttpApi, Routes};
use moirai_netstack_node::Node;
use moirai_netstack_testkit::{ToyFactory, ToyReplica};
use moirai_netstack_transport::transport::{
    CrdtTransport, PeerId, PeerInfo, PeerStatus, TransportResult,
};
use serde_json::{Value, json};

/// The default log of every node here, fixed so bodies can be asserted verbatim.
const TEST_LOG_ID: &str = "00112233445566778899aabbccddeeff";
/// Two more models, as in 601c887's mp3.
const BT: &str = "a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2";
const UML: &str = "c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d4";
/// A well-formed id no node here hosts.
const UNHOSTED: &str = "e5f6e5f6e5f6e5f6e5f6e5f6e5f6e5f6";
/// 601c887's REPLY_TIMEOUT, for the tests that never reach it.
const WAIT: Duration = Duration::from_secs(5);

fn id(text: &str) -> LogId {
    LogId::parse(text).expect("a fixed, valid log id")
}

// ------------------------------------------------------------------ transport

/// Sends nowhere and receives nothing; knows one peer, "bob", and records the
/// pause and resume calls the routes make.
struct Quiet {
    id: PeerId,
    bob: PeerStatus,
    paused: Vec<PeerId>,
    resumed: usize,
}

impl Quiet {
    fn new() -> Self {
        Self {
            id: "test-replica".to_string(),
            bob: PeerStatus::Connected,
            paused: Vec::new(),
            resumed: 0,
        }
    }
}

impl CrdtTransport for Quiet {
    fn local_id(&self) -> &PeerId {
        &self.id
    }
    fn send(&mut self, _peer: &PeerId, _msg: TransportMessage) -> TransportResult<()> {
        Ok(())
    }
    fn broadcast(&mut self, _msg: TransportMessage) -> TransportResult<()> {
        Ok(())
    }
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        Ok(None)
    }
    fn peers(&self) -> Vec<PeerInfo> {
        vec![PeerInfo {
            id: "bob".to_string(),
            address: "127.0.0.1:1".to_string(),
            status: self.bob.clone(),
            buffered_messages: 0,
        }]
    }
    fn is_connected(&self, peer: &PeerId) -> bool {
        peer == "bob"
    }
    fn pause_peer(&mut self, peer: &PeerId) -> TransportResult<()> {
        if peer == "bob" {
            self.bob = PeerStatus::Paused;
        }
        self.paused.push(peer.clone());
        Ok(())
    }
    fn resume_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        self.resumed += 1;
        Ok(())
    }
    fn pause_all(&mut self) -> TransportResult<()> {
        self.bob = PeerStatus::Paused;
        self.paused.push("*".to_string());
        Ok(())
    }
    fn resume_all(&mut self) -> TransportResult<()> {
        self.resumed += 1;
        Ok(())
    }
    fn buffered_count(&self, _peer: &PeerId) -> usize {
        0
    }
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(Vec::new())
    }
}

// ------------------------------------------------------------------ replicas

/// The toy, taking any JSON value as an op by storing its compact text as the
/// element, so an operation of any shape reaches a replica (601c887's test op
/// was a serde_json::Value for the same reason).
struct Deep(ToyReplica);

/// The toy, slow to apply a batch it has taken.
struct Slow(ToyReplica);

macro_rules! delegate_to_the_toy {
    () => {
        fn replica_id(&self) -> &str {
            self.0.replica_id()
        }
        fn apply_remote(&mut self, from: &str, ops: Opaque) -> Result<(), String> {
            self.0.apply_remote(from, ops)
        }
        fn frontier(&self) -> Opaque {
            self.0.frontier()
        }
        fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
            self.0.ops_since(frontier)
        }
        fn progress(&self) -> Progress {
            self.0.progress()
        }
        fn members(&self) -> Vec<String> {
            self.0.members()
        }
        fn has_history_from(&self, peer: &str) -> bool {
            self.0.has_history_from(peer)
        }
        fn read_state(&self) -> Value {
            self.0.read_state()
        }
    };
}

impl ReplicaPort for Deep {
    delegate_to_the_toy!();
    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        let element = match op {
            Value::String(_) => op,
            other => Value::String(other.to_string()),
        };
        self.0.submit(element)
    }
}

impl ReplicaPort for Slow {
    delegate_to_the_toy!();
    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        self.0.submit(op)
    }
    fn submit_batch(&mut self, ops: Vec<Value>) -> (usize, Option<String>, Vec<Opaque>) {
        // Taken, and then slow to apply.
        thread::sleep(Duration::from_millis(600));
        self.0.submit_batch(ops)
    }
}

struct DeepFactory;
struct SlowFactory;

impl ReplicaFactory for DeepFactory {
    type Replica = Deep;
    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Deep {
        Deep(ToyFactory.open(log, self_id, members))
    }
}

impl ReplicaFactory for SlowFactory {
    type Replica = Slow;
    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Slow {
        Slow(ToyFactory.open(log, self_id, members))
    }
}

// ------------------------------------------------------------------ the node and its loop

/// A node named "test-replica" whose default log is TEST_LOG_ID, hosting `logs` too.
fn node<F: ReplicaFactory>(factory: F, logs: &[&str]) -> Node<F, Quiet> {
    let mut node = Node::new(
        factory,
        "test-replica".to_string(),
        &["test-replica"],
        Quiet::new(),
        id(TEST_LOG_ID),
    );
    for log in logs {
        node.host_log(id(log)).expect("a log not hosted yet");
    }
    node
}

/// The API on 127.0.0.1, port 0, and the port the kernel chose.
fn api(timeout: Duration) -> (HttpApi, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("loopback bind");
    let port = listener.local_addr().expect("local addr").port();
    (
        HttpApi::start(listener, "test-replica", id(TEST_LOG_ID), timeout),
        port,
    )
}

/// Run `client` on its own thread while this thread plays the node's event loop.
fn serving<F, T, R, X>(
    api: &HttpApi,
    node: &mut Node<F, T>,
    routes: &mut R,
    client: impl FnOnce() -> X + Send + 'static,
) -> X
where
    F: ReplicaFactory,
    T: CrdtTransport,
    R: Routes<F, T>,
    X: Send + 'static,
{
    let client = thread::spawn(client);
    while !client.is_finished() {
        api.serve(node, routes);
        thread::sleep(Duration::from_millis(2));
    }
    client.join().expect("the client thread")
}

/// One request while the loop runs, with no application routes.
fn ask<F: ReplicaFactory>(
    api: &HttpApi,
    node: &mut Node<F, Quiet>,
    port: u16,
    head: &str,
    body: Option<&str>,
) -> (u16, Value) {
    let (head, body) = (head.to_string(), body.map(str::to_string));
    let (status, _, answer) = serving(api, node, &mut (), move || {
        exchange(port, &head, body.as_deref())
    });
    (status, parse(&answer))
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("not a JSON body ({e}): {body}"))
}

fn state_of<F: ReplicaFactory>(node: &Node<F, Quiet>, log: &str) -> Value {
    node.hosted(&id(log)).expect("hosted").read_state()
}

fn version_of<F: ReplicaFactory>(node: &Node<F, Quiet>, log: &str) -> String {
    node.version_of(&id(log)).expect("hosted")
}

fn not_found() -> Value {
    json!({ "error": "not found" })
}

// ------------------------------------------------------------------ raw HTTP (601c887's helpers)

/// One raw HTTP/1.1 exchange; returns (status, header block, body).
fn exchange(port: u16, head: &str, body: Option<&str>) -> (u16, String, String) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stream = loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => break stream,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("server never came up on port {port}: {e}"),
        }
    };
    let payload = body.unwrap_or("");
    let content_type = if body.is_some() {
        "Content-Type: application/json\r\n"
    } else {
        ""
    };
    let raw = format!(
        "{head} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\
         {content_type}Content-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(raw.as_bytes()).expect("request written");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response read");
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .map(|(headers, body)| (headers.to_string(), body.to_string()))
        .expect("header/body separator");
    (status, headers, body)
}

fn request(port: u16, head: &str, body: Option<&str>) -> (u16, String) {
    let (status, _, body) = exchange(port, head, body);
    (status, body)
}

/// The value of the header `name` in a header block, case-insensitively.
fn header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

// ------------------------------------------------------------------ the 13 N tests of 601c887

#[test]
fn health_endpoint_names_the_replica_and_its_log() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);

    let answer = ask(&api, &mut node, port, "GET /api/health", None);

    assert_eq!(
        answer,
        (
            200,
            json!({ "status": "ok", "replica_id": "test-replica", "log_id": TEST_LOG_ID })
        )
    );
}

#[test]
fn log_id_endpoint_serves_the_id_on_its_own() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);

    let answer = ask(&api, &mut node, port, "GET /api/log-id", None);

    assert_eq!(answer, (200, json!({ "log_id": TEST_LOG_ID })));
}

#[test]
fn state_endpoint_is_unchanged_beside_the_metamodel_route() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);
    assert!(node.submit_op(None, json!("a")).success);

    let answer = ask(&api, &mut node, port, "GET /api/state", None);

    assert_eq!(answer, (200, json!(["a"])));
}

#[test]
fn op_endpoint_is_unchanged_beside_the_metamodel_route() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);

    let (status, body) = ask(&api, &mut node, port, "POST /api/op", Some(r#""x""#));

    assert_eq!(
        (status, body["success"].as_bool()),
        (200, Some(true)),
        "unexpected body: {body}"
    );
    assert_eq!(
        state_of(&node, TEST_LOG_ID),
        json!(["x"]),
        "the op never reached the default log"
    );
}

/// A body nesting `levels` JSON objects, the innermost holding `leaf`.
fn nested(levels: usize, leaf: usize) -> String {
    let mut body = leaf.to_string();
    for _ in 0..levels {
        body = format!("{{\"a\":{body}}}");
    }
    body
}

/// Both op routes, and both batch routes, take an operation nested past
/// serde_json's default recursion limit of 128, and the replica receives it whole.
#[test]
fn both_op_routes_take_an_operation_nested_past_the_default_recursion_limit() {
    let refused = serde_json::from_str::<Value>(&nested(200, 0))
        .expect_err("200 levels is past serde_json's default limit of 128");
    assert!(
        refused.to_string().contains("recursion limit exceeded"),
        "unexpected error: {refused}"
    );
    let (api, port) = api(WAIT);
    let mut node = node(DeepFactory, &[]);

    for (leaf, route) in [
        (1, "POST /api/op".to_string()),
        (2, format!("POST /api/model/{TEST_LOG_ID}/op")),
    ] {
        let (status, body) = ask(&api, &mut node, port, &route, Some(&nested(200, leaf)));
        assert_eq!(
            (status, body["success"].as_bool()),
            (200, Some(true)),
            "`{route}` refused a 200-level operation: {body}"
        );
    }
    for (leaf, route) in [
        (3, "POST /api/ops".to_string()),
        (5, format!("POST /api/model/{TEST_LOG_ID}/ops")),
    ] {
        let batch = format!(
            "{{\"ops\":[{},{}]}}",
            nested(200, leaf),
            nested(200, leaf + 1)
        );
        let (status, body) = ask(&api, &mut node, port, &route, Some(&batch));
        assert_eq!(
            (status, body["outcome"].as_str(), body["applied"].as_u64()),
            (200, Some("applied"), Some(2)),
            "`{route}` refused a batch of 200-level operations: {body}"
        );
    }

    let mut whole: Vec<String> = (1..=6).map(|leaf| nested(200, leaf)).collect();
    whole.sort();
    assert_eq!(
        state_of(&node, TEST_LOG_ID),
        json!(whole),
        "the replica received something other than the six operations sent"
    );
}

/// 601c887 called its private path parser; here the same paths go over HTTP. A
/// path the parser routes is answered by its route (a malformed id is 400, not
/// 404), and every path it does not route is the catch-all 404.
#[test]
fn the_path_parser_names_the_model_routes_and_nothing_by_prefix() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[BT]);
    let mut status_of = |head: &str, body: Option<&str>| ask(&api, &mut node, port, head, body);

    assert_eq!(
        status_of(&format!("GET /api/model/{BT}/state"), None).0,
        200
    );
    assert_eq!(status_of("GET /api/model/a1/state", None).0, 400);
    assert_eq!(
        status_of(&format!("POST /api/model/{BT}/op"), Some(r#""x""#)).0,
        200
    );
    assert_eq!(
        status_of(
            &format!("POST /api/model/{BT}/ops"),
            Some(r#"{"ops":["y"]}"#)
        )
        .0,
        200
    );
    assert_eq!(status_of("POST /api/ops", Some(r#"{"ops":["z"]}"#)).0, 200);
    assert_eq!(status_of("GET /api/models", None).0, 200);
    for head in [
        format!("GET /api/model/{BT}/ops"),
        format!("GET /api/model/{BT}/op"),
        format!("GET /api/model/{BT}"),
        "GET /api/state/".to_string(),
        "GET /api/pause/peer-1".to_string(),
    ] {
        assert_eq!(status_of(&head, None), (404, not_found()), "`{head}`");
    }
    assert_eq!(
        status_of("POST /api/pause/peer-1", None),
        (
            200,
            json!({ "success": true, "message": "Paused peer 'peer-1'" })
        )
    );
    assert_eq!(node.transport().paused, vec!["peer-1".to_string()]);
    assert_eq!(state_of(&node, BT), json!(["x", "y"]));
    assert_eq!(state_of(&node, TEST_LOG_ID), json!(["z"]));
}

#[test]
fn mp3_the_model_routes_parse_the_id_and_reject_a_malformed_one() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[BT, UML]);
    assert!(node.submit_op(Some(id(BT)), json!("Sequence")).success);
    assert!(node.submit_op(Some(id(UML)), json!("Class")).success);
    assert!(node.submit_op(None, json!("default")).success);

    let answer = ask(
        &api,
        &mut node,
        port,
        &format!("GET /api/model/{BT}/state"),
        None,
    );
    assert_eq!(answer, (200, json!(["Sequence"])));

    let answer = ask(
        &api,
        &mut node,
        port,
        &format!("GET /api/model/{UML}/state"),
        None,
    );
    assert_eq!(answer, (200, json!(["Class"])));

    let (status, body) = ask(&api, &mut node, port, "GET /api/model/zz/state", None);
    let expected = LogId::parse("zz").unwrap_err().to_string();
    assert_eq!(status, 400, "a malformed id reached a model: {body}");
    assert!(
        body.to_string().contains(&expected),
        "the 400 must carry the parse error `{expected}`, got: {body}"
    );

    let (status, _) = ask(
        &api,
        &mut node,
        port,
        &format!("GET /api/model/{UNHOSTED}/state"),
        None,
    );
    assert_eq!(status, 404, "a well-formed id nobody registered was served");

    let answer = ask(&api, &mut node, port, "GET /api/state", None);
    assert_eq!(answer, (200, json!(["default"])));
}

#[test]
fn the_state_routes_serve_the_version_they_read_at_as_the_etag() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[BT]);
    assert!(node.submit_op(Some(id(BT)), json!("x")).success);
    let (bt, default) = (version_of(&node, BT), version_of(&node, TEST_LOG_ID));
    assert_ne!(bt, default, "the fixture needs two different versions");

    let head = format!("GET /api/model/{BT}/state");
    let (status, headers, body) = serving(&api, &mut node, &mut (), move || {
        exchange(port, &head, None)
    });
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        header(&headers, "ETag"),
        Some(format!("\"{bt}\"").as_str()),
        "{headers}"
    );
    assert_eq!(header(&headers, "Cache-Control"), Some("no-store"));
    assert_eq!(
        header(&headers, "Access-Control-Expose-Headers"),
        Some("ETag"),
        "a page on another origin could not read the version"
    );

    let (status, headers, _) = serving(&api, &mut node, &mut (), move || {
        exchange(port, "GET /api/state", None)
    });
    assert_eq!(status, 200);
    assert_eq!(
        header(&headers, "ETag"),
        Some(format!("\"{default}\"").as_str()),
        "{headers}"
    );
}

#[test]
fn a_batch_is_one_submission_and_its_outcome_sets_the_status() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);
    let route = format!("POST /api/model/{TEST_LOG_ID}/ops");
    let read = version_of(&node, TEST_LOG_ID);

    let body = format!(r#"{{"ops":["a","b","c"],"base":"{read}"}}"#);
    let (status, parsed) = ask(&api, &mut node, port, &route, Some(&body));
    assert_eq!(status, 200, "{parsed}");
    assert_eq!(
        (
            parsed["outcome"].as_str(),
            parsed["applied"].as_u64(),
            parsed["of"].as_u64()
        ),
        (Some("applied"), Some(3), Some(3))
    );
    assert_eq!(
        parsed["version"].as_str(),
        Some(version_of(&node, TEST_LOG_ID).as_str())
    );

    // The same read again: the model moved since, so nothing is applied.
    let body = format!(r#"{{"ops":["d"],"base":"{read}"}}"#);
    let (status, parsed) = ask(&api, &mut node, port, &route, Some(&body));
    assert_eq!(
        (status, parsed["outcome"].as_str()),
        (409, Some("stale")),
        "{parsed}"
    );
    assert_eq!(parsed["applied"].as_u64(), Some(0));
    assert_eq!(state_of(&node, TEST_LOG_ID), json!(["a", "b", "c"]));

    // The toy refuses a non-string op: the second of three.
    let body = format!(
        r#"{{"ops":["d",1,"e"],"base":"{}"}}"#,
        version_of(&node, TEST_LOG_ID)
    );
    let (status, parsed) = ask(&api, &mut node, port, &route, Some(&body));
    assert_eq!(
        (status, parsed["outcome"].as_str()),
        (422, Some("refused")),
        "{parsed}"
    );
    assert_eq!(
        (parsed["applied"].as_u64(), parsed["refused_at"].as_u64()),
        (Some(1), Some(1)),
        "the answer must say exactly which prefix was applied"
    );
    assert_eq!(state_of(&node, TEST_LOG_ID), json!(["a", "b", "c", "d"]));

    let (status, parsed) = ask(
        &api,
        &mut node,
        port,
        &format!("POST /api/model/{UNHOSTED}/ops"),
        Some(r#"{"ops":["x"]}"#),
    );
    assert_eq!(
        (status, parsed["outcome"].as_str()),
        (404, Some("nothosted")),
        "{parsed}"
    );

    // With no base at all, the batch is applied to whatever the log holds.
    let (status, parsed) = ask(&api, &mut node, port, &route, Some(r#"{"ops":["f"]}"#));
    assert_eq!(status, 200, "{parsed}");
    assert_eq!(
        state_of(&node, TEST_LOG_ID),
        json!(["a", "b", "c", "d", "f"])
    );
}

#[test]
fn a_batch_body_of_the_wrong_shape_is_400_and_reaches_nothing() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);
    let route = format!("POST /api/model/{TEST_LOG_ID}/ops");
    for body in [
        r#"["a"]"#,
        r#"{"ops":"a"}"#,
        // 601c887's third case was an op of the wrong type; with no op type,
        // a base of the wrong type stands in.
        r#"{"ops":["a"],"base":7}"#,
        // A misspelled `base` would turn the stale check off in silence.
        r#"{"ops":["a"],"bsae":"v.1"}"#,
        r#"{"ops":["a"]} trailing"#,
    ] {
        let head = route.clone();
        let (status, _, answer) = serving(&api, &mut node, &mut (), move || {
            exchange(port, &head, Some(body))
        });
        assert_eq!(status, 400, "`{body}` was taken: {answer}");
    }
    assert_eq!(
        state_of(&node, TEST_LOG_ID),
        json!([]),
        "a malformed batch reached the log"
    );
    assert_eq!(
        node.hosted(&id(TEST_LOG_ID))
            .expect("hosted")
            .progress()
            .delivered,
        0
    );
}

/// The ip10 504: a 504 is answered only for a submission the loop had not taken,
/// and the loop, when it gets to it, applies nothing.
#[test]
fn a_batch_the_loop_did_not_take_in_time_is_expired_and_never_applied() {
    let (api, port) = api(Duration::from_millis(200));
    let mut node = node(ToyFactory, &[]);
    let client = thread::spawn(move || {
        request(
            port,
            &format!("POST /api/model/{TEST_LOG_ID}/ops"),
            Some(r#"{"ops":["a","b"]}"#),
        )
    });
    // A loop busy delivering a backlog, past the request's deadline.
    thread::sleep(Duration::from_millis(600));
    let (status, answer) = client.join().expect("the client thread");

    assert_eq!(
        (status, parse(&answer)),
        (
            504,
            json!({
                "outcome": "expired",
                "applied": 0,
                "of": 2,
                "message": "the node did not take this batch in time; none of it was applied, and none of it ever will be",
                "version": null,
            })
        )
    );
    for _ in 0..5 {
        api.serve(&mut node, &mut ());
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        state_of(&node, TEST_LOG_ID),
        json!([]),
        "the loop applied a batch its caller had been told was never applied"
    );
}

/// The other side of the race: a submission the loop took before the deadline is
/// answered with what the loop did, however long that took.
#[test]
fn a_batch_the_loop_took_in_time_is_answered_even_past_the_deadline() {
    let (api, port) = api(Duration::from_millis(200));
    let mut node = node(SlowFactory, &[]);

    let (status, _, answer) = serving(&api, &mut node, &mut (), move || {
        exchange(
            port,
            &format!("POST /api/model/{TEST_LOG_ID}/ops"),
            Some(r#"{"ops":["a","b"]}"#),
        )
    });

    let parsed = parse(&answer);
    assert_eq!(
        (
            status,
            parsed["outcome"].as_str(),
            parsed["applied"].as_u64()
        ),
        (200, Some("applied"), Some(2)),
        "{answer}"
    );
    assert_eq!(state_of(&node, TEST_LOG_ID), json!(["a", "b"]));
}

#[test]
fn an_operation_the_loop_did_not_take_in_time_is_expired_and_never_applied() {
    let (api, port) = api(Duration::from_millis(200));
    let mut node = node(ToyFactory, &[]);
    let client = thread::spawn(move || request(port, "POST /api/op", Some(r#""a""#)));
    thread::sleep(Duration::from_millis(600));
    let (status, answer) = client.join().expect("the client thread");

    assert_eq!(
        (status, parse(&answer)),
        (
            504,
            json!({
                "error": "the node did not take this operation in time; it was not applied and never will be",
                "applied": false,
            })
        )
    );
    for _ in 0..5 {
        api.serve(&mut node, &mut ());
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(state_of(&node, TEST_LOG_ID), json!([]));
}

// ------------------------------------------------------------------ the extension point

/// An application's own routes: it hosts a log by id and counts the hosted logs,
/// and it tries to answer /api/health, which the crate's own route keeps.
struct App;

impl<F: ReplicaFactory, T: CrdtTransport> Routes<F, T> for App {
    fn answer(
        &mut self,
        node: &mut Node<F, T>,
        method: &str,
        path: &str,
        body: &str,
    ) -> Option<(u16, Value)> {
        match (method, path) {
            ("POST", "/api/app/host") => {
                let body: Value = serde_json::from_str(body).ok()?;
                let log = LogId::parse(body["log"].as_str()?).ok()?;
                Some(match node.host_log(log.clone()) {
                    Ok(()) => (201, json!({ "hosted": log.as_str() })),
                    Err(e) => (409, json!({ "error": e.to_string() })),
                })
            }
            ("GET", "/api/app/count") => {
                Some((200, json!({ "hosted": node.hosted_logs().count() })))
            }
            ("GET", "/api/health") => Some((200, json!({ "shadowed": true }))),
            _ => None,
        }
    }
}

#[test]
fn an_application_route_is_served_through_the_extension_point() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[]);
    let mut call = |head: &str, body: Option<&str>| {
        let (head, body) = (head.to_string(), body.map(str::to_string));
        let (status, _, answer) = serving(&api, &mut node, &mut App, move || {
            exchange(port, &head, body.as_deref())
        });
        (status, parse(&answer))
    };
    let host = format!(r#"{{"log":"{BT}"}}"#);

    assert_eq!(
        call("POST /api/app/host", Some(&host)),
        (201, json!({ "hosted": BT }))
    );
    assert_eq!(call("POST /api/app/host", Some(&host)).0, 409);
    assert_eq!(
        call("GET /api/app/count", None),
        (200, json!({ "hosted": 2 }))
    );
    let (status, models) = call("GET /api/models", None);
    assert_eq!(status, 200);
    let ids: Vec<&str> = models["models"]
        .as_array()
        .expect("a models array")
        .iter()
        .filter_map(|model| model["model_id"].as_str())
        .collect();
    assert_eq!(
        ids,
        vec![TEST_LOG_ID, BT],
        "the application's hosting is not what the node lists"
    );
    assert_eq!(call("GET /api/app/other", None), (404, not_found()));
    assert_eq!(
        call("GET /api/health", None),
        (
            200,
            json!({ "status": "ok", "replica_id": "test-replica", "log_id": TEST_LOG_ID })
        ),
        "an application route shadowed a generic one"
    );
    assert!(node.hosts(&id(BT)));
}

// ------------------------------------------------------------------ the rest of the generic routes

/// The generic routes the 13 do not reach, each answering as on 601c887; and the
/// routes left for later answered as any unknown path, never stubbed: resume and
/// resume-all (601c887 requests a sync on resume, S5b's catch-up) and operations
/// (no ReplicaPort call reads the operation log).
#[test]
fn the_other_generic_routes_answer_as_on_601c887_and_the_later_routes_are_not_served() {
    let (api, port) = api(WAIT);
    let mut node = node(ToyFactory, &[BT]);
    assert!(node.submit_op(Some(id(BT)), json!("x")).success);
    node.handle_transport_message(
        "bob".to_string(),
        TransportMessage::Event {
            event: json!({ "log": UNHOSTED, "origin": "bob", "op": "q", "log_id": UNHOSTED }),
        },
    );
    let (metrics, bt_metrics) = (node.metrics(), node.log_metrics(&id(BT)).expect("hosted"));
    assert_eq!(
        metrics["frames_not_hosted"],
        json!(1),
        "the fixture filters one frame"
    );
    let bt = version_of(&node, BT);
    let mut call = |head: &str, body: Option<&str>| ask(&api, &mut node, port, head, body);

    assert_eq!(call("GET /api/metrics", None), (200, metrics));
    assert_eq!(
        call(&format!("GET /api/model/{BT}/metrics"), None),
        (200, bt_metrics)
    );
    assert_eq!(
        call(&format!("GET /api/model/{UNHOSTED}/metrics"), None),
        (404, not_found())
    );
    assert_eq!(call("GET /api/model/zz/metrics", None).0, 400);

    let (status, models) = call("GET /api/models", None);
    assert_eq!(status, 200);
    let ids: Vec<&str> = models["models"]
        .as_array()
        .expect("a models array")
        .iter()
        .filter_map(|model| model["model_id"].as_str())
        .collect();
    assert_eq!(ids, vec![TEST_LOG_ID, BT], "{models}");
    assert_eq!(models["seen"], json!([UNHOSTED]), "{models}");

    assert_eq!(
        call("GET /api/peers", None),
        (
            200,
            json!({ "peers": [{ "id": "bob", "status": "Connected", "buffered": 0 }] })
        )
    );
    assert_eq!(
        call("POST /api/pause/bob", None),
        (
            200,
            json!({ "success": true, "message": "Paused peer 'bob'" })
        )
    );
    assert_eq!(
        call("POST /api/pause-all", None),
        (
            200,
            json!({ "success": true, "message": "All peers paused" })
        )
    );
    assert_eq!(
        call("GET /api/peers", None),
        (
            200,
            json!({ "peers": [{ "id": "bob", "status": "Paused", "buffered": 0 }] })
        )
    );
    assert_eq!(
        call("POST /api/leave", None),
        (
            200,
            json!({ "success": false, "message": "discovery is not enabled; nothing to leave" })
        )
    );
    assert_eq!(
        call(&format!("POST /api/model/{UNHOSTED}/op"), Some(r#""x""#)),
        (404, not_found())
    );
    assert_eq!(call("POST /api/model/zz/op", Some(r#""x""#)).0, 400);

    for (head, body) in [
        ("POST /api/resume/bob", None),
        ("POST /api/resume-all", None),
        ("GET /api/operations", None),
    ] {
        assert_eq!(
            call(head, body),
            (404, not_found()),
            "`{head}` is later, not stubbed"
        );
    }

    let head = format!("GET /api/model/{BT}/state/identified");
    let (status, headers, body) = serving(&api, &mut node, &mut (), move || {
        exchange(port, &head, None)
    });
    assert_eq!((status, parse(&body)), (200, json!(["x"])));
    assert_eq!(
        header(&headers, "ETag"),
        Some(format!("\"{bt}\"").as_str()),
        "{headers}"
    );

    assert_eq!(
        node.transport().paused,
        vec!["bob".to_string(), "*".to_string()]
    );
    assert_eq!(
        node.transport().resumed,
        0,
        "a later route reached the transport"
    );
}
