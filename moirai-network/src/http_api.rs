//! HTTP API implementation for [`GenericNode`].
//!
//! This module is intentionally transport-agnostic: it speaks to the node via
//! channels and control commands.
//!
//! [`GenericNode`]: crate::generic::GenericNode

use std::fmt::Display;
use std::io::{Cursor, Read};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, PoisonError, RwLock};
use std::thread;
use std::time::Duration;

use moirai_protocol::log_id::LogId;
use serde_json::json;
use tiny_http::{Header, Method, Request, Response, Server};

use crate::generic::{
    ControlCmd, NetworkOp, OpEnvelope, OpResult, RegisterRefused, ServeRefused, ServedDescriptor,
};

/// How long a request waits for the node's event loop to answer before it
/// gives up with a 504.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// Start the optional HTTP API on the given port.
///
/// Model-scoped endpoints, where `{id}` is a [`LogId`] — 32 lowercase hex
/// characters, answered 400 with the parse error otherwise, and 404 when the
/// node hosts no log by that id:
/// - `GET  /api/models`               the hosted models as `{model_id, metamodel_id}`
/// - `POST /api/models`               register a model: `{metamodel_id}` creates
///   one and the node mints its id; `{model_id, metamodel_id}` joins one by
///   id and writes nothing. 409 for an id already hosted, 422 for a
///   `metamodel_id` naming no descriptor the node holds
/// - `GET  /api/metamodels`           the descriptors the node holds, as the
///   application listed them
/// - `POST /api/metamodels`           serve one more descriptor, now: the body
///   is the descriptor text, 201 with the listing when the node did not hold
///   it, 200 when it did, 422 with the reason the application could not
///   describe it, 501 when the node was started without the hook
/// - `GET  /api/model/{id}/state`     that model's state as JSON
/// - `POST /api/model/{id}/op`        submit an operation to that model
/// - `GET  /api/model/{id}/metamodel` the descriptor that model was registered under
/// - `GET  /api/model/{id}/metrics`   that model's counters, `foreign_log_refusals` included
///
/// Unscoped endpoints, which answer for the node's default log:
/// - `POST /api/op`              submit an operation (JSON body = serialized op)
/// - `GET  /api/health`          health check, names the replica and its log
/// - `GET  /api/log-id`          the log this replica hosts, on its own
/// - `GET  /api/state`           query current CRDT state as JSON
/// - `GET  /api/metamodel`       the first metamodel descriptor the node holds
///   (see [`crate::generic::GenericNode::serve_metamodels`]); 404 without one,
///   exactly like any unknown path
/// - `GET  /api/metrics`         causal-stability and log-size counters, beside
///   the node's `hosted_logs` and `frames_not_hosted`
/// - `GET  /api/operations`      list operations delivered to this replica
///   (display only — it double-counts remote deliveries; use `/api/metrics`)
/// - `POST /api/pause/<peer>`    pause a peer connection
/// - `POST /api/resume/<peer>`   resume a peer connection
/// - `POST /api/pause-all`       pause all peers
/// - `POST /api/resume-all`      resume all peers
/// - `GET  /api/peers`           list peers and status
/// - `POST /api/leave`           deregister from the bootnode session
pub(crate) fn start_http_api<O: NetworkOp>(
    port: u16,
    replica_id: String,
    log_id: LogId,
    sender: Sender<OpEnvelope<O>>,
    ctrl: Sender<ControlCmd>,
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
) {
    thread::spawn(move || {
        let addr = format!("0.0.0.0:{}", port);
        let server = Server::http(&addr).expect("Failed to start HTTP server");
        eprintln!("[{}] HTTP API listening on {}", replica_id, addr);

        let api = Api {
            replica_id,
            default_log: log_id,
            sender,
            ctrl,
            metamodels,
        };
        for request in server.incoming_requests() {
            api.serve(request);
        }
    });
}

/// What the HTTP thread owns: the node's fixed identity, the channels to its
/// event loop, and a read handle on the descriptor list. Everything that
/// changes while the node runs, the hosted set above all, is read live —
/// through a [`ControlCmd`], or, for the descriptors, through the shared
/// list the event loop writes.
struct Api<O> {
    replica_id: String,
    default_log: LogId,
    sender: Sender<OpEnvelope<O>>,
    ctrl: Sender<ControlCmd>,
    /// Shared with the event loop, which is the only writer: a descriptor
    /// posted to a running node is listed by the very next request, and this
    /// thread never mutates the list itself.
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
}

type Reply = Response<Cursor<Vec<u8>>>;

/// The leaf of a `/api/model/{id}/...` route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelLeaf {
    State,
    Op,
    Metamodel,
    Metrics,
}

/// The route a request names. Parsed from the path split on `/`, so that
/// `{id}` is one segment and nothing is matched by prefix except the two
/// peer routes, whose remainder is the peer id verbatim.
#[derive(Debug, PartialEq, Eq)]
enum Route<'a> {
    Health,
    LogId,
    State,
    Metamodel,
    Metrics,
    Operations,
    Op,
    Pause(&'a str),
    Resume(&'a str),
    PauseAll,
    ResumeAll,
    Leave,
    Peers,
    Models,
    Register,
    Metamodels,
    AddMetamodel,
    Model { id: &'a str, leaf: ModelLeaf },
    Unknown,
}

fn route<'a>(method: &Method, path: &'a str) -> Route<'a> {
    if *method == Method::Post {
        if let Some(peer) = path.strip_prefix("/api/pause/") {
            return Route::Pause(peer);
        }
        if let Some(peer) = path.strip_prefix("/api/resume/") {
            return Route::Resume(peer);
        }
    }
    let Some(rest) = path.strip_prefix('/') else {
        return Route::Unknown;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    match (method, segments.as_slice()) {
        (&Method::Get, ["api", "health"]) => Route::Health,
        (&Method::Get, ["api", "log-id"]) => Route::LogId,
        (&Method::Get, ["api", "state"]) => Route::State,
        (&Method::Get, ["api", "metamodel"]) => Route::Metamodel,
        (&Method::Get, ["api", "metrics"]) => Route::Metrics,
        (&Method::Get, ["api", "operations"]) => Route::Operations,
        (&Method::Post, ["api", "op"]) => Route::Op,
        (&Method::Post, ["api", "pause-all"]) => Route::PauseAll,
        (&Method::Post, ["api", "resume-all"]) => Route::ResumeAll,
        (&Method::Post, ["api", "leave"]) => Route::Leave,
        (&Method::Get, ["api", "peers"]) => Route::Peers,
        (&Method::Get, ["api", "models"]) => Route::Models,
        (&Method::Post, ["api", "models"]) => Route::Register,
        (&Method::Get, ["api", "metamodels"]) => Route::Metamodels,
        (&Method::Post, ["api", "metamodels"]) => Route::AddMetamodel,
        (&Method::Get, ["api", "model", id, "state"]) => Route::Model {
            id,
            leaf: ModelLeaf::State,
        },
        (&Method::Post, ["api", "model", id, "op"]) => Route::Model {
            id,
            leaf: ModelLeaf::Op,
        },
        (&Method::Get, ["api", "model", id, "metamodel"]) => Route::Model {
            id,
            leaf: ModelLeaf::Metamodel,
        },
        (&Method::Get, ["api", "model", id, "metrics"]) => Route::Model {
            id,
            leaf: ModelLeaf::Metrics,
        },
        _ => Route::Unknown,
    }
}

fn json_header() -> Header {
    Header::from_bytes(b"Content-Type", b"application/json").unwrap()
}

/// A reply whose body is already JSON text.
fn json_text(status: u16, body: String) -> Reply {
    Response::from_string(body)
        .with_status_code(status)
        .with_header(json_header())
}

fn json_value(status: u16, body: &serde_json::Value) -> Reply {
    json_text(status, body.to_string())
}

fn error(status: u16, message: impl Display) -> Reply {
    json_value(status, &json!({ "error": message.to_string() }))
}

fn not_found() -> Reply {
    error(404, "not found")
}

fn timeout() -> Reply {
    error(504, "timeout")
}

fn op_result(result: &OpResult) -> Reply {
    match serde_json::to_string(result) {
        Ok(body) => json_text(200, body),
        Err(_) => error(200, "serialize"),
    }
}

fn add_cors(mut resp: Reply) -> Reply {
    resp.add_header(Header::from_bytes(b"Access-Control-Allow-Origin", b"*").unwrap());
    resp.add_header(
        Header::from_bytes(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS").unwrap(),
    );
    resp.add_header(Header::from_bytes(b"Access-Control-Allow-Headers", b"Content-Type").unwrap());
    resp
}

/// The request body, or the 400 to answer with.
fn read_body(request: &mut Request) -> Result<String, Reply> {
    let mut body = String::new();
    match Read::read_to_string(&mut request.as_reader(), &mut body) {
        Ok(_) => Ok(body),
        Err(_) => Err(error(400, "Failed to read body")),
    }
}

impl<O: NetworkOp> Api<O> {
    fn serve(&self, mut request: Request) {
        let path = request.url().to_string();
        let method = request.method().clone();

        let reply = if method == Method::Options {
            Response::from_string("").with_status_code(204)
        } else {
            match route(&method, &path) {
                Route::Health => json_value(
                    200,
                    &json!({
                        "status": "ok",
                        "replica_id": self.replica_id,
                        "log_id": self.default_log.as_str(),
                    }),
                ),
                Route::LogId => json_value(200, &json!({ "log_id": self.default_log.as_str() })),
                Route::State => self.query(|reply| ControlCmd::Query { reply }),
                Route::Metamodel => self.first_descriptor(),
                Route::Metrics => self.query(|reply| ControlCmd::Metrics { reply }),
                Route::Operations => self.query(|reply| ControlCmd::Operations { reply }),
                Route::Peers => self.query(|reply| ControlCmd::Peers { reply }),
                Route::Models => self.query(|reply| ControlCmd::Models { reply }),
                Route::Metamodels => self.listing(),
                Route::AddMetamodel => self.add_metamodel(&mut request),
                Route::Op => self.submit(&mut request, None),
                Route::Register => self.register(&mut request),
                Route::Model { id, leaf } => self.model(&mut request, id, leaf),
                Route::Pause(peer) => self.control(|reply| ControlCmd::Pause {
                    peer_id: peer.to_string(),
                    reply,
                }),
                Route::Resume(peer) => self.control(|reply| ControlCmd::Resume {
                    peer_id: peer.to_string(),
                    reply,
                }),
                Route::PauseAll => self.control(|reply| ControlCmd::PauseAll { reply }),
                Route::ResumeAll => self.control(|reply| ControlCmd::ResumeAll { reply }),
                Route::Leave => self.control(|reply| ControlCmd::Leave { reply }),
                Route::Unknown => not_found(),
            }
        };
        let _ = request.respond(add_cors(reply));
    }

    /// One round trip to the event loop: `None` when it did not answer in
    /// time, or is gone.
    fn ask<R>(&self, make: impl FnOnce(Sender<R>) -> ControlCmd) -> Option<R> {
        let (tx, rx) = mpsc::channel();
        self.ctrl.send(make(tx)).ok()?;
        rx.recv_timeout(REPLY_TIMEOUT).ok()
    }

    /// A command answered with a JSON value, served as it is.
    fn query(&self, make: impl FnOnce(Sender<serde_json::Value>) -> ControlCmd) -> Reply {
        match self.ask(make) {
            Some(value) => json_text(200, value.to_string()),
            None => timeout(),
        }
    }

    /// A command answered with an [`OpResult`].
    fn control(&self, make: impl FnOnce(Sender<OpResult>) -> ControlCmd) -> Reply {
        let result = self.ask(make).unwrap_or(OpResult {
            success: false,
            message: "timeout".into(),
        });
        op_result(&result)
    }

    /// `GET /api/metamodel`: the first descriptor the node holds. Byte-identical
    /// to the catch-all 404 when there is none, so a node that never called
    /// `serve_metamodel` keeps its old behaviour in full.
    fn first_descriptor(&self) -> Reply {
        match self.metamodels().first() {
            Some(descriptor) => json_text(200, descriptor.text.clone()),
            None => not_found(),
        }
    }

    /// The descriptors the node serves right now.
    fn metamodels(&self) -> std::sync::RwLockReadGuard<'_, Vec<ServedDescriptor>> {
        self.metamodels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The listing entries, verbatim, as a JSON array.
    fn listing_entries(&self) -> Vec<serde_json::Value> {
        self.metamodels()
            .iter()
            .filter(|descriptor| !descriptor.listing.is_null())
            .map(|descriptor| descriptor.listing.clone())
            .collect()
    }

    /// `GET /api/metamodels`: the listing entries, verbatim, of every
    /// descriptor the application listed.
    fn listing(&self) -> Reply {
        json_value(200, &json!({ "metamodels": self.listing_entries() }))
    }

    /// `POST /api/metamodels`: the body is one descriptor's text, and the
    /// event loop is asked to serve it.
    ///
    /// The body is not parsed here, not even as JSON: what a descriptor is
    /// belongs to the application, which describes the text through the hook
    /// it installed, and the answer is either the entry it listed or the
    /// sentence it refused with.
    fn add_metamodel(&self, request: &mut Request) -> Reply {
        let text = match read_body(request) {
            Ok(text) => text,
            Err(reply) => return reply,
        };
        match self.ask(|reply| ControlCmd::AddMetamodel { text, reply }) {
            Some(Ok(served)) => json_value(
                if served.added { 201 } else { 200 },
                &json!({
                    "added": served.added,
                    "metamodel": served.listing,
                    "metamodels": self.listing_entries(),
                }),
            ),
            Some(Err(refused @ ServeRefused::Unreadable(_))) => error(422, refused),
            Some(Err(refused @ ServeRefused::NotEnabled)) => error(501, refused),
            None => timeout(),
        }
    }

    /// `GET /api/model/{id}/metamodel`: the descriptor under the key the model
    /// was registered with. A hosted model with no binding — the default log
    /// — is served the first descriptor, as `/api/metamodel` is.
    fn descriptor_for(&self, key: Option<&str>) -> Reply {
        match key {
            Some(key) => match self.metamodels().iter().find(|d| d.key == key) {
                Some(descriptor) => json_text(200, descriptor.text.clone()),
                None => not_found(),
            },
            None => self.first_descriptor(),
        }
    }

    /// `POST /api/op` and `POST /api/model/{id}/op`: parse the body as an
    /// operation and hand it to the event loop.
    fn submit(&self, request: &mut Request, log_id: Option<LogId>) -> Reply {
        let body = match read_body(request) {
            Ok(body) => body,
            Err(reply) => return reply,
        };
        let op = match serde_json::from_str::<O>(&body) {
            Ok(op) => op,
            Err(e) => return error(400, format!("Invalid op JSON: {e}")),
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        let envelope = OpEnvelope {
            op,
            log_id,
            reply: reply_tx,
        };
        if self.sender.send(envelope).is_err() {
            return error(500, "channel closed");
        }
        match reply_rx.recv_timeout(REPLY_TIMEOUT) {
            Ok(result) => op_result(&result),
            Err(_) => timeout(),
        }
    }

    /// `POST /api/models`.
    fn register(&self, request: &mut Request) -> Reply {
        let body = match read_body(request) {
            Ok(body) => body,
            Err(reply) => return reply,
        };
        let body: serde_json::Value = match serde_json::from_str(&body) {
            Ok(body) => body,
            Err(e) => return error(400, format!("Invalid JSON: {e}")),
        };
        let Some(metamodel_id) = body.get("metamodel_id").cloned() else {
            return error(400, "`metamodel_id` is required");
        };
        let model_id = match body.get("model_id") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(id)) => match LogId::parse(id) {
                Ok(id) => Some(id),
                Err(e) => return error(400, format!("`model_id`: {e}")),
            },
            Some(_) => return error(400, "`model_id` must be a string"),
        };
        match self.ask(|reply| ControlCmd::Register {
            model_id,
            metamodel_id: metamodel_id.clone(),
            reply,
        }) {
            Some(Ok(registered)) => json_value(
                if registered.created { 201 } else { 200 },
                &json!({
                    "model_id": registered.model_id.as_str(),
                    "metamodel_id": metamodel_id,
                    "created": registered.created,
                }),
            ),
            Some(Err(refused @ RegisterRefused::AlreadyHosted(_))) => error(409, refused),
            Some(Err(refused @ RegisterRefused::UnknownMetamodel(_))) => error(422, refused),
            Some(Err(refused @ RegisterRefused::NotEnabled)) => error(501, refused),
            None => timeout(),
        }
    }

    /// `/api/model/{id}/...`.
    fn model(&self, request: &mut Request, id: &str, leaf: ModelLeaf) -> Reply {
        let log_id = match LogId::parse(id) {
            Ok(log_id) => log_id,
            Err(e) => return error(400, e),
        };
        match leaf {
            ModelLeaf::State => match self.ask(|reply| ControlCmd::QueryLog { log_id, reply }) {
                Some(Some(state)) => json_text(200, state.to_string()),
                Some(None) => not_found(),
                None => timeout(),
            },
            ModelLeaf::Metrics => {
                match self.ask(|reply| ControlCmd::LogMetrics { log_id, reply }) {
                    Some(Some(metrics)) => json_text(200, metrics.to_string()),
                    Some(None) => not_found(),
                    None => timeout(),
                }
            }
            ModelLeaf::Metamodel => match self.ask(|reply| ControlCmd::Binding { log_id, reply }) {
                Some(Some(key)) => self.descriptor_for(key.as_deref()),
                Some(None) => not_found(),
                None => timeout(),
            },
            ModelLeaf::Op => match self.ask(|reply| ControlCmd::Hosts {
                log_id: log_id.clone(),
                reply,
            }) {
                Some(true) => self.submit(request, Some(log_id)),
                Some(false) => not_found(),
                None => timeout(),
            },
        }
    }
}

/// Raw HTTP against a spawned API, shared with the node tests in `generic.rs`
/// that drive a real event loop through the same routes.
#[cfg(test)]
pub(crate) mod testing {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};

    /// A port nobody is listening on. Grab one, release it, and let the
    /// server re-bind it: the race window is negligible on loopback and only
    /// affects tests.
    pub(crate) fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback bind");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        port
    }

    /// One raw HTTP/1.1 exchange; returns `(status, body)`.
    pub(crate) fn request(port: u16, head: &str, body: Option<&str>) -> (u16, String) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => break stream,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("server never came up on port {port}: {e}"),
            }
        };

        let payload = body.unwrap_or("");
        let raw = format!(
            "{head} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\
             Content-Length: {}\r\n\r\n{payload}",
            payload.len()
        );
        stream.write_all(raw.as_bytes()).expect("request written");

        let mut response = String::new();
        std::io::Read::read_to_string(&mut stream, &mut response).expect("response read");

        let status: u16 = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status line");
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .expect("header/body separator");
        (status, body)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, RwLock};

    use moirai_protocol::log_id::LogId;
    use moirai_protocol::utils::intern_str::{InternalizeOp, Interner};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::testing::{free_port, request};
    use super::{route, start_http_api, ModelLeaf, Route};
    use crate::generic::{
        ControlCmd, OpEnvelope, OpResult, ServeRefused, Served, ServedDescriptor,
    };

    /// The log id every spawned API reports, fixed so bodies can be asserted
    /// verbatim.
    const TEST_LOG_ID: &str = "00112233445566778899aabbccddeeff";

    /// Minimal operation satisfying the `NetworkOp` bounds, so the HTTP layer
    /// can be exercised without a replica behind it.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestOp {
        value: i64,
    }

    impl InternalizeOp for TestOp {
        fn internalize(self, _interner: &Interner) -> Self {
            self
        }
    }

    /// The channels a spawned API is wired to, kept alive for the test's
    /// duration so the server never observes a closed channel.
    struct Api {
        port: u16,
        op_rx: mpsc::Receiver<OpEnvelope<TestOp>>,
        ctrl_rx: mpsc::Receiver<ControlCmd>,
        /// The list the event loop would own, so a test can play the loop:
        /// take the command, write the list, answer.
        metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
    }

    fn spawn_api(metamodel: Option<String>) -> Api {
        let port = free_port();
        let (op_tx, op_rx) = mpsc::channel();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();
        let log_id = LogId::parse(TEST_LOG_ID).expect("a fixed, valid log id");
        let metamodels = Arc::new(RwLock::new(
            metamodel
                .into_iter()
                .map(ServedDescriptor::unlisted)
                .collect::<Vec<_>>(),
        ));
        start_http_api::<TestOp>(
            port,
            "test-replica".into(),
            log_id,
            op_tx,
            ctrl_tx,
            Arc::clone(&metamodels),
        );

        Api {
            port,
            op_rx,
            ctrl_rx,
            metamodels,
        }
    }

    #[test]
    fn metamodel_endpoint_serves_the_configured_descriptor() {
        let descriptor = r#"{"formatVersion":1,"package":"demo"}"#;
        let api = spawn_api(Some(descriptor.to_string()));

        let (status, body) = request(api.port, "GET /api/metamodel", None);

        assert_eq!((status, body.as_str()), (200, descriptor));
    }

    #[test]
    fn metamodel_endpoint_stays_404_without_a_descriptor() {
        let api = spawn_api(None);

        let (status, body) = request(api.port, "GET /api/metamodel", None);

        assert_eq!((status, body.as_str()), (404, r#"{"error":"not found"}"#));
    }

    #[test]
    fn health_endpoint_names_the_replica_and_its_log() {
        let api = spawn_api(Some("{}".to_string()));

        let (status, body) = request(api.port, "GET /api/health", None);

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(
            (status, parsed),
            (
                200,
                json!({
                    "status": "ok",
                    "replica_id": "test-replica",
                    "log_id": TEST_LOG_ID,
                })
            )
        );
    }

    #[test]
    fn log_id_endpoint_serves_the_id_on_its_own() {
        let api = spawn_api(None);

        let (status, body) = request(api.port, "GET /api/log-id", None);

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!((status, parsed), (200, json!({ "log_id": TEST_LOG_ID })));
    }

    #[test]
    fn state_endpoint_is_unchanged_beside_the_metamodel_route() {
        let api = spawn_api(Some("{}".to_string()));
        let state = json!({"json": "Unset"});
        let answer = state.clone();
        let ctrl_rx = api.ctrl_rx;
        std::thread::spawn(move || {
            if let Ok(ControlCmd::Query { reply }) = ctrl_rx.recv() {
                let _ = reply.send(answer);
            }
        });

        let (status, body) = request(api.port, "GET /api/state", None);

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!((status, parsed), (200, state));
    }

    #[test]
    fn op_endpoint_is_unchanged_beside_the_metamodel_route() {
        let api = spawn_api(Some("{}".to_string()));
        let op_rx = api.op_rx;
        std::thread::spawn(move || {
            if let Ok(envelope) = op_rx.recv() {
                let _ = envelope.reply.send(OpResult {
                    success: true,
                    message: format!("applied {:?}", envelope.op),
                });
            }
        });

        let (status, body) = request(api.port, "POST /api/op", Some(r#"{"value":3}"#));

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(
            (status, parsed["success"].as_bool()),
            (200, Some(true)),
            "unexpected body: {body}"
        );
    }

    #[test]
    fn the_path_parser_names_the_model_routes_and_nothing_by_prefix() {
        use tiny_http::Method;

        assert_eq!(
            route(&Method::Get, "/api/model/a1/state"),
            Route::Model {
                id: "a1",
                leaf: ModelLeaf::State
            }
        );
        assert_eq!(
            route(&Method::Post, "/api/model/a1/op"),
            Route::Model {
                id: "a1",
                leaf: ModelLeaf::Op
            }
        );
        assert_eq!(route(&Method::Get, "/api/model/a1/op"), Route::Unknown);
        assert_eq!(route(&Method::Get, "/api/model/a1"), Route::Unknown);
        assert_eq!(route(&Method::Get, "/api/models"), Route::Models);
        assert_eq!(route(&Method::Post, "/api/models"), Route::Register);
        assert_eq!(route(&Method::Get, "/api/metamodels"), Route::Metamodels);
        assert_eq!(route(&Method::Post, "/api/metamodels"), Route::AddMetamodel);
        assert_eq!(route(&Method::Get, "/api/state/"), Route::Unknown);
        assert_eq!(
            route(&Method::Post, "/api/pause/peer-1"),
            Route::Pause("peer-1")
        );
        assert_eq!(route(&Method::Get, "/api/pause/peer-1"), Route::Unknown);
    }

    /// A node answering for two models: `a1b2…` holds the behaviour tree and
    /// `c3d4…` the UML model, the default log a third document.
    const BT: &str = "a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2";
    const UML: &str = "c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d4";

    fn answering_for_two_models(ctrl_rx: mpsc::Receiver<ControlCmd>) {
        std::thread::spawn(move || {
            while let Ok(cmd) = ctrl_rx.recv() {
                match cmd {
                    ControlCmd::Query { reply } => {
                        let _ = reply.send(json!({ "default": true }));
                    }
                    ControlCmd::QueryLog { log_id, reply } => {
                        let state = match log_id.as_str() {
                            BT => Some(json!({ "Sequence": {} })),
                            UML => Some(json!({ "Class": {} })),
                            _ => None,
                        };
                        let _ = reply.send(state);
                    }
                    ControlCmd::Hosts { log_id, reply } => {
                        let _ = reply.send(matches!(log_id.as_str(), BT | UML));
                    }
                    _ => {}
                }
            }
        });
    }

    #[test]
    fn mp3_the_model_routes_parse_the_id_and_reject_a_malformed_one() {
        let api = spawn_api(None);
        answering_for_two_models(api.ctrl_rx);
        let parse =
            |body: &str| serde_json::from_str::<serde_json::Value>(body).expect("json body");

        let (status, body) = request(api.port, &format!("GET /api/model/{BT}/state"), None);
        assert_eq!((status, parse(&body)), (200, json!({ "Sequence": {} })));

        let (status, body) = request(api.port, &format!("GET /api/model/{UML}/state"), None);
        assert_eq!((status, parse(&body)), (200, json!({ "Class": {} })));

        let (status, body) = request(api.port, "GET /api/model/zz/state", None);
        let expected = LogId::parse("zz").unwrap_err().to_string();
        assert_eq!(status, 400, "a malformed id reached a model: {body}");
        assert!(
            body.contains(&expected),
            "the 400 must carry the parse error `{expected}`, got: {body}"
        );

        let unregistered = "e5f6e5f6e5f6e5f6e5f6e5f6e5f6e5f6";
        let (status, _) = request(
            api.port,
            &format!("GET /api/model/{unregistered}/state"),
            None,
        );
        assert_eq!(status, 404, "a well-formed id nobody registered was served");

        let (status, body) = request(api.port, "GET /api/state", None);
        assert_eq!((status, parse(&body)), (200, json!({ "default": true })));
    }
    // ------------------------------------------------- POST /api/metamodels

    /// The event loop as this route sees it: a thread that takes the command,
    /// writes the shared list, and answers. `enabled` is a node started
    /// without [`GenericNode::enable_metamodel_upload`].
    ///
    /// [`GenericNode::enable_metamodel_upload`]: crate::generic::GenericNode::enable_metamodel_upload
    fn answering_metamodel_posts(
        ctrl_rx: mpsc::Receiver<ControlCmd>,
        metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
        enabled: bool,
    ) {
        std::thread::spawn(move || {
            while let Ok(cmd) = ctrl_rx.recv() {
                if let ControlCmd::AddMetamodel { text, reply } = cmd {
                    let answer = if !enabled {
                        Err(ServeRefused::NotEnabled)
                    } else {
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Err(err) => Err(ServeRefused::Unreadable(format!("not JSON: {err}"))),
                            Ok(parsed) => {
                                let key = parsed["digest"].as_str().unwrap_or_default().to_string();
                                let listing = json!({
                                    "nsURI": parsed["nsURI"].as_str().unwrap_or_default(),
                                    "digest": key,
                                });
                                let mut held = metamodels.write().expect("uncontended");
                                let added = !held.iter().any(|d| d.key == key);
                                if added {
                                    held.push(ServedDescriptor {
                                        key: key.clone(),
                                        listing: listing.clone(),
                                        text,
                                    });
                                }
                                Ok(Served {
                                    key,
                                    listing,
                                    added,
                                })
                            }
                        }
                    };
                    let _ = reply.send(answer);
                }
            }
        });
    }

    const UML_BODY: &str = r#"{"nsURI":"http://example.org/uml","digest":"sha256:uml"}"#;

    #[test]
    fn a_posted_descriptor_is_answered_201_and_listed_immediately() {
        let api = spawn_api(None);
        answering_metamodel_posts(api.ctrl_rx, Arc::clone(&api.metamodels), true);

        let (status, body) = request(api.port, "POST /api/metamodels", Some(UML_BODY));

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(
            (status, parsed["added"].as_bool()),
            (201, Some(true)),
            "unexpected body: {body}"
        );
        assert_eq!(
            parsed["metamodels"],
            json!([{ "nsURI": "http://example.org/uml", "digest": "sha256:uml" }]),
            "the answer lists what the node holds after the command"
        );

        // And the very next GET sees it, through the same shared list rather
        // than through a snapshot taken when this thread spawned.
        let (status, body) = request(api.port, "GET /api/metamodels", None);
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(status, 200);
        assert_eq!(parsed["metamodels"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn a_descriptor_the_node_already_held_is_answered_200() {
        let api = spawn_api(None);
        answering_metamodel_posts(api.ctrl_rx, Arc::clone(&api.metamodels), true);

        let (first, _) = request(api.port, "POST /api/metamodels", Some(UML_BODY));
        let (status, body) = request(api.port, "POST /api/metamodels", Some(UML_BODY));

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(first, 201);
        assert_eq!((status, parsed["added"].as_bool()), (200, Some(false)));
    }

    #[test]
    fn a_body_the_application_refused_is_answered_422_with_its_reason() {
        let api = spawn_api(None);
        answering_metamodel_posts(api.ctrl_rx, Arc::clone(&api.metamodels), true);

        let (status, body) = request(api.port, "POST /api/metamodels", Some("not a descriptor"));

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(status, 422);
        assert!(
            parsed["error"]
                .as_str()
                .unwrap_or_default()
                .contains("not JSON"),
            "the reason the application gave must reach the caller: {body}"
        );
        assert!(api.metamodels.read().expect("uncontended").is_empty());
    }

    #[test]
    fn a_node_started_without_the_upload_hook_answers_501() {
        let api = spawn_api(None);
        answering_metamodel_posts(api.ctrl_rx, Arc::clone(&api.metamodels), false);

        let (status, _) = request(api.port, "POST /api/metamodels", Some(UML_BODY));

        assert_eq!(status, 501);
    }
}
