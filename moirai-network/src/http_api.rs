//! HTTP API implementation for [`GenericNode`].
//!
//! This module is intentionally transport-agnostic: it speaks to the node via
//! channels and control commands.
//!
//! [`GenericNode`]: crate::generic::GenericNode

use std::fmt::Display;
use std::io::{Cursor, Read};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, PoisonError, RwLock};
use std::thread;
use std::time::Duration;

use moirai_protocol::log_id::LogId;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use tiny_http::{Header, Method, Request, Response, Server};

use crate::generic::{
    BatchEnvelope, BatchOutcome, BatchResult, BoundDescriptor, Claim, ControlCmd, Intake,
    NetworkOp, OpEnvelope, OpResult, RegisterRefused, ServeRefused, ServedDescriptor, StateRead,
};

/// How long a request waits for the node's event loop to take it before it
/// gives up with a 504.
///
/// For an operation or a batch, giving up is settled with the loop through a
/// [`Claim`]: a 504 is answered only when the loop had not taken the
/// submission, which it then never applies, and a submission the loop took
/// in time is answered with what the loop did however long that took. For
/// every other command a 504 changes nothing, since nothing was to be
/// applied.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// Start the optional HTTP API on the given port.
///
/// Model-scoped endpoints, where `{id}` is a [`LogId`] — 32 lowercase hex
/// characters, bare or followed by `_` and a name of 1 to 32 ASCII letters
/// and digits, answered 400 with the parse error otherwise, and 404 when the
/// node hosts no log by that whole id:
/// - `GET  /api/models`               the hosted models as `{model_id, metamodel_id}`
///   under `models`, and beside them under `seen` the ids of logs this
///   node does not host but has seen traffic for since it connected
/// - `POST /api/models`               register a model: `{metamodel_id}` creates
///   one and the node mints its id; `{model_id, metamodel_id}` joins one by
///   id and writes nothing; `{model_id}` alone (or with a `null`
///   `metamodel_id`) joins one by id and binds it to the metamodel its own
///   history carries, listing it with a `null` `metamodel_id` until then.
///   409 for an id already hosted, 422 for a `metamodel_id` naming no
///   descriptor the node holds on a create, or one the node cannot read
/// - `GET  /api/metamodels`           the descriptors the node holds, as the
///   application listed them
/// - `POST /api/metamodels`           serve one more descriptor, now: the body
///   is the descriptor text, 201 with the listing when the node did not hold
///   it, 200 when it did, 422 with the reason the application could not
///   describe it, 501 when the node was started without the hook
/// - `GET  /api/model/{id}/state`     that model's state as JSON, with the
///   version it was read at in the `ETag` header (see
///   [`crate::generic::GenericNode::version_of`])
/// - `GET  /api/model/{id}/state/identified` the same state with every
///   element's identity in it, as the log's
///   [`crate::query::QueryableLog::query_identified_state_json`] answers,
///   with the version in the `ETag` header as above
/// - `POST /api/model/{id}/op`        submit an operation to that model
/// - `POST /api/model/{id}/ops`       submit a batch to that model: the body
///   is `{"ops": [...], "base": "<version>"}`, `base` optional, and the batch
///   is applied in one intake step with nothing delivered between two of its
///   operations (see [`crate::generic::GenericNode::submit_batch`]). The
///   answer is a [`BatchResult`]: 200 applied, 409 stale (the model moved
///   since `base`, nothing applied), 422 refused (`applied` says how many
///   were), 404 not hosted, and 504 `expired` when the event loop did not
///   take the batch in time, in which case nothing was or ever will be
///   applied
/// - `GET  /api/model/{id}/metamodel` the descriptor that model was registered
///   under; 404 while the model's own history has not brought it, and 404
///   again when that history brought a different one, because a node that
///   cannot say what a model is written in must not guess
/// - `GET  /api/model/{id}/metrics`   that model's counters, `foreign_log_refusals` included
///
/// Unscoped endpoints, which answer for the node's default log:
/// - `POST /api/op`              submit an operation (JSON body = serialized op)
/// - `POST /api/ops`             submit a batch, as the model route does
/// - `GET  /api/health`          health check, names the replica and its log
/// - `GET  /api/log-id`          the log this replica hosts, on its own
/// - `GET  /api/state`           query current CRDT state as JSON, with its
///   version in the `ETag` header
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
///
/// Every request passes the [`OriginPolicy`] before any route reads its body
/// or sends anything to the event loop (NW.11): a request whose `Origin` the
/// allow-list does not admit is answered 403 to anything but a read and is
/// granted no `Access-Control-*` header; a `POST` that declares a content
/// type other than `application/json`, or carries a body and declares none,
/// is answered 415. A request with no `Origin` is a local process and is
/// served: the API authenticates no one, so whoever can open a connection to
/// the port without a browser can read and write as the node.
pub(crate) fn start_http_api<O: NetworkOp>(
    port: u16,
    replica_id: String,
    log_id: LogId,
    sender: Sender<Intake<O>>,
    ctrl: Sender<ControlCmd>,
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
) {
    start_http_api_waiting::<O>(
        port,
        replica_id,
        log_id,
        sender,
        ctrl,
        metamodels,
        REPLY_TIMEOUT,
    );
}

/// [`start_http_api`] with the time a request waits for the event loop as a
/// parameter, so a test can reach the timeout without waiting five seconds.
fn start_http_api_waiting<O: NetworkOp>(
    port: u16,
    replica_id: String,
    log_id: LogId,
    sender: Sender<Intake<O>>,
    ctrl: Sender<ControlCmd>,
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
    timeout: Duration,
) {
    spawn_http_api::<O>(
        HttpBind::Port(port),
        replica_id,
        log_id,
        sender,
        ctrl,
        metamodels,
        timeout,
    );
}

/// [`start_http_api`] on a listener the caller has already bound, for a node
/// started on port 0: the kernel chose the port in the bind that keeps it, so
/// the caller could name it before the API serves (NW.02). The start-up line
/// names the port the listener holds.
pub(crate) fn start_http_api_on<O: NetworkOp>(
    listener: TcpListener,
    replica_id: String,
    log_id: LogId,
    sender: Sender<Intake<O>>,
    ctrl: Sender<ControlCmd>,
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
) {
    spawn_http_api::<O>(
        HttpBind::Listener(listener),
        replica_id,
        log_id,
        sender,
        ctrl,
        metamodels,
        REPLY_TIMEOUT,
    );
}

/// Where the API's server comes from: a port bound inside the API's thread,
/// as it always was, or a listener bound before it.
enum HttpBind {
    Port(u16),
    Listener(TcpListener),
}

fn spawn_http_api<O: NetworkOp>(
    bind: HttpBind,
    replica_id: String,
    log_id: LogId,
    sender: Sender<Intake<O>>,
    ctrl: Sender<ControlCmd>,
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
    timeout: Duration,
) {
    thread::spawn(move || {
        let (server, addr) = match bind {
            HttpBind::Port(port) => {
                let addr = format!("0.0.0.0:{}", port);
                let server = Server::http(&addr).expect("Failed to start HTTP server");
                (server, addr)
            }
            HttpBind::Listener(listener) => {
                let addr = listener
                    .local_addr()
                    .map_or_else(|err| format!("<unknown: {err}>"), |addr| addr.to_string());
                let server =
                    Server::from_listener(listener, None).expect("Failed to start HTTP server");
                (server, addr)
            }
        };
        eprintln!("[{}] HTTP API listening on {}", replica_id, addr);
        let origins = OriginPolicy::from_env();
        eprintln!(
            "[{}] HTTP API answers the browser origins {}",
            replica_id, origins
        );

        let api = Api {
            replica_id,
            default_log: log_id,
            sender,
            ctrl,
            metamodels,
            timeout,
            origins,
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
    sender: Sender<Intake<O>>,
    ctrl: Sender<ControlCmd>,
    /// Shared with the event loop, which is the only writer: a descriptor
    /// posted to a running node is listed by the very next request, and this
    /// thread never mutates the list itself.
    metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
    /// How long a request waits for the event loop; [`REPLY_TIMEOUT`] but in
    /// tests.
    timeout: Duration,
    /// Which browser origins are answered, read once at start.
    origins: OriginPolicy,
}

type Reply = Response<Cursor<Vec<u8>>>;

/// The leaf of a `/api/model/{id}/...` route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelLeaf {
    State,
    /// `state/identified`: the state with every element's identity in it.
    IdentifiedState,
    Op,
    Ops,
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
    Ops,
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
        (&Method::Post, ["api", "ops"]) => Route::Ops,
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
        (&Method::Get, ["api", "model", id, "state", "identified"]) => Route::Model {
            id,
            leaf: ModelLeaf::IdentifiedState,
        },
        (&Method::Post, ["api", "model", id, "op"]) => Route::Model {
            id,
            leaf: ModelLeaf::Op,
        },
        (&Method::Post, ["api", "model", id, "ops"]) => Route::Model {
            id,
            leaf: ModelLeaf::Ops,
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

/// A state read: the state as the body, and the version it was read at as
/// the `ETag`, which is what a batch computed against this read names as its
/// `base`. `no-store` because a model's state is never the same resource
/// twice for long, and a cached copy would come with a version the batch
/// would then be refused over.
fn state_reply(read: StateRead) -> Reply {
    let mut reply = json_text(200, read.state.to_string());
    if !read.version.is_empty() {
        if let Ok(etag) = Header::from_bytes(b"ETag", format!("\"{}\"", read.version).as_bytes()) {
            reply.add_header(etag);
        }
    }
    reply.add_header(Header::from_bytes(b"Cache-Control", b"no-store").unwrap());
    reply
}

/// A batch's answer, with the status its outcome maps to.
fn batch_reply(result: &BatchResult) -> Reply {
    let status = match result.outcome {
        BatchOutcome::Applied => 200,
        BatchOutcome::Stale => 409,
        BatchOutcome::Refused => 422,
        BatchOutcome::NotHosted => 404,
    };
    match serde_json::to_string(result) {
        Ok(body) => json_text(status, body),
        Err(_) => error(500, "serialize"),
    }
}

/// The 504 for an operation the event loop did not take in time. The claim
/// was abandoned, so the loop will skip it: the sentence can say so.
fn expired_op() -> Reply {
    json_value(
        504,
        &json!({
            "error": "the node did not take this operation in time; it was not applied and \
                      never will be",
            "applied": false,
        }),
    )
}

/// The 504 for a batch the event loop did not take in time, in the shape of
/// every other batch answer.
fn expired_batch(of: usize) -> Reply {
    json_value(
        504,
        &json!({
            "outcome": "expired",
            "applied": 0,
            "of": of,
            "message": "the node did not take this batch in time; none of it was applied, and \
                        none of it ever will be",
            "version": null,
        }),
    )
}

/// The variable that names the browser origins the API answers,
/// comma-separated.
const ALLOWED_ORIGINS_VAR: &str = "HTTP_ALLOWED_ORIGINS";

/// The allow-list when [`ALLOWED_ORIGINS_VAR`] is unset or empty: the two
/// browser editors' Vite dev servers, the first on 5173 and the second on
/// 5174, under the two names a browser on this machine reaches them by.
const DEFAULT_ALLOWED_ORIGINS: [&str; 4] = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
];

/// One entry of the allow-list.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AllowedOrigin {
    /// An origin, compared as a whole string.
    Exact(String),
    /// `scheme://host:*`: that scheme and that host with any decimal port.
    /// Holds `scheme://host:`, the part before the port.
    AnyPort(String),
}

impl AllowedOrigin {
    fn parse(entry: &str) -> Self {
        match entry.strip_suffix('*') {
            Some(head) if head.ends_with(':') && head.contains("://") => {
                AllowedOrigin::AnyPort(head.to_string())
            }
            _ => AllowedOrigin::Exact(entry.to_string()),
        }
    }

    fn admits(&self, origin: &str) -> bool {
        match self {
            AllowedOrigin::Exact(exact) => origin == exact,
            AllowedOrigin::AnyPort(head) => {
                origin.strip_prefix(head.as_str()).is_some_and(|port| {
                    !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit())
                })
            }
        }
    }
}

impl Display for AllowedOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllowedOrigin::Exact(exact) => f.write_str(exact),
            AllowedOrigin::AnyPort(head) => write!(f, "{head}*"),
        }
    }
}

/// The browser origins the API answers (NW.11).
///
/// [`ALLOWED_ORIGINS_VAR`] lists them, separated by commas, each entry an
/// exact origin or `scheme://host:*`; a configured list replaces
/// [`DEFAULT_ALLOWED_ORIGINS`], which applies when the variable is unset or
/// empty.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OriginPolicy {
    allowed: Vec<AllowedOrigin>,
}

impl OriginPolicy {
    fn from_env() -> Self {
        Self::from_list(std::env::var(ALLOWED_ORIGINS_VAR).ok().as_deref())
    }

    fn from_list(list: Option<&str>) -> Self {
        let mut allowed: Vec<AllowedOrigin> = list
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(AllowedOrigin::parse)
            .collect();
        if allowed.is_empty() {
            allowed = DEFAULT_ALLOWED_ORIGINS
                .iter()
                .map(|origin| AllowedOrigin::parse(origin))
                .collect();
        }
        OriginPolicy { allowed }
    }

    /// Who sent `request`, by its `Origin` header: none is a local process,
    /// one the list admits is an allowed page, and anything else (`null`,
    /// a near miss, two `Origin` headers) is foreign. `*` is never admitted,
    /// since it would be echoed as a grant to every origin.
    fn classify(&self, request: &Request) -> Caller {
        let mut origins = request
            .headers()
            .iter()
            .filter(|header| header.field.equiv("Origin"))
            .map(|header| header.value.as_str());
        match (origins.next(), origins.next()) {
            (None, _) => Caller::Local,
            (Some(origin), None)
                if origin != "*" && self.allowed.iter().any(|entry| entry.admits(origin)) =>
            {
                Caller::Allowed(origin.to_string())
            }
            _ => Caller::Foreign,
        }
    }
}

impl Display for OriginPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (at, entry) in self.allowed.iter().enumerate() {
            if at > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{entry}")?;
        }
        Ok(())
    }
}

/// Who sent a request, as the [`OriginPolicy`] classifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Caller {
    /// No `Origin`: a local process (curl, a Node client, a test harness).
    Local,
    /// A page on an origin the list admits; the origin, to echo.
    Allowed(String),
    /// A page on any other origin.
    Foreign,
}

/// Whether a method only reads. A foreign caller may send these and is
/// answered without any `Access-Control-*` header, so its page cannot read
/// the answer; every other method from it is refused.
fn reads_only(method: &Method) -> bool {
    matches!(method, Method::Get | Method::Head | Method::Options)
}

/// Whether a request's body is declared JSON, or it carries no body and
/// declares no type: a `Content-Type` whose media type is not
/// `application/json` (in any ASCII case, parameters allowed) fails, and so
/// does a body (a non-zero `Content-Length`, or a `Transfer-Encoding`) with
/// no `Content-Type`.
fn declares_json_or_nothing(request: &Request) -> bool {
    let mut declared = request
        .headers()
        .iter()
        .filter(|header| header.field.equiv("Content-Type"))
        .peekable();
    if declared.peek().is_none() {
        let chunked = request
            .headers()
            .iter()
            .any(|header| header.field.equiv("Transfer-Encoding"));
        return !chunked && request.body_length().unwrap_or(0) == 0;
    }
    declared.all(|header| {
        let value = header.value.as_str();
        let media = value.split(';').next().unwrap_or(value).trim();
        media.eq_ignore_ascii_case("application/json")
    })
}

/// The CORS headers `caller` is granted: its own origin echoed once to an
/// allowed page, no `Access-Control-Allow-Origin` to a local process, and no
/// `Access-Control-*` header at all to a foreign page. Never `*`.
fn add_cors(mut resp: Reply, caller: &Caller) -> Reply {
    resp.add_header(Header::from_bytes(b"Vary", b"Origin").unwrap());
    match caller {
        Caller::Foreign => return resp,
        Caller::Allowed(origin) => {
            if let Ok(allow) = Header::from_bytes(b"Access-Control-Allow-Origin", origin.as_bytes())
            {
                resp.add_header(allow);
            }
        }
        Caller::Local => {}
    }
    resp.add_header(
        Header::from_bytes(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS").unwrap(),
    );
    resp.add_header(Header::from_bytes(b"Access-Control-Allow-Headers", b"Content-Type").unwrap());
    // A page on another origin reads a state's version from here; without
    // this the browser hides the header from the script.
    resp.add_header(Header::from_bytes(b"Access-Control-Expose-Headers", b"ETag").unwrap());
    resp
}

/// The body of a batch: the operations, and the version they were computed
/// against. Unknown fields are refused, because a misspelled `base` would
/// otherwise turn the stale check off without a word.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, bound = "O: DeserializeOwned")]
struct BatchBody<O> {
    ops: Vec<O>,
    #[serde(default)]
    base: Option<String>,
}

/// Parse a request body that holds operations, with the recursion limit
/// lifted. See [`Api::submit`] for why.
fn parse_lifted<T: DeserializeOwned>(body: &str) -> Result<T, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(body);
    deserializer.disable_recursion_limit();
    let value = T::deserialize(&mut deserializer)?;
    // `from_str` ends the input for us; a hand-built `Deserializer` does
    // not, and without this a body with an operation and then trailing
    // rubbish would be accepted.
    deserializer.end()?;
    Ok(value)
}

/// What became of a submission handed to the event loop.
enum Settled<R> {
    /// The loop took it and this is what it did.
    Answered(R),
    /// The loop had not taken it when the wait ran out, and never will.
    Expired,
    /// The loop dropped the reply without answering: it is gone.
    Gone,
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
        // Decided before any route reads the body or reaches the event loop,
        // so a refused request changes nothing.
        let caller = self.origins.classify(&request);

        let reply = if caller == Caller::Foreign && !reads_only(&method) {
            error(403, "this origin is not allowed to write to this node")
        } else if method == Method::Post && !declares_json_or_nothing(&request) {
            error(415, "a request body must be declared application/json")
        } else if method == Method::Options {
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
                Route::State => match self.ask(|reply| ControlCmd::Query { reply }) {
                    Some(read) => state_reply(read),
                    None => timeout(),
                },
                Route::Metamodel => self.first_descriptor(),
                Route::Metrics => self.query(|reply| ControlCmd::Metrics { reply }),
                Route::Operations => self.query(|reply| ControlCmd::Operations { reply }),
                Route::Peers => self.query(|reply| ControlCmd::Peers { reply }),
                Route::Models => self.query(|reply| ControlCmd::Models { reply }),
                Route::Metamodels => self.listing(),
                Route::AddMetamodel => self.add_metamodel(&mut request),
                Route::Op => self.submit(&mut request, None),
                Route::Ops => self.submit_batch(&mut request, None),
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
        let _ = request.respond(add_cors(reply, &caller));
    }

    /// One round trip to the event loop: `None` when it did not answer in
    /// time, or is gone.
    fn ask<R>(&self, make: impl FnOnce(Sender<R>) -> ControlCmd) -> Option<R> {
        let (tx, rx) = mpsc::channel();
        self.ctrl.send(make(tx)).ok()?;
        rx.recv_timeout(self.timeout).ok()
    }

    /// Wait for the event loop's answer to a submission, and settle the
    /// deadline with it through `claim`: past the deadline, either this
    /// thread abandons the submission, which the loop will then skip, or
    /// the loop has already taken it, and its answer is waited for. So a
    /// caller told [`Settled::Expired`] was told the truth.
    fn settle<R>(&self, claim: &Claim, reply: &Receiver<R>) -> Settled<R> {
        match reply.recv_timeout(self.timeout) {
            Ok(answer) => Settled::Answered(answer),
            Err(RecvTimeoutError::Timeout) if claim.abandon() => Settled::Expired,
            // Taken by the loop just as the wait ran out: it is applying the
            // submission now and will answer.
            Err(RecvTimeoutError::Timeout) => match reply.recv() {
                Ok(answer) => Settled::Answered(answer),
                Err(_) => Settled::Gone,
            },
            Err(RecvTimeoutError::Disconnected) => Settled::Gone,
        }
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

    /// The descriptor served under one key, or 404. A `None` key is a hosted
    /// model with no binding — the default log — which is served the first
    /// descriptor, as `/api/metamodel` is.
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
    ///
    /// The body is parsed through a [`serde_json::Deserializer`] with the
    /// recursion limit lifted rather than through `from_str`, because an
    /// operation's depth is the *model's* depth and not the protocol's. An
    /// interpreted `ModelOp` carries its whole path from the model root, about
    /// seven JSON levels per containment step, so an element at containment
    /// depth 24 — which SysON's largest public example model reaches, and which the SysML v2
    /// standard libraries pass three times over — produces an operation
    /// nesting 168 levels. With the default limit of 128 such an operation is
    /// answered `400 Invalid op JSON: recursion limit exceeded` and a real
    /// model cannot be handed to a node at all.
    ///
    /// What the limit protects against is a stack overflow on a hostile body,
    /// and that is still a property of this endpoint: it is reachable only by
    /// whoever may already submit operations to this replica.
    fn submit(&self, request: &mut Request, log_id: Option<LogId>) -> Reply {
        let body = match read_body(request) {
            Ok(body) => body,
            Err(reply) => return reply,
        };
        let op = match parse_lifted::<O>(&body) {
            Ok(op) => op,
            Err(e) => return error(400, format!("Invalid op JSON: {e}")),
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        let claim = Claim::default();
        let envelope = OpEnvelope {
            op,
            log_id,
            claim: claim.clone(),
            reply: reply_tx,
        };
        if self.sender.send(Intake::Op(envelope)).is_err() {
            return error(500, "channel closed");
        }
        match self.settle(&claim, &reply_rx) {
            Settled::Answered(result) => op_result(&result),
            Settled::Expired => expired_op(),
            Settled::Gone => error(500, "the node's event loop stopped before it answered"),
        }
    }

    /// `POST /api/ops` and `POST /api/model/{id}/ops`: parse the body as a
    /// batch and hand it to the event loop as one submission. See
    /// [`crate::generic::GenericNode::submit_batch`] for what the loop does
    /// with it, and [`batch_reply`] for the status each outcome is answered
    /// with.
    ///
    /// No round trip to ask whether the node hosts the log first, as the
    /// single-operation route makes: the loop answers that itself, and one
    /// pass less between the caller's read and the batch is one pass less in
    /// which a peer's operation can be delivered and make the batch stale.
    fn submit_batch(&self, request: &mut Request, log_id: Option<LogId>) -> Reply {
        let body = match read_body(request) {
            Ok(body) => body,
            Err(reply) => return reply,
        };
        let BatchBody { ops, base } = match parse_lifted::<BatchBody<O>>(&body) {
            Ok(batch) => batch,
            Err(e) => return error(400, format!("Invalid batch JSON: {e}")),
        };
        let of = ops.len();
        let (reply_tx, reply_rx) = mpsc::channel();
        let claim = Claim::default();
        let envelope = BatchEnvelope {
            ops,
            log_id,
            base,
            claim: claim.clone(),
            reply: reply_tx,
        };
        if self.sender.send(Intake::Batch(envelope)).is_err() {
            return error(500, "channel closed");
        }
        match self.settle(&claim, &reply_rx) {
            Settled::Answered(result) => batch_reply(&result),
            Settled::Expired => expired_batch(of),
            Settled::Gone => error(500, "the node's event loop stopped before it answered"),
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
        let model_id = match body.get("model_id") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(id)) => match LogId::parse(id) {
                Ok(id) => Some(id),
                Err(e) => return error(400, format!("`model_id`: {e}")),
            },
            Some(_) => return error(400, "`model_id` must be a string"),
        };
        // A join may name no metamodel: the model's own history names it.
        // A create may not, since its opening operations are written from
        // the descriptor.
        let metamodel_id = match (body.get("metamodel_id").cloned(), &model_id) {
            (Some(metamodel_id), _) => metamodel_id,
            (None, Some(_)) => serde_json::Value::Null,
            (None, None) => return error(400, "`metamodel_id` is required"),
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
                Some(Some(read)) => state_reply(read),
                Some(None) => not_found(),
                None => timeout(),
            },
            ModelLeaf::IdentifiedState => {
                match self.ask(|reply| ControlCmd::QueryLogIdentified { log_id, reply }) {
                    Some(Some(read)) => state_reply(read),
                    Some(None) => not_found(),
                    None => timeout(),
                }
            }
            ModelLeaf::Metrics => {
                match self.ask(|reply| ControlCmd::LogMetrics { log_id, reply }) {
                    Some(Some(metrics)) => json_text(200, metrics.to_string()),
                    Some(None) => not_found(),
                    None => timeout(),
                }
            }
            ModelLeaf::Metamodel => match self.ask(|reply| ControlCmd::Binding { log_id, reply }) {
                Some(Some(BoundDescriptor::Key(key))) => self.descriptor_for(Some(&key)),
                Some(Some(BoundDescriptor::Unbound)) => self.descriptor_for(None),
                // The node hosts the model and will not say what it is
                // written in, which is the honest answer when the binding
                // and the model disagree: the key would resolve, and to the
                // wrong descriptor.
                // Joined by id alone, and the model's history has not named
                // its metamodel yet: nothing to serve, and no guess either.
                Some(Some(BoundDescriptor::Disputed | BoundDescriptor::Awaiting)) | Some(None) => {
                    not_found()
                }
                None => timeout(),
            },
            ModelLeaf::Op => match self.ask(|reply| ControlCmd::Hosts {
                log_id: log_id.clone(),
                reply,
            }) {
                Some(true) => self.submit(request, Some(log_id)),
                Some(false) => not_found(),
                // Nothing was submitted yet, so nothing can have been applied.
                None => expired_op(),
            },
            ModelLeaf::Ops => self.submit_batch(request, Some(log_id)),
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
        let (status, _, body) = exchange(port, head, body);
        (status, body)
    }

    /// One raw HTTP/1.1 exchange; returns `(status, header block, body)`.
    pub(crate) fn exchange(port: u16, head: &str, body: Option<&str>) -> (u16, String, String) {
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
        // NW.11: a body is declared JSON, as every client of the API must now
        // declare it; a request without a body sends no type, as before.
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
        std::io::Read::read_to_string(&mut stream, &mut response).expect("response read");

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

    /// The value of the header `name` in a header block, case-insensitively.
    pub(crate) fn header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
        headers.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, RwLock};

    use moirai_protocol::log_id::LogId;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use std::time::Duration;

    use super::testing::{exchange, free_port, header, request};
    use super::{route, start_http_api_waiting, ModelLeaf, Route};
    use crate::generic::{
        BatchEnvelope, BatchOutcome, BatchResult, ControlCmd, Intake, NetworkOp, OpEnvelope,
        OpResult, ServeRefused, Served, ServedDescriptor, StateRead,
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

    /// The channels a spawned API is wired to, kept alive for the test's
    /// duration so the server never observes a closed channel.
    ///
    /// Generic over the operation type, with [`TestOp`] as the default, so that
    /// the one test which needs an operation of an arbitrary *shape* — the
    /// deep-nesting test below — can have one without every other test
    /// changing.
    struct Api<O = TestOp> {
        port: u16,
        op_rx: mpsc::Receiver<Intake<O>>,
        ctrl_rx: mpsc::Receiver<ControlCmd>,
        /// The list the event loop would own, so a test can play the loop:
        /// take the command, write the list, answer.
        metamodels: Arc<RwLock<Vec<ServedDescriptor>>>,
    }

    fn spawn_api(metamodel: Option<String>) -> Api {
        spawn_api_for::<TestOp>(metamodel)
    }

    fn spawn_api_for<O: NetworkOp>(metamodel: Option<String>) -> Api<O> {
        spawn_api_waiting::<O>(metamodel, super::REPLY_TIMEOUT)
    }

    /// An API whose requests wait `timeout` for the event loop, for the tests
    /// that reach the deadline.
    fn spawn_api_waiting<O: NetworkOp>(metamodel: Option<String>, timeout: Duration) -> Api<O> {
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
        start_http_api_waiting::<O>(
            port,
            "test-replica".into(),
            log_id,
            op_tx,
            ctrl_tx,
            Arc::clone(&metamodels),
            timeout,
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
                let _ = reply.send(StateRead {
                    state: answer,
                    version: "v.1".into(),
                });
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
            if let Ok(Intake::Op(envelope)) = op_rx.recv() {
                assert!(envelope.claim.take());
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

    /// A body nesting `levels` JSON objects, the innermost holding a number.
    fn nested(levels: usize) -> String {
        let mut body = String::from("1");
        for _ in 0..levels {
            body = format!("{{\"a\":{body}}}");
        }
        body
    }

    /// Both op routes take an operation nested past `serde_json`'s default
    /// recursion limit of 128.
    ///
    /// The limit is not a protocol bound: an interpreted `ModelOp` carries its
    /// whole path from the model root, about seven JSON levels per containment
    /// step, so an element at containment depth 24 produces an operation
    /// nesting 168 levels. SysON's largest public example model — 741 elements, containment depth
    /// 24 — has 335 such operations out of 30,344, and the SysML v2 standard
    /// libraries reach containment depth 69. Before this test, a real SysML
    /// model could not be handed to a node over HTTP at all.
    #[test]
    fn both_op_routes_take_an_operation_nested_past_the_default_recursion_limit() {
        let body = nested(200);
        // The reason the ingest cannot use `from_str`, pinned rather than
        // described: this is what the node used to answer 400 over.
        let refused = serde_json::from_str::<serde_json::Value>(&body)
            .expect_err("200 levels is past serde_json's default limit of 128");
        assert!(
            refused.to_string().contains("recursion limit exceeded"),
            "unexpected error: {refused}"
        );

        let api = spawn_api_for::<serde_json::Value>(Some("{}".to_string()));
        let port = api.port;
        let op_rx = api.op_rx;
        let ctrl_rx = api.ctrl_rx;
        // The event loop's two answers: every operation applies, and this node
        // hosts the log the model route names.
        std::thread::spawn(move || {
            while let Ok(intake) = op_rx.recv() {
                match intake {
                    Intake::Op(envelope) => {
                        let _ = envelope.reply.send(OpResult {
                            success: true,
                            message: format!("applied at depth {}", depth_of(&envelope.op)),
                        });
                    }
                    Intake::Batch(batch) => {
                        let depth = batch.ops.iter().map(depth_of).max().unwrap_or(0);
                        let _ = batch.reply.send(BatchResult {
                            outcome: BatchOutcome::Applied,
                            applied: batch.ops.len(),
                            of: batch.ops.len(),
                            refused_at: None,
                            message: format!("applied at depth {depth}"),
                            version: Some("v.2".into()),
                        });
                    }
                }
            }
        });
        std::thread::spawn(move || {
            while let Ok(cmd) = ctrl_rx.recv() {
                if let ControlCmd::Hosts { reply, .. } = cmd {
                    let _ = reply.send(true);
                }
            }
        });

        for route in [
            "POST /api/op".to_string(),
            format!("POST /api/model/{TEST_LOG_ID}/op"),
        ] {
            let (status, answer) = request(port, &route, Some(&body));
            let parsed: serde_json::Value = serde_json::from_str(&answer).expect("json body");
            assert_eq!(
                (status, parsed["success"].as_bool()),
                (200, Some(true)),
                "`{route}` refused a 200-level operation: {answer}"
            );
            assert_eq!(
                parsed["message"].as_str(),
                Some("applied at depth 201"),
                "`{route}` delivered something other than the operation sent: {answer}"
            );
        }

        // And the batch routes, whose operations sit one level further in.
        let batch = format!("{{\"ops\":[{body},{body}]}}");
        for route in [
            "POST /api/ops".to_string(),
            format!("POST /api/model/{TEST_LOG_ID}/ops"),
        ] {
            let (status, answer) = request(port, &route, Some(&batch));
            let parsed: serde_json::Value = serde_json::from_str(&answer).expect("json body");
            assert_eq!(
                (
                    status,
                    parsed["outcome"].as_str(),
                    parsed["applied"].as_u64()
                ),
                (200, Some("applied"), Some(2)),
                "`{route}` refused a batch of 200-level operations: {answer}"
            );
            assert_eq!(parsed["message"].as_str(), Some("applied at depth 201"));
        }
    }

    /// How many JSON levels a value nests, so the test can show that the whole
    /// operation arrived and not a truncated one.
    fn depth_of(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::Object(map) => 1 + map.values().map(depth_of).max().unwrap_or(0),
            serde_json::Value::Array(items) => 1 + items.iter().map(depth_of).max().unwrap_or(0),
            _ => 1,
        }
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
        assert_eq!(
            route(&Method::Post, "/api/model/a1/ops"),
            Route::Model {
                id: "a1",
                leaf: ModelLeaf::Ops
            }
        );
        assert_eq!(route(&Method::Post, "/api/ops"), Route::Ops);
        assert_eq!(route(&Method::Get, "/api/model/a1/ops"), Route::Unknown);
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
                        let _ = reply.send(StateRead {
                            state: json!({ "default": true }),
                            version: "v.0".into(),
                        });
                    }
                    ControlCmd::QueryLog { log_id, reply } => {
                        let state = match log_id.as_str() {
                            BT => Some(json!({ "Sequence": {} })),
                            UML => Some(json!({ "Class": {} })),
                            _ => None,
                        };
                        let _ = reply.send(state.map(|state| StateRead {
                            state,
                            version: format!("v.{}", &log_id.as_str()[..4]),
                        }));
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

    // ------------------------------------------------------ batches and timeouts

    #[test]
    fn the_state_routes_serve_the_version_they_read_at_as_the_etag() {
        let api = spawn_api(None);
        answering_for_two_models(api.ctrl_rx);

        let (status, headers, body) =
            exchange(api.port, &format!("GET /api/model/{BT}/state"), None);
        assert_eq!(status, 200, "{body}");
        assert_eq!(header(&headers, "ETag"), Some("\"v.a1b2\""), "{headers}");
        assert_eq!(header(&headers, "Cache-Control"), Some("no-store"));
        assert_eq!(
            header(&headers, "Access-Control-Expose-Headers"),
            Some("ETag"),
            "a page on another origin could not read the version"
        );

        let (status, headers, _) = exchange(api.port, "GET /api/state", None);
        assert_eq!(status, 200);
        assert_eq!(header(&headers, "ETag"), Some("\"v.0\""), "{headers}");
    }

    /// The event loop as a batch route sees it: every batch is taken and
    /// answered by the rule its `base` spells, `stale`, `refused-at-<k>`, or
    /// anything else applied. What a real loop does with a batch is tested
    /// in `generic.rs`; this is the route's half.
    fn answering_batches(op_rx: mpsc::Receiver<Intake<TestOp>>) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while let Ok(intake) = op_rx.recv() {
                let Intake::Batch(BatchEnvelope {
                    ops,
                    base,
                    claim,
                    reply,
                    ..
                }) = intake
                else {
                    continue;
                };
                assert!(claim.take(), "a batch answered in time was abandoned");
                let of = ops.len();
                let (outcome, applied, refused_at) = match base.as_deref() {
                    Some("stale") => (BatchOutcome::Stale, 0, None),
                    Some(rule) if rule.starts_with("refused-at-") => {
                        let k: usize = rule["refused-at-".len()..].parse().unwrap();
                        (BatchOutcome::Refused, k, Some(k))
                    }
                    Some("unhosted") => (BatchOutcome::NotHosted, 0, None),
                    _ => (BatchOutcome::Applied, of, None),
                };
                let _ = reply.send(BatchResult {
                    outcome,
                    applied,
                    of,
                    refused_at,
                    message: format!("{outcome:?}"),
                    version: Some("v.9".into()),
                });
            }
        })
    }

    #[test]
    fn a_batch_is_one_submission_and_its_outcome_sets_the_status() {
        let api = spawn_api(None);
        let _loop = answering_batches(api.op_rx);
        let route = format!("POST /api/model/{TEST_LOG_ID}/ops");
        let ops = r#"[{"value":1},{"value":2},{"value":3}]"#;
        let post = |base: &str| {
            let body = format!(r#"{{"ops":{ops},"base":"{base}"}}"#);
            let (status, answer) = request(api.port, &route, Some(&body));
            let parsed: serde_json::Value = serde_json::from_str(&answer).expect("json body");
            (status, parsed)
        };

        let (status, parsed) = post("v.8");
        assert_eq!(status, 200, "{parsed}");
        assert_eq!(
            (
                parsed["outcome"].as_str(),
                parsed["applied"].as_u64(),
                parsed["of"].as_u64()
            ),
            (Some("applied"), Some(3), Some(3))
        );
        assert_eq!(parsed["version"].as_str(), Some("v.9"));

        let (status, parsed) = post("stale");
        assert_eq!(
            (status, parsed["outcome"].as_str()),
            (409, Some("stale")),
            "{parsed}"
        );
        assert_eq!(parsed["applied"].as_u64(), Some(0));

        let (status, parsed) = post("refused-at-1");
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

        let (status, parsed) = post("unhosted");
        assert_eq!(
            (status, parsed["outcome"].as_str()),
            (404, Some("nothosted"))
        );

        // With no base at all, the batch is applied to whatever the log holds.
        let (status, answer) = request(api.port, &route, Some(&format!(r#"{{"ops":{ops}}}"#)));
        assert_eq!(status, 200, "{answer}");
    }

    #[test]
    fn a_batch_body_of_the_wrong_shape_is_400_and_reaches_nothing() {
        let api = spawn_api(None);
        let route = format!("POST /api/model/{TEST_LOG_ID}/ops");
        for body in [
            r#"[{"value":1}]"#,
            r#"{"ops":{"value":1}}"#,
            r#"{"ops":[{"value":"one"}]}"#,
            // A misspelled `base` would turn the stale check off in silence.
            r#"{"ops":[{"value":1}],"bsae":"v.1"}"#,
            r#"{"ops":[{"value":1}]} trailing"#,
        ] {
            let (status, answer) = request(api.port, &route, Some(body));
            assert_eq!(status, 400, "`{body}` was taken: {answer}");
        }
        assert!(
            api.op_rx.try_recv().is_err(),
            "a malformed batch reached the event loop"
        );
    }

    /// The ip10 504: a node answered 504 while the operation still sat in
    /// its channel, and applied it afterwards. Now a 504 is answered only for
    /// a submission the loop had not taken, and the loop, when it gets to
    /// it, finds it abandoned and applies nothing.
    #[test]
    fn a_batch_the_loop_did_not_take_in_time_is_expired_and_never_applied() {
        let api = spawn_api_waiting::<TestOp>(None, Duration::from_millis(200));
        let op_rx = api.op_rx;
        let late = std::thread::spawn(move || {
            // A loop busy delivering a backlog, past the request's deadline.
            std::thread::sleep(Duration::from_millis(600));
            match op_rx.recv().expect("the batch was submitted") {
                Intake::Batch(batch) => batch.claim.take(),
                Intake::Op(_) => panic!("a batch route submitted a single operation"),
            }
        });

        let (status, answer) = request(
            api.port,
            &format!("POST /api/model/{TEST_LOG_ID}/ops"),
            Some(r#"{"ops":[{"value":1},{"value":2}],"base":"v.1"}"#),
        );

        let parsed: serde_json::Value = serde_json::from_str(&answer).expect("json body");
        assert_eq!(status, 504, "{answer}");
        assert_eq!(
            (
                parsed["outcome"].as_str(),
                parsed["applied"].as_u64(),
                parsed["of"].as_u64()
            ),
            (Some("expired"), Some(0), Some(2))
        );
        assert!(
            !late.join().expect("the late loop"),
            "the loop took a batch its caller had been told was never applied"
        );
    }

    /// The other side of the race: a submission the loop took before the
    /// deadline is answered with what the loop did, however long that took,
    /// and not with a 504 that would lie about it.
    #[test]
    fn a_batch_the_loop_took_in_time_is_answered_even_past_the_deadline() {
        let api = spawn_api_waiting::<TestOp>(None, Duration::from_millis(200));
        let op_rx = api.op_rx;
        std::thread::spawn(move || {
            if let Ok(Intake::Batch(batch)) = op_rx.recv() {
                assert!(batch.claim.take());
                // Taken, and then slow to apply.
                std::thread::sleep(Duration::from_millis(600));
                let _ = batch.reply.send(BatchResult {
                    outcome: BatchOutcome::Applied,
                    applied: batch.ops.len(),
                    of: batch.ops.len(),
                    refused_at: None,
                    message: "applied, slowly".into(),
                    version: Some("v.3".into()),
                });
            }
        });

        let (status, answer) = request(
            api.port,
            &format!("POST /api/model/{TEST_LOG_ID}/ops"),
            Some(r#"{"ops":[{"value":1},{"value":2}]}"#),
        );

        let parsed: serde_json::Value = serde_json::from_str(&answer).expect("json body");
        assert_eq!(
            (
                status,
                parsed["outcome"].as_str(),
                parsed["applied"].as_u64()
            ),
            (200, Some("applied"), Some(2)),
            "{answer}"
        );
    }

    /// The single-operation route settles its deadline the same way, and
    /// its 504 says the operation was not applied.
    #[test]
    fn an_operation_the_loop_did_not_take_in_time_is_expired_and_never_applied() {
        let api = spawn_api_waiting::<TestOp>(None, Duration::from_millis(200));
        let op_rx = api.op_rx;
        let late = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            match op_rx.recv().expect("the operation was submitted") {
                Intake::Op(OpEnvelope { claim, .. }) => claim.take(),
                Intake::Batch(_) => panic!("the single route submitted a batch"),
            }
        });

        let (status, answer) = request(api.port, "POST /api/op", Some(r#"{"value":3}"#));

        let parsed: serde_json::Value = serde_json::from_str(&answer).expect("json body");
        assert_eq!(status, 504, "{answer}");
        assert_eq!(parsed["applied"].as_bool(), Some(false), "{answer}");
        assert!(!late.join().expect("the late loop"));
    }
}
