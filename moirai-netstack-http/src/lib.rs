//! The HTTP API for a [`Node`]: 601c887's generic routes over the node's
//! public API, plus an extension point through which an application adds its
//! own routes (NW.31 design row S6). No operation type and no Moirai: the
//! replica behind every log is a [`ReplicaPort`](moirai_netstack::ReplicaPort).
//!
//! [`HttpApi::start`] serves on its own thread, on the listener it is given;
//! the application's event loop calls [`HttpApi::serve`], which carries the
//! waiting requests out on the node and answers them.

use std::collections::VecDeque;
use std::fmt::{self, Display};
use std::io::Cursor;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use moirai_netstack::{LogId, LogIdError, ReplicaFactory, ReplicaPort, decode_json};
use moirai_netstack_node::{BatchOutcome, BatchResult, Node, OpResult};
use moirai_netstack_transport::transport::{CrdtTransport, PeerStatus};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tiny_http::{Header, Method, Request, Response, Server};

/// An application's own routes. Called only for a path the crate does not
/// route, with the method uppercase and the path as requested; `None` is the
/// catch-all 404.
pub trait Routes<F: ReplicaFactory, T: CrdtTransport> {
    fn answer(
        &mut self,
        node: &mut Node<F, T>,
        method: &str,
        path: &str,
        body: &str,
    ) -> Option<(u16, Value)>;
}

/// No application routes: every path the crate does not route is a 404.
impl<F: ReplicaFactory, T: CrdtTransport> Routes<F, T> for () {
    fn answer(
        &mut self,
        _node: &mut Node<F, T>,
        _method: &str,
        _path: &str,
        _body: &str,
    ) -> Option<(u16, Value)> {
        None
    }
}

/// The API's handle: the requests waiting for the application's event loop.
pub struct HttpApi {
    jobs: Arc<Mutex<VecDeque<Job>>>,
}

impl HttpApi {
    /// Serve on `listener` from its own thread. `timeout` is how long a
    /// request waits for the event loop to take it before its caller is told
    /// the answer is a 504. `default_log` must be the node's default log:
    /// `/api/health` and `/api/log-id` name it, while `/api/state`, `/api/op`
    /// and `/api/ops` act on [`Node::log_id`]. A listener that fails to serve
    /// is only printed, since this signature cannot return it.
    pub fn start(
        listener: TcpListener,
        replica_id: &str,
        default_log: LogId,
        timeout: Duration,
    ) -> Self {
        let jobs = Arc::new(Mutex::new(VecDeque::new()));
        let inner = Arc::new(Inner {
            replica_id: replica_id.to_string(),
            default_log,
            timeout,
            origins: OriginPolicy::from_env(),
            jobs: jobs.clone(),
        });
        thread::spawn(move || {
            let server = match Server::from_listener(listener, None) {
                Ok(server) => server,
                Err(e) => {
                    eprintln!("[{}] HTTP API failed to start: {e}", inner.replica_id);
                    return;
                }
            };
            for request in server.incoming_requests() {
                inner.serve(request);
            }
        });
        Self { jobs }
    }

    /// Take every request waiting now, carry it out on the node, answer it,
    /// and return how many were taken. A request whose caller has already
    /// been answered 504 is skipped: it was never applied and never will be.
    pub fn serve<F, T, R>(&self, node: &mut Node<F, T>, routes: &mut R) -> usize
    where
        F: ReplicaFactory,
        T: CrdtTransport,
        R: Routes<F, T>,
    {
        let jobs: Vec<Job> = {
            let mut queue = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
            queue.drain(..).collect()
        };
        let mut taken = 0;
        for job in jobs {
            if !job.claim.take() {
                continue;
            }
            taken += 1;
            let answer = run(job.work, node, routes, &job.method, &job.path, &job.body);
            let _ = job.reply.send(answer);
        }
        taken
    }
}

/// What the HTTP thread owns besides the queue.
struct Inner {
    replica_id: String,
    default_log: LogId,
    /// How long a request waits for the event loop; 601c887's `REPLY_TIMEOUT`
    /// except in tests.
    timeout: Duration,
    /// Which browser origins are answered, read once at start.
    origins: OriginPolicy,
    jobs: Arc<Mutex<VecDeque<Job>>>,
}

/// One request waiting for the event loop.
struct Job {
    /// The method and path, handed to [`Routes`] verbatim.
    method: String,
    path: String,
    body: String,
    work: Work,
    claim: Claim,
    reply: Sender<Answer>,
}

/// What the event loop must do with one request.
enum Work {
    Extension,
    State,
    Metrics,
    Models,
    Peers,
    Pause(String),
    PauseAll,
    Resume(String),
    ResumeAll,
    Leave,
    Op(Value),
    Ops {
        ops: Vec<Value>,
        base: Option<String>,
    },
    ModelState(LogId),
    ModelIdentified(LogId),
    ModelMetrics(LogId),
    ModelOp {
        id: LogId,
        op: Value,
    },
    ModelOps {
        id: LogId,
        ops: Vec<Value>,
        base: Option<String>,
    },
}

/// The 504 a request gets when the loop did not take it in time. For a
/// submission it says nothing was applied and never will be; for a command
/// that writes, 601c887's `{"success": false, "message": "timeout"}`.
#[derive(Clone, Copy)]
enum Expiry {
    Op,
    Batch(usize),
    Control,
    Read,
}

/// The deadline race with the event loop: the loop takes a job exactly once,
/// and a caller that abandons a job first is answered 504 while the loop
/// skips it.
#[derive(Clone, Default)]
struct Claim(Arc<AtomicU8>);

const PENDING: u8 = 0;
const TAKEN: u8 = 1;
const ABANDONED: u8 = 2;

impl Claim {
    fn take(&self) -> bool {
        self.0
            .compare_exchange(PENDING, TAKEN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn abandon(&self) -> bool {
        self.0
            .compare_exchange(PENDING, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

impl Inner {
    fn serve(&self, mut request: Request) {
        let method = request.method().clone();
        let path = request.url().to_string();
        let caller = self.origins.classify(&request);
        let answer = if caller == Caller::Foreign && !reads_only(&method) {
            error(403, "this origin is not allowed to write to this node")
        } else if method == Method::Post && !declares_json_or_nothing(&request) {
            error(415, "a request body must be declared application/json")
        } else if method == Method::Options {
            Answer {
                status: 204,
                headers: Vec::new(),
                body: String::new(),
            }
        } else {
            self.route(&method, &path, &mut request)
        };
        let _ = request.respond(add_cors(reply_of(answer), &caller));
    }

    fn route(&self, method: &Method, path: &str, request: &mut Request) -> Answer {
        let route = match route(method, path) {
            Ok(route) => route,
            Err(parse) => return error(400, parse),
        };
        match route {
            Route::Health => json_answer(
                200,
                &json!({
                    "status": "ok",
                    "replica_id": self.replica_id.as_str(),
                    "log_id": self.default_log.as_str(),
                }),
            ),
            Route::LogId => json_answer(200, &json!({ "log_id": self.default_log.as_str() })),
            // An unrouted path still goes to the event loop, the only place an
            // application's routes are asked: with `()` routes a stray path is
            // a 404 while the loop is current and a 504 while it is late.
            Route::Unknown => {
                let body = match read_body(request) {
                    Ok(body) => body,
                    Err(answer) => return answer,
                };
                self.dispatch(
                    Work::Extension,
                    method.to_string(),
                    path.to_string(),
                    body,
                    Expiry::Read,
                )
            }
            Route::Op => match read_op(request) {
                Ok(op) => self.dispatch(
                    Work::Op(op),
                    String::new(),
                    String::new(),
                    String::new(),
                    Expiry::Op,
                ),
                Err(answer) => answer,
            },
            Route::Ops => match read_batch(request) {
                Ok(BatchBody { ops, base }) => {
                    let of = ops.len();
                    self.dispatch(
                        Work::Ops { ops, base },
                        String::new(),
                        String::new(),
                        String::new(),
                        Expiry::Batch(of),
                    )
                }
                Err(answer) => answer,
            },
            Route::State => self.dispatch(
                Work::State,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Read,
            ),
            Route::Metrics => self.dispatch(
                Work::Metrics,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Read,
            ),
            Route::Models => self.dispatch(
                Work::Models,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Read,
            ),
            Route::Peers => self.dispatch(
                Work::Peers,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Read,
            ),
            Route::PauseAll => self.dispatch(
                Work::PauseAll,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Control,
            ),
            Route::Leave => self.dispatch(
                Work::Leave,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Control,
            ),
            Route::Pause(peer) => self.dispatch(
                Work::Pause(peer),
                String::new(),
                String::new(),
                String::new(),
                Expiry::Control,
            ),
            Route::Resume(peer) => self.dispatch(
                Work::Resume(peer),
                String::new(),
                String::new(),
                String::new(),
                Expiry::Control,
            ),
            Route::ResumeAll => self.dispatch(
                Work::ResumeAll,
                String::new(),
                String::new(),
                String::new(),
                Expiry::Control,
            ),
            Route::Model { id, leaf } => match leaf {
                ModelLeaf::State => self.dispatch(
                    Work::ModelState(id),
                    String::new(),
                    String::new(),
                    String::new(),
                    Expiry::Read,
                ),
                ModelLeaf::Identified => self.dispatch(
                    Work::ModelIdentified(id),
                    String::new(),
                    String::new(),
                    String::new(),
                    Expiry::Read,
                ),
                ModelLeaf::Metrics => self.dispatch(
                    Work::ModelMetrics(id),
                    String::new(),
                    String::new(),
                    String::new(),
                    Expiry::Read,
                ),
                ModelLeaf::Op => match read_op(request) {
                    Ok(op) => self.dispatch(
                        Work::ModelOp { id, op },
                        String::new(),
                        String::new(),
                        String::new(),
                        Expiry::Op,
                    ),
                    Err(answer) => answer,
                },
                ModelLeaf::Ops => match read_batch(request) {
                    Ok(BatchBody { ops, base }) => {
                        let of = ops.len();
                        self.dispatch(
                            Work::ModelOps { id, ops, base },
                            String::new(),
                            String::new(),
                            String::new(),
                            Expiry::Batch(of),
                        )
                    }
                    Err(answer) => answer,
                },
            },
        }
    }

    /// Hand one request to the event loop and wait for it, settling the
    /// deadline through its claim: a 504 only when the loop had not taken it,
    /// which it then skips; one taken in time is answered with what the loop
    /// did, however long that took.
    fn dispatch(
        &self,
        work: Work,
        method: String,
        path: String,
        body: String,
        expiry: Expiry,
    ) -> Answer {
        let (reply, answer) = mpsc::channel();
        let claim = Claim::default();
        let job = Job {
            method,
            path,
            body,
            work,
            claim: claim.clone(),
            reply,
        };
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(job);
        match answer.recv_timeout(self.timeout) {
            Ok(answer) => answer,
            Err(RecvTimeoutError::Timeout) if claim.abandon() => expired(expiry),
            Err(RecvTimeoutError::Timeout) => match answer.recv() {
                Ok(answer) => answer,
                Err(_) => error(500, "the node's event loop stopped before it answered"),
            },
            Err(RecvTimeoutError::Disconnected) => {
                error(500, "the node's event loop stopped before it answered")
            }
        }
    }
}

/// Carry one job out on the node.
fn run<F, T, R>(
    work: Work,
    node: &mut Node<F, T>,
    routes: &mut R,
    method: &str,
    path: &str,
    body: &str,
) -> Answer
where
    F: ReplicaFactory,
    T: CrdtTransport,
    R: Routes<F, T>,
{
    match work {
        Work::Extension => match routes.answer(node, method, path, body) {
            Some((status, value)) => json_answer(status, &value),
            None => not_found(),
        },
        Work::State => {
            let log = node.log_id().clone();
            match node.hosted(&log) {
                Some(replica) => state_answer(replica.read_state(), node.version_of(&log)),
                None => not_found(),
            }
        }
        Work::Metrics => json_answer(200, &node.metrics()),
        Work::Models => json_answer(200, &models(node)),
        Work::Peers => json_answer(200, &peers(node)),
        Work::Pause(peer) => op_answer(&control(
            node.transport_mut().pause_peer(&peer),
            format!("Paused peer '{peer}'"),
            format!("Failed to pause '{peer}'"),
        )),
        Work::PauseAll => op_answer(&control(
            node.transport_mut().pause_all(),
            "All peers paused".to_string(),
            "Failed".to_string(),
        )),
        Work::Resume(peer) => op_answer(&node.resume_peer(&peer)),
        Work::ResumeAll => op_answer(&resume_all(node)),
        Work::Leave => op_answer(&OpResult {
            success: false,
            message: "discovery is not enabled; nothing to leave".to_string(),
        }),
        Work::Op(op) => op_answer(&node.submit_op(None, op)),
        Work::Ops { ops, base } => batch_answer(&node.submit_batch(None, ops, base.as_deref())),
        Work::ModelState(id) => match node.hosted(&id) {
            Some(replica) => state_answer(replica.read_state(), node.version_of(&id)),
            None => not_found(),
        },
        Work::ModelIdentified(id) => match node.hosted(&id) {
            Some(replica) => state_answer(replica.read_identified(), node.version_of(&id)),
            None => not_found(),
        },
        Work::ModelMetrics(id) => match node.log_metrics(&id) {
            Some(metrics) => json_answer(200, &metrics),
            None => not_found(),
        },
        Work::ModelOp { id, op } => {
            if node.hosts(&id) {
                op_answer(&node.submit_op(Some(id), op))
            } else {
                not_found()
            }
        }
        Work::ModelOps { id, ops, base } => {
            batch_answer(&node.submit_batch(Some(id), ops, base.as_deref()))
        }
    }
}

/// `GET /api/models`: one `{model_id}` per hosted log, in id order, beside the
/// ids this node has seen traffic for but does not host.
fn models<F: ReplicaFactory, T: CrdtTransport>(node: &Node<F, T>) -> Value {
    let models: Vec<Value> = node
        .hosted_logs()
        .map(|id| json!({ "model_id": id.as_str() }))
        .collect();
    let seen: Vec<&str> = node.seen_not_hosted().map(LogId::as_str).collect();
    json!({ "models": models, "seen": seen })
}

/// `GET /api/peers`, as 601c887's listing: id, status and buffer size.
fn peers<F: ReplicaFactory, T: CrdtTransport>(node: &Node<F, T>) -> Value {
    let transport = node.transport();
    let peers: Vec<Value> = transport
        .peers()
        .into_iter()
        .map(|peer| {
            json!({
                "id": peer.id,
                "status": format!("{:?}", peer.status),
                "buffered": transport.buffered_count(&peer.id),
            })
        })
        .collect();
    json!({ "peers": peers })
}

/// 601c887's resume-all: every peer paused before the call, resumed through
/// the node, which hands its buffer over and asks it for a delta.
fn resume_all<F: ReplicaFactory, T: CrdtTransport>(node: &mut Node<F, T>) -> OpResult {
    let paused: Vec<String> = node
        .transport()
        .peers()
        .into_iter()
        .filter(|peer| peer.status == PeerStatus::Paused)
        .map(|peer| peer.id)
        .collect();
    let delivered: usize = paused
        .iter()
        .map(|peer| node.transport().buffered_count(peer))
        .sum();
    for peer in &paused {
        node.resume_peer(peer);
    }
    OpResult {
        success: true,
        message: format!(
            "Resumed {} peers, delivered {delivered} buffered msgs",
            paused.len()
        ),
    }
}

fn control(result: Result<(), impl Display>, success: String, failure: String) -> OpResult {
    match result {
        Ok(()) => OpResult {
            success: true,
            message: success,
        },
        Err(e) => OpResult {
            success: false,
            message: format!("{failure}: {e}"),
        },
    }
}

/// The leaf of a `/api/model/{id}/...` route.
enum ModelLeaf {
    State,
    /// `state/identified`: the state with every element's identity in it.
    Identified,
    Metrics,
    Op,
    Ops,
}

/// The route a request names. Parsed from the path split on `/`, so `{id}` is
/// one segment and nothing is matched by prefix except the pause peer route.
enum Route {
    Health,
    LogId,
    State,
    Metrics,
    Models,
    Peers,
    Pause(String),
    PauseAll,
    Resume(String),
    ResumeAll,
    Leave,
    Op,
    Ops,
    Model { id: LogId, leaf: ModelLeaf },
    Unknown,
}

fn route(method: &Method, path: &str) -> Result<Route, LogIdError> {
    if *method == Method::Post
        && let Some(peer) = path.strip_prefix("/api/pause/")
    {
        return Ok(Route::Pause(peer.to_string()));
    }
    if *method == Method::Post
        && let Some(peer) = path.strip_prefix("/api/resume/")
    {
        return Ok(Route::Resume(peer.to_string()));
    }
    let Some(rest) = path.strip_prefix('/') else {
        return Ok(Route::Unknown);
    };
    let segments: Vec<&str> = rest.split('/').collect();
    Ok(match (method, segments.as_slice()) {
        (&Method::Get, ["api", "health"]) => Route::Health,
        (&Method::Get, ["api", "log-id"]) => Route::LogId,
        (&Method::Get, ["api", "state"]) => Route::State,
        (&Method::Get, ["api", "metrics"]) => Route::Metrics,
        (&Method::Get, ["api", "models"]) => Route::Models,
        (&Method::Get, ["api", "peers"]) => Route::Peers,
        (&Method::Post, ["api", "pause-all"]) => Route::PauseAll,
        (&Method::Post, ["api", "resume-all"]) => Route::ResumeAll,
        (&Method::Post, ["api", "leave"]) => Route::Leave,
        (&Method::Post, ["api", "op"]) => Route::Op,
        (&Method::Post, ["api", "ops"]) => Route::Ops,
        (&Method::Get, ["api", "model", id, "state"]) => Route::Model {
            id: LogId::parse(id)?,
            leaf: ModelLeaf::State,
        },
        (&Method::Get, ["api", "model", id, "state", "identified"]) => Route::Model {
            id: LogId::parse(id)?,
            leaf: ModelLeaf::Identified,
        },
        (&Method::Get, ["api", "model", id, "metrics"]) => Route::Model {
            id: LogId::parse(id)?,
            leaf: ModelLeaf::Metrics,
        },
        (&Method::Post, ["api", "model", id, "op"]) => Route::Model {
            id: LogId::parse(id)?,
            leaf: ModelLeaf::Op,
        },
        (&Method::Post, ["api", "model", id, "ops"]) => Route::Model {
            id: LogId::parse(id)?,
            leaf: ModelLeaf::Ops,
        },
        _ => Route::Unknown,
    })
}

/// The body of a batch: the operations, and the version they were computed
/// against. Unknown fields are refused, because a misspelled `base` would
/// otherwise turn the stale check off without a word.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchBody {
    ops: Vec<Value>,
    #[serde(default)]
    base: Option<String>,
}

/// The request body, or the 400 to answer with.
fn read_body(request: &mut Request) -> Result<String, Answer> {
    let mut body = String::new();
    match request.as_reader().read_to_string(&mut body) {
        Ok(_) => Ok(body),
        Err(_) => Err(error(400, "Failed to read body")),
    }
}

/// A JSON body decoded under the shared depth rule, which accepts a body
/// nested past serde's default limit, with trailing content refused. Uses the
/// netstack's `decode_json` although it is doc-hidden and built for the relay:
/// it is the only public door to that shared rule.
fn read_json<T: DeserializeOwned>(request: &mut Request, what: &str) -> Result<T, Answer> {
    let body = read_body(request)?;
    decode_json(body.as_bytes()).map_err(|e| error(400, format!("Invalid {what} JSON: {e}")))
}

/// 601c887's per-operation cap, below the shared 1024: the transport frame and
/// the state transfer wrap an operation, so a deeper one would be applied here
/// and undecodable at peers and joiners.
const MAX_OPERATION_DEPTH: usize = 768;

/// The request body decoded as one operation, refused when it nests past
/// [`MAX_OPERATION_DEPTH`].
fn read_op(request: &mut Request) -> Result<Value, Answer> {
    let op = read_json::<Value>(request, "op")?;
    check_operation_depth(&op, "op")?;
    Ok(op)
}

/// The request body decoded as a batch, refused when any operation in it nests
/// past [`MAX_OPERATION_DEPTH`], before any of the batch reaches the loop.
fn read_batch(request: &mut Request) -> Result<BatchBody, Answer> {
    let body = read_json::<BatchBody>(request, "batch")?;
    for op in &body.ops {
        check_operation_depth(op, "batch")?;
    }
    Ok(body)
}

/// Refuse an operation nested past [`MAX_OPERATION_DEPTH`], which 601c887 did
/// on the raw text; the decoded value's containers nest the same way.
fn check_operation_depth(op: &Value, what: &str) -> Result<(), Answer> {
    let depth = depth(op);
    if depth > MAX_OPERATION_DEPTH {
        return Err(error(
            400,
            format!("Invalid {what} JSON: JSON depth {depth} exceeds limit {MAX_OPERATION_DEPTH}"),
        ));
    }
    Ok(())
}

/// The deepest container nesting in `value`, counted without recursion.
fn depth(value: &Value) -> usize {
    let mut maximum = 0;
    let mut pending = vec![(value, 0)];
    while let Some((node, parent)) = pending.pop() {
        let depth = parent + 1;
        match node {
            Value::Array(children) => {
                maximum = maximum.max(depth);
                pending.extend(children.iter().map(|child| (child, depth)));
            }
            Value::Object(children) => {
                maximum = maximum.max(depth);
                pending.extend(children.values().map(|child| (child, depth)));
            }
            _ => (),
        }
    }
    maximum
}

/// One reply, before the transport's headers are added.
struct Answer {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
}

type Reply = Response<Cursor<Vec<u8>>>;

fn reply_of(answer: Answer) -> Reply {
    let mut reply = Response::from_string(answer.body).with_status_code(answer.status);
    if answer.status != 204 {
        reply.add_header(
            Header::from_bytes(b"Content-Type", b"application/json").expect("a valid header"),
        );
    }
    for (name, value) in answer.headers {
        if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
            reply.add_header(header);
        }
    }
    reply
}

fn json_answer(status: u16, body: &Value) -> Answer {
    Answer {
        status,
        headers: Vec::new(),
        body: body.to_string(),
    }
}

fn error(status: u16, message: impl Display) -> Answer {
    json_answer(status, &json!({ "error": message.to_string() }))
}

fn not_found() -> Answer {
    error(404, "not found")
}

fn op_answer(result: &OpResult) -> Answer {
    match serde_json::to_string(result) {
        Ok(body) => Answer {
            status: 200,
            headers: Vec::new(),
            body,
        },
        Err(_) => error(200, "serialize"),
    }
}

/// A batch's answer, with the status its outcome maps to.
fn batch_answer(result: &BatchResult) -> Answer {
    let status = match result.outcome {
        BatchOutcome::Applied => 200,
        BatchOutcome::Stale => 409,
        BatchOutcome::Refused => 422,
        BatchOutcome::NotHosted => 404,
    };
    match serde_json::to_string(result) {
        Ok(body) => Answer {
            status,
            headers: Vec::new(),
            body,
        },
        Err(_) => error(500, "serialize"),
    }
}

/// A state read: the state as the body, and the version it was read at as the
/// `ETag`, which is what a batch computed against this read names as its
/// `base`. `no-store` because a model's state is never the same resource twice
/// for long, and a cached copy would come with a version the batch would then
/// be refused over.
fn state_answer(state: Value, version: Option<String>) -> Answer {
    let mut headers = Vec::new();
    if let Some(version) = version {
        headers.push(("ETag", format!("\"{version}\"")));
    }
    headers.push(("Cache-Control", "no-store".to_string()));
    Answer {
        status: 200,
        headers,
        body: state.to_string(),
    }
}

fn expired(expiry: Expiry) -> Answer {
    match expiry {
        Expiry::Op => json_answer(
            504,
            &json!({
                "error": "the node did not take this operation in time; it was not applied and \
                          never will be",
                "applied": false,
            }),
        ),
        Expiry::Batch(of) => json_answer(
            504,
            &json!({
                "outcome": "expired",
                "applied": 0,
                "of": of,
                "message": "the node did not take this batch in time; none of it was applied, and \
                            none of it ever will be",
                "version": null,
            }),
        ),
        Expiry::Control => json_answer(200, &json!({ "success": false, "message": "timeout" })),
        Expiry::Read => error(504, "timeout"),
    }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllowedOrigin::Exact(exact) => f.write_str(exact),
            AllowedOrigin::AnyPort(head) => write!(f, "{head}*"),
        }
    }
}

/// The browser origins the API answers (NW.11).
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
    /// one the list admits is an allowed page, and anything else (`null`, a
    /// near miss, two `Origin` headers) is foreign. `*` is never admitted,
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
/// does a body (a non-zero `Content-Length`, or a `Transfer-Encoding`) with no
/// `Content-Type`.
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
fn add_cors(reply: Reply, caller: &Caller) -> Reply {
    let mut reply = reply;
    reply.add_header(Header::from_bytes(b"Vary", b"Origin").expect("a valid header"));
    match caller {
        Caller::Foreign => return reply,
        Caller::Allowed(origin) => {
            if let Ok(allow) = Header::from_bytes(b"Access-Control-Allow-Origin", origin.as_bytes())
            {
                reply.add_header(allow);
            }
        }
        Caller::Local => {}
    }
    reply.add_header(
        Header::from_bytes(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS")
            .expect("a valid header"),
    );
    reply.add_header(
        Header::from_bytes(b"Access-Control-Allow-Headers", b"Content-Type")
            .expect("a valid header"),
    );
    // A page on another origin reads a state's version from here; without
    // this the browser hides the header from the script.
    reply.add_header(
        Header::from_bytes(b"Access-Control-Expose-Headers", b"ETag").expect("a valid header"),
    );
    reply
}
