use crate::{NodeHandle, transport::MAX_FRAME_BYTES};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    io::{self, Read},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::Receiver,
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use tiny_http::{Header, Method, Request, Response, Server};

pub(crate) struct HttpServer {
    stopped: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) fn start<C: DeserializeOwned + Send + 'static>(
    port: u16,
    handle: NodeHandle<C>,
    optimistic_limit: usize,
    consistent_limit: usize,
) -> io::Result<HttpServer> {
    // Bind on the caller thread so a port conflict is a normal startup error.
    let server = Server::http(("0.0.0.0", port)).map_err(io::Error::other)?;
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    let thread = thread::spawn(move || {
        let optimistic_active = Arc::new(AtomicUsize::new(0));
        let consistent_active = Arc::new(AtomicUsize::new(0));
        while !stop.load(Ordering::Acquire) && !handle.is_stopped() {
            let request = match server.recv_timeout(Duration::from_millis(20)) {
                Ok(Some(request)) => request,
                Ok(None) => continue,
                Err(_) => break,
            };
            // Kubernetes probes must remain available when all update workers
            // are waiting for commitment. This round trip still checks the loop.
            if request.method() == &Method::Get && request.url() == "/api/health" {
                query_response(request, handle.health(), Duration::from_secs(1));
                continue;
            }
            let consistent = request.method() == &Method::Post
                && request.url() == "/api/op?wait_for_commit=true";
            let (active, limit) = if consistent {
                (&consistent_active, consistent_limit)
            } else {
                (&optimistic_active, optimistic_limit)
            };
            if active.load(Ordering::Relaxed) >= limit {
                respond(
                    request,
                    503,
                    json!({"error":"too many concurrent requests", "request_class":if consistent { "consistent" } else { "optimistic" }}),
                );
                continue;
            }
            active.fetch_add(1, Ordering::Relaxed);
            let guard = Active(active.clone());
            let handle = handle.clone();
            thread::spawn(move || {
                let _guard = guard;
                serve(request, handle);
            });
        }
    });
    Ok(HttpServer {
        stopped,
        thread: Some(thread),
    })
}

fn respond(request: Request, status: u16, body: Value) {
    let response = Response::from_string(body.to_string())
        .with_status_code(status)
        .with_header(Header::from_bytes("Content-Type", "application/json").unwrap());
    let _ = request.respond(response);
}

fn query_response(request: Request, response: io::Result<Receiver<Value>>, timeout: Duration) {
    match response {
        Ok(response) => match response.recv_timeout(timeout) {
            Ok(body) => respond(request, 200, body),
            Err(_) => respond(request, 503, json!({"error":"replica unavailable"})),
        },
        Err(_) => respond(request, 503, json!({"error":"replica unavailable or busy"})),
    }
}

fn serve<C: DeserializeOwned>(mut request: Request, handle: NodeHandle<C>) {
    let path = request.url().to_owned();
    match (request.method(), path.as_str()) {
        (&Method::Get, "/api/state") => {
            query_response(request, handle.state(), Duration::from_secs(5));
        }
        (
            &Method::Post,
            "/api/op" | "/api/op?wait_for_commit=false" | "/api/op?wait_for_commit=true",
        ) => {
            let mut body = Vec::new();
            if request
                .as_reader()
                .take((MAX_FRAME_BYTES + 1) as u64)
                .read_to_end(&mut body)
                .is_err()
            {
                respond(request, 400, json!({"error":"cannot read request body"}));
                return;
            }
            if body.len() > MAX_FRAME_BYTES {
                respond(
                    request,
                    413,
                    json!({"error":"request body exceeds frame limit"}),
                );
                return;
            }
            let command = match serde_json::from_slice(&body) {
                Ok(command) => command,
                Err(error) => {
                    respond(request, 400, json!({"error":error.to_string()}));
                    return;
                }
            };
            let response = match handle.update(command, path.ends_with("=true")) {
                Ok(response) => response,
                Err(_) => {
                    respond(request, 503, json!({"error":"replica unavailable or busy"}));
                    return;
                }
            };
            match response.recv_timeout(
                handle
                    .request_timeout()
                    .saturating_add(Duration::from_secs(1)),
            ) {
                Ok(result) => {
                    let status = if result.success {
                        200
                    } else if result.applied {
                        504
                    } else {
                        422
                    };
                    respond(request, status, serde_json::to_value(result).unwrap());
                }
                Err(_) => respond(
                    request,
                    504,
                    json!({"error":"response timeout; update may have been applied"}),
                ),
            }
        }
        _ => respond(request, 404, json!({"error":"not found"})),
    }
}
