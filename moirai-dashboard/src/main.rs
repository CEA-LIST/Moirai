//! Live view of a running Moirai session.
//!
//! Replicas with `DASHBOARD_URL` set post here; browsers connect here and are
//! streamed what arrives. Nothing in this binary ever dials a replica, which is
//! what lets it attach to a session it did not start, whose members sit behind
//! NAT, and detach again without anything noticing.
//!
//! ```bash
//! # 1. the rig — nodes run perfectly well with nobody watching
//! docker compose -f docker/compose/docker-compose.yml up -d --scale node=5
//!
//! # 2. optional: attach the dashboard
//! docker compose -f docker/compose/docker-compose.yml --profile dashboard up -d
//! #    …or on the host, against any reachable session
//! cargo run -p moirai-dashboard -- --port 8090
//!
//! # 3. optional: give it something to show
//! cargo run -p moirai-dashboard -- --random --seed 7 --rate 5 \
//!     --nodes http://127.0.0.1:8081,http://127.0.0.1:8082
//! ```
//!
//! # Endpoints
//!
//! - `GET  /`             the page, self-contained, no CDN
//! - `GET  /api/snapshot` everything currently known, as JSON
//! - `GET  /api/stream`   server-sent events: `snapshot` and `feed`
//! - `POST /api/ingest`   where replicas report
//! - `GET  /api/health`   liveness

mod driver;
mod page;
mod state;

use std::io::{self, Read};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

use crate::state::{feed_json, Dashboard};

/// How often a full snapshot is pushed to connected browsers.
///
/// Feed entries are pushed the instant they arrive; the table is not, because
/// re-rendering it per delivery would be a lot of work to show the same numbers.
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);

/// Events queued per browser before the slowest one starts losing them.
const STREAM_DEPTH: usize = 64;

/// Idle gap after which a keep-alive comment is written, so a proxy does not
/// close a quiet stream.
const KEEPALIVE: Duration = Duration::from_secs(15);

struct Shared {
    dashboard: Mutex<Dashboard>,
    subscribers: Mutex<Vec<SyncSender<String>>>,
}

impl Shared {
    /// Send one SSE frame to every browser, dropping the ones that have gone.
    fn broadcast(&self, event: &str, data: &Value) {
        let frame = format!("event: {event}\ndata: {data}\n\n");
        let mut subscribers = self.subscribers.lock().expect("subscribers");
        subscribers.retain(|tx| tx.try_send(frame.clone()).is_ok());
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!(
            "moirai-dashboard [--port N] [--random --seed N --rate R --nodes URL,URL] [--count N]"
        );
        return;
    }

    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };

    if args.iter().any(|a| a == "--random") {
        driver::Driver {
            targets: flag("--nodes")
                .unwrap_or_default()
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
                .collect(),
            seed: flag("--seed")
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(now_ms),
            rate: flag("--rate").and_then(|s| s.parse().ok()).unwrap_or(2.0),
            count: flag("--count").and_then(|s| s.parse().ok()),
        }
        .run();
        return;
    }

    let port: u16 = flag("--port")
        .or_else(|| std::env::var("DASHBOARD_PORT").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(8090);

    let shared = Arc::new(Shared {
        dashboard: Mutex::new(Dashboard::default()),
        subscribers: Mutex::new(Vec::new()),
    });

    // The table ticks on its own so that a session in which nothing is
    // happening still shows replicas ageing into staleness. A dashboard that
    // only updates when something arrives cannot show absence, and absence is
    // the interesting case here.
    {
        let shared = Arc::clone(&shared);
        thread::spawn(move || loop {
            thread::sleep(SNAPSHOT_INTERVAL);
            let snapshot = shared
                .dashboard
                .lock()
                .expect("dashboard")
                .snapshot(now_ms());
            shared.broadcast("snapshot", &snapshot);
        });
    }

    let server = Server::http(("0.0.0.0", port)).unwrap_or_else(|e| {
        eprintln!("cannot bind 0.0.0.0:{port}: {e}");
        std::process::exit(1);
    });
    println!("moirai-dashboard on http://0.0.0.0:{port}");
    println!("point replicas at it with DASHBOARD_URL=http://<this host>:{port}");

    // A thread per request. One SSE stream occupies its thread for as long as
    // the browser is open, so a shared worker pool would be starved by the
    // second viewer.
    for request in server.incoming_requests() {
        let shared = Arc::clone(&shared);
        thread::spawn(move || handle(request, &shared));
    }
}

fn json_response(value: &Value) -> Response<io::Cursor<Vec<u8>>> {
    Response::from_string(value.to_string())
        .with_header(Header::from_bytes(b"Content-Type", b"application/json").unwrap())
        .with_header(Header::from_bytes(b"Access-Control-Allow-Origin", b"*").unwrap())
}

fn handle(mut request: Request, shared: &Arc<Shared>) {
    let method = request.method().clone();
    let url = request.url().split('?').next().unwrap_or("/").to_string();

    match (&method, url.as_str()) {
        (&Method::Get, "/") | (&Method::Get, "/index.html") => {
            let response = Response::from_string(page::PAGE).with_header(
                Header::from_bytes(b"Content-Type", b"text/html; charset=utf-8").unwrap(),
            );
            let _ = request.respond(response);
        }
        (&Method::Get, "/api/health") => {
            let _ = request.respond(json_response(&json!({ "status": "ok" })));
        }
        (&Method::Get, "/api/snapshot") => {
            let snapshot = shared
                .dashboard
                .lock()
                .expect("dashboard")
                .snapshot(now_ms());
            let _ = request.respond(json_response(&snapshot));
        }
        (&Method::Post, "/api/ingest") => {
            let mut body = String::new();
            if request.as_reader().read_to_string(&mut body).is_err() {
                let _ = request.respond(
                    Response::from_string(r#"{"error":"unreadable body"}"#).with_status_code(400),
                );
                return;
            }
            match serde_json::from_str::<Value>(&body) {
                Ok(report) => {
                    let now = now_ms();
                    let produced = shared
                        .dashboard
                        .lock()
                        .expect("dashboard")
                        .ingest(&report, now);
                    if !produced.is_empty() {
                        let entries: Vec<Value> = produced.iter().map(feed_json).collect();
                        shared.broadcast("feed", &Value::Array(entries));
                    }
                    let _ = request.respond(json_response(&json!({ "ok": true })));
                }
                Err(e) => {
                    let _ = request.respond(
                        json_response(&json!({ "error": e.to_string() })).with_status_code(400),
                    );
                }
            }
        }
        (&Method::Get, "/api/stream") => {
            let (tx, rx) = sync_channel::<String>(STREAM_DEPTH);
            // Paint immediately rather than after the first tick, so a browser
            // that connects to a quiet session still sees the session.
            let snapshot = shared
                .dashboard
                .lock()
                .expect("dashboard")
                .snapshot(now_ms());
            let _ = tx.try_send(format!("event: snapshot\ndata: {snapshot}\n\n"));
            shared.subscribers.lock().expect("subscribers").push(tx);

            let headers = vec![
                Header::from_bytes(b"Content-Type", b"text/event-stream").unwrap(),
                Header::from_bytes(b"Cache-Control", b"no-cache").unwrap(),
                Header::from_bytes(b"Connection", b"keep-alive").unwrap(),
                Header::from_bytes(b"Access-Control-Allow-Origin", b"*").unwrap(),
            ];
            // `None` length: tiny_http then streams, which is the whole point.
            let _ = request.respond(Response::new(
                StatusCode(200),
                headers,
                SseBody::new(rx),
                None,
                None,
            ));
        }
        _ => {
            let _ = request
                .respond(json_response(&json!({ "error": "not found" })).with_status_code(404));
        }
    }
}

/// A never-ending response body fed by an `mpsc` channel.
///
/// `Read::read` blocks on the channel, which is exactly what a streaming
/// response needs and why each stream gets its own thread. A timeout turns into
/// an SSE comment rather than an end of stream, so an idle session keeps its
/// connections rather than reconnecting every few seconds.
struct SseBody {
    rx: Receiver<String>,
    buffer: Vec<u8>,
    position: usize,
}

impl SseBody {
    fn new(rx: Receiver<String>) -> Self {
        Self {
            rx,
            buffer: Vec::new(),
            position: 0,
        }
    }
}

impl Read for SseBody {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.position >= self.buffer.len() {
            match self.rx.recv_timeout(KEEPALIVE) {
                Ok(frame) => {
                    self.buffer = frame.into_bytes();
                    self.position = 0;
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.buffer = b": keep-alive\n\n".to_vec();
                    self.position = 0;
                }
                // The subscriber list dropped our sender: the dashboard is
                // gone, or this browser was pruned as too slow.
                Err(RecvTimeoutError::Disconnected) => return Ok(0),
            }
        }
        let n = out.len().min(self.buffer.len() - self.position);
        out[..n].copy_from_slice(&self.buffer[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_is_self_contained() {
        // No CDN, no external font, no external script: the rig routinely runs
        // with no route off the host.
        for forbidden in ["http://", "https://", "//cdn", "src=\"//"] {
            assert!(
                !page::PAGE.contains(forbidden),
                "the page references `{forbidden}`, which will not load offline"
            );
        }
        assert!(page::PAGE.contains("EventSource"), "the page must stream");
    }

    #[test]
    fn sse_frames_are_well_formed() {
        let (tx, rx) = sync_channel::<String>(4);
        tx.send("event: feed\ndata: {\"a\":1}\n\n".to_string())
            .unwrap();
        drop(tx);
        let mut body = SseBody::new(rx);
        let mut out = [0u8; 64];
        let n = body.read(&mut out).unwrap();
        let frame = std::str::from_utf8(&out[..n]).unwrap();
        assert!(frame.starts_with("event: feed\n"));
        assert!(frame.ends_with("\n\n"));
        assert_eq!(
            body.read(&mut out).unwrap(),
            0,
            "a closed channel ends the body"
        );
    }
}
