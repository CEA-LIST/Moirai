//! Frame relay for Moirai replicas that cannot reach each other directly.
//!
//! Two replicas behind NAT both reach a hosted bootnode outbound, so discovery
//! succeeds and each learns the other's address — and neither address routes.
//! That is the worst failure shape available, because the roster looks healthy
//! while every connection fails. This is the smallest component that fixes it:
//! each replica opens **one outbound** session here and addresses peers by id,
//! so no replica needs an inbound port.
//!
//! # It is not the bootnode, and that is on purpose
//!
//! They could be one process. They must not be (C-D3): the bootnode is
//! specified as **blob-blind** — it sees routing and never model content — and
//! that property is what makes hosting one acceptable under the security
//! design. A relay is by definition a data path. Merging them would destroy the
//! ability to state the bootnode's property at all.
//!
//! # Protocol
//!
//! Newline-delimited JSON both ways, which is the framing replicas already
//! speak, so there is no new codec anywhere:
//!
//! ```text
//! -> {"hello":{"session_id":"demo","replica_id":"a"}}      once, first line
//! -> {"to":"b","from":"a","msg":<anything>}                a frame
//! -> {"to":["b","c"],"from":"a","msg":<anything>}          fan-out, one upload
//! <- {"to":"a","from":"b","msg":<anything>}                a frame for us
//! ```
//!
//! `msg` is **never parsed**. This crate has no dependency on
//! `moirai-protocol`, so it could not parse an operation if it tried; see
//! `routing.rs` for the three things that buys.
//!
//! # What it deliberately does not do
//!
//! No storage, no queueing for a replica that is not connected, no ordering
//! guarantees beyond one TCP connection's, no merging, and no authentication
//! whatsoever (C-D1 — any client claiming an id receives that replica's
//! frames). A frame for a recipient with no live session is **dropped**: the
//! CRDT's own catch-up path is what repairs a gap, and duplicating it here
//! would be a second, weaker replication mechanism.
//!
//! # Environment
//!
//! - `RELAY_PORT` — frame listener, default `7100`
//! - `RELAY_HTTP_PORT` — `GET /health`, default `7101`
//! - `RELAY_IDLE_TIMEOUT_SECS` — session reclaimed after this much silence,
//!   default `300`
//! - `RELAY_VERBOSE` — set to log one line per frame; off by default, because a
//!   relay that logs per frame is a relay that costs what it carries

mod routing;

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;
use tiny_http::{Header, Method, Response, Server};

use routing::{Envelope, HelloFrame, Sessions};

/// Gap between idle sweeps. A tenth of the timeout, clamped, so a session is
/// reclaimed within 10% of when it was due and the thread wakes rarely.
fn sweep_interval(idle_timeout: Duration) -> Duration {
    (idle_timeout / 10).clamp(Duration::from_secs(1), Duration::from_secs(30))
}

fn main() {
    let port = env_u16("RELAY_PORT", 7100);
    let http_port = env_u16("RELAY_HTTP_PORT", 7101);
    let idle_timeout = Duration::from_secs(env_u64("RELAY_IDLE_TIMEOUT_SECS", 300));
    let verbose = env::var("RELAY_VERBOSE").is_ok_and(|v| !v.is_empty() && v != "0");

    let sessions = Arc::new(Mutex::new(Sessions::new(idle_timeout)));

    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).expect("Failed to bind the relay frame listener");
    eprintln!("[relay] frames on {addr}, idle timeout {idle_timeout:?}");

    spawn_sweeper(Arc::clone(&sessions), idle_timeout);
    spawn_health(Arc::clone(&sessions), http_port);

    // Thread per connection, blocking accept: the whole workload is a handful
    // of long-lived sessions, and this is the shape the rest of the codebase
    // already has. Nothing here is async and nothing needs to be.
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let sessions = Arc::clone(&sessions);
                thread::spawn(move || serve(stream, sessions, verbose));
            }
            Err(e) => eprintln!("[relay] accept failed: {e}"),
        }
    }
}

/// Read one connection until it ends: handshake, then frames.
fn serve(stream: TcpStream, sessions: Arc<Mutex<Sessions>>, verbose: bool) {
    let who = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());

    let read_half = match stream.try_clone() {
        Ok(half) => half,
        Err(e) => {
            eprintln!("[relay] {who}: could not split the stream: {e}");
            return;
        }
    };
    let mut lines = BufReader::new(read_half).lines();

    // --- handshake ---
    let hello = match lines.next() {
        Some(Ok(line)) => match serde_json::from_str::<HelloFrame>(&line) {
            Ok(frame) => frame.hello,
            Err(e) => {
                // Deliberately quiet about the content: it is somebody's traffic.
                eprintln!(
                    "[relay] {who}: first line is not a hello ({} bytes): {e}",
                    line.len()
                );
                return;
            }
        },
        Some(Err(e)) => {
            eprintln!("[relay] {who}: read failed before the hello: {e}");
            return;
        }
        None => return,
    };
    let label = format!("{}/{}", hello.session_id, hello.replica_id);

    let (tx, rx) = Sessions::queue();
    let token = sessions
        .lock()
        .expect("the sessions lock is never held across a panic")
        .open(&hello.session_id, &hello.replica_id, tx, Instant::now());
    eprintln!("[relay] {label} session open (from {who})");

    // The writer owns the stream from here, and shutting it down on its way out
    // is what wakes the loop below — so a session displaced, expired or closed
    // by anybody ends the connection without this thread polling for it.
    spawn_writer(stream, rx);

    // --- frames ---
    for line in lines {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                eprintln!("[relay] {label}: read error: {e}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let envelope = match serde_json::from_str::<Envelope>(&line) {
            Ok(envelope) => envelope,
            Err(e) => {
                // Say so rather than dropping it silently: a serialisation
                // defect that vanishes here looks like a stall at both ends.
                eprintln!(
                    "[relay] {label}: unparseable frame ({} bytes): {e}",
                    line.len()
                );
                continue;
            }
        };
        let outcome = sessions
            .lock()
            .expect("the sessions lock is never held across a panic")
            .route(&hello.session_id, &envelope, Instant::now());
        if verbose {
            eprintln!(
                "[relay] {label} -> {:?}: {} delivered, {} dropped, {} absent",
                envelope.to.as_slice(),
                outcome.delivered,
                outcome.dropped,
                outcome.absent
            );
        }
    }

    let closed = sessions
        .lock()
        .expect("the sessions lock is never held across a panic")
        .close(&hello.session_id, &hello.replica_id, token);
    if closed {
        eprintln!("[relay] {label} session closed");
    }
}

/// Write queued frames to one session's socket.
///
/// The queue is bounded and the enqueue side never blocks, so this thread being
/// slow costs its own recipient dropped frames and costs every other session
/// nothing. That is the head-of-line property the design accepts and names.
fn spawn_writer(mut stream: TcpStream, rx: Receiver<String>) {
    thread::spawn(move || {
        while let Ok(line) = rx.recv() {
            if writeln!(stream, "{line}").is_err() || stream.flush().is_err() {
                break;
            }
        }
        // The sender is gone (session closed, displaced or expired) or the
        // write failed. Either way the session is over, and shutting the socket
        // down is what tells the reader thread so.
        let _ = stream.shutdown(Shutdown::Both);
    });
}

/// Reclaim sessions that have carried nothing for the idle timeout.
///
/// A thread of its own, unlike the bootnode's sweep-on-access — see
/// [`Sessions::sweep`] for why the difference is deliberate.
fn spawn_sweeper(sessions: Arc<Mutex<Sessions>>, idle_timeout: Duration) {
    let interval = sweep_interval(idle_timeout);
    thread::spawn(move || loop {
        thread::sleep(interval);
        let expired = sessions
            .lock()
            .expect("the sessions lock is never held across a panic")
            .sweep(Instant::now());
        if expired > 0 {
            eprintln!("[relay] reclaimed {expired} idle session(s)");
        }
    });
}

/// `GET /health` — the counters, and who is connected.
///
/// The relay is a frame service and does not need HTTP for its own sake. This
/// exists so that fan-out is *observable*: one broadcast to N relayed peers
/// must read as one `frames_in` and N `frames_out`, which is the R4 claim, and
/// asserting it any other way would mean parsing logs or building new
/// machinery. It doubles as a container healthcheck.
fn spawn_health(sessions: Arc<Mutex<Sessions>>, port: u16) {
    thread::spawn(move || {
        let addr = format!("0.0.0.0:{port}");
        let server = match Server::http(&addr) {
            Ok(server) => server,
            Err(e) => {
                // Not fatal: relaying is the job, and a relay without its
                // health endpoint still relays.
                eprintln!("[relay] health endpoint disabled ({addr}): {e}");
                return;
            }
        };
        eprintln!("[relay] health on {addr}/health");

        for request in server.incoming_requests() {
            let (status, body) = match (request.method(), request.url()) {
                (&Method::Get, "/health") => {
                    let guard = sessions
                        .lock()
                        .expect("the sessions lock is never held across a panic");
                    let stats = guard.stats();
                    let members: Vec<_> = guard
                        .members()
                        .into_iter()
                        .map(|(session, replica)| json!({ "session": session, "replica": replica }))
                        .collect();
                    drop(guard);
                    let mut body = json!(stats);
                    body["status"] = json!("ok");
                    body["members"] = json!(members);
                    (200, body)
                }
                (_, url) => (404, json!({ "error": format!("no route for {url}") })),
            };
            let response = Response::from_string(body.to_string())
                .with_status_code(status)
                .with_header(
                    Header::from_bytes(b"Content-Type", b"application/json")
                        .expect("a static header is always valid"),
                );
            let _ = request.respond(response);
        }
    });
}

fn env_u16(name: &str, fallback: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sweep_interval_stays_inside_its_bounds() {
        assert_eq!(
            sweep_interval(Duration::from_secs(300)),
            Duration::from_secs(30),
            "clamped at the top, so a long timeout does not mean a rare sweep"
        );
        assert_eq!(
            sweep_interval(Duration::from_secs(20)),
            Duration::from_secs(2)
        );
        assert_eq!(
            sweep_interval(Duration::from_secs(2)),
            Duration::from_secs(1),
            "clamped at the bottom, so a short timeout in a test does not spin"
        );
    }
}
