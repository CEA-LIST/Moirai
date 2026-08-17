//! Session directory for Moirai replicas.
//!
//! Replicas have no way to find each other: peers come from a static `PEERS`
//! environment variable, dialled once at startup, so nothing that is already
//! running can ever reach a node that appears later. This is the smallest
//! component that removes that constraint.
//!
//! It is a **directory, not a router**. It never sees an operation, never
//! holds model state, and can be restarted mid-session without any replica
//! noticing beyond a failed poll. A hostile bootnode can withhold addresses;
//! it cannot corrupt, read, or reorder anything replicated. That property is
//! why discovery is allowed to be centralised in a local-first system, and it
//! is worth keeping when authentication arrives in phase 3.
//!
//! # API
//!
//! ```text
//! POST /session/{sid}/register  {"id":..,"addr":..} -> {"peers":[..],"epoch":n,"relay":s|null}
//! GET  /session/{sid}/peers                         -> {"peers":[..],"epoch":n,"relay":s|null}
//! POST /session/{sid}/leave     {"id":..}           -> {"ok":b,"peers":[..],"epoch":n,"relay":s|null}
//! GET  /health                                      -> {"status":"ok",..}
//! ```
//!
//! `register` is an upsert plus a read, so a joining replica announces itself
//! and learns the roster in one call; repeating it is also how it refreshes
//! its TTL. An entry not refreshed within `BOOTNODE_TTL_SECS` (default 30) is
//! dropped.
//!
//! **Directory expiry is not eviction.** A replica that stops registering
//! vanishes from here and stays in every peer's matrix clock, so causal
//! stability keeps waiting for it. Conflating the two would erase the very
//! effect this rig is built to measure.
//!
//! # Environment
//!
//! - `BOOTNODE_PORT` — default `7000`
//! - `BOOTNODE_TTL_SECS` — default `30`
//! - `RELAY_ADDR` — `host:port` of a `moirai-relay`, repeated to replicas as
//!   `relay` in every roster. Unset means `null`, which is exactly the
//!   behaviour before a relay existed
//!
//! Repeating a relay address does **not** make this a data path: it is a string
//! the bootnode was given and hands on, and it still never sees an operation.
//! That property is why the relay is a separate service (C-D3) and why the two
//! are not merged for the convenience of one fewer process.
//!
//! # Deliberately absent
//!
//! No authentication, no TLS, no signed epochs. The `epoch` counter is on the
//! wire from day one so that phase 2 can give it meaning without a breaking
//! change, but nothing reads it yet.

mod registry;

use std::env;
use std::io::Read;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;
use tiny_http::{Header, Method, Request, Response, Server};

use registry::Registry;

#[derive(Debug, Deserialize)]
struct RegisterBody {
    id: String,
    addr: String,
}

#[derive(Debug, Deserialize)]
struct LeaveBody {
    id: String,
}

fn main() {
    let port: u16 = env::var("BOOTNODE_PORT")
        .unwrap_or_else(|_| "7000".to_string())
        .parse()
        .expect("Invalid BOOTNODE_PORT");
    let ttl = Duration::from_secs(
        env::var("BOOTNODE_TTL_SECS")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .expect("Invalid BOOTNODE_TTL_SECS"),
    );

    // An empty `RELAY_ADDR` reads as unset, so a Compose file can blank it to
    // get a rig with no relay without deleting the line.
    let relay = env::var("RELAY_ADDR")
        .ok()
        .map(|addr| addr.trim().to_string())
        .filter(|addr| !addr.is_empty());

    let addr = format!("0.0.0.0:{port}");
    let server = Server::http(&addr).expect("Failed to start bootnode HTTP server");
    let registry = Mutex::new(Registry::new(ttl).with_relay(relay.clone()));
    eprintln!(
        "[bootnode] listening on {addr}, peer TTL {ttl:?}, relay {}",
        relay.as_deref().unwrap_or("<none>")
    );

    // Single-threaded on purpose. The whole workload is a handful of replicas
    // polling every few seconds; a thread pool would be more moving parts than
    // the component it serves.
    for request in server.incoming_requests() {
        handle(request, &registry);
    }
}

fn handle(mut request: Request, registry: &Mutex<Registry>) {
    // tiny_http hands back the raw target, query string included.
    let url = request.url().split('?').next().unwrap_or("").to_string();
    let method = request.method().clone();
    let now = Instant::now();

    let (status, body) = match route(&url) {
        Some(Route::Health) if method == Method::Get => {
            let registry = registry.lock().unwrap();
            (
                200,
                json!({
                    "status": "ok",
                    "sessions": registry.session_count(),
                    "ttl_secs": registry.ttl().as_secs(),
                    "relay": registry.relay(),
                }),
            )
        }
        Some(Route::Register(session)) if method == Method::Post => {
            match read_body::<RegisterBody>(&mut request) {
                Ok(body) => {
                    let roster = registry
                        .lock()
                        .unwrap()
                        .register(&session, &body.id, &body.addr, now);
                    eprintln!(
                        "[bootnode] {session}: {} registered at {} ({} peer(s), epoch {})",
                        body.id,
                        body.addr,
                        roster.peers.len(),
                        roster.epoch
                    );
                    (200, json!(roster))
                }
                Err(e) => (400, json!({ "error": e })),
            }
        }
        Some(Route::Peers(session)) if method == Method::Get => {
            let roster = registry.lock().unwrap().peers(&session, now);
            (200, json!(roster))
        }
        Some(Route::Leave(session)) if method == Method::Post => {
            match read_body::<LeaveBody>(&mut request) {
                Ok(body) => {
                    let (roster, removed) = registry.lock().unwrap().leave(&session, &body.id, now);
                    eprintln!(
                        "[bootnode] {session}: {} left (removed: {removed}, epoch {})",
                        body.id, roster.epoch
                    );
                    let mut value = json!(roster);
                    value["ok"] = json!(removed);
                    (200, value)
                }
                Err(e) => (400, json!({ "error": e })),
            }
        }
        Some(_) => (
            405,
            json!({ "error": format!("{method} is not allowed on {url}") }),
        ),
        None => (404, json!({ "error": format!("no route for {url}") })),
    };

    let response = Response::from_string(body.to_string())
        .with_status_code(status)
        .with_header(Header::from_bytes(b"Content-Type", b"application/json").unwrap());
    let _ = request.respond(response);
}

enum Route {
    Health,
    Register(String),
    Peers(String),
    Leave(String),
}

/// `/session/{sid}/{verb}` and `/health`, and nothing else.
///
/// Hand-matched rather than routed by a crate: four paths do not justify a
/// dependency, and the session id is the only variable part.
fn route(url: &str) -> Option<Route> {
    if url == "/health" {
        return Some(Route::Health);
    }
    let rest = url.strip_prefix("/session/")?;
    let (session, verb) = rest.rsplit_once('/')?;
    if session.is_empty() || session.contains('/') {
        return None;
    }
    match verb {
        "register" => Some(Route::Register(session.to_string())),
        "peers" => Some(Route::Peers(session.to_string())),
        "leave" => Some(Route::Leave(session.to_string())),
        _ => None,
    }
}

/// Largest request body the directory will read.
///
/// A register or leave payload is an id and an address — a few hundred bytes at
/// the outside. The cap matters more here than the size suggests: request
/// handling is single-threaded, and `tiny_http` hands the body over as a lazy
/// reader, so one client streaming an endless body does not merely exhaust
/// memory, it stalls discovery for every replica in every session until it
/// stops. Bounding the read turns that from an outage into a 400.
const MAX_BODY_BYTES: u64 = 64 * 1024;

fn read_body<T: for<'de> Deserialize<'de>>(request: &mut Request) -> Result<T, String> {
    let mut raw = String::new();
    // One byte past the limit, so an oversized body is detected rather than
    // silently truncated into something that might still parse.
    request
        .as_reader()
        .take(MAX_BODY_BYTES + 1)
        .read_to_string(&mut raw)
        .map_err(|e| format!("failed to read body: {e}"))?;
    if raw.len() as u64 > MAX_BODY_BYTES {
        return Err(format!("body exceeds the {MAX_BODY_BYTES} byte limit"));
    }
    serde_json::from_str(&raw).map_err(|e| format!("invalid JSON body: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_are_recognised() {
        assert!(matches!(route("/health"), Some(Route::Health)));
        assert!(matches!(route("/session/demo/register"), Some(Route::Register(s)) if s == "demo"));
        assert!(matches!(route("/session/demo/peers"), Some(Route::Peers(s)) if s == "demo"));
        assert!(matches!(route("/session/demo/leave"), Some(Route::Leave(s)) if s == "demo"));
    }

    #[test]
    fn nonsense_paths_are_rejected() {
        assert!(route("/").is_none());
        assert!(route("/session/demo").is_none());
        assert!(route("/session//peers").is_none());
        assert!(route("/session/demo/evict").is_none());
        assert!(route("/api/state").is_none());
    }
}
