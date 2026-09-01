//! HTTP API implementation for [`GenericNode`].
//!
//! This module is intentionally transport-agnostic: it speaks to the node via
//! channels and control commands.

use std::io::Read;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use moirai_protocol::log_id::LogId;
use serde_json::json;
use tiny_http::{Header, Method, Response, Server};

use crate::generic::{ControlCmd, NetworkOp, OpEnvelope, OpResult};

/// Start the optional HTTP API on the given port.
///
/// Endpoints:
/// - `POST /api/op`              submit an operation (JSON body = serialized op)
/// - `GET  /api/health`          health check, names the replica and its log
/// - `GET  /api/log-id`          the log this replica hosts, on its own
/// - `GET  /api/state`           query current CRDT state as JSON
/// - `GET  /api/metamodel`       metamodel descriptor, when the node carries
///   one (see [`crate::generic::GenericNode::serve_metamodel`]); 404
///   otherwise, exactly like any unknown path
/// - `GET  /api/metrics`         causal-stability and log-size counters
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
    metamodel: Option<String>,
) {
    thread::spawn(move || {
        let addr = format!("0.0.0.0:{}", port);
        let server = Server::http(&addr).expect("Failed to start HTTP server");
        eprintln!("[{}] HTTP API listening on {}", replica_id, addr);

        let add_cors = |mut resp: Response<std::io::Cursor<Vec<u8>>>| {
            resp.add_header(Header::from_bytes(b"Access-Control-Allow-Origin", b"*").unwrap());
            resp.add_header(
                Header::from_bytes(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS").unwrap(),
            );
            resp.add_header(
                Header::from_bytes(b"Access-Control-Allow-Headers", b"Content-Type").unwrap(),
            );
            resp
        };

        for mut request in server.incoming_requests() {
            let path = request.url().to_string();
            let method = request.method().clone();

            if method == Method::Options {
                let resp = Response::from_string("").with_status_code(204);
                let _ = request.respond(add_cors(resp));
                continue;
            }

            match (&method, path.as_str()) {
                (&Method::Get, "/api/health") => {
                    let body = json!({
                        "status": "ok",
                        "replica_id": replica_id,
                        "log_id": log_id.as_str(),
                    });
                    let resp = Response::from_string(body.to_string()).with_header(
                        Header::from_bytes(b"Content-Type", b"application/json").unwrap(),
                    );
                    let _ = request.respond(add_cors(resp));
                }
                (&Method::Get, "/api/log-id") => {
                    let body = json!({ "log_id": log_id.as_str() });
                    let resp = Response::from_string(body.to_string()).with_header(
                        Header::from_bytes(b"Content-Type", b"application/json").unwrap(),
                    );
                    let _ = request.respond(add_cors(resp));
                }
                (&Method::Get, "/api/state") => {
                    let (reply_tx, reply_rx) = mpsc::channel();
                    let _ = ctrl.send(ControlCmd::Query { reply: reply_tx });
                    let resp = match reply_rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(state) => Response::from_string(state.to_string()).with_header(
                            Header::from_bytes(b"Content-Type", b"application/json").unwrap(),
                        ),
                        Err(_) => {
                            Response::from_string(r#"{"error":"timeout"}"#).with_status_code(504)
                        }
                    };
                    let _ = request.respond(add_cors(resp));
                }
                (&Method::Get, "/api/metamodel") => {
                    // Byte-identical to the catch-all 404 when no descriptor
                    // was configured: a node that never called
                    // `serve_metamodel` keeps its old behaviour in full.
                    let resp = match &metamodel {
                        Some(descriptor) => Response::from_string(descriptor.as_str()).with_header(
                            Header::from_bytes(b"Content-Type", b"application/json").unwrap(),
                        ),
                        None => {
                            Response::from_string(r#"{"error":"not found"}"#).with_status_code(404)
                        }
                    };
                    let _ = request.respond(add_cors(resp));
                }
                (&Method::Get, "/api/metrics") => {
                    let (reply_tx, reply_rx) = mpsc::channel();
                    let _ = ctrl.send(ControlCmd::Metrics { reply: reply_tx });
                    let resp = match reply_rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(metrics) => Response::from_string(metrics.to_string()).with_header(
                            Header::from_bytes(b"Content-Type", b"application/json").unwrap(),
                        ),
                        Err(_) => {
                            Response::from_string(r#"{"error":"timeout"}"#).with_status_code(504)
                        }
                    };
                    let _ = request.respond(add_cors(resp));
                }
                (&Method::Get, "/api/operations") => {
                    let (reply_tx, reply_rx) = mpsc::channel();
                    let _ = ctrl.send(ControlCmd::Operations { reply: reply_tx });
                    let resp = match reply_rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(ops) => Response::from_string(ops.to_string()).with_header(
                            Header::from_bytes(b"Content-Type", b"application/json").unwrap(),
                        ),
                        Err(_) => {
                            Response::from_string(r#"{"error":"timeout"}"#).with_status_code(504)
                        }
                    };
                    let _ = request.respond(add_cors(resp));
                }
                (&Method::Post, "/api/op") => {
                    let mut body = String::new();
                    if Read::read_to_string(&mut request.as_reader(), &mut body).is_err() {
                        let resp = Response::from_string(r#"{"error":"Failed to read body"}"#)
                            .with_status_code(400);
                        let _ = request.respond(add_cors(resp));
                        continue;
                    }

                    match serde_json::from_str::<O>(&body) {
                        Ok(op) => {
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let envelope = OpEnvelope {
                                op,
                                reply: reply_tx,
                            };
                            if sender.send(envelope).is_ok() {
                                let resp = match reply_rx.recv_timeout(Duration::from_secs(5)) {
                                    Ok(result) => {
                                        let resp_body = serde_json::to_string(&result)
                                            .unwrap_or_else(|_| {
                                                r#"{"error":"serialize"}"#.to_string()
                                            });
                                        Response::from_string(resp_body).with_header(
                                            Header::from_bytes(
                                                b"Content-Type",
                                                b"application/json",
                                            )
                                            .unwrap(),
                                        )
                                    }
                                    Err(_) => Response::from_string(r#"{"error":"timeout"}"#)
                                        .with_status_code(504),
                                };
                                let _ = request.respond(add_cors(resp));
                            } else {
                                let resp = Response::from_string(r#"{"error":"channel closed"}"#)
                                    .with_status_code(500);
                                let _ = request.respond(add_cors(resp));
                            }
                        }
                        Err(e) => {
                            let msg = json!({ "error": format!("Invalid op JSON: {}", e) });
                            let resp = Response::from_string(msg.to_string())
                                .with_status_code(400)
                                .with_header(
                                    Header::from_bytes(b"Content-Type", b"application/json")
                                        .unwrap(),
                                );
                            let _ = request.respond(add_cors(resp));
                        }
                    }
                }
                _ => {
                    let json_header =
                        Header::from_bytes(b"Content-Type", b"application/json").unwrap();

                    match (&method, path.as_str()) {
                        (&Method::Post, p) if p.starts_with("/api/pause/") => {
                            let peer_id = p.trim_start_matches("/api/pause/").to_string();
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let _ = ctrl.send(ControlCmd::Pause {
                                peer_id,
                                reply: reply_tx,
                            });
                            let result =
                                reply_rx
                                    .recv_timeout(Duration::from_secs(5))
                                    .unwrap_or(OpResult {
                                        success: false,
                                        message: "timeout".into(),
                                    });
                            let resp =
                                Response::from_string(serde_json::to_string(&result).unwrap())
                                    .with_header(json_header);
                            let _ = request.respond(add_cors(resp));
                        }
                        (&Method::Post, p) if p.starts_with("/api/resume/") => {
                            let peer_id = p.trim_start_matches("/api/resume/").to_string();
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let _ = ctrl.send(ControlCmd::Resume {
                                peer_id,
                                reply: reply_tx,
                            });
                            let result =
                                reply_rx
                                    .recv_timeout(Duration::from_secs(5))
                                    .unwrap_or(OpResult {
                                        success: false,
                                        message: "timeout".into(),
                                    });
                            let resp =
                                Response::from_string(serde_json::to_string(&result).unwrap())
                                    .with_header(json_header);
                            let _ = request.respond(add_cors(resp));
                        }
                        (&Method::Post, "/api/pause-all") => {
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let _ = ctrl.send(ControlCmd::PauseAll { reply: reply_tx });
                            let result =
                                reply_rx
                                    .recv_timeout(Duration::from_secs(5))
                                    .unwrap_or(OpResult {
                                        success: false,
                                        message: "timeout".into(),
                                    });
                            let resp =
                                Response::from_string(serde_json::to_string(&result).unwrap())
                                    .with_header(json_header);
                            let _ = request.respond(add_cors(resp));
                        }
                        (&Method::Post, "/api/resume-all") => {
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let _ = ctrl.send(ControlCmd::ResumeAll { reply: reply_tx });
                            let result =
                                reply_rx
                                    .recv_timeout(Duration::from_secs(5))
                                    .unwrap_or(OpResult {
                                        success: false,
                                        message: "timeout".into(),
                                    });
                            let resp =
                                Response::from_string(serde_json::to_string(&result).unwrap())
                                    .with_header(json_header);
                            let _ = request.respond(add_cors(resp));
                        }
                        (&Method::Post, "/api/leave") => {
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let _ = ctrl.send(ControlCmd::Leave { reply: reply_tx });
                            let result =
                                reply_rx
                                    .recv_timeout(Duration::from_secs(5))
                                    .unwrap_or(OpResult {
                                        success: false,
                                        message: "timeout".into(),
                                    });
                            let resp =
                                Response::from_string(serde_json::to_string(&result).unwrap())
                                    .with_header(json_header);
                            let _ = request.respond(add_cors(resp));
                        }
                        (&Method::Get, "/api/peers") => {
                            let (reply_tx, reply_rx) = mpsc::channel();
                            let _ = ctrl.send(ControlCmd::Peers { reply: reply_tx });
                            let result = reply_rx
                                .recv_timeout(Duration::from_secs(5))
                                .unwrap_or(json!({"error": "timeout"}));
                            let resp =
                                Response::from_string(result.to_string()).with_header(json_header);
                            let _ = request.respond(add_cors(resp));
                        }
                        _ => {
                            let resp = Response::from_string(r#"{"error":"not found"}"#)
                                .with_status_code(404);
                            let _ = request.respond(add_cors(resp));
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use moirai_protocol::log_id::LogId;
    use moirai_protocol::utils::intern_str::{InternalizeOp, Interner};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::start_http_api;
    use crate::generic::{ControlCmd, OpEnvelope, OpResult};

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
    }

    fn spawn_api(metamodel: Option<String>) -> Api {
        // Grab a free port, release it, and let the server re-bind it. The
        // race window is negligible on loopback and only affects tests.
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback bind");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);

        let (op_tx, op_rx) = mpsc::channel();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();
        let log_id = LogId::parse(TEST_LOG_ID).expect("a fixed, valid log id");
        start_http_api::<TestOp>(
            port,
            "test-replica".into(),
            log_id,
            op_tx,
            ctrl_tx,
            metamodel,
        );

        Api {
            port,
            op_rx,
            ctrl_rx,
        }
    }

    /// One raw HTTP/1.1 exchange; returns `(status, body)`.
    fn request(port: u16, head: &str, body: Option<&str>) -> (u16, String) {
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
}
