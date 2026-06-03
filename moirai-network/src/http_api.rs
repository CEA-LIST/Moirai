//! HTTP API implementation for [`GenericNode`].
//!
//! This module is intentionally transport-agnostic: it speaks to the node via
//! channels and control commands.

use std::io::Read;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use serde_json::json;
use tiny_http::{Header, Method, Response, Server};

use crate::generic::{ControlCmd, NetworkOp, OpEnvelope, OpResult};

/// Start the optional HTTP API on the given port.
///
/// Endpoints:
/// - `POST /api/op`              submit an operation (JSON body = serialized op)
/// - `GET  /api/health`          health check
/// - `GET  /api/state`           query current CRDT state as JSON
/// - `GET  /api/operations`      list operations delivered to this replica
/// - `POST /api/pause/<peer>`    pause a peer connection
/// - `POST /api/resume/<peer>`   resume a peer connection
/// - `POST /api/pause-all`       pause all peers
/// - `POST /api/resume-all`      resume all peers
/// - `GET  /api/peers`           list peers and status
pub(crate) fn start_http_api<O: NetworkOp>(
    port: u16,
    replica_id: String,
    sender: Sender<OpEnvelope<O>>,
    ctrl: Sender<ControlCmd>,
) {
    thread::spawn(move || {
        let addr = format!("0.0.0.0:{}", port);
        let server = Server::http(&addr).expect("Failed to start HTTP server");
        eprintln!("[{}] HTTP API listening on {}", replica_id, addr);

        let add_cors = |mut resp: Response<std::io::Cursor<Vec<u8>>>| {
            resp.add_header(Header::from_bytes(b"Access-Control-Allow-Origin", b"*").unwrap());
            resp.add_header(
                Header::from_bytes(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS")
                    .unwrap(),
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
                    let body = json!({ "status": "ok", "replica_id": replica_id });
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
                        let resp =
                            Response::from_string(r#"{"error":"Failed to read body"}"#)
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
                                        let resp_body = serde_json::to_string(&result).unwrap_or_else(
                                            |_| r#"{"error":"serialize"}"#.to_string(),
                                        );
                                        Response::from_string(resp_body).with_header(
                                            Header::from_bytes(b"Content-Type", b"application/json")
                                                .unwrap(),
                                        )
                                    }
                                    Err(_) => Response::from_string(r#"{"error":"timeout"}"#)
                                        .with_status_code(504),
                                };
                                let _ = request.respond(add_cors(resp));
                            } else {
                                let resp =
                                    Response::from_string(r#"{"error":"channel closed"}"#)
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
                            let result = reply_rx
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
                            let result = reply_rx
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
                            let result = reply_rx
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
                            let result = reply_rx
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