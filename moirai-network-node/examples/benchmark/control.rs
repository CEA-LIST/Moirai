use serde_json::json;
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tiny_http::{Method, Response, Server};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Ready,
    Starting,
    Running,
    Drained,
    Sealing,
    Sealed,
    Crashing,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Drained => "drained",
            Self::Sealing => "sealing",
            Self::Sealed => "sealed",
            Self::Crashing => "crashing",
        }
    }
}

pub struct Control {
    pub phase: Mutex<Phase>,
    pub connected: AtomicBool,
}

impl Control {
    pub fn bind(port: u16, replica_id: u16) -> io::Result<Arc<Self>> {
        let server = Server::http(("0.0.0.0", port)).map_err(io::Error::other)?;
        let state = Arc::new(Self {
            phase: Mutex::new(Phase::Ready),
            connected: AtomicBool::new(false),
        });
        let admin = Arc::clone(&state);
        thread::spawn(move || {
            for request in server.incoming_requests() {
                let mut phase = admin.phase.lock().unwrap();
                let connected = admin.connected.load(Ordering::Relaxed);
                let status = match (request.method(), request.url()) {
                    (&Method::Get, "/api/health") => 200,
                    (&Method::Get, "/api/benchmark/status") => 200,
                    (&Method::Post, "/api/benchmark/start")
                        if *phase == Phase::Ready && connected =>
                    {
                        *phase = Phase::Starting;
                        202
                    }
                    (&Method::Post, "/api/benchmark/start")
                        if matches!(*phase, Phase::Starting | Phase::Running | Phase::Drained) =>
                    {
                        202
                    }
                    (&Method::Post, "/api/benchmark/seal") if *phase == Phase::Drained => {
                        *phase = Phase::Sealing;
                        202
                    }
                    (&Method::Post, "/api/benchmark/seal")
                        if matches!(*phase, Phase::Sealing | Phase::Sealed) =>
                    {
                        202
                    }
                    (&Method::Post, "/api/benchmark/crash")
                        if matches!(*phase, Phase::Running | Phase::Drained | Phase::Crashing) =>
                    {
                        202
                    }
                    (&Method::Post, "/api/benchmark/start" | "/api/benchmark/seal") => 409,
                    (&Method::Post, "/api/benchmark/crash") => 409,
                    _ => 404,
                };
                if request.url() == "/api/benchmark/crash" && status == 202 {
                    // Acknowledge the fault before the main loop exits.
                    let _ = request.respond(
                        Response::from_string("{\"phase\":\"crashing\"}").with_status_code(status),
                    );
                    *phase = Phase::Crashing;
                    continue;
                }
                let body = json!({"phase":phase.name(), "replica_id":replica_id, "peers_connected":connected});
                drop(phase);
                let _ = request
                    .respond(Response::from_string(body.to_string()).with_status_code(status));
            }
        });
        Ok(state)
    }
}
