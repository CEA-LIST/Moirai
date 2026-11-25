//! NW.31j end to end (vault spec/items/NW.31j.md): the built
//! moirai-netstack-router on 127.0.0.1 port 0 and three nodes of the generated
//! JSON crate (tests/nw31j/node.rs) in Zenoh peer mode, told only the router.
//! Ground truth: the states written below by hand from the ops posted (a
//! JSON object of counters: an update-wins map of Number counters, so
//! concurrent increments of one key add up), read back over GET /api/state.
//!
//! Phases, each a panic prefix on failure: "NW31J mesh:", "NW31J first:"
//! (each node writes one key, all converge), "NW31J pause:" (c pauses all its
//! peers over HTTP, a and b write, a and b converge while c still serves the
//! first state), "NW31J resume:" (c resumes all over HTTP and converges, then
//! writes, and all converge), "NW31J peer:" (b pauses and resumes a by the
//! peer routes around a write of a, and all converge). "set-up:" is a failure
//! of the instrument, never a pass. Last line on success: NW31J E2E OK.
//!
//! Environment: NW31J_ROUTER_BIN, NW31J_NODE_BIN, NW31J_DIR (a scratch folder).

use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const LOG_ID: &str = "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5";
const LIMIT: Duration = Duration::from_secs(30);
/// How long c is watched still serving the first state while paused.
const HOLD: Duration = Duration::from_secs(3);

struct Proc {
    id: String,
    child: Child,
    log: PathBuf,
    ports: PathBuf,
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn var(name: &str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("set-up: {name} is not set"))
}

/// A program with PORTS_FILE and LISTEN_PORT=0 on top of an environment
/// cleared of every launcher and Zenoh variable.
fn spawn(program: &Path, dir: &Path, id: &str, env: &[(&str, String)]) -> Proc {
    let log = dir.join(format!("{id}.log"));
    let ports = dir.join(format!("{id}.ports.json"));
    let out = fs::File::create(&log).unwrap_or_else(|e| panic!("set-up: {e}"));
    let err = out.try_clone().unwrap_or_else(|e| panic!("set-up: {e}"));
    let mut command = Command::new(program);
    for name in [
        "PEERS",
        "PEERS_FILE",
        "BOOTNODE_URL",
        "ADVERTISE_ADDR",
        "RECONCILE_SECS",
        "SESSION_ID",
        "DASHBOARD_URL",
        "MOIRAI_TRANSPORT",
        "ZENOH_ROUTER",
        "ZENOH_MODE",
        "ZENOH_SCOUTING",
        "ZENOH_CONFIG",
        "ZENOH_REGION",
        "ROUTER_REGIONS",
        "HTTP_ALLOWED_ORIGINS",
        "HTTP_PORT",
        "HTTP_BIND_ADDR",
        "REPLICA_ID",
        "LOG_ID",
    ] {
        command.env_remove(name);
    }
    let child = command
        .env("PORTS_FILE", &ports)
        .env("LISTEN_PORT", "0")
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap_or_else(|e| panic!("set-up: spawning {id}: {e}"));
    Proc {
        id: id.to_string(),
        child,
        log,
        ports,
    }
}

impl Proc {
    fn tail(&self) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(25)..].join("\n")
    }

    /// (listen_port, http_port) from the ports file, once it is whole JSON.
    fn report(&mut self) -> (u16, u16) {
        let deadline = Instant::now() + LIMIT;
        loop {
            match fs::read_to_string(&self.ports) {
                Ok(text) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let port = |k: &str| v[k].as_u64().unwrap_or(0) as u16;
                        return (port("listen_port"), port("http_port"));
                    }
                }
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => panic!("set-up: {}'s PORTS_FILE: {e}", self.id),
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!(
                    "set-up: {} exited with {status} before reporting:\n{}",
                    self.id,
                    self.tail()
                );
            }
            if Instant::now() > deadline {
                panic!(
                    "set-up: {} never wrote PORTS_FILE:\n{}",
                    self.id,
                    self.tail()
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn alive(&mut self, phase: &str) {
        if let Ok(Some(status)) = self.child.try_wait() {
            panic!(
                "NW31J {phase}: {} exited with {status}:\n{}",
                self.id,
                self.tail()
            );
        }
    }
}

fn http(port: u16, method: &str, path: &str, body: Option<&str>) -> Result<(u16, Value), String> {
    let mut stream =
        TcpStream::connect(("127.0.0.1", port)).map_err(|e| format!("connect {port}: {e}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let payload = body.unwrap_or("");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    )
    .map_err(|e| format!("{method} {path} on {port}: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("{method} {path} on {port}: {e}"))?;
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("{method} {path} on {port}: no status in {response:?}"))?;
    let text = response.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    Ok((status, serde_json::from_str(text).unwrap_or(Value::Null)))
}

fn until<T>(mut probe: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    let deadline = Instant::now() + LIMIT;
    loop {
        match probe() {
            Ok(value) => return Ok(value),
            Err(why) if Instant::now() > deadline => return Err(why),
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn check<T>(phase: &str, result: Result<T, String>) -> T {
    result.unwrap_or_else(|why| panic!("NW31J {phase}: {why}"))
}

/// The generated crate's command adding `n` to the counter at `key` of the
/// root JSON object (its serde form).
fn inc(key: &str, n: f64) -> String {
    json!({ "JsonKind": { "Object": { "Update": [key, { "Number": { "Inc": n } }] } } }).to_string()
}

/// The state a node serves for a root object of counters.
fn object(counters: &[(&str, f64)]) -> Value {
    let entries: serde_json::Map<String, Value> = counters
        .iter()
        .map(|(k, n)| (k.to_string(), json!({ "Value": { "Number": n } })))
        .collect();
    json!({ "json": { "Value": { "Object": entries } } })
}

/// POST one op once; it must be answered 200 with success.
fn post_op(phase: &str, port: u16, key: &str, n: f64) {
    let answer = check(phase, http(port, "POST", "/api/op", Some(&inc(key, n))));
    if answer.0 != 200 || answer.1["success"] != json!(true) {
        panic!("NW31J {phase}: POST /api/op {key} {n} on {port} answered {answer:?}");
    }
}

/// POST a control route once; the body must be success true with a message
/// starting `prefix`.
fn control(phase: &str, port: u16, path: &str, prefix: &str) -> String {
    let (status, body) = check(phase, http(port, "POST", path, None));
    let message = body["message"].as_str().unwrap_or_default().to_string();
    if status != 200 || body["success"] != json!(true) || !message.starts_with(prefix) {
        panic!(
            "NW31J {phase}: POST {path} on {port} answered {status} {body}, not success `{prefix}...`"
        );
    }
    message
}

fn state(port: u16) -> Result<Value, String> {
    match http(port, "GET", "/api/state", None)? {
        (200, state) => Ok(state),
        other => Err(format!("GET /api/state on {port} answered {other:?}")),
    }
}

fn converge(phase: &str, nodes: &[(&str, u16)], want: &Value) {
    check(
        phase,
        until(|| {
            for (id, port) in nodes {
                let got = state(*port)?;
                if &got != want {
                    return Err(format!("{id} serves {got}, not {want}"));
                }
            }
            Ok(())
        }),
    );
}

/// The status each node lists for each other node on GET /api/peers.
fn peers_are(phase: &str, nodes: &[(&str, u16)], others: &[&str], want: &str) {
    check(
        phase,
        until(|| {
            for (id, port) in nodes {
                let (_, peers) = http(*port, "GET", "/api/peers", None)?;
                for other in others {
                    if other == id {
                        continue;
                    }
                    let status = peers["peers"]
                        .as_array()
                        .and_then(|l| l.iter().find(|p| p["id"] == json!(other)))
                        .map(|p| p["status"].clone());
                    if status != Some(json!(want)) {
                        return Err(format!("{id} lists {other} {status:?}, not {want}"));
                    }
                }
            }
            Ok(())
        }),
    );
}

fn main() {
    let (router_bin, node_bin, dir) = (
        var("NW31J_ROUTER_BIN"),
        var("NW31J_NODE_BIN"),
        var("NW31J_DIR"),
    );
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("set-up: {}: {e}", dir.display()));
    let mut router = spawn(&router_bin, &dir, "router", &[]);
    let (router_port, _) = router.report();
    if router_port == 0 {
        panic!("set-up: the router reported port 0");
    }
    let endpoint = format!("tcp/127.0.0.1:{router_port}");
    println!("NW31J router pid {} port {router_port}", router.child.id());
    let ids = ["a", "b", "c"];
    let mut procs: Vec<Proc> = ids
        .iter()
        .map(|id| {
            let env = [
                ("REPLICA_ID", id.to_string()),
                ("HTTP_PORT", "0".to_string()),
                ("HTTP_BIND_ADDR", "127.0.0.1".to_string()),
                ("LOG_ID", LOG_ID.to_string()),
                ("MOIRAI_TRANSPORT", "zenoh".to_string()),
                ("ZENOH_ROUTER", endpoint.clone()),
            ];
            spawn(&node_bin, &dir, id, &env)
        })
        .collect();
    let http_ports: Vec<u16> = procs.iter_mut().map(|p| p.report().1).collect();
    let nodes: Vec<(&str, u16)> = ids
        .iter()
        .copied()
        .zip(http_ports.iter().copied())
        .collect();
    let [a, b, c] = [nodes[0].1, nodes[1].1, nodes[2].1];

    peers_are("mesh", &nodes, &ids, "Connected");

    post_op("first", a, "a", 1.0);
    post_op("first", b, "b", 2.0);
    post_op("first", c, "c", 3.0);
    let first = object(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]);
    converge("first", &nodes, &first);

    control("pause", c, "/api/pause-all", "All peers paused");
    peers_are("pause", &nodes[2..], &["a", "b"], "Paused");
    post_op("pause", a, "a", 10.0);
    post_op("pause", a, "shared", 1.0);
    post_op("pause", b, "b", 20.0);
    post_op("pause", b, "shared", 2.0);
    let second = object(&[("a", 11.0), ("b", 22.0), ("c", 3.0), ("shared", 3.0)]);
    converge("pause", &nodes[..2], &second);
    let held = Instant::now() + HOLD;
    while Instant::now() < held {
        let got = check("pause", state(c));
        if got != first {
            panic!("NW31J pause: c, paused, serves {got}, not the first state {first}");
        }
        thread::sleep(Duration::from_millis(200));
    }

    let message = control(
        "resume",
        c,
        "/api/resume-all",
        "Resumed 2 peers, delivered ",
    );
    println!("NW31J resume-all on c: {message}");
    converge("resume", &nodes, &second);
    post_op("resume", c, "c", 30.0);
    let third = object(&[("a", 11.0), ("b", 22.0), ("c", 33.0), ("shared", 3.0)]);
    converge("resume", &nodes, &third);

    control("peer", b, "/api/pause/a", "Paused peer 'a'");
    post_op("peer", a, "a", 100.0);
    let fourth = object(&[("a", 111.0), ("b", 22.0), ("c", 33.0), ("shared", 3.0)]);
    converge("peer", &[nodes[0], nodes[2]], &fourth);
    let message = control("peer", b, "/api/resume/a", "Resumed peer 'a', delivered ");
    println!("NW31J resume a on b: {message}");
    converge("peer", &nodes, &fourth);

    for p in procs.iter_mut() {
        p.alive("end");
    }
    router.alive("end");
    println!("NW31J E2E OK");
}
