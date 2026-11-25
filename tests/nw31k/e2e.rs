//! NW.31k end to end, claim part 4 (vault spec/items/NW.31k.md, round 2): the
//! built moirai-netstack-router on 127.0.0.1 port 0 and nodes of
//! moirai-netstack-crdt's awset::factory() (tests/nw31k/node.rs) in Zenoh
//! peer mode, told only the router. Ground truth: the states written below by
//! hand from the ops posted (a sorted JSON array of strings), read over GET
//! /api/state.
//!
//! Phases, each a panic prefix on failure: "NW31K mesh:", "NW31K rounds:"
//! (a, b and c each add one string in each of three rounds and converge after
//! each round, so the first two rounds are stable at all three), "NW31K
//! join:" (a fourth node d starts with no history, takes a state transfer,
//! its log says so, and serves the third state), "NW31K after:" (d writes and
//! b writes, and all four converge). "set-up:" is a failure of the
//! instrument, never a pass. Last line on success: NW31K E2E OK.
//!
//! Environment: NW31K_ROUTER_BIN, NW31K_NODE_BIN, NW31K_DIR (a scratch folder).

use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const LOG_ID: &str = "b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6";
const LIMIT: Duration = Duration::from_secs(30);
/// The wait for the joiner: its state transfer retries every 2 s.
const JOIN_LIMIT: Duration = Duration::from_secs(60);

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
                "NW31K {phase}: {} exited with {status}:\n{}",
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
    within(LIMIT, &mut probe)
}

fn within<T>(limit: Duration, mut probe: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    let deadline = Instant::now() + limit;
    loop {
        match probe() {
            Ok(value) => return Ok(value),
            Err(why) if Instant::now() > deadline => return Err(why),
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn check<T>(phase: &str, result: Result<T, String>) -> T {
    result.unwrap_or_else(|why| panic!("NW31K {phase}: {why}"))
}

/// The state a node serves: the set as a sorted JSON array.
fn set(items: &[&str]) -> Value {
    let mut items: Vec<&str> = items.to_vec();
    items.sort();
    json!(items)
}

/// POST one add once; it must be answered 200 with success.
fn post_op(phase: &str, port: u16, item: &str) {
    let op = json!({ "Add": item }).to_string();
    let answer = check(phase, http(port, "POST", "/api/op", Some(&op)));
    if answer.0 != 200 || answer.1["success"] != json!(true) {
        panic!("NW31K {phase}: POST /api/op {op} on {port} answered {answer:?}");
    }
}

fn state(port: u16) -> Result<Value, String> {
    match http(port, "GET", "/api/state", None)? {
        (200, state) => Ok(state),
        other => Err(format!("GET /api/state on {port} answered {other:?}")),
    }
}

fn converge(phase: &str, nodes: &[(&str, u16)], want: &Value) {
    converge_within(phase, LIMIT, nodes, want)
}

fn converge_within(phase: &str, limit: Duration, nodes: &[(&str, u16)], want: &Value) {
    check(
        phase,
        within(limit, || {
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
        var("NW31K_ROUTER_BIN"),
        var("NW31K_NODE_BIN"),
        var("NW31K_DIR"),
    );
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("set-up: {}: {e}", dir.display()));
    let mut router = spawn(&router_bin, &dir, "router", &[]);
    let (router_port, _) = router.report();
    if router_port == 0 {
        panic!("set-up: the router reported port 0");
    }
    let endpoint = format!("tcp/127.0.0.1:{router_port}");
    println!("NW31K router pid {} port {router_port}", router.child.id());
    let start = |id: &str| {
        let env = [
            ("REPLICA_ID", id.to_string()),
            ("HTTP_PORT", "0".to_string()),
            ("HTTP_BIND_ADDR", "127.0.0.1".to_string()),
            ("LOG_ID", LOG_ID.to_string()),
            ("MOIRAI_TRANSPORT", "zenoh".to_string()),
            ("ZENOH_ROUTER", endpoint.clone()),
        ];
        spawn(&node_bin, &dir, id, &env)
    };
    let ids = ["a", "b", "c"];
    let mut procs: Vec<Proc> = ids.iter().map(|id| start(id)).collect();
    let http_ports: Vec<u16> = procs.iter_mut().map(|p| p.report().1).collect();
    let nodes: Vec<(&str, u16)> = ids
        .iter()
        .copied()
        .zip(http_ports.iter().copied())
        .collect();
    let [a, b, c] = [nodes[0].1, nodes[1].1, nodes[2].1];

    peers_are("mesh", &nodes, &ids, "Connected");

    // Each round starts once every node serves the previous one, so after the
    // second round every node holds an event from each other node that
    // follows the whole first round: the first round is stable everywhere,
    // folded and pruned from the outboxes; the third does the same for the
    // second.
    let mut written: Vec<String> = Vec::new();
    for round in 1..=3 {
        for (id, port) in [("a", a), ("b", b), ("c", c)] {
            let item = format!("{id}{round}");
            post_op("rounds", port, &item);
            written.push(item);
        }
        let items: Vec<&str> = written.iter().map(String::as_str).collect();
        converge("rounds", &nodes, &set(&items));
    }
    let third = set(&["a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3"]);

    let mut d = start("d");
    let d_port = d.report().1;
    let mut all = nodes.clone();
    all.push(("d", d_port));
    converge_within("join", JOIN_LIMIT, &all[3..], &third);
    if !fs::read_to_string(&d.log)
        .unwrap_or_default()
        .contains("[d] adopted state from ")
    {
        panic!(
            "NW31K join: d serves the third state but its log shows no adopted state:\n{}",
            d.tail()
        );
    }
    converge("join", &all, &third);

    post_op("after", d_port, "d1");
    post_op("after", b, "b4");
    let last = set(&[
        "a1", "a2", "a3", "b1", "b2", "b3", "b4", "c1", "c2", "c3", "d1",
    ]);
    converge("after", &all, &last);

    for p in procs.iter_mut() {
        p.alive("end");
    }
    d.alive("end");
    router.alive("end");
    println!("NW31K E2E OK");
}
