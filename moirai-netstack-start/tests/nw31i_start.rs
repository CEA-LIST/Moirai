//! NW.31i (vault spec/items/NW.31i.md): a node started by
//! moirai_netstack_start::run from the environment honours model_node's
//! launcher contract (601c887, model_node.rs 416 to 716): ports chosen by the
//! kernel and reported in PORTS_FILE, peers read from a PEERS_FILE written
//! after the report, the HTTP port bound before the peers are read, and under
//! Zenoh the replication port held from the report until it exits (NW.30).
//!
//! The nodes are this test binary run again with NW31I_NODE=1 and the filter
//! nw31i_node, which then calls run(ToyFactory, ()) and never returns; in a
//! normal run that test does nothing. Ground truth is outside the code under
//! test: the kernel's tables (/proc/net/tcp*, /proc/<pid>/fd), binds this
//! process makes itself, and the toy state read back over HTTP.
//!
//! nw31i_three_zenoh_nodes_told_only_the_router_converge needs the built
//! moirai-netstack-router binary in NW31I_ROUTER_BIN; tests/nw31i/gate.sh
//! builds it and runs this file under nodes.lock with one test thread.
//! A panic starting "set-up:" is a failure of the instrument, never a pass.

use std::collections::BTreeSet;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use moirai_netstack_testkit::ToyFactory;
use serde_json::{Value, json};

/// The default log every node of a test hosts.
const LOG_ID: &str = "0123456789abcdef0123456789abcdef";
const REPORT: Duration = Duration::from_secs(30);
const MESH: Duration = Duration::from_secs(30);
const CONVERGE: Duration = Duration::from_secs(30);
/// How long the Zenoh nodes are watched after their report, before their peers are written.
const BEFORE_PEERS: Duration = Duration::from_secs(3);

/// The node this binary becomes when run again with NW31I_NODE=1.
#[test]
#[allow(unreachable_code, unused_must_use, clippy::unit_arg)]
fn nw31i_node() {
    if std::env::var_os("NW31I_NODE").is_none() {
        return;
    }
    moirai_netstack_start::run(ToyFactory, ());
    eprintln!("NW31I node: run returned");
    std::process::exit(3);
}

// ------------------------------------------------------------------ processes

/// A child process, killed and reaped on every exit path.
struct Proc {
    id: String,
    child: Child,
    log: PathBuf,
    ports: PathBuf,
    peers: PathBuf,
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug)]
struct Report {
    pid: u32,
    listen: u16,
    http: u16,
}

fn scratch(tag: &str) -> PathBuf {
    let base = std::env::var_os("NW31I_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(|home| PathBuf::from(home).join(".cache/cea-crdt-scratch/NW.31i/run"))
        })
        .unwrap_or_else(|| panic!("set-up: neither NW31I_DIR nor HOME is set"));
    let dir = base.join(format!("{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("set-up: {}: {e}", dir.display()));
    dir
}

/// Start a program with a ports file, a log and the given variables on top
/// of an environment cleared of every launcher and Zenoh variable.
fn spawn(program: &Path, args: &[&str], dir: &Path, id: &str, env: &[(&str, String)]) -> Proc {
    let log = dir.join(format!("{id}.log"));
    let ports = dir.join(format!("{id}.ports.json"));
    let peers = dir.join(format!("{id}.peers"));
    let out = fs::File::create(&log).unwrap_or_else(|e| panic!("set-up: {e}"));
    let err = out.try_clone().unwrap_or_else(|e| panic!("set-up: {e}"));
    let mut command = Command::new(program);
    command.args(args);
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
        "NW31I_NODE",
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
        peers,
    }
}

/// A node: this binary as nw31i_node, on the toy, ports 0, HTTP on 127.0.0.1.
fn node(dir: &Path, id: &str, env: &[(&str, String)]) -> Proc {
    let exe = std::env::current_exe().unwrap_or_else(|e| panic!("set-up: {e}"));
    let mut all = vec![
        ("NW31I_NODE", "1".to_string()),
        ("REPLICA_ID", id.to_string()),
        ("HTTP_PORT", "0".to_string()),
        ("HTTP_BIND_ADDR", "127.0.0.1".to_string()),
        ("LOG_ID", LOG_ID.to_string()),
    ];
    all.extend(env.iter().cloned());
    let args = ["nw31i_node", "--exact", "--nocapture", "--test-threads=1"];
    spawn(&exe, &args, dir, id, &all)
}

impl Proc {
    fn tail(&self) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(25)..].join("\n")
    }

    /// The ports file, read every 1 ms until it appears: whenever it exists
    /// it must be whole JSON (written beside and renamed), name this process
    /// and, for a node, its own id and two distinct ports that are not 0.
    fn await_report(&mut self, is_node: bool) -> Result<Report, String> {
        let deadline = Instant::now() + REPORT;
        loop {
            match fs::read_to_string(&self.ports) {
                Ok(text) => {
                    let v: Value = serde_json::from_str(&text).map_err(|e| {
                        format!(
                            "{}'s PORTS_FILE was read as {text:?}, not whole JSON: {e}",
                            self.id
                        )
                    })?;
                    let port = |k: &str| v[k].as_u64().unwrap_or(0) as u16;
                    let report = Report {
                        pid: v["pid"].as_u64().unwrap_or(0) as u32,
                        listen: port("listen_port"),
                        http: port("http_port"),
                    };
                    let wrong = report.pid != self.child.id()
                        || report.listen == 0
                        || (is_node
                            && (v["replica_id"] != json!(self.id)
                                || report.http == 0
                                || report.http == report.listen));
                    if wrong {
                        return Err(format!(
                            "{}'s PORTS_FILE is not its own or names an unbound port: {v}",
                            self.id
                        ));
                    }
                    return Ok(report);
                }
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(format!("{}'s PORTS_FILE: {e}", self.id)),
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!(
                    "{} exited with {status} before reporting its ports:\n{}",
                    self.id,
                    self.tail()
                ));
            }
            if Instant::now() > deadline {
                return Err(format!("{} never wrote PORTS_FILE:\n{}", self.id, self.tail()));
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Write the node's PEERS_FILE, whole: id:127.0.0.1:port,...
    fn tell_peers(&self, peers: &[(&str, u16)]) {
        let text: Vec<String> = peers
            .iter()
            .map(|(id, p)| format!("{id}:127.0.0.1:{p}"))
            .collect();
        let staging = self.peers.with_extension("staging");
        fs::write(&staging, text.join(",")).unwrap_or_else(|e| panic!("set-up: {e}"));
        fs::rename(&staging, &self.peers).unwrap_or_else(|e| panic!("set-up: {e}"));
    }

    fn alive(&mut self) -> Result<(), String> {
        match self.child.try_wait() {
            Ok(None) => Ok(()),
            other => Err(format!("{} is gone ({other:?}):\n{}", self.id, self.tail())),
        }
    }
}

fn ok<T>(test: &str, phase: &str, result: Result<T, String>) -> T {
    result.unwrap_or_else(|why| panic!("NW31I {test} {phase}: {why}"))
}

// ------------------------------------------------------------------ HTTP and state

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

/// Retry the probe until it holds or the limit passes.
fn until<T>(limit: Duration, mut probe: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    let deadline = Instant::now() + limit;
    loop {
        match probe() {
            Ok(value) => return Ok(value),
            Err(why) if Instant::now() > deadline => return Err(why),
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// POST one toy element to a node's default log.
fn post_op(port: u16, element: &str) -> Result<(), String> {
    let body = json!(element).to_string();
    until(CONVERGE, || match http(port, "POST", "/api/op", Some(&body))? {
        (200, answer) if answer["success"] == json!(true) => Ok(()),
        other => Err(format!("POST /api/op {element:?} on {port} answered {other:?}")),
    })
}

/// Every node on the given HTTP ports serves the wanted set on GET /api/state.
fn converge(http_ports: &[u16], want: &[&str]) -> Result<(), String> {
    let want = json!(want);
    until(CONVERGE, || {
        for &port in http_ports {
            let got = http(port, "GET", "/api/state", None)?;
            if got != (200, want.clone()) {
                return Err(format!("node on HTTP {port} serves {got:?}, not {want}"));
            }
        }
        Ok(())
    })
}

/// Every node lists every other one Connected on GET /api/peers.
fn mesh(nodes: &[(&str, u16)]) -> Result<(), String> {
    until(MESH, || {
        for (id, port) in nodes {
            let (_, peers) = http(*port, "GET", "/api/peers", None)?;
            for (other, _) in nodes.iter().filter(|(o, _)| o != id) {
                let status = peers["peers"]
                    .as_array()
                    .and_then(|l| l.iter().find(|p| p["id"] == json!(other)))
                    .map(|p| p["status"].clone());
                if status != Some(json!("Connected")) {
                    return Err(format!("{id} reads {other} {status:?}: the mesh never formed"));
                }
            }
        }
        Ok(())
    })
}

// ------------------------------------------------------------------ the kernel's tables (NW.16a, NW.30)

/// (state, local port, inode) of every TCP socket, from /proc/net/tcp and tcp6.
fn sockets() -> Vec<(String, u16, u64)> {
    let mut found = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 10 {
                continue;
            }
            let port = f[1]
                .rsplit_once(':')
                .and_then(|(_, h)| u16::from_str_radix(h, 16).ok());
            if let (Some(port), Ok(inode)) = (port, f[9].parse()) {
                found.push((f[3].to_string(), port, inode));
            }
        }
    }
    found
}

/// The socket inodes a process holds open.
fn socket_inodes(pid: u32) -> BTreeSet<u64> {
    let mut inodes = BTreeSet::new();
    for entry in fs::read_dir(format!("/proc/{pid}/fd"))
        .into_iter()
        .flatten()
        .flatten()
    {
        if let Ok(target) = fs::read_link(entry.path()) {
            let text = target.to_string_lossy().into_owned();
            if let Some(n) = text
                .strip_prefix("socket:[")
                .and_then(|r| r.strip_suffix(']'))
            {
                inodes.extend(n.parse::<u64>().ok());
            }
        }
    }
    inodes
}

/// The ports of the LISTEN sockets a process holds.
fn held_listen_ports(pid: u32) -> BTreeSet<u16> {
    let held = socket_inodes(pid);
    sockets()
        .into_iter()
        .filter(|(s, _, i)| s == "0A" && held.contains(i))
        .map(|(_, p, _)| p)
        .collect()
}

/// The inode of the one LISTEN socket on a port, which the process must hold.
fn the_listener(pid: u32, port: u16) -> Result<u64, String> {
    let all: Vec<u64> = sockets()
        .into_iter()
        .filter(|(s, p, _)| s == "0A" && *p == port)
        .map(|(_, _, i)| i)
        .collect();
    match all.as_slice() {
        [one] if socket_inodes(pid).contains(one) => Ok(*one),
        [one] => Err(format!(
            "the one LISTEN socket on port {port} (inode {one}) is not held by pid {pid}"
        )),
        [] => Err(format!("no socket listens on port {port}")),
        many => Err(format!("{} sockets listen on port {port}", many.len())),
    }
}

/// A bind of the port by this process, on 127.0.0.1 and on 0.0.0.0, must be
/// refused as address in use.
fn taken_by_another(port: u16) -> Result<(), String> {
    for addr in ["127.0.0.1", "0.0.0.0"] {
        match TcpListener::bind((addr, port)) {
            Err(e) if e.kind() == ErrorKind::AddrInUse => {}
            Err(e) => {
                return Err(format!(
                    "binding {addr}:{port} failed with {e}, not address in use"
                ));
            }
            Ok(_) => {
                return Err(format!(
                    "this test bound {addr}:{port}, the port the node reported: the node did not \
                     hold it"
                ));
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ the gate's tests

/// TCP direct: three nodes on ports 0 report before any peer is known, hold
/// the ports they report, read a PEERS_FILE written after every report, and
/// converge one toy element written on each.
#[test]
fn nw31i_tcp_port_0_nodes_report_their_ports_read_a_late_peers_file_and_converge() {
    const T: &str = "tcp";
    let dir = scratch("tcp");
    let ids = ["a", "b", "c"];
    let mut procs: Vec<Proc> = ids
        .iter()
        .map(|id| {
            let peers = dir.join(format!("{id}.peers")).display().to_string();
            let env = [("MOIRAI_TRANSPORT", "tcp".into()), ("PEERS_FILE", peers)];
            node(&dir, id, &env)
        })
        .collect();
    let reports: Vec<Report> = procs
        .iter_mut()
        .map(|p| ok(T, "report", p.await_report(true)))
        .collect();
    for (p, r) in procs.iter().zip(&reports) {
        let held = held_listen_ports(r.pid);
        assert_eq!(
            held,
            BTreeSet::from([r.listen, r.http]),
            "NW31I {T} report: {} holds LISTEN sockets on {held:?}, not on its reported ports {r:?}",
            p.id
        );
    }
    thread::sleep(Duration::from_secs(1));
    for p in &mut procs {
        ok(T, "waiting for PEERS_FILE", p.alive());
    }
    for (i, p) in procs.iter().enumerate() {
        let others: Vec<(&str, u16)> = (0..3)
            .filter(|&j| j != i)
            .map(|j| (ids[j], reports[j].listen))
            .collect();
        p.tell_peers(&others);
    }
    let http: Vec<u16> = reports.iter().map(|r| r.http).collect();
    for (i, id) in ids.iter().enumerate() {
        ok(T, "op", post_op(http[i], &format!("{id}1")));
    }
    ok(T, "convergence", converge(&http, &["a1", "b1", "c1"]));
    for (p, r) in procs.iter().zip(&reports) {
        let held = the_listener(r.pid, r.listen).map_err(|w| format!("{}: {w}", p.id));
        ok(T, "after", held);
    }
}

/// NW.30's H and P on the launcher (601c887's nw30_zenoh_port0_held_l3.rs,
/// ported by name): under Zenoh, two nodes on port 0 with a PEERS_FILE
/// written only later hold the replication port they report from the report
/// until the end (no bind by another process succeeds, same socket inode,
/// LISTEN sockets on the reported ports only), and still mesh and exchange
/// once the file names each other.
#[test]
fn h_p_a_port_0_node_with_a_peers_file_holds_its_reported_port_and_meshes_zenoh() {
    const T: &str = "zenoh-held";
    let dir = scratch("zenoh-held");
    let ids = ["a", "b"];
    let mut procs: Vec<Proc> = ids
        .iter()
        .map(|id| {
            let peers = dir.join(format!("{id}.peers")).display().to_string();
            let env = [("MOIRAI_TRANSPORT", "zenoh".into()), ("PEERS_FILE", peers)];
            node(&dir, id, &env)
        })
        .collect();
    let reports: Vec<Report> = procs
        .iter_mut()
        .map(|p| ok(T, "report", p.await_report(true)))
        .collect();
    let at_report: Vec<u64> = reports
        .iter()
        .map(|r| ok(T, "at the report", the_listener(r.pid, r.listen)))
        .collect();
    let until_peers = Instant::now() + BEFORE_PEERS;
    while Instant::now() < until_peers {
        for r in &reports {
            ok(T, "before PEERS_FILE", taken_by_another(r.listen));
        }
        thread::sleep(Duration::from_millis(1));
    }
    for p in &mut procs {
        ok(T, "waiting for PEERS_FILE", p.alive());
    }
    let stop = Arc::new(AtomicBool::new(false));
    let prober = {
        let stop = Arc::clone(&stop);
        let ports: Vec<u16> = reports.iter().map(|r| r.listen).collect();
        thread::spawn(move || -> Result<u64, String> {
            let mut rounds = 0;
            while !stop.load(Ordering::Relaxed) {
                for &port in &ports {
                    taken_by_another(port)?;
                }
                rounds += 1;
                thread::sleep(Duration::from_millis(1));
            }
            Ok(rounds)
        })
    };
    procs[0].tell_peers(&[("b", reports[1].listen)]);
    procs[1].tell_peers(&[("a", reports[0].listen)]);
    let meshed = mesh(&[("a", reports[0].http), ("b", reports[1].http)]);
    stop.store(true, Ordering::Relaxed);
    let rounds = prober.join().expect("the prober does not panic");
    let rounds = ok(T, "between PEERS_FILE and the mesh", rounds);
    ok(T, "the mesh", meshed);
    println!("NW31I {T}: {rounds} bind rounds between PEERS_FILE and the mesh, all refused");
    ok(T, "op", post_op(reports[0].http, "a1"));
    ok(T, "op", post_op(reports[1].http, "b1"));
    let both = [reports[0].http, reports[1].http];
    ok(T, "exchange", converge(&both, &["a1", "b1"]));
    for (i, r) in reports.iter().enumerate() {
        ok(T, "after the exchange", taken_by_another(r.listen));
        let now = ok(T, "after the exchange", the_listener(r.pid, r.listen));
        assert_eq!(
            now, at_report[i],
            "NW31I {T} after the exchange: {} released its reported port {} (inode {} at the \
             report, {now} now)",
            ids[i], r.listen, at_report[i]
        );
        let held = held_listen_ports(r.pid);
        assert_eq!(
            held,
            BTreeSet::from([r.http, r.listen]),
            "NW31I {T} after the exchange: {} holds LISTEN sockets on {held:?}, not on its \
             reported ports only",
            ids[i]
        );
    }
}

/// Three Zenoh nodes given no peer, only ZENOH_ROUTER naming the built
/// moirai-netstack-router started on port 0, converge one toy element
/// written on each.
#[test]
fn nw31i_three_zenoh_nodes_told_only_the_router_converge() {
    const T: &str = "router";
    let bin = std::env::var_os("NW31I_ROUTER_BIN")
        .map(PathBuf::from)
        .filter(|b| b.is_file())
        .unwrap_or_else(|| {
            panic!("set-up: NW31I_ROUTER_BIN does not name the built moirai-netstack-router")
        });
    let dir = scratch("router");
    let mut router = spawn(&bin, &[], &dir, "router", &[]);
    let at = ok(T, "router report", router.await_report(false));
    let endpoint = format!("tcp/127.0.0.1:{}", at.listen);
    let ids = ["a", "b", "c"];
    let mut procs: Vec<Proc> = ids
        .iter()
        .map(|id| {
            let env = [
                ("MOIRAI_TRANSPORT", "zenoh".into()),
                ("ZENOH_ROUTER", endpoint.clone()),
            ];
            node(&dir, id, &env)
        })
        .collect();
    let http: Vec<u16> = procs
        .iter_mut()
        .map(|p| ok(T, "report", p.await_report(true)).http)
        .collect();
    for (i, id) in ids.iter().enumerate() {
        ok(T, "op", post_op(http[i], &format!("{id}1")));
    }
    ok(T, "convergence", converge(&http, &["a1", "b1", "c1"]));
    ok(T, "router", router.alive());
}
