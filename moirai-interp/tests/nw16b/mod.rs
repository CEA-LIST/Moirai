//! NW.16b, the part its process tests share (vault `spec/items/NW.16b.md`):
//! the in-tree router `moirai-router` and `model_node` processes started
//! with `PEERS` in their environment (empty with `ZENOH_ROUTER`), on
//! loopback or inside the spec's isolated container, read from outside.
//!
//! Ground truth never comes from the code under test:
//!
//! - which processes a TCP link joins is read in the kernel's tables
//!   (`/proc/net/tcp`, `/proc/net/tcp6`): two ESTABLISHED sockets whose
//!   local and remote addresses mirror each other, each held by one of the
//!   two pids (`/proc/<pid>/fd`); a link between two node processes is a
//!   direct link, and data on it never crosses the router;
//! - listeners and UDP sockets likewise (`nw16a`'s readers);
//! - what a node holds and whom it lists is read through its HTTP API
//!   against class names written in the tests (`nw16a::Cluster`);
//! - whether a peer on Ledgera's scouting configuration met anyone is read
//!   from that session, which the test opens itself from a fixture copied
//!   from Ledgera's code, and from the kernel's tables.
//!
//! Every process started here is killed when its handle is dropped, so a
//! red test leaves none behind. Nodes are started with `PEERS` in the
//! environment, never `PEERS_FILE` with `LISTEN_PORT=0`, so they take
//! NW.16a's P5a path, not the P5b gap.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use zenoh::{Config, Session, Wait};

use crate::nw16a::{
    self, Cluster, Listening, Node, Pending, Reported, Transport, alarms, listening_on,
    socket_inodes, tail, udp_ports_of,
};

/// How long a process may take to write its `PORTS_FILE`.
pub const REPORT_TIMEOUT: Duration = Duration::from_secs(30);
/// Zenoh's (and Ledgera's) multicast scouting port, and Moirai's own.
pub const SCOUTING_PORTS: [u16; 2] = [7446, 7450];
/// How long, once every node lists the others `Connected`, a TCP link may
/// take to join every pair.
pub const LINK_TIMEOUT: Duration = Duration::from_secs(30);

static SEQ: AtomicU32 = AtomicU32::new(0);

fn next() -> u32 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// The router
// ---------------------------------------------------------------------------

/// The `moirai-router` binary: `MOIRAI_E2E_ROUTER_BIN`, or the workspace's
/// debug build. Its absence is the claim's first failure, not a set-up one:
/// the claim says the workspace builds a router binary of that name.
pub fn router_binary() -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var("MOIRAI_E2E_ROUTER_BIN") {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "NW16B ROUTER: MOIRAI_E2E_ROUTER_BIN points at `{raw}`, which is not a file: no \
                 moirai-router binary to run"
            ))
        };
    }
    let built = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/moirai-router");
    if built.is_file() {
        Ok(built)
    } else {
        Err(format!(
            "NW16B ROUTER: no moirai-router binary at {}: the workspace has no router crate \
             `moirai-router` with a binary of that name, or it is not built (`cargo build -p \
             moirai-router`)",
            built.display()
        ))
    }
}

/// A running `moirai-router`, killed when dropped.
pub struct Router {
    pub pid: u32,
    /// The port it reported in `PORTS_FILE`.
    pub port: u16,
    pub log: PathBuf,
    child: Option<Child>,
}

/// The router's `PORTS_FILE`: `None` while absent or not yet whole; its pid
/// and listen port once it parses. Other keys are allowed.
fn read_router_report(path: &Path) -> Option<Result<(u32, u16), String>> {
    let text = fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let pid = value["pid"].as_u64().and_then(|n| u32::try_from(n).ok());
    let port = value["listen_port"]
        .as_u64()
        .and_then(|n| u16::try_from(n).ok());
    Some(match (pid, port) {
        (Some(pid), Some(port)) => Ok((pid, port)),
        _ => Err(format!(
            "it reads {text}, which names no `pid` and `listen_port` numbers"
        )),
    })
}

impl Router {
    /// Start the router with `LISTEN_PORT` (`None`: unset, so its default
    /// port; only ever inside the container) and `PORTS_FILE`, and wait for
    /// its report. From its spawn on, the router is checked for a UDP
    /// socket on a scouting port at every poll and killed at once if it
    /// holds one (R6), so a router that scouts by mistake never runs on.
    pub fn start(scratch: &Path, listen_port: Option<u16>) -> Result<Router, String> {
        let binary = router_binary()?;
        let n = next();
        let log = scratch.join(format!("router.{n}.log"));
        let ports = scratch.join(format!("router.{n}.ports.json"));
        let out = fs::File::create(&log).map_err(|e| format!("set-up: {e}"))?;
        let err_out = out.try_clone().map_err(|e| format!("set-up: {e}"))?;
        let mut command = Command::new(&binary);
        command
            .env("PORTS_FILE", &ports)
            .env_remove("LISTEN_PORT")
            .env_remove("PEERS")
            .env_remove("PEERS_FILE")
            .env_remove("MOIRAI_TRANSPORT")
            .env_remove("ZENOH_CONFIG")
            .env_remove("ZENOH_ROUTER")
            .env_remove("ZENOH_MODE")
            .env_remove("ZENOH_SCOUTING")
            .env_remove("LD_PRELOAD")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err_out));
        if let Some(port) = listen_port {
            command.env("LISTEN_PORT", port.to_string());
        }
        let child = command
            .spawn()
            .map_err(|e| format!("spawning {}: {e}", binary.display()))?;
        let mut router = Router {
            pid: child.id(),
            port: 0,
            log,
            child: Some(child),
        };
        let deadline = Instant::now() + REPORT_TIMEOUT;
        loop {
            router.refuse_scouting()?;
            if let Some(report) = read_router_report(&ports) {
                let (pid, port) =
                    report.map_err(|why| format!("R0: the router's PORTS_FILE: {why}"))?;
                if pid != router.pid {
                    return Err(format!(
                        "R0: the router's PORTS_FILE names pid {pid}, not its own {}",
                        router.pid
                    ));
                }
                if port == 0 {
                    return Err("R0: the router reported listen port 0".into());
                }
                if let Some(given) = listen_port.filter(|p| *p != 0)
                    && port != given
                {
                    return Err(format!(
                        "R0: the router was given LISTEN_PORT={given} and reported {port}"
                    ));
                }
                router.port = port;
                router.refuse_scouting()?;
                return Ok(router);
            }
            if let Some(status) = router.exited() {
                // A port the kernel chose for the test and released may have
                // been taken since by any socket on the host: that is the
                // test's lot, not the router's.
                if let Some(given) = listen_port.filter(|p| *p != 0) {
                    let holders: Vec<String> = listening_on(given)
                        .iter()
                        .map(|l| format!("LISTEN inode {}", l.inode))
                        .chain(
                            established()
                                .iter()
                                .filter(|c| c.local_port() == given)
                                .map(|c| format!("ESTABLISHED inode {}", c.inode)),
                        )
                        .collect();
                    if !holders.is_empty() {
                        return Err(format!(
                            "set-up: port {given}, chosen by the kernel for the test, is held by \
                             another socket ({holders:?}), so the router (exit {status}) could \
                             not bind it; log:\n{}",
                            tail(&router.log, 25)
                        ));
                    }
                }
                return Err(format!(
                    "R0: the router exited with {status} before reporting its port; log:\n{}",
                    tail(&router.log, 25)
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "R0: the router never reported its port within {} s; log:\n{}",
                    REPORT_TIMEOUT.as_secs(),
                    tail(&router.log, 25)
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// R6: the router holds no UDP socket on 7446 or 7450. One that does is
    /// killed before this answers.
    pub fn refuse_scouting(&mut self) -> Result<(), String> {
        let udp = udp_ports_of(self.pid);
        let scouting: Vec<u16> = SCOUTING_PORTS
            .iter()
            .copied()
            .filter(|p| udp.contains(p))
            .collect();
        if scouting.is_empty() {
            return Ok(());
        }
        self.stop();
        Err(format!(
            "R6: the router (pid {}) holds a UDP socket on {scouting:?}, a multicast scouting \
             port, and was killed at once; its UDP ports were {udp:?}",
            self.pid
        ))
    }

    /// R0: exactly one socket listens on the reported port, the router holds
    /// it, it is bound to 0.0.0.0, and the router listens nowhere else.
    pub fn check_listening(&self) -> Result<Listening, String> {
        let one = nw16a::the_listener(self.pid, self.port)
            .map_err(|why| format!("R0: the router reported port {}: {why}", self.port))?;
        if one.local != "00000000" {
            return Err(format!(
                "R0: the router listens on port {} at address {} (hex), not 0.0.0.0",
                self.port, one.local
            ));
        }
        let held = listen_ports_of(self.pid);
        if held != BTreeSet::from([self.port]) {
            return Err(format!(
                "R0: the router holds LISTEN sockets on {held:?}, where it listens on its port {} \
                 only",
                self.port
            ));
        }
        Ok(one)
    }

    pub fn exited(&mut self) -> Option<ExitStatus> {
        self.child
            .as_mut()
            .and_then(|c| c.try_wait().ok().flatten())
    }

    /// SIGKILL and reap, then wait until nothing listens on its port.
    pub fn stop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.port != 0 && !listening_on(self.port).is_empty() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// `VmRSS` and `VmHWM` of the running router, as `/proc` writes them.
    pub fn memory(&self) -> String {
        memory_of(self.pid)
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// `VmRSS` and `VmHWM` of `pid`.
pub fn memory_of(pid: u32) -> String {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    status
        .lines()
        .filter(|l| l.starts_with("VmRSS") || l.starts_with("VmHWM"))
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The byte counters of every ESTABLISHED TCP socket in this network
/// namespace, from the kernel's TCP info as `ss -Htin` prints it: local
/// port, remote port, bytes acknowledged by the other end, bytes received.
/// (`/proc/<pid>/io` does not count what a process takes with `recv`, so it
/// cannot tell what crossed a socket.)
pub fn tcp_bytes() -> Result<Vec<(u16, u16, u64, u64)>, String> {
    let output = Command::new("ss")
        .args(["-Htin", "state", "established"])
        .output()
        .map_err(|e| format!("set-up: running ss: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "set-up: ss failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let port = |endpoint: &str| {
        endpoint
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u16>().ok())
            .unwrap_or(0)
    };
    let mut found = Vec::new();
    let mut current: Option<(u16, u16)> = None;
    for line in text.lines() {
        if !line.starts_with(char::is_whitespace) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            current = (fields.len() >= 4).then(|| (port(fields[2]), port(fields[3])));
        } else if let Some((local, remote)) = current.take() {
            let counter = |key: &str| {
                line.split_whitespace()
                    .find_map(|f| f.strip_prefix(key))
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
            };
            found.push((
                local,
                remote,
                counter("bytes_acked:"),
                counter("bytes_received:"),
            ));
        }
    }
    Ok(found)
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// How one `model_node` is started here: always `MOIRAI_TRANSPORT=zenoh`,
/// `LOG_ID`, `METAMODEL_DIR` and `HTTP_BIND_ADDR=127.0.0.1` (NW.16a's
/// `node_env`), `PEERS` in the environment, never `PEERS_FILE`.
pub struct Spec<'a> {
    pub id: &'a str,
    /// `LISTEN_PORT`: 0, or a port the kernel chose for the test.
    pub listen_port: u16,
    /// `HTTP_PORT`: 0 (the kernel's, reported in `PORTS_FILE`), or a port the
    /// kernel chose for the test.
    pub http_port: u16,
    pub ports_file: bool,
    /// `PEERS`, set even when empty.
    pub peers: String,
    /// `ZENOH_ROUTER=tcp/127.0.0.1:<port>`.
    pub router: Option<u16>,
    /// `ZENOH_MODE=client`.
    pub client: bool,
    /// A library to preload and the variables it reads.
    pub preload: Option<(PathBuf, Vec<(String, String)>)>,
}

impl<'a> Spec<'a> {
    /// A peer-mode node introduced by the router on `router`: port 0,
    /// `PEERS` empty, `PORTS_FILE`.
    pub fn peer(id: &'a str, router: u16) -> Spec<'a> {
        Spec {
            id,
            listen_port: 0,
            http_port: 0,
            ports_file: true,
            peers: String::new(),
            router: Some(router),
            client: false,
            preload: None,
        }
    }

    /// A client-mode node of the router on `router`, given `listen_port`
    /// (a port the kernel chose for the test, never one Moirai would bind
    /// in a deployment) so that a node that binds it anyway is seen.
    pub fn client(id: &'a str, router: u16, listen_port: u16) -> Spec<'a> {
        Spec {
            id,
            listen_port,
            http_port: 0,
            ports_file: true,
            peers: String::new(),
            router: Some(router),
            client: true,
            preload: None,
        }
    }
}

/// What a node wrote to its `PORTS_FILE`, read leniently: a client's
/// `listen_port` may be anything, including null.
#[derive(Clone, Debug)]
pub struct NodeReport {
    pub pid: u32,
    pub http: u16,
    pub listen: Option<u16>,
}

fn read_node_report(path: &Path, id: &str) -> Option<Result<NodeReport, String>> {
    let text = fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let number = |key: &str| value[key].as_u64();
    Some((|| {
        if value["replica_id"].as_str() != Some(id) {
            return Err(format!("{id}'s PORTS_FILE is not its own: {text}"));
        }
        let pid = number("pid")
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| format!("{id}'s PORTS_FILE names no pid: {text}"))?;
        let http = number("http_port")
            .and_then(|n| u16::try_from(n).ok())
            .filter(|p| *p != 0)
            .ok_or_else(|| format!("{id}'s PORTS_FILE names no HTTP port: {text}"))?;
        let listen = number("listen_port").and_then(|n| u16::try_from(n).ok());
        Ok(NodeReport { pid, http, listen })
    })())
}

/// A node started here and not yet handed over: killed when dropped.
pub struct Started {
    pending: Option<Pending>,
    pub ports_file: Option<PathBuf>,
    pub http_fixed: Option<u16>,
    pub listen_port: u16,
}

impl Drop for Started {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            let _ = pending.child.kill();
            let _ = pending.child.wait();
        }
    }
}

/// Start one node as `spec` says.
pub fn start(binary: &Path, scratch: &Path, spec: &Spec) -> Result<Started, String> {
    nw16a::guard_scouting(&[])?;
    let n = next();
    let log = scratch.join(format!("{}.nw16b.{n}.log", spec.id));
    let ports = scratch.join(format!("{}.nw16b.{n}.ports.json", spec.id));
    let out = fs::File::create(&log).map_err(|e| format!("set-up: {e}"))?;
    let err_out = out.try_clone().map_err(|e| format!("set-up: {e}"))?;
    let mut command = Command::new(binary);
    command
        .env("REPLICA_ID", spec.id)
        .env("LISTEN_PORT", spec.listen_port.to_string())
        .env("HTTP_PORT", spec.http_port.to_string())
        .env("PEERS", &spec.peers)
        .env_remove("PEERS_FILE")
        .env_remove("PORTS_FILE")
        .env_remove("BOOTNODE_URL")
        .env_remove("DASHBOARD_URL")
        .env_remove("METAMODEL_PATH")
        .env_remove("ADVERTISE_ADDR")
        .env_remove("ZENOH_SCOUTING")
        .env_remove("ZENOH_ROUTER")
        .env_remove("ZENOH_MODE")
        .env_remove("ZENOH_CONFIG")
        .env_remove("SESSION_ID")
        .env_remove("LD_PRELOAD")
        .envs(nw16a::node_env(Transport::Zenoh, scratch));
    if spec.ports_file {
        command.env("PORTS_FILE", &ports);
    }
    if let Some(port) = spec.router {
        command.env("ZENOH_ROUTER", format!("tcp/127.0.0.1:{port}"));
    }
    if spec.client {
        command.env("ZENOH_MODE", "client");
    }
    if let Some((library, vars)) = &spec.preload {
        command.env("LD_PRELOAD", library);
        command.envs(vars.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|e| format!("spawning {}: {e}", spec.id))?;
    Ok(Started {
        pending: Some(Pending {
            id: spec.id.to_string(),
            child,
            log,
            ports: ports.clone(),
            peers: scratch.join(format!("{}.nw16b.{n}.peers.unused", spec.id)),
            transport: Transport::Zenoh,
        }),
        ports_file: spec.ports_file.then_some(ports),
        http_fixed: (spec.http_port != 0).then_some(spec.http_port),
        listen_port: spec.listen_port,
    })
}

impl Started {
    fn pending(&self) -> &Pending {
        self.pending.as_ref().expect("handed over once")
    }

    fn pending_mut(&mut self) -> &mut Pending {
        self.pending.as_mut().expect("handed over once")
    }

    pub fn id(&self) -> String {
        self.pending().id.clone()
    }

    pub fn pid(&self) -> u32 {
        self.pending().child.id()
    }

    pub fn log(&self) -> PathBuf {
        self.pending().log.clone()
    }

    pub fn tail(&self) -> String {
        tail(&self.pending().log, 25)
    }

    /// The exit status, if the process has exited.
    pub fn exited(&mut self) -> Option<ExitStatus> {
        self.pending_mut().child.try_wait().ok().flatten()
    }

    /// `Err` naming the status and the log if the process has exited.
    pub fn alive(&mut self) -> Result<(), String> {
        match self.exited() {
            None => Ok(()),
            Some(status) => Err(format!(
                "{} (pid {}) exited with {status}; alarms:\n{}\nlast log lines:\n{}",
                self.id(),
                self.pid(),
                alarms(&self.log()),
                self.tail()
            )),
        }
    }

    /// The exit status within `timeout`, if the process exits by then.
    pub fn exit_within(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.exited() {
                return Some(status);
            }
            if Instant::now() > deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// The report the node wrote to its `PORTS_FILE` within `timeout`.
    pub fn await_report(&mut self, timeout: Duration) -> Result<NodeReport, String> {
        let path = self
            .ports_file
            .clone()
            .ok_or("set-up: this node was started without PORTS_FILE")?;
        let id = self.id();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(report) = read_node_report(&path, &id) {
                let report = report?;
                if report.pid != self.pid() {
                    return Err(format!(
                        "{id}'s PORTS_FILE names pid {}, not {}",
                        report.pid,
                        self.pid()
                    ));
                }
                return Ok(report);
            }
            self.alive()
                .map_err(|why| format!("before reporting its ports, {why}"))?;
            if Instant::now() > deadline {
                return Err(format!(
                    "{id} never wrote its PORTS_FILE within {} s; last log lines:\n{}",
                    timeout.as_secs(),
                    self.tail()
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// Hand the node over as an `nw16a::Node`, which kills it when dropped.
    pub fn into_node(mut self, http: u16, listen: u16) -> Node {
        let pending = self.pending.take().expect("handed over once");
        let pid = pending.child.id();
        let reported = Reported {
            replica_id: pending.id.clone(),
            pid,
            listen_port: listen,
            http_port: Some(http),
        };
        pending.into_node(&reported)
    }
}

/// Start the peer-mode nodes `ids`, introduced by the router on `router`,
/// one after the other, each once the one before has reported; healthy and
/// each witnessed on Zenoh (its token seen at the port it reported).
pub fn peers(
    binary: &Path,
    scratch: &Path,
    ids: &[&str],
    router: u16,
) -> Result<Vec<Node>, String> {
    let mut nodes = Vec::new();
    for id in ids {
        let mut started = start(binary, scratch, &Spec::peer(id, router))?;
        let report = started.await_report(REPORT_TIMEOUT)?;
        let listen = report.listen.filter(|p| *p != 0).ok_or_else(|| {
            format!("{id}, a peer-mode node on LISTEN_PORT=0, reported no listen port")
        })?;
        nodes.push(started.into_node(report.http, listen));
    }
    Ok(nodes)
}

/// A cluster of nodes already started, each healthy and, when `witnessed`,
/// seen on Zenoh at its port.
pub fn cluster(
    binary: &Path,
    scratch: &Path,
    nodes: Vec<Node>,
    witnessed: bool,
) -> Result<Cluster, String> {
    let cluster = Cluster::from_nodes(
        Transport::Zenoh,
        nodes,
        scratch.to_path_buf(),
        binary.to_path_buf(),
    )?;
    for index in 0..cluster.nodes.len() {
        cluster.await_health(index)?;
        if witnessed {
            nw16a::witness(&cluster.nodes[index])?;
        }
    }
    Ok(cluster)
}

/// The pids of a cluster's nodes, by id.
pub fn pids(cluster: &Cluster) -> Vec<(String, u32)> {
    cluster
        .nodes
        .iter()
        .map(|n| (n.id.clone(), n.pid))
        .collect()
}

// ---------------------------------------------------------------------------
// The kernel's tables: links between processes
// ---------------------------------------------------------------------------

/// One ESTABLISHED TCP socket: its local and remote `address:port` as the
/// table writes them (hex), and its inode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conn {
    pub local: String,
    pub remote: String,
    pub inode: u64,
}

impl Conn {
    pub fn local_port(&self) -> u16 {
        hex_port(&self.local)
    }

    pub fn remote_port(&self) -> u16 {
        hex_port(&self.remote)
    }
}

fn hex_port(endpoint: &str) -> u16 {
    endpoint
        .rsplit_once(':')
        .and_then(|(_, hex)| u16::from_str_radix(hex, 16).ok())
        .unwrap_or(0)
}

/// Every ESTABLISHED TCP socket in this network namespace.
pub fn established() -> Vec<Conn> {
    let mut found = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 10 || fields[3] != "01" {
                continue;
            }
            if let Ok(inode) = fields[9].parse::<u64>()
                && inode != 0
            {
                found.push(Conn {
                    local: fields[1].to_string(),
                    remote: fields[2].to_string(),
                    inode,
                });
            }
        }
    }
    found
}

/// The ESTABLISHED TCP sockets `pid` holds.
pub fn established_of(pid: u32) -> Vec<Conn> {
    let held = socket_inodes(pid);
    established()
        .into_iter()
        .filter(|c| held.contains(&c.inode))
        .collect()
}

/// Every TCP link between `a` and `b`: the inode pairs of two ESTABLISHED
/// sockets, one held by each, whose ends mirror each other.
pub fn links_between(a: u32, b: u32) -> BTreeSet<(u64, u64)> {
    let all = established();
    let of = |pid: u32| {
        let held = socket_inodes(pid);
        all.iter()
            .filter(|c| held.contains(&c.inode))
            .cloned()
            .collect::<Vec<_>>()
    };
    let (mine, theirs) = (of(a), of(b));
    let mut links = BTreeSet::new();
    for x in &mine {
        for y in &theirs {
            if x.local == y.remote && x.remote == y.local {
                links.insert((x.inode, y.inode));
            }
        }
    }
    links
}

/// Pairs of node ids, each with the links that join it (socket inode pairs).
pub type PairLinks = Vec<((String, String), BTreeSet<(u64, u64)>)>;

/// Every pair of `nodes` with the links that join it.
pub fn pair_links(nodes: &[(String, u32)]) -> PairLinks {
    let mut out = Vec::new();
    for i in 0..nodes.len() {
        for j in i + 1..nodes.len() {
            out.push((
                (nodes[i].0.clone(), nodes[j].0.clone()),
                links_between(nodes[i].1, nodes[j].1),
            ));
        }
    }
    out
}

/// Wait until a TCP link joins every pair of `nodes`; answer the links.
pub fn await_links(nodes: &[(String, u32)], timeout: Duration) -> Result<PairLinks, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let links = pair_links(nodes);
        let missing: Vec<String> = links
            .iter()
            .filter(|(_, l)| l.is_empty())
            .map(|((a, b), _)| format!("{a} and {b}"))
            .collect();
        if missing.is_empty() {
            return Ok(links);
        }
        if Instant::now() > deadline {
            let held: Vec<String> = nodes
                .iter()
                .map(|(id, pid)| {
                    let conns: Vec<String> = established_of(*pid)
                        .iter()
                        .map(|c| format!("{}->{}", c.local_port(), c.remote_port()))
                        .collect();
                    format!("{id} (pid {pid}) holds {conns:?}")
                })
                .collect();
            return Err(format!(
                "no TCP link joins {} within {} s, so their data cannot go peer to peer; {}",
                missing.join(", "),
                timeout.as_secs(),
                held.join("; ")
            ));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// Every port `pid` holds a LISTEN socket on.
pub fn listen_ports_of(pid: u32) -> BTreeSet<u16> {
    let held = socket_inodes(pid);
    let mut ports = BTreeSet::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 10 || fields[3] != "0A" {
                continue;
            }
            if let Ok(inode) = fields[9].parse::<u64>()
                && held.contains(&inode)
            {
                ports.insert(hex_port(fields[1]));
            }
        }
    }
    ports
}

// ---------------------------------------------------------------------------
// The isolated container: addresses, Ledgera's scouting, a default router
// ---------------------------------------------------------------------------

/// The IPv4 addresses local to this network namespace, from
/// `/proc/net/fib_trie` (each `|-- <address>` followed by `/32 host LOCAL`).
pub fn local_ipv4() -> BTreeSet<String> {
    let text = fs::read_to_string("/proc/net/fib_trie").unwrap_or_default();
    let mut found = BTreeSet::new();
    let mut last: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(address) = trimmed.strip_prefix("|-- ") {
            last = Some(address.to_string());
        } else if trimmed.contains("/32 host LOCAL")
            && let Some(address) = last.clone()
        {
            found.insert(address);
        }
    }
    found
}

/// The spec's container, phase 1: `lo` alone, no address but loopback, so a
/// Zenoh session listening on 0.0.0.0 lists no locator.
pub fn isolated_without_addresses() -> Result<(), String> {
    nw16a::isolated().map_err(|why| format!("set-up: not the spec's container: {why}"))?;
    let addresses = local_ipv4();
    if addresses.iter().any(|a| !a.starts_with("127.")) {
        return Err(format!(
            "set-up: L2 needs a namespace with no address but loopback; it has {addresses:?}"
        ));
    }
    Ok(())
}

/// The spec's container, phase 2: `lo` alone, with an address that is not
/// loopback, so a session on 0.0.0.0 lists a locator, and multicast on `lo`.
pub fn isolated_with_an_address() -> Result<(), String> {
    nw16a::isolated().map_err(|why| format!("set-up: not the spec's container: {why}"))?;
    let addresses = local_ipv4();
    if !addresses.iter().any(|a| !a.starts_with("127.")) {
        return Err(format!(
            "set-up: R6 needs an address on lo that is not loopback (10.77.0.1); it has \
             {addresses:?}"
        ));
    }
    Ok(())
}

/// Ledgera's compiled scouting, copied into the fixture with its lines.
pub const LEDGERA_SCOUTING: &str = include_str!("../fixtures/nw16b_ledgera_scouting.json5");

/// A peer opened with Ledgera's compiled scouting configuration: multicast
/// on 224.0.0.224:7446, TTL 1, autoconnect to routers and peers, answering
/// scouts; gossip on, autoconnect to routers and peers. Refused outside the
/// spec's container.
pub fn ledgera_peer() -> Result<Session, String> {
    nw16a::isolated().map_err(|why| {
        format!("refusing to open a multicast-scouting session outside the container: {why}")
    })?;
    let config = Config::from_json5(LEDGERA_SCOUTING)
        .map_err(|e| format!("set-up: the Ledgera scouting fixture: {e}"))?;
    zenoh::open(config)
        .wait()
        .map_err(|e| format!("set-up: opening the Ledgera-configured peer: {e}"))
}

/// A router left on Zenoh's defaults for scouting (multicast on, at
/// 224.0.0.224:7446, answering scouts), listening on a port the kernel
/// chooses: what a `moirai-router` that forgot to turn multicast off would
/// be. The instrument's witness, refused outside the spec's container.
pub fn default_router() -> Result<Session, String> {
    nw16a::isolated().map_err(|why| {
        format!("refusing to open a scouting router outside the container: {why}")
    })?;
    let mut config = Config::default();
    for (key, value) in [
        ("mode", "\"router\""),
        ("listen/endpoints", "[\"tcp/0.0.0.0:0\"]"),
    ] {
        config
            .insert_json5(key, value)
            .map_err(|e| format!("set-up: default router {key}: {e}"))?;
    }
    zenoh::open(config)
        .wait()
        .map_err(|e| format!("set-up: opening the default router: {e}"))
}

/// The Zenoh ids of the peers and of the routers `session` is connected to.
pub fn met(session: &Session) -> (Vec<String>, Vec<String>) {
    let peers = session
        .info()
        .peers_zid()
        .wait()
        .map(|z| z.to_string())
        .collect();
    let routers = session
        .info()
        .routers_zid()
        .wait()
        .map(|z| z.to_string())
        .collect();
    (peers, routers)
}

/// Zenoh ids a session met: id, `peer` or `router`, time since it was
/// first watched.
pub type Seen = Vec<(String, &'static str, Duration)>;

/// A thread that reads whom `session` is connected to every 100 ms and
/// keeps every Zenoh id it ever saw, with when.
pub struct MetWatch {
    stop: Arc<Mutex<bool>>,
    seen: Arc<Mutex<Seen>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MetWatch {
    pub fn start(session: Session) -> MetWatch {
        let stop = Arc::new(Mutex::new(false));
        let seen: Arc<Mutex<Seen>> = Arc::default();
        let since = Instant::now();
        let thread = {
            let stop = Arc::clone(&stop);
            let seen = Arc::clone(&seen);
            thread::spawn(move || {
                while !*stop.lock().unwrap() {
                    let (peers, routers) = met(&session);
                    let mut seen = seen.lock().unwrap();
                    for (zids, kind) in [(peers, "peer"), (routers, "router")] {
                        for zid in zids {
                            if !seen.iter().any(|(z, k, _)| *z == zid && *k == kind) {
                                seen.push((zid, kind, since.elapsed()));
                            }
                        }
                    }
                    drop(seen);
                    thread::sleep(Duration::from_millis(100));
                }
            })
        };
        MetWatch {
            stop,
            seen,
            thread: Some(thread),
        }
    }

    /// Every Zenoh id seen so far: id, peer or router, when.
    pub fn seen(&self) -> Seen {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for MetWatch {
    fn drop(&mut self) {
        *self.stop.lock().unwrap() = true;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

// ---------------------------------------------------------------------------
// O1: a device that blocks one direct link
// ---------------------------------------------------------------------------

/// `connect_shim.c`, built into `scratch` with the host's C compiler.
pub fn connect_shim(scratch: &Path) -> Result<PathBuf, String> {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/nw16b/connect_shim.c");
    let out = scratch.join("nw16b_connect_shim.so");
    let output = Command::new("cc")
        .args(["-shared", "-fPIC", "-O2", "-Wall", "-o"])
        .arg(&out)
        .arg(&source)
        .args(["-ldl"])
        .output()
        .map_err(|e| format!("set-up: running cc on {}: {e}", source.display()))?;
    if !output.status.success() {
        return Err(format!(
            "set-up: cc could not build {}: {}",
            source.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(out)
}

/// The ports the shim refused a `connect` to, from its log.
pub fn refused_connects(log: &Path) -> Vec<u16> {
    fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.strip_prefix("refused "))
        .filter_map(|p| p.trim().parse().ok())
        .collect()
}
