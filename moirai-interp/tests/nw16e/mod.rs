//! NW.16e, the part its process tests share (vault `spec/items/NW.16e.md`):
//! `moirai-router` started with `ROUTER_REGIONS`, and `model_node`
//! processes started with `ZENOH_REGION`, on loopback, read from outside.
//!
//! Ground truth never comes from the code under test:
//!
//! - what the router applied is read in the `PORTS_FILE` it writes, and
//!   what it does with it in what the nodes it serves hold and whom they
//!   list (`nw16a::Cluster`, through each node's HTTP API);
//! - which processes a TCP link joins is read in the kernel's tables
//!   (`nw16b::links_between`), and what a process's sockets received in a
//!   window in the kernel's TCP counters (`nw16d::Window`);
//! - a refusal is read in the exit status and the output of the process.
//!
//! Every process started here is killed when its handle is dropped, so a
//! red test leaves none behind. A router started here binds port 0 only,
//! and is killed at once if it ever holds a UDP socket on a scouting port.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::nw16a::{self, Node, Pending, Reported, Transport, alarms, tail, udp_ports_of};
use crate::nw16b::{self, REPORT_TIMEOUT, SCOUTING_PORTS};

/// The router's variable: a comma-separated list of region names.
pub const ROUTER_REGIONS: &str = "ROUTER_REGIONS";
/// The node's variable: the Zenoh region name it claims.
pub const ZENOH_REGION: &str = "ZENOH_REGION";
/// How long a process given a refused configuration may take to exit.
pub const REFUSAL_TIMEOUT: Duration = Duration::from_secs(15);

static SEQ: AtomicU32 = AtomicU32::new(0);

fn next() -> u32 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Refuse to start anything when the test process carries a variable every
/// child would inherit: NW.16a's Zenoh variables, and this item's two.
pub fn guard_env() -> Result<(), String> {
    nw16a::guard_scouting(&[]).map_err(|why| format!("set-up: {why}"))?;
    for name in [ROUTER_REGIONS, ZENOH_REGION] {
        if std::env::var_os(name).is_some() {
            return Err(format!(
                "set-up: the test process carries {name}, which every process it starts would \
                 inherit; NW.16e sets it per process"
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The router
// ---------------------------------------------------------------------------

/// A `moirai-router` started with `LISTEN_PORT=0`, `PORTS_FILE` and, when
/// given, `ROUTER_REGIONS`; killed when dropped.
pub struct RegionRouter {
    pub pid: u32,
    /// The port it reported.
    pub port: u16,
    pub log: PathBuf,
    /// Its `PORTS_FILE`, parsed.
    pub report: Value,
    child: Option<Child>,
}

/// Spawn the router; answers the child, its log and its ports file.
fn spawn_router(
    scratch: &Path,
    regions: Option<&str>,
) -> Result<(Child, PathBuf, PathBuf), String> {
    guard_env()?;
    let binary = nw16b::router_binary().map_err(|why| format!("set-up: {why}"))?;
    let n = next();
    let log = scratch.join(format!("router.nw16e.{n}.log"));
    let ports = scratch.join(format!("router.nw16e.{n}.ports.json"));
    let out = fs::File::create(&log).map_err(|e| format!("set-up: {e}"))?;
    let err_out = out.try_clone().map_err(|e| format!("set-up: {e}"))?;
    let mut command = Command::new(&binary);
    command
        .env("LISTEN_PORT", "0")
        .env("PORTS_FILE", &ports)
        .env_remove(ROUTER_REGIONS)
        .env_remove(ZENOH_REGION)
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
    if let Some(list) = regions {
        command.env(ROUTER_REGIONS, list);
    }
    let child = command
        .spawn()
        .map_err(|e| format!("set-up: spawning {}: {e}", binary.display()))?;
    Ok((child, log, ports))
}

/// The UDP scouting ports `pid` holds, if any.
fn scouting_ports(pid: u32) -> Vec<u16> {
    let udp = udp_ports_of(pid);
    SCOUTING_PORTS
        .iter()
        .copied()
        .filter(|p| udp.contains(p))
        .collect()
}

impl RegionRouter {
    /// Start the router and wait for its report. A router that holds a UDP
    /// socket on a scouting port at any poll is killed at once (NW.16b R6).
    /// Every failure but a set-up one names point RF: a router given a valid
    /// `ROUTER_REGIONS`, or none, must start and report.
    pub fn start(scratch: &Path, regions: Option<&str>) -> Result<RegionRouter, String> {
        let (child, log, ports) = spawn_router(scratch, regions)?;
        let mut router = RegionRouter {
            pid: child.id(),
            port: 0,
            log,
            report: Value::Null,
            child: Some(child),
        };
        let given = match regions {
            Some(list) => format!("ROUTER_REGIONS=`{list}`"),
            None => "ROUTER_REGIONS unset".to_string(),
        };
        let deadline = Instant::now() + REPORT_TIMEOUT;
        loop {
            router.refuse_scouting()?;
            if let Some(report) = fs::read_to_string(&ports)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            {
                let pid = report["pid"].as_u64();
                let port = report["listen_port"]
                    .as_u64()
                    .and_then(|n| u16::try_from(n).ok());
                if pid != Some(u64::from(router.pid)) {
                    return Err(format!(
                        "RF: the router started with {given} wrote a PORTS_FILE naming pid {pid:?}, \
                         not its own {}: {report}",
                        router.pid
                    ));
                }
                match port {
                    Some(port) if port != 0 => router.port = port,
                    _ => {
                        return Err(format!(
                            "RF: the router started with {given} reported no listen port: {report}"
                        ));
                    }
                }
                router.report = report;
                router.refuse_scouting()?;
                return Ok(router);
            }
            if let Some(status) = router.exited() {
                return Err(format!(
                    "RF: the router started with {given} exited with {status} before reporting \
                     its port; log:\n{}",
                    tail(&router.log, 25)
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "RF: the router started with {given} never reported its port within {} s; \
                     log:\n{}",
                    REPORT_TIMEOUT.as_secs(),
                    tail(&router.log, 25)
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// NW.16b R6: no UDP socket on 7446 or 7450; a router that holds one is
    /// killed before this answers.
    pub fn refuse_scouting(&mut self) -> Result<(), String> {
        let found = scouting_ports(self.pid);
        if found.is_empty() {
            return Ok(());
        }
        self.stop();
        Err(format!(
            "R6: the router (pid {}) holds a UDP socket on {found:?}, a multicast scouting port, \
             and was killed at once",
            self.pid
        ))
    }

    /// NW.16b R0: exactly one socket listens on the reported port, the
    /// router holds it, bound to 0.0.0.0, and it listens nowhere else.
    pub fn check_listening(&self) -> Result<(), String> {
        let one = nw16a::the_listener(self.pid, self.port)
            .map_err(|why| format!("RF: the router reported port {}: {why}", self.port))?;
        if one.local != "00000000" {
            return Err(format!(
                "RF: the router listens on port {} at address {} (hex), not 0.0.0.0",
                self.port, one.local
            ));
        }
        let held = nw16b::listen_ports_of(self.pid);
        if held != std::collections::BTreeSet::from([self.port]) {
            return Err(format!(
                "RF: the router holds LISTEN sockets on {held:?}, where it listens on its port {} \
                 only",
                self.port
            ));
        }
        Ok(())
    }

    /// The `regions` member of its report, as written (absent: `None`).
    pub fn applied(&self) -> Option<&Value> {
        self.report.get("regions")
    }

    pub fn exited(&mut self) -> Option<ExitStatus> {
        self.child
            .as_mut()
            .and_then(|c| c.try_wait().ok().flatten())
    }

    pub fn tail(&self) -> String {
        tail(&self.log, 25)
    }

    /// SIGKILL and reap.
    pub fn stop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}

impl Drop for RegionRouter {
    fn drop(&mut self) {
        self.stop();
    }
}

/// How a process given a configuration it must refuse ended.
#[derive(Debug)]
pub struct Ended {
    pub label: String,
    /// Its exit status, or `None` if it was still running at the deadline
    /// (it is then killed).
    pub status: Option<ExitStatus>,
    pub took: Duration,
    /// Everything it printed.
    pub output: String,
    /// Whether it wrote its `PORTS_FILE`.
    pub reported: bool,
    /// Scouting ports it was seen holding (it is killed at once if so).
    pub scouting: Vec<u16>,
}

impl Ended {
    /// It exited by itself, non-zero.
    pub fn refused(&self) -> bool {
        self.status
            .is_some_and(|status| status.code().is_some_and(|code| code != 0))
    }

    /// Whether its output names every one of `names`.
    pub fn names_all(&self, names: &[&str]) -> bool {
        names.iter().all(|name| self.output.contains(name))
    }

    pub fn line(&self) -> String {
        let first: Vec<&str> = self.output.lines().take(6).collect();
        format!(
            "{}: status {:?} after {:?}, PORTS_FILE written {}, scouting ports {:?}, output: {:?}",
            self.label, self.status, self.took, self.reported, self.scouting, first
        )
    }
}

/// One process per case, started together; each waited for until it exits
/// or `REFUSAL_TIMEOUT` passes, then killed. `pid` reads the child's id.
fn wait_all(mut running: Vec<(String, Child, PathBuf, PathBuf, Instant)>) -> Vec<Ended> {
    let deadline = Instant::now() + REFUSAL_TIMEOUT;
    let mut done: Vec<Option<Ended>> = running.iter().map(|_| None).collect();
    loop {
        let mut open = 0;
        for (index, (label, child, log, ports, started)) in running.iter_mut().enumerate() {
            if done[index].is_some() {
                continue;
            }
            let scouting = scouting_ports(child.id());
            let status = child.try_wait().ok().flatten();
            let killed_for_scouting = !scouting.is_empty() && status.is_none();
            if killed_for_scouting {
                let _ = child.kill();
                let _ = child.wait();
            }
            if status.is_some() || killed_for_scouting || Instant::now() > deadline {
                if status.is_none() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                done[index] = Some(Ended {
                    label: label.clone(),
                    status,
                    took: started.elapsed(),
                    output: fs::read_to_string(&*log).unwrap_or_default(),
                    reported: ports.exists(),
                    scouting,
                });
            } else {
                open += 1;
            }
        }
        if open == 0 {
            return done
                .into_iter()
                .map(|e| e.expect("every case ended"))
                .collect();
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Start one router per `(label, ROUTER_REGIONS)` case, all at once, and
/// read how each ended within `REFUSAL_TIMEOUT`.
pub fn refused_routers(scratch: &Path, cases: &[(&str, &str)]) -> Result<Vec<Ended>, String> {
    let mut running = Vec::new();
    for (label, list) in cases {
        let (child, log, ports) = spawn_router(scratch, Some(list))?;
        running.push((label.to_string(), child, log, ports, Instant::now()));
    }
    Ok(wait_all(running))
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// How one `model_node` is started here: `MOIRAI_TRANSPORT=zenoh`, `LOG_ID`,
/// `METAMODEL_DIR` and `HTTP_BIND_ADDR=127.0.0.1` (NW.16a's `node_env`),
/// `HTTP_PORT=0`, `PEERS` set even when empty, never `PEERS_FILE`, and
/// `PORTS_FILE`.
#[derive(Clone, Debug)]
pub struct NodeSpec {
    pub id: String,
    /// `LISTEN_PORT`: 0, or a port the kernel chose for the test.
    pub listen_port: u16,
    /// `PEERS`.
    pub peers: String,
    /// `ZENOH_ROUTER=tcp/127.0.0.1:<port>`.
    pub router: Option<u16>,
    /// `ZENOH_REGION`, set to this value (possibly empty) when `Some`.
    pub region: Option<String>,
    /// `ZENOH_MODE=client`.
    pub client: bool,
    /// A library to preload and the variables it reads.
    pub preload: Option<(PathBuf, Vec<(String, String)>)>,
    /// Further variables, set last.
    pub extra: Vec<(String, String)>,
}

impl NodeSpec {
    /// A peer-mode node introduced by the router on `router`, of `region`
    /// (`None`: no `ZENOH_REGION`), on port 0, `PEERS` empty.
    pub fn peer(id: &str, router: u16, region: Option<&str>) -> NodeSpec {
        NodeSpec {
            id: id.to_string(),
            listen_port: 0,
            peers: String::new(),
            router: Some(router),
            region: region.map(str::to_string),
            client: false,
            preload: None,
            extra: Vec::new(),
        }
    }

    /// A client-mode node of the router on `router`, no `ZENOH_REGION`,
    /// given `listen_port` (a port the kernel chose for the test), as NW.16b
    /// starts its clients.
    pub fn client(id: &str, router: u16, listen_port: u16) -> NodeSpec {
        NodeSpec {
            id: id.to_string(),
            listen_port,
            peers: String::new(),
            router: Some(router),
            region: None,
            client: true,
            preload: None,
            extra: Vec::new(),
        }
    }
}

/// A node started here and not yet handed over: killed when dropped.
pub struct Booting {
    pending: Option<Pending>,
    ports: PathBuf,
    started: Instant,
}

impl Drop for Booting {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            let _ = pending.child.kill();
            let _ = pending.child.wait();
        }
    }
}

/// Start one node as `spec` says.
pub fn start_node(binary: &Path, scratch: &Path, spec: &NodeSpec) -> Result<Booting, String> {
    guard_env()?;
    let n = next();
    let log = scratch.join(format!("{}.nw16e.{n}.log", spec.id));
    let ports = scratch.join(format!("{}.nw16e.{n}.ports.json", spec.id));
    let out = fs::File::create(&log).map_err(|e| format!("set-up: {e}"))?;
    let err_out = out.try_clone().map_err(|e| format!("set-up: {e}"))?;
    let mut command = Command::new(binary);
    command
        .env("REPLICA_ID", &spec.id)
        .env("LISTEN_PORT", spec.listen_port.to_string())
        .env("HTTP_PORT", "0")
        .env("PEERS", &spec.peers)
        .env("PORTS_FILE", &ports)
        .env_remove("PEERS_FILE")
        .env_remove("BOOTNODE_URL")
        .env_remove("DASHBOARD_URL")
        .env_remove("METAMODEL_PATH")
        .env_remove("ADVERTISE_ADDR")
        .env_remove("ZENOH_SCOUTING")
        .env_remove("ZENOH_ROUTER")
        .env_remove("ZENOH_MODE")
        .env_remove("ZENOH_CONFIG")
        .env_remove(ZENOH_REGION)
        .env_remove(ROUTER_REGIONS)
        .env_remove("SESSION_ID")
        .env_remove("LD_PRELOAD")
        .envs(nw16a::node_env(Transport::Zenoh, scratch));
    if let Some(port) = spec.router {
        command.env("ZENOH_ROUTER", format!("tcp/127.0.0.1:{port}"));
    }
    if spec.client {
        command.env("ZENOH_MODE", "client");
    }
    if let Some(region) = &spec.region {
        command.env(ZENOH_REGION, region);
    }
    if let Some((library, vars)) = &spec.preload {
        command.env("LD_PRELOAD", library);
        command.envs(vars.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }
    command.envs(spec.extra.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|e| format!("set-up: spawning {}: {e}", spec.id))?;
    Ok(Booting {
        pending: Some(Pending {
            id: spec.id.clone(),
            child,
            log,
            ports: ports.clone(),
            peers: scratch.join(format!("{}.nw16e.{n}.peers.unused", spec.id)),
            transport: Transport::Zenoh,
        }),
        ports,
        started: Instant::now(),
    })
}

/// What a node wrote to its `PORTS_FILE`: pid, HTTP port, listen port.
#[derive(Clone, Debug)]
pub struct NodeReport {
    pub pid: u32,
    pub http: u16,
    pub listen: Option<u16>,
}

impl Booting {
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

    pub fn tail(&self) -> String {
        tail(&self.pending().log, 25)
    }

    pub fn exited(&mut self) -> Option<ExitStatus> {
        self.pending_mut().child.try_wait().ok().flatten()
    }

    /// The report the node wrote within `timeout`. Its errors name no point:
    /// the caller labels them (a client the router does not carry never
    /// opens its session, so never reports, which is the claim's failure).
    pub fn await_report(&mut self, timeout: Duration) -> Result<NodeReport, String> {
        let id = self.id();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(value) = fs::read_to_string(&self.ports)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            {
                if value["replica_id"].as_str() != Some(id.as_str()) {
                    return Err(format!("{id}'s PORTS_FILE is not its own: {value}"));
                }
                let pid = value["pid"].as_u64().and_then(|n| u32::try_from(n).ok());
                if pid != Some(self.pid()) {
                    return Err(format!(
                        "{id}'s PORTS_FILE names pid {pid:?}, not {}",
                        self.pid()
                    ));
                }
                let http = value["http_port"]
                    .as_u64()
                    .and_then(|n| u16::try_from(n).ok())
                    .filter(|p| *p != 0)
                    .ok_or_else(|| format!("{id}'s PORTS_FILE names no HTTP port: {value}"))?;
                let listen = value["listen_port"]
                    .as_u64()
                    .and_then(|n| u16::try_from(n).ok());
                return Ok(NodeReport {
                    pid: self.pid(),
                    http,
                    listen,
                });
            }
            if let Some(status) = self.exited() {
                return Err(format!(
                    "{id} (pid {}) exited with {status} before reporting its ports; alarms:\n{}\n\
                     last log lines:\n{}",
                    self.pid(),
                    alarms(&self.pending().log),
                    self.tail()
                ));
            }
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
        let reported = Reported {
            replica_id: pending.id.clone(),
            pid: pending.child.id(),
            listen_port: listen,
            http_port: Some(http),
        };
        pending.into_node(&reported)
    }

    /// Read how the node ended: its status if it exited by `deadline`
    /// (otherwise it is killed and `None`), its output, whether it reported.
    fn end(mut self, label: &str, deadline: Instant) -> Ended {
        let mut status = None;
        while Instant::now() <= deadline {
            status = self.exited();
            if status.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let pending = self.pending.as_mut().expect("not handed over");
        let scouting = scouting_ports(pending.child.id());
        if status.is_none() {
            let _ = pending.child.kill();
            let _ = pending.child.wait();
        }
        Ended {
            label: label.to_string(),
            status,
            took: self.started.elapsed(),
            output: fs::read_to_string(&pending.log).unwrap_or_default(),
            reported: self.ports.exists(),
            scouting,
        }
    }
}

/// Start one node per `(label, spec)` case, all at once, and read how each
/// ended within `REFUSAL_TIMEOUT` (a node still running then is killed and
/// read as not exited).
pub fn nodes_until(
    binary: &Path,
    scratch: &Path,
    cases: &[(&str, NodeSpec)],
) -> Result<Vec<Ended>, String> {
    let mut started = Vec::new();
    for (label, spec) in cases {
        started.push((label.to_string(), start_node(binary, scratch, spec)?));
    }
    let deadline = Instant::now() + REFUSAL_TIMEOUT;
    Ok(started
        .into_iter()
        .map(|(label, booting)| booting.end(&label, deadline))
        .collect())
}

/// Start a peer-mode node as `spec` says, wait for its report, and hand it
/// over; it reports the port its session listens on.
pub fn peer_node(binary: &Path, scratch: &Path, spec: &NodeSpec) -> Result<Node, String> {
    let mut booting = start_node(binary, scratch, spec)?;
    let report = booting.await_report(REPORT_TIMEOUT)?;
    let listen = report
        .listen
        .filter(|p| *p != 0)
        .ok_or_else(|| format!("{}, a peer-mode node, reported no listen port", spec.id))?;
    Ok(booting.into_node(report.http, listen))
}

/// Start a peer-mode node `id` of `region` (`None`: no `ZENOH_REGION`),
/// introduced by the router on `router`.
pub fn peer(
    binary: &Path,
    scratch: &Path,
    id: &str,
    router: u16,
    region: Option<&str>,
) -> Result<Node, String> {
    peer_node(binary, scratch, &NodeSpec::peer(id, router, region))
}

/// The links joining `pid` and `other`, for the record.
pub fn link_count(pid: u32, other: u32) -> usize {
    nw16b::links_between(pid, other).len()
}
