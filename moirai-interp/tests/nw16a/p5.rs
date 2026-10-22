//! NW.16a round 2, the helpers of P5a and P5b (vault `spec/items/NW.16a.md`,
//! section "Round 2, Cam's ruling A (2026-10-22)").
//!
//! A node is started here with the port and peers each case names
//! (`LISTEN_PORT` 0 or a port the kernel chose for the test, `PEERS` in the
//! environment or `PEERS_FILE`) and with `bind_shim.c` preloaded, which
//! records every TCP bind the node makes, can hold the node at the moment
//! its `PORTS_FILE` appears, and can make a second bind of a kernel-chosen
//! port fail as a port taken in the gap fails. Ground truth stays outside
//! the code under test: the kernel's tables (`/proc/net/tcp`,
//! `/proc/net/tcp6`, `/proc/<pid>/fd`) and the binds as the C library sees
//! them.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::nw16a::{
    Node, Pending, Reported, Transport, alarms, guard_scouting, launch, listening_on,
    socket_inodes, tail,
};

/// How long a node with a fixed `LISTEN_PORT` and `PEERS_FILE` is watched
/// after its report, before it is told its peers, for a bind of its port.
pub const BEFORE_PEERS: Duration = Duration::from_secs(2);
/// How long a node whose session cannot bind its reported port has to stop.
pub const TO_EXIT: Duration = Duration::from_secs(30);

/// `bind_shim.c`, built into `scratch` with the host's C compiler.
pub fn shim(scratch: &Path) -> Result<PathBuf, String> {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/nw16a/bind_shim.c");
    let out = scratch.join("nw16a_bind_shim.so");
    let output = Command::new("cc")
        .args(["-shared", "-fPIC", "-O2", "-Wall", "-o"])
        .arg(&out)
        .arg(&source)
        .args(["-ldl", "-pthread"])
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

/// A port the kernel chose for the test, released at once, to hand a node
/// as a fixed `LISTEN_PORT` (a test device: no well-known port is used).
pub fn kernel_chosen_port() -> Result<u16, String> {
    let listener =
        TcpListener::bind("0.0.0.0:0").map_err(|e| format!("set-up: binding port 0: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("set-up: {e}"))?
        .port();
    drop(listener);
    Ok(port)
}

/// Where a node learns its peers.
pub enum Peers {
    /// `PEERS` in its environment, in NW.02's text (`id:127.0.0.1:port,...`),
    /// possibly empty; no `PEERS_FILE`.
    Env(String),
    /// `PEERS_FILE`, written by the test later.
    File,
}

/// What the preloaded library does in this node.
#[derive(Clone, Copy)]
pub struct Device {
    /// Hold the node at the rename of its `PORTS_FILE` until released.
    pub hold: bool,
    /// Refuse a second bind of a kernel-chosen port (the gap).
    pub gap: bool,
}

/// A node started here: NW.02's pending node, its bind log, and whether it
/// is held at its report. Killed when dropped before it is handed over as a
/// [`Node`], so a red test leaves no process behind.
pub struct Started {
    pending: Option<Pending>,
    pub bind_log: PathBuf,
    pub hold: bool,
}

impl Drop for Started {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            let _ = pending.child.kill();
            let _ = pending.child.wait();
        }
    }
}

/// Start `id` with `LISTEN_PORT=listen_port`, `HTTP_PORT=0`, `PORTS_FILE`,
/// its peers as `peers` says, the transport and `env`, and the library
/// preloaded as `device` says.
#[allow(clippy::too_many_arguments)]
pub fn start(
    binary: &Path,
    scratch: &Path,
    shim: &Path,
    id: &str,
    transport: Transport,
    env: &[(String, String)],
    listen_port: u16,
    peers: Peers,
    device: Device,
) -> Result<Started, String> {
    guard_scouting(&[])?;
    let stem = scratch.join(format!("{id}.p5"));
    let log = stem.with_extension("p5.log");
    let ports = stem.with_extension("p5.ports.json");
    let peers_file = stem.with_extension("p5.peers");
    let bind_log = stem.with_extension("p5.binds");
    for stale in [&ports, &peers_file, &bind_log] {
        let _ = fs::remove_file(stale);
    }
    let out = fs::File::create(&log).map_err(|e| e.to_string())?;
    let err_out = out.try_clone().map_err(|e| e.to_string())?;
    let mut command = Command::new(binary);
    command
        .env("REPLICA_ID", id)
        .env("LISTEN_PORT", listen_port.to_string())
        .env("HTTP_PORT", "0")
        .env("PORTS_FILE", &ports)
        .env_remove("PEERS")
        .env_remove("PEERS_FILE")
        .env_remove("BOOTNODE_URL")
        .env_remove("DASHBOARD_URL")
        .env_remove("METAMODEL_PATH")
        .env_remove("ADVERTISE_ADDR")
        .env_remove("ZENOH_SCOUTING")
        .env_remove("ZENOH_ROUTER")
        .env_remove("ZENOH_MODE")
        .env_remove("ZENOH_CONFIG")
        .env_remove("SESSION_ID")
        .env("LD_PRELOAD", shim)
        .env("NW16A_BIND_LOG", &bind_log)
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    match &peers {
        Peers::Env(text) => {
            command.env("PEERS", text);
        }
        Peers::File => {
            command.env("PEERS_FILE", &peers_file);
        }
    }
    if device.hold {
        command.env("NW16A_HOLD_AT", &ports);
    }
    if device.gap {
        command.env("NW16A_GAP", "1");
    }
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|e| format!("spawning {id}: {e}"))?;
    Ok(Started {
        pending: Some(Pending {
            id: id.to_string(),
            child,
            log,
            ports,
            peers: peers_file,
            transport,
        }),
        bind_log,
        hold: device.hold,
    })
}

fn marker(ports: &Path, suffix: &str) -> PathBuf {
    let mut name = ports.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

impl Started {
    /// The node, not yet handed over.
    pub fn pending(&self) -> &Pending {
        self.pending.as_ref().expect("handed over once")
    }

    pub fn pending_mut(&mut self) -> &mut Pending {
        self.pending.as_mut().expect("handed over once")
    }

    /// Hand the node over to a cluster, which kills it when dropped.
    pub fn into_node(mut self, reported: &Reported) -> Node {
        self.pending
            .take()
            .expect("handed over once")
            .into_node(reported)
    }

    /// The node's report, once it stands still at it: the library created
    /// `<PORTS_FILE>.held` right after the rename, and the node waits for
    /// [`Started::go`]. Checked as NW.02 checks a report.
    pub fn await_held(&mut self) -> Result<Reported, String> {
        let held = marker(&self.pending().ports, ".held");
        let deadline = Instant::now() + launch::PORTS_TIMEOUT;
        let mut ports_seen_at: Option<Instant> = None;
        loop {
            if held.exists() {
                return self.pending_mut().await_ports();
            }
            if ports_seen_at.is_none() && self.pending().ports.exists() {
                ports_seen_at = Some(Instant::now());
            }
            if let Some(seen) = ports_seen_at
                && seen.elapsed() > Duration::from_secs(5)
            {
                return Err(format!(
                    "NW16A HOLD WITNESS: {}'s PORTS_FILE appeared without the node renaming a \
                     staging file onto it (NW.02's contract), so the test cannot hold it at its \
                     report; P5a inconclusive",
                    self.pending().id
                ));
            }
            if let Ok(Some(status)) = self.pending_mut().child.try_wait() {
                return Err(format!(
                    "{} exited with {status} before reporting its ports; alarms:\n{}\nlast log \
                     lines:\n{}",
                    self.pending().id,
                    alarms(&self.pending().log),
                    tail(&self.pending().log, 25)
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} never reported its ports:\n{}",
                    self.pending().id,
                    tail(&self.pending().log, 25)
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Let a held node go on from its report.
    pub fn go(&self) -> Result<(), String> {
        fs::write(marker(&self.pending().ports, ".go"), b"")
            .map_err(|e| format!("releasing {}: {e}", self.pending().id))
    }

    /// The binds the library recorded so far.
    pub fn binds(&self) -> Binds {
        Binds::read(&self.bind_log)
    }
}

/// One successful TCP bind the library recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bind {
    pub asked: u16,
    pub got: u16,
    pub inode: u64,
}

/// The node's TCP binds, and the binds the gap refused.
#[derive(Clone, Debug, Default)]
pub struct Binds {
    pub done: Vec<Bind>,
    pub refused: Vec<u16>,
}

impl Binds {
    pub fn read(path: &Path) -> Binds {
        let mut binds = Binds::default();
        let text = fs::read_to_string(path).unwrap_or_default();
        for line in text.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            match fields.as_slice() {
                ["bind", asked, got, inode] => {
                    if let (Ok(asked), Ok(got), Ok(inode)) =
                        (asked.parse(), got.parse(), inode.parse())
                    {
                        binds.done.push(Bind { asked, got, inode });
                    }
                }
                ["refused", port] => {
                    if let Ok(port) = port.parse() {
                        binds.refused.push(port);
                    }
                }
                _ => {}
            }
        }
        binds
    }

    /// The successful binds that ended on `port`.
    pub fn of(&self, port: u16) -> Vec<Bind> {
        self.done
            .iter()
            .filter(|bind| bind.got == port)
            .cloned()
            .collect()
    }

    /// Every port a successful bind ended on.
    pub fn ports(&self) -> BTreeSet<u16> {
        self.done.iter().map(|bind| bind.got).collect()
    }
}

/// The ports of the LISTEN sockets `pid` holds, from `/proc/net/tcp` and
/// `/proc/net/tcp6` against `/proc/<pid>/fd`.
pub fn held_listen_ports(pid: u32) -> BTreeSet<u16> {
    let held = socket_inodes(pid);
    sockets("0A")
        .into_iter()
        .filter(|socket| held.contains(&socket.inode))
        .map(|socket| socket.local_port)
        .collect()
}

/// Whether `pid` holds an ESTABLISHED socket whose remote port is `port`:
/// it dialled the listener there (a node never accepts on a peer's port).
pub fn dialed(pid: u32, port: u16) -> bool {
    let held = socket_inodes(pid);
    sockets("01")
        .into_iter()
        .any(|socket| socket.remote_port == port && held.contains(&socket.inode))
}

struct Socket {
    local_port: u16,
    remote_port: u16,
    inode: u64,
}

/// Every socket in state `state` (hex, as the tables write it).
fn sockets(state: &str) -> Vec<Socket> {
    let mut found = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 10 || fields[3] != state {
                continue;
            }
            let port = |field: &str| {
                field
                    .rsplit_once(':')
                    .and_then(|(_, hex)| u16::from_str_radix(hex, 16).ok())
            };
            if let (Some(local_port), Some(remote_port), Ok(inode)) =
                (port(fields[1]), port(fields[2]), fields[9].parse::<u64>())
            {
                found.push(Socket {
                    local_port,
                    remote_port,
                    inode,
                });
            }
        }
    }
    found
}

/// A thread that reads the LISTEN sockets on one port every 2 ms and keeps
/// every inode it saw there.
pub struct PortWatch {
    stop: Arc<AtomicBool>,
    seen: Arc<Mutex<BTreeSet<u64>>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl PortWatch {
    pub fn start(port: u16) -> PortWatch {
        let stop = Arc::new(AtomicBool::new(false));
        let seen: Arc<Mutex<BTreeSet<u64>>> = Arc::default();
        let thread = {
            let stop = Arc::clone(&stop);
            let seen = Arc::clone(&seen);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let now: Vec<u64> = listening_on(port).iter().map(|l| l.inode).collect();
                    seen.lock().unwrap().extend(now);
                    thread::sleep(Duration::from_millis(2));
                }
            })
        };
        PortWatch {
            stop,
            seen,
            thread: Some(thread),
        }
    }

    /// Stop, and answer every inode seen listening on the port.
    pub fn finish(mut self) -> BTreeSet<u64> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for PortWatch {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Whether `text` names `port` as a number of its own (not inside a longer
/// run of digits).
pub fn names_port(text: &str, port: u16) -> bool {
    let wanted = port.to_string();
    text.match_indices(&wanted).any(|(at, _)| {
        let before = text[..at].chars().next_back();
        let after = text[at + wanted.len()..].chars().next();
        !before.is_some_and(|c| c.is_ascii_digit()) && !after.is_some_and(|c| c.is_ascii_digit())
    })
}
