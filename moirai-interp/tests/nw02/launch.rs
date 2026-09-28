//! NW.02: the one way the loopback harnesses of this crate start `model_node`
//! processes, so that no port is chosen for a node by anyone but the kernel,
//! in the bind that keeps it.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.02.md`. Before NW.02 every
//! harness asked the kernel for a free port (bind port 0, read the number,
//! release it) and handed the number to the node, which bound it later.
//! Between the release and the node's bind any process could take the port,
//! and two picks of one cluster could be the same port, since each pick is
//! released before the next is made: a node then died on `AddrInUse`, or its
//! HTTP thread did and the harness talked to another node on that port.
//!
//! Here each node is started with `LISTEN_PORT=0` and `HTTP_PORT=0`, binds
//! whatever the kernel gives it, and writes what it bound to the file
//! `PORTS_FILE` names. The harness reads the ports there and only then tells
//! each node its peers, in the file `PEERS_FILE` names, in exactly the text
//! the harnesses used to pass as `PEERS`, in the same order, so every node
//! holds the members it held before.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

/// How long a node may take, once spawned, to report the ports it bound.
pub const PORTS_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);

/// One node to start: its replica id and the environment it needs besides
/// its ports and its peers (`LOG_ID`, `METAMODEL_DIR`, ...).
pub struct NodeSpec {
    pub id: String,
    pub env: Vec<(String, String)>,
}

impl NodeSpec {
    pub fn new(id: &str, env: &[(&str, &str)]) -> Self {
        NodeSpec {
            id: id.to_string(),
            env: env
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }
}

/// A started node: its process, its log, and the ports it reported.
pub struct Launched {
    pub id: String,
    pub child: Child,
    pub log: PathBuf,
    pub listen: u16,
    pub http: u16,
}

/// What a node wrote to its ports file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    pub replica_id: String,
    pub pid: u32,
    pub listen_port: u16,
    pub http_port: Option<u16>,
}

/// The ports file at `path`: `None` while it is absent or does not yet parse
/// as the object the contract names, which a reader may meet only if the node
/// wrote it in place; `Some` once it does.
pub fn read_ports_file(path: &Path) -> Option<Result<Reported, String>> {
    let text = fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let port = |key: &str| -> Result<Option<u16>, String> {
        match &value[key] {
            Value::Null => Ok(None),
            Value::Number(n) => n
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .map(Some)
                .ok_or_else(|| format!("`{key}` is not a port in {text}")),
            other => Err(format!("`{key}` is {other}, not a port, in {text}")),
        }
    };
    let parsed = (|| {
        let replica_id = value["replica_id"]
            .as_str()
            .ok_or_else(|| format!("no `replica_id` string in {text}"))?
            .to_string();
        let pid = value["pid"]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| format!("no `pid` number in {text}"))?;
        let listen_port =
            port("listen_port")?.ok_or_else(|| format!("no `listen_port` in {text}"))?;
        let http_port = port("http_port")?;
        Ok(Reported {
            replica_id,
            pid,
            listen_port,
            http_port,
        })
    })();
    Some(parsed)
}

/// `text` at `path`, appearing whole: written beside it and renamed.
pub fn write_atomically(path: &Path, text: &str) -> Result<(), String> {
    let staging = path.with_extension("staging");
    fs::write(&staging, text).map_err(|err| format!("writing {}: {err}", staging.display()))?;
    fs::rename(&staging, path).map_err(|err| format!("renaming onto {}: {err}", path.display()))
}

/// The last `lines` lines of a log file, newest last.
pub fn tail(log: &Path, lines: usize) -> String {
    let text = fs::read_to_string(log).unwrap_or_default();
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// The lines of a node's log that say it panicked or could not bind,
/// wherever they are. A failure at start-up is printed among the first lines
/// and is soon pushed out of any tail by what the node keeps printing, which
/// is how NW.01's cause went unrecorded; a harness quotes these beside the
/// tail.
pub fn alarms(log: &Path) -> String {
    let text = fs::read_to_string(log).unwrap_or_default();
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| {
            line.contains("panicked")
                || line.contains("AddrInUse")
                || line.contains("Address already in use")
                || line.starts_with("Failed to")
        })
        .take(10)
        .collect();
    if lines.is_empty() {
        "none".to_string()
    } else {
        lines.join("\n")
    }
}

/// A node spawned and not yet handed over: killed if the launch fails.
struct Pending {
    id: String,
    child: Option<Child>,
    log: PathBuf,
    ports: PathBuf,
    peers: PathBuf,
}

struct Guard(Vec<Pending>);

impl Drop for Guard {
    fn drop(&mut self) {
        for pending in &mut self.0 {
            if let Some(child) = pending.child.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// Start `nodes` as one session meshed by static peers: every node gets every
/// other node as a peer, in the order of `nodes`, which is the `PEERS` text
/// the harnesses built before NW.02. Answers once every node has reported
/// ports that are its own and every peers file is written; the caller then
/// waits for health and the mesh as it always did.
pub fn launch(binary: &Path, scratch: &Path, nodes: &[NodeSpec]) -> Result<Vec<Launched>, String> {
    fs::create_dir_all(scratch).map_err(|err| format!("{}: {err}", scratch.display()))?;
    let mut guard = Guard(Vec::new());
    for spec in nodes {
        let log = scratch.join(format!("{}.log", spec.id));
        let ports = scratch.join(format!("{}.ports.json", spec.id));
        let peers = scratch.join(format!("{}.peers", spec.id));
        let _ = fs::remove_file(&ports);
        let _ = fs::remove_file(&peers);
        let out = fs::File::create(&log).map_err(|err| err.to_string())?;
        let err_out = out.try_clone().map_err(|err| err.to_string())?;
        let child = Command::new(binary)
            .env("REPLICA_ID", &spec.id)
            .env("LISTEN_PORT", "0")
            .env("HTTP_PORT", "0")
            .env("PORTS_FILE", &ports)
            .env("PEERS_FILE", &peers)
            .env_remove("PEERS")
            .env_remove("BOOTNODE_URL")
            .env_remove("DASHBOARD_URL")
            .env_remove("METAMODEL_PATH")
            .env_remove("ADVERTISE_ADDR")
            .envs(
                spec.env
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err_out))
            .spawn()
            .map_err(|err| format!("spawning {}: {err}", spec.id))?;
        guard.0.push(Pending {
            id: spec.id.clone(),
            child: Some(child),
            log,
            ports,
            peers,
        });
    }

    let reported = await_ports(&mut guard.0)?;

    for (index, pending) in guard.0.iter().enumerate() {
        let text = nodes
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(other, spec)| format!("{}:127.0.0.1:{}", spec.id, reported[other].listen_port))
            .collect::<Vec<_>>()
            .join(",");
        write_atomically(&pending.peers, &text)?;
    }

    Ok(guard
        .0
        .iter_mut()
        .zip(reported)
        .map(|(pending, ports)| Launched {
            id: pending.id.clone(),
            child: pending.child.take().expect("each node is handed over once"),
            log: pending.log.clone(),
            listen: ports.listen_port,
            http: ports.http_port.expect("checked in await_ports"),
        })
        .collect())
}

/// Wait until every pending node has written a ports file that is its own:
/// its replica id, the pid of the process this harness spawned, two ports
/// that are not 0 and not the same. A node that exits first, or a file that
/// says anything else, fails the launch at once, with the node's log.
fn await_ports(pending: &mut [Pending]) -> Result<Vec<Reported>, String> {
    let deadline = Instant::now() + PORTS_TIMEOUT;
    let mut reported: Vec<Option<Reported>> = vec![None; pending.len()];
    loop {
        for (slot, node) in reported.iter_mut().zip(pending.iter_mut()) {
            if slot.is_some() {
                continue;
            }
            let child = node.child.as_mut().expect("not handed over yet");
            if let Some(ports) = read_ports_file(&node.ports) {
                let ports = ports.map_err(|why| format!("{}'s ports file: {why}", node.id))?;
                let pid = child.id();
                if ports.replica_id != node.id || ports.pid != pid {
                    return Err(format!(
                        "{}'s ports file names replica `{}` and pid {}, not `{}` and pid {pid}",
                        node.id, ports.replica_id, ports.pid, node.id
                    ));
                }
                let http = ports.http_port.unwrap_or(0);
                if ports.listen_port == 0 || http == 0 || ports.listen_port == http {
                    return Err(format!(
                        "{} reported listen port {} and HTTP port {:?}: a port it bound is \
                         never 0, and the two are two sockets",
                        node.id, ports.listen_port, ports.http_port
                    ));
                }
                *slot = Some(ports);
                continue;
            }
            if let Ok(Some(status)) = child.try_wait() {
                return Err(format!(
                    "{} exited with {status} before reporting its ports in {}; alarms:\n{}\nlast \
                     log lines:\n{}",
                    node.id,
                    node.ports.display(),
                    alarms(&node.log),
                    tail(&node.log, 25)
                ));
            }
        }
        if reported.iter().all(Option::is_some) {
            return Ok(reported.into_iter().map(Option::unwrap).collect());
        }
        if Instant::now() > deadline {
            let missing: Vec<String> = reported
                .iter()
                .zip(pending.iter())
                .filter(|(slot, _)| slot.is_none())
                .map(|(_, node)| {
                    format!(
                        "{} never reported its ports in {} within {} s (still running); last \
                         log lines:\n{}",
                        node.id,
                        node.ports.display(),
                        PORTS_TIMEOUT.as_secs(),
                        tail(&node.log, 25)
                    )
                })
                .collect();
            return Err(missing.join("\n"));
        }
        std::thread::sleep(POLL);
    }
}
