//! NW.19, P1's checks on processes (vault `spec/items/NW.19.md`, section 1,
//! P1): `model_node` on both of its start paths, and Arachne's `network_node`,
//! each started with no transport variable in its environment, on loopback.
//!
//! - PATH-A: `model_node` started with its peers known at start. `x1` with
//!   `LISTEN_PORT=0`, `PORTS_FILE` and no peers, then `x2` with
//!   `LISTEN_PORT=0`, `PORTS_FILE` and `PEERS` naming `x1`: one Zenoh open each,
//!   nothing released. Each reports its ports, answers `/api/health`, holds a
//!   LISTEN socket on its reported port (the kernel's tables), and a raw Zenoh
//!   session this test opens at that port sees its liveliness token
//!   `moirai/<session>/node/<REPLICA_ID>`.
//! - PATH-B: `model_node` with `LISTEN_PORT=0` and `PEERS_FILE` (NW.02's
//!   launcher): `y` reports, then is given a peers file naming `x1`; the same
//!   four checks.
//! - BOOTNODE: `model_node` with `BOOTNODE_URL` set and the transport unset
//!   exits non-zero by itself within 10 s, its output names
//!   `MOIRAI_TRANSPORT=tcp`, it writes no ports file and opens no Zenoh session
//!   (no `Zenoh transport in session` line).
//! - NETWORK-NODE: `network_node` (`MOIRAI_E2E_NODE_BIN`, Arachne's generated
//!   node built against this tree) with a fixed `LISTEN_PORT` the kernel handed
//!   out and no peers: it answers `/api/health`, holds a LISTEN socket on that
//!   port, and its token is seen there.
//! - NETWORK-NODE-BOOTNODE: `network_node` with `BOOTNODE_URL` set and the
//!   transport unset exits non-zero within 10 s naming `MOIRAI_TRANSPORT=tcp`,
//!   before any Zenoh session and before its HTTP API.
//!
//! A node on TCP has no Zenoh session at its port, so no witness can pass for
//! it; a node that took `BOOTNODE_URL` keeps running, so no refusal can pass
//! for it.
//!
//! ```text
//! cargo build -j 2 -p moirai-interp --features network --example model_node
//! MOIRAI_E2E_NODE_BIN=<network_node> cargo test -j 2 -p moirai-interp --features network \
//!     --test nw19_default_node_l3 -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

const LOG_ID: &str = "19191919191919191919191919191919";
/// How long a refusal may take.
const REFUSAL: Duration = Duration::from_secs(10);
/// How long a raw session may take to see a node's token.
const WITNESS: Duration = Duration::from_secs(30);
/// Every variable that names or shapes a transport, removed from each node's
/// environment before a case adds its own.
const TRANSPORT_VARS: [&str; 11] = [
    "MOIRAI_TRANSPORT",
    "BOOTNODE_URL",
    "ZENOH_ROUTER",
    "ZENOH_MODE",
    "ZENOH_CONFIG",
    "ZENOH_SCOUTING",
    "ZENOH_REGION",
    "SESSION_ID",
    "PEERS",
    "PEERS_FILE",
    "ADVERTISE_ADDR",
];

struct Started {
    child: Child,
    log: PathBuf,
    ports: PathBuf,
}

impl Drop for Started {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("moirai-nw19-l3-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("NW19 HARNESS ERROR scratch: {e}"));
    dir
}

fn network_node_binary() -> PathBuf {
    let raw = std::env::var("MOIRAI_E2E_NODE_BIN").unwrap_or_else(|_| {
        panic!(
            "NW19 HARNESS ERROR MOIRAI_E2E_NODE_BIN is unset; point it at Arachne's network_node \
             built against this tree"
        )
    });
    let path = PathBuf::from(&raw);
    assert!(
        path.is_file(),
        "NW19 HARNESS ERROR MOIRAI_E2E_NODE_BIN points at `{raw}`, which is not a file"
    );
    path
}

/// Starts `binary` as `id` in `dir` with every transport variable removed and
/// `env` added; output to `<id>.log`.
fn start_binary(binary: &Path, id: &str, dir: &Path, env: &[(&str, &str)]) -> Started {
    let log = dir.join(format!("{id}.log"));
    let ports = dir.join(format!("{id}.ports.json"));
    let out = fs::File::create(&log).unwrap_or_else(|e| panic!("NW19 HARNESS ERROR log: {e}"));
    let err = out
        .try_clone()
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR log: {e}"));
    let mut command = Command::new(binary);
    for var in TRANSPORT_VARS {
        command.env_remove(var);
    }
    let child = command
        .current_dir(dir)
        .env("REPLICA_ID", id)
        .env("HTTP_BIND_ADDR", "127.0.0.1")
        .env("LOG_ID", LOG_ID)
        .env_remove("METAMODEL_PATH")
        .env_remove("METAMODEL_DIR")
        .env_remove("DASHBOARD_URL")
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR spawn {}: {e}", binary.display()));
    Started { child, log, ports }
}

/// A `model_node` with `LISTEN_PORT=0`, `HTTP_PORT=0` and a ports file.
fn start_model_node(id: &str, dir: &Path, env: &[(&str, &str)]) -> Started {
    let binary = nw16a::node_binary().unwrap_or_else(|why| panic!("NW19 HARNESS ERROR {why}"));
    let ports = dir.join(format!("{id}.ports.json"));
    let ports = ports.to_string_lossy().into_owned();
    let mut all = vec![
        ("LISTEN_PORT", "0"),
        ("HTTP_PORT", "0"),
        ("PORTS_FILE", ports.as_str()),
    ];
    all.extend_from_slice(env);
    start_binary(&binary, id, dir, &all)
}

fn log_text(p: &Started) -> String {
    fs::read_to_string(&p.log).unwrap_or_default()
}

/// The node's reported ports, checked as its own; `Err` if it exits first or
/// never reports.
fn await_ports(p: &mut Started, id: &str) -> Result<(u16, u16), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(parsed) = nw16a::read_ports_file(&p.ports) {
            let r = parsed?;
            let http = r.http_port.unwrap_or(0);
            if r.replica_id != id || r.pid != p.child.id() || r.listen_port == 0 || http == 0 {
                return Err(format!("the ports file is not this node's: {r:?}"));
            }
            return Ok((r.listen_port, http));
        }
        if let Ok(Some(status)) = p.child.try_wait() {
            return Err(format!(
                "exited with {status} before reporting: {}",
                log_text(p)
            ));
        }
        if Instant::now() > deadline {
            return Err(format!("no ports file within 30 s: {}", log_text(p)));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn health(http: u16) -> Result<Value, String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|e| e.to_string())?;
    loop {
        if let Ok(resp) = client
            .get(format!("http://127.0.0.1:{http}/api/health"))
            .send()
            && resp.status().as_u16() == 200
        {
            return resp.json::<Value>().map_err(|e| e.to_string());
        }
        if Instant::now() > deadline {
            return Err("no /api/health 200 within 30 s".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// Whether `pid` holds a LISTEN socket on `port`, in the kernel's tables.
fn listens(pid: u32, port: u16) -> bool {
    let held = nw16a::socket_inodes(pid);
    nw16a::listening_on(port)
        .iter()
        .any(|l| held.contains(&l.inode))
}

/// Health, the LISTEN socket on `listen`, and the token at `listen`.
fn on_zenoh(case: &str, p: &Started, id: &str, listen: u16, http: u16) {
    let h = health(http).unwrap_or_else(|why| panic!("NW19 {case}: {why}; {}", log_text(p)));
    let pid = p.child.id();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !listens(pid, listen) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        listens(pid, listen),
        "NW19 {case}: pid {pid} holds no LISTEN socket on the port it holds, {listen}"
    );
    let token = nw16a::token_at(listen, id, WITNESS).unwrap_or_else(|why| {
        panic!(
            "NW19 {case} WITNESS: with MOIRAI_TRANSPORT unset the node runs no Zenoh session at \
             its port: {why}; health {h}; log:\n{}",
            log_text(p)
        )
    });
    println!("NW19 {case} PASS {id} port {listen} token {token}");
}

/// Waits for `p` to exit by itself within [`REFUSAL`]; then its status and
/// output.
fn refused(p: &mut Started) -> (Option<std::process::ExitStatus>, String) {
    let deadline = Instant::now() + REFUSAL;
    let status = loop {
        if let Ok(Some(status)) = p.child.try_wait() {
            break Some(status);
        }
        if Instant::now() > deadline {
            break None;
        }
        thread::sleep(Duration::from_millis(50));
    };
    (status, log_text(p))
}

/// A loopback port the kernel hands out, released for a node that takes a
/// fixed `LISTEN_PORT` (`network_node` writes no ports file).
fn kernel_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR free port: {e}"))
}

#[test]
fn nw19_l3_model_node_with_its_peers_known_at_start_runs_on_zenoh() {
    let dir = scratch("path-a");
    let mut x1 = start_model_node("nw19-x1", &dir, &[]);
    let (l1, h1) =
        await_ports(&mut x1, "nw19-x1").unwrap_or_else(|why| panic!("NW19 PATH-A x1: {why}"));
    let peers = format!("nw19-x1:127.0.0.1:{l1}");
    let mut x2 = start_model_node("nw19-x2", &dir, &[("PEERS", peers.as_str())]);
    let (l2, h2) =
        await_ports(&mut x2, "nw19-x2").unwrap_or_else(|why| panic!("NW19 PATH-A x2: {why}"));
    on_zenoh("PATH-A", &x1, "nw19-x1", l1, h1);
    on_zenoh("PATH-A", &x2, "nw19-x2", l2, h2);
}

#[test]
fn nw19_l3_model_node_with_a_peers_file_runs_on_zenoh() {
    let dir = scratch("path-b");
    let mut x1 = start_model_node("nw19-x1", &dir, &[]);
    let (l1, _) =
        await_ports(&mut x1, "nw19-x1").unwrap_or_else(|why| panic!("NW19 PATH-B x1: {why}"));
    let peers_file = dir.join("nw19-y.peers");
    let peers_path = peers_file.to_string_lossy().into_owned();
    let mut y = start_model_node("nw19-y", &dir, &[("PEERS_FILE", peers_path.as_str())]);
    let (ly, hy) =
        await_ports(&mut y, "nw19-y").unwrap_or_else(|why| panic!("NW19 PATH-B y: {why}"));
    nw16a::write_atomically(&peers_file, &format!("nw19-x1:127.0.0.1:{l1}"))
        .unwrap_or_else(|e| panic!("NW19 HARNESS ERROR peers file: {e}"));
    on_zenoh("PATH-B", &y, "nw19-y", ly, hy);
}

#[test]
fn nw19_l3_model_node_with_a_bootnode_and_the_transport_unset_is_refused_naming_tcp() {
    let dir = scratch("bootnode");
    let mut p = start_model_node(
        "nw19-boot",
        &dir,
        &[("BOOTNODE_URL", "http://127.0.0.1:9/")],
    );
    let (status, text) = refused(&mut p);
    let ports = p.ports.exists();
    let Some(status) = status else {
        panic!(
            "NW19 BOOTNODE: with BOOTNODE_URL set and MOIRAI_TRANSPORT unset model_node still runs \
             after {REFUSAL:?} (ports file {ports}); log:\n{text}"
        )
    };
    assert!(!status.success(), "NW19 BOOTNODE: exited 0; log:\n{text}");
    assert!(
        text.contains("MOIRAI_TRANSPORT=tcp"),
        "NW19 BOOTNODE: the refusal names no MOIRAI_TRANSPORT=tcp; log:\n{text}"
    );
    assert!(
        !text.contains("Zenoh transport in session"),
        "NW19 BOOTNODE: a Zenoh session opened before the refusal; log:\n{text}"
    );
    assert!(
        !ports,
        "NW19 BOOTNODE: a ports file was written before the refusal"
    );
    println!("NW19 BOOTNODE PASS exit {status}");
}

#[test]
fn nw19_l3_network_node_with_the_transport_unset_runs_on_zenoh() {
    let dir = scratch("network-node");
    let binary = network_node_binary();
    let (listen, http) = (kernel_port(), kernel_port());
    let (listen_s, http_s) = (listen.to_string(), http.to_string());
    let p = start_binary(
        &binary,
        "nw19-nn",
        &dir,
        &[
            ("LISTEN_PORT", listen_s.as_str()),
            ("HTTP_PORT", http_s.as_str()),
            ("PEERS", ""),
        ],
    );
    on_zenoh("NETWORK-NODE", &p, "nw19-nn", listen, http);
}

#[test]
fn nw19_l3_network_node_with_a_bootnode_and_the_transport_unset_is_refused_naming_tcp() {
    let dir = scratch("network-node-bootnode");
    let binary = network_node_binary();
    let (listen, http) = (kernel_port(), kernel_port());
    let (listen_s, http_s) = (listen.to_string(), http.to_string());
    let mut p = start_binary(
        &binary,
        "nw19-nnboot",
        &dir,
        &[
            ("LISTEN_PORT", listen_s.as_str()),
            ("HTTP_PORT", http_s.as_str()),
            ("PEERS", ""),
            ("BOOTNODE_URL", "http://127.0.0.1:9/"),
        ],
    );
    let (status, text) = refused(&mut p);
    let Some(status) = status else {
        panic!(
            "NW19 NETWORK-NODE-BOOTNODE: with BOOTNODE_URL set and MOIRAI_TRANSPORT unset \
             network_node still runs after {REFUSAL:?}; log:\n{text}"
        )
    };
    assert!(
        !status.success(),
        "NW19 NETWORK-NODE-BOOTNODE: exited 0; log:\n{text}"
    );
    assert!(
        text.contains("MOIRAI_TRANSPORT=tcp"),
        "NW19 NETWORK-NODE-BOOTNODE: the refusal names no MOIRAI_TRANSPORT=tcp; log:\n{text}"
    );
    assert!(
        !text.contains("Zenoh transport in session") && !text.contains("HTTP API listening"),
        "NW19 NETWORK-NODE-BOOTNODE: a Zenoh session or the HTTP API came up before the \
         refusal; log:\n{text}"
    );
    println!("NW19 NETWORK-NODE-BOOTNODE PASS exit {status}");
}
