//! NW.02: loopback `model_node` processes started by a test never lose their
//! ports to another process, because nobody but the kernel chooses them, in
//! the bind that keeps them.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.02.md`. Four tests:
//!
//! - `n1`, the node's half of the contract, on one process: started with
//!   `LISTEN_PORT=0` and `HTTP_PORT=0`, it writes to `PORTS_FILE` its replica
//!   id, its pid and the two ports it bound, and the kernel's own table
//!   (`/proc/net/tcp` against `/proc/<pid>/fd`) says that process listens on
//!   exactly those; `/api/health` on the HTTP one answers as that replica.
//! - `n2`, the peers: two nodes told their peers through `PEERS_FILE`, written
//!   from the ports each reported, form a mesh, and a `PEERS` left in their
//!   environment is ignored.
//! - `n3`, the claim under an adversary: a neighbour holds every odd port of
//!   the local range but two and keeps taking those two (`nw02/squeeze.rs`),
//!   and four clusters of three nodes started by the harness every L3 suite
//!   of this crate uses (`nw02/launch.rs`) all come up, each node answering
//!   as itself, with no address-in-use line in any node log.
//! - `base`, ignored by default, the paired baseline of the control: the same
//!   four clusters under the same neighbour, with ports chosen the way the
//!   harnesses chose them before NW.02 (bind port 0, read, release, pass the
//!   number). It must fail at least one cluster, or the neighbour proves
//!   nothing. Run with `--ignored`.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw02_loopback_ports \
//!     -- --test-threads=1 --nocapture
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw02_loopback_ports \
//!     -- --test-threads=1 --nocapture --ignored
//! ```
//!
//! No binary: the tests print `E2E-SKIP <scenario>: <why>` and return. A
//! skip is not a pass.
#![cfg(feature = "network")]

#[path = "nw02/launch.rs"]
mod launch;
#[path = "nw02/squeeze.rs"]
mod squeeze;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use launch::{Launched, NodeSpec, alarms, read_ports_file, tail};
use squeeze::{Squeeze, pick_and_release};

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(100);
/// Clusters started under the neighbour, and their size.
const CLUSTERS: usize = 4;
const CLUSTER_IDS: [&str; 3] = ["n1", "n2", "n3"];
/// Ports the neighbour leaves free.
const LEFT_FREE: usize = 2;
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

fn node_binary() -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var("MOIRAI_E2E_INTERP_NODE_BIN") {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "MOIRAI_E2E_INTERP_NODE_BIN points at `{raw}`, which is not a file"
            ))
        };
    }
    let built =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/examples/model_node");
    if built.is_file() {
        Ok(built)
    } else {
        Err(format!(
            "no `model_node` at {}; build it with `cargo build -p moirai-interp --features \
             network --example model_node`",
            built.display()
        ))
    }
}

fn scratch(tag: &str) -> PathBuf {
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("moirai-nw02-{tag}-{}-{run}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("a scratch directory");
    dir
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("an HTTP client")
}

fn get(client: &reqwest::blocking::Client, port: u16, path: &str) -> Result<(u16, Value), String> {
    let response = client
        .get(format!("http://127.0.0.1:{port}{path}"))
        .send()
        .map_err(|err| format!("GET {path} on port {port}: {err}"))?;
    let status = response.status().as_u16();
    let body = response.text().map_err(|err| err.to_string())?;
    Ok((
        status,
        serde_json::from_str(&body).unwrap_or(Value::String(body)),
    ))
}

/// The TCP ports process `pid` is listening on, read from the kernel: the
/// socket inodes among its open files, looked up in `/proc/net/tcp{,6}`.
fn listening_ports_of(pid: u32) -> Result<BTreeSet<u16>, String> {
    let mut inodes = BTreeSet::new();
    let fds =
        fs::read_dir(format!("/proc/{pid}/fd")).map_err(|err| format!("/proc/{pid}/fd: {err}"))?;
    for entry in fds.flatten() {
        if let Ok(link) = fs::read_link(entry.path())
            && let Some(inode) = link
                .to_str()
                .and_then(|text| text.strip_prefix("socket:["))
                .and_then(|rest| rest.strip_suffix(']'))
        {
            inodes.insert(inode.to_string());
        }
    }
    let mut ports = BTreeSet::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // local_address is fields[1] (`ADDR:PORT`, hex), st is fields[3]
            // (`0A` is LISTEN), inode is fields[9].
            if fields.len() > 9 && fields[3] == "0A" && inodes.contains(fields[9]) {
                let port = fields[1].rsplit(':').next().unwrap_or_default();
                ports.insert(
                    u16::from_str_radix(port, 16).map_err(|err| format!("{table}: {err}"))?,
                );
            }
        }
    }
    Ok(ports)
}

/// Lines of a log that say a bind found its address taken.
fn address_in_use_lines(log: &Path) -> usize {
    fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter(|line| {
            line.contains("AddrInUse")
                || line.contains("Address already in use")
                || line.contains("os error 98")
        })
        .count()
}

/// Started nodes, killed and reaped when dropped.
struct Nodes(Vec<Launched>);

impl Drop for Nodes {
    fn drop(&mut self) {
        for node in &mut self.0 {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

impl Nodes {
    /// Every node answers `/api/health` on its HTTP port as itself, then
    /// every node sees every other connected: the readiness the L3 suites
    /// wait for. A port that answers as another replica fails at once.
    fn ready(&mut self, health: Duration, mesh: Duration) -> Result<(), String> {
        let client = client();
        for node in &mut self.0 {
            let deadline = Instant::now() + health;
            loop {
                if let Ok((200, body)) = get(&client, node.http, "/api/health") {
                    if body["replica_id"] == json!(node.id) {
                        break;
                    }
                    return Err(format!(
                        "{}'s HTTP port {} answered as {body}",
                        node.id, node.http
                    ));
                }
                if let Ok(Some(status)) = node.child.try_wait() {
                    return Err(format!(
                        "{} exited with {status}; start-up alarms:\n{}",
                        node.id,
                        alarms(&node.log)
                    ));
                }
                if Instant::now() > deadline {
                    return Err(format!(
                        "{} never answered /api/health on port {}; start-up alarms:\n{}",
                        node.id,
                        node.http,
                        alarms(&node.log)
                    ));
                }
                std::thread::sleep(POLL);
            }
        }
        let deadline = Instant::now() + mesh;
        loop {
            let mut missing = Vec::new();
            for node in &self.0 {
                let connected: BTreeSet<String> = match get(&client, node.http, "/api/peers") {
                    Ok((200, peers)) => peers["peers"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|peer| peer["status"] == json!("Connected"))
                        .filter_map(|peer| peer["id"].as_str().map(str::to_string))
                        .collect(),
                    _ => BTreeSet::new(),
                };
                for other in &self.0 {
                    if other.id != node.id && !connected.contains(&other.id) {
                        missing.push(format!("{} does not see {}", node.id, other.id));
                    }
                }
            }
            if missing.is_empty() {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("the mesh never formed: {}", missing.join("; ")));
            }
            std::thread::sleep(POLL);
        }
    }

    fn address_in_use_lines(&self) -> usize {
        self.0
            .iter()
            .map(|node| address_in_use_lines(&node.log))
            .sum()
    }
}

/// The way every harness of this crate started a cluster before NW.02, kept
/// here for the paired baseline only: two ports per node picked up front by
/// binding port 0, reading the number and releasing it, then handed to the
/// node as `LISTEN_PORT`, `HTTP_PORT` and its peers' `PEERS`.
fn launch_prepicked(binary: &Path, scratch: &Path, ids: &[&str]) -> Result<Vec<Launched>, String> {
    let ports: Vec<(u16, u16)> = ids
        .iter()
        .map(|_| (pick_and_release(), pick_and_release()))
        .collect();
    let mut nodes = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let peers: Vec<String> = ids
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(other, peer)| format!("{peer}:127.0.0.1:{}", ports[other].0))
            .collect();
        let log = scratch.join(format!("{id}.log"));
        let out = fs::File::create(&log).map_err(|err| err.to_string())?;
        let err_out = out.try_clone().map_err(|err| err.to_string())?;
        let child = Command::new(binary)
            .env("REPLICA_ID", id)
            .env("LISTEN_PORT", ports[index].0.to_string())
            .env("HTTP_PORT", ports[index].1.to_string())
            .env("PEERS", peers.join(","))
            .env("LOG_ID", "0e020e020e020e020e020e020e020e02")
            .env_remove("PORTS_FILE")
            .env_remove("PEERS_FILE")
            .env_remove("BOOTNODE_URL")
            .env_remove("DASHBOARD_URL")
            .env_remove("METAMODEL_PATH")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err_out))
            .spawn()
            .map_err(|err| format!("spawning {id}: {err}"))?;
        nodes.push(Launched {
            id: id.to_string(),
            child,
            log,
            listen: ports[index].0,
            http: ports[index].1,
        });
    }
    Ok(nodes)
}

fn cluster_specs() -> Vec<NodeSpec> {
    CLUSTER_IDS
        .iter()
        .map(|id| NodeSpec::new(id, &[("LOG_ID", "0e020e020e020e020e020e020e020e02")]))
        .collect()
}

/// What `CLUSTERS` cluster starts under the neighbour came to.
struct Tally {
    started: usize,
    failed: Vec<String>,
    address_in_use: usize,
}

/// Start `CLUSTERS` clusters, each under a neighbour of its own: a fresh
/// squeeze per cluster, because the ports one cluster's nodes held keep
/// TIME_WAIT sockets for a minute after they die, and a bind to port 0 skips
/// such ports, so a neighbour kept across clusters would leave the later
/// ones a pool the earlier ones had emptied.
fn under_the_neighbour(
    tag: &str,
    health: Duration,
    mesh: Duration,
    start: impl Fn(&Path) -> Result<Vec<Launched>, String>,
) -> (Tally, String) {
    let mut tally = Tally {
        started: 0,
        failed: Vec::new(),
        address_in_use: 0,
    };
    let mut neighbours = Vec::new();
    for round in 0..CLUSTERS {
        let neighbour = Squeeze::new(LEFT_FREE, true).unwrap_or_else(|why| panic!("{tag}: {why}"));
        let dir = scratch(&format!("{tag}-{round}"));
        let outcome = match start(&dir) {
            Ok(launched) => {
                let mut nodes = Nodes(launched);
                let ready = nodes.ready(health, mesh);
                tally.address_in_use += nodes.address_in_use_lines();
                ready
            }
            Err(why) => {
                tally.address_in_use += CLUSTER_IDS
                    .iter()
                    .map(|id| address_in_use_lines(&dir.join(format!("{id}.log"))))
                    .sum::<usize>();
                Err(why)
            }
        };
        match outcome {
            Ok(()) => tally.started += 1,
            Err(why) => tally.failed.push(format!("cluster {round}: {why}")),
        }
        neighbours.push(format!("cluster {round}: {}", neighbour.describe()));
        drop(neighbour);
    }
    (tally, neighbours.join("; "))
}

// ---------------------------------------------------------------------------
// n1: the node's half of the contract
// ---------------------------------------------------------------------------

#[test]
fn nw02_n1_a_node_started_on_port_zero_reports_the_ports_it_holds() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return eprintln!("\nE2E-SKIP nw02-n1: {why}"),
    };
    let dir = scratch("n1");
    // With an HTTP API, and without one.
    for (id, http) in [("solo", true), ("quiet", false)] {
        let ports_file = dir.join(format!("{id}.ports.json"));
        let log = dir.join(format!("{id}.log"));
        let out = fs::File::create(&log).expect("a log file");
        let mut command = Command::new(&binary);
        command
            .env("REPLICA_ID", id)
            .env("LISTEN_PORT", "0")
            .env("PORTS_FILE", &ports_file)
            .env("LOG_ID", "0e020e020e020e020e020e020e020e02")
            .env_remove("PEERS")
            .env_remove("PEERS_FILE")
            .env_remove("HTTP_PORT")
            .env_remove("BOOTNODE_URL")
            .env_remove("DASHBOARD_URL")
            .env_remove("METAMODEL_PATH")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out.try_clone().expect("a log handle")))
            .stderr(Stdio::from(out));
        if http {
            command.env("HTTP_PORT", "0");
        }
        let child = command.spawn().expect("spawning model_node");
        let pid = child.id();
        let mut nodes = Nodes(vec![Launched {
            id: id.to_string(),
            child,
            log: log.clone(),
            listen: 0,
            http: 0,
        }]);

        let deadline = Instant::now() + launch::PORTS_TIMEOUT;
        let reported = loop {
            if let Some(reported) = read_ports_file(&ports_file) {
                break reported.unwrap_or_else(|why| panic!("nw02-n1 {id}: {why}"));
            }
            if let Ok(Some(status)) = nodes.0[0].child.try_wait() {
                panic!(
                    "nw02-n1 {id}: model_node exited with {status} and wrote no ports file; \
                     last log lines:\n{}",
                    tail(&log, 15)
                );
            }
            assert!(
                Instant::now() < deadline,
                "nw02-n1 {id}: no ports file at {} within {} s; last log lines:\n{}",
                ports_file.display(),
                launch::PORTS_TIMEOUT.as_secs(),
                tail(&log, 15)
            );
            std::thread::sleep(Duration::from_millis(50));
        };

        assert_eq!(
            reported.replica_id, id,
            "nw02-n1: the ports file names another replica"
        );
        assert_eq!(
            reported.pid, pid,
            "nw02-n1 {id}: the ports file names another process"
        );
        assert_ne!(
            reported.listen_port, 0,
            "nw02-n1 {id}: a bound listen port is never 0"
        );
        let listening = listening_ports_of(pid).unwrap_or_else(|why| panic!("nw02-n1 {id}: {why}"));
        let mut expected = BTreeSet::from([reported.listen_port]);
        if http {
            let http_port = reported
                .http_port
                .unwrap_or_else(|| panic!("nw02-n1 {id}: HTTP_PORT=0 and no http_port reported"));
            assert_ne!(http_port, 0, "nw02-n1 {id}: a bound HTTP port is never 0");
            assert_ne!(
                http_port, reported.listen_port,
                "nw02-n1 {id}: one port for two sockets"
            );
            expected.insert(http_port);
            let (status, health) = get(&client(), http_port, "/api/health")
                .unwrap_or_else(|why| panic!("nw02-n1 {id}: {why}"));
            assert_eq!(
                (status, health["replica_id"].clone()),
                (200, json!(id)),
                "nw02-n1 {id}: /api/health on the reported port answered {status} {health}"
            );
        } else {
            assert_eq!(
                reported.http_port, None,
                "nw02-n1 {id}: no HTTP_PORT, and an http_port reported"
            );
        }
        assert_eq!(
            listening, expected,
            "nw02-n1 {id}: the kernel says pid {pid} listens on {listening:?}, the ports file \
             says {expected:?}"
        );
        println!(
            "NW02-N1 {id}: reported {reported:?}; pid {pid} listens on {listening:?}; matches: true"
        );
        drop(nodes);
    }
}

// ---------------------------------------------------------------------------
// n2: the peers, from the ports reported
// ---------------------------------------------------------------------------

#[test]
fn nw02_n2_peers_named_in_the_peers_file_from_the_reported_ports_form_a_mesh() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return eprintln!("\nE2E-SKIP nw02-n2: {why}"),
    };
    let dir = scratch("n2");
    // A `PEERS` in the environment names a member nobody runs; with
    // `PEERS_FILE` set it must not count.
    let specs: Vec<NodeSpec> = ["alpha", "beta"]
        .iter()
        .map(|id| {
            NodeSpec::new(
                id,
                &[
                    ("LOG_ID", "0e020e020e020e020e020e020e020e02"),
                    ("PEERS", "zed:127.0.0.1:9"),
                ],
            )
        })
        .collect();
    let launched =
        launch::launch(&binary, &dir, &specs).unwrap_or_else(|why| panic!("nw02-n2: {why}"));
    let mut nodes = Nodes(launched);
    nodes
        .ready(HEALTH_TIMEOUT, MESH_TIMEOUT)
        .unwrap_or_else(|why| panic!("nw02-n2: {why}"));
    let client = client();
    for node in &nodes.0 {
        let (_, peers) =
            get(&client, node.http, "/api/peers").unwrap_or_else(|why| panic!("nw02-n2: {why}"));
        let ids: BTreeSet<&str> = peers["peers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|peer| peer["id"].as_str())
            .collect();
        assert!(
            !ids.contains("zed"),
            "nw02-n2: {} took `PEERS` from its environment although `PEERS_FILE` was set: {peers}",
            node.id
        );
        println!("NW02-N2 {}: peers {peers}", node.id);
    }
    assert_eq!(
        nodes.address_in_use_lines(),
        0,
        "nw02-n2: an address was in use"
    );
}

// ---------------------------------------------------------------------------
// n3: the claim, under the neighbour
// ---------------------------------------------------------------------------

#[test]
fn nw02_n3_clusters_come_up_on_their_own_ports_while_a_neighbour_holds_the_rest() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return eprintln!("\nE2E-SKIP nw02-n3: {why}"),
    };
    let (tally, neighbour) = under_the_neighbour("n3", HEALTH_TIMEOUT, MESH_TIMEOUT, |dir| {
        launch::launch(&binary, dir, &cluster_specs())
    });
    println!(
        "NW02-N3 ports the nodes bound: clusters {CLUSTERS} of {} nodes, started {}, failed {}, \
         address-in-use lines {}; neighbour: {neighbour}",
        CLUSTER_IDS.len(),
        tally.started,
        tally.failed.len(),
        tally.address_in_use
    );
    for why in &tally.failed {
        println!("NW02-N3 {why}");
    }
    assert!(
        tally.failed.is_empty() && tally.address_in_use == 0,
        "nw02-n3: {} of {CLUSTERS} clusters failed and {} address-in-use lines were logged; \
         first failure: {}",
        tally.failed.len(),
        tally.address_in_use,
        tally.failed.first().map(String::as_str).unwrap_or("none")
    );
}

// ---------------------------------------------------------------------------
// base: the paired baseline of the control
// ---------------------------------------------------------------------------

#[test]
#[ignore = "NW.02 control, the paired baseline: run with --ignored --nocapture"]
fn nw02_base_ports_chosen_by_the_harness_collide_under_the_same_neighbour() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => return eprintln!("\nE2E-SKIP nw02-base: {why}"),
    };
    // Shorter waits than the claim's: a collision shows as a node that dies
    // or never answers, and waiting thirty seconds per node for it proves no
    // more.
    let (tally, neighbour) = under_the_neighbour(
        "base",
        Duration::from_secs(10),
        Duration::from_secs(20),
        |dir| launch_prepicked(&binary, dir, &CLUSTER_IDS),
    );
    println!(
        "NW02-BASE ports the harness picked: clusters {CLUSTERS} of {} nodes, started {}, failed {}, \
         address-in-use lines {}; neighbour: {neighbour}",
        CLUSTER_IDS.len(),
        tally.started,
        tally.failed.len(),
        tally.address_in_use
    );
    for why in &tally.failed {
        println!("NW02-BASE {why}");
    }
    assert!(
        !tally.failed.is_empty(),
        "nw02-base: the neighbour did not make a single pre-picked cluster fail, so it cannot \
         show anything about the claim"
    );
}
