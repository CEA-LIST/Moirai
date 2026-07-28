//! End-to-end convergence tests.
//!
//! Each test starts a small cluster of real replica processes, drives them
//! through the HTTP API of `moirai-network`, and asserts that every replica
//! ends up with the *same* CRDT state — with and without conflicts.
//!
//! # What is under test
//!
//! The binary under test is `network_node`, the example shipped by the
//! Arachne-generated `json_crdt` crate. It lives in a sibling repository, so
//! it is built out-of-tree and located through the environment.
//!
//! Two interchangeable backends run it, behind the same [`Backend`] trait:
//!
//! - **process** — child processes on the loopback interface. Fast, needs no
//!   Docker, and covers every scenario except a genuine partition. Point
//!   `MOIRAI_E2E_NODE_BIN` at the binary:
//!
//!   ```bash
//!   MOIRAI_E2E_NODE_BIN=/path/to/target/debug/examples/network_node \
//!       cargo test -p moirai-network --test e2e_convergence -- --test-threads=1
//!   ```
//!
//! - **container** — one container per replica, with replication traffic on a
//!   network of its own so it can be cut for real. Needed by S4. Build the
//!   image from `docker/e2e/Dockerfile` first, and note that the *test
//!   process* talks to the Docker socket, so it must itself carry the `docker`
//!   group:
//!
//!   ```bash
//!   docker build -f moirai/docker/e2e/Dockerfile -t moirai-json-crdt:test .
//!   sg docker -c "cargo test -p moirai-network --test e2e_convergence -- --test-threads=1"
//!   ```
//!
//! A scenario whose backend is unavailable — no node binary, no daemon, no
//! image — prints `E2E-SKIP <scenario>: <why>` and returns green rather than
//! failing, so the suite stays runnable on a laptop with neither. Run with
//! `--nocapture` to see the notice; CI greps for that marker and fails the
//! job, because a silently skipped suite is worse than a red one.
//!
//! # Rules the harness obeys
//!
//! 1. **Poll to a deadline, never `sleep(n)` then assert.** Fixed sleeps are
//!    the primary source of flakiness in distributed tests.
//! 2. **`/api/state` is the only oracle.** `/api/operations` is a display-only
//!    `Vec` in the network layer and over-counts remote operations 2x, because
//!    both peers dial each other. See `opcount_double_delivery` at the bottom
//!    of this file, which documents that defect as an executable bug report.
//! 3. **States are compared as parsed `serde_json::Value`**, so map ordering is
//!    irrelevant — but string-CRDT ordering is semantic and still must match.
//! 4. **Readiness is mutual peer visibility, never process start or log
//!    contents.** `network_node` sleeps 2 s and then dials `PEERS` exactly
//!    once, so the first replica started always fails its initial dial: the
//!    peer is not listening yet. The mesh still forms, because the peer dials
//!    back and the inbound connection is adopted, so a connection error on
//!    stderr at startup is expected and must not fail a test.

#![allow(clippy::disallowed_names)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;

/// How long a mesh may take to form. Generous: the node sleeps 2 s before its
/// single dial attempt, and a debug-profile binary starts slowly under load.
const MESH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long replicas may take to agree once every operation has been accepted.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a node may take to answer `/api/health` after being spawned.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

/// Gap between polls. Small enough to keep tests fast, large enough not to
/// spin the single-threaded `tiny_http` server.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Number of log lines quoted in a failure report.
const LOG_TAIL_LINES: usize = 40;

/// `(peer_id, address)` pairs, or `(env_var, value)` pairs — the two lists a
/// replica is launched with.
type Pairs = Vec<(String, String)>;

// ---------------------------------------------------------------------------
// Operation payloads
// ---------------------------------------------------------------------------

/// Builders for the wire format of `json_crdt`'s operation type.
///
/// The shapes follow serde's default externally-tagged enum representation of
/// the generated types:
///
/// - `Json::JsonKind(JsonKind)` — `{"JsonKind": <kind>}`
/// - `JsonKind::{Object,String,Number,Boolean,Array}(..)` — `{"Object": <op>}`
/// - `UWMap::Update(K, O)` — `{"Update": [<key>, <op>]}`
/// - `UWMap::Remove(K)` — `{"Remove": <key>}`
/// - `List::Insert { content, pos }` — `{"Insert": {"content": .., "pos": ..}}`
/// - `Counter::Inc(V)` — `{"Inc": <n>}`
/// - `EWFlag::Enable` (a unit variant) — `"Enable"`
/// - `NestedList::Insert { pos, op }` — `{"Insert": {"pos": .., "op": ..}}`
///
/// Every builder here has been executed against a live node; none is guessed.
mod ops {
    use serde_json::{json, Value};

    /// `Json::JsonKind(JsonKind::Object(UWMap::Update(key, inner)))`.
    pub fn object_update(key: &str, inner: Value) -> Value {
        json!({ "JsonKind": { "Object": { "Update": [key, inner] } } })
    }

    /// `Json::JsonKind(JsonKind::Object(UWMap::Remove(key)))`.
    ///
    /// Note the observed semantics: `Remove` resets the child CRDT to its
    /// default value rather than dropping the key from the rendered state, so
    /// a removed string reads back as `[]`, not as an absent key.
    pub fn object_remove(key: &str) -> Value {
        json!({ "JsonKind": { "Object": { "Remove": key } } })
    }

    /// `JsonKind::String(List::Insert { content, pos })` — a sequence CRDT
    /// insert of a single character.
    pub fn string_insert(content: char, pos: usize) -> Value {
        json!({ "String": { "Insert": { "content": content.to_string(), "pos": pos } } })
    }

    /// `JsonKind::Number(Counter::Inc(by))`.
    pub fn number_inc(by: f64) -> Value {
        json!({ "Number": { "Inc": by } })
    }

    /// `JsonKind::Boolean(EWFlag::Enable)` — an enable-wins flag.
    #[allow(dead_code)]
    pub fn bool_enable() -> Value {
        json!({ "Boolean": "Enable" })
    }

    /// `JsonKind::Array(NestedList::Insert { pos, op })`.
    #[allow(dead_code)]
    pub fn array_insert(pos: usize, inner: Value) -> Value {
        json!({ "Array": { "Insert": { "pos": pos, "op": inner } } })
    }
}

// ---------------------------------------------------------------------------
// State readers
// ---------------------------------------------------------------------------

/// The rendered state is `{"json": {"Value": {"Object": { .. }}}}` once the
/// root object has at least one key, and `{"json": "Unset"}` before that.
fn root_object(state: &Value) -> Result<&serde_json::Map<String, Value>> {
    state
        .pointer("/json/Value/Object")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("state has no root object: {state}"))
}

/// Reads a string-valued key. The string CRDT renders as an array of
/// single-character strings, which this joins back into a `String`.
fn read_string(state: &Value, key: &str) -> Result<String> {
    let entry = root_object(state)?
        .get(key)
        .ok_or_else(|| anyhow!("no key `{key}` in {state}"))?;
    let chars = entry
        .pointer("/Value/String")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("key `{key}` is not a string: {entry}"))?;
    Ok(chars.iter().filter_map(Value::as_str).collect())
}

/// Reads a number-valued key (a counter).
fn read_number(state: &Value, key: &str) -> Result<f64> {
    let entry = root_object(state)?
        .get(key)
        .ok_or_else(|| anyhow!("no key `{key}` in {state}"))?;
    entry
        .pointer("/Value/Number")
        .and_then(Value::as_f64)
        .ok_or_else(|| anyhow!("key `{key}` is not a number: {entry}"))
}

// ---------------------------------------------------------------------------
// Node and backend traits
// ---------------------------------------------------------------------------

/// Where a replica can be reached. Fixed before anything starts, so that a
/// replica launched later (S6) can still be named in its peers' `PEERS` list.
#[derive(Clone, Debug)]
struct Endpoint {
    /// `host:port` of the replication listener, as *other replicas* see it.
    /// For processes that is `127.0.0.1:<port>`; for containers it is the
    /// container alias on the user-defined network.
    sync_addr: String,
    /// Port the replication listener binds inside its own namespace.
    listen_port: u16,
    /// Port the HTTP API binds inside its own namespace.
    http_port: u16,
}

/// A running replica, addressable over HTTP by the test process.
///
/// Implementors must terminate the replica in `Drop`: a leaked node holds its
/// ports and breaks every later run.
trait Node {
    fn id(&self) -> &str;

    /// Base URL of the HTTP API as reachable *from the test process*, e.g.
    /// `http://127.0.0.1:38119`.
    fn http_base(&self) -> &str;

    /// The last `lines` lines the replica emitted, for failure reports.
    fn log_tail(&self, lines: usize) -> String;
}

/// Something that can run replicas: local processes today, containers once
/// Docker is usable. Keeping this behind a trait is what lets S4 (a *real*
/// network partition) be written now and enabled later without touching the
/// scenarios.
trait Backend {
    /// Human-readable backend name, used in skip and failure messages.
    fn name(&self) -> &'static str;

    /// Fix `id`'s addresses. Called for every replica before any is started.
    fn reserve(&mut self, id: &str) -> Result<Endpoint>;

    /// Launch `id` with a hardcoded peer list of `(peer_id, host:port)` and
    /// any extra environment the scenario needs — `BOOTNODE_URL` and friends
    /// for the discovery scenarios, empty for every other.
    fn start(
        &mut self,
        id: &str,
        endpoint: &Endpoint,
        peers: &[(String, String)],
        extra_env: &[(String, String)],
    ) -> Result<Box<dyn Node>>;

    /// Sever `id` from the network at the infrastructure level — a genuine
    /// partition, not the in-process `pause` flag. Only the container backend
    /// can do this.
    fn cut_network(&mut self, id: &str) -> Result<()>;

    /// Undo [`Backend::cut_network`].
    fn restore_network(&mut self, id: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Process backend
// ---------------------------------------------------------------------------

/// Per-run counter, so concurrent test binaries do not share scratch dirs.
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

/// Asks the OS for an unused TCP port and hands it back.
///
/// There is an unavoidable window between releasing the listener and the child
/// binding the port, but the kernel does not hand out the same ephemeral port
/// twice in quick succession, which is enough in practice. It is still far
/// better than hardcoding 8081/9001, which makes repeated runs collide.
fn free_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("bind ephemeral port")?;
    Ok(listener.local_addr()?.port())
}

/// Runs replicas as child processes of the test binary.
struct ProcessBackend {
    binary: PathBuf,
    log_dir: PathBuf,
}

impl ProcessBackend {
    /// Returns the backend, or the reason the tests should be skipped.
    fn new() -> Result<Self> {
        let raw = std::env::var("MOIRAI_E2E_NODE_BIN").map_err(|_| {
            anyhow!(
                "MOIRAI_E2E_NODE_BIN is not set; point it at the `network_node` \
                 binary built from the generated json_crdt crate, e.g. \
                 `cargo build --example network_node` in arachne/generated/json_crdt"
            )
        })?;
        let binary = PathBuf::from(&raw);
        if !binary.is_file() {
            bail!("MOIRAI_E2E_NODE_BIN points at `{raw}`, which is not a file");
        }

        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let log_dir = std::env::temp_dir().join(format!("moirai-e2e-{}-{run}", std::process::id()));
        fs::create_dir_all(&log_dir)
            .with_context(|| format!("create log dir {}", log_dir.display()))?;

        Ok(Self { binary, log_dir })
    }
}

impl Backend for ProcessBackend {
    fn name(&self) -> &'static str {
        "process"
    }

    fn reserve(&mut self, _id: &str) -> Result<Endpoint> {
        let listen_port = free_port()?;
        let http_port = free_port()?;
        Ok(Endpoint {
            sync_addr: format!("127.0.0.1:{listen_port}"),
            listen_port,
            http_port,
        })
    }

    fn start(
        &mut self,
        id: &str,
        endpoint: &Endpoint,
        peers: &[(String, String)],
        extra_env: &[(String, String)],
    ) -> Result<Box<dyn Node>> {
        let peers_env = peers
            .iter()
            .map(|(peer, addr)| format!("{peer}:{addr}"))
            .collect::<Vec<_>>()
            .join(",");

        let log_path = self.log_dir.join(format!("{id}.log"));
        let log = fs::File::create(&log_path)
            .with_context(|| format!("create log file {}", log_path.display()))?;
        let log_err = log.try_clone().context("clone log handle")?;

        let child = Command::new(&self.binary)
            .env("REPLICA_ID", id)
            .env("LISTEN_PORT", endpoint.listen_port.to_string())
            .env("HTTP_PORT", endpoint.http_port.to_string())
            .env("PEERS", &peers_env)
            .envs(extra_env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()
            .with_context(|| format!("spawn {}", self.binary.display()))?;

        Ok(Box::new(ProcessNode {
            id: id.to_string(),
            http_base: format!("http://127.0.0.1:{}", endpoint.http_port),
            log_path,
            child,
        }))
    }

    fn cut_network(&mut self, _id: &str) -> Result<()> {
        bail!("the process backend cannot cut the network; S4 needs the container backend")
    }

    fn restore_network(&mut self, _id: &str) -> Result<()> {
        bail!("the process backend cannot cut the network; S4 needs the container backend")
    }
}

/// A replica running as a child process. Killed on drop.
struct ProcessNode {
    id: String,
    http_base: String,
    log_path: PathBuf,
    child: Child,
}

impl Node for ProcessNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn http_base(&self) -> &str {
        &self.http_base
    }

    fn log_tail(&self, lines: usize) -> String {
        let text = fs::read_to_string(&self.log_path).unwrap_or_else(|e| format!("<{e}>"));
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        // `network_node` has no shutdown endpoint and blocks in `run()`, so
        // SIGKILL is the only option. Reaping matters as much as killing: an
        // unreaped child keeps its listener until the test binary exits.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Container backend
// ---------------------------------------------------------------------------
//
// Driven through the `docker` CLI rather than a client library. The one thing
// the process backend cannot do is a genuine partition, and that is precisely
// `docker network disconnect` — an operation no Rust container crate exposes
// as a first-class primitive anyway.
//
// # Two networks, on purpose
//
// Each replica joins two user-defined networks:
//
// - a **control** network, joined at `docker run` time, which carries the
//   published HTTP port the test process drives the replica through;
// - a **replication** network, joined afterwards, which carries peer-to-peer
//   sync traffic and is the only network whose alias appears in `PEERS`.
//
// The split is what makes S4 possible. Disconnecting a container from its only
// network also tears down its published port, so the test process loses the
// ability to submit operations to — or even read the state of — the very
// replica it just partitioned. Measured: with a single network, `/api/state`
// on the partitioned replica stops answering entirely. Keeping the control
// plane on a separate network that is never cut leaves the replica observable
// and writable while it is genuinely severed from its peers.
//
// Peers are addressed by a replication-network **alias** rather than by the
// container name, because Docker registers the container name for DNS on every
// network it joins — including the control network. Using the alias guarantees
// that peer traffic has no route once the replication network is cut.

/// Runs replicas as containers, with replication traffic isolated on its own
/// network so that it can be cut for real.
struct ContainerBackend {
    image: String,
    /// Never cut: carries the published HTTP port.
    control_net: String,
    /// Cut and restored by [`Backend::cut_network`] / [`Backend::restore_network`].
    replication_net: String,
    /// `replica id -> (container name, replication-network alias)`.
    containers: BTreeMap<String, (String, String)>,
}

/// Runs `docker` with `args`, returning stdout on success.
fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .context("exec docker (is the CLI on PATH?)")?;
    if !out.status.success() {
        bail!(
            "docker {} failed ({}): {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

impl ContainerBackend {
    /// Returns the backend, or the reason the scenario should be skipped.
    ///
    /// Note that the *test process itself* talks to the Docker socket, so it
    /// must carry the `docker` group. A shell whose credentials predate being
    /// added to the group will not, and neither will anything it spawns; run
    /// the tests under `sg docker -c "cargo test ..."` in that case.
    fn new() -> Result<Self> {
        docker(&["info", "--format", "{{.ServerVersion}}"]).context(
            "the Docker daemon is not reachable; if the account was only just \
             added to the `docker` group, run the tests under \
             `sg docker -c \"cargo test ...\"` so the test process inherits it",
        )?;

        let image =
            std::env::var("MOIRAI_E2E_IMAGE").unwrap_or_else(|_| "moirai-json-crdt:test".into());
        docker(&["image", "inspect", &image]).with_context(|| {
            format!(
                "the replica image `{image}` does not exist; build it with \
                 `docker build -f moirai/docker/e2e/Dockerfile -t {image} .` \
                 from the directory holding the moirai and arachne checkouts, \
                 or point MOIRAI_E2E_IMAGE at an existing image"
            )
        })?;

        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("moirai-e2e-{}-{run}", std::process::id());
        let control_net = format!("{prefix}-ctl");
        let replication_net = format!("{prefix}-repl");
        docker(&["network", "create", &control_net]).context("create control network")?;
        docker(&["network", "create", &replication_net]).context("create replication network")?;

        Ok(Self {
            image,
            control_net,
            replication_net,
            containers: BTreeMap::new(),
        })
    }

    fn names_of(&self, id: &str) -> Result<&(String, String)> {
        self.containers
            .get(id)
            .ok_or_else(|| anyhow!("no container reserved for replica `{id}`"))
    }
}

impl Backend for ContainerBackend {
    fn name(&self) -> &'static str {
        "container"
    }

    fn reserve(&mut self, id: &str) -> Result<Endpoint> {
        let container = format!("{}-{id}", self.control_net);
        let alias = format!("{}-node-{id}", self.replication_net);
        self.containers
            .insert(id.to_string(), (container, alias.clone()));
        // Inside a container the ports are fixed and private; peers address
        // each other by replication alias, and only HTTP is published.
        Ok(Endpoint {
            sync_addr: format!("{alias}:9001"),
            listen_port: 9001,
            http_port: 8081,
        })
    }

    fn start(
        &mut self,
        id: &str,
        endpoint: &Endpoint,
        peers: &[(String, String)],
        extra_env: &[(String, String)],
    ) -> Result<Box<dyn Node>> {
        let (container, alias) = self.names_of(id)?.clone();
        let peers_env = peers
            .iter()
            .map(|(peer, addr)| format!("{peer}:{addr}"))
            .collect::<Vec<_>>()
            .join(",");

        let mut args: Vec<String> = [
            "run",
            "--detach",
            "--name",
            &container,
            // The control network is joined at creation, which is what binds
            // the published port; it is never disconnected.
            "--network",
            &self.control_net,
            "--env",
            &format!("REPLICA_ID={id}"),
            "--env",
            &format!("LISTEN_PORT={}", endpoint.listen_port),
            "--env",
            &format!("HTTP_PORT={}", endpoint.http_port),
            "--env",
            &format!("PEERS={peers_env}"),
            "--publish",
            &format!("127.0.0.1::{}", endpoint.http_port),
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        for (key, value) in extra_env {
            args.push("--env".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(self.image.clone());
        docker(&args.iter().map(String::as_str).collect::<Vec<_>>())?;

        // Join the replication network only now, so that disconnecting it
        // later leaves the control network — and the published port — intact.
        docker(&[
            "network",
            "connect",
            "--alias",
            &alias,
            &self.replication_net,
            &container,
        ])?;

        // Ask Docker which ephemeral host port it chose.
        let mapping = docker(&["port", &container, &format!("{}/tcp", endpoint.http_port)])?;
        let host_addr = mapping
            .lines()
            .next()
            .ok_or_else(|| anyhow!("`docker port {container}` returned nothing"))?
            .trim()
            .to_string();

        Ok(Box::new(ContainerNode {
            id: id.to_string(),
            http_base: format!("http://{host_addr}"),
            container,
        }))
    }

    fn cut_network(&mut self, id: &str) -> Result<()> {
        let (container, _) = self.names_of(id)?.clone();
        docker(&["network", "disconnect", &self.replication_net, &container]).map(|_| ())
    }

    fn restore_network(&mut self, id: &str) -> Result<()> {
        let (container, alias) = self.names_of(id)?.clone();
        docker(&[
            "network",
            "connect",
            "--alias",
            &alias,
            &self.replication_net,
            &container,
        ])
        .map(|_| ())
    }
}

impl Drop for ContainerBackend {
    fn drop(&mut self) {
        // The containers remove themselves in their own `Drop`, which runs
        // first; a network cannot be removed while an endpoint is attached.
        let _ = docker(&["network", "rm", &self.control_net, &self.replication_net]);
    }
}

/// A replica running as a container. Force-removed on drop.
struct ContainerNode {
    id: String,
    http_base: String,
    container: String,
}

impl Node for ContainerNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn http_base(&self) -> &str {
        &self.http_base
    }

    fn log_tail(&self, lines: usize) -> String {
        docker(&["logs", "--tail", &lines.to_string(), &self.container])
            .unwrap_or_else(|e| format!("<{e:#}>"))
    }
}

impl Drop for ContainerNode {
    fn drop(&mut self) {
        let _ = docker(&["rm", "--force", "--volumes", &self.container]);
    }
}

// ---------------------------------------------------------------------------
// Bootnode
// ---------------------------------------------------------------------------

/// A `moirai-bootnode` running as a child process, killed on drop.
///
/// Process-backed even for container scenarios would not work — a container
/// cannot reach the test host's loopback — so the discovery scenarios below
/// use the process backend only. The Compose rig is where discovery is
/// exercised across containers.
struct Bootnode {
    base_url: String,
    /// Session name, unique per run so concurrent test binaries do not share a
    /// roster through a bootnode one of them left behind.
    session: String,
    child: Child,
}

impl Bootnode {
    /// Starts a bootnode, or returns the reason the scenario should be skipped.
    fn start() -> Result<Self> {
        // Built by the same workspace as this test, so its path is derivable;
        // the environment override exists for a split target directory.
        let binary = match std::env::var("MOIRAI_E2E_BOOTNODE_BIN") {
            Ok(path) => PathBuf::from(path),
            Err(_) => {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/moirai-bootnode")
            }
        };
        if !binary.is_file() {
            bail!(
                "no bootnode binary at `{}`; build it with \
                 `cargo build -p moirai-bootnode`, or point \
                 MOIRAI_E2E_BOOTNODE_BIN at one",
                binary.display()
            );
        }

        let port = free_port()?;
        // Short TTL, so a scenario that watches an entry expire does not have
        // to wait 30 s for it.
        let child = Command::new(&binary)
            .env("BOOTNODE_PORT", port.to_string())
            .env("BOOTNODE_TTL_SECS", "10")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawn {}", binary.display()))?;

        let bootnode = Self {
            base_url: format!("http://127.0.0.1:{port}"),
            session: format!(
                "e2e-{}-{}",
                std::process::id(),
                RUN_SEQ.fetch_add(1, Ordering::Relaxed)
            ),
            child,
        };

        poll_until(HEALTH_TIMEOUT, || {
            let body = client()
                .get(format!("{}/health", bootnode.base_url))
                .send()?
                .error_for_status()?
                .text()?;
            let value: Value = serde_json::from_str(&body)?;
            Ok((value.get("status").and_then(Value::as_str) == Some("ok")).then_some(()))
        })
        .map_err(|e| {
            anyhow!(
                "bootnode never became healthy{}",
                e.map(|e| format!(" ({e:#})")).unwrap_or_default()
            )
        })?;

        Ok(bootnode)
    }

    /// The roster the directory currently holds, by replica id.
    fn roster(&self) -> Result<BTreeSet<String>> {
        let url = format!("{}/session/{}/peers", self.base_url, self.session);
        let body = client().get(&url).send()?.error_for_status()?.text()?;
        let value: Value = serde_json::from_str(&body)?;
        Ok(value
            .get("peers")
            .and_then(Value::as_array)
            .map(|peers| {
                peers
                    .iter()
                    .filter_map(|p| p.get("id").and_then(Value::as_str))
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl Drop for Bootnode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

/// A set of replicas whose addresses are all fixed up front but which start
/// independently, so a scenario can leave one offline for a while (S6).
struct Cluster {
    /// Declared first on purpose. Struct fields drop in declaration order, and
    /// a Docker network cannot be removed while a container is still attached
    /// to it, so the replicas must go before the backend that owns their
    /// networks. Moving this field below `backend` leaks a network per run.
    running: BTreeMap<String, Box<dyn Node>>,
    backend: Box<dyn Backend>,
    order: Vec<String>,
    endpoints: BTreeMap<String, Endpoint>,
    /// When set, replicas are started with **no** `PEERS` at all and find each
    /// other through the directory. Dropped after the replicas, like the
    /// backend, so nothing is polling a dead bootnode during teardown.
    bootnode: Option<Bootnode>,
}

impl Cluster {
    /// Reserves an endpoint for every id. Nothing is started yet.
    fn new(backend: Box<dyn Backend>, ids: &[&str]) -> Result<Self> {
        let mut cluster = Self {
            backend,
            order: ids.iter().map(|s| s.to_string()).collect(),
            endpoints: BTreeMap::new(),
            running: BTreeMap::new(),
            bootnode: None,
        };
        for id in ids {
            let endpoint = cluster.backend.reserve(id)?;
            cluster.endpoints.insert((*id).to_string(), endpoint);
        }
        Ok(cluster)
    }

    /// Switches this cluster to discovery: replicas started from now on get an
    /// empty `PEERS` and are told only where the bootnode is.
    fn with_bootnode(mut self, bootnode: Bootnode) -> Self {
        self.bootnode = Some(bootnode);
        self
    }

    fn bootnode(&self) -> &Bootnode {
        self.bootnode
            .as_ref()
            .expect("this cluster was not built with a bootnode")
    }

    /// Starts `id` and waits for its HTTP API to answer.
    ///
    /// Without a bootnode the replica gets a hardcoded peer list naming every
    /// *other* replica of the cluster, running or not — the pre-phase-1 shape,
    /// which every scenario below S8 relies on. With one, it gets no peers at
    /// all and has to find them.
    fn start(&mut self, id: &str) -> Result<()> {
        let endpoint = self
            .endpoints
            .get(id)
            .ok_or_else(|| anyhow!("replica `{id}` is not part of this cluster"))?
            .clone();

        let (peers, extra_env): (Pairs, Pairs) = match &self.bootnode {
            Some(bootnode) => (
                Vec::new(),
                vec![
                    ("BOOTNODE_URL".into(), bootnode.base_url.clone()),
                    ("SESSION_ID".into(), bootnode.session.clone()),
                    ("ADVERTISE_ADDR".into(), endpoint.sync_addr.clone()),
                    // Faster than the 5 s default so a scenario is not gated
                    // on the reconcile interval; still far below the TTL.
                    ("RECONCILE_SECS".into(), "1".into()),
                ],
            ),
            None => (
                self.order
                    .iter()
                    .filter(|other| other.as_str() != id)
                    .map(|other| (other.clone(), self.endpoints[other].sync_addr.clone()))
                    .collect(),
                Vec::new(),
            ),
        };

        let node = self
            .backend
            .start(id, &endpoint, &peers, &extra_env)
            .with_context(|| format!("start `{id}` on the {} backend", self.backend.name()))?;
        await_healthy(node.as_ref(), HEALTH_TIMEOUT)?;
        self.running.insert(id.to_string(), node);
        Ok(())
    }

    /// Starts every replica of the cluster.
    fn start_all(&mut self) -> Result<()> {
        for id in self.order.clone() {
            self.start(&id)?;
        }
        Ok(())
    }

    fn node(&self, id: &str) -> &dyn Node {
        self.running
            .get(id)
            .unwrap_or_else(|| panic!("replica `{id}` is not running"))
            .as_ref()
    }

    /// Every running replica, in declaration order.
    fn nodes(&self) -> Vec<&dyn Node> {
        self.order
            .iter()
            .filter_map(|id| self.running.get(id))
            .map(AsRef::as_ref)
            .collect()
    }

    fn cut_network(&mut self, id: &str) -> Result<()> {
        self.backend.cut_network(id)
    }

    fn restore_network(&mut self, id: &str) -> Result<()> {
        self.backend.restore_network(id)
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn client() -> &'static reqwest::blocking::Client {
    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build blocking HTTP client")
    })
}

fn get_json(node: &dyn Node, path: &str) -> Result<Value> {
    let url = format!("{}{path}", node.http_base());
    let body = client()
        .get(&url)
        .send()
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url}"))?
        .text()?;
    serde_json::from_str(&body).with_context(|| format!("GET {url} returned non-JSON: {body}"))
}

fn post_json(node: &dyn Node, path: &str, body: Option<&Value>) -> Result<Value> {
    let url = format!("{}{path}", node.http_base());
    let mut req = client().post(&url);
    if let Some(body) = body {
        req = req.json(body);
    }
    let text = req
        .send()
        .with_context(|| format!("POST {url}"))?
        .error_for_status()
        .with_context(|| format!("POST {url}"))?
        .text()?;
    serde_json::from_str(&text).with_context(|| format!("POST {url} returned non-JSON: {text}"))
}

/// Submits an operation and fails if the node did not accept it.
fn apply(node: &dyn Node, op: &Value) -> Result<()> {
    let reply = post_json(node, "/api/op", Some(op))?;
    if reply.get("success").and_then(Value::as_bool) != Some(true) {
        bail!("{} rejected {op}: {reply}", node.id());
    }
    Ok(())
}

/// Submits an operation, panicking on rejection. For use inside scenarios,
/// where a rejected operation is a test failure and not a condition to handle.
fn apply_ok(node: &dyn Node, op: Value) {
    apply(node, &op).unwrap_or_else(|e| panic!("apply on `{}`: {e:#}", node.id()));
}

fn state_of(node: &dyn Node) -> Result<Value> {
    get_json(node, "/api/state")
}

fn metrics_of(node: &dyn Node) -> Result<Value> {
    get_json(node, "/api/metrics")
}

/// Reads one unsigned counter out of `/api/metrics`.
fn metric(node: &dyn Node, field: &str) -> Result<u64> {
    let metrics = metrics_of(node)?;
    metrics
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("`{}` has no `{field}` in {metrics}", node.id()))
}

/// Cuts `node` off from every peer using the in-process pause flag.
///
/// This is a *simulated* partition: `transport.rs` buffers outbound messages
/// behind a flag instead of the socket actually failing. It is the strongest
/// partition available without Docker. S4 (`s4_real_partition_heals`) is the
/// real-network version, using `docker network disconnect`.
fn pause_all(node: &dyn Node) -> Result<()> {
    post_json(node, "/api/pause-all", None).map(|_| ())
}

/// Undoes [`pause_all`], flushing whatever was buffered while paused.
fn resume_all(node: &dyn Node) -> Result<()> {
    post_json(node, "/api/resume-all", None).map(|_| ())
}

/// Simulated partition of a single link, the per-peer counterpart of
/// [`pause_all`]. Used where a scenario needs to cut one link, not all.
#[allow(dead_code)]
fn pause_peer(node: &dyn Node, peer: &str) -> Result<()> {
    post_json(node, &format!("/api/pause/{peer}"), None).map(|_| ())
}

#[allow(dead_code)]
fn resume_peer(node: &dyn Node, peer: &str) -> Result<()> {
    post_json(node, &format!("/api/resume/{peer}"), None).map(|_| ())
}

// ---------------------------------------------------------------------------
// Polling primitives
// ---------------------------------------------------------------------------

/// Calls `check` until it returns `Ok(Some(v))` or `timeout` elapses.
///
/// Errors from `check` are *not* fatal: while a cluster converges, a node may
/// legitimately be mid-restart or briefly unreachable. The last error is
/// returned if the deadline passes, so failure reports still explain why.
fn poll_until<T>(
    timeout: Duration,
    mut check: impl FnMut() -> Result<Option<T>>,
) -> std::result::Result<T, Option<anyhow::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last_err = None;
    loop {
        match check() {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(e) => last_err = Some(e),
        }
        if Instant::now() >= deadline {
            return Err(last_err);
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Waits until the node answers `/api/health`.
fn await_healthy(node: &dyn Node, timeout: Duration) -> Result<()> {
    poll_until(timeout, || {
        let health = get_json(node, "/api/health")?;
        Ok((health.get("status").and_then(Value::as_str) == Some("ok")).then_some(()))
    })
    .map_err(|e| {
        anyhow!(
            "`{}` never became healthy within {timeout:?}{}\n--- log ---\n{}",
            node.id(),
            e.map(|e| format!(" (last error: {e:#})"))
                .unwrap_or_default(),
            node.log_tail(LOG_TAIL_LINES)
        )
    })
}

/// Waits until every node reports every other node as `Connected`.
///
/// This — not process start, and never log contents — is the readiness gate.
/// The first replica started always fails its one dial attempt because its
/// peer is not listening yet; the mesh is repaired by the peer's inbound
/// `Hello` -> `SyncRequest`, which is only observable here.
fn await_mesh(nodes: &[&dyn Node], timeout: Duration) -> Result<()> {
    let expected: BTreeSet<&str> = nodes.iter().map(|n| n.id()).collect();

    let result = poll_until(timeout, || {
        for node in nodes {
            let peers = get_json(*node, "/api/peers")?;
            let connected: BTreeSet<&str> = peers
                .get("peers")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter(|p| p.get("status").and_then(Value::as_str) == Some("Connected"))
                        .filter_map(|p| p.get("id").and_then(Value::as_str))
                        .collect()
                })
                .unwrap_or_default();
            let missing: Vec<&str> = expected
                .iter()
                .filter(|id| **id != node.id() && !connected.contains(*id))
                .copied()
                .collect();
            if !missing.is_empty() {
                return Ok(None);
            }
        }
        Ok(Some(()))
    });

    result.map_err(|last_err| {
        let mut report = format!("mesh did not form within {timeout:?}");
        if let Some(e) = last_err {
            let _ = write!(report, " (last error: {e:#})");
        }
        for node in nodes {
            let peers = get_json(*node, "/api/peers")
                .map(|v| v.to_string())
                .unwrap_or_else(|e| format!("<{e:#}>"));
            let _ = write!(
                report,
                "\n\n=== {} ===\npeers: {peers}\n--- log ---\n{}",
                node.id(),
                node.log_tail(LOG_TAIL_LINES)
            );
        }
        anyhow!(report)
    })
}

/// Polls `/api/state` until every node reports a structurally identical value,
/// and returns that agreed value.
///
/// States are compared as parsed `serde_json::Value`, so key ordering in the
/// map is irrelevant; the ordering *inside* a string CRDT is semantic and is
/// still compared exactly, because it is an array.
///
/// On timeout this panics with the states side by side and the tail of every
/// node's log: a convergence failure with no diff is unactionable in CI.
fn assert_converged(nodes: &[&dyn Node], timeout: Duration) -> Value {
    assert!(
        !nodes.is_empty(),
        "assert_converged needs at least one node"
    );

    let result = poll_until(timeout, || {
        let mut states = Vec::with_capacity(nodes.len());
        for node in nodes {
            states.push(state_of(*node)?);
        }
        Ok(states
            .windows(2)
            .all(|w| w[0] == w[1])
            .then(|| states.remove(0)))
    });

    match result {
        Ok(state) => state,
        Err(last_err) => {
            let mut report = format!(
                "replicas did not converge within {timeout:?}: {}",
                nodes.iter().map(|n| n.id()).collect::<Vec<_>>().join(", ")
            );
            if let Some(e) = last_err {
                let _ = write!(report, "\nlast error: {e:#}");
            }
            for node in nodes {
                let state = state_of(*node)
                    .map(|v| serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string()))
                    .unwrap_or_else(|e| format!("<{e:#}>"));
                let _ = write!(
                    report,
                    "\n\n=== {} state ===\n{state}\n--- {} log (last {LOG_TAIL_LINES} lines) ---\n{}",
                    node.id(),
                    node.id(),
                    node.log_tail(LOG_TAIL_LINES)
                );
            }
            panic!("{report}");
        }
    }
}

/// Asserts that two replicas currently *disagree* — used to prove a partition
/// is genuine before healing it. Retries briefly, because the divergent
/// operations still have to be applied locally.
fn assert_diverged(a: &dyn Node, b: &dyn Node, timeout: Duration) {
    let result = poll_until(timeout, || {
        let (sa, sb) = (state_of(a)?, state_of(b)?);
        Ok((sa != sb).then_some(()))
    });
    if result.is_err() {
        panic!(
            "`{}` and `{}` agree, so the partition did not take effect; both report {}",
            a.id(),
            b.id(),
            state_of(a)
                .map(|v| v.to_string())
                .unwrap_or_else(|e| format!("<{e:#}>"))
        );
    }
}

// ---------------------------------------------------------------------------
// Test setup
// ---------------------------------------------------------------------------

/// Builds a process-backed cluster, or returns `None` after explaining why the
/// scenario is being skipped.
///
/// A missing node binary is a *skip*, not a failure: the binary is produced by
/// a sibling repository and is not always present.
fn process_cluster(scenario: &str, ids: &[&str]) -> Option<Cluster> {
    match ProcessBackend::new() {
        Ok(backend) => Some(
            Cluster::new(Box::new(backend), ids)
                .unwrap_or_else(|e| panic!("{scenario}: reserve endpoints: {e:#}")),
        ),
        Err(why) => {
            // `E2E-SKIP` is a machine-readable marker: CI greps for it, because
            // a skipped scenario otherwise reports as a pass.
            eprintln!("\nE2E-SKIP {scenario}: {why:#}");
            None
        }
    }
}

/// Same, for the container backend. An unreachable daemon or a missing replica
/// image is a skip, with the command needed to fix it in the message.
fn container_cluster(scenario: &str, ids: &[&str]) -> Option<Cluster> {
    match ContainerBackend::new() {
        Ok(backend) => Some(
            Cluster::new(Box::new(backend), ids)
                .unwrap_or_else(|e| panic!("{scenario}: reserve endpoints: {e:#}")),
        ),
        Err(why) => {
            // `E2E-SKIP` is a machine-readable marker: CI greps for it, because
            // a skipped scenario otherwise reports as a pass.
            eprintln!("\nE2E-SKIP {scenario}: {why:#}");
            None
        }
    }
}

/// Boilerplate shared by the two-replica scenarios: reserve, start, wait for
/// the mesh. Returns `None` if the scenario must be skipped.
fn started_pair(scenario: &str) -> Option<Cluster> {
    let mut cluster = process_cluster(scenario, &["a", "b"])?;
    cluster
        .start_all()
        .unwrap_or_else(|e| panic!("{scenario}: start cluster: {e:#}"));
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).unwrap_or_else(|e| panic!("{scenario}: {e:#}"));
    Some(cluster)
}

// ---------------------------------------------------------------------------
// S1 — no conflict, disjoint keys
// ---------------------------------------------------------------------------

/// A writes `name`, B writes `city`. Both keys must be visible on both.
#[test]
fn s1_disjoint_keys_converge() {
    let Some(cluster) = started_pair("S1") else {
        return;
    };
    let (a, b) = (cluster.node("a"), cluster.node("b"));

    apply_ok(a, ops::object_update("name", ops::string_insert('B', 0)));
    apply_ok(b, ops::object_update("city", ops::string_insert('P', 0)));

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    assert_eq!(read_string(&state, "name").unwrap(), "B", "state: {state}");
    assert_eq!(read_string(&state, "city").unwrap(), "P", "state: {state}");
}

// ---------------------------------------------------------------------------
// S2 — no conflict, commuting counter
// ---------------------------------------------------------------------------

/// Both replicas increment the same counter while partitioned. Increments
/// commute, so the healed value must be the sum — the cleanest possible
/// convergence assertion, a single scalar.
#[test]
fn s2_concurrent_counter_increments_sum() {
    let Some(cluster) = started_pair("S2") else {
        return;
    };
    let (a, b) = (cluster.node("a"), cluster.node("b"));

    // Simulated partition (see `pause_all`), so the increments really are
    // concurrent rather than merely near-simultaneous.
    pause_all(a).unwrap();
    pause_all(b).unwrap();

    apply_ok(a, ops::object_update("age", ops::number_inc(5.0)));
    apply_ok(b, ops::object_update("age", ops::number_inc(3.0)));

    resume_all(a).unwrap();
    resume_all(b).unwrap();

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_number(&state, "age").unwrap(), 8.0, "state: {state}");
}

// ---------------------------------------------------------------------------
// S3 — conflict on the same string
// ---------------------------------------------------------------------------

/// Both replicas insert a different character at the *same* position of the
/// same string, concurrently.
///
/// This is the no-silent-loss assertion: after healing, the replicas must not
/// only agree, they must have kept **both** characters. A last-writer-wins
/// store would drop one of them and still "converge".
#[test]
fn s3_concurrent_string_inserts_keep_both() {
    let Some(cluster) = started_pair("S3") else {
        return;
    };
    let (a, b) = (cluster.node("a"), cluster.node("b"));

    // Common ancestor: "B".
    apply_ok(a, ops::object_update("name", ops::string_insert('B', 0)));
    let seeded = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&seeded, "name").unwrap(), "B");

    pause_all(a).unwrap();
    pause_all(b).unwrap();

    apply_ok(a, ops::object_update("name", ops::string_insert('X', 1)));
    apply_ok(b, ops::object_update("name", ops::string_insert('Y', 1)));
    assert_diverged(a, b, CONVERGE_TIMEOUT);

    resume_all(a).unwrap();
    resume_all(b).unwrap();

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    let name = read_string(&state, "name").unwrap();

    // The interleaving is deterministic (observed: "BXY"), but the claim worth
    // asserting is the semantic one: no edit was silently discarded.
    assert!(name.contains('X'), "A's insert was lost: name = {name:?}");
    assert!(name.contains('Y'), "B's insert was lost: name = {name:?}");
    assert!(name.contains('B'), "the ancestor was lost: name = {name:?}");
    assert_eq!(name.len(), 3, "unexpected extra characters: {name:?}");
}

// ---------------------------------------------------------------------------
// S4 — conflict under a real network partition
// ---------------------------------------------------------------------------

/// The real-network counterpart of S3: instead of the in-process pause flag,
/// node B is genuinely disconnected with `docker network disconnect`.
///
/// This is what makes "presence / failure detection" measurable rather than
/// simulated, and it is the one scenario the process backend cannot express.
///
/// The partition is real in the sense that matters: while it is in force, IP
/// packets between the two replicas have no route at all, and the assertion
/// that the states have diverged proves it took effect. The replicas keep
/// answering the test process throughout, because the control plane rides a
/// second network that is never cut — see the container backend's header.
///
/// `network_node` dials its peers exactly once at startup and never redials,
/// so healing here depends on the TCP sessions surviving the outage rather
/// than on any reconnection logic. Measured: they do — the container keeps its
/// address on the replication network and the sessions resume, so buffered
/// operations flow and both replicas reach `"BXY"`. A partition long enough to
/// trip a keepalive timeout, or one that changes the container's address,
/// would not heal; that is a real limitation of the current transport and is
/// worth its own scenario once reconnection exists.
#[test]
fn s4_real_partition_heals() {
    let Some(mut cluster) = container_cluster("S4", &["a", "b"]) else {
        return;
    };
    cluster.start_all().expect("S4: start containers");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("S4: mesh");

    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    cluster.cut_network("b").expect("S4: disconnect b");

    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('X', 1)),
    );
    apply_ok(
        cluster.node("b"),
        ops::object_update("name", ops::string_insert('Y', 1)),
    );
    assert_diverged(cluster.node("a"), cluster.node("b"), CONVERGE_TIMEOUT);

    cluster.restore_network("b").expect("S4: reconnect b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("S4: mesh after heal");

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    let name = read_string(&state, "name").unwrap();
    assert!(name.contains('X') && name.contains('Y'), "name = {name:?}");
}

// ---------------------------------------------------------------------------
// S5 — Update-Wins map semantics
// ---------------------------------------------------------------------------

/// A updates a key while B concurrently removes it.
///
/// Under an Update-Wins map the concurrent update must survive the removal.
/// This asserts a *semantic policy*, not merely convergence: an LWW map would
/// converge just as happily on the empty value, and be wrong.
///
/// Precise expectation, verified against a live pair: the ancestor character
/// `B` was written *before* the removal, so it is causally dominated and is
/// removed; `X` was written *concurrently* with the removal, so it wins. Both
/// replicas settle on `"X"`.
#[test]
fn s5_update_wins_over_concurrent_remove() {
    let Some(cluster) = started_pair("S5") else {
        return;
    };
    let (a, b) = (cluster.node("a"), cluster.node("b"));

    apply_ok(a, ops::object_update("name", ops::string_insert('B', 0)));
    let seeded = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&seeded, "name").unwrap(), "B");

    // Simulated partition; S4 is the real-network version.
    pause_all(a).unwrap();
    pause_all(b).unwrap();

    apply_ok(a, ops::object_update("name", ops::string_insert('X', 1)));
    apply_ok(b, ops::object_remove("name"));
    assert_diverged(a, b, CONVERGE_TIMEOUT);

    resume_all(a).unwrap();
    resume_all(b).unwrap();

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    let name = read_string(&state, "name").unwrap();
    assert_eq!(
        name, "X",
        "update-wins violated: the concurrent insert should survive the remove, \
         and the causally-prior character should not; state: {state}"
    );
}

// ---------------------------------------------------------------------------
// S6 — offline catch-up
// ---------------------------------------------------------------------------

/// Sets up the S6 topology: A alone with three operations applied, B reserved
/// but not started. Returns `None` when the scenario must be skipped.
fn s6_seeded_solo_cluster(scenario: &str) -> Option<Cluster> {
    let mut cluster = process_cluster(scenario, &["a", "b"])?;

    // Both endpoints are reserved, but only A is started: A's single dial
    // attempt at t+2 s will fail, which is expected and must not fail the test.
    cluster
        .start("a")
        .unwrap_or_else(|e| panic!("{scenario}: start a: {e:#}"));

    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    apply_ok(
        cluster.node("a"),
        ops::object_update("age", ops::number_inc(30.0)),
    );
    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('o', 1)),
    );

    // Sanity: A really did accumulate the operations before B existed.
    let solo = assert_converged(&[cluster.node("a")], CONVERGE_TIMEOUT);
    assert_eq!(read_string(&solo, "name").unwrap(), "Bo");
    assert_eq!(read_number(&solo, "age").unwrap(), 30.0);

    Some(cluster)
}

/// A runs alone, then B starts and must at least become a visible peer on both
/// sides — the precondition for any state transfer.
///
/// This is a regression test for a transport defect this suite uncovered. The
/// reader thread used to rewrite a connection's peer id to the identity in the
/// `Hello` *before* handing the message to `try_recv`, so the remap that
/// rekeys an accepted connection from its temporary id to the real one never
/// fired. The consequence was invisible while both replicas started together
/// — each had also dialled the other, so the correct key existed anyway — but
/// a replica whose own dial had failed ended up able to receive from its peer
/// and never able to send to it, reporting the peer as `Disconnected` and
/// logging `Failed to request sync from b: Peer not found: b`.
#[test]
fn s6_late_joiner_joins_mesh() {
    let Some(mut cluster) = s6_seeded_solo_cluster("S6-mesh") else {
        return;
    };
    cluster.start("b").expect("S6: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("S6: mesh after b joined");
}

/// A runs alone and accumulates operations; B starts afterwards and must catch
/// up from nothing.
///
/// This was `#[ignore]`d as an executable bug report until phase 1 step 1.
///
/// The defect: only the *dialling* side sends a `Hello`, and the *accepting*
/// side is the one that answers it with a `SyncRequest`. So the acceptor
/// pulled the dialer's history and the dialer pulled none. When both replicas
/// start together each is dialer and acceptor at once, which masks it; a late
/// joiner is only ever a dialer, which exposes it. Measured before the fix: B
/// connected, both replicas listed each other as `Connected`, and B's
/// `/api/state` stayed `"Unset"` indefinitely.
///
/// The fix needed no protocol change. `connect_to_peers()` already returns the
/// peers it newly connected to, so the dialer now issues its own `SyncRequest`
/// per new link — the same three lines the resume path already used.
///
/// Do not "fix" a regression here by calling `/api/resume/<peer>`, which also
/// emits a `SyncRequest`: that endpoint exists for partition simulation and
/// using it would hide the join path behind the harness.
#[test]
fn s6_late_joiner_catches_up() {
    let Some(mut cluster) = s6_seeded_solo_cluster("S6-catchup") else {
        return;
    };
    cluster.start("b").expect("S6: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("S6: mesh after b joined");

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&state, "name").unwrap(), "Bo", "state: {state}");
    assert_eq!(read_number(&state, "age").unwrap(), 30.0, "state: {state}");
}

// ---------------------------------------------------------------------------
// S8 — causal stability is observable, and stalls on a silent member
// ---------------------------------------------------------------------------

/// `/api/metrics` reports a stable prefix that advances while both replicas
/// talk, freezes while one is silent, and resumes when it comes back.
///
/// This is the phase-1 measuring instrument under test, not the CRDT. The
/// whole membership problem reduces to that middle sentence: `stable_prefix`
/// is a column-wise minimum over every *known* replica, so a single member
/// that stops acknowledging pins it forever, and nothing can be compacted out
/// of the log behind it. E4 is this scenario without the resume.
///
/// The freeze assertion is not a timing guess. The pause is in force, and the
/// operations are confirmed to have landed locally (`retained_ops` grew), so
/// any advance in `stable_prefix` would require a message to have crossed a
/// severed link.
#[test]
fn s8_stable_prefix_stalls_while_a_peer_is_silent() {
    let Some(cluster) = started_pair("S8") else {
        return;
    };
    let (a, b) = (cluster.node("a"), cluster.node("b"));

    // Both replicas must contribute: a column-wise minimum over a matrix where
    // one replica has never sent anything is pinned at zero regardless.
    apply_ok(a, ops::object_update("name", ops::string_insert('B', 0)));
    apply_ok(b, ops::object_update("city", ops::string_insert('P', 0)));
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // Stability needs one more round trip than convergence does: a replica
    // learns an operation is stable only once it sees the peer's clock move
    // past it, which happens on the next message.
    let baseline = poll_until(CONVERGE_TIMEOUT, || {
        let sa = metric(a, "stable_prefix")?;
        let sb = metric(b, "stable_prefix")?;
        Ok((sa > 0 && sb > 0).then_some(sa.min(sb)))
    })
    .unwrap_or_else(|e| {
        panic!(
            "stable_prefix never advanced on a healthy pair{}; a: {:?}, b: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(a),
            metrics_of(b),
        )
    });

    // --- one member goes silent ---
    pause_all(a).unwrap();
    pause_all(b).unwrap();

    let retained_before = metric(b, "retained_ops").unwrap();
    let frozen_at = metric(b, "stable_prefix").unwrap();

    for pos in 1..=5 {
        apply_ok(b, ops::object_update("city", ops::string_insert('x', pos)));
    }

    // The operations are on B: the replication buffer grew by exactly five.
    let retained_after = metric(b, "retained_ops").unwrap();
    assert_eq!(
        retained_after,
        retained_before + 5,
        "B applied five operations but its retained buffer went {retained_before} -> \
         {retained_after}; metrics: {:?}",
        metrics_of(b)
    );

    // And none of them can ever become stable, because A cannot acknowledge.
    assert_eq!(
        metric(b, "stable_prefix").unwrap(),
        frozen_at,
        "stable_prefix advanced across a severed link; metrics: {:?}",
        metrics_of(b)
    );

    // --- the member comes back ---
    resume_all(a).unwrap();
    resume_all(b).unwrap();
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // Converged is not the same as stable, and the gap is a protocol property
    // worth stating: a replica learns the peer's clock only from a message the
    // peer sends. There is no acknowledgement or heartbeat. So after the heal
    // B has A's operations, but B's own column stays pinned until A speaks
    // again — measured directly here: without this operation, B sat at
    // `stable_version {a: 1, b: 0}` indefinitely.
    //
    // Consequence for the measurement runs: an E4 baseline in which only some
    // replicas write does not produce an advancing stable prefix for the
    // silent ones, and would be mistaken for the very stall under study. Every
    // replica must write.
    apply_ok(a, ops::object_update("name", ops::string_insert('C', 1)));

    poll_until(CONVERGE_TIMEOUT, || {
        Ok((metric(b, "stable_prefix")? > frozen_at).then_some(()))
    })
    .unwrap_or_else(|e| {
        panic!(
            "stable_prefix stayed at {frozen_at} after the peer returned{}; metrics: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(b)
        )
    });

    // `ops_applied` counts what this replica originated, and is exact — unlike
    // `/api/operations`, which double-counts remote deliveries.
    assert_eq!(metric(a, "ops_applied").unwrap(), 2);
    assert_eq!(metric(b, "ops_applied").unwrap(), 6);
    assert!(baseline > 0);
}

// ---------------------------------------------------------------------------
// E1–E3 — discovery through a bootnode, with no static peer configuration
// ---------------------------------------------------------------------------

/// Builds a cluster whose replicas are told nothing about each other.
///
/// A missing bootnode binary is a skip for the same reason a missing node
/// binary is: it makes the suite runnable on a checkout that has not built
/// everything.
fn discovered_cluster(scenario: &str, ids: &[&str]) -> Option<Cluster> {
    let cluster = process_cluster(scenario, ids)?;
    match Bootnode::start() {
        Ok(bootnode) => Some(cluster.with_bootnode(bootnode)),
        Err(why) => {
            eprintln!("\nE2E-SKIP {scenario}: {why:#}");
            None
        }
    }
}

/// Asks a replica to deregister from its session while continuing to run.
fn leave_session(node: &dyn Node) -> Result<()> {
    let reply = post_json(node, "/api/leave", None)?;
    if reply.get("success").and_then(Value::as_bool) != Some(true) {
        bail!("{} refused to leave: {reply}", node.id());
    }
    Ok(())
}

/// Waits until the directory's roster is exactly `expected`.
fn await_roster(bootnode: &Bootnode, expected: &[&str], timeout: Duration) -> Result<()> {
    let want: BTreeSet<String> = expected.iter().map(ToString::to_string).collect();
    poll_until(timeout, || Ok((bootnode.roster()? == want).then_some(()))).map_err(|e| {
        anyhow!(
            "directory roster never became {want:?} (last seen {:?}){}",
            bootnode.roster(),
            e.map(|e| format!("; last error: {e:#}"))
                .unwrap_or_default()
        )
    })
}

/// **E1** — three replicas start together with an empty `PEERS` and find each
/// other through the directory.
///
/// This is what the whole phase is for. Every scenario above hardcodes the
/// full peer list at launch, which is only possible because the membership is
/// known before anything starts. Here nothing knows anything: each replica
/// registers its own address, reads back the others, and dials them.
#[test]
fn e1_discovery_forms_a_mesh_with_no_static_peers() {
    let Some(mut cluster) = discovered_cluster("E1", &["a", "b", "c"]) else {
        return;
    };
    cluster.start_all().expect("E1: start cluster");

    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("E1: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("E1: mesh");

    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    apply_ok(
        cluster.node("b"),
        ops::object_update("city", ops::string_insert('P', 0)),
    );
    apply_ok(
        cluster.node("c"),
        ops::object_update("age", ops::number_inc(7.0)),
    );

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&state, "name").unwrap(), "B", "state: {state}");
    assert_eq!(read_string(&state, "city").unwrap(), "P", "state: {state}");
    assert_eq!(read_number(&state, "age").unwrap(), 7.0, "state: {state}");
}

/// **E2 / T1** — a replica that joins after the others have done work.
///
/// **The acceptance test for phase 2**, and the phase-1 exit criterion that
/// could not be met. It was `#[ignore]`d as an executable report of the gap:
/// `SyncRequest` is served from the TCSB outbox, and `prune_outbox` drops
/// everything at or below the causally stable version, because by then it has
/// been folded into the compacted stable state. A joiner could replay the
/// unstable suffix and nothing else.
///
/// Measured before the fix, on two replicas that exchanged five operations each
/// and converged, immediately before a third joined:
///
/// ```text
/// a: {"stable_prefix":10, "retained_ops":0, ...}
/// b: {"stable_prefix": 9, "retained_ops":1, ...}
/// c: {"delivered_ops":0, "pending_ops":1, ...}   state: "Unset"
/// ```
///
/// `a` had nothing left to send. `c` received `b`'s single unstable event and
/// could not deliver it, correctly, because it depends on history `c` would
/// never receive.
///
/// # Why this cannot pass for the wrong reason
///
/// `s6_late_joiner_catches_up` passes even without state transfer, because
/// there `a` runs alone: `b`'s column never advances, so nothing ever becomes
/// stable, so nothing is pruned and the entire history is still replayable out
/// of the outbox. A test that passes because compaction never ran has not
/// tested state transfer.
///
/// So the middle section below is not decoration. It waits until the stable
/// frontier has advanced past almost everything and asserts, on `/api/metrics`,
/// that what remains replayable is a small fraction of what was applied. Only
/// then does the joiner start. Measured on a passing run: the joiner adopted a
/// stable prefix of 19 with a single event above it, out of 20 operations —
/// i.e. 19 of them existed nowhere as operations any more.
#[test]
fn e2_a_replica_joining_late_catches_up() {
    /// Operations applied before the joiner arrives, half on each replica.
    const APPLIED: u64 = 20;
    /// How much of that must be causally stable — and therefore unreachable by
    /// replay — before the joiner is allowed to start.
    const MUST_BE_STABLE: u64 = 15;

    let Some(mut cluster) = discovered_cluster("E2", &["a", "b", "c"]) else {
        return;
    };
    cluster.start("a").expect("E2: start a");
    cluster.start("b").expect("E2: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("E2: initial mesh");

    for pos in 0..(APPLIED / 2) {
        apply_ok(
            cluster.node("a"),
            ops::object_update("name", ops::string_insert('x', pos as usize)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("age", ops::number_inc(1.0)),
        );
    }
    let before = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&before, "name").unwrap().len(), 10);
    assert_eq!(read_number(&before, "age").unwrap(), 10.0);

    // --- the precondition that makes this test mean anything ---
    //
    // Stability lags convergence by one message in each direction: a replica
    // learns an operation is stable only once it sees every peer's clock move
    // past it. Poll for it rather than sleeping.
    let (a, b) = (cluster.node("a"), cluster.node("b"));
    poll_until(CONVERGE_TIMEOUT, || {
        let stable = metric(a, "stable_prefix")?.min(metric(b, "stable_prefix")?);
        Ok((stable >= MUST_BE_STABLE).then_some(stable))
    })
    .unwrap_or_else(|e| {
        panic!(
            "the stable frontier never passed {MUST_BE_STABLE} of {APPLIED} operations{}, so \
             nothing was compacted away and this scenario would prove nothing; a: {:?}, b: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(a),
            metrics_of(b),
        )
    });
    for node in [a, b] {
        let retained = metric(node, "retained_ops").unwrap();
        let delivered = metric(node, "delivered_ops").unwrap();
        assert_eq!(
            delivered,
            APPLIED,
            "`{}`: {:?}",
            node.id(),
            metrics_of(node)
        );
        assert!(
            retained <= APPLIED - MUST_BE_STABLE,
            "`{}` still holds {retained} of {delivered} operations in its outbox, so a joiner \
             could reach the state by plain replay and this scenario would prove nothing; \
             metrics: {:?}",
            node.id(),
            metrics_of(node)
        );
    }

    // --- the joiner ---
    cluster.start("c").expect("E2: start c");
    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("E2: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("E2: mesh after c joined");

    let after = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(after, before, "the joiner did not reach the existing state");

    // Converging on the rendered value is not quite enough: an empty log and a
    // full one can render alike if the value happens to be default. The joiner
    // must also account for every operation, or its own next write would look
    // causally concurrent with history it has already folded in.
    let c = cluster.node("c");
    assert_eq!(
        metric(c, "delivered_ops").unwrap(),
        APPLIED,
        "the joiner rendered the right state but does not account for the history behind it; \
         metrics: {:?}",
        metrics_of(c)
    );
    assert_eq!(
        metric(c, "ops_applied").unwrap(),
        0,
        "the joiner originated operations of its own; this scenario is about adoption"
    );
}

/// **E3** — a replica leaves the directory and the session carries on.
///
/// Note what this does *not* assert, because it is the finding phase 2 is
/// about: after `c` leaves, the remaining replicas keep converging, but their
/// stable prefix is still waiting on `c`. Leaving the directory is not being
/// evicted from the causal member set, and `stable_version` still carries a
/// column for `c`. Making that column go away is the phase-2 contribution.
#[test]
fn e3_a_replica_leaves_and_the_session_continues() {
    let Some(mut cluster) = discovered_cluster("E3", &["a", "b", "c"]) else {
        return;
    };
    cluster.start_all().expect("E3: start cluster");
    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("E3: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("E3: mesh");

    apply_ok(
        cluster.node("c"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    leave_session(cluster.node("c")).expect("E3: c leaves");
    await_roster(cluster.bootnode(), &["a", "b"], MESH_TIMEOUT).expect("E3: c left the directory");

    // The survivors still replicate to each other.
    apply_ok(
        cluster.node("a"),
        ops::object_update("city", ops::string_insert('P', 0)),
    );
    let survivors = [cluster.node("a"), cluster.node("b")];
    let state = assert_converged(&survivors, CONVERGE_TIMEOUT);
    assert_eq!(read_string(&state, "city").unwrap(), "P", "state: {state}");

    // And they are still carrying `c` in their causal member set, which is
    // exactly the cost this rig exists to measure.
    let metrics = metrics_of(cluster.node("a")).unwrap();
    assert!(
        metrics.pointer("/stable_version/c").is_some(),
        "leaving the directory silently evicted `c` from the causal member \
         set; that would make E4 unmeasurable. metrics: {metrics}"
    );
}

// ---------------------------------------------------------------------------
// S7 — restart / durability
// ---------------------------------------------------------------------------

/// A replica is killed and restarted; it should come back with its state.
///
/// **This is expected to fail.** Replica state is held in memory only — there
/// is no persistence layer (decision D-6, still open). The test is written now
/// so that it turns green by itself the day durability lands, rather than
/// having to be remembered and re-derived.
///
/// It also cannot be written honestly against the process backend as it
/// stands: `Cluster` has no restart primitive because there is nothing for a
/// restarted process to reload. Adding one is part of the durability work.
#[test]
#[ignore = "expected to fail: no persistence (D-6); state is in memory only"]
fn s7_restart_recovers_state() {
    let Some(cluster) = started_pair("S7") else {
        return;
    };
    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    unimplemented!(
        "restart B and assert it reloads `name` == \"B\"; blocked on D-6 \
         (persistence). Until a replica can reload state from disk, a restarted \
         node comes back empty and re-syncs from its peer, which would make this \
         test pass for the wrong reason."
    );
}

// ---------------------------------------------------------------------------
// Executable bug report — operation over-counting
// ---------------------------------------------------------------------------

/// `/api/operations` over-counts remote operations by a factor of two.
///
/// Both peers dial each other, so two TCP sessions exist between any pair and
/// every remote operation is delivered — and appended to the network layer's
/// display-only operation log — twice. The CRDT state is unaffected, which is
/// why every scenario above uses `/api/state` as its oracle.
///
/// This test asserts the *correct* count and therefore fails today. It is kept
/// `#[ignore]`d as an executable description of the defect: when the duplicate
/// dial is fixed, un-ignoring it is the regression test.
#[test]
#[ignore = "known defect: double-dial makes /api/operations over-count remote ops 2x"]
fn opcount_double_delivery() {
    let Some(cluster) = started_pair("op-count") else {
        return;
    };
    let (a, b) = (cluster.node("a"), cluster.node("b"));

    apply_ok(a, ops::object_update("name", ops::string_insert('B', 0)));
    apply_ok(b, ops::object_update("city", ops::string_insert('P', 0)));
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    for node in cluster.nodes() {
        let ops = get_json(node, "/api/operations").unwrap();
        let count = ops
            .get("operations")
            .and_then(Value::as_array)
            .map_or_else(|| ops.as_array().map_or(0, Vec::len), Vec::len);
        assert_eq!(
            count,
            2,
            "`{}` logged {count} operations; exactly two were submitted \
             cluster-wide, so anything above two is the double-dial defect",
            node.id()
        );
    }
}
