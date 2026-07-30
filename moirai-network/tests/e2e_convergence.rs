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
//! - **testcontainers** — one container per replica, with replication traffic
//!   on a network of its own so it can be cut for real. Needed by S4, T5 and
//!   C1–C4. Build the image first, and note that the *test process* talks to
//!   the Docker socket, so it must itself carry the `docker` group:
//!
//!   ```bash
//!   docker build -f moirai/docker/e2e/Dockerfile -t moirai-json-crdt:test .
//!   sg docker -c "cargo test -p moirai-network --test e2e_convergence -- --test-threads=1"
//!   ```
//!
//! `MOIRAI_E2E_BACKEND` selects: `testcontainers` (the default) or `process`.
//! Under `process`, the scenarios that need a real partition skip rather than
//! silently degrading to the in-process `pause` flag, which is a weaker claim
//! wearing the same name.
//!
//! A third backend, driving `docker run` through the CLI, was removed once
//! testcontainers covered it. It is worth saying why rather than leaving the
//! absence to be rediscovered: its cleanup depended on the test process exiting
//! tidily, so a panic between `docker run` and the end of a scenario leaked a
//! container and two networks, and the next run collided with them.
//!
//! # Which scenarios run where, and why
//!
//! - The **discovery** scenarios (E1–E3, T2–T4, J1–J5) run on processes,
//!   because they need a bootnode and a container cannot reach the test host's
//!   loopback. They do not need a partition, so nothing is lost.
//! - The **partition** scenarios (S4, T5, C1–C4) and the **randomised** one
//!   (R1) run on containers. R1 is there because it is the scenario CI leans on
//!   most and it should exercise the backend the shipped image runs under.
//!
//! A scenario whose backend is unavailable — no node binary, no daemon, no
//! image, or a backend the environment has opted out of — prints
//! `E2E-SKIP <scenario>: <why>` and returns green rather than failing, so the
//! suite stays runnable on a laptop with neither. Run with `--nocapture` to see
//! the notice; CI greps for that marker and fails the job, because a silently
//! skipped suite is worse than a red one.
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
use serde_json::{json, Value};

// The operation builders and the seeded generator live in the library, because
// the dashboard's `--random` driver needs exactly the same ones and a second
// copy would drift. `ops` is re-exported under its old name so the scenarios
// below read as they did.
use moirai_network::workload::{ops, state_digest, Workload};

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
// Container backend, on testcontainers
// ---------------------------------------------------------------------------
//
// `testcontainers` owns the container lifecycle: create, wait for readiness,
// map ports, read logs, and — the reason it is here — remove on drop. The
// previous backend shelled out to `docker run` and relied on the test process
// exiting tidily, so a panic between `docker run` and the end of the scenario
// left a container and two networks behind. A `Container` cleans itself up as
// part of unwinding, and the crate's session reaper covers the case where the
// process does not unwind at all.
//
// # Two networks, on purpose, and the one thing testcontainers cannot do
//
// Each replica joins two user-defined networks:
//
// - a **control** network, joined at creation, which carries the published
//   HTTP port the test process drives the replica through;
// - a **replication** network, joined afterwards, which carries peer-to-peer
//   sync traffic and is the only network whose alias appears in `PEERS`.
//
// The split is what makes a real partition testable. Measured on this host,
// not assumed: a container on a single user-defined network with `-p
// 127.0.0.1::8081` published stops answering that port entirely the moment it
// is disconnected — `curl` gets connection refused. So cutting the only
// network also cuts the test process off from the replica it just partitioned,
// and nothing can be asserted about it. Keeping the control plane on a second
// network that is never cut leaves the replica observable and writable while
// it is genuinely severed from its peers.
//
// Peers are addressed by a replication-network **alias** rather than by the
// container name, because Docker registers the container name for DNS on every
// network it joins — including the control network. Using the alias guarantees
// that peer traffic has no route once the replication network is cut.
//
// `testcontainers` attaches a container to exactly one network and exposes no
// primitive for a second, so the replication network is administered through
// `bollard` — which is not a new dependency but the Docker client
// `testcontainers` is itself built on. That is four calls (`create`,
// `connect`, `disconnect`, `remove`) against the Docker API, not a second
// container lifecycle.

use bollard::models::{
    EndpointSettings, NetworkConnectRequest, NetworkCreateRequest, NetworkDisconnectRequest,
};
use bollard::Docker;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

/// Ports inside a replica container. Fixed and private: only HTTP is published,
/// and peers reach each other by alias on the replication network.
const CONTAINER_LISTEN_PORT: u16 = 9001;
const CONTAINER_HTTP_PORT: u16 = 8081;

/// The line `network_node` prints once its HTTP API is bound. Readiness is
/// still asserted by polling `/api/health`; this only avoids handing back a
/// container whose port mapping is not published yet.
const READY_LINE: &str = "HTTP API listening on";

/// Blocking wrapper around the handful of Docker network calls the harness
/// needs. Its own current-thread runtime, so every method is a plain function
/// and the harness stays synchronous.
struct Networks {
    runtime: tokio::runtime::Runtime,
    docker: Docker,
}

impl Networks {
    fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build the runtime the Docker client needs")?;
        let docker = Docker::connect_with_local_defaults().context(
            "the Docker daemon is not reachable; if the account was only just \
             added to the `docker` group, run the tests under \
             `sg docker -c \"cargo test ...\"` so the test process inherits it",
        )?;
        runtime
            .block_on(docker.version())
            .context("the Docker daemon did not answer")?;
        Ok(Self { runtime, docker })
    }

    fn create(&self, name: &str) -> Result<()> {
        self.runtime
            .block_on(self.docker.create_network(NetworkCreateRequest {
                name: name.to_string(),
                ..Default::default()
            }))
            .with_context(|| format!("create network {name}"))?;
        Ok(())
    }

    fn remove(&self, name: &str) -> Result<()> {
        self.runtime
            .block_on(self.docker.remove_network(name))
            .with_context(|| format!("remove network {name}"))
    }

    fn connect(&self, network: &str, container: &str, alias: &str) -> Result<()> {
        self.runtime
            .block_on(self.docker.connect_network(
                network,
                NetworkConnectRequest {
                    container: container.to_string(),
                    endpoint_config: Some(EndpointSettings {
                        aliases: Some(vec![alias.to_string()]),
                        ..Default::default()
                    }),
                },
            ))
            .with_context(|| format!("attach {container} to {network} as {alias}"))
    }

    fn disconnect(&self, network: &str, container: &str) -> Result<()> {
        self.runtime
            .block_on(self.docker.disconnect_network(
                network,
                NetworkDisconnectRequest {
                    container: container.to_string(),
                    force: Some(true),
                },
            ))
            .with_context(|| format!("detach {container} from {network}"))
    }
}

/// Runs replicas as containers, with replication traffic isolated on its own
/// network so that it can be cut for real.
struct ContainerBackend {
    image: String,
    networks: Networks,
    /// Never cut: carries the published HTTP port.
    control_net: String,
    /// Cut and restored by [`Backend::cut_network`] / [`Backend::restore_network`].
    replication_net: String,
    /// `replica id -> (replication alias, container id once started)`.
    reserved: BTreeMap<String, (String, Option<String>)>,
}

impl ContainerBackend {
    /// Returns the backend, or the reason the scenario should be skipped.
    fn new() -> Result<Self> {
        let networks = Networks::new()?;

        let image =
            std::env::var("MOIRAI_E2E_IMAGE").unwrap_or_else(|_| "moirai-json-crdt:test".into());
        // `testcontainers` would happily try to pull a missing image from a
        // registry it is not in. Failing here instead turns a twenty-minute
        // timeout into a one-line skip with the build command in it.
        networks
            .runtime
            .block_on(networks.docker.inspect_image(&image))
            .with_context(|| {
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
        networks.create(&control_net)?;
        networks.create(&replication_net)?;

        Ok(Self {
            image,
            networks,
            control_net,
            replication_net,
            reserved: BTreeMap::new(),
        })
    }

    fn container_of(&self, id: &str) -> Result<String> {
        self.reserved
            .get(id)
            .and_then(|(_, container)| container.clone())
            .ok_or_else(|| anyhow!("replica `{id}` is not running"))
    }

    fn alias_of(&self, id: &str) -> Result<String> {
        self.reserved
            .get(id)
            .map(|(alias, _)| alias.clone())
            .ok_or_else(|| anyhow!("no endpoint reserved for replica `{id}`"))
    }
}

impl Backend for ContainerBackend {
    fn name(&self) -> &'static str {
        "testcontainers"
    }

    fn reserve(&mut self, id: &str) -> Result<Endpoint> {
        let alias = format!("{}-node-{id}", self.replication_net);
        self.reserved.insert(id.to_string(), (alias.clone(), None));
        Ok(Endpoint {
            sync_addr: format!("{alias}:{CONTAINER_LISTEN_PORT}"),
            listen_port: CONTAINER_LISTEN_PORT,
            http_port: CONTAINER_HTTP_PORT,
        })
    }

    fn start(
        &mut self,
        id: &str,
        endpoint: &Endpoint,
        peers: &[(String, String)],
        extra_env: &[(String, String)],
    ) -> Result<Box<dyn Node>> {
        let alias = self.alias_of(id)?;
        let peers_env = peers
            .iter()
            .map(|(peer, addr)| format!("{peer}:{addr}"))
            .collect::<Vec<_>>()
            .join(",");

        let (name, tag) = match self.image.rsplit_once(':') {
            Some((name, tag)) => (name.to_string(), tag.to_string()),
            None => (self.image.clone(), "latest".to_string()),
        };
        let mut request = GenericImage::new(name, tag)
            .with_exposed_port(endpoint.http_port.tcp())
            .with_wait_for(WaitFor::message_on_stderr(READY_LINE))
            // The control network is joined at creation, which is what binds
            // the published port; it is never disconnected.
            .with_network(&self.control_net)
            .with_env_var("REPLICA_ID", id)
            .with_env_var("LISTEN_PORT", endpoint.listen_port.to_string())
            .with_env_var("HTTP_PORT", endpoint.http_port.to_string())
            .with_env_var("PEERS", &peers_env);
        for (key, value) in extra_env {
            request = request.with_env_var(key, value);
        }

        let container = request
            .start()
            .with_context(|| format!("start a container for replica `{id}`"))?;
        let container_id = container.id().to_string();

        // Join the replication network only now, so that disconnecting it
        // later leaves the control network — and the published port — intact.
        self.networks
            .connect(&self.replication_net, &container_id, &alias)?;
        if let Some(entry) = self.reserved.get_mut(id) {
            entry.1 = Some(container_id);
        }

        let host_port = container
            .get_host_port_ipv4(endpoint.http_port.tcp())
            .with_context(|| format!("read the published HTTP port of replica `{id}`"))?;

        Ok(Box::new(ContainerNode {
            id: id.to_string(),
            http_base: format!("http://127.0.0.1:{host_port}"),
            container,
        }))
    }

    fn cut_network(&mut self, id: &str) -> Result<()> {
        let container = self.container_of(id)?;
        self.networks.disconnect(&self.replication_net, &container)
    }

    fn restore_network(&mut self, id: &str) -> Result<()> {
        let container = self.container_of(id)?;
        let alias = self.alias_of(id)?;
        self.networks
            .connect(&self.replication_net, &container, &alias)
    }
}

impl Drop for ContainerBackend {
    fn drop(&mut self) {
        // The containers remove themselves in their own `Drop`, which runs
        // first because `Cluster` declares them before the backend; a network
        // cannot be removed while an endpoint is attached.
        let _ = self.networks.remove(&self.control_net);
        let _ = self.networks.remove(&self.replication_net);
    }
}

/// A replica running as a container. Removed when this is dropped, including
/// while a panic is unwinding — which is the difference from the `docker run`
/// backend this replaced.
struct ContainerNode {
    id: String,
    http_base: String,
    container: Container<GenericImage>,
}

impl Node for ContainerNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn http_base(&self) -> &str {
        &self.http_base
    }

    fn log_tail(&self, lines: usize) -> String {
        // `network_node` logs to stderr, including the connection error every
        // first-started replica emits when its single dial finds nobody
        // listening yet. That is expected; see the header's readiness rule.
        let text = self
            .container
            .stderr_to_vec()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|e| format!("<{e}>"));
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
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

    /// Starts `id` knowing only the replicas named in `peers`.
    ///
    /// [`Cluster::start`] hands a replica the whole roster, running or not,
    /// which is right for scenarios where the membership is fixed up front. It
    /// is wrong wherever the stable frontier has to move before a joiner
    /// arrives: a member that is listed and never speaks pins the column-wise
    /// minimum at zero, so nothing is ever compacted — the E4 phenomenon,
    /// arrived at by accident. This lets a scenario introduce the joiner to the
    /// session without introducing the session to the joiner.
    fn start_with_peers(&mut self, id: &str, peers: &[&str]) -> Result<()> {
        let endpoint = self
            .endpoints
            .get(id)
            .ok_or_else(|| anyhow!("replica `{id}` is not part of this cluster"))?
            .clone();
        let peer_list: Pairs = peers
            .iter()
            .map(|other| {
                (
                    (*other).to_string(),
                    self.endpoints[*other].sync_addr.clone(),
                )
            })
            .collect();
        let node = self
            .backend
            .start(id, &endpoint, &peer_list, &[])
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

    /// Stops `id`, keeping its reserved endpoint so it can be started again.
    ///
    /// The counterpart `Cluster` lacked, and the reason S7 says it lacked one:
    /// a restarted *process* comes back empty, because there is no persistence.
    /// That is exactly the shape the join scenarios want — J3 and J4 are about
    /// a member that leaves and returns as a fresh replica under a familiar
    /// name, which is a membership question and not a durability one.
    fn stop(&mut self, id: &str) -> Result<()> {
        self.running
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| anyhow!("replica `{id}` is not running"))
    }

    fn is_running(&self, id: &str) -> bool {
        self.running.contains_key(id)
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

/// Which backend a scenario that needs containers is allowed to use.
///
/// The env var exists so a developer without a daemon, or a CI job deliberately
/// running the cheap half, can say so once instead of filtering test names. It
/// does **not** let a partition scenario fall back to processes: the process
/// backend cannot cut a network, and quietly substituting the in-process
/// `pause` flag would turn "no route exists" into "outbound messages are
/// buffered" while the test kept its name.
fn container_backend_selected() -> std::result::Result<(), String> {
    match std::env::var("MOIRAI_E2E_BACKEND").as_deref() {
        Ok("testcontainers") | Err(_) => Ok(()),
        Ok("process") => Err(
            "MOIRAI_E2E_BACKEND=process, and this scenario needs a real network \
             partition, which only the container backend can produce"
                .to_string(),
        ),
        Ok("docker") => Err(
            "MOIRAI_E2E_BACKEND=docker: the `docker` CLI backend was removed once \
             testcontainers covered it — it leaked a container and two networks \
             on any panic. Use `testcontainers`"
                .to_string(),
        ),
        Ok(other) => Err(format!(
            "MOIRAI_E2E_BACKEND=`{other}` is not a backend; use `testcontainers` \
             or `process`"
        )),
    }
}

/// Same, for the container backend. An unreachable daemon or a missing replica
/// image is a skip, with the command needed to fix it in the message.
fn container_cluster(scenario: &str, ids: &[&str]) -> Option<Cluster> {
    if let Err(why) = container_backend_selected() {
        eprintln!("\nE2E-SKIP {scenario}: {why}");
        return None;
    }
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

/// **T3** — a joiner writes immediately after adopting.
///
/// The second of the two traps in the design. The joiner installs every other
/// replica's column at the donor's values while its own starts at zero, so its
/// first operation has to come out causally *after* the state it just adopted.
/// If it did not, the operation would be concurrent with history the joiner has
/// already folded into its state, and the session would converge on something
/// no replica ever intended.
///
/// Asserted through the only honest oracle available from outside: the write
/// lands on every replica *and* the pre-existing state survives it. A causally
/// misplaced insert would either be dropped as redundant or would reorder the
/// string it was inserted into.
#[test]
fn t3_a_joiner_writes_immediately_after_adopting() {
    let Some(mut cluster) = discovered_cluster("T3", &["a", "b"]) else {
        return;
    };
    cluster.start("a").expect("T3: start a");
    for pos in 0..6 {
        apply_ok(
            cluster.node("a"),
            ops::object_update("name", ops::string_insert('x', pos)),
        );
    }
    apply_ok(
        cluster.node("a"),
        ops::object_update("age", ops::number_inc(4.0)),
    );

    cluster.start("b").expect("T3: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T3: mesh");

    // Wait for the joiner to hold the history, then write from it at once.
    let b = cluster.node("b");
    poll_until(CONVERGE_TIMEOUT, || {
        Ok((metric(b, "delivered_ops")? == 7).then_some(()))
    })
    .unwrap_or_else(|e| {
        panic!(
            "the joiner never took the history{}; metrics: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(b)
        )
    });
    apply_ok(b, ops::object_update("age", ops::number_inc(3.0)));
    apply_ok(b, ops::object_update("name", ops::string_insert('z', 6)));

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        read_number(&state, "age").unwrap(),
        7.0,
        "the joiner's increment did not compose with the one it adopted; state: {state}"
    );
    assert_eq!(
        read_string(&state, "name").unwrap(),
        "xxxxxxz",
        "the joiner's insert is not causally after the state it adopted; state: {state}"
    );
}

/// **T2** — a joiner arrives at a busy four-replica session.
///
/// E2 stops writing before the joiner starts, which makes the transfer a still
/// photograph. Here the session keeps moving across it: operations are applied
/// while the joiner is adopting, so the snapshot it installs is stale the
/// moment it lands and the events above it have to close the gap.
///
/// The count assertion is the one that matters. Convergence on a value would
/// also hold if an operation were applied twice — the string CRDT would simply
/// show an extra character — so the exact rendered string is asserted, not just
/// agreement.
#[test]
fn t2_a_joiner_arrives_at_a_busy_session() {
    let Some(mut cluster) = discovered_cluster("T2", &["a", "b", "c", "d"]) else {
        return;
    };
    for id in ["a", "b", "c"] {
        cluster
            .start(id)
            .unwrap_or_else(|e| panic!("T2: start {id}: {e:#}"));
    }
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T2: initial mesh");

    // Every replica writes: a stable frontier is a column-wise minimum, so one
    // member that never contributes pins it at zero and nothing is compacted.
    for round in 0..4 {
        for id in ["a", "b", "c"] {
            apply_ok(
                cluster.node(id),
                ops::object_update(id, ops::string_insert('x', round)),
            );
        }
    }
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    cluster.start("d").expect("T2: start d");
    // Keep the session moving *while* the joiner is catching up.
    for round in 4..7 {
        for id in ["a", "b", "c"] {
            apply_ok(
                cluster.node(id),
                ops::object_update(id, ops::string_insert('x', round)),
            );
        }
    }
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T2: mesh after d joined");

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    for id in ["a", "b", "c"] {
        assert_eq!(
            read_string(&state, id).unwrap(),
            "xxxxxxx",
            "`{id}` should hold exactly seven characters; anything longer is an operation \
             applied twice, anything shorter is one lost. state: {state}"
        );
    }
    let d = cluster.node("d");
    assert_eq!(
        metric(d, "delivered_ops").unwrap(),
        21,
        "the joiner rendered the right state but does not account for the history behind it; \
         metrics: {:?}",
        metrics_of(d)
    );
}

/// **T4** — two joiners arrive at once, and not from the same donor.
///
/// Each of them asks every connected peer, so each receives more than one
/// answer and has to discard all but the first: `adopt` replaces rather than
/// merges, and a second snapshot installed after the first would roll the
/// replica back. They also adopt *different* index orderings — a joiner takes
/// over its donor's — which is the case most likely to corrupt silently.
#[test]
fn t4_two_joiners_arrive_together() {
    let Some(mut cluster) = discovered_cluster("T4", &["a", "b", "c", "d"]) else {
        return;
    };
    cluster.start("a").expect("T4: start a");
    cluster.start("b").expect("T4: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T4: initial mesh");

    for pos in 0..5 {
        apply_ok(
            cluster.node("a"),
            ops::object_update("name", ops::string_insert('x', pos)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("age", ops::number_inc(1.0)),
        );
    }
    let before = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    cluster.start("c").expect("T4: start c");
    cluster.start("d").expect("T4: start d");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T4: mesh after the joiners");

    let after = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        after, before,
        "the joiners did not reach the existing state"
    );
    for id in ["c", "d"] {
        let node = cluster.node(id);
        assert_eq!(
            metric(node, "delivered_ops").unwrap(),
            10,
            "`{id}` does not account for the history it adopted; metrics: {:?}",
            metrics_of(node)
        );
    }
}

/// **T5** — a donor is severed as the joiner arrives.
///
/// The joiner asks every connected peer, so losing one of them has to cost a
/// round trip and nothing else. What must *not* happen is a half-installed
/// state: `adopt` replaces the log and the causal bookkeeping together, and a
/// joiner left holding one without the other would reject the operations it is
/// missing as duplicates, for ever.
///
/// The severing is real — `docker network disconnect`, so packets between `a`
/// and the others have no route — which is why this is the one phase-2 scenario
/// that needs the container backend. It is also why it uses `PEERS` rather than
/// the directory: a container cannot reach a bootnode on the test host's
/// loopback.
///
/// Honest about what it does not control: the cut lands as the joiner starts
/// dialling, and whether that is before or after `a`'s first answer is a race.
/// So this is "a donor disappears around the time of the transfer", not "at a
/// chosen instant inside it" — which would need a fault-injection point in the
/// transport that does not exist. Both outcomes of the race are covered by the
/// same assertion, because `b` can serve the transfer just as well.
#[test]
fn t5_a_severed_donor_does_not_strand_the_joiner() {
    const APPLIED: u64 = 12;

    let Some(mut cluster) = container_cluster("T5", &["a", "b", "c"]) else {
        return;
    };
    // `a` and `b` are told about each other and nothing else. Listing the
    // joiner before it exists would pin the stable frontier at zero and the
    // precondition below could never hold.
    cluster.start_with_peers("a", &["b"]).expect("T5: start a");
    cluster.start_with_peers("b", &["a"]).expect("T5: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T5: initial mesh");

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

    // Same precondition as E2: without a stable frontier that has moved, the
    // joiner could reach this state by plain replay and the scenario would be
    // testing nothing.
    let (a, b) = (cluster.node("a"), cluster.node("b"));
    poll_until(CONVERGE_TIMEOUT, || {
        let stable = metric(a, "stable_prefix")?.min(metric(b, "stable_prefix")?);
        Ok((stable >= APPLIED - 4).then_some(()))
    })
    .unwrap_or_else(|e| {
        panic!(
            "the stable frontier never advanced{}; a: {:?}, b: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(a),
            metrics_of(b),
        )
    });

    cluster
        .start_with_peers("c", &["a", "b"])
        .expect("T5: start c");
    cluster.cut_network("a").expect("T5: sever a");

    // `a` is gone; the joiner has to reach the same state through `b`.
    let survivors = [cluster.node("b"), cluster.node("c")];
    let after = assert_converged(&survivors, CONVERGE_TIMEOUT);
    assert_eq!(
        after, before,
        "the joiner did not reach the state the session held before it arrived"
    );
    let c = cluster.node("c");
    assert_eq!(
        metric(c, "delivered_ops").unwrap(),
        APPLIED,
        "the joiner rendered the right state but does not account for the history behind it, \
         which is what a half-installed transfer would look like; metrics: {:?}",
        metrics_of(c)
    );
}

/// **T6** — a `StateRequest` from a replica the donor already has history from
/// is refused.
///
/// The guard that keeps returning-member merge out of this phase rather than
/// letting it half-happen. A returning member — evicted, or long partitioned —
/// may hold operations the session has never seen, and adopting a snapshot is a
/// replace, not a merge, so serving one would discard them silently.
///
/// No running replica can reach this state on its own: a replica with history
/// asks for a delta, never for a transfer. So the request is made by the test,
/// speaking the replication protocol directly over TCP — newline-delimited
/// `TransportMessage` JSON, which is the real wire format and not a stand-in.
/// Both branches are exercised against the same donor: a fresh id is served, a
/// known one is refused.
#[test]
fn t6_a_returning_member_is_refused_a_state_transfer() {
    let Some(mut cluster) = process_cluster("T6", &["a", "b"]) else {
        return;
    };
    cluster.start_all().expect("T6: start cluster");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("T6: mesh");

    apply_ok(
        cluster.node("a"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    apply_ok(
        cluster.node("b"),
        ops::object_update("city", ops::string_insert('P', 0)),
    );
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // `a` has delivered an operation originated by `b`, and none from `zz`.
    let addr = cluster.endpoints["a"].sync_addr.clone();
    let fresh = ask_for_state(&addr, "zz").expect("T6: ask as a stranger");
    assert_eq!(
        fresh, "StateResponse",
        "the donor refused a genuinely fresh joiner, so the refusal below proves nothing"
    );
    let returning = ask_for_state(&addr, "b").expect("T6: ask as a returning member");
    assert_eq!(
        returning, "StateUnavailable",
        "the donor served a snapshot to a replica it already has history from; that would \
         silently discard whatever the returning member did while it was away"
    );
}

/// Opens a replication connection to `addr`, introduces itself as `id`, asks for
/// a state transfer and returns the `type` of the answer.
///
/// Deliberately low-level. The HTTP API has no way to send a `StateRequest`,
/// and adding one would mean testing an endpoint that exists only for the test.
fn ask_for_state(addr: &str, id: &str) -> Result<String> {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;

    let mut stream = TcpStream::connect(addr).with_context(|| format!("connect to {addr}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    writeln!(
        stream,
        "{}",
        json!({ "type": "Hello", "id": id, "metadata": null })
    )?;
    writeln!(stream, "{}", json!({ "type": "StateRequest", "id": id }))?;
    stream.flush()?;

    let reader = BufReader::new(stream.try_clone()?);
    for line in reader.lines() {
        let line = line.context("read the donor's reply")?;
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("the donor sent a non-JSON line: {line}"))?;
        match value.get("type").and_then(Value::as_str) {
            Some("StateResponse") => return Ok("StateResponse".to_string()),
            Some("StateUnavailable") => return Ok("StateUnavailable".to_string()),
            // The donor answers a `Hello` with a sync request of its own, and
            // may push a batch; neither is the answer being waited for.
            _ => continue,
        }
    }
    bail!("the donor closed the connection without answering the state request")
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
// C1–C4 — automatic conflict resolution across a genuine partition
// ---------------------------------------------------------------------------
//
// S1–S5 established these shapes on two replicas, mostly behind the in-process
// `pause` flag. These are their three-replica, real-network counterparts: `c`
// is disconnected with a Docker network operation, so while the operations are
// applied there is no route at all between the two sides. That difference
// matters for exactly one reason — the pause flag buffers *outbound* messages
// inside the replica, so a defect that only shows when a message is genuinely
// lost rather than delayed cannot appear behind it.

/// Reserves, starts and meshes a container-backed cluster. `None` when the
/// scenario must be skipped.
fn started_container_cluster(scenario: &str, ids: &[&str]) -> Option<Cluster> {
    let mut cluster = container_cluster(scenario, ids)?;
    cluster
        .start_all()
        .unwrap_or_else(|e| panic!("{scenario}: start containers: {e:#}"));
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).unwrap_or_else(|e| panic!("{scenario}: {e:#}"));
    Some(cluster)
}

/// **C1** — three replicas, concurrent updates to *different* keys.
///
/// The base case, and the one that would still pass under almost any merge
/// strategy: disjoint keys cannot conflict. It is here as the positive control
/// for C2–C4 — if C1 failed, the partition machinery rather than the resolution
/// policy would be the thing under suspicion.
#[test]
fn c1_disjoint_keys_across_a_partition() {
    let Some(mut cluster) = started_container_cluster("C1", &["a", "b", "c"]) else {
        return;
    };
    cluster.cut_network("c").expect("C1: sever c");

    apply_ok(
        cluster.node("a"),
        ops::object_update("alpha", ops::string_insert('A', 0)),
    );
    apply_ok(
        cluster.node("b"),
        ops::object_update("beta", ops::string_insert('B', 0)),
    );
    apply_ok(
        cluster.node("c"),
        ops::object_update("gamma", ops::string_insert('C', 0)),
    );
    assert_diverged(cluster.node("a"), cluster.node("c"), CONVERGE_TIMEOUT);

    cluster.restore_network("c").expect("C1: reconnect c");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("C1: mesh after heal");

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&state, "alpha").unwrap(), "A", "state: {state}");
    assert_eq!(read_string(&state, "beta").unwrap(), "B", "state: {state}");
    assert_eq!(read_string(&state, "gamma").unwrap(), "C", "state: {state}");
}

/// **C2** — three replicas, concurrent updates to the *same* key.
///
/// Counter increments commute, so the only correct healed value is the sum. A
/// single scalar is the cleanest convergence oracle there is: a store that
/// dropped one side's write would still "converge", on 5 or on 8, and the
/// assertion catches it.
///
/// Distinct from S2, which is two replicas behind the in-process pause flag.
/// Here the third replica's increment crosses a link that does not exist while
/// it is applied.
#[test]
fn c2_same_key_concurrent_updates_converge() {
    let Some(mut cluster) = started_container_cluster("C2", &["a", "b", "c"]) else {
        return;
    };
    // A common ancestor, so every replica has the key before the split.
    apply_ok(
        cluster.node("a"),
        ops::object_update("gamma", ops::number_inc(1.0)),
    );
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    cluster.cut_network("c").expect("C2: sever c");
    apply_ok(
        cluster.node("a"),
        ops::object_update("gamma", ops::number_inc(2.0)),
    );
    apply_ok(
        cluster.node("b"),
        ops::object_update("gamma", ops::number_inc(4.0)),
    );
    apply_ok(
        cluster.node("c"),
        ops::object_update("gamma", ops::number_inc(8.0)),
    );
    assert_diverged(cluster.node("a"), cluster.node("c"), CONVERGE_TIMEOUT);

    cluster.restore_network("c").expect("C2: reconnect c");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("C2: mesh after heal");

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        read_number(&state, "gamma").unwrap(),
        15.0,
        "increments commute, so every one of them must survive; state: {state}"
    );
}

/// **C3** — concurrent insert at the *same* list position.
///
/// The no-silent-loss assertion, and the one a last-writer-wins store fails
/// while still converging. All three replicas insert a different character at
/// position 0 of the same string while `c` has no route to the others. After
/// healing every character must be present, exactly once, and — the part that
/// makes it a sequence CRDT rather than a set — in the *same order* on every
/// replica.
#[test]
fn c3_concurrent_inserts_at_the_same_position() {
    let Some(mut cluster) = started_container_cluster("C3", &["a", "b", "c"]) else {
        return;
    };
    apply_ok(
        cluster.node("a"),
        ops::object_update("alpha", ops::string_insert('.', 0)),
    );
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    cluster.cut_network("c").expect("C3: sever c");
    for (id, ch) in [("a", 'X'), ("b", 'Y'), ("c", 'Z')] {
        apply_ok(
            cluster.node(id),
            ops::object_update("alpha", ops::string_insert(ch, 0)),
        );
    }
    assert_diverged(cluster.node("a"), cluster.node("c"), CONVERGE_TIMEOUT);

    cluster.restore_network("c").expect("C3: reconnect c");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("C3: mesh after heal");

    // `assert_converged` compares parsed JSON, and the string CRDT renders as
    // an *array*, so agreement here already includes agreement on the order.
    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    let alpha = read_string(&state, "alpha").unwrap();
    for ch in ['X', 'Y', 'Z', '.'] {
        assert!(
            alpha.contains(ch),
            "insert `{ch}` was silently discarded: alpha = {alpha:?}"
        );
    }
    assert_eq!(
        alpha.len(),
        4,
        "an operation was applied twice or lost: alpha = {alpha:?}"
    );
}

/// **C4** — update versus concurrent remove, across a real partition.
///
/// Under an Update-Wins map a concurrent update survives the removal, and an
/// update the removal causally dominates does not. The precise expectation,
/// verified against a live cluster: `gamma` is incremented to 3 before the
/// split, so that increment is dominated and goes; the concurrent `+9` on the
/// severed replica survives; the healed value is 9 exactly.
///
/// This asserts a *policy*, not merely convergence — a last-writer-wins map
/// would converge just as happily on 0, and be wrong. It is also the resolution
/// the dashboard renders as a `superseded` chip, from the same bookkeeping.
#[test]
fn c4_update_wins_over_a_concurrent_remove() {
    let Some(mut cluster) = started_container_cluster("C4", &["a", "b", "c"]) else {
        return;
    };
    apply_ok(
        cluster.node("a"),
        ops::object_update("gamma", ops::number_inc(3.0)),
    );
    let seeded = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_number(&seeded, "gamma").unwrap(), 3.0);

    cluster.cut_network("c").expect("C4: sever c");
    // `c` updates; `a` removes. Neither can see the other.
    apply_ok(
        cluster.node("c"),
        ops::object_update("gamma", ops::number_inc(9.0)),
    );
    apply_ok(cluster.node("a"), ops::object_remove("gamma"));
    assert_diverged(cluster.node("a"), cluster.node("c"), CONVERGE_TIMEOUT);

    cluster.restore_network("c").expect("C4: reconnect c");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("C4: mesh after heal");

    let state = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        read_number(&state, "gamma").unwrap(),
        9.0,
        "update-wins violated: the concurrent increment must survive the remove, \
         and the causally-prior one must not; state: {state}"
    );
}

// ---------------------------------------------------------------------------
// J1–J4 — joining, leaving, and coming back
// ---------------------------------------------------------------------------
//
// These run on the process backend with a process bootnode, for the reason
// recorded on `Bootnode`: a container cannot reach the test host's loopback.
// None of them needs a real partition — a member that has left is not a member
// that is unreachable — so nothing is lost by it, and the scenarios stay fast
// enough to run a churn loop.

/// Waits until every running replica reports `expected` known replicas.
///
/// The causal member set, not the directory: a replica is a member as soon as
/// anything it originated has been delivered, and leaving the directory does
/// not remove it. Separating those two is the whole subject of J2.
fn await_known_replicas(nodes: &[&dyn Node], expected: u64, timeout: Duration) -> Result<()> {
    poll_until(timeout, || {
        for node in nodes {
            if metric(*node, "known_replicas")? != expected {
                return Ok(None);
            }
        }
        Ok(Some(()))
    })
    .map_err(|e| {
        anyhow!(
            "not every replica reached {expected} known replicas{}: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            nodes
                .iter()
                .map(|n| (n.id().to_string(), metrics_of(*n).ok()))
                .collect::<Vec<_>>()
        )
    })
}

/// Waits until `node` holds a rendered state at all, i.e. it is no longer
/// `"Unset"`.
///
/// The honest "has this joiner received anything yet" gate. `delivered_ops`
/// would do as well, but a joiner that adopted a snapshot has delivered nothing
/// in its own reckoning until the delta sync behind it lands, and this is the
/// question every caller actually means.
fn await_state(node: &dyn Node, timeout: Duration) -> Result<Value> {
    poll_until(timeout, || {
        let state = state_of(node)?;
        Ok((!moirai_network::workload::is_unset(&state)).then_some(state))
    })
    .map_err(|e| {
        anyhow!(
            "`{}` still reports no state at all{}",
            node.id(),
            e.map(|e| format!(" ({e:#})")).unwrap_or_default()
        )
    })
}

/// **J1** — a replica joins a running session and becomes a full member.
///
/// E2 and T2 already assert that a joiner *catches up*. What neither asserts is
/// the property that makes it a member rather than an observer: once it has
/// joined, the session's causal stability has to keep advancing *through* it.
/// A joiner that received the history but never had its own column recognised
/// would read correctly and silently freeze compaction for everybody — the E4
/// failure, arrived at through the join path instead of through a severed link.
///
/// So the assertion is on `stable_prefix` after the join, measured against the
/// value before it, with every member writing. Every member has to write: the
/// stable frontier is a column-wise minimum, and a member that has never sent
/// anything pins it regardless of whether it joined or was there all along.
#[test]
fn j1_a_joiner_becomes_a_full_member() {
    let Some(mut cluster) = discovered_cluster("J1", &["a", "b", "c"]) else {
        return;
    };
    cluster.start("a").expect("J1: start a");
    cluster.start("b").expect("J1: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J1: initial mesh");

    for round in 0..3 {
        apply_ok(
            cluster.node("a"),
            ops::object_update("alpha", ops::string_insert('a', round)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("beta", ops::string_insert('b', round)),
        );
    }
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // --- the joiner ---
    cluster.start("c").expect("J1: start c");
    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("J1: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J1: mesh after c joined");
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // Everybody, the joiner included, is in everybody's causal member set.
    await_known_replicas(&cluster.nodes(), 3, MESH_TIMEOUT).expect("J1: member set");

    let before = metric(cluster.node("a"), "stable_prefix").expect("J1: stable_prefix");

    // Every member writes, the joiner too.
    for round in 3..6 {
        for (id, key) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
            apply_ok(
                cluster.node(id),
                ops::object_update(key, ops::string_insert('x', round - 3)),
            );
        }
    }
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    poll_until(CONVERGE_TIMEOUT, || {
        Ok((metric(cluster.node("a"), "stable_prefix")? > before).then_some(()))
    })
    .unwrap_or_else(|e| {
        panic!(
            "causal stability did not advance past the join{}; the joiner is being \
             carried as a member that never acknowledges, which freezes compaction \
             for the whole session. before: {before}, now: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(cluster.node("a"))
        )
    });
}

/// **J2** — a graceful leave does *not* let causal stability advance past the
/// leaver.
///
/// **This is expected to fail**, and it is `#[ignore]`d as an executable report
/// of the gap phase 3 exists to close, in the same spirit as
/// `s7_restart_recovers_state`.
///
/// The plan asked for "node leaves gracefully; the rest continue and stability
/// advances past it". The first half holds and E3 already asserts it. The
/// second does not and cannot yet: `POST /api/leave` is a *directory*
/// departure. It stops the replica re-registering so nobody dials it again, and
/// it changes nothing about any peer's matrix clock — `stable_version` keeps a
/// column for the leaver, and the column-wise minimum keeps waiting on it
/// exactly as it would for a crash. E3 asserts that the column is still there;
/// this asserts what has to become true instead.
///
/// Un-ignoring this is the regression test for epoch-based eviction.
#[test]
#[ignore = "expected to fail: leaving the directory is not causal eviction (phase 3)"]
fn j2_stability_advances_past_a_departed_member() {
    let Some(mut cluster) = discovered_cluster("J2", &["a", "b", "c"]) else {
        return;
    };
    cluster.start_all().expect("J2: start cluster");
    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("J2: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J2: mesh");

    // Every member writes, so the frontier is moving before anyone leaves.
    for (id, key) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        apply_ok(
            cluster.node(id),
            ops::object_update(key, ops::number_inc(1.0)),
        );
    }
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    leave_session(cluster.node("c")).expect("J2: c leaves");
    await_roster(cluster.bootnode(), &["a", "b"], MESH_TIMEOUT).expect("J2: c left the directory");
    cluster.stop("c").expect("J2: stop c");

    // The survivors keep writing to each other.
    let (a, b) = (cluster.node("a"), cluster.node("b"));
    let frozen_at = metric(a, "stable_prefix").unwrap();
    for _ in 0..5 {
        apply_ok(a, ops::object_update("alpha", ops::number_inc(1.0)));
        apply_ok(b, ops::object_update("beta", ops::number_inc(1.0)));
    }
    assert_converged(&[a, b], CONVERGE_TIMEOUT);

    poll_until(Duration::from_secs(20), || {
        Ok((metric(a, "stable_prefix")? > frozen_at + 5).then_some(()))
    })
    .unwrap_or_else(|e| {
        panic!(
            "stable_prefix stayed at {frozen_at} after `c` left gracefully{}. A \
             departure that does not release the causal member set leaves every \
             survivor unable to compact anything; metrics: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(a)
        )
    });
}

/// **J3** — a member leaves, the session carries on, and it comes back.
///
/// It comes back under a **new id**, and that is the finding rather than a
/// convenience. Spec open question 2 asks whether a returning member needs a
/// fresh replica id; measured here, it does. A replica restarting under its old
/// name is refused a state transfer — `state_response_for` will not serve a
/// snapshot to a replica the donor already has history from, because adopting
/// replaces rather than merges and would discard whatever the returning member
/// did while away (T6) — and the delta sync it falls back to leaves it holding
/// nothing at all. That half is
/// `j3b_a_returning_member_under_its_old_id_is_stranded`, ignored as an
/// executable report.
///
/// What this asserts, all of it green:
///
/// 1. The survivors keep converging with a member gone.
/// 2. Their causal stability **freezes** while it is away, and their
///    replication buffer grows by exactly what they applied — the departed
///    member is still a column in every matrix clock, so the column-wise
///    minimum waits on it exactly as it would for a crash. E3 asserts the
///    column is still there; this measures what it costs.
/// 3. The returning replica, under a fresh id, reaches the state the session
///    held while it was away — **including** the operation the departed replica
///    originated before it left, which no longer exists anywhere as an
///    operation but is folded into what the survivors hold.
#[test]
fn j3_a_member_leaves_and_comes_back() {
    /// Applied before anyone leaves: one per replica.
    const BEFORE_DEPARTURE: u64 = 3;
    /// Applied by the survivors while the third replica is gone.
    const WHILE_AWAY: u64 = 12;

    let Some(mut cluster) = discovered_cluster("J3", &["a", "b", "c", "c-again"]) else {
        return;
    };
    for id in ["a", "b", "c"] {
        cluster
            .start(id)
            .unwrap_or_else(|e| panic!("J3: start {id}: {e:#}"));
    }
    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("J3: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J3: mesh");

    for (id, key) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        apply_ok(
            cluster.node(id),
            ops::object_update(key, ops::number_inc(1.0)),
        );
    }
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // --- the departure ---
    leave_session(cluster.node("c")).expect("J3: c leaves");
    await_roster(cluster.bootnode(), &["a", "b"], MESH_TIMEOUT).expect("J3: c left the directory");
    cluster.stop("c").expect("J3: stop c");

    // Deliberately *not* a sample taken here: causal stability lags the last
    // message by a round trip, so reading `stable_prefix` the instant `c` stops
    // catches it still rising and any equality assertion would be a race. What
    // can be stated exactly is the bound — three operations existed before `c`
    // left, so nothing above three can ever become stable once it is gone.
    for _ in 0..(WHILE_AWAY / 2) {
        apply_ok(
            cluster.node("a"),
            ops::object_update("alpha", ops::number_inc(1.0)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("beta", ops::number_inc(1.0)),
        );
    }
    let survivors_state =
        assert_converged(&[cluster.node("a"), cluster.node("b")], CONVERGE_TIMEOUT);

    // The absence is measurable, and it is what the phase-3 contribution has to
    // remove.
    let a = cluster.node("a");
    assert_eq!(
        metric(a, "delivered_ops").unwrap(),
        BEFORE_DEPARTURE + WHILE_AWAY,
        "`a` did not deliver what the scenario applied; metrics: {:?}",
        metrics_of(a)
    );
    assert!(
        metric(a, "stable_prefix").unwrap() <= BEFORE_DEPARTURE,
        "`c` is gone and cannot acknowledge, so nothing applied after it left may \
         become stable; metrics: {:?}",
        metrics_of(a)
    );
    assert!(
        metric(a, "retained_ops").unwrap() >= WHILE_AWAY,
        "every operation applied while `c` was away is unacknowledgeable and must \
         still be in the replication buffer; metrics: {:?}",
        metrics_of(a)
    );

    // --- the return, under a fresh id ---
    cluster
        .start("c-again")
        .expect("J3: start the returning member");
    await_roster(cluster.bootnode(), &["a", "b", "c-again"], MESH_TIMEOUT)
        .expect("J3: the returning member is in the directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J3: mesh after the return");

    let after = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        after, survivors_state,
        "the returning member did not reach the state the session held while it \
         was away"
    );
    assert_eq!(
        read_number(&after, "gamma").unwrap(),
        1.0,
        "the departed replica's own pre-departure write was lost; it exists \
         nowhere as an operation any more, so this is the state transfer being \
         load-bearing. state: {after}"
    );
}

/// **J3b** — a member that comes back under its *old* id is stranded.
///
/// **This is expected to fail**, and is ignored as an executable report of the
/// gap. It is J3 with one thing changed: the returning replica keeps its name.
///
/// Observed, in full, on the run that produced this test: every donor answers
/// `StateUnavailable` — "`c` is a returning member, not a fresh one; merging
/// its history with a snapshot is not implemented" — the replica falls back to
/// a delta sync, and its `/api/state` stays `"Unset"` indefinitely. Not
/// partially caught up. Nothing.
///
/// The refusal itself is deliberate and right (T6 asserts it): serving a
/// snapshot to a replica the donor has history from would silently discard
/// whatever that replica did while away. What is missing is the other branch —
/// a returning member needs a merge, or an eviction that makes it fresh again.
/// Both are phase 3.
///
/// So the answer to spec open question 2, "does a returning evicted member need
/// a fresh replica id", is currently **yes, it has no choice**. Un-ignoring
/// this is the regression test for whichever mechanism removes that constraint.
#[test]
#[ignore = "expected to fail: a returning member is refused a transfer and a delta sync gives it nothing (phase 3)"]
fn j3b_a_returning_member_under_its_old_id_is_stranded() {
    let Some(mut cluster) = discovered_cluster("J3b", &["a", "b", "c"]) else {
        return;
    };
    cluster.start_all().expect("J3b: start cluster");
    await_roster(cluster.bootnode(), &["a", "b", "c"], MESH_TIMEOUT).expect("J3b: directory");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J3b: mesh");

    for (id, key) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        apply_ok(
            cluster.node(id),
            ops::object_update(key, ops::number_inc(1.0)),
        );
    }
    assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    leave_session(cluster.node("c")).expect("J3b: c leaves");
    await_roster(cluster.bootnode(), &["a", "b"], MESH_TIMEOUT).expect("J3b: c left");
    cluster.stop("c").expect("J3b: stop c");

    for _ in 0..6 {
        apply_ok(
            cluster.node("a"),
            ops::object_update("alpha", ops::number_inc(1.0)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("beta", ops::number_inc(1.0)),
        );
    }
    let survivors_state =
        assert_converged(&[cluster.node("a"), cluster.node("b")], CONVERGE_TIMEOUT);

    // Same id as before.
    cluster.start("c").expect("J3b: restart c");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J3b: mesh after the return");

    let after = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        after, survivors_state,
        "a replica returning under its old id did not reach the session's state"
    );
}

/// **J4** — rolling churn while writes never stop.
///
/// One member at a time leaves and is replaced, three times round, with `a` and
/// `b` writing throughout. Nothing here is a new mechanism — it is J1 and J3
/// repeatedly and without the session ever going quiet, which is the case that
/// exposes ordering assumptions the one-shot scenarios cannot: a joiner
/// adopting while another member is mid-departure, a roster that is wrong for a
/// moment in both directions at once.
///
/// The assertion is deliberately the strong one — full convergence of every
/// replica still running at the end, including the last joiner — rather than a
/// per-round check, which would let the session limp between rounds and still
/// pass.
#[test]
fn j4_rolling_churn_while_writes_continue() {
    /// Replicas that come and go, one per round. Fresh ids rather than a reused
    /// one: J3 covers the return of a familiar name, and a rejoin under a new
    /// id is the other lifecycle, the one a fresh member has.
    const CHURNING: [&str; 3] = ["c1", "c2", "c3"];

    let Some(mut cluster) = discovered_cluster("J4", &["a", "b", "c1", "c2", "c3"]) else {
        return;
    };
    cluster.start("a").expect("J4: start a");
    cluster.start("b").expect("J4: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J4: initial mesh");

    let mut pos = 0usize;
    let mut previous: Option<&str> = None;
    for id in CHURNING {
        // Writes continue across the churn, not between bursts of it.
        for _ in 0..2 {
            apply_ok(
                cluster.node("a"),
                ops::object_update("alpha", ops::string_insert('a', pos)),
            );
            apply_ok(
                cluster.node("b"),
                ops::object_update("beta", ops::string_insert('b', pos)),
            );
            pos += 1;
        }

        cluster
            .start(id)
            .unwrap_or_else(|e| panic!("J4: start {id}: {e:#}"));
        // The previous joiner leaves only once its successor is up, so the
        // session is briefly four replicas and the roster is stale in both
        // directions at the same time.
        if let Some(leaving) = previous {
            leave_session(cluster.node(leaving))
                .unwrap_or_else(|e| panic!("J4: {leaving} leaves: {e:#}"));
            cluster
                .stop(leaving)
                .unwrap_or_else(|e| panic!("J4: stop {leaving}: {e:#}"));
        }
        previous = Some(id);

        // The joiner must hold the session's history *before* it writes.
        // Writing first is not a harness convenience being avoided — it is a
        // defect, and it has its own scenario: see
        // `j5_a_joiner_that_writes_before_adopting_is_stranded`. Until that is
        // fixed, a joiner here that wrote immediately would never receive a
        // state transfer and J4 would be reporting that defect rather than
        // testing churn.
        await_state(cluster.node(id), CONVERGE_TIMEOUT)
            .unwrap_or_else(|e| panic!("J4: {id} never received the session's state: {e:#}"));
        apply_ok(
            cluster.node(id),
            ops::object_update("gamma", ops::number_inc(1.0)),
        );
    }

    for _ in 0..2 {
        apply_ok(
            cluster.node("a"),
            ops::object_update("alpha", ops::string_insert('a', pos)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("beta", ops::string_insert('b', pos)),
        );
        pos += 1;
    }

    let survivors: Vec<&dyn Node> = cluster.nodes();
    assert_eq!(
        survivors.len(),
        3,
        "expected `a`, `b` and the last joiner to still be running; got {:?}",
        survivors.iter().map(|n| n.id()).collect::<Vec<_>>()
    );
    let state = assert_converged(&survivors, CONVERGE_TIMEOUT);
    assert_eq!(
        read_string(&state, "alpha").unwrap().len(),
        pos,
        "a write applied during churn was lost or duplicated; state: {state}"
    );
    assert_eq!(
        read_number(&state, "gamma").unwrap(),
        CHURNING.len() as f64,
        "every joiner's own write must survive its departure; state: {state}"
    );
    assert!(
        cluster.is_running("c3"),
        "the last joiner should still be running"
    );
}

/// **J5** — a joiner that writes before its state transfer arrives is
/// stranded, permanently.
///
/// **This is expected to fail.** It is an executable report of a defect J4
/// found, in the same spirit as `opcount_double_delivery` — the assertion is
/// the *correct* behaviour, so un-ignoring it is the regression test.
///
/// The mechanism, traced to two lines that are each right on their own:
///
/// - `retry_state_transfer` and `request_sync` both ask for a snapshot only
///   while `has_no_history()` — "this replica has delivered nothing".
/// - `adopt_state` re-checks the same condition before installing one, and
///   correctly so: `adopt` replaces rather than merges, and a second donor's
///   answer arriving after the first must not roll the replica back (T4).
///
/// A locally applied operation is a delivery. So a joiner that writes before a
/// donor has answered flips `has_no_history()` itself, and from then on it will
/// neither ask for a transfer nor accept one that is already in flight. It
/// falls back to a delta sync for ever — which replays only what its peers have
/// not compacted away, and in a healthy session that is almost nothing.
///
/// Measured on the four-replica churn scenario before it was made to wait: the
/// last joiner ended with `beta` one character long against the session's
/// eight, and no `alpha` at all, while every other replica agreed.
///
/// This is not the same as T3, which is a joiner writing immediately *after*
/// adopting and is green. The window here is before, and nothing closes it: the
/// replica cannot know a transfer is coming, and a client has no way to ask.
/// The fix is a protocol decision, not a harness one — either a joiner refuses
/// local writes until it has state, or `adopt` learns to merge — which is why
/// this is reported rather than patched.
#[test]
#[ignore = "known defect: a local write before the transfer lands makes the joiner ineligible for one, for ever"]
fn j5_a_joiner_that_writes_before_adopting_is_stranded() {
    /// Enough that the stable frontier passes most of it, so a delta sync
    /// cannot substitute for a transfer.
    const APPLIED: u64 = 20;

    let Some(mut cluster) = discovered_cluster("J5", &["a", "b", "c"]) else {
        return;
    };
    cluster.start("a").expect("J5: start a");
    cluster.start("b").expect("J5: start b");
    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J5: initial mesh");

    for pos in 0..(APPLIED / 2) {
        apply_ok(
            cluster.node("a"),
            ops::object_update("alpha", ops::string_insert('x', pos as usize)),
        );
        apply_ok(
            cluster.node("b"),
            ops::object_update("gamma", ops::number_inc(1.0)),
        );
    }
    let before = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);

    // The same precondition E2 uses: without a frontier that has moved, a delta
    // sync would carry the whole history and the scenario would prove nothing.
    let (a, b) = (cluster.node("a"), cluster.node("b"));
    poll_until(CONVERGE_TIMEOUT, || {
        let stable = metric(a, "stable_prefix")?.min(metric(b, "stable_prefix")?);
        Ok((stable >= APPLIED - 5).then_some(()))
    })
    .unwrap_or_else(|e| {
        panic!(
            "the stable frontier never advanced{}; a: {:?}, b: {:?}",
            e.map(|e| format!(" ({e:#})")).unwrap_or_default(),
            metrics_of(a),
            metrics_of(b)
        )
    });

    // --- the joiner writes first ---
    //
    // `start` returns once `/api/health` answers, and the node sleeps two
    // seconds before it dials anybody, so this write lands well inside the
    // window. No race: the operation is applied before any peer knows `c`
    // exists.
    cluster.start("c").expect("J5: start c");
    apply_ok(
        cluster.node("c"),
        ops::object_update("beta", ops::string_insert('!', 0)),
    );

    await_mesh(&cluster.nodes(), MESH_TIMEOUT).expect("J5: mesh after c joined");

    let after = assert_converged(&cluster.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        read_string(&after, "alpha").unwrap(),
        read_string(&before, "alpha").unwrap(),
        "the joiner never received the history it wrote on top of"
    );
}

// ---------------------------------------------------------------------------
// R1 — seeded random operations
// ---------------------------------------------------------------------------

/// **R1** — a seeded random workload across three replicas, then convergence.
///
/// The scenario that finds what hand-written cases miss, and the reason the
/// workload generator lives in the library: this and the dashboard's `--random`
/// driver produce the identical sequence for a given seed, so a run somebody
/// watched can be replayed here from the seed it printed.
///
/// The seed is fresh on every run unless `MOIRAI_E2E_SEED` pins it, and it is
/// printed on the way in and again in the failure message. A fixed seed would
/// make CI deterministic and useless — it would re-run the same twelve hundred
/// operations for ever. A random one that failed silently would be worse. This
/// is the pair that works: vary it, and make a failure carry its reproduction.
///
/// Runs on containers rather than processes because this is the scenario CI
/// leans on most, and it should exercise the same backend the shipped image
/// runs under.
#[test]
fn r1_seeded_random_operations_converge() {
    /// Enough to interleave, short enough to keep the suite under a minute.
    const OPERATIONS: usize = 180;

    let seed: u64 = std::env::var("MOIRAI_E2E_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1)
        });
    let reproduce = format!(
        "MOIRAI_E2E_SEED={seed} cargo test -p moirai-network \
                             --test e2e_convergence -- --test-threads=1 r1_"
    );
    eprintln!("\nR1 seed {seed} — reproduce with:\n  {reproduce}\n");

    let Some(cluster) = started_container_cluster("R1", &["a", "b", "c"]) else {
        return;
    };
    let nodes = cluster.nodes();
    let mut workload = Workload::new(seed, nodes.len());

    for (n, step) in workload.take(OPERATIONS).into_iter().enumerate() {
        let node = nodes[step.replica];
        apply(node, &step.op).unwrap_or_else(|e| {
            panic!(
                "R1 seed {seed}: step {n} (`{}` on `{}`) was refused: {e:#}\n\
                 reproduce with:\n  {reproduce}",
                step.label,
                node.id()
            )
        });
    }

    // `assert_converged` panics with every state and every log on failure, and
    // the seed has already been printed above it, so a CI failure carries both
    // the diff and the way to replay it.
    let state = assert_converged(&nodes, CONVERGE_TIMEOUT);
    eprintln!(
        "R1 seed {seed} converged on digest {}",
        state_digest(&state)
    );

    // Convergence on `Unset` would be vacuous: three replicas that applied
    // nothing agree perfectly.
    let object = root_object(&state)
        .unwrap_or_else(|e| panic!("R1 seed {seed}: {e:#}\nreproduce with:\n  {reproduce}"));
    assert!(
        !object.is_empty(),
        "R1 seed {seed}: the replicas agree on an empty state, which proves \
         nothing; state: {state}\nreproduce with:\n  {reproduce}"
    );

    // Every replica must also account for the same number of deliveries.
    // Agreeing on a rendered value while holding different histories is the
    // failure mode E2 had to be strengthened against.
    let delivered: Vec<(String, u64)> = nodes
        .iter()
        .map(|n| (n.id().to_string(), metric(*n, "delivered_ops").unwrap_or(0)))
        .collect();
    assert!(
        delivered.windows(2).all(|w| w[0].1 == w[1].1),
        "R1 seed {seed}: the replicas render the same state but do not account \
         for the same history: {delivered:?}\nreproduce with:\n  {reproduce}"
    );
    assert_eq!(
        delivered[0].1, OPERATIONS as u64,
        "R1 seed {seed}: {} of {OPERATIONS} operations were delivered\n\
         reproduce with:\n  {reproduce}",
        delivered[0].1
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

// ---------------------------------------------------------------------------
// P3 — relay connectivity
// ---------------------------------------------------------------------------
//
// The scenarios below need a rig the `Cluster` above cannot build: replicas with
// **no route to each other at all**. `ContainerBackend` puts every replica on
// one shared replication network, which is right for a partition — cut it and
// the link is gone — and wrong here, because a relay test on a rig where a
// direct path exists proves nothing about the relay.
//
// So `RelayRig` builds islands instead. Each replica is created on a network of
// its own, which is what binds its published HTTP port and is never cut, and is
// then attached to one or more *island* networks under a DNS alias. Two replicas
// on different islands share no network: Docker's embedded DNS will not resolve
// the address the bootnode hands out, and its inter-bridge isolation would drop
// the packet even with the address in hand. The bootnode and the relay sit on
// every island, so both replicas reach both services while reaching nothing else.
//
// The claim is asserted rather than assumed. `/api/metrics` reports how each
// peer is reached, so a scenario that expects `relayed` and reads `direct` fails
// on the rig instead of passing on the wrong topology.

/// Ports inside the service containers.
const RELAY_PORT: u16 = 7100;
const RELAY_HTTP_PORT: u16 = 7101;
const BOOTNODE_PORT: u16 = 7000;

/// Lines the two services print once every listener is bound. The relay's health
/// endpoint comes up last, so waiting for it also means the frame listener is up.
const RELAY_READY_LINE: &str = "[relay] health on";
const BOOTNODE_READY_LINE: &str = "[bootnode] listening on";

/// How long a relay-less pair is watched to confirm it does *not* converge.
///
/// The negative control's whole value is in this being long enough to be
/// convincing and short enough to keep the suite fast. With the relay stopped
/// there is no other path at all, so nothing is racing: this is a margin, not a
/// deadline.
const NO_CONVERGENCE_WINDOW: Duration = Duration::from_secs(15);

/// A container that is not a replica: the bootnode, or the relay.
struct Service {
    name: &'static str,
    container: Container<GenericImage>,
    http_base: String,
}

impl Service {
    fn log_tail(&self, lines: usize) -> String {
        let text = self
            .container
            .stderr_to_vec()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|e| format!("<{e}>"));
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

/// Replicas placed on islands, with a bootnode and a relay reachable from all of
/// them.
struct RelayRig {
    /// Declared first on purpose, as in [`Cluster`]: a Docker network cannot be
    /// removed while a container is attached, so every container must drop
    /// before the networks it is on.
    replicas: BTreeMap<String, ContainerNode>,
    /// `Option` so a scenario can stop the relay mid-run — which is the negative
    /// control that proves the relay was carrying the traffic.
    relay: Option<Service>,
    bootnode: Option<Service>,
    networks: Networks,
    /// Every network this rig created, removed on drop.
    created: Vec<String>,
    prefix: String,
    image: String,
    session: String,
}

impl RelayRig {
    /// Build the rig: one network per island, a bootnode and a relay on all of
    /// them. Returns the reason to skip rather than failing when Docker or the
    /// image is unavailable.
    fn new(islands: &[&str]) -> Result<Self> {
        let networks = Networks::new()?;
        let image =
            std::env::var("MOIRAI_E2E_IMAGE").unwrap_or_else(|_| "moirai-json-crdt:test".into());
        networks
            .runtime
            .block_on(networks.docker.inspect_image(&image))
            .with_context(|| {
                format!(
                    "the replica image `{image}` does not exist; build it with \
                     `docker build -f moirai/docker/e2e/Dockerfile -t {image} .` \
                     from the directory holding the moirai and arachne checkouts"
                )
            })?;

        let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("moirai-p3-{}-{run}", std::process::id());
        let mut rig = Self {
            replicas: BTreeMap::new(),
            relay: None,
            bootnode: None,
            networks,
            created: Vec::new(),
            prefix: prefix.clone(),
            image,
            session: format!("p3-{}-{run}", std::process::id()),
        };

        // The services are *created* here and attached to the islands below, so
        // that each attachment can carry a DNS alias. A network they share with
        // nobody else is the price of that; it gives no replica a route anywhere.
        rig.create_network(&rig.service_net())?;
        for island in islands {
            rig.create_network(&rig.island_net(island))?;
        }

        rig.start_relay(islands)?;
        rig.start_bootnode(islands)?;
        Ok(rig)
    }

    fn service_net(&self) -> String {
        format!("{}-svc", self.prefix)
    }

    fn island_net(&self, island: &str) -> String {
        format!("{}-island-{island}", self.prefix)
    }

    fn own_net(&self, id: &str) -> String {
        format!("{}-own-{id}", self.prefix)
    }

    fn create_network(&mut self, name: &str) -> Result<()> {
        self.networks.create(name)?;
        self.created.push(name.to_string());
        Ok(())
    }

    fn image_parts(&self) -> (String, String) {
        match self.image.rsplit_once(':') {
            Some((name, tag)) => (name.to_string(), tag.to_string()),
            None => (self.image.clone(), "latest".to_string()),
        }
    }

    /// Start one service and attach it to every island under `alias`.
    fn start_service(
        &mut self,
        name: &'static str,
        command: &str,
        ready_line: &str,
        http_port: u16,
        env: &[(&str, String)],
        alias: &str,
        islands: &[&str],
    ) -> Result<Service> {
        let (image_name, tag) = self.image_parts();
        let mut request = GenericImage::new(image_name, tag)
            .with_exposed_port(http_port.tcp())
            .with_wait_for(WaitFor::message_on_stderr(ready_line))
            .with_network(self.service_net())
            .with_cmd(vec![command]);
        for (key, value) in env {
            request = request.with_env_var(*key, value);
        }
        let container = request
            .start()
            .with_context(|| format!("start the {name} container"))?;
        let container_id = container.id().to_string();

        for island in islands {
            self.networks
                .connect(&self.island_net(island), &container_id, alias)?;
        }

        let host_port = container
            .get_host_port_ipv4(http_port.tcp())
            .with_context(|| format!("read the published port of {name}"))?;
        Ok(Service {
            name,
            container,
            http_base: format!("http://127.0.0.1:{host_port}"),
        })
    }

    fn start_relay(&mut self, islands: &[&str]) -> Result<()> {
        let relay = self.start_service(
            "relay",
            "moirai-relay",
            RELAY_READY_LINE,
            RELAY_HTTP_PORT,
            &[
                ("RELAY_PORT", RELAY_PORT.to_string()),
                ("RELAY_HTTP_PORT", RELAY_HTTP_PORT.to_string()),
                // One line per frame. A relay scenario that fails is almost
                // always a routing question, and the answer is in this log.
                ("RELAY_VERBOSE", "1".to_string()),
            ],
            "relay",
            islands,
        )?;
        self.relay = Some(relay);
        Ok(())
    }

    fn start_bootnode(&mut self, islands: &[&str]) -> Result<()> {
        let bootnode = self.start_service(
            "bootnode",
            "moirai-bootnode",
            BOOTNODE_READY_LINE,
            BOOTNODE_PORT,
            &[
                ("BOOTNODE_PORT", BOOTNODE_PORT.to_string()),
                ("BOOTNODE_TTL_SECS", "10".to_string()),
                // The whole reason a replica needs no relay configuration of its
                // own: the directory tells it where one is.
                ("RELAY_ADDR", format!("relay:{RELAY_PORT}")),
            ],
            "bootnode",
            islands,
        )?;
        self.bootnode = Some(bootnode);
        Ok(())
    }

    /// Start replica `id`, reachable only on `islands`.
    ///
    /// It gets no `PEERS` at all: everything it knows comes from the directory,
    /// which is what makes the routing decision the replica's own.
    fn start_replica(&mut self, id: &str, islands: &[&str]) -> Result<()> {
        let own = self.own_net(id);
        self.create_network(&own)?;

        let alias = format!("node-{id}");
        let (image_name, tag) = self.image_parts();
        let container = GenericImage::new(image_name, tag)
            .with_exposed_port(CONTAINER_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(READY_LINE))
            // Its own network, joined at creation: that is what binds the
            // published port, and it is never cut. Nobody else is on it, so it
            // is not a route to anywhere.
            .with_network(&own)
            .with_env_var("REPLICA_ID", id)
            .with_env_var("LISTEN_PORT", CONTAINER_LISTEN_PORT.to_string())
            .with_env_var("HTTP_PORT", CONTAINER_HTTP_PORT.to_string())
            .with_env_var("PEERS", "")
            .with_env_var("BOOTNODE_URL", format!("http://bootnode:{BOOTNODE_PORT}"))
            .with_env_var("SESSION_ID", &self.session)
            .with_env_var("ADVERTISE_ADDR", format!("{alias}:{CONTAINER_LISTEN_PORT}"))
            .with_env_var("RECONCILE_SECS", "1")
            .start()
            .with_context(|| format!("start a container for replica `{id}`"))?;
        let container_id = container.id().to_string();

        for island in islands {
            self.networks
                .connect(&self.island_net(island), &container_id, &alias)?;
        }

        let host_port = container
            .get_host_port_ipv4(CONTAINER_HTTP_PORT.tcp())
            .with_context(|| format!("read the published HTTP port of replica `{id}`"))?;
        let node = ContainerNode {
            id: id.to_string(),
            http_base: format!("http://127.0.0.1:{host_port}"),
            container,
        };
        await_healthy(&node, HEALTH_TIMEOUT)?;
        self.replicas.insert(id.to_string(), node);
        Ok(())
    }

    fn node(&self, id: &str) -> &dyn Node {
        self.replicas
            .get(id)
            .unwrap_or_else(|| panic!("replica `{id}` is not running"))
    }

    fn nodes(&self) -> Vec<&dyn Node> {
        self.replicas.values().map(|n| n as &dyn Node).collect()
    }

    /// The relay's counters. `frames_in` / `frames_out` are how the fan-out
    /// claim — one upload, N downloads — is observed rather than asserted.
    fn relay_health(&self) -> Result<Value> {
        let relay = self.relay.as_ref().ok_or_else(|| anyhow!("no relay"))?;
        let body = client()
            .get(format!("{}/health", relay.http_base))
            .send()?
            .error_for_status()?
            .text()?;
        Ok(serde_json::from_str(&body)?)
    }

    /// Kill the relay. The negative control: with no relay and no direct route,
    /// new writes must stop propagating.
    fn stop_relay(&mut self) {
        let Some(relay) = self.relay.take() else {
            return;
        };
        eprintln!(
            "--- relay log before it was stopped ---\n{}",
            relay.log_tail(LOG_TAIL_LINES)
        );
        drop(relay);
    }

    /// Everything worth reading when a scenario fails.
    fn diagnostics(&self) -> String {
        let mut out = String::new();
        for (id, node) in &self.replicas {
            let _ = writeln!(
                out,
                "--- {id} state {:?} metrics {:?} ---\n{}",
                state_of(node).map(|v| v.to_string()),
                metrics_of(node).map(|v| v.to_string()),
                node.log_tail(LOG_TAIL_LINES)
            );
        }
        for service in [self.relay.as_ref(), self.bootnode.as_ref()]
            .into_iter()
            .flatten()
        {
            let _ = writeln!(
                out,
                "--- {} ---\n{}",
                service.name,
                service.log_tail(LOG_TAIL_LINES)
            );
        }
        out
    }
}

impl Drop for RelayRig {
    fn drop(&mut self) {
        // Containers first: `replicas`, `relay` and `bootnode` are declared
        // before `networks`, so they are already gone by the time this returns
        // — but `Drop::drop` runs *before* the fields, so the removals have to
        // happen here, after dropping them explicitly.
        self.replicas.clear();
        self.relay = None;
        self.bootnode = None;
        for network in &self.created {
            let _ = self.networks.remove(network);
        }
    }
}

/// Builds a relay rig, or explains why the scenario is being skipped.
fn relay_rig(scenario: &str, islands: &[&str]) -> Option<RelayRig> {
    if let Err(why) = container_backend_selected() {
        eprintln!("\nE2E-SKIP {scenario}: {why}");
        return None;
    }
    match RelayRig::new(islands) {
        Ok(rig) => Some(rig),
        Err(why) => {
            eprintln!("\nE2E-SKIP {scenario}: {why:#}");
            None
        }
    }
}

/// How `node` says it reaches each of its peers: `direct`, `relayed`, or
/// `unreachable`.
fn routes_of(node: &dyn Node) -> Result<BTreeMap<String, String>> {
    let metrics = metrics_of(node)?;
    let routes = metrics
        .get("routes")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("`{}` reported no routes: {metrics}", node.id()))?;
    Ok(routes
        .iter()
        .filter_map(|(peer, route)| route.as_str().map(|r| (peer.clone(), r.to_string())))
        .collect())
}

/// Waits until `node` reports reaching `peer` by `expected`.
///
/// This is the assertion that the *rig* is what the scenario claims. A relay
/// scenario on a topology that accidentally has a direct path would otherwise
/// pass while testing nothing, which is the exact failure the design warns
/// about.
fn await_route(node: &dyn Node, peer: &str, expected: &str, timeout: Duration) -> Result<()> {
    poll_until(timeout, || {
        let routes = routes_of(node)?;
        Ok((routes.get(peer).map(String::as_str) == Some(expected)).then_some(()))
    })
    .map_err(|e| {
        anyhow!(
            "`{}` never reported reaching `{peer}` as `{expected}` (last seen {:?}){}",
            node.id(),
            routes_of(node).ok(),
            e.map(|e| format!("; last error: {e:#}"))
                .unwrap_or_default()
        )
    })
}

/// Asserts that `laggard` does **not** catch up with `writer` for `window`.
///
/// Polled for the whole window rather than checked once, and it refuses to be
/// vacuous: `writer` must actually have moved away from `before`, otherwise
/// "they never agreed" would be satisfied by nothing having happened.
fn assert_does_not_converge(
    writer: &dyn Node,
    laggard: &dyn Node,
    before: &Value,
    window: Duration,
    diagnostics: impl Fn() -> String,
) {
    let deadline = Instant::now() + window;
    let mut writer_moved = false;
    while Instant::now() < deadline {
        if let (Ok(w), Ok(l)) = (state_of(writer), state_of(laggard)) {
            if w != *before {
                writer_moved = true;
            }
            assert_ne!(
                w,
                l,
                "`{}` caught up with `{}` after the relay was stopped, so the \
                 rig has a path the scenario does not know about and the \
                 convergence above proved nothing about relaying\n{}",
                laggard.id(),
                writer.id(),
                diagnostics()
            );
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    assert!(
        writer_moved,
        "`{}` never left the state it shared with `{}`, so this control is \
         vacuous: nothing was written for the relay to fail to carry\n{}",
        writer.id(),
        laggard.id(),
        diagnostics()
    );
}

/// **P3-1** — two replicas that cannot open a direct connection converge, and
/// stop converging the moment the relay is taken away.
///
/// The second half is the whole test. A pair on a rig with a hidden path would
/// converge with the relay dead and the first half would still pass, which is
/// the failure the design's figure 6 names: *if they converge with the relay
/// stopped, the test is lying.*
#[test]
fn p3_two_replicas_with_no_route_converge() {
    let Some(mut rig) = relay_rig("P3-1", &["left", "right"]) else {
        return;
    };
    rig.start_replica("a", &["left"]).expect("P3-1: start a");
    rig.start_replica("b", &["right"]).expect("P3-1: start b");

    // The rig's own precondition: each replica has learnt about the other from
    // the directory and can only reach it through the relay. If either of these
    // read `direct`, the islands are not islands.
    await_route(rig.node("a"), "b", "relayed", MESH_TIMEOUT)
        .unwrap_or_else(|e| panic!("P3-1: {e:#}\n{}", rig.diagnostics()));
    await_route(rig.node("b"), "a", "relayed", MESH_TIMEOUT)
        .unwrap_or_else(|e| panic!("P3-1: {e:#}\n{}", rig.diagnostics()));

    apply_ok(
        rig.node("a"),
        ops::object_update("name", ops::string_insert('B', 0)),
    );
    apply_ok(
        rig.node("b"),
        ops::object_update("city", ops::string_insert('P', 0)),
    );
    let agreed = assert_converged(&rig.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(read_string(&agreed, "name").unwrap(), "B");
    assert_eq!(read_string(&agreed, "city").unwrap(), "P");

    let health = rig.relay_health().expect("P3-1: relay health");
    assert!(
        health["frames_out"].as_u64().unwrap_or(0) > 0,
        "the replicas converged without the relay forwarding anything: {health}"
    );
    assert_eq!(
        health["sessions"].as_u64(),
        Some(2),
        "both replicas must hold a session: {health}"
    );

    // --- the control ---
    rig.stop_relay();
    apply_ok(
        rig.node("a"),
        ops::object_update("name", ops::string_insert('R', 1)),
    );
    assert_does_not_converge(
        rig.node("a"),
        rig.node("b"),
        &agreed,
        NO_CONVERGENCE_WINDOW,
        || rig.diagnostics(),
    );
}

/// **P3-2** — a mixed rig: reachable pairs go direct, the unreachable pair goes
/// through the relay, and all three converge.
///
/// `c` is on both islands, so `a`↔`c` and `b`↔`c` have a path and `a`↔`b` does
/// not. The routes are asserted per replica, because "they all converged" is
/// also true of a rig where everything went through the relay — and a relay that
/// carried traffic it did not need to would be a regression in the direction
/// nobody would notice.
#[test]
fn p3_mixed_rig_routes_each_pair_the_cheapest_way() {
    let Some(mut rig) = relay_rig("P3-2", &["left", "right"]) else {
        return;
    };
    rig.start_replica("c", &["left", "right"])
        .expect("P3-2: start c");
    rig.start_replica("a", &["left"]).expect("P3-2: start a");
    rig.start_replica("b", &["right"]).expect("P3-2: start b");

    for (node, peer, route) in [
        ("a", "c", "direct"),
        ("a", "b", "relayed"),
        ("b", "c", "direct"),
        ("b", "a", "relayed"),
        ("c", "a", "direct"),
        ("c", "b", "direct"),
    ] {
        await_route(rig.node(node), peer, route, MESH_TIMEOUT)
            .unwrap_or_else(|e| panic!("P3-2: {e:#}\n{}", rig.diagnostics()));
    }

    for (id, key) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        apply_ok(rig.node(id), ops::object_update(key, ops::number_inc(1.0)));
    }
    let agreed = assert_converged(&rig.nodes(), CONVERGE_TIMEOUT);
    for key in ["alpha", "beta", "gamma"] {
        assert_eq!(
            read_number(&agreed, key).unwrap(),
            1.0,
            "`{key}` did not reach every replica: {agreed}"
        );
    }
}

/// **P3-3** — a replica that joins an established, routeless session over the
/// relay receives the history it missed.
///
/// The point of separating this from P3-1: catching up is not the same as
/// replicating forwards. A joiner's history arrives because the composite
/// returns a newly *relayed* peer from `connect_to_peers`, which is what makes
/// `GenericNode::connect` answer it with a sync or state request — phase 1's
/// dialer-requests-sync chain, firing over a route it knows nothing about. Leave
/// a relayed peer out of that list and this scenario is the one that notices.
#[test]
fn p3_a_joiner_over_the_relay_receives_history() {
    /// Applied before the joiner arrives.
    const APPLIED: usize = 6;

    let Some(mut rig) = relay_rig("P3-3", &["left", "right"]) else {
        return;
    };
    rig.start_replica("a", &["left"]).expect("P3-3: start a");
    for pos in 0..APPLIED {
        apply_ok(
            rig.node("a"),
            ops::object_update("name", ops::string_insert('x', pos)),
        );
    }

    rig.start_replica("b", &["right"]).expect("P3-3: start b");
    await_route(rig.node("b"), "a", "relayed", MESH_TIMEOUT)
        .unwrap_or_else(|e| panic!("P3-3: {e:#}\n{}", rig.diagnostics()));

    let agreed = assert_converged(&rig.nodes(), CONVERGE_TIMEOUT);
    assert_eq!(
        read_string(&agreed, "name").unwrap().len(),
        APPLIED,
        "the joiner did not receive what happened before it arrived: {agreed}"
    );
}
