//! Starts a node from the environment, as 601c887's `model_node` does
//! (NW.31 design row S7): the variables name the replication and HTTP ports,
//! `PORTS_FILE` receives the ports the kernel chose, `PEERS_FILE` is waited
//! for once they are reported, and Zenoh binds the kernel's port before the
//! peers are known and keeps it (NW.30).
//!
//! Release the replication port only where 601c887 did: under
//! `ZENOH_MODE=client`, which listens on no port at all.

use std::env;
use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, ReplicaFactory};
use moirai_netstack_http::{HttpApi, Routes};
use moirai_netstack_node::Node;
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::any_transport::AnyTransport;
use moirai_netstack_transport::transport_config::{TransportConfig, TransportKind};
use serde_json::json;

/// How long a node told `PEERS_FILE` waits for the file to appear.
const PEERS_FILE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long an HTTP request waits for the event loop, 601c887's `REPLY_TIMEOUT`.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// `LISTEN_PORT` when it is unset, as 601c887 defaults it.
const DEFAULT_LISTEN_PORT: u16 = 9001;

/// Start a node over `factory` from the environment and serve it until the
/// process ends. The second argument is the HTTP crate's [`Routes`]: an
/// application's own routes, or `()` for none.
pub fn run<F, R>(factory: F, mut routes: R) -> !
where
    F: ReplicaFactory,
    R: Routes<F, AnyTransport>,
{
    let replica_id = env::var("REPLICA_ID").unwrap_or_else(|_| "replica-a".to_string());
    let listen_port = match env::var("LISTEN_PORT") {
        Ok(raw) => raw.parse().unwrap_or_else(|err| {
            eprintln!("[{replica_id}] invalid LISTEN_PORT `{raw}`: {err}");
            std::process::exit(1);
        }),
        Err(_) => DEFAULT_LISTEN_PORT,
    };
    let http_port: Option<u16> = env::var("HTTP_PORT").ok().and_then(|p| p.parse().ok());
    let ports_file = path_var("PORTS_FILE");
    let peers_file = path_var("PEERS_FILE");
    let http_bind = http_bind_addr(&replica_id);

    // Set, `LOG_ID` means join that log; unset, a fresh id is minted, which is
    // right only for the replica that creates the session.
    let log_id = match env::var("LOG_ID") {
        Ok(raw) => LogId::parse(&raw).unwrap_or_else(|err| {
            eprintln!("[{replica_id}] invalid LOG_ID `{raw}`: {err}");
            std::process::exit(1);
        }),
        Err(_) => {
            let id = LogId::generate();
            eprintln!("[{replica_id}] log id: {id}");
            id
        }
    };

    // A variable the transport refuses ends the process here, naming it,
    // before anything is bound.
    let config = TransportConfig::from_env().unwrap_or_else(|err| {
        eprintln!("[{replica_id}] {err}");
        std::process::exit(1);
    });

    let (mut node, http_listener, http_port) = match config.kind() {
        TransportKind::Tcp => start_tcp(
            factory,
            &replica_id,
            listen_port,
            http_bind,
            http_port,
            ports_file.as_deref(),
            peers_file.as_deref(),
            log_id,
        ),
        TransportKind::Zenoh => start_zenoh(
            factory,
            &replica_id,
            &config,
            listen_port,
            http_bind,
            http_port,
            ports_file.as_deref(),
            peers_file.as_deref(),
            log_id,
        ),
    };

    node.connect();
    let api = http_listener.map(|listener| {
        HttpApi::start(listener, &replica_id, node.log_id().clone(), REPLY_TIMEOUT)
    });
    eprintln!(
        "[{replica_id}] Running. POST ops to http://localhost:{}/api/op",
        http_port.unwrap_or(0)
    );
    loop {
        node.poll();
        if let Some(api) = &api {
            api.serve(&mut node, &mut routes);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// The TCP direct path: the replication listener is bound here, so a port of
/// 0 is the kernel's and the listener that holds it is the one the transport
/// keeps.
#[allow(clippy::too_many_arguments)]
fn start_tcp<F: ReplicaFactory>(
    factory: F,
    replica_id: &str,
    listen_port: u16,
    http_bind: IpAddr,
    http_port: Option<u16>,
    ports_file: Option<&Path>,
    peers_file: Option<&Path>,
    log_id: LogId,
) -> (Node<F, AnyTransport>, Option<TcpListener>, Option<u16>) {
    let listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), listen_port))
            .unwrap_or_else(|err| {
                eprintln!("[{replica_id}] cannot bind the replication port {listen_port}: {err}");
                std::process::exit(1);
            });
    let listen_port = bound_port(&listener);
    let (http_listener, http_port) = bind_http_early(replica_id, http_bind, http_port);
    report_ports(ports_file, replica_id, listen_port, http_port);
    let Peers { addresses, members } = read_peers(replica_id, peers_file);
    let transport =
        AnyTransport::from_env_with_listener(replica_id.to_string(), listener, addresses)
            .unwrap_or_else(|err| {
                eprintln!(
                    "[{replica_id}] cannot build the TCP transport on port {listen_port}: {err}"
                );
                std::process::exit(1);
            });
    let node = Node::new(
        factory,
        replica_id.to_string(),
        &member_refs(&members),
        transport,
        log_id,
    );
    (node, http_listener, http_port)
}

/// The Zenoh path, 601c887's `start_zenoh` arm for arm. `LISTEN_PORT=0` with
/// `PEERS_FILE` opens the session before the peers are known, so the session
/// binds and keeps the kernel's port; a second, listening-less session dials
/// the peers once they are read, because Zenoh 1.10.1 dials no peer after a
/// session opens (NW.30).
#[allow(clippy::too_many_arguments)]
fn start_zenoh<F: ReplicaFactory>(
    factory: F,
    replica_id: &str,
    config: &TransportConfig,
    listen_port: u16,
    http_bind: IpAddr,
    http_port: Option<u16>,
    ports_file: Option<&Path>,
    peers_file: Option<&Path>,
    log_id: LogId,
) -> (Node<F, AnyTransport>, Option<TcpListener>, Option<u16>) {
    let id = replica_id.to_string();
    let opened = match peers_file {
        None => {
            let Peers { addresses, members } = read_peers(replica_id, None);
            let transport = zenoh_or_exit(
                replica_id,
                listen_port,
                AnyTransport::from_env(id.clone(), listen_port, addresses),
            );
            let listen_port = session_port(replica_id, &transport, listen_port, ports_file);
            let (http_listener, http_port) = bind_http_early(replica_id, http_bind, http_port);
            report_ports(ports_file, replica_id, listen_port, http_port);
            (transport, members, http_listener, http_port)
        }
        Some(path) if listen_port != 0 => {
            let (http_listener, http_port) = bind_http_early(replica_id, http_bind, http_port);
            report_ports(ports_file, replica_id, listen_port, http_port);
            let Peers { addresses, members } = read_peers(replica_id, Some(path));
            let transport = zenoh_or_exit(
                replica_id,
                listen_port,
                AnyTransport::from_env(id.clone(), listen_port, addresses),
            );
            (transport, members, http_listener, http_port)
        }
        Some(path) if !config.is_client() => {
            let mut transport = zenoh_or_exit(
                replica_id,
                listen_port,
                AnyTransport::from_env(id.clone(), listen_port, HashMap::default()),
            );
            let listen_port = session_port(replica_id, &transport, listen_port, ports_file);
            let (http_listener, http_port) = bind_http_early(replica_id, http_bind, http_port);
            report_ports(ports_file, replica_id, listen_port, http_port);
            let Peers { addresses, members } = read_peers(replica_id, Some(path));
            if let Err(err) = transport.dial_peers(addresses) {
                eprintln!(
                    "[{replica_id}] cannot dial the peers named in PEERS_FILE `{}`: {err}",
                    path.display()
                );
                std::process::exit(1);
            }
            (transport, members, http_listener, http_port)
        }
        Some(path) => {
            // A client listens on no port: reserve one only to report it, as
            // 601c887 does, and release it for the session.
            let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
                .unwrap_or_else(|err| {
                    eprintln!("[{replica_id}] cannot reserve a replication port: {err}");
                    std::process::exit(1);
                });
            let listen_port = bound_port(&listener);
            let (http_listener, http_port) = bind_http_early(replica_id, http_bind, http_port);
            report_ports(ports_file, replica_id, listen_port, http_port);
            let Peers { addresses, members } = read_peers(replica_id, Some(path));
            let transport = zenoh_or_exit(
                replica_id,
                listen_port,
                AnyTransport::from_env_with_listener(id.clone(), listener, addresses),
            );
            (transport, members, http_listener, http_port)
        }
    };
    let (transport, members, http_listener, http_port) = opened;
    let node = Node::new(
        factory,
        replica_id.to_string(),
        &member_refs(&members),
        transport,
        log_id,
    );
    (node, http_listener, http_port)
}

/// The port a Zenoh session listens on; the configured one when nobody reads
/// it, and the end of the process when it is unknown and `PORTS_FILE` must
/// name it.
fn session_port(
    replica_id: &str,
    transport: &AnyTransport,
    listen_port: u16,
    ports_file: Option<&Path>,
) -> u16 {
    match transport.session_port() {
        Some(port) => port,
        None if listen_port != 0 || ports_file.is_none() => listen_port,
        None => {
            eprintln!(
                "[{replica_id}] the Zenoh session lists no locator, so the port the kernel chose \
                 for LISTEN_PORT=0 is unknown and cannot be written to PORTS_FILE; give a fixed \
                 LISTEN_PORT"
            );
            std::process::exit(1);
        }
    }
}

/// The transport, or the end of the process naming the port it could not open.
fn zenoh_or_exit(
    replica_id: &str,
    listen_port: u16,
    opened: moirai_netstack_transport::transport::TransportResult<AnyTransport>,
) -> AnyTransport {
    opened.unwrap_or_else(|err| {
        eprintln!(
            "[{replica_id}] cannot open the Zenoh session listening on port {listen_port}: {err}"
        );
        std::process::exit(1);
    })
}

/// A path-valued variable, unset when absent or empty.
fn path_var(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// The address the HTTP listener binds: `HTTP_BIND_ADDR`, or `0.0.0.0` when it
/// is unset or empty. An address that does not parse ends the process, naming
/// the variable, rather than falling back to any address (NW.11).
fn http_bind_addr(replica_id: &str) -> IpAddr {
    match env::var("HTTP_BIND_ADDR") {
        Ok(raw) if raw.trim().is_empty() => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        Ok(raw) => raw.trim().parse().unwrap_or_else(|err| {
            eprintln!("[{replica_id}] invalid HTTP_BIND_ADDR `{raw}`: {err}");
            std::process::exit(1);
        }),
        Err(env::VarError::NotPresent) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        Err(err) => {
            eprintln!("[{replica_id}] invalid HTTP_BIND_ADDR: {err}");
            std::process::exit(1);
        }
    }
}

/// The HTTP listener, bound now so that its port is known before the report
/// and before the peers are read, with the port it holds.
fn bind_http_early(
    replica_id: &str,
    addr: IpAddr,
    port: Option<u16>,
) -> (Option<TcpListener>, Option<u16>) {
    let Some(port) = port else {
        return (None, None);
    };
    let listener = TcpListener::bind(SocketAddr::new(addr, port)).unwrap_or_else(|err| {
        eprintln!("[{replica_id}] Failed to start HTTP server on {addr}:{port}: {err}");
        std::process::exit(1);
    });
    let port = bound_port(&listener);
    (Some(listener), Some(port))
}

/// The port a bound listener holds.
fn bound_port(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .expect("a bound listener has a local address")
        .port()
}

/// Write the ports report to `path`, beside it first and renamed onto it, so a
/// reader never sees half of it.
fn write_ports_file(
    path: &Path,
    replica_id: &str,
    listen_port: u16,
    http_port: Option<u16>,
) -> std::io::Result<()> {
    let report = json!({
        "replica_id": replica_id,
        "pid": std::process::id(),
        "listen_port": listen_port,
        "http_port": http_port,
    });
    let mut staging = OsString::from(path.as_os_str());
    staging.push(".staging");
    std::fs::write(&staging, report.to_string())?;
    std::fs::rename(&staging, path)
}

/// Write `PORTS_FILE`, when there is one, or end the process naming it.
fn report_ports(
    ports_file: Option<&Path>,
    replica_id: &str,
    listen_port: u16,
    http_port: Option<u16>,
) {
    if let Some(path) = ports_file
        && let Err(err) = write_ports_file(path, replica_id, listen_port, http_port)
    {
        eprintln!(
            "[{replica_id}] cannot write PORTS_FILE `{}`: {err}",
            path.display()
        );
        std::process::exit(1);
    }
}

/// The peers a node is given: their addresses (`id → host:port`) and every
/// member of the log, this node first.
struct Peers {
    addresses: HashMap<String, String>,
    members: Vec<String>,
}

/// The members as the node constructor takes them.
fn member_refs(members: &[String]) -> Vec<&str> {
    members.iter().map(String::as_str).collect()
}

/// Parse `PEERS=id:host:port,...`, or the same text from `PEERS_FILE`, which
/// the harness writes once every node of its session has reported.
fn read_peers(replica_id: &str, peers_file: Option<&Path>) -> Peers {
    let peers_str = match peers_file {
        Some(path) => match await_peers_file(path) {
            Some(text) => text.trim().to_string(),
            None => {
                eprintln!(
                    "[{replica_id}] PEERS_FILE `{}` did not appear within {} s",
                    path.display(),
                    PEERS_FILE_TIMEOUT.as_secs()
                );
                std::process::exit(1);
            }
        },
        None => env::var("PEERS").unwrap_or_default(),
    };
    let mut addresses: HashMap<String, String> = HashMap::default();
    let mut members: Vec<String> = vec![replica_id.to_string()];
    for spec in peers_str.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() >= 3 {
            let peer_id = parts[0].to_string();
            let addr = format!("{}:{}", parts[1], parts[2]);
            members.push(peer_id.clone());
            addresses.insert(peer_id, addr);
        }
    }
    Peers { addresses, members }
}

/// The text of `PEERS_FILE`, once it has appeared; `None` after
/// [`PEERS_FILE_TIMEOUT`].
fn await_peers_file(path: &Path) -> Option<String> {
    let deadline = Instant::now() + PEERS_FILE_TIMEOUT;
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            return Some(text);
        }
        if Instant::now() > deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(50));
    }
}
