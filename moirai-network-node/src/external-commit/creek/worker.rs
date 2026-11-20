//! Independent Paxos execution. The mutex only copies ID metadata and transfers
//! chosen IDs; promises, ballots, votes, and the journal stay on the worker.
use super::{Creek, IsOmega, Shared, paxos::Paxos, wire};
use crate::{Config, PeerConfig, TcpTransport, Transport};
use std::{
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct WorkerConfig {
    pub retry_interval: Duration,
    pub poll_interval: Duration,
}
impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_millis(200),
            poll_interval: Duration::from_millis(2),
        }
    }
}

/// Owns the independent consensus thread. Keep alive throughout Node::run.
/// Dropping it stops and joins the worker; volatile acceptors cannot restart.
#[derive(Debug)]
pub struct Worker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl<O: IsOmega> Creek<O> {
    /// Start the independent TCP worker using the node's single configuration.
    /// Local and peer consensus ports are their configured causal ports + 1.
    pub fn spawn_tcp(&self, node: &Config, config: WorkerConfig) -> io::Result<Worker> {
        let (port, peers) = network(node)?;
        let mut expected: Vec<_> = self.members.iter().map(|(id, _)| id.0).collect();
        expected.sort_unstable();
        let actual: Vec<_> = node.members().iter().map(|(id, _)| id.0).collect();
        if self.members.first().map(|(id, _)| id.0) != Some(node.replica_id) || expected != actual {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Creek and node membership differ",
            ));
        }
        self.spawn(
            TcpTransport::bind(node.replica_id, ("0.0.0.0", port), &peers)?,
            config,
        )
    }

    /// Move an independent transport to the worker. The oracle and the log stay
    /// on their owner thread and do not need to implement Send.
    pub fn spawn<T: Transport + Send + 'static>(
        &self,
        transport: T,
        config: WorkerConfig,
    ) -> io::Result<Worker> {
        if self.members.is_empty()
            || config.retry_interval.is_zero()
            || config.poll_interval.is_zero()
            || config.poll_interval > Duration::from_millis(50)
            || Instant::now().checked_add(config.retry_interval).is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid Creek worker configuration",
            ));
        }
        {
            let mut shared = self
                .shared
                .lock()
                .map_err(|_| io::Error::other("Creek handoff poisoned"))?;
            if shared.started {
                return Err(io::Error::other("Creek worker cannot be restarted"));
            }
            shared.started = true;
        }
        let own = self.members[0].0.0;
        let members = self.members.iter().map(|(id, _)| id.0).collect();
        let shared = self.shared.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let thread = thread::Builder::new()
            .name(format!("creek-{own}"))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    run(transport, own, members, config, &worker_stop, &shared)
                }))
                .unwrap_or_else(|_| Err(io::Error::other("Creek worker panicked")));
                let mut shared_state = shared.lock().unwrap_or_else(|e| e.into_inner());
                shared_state.error = Some(match result {
                    Ok(()) => "Creek worker stopped".into(),
                    Err(error) => error.to_string(),
                });
                shared.clear_poison();
            });
        match thread {
            Ok(thread) => Ok(Worker {
                stop,
                thread: Some(thread),
            }),
            Err(error) => {
                // Startup failed before any acceptor state was used.
                self.shared
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .started = false;
                Err(error)
            }
        }
    }
}

fn run<T: Transport>(
    mut transport: T,
    own: u16,
    members: Vec<u16>,
    config: WorkerConfig,
    stop: &AtomicBool,
    shared: &Mutex<Shared>,
) -> io::Result<()> {
    let mut paxos = Paxos::new(own, members, config.retry_interval, Instant::now());
    while !stop.load(Ordering::Acquire) {
        let snapshot = {
            let shared = shared
                .lock()
                .map_err(|_| io::Error::other("Creek handoff poisoned"))?;
            if shared.closed {
                return Ok(());
            }
            shared.snapshot.clone()
        };
        // All consensus computation and network work happen outside the mutex.
        paxos.tick(snapshot, Instant::now())?;
        for packet in transport.poll()? {
            match wire::decode(&packet.bytes) {
                Ok(message) => paxos.receive(packet.peer, message)?,
                Err(error) => eprintln!(
                    "discarding invalid Creek message from {}: {error}",
                    packet.peer
                ),
            }
        }
        for _ in 0..256 {
            let Some((peer, message)) = paxos.outgoing.pop_front() else {
                break;
            };
            if peer == own {
                paxos.receive(own, message)?;
            } else {
                // A send/queue failure never counts as a vote. Phase retries
                // and journal synchronization repair dropped control frames.
                let _ = transport.send(peer, wire::encode(&message)?);
            }
        }
        if !paxos.learned.is_empty() {
            let mut shared = shared
                .lock()
                .map_err(|_| io::Error::other("Creek handoff poisoned"))?;
            shared.learned.append(&mut paxos.learned);
        }
        thread::sleep(config.poll_interval);
    }
    Ok(())
}

fn next_port(port: u16) -> io::Result<u16> {
    if port == 0 || port == u16::MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Creek requires base ports in 1..=65534",
        ));
    }
    Ok(port + 1)
}

fn network(config: &Config) -> io::Result<(u16, Vec<PeerConfig>)> {
    config.validate()?;
    let port = next_port(config.listen_port)?;
    if config.http_port == Some(port) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Creek port collides with the HTTP port",
        ));
    }
    let peers = config
        .peers
        .iter()
        .map(|peer| {
            let (host, port) = peer
                .address
                .rsplit_once(':')
                .filter(|(host, _)| !host.is_empty())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "peer address must contain host:port",
                    )
                })?;
            let port = port
                .parse::<u16>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer port"))?;
            Ok(PeerConfig {
                address: format!("{host}:{}", next_port(port)?),
                ..peer.clone()
            })
        })
        .collect::<io::Result<_>>()?;
    Ok((port, peers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(port: u16, peers: &[&str]) -> Config {
        serde_json::from_value(json!({"replica_id": 1, "listen_port": port,
            "peers": peers.iter().enumerate().map(|(i, address)| json!({"replica_id": i + 2, "address": address})).collect::<Vec<_>>() })).unwrap()
    }

    #[test]
    fn derived_ports_preserve_hostnames_and_ipv6_and_leave_config_unchanged() {
        let config = config(19001, &["replica-b.service:19003", "[::1]:20000"]);
        let (port, peers) = network(&config).unwrap();
        assert_eq!(port, 19002);
        assert_eq!(peers[0].address, "replica-b.service:19004");
        assert_eq!(peers[1].address, "[::1]:20001");
        assert_eq!(config.peers[0].address, "replica-b.service:19003");
    }

    #[test]
    fn invalid_base_ports_and_http_collisions_fail_before_binding() {
        for port in [0, u16::MAX] {
            assert!(network(&config(port, &[])).is_err());
        }
        for peer in ["host:0", "host:65535", "host:no-port", "host", ":19001"] {
            assert!(network(&config(19001, &[peer])).is_err());
        }
        let mut config = config(19001, &[]);
        config.http_port = Some(19002);
        assert!(network(&config).is_err());
    }
}
