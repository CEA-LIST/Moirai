use deepsize::DeepSizeOf;
use moirai_network_node::{
    Application, Config, Node,
    external_commit::creek::{Creek, Worker, WorkerConfig},
};
use moirai_protocol::{
    commitment::{
        commit_log::CommitmentLog,
        commit_protocols::{
            lamport::lamport_commit::LamportCommit,
            omega::{omega_commit::OmegaCommit, oracle::EventOracle},
        },
        examples::commit_set::{CommitSet, SetUpdate},
        protocol::AtomicBroadcastProtocol,
        wire_id::StableReplicaId,
    },
    data_type::query::Contains,
    event::tagged_op::TaggedOp,
    replica::IsReplica,
    state::po_log::POLog,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use std::{
    fs,
    hint::black_box,
    io::{self, Write},
    net::ToSocketAddrs,
    num::NonZeroU64,
    sync::{
        atomic::Ordering,
        mpsc::{self, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

use super::{
    control::{Control, Phase},
    measurement::{Measured, Resources, unix_ms},
    workload::{Operation, Workload},
};

pub type Child = POLog<CommitSet<u32>, Vec<TaggedOp<Update>>>;
pub type Update = SetUpdate<u32>;
type Log<A> = CommitmentLog<Child, Measured<A>>;
const POLL_INTERVAL: Duration = Duration::from_millis(2);

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Protocol {
    #[serde(alias = "omega")]
    Commit,
    #[serde(alias = "lamport")]
    Timestamp,
    Creek,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkConfig {
    replica: Config,
    protocol: Protocol,
    workload: Workload,
    sample_interval_ms: NonZeroU64,
    network_interface: String,
    #[serde(default = "default_dns_timeout")]
    peer_dns_timeout_s: u64,
}

fn default_dns_timeout() -> u64 {
    120
}

// Headless Service DNS can appear after the Pods have already started.
fn resolve_peers(config: &mut Config, timeout: Duration) -> io::Result<()> {
    let start = Instant::now();
    for peer in &mut config.peers {
        loop {
            if let Ok(mut addresses) = peer.address.to_socket_addrs()
                && let Some(address) = addresses.next()
            {
                peer.address = address.to_string();
                break;
            }
            if start.elapsed() >= timeout {
                return Err(io::Error::other(format!(
                    "cannot resolve peer {}",
                    peer.address
                )));
            }
            thread::sleep(Duration::from_millis(250));
        }
    }
    Ok(())
}

pub fn main() -> io::Result<()> {
    let path = std::env::args_os().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: benchmark <replica.json>",
        )
    })?;
    let mut config: BenchmarkConfig =
        serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
    config.replica.validate()?;
    config.workload.validate()?;
    if config.sample_interval_ms.get() > 60_000 || config.replica.noop_interval_ms.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark needs idle no-ops and a sample interval of at most 60000 ms",
        ));
    }
    resolve_peers(
        &mut config.replica,
        Duration::from_secs(config.peer_dns_timeout_s),
    )?;
    let id = StableReplicaId(config.replica.replica_id);
    let members = config.replica.members();
    let timeout = Duration::from_millis(config.replica.event_timeout_ms.get());
    match config.protocol {
        Protocol::Commit => {
            let protocol =
                OmegaCommit::from_parts(EventOracle::with_timeout(id, timeout), members, id);
            run(config, protocol, None)
        }
        Protocol::Timestamp => run(config, LamportCommit::new(members.len()), None),
        Protocol::Creek => {
            let protocol = Creek::new(EventOracle::with_timeout(id, timeout), members, id)?;
            let worker = protocol.spawn_tcp(&config.replica, WorkerConfig::default())?;
            run(config, protocol, Some((worker, Creek::poll)))
        }
    }
}

fn run<A>(
    config: BenchmarkConfig,
    protocol: A,
    mut external: Option<(Worker, fn(&mut A) -> io::Result<()>)>,
) -> io::Result<()>
where
    A: AtomicBroadcastProtocol<Child> + DeepSizeOf,
    <A as AtomicBroadcastProtocol<Child>>::Update: Serialize + DeserializeOwned,
{
    let id = config.replica.replica_id;
    let port = config.replica.http_port.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark requires an admin port",
        )
    })?;
    let mut replica = config.replica;
    let idle = Duration::from_millis(replica.noop_interval_ms.take().unwrap().get());
    // Take all memory readings together with our own CPU and network samples.
    replica.memory_usage_enabled = false;
    let peer_ids: Vec<_> = replica.peers.iter().map(|peer| peer.replica_id).collect();
    let log = CommitmentLog::new(Child::default(), Measured::new(protocol));
    let application: Application<Log<A>> = Application {
        query: |_| json!(null),
        query_stable: |_| json!(null),
        committed: |_, _| false,
        no_op: || Update::NoOp,
    };
    let mut node = Node::new(replica, log, application)?;
    Resources::read(&config.network_interface)?;
    let control = Control::bind(port, id)?;
    let sample_period = Duration::from_millis(config.sample_interval_ms.get());
    let (send, receive) = mpsc::sync_channel(config.workload.queue_capacity.get());
    let mut send = Some(send);
    let mut producer = None;
    let mut baseline = None;
    let mut updates = 0;
    let mut queries = 0;
    let mut next_noop = Instant::now() + idle;
    let mut next_sample = None;

    loop {
        let phase = *control.phase.lock().unwrap();
        if phase == Phase::Crashing {
            std::process::exit(99);
        }
        if phase == Phase::Sealed {
            thread::sleep(Duration::from_millis(20));
            continue;
        }
        if phase == Phase::Starting {
            let resources = Resources::read(&config.network_interface)?;
            let start = Instant::now();
            let start_unix_ms = unix_ms();
            baseline = Some((start, resources));
            node.replica_mut().state_mut().protocol_mut().observer.start = Some(start);
            println!(
                "{}",
                json!({"type":"benchmark_start","benchmark_version":3,"replica_id":id,"timestamp_unix_ms":start_unix_ms})
            );
            let workload = config.workload;
            let queue = send.take().unwrap();
            producer = Some(thread::spawn(move || workload.produce(id, start, queue)));
            *control.phase.lock().unwrap() = Phase::Running;
            next_sample = Some(start);
        }

        node.tick()?;
        if let Some((_, poll)) = &external {
            let log = node.replica_mut().state_mut();
            poll(log.protocol_mut().inner_mut())?;
            log.commit_ready();
        }
        if phase == Phase::Ready {
            control.connected.store(
                peer_ids.iter().all(|id| node.transport().is_connected(*id)),
                Ordering::Relaxed,
            );
        }

        let mut drained = false;
        let mut queue_busy = false;
        if phase == Phase::Running {
            let poll_at = Instant::now() + POLL_INTERVAL;
            loop {
                let operation = match receive.try_recv() {
                    Ok(operation) => operation,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        drained = true;
                        break;
                    }
                };
                let dequeued_at = Instant::now();
                match operation {
                    Operation::Contains(key) => {
                        black_box(node.replica().query(&Contains(key)));
                        queries += 1;
                    }
                    Operation::Add(_) | Operation::Remove(_) => {
                        let update = match operation {
                            Operation::Add(key) => Update::Add(key),
                            Operation::Remove(key) => Update::Remove(key),
                            Operation::Contains(_) => unreachable!(),
                        };
                        node.replica_mut()
                            .state_mut()
                            .protocol_mut()
                            .observer
                            .dequeued_at = Some(dequeued_at);
                        node.broadcast(update)?;
                        updates += 1;
                        next_noop = Instant::now() + idle;
                    }
                }
                // A continuous producer must leave time for network and commit polling.
                if Instant::now() >= poll_at {
                    queue_busy = true;
                    break;
                }
            }
        }

        if Instant::now() >= next_noop {
            node.broadcast(Update::NoOp)?;
            next_noop = Instant::now() + idle;
        }

        let final_sample = phase == Phase::Sealing;
        if let Some((start, resources)) = baseline
            && (drained || final_sample || next_sample.is_some_and(|next| Instant::now() >= next))
        {
            let now = Instant::now();
            let resource_delta = Resources::read(&config.network_interface)?.since(resources)?;
            let memory = node.log_memory_usage_bytes();
            let observer = &mut node.replica_mut().state_mut().protocol_mut().observer;
            println!(
                "{}",
                json!({
                    "type":"benchmark_sample", "replica_id":id, "timestamp_unix_ms":unix_ms(),
                    "elapsed_ns":now.duration_since(start).as_nanos() as u64,
                    "resources":resource_delta, "log_memory_bytes":memory,
                    "processed_updates":updates, "processed_queries":queries,
                    "committed_updates":observer.committed_updates,
                    "committed_local_updates":observer.committed_local_updates,
                    "commit_latency_samples":observer.latency_samples,
                    "drained":drained, "final":final_sample,
                })
            );
            observer.latency_samples.clear();
            if final_sample {
                // Keep the seal record small when a protocol leaves a long backlog.
                let mut pending = observer.uncommitted_local().peekable();
                while pending.peek().is_some() {
                    let batch: Vec<_> = pending.by_ref().take(256).collect();
                    println!(
                        "{}",
                        json!({
                            "type":"benchmark_uncommitted", "replica_id":id,
                            "seal_elapsed_ns":now.duration_since(start).as_nanos() as u64,
                            "updates":batch,
                        })
                    );
                }
            }
            next_sample = Some(now + sample_period);
        }

        if drained {
            let stats = producer
                .take()
                .unwrap()
                .join()
                .map_err(|_| io::Error::other("workload thread panicked"))?;
            println!(
                "{}",
                json!({
                    "type":"benchmark_workload_done", "replica_id":id, "timestamp_unix_ms":unix_ms(),
                    "scheduled":stats.scheduled, "enqueued":stats.enqueued, "queue_full":stats.queue_full,
                    "missed_slots":stats.missed_slots, "processed_updates":updates, "processed_queries":queries,
                })
            );
            let mut current = control.phase.lock().unwrap();
            if *current == Phase::Running {
                *current = Phase::Drained;
            }
        }
        if final_sample {
            drop(external.take());
            println!("{}", json!({"type":"benchmark_sealed","replica_id":id}));
            io::stdout().flush()?;
            *control.phase.lock().unwrap() = Phase::Sealed;
        }
        if !queue_busy {
            thread::sleep(POLL_INTERVAL);
        }
    }
}
