//! Actual node loops and parallel consensus workers on derived TCP port pairs.
use moirai_network_node::{
    Application, Config, Node, NodeHandle,
    external_commit::creek::{self, Creek, WorkerConfig},
};
use moirai_protocol::{
    commitment::{
        commit_log::CommitmentLog,
        commit_protocols::omega::oracle::EventOracle,
        examples::commit_set::{CommitSet, SetUpdate},
        wire_id::StableReplicaId,
    },
    crdt::query::Read,
    replica::IsReplica,
    state::po_log::VecLog,
};
use serde_json::json;
use std::{
    collections::HashSet,
    io,
    net::TcpListener,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::Duration,
};

type Update = SetUpdate<String>;
type Log = CommitmentLog<VecLog<CommitSet<String>>, Creek<EventOracle>>;
struct Cluster {
    handles: Vec<NodeHandle<Update>>,
    threads: Vec<Option<JoinHandle<io::Result<usize>>>>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.stop();
        }
        for thread in &mut self.threads {
            if let Some(thread) = thread.take() {
                let _ = thread.join();
            }
        }
    }
}
impl Cluster {
    fn start(n: usize, noops: Option<u64>) -> Self {
        let reservations: Vec<_> = (0..n)
            .map(|_| {
                loop {
                    let causal = TcpListener::bind("127.0.0.1:0").unwrap();
                    let port = causal.local_addr().unwrap().port();
                    if let Some(consensus) = port
                        .checked_add(1)
                        .and_then(|p| TcpListener::bind(("127.0.0.1", p)).ok())
                    {
                        break (causal, consensus);
                    }
                }
            })
            .collect();
        let ports: Vec<_> = reservations
            .iter()
            .map(|(listener, _)| listener.local_addr().unwrap().port())
            .collect();
        drop(reservations);
        let (tx, rx) = mpsc::channel();
        let mut threads = Vec::new();
        for index in 0..n {
            let ports = ports.clone();
            let tx = tx.clone();
            threads.push(Some(thread::spawn(move || {
                let config: Config = serde_json::from_value(json!({"replica_id":index+1, "listen_port":ports[index],
                    "peers":(0..n).filter(|i| *i != index).map(|i| json!({"replica_id":i+1, "address":format!("127.0.0.1:{}", ports[i])})).collect::<Vec<_>>(),
                    "pull_interval_ms":10, "pull_peer_count":n, "noop_interval_ms":noops,
                    "request_timeout_ms":2500, "memory_usage_enabled":false})).unwrap();
                let protocol = Creek::new(EventOracle::with_timeout(StableReplicaId((index+1) as u16), Duration::from_millis(80)), config.members(), StableReplicaId((index+1) as u16))?;
                let _worker = protocol.spawn_tcp(&config, WorkerConfig { retry_interval: Duration::from_millis(20), ..WorkerConfig::default() })?;
                let application: Application<Log> = Application {
                    query: |replica| {
                        let values: HashSet<String, _> = replica.query(&Read::new());
                        let mut values: Vec<_> = values.into_iter().collect(); values.sort(); json!(values)
                    },
                    committed: |replica, id| replica.state().committed_frontier().is_some_and(|cut| cut.contains(id)),
                    no_op: || SetUpdate::NoOp,
                };
                let mut node = Node::new(config, Log::new(VecLog::default(), protocol), application)?;
                tx.send((index, node.handle())).unwrap();
                node.run(Some(&mut creek::drive))?;
                Ok(node.replica().delivered_count())
            })));
        }
        let mut handles: Vec<Option<NodeHandle<Update>>> = (0..n).map(|_| None).collect();
        for _ in 0..n {
            let (index, handle) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            handles[index] = Some(handle);
        }
        Self {
            handles: handles.into_iter().map(Option::unwrap).collect(),
            threads,
        }
    }
    fn stop(&mut self, index: usize) -> usize {
        self.handles[index].stop();
        self.threads[index].take().unwrap().join().unwrap().unwrap()
    }
    fn strong(&self, index: usize, value: &str) {
        let result = self.handles[index]
            .update(SetUpdate::Add(value.into()), true)
            .unwrap()
            .recv_timeout(Duration::from_secs(4))
            .unwrap();
        assert!(result.success, "{}", result.message);
    }
}

#[test]
fn run_hook_commits_a_single_operation_without_periodic_noops_or_new_requests() {
    let mut cluster = Cluster::start(1, None);
    cluster.strong(0, "idle");
    assert_eq!(
        cluster.handles[0]
            .state()
            .unwrap()
            .recv_timeout(Duration::from_secs(1))
            .unwrap()["state"],
        json!(["idle"])
    );
    assert_eq!(cluster.stop(0), 1);
}

#[test]
fn three_tcp_nodes_commit_after_the_omega_leader_exits() {
    let mut cluster = Cluster::start(3, Some(20));
    cluster.strong(0, "before");
    cluster.stop(0);
    cluster.strong(1, "after");
    for index in 1..3 {
        assert_eq!(
            cluster.handles[index]
                .state()
                .unwrap()
                .recv_timeout(Duration::from_secs(1))
                .unwrap()["state"],
            json!(["after", "before"])
        );
    }
}
