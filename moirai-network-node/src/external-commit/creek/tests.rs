use super::{
    paxos::{Ballot, Message, Paxos, Snapshot},
    *,
};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    commitment::{
        commit_protocols::omega::oracle::Omega,
        examples::commit_set::{CommitSet, SetUpdate},
    },
    crdt::query::Contains,
    replica::{IsReplica, Replica},
    state::po_log::VecLog,
};
use std::time::{Duration, Instant};

fn tick(node: &mut Paxos, state: &Snapshot, now: Instant) -> io::Result<()> {
    node.tick(state.clone(), now)
}

fn id(origin: u16) -> VertexId {
    VertexId { origin, seq: 1 }
}
fn snapshot(leader: u16, n: u16) -> Snapshot {
    Snapshot {
        leader,
        applied: 0,
        candidate: Some(id(leader)),
        delivered: (1..=n).map(|id| (id, 1)).collect(),
    }
}

#[test]
fn an_empty_tail_does_not_start_consensus_instances() {
    let now = Instant::now();
    let mut node = Paxos::new(1, vec![1], Duration::from_millis(10), now);
    let state = Snapshot {
        leader: 1,
        ..Snapshot::default()
    };
    for step in 0..10 {
        tick(&mut node, &state, now + Duration::from_millis(step * 10)).unwrap();
    }
    assert!(node.outgoing.is_empty());
    assert!(node.decisions.is_empty());
}

struct Cluster {
    nodes: Vec<Paxos>,
    snapshots: Vec<Snapshot>,
    online: Vec<bool>,
    now: Instant,
}
impl Cluster {
    fn new(n: u16) -> Self {
        let now = Instant::now();
        Self {
            nodes: (1..=n)
                .map(|own| Paxos::new(own, (1..=n).collect(), Duration::from_millis(10), now))
                .collect(),
            snapshots: (1..=n).map(|_| snapshot(1, n)).collect(),
            online: vec![true; usize::from(n)],
            now,
        }
    }
    fn step(&mut self, mut allow: impl FnMut(u16, u16, &Message) -> bool) {
        self.now += Duration::from_millis(5);
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if self.online[i] {
                tick(node, &self.snapshots[i], self.now).unwrap();
            }
        }
        for _ in 0..50 {
            let mut packets = Vec::new();
            for (i, node) in self.nodes.iter_mut().enumerate() {
                packets.extend(
                    node.outgoing
                        .drain(..)
                        .map(|(to, message)| ((i + 1) as u16, to, message)),
                );
            }
            if packets.is_empty() {
                break;
            }
            for (from, to, message) in packets {
                if self.online[usize::from(from - 1)]
                    && self.online[usize::from(to - 1)]
                    && allow(from, to, &message)
                {
                    self.nodes[usize::from(to - 1)]
                        .receive(from, message)
                        .unwrap();
                }
            }
        }
        for (node, snapshot) in self.nodes.iter().zip(&mut self.snapshots) {
            while let Some(vertex) = node.decisions.get(&(snapshot.applied + 1)) {
                snapshot.applied += 1;
                if snapshot.candidate == Some(*vertex) {
                    snapshot.candidate = None;
                }
            }
        }
    }
}

#[test]
fn duplicate_replies_do_not_form_a_quorum() {
    let now = Instant::now();
    let mut node = Paxos::new(1, vec![1, 2, 3], Duration::from_secs(1), now);
    tick(&mut node, &snapshot(1, 3), now).unwrap();
    let ballot = Ballot {
        round: 1,
        leader: 1,
    };
    for _ in 0..5 {
        node.receive(
            1,
            Message::Promise {
                slot: 1,
                ballot,
                accepted: None,
            },
        )
        .unwrap();
    }
    tick(&mut node, &snapshot(1, 3), now).unwrap();
    assert!(
        !node
            .outgoing
            .iter()
            .any(|(_, m)| matches!(m, Message::Accept { .. }))
    );
    node.receive(
        2,
        Message::Promise {
            slot: 1,
            ballot,
            accepted: None,
        },
    )
    .unwrap();
    tick(&mut node, &snapshot(1, 3), now).unwrap();
    for _ in 0..5 {
        node.receive(
            1,
            Message::Accepted {
                slot: 1,
                ballot,
                vertex: id(1),
            },
        )
        .unwrap();
    }
    assert!(node.decisions.is_empty());
    node.receive(
        3,
        Message::Accepted {
            slot: 1,
            ballot,
            vertex: id(1),
        },
    )
    .unwrap();
    assert_eq!(node.decisions[&1], id(1));
}

#[test]
fn acceptors_wait_for_tcsb_and_reject_stale_ballots() {
    let now = Instant::now();
    let mut node = Paxos::new(3, vec![1, 2, 3], Duration::from_millis(10), now);
    let mut state = snapshot(1, 3);
    state.delivered.clear();
    tick(&mut node, &state, now).unwrap();
    node.outgoing.clear();
    let ballot = Ballot {
        round: 2,
        leader: 2,
    };
    node.receive(
        2,
        Message::Accept {
            slot: 1,
            ballot,
            vertex: id(1),
        },
    )
    .unwrap();
    assert!(node.outgoing.is_empty());
    state.delivered.insert(1, 1);
    tick(&mut node, &state, now).unwrap();
    node.outgoing.clear();
    node.receive(
        2,
        Message::Accept {
            slot: 1,
            ballot,
            vertex: id(1),
        },
    )
    .unwrap();
    assert!(matches!(
        node.outgoing.pop_front(),
        Some((2, Message::Accepted { .. }))
    ));
    node.receive(
        1,
        Message::Accept {
            slot: 1,
            ballot: Ballot {
                round: 1,
                leader: 1,
            },
            vertex: id(2),
        },
    )
    .unwrap();
    assert!(matches!(
        node.outgoing.pop_front(),
        Some((1, Message::Nack { .. }))
    ));
    assert!(
        node.receive(
            2,
            Message::Accept {
                slot: 1,
                ballot,
                vertex: id(2)
            }
        )
        .is_err()
    );
}

#[test]
fn replacement_leader_recovers_an_accepted_value_before_its_own_candidate() {
    let mut cluster = Cluster::new(3);
    let old = Ballot {
        round: 1,
        leader: 1,
    };
    tick(&mut cluster.nodes[1], &snapshot(1, 3), cluster.now).unwrap();
    // Together with the now-crashed leader, node 2 may already form a chosen
    // majority. The new leader must recover A, rather than substitute B.
    cluster.nodes[1]
        .receive(
            1,
            Message::Accept {
                slot: 1,
                ballot: old,
                vertex: id(1),
            },
        )
        .unwrap();
    cluster.nodes[1].outgoing.clear();
    cluster.online[0] = false;
    for state in &mut cluster.snapshots {
        state.leader = 2;
        state.candidate = Some(id(2));
    }
    for _ in 0..30 {
        cluster.step(|_, _, _| true);
    }
    for node in &cluster.nodes[1..] {
        assert_eq!(node.decisions[&1], id(1));
        assert_eq!(node.decisions[&2], id(2));
    }
}

#[test]
fn a_minority_cannot_commit_and_a_healed_partition_can() {
    let mut cluster = Cluster::new(4);
    cluster.online[2] = false;
    cluster.online[3] = false;
    for _ in 0..30 {
        cluster.step(|_, _, _| true);
    }
    assert!(cluster.nodes.iter().all(|node| node.decisions.is_empty()));
    cluster.online[2] = true;
    for _ in 0..30 {
        cluster.step(|_, _, _| true);
    }
    assert_eq!(cluster.nodes[0].decisions[&1], id(1));
}

#[test]
fn decision_journal_repairs_a_lost_announcement() {
    let mut cluster = Cluster::new(3);
    for _ in 0..15 {
        cluster.step(|_, to, message| {
            to != 3 || !matches!(message, Message::Decide { .. } | Message::Decisions { .. })
        });
    }
    assert_eq!(cluster.nodes[0].decisions[&1], id(1));
    assert!(cluster.nodes[2].decisions.is_empty());
    for _ in 0..20 {
        cluster.step(|_, _, _| true);
    }
    assert_eq!(cluster.nodes[2].decisions[&1], id(1));
}

#[test]
fn reordered_decisions_and_duplicates_preserve_the_prefix() {
    let now = Instant::now();
    let mut node = Paxos::new(3, vec![1, 2, 3], Duration::from_millis(10), now);
    node.receive(
        2,
        Message::Decide {
            slot: 2,
            vertex: id(2),
        },
    )
    .unwrap();
    node.receive(
        1,
        Message::Decide {
            slot: 1,
            vertex: id(1),
        },
    )
    .unwrap();
    node.receive(
        1,
        Message::Decide {
            slot: 1,
            vertex: id(1),
        },
    )
    .unwrap();
    assert_eq!(node.decisions.len(), 2);
    assert!(
        node.receive(
            2,
            Message::Decide {
                slot: 1,
                vertex: id(2)
            }
        )
        .is_err()
    );
}

#[test]
fn changing_leaders_and_dropped_messages_never_choose_conflicting_values() {
    for seed in 1..=24u64 {
        let mut rng = seed;
        let mut random = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut cluster = Cluster::new(3);
        for step in 0..120 {
            for (i, state) in cluster.snapshots.iter_mut().enumerate() {
                state.leader = if step < 70 {
                    (random() % 3 + 1) as u16
                } else {
                    2
                };
                if step == 70 {
                    state.candidate = Some(id((i + 1) as u16));
                }
            }
            cluster.step(|_, _, _| step >= 70 || random() % 4 != 0);
            let mut chosen = BTreeMap::new();
            for node in &cluster.nodes {
                for (&slot, &value) in &node.decisions {
                    if let Some(previous) = chosen.insert(slot, value) {
                        assert_eq!(previous, value, "seed {seed}");
                    }
                }
            }
        }
        assert!(
            !cluster.nodes[1].decisions.is_empty(),
            "no progress after leader stabilization"
        );
        assert_eq!(cluster.nodes[0].decisions, cluster.nodes[1].decisions);
        assert_eq!(cluster.nodes[1].decisions, cluster.nodes[2].decisions);
    }
}

type Child = VecLog<CommitSet<String>>;
type Log = CommitmentLog<Child, Creek<Omega>>;
type TestReplica = Replica<Log, Tcsb<SetUpdate<String>>>;
fn replica(own: u16) -> TestReplica {
    let members = vec![
        (StableReplicaId(1), ReplicaId::from("A")),
        (StableReplicaId(2), ReplicaId::from("B")),
    ];
    let protocol = Creek::new(
        Omega::with_leader(StableReplicaId(1)),
        members,
        StableReplicaId(own),
    )
    .unwrap();
    Replica::bootstrap_with_state(
        if own == 1 { "A" } else { "B" }.into(),
        &["A", "B"],
        Log::new(Child::default(), protocol),
    )
}
fn learn(node: &mut TestReplica, slot: u64, vertex: VertexId) {
    node.state()
        .protocol()
        .shared
        .lock()
        .unwrap()
        .learned
        .push_back((slot, vertex));
}

fn poll(node: &mut TestReplica) {
    node.state_mut().protocol_mut().poll().unwrap();
    node.state_mut().commit_ready();
}

#[test]
fn ordered_concurrent_checkpoints_extend_the_cut_and_preserve_sequential_set_order() {
    let mut a = replica(1);
    let mut b = replica(2);
    let add = a.send(SetUpdate::Add("x".into())).unwrap();
    let remove = b.send(SetUpdate::Remove("x".into())).unwrap();
    a.receive(remove);
    b.receive(add);
    for node in [&mut a, &mut b] {
        learn(node, 2, id(1));
        learn(node, 1, id(2));
        poll(node);
        // CAB puts the concurrent removal before the insertion. Stabilization
        // must append these two batches, not globally re-sort their union.
        assert!(node.query(&Contains("x".to_owned())));
        assert_eq!(node.state().committed_frontier().unwrap().sum(), 2);
        assert!(node.state().child().unstable().is_empty());
        assert_eq!(node.state().protocol().applied_slot(), 2);
    }
}

#[test]
fn choosing_a_vertex_commits_its_entire_causal_past() {
    let mut a = replica(1);
    let mut b = replica(2);
    let add = a.send(SetUpdate::Add("x".into())).unwrap();
    b.receive(add);
    let remove = b.send(SetUpdate::Remove("x".into())).unwrap();
    a.receive(remove);
    for node in [&mut a, &mut b] {
        learn(node, 1, id(2));
        poll(node);
        assert_eq!(node.state().committed_frontier().unwrap().sum(), 2);
        assert!(!node.query(&Contains("x".to_owned())));
    }
}

#[test]
fn a_learned_id_waits_for_causal_delivery_and_needs_no_new_operation_to_commit() {
    let mut a = replica(1);
    let mut b = replica(2);
    let add = a.send(SetUpdate::Add("x".into())).unwrap();
    learn(&mut b, 1, id(1));
    poll(&mut b);
    assert!(b.state().committed_frontier().is_none());
    b.receive(add);
    poll(&mut b);
    assert!(b.query(&Contains("x".to_owned())));
    assert_eq!(b.state().protocol().applied_slot(), 1);
    assert_eq!(b.delivered_count(), 1);
}

#[test]
fn candidates_are_uncommitted_frontier_vertices_not_old_ancestors() {
    let mut a = replica(1);
    let mut b = replica(2);
    a.send(SetUpdate::Add("first".into())).unwrap();
    a.send(SetUpdate::Add("second".into())).unwrap();
    let remote = b.send(SetUpdate::NoOp).unwrap();
    a.receive(remote);
    poll(&mut a);
    assert_eq!(a.state().protocol().candidate(), Some(id(2)));
    learn(&mut a, 1, id(2));
    poll(&mut a);
    poll(&mut a);
    assert_eq!(
        a.state().protocol().candidate(),
        Some(VertexId { origin: 1, seq: 2 })
    );
}

#[test]
fn applied_decisions_leave_the_owner_pending_buffer() {
    let mut a = replica(1);
    a.send(SetUpdate::NoOp).unwrap();
    learn(&mut a, 1, id(1));
    poll(&mut a);
    let protocol = a.state().protocol();
    assert_eq!(protocol.applied_slot(), 1);
    assert!(protocol.versions.is_empty());
    assert!(protocol.decisions.is_empty());
    assert!(protocol.ready.is_empty());
}

fn worker_config() -> WorkerConfig {
    WorkerConfig {
        retry_interval: Duration::from_millis(10),
        poll_interval: Duration::from_millis(1),
    }
}

#[test]
fn retry_interval_must_be_nonzero() {
    let a = replica(1);
    let config = WorkerConfig {
        retry_interval: Duration::ZERO,
        ..WorkerConfig::default()
    };
    assert!(
        a.state()
            .protocol()
            .spawn(
                Memory {
                    own: 1,
                    bus: Arc::default()
                },
                config
            )
            .is_err()
    );
}

#[derive(Clone)]
struct Memory {
    own: u16,
    bus: Arc<Mutex<BTreeMap<u16, Vec<crate::Packet>>>>,
}
impl Transport for Memory {
    fn poll(&mut self) -> io::Result<Vec<crate::Packet>> {
        Ok(self
            .bus
            .lock()
            .unwrap()
            .remove(&self.own)
            .unwrap_or_default())
    }
    fn send(&mut self, peer: u16, bytes: Arc<[u8]>) -> io::Result<()> {
        self.bus
            .lock()
            .unwrap()
            .entry(peer)
            .or_default()
            .push(crate::Packet {
                peer: self.own,
                bytes: bytes.to_vec(),
            });
        Ok(())
    }
}
fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "Creek worker failed to make progress"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn consensus_runs_in_parallel_while_both_replica_owners_are_paused() {
    let mut a = replica(1);
    let mut b = replica(2);
    let message = a.send(SetUpdate::Add("parallel".into())).unwrap();
    b.receive(message);
    // Publish delivery metadata before pausing both owners.
    poll(&mut a);
    poll(&mut b);
    let bus = Arc::new(Mutex::new(BTreeMap::new()));
    let _a_worker = a
        .state()
        .protocol()
        .spawn(
            Memory {
                own: 1,
                bus: bus.clone(),
            },
            worker_config(),
        )
        .unwrap();
    let b_transport = Memory { own: 2, bus };
    let b_worker = b
        .state()
        .protocol()
        .spawn(b_transport.clone(), worker_config())
        .unwrap();
    // Neither owner polls the protocol or runs a node tick. Both independent
    // workers must receive, vote, and learn using already delivered metadata.
    eventually(|| {
        [&a, &b].iter().all(|node| {
            node.state()
                .protocol()
                .shared
                .lock()
                .unwrap()
                .learned
                .contains(&(1, id(1)))
        })
    });
    assert!(a.state().committed_frontier().is_none());
    assert!(b.state().committed_frontier().is_none());
    for node in [&mut a, &mut b] {
        poll(node);
        assert_eq!(node.state().protocol().applied_slot(), 1);
        assert!(node.query(&Contains("parallel".to_owned())));
    }
    drop(b_worker);
    assert!(b.state_mut().protocol_mut().poll().is_err());
    assert!(
        b.state()
            .protocol()
            .spawn(b_transport, worker_config())
            .is_err()
    );
}

#[test]
fn paxos_processing_and_sends_do_not_hold_the_handoff_mutex() {
    use std::sync::mpsc;
    struct Gated {
        entered: Option<mpsc::Sender<()>>,
        resume: mpsc::Receiver<()>,
        sent: mpsc::Sender<Message>,
    }
    impl Transport for Gated {
        fn poll(&mut self) -> io::Result<Vec<crate::Packet>> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).map_err(io::Error::other)?;
                self.resume
                    .recv_timeout(Duration::from_secs(3))
                    .map_err(io::Error::other)?;
            }
            Ok(Vec::new())
        }
        fn send(&mut self, _: u16, bytes: Arc<[u8]>) -> io::Result<()> {
            self.sent
                .send(super::wire::decode(&bytes)?)
                .map_err(io::Error::other)
        }
    }
    let mut node = replica(1);
    node.send(SetUpdate::NoOp).unwrap();
    poll(&mut node);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (sent_tx, sent_rx) = mpsc::channel();
    let worker = node
        .state()
        .protocol()
        .spawn(
            Gated {
                entered: Some(entered_tx),
                resume: resume_rx,
                sent: sent_tx,
            },
            worker_config(),
        )
        .unwrap();
    // Pause transport polling after the worker has copied its input snapshot.
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let handoff = node.state().protocol().shared.lock().unwrap();
    resume_tx.send(()).unwrap();
    // Processing self-votes and sending the proposal must remain possible even
    // while the owner holds the handoff. A mutex around Paxos would block here.
    loop {
        if matches!(
            sent_rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            Message::Prepare { .. }
        ) {
            break;
        }
    }
    drop(handoff);
    drop(worker);
}

struct FailingTransport(bool);
impl Transport for FailingTransport {
    fn poll(&mut self) -> io::Result<Vec<crate::Packet>> {
        if self.0 {
            panic!("test transport panic")
        }
        Err(io::Error::other("test transport failure"))
    }
    fn send(&mut self, _: u16, _: Arc<[u8]>) -> io::Result<()> {
        unreachable!()
    }
}

#[test]
fn worker_failures_and_panics_are_reported_to_the_owner() {
    for panic in [false, true] {
        let mut node = replica(1);
        let _worker = node
            .state()
            .protocol()
            .spawn(FailingTransport(panic), worker_config())
            .unwrap();
        eventually(|| node.state_mut().protocol_mut().poll().is_err());
        let error = node
            .state_mut()
            .protocol_mut()
            .poll()
            .unwrap_err()
            .to_string();
        assert!(error.contains(if panic {
            "panicked"
        } else {
            "test transport failure"
        }));
    }
}

#[test]
fn dropping_the_replica_stops_its_worker() {
    let node = replica(1);
    let shared = node.state().protocol().shared.clone();
    let _worker = node
        .state()
        .protocol()
        .spawn(
            Memory {
                own: 1,
                bus: Arc::default(),
            },
            worker_config(),
        )
        .unwrap();
    drop(node);
    eventually(|| shared.lock().unwrap().error.is_some());
}

#[test]
fn consensus_tcp_receives_while_the_causal_tcp_channels_are_not_polled() {
    use crate::{Application, Config};
    use std::net::TcpListener;
    let reservations: Vec<_> = (0..2)
        .map(|_| {
            loop {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let base = listener.local_addr().unwrap().port();
                if let Some(consensus) = base
                    .checked_add(1)
                    .and_then(|p| TcpListener::bind(("127.0.0.1", p)).ok())
                {
                    break (listener, consensus);
                }
            }
        })
        .collect();
    let ports: Vec<_> = reservations
        .iter()
        .map(|(base, _)| base.local_addr().unwrap().port())
        .collect();
    let configs: Vec<Config> = (0..2)
        .map(|i| {
            serde_json::from_value(serde_json::json!({
                "replica_id": i + 1, "listen_port": ports[i],
                "peers": [{"replica_id": 2-i, "address": format!("127.0.0.1:{}", ports[1-i])}],
                "noop_interval_ms": null, "memory_usage_enabled": false
            }))
            .unwrap()
        })
        .collect();
    drop(reservations);
    let app = || Application::<Log> {
        query: |_| serde_json::Value::Null,
        query_stable: |_| serde_json::Value::Null,
        committed: |replica, id| {
            replica
                .state()
                .committed_frontier()
                .is_some_and(|cut| cut.contains(id))
        },
        no_op: || SetUpdate::NoOp,
    };
    let make = |config: &Config| {
        let protocol = Creek::new(
            Omega::with_leader(StableReplicaId(1)),
            config.members(),
            StableReplicaId(config.replica_id),
        )
        .unwrap();
        Node::new(config.clone(), Log::new(Child::default(), protocol), app()).unwrap()
    };
    let mut a = make(&configs[0]);
    let mut b = make(&configs[1]);
    // Deliver causal data explicitly during setup. From now on the normal TCP
    // listeners and both node loops remain idle while Paxos uses its own sockets.
    let message = a.replica_mut().send(SetUpdate::NoOp).unwrap();
    b.replica_mut().receive(message);
    drive(&mut a).unwrap();
    drive(&mut b).unwrap();
    let _a_worker = a
        .replica()
        .state()
        .protocol()
        .spawn_tcp(&configs[0], worker_config())
        .unwrap();
    let _b_worker = b
        .replica()
        .state()
        .protocol()
        .spawn_tcp(&configs[1], worker_config())
        .unwrap();
    eventually(|| {
        [&a, &b].iter().all(|node| {
            node.replica()
                .state()
                .protocol()
                .shared
                .lock()
                .unwrap()
                .learned
                .contains(&(1, id(1)))
        })
    });
    assert!(a.replica().state().committed_frontier().is_none());
    assert!(b.replica().state().committed_frontier().is_none());
    drive(&mut a).unwrap();
    drive(&mut b).unwrap();
    assert_eq!(a.replica().state().protocol().applied_slot(), 1);
    assert_eq!(b.replica().state().protocol().applied_slot(), 1);
}
