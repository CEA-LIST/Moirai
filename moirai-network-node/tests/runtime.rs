use moirai_network_node::{
    Application, Config, Node, Packet, Transport, transport::MAX_FRAME_BYTES,
};
use moirai_protocol::{
    commitment::{
        commit_log::CommitmentLog,
        commit_protocols::{
            lamport::{lamport_commit::LamportCommit, lamport_tracker::LamportTracker},
            omega::{omega_commit::OmegaCommit, oracle::EventOracle},
        },
        examples::commit_set::{CommitSet, SetUpdate},
        protocol::AtomicBroadcastProtocol,
        wire_id::StableReplicaId,
    },
    crdt::query::Read,
    replica::IsReplica,
    state::po_log::VecLog,
};
use serde_json::json;
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashSet},
    io,
    rc::Rc,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

type Child = VecLog<CommitSet<String>>;
type Plain = CommitmentLog<Child, LamportCommit>;
type Commit = CommitmentLog<Child, OmegaCommit<EventOracle>>;
type Bus = Rc<RefCell<BTreeMap<u16, Vec<Packet>>>>;

struct MemoryTransport {
    id: u16,
    bus: Bus,
    drop: Rc<Cell<bool>>,
}
impl Transport for MemoryTransport {
    fn poll(&mut self) -> io::Result<Vec<Packet>> {
        Ok(self.bus.borrow_mut().remove(&self.id).unwrap_or_default())
    }
    fn send(&mut self, peer: u16, bytes: Arc<[u8]>) -> io::Result<()> {
        if !self.drop.get() {
            self.bus.borrow_mut().entry(peer).or_default().push(Packet {
                peer: self.id,
                bytes: bytes.to_vec(),
            });
        }
        Ok(())
    }
}

fn config(id: u16, members: &[u16], noop: Option<u64>) -> Config {
    serde_json::from_value(json!({"replica_id":id,"listen_port":0,"peers":members.iter().filter(|&&p| p != id).map(|p| json!({"replica_id":p,"address":"127.0.0.1:1"})).collect::<Vec<_>>(),
        "pull_interval_ms":5,"pull_peer_count":2,"noop_interval_ms":noop,"memory_usage_enabled":false,"request_timeout_ms":500})).unwrap()
}

fn application<A: AtomicBroadcastProtocol<Child>>() -> Application<CommitmentLog<Child, A>> {
    Application {
        query: |replica| {
            let values: HashSet<String, _> = replica.query(&Read::new());
            let mut values: Vec<_> = values.into_iter().collect();
            values.sort();
            json!(values)
        },
        committed: |replica, id| {
            replica
                .state()
                .committed_frontier()
                .is_some_and(|f| f.contains(id))
        },
        no_op: || SetUpdate::NoOp,
    }
}

fn plain(id: u16, members: &[u16], noop: Option<u64>, bus: &Bus) -> Node<Plain, MemoryTransport> {
    let config = config(id, members, noop);
    let protocol = LamportCommit::from_parts(config.members(), LamportTracker::default());
    let transport = MemoryTransport {
        id,
        bus: bus.clone(),
        drop: Rc::default(),
    };
    Node::with_transport(
        config,
        Plain::new(Child::default(), protocol),
        application(),
        transport,
    )
    .unwrap()
}

fn commit(id: u16, members: &[u16], bus: &Bus) -> Node<Commit, MemoryTransport> {
    let config = config(id, members, Some(5));
    let oracle = EventOracle::from_parts(StableReplicaId(id), 30, Default::default(), None);
    let protocol = OmegaCommit::from_parts(oracle, config.members(), StableReplicaId(id));
    Node::with_transport(
        config,
        Commit::new(Child::default(), protocol),
        application(),
        MemoryTransport {
            id,
            bus: bus.clone(),
            drop: Rc::default(),
        },
    )
    .unwrap()
}

fn eventually(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if check() {
            break;
        }
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn pulls_repair_a_dropped_push_and_deliver_concurrent_updates() {
    let bus = Bus::default();
    let mut a = plain(2, &[2, 10], None, &bus);
    let mut b = plain(10, &[2, 10], None, &bus);
    a.transport().drop.set(true);
    a.broadcast(SetUpdate::Add("apple".into())).unwrap();
    a.transport().drop.set(false);
    b.broadcast(SetUpdate::Add("pear".into())).unwrap();
    eventually(|| {
        a.tick().unwrap();
        b.tick().unwrap();
        a.replica().delivered_count() == 2 && b.replica().delivered_count() == 2
    });
    for node in [&a, &b] {
        assert_eq!(
            (application().query)(node.replica()),
            json!(["apple", "pear"])
        );
    }
}

#[test]
fn lamport_strong_update_waits_then_idle_noops_advance_commitment() {
    let bus = Bus::default();
    let mut a = plain(1, &[1, 2, 3], Some(5), &bus);
    let mut b = plain(2, &[1, 2, 3], Some(5), &bus);
    let mut c = plain(3, &[1, 2, 3], Some(5), &bus);
    let response = a.handle().update(SetUpdate::Add("x".into()), true).unwrap();
    a.tick().unwrap();
    assert!(response.try_recv().is_err());
    eventually(|| {
        a.tick().unwrap();
        b.tick().unwrap();
        c.tick().unwrap();
        match response.try_recv() {
            Ok(result) => {
                assert!(result.success);
                true
            }
            Err(_) => false,
        }
    });
    for node in [&a, &b, &c] {
        assert_eq!((application().query)(node.replica()), json!(["x"]));
    }
}

#[test]
fn event_oracle_commits_with_a_majority_after_a_leader_stops() {
    let bus = Bus::default();
    let mut a = commit(1, &[1, 2, 3], &bus);
    let mut b = commit(2, &[1, 2, 3], &bus);
    let mut c = commit(3, &[1, 2, 3], &bus);
    let first = b
        .handle()
        .update(SetUpdate::Add("before".into()), true)
        .unwrap();
    eventually(|| {
        a.tick().unwrap();
        b.tick().unwrap();
        c.tick().unwrap();
        match first.try_recv() {
            Ok(result) => {
                assert!(result.success);
                true
            }
            Err(_) => false,
        }
    });
    drop(a);
    // Old leader events drain, then expire inside the core EventOracle.
    let response = c
        .handle()
        .update(SetUpdate::Add("after".into()), true)
        .unwrap();
    eventually(|| {
        b.tick().unwrap();
        c.tick().unwrap();
        match response.try_recv() {
            Ok(result) => {
                assert!(result.success, "{}", result.message);
                true
            }
            Err(_) => false,
        }
    });
    assert_eq!(
        (application().query)(b.replica()),
        json!(["after", "before"])
    );
    assert_eq!(
        (application().query)(c.replica()),
        json!(["after", "before"])
    );
}

#[test]
fn weak_updates_work_while_a_strong_update_times_out_without_rollback() {
    let bus = Bus::default();
    let mut a = plain(1, &[1, 2], None, &bus);
    let handle = a.handle();
    let strong = handle
        .update(SetUpdate::Add("strong".into()), true)
        .unwrap();
    let weak = handle.update(SetUpdate::Add("weak".into()), false).unwrap();
    let state = handle.state().unwrap();
    a.tick().unwrap();
    assert!(weak.try_recv().unwrap().success);
    assert_eq!(
        state.try_recv().unwrap()["state"],
        json!(["strong", "weak"])
    );
    eventually(|| {
        a.tick().unwrap();
        match strong.try_recv() {
            Ok(result) => {
                assert!(!result.success);
                assert!(result.applied);
                true
            }
            Err(_) => false,
        }
    });
    assert_eq!(
        (application().query)(a.replica()),
        json!(["strong", "weak"])
    );
}

#[test]
fn noop_timer_resets_on_client_updates_and_can_be_disabled() {
    let bus = Bus::default();
    let mut a = plain(1, &[1], Some(40), &bus);
    let mut disabled = plain(2, &[2], None, &bus);
    for _ in 0..4 {
        let response = a
            .handle()
            .update(SetUpdate::Add("x".into()), false)
            .unwrap();
        a.tick().unwrap();
        disabled.tick().unwrap();
        assert!(response.try_recv().unwrap().success);
        thread::sleep(Duration::from_millis(15));
    }
    assert_eq!(a.replica().delivered_count(), 4);
    eventually(|| {
        a.tick().unwrap();
        a.replica().delivered_count() > 4
    });
    disabled.tick().unwrap();
    assert_eq!(disabled.replica().delivered_count(), 0);
}

#[test]
fn oversized_updates_are_rejected_before_local_application() {
    let bus = Bus::default();
    let mut a = plain(1, &[1], None, &bus);
    let response = a
        .handle()
        .update(SetUpdate::Add("x".repeat(MAX_FRAME_BYTES)), false)
        .unwrap();
    a.tick().unwrap();
    let result = response.try_recv().unwrap();
    assert!(!result.success);
    assert!(!result.applied);
    assert_eq!(a.replica().delivered_count(), 0);
}

#[test]
fn run_calls_a_capturing_external_protocol_hook_and_stops_cooperatively() {
    let bus = Bus::default();
    let mut a = plain(1, &[1], None, &bus);
    let mut calls = 0;
    a.run(Some(&mut |node| {
        calls += 1;
        node.broadcast(SetUpdate::Add("from-hook".into()))?;
        node.handle().stop();
        Ok(())
    }))
    .unwrap();
    assert_eq!(calls, 1);
    assert_eq!((application().query)(a.replica()), json!(["from-hook"]));
    assert!(a.handle().update(SetUpdate::NoOp, false).is_err());
}

#[test]
fn dropping_a_node_resolves_pending_commitment_waits() {
    let bus = Bus::default();
    let mut a = plain(1, &[1, 2], None, &bus);
    let response = a.handle().update(SetUpdate::NoOp, true).unwrap();
    a.tick().unwrap();
    drop(a);
    let result = response.recv_timeout(Duration::from_millis(50)).unwrap();
    assert!(!result.success);
    assert!(result.applied);
}

#[test]
fn configuration_validates_ids_timers_and_legacy_launcher_fields() {
    let legacy = json!({"replica_id":2,"listen_port":0,"peers":[],"oracle_mode":"event","heartbeat_interval_ms":42,"heartbeat_peer_count":1});
    let parsed: Config = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(parsed.http_optimistic_limit.get(), 64);
    assert_eq!(parsed.http_consistent_limit.get(), 64);
    parsed.validate().unwrap();
    assert_eq!(parsed.event_timeout_ms.get(), 42);
    assert_eq!(parsed.noop_interval_ms.unwrap().get(), 1000);
    for (field, value) in [
        ("replica_id", json!(65536)),
        ("noop_interval_ms", json!(0)),
        ("pull_interval_ms", json!(0)),
        ("http_optimistic_limit", json!(0)),
        ("http_consistent_limit", json!(0)),
    ] {
        let mut invalid = legacy.clone();
        invalid[field] = value;
        assert!(serde_json::from_value::<Config>(invalid).is_err());
    }
    let duplicate = config(2, &[2, 2], None);
    // The helper omits self IDs; construct an explicitly invalid membership.
    let mut invalid = duplicate;
    invalid.peers.push(moirai_network_node::PeerConfig {
        replica_id: 2,
        replica_name: String::new(),
        address: "127.0.0.1:1".into(),
    });
    assert!(invalid.validate().is_err());
}
