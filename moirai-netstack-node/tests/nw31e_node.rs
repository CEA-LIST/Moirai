//! NW.31e oracle: moirai-netstack-node's Node<F, T>, hosting and intake (vault
//! spec/items/NW.31e.md, NW.31 design row S5a).
//!
//! The 17 hosting and intake N tests of 601c887's moirai-network/src/generic.rs,
//! ported by name onto the toy replica of moirai-netstack-testkit: no op type, no
//! Moirai. Where 601c887 made a frame with a bare LogReplica, a second node makes
//! it here, so the event and batch bodies stay the node's own; where it read a
//! list CRDT, the toy's refusal (a non-string op) stands in. mp4 and the batch
//! route read the node directly: HTTP is S6. One more test drives three nodes over
//! DirectTransport on 127.0.0.1.
//!
//! Catch-up, transfer, late joiner, paused peer and returning member are S5b and
//! are not here.
//!
//! flock ~/.cache/cea-crdt-scratch/cargo.lock flock ~/.cache/cea-crdt-scratch/nodes.lock \
//!   nice -n 10 cargo test -j 2 -p moirai-netstack-node --locked --test nw31e_node

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use moirai_netstack::{LogId, LogPayload, ReplicaPort, TransportMessage};
use moirai_netstack_node::{BatchOutcome, Node};
use moirai_netstack_testkit::ToyFactory;
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::transport::{CrdtTransport, PeerId, PeerInfo, TransportResult};
use serde_json::{Value, json};

/// Records everything sent and broadcast, and receives nothing.
struct RecordingTransport {
    id: PeerId,
    sent: Vec<(PeerId, TransportMessage)>,
    broadcast: Vec<TransportMessage>,
}

impl RecordingTransport {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            sent: Vec::new(),
            broadcast: Vec::new(),
        }
    }
}

impl CrdtTransport for RecordingTransport {
    fn local_id(&self) -> &PeerId {
        &self.id
    }
    fn send(&mut self, peer: &PeerId, msg: TransportMessage) -> TransportResult<()> {
        self.sent.push((peer.clone(), msg));
        Ok(())
    }
    fn broadcast(&mut self, msg: TransportMessage) -> TransportResult<()> {
        self.broadcast.push(msg);
        Ok(())
    }
    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage)>> {
        Ok(None)
    }
    fn peers(&self) -> Vec<PeerInfo> {
        Vec::new()
    }
    fn is_connected(&self, _peer: &PeerId) -> bool {
        false
    }
    fn pause_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        Ok(())
    }
    fn resume_peer(&mut self, _peer: &PeerId) -> TransportResult<()> {
        Ok(())
    }
    fn pause_all(&mut self) -> TransportResult<()> {
        Ok(())
    }
    fn resume_all(&mut self) -> TransportResult<()> {
        Ok(())
    }
    fn buffered_count(&self, _peer: &PeerId) -> usize {
        0
    }
    fn accept_connections(&mut self) -> TransportResult<Vec<PeerId>> {
        Ok(Vec::new())
    }
}

type TestNode = Node<ToyFactory, RecordingTransport>;

fn bt() -> LogId {
    LogId::parse("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2").unwrap()
}

fn uml() -> LogId {
    LogId::parse("c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d4").unwrap()
}

/// A node hosting `logs`, the first of them as its default log.
fn node(id: &str, logs: &[LogId]) -> TestNode {
    let (first, rest) = logs.split_first().expect("a node hosts at least one log");
    let mut node = Node::new(
        ToyFactory,
        id.to_string(),
        &[id],
        RecordingTransport::new(id),
        first.clone(),
    );
    for log in rest {
        node.host_log(log.clone()).expect("a fresh id");
    }
    node
}

/// The elements of a hosted log, sorted.
fn members<T: CrdtTransport>(node: &Node<ToyFactory, T>, log: &LogId) -> BTreeSet<String> {
    let state = node
        .hosted(log)
        .unwrap_or_else(|| panic!("{log} is hosted"))
        .read_state();
    state
        .as_array()
        .unwrap_or_else(|| panic!("the toy reads out an array, got {state}"))
        .iter()
        .map(|e| e.as_str().expect("string elements").to_string())
        .collect()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// The per-log counter of remote input a log's replica refused.
fn refusals<T: CrdtTransport>(node: &Node<ToyFactory, T>, log: &LogId) -> u64 {
    let metrics = node
        .log_metrics(log)
        .unwrap_or_else(|| panic!("{log} is hosted"));
    metrics["foreign_log_refusals"]
        .as_u64()
        .unwrap_or_else(|| panic!("no foreign_log_refusals in {metrics}"))
}

fn take_broadcast(node: &mut TestNode) -> Vec<TransportMessage> {
    std::mem::take(&mut node.transport_mut().broadcast)
}

/// The frames one write on a fresh node hosting `log` broadcasts.
fn written(writer: &str, log: LogId, element: &str) -> Vec<TransportMessage> {
    let mut w = node(writer, &[log]);
    assert!(w.apply_op(json!(element)).success);
    take_broadcast(&mut w)
}

/// Hand every frame `from` broadcast since the last call to `to`.
fn carry(from: &mut TestNode, to: &mut TestNode) {
    let sender = from.transport().id.clone();
    for frame in take_broadcast(from) {
        to.handle_transport_message(sender.clone(), frame);
    }
}

/// The count after the last dot of a version, `<incarnation>.<delivered>`.
fn count(version: &str) -> u64 {
    version.rsplit('.').next().unwrap().parse().unwrap()
}

/// The log id a frame names, from the key the library owns in its body.
fn frame_log(frame: &TransportMessage) -> Option<String> {
    let body = match frame {
        TransportMessage::Event { event } => event,
        TransportMessage::Batch { batch } => batch,
        TransportMessage::SyncRequest { since } => since,
        _ => return None,
    };
    body.get("log_id")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// mp2's frames: one of every id-carrying variant, all for `uml`.
fn frames_for_an_unhosted_log() -> Vec<TransportMessage> {
    let mut writer = node("writer", &[uml()]);
    assert!(writer.apply_op(json!("Class")).success);
    let applied = writer.submit_batch(None, vec![json!("Property"), json!("Operation")], None);
    assert_eq!(
        applied.outcome,
        BatchOutcome::Applied,
        "{}",
        applied.message
    );
    let mut frames = take_broadcast(&mut writer);
    assert!(
        matches!(
            frames.as_slice(),
            [
                TransportMessage::Event { .. },
                TransportMessage::Batch { .. }
            ]
        ),
        "a write and a two-operation batch: {frames:?}"
    );
    frames.push(TransportMessage::SyncRequest {
        since: json!({ "log_id": uml().as_str() }),
    });
    frames.push(TransportMessage::StateRequest {
        id: "reader".to_string(),
        log_id: uml(),
    });
    frames.push(TransportMessage::StateResponse {
        snapshot: json!({}),
        log: LogPayload::Plain(Value::Null),
        log_id: uml(),
    });
    frames
}

#[test]
fn a_state_request_for_a_foreign_log_is_refused_naming_both_ids() {
    let ours = LogId::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let theirs = LogId::parse("ffffffffffffffffffffffffffffffff").unwrap();
    let mut node = Node::new(
        ToyFactory,
        "a".to_string(),
        &["a"],
        RecordingTransport::new("a"),
        ours.clone(),
    );
    assert_eq!(node.log_id(), &ours, "the node hosts the log it was given");

    node.handle_transport_message(
        "b".to_string(),
        TransportMessage::StateRequest {
            id: "b".to_string(),
            log_id: theirs.clone(),
        },
    );

    let (to, answer) = node
        .transport_mut()
        .sent
        .pop()
        .expect("the request was answered");
    assert_eq!(to, "b");
    match answer {
        TransportMessage::StateUnavailable { reason, log_id } => {
            assert_eq!(log_id, ours, "the refusal is stamped with the donor's log");
            assert!(
                reason.contains(ours.as_str()) && reason.contains(theirs.as_str()),
                "the refusal must name both logs, got: {reason}"
            );
        }
        other => panic!("expected StateUnavailable, got {other:?}"),
    }
}

#[test]
fn mp1_dispatch_hands_a_frame_to_the_log_whose_id_matches() {
    let mut node = node("n", &[bt(), uml()]);
    let bt_before = (
        members(&node, &bt()),
        node.hosted(&bt()).unwrap().progress(),
    );

    let frames = written("editor", uml(), "Class");
    assert_eq!(frames.len(), 1, "one write, one frame: {frames:?}");
    for frame in frames {
        node.handle_transport_message("editor".to_string(), frame);
    }

    assert_eq!(
        members(&node, &uml()),
        set(&["Class"]),
        "the Class did not land in the UML model"
    );
    assert_eq!(
        members(&node, &bt()),
        set(&[]),
        "the Class landed in the behaviour tree"
    );
    assert_eq!(
        (
            members(&node, &bt()),
            node.hosted(&bt()).unwrap().progress()
        ),
        bt_before,
        "the behaviour tree changed"
    );
    assert_eq!(refusals(&node, &bt()), 0);
    assert_eq!(refusals(&node, &uml()), 0);
    assert_eq!(node.frames_not_hosted(), 0);
}

#[test]
fn mp2_a_frame_for_an_unhosted_log_is_filtered_and_counted() {
    let mut node = node("n", &[bt()]);
    let bt_before = node.hosted(&bt()).unwrap().progress();
    let frames = frames_for_an_unhosted_log();
    let expected = frames.len() as u64;

    for frame in frames {
        node.handle_transport_message("writer".to_string(), frame);
    }

    assert!(!node.hosts(&uml()), "a frame made the node host a log");
    assert_eq!(
        node.hosted(&bt()).unwrap().progress(),
        bt_before,
        "the behaviour tree changed"
    );
    assert_eq!(members(&node, &bt()), set(&[]));
    assert_eq!(
        node.frames_not_hosted(),
        expected,
        "the filter did not count once per frame"
    );
    assert_eq!(refusals(&node, &bt()), 0);
    assert!(
        !node.transport().sent.is_empty(),
        "the state request for the unhosted log went unanswered"
    );
    assert!(
        take_broadcast(&mut node).is_empty(),
        "a filtered frame was broadcast"
    );
}

#[test]
fn a_frame_for_an_unhosted_log_leaves_its_id_behind_and_hosting_takes_it_back() {
    let mut node = node("n", &[bt()]);
    assert_eq!(
        node.seen_not_hosted().count(),
        0,
        "a node that has seen nothing listed something"
    );
    assert_eq!(
        node.models()["seen"],
        json!([]),
        "the empty case is not an empty list"
    );

    for frame in frames_for_an_unhosted_log() {
        node.handle_transport_message("writer".to_string(), frame);
    }

    assert_eq!(
        node.seen_not_hosted().cloned().collect::<Vec<_>>(),
        vec![uml()],
        "the filter dropped the id with the frame"
    );
    assert_eq!(node.models()["seen"], json!([uml().as_str()]));
    assert!(!node.hosts(&uml()), "a frame made the node host a log");

    node.host_log(uml()).expect("the node did not host it yet");

    assert_eq!(
        node.seen_not_hosted().count(),
        0,
        "a model the node now hosts is still offered as one it could join"
    );
    assert_eq!(node.models()["seen"], json!([]));
    let hosted: Vec<String> = node
        .hosted_logs()
        .map(|id| id.as_str().to_string())
        .collect();
    assert!(
        hosted.contains(&uml().as_str().to_string()),
        "the id left the seen list without joining the hosted one"
    );
}

#[test]
fn the_seen_list_keeps_every_id_and_the_counter_counts_every_frame() {
    let mut node = node("n", &[bt()]);
    let wanted = 72;
    for i in 0..wanted {
        let id = LogId::parse(&format!("{:032x}", i + 1)).expect("32 hex characters");
        for frame in written("writer", id, "Class") {
            node.handle_transport_message("writer".to_string(), frame);
        }
    }

    assert_eq!(
        node.seen_not_hosted().count(),
        wanted,
        "an id seen was not remembered"
    );
    assert_eq!(
        node.frames_not_hosted(),
        wanted as u64,
        "a frame went uncounted"
    );
}

#[test]
fn mp16_a_state_request_for_an_unhosted_log_is_answered_unavailable_naming_the_id() {
    let mut node = node("donor", &[bt(), uml()]);
    assert!(node.apply_op(json!("Sequence")).success);
    assert!(node.apply_op_to(&uml(), json!("Class")).success);
    let third = LogId::parse("e5f6e5f6e5f6e5f6e5f6e5f6e5f6e5f6").unwrap();
    let before = (
        node.hosted(&bt()).unwrap().progress(),
        node.hosted(&uml()).unwrap().progress(),
    );

    node.handle_transport_message(
        "joiner".to_string(),
        TransportMessage::StateRequest {
            id: "joiner".to_string(),
            log_id: third.clone(),
        },
    );

    let (to, answer) = node
        .transport_mut()
        .sent
        .pop()
        .expect("the request was answered");
    assert_eq!(to, "joiner");
    match answer {
        TransportMessage::StateUnavailable { reason, .. } => assert!(
            reason.contains(third.as_str()),
            "the refusal must name the id asked for, got: {reason}"
        ),
        other => panic!("a snapshot was served for a log the donor does not have: {other:?}"),
    }
    assert_eq!(node.frames_not_hosted(), 1);
    assert_eq!(
        (
            node.hosted(&bt()).unwrap().progress(),
            node.hosted(&uml()).unwrap().progress()
        ),
        before,
        "a hosted model changed"
    );
}

#[test]
fn mp17_held_back_frames_for_one_model_do_not_stall_the_other() {
    let mut a = node("a", &[bt(), uml()]);
    let mut b = node("b", &[bt(), uml()]);
    assert!(a.apply_op(json!("Sequence")).success);
    assert!(a.apply_op_to(&uml(), json!("Class")).success);
    assert!(a.apply_op(json!("Fallback")).success);
    assert!(a.apply_op_to(&uml(), json!("Property")).success);
    let (held, flowing): (Vec<_>, Vec<_>) = take_broadcast(&mut a)
        .into_iter()
        .partition(|frame| frame_log(frame).as_deref() == Some(bt().as_str()));
    assert_eq!((held.len(), flowing.len()), (2, 2));

    for frame in flowing {
        b.handle_transport_message("a".to_string(), frame);
    }
    assert_eq!(
        members(&b, &uml()),
        members(&a, &uml()),
        "the UML model waited on frames for a model nobody is touching"
    );
    assert_eq!(members(&b, &bt()), set(&[]), "held frames were delivered");

    for frame in held {
        b.handle_transport_message("a".to_string(), frame);
    }
    assert_eq!(members(&b, &bt()), members(&a, &bt()));
    assert_eq!(members(&b, &uml()), members(&a, &uml()));
    for log in [bt(), uml()] {
        assert_eq!(refusals(&b, &log), 0);
    }
}

/// xorshift64*, seeded: the schedule is reproducible from the seed.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n as u64) as usize
    }
}

/// Two nodes, four models, a seeded workload tagged with a model, and frames
/// delivered in an interleaved order drawn from the seed. The single-model
/// answer is hand-computed: a grow-only set holds every element written to it.
#[test]
fn mp14_two_nodes_four_models_converge_per_model_with_no_cross_talk() {
    let seed = std::env::var("MP14_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5eed_0000_0000_0014u64);
    let mut rng = Rng(seed);
    let models = [
        bt(),
        LogId::parse("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b3").unwrap(),
        uml(),
        LogId::parse("c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d5").unwrap(),
    ];
    let vocabulary = [
        ["Sequence", "Fallback", "Action", "Condition"],
        ["Sequence", "Fallback", "Action", "Condition"],
        ["Class", "Property", "Operation", "Package"],
        ["Class", "Property", "Operation", "Package"],
    ];
    let mut nodes = [node("a", &models), node("b", &models)];
    let mut expected: Vec<BTreeSet<String>> = models.iter().map(|_| BTreeSet::new()).collect();

    for _round in 0..4 {
        let mut frames: Vec<(usize, TransportMessage)> = Vec::new();
        for _ in 0..10 {
            let model = rng.below(models.len());
            let writer = rng.below(2);
            let word = vocabulary[model][rng.below(4)];
            let result = nodes[writer].apply_op_to(&models[model], json!(word));
            assert!(result.success, "seed {seed}: {}", result.message);
            expected[model].insert(word.to_string());
            for frame in take_broadcast(&mut nodes[writer]) {
                frames.push((1 - writer, frame));
            }
        }
        for i in (1..frames.len()).rev() {
            frames.swap(i, rng.below(i + 1));
        }
        for (receiver, frame) in frames {
            let from = if receiver == 0 { "b" } else { "a" };
            nodes[receiver].handle_transport_message(from.to_string(), frame);
        }
    }

    for (model, log) in models.iter().enumerate() {
        let on_a = members(&nodes[0], log);
        let on_b = members(&nodes[1], log);
        assert_eq!(
            on_a, on_b,
            "seed {seed}: model {log} differs between the nodes"
        );
        assert_eq!(
            on_a, expected[model],
            "seed {seed}: model {log} differs from its single-model run"
        );
        for node in &nodes {
            assert_eq!(
                refusals(node, log),
                0,
                "seed {seed}: a frame was handed to a log that does not own it"
            );
        }
    }
    for node in &nodes {
        assert_eq!(node.frames_not_hosted(), 0, "seed {seed}");
    }
}

/// mp2's setup, read back through the node's metrics: the per-log counter and
/// the node's two (601c887 read them over HTTP; the routes are S6).
#[test]
fn mp4_the_metrics_expose_the_filter_and_the_refusal_counters() {
    let mut node = node("n", &[bt()]);
    let frames = frames_for_an_unhosted_log();
    let filtered = frames.len() as u64;
    for frame in frames {
        node.handle_transport_message("writer".to_string(), frame);
    }

    let per_log = node.log_metrics(&bt()).expect("bt is hosted");
    assert_eq!(
        per_log["foreign_log_refusals"].as_u64(),
        Some(0),
        "{per_log}"
    );
    assert_eq!(per_log["log_id"].as_str(), Some(bt().as_str()), "{per_log}");

    let node_wide = node.metrics();
    assert_eq!(
        node_wide["frames_not_hosted"].as_u64(),
        Some(filtered),
        "{node_wide}"
    );
    assert_eq!(node_wide["hosted_logs"].as_u64(), Some(1), "{node_wide}");
    assert_eq!(
        node_wide["foreign_log_refusals"].as_u64(),
        Some(0),
        "{node_wide}"
    );

    assert!(
        node.log_metrics(&uml()).is_none(),
        "metrics were served for a log the node does not host"
    );
}

#[test]
fn a_batch_is_applied_in_one_step_and_moves_the_version_by_its_length() {
    let mut node = node("n", &[bt()]);
    let before = node.version_of(&bt()).expect("hosted");

    let result = node.submit_batch(
        Some(bt()),
        vec![json!("a"), json!("b"), json!("c")],
        Some(&before),
    );

    assert_eq!(result.outcome, BatchOutcome::Applied, "{}", result.message);
    assert_eq!((result.applied, result.of, result.refused_at), (3, 3, None));
    assert_eq!(members(&node, &bt()), set(&["a", "b", "c"]));
    let frames = take_broadcast(&mut node);
    assert!(
        matches!(frames.as_slice(), [TransportMessage::Batch { .. }]),
        "a batch goes to the peers as one frame: {frames:?}"
    );
    let mut peer = peer("p");
    for frame in frames {
        peer.handle_transport_message("n".to_string(), frame);
    }
    assert_eq!(
        members(&peer, &bt()),
        set(&["a", "b", "c"]),
        "the frame did not hold the three events"
    );
    let after = node.version_of(&bt()).expect("hosted");
    assert_eq!(result.version.as_deref(), Some(after.as_str()));
    assert_eq!(count(&after), count(&before) + 3);
    assert_eq!(
        before.split('.').next(),
        after.split('.').next(),
        "one process, one incarnation"
    );
}

fn peer(id: &str) -> TestNode {
    node(id, &[bt()])
}

#[test]
fn a_batch_computed_before_a_delivery_is_stale_and_applies_nothing() {
    let mut node = node("n", &[bt()]);
    let read = node.version_of(&bt()).expect("hosted");
    for frame in written("writer", bt(), "x") {
        node.handle_transport_message("writer".to_string(), frame);
    }
    let moved = node.version_of(&bt()).expect("hosted");
    assert_ne!(read, moved, "a delivery must move the version");

    let stale = node.submit_batch(Some(bt()), vec![json!("a"), json!("b")], Some(&read));

    assert_eq!(stale.outcome, BatchOutcome::Stale, "{}", stale.message);
    assert_eq!((stale.applied, stale.of), (0, 2));
    assert_eq!(stale.version.as_deref(), Some(moved.as_str()));
    assert_eq!(members(&node, &bt()), set(&["x"]));
    assert!(
        take_broadcast(&mut node).is_empty(),
        "a stale batch sent a frame"
    );

    let again = node.submit_batch(Some(bt()), vec![json!("a"), json!("b")], Some(&moved));
    assert_eq!(again.outcome, BatchOutcome::Applied, "{}", again.message);
    assert_eq!(members(&node, &bt()).len(), 3);
}

#[test]
fn a_batch_for_a_log_the_node_does_not_host_is_answered_as_such() {
    let mut node = node("n", &[bt()]);
    let result = node.submit_batch(Some(uml()), vec![json!("a")], None);
    assert_eq!(result.outcome, BatchOutcome::NotHosted);
    assert_eq!((result.applied, result.version), (0, None));
    assert!(take_broadcast(&mut node).is_empty());
}

/// On the toy a refusal is a non-string op; 601c887 used a list insert out of bounds.
#[test]
fn a_log_refusal_inside_a_batch_reports_exactly_the_prefix_applied() {
    let mut node = node("n", &[bt()]);
    let result = node.submit_batch(
        None,
        vec![json!("a"), json!("b"), json!(42), json!("d")],
        None,
    );

    assert_eq!(result.outcome, BatchOutcome::Refused, "{}", result.message);
    assert_eq!(
        (result.applied, result.of, result.refused_at),
        (2, 4, Some(2))
    );
    assert!(
        result.message.contains("op is not a string element"),
        "the replica's reason is passed on: {}",
        result.message
    );
    assert_eq!(members(&node, &bt()), set(&["a", "b"]));
    let frames = take_broadcast(&mut node);
    assert!(
        matches!(frames.as_slice(), [TransportMessage::Batch { .. }]),
        "the applied prefix goes to the peers as one frame, and nothing after it: {frames:?}"
    );
    let mut peer = peer("p");
    let before = peer.version_of(&bt()).unwrap();
    for frame in frames {
        peer.handle_transport_message("n".to_string(), frame);
    }
    assert_eq!(members(&peer, &bt()), set(&["a", "b"]));
    assert_eq!(count(&peer.version_of(&bt()).unwrap()), count(&before) + 2);
    assert_eq!(result.version, node.version_of(&bt()));
}

/// The ip10 race, at the node, on a set: one operation at a time, a peer's
/// write lands between two operations of one edit and a read sees the edit
/// half applied; as one batch the delivery is either before (stale, nothing
/// written, applied whole once computed again) or after, and both replicas end
/// equal.
#[test]
fn a_peer_delivery_cannot_land_inside_a_batch() {
    fn seed_ab(node: &mut TestNode, writer: &mut TestNode) {
        let seeded = node.submit_batch(None, vec![json!("a"), json!("b")], None);
        assert_eq!(seeded.outcome, BatchOutcome::Applied, "{}", seeded.message);
        carry(node, writer);
        assert_eq!(members(writer, &bt()), set(&["a", "b"]));
    }

    // One at a time, the peer's delivery between the two operations.
    let (mut node, mut writer) = (peer("n"), peer("w"));
    seed_ab(&mut node, &mut writer);
    assert!(writer.apply_op(json!("X")).success);
    assert!(node.submit_op(None, json!("c")).success);
    carry(&mut writer, &mut node);
    assert_eq!(
        members(&node, &bt()),
        set(&["X", "a", "b", "c"]),
        "the race this test is about no longer shows one operation at a time"
    );
    assert!(node.submit_op(None, json!("d")).success);

    // As a batch, the delivery first: stale, and computed again it applies whole.
    let (mut node, mut writer) = (peer("n"), peer("w"));
    seed_ab(&mut node, &mut writer);
    let read = node.version_of(&bt()).unwrap();
    assert!(writer.apply_op(json!("X")).success);
    carry(&mut writer, &mut node);
    let stale = node.submit_batch(None, vec![json!("c"), json!("d")], Some(&read));
    assert_eq!(stale.outcome, BatchOutcome::Stale);
    assert_eq!(
        members(&node, &bt()),
        set(&["X", "a", "b"]),
        "a stale batch wrote"
    );
    let reread = node.version_of(&bt()).unwrap();
    let applied = node.submit_batch(None, vec![json!("c"), json!("d")], Some(&reread));
    assert_eq!(
        applied.outcome,
        BatchOutcome::Applied,
        "{}",
        applied.message
    );
    carry(&mut node, &mut writer);
    assert_eq!(members(&node, &bt()), set(&["X", "a", "b", "c", "d"]));
    assert_eq!(
        members(&writer, &bt()),
        members(&node, &bt()),
        "the replicas disagree"
    );

    // As a batch, the delivery after: applied against its read, the peer's write merges.
    let (mut node, mut writer) = (peer("n"), peer("w"));
    seed_ab(&mut node, &mut writer);
    let read = node.version_of(&bt()).unwrap();
    assert!(writer.apply_op(json!("X")).success);
    let applied = node.submit_batch(None, vec![json!("c"), json!("d")], Some(&read));
    assert_eq!(
        applied.outcome,
        BatchOutcome::Applied,
        "{}",
        applied.message
    );
    carry(&mut writer, &mut node);
    carry(&mut node, &mut writer);
    assert_eq!(members(&node, &bt()), set(&["X", "a", "b", "c", "d"]));
    assert_eq!(
        members(&writer, &bt()),
        members(&node, &bt()),
        "the replicas disagree"
    );
}

#[test]
fn a_batch_reaches_a_peer_as_one_frame_delivered_in_one_step() {
    let mut node = node("n", &[bt()]);
    let mut peer = peer("p");
    let applied = node.submit_batch(None, vec![json!("a"), json!("b"), json!("c")], None);
    assert_eq!(
        applied.outcome,
        BatchOutcome::Applied,
        "{}",
        applied.message
    );
    let frames = take_broadcast(&mut node);
    assert_eq!(frames.len(), 1, "one frame for the whole batch");
    let before = peer.version_of(&bt()).unwrap();
    for frame in frames {
        peer.handle_transport_message("n".into(), frame);
    }
    assert_eq!(members(&peer, &bt()), set(&["a", "b", "c"]));
    assert_eq!(count(&peer.version_of(&bt()).unwrap()), count(&before) + 3);
}

/// 601c887's bound: a batch frame over 768 KiB goes out one Event per operation.
#[test]
fn a_batch_too_large_for_one_frame_goes_out_one_frame_per_operation() {
    let mut node = node("n", &[bt()]);
    let big = "x".repeat(768 * 1024 / 2);
    let result = node.submit_batch(
        None,
        vec![
            json!(format!("a{big}")),
            json!(format!("b{big}")),
            json!("c"),
        ],
        None,
    );
    assert_eq!(result.outcome, BatchOutcome::Applied, "{}", result.message);
    let frames = take_broadcast(&mut node);
    assert_eq!(frames.len(), 3);
    assert!(
        frames
            .iter()
            .all(|frame| matches!(frame, TransportMessage::Event { .. }))
    );
}

/// 601c887's route test at the node: a batch names the version of the read it
/// was computed from, is applied against it, and is refused whole once the log
/// has moved past it.
#[test]
fn the_batch_route_applies_against_the_etag_it_was_read_at_and_refuses_it_once_moved() {
    let mut node = node("n", &[bt()]);
    let ops = |values: &[&str]| values.iter().map(|v| json!(v)).collect::<Vec<Value>>();

    assert_eq!(members(&node, &bt()), set(&[]));
    let read = node.version_of(&bt()).expect("hosted");

    let applied = node.submit_batch(Some(bt()), ops(&["a", "b"]), Some(&read));
    assert_eq!(
        (applied.outcome, applied.applied),
        (BatchOutcome::Applied, 2),
        "{}",
        applied.message
    );
    let after = applied.version.clone().expect("a version");

    let stale = node.submit_batch(Some(bt()), ops(&["c"]), Some(&read));
    assert_eq!(
        (stale.outcome, stale.applied),
        (BatchOutcome::Stale, 0),
        "{}",
        stale.message
    );
    assert_eq!(serde_json::to_value(stale.outcome).unwrap(), json!("stale"));
    assert_eq!(stale.version.as_deref(), Some(after.as_str()));

    assert_eq!(
        members(&node, &bt()),
        set(&["a", "b"]),
        "the stale batch wrote"
    );
    assert_eq!(
        node.version_of(&bt()).as_deref(),
        Some(after.as_str()),
        "the read and the batch answer disagree"
    );

    let again = node.submit_batch(Some(bt()), ops(&["c"]), Some(&after));
    assert_eq!(again.outcome, BatchOutcome::Applied, "{}", again.message);

    // The single-operation intake still answers as it always has.
    assert!(node.submit_op(Some(bt()), json!("d")).success);
    assert_eq!(members(&node, &bt()), set(&["a", "b", "c", "d"]));
}

// ---- three nodes over DirectTransport, 127.0.0.1 ---------------------------

type TcpNode = Node<ToyFactory, DirectTransport>;

fn pump_until(nodes: &mut [TcpNode], within: Duration, done: impl Fn(&[TcpNode]) -> bool) -> bool {
    let deadline = Instant::now() + within;
    loop {
        for node in nodes.iter_mut() {
            node.poll();
        }
        if done(nodes) {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Three toy nodes on one shared log; a and b also host a second log, which c
/// does not. Every write happens before any frame moves (a concurrent
/// schedule), one of them a batch and one element written on two nodes.
#[test]
fn three_toy_nodes_converge_over_the_direct_transport() {
    let ids = ["a", "b", "c"];
    let shared = bt();
    let pair = uml();
    let listeners: Vec<TcpListener> = ids
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0"))
        .collect();
    let addresses: Vec<String> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();
    let mut nodes: Vec<TcpNode> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let peers: HashMap<PeerId, String> = ids
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(j, other)| (other.to_string(), addresses[j].clone()))
                .collect();
            let transport = DirectTransport::with_listener(ids[i].to_string(), listener, peers)
                .expect("DirectTransport::with_listener");
            let mut node = Node::new(
                ToyFactory,
                ids[i].to_string(),
                &ids,
                transport,
                shared.clone(),
            );
            if i < 2 {
                node.host_log(pair.clone()).expect("a fresh id");
            }
            node
        })
        .collect();
    for i in 0..ids.len() {
        for other in &ids[i + 1..] {
            nodes[i]
                .transport_mut()
                .connect_to(&other.to_string())
                .expect("dial a peer on loopback");
        }
    }
    let meshed = pump_until(&mut nodes, Duration::from_secs(30), |nodes| {
        nodes.iter().enumerate().all(|(i, n)| {
            ids.iter()
                .enumerate()
                .all(|(j, other)| i == j || n.transport().is_connected(&other.to_string()))
        })
    });
    assert!(meshed, "the mesh did not form within 30 s");
    let c_filtered_before = nodes[2].frames_not_hosted();

    assert!(nodes[0].apply_op(json!("red")).success);
    assert!(nodes[0].apply_op_to(&pair, json!("p-a")).success);
    assert!(nodes[1].apply_op(json!("green")).success);
    let batch = nodes[1].submit_batch(None, vec![json!("amber"), json!("teal")], None);
    assert_eq!(batch.outcome, BatchOutcome::Applied, "{}", batch.message);
    assert!(nodes[1].apply_op_to(&pair, json!("p-b")).success);
    assert!(nodes[2].apply_op(json!("blue")).success);
    assert!(nodes[2].apply_op(json!("red")).success);

    let union = set(&["amber", "blue", "green", "red", "teal"]);
    let pair_union = set(&["p-a", "p-b"]);
    let converged = pump_until(&mut nodes, Duration::from_secs(30), |nodes| {
        nodes.iter().all(|n| members(n, &shared) == union)
            && nodes[..2].iter().all(|n| members(n, &pair) == pair_union)
    });
    let views: Vec<_> = nodes.iter().map(|n| members(n, &shared)).collect();
    assert!(
        converged,
        "the three nodes did not converge within 30 s: {views:?}"
    );
    // Settle: nothing more may arrive and change the answer.
    pump_until(&mut nodes, Duration::from_millis(300), |_| false);

    for node in &nodes {
        assert_eq!(members(node, &shared), union);
        // (origin, element) pairs: a red; b green, amber, teal; c blue, red.
        assert_eq!(count(&node.version_of(&shared).unwrap()), 6);
        assert_eq!(refusals(node, &shared), 0);
    }
    for node in &nodes[..2] {
        assert_eq!(members(node, &pair), pair_union);
        assert_eq!(refusals(node, &pair), 0);
    }
    let c = &nodes[2];
    assert!(!c.hosts(&pair), "a frame made c host a log");
    assert!(
        c.frames_not_hosted() >= c_filtered_before + 2,
        "c did not filter the two writes to the log it does not host: {} then {}",
        c_filtered_before,
        c.frames_not_hosted()
    );
    assert_eq!(
        c.seen_not_hosted().cloned().collect::<Vec<_>>(),
        vec![pair.clone()]
    );
}
