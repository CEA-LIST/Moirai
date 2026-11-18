//! NW.04, amendment of 2026-09-29 (vault `spec/items/NW.04.md`): one member
//! table per hosted log, the design Cam chose for NW.04 (option O3).
//!
//! Two inline tests of `moirai-network/src/generic.rs` stated the design this
//! decision replaces, one member table shared by every log a node hosts, and
//! are deleted by the hunk the spec registers. What each of them guarded is
//! kept here, restated for a table per log:
//!
//! - `a_reordering_transfer_is_refused_while_a_sibling_holds_an_undelivered_frame`
//!   required the node to turn a snapshot away while another hosted log held
//!   a frame it could not yet deliver, because adopting would have rebuilt the
//!   table under that log (M-E3). With a table per log nothing is rebuilt
//!   under a sibling, so the snapshot is adopted, and the guard that stays is
//!   the sibling's: its held frame is kept and delivers once its predecessor
//!   arrives, and both logs go on exchanging edits afterwards.
//! - `at_sixteen_and_sixty_four_hosted_logs_the_node_holds_one_member_table`
//!   required one table for the whole node (M-E2). It becomes one table per
//!   hosted log, read from what each log lists as its members, with the
//!   memory per hosted log reported and bounded so that a regression in what
//!   one hosted log costs is still caught.
//!
//! Every frame is carried by hand between nodes over a recording transport;
//! nothing here opens a socket or sleeps.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --features test_utils \
//!     --test nw04_member_tables -- --nocapture
//! ```
//!
//! The memory test is compiled with the `test_utils` feature only, which is
//! what supplies the deep sizes; the command above names it.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use moirai_crdt::set::ewflag_set::{EWFlagSet, EWFlagSetLog};
use moirai_network::generic::{GenericNode, LogReplica};
use moirai_network::transport::{
    CrdtTransport, PeerId, PeerInfo, TransportMessage, TransportResult,
};
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::log_id::LogId;
use moirai_protocol::replica::IsReplica;

type Op = EWFlagSet<String>;
type Log = EWFlagSetLog<String>;
type TestNode = GenericNode<Log, Wire>;

/// Frames a node sent to one peer, in order.
type Sent = Arc<Mutex<Vec<(PeerId, TransportMessage<Op>)>>>;
/// Frames a node broadcast, in order.
type Broadcast = Arc<Mutex<Vec<TransportMessage<Op>>>>;

/// Records everything a node sends and broadcasts and receives nothing. The
/// test keeps a handle on both records, since the node owns the transport.
struct Wire {
    id: PeerId,
    sent: Sent,
    broadcast: Broadcast,
}

impl CrdtTransport for Wire {
    type Op = Op;

    fn local_id(&self) -> &PeerId {
        &self.id
    }

    fn send(&mut self, peer: &PeerId, msg: TransportMessage<Op>) -> TransportResult<()> {
        self.sent.lock().unwrap().push((peer.clone(), msg));
        Ok(())
    }

    fn broadcast(&mut self, msg: TransportMessage<Op>) -> TransportResult<()> {
        self.broadcast.lock().unwrap().push(msg);
        Ok(())
    }

    fn try_recv(&mut self) -> TransportResult<Option<(PeerId, TransportMessage<Op>)>> {
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

/// A node under test and the test's handles on what it sends.
struct Hand {
    node: TestNode,
    sent: Sent,
    broadcast: Broadcast,
}

impl Hand {
    /// Every frame the node broadcast since the last call, in order.
    fn take_broadcast(&self) -> Vec<TransportMessage<Op>> {
        std::mem::take(&mut *self.broadcast.lock().unwrap())
    }

    /// Every frame the node sent to one peer since the last call, in order.
    fn take_sent(&self) -> Vec<(PeerId, TransportMessage<Op>)> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

/// A node named `id` hosting `logs`, the first as its default log, knowing
/// no member but itself, able to serve and accept a state transfer: the
/// construction of the inline tests this file replaces.
fn node(id: &str, logs: &[LogId]) -> Hand {
    let (first, rest) = logs.split_first().expect("a node hosts at least one log");
    let sent = Sent::default();
    let broadcast = Broadcast::default();
    let wire = Wire {
        id: id.to_string(),
        sent: Arc::clone(&sent),
        broadcast: Arc::clone(&broadcast),
    };
    let mut node = TestNode::with_transport_and_log_id(id.to_string(), &[id], wire, first.clone());
    for log in rest {
        node.host_log(log.clone()).expect("a fresh id");
    }
    node.enable_state_transfer();
    Hand {
        node,
        sent,
        broadcast,
    }
}

/// A bare replica hosting one log, for frames that come from outside a node.
fn peer(id: &str, log_id: LogId) -> LogReplica<Log> {
    IsReplica::bootstrap_with_log_id(id.to_string(), &[id], log_id)
}

fn add(value: &str) -> Op {
    EWFlagSet::Add(value.to_string())
}

/// The behaviour-tree model of the phase-4 validation plan, `a1b2…`.
fn bt() -> LogId {
    LogId::parse("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2").unwrap()
}

/// The SimpleUML model of the phase-4 validation plan, `c3d4…`.
fn uml() -> LogId {
    LogId::parse("c3d4c3d4c3d4c3d4c3d4c3d4c3d4c3d4").unwrap()
}

/// The `i`th hosted log's id in M-E2's harness
/// (`experiments/mp2-dispatch-cost/model_plane_cost.rs` in the vault), so the
/// memory read here is taken on the construction M-E2 measured.
fn hosted_id(i: usize) -> LogId {
    let mut bytes = [0u8; 16];
    bytes[0] = (i >> 8) as u8;
    bytes[1] = i as u8;
    LogId::from_bytes(bytes)
}

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| value.to_string()).collect()
}

/// The members of the set a replica holds, sorted.
fn members_of(replica: &LogReplica<Log>) -> BTreeSet<String> {
    replica.query(&Read::new()).into_iter().collect()
}

/// The members of the set a node's hosted log holds, sorted.
fn members(node: &TestNode, log_id: &LogId) -> BTreeSet<String> {
    members_of(
        node.hosted(log_id)
            .unwrap_or_else(|| panic!("{log_id} is hosted")),
    )
}

/// The replica ids a hosted log's member table lists, in its order: what its
/// stable version is keyed by.
fn table_of(node: &TestNode, log_id: &LogId) -> Vec<String> {
    node.hosted(log_id)
        .unwrap_or_else(|| panic!("{log_id} is hosted"))
        .stability()
        .stable_version
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// The events among `frames` that belong to `log_id`, in order.
fn events_for(frames: &[TransportMessage<Op>], log_id: &LogId) -> Vec<EventMessage<Op>> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            TransportMessage::Event { event } if event.log_id() == log_id => Some(event.clone()),
            _ => None,
        })
        .collect()
}

/// `donor` serves `joiner` its state of `log_id`, and the answer is handed to
/// `joiner`. Returns the ids the snapshot names, in the donor's order.
fn transfer(
    donor: &mut Hand,
    joiner: &mut Hand,
    joiner_id: &str,
    donor_id: &str,
    log_id: &LogId,
) -> Vec<String> {
    donor.node.handle_transport_message(
        joiner_id.to_string(),
        TransportMessage::StateRequest {
            id: joiner_id.to_string(),
            log_id: log_id.clone(),
        },
    );
    let mut answers = donor.take_sent();
    let (to, response) = answers.pop().expect("the request was answered");
    assert_eq!(to, joiner_id);
    let named = match &response {
        TransportMessage::StateResponse { snapshot, .. } => snapshot.resolver().into_vec(),
        other => panic!("the donor did not serve its log: {other:?}"),
    };
    joiner
        .node
        .handle_transport_message(donor_id.to_string(), response);
    named
}

/// The successor of the inline test
/// `a_reordering_transfer_is_refused_while_a_sibling_holds_an_undelivered_frame`.
///
/// `n` hosts two models, both empty. A writer of the UML model `n` has never
/// met sends two events; the first is withheld and the second waits in `n`'s
/// inbox, so the UML log holds a frame and has delivered nothing, the state
/// M-E3 found two of five replicas in when a snapshot for another log arrived.
/// A donor of the behaviour tree whose ordering disagrees with `n`'s at index
/// 0 then serves `n` its state.
///
/// With a member table per log the behaviour tree's table is its own, so the
/// snapshot is adopted. The UML log's table is not touched: the held frame is
/// kept and delivers when its predecessor arrives, and after the adoption
/// both logs exchange an edit each way with their peers.
#[test]
#[ignore = "NW.04 deferred to the governance plane phase (Cam, 2026-09-29); red by design until then, run with --ignored"]
fn a_reordering_transfer_is_adopted_while_a_sibling_holds_an_undelivered_frame() {
    let mut n = node("n", &[bt(), uml()]);
    let mut writer = peer("writer", uml());
    let first = writer.send(add("Class")).expect("an enabled operation");
    let second = writer.send(add("Property")).expect("an enabled operation");
    n.node.handle_transport_message(
        "writer".to_string(),
        TransportMessage::Event { event: second },
    );
    let waiting = n.node.hosted(&uml()).unwrap().stability();
    assert_eq!(
        (waiting.delivered, waiting.pending),
        (0, 1),
        "premise: the second event should wait for the first"
    );

    let mut donor = node("donor", &[bt()]);
    assert!(donor.node.apply_op(add("Sequence")).success);
    donor.take_broadcast();
    let joiners_order = table_of(&n.node, &bt());
    let named = transfer(&mut donor, &mut n, "n", "donor", &bt());
    assert_eq!(
        (named.first().map(String::as_str), joiners_order.first().map(String::as_str)),
        (Some("donor"), Some("n")),
        "premise: the snapshot names {named:?} and n's behaviour-tree log listed {joiners_order:?}, \
         so the two orders should disagree at index 0"
    );

    assert_eq!(
        members(&n.node, &bt()),
        set(&["Sequence"]),
        "n did not adopt the donor's snapshot of the behaviour tree although that log's member \
         table is its own"
    );
    assert_eq!(
        n.node.hosted(&uml()).unwrap().stability().pending,
        1,
        "the adoption dropped the UML log's held frame"
    );

    n.node.handle_transport_message(
        "writer".to_string(),
        TransportMessage::Event { event: first },
    );
    assert_eq!(
        members(&n.node, &uml()),
        set(&["Class", "Property"]),
        "the held frame and its predecessor did not both deliver after the adoption"
    );
    let after = n.node.hosted(&uml()).unwrap().stability();
    assert_eq!((after.delivered, after.pending), (2, 0));

    // Both logs keep working in both directions after the adoption.
    n.take_broadcast();
    n.take_sent();
    assert!(n.node.apply_op_to(&bt(), add("Fallback")).success);
    assert!(n.node.apply_op_to(&uml(), add("Package")).success);
    let frames = n.take_broadcast();
    for event in events_for(&frames, &bt()) {
        donor
            .node
            .handle_transport_message("n".to_string(), TransportMessage::Event { event });
    }
    for event in events_for(&frames, &uml()) {
        writer.receive(event);
    }
    assert_eq!(members(&donor.node, &bt()), set(&["Sequence", "Fallback"]));
    assert_eq!(members(&n.node, &bt()), set(&["Sequence", "Fallback"]));
    assert_eq!(members_of(&writer), set(&["Class", "Property", "Package"]));
    assert_eq!(
        members(&n.node, &uml()),
        set(&["Class", "Property", "Package"])
    );

    let from_donor = donor.node.apply_op(add("Action"));
    assert!(from_donor.success);
    for event in events_for(&donor.take_broadcast(), &bt()) {
        n.node
            .handle_transport_message("donor".to_string(), TransportMessage::Event { event });
    }
    let from_writer = writer.send(add("Operation")).expect("an enabled operation");
    n.node.handle_transport_message(
        "writer".to_string(),
        TransportMessage::Event { event: from_writer },
    );
    assert_eq!(
        members(&n.node, &bt()),
        set(&["Sequence", "Fallback", "Action"])
    );
    assert_eq!(
        members(&n.node, &uml()),
        set(&["Class", "Property", "Package", "Operation"])
    );
}

/// NW.04's K1 at the level of one process: the sibling holds history, not
/// only a frame. `n` has delivered a writer's two UML events and written one
/// of its own before a donor whose ordering disagrees with `n`'s serves it
/// the behaviour tree. The snapshot is adopted, and the UML log goes on
/// delivering and being delivered, its history intact.
#[test]
#[ignore = "NW.04 deferred to the governance plane phase (Cam, 2026-09-29); red by design until then, run with --ignored"]
fn a_reordering_transfer_is_adopted_while_a_sibling_holds_history() {
    let mut n = node("n", &[bt(), uml()]);
    let mut writer = peer("writer", uml());
    for value in ["Class", "Property"] {
        let event = writer.send(add(value)).expect("an enabled operation");
        n.node
            .handle_transport_message("writer".to_string(), TransportMessage::Event { event });
    }
    assert!(n.node.apply_op_to(&uml(), add("Package")).success);
    for event in events_for(&n.take_broadcast(), &uml()) {
        writer.receive(event);
    }
    let history = n.node.hosted(&uml()).unwrap().stability();
    assert_eq!(history.delivered, 3, "premise: the UML log holds history");
    assert_eq!(members_of(&writer), members(&n.node, &uml()));

    let mut donor = node("donor", &[bt()]);
    assert!(donor.node.apply_op(add("Sequence")).success);
    donor.take_broadcast();
    let joiners_order = table_of(&n.node, &bt());
    let named = transfer(&mut donor, &mut n, "n", "donor", &bt());
    assert_eq!(
        (
            named.first().map(String::as_str),
            joiners_order.first().map(String::as_str)
        ),
        (Some("donor"), Some("n")),
        "premise: the snapshot names {named:?} and n's behaviour-tree log listed {joiners_order:?}"
    );

    assert_eq!(
        members(&n.node, &bt()),
        set(&["Sequence"]),
        "n turned the donor's snapshot away because another log holds history"
    );
    assert_eq!(
        n.node.hosted(&uml()).unwrap().stability().delivered,
        3,
        "the adoption changed what the UML log had delivered"
    );

    n.take_sent();
    let third = writer.send(add("Operation")).expect("an enabled operation");
    n.node.handle_transport_message(
        "writer".to_string(),
        TransportMessage::Event { event: third },
    );
    assert!(n.node.apply_op_to(&uml(), add("Association")).success);
    assert!(n.node.apply_op_to(&bt(), add("Fallback")).success);
    let frames = n.take_broadcast();
    for event in events_for(&frames, &uml()) {
        writer.receive(event);
    }
    for event in events_for(&frames, &bt()) {
        donor
            .node
            .handle_transport_message("n".to_string(), TransportMessage::Event { event });
    }
    let uml_now = set(&["Class", "Property", "Package", "Operation", "Association"]);
    assert_eq!(
        members(&n.node, &uml()),
        uml_now,
        "the UML log lost or refused an edit"
    );
    assert_eq!(
        members_of(&writer),
        uml_now,
        "the writer did not deliver n's UML edit"
    );
    assert_eq!(members(&donor.node, &bt()), set(&["Sequence", "Fallback"]));
    assert_eq!(members(&n.node, &bt()), set(&["Sequence", "Fallback"]));
}

/// The successor of the inline test
/// `at_sixteen_and_sixty_four_hosted_logs_the_node_holds_one_member_table`,
/// read from behaviour: at sixteen and at sixty-four hosted logs, each log
/// hears from a writer of its own, and each log's member table then lists
/// the node and that one writer, and no writer that spoke only on another
/// log. One table shared by the node would list every writer in every log.
#[test]
#[ignore = "NW.04 deferred to the governance plane phase (Cam, 2026-09-29); red by design until then, run with --ignored"]
fn at_sixteen_and_sixty_four_hosted_logs_each_log_holds_a_member_table_of_its_own() {
    for n in [16usize, 64] {
        let logs: Vec<LogId> = (0..n).map(hosted_id).collect();
        let mut node = node("m", &logs);
        assert_eq!(node.node.hosted_logs().count(), n);

        for (i, log) in logs.iter().enumerate() {
            let writer_id = format!("w{i}");
            let mut writer = peer(&writer_id, log.clone());
            let event = writer.send(add("x")).expect("an enabled operation");
            node.node
                .handle_transport_message(writer_id, TransportMessage::Event { event });
        }

        let mut shared: Vec<(usize, Vec<String>)> = Vec::new();
        for (i, log) in logs.iter().enumerate() {
            assert_eq!(
                node.node.hosted(log).unwrap().stability().delivered,
                1,
                "at {n} hosted logs, log {i} did not deliver its writer's event"
            );
            let table = table_of(&node.node, log);
            let listed: BTreeSet<String> = table.iter().cloned().collect();
            if listed != set(&["m", &format!("w{i}")]) {
                shared.push((i, table));
            }
        }
        assert!(
            shared.is_empty(),
            "at {n} hosted logs, {} of {n} logs list members other than the node and their own \
             writer, so they do not hold a member table of their own; the first: log {} lists {:?}",
            shared.len(),
            shared[0].0,
            shared[0].1
        );
    }
}

/// F3 of NW.03 under a table per log: a donor that serves a log's state
/// takes the requester into that log's member table before the snapshot, so
/// the snapshot names it, and into no other log's table.
#[test]
#[ignore = "NW.04 deferred to the governance plane phase (Cam, 2026-09-29); red by design until then, run with --ignored"]
fn a_state_request_admits_the_requester_into_the_requested_log_and_no_other() {
    let mut donor = node("donor", &[bt(), uml()]);
    assert!(donor.node.apply_op_to(&bt(), add("Sequence")).success);
    assert!(donor.node.apply_op_to(&uml(), add("Class")).success);
    donor.take_broadcast();
    let mut joiner = node("joiner", &[bt()]);

    let named = transfer(&mut donor, &mut joiner, "joiner", "donor", &bt());

    assert!(
        named.iter().any(|id| id == "joiner"),
        "the snapshot does not name the requester (NW.03's F3): {named:?}"
    );
    assert!(
        table_of(&donor.node, &bt()).iter().any(|id| id == "joiner"),
        "the requested log's table does not list the requester"
    );
    assert_eq!(
        table_of(&donor.node, &uml()),
        vec!["donor".to_string()],
        "serving the behaviour tree changed the UML log's member table"
    );
    assert_eq!(
        members(&joiner.node, &bt()),
        set(&["Sequence"]),
        "the joiner did not adopt"
    );
}

/// M-E2's memory reading under a table per log. At N hosted empty logs on
/// M-E2's construction (a node `m` knowing only itself, logs `hosted_id(0)`
/// to `hosted_id(N - 1)`), `m(N)` is the deep size of the hosted map with
/// every allocation charged once, `s = m(1)` one hosted log on its own, and
/// the cost of each added log is `(m(N) - m(1)) / (N - 1)`. Each added log now
/// carries its own member table, so that cost is bounded by
/// `PER_LOG_LIMIT_BYTES`, fixed in the spec's amendment from M-E2's figures
/// and the size of one table. The reading is printed on a line starting
/// `NW04-MEM`.
#[cfg(feature = "test_utils")]
#[test]
#[ignore = "NW.04 deferred to the governance plane phase (Cam, 2026-09-29); red by design until then, run with --ignored"]
fn at_sixteen_and_sixty_four_hosted_logs_the_memory_per_log_stays_within_its_bound() {
    use deepsize::DeepSizeOf;
    use moirai_protocol::broadcast::internalizer::Interner;

    let m = |n: usize| {
        let logs: Vec<LogId> = (0..n).map(hosted_id).collect();
        let node = node("m", &logs);
        assert_eq!(node.node.hosted_logs().count(), n);
        node.node.hosted_logs_deep_size()
    };
    let one_table = {
        let mut table = Interner::new();
        table.intern("m");
        let shared = table.into_shared();
        shared.deep_size_of() - std::mem::size_of_val(&shared)
    };
    let s = m(1);
    for n in [16usize, 64] {
        let m_n = m(n);
        let per_log = (m_n - s) as f64 / (n - 1) as f64;
        println!(
            "NW04-MEM n={n} m(n)={m_n} B s=m(1)={s} B per_added_log={per_log:.1} B \
             one_table={one_table} B limit={PER_LOG_LIMIT_BYTES} B"
        );
        assert!(
            per_log <= PER_LOG_LIMIT_BYTES as f64,
            "at {n} hosted logs each added log costs {per_log:.1} B, above the \
             {PER_LOG_LIMIT_BYTES} B limit (m({n}) = {m_n} B, m(1) = {s} B, one table {one_table} B)"
        );
    }
}

/// What one added hosted log may cost, in bytes of deep size, fixed in the
/// spec's amendment before implementation: what M-E2 measured an added log
/// to cost (628 B, phase-4 evaluation report §3.2), plus one member table
/// holding one member (329 B, read at `d325948` by the reading above), with
/// a quarter of slack: (628 + 329) × 5 / 4 = 1,196 B.
#[cfg(feature = "test_utils")]
const PER_LOG_LIMIT_BYTES: usize = (628 + 329) * 5 / 4;
