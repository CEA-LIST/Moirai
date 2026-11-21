//! EC.M2 row N4: our list tests from `akira/moirai-modelstore` at `748a185`
//! that Léo's line does not carry, translated to his API on the merged line
//! (lane HEAD `b1e254c`).
//!
//! Translation rules: same operations, same delivery orders, same assertions.
//! Only names change: `List::{insert,delete}` -> `ListUpdate::*`,
//! `NestedList::{insert,update,delete}` -> `NestedListUpdate::*`, resettable
//! `Counter::{Inc,Dec}` -> `CounterUpdate::*` on his `counter::Counter`,
//! `state::graph_log::GraphLog` -> `state::po_log::GraphLog`,
//! `utils::membership::twins_log` -> `utils::bootstrap::twins_log`; the fuzz
//! configuration is mapped as written on `fuzz_list` below.
//!
//! Sources at 748a185:
//! - moirai-crdt/src/list/eg_walker/tests.rs:
//!   a_read_between_deliveries_changes_what_the_cached_list_holds (9c0ecee),
//!   fuzz_list (ignored)
//! - moirai-crdt/src/list/nested_list.rs tests:
//!   nested_list_reads_a_created_child_that_holds_nothing_yet (1e05cc1),
//!   nested_list_reads_a_child_whose_value_returned_to_default (1e05cc1),
//!   a_read_between_deliveries_drops_a_child_a_concurrent_update_keeps (9c0ecee),
//!   a_read_between_deliveries_panics_the_positions_cache_on_the_next_insert (9c0ecee),
//!   a_read_between_deliveries_puts_two_inserts_from_one_replica_in_the_wrong_order (9c0ecee)

use moirai_crdt::{
    counter::{Counter, update::CounterUpdate},
    list::{
        eg_walker::List,
        nested_list::NestedListLog,
        update::{ListUpdate, NestedListUpdate},
    },
    utils::bootstrap::twins_log,
};
use moirai_macros::record;
use moirai_protocol::{
    crdt::query::Read,
    replica::IsReplica,
    state::{
        cache::CachedLog,
        po_log::{GraphLog, VecLog},
    },
};

// ------------------------------------------------------------ eg_walker (1 + 1 ignored)

/// Ours: `list::eg_walker::tests::a_read_between_deliveries_changes_what_the_cached_list_holds`.
#[test]
fn eg_walker_a_read_between_deliveries_changes_what_the_cached_list_holds() {
    let (mut replica_a, mut replica_b) = twins_log::<CachedLog<GraphLog<List<char>>, Vec<char>>>();

    let e1 = replica_a.send(ListUpdate::insert('x', 0)).unwrap();
    replica_b.receive(e1);

    // `b` writes once; `a` writes twice without having seen it.
    let event_b = replica_b.send(ListUpdate::insert('b', 0)).unwrap();
    let event_a_second = replica_a.send(ListUpdate::insert('y', 1)).unwrap();
    let event_a_third = replica_a.send(ListUpdate::insert('z', 2)).unwrap();

    replica_b.receive(event_a_second);
    // The only thing this read does is populate the cache.
    assert_eq!(
        replica_b.query(&Read::<Vec<char>>::new()),
        vec!['b', 'x', 'y']
    );
    replica_b.receive(event_a_third);
    replica_a.receive(event_b);

    let expected = vec!['b', 'x', 'y', 'z'];
    assert_eq!(replica_a.query(&Read::<Vec<char>>::new()), expected);
    assert_eq!(replica_b.query(&Read::<Vec<char>>::new()), expected);
}

/// Ours: `list::eg_walker::tests::fuzz_list`, at 748a185:
/// `RunConfig::new(0.5, 8, 1_000, None, None, true, false)`, one run,
/// `FuzzerConfig::<GraphLog<List<char>>, Read<String>>::new("list", runs, true /* final merge */, Read == Read, false /* no save */)`.
/// Our churn rule was `online = !random_bool(0.5)` on every pick, whatever the
/// previous state, which is his Markov churn with p_disconnect 0.5 and
/// p_reconnect 0.5. Our sixth argument `true` is the execution graph, his
/// `with_execution_graph()`; our seventh `false` (stability not disabled) is
/// his default (log stabilization on). His defaults: final merge on, no save.
#[test]
#[ignore]
fn eg_walker_fuzz_list() {
    use moirai_fuzz::{
        config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
        fuzzer::fuzzer,
    };

    let run =
        RunConfig::new(ChurnConfig::new(0.5, 0.5), 8, 1_000, None, None).with_execution_graph();
    let runs = vec![run; 1];

    let config = FuzzerConfig::<GraphLog<List<char>>, Read<String>>::new(
        "list",
        runs,
        Predicate::new(Read::new(), |a, b| a == b),
    );

    fuzzer::<GraphLog<List<char>>, Read<String>>(config);
}

// ------------------------------------------------------------ nested_list (5)

/// Ours: `list::nested_list::tests::nested_list_reads_a_created_child_that_holds_nothing_yet`.
#[test]
fn nested_list_reads_a_created_child_that_holds_nothing_yet() {
    record!(Duet {
        first: VecLog<Counter<i32>>,
        second: VecLog<Counter<i32>>,
    });

    let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<DuetLog>>();

    let event = replica_a
        .send(NestedListUpdate::insert(0, Duet::New))
        .unwrap();
    replica_b.receive(event);

    let expected = vec![DuetValue::<i32, i32>::default()];
    assert_eq!(replica_a.query(&Read::new()), expected);
    assert_eq!(replica_b.query(&Read::new()), expected);
}

/// Ours: `list::nested_list::tests::nested_list_reads_a_child_whose_value_returned_to_default`.
#[test]
fn nested_list_reads_a_child_whose_value_returned_to_default() {
    let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

    let event = replica_a
        .send(NestedListUpdate::insert(0, CounterUpdate::Inc(10)))
        .unwrap();
    replica_b.receive(event);
    let event = replica_a
        .send(NestedListUpdate::update(0, CounterUpdate::Dec(10)))
        .unwrap();
    replica_b.receive(event);

    assert!(
        replica_b
            .send(NestedListUpdate::update(0, CounterUpdate::Inc(0)))
            .is_ok()
    );
    assert_eq!(replica_a.query(&Read::new()), vec![0]);
}

/// Ours: `list::nested_list::tests::a_read_between_deliveries_drops_a_child_a_concurrent_update_keeps`.
#[test]
fn nested_list_a_read_between_deliveries_drops_a_child_a_concurrent_update_keeps() {
    let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

    let event_a = replica_a
        .send(NestedListUpdate::insert(0, CounterUpdate::Inc(10)))
        .unwrap();
    replica_b.receive(event_a);

    let event_b = replica_b
        .send(NestedListUpdate::update(0, CounterUpdate::Inc(5)))
        .unwrap();
    let event_a_update = replica_a
        .send(NestedListUpdate::update(0, CounterUpdate::Inc(1)))
        .unwrap();
    let event_a_delete = replica_a.send(NestedListUpdate::delete(0)).unwrap();

    replica_b.receive(event_a_update);
    // The only thing this read does is populate the positions cache.
    assert_eq!(replica_b.query(&Read::new()), vec![16]);
    replica_b.receive(event_a_delete);
    replica_a.receive(event_b);

    assert_eq!(replica_a.query(&Read::new()), vec![5]);
    assert_eq!(replica_b.query(&Read::new()), vec![5]);
    assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
}

/// Ours: `list::nested_list::tests::a_read_between_deliveries_panics_the_positions_cache_on_the_next_insert`.
#[test]
fn nested_list_a_read_between_deliveries_panics_the_positions_cache_on_the_next_insert() {
    let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

    let event_a = replica_a
        .send(NestedListUpdate::insert(0, CounterUpdate::Inc(1)))
        .unwrap();
    replica_b.receive(event_a);

    let event_b = replica_b.send(NestedListUpdate::delete(0)).unwrap();
    let event_a_second = replica_a
        .send(NestedListUpdate::insert(1, CounterUpdate::Inc(2)))
        .unwrap();
    let event_a_third = replica_a
        .send(NestedListUpdate::insert(2, CounterUpdate::Inc(3)))
        .unwrap();

    replica_b.receive(event_a_second);
    // The only thing this read does is populate the positions cache.
    assert_eq!(replica_b.query(&Read::new()), vec![2]);
    // Used to panic: `insertion index (is 2) should be <= len (is 1)`.
    replica_b.receive(event_a_third);
    replica_a.receive(event_b);

    assert_eq!(replica_a.query(&Read::new()), vec![2, 3]);
    assert_eq!(replica_b.query(&Read::new()), vec![2, 3]);
}

/// Ours: `list::nested_list::tests::a_read_between_deliveries_puts_two_inserts_from_one_replica_in_the_wrong_order`.
#[test]
fn nested_list_a_read_between_deliveries_puts_two_inserts_from_one_replica_in_the_wrong_order() {
    let (mut replica_a, mut replica_b) = twins_log::<NestedListLog<VecLog<Counter<i32>>>>();

    let event_a = replica_a
        .send(NestedListUpdate::insert(0, CounterUpdate::Inc(1)))
        .unwrap();
    replica_b.receive(event_a);

    let event_b = replica_b
        .send(NestedListUpdate::insert(0, CounterUpdate::Inc(2)))
        .unwrap();
    let event_a_second = replica_a
        .send(NestedListUpdate::insert(1, CounterUpdate::Inc(3)))
        .unwrap();
    let event_a_third = replica_a
        .send(NestedListUpdate::insert(2, CounterUpdate::Inc(4)))
        .unwrap();

    replica_b.receive(event_a_second);
    // The only thing this read does is populate the positions cache.
    assert_eq!(replica_b.query(&Read::new()), vec![2, 1, 3]);
    replica_b.receive(event_a_third);
    replica_a.receive(event_b);

    assert_eq!(replica_a.query(&Read::new()), vec![2, 1, 3, 4]);
    assert_eq!(replica_b.query(&Read::new()), vec![2, 1, 3, 4]);
}
