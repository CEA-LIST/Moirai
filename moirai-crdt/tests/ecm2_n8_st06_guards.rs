//! EC.M2 row N8: the four behavior guards G1 to G4 of our
//! `moirai-crdt/tests/st06_optional_content_required.rs` from
//! `akira/moirai-modelstore` at `748a185`, translated to Léo's API on the
//! merged line (lane HEAD `b1e254c`). Same operations, delivery orders and
//! assertions. Only names change: resettable `Counter::Inc` ->
//! `CounterUpdate::Inc` on his `counter::Counter`, `List::insert` ->
//! `ListUpdate::insert`, `option::Optional` -> `option::update::OptionUpdate`
//! (imported as `Optional`), `state::graph_log::GraphLog` ->
//! `state::po_log::GraphLog`, `utils::membership::twins_log` ->
//! `utils::bootstrap::twins_log`.
//!
//! `st06_c1_an_empty_optional_content_impl_for_a_wrapper_does_not_compile`
//! is not here: its subject is our trait `OptionalContent`, which his API
//! does not have (his `OptionLog<L>` asks only `L: IsLog`).

use moirai_crdt::counter::{Counter, update::CounterUpdate};
use moirai_crdt::list::{eg_walker::List, update::ListUpdate};
use moirai_crdt::option::{OptionLog, update::OptionUpdate as Optional};
use moirai_crdt::utils::bootstrap::twins_log;
use moirai_macros::record;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::replica::IsReplica;
use moirai_protocol::state::po_log::{GraphLog, VecLog};

/// G1, `POLog`: an optional counter. Unset alone, it is absent; unset while
/// a concurrent write increments it, it holds that write alone.
#[test]
fn st06_g1_po_log_under_an_optional_takes_its_unset_as_from_any_parent() {
    type Opt = OptionLog<VecLog<Counter<i32>>>;
    let read = Read::<Option<i32>>::new();

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(CounterUpdate::Inc(3))).unwrap();
    b.receive(x);
    let u = a.send(Optional::Unset).unwrap();
    b.receive(u);
    let alone = [a.query(&read), b.query(&read)];

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(CounterUpdate::Inc(10))).unwrap();
    b.receive(x);
    let w = a.send(Optional::Set(CounterUpdate::Inc(2))).unwrap();
    let u = b.send(Optional::Unset).unwrap();
    a.receive(u);
    b.receive(w);
    let raced = [a.query(&read), b.query(&read)];

    println!("ST06-G1 POLog: unset alone {alone:?}, unset raced by +2 {raced:?}");
    assert_eq!(alone, [None, None], "ST.06 G1: POLog, an unset alone");
    assert_eq!(raced, [Some(2), Some(2)], "ST.06 G1: POLog, an unset raced");
}

/// G2, `GraphLog`: an optional text. A text never empties, since its reset
/// records the deletion: unset alone, it stays and reads ""; unset while a
/// concurrent write inserts, it reads that insert alone.
#[test]
fn st06_g2_graph_log_under_an_optional_takes_its_unset_as_from_any_parent() {
    type Opt = OptionLog<GraphLog<List<char>>>;
    let read = Read::<Option<String>>::new();

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(ListUpdate::insert('x', 0))).unwrap();
    b.receive(x);
    let y = a.send(Optional::Set(ListUpdate::insert('y', 1))).unwrap();
    b.receive(y);
    let u = a.send(Optional::Unset).unwrap();
    b.receive(u);
    let alone = [a.query(&read), b.query(&read)];

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(Optional::Set(ListUpdate::insert('x', 0))).unwrap();
    b.receive(x);
    let w = a.send(Optional::Set(ListUpdate::insert('y', 1))).unwrap();
    let u = b.send(Optional::Unset).unwrap();
    a.receive(u);
    b.receive(w);
    let raced = [a.query(&read), b.query(&read)];

    println!("ST06-G2 GraphLog: unset alone {alone:?}, unset raced by an insert {raced:?}");
    let empty = Some(String::new());
    assert_eq!(
        alone,
        [empty.clone(), empty],
        "ST.06 G2: GraphLog, an unset alone"
    );
    let y = Some("y".to_string());
    assert_eq!(raced, [y.clone(), y], "ST.06 G2: GraphLog, an unset raced");
}

record!(G3Box {
    k: VecLog<Counter<i32>> => i32,
    note: OptionLog<VecLog<Counter<i32>>> => Option<i32>,
});
record!(G3Holder {
    tick: VecLog<Counter<i32>> => i32,
    inner: OptionLog<G3BoxLog> => Option<G3BoxValue>,
});

/// G3, the log `record!` generates: a `G3Box` in an optional. Unset alone,
/// it is absent. Unset while a concurrent write increments its `k`, it holds
/// `k` 10 alone, and its optional `note`, which the unset emptied, is absent.
#[test]
fn st06_g3_a_record_under_an_optional_hands_its_unset_to_every_field() {
    let read = Read::<G3HolderValue>::new();
    let into = |op: G3Box| G3Holder::Inner(Optional::Set(op));

    let (mut a, mut b) = twins_log::<G3HolderLog>();
    let x = a.send(into(G3Box::K(CounterUpdate::Inc(4)))).unwrap();
    b.receive(x);
    let u = a.send(G3Holder::Inner(Optional::Unset)).unwrap();
    b.receive(u);
    let alone = [a.query(&read).inner, b.query(&read).inner];

    let (mut a, mut b) = twins_log::<G3HolderLog>();
    let x = a
        .send(into(G3Box::Note(Optional::Set(CounterUpdate::Inc(1)))))
        .unwrap();
    b.receive(x);
    let u = a.send(G3Holder::Inner(Optional::Unset)).unwrap();
    let w = b.send(into(G3Box::K(CounterUpdate::Inc(10)))).unwrap();
    a.receive(w);
    b.receive(u);
    let raced: Vec<Option<(i32, Option<i32>)>> = [a.query(&read), b.query(&read)]
        .into_iter()
        .map(|value| value.inner.map(|inner| (inner.k, inner.note)))
        .collect();

    println!("ST06-G3 record!: unset alone {alone:?}, unset raced by k +10 {raced:?}");
    assert_eq!(alone, [None, None], "ST.06 G3: record!, an unset alone");
    assert_eq!(
        raced,
        vec![Some((10, None)), Some((10, None))],
        "ST.06 G3: record!, an unset raced by a write to k"
    );
}

/// G4, `OptionLog`: an optional counter held by an optional. The outer
/// unset alone leaves both absent; raced by a concurrent write through both,
/// it leaves that write alone.
#[test]
fn st06_g4_an_optional_under_an_optional_takes_its_unset_as_from_any_parent() {
    type Opt = OptionLog<OptionLog<VecLog<Counter<i32>>>>;
    let read = Read::<Option<Option<i32>>>::new();
    let set = |by: i32| Optional::Set(Optional::Set(CounterUpdate::Inc(by)));

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(set(2)).unwrap();
    b.receive(x);
    let u = a.send(Optional::Unset).unwrap();
    b.receive(u);
    let alone = [a.query(&read), b.query(&read)];

    let (mut a, mut b) = twins_log::<Opt>();
    let x = a.send(set(2)).unwrap();
    b.receive(x);
    let w = a.send(set(3)).unwrap();
    let u = b.send(Optional::Unset).unwrap();
    a.receive(u);
    b.receive(w);
    let raced = [a.query(&read), b.query(&read)];

    println!("ST06-G4 OptionLog: outer unset alone {alone:?}, raced by +3 {raced:?}");
    assert_eq!(alone, [None, None], "ST.06 G4: OptionLog, an unset alone");
    assert_eq!(
        raced,
        [Some(Some(3)), Some(Some(3))],
        "ST.06 G4: OptionLog, an unset raced"
    );
}
