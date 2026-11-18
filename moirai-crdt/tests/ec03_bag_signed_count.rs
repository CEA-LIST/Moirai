//! EC.03, fix F1: a bag's copy count is a signed net, read out clamped at
//! zero (our `07d8825`, on `akira/moirai-modelstore` since 2026-09).
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03.md`. Two replicas that
//! each observe the single copy of a value and each remove it both issue a
//! valid remove; with an unsigned count the net `1 - 1 - 1` underflows, on
//! the next read or when the removes stabilize. The cases are the inline
//! tests of `moirai-crdt/src/bag/aw_bag.rs@748a185` (four) and
//! `rw_bag.rs@748a185` (three), written against this tree's API, with their
//! reads as written there: a value whose net is zero or below holds no copy.

use std::{collections::BTreeMap, fmt::Debug};

use moirai_crdt::{
    bag::{aw_bag::AWBagLog, rw_bag::RWBagLog, update::BagUpdate},
    utils::bootstrap::twins_log,
};
use moirai_protocol::{
    crdt::{eval::EvalNested, query::Read},
    replica::IsReplica,
    state::log::IsLog,
};
use rustc_hash::FxHashMap;

type Counts = Read<FxHashMap<usize, usize>>;

fn read<L>(replica: &impl IsReplica<L>) -> BTreeMap<usize, usize>
where
    L: IsLog + EvalNested<Counts>,
{
    replica.query(&Counts::new()).into_iter().collect()
}

fn concurrent_removes_of_one_copy<L>()
where
    L: IsLog<Command = BagUpdate<usize>> + EvalNested<Counts>,
    L::Rejection: Debug,
{
    let (mut a, mut b) = twins_log::<L>();
    let add = a.send(BagUpdate::Add(7)).unwrap();
    b.receive(add);
    let remove_a = a.send(BagUpdate::Remove(7)).unwrap();
    let remove_b = b.send(BagUpdate::Remove(7)).unwrap();
    a.receive(remove_b);
    b.receive(remove_a);
    assert_eq!(read(&a), BTreeMap::new());
    assert_eq!(read(&b), BTreeMap::new());
}

fn concurrent_removes_then_stable<L>()
where
    L: IsLog<Command = BagUpdate<usize>> + EvalNested<Counts>,
    L::Rejection: Debug,
{
    let (mut a, mut b) = twins_log::<L>();
    let add = a.send(BagUpdate::Add(7)).unwrap();
    b.receive(add);
    let remove_a = a.send(BagUpdate::Remove(7)).unwrap();
    let remove_b = b.send(BagUpdate::Remove(7)).unwrap();
    a.receive(remove_b);
    b.receive(remove_a);
    // One more write from each side makes both removes causally stable.
    let spacer_a = a.send(BagUpdate::Add(8)).unwrap();
    let spacer_b = b.send(BagUpdate::Add(9)).unwrap();
    a.receive(spacer_b);
    b.receive(spacer_a);
    let want = BTreeMap::from([(8, 1), (9, 1)]);
    assert_eq!(read(&a), want);
    assert_eq!(read(&b), want);
}

fn remove_of_an_absent_value<L>()
where
    L: IsLog<Command = BagUpdate<usize>> + EvalNested<Counts>,
    L::Rejection: Debug,
{
    let (mut a, mut b) = twins_log::<L>();
    let remove = a.send(BagUpdate::Remove(7)).unwrap();
    b.receive(remove);
    assert_eq!(read(&a), BTreeMap::new());
    assert_eq!(read(&b), BTreeMap::new());
}

/// `a` folds the remove before the add, `b` the add before the remove; an
/// exact net reads 0 on both, a count clamped at each step would not.
fn remove_concurrent_with_an_add<L>()
where
    L: IsLog<Command = BagUpdate<usize>> + EvalNested<Counts>,
    L::Rejection: Debug,
{
    let (mut a, mut b) = twins_log::<L>();
    let remove_a = a.send(BagUpdate::Remove(7)).unwrap();
    let add_b = b.send(BagUpdate::Add(7)).unwrap();
    a.receive(add_b);
    b.receive(remove_a);
    assert_eq!(read(&a), BTreeMap::new());
    assert_eq!(read(&b), BTreeMap::new());
}

#[test]
fn ec03_f1_aw_bag_concurrent_removes_of_one_copy_read_empty() {
    concurrent_removes_of_one_copy::<AWBagLog<usize>>();
}

#[test]
fn ec03_f1_aw_bag_concurrent_removes_stabilize_without_underflow() {
    concurrent_removes_then_stable::<AWBagLog<usize>>();
}

#[test]
fn ec03_f1_aw_bag_remove_of_an_absent_value_reads_empty() {
    remove_of_an_absent_value::<AWBagLog<usize>>();
}

#[test]
fn ec03_f1_aw_bag_remove_concurrent_with_an_add_converges() {
    remove_concurrent_with_an_add::<AWBagLog<usize>>();
}

#[test]
fn ec03_f1_rw_bag_concurrent_removes_of_one_copy_read_empty() {
    concurrent_removes_of_one_copy::<RWBagLog<usize>>();
}

#[test]
fn ec03_f1_rw_bag_concurrent_removes_stabilize_without_underflow() {
    concurrent_removes_then_stable::<RWBagLog<usize>>();
}

#[test]
fn ec03_f1_rw_bag_remove_of_an_absent_value_reads_empty() {
    remove_of_an_absent_value::<RWBagLog<usize>>();
}
