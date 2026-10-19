use moirai_protocol::{
    broadcast::internalizer::{Interner, Resolver},
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::query::Read,
    event::{Event, id::EventId, lamport::Lamport},
    replica::ReplicaIdx,
    state::{
        effect_context::EffectContext, graph_log::GraphLog, log::IsLog,
        unstable_state::IsUnstableCore,
    },
};

use super::{List, ReadAt};
use crate::{
    counter::resettable_counter::Counter,
    list::nested_list::{NestedList, NestedListLog},
};

type Log = GraphLog<List<char>>;

fn resolver() -> Resolver {
    let mut interner = Interner::new();
    for id in ["a", "b", "c"] {
        interner.intern(&id.into());
    }
    interner.resolver().clone()
}

fn event<O>(resolver: &Resolver, origin: usize, clock: [usize; 3], op: O) -> Event<O> {
    let mut version = Version::new(ReplicaIdx(origin), resolver.clone());
    for (idx, seq) in clock.into_iter().enumerate() {
        version.set_by_idx(ReplicaIdx(idx), seq);
    }
    Event::new(
        EventId::from(&version),
        Lamport::from(&version),
        op,
        version,
    )
}

fn apply<L: IsLog>(log: &mut L, event: Event<L::Op>) {
    log.effect(event, &mut EffectContext::silent());
}

fn read(log: &Log) -> String {
    log.eval(&Read::new())
}

#[test]
fn flushes_only_stable_critical_prefixes_and_preserves_retained_edges() {
    let r = resolver();
    let a = event(&r, 0, [1, 0, 0], List::insert('A', 0));
    let b = event(&r, 1, [0, 1, 0], List::insert('B', 0));
    let c = event(&r, 2, [1, 1, 1], List::insert('C', 2));
    let mut log = Log::default();
    for e in [&a, &b, &c] {
        apply(&mut log, e.clone());
    }

    // A is stable, but concurrent B prevents this cut from being critical.
    log.stabilize(&CausalFrontier::from(a.version()));
    assert!(log.stable().is_empty());
    assert_eq!(log.unstable().len(), 3);
    assert!(log.retention_boundary().is_none());

    // {A, B} is a multi-head critical version; C is still unstable.
    let mut cut = a.version().clone();
    cut.join(b.version());
    log.stabilize(&CausalFrontier::from(&cut));
    assert_eq!(log.stable(), &vec!['A', 'B']);
    assert_eq!(log.unstable().len(), 1);
    assert_eq!(read(&log), "ABC");
    assert_eq!(
        log.eval(&ReadAt::<Vec<char>>::new(&cut)).unwrap(),
        vec!['A', 'B']
    );
    let expired = log
        .eval(&ReadAt::<Vec<char>>::new(a.version()))
        .unwrap_err();
    assert_eq!(&expired.boundary, log.retention_boundary().unwrap());
    assert!(log.eval(&ReadAt::<Vec<char>>::new(b.version())).is_err());

    let d = event(&r, 0, [2, 1, 1], List::insert('D', 3));
    apply(&mut log, d.clone());
    assert_eq!(read(&log), "ABCD");
    log.stabilize(&CausalFrontier::from(c.version()));
    assert_eq!(log.stable(), &vec!['A', 'B', 'C']);
    assert_eq!(read(&log), "ABCD");
    log.stabilize(&CausalFrontier::from(d.version()));
    assert!(log.unstable().is_empty());
    assert_eq!(read(&log), "ABCD");

    let boundary = log.retention_boundary().unwrap().clone();
    let mut restored = Log::from_checkpoint(log.stable().clone(), boundary.clone());
    assert!(
        restored
            .eval(&ReadAt::<Vec<char>>::new(c.version()))
            .is_err()
    );
    assert_eq!(
        restored
            .eval(&ReadAt::<Vec<char>>::new(d.version()))
            .unwrap(),
        vec!['A', 'B', 'C', 'D']
    );
    let delete_first = event(&r, 0, [3, 1, 1], List::delete(0));
    apply(&mut restored, delete_first.clone());
    apply(&mut log, delete_first);
    assert_eq!(read(&restored), "BCD");
    assert_eq!(read(&restored), read(&log));
    // Repeated or older stability notifications never move the boundary back.
    log.stabilize(&boundary);
    log.stabilize(&CausalFrontier::from(a.version()));
    assert_eq!(log.retention_boundary(), Some(&boundary));
}

#[test]
fn chooses_an_earlier_critical_cut_when_the_stable_tail_is_concurrent() {
    let r = resolver();
    let a = event(&r, 0, [1, 0, 0], List::insert('A', 0));
    let b = event(&r, 1, [1, 1, 0], List::insert('B', 1));
    let c = event(&r, 2, [1, 0, 1], List::insert('C', 1));
    let mut log = Log::default();
    for e in [&a, &b, &c] {
        apply(&mut log, e.clone());
    }
    log.stabilize(&CausalFrontier::new(r));
    assert!(log.stable().is_empty());
    log.stabilize(&CausalFrontier::from(b.version()));
    assert_eq!(log.stable(), &vec!['A']);
    assert_eq!(log.unstable().len(), 2);
    assert_eq!(read(&log), "ABC");
}

#[test]
fn flushed_life_dots_preserve_update_wins_and_later_observed_deletion() {
    let r = resolver();
    let mut log = Log::default();
    for e in [
        event(&r, 0, [1, 0, 0], List::insert('X', 0)),
        event(&r, 1, [1, 1, 0], List::update(0)),
        event(&r, 2, [1, 0, 1], List::update(0)),
        event(&r, 0, [2, 1, 1], List::insert('Y', 1)),
    ] {
        apply(&mut log, e);
    }
    let delete = event(&r, 0, [3, 1, 1], List::delete(0));
    let update = event(&r, 1, [2, 2, 1], List::update(0));
    let mut other = log.clone();
    apply(&mut log, delete.clone());
    apply(&mut log, update.clone());
    apply(&mut other, update);
    apply(&mut other, delete);
    let cut = event(&r, 0, [2, 1, 1], ()).version().clone();
    for log in [&mut log, &mut other] {
        log.stabilize(&CausalFrontier::from(&cut));
        assert_eq!(log.stable(), &vec!['X', 'Y']);
        assert_eq!(read(log), "XY");
        let delete = event(&r, 0, [4, 2, 1], List::delete(0));
        apply(log, delete.clone());
        log.stabilize(&CausalFrontier::from(delete.version()));
        assert_eq!(read(log), "Y");
        assert!(log.unstable().is_empty());
    }
}

#[test]
fn rebuilding_a_concurrent_suffix_preserves_current_and_historical_reads() {
    let r = resolver();
    let events = [
        event(&r, 0, [1, 0, 0], List::insert('A', 0)),
        event(&r, 1, [1, 1, 0], List::update(0)),
        event(&r, 2, [1, 0, 1], List::insert('B', 1)),
        event(&r, 0, [2, 1, 1], List::delete_range(0, 2)),
        event(&r, 2, [1, 0, 2], List::update(1)),
        event(&r, 1, [1, 2, 0], List::insert('C', 1)),
        event(&r, 0, [3, 2, 2], List::insert('D', 1)),
    ];
    let mut full = Log::default();
    for e in &events {
        apply(&mut full, e.clone());
    }
    let mut compact = full.clone();
    compact.stabilize(&CausalFrontier::from(events[0].version()));
    assert_eq!(compact.stable(), &vec!['A']);
    assert_eq!(compact.unstable().len(), 6);
    assert_eq!(read(&compact), "CDB");
    for e in &events {
        let q = ReadAt::<Vec<char>>::new(e.version());
        assert_eq!(compact.eval(&q).unwrap(), full.eval(&q).unwrap());
    }
    let delete = event(&r, 1, [3, 3, 2], List::delete(0));
    apply(&mut full, delete.clone());
    apply(&mut compact, delete.clone());
    compact.stabilize(&CausalFrontier::from(delete.version()));
    assert_eq!(read(&compact), "DB");
    assert_eq!(read(&compact), read(&full));
    assert!(compact.unstable().is_empty());
}

#[test]
fn parent_delete_resets_a_flushed_child_list_but_preserves_concurrent_insert() {
    let r = resolver();
    let mut log = NestedListLog::<Log>::default();
    apply(
        &mut log,
        event(
            &r,
            0,
            [1, 0, 0],
            NestedList::insert(0, List::insert('X', 0)),
        ),
    );
    let update = event(
        &r,
        1,
        [1, 1, 0],
        NestedList::update(0, List::insert('Y', 1)),
    );
    apply(&mut log, update.clone());
    log.stabilize(&CausalFrontier::from(update.version()));
    let child = log.children().children().values().next().unwrap();
    assert_eq!(child.stable(), &vec!['X', 'Y']);
    assert!(child.unstable().is_empty());

    let delete = event(&r, 0, [2, 1, 0], NestedList::delete(0));
    let insert = event(
        &r,
        2,
        [1, 1, 1],
        NestedList::update(0, List::insert('Z', 2)),
    );
    let mut other = log.clone();
    apply(&mut log, delete.clone());
    apply(&mut log, insert.clone());
    apply(&mut other, insert.clone());
    apply(&mut other, delete.clone());
    let mut cut = delete.version().clone();
    cut.join(insert.version());
    for log in [&mut log, &mut other] {
        log.stabilize(&CausalFrontier::from(&cut));
        assert_eq!(log.eval(&Read::<Vec<Vec<char>>>::new()), vec![vec!['Z']]);
        apply(
            log,
            event(
                &r,
                0,
                [3, 1, 1],
                NestedList::update(0, List::insert('T', 0)),
            ),
        );
        assert_eq!(
            log.eval(&Read::<Vec<Vec<char>>>::new()),
            vec![vec!['T', 'Z']]
        );
    }
}

#[test]
fn nested_updates_keep_child_identity_after_positions_are_flushed() {
    use moirai_protocol::state::po_log::VecLog;
    let r = resolver();
    let mut left = NestedListLog::<VecLog<Counter<i32>>>::default();
    let insert = event(&r, 0, [1, 0, 0], NestedList::insert(0, Counter::Inc(10)));
    let id = insert.id().clone();
    apply(&mut left, insert);
    let update = event(&r, 1, [1, 1, 0], NestedList::update(0, Counter::Inc(2)));
    apply(&mut left, update.clone());
    assert_eq!(left.eval(&Read::<Vec<i32>>::new()), vec![12]);
    left.stabilize(&CausalFrontier::from(update.version()));
    assert_eq!(left.positions().stable(), &vec![id.clone()]);
    assert!(left.positions().unstable().is_empty());
    let mut right = left.clone();
    let delete = event(&r, 0, [2, 1, 0], NestedList::delete(0));
    let update = event(&r, 2, [1, 1, 1], NestedList::update(0, Counter::Inc(5)));
    apply(&mut left, delete.clone());
    apply(&mut left, update.clone());
    apply(&mut right, update.clone());
    apply(&mut right, delete.clone());
    let mut cut = delete.version().clone();
    cut.join(update.version());
    for log in [&mut left, &mut right] {
        log.stabilize(&CausalFrontier::from(&cut));
        assert_eq!(log.positions().stable(), &vec![id.clone()]);
        assert_eq!(log.eval(&Read::<Vec<i32>>::new()), vec![5]);
        apply(
            log,
            event(&r, 0, [3, 1, 1], NestedList::update(0, Counter::Inc(2))),
        );
        assert_eq!(log.eval(&Read::<Vec<i32>>::new()), vec![7]);
        apply(log, event(&r, 1, [3, 2, 1], NestedList::delete(0)));
        assert!(log.eval(&Read::<Vec<i32>>::new()).is_empty());
    }
}

#[test]
#[should_panic(expected = "predates checkpoint")]
fn rejects_events_that_do_not_observe_the_checkpoint() {
    let r = resolver();
    let mut log = Log::default();
    let insert = event(&r, 0, [1, 0, 0], List::insert('A', 0));
    apply(&mut log, insert.clone());
    log.stabilize(&CausalFrontier::from(insert.version()));
    apply(&mut log, event(&r, 1, [0, 1, 0], List::insert('B', 0)));
}

#[test]
#[should_panic(expected = "delete range exceeds prepared document")]
fn invalid_replay_range_fails_explicitly() {
    let r = resolver();
    let mut log = Log::from_stable(vec!['A']);
    apply(&mut log, event(&r, 0, [1, 0, 0], List::delete_range(0, 2)));
    read(&log);
}
