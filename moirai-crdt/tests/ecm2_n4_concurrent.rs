//! EC.M2f (spec: cea-cdrt-knowledge/spec/items/EC.M2f.md), claim C2: concurrent
//! insert, update and delete schedules on a nested list converge to the list
//! our line's rule (1e05cc1) gives. An id the ordering holds is an element,
//! whatever its child reads, the default included; an id the ordering no
//! longer holds is not, even though its child stays in the map.
//!
//! Each scenario is a script of sends and deliveries with a hand-computed
//! list, checked in two configurations, as `ecm2_n1_json_remove.rs` does:
//! (a) the scripted replicas are the only members, and each one receives the
//! events it has not seen in every order; (b) an observer member receives every
//! event in every order. Every read must equal the expected list, and all the
//! reads of a scenario must be equal.
//!
//! Where two inserts are concurrent at one position, `a`'s comes first: that is
//! the order Léo's own `nested_list::tests::concurrent_insert` asserts.

use std::{collections::BTreeSet, fmt::Debug};

use moirai_crdt::{
    counter::{Counter, update::CounterUpdate},
    list::{nested_list::NestedListLog, update::NestedListUpdate},
};
use moirai_macros::record;
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    crdt::{eval::EvalNested, query::Read},
    replica::{IsReplica, Replica},
    state::{log::IsLog, po_log::VecLog},
};

record!(Duet {
    first: VecLog<Counter<i32>>,
    second: VecLog<Counter<i32>>,
});

type CounterLog = VecLog<Counter<i32>>;
type Op<L> = NestedListUpdate<<L as IsLog>::Update>;

struct Run<L: IsLog> {
    replicas: Vec<Replica<NestedListLog<L>, Tcsb<Op<L>>>>,
    events: Vec<EventMessage<Op<L>>>,
    seen: Vec<BTreeSet<usize>>,
}

impl<L: IsLog> Run<L> {
    fn new(members: &[&str]) -> Self {
        Self {
            replicas: members
                .iter()
                .map(|id| Replica::bootstrap((*id).into(), members))
                .collect(),
            events: Vec::new(),
            seen: vec![BTreeSet::new(); members.len()],
        }
    }

    /// Replica `r` issues `op`; returns the event's index.
    fn send(&mut self, r: usize, op: Op<L>) -> usize {
        let description = format!("{op:?}");
        let event = self.replicas[r]
            .send(op)
            .unwrap_or_else(|e| panic!("replica {r} rejected {description}: {e}"));
        self.events.push(event);
        self.seen[r].insert(self.events.len() - 1);
        self.events.len() - 1
    }

    fn deliver(&mut self, r: usize, e: usize) {
        assert!(self.seen[r].insert(e), "script delivers event {e} twice");
        self.replicas[r].receive(self.events[e].clone());
    }

    fn missing(&self, r: usize) -> Vec<usize> {
        (0..self.events.len())
            .filter(|e| !self.seen[r].contains(e))
            .collect()
    }
}

fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
    if items.is_empty() {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, head);
            out.push(tail);
        }
    }
    out
}

/// Runs `script` on `replicas` in both configurations, every delivery order;
/// panics with every read that is not `expected` and every pair that disagrees.
fn check<L, V>(replicas: &[&str], expected: Vec<V>, script: fn(&mut Run<L>))
where
    L: IsLog,
    NestedListLog<L>: EvalNested<Read<Vec<V>>>,
    V: Clone + Debug + Default + PartialEq,
{
    let mut failures = Vec::new();
    let mut reads: Vec<(String, Vec<V>)> = Vec::new();
    let mut with_observer = replicas.to_vec();
    with_observer.push("o");
    for (config, members, targets) in [
        (
            "members only",
            replicas.to_vec(),
            (0..replicas.len()).collect(),
        ),
        ("with observer", with_observer, vec![replicas.len()]),
    ] {
        let mut probe = Run::<L>::new(&members);
        script(&mut probe);
        for r in targets {
            for order in permutations(&probe.missing(r)) {
                let mut run = Run::<L>::new(&members);
                script(&mut run);
                for e in &order {
                    run.deliver(r, *e);
                }
                let got = run.replicas[r].query(&Read::<Vec<V>>::new());
                let label = format!("[{config}] {} after {order:?}", members[r]);
                if got != expected {
                    failures.push(format!("{label}: expected {expected:?}, read {got:?}"));
                }
                reads.push((label, got));
            }
        }
    }
    for (label, got) in &reads[1..] {
        if *got != reads[0].1 {
            failures.push(format!(
                "diverged: {label} reads {got:?} but {} reads {:?}",
                reads[0].0, reads[0].1
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failure(s) over {} read(s), first 12:\n  {}",
        failures.len(),
        reads.len(),
        failures[..failures.len().min(12)].join("\n  ")
    );
}

/// `a` creates an element holding nothing (a reset counter) while `b` inserts
/// at the same position: both are elements, `a`'s first.
#[test]
fn a_created_child_beside_a_concurrent_insert() {
    check::<CounterLog, i32>(&["a", "b"], vec![0, 2], |run| {
        run.send(0, NestedListUpdate::insert(0, CounterUpdate::Reset));
        run.send(1, NestedListUpdate::insert(0, CounterUpdate::Inc(2)));
    });
}

/// `a` writes its element back to zero while `b` inserts after it: the
/// element stays, reading 0.
#[test]
fn a_child_returned_to_default_beside_a_concurrent_insert() {
    check::<CounterLog, i32>(&["a", "b"], vec![0, 1], |run| {
        let e = run.send(0, NestedListUpdate::insert(0, CounterUpdate::Inc(10)));
        run.deliver(1, e);
        run.send(0, NestedListUpdate::update(0, CounterUpdate::Dec(10)));
        run.send(1, NestedListUpdate::insert(1, CounterUpdate::Inc(1)));
    });
}

/// `a` deletes the element while `b` resets it: the concurrent update keeps
/// the element (Léo's `concurrent_update_delete` keeps it at 5), and what it
/// holds is the reset, 0.
#[test]
fn an_update_to_default_concurrent_with_a_delete_keeps_the_element() {
    check::<CounterLog, i32>(&["a", "b"], vec![0], |run| {
        let e = run.send(0, NestedListUpdate::insert(0, CounterUpdate::Inc(3)));
        run.deliver(1, e);
        run.send(0, NestedListUpdate::delete(0));
        run.send(1, NestedListUpdate::update(0, CounterUpdate::Reset));
    });
}

/// `a` deletes its element while `b` inserts an empty one after it: the
/// deleted element stays out, though its child is still in the map, and the
/// empty one is in.
#[test]
fn a_delete_concurrent_with_the_insert_of_an_empty_child() {
    check::<CounterLog, i32>(&["a", "b"], vec![0], |run| {
        let e = run.send(0, NestedListUpdate::insert(0, CounterUpdate::Inc(1)));
        run.deliver(1, e);
        run.send(0, NestedListUpdate::delete(0));
        run.send(1, NestedListUpdate::insert(1, CounterUpdate::Reset));
    });
}

/// A record created with `New` and nothing written into it, concurrent with
/// one written into: both are elements, `a`'s first.
#[test]
fn a_created_record_beside_a_concurrent_written_one() {
    let written = DuetValue::<i32, i32> {
        first: 1,
        ..Default::default()
    };
    let expected = vec![DuetValue::default(), written];
    check::<DuetLog, DuetValue<i32, i32>>(&["a", "b"], expected, |run| {
        run.send(0, NestedListUpdate::insert(0, Duet::New));
        run.send(
            1,
            NestedListUpdate::insert(0, Duet::First(CounterUpdate::Inc(1))),
        );
    });
}

/// Léo's `list::nested_list::tests::concurrent_update_delete_insert_2`
/// (`moirai-crdt/src/list/nested_list.rs` at 4a42422), the same script, re-ruled
/// under 1e05cc1 and checked in every delivery order (his three orders are
/// among them). It read `[1]`; it reads `[0, 1]`. `c` inserted its element with
/// a reset counter, ahead of `b`'s, and nobody deleted it, so it is an element
/// reading 0; `b`'s element, deleted by `b` and by `c`, is kept by `a`'s
/// concurrent update at 1, the insert's 1 being seen by both deletes.
#[test]
fn concurrent_update_delete_insert_2() {
    check::<CounterLog, i32>(&["a", "b", "c"], vec![0, 1], |run| {
        let b_1 = run.send(1, NestedListUpdate::insert(0, CounterUpdate::Inc(1)));
        run.deliver(0, b_1);
        run.deliver(2, b_1);
        run.send(1, NestedListUpdate::delete(0));
        run.send(0, NestedListUpdate::update(0, CounterUpdate::Inc(1)));
        run.send(2, NestedListUpdate::insert(0, CounterUpdate::Reset));
        run.send(2, NestedListUpdate::delete(1));
    });
}
