//! EC.03b: a `GraphLog` crosses a state transfer with its roots and its
//! critical versions, bound to the joiner's member table.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03b.md`. Léo's `List` folds
//! its history at a critical frontier (`eg_walker/hooks.rs`), which sets the
//! `EventGraph`'s roots and reads its critical versions. Three replicas build
//! a history whose first three events are folded and whose last two are
//! concurrent; a joiner `j` adopts `a`'s snapshot and log after a serde
//! round trip; then `a` and `j` deliver the same two events, the first
//! leaving the stable frontier non-critical, the second making x2, y2 and
//! z2 stable and critical, then `j` delivers `a`'s next event (which tells
//! it that `a` delivered them too). After the transfer and after each step, `j` holds what `a` holds: the same read, the same roots, the
//! same greatest critical frontier below its own version, the same graph
//! size and heads (compared by replica name, since `j`'s indices are its own).

use std::collections::{BTreeMap, BTreeSet};

use moirai_crdt::list::{eg_walker::List, update::ListUpdate};
use moirai_crdt::utils::bootstrap::triplet_log;
use moirai_protocol::{
    broadcast::tcsb::{StateSnapshot, Tcsb},
    clock::causal_frontier::CausalFrontier,
    crdt::query::Read,
    replica::{IsReplica, Replica},
    state::{log::IsLog, po_log::GraphLog, unstable_state::IsUnstableCausal},
};

type Log = GraphLog<List<char>>;
type Update = <Log as IsLog>::Update;
type Rep = Replica<Log, Tcsb<Update>>;
type Named = BTreeMap<String, usize>;

fn named(frontier: &CausalFrontier) -> Named {
    frontier
        .iter()
        .filter(|(_, seq)| *seq > 0)
        .map(|(idx, seq)| {
            let id = frontier
                .resolver()
                .resolve(idx)
                .expect("a frontier names its columns");
            (id.to_string(), seq)
        })
        .collect()
}

#[derive(Debug, PartialEq)]
struct Shape {
    read: String,
    roots: Option<Named>,
    critical: Option<Named>,
    nodes: usize,
    heads: BTreeSet<(String, usize)>,
}

fn shape(replica: &Rep) -> Shape {
    let graph = replica.log().unstable();
    let upper = CausalFrontier::from(replica.tcsb().matrix_clock().origin_version());
    Shape {
        read: replica.query(&Read::<String>::new()),
        roots: graph.roots().map(named),
        critical: graph.greatest_critical_frontier(&upper).as_ref().map(named),
        nodes: graph.graph().node_count(),
        heads: graph
            .heads()
            .iter()
            .map(|id| (id.origin_id().to_string(), id.seq()))
            .collect(),
    }
}

fn send(
    replica: &mut Rep,
    ch: char,
    pos: usize,
) -> moirai_protocol::broadcast::message::EventMessage<Update> {
    replica
        .send(ListUpdate::insert(ch, pos))
        .expect("an insert in bounds")
}

fn transfer(donor: &Rep) -> Rep {
    let snapshot: StateSnapshot<Update> = serde_json::from_str(
        &serde_json::to_string(&donor.snapshot()).expect("a snapshot serializes"),
    )
    .expect("and comes back");
    let log: Log =
        serde_json::from_str(&serde_json::to_string(donor.log()).expect("a log serializes"))
            .expect("and comes back");
    let mut joiner = Rep::bootstrap("j".into(), &["j"]);
    joiner.adopt(snapshot, log);
    joiner
}

fn names(pairs: &[(&str, usize)]) -> Named {
    pairs
        .iter()
        .map(|(id, seq)| (id.to_string(), *seq))
        .collect()
}

#[test]
fn ec03b_g1_a_joiner_holds_the_donors_roots_and_critical_versions_and_folds_as_it_does() {
    let (mut a, mut b, mut c) = triplet_log::<Log>();
    let x1 = send(&mut a, 'a', 0);
    b.receive(x1.clone());
    c.receive(x1);
    let y1 = send(&mut b, 'b', 1);
    a.receive(y1.clone());
    c.receive(y1);
    let z1 = send(&mut c, 'c', 2);
    a.receive(z1.clone());
    b.receive(z1);
    // Concurrent: neither has seen the other.
    let x2 = send(&mut a, 'd', 0);
    let y2 = send(&mut b, 'e', 3);
    a.receive(y2.clone());
    c.receive(x2.clone());
    c.receive(y2);
    let z2 = send(&mut c, 'f', 0);
    b.receive(x2);
    b.receive(z2.clone());
    let y3 = send(&mut b, 'g', 0);

    // Set-up: a has folded x1, y1, z1 and holds x2 and y2 unstable.
    let before = shape(&a);
    let set_up = Shape {
        read: before.read.clone(),
        roots: Some(names(&[("a", 1), ("b", 1), ("c", 1)])),
        critical: Some(names(&[("a", 2), ("b", 2), ("c", 1)])),
        nodes: 2,
        heads: BTreeSet::from([("a".to_string(), 2), ("b".to_string(), 2)]),
    };
    assert_eq!(
        before, set_up,
        "set-up: a's history is not the one this test needs"
    );

    let mut j = transfer(&a);
    let mut failures = Vec::new();
    let mut compare = |when: &str, a: &Rep, j: &Rep| {
        let (sa, sj) = (shape(a), shape(j));
        if sa != sj {
            failures.push(format!("{when}: a holds {sa:?}\n  and j holds {sj:?}"));
        }
    };
    compare("right after the transfer", &a, &j);
    a.receive(z2.clone());
    j.receive(z2);
    compare("after z2 (stable frontier not critical)", &a, &j);
    // y3 makes x2, y2 and z2 stable at a. j holds a's row as of the
    // snapshot, so it learns that a delivered z2 only from a's next event.
    a.receive(y3.clone());
    j.receive(y3);
    let x3 = send(&mut a, 'h', 0);
    j.receive(x3);
    compare(
        "after y3 and a's x3 (x2, y2, z2 stable and critical)",
        &a,
        &j,
    );
    drop(compare);
    let end = shape(&a);
    if end.roots != Some(names(&[("a", 2), ("b", 2), ("c", 2)])) || end.nodes != 2 {
        failures.push(format!("set-up: a did not fold x2, y2 and z2: {end:?}"));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
