//! SI.07, claim H1: the `test_utils` `Harness` reads a reference by identity
//! as the full model read does, a link read only while its target is in the
//! read-out.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.07.md`. The `Harness`
//! (`moirai_interp::testing`) is one model tree under one table, hosted by a
//! `Replica` as a log, that the equivalence oracles drive and read with
//! `Read<Value>`. The full model read (`eval::read`, what a node serves) reads
//! a link only while its target's identity is served (SI.05, K3); the
//! harness's read must read the same document from the same tree.
//!
//! Ground truth: which cards each document holds, read off the schedule, and
//! the full read `eval::read` of the same tree, which SI.05's oracle holds to
//! K3.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si07_h1_harness_read
//! ```

mod si05;

use std::collections::BTreeSet;
use std::sync::Arc;

use moirai_interp::InstanceOp;
use moirai_interp::eval::{read, read_identified};
use moirai_interp::leaf::LeafOp;
use moirai_interp::testing::{Harness, table, twins};
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::{FeatureSlot, MetamodelSemantics};
use serde_json::Value;

use si05::*;

type HarnessReplica = Replica<Harness, Tcsb<InstanceOp>>;

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// The board's operations as the harness takes them: the `InstanceOp` under
/// the root's `Variant`, with no `ModelOp` around it and no `Install`.
fn bare(op: moirai_interp::ModelOp) -> InstanceOp {
    match op {
        moirai_interp::ModelOp::Instance(inner) => inner,
        other => panic!("not an instance operation: {other:?}"),
    }
}

fn send_both(a: &mut HarnessReplica, b: &mut HarnessReplica, op: InstanceOp, what: &str) {
    let message = a
        .send(op.clone())
        .unwrap_or_else(|why| panic!("{what}: the harness refused {op:?}: {why:?}"));
    b.receive(message);
}

/// What the harness serves through `Read<Value>`.
fn harness_read(replica: &HarnessReplica) -> Value {
    replica.query(&Read::<Value>::new())
}

/// What the full model read serves from the same tree.
fn full_read(sem: &MetamodelSemantics, replica: &HarnessReplica) -> Value {
    read(sem, &replica.state().root)
}

/// The two harness replicas with `x`, `y`, `z` titled at `Board.cards[0..3]`;
/// answers them and the identities of `x`, `y`, `z`.
fn base(
    board: &Board,
    sem: &Arc<MetamodelSemantics>,
) -> (HarnessReplica, HarnessReplica, [String; 3]) {
    let (mut a, mut b) = twins(sem, "Board");
    let mut ops = vec![
        board.root(),
        board.new_card(0),
        board.new_card(1),
        board.new_card(2),
    ];
    ops.extend(board.title_ops(At::Board(0), "x"));
    ops.extend(board.title_ops(At::Board(1), "y"));
    ops.extend(board.title_ops(At::Board(2), "z"));
    for op in ops {
        send_both(&mut a, &mut b, bare(op), "building the board");
    }
    let doc = read_identified(sem, &a.state().root);
    let ids = [0, 1, 2].map(|i| id_at(&doc, At::Board(i)));
    (a, b, ids)
}

fn link(board: &Board, feature: FeatureSlot, id: &str) -> InstanceOp {
    bare(board.on_card(
        At::Board(0),
        InstanceOp::field(feature, InstanceOp::Leaf(LeafOp::Link(id.to_string()))),
    ))
}

/// H1-1. `x.blocker -> y` and `x.related -> y, z`, then `y` deleted, on two
/// harness replicas. Before the deletion both reads serve the three links;
/// after it, `y` is not in the read-out, so neither `x.blocker` nor `y` in
/// `x.related` is read, by the harness as by the full read, on both
/// replicas.
#[test]
fn si07_h1_1_the_harness_reads_no_link_to_a_target_the_read_out_no_longer_holds() {
    let board = Board::new();
    let sem = table(&board_descriptor());
    let (mut a, mut b, [_, y, z]) = base(&board, &sem);
    for (feature, target) in [
        (board.blocker, &y),
        (board.related, &y),
        (board.related, &z),
    ] {
        send_both(
            &mut a,
            &mut b,
            link(&board, feature, target),
            "a links from x",
        );
    }
    for (name, replica) in [("a", &a), ("b", &b)] {
        let doc = harness_read(replica);
        assert_eq!(
            (
                refs(&doc, At::Board(0), "blocker"),
                refs(&doc, At::Board(0), "related")
            ),
            (set(&[&y]), set(&[&y, &z])),
            "H1 premise: harness replica {name} reads the links to served targets"
        );
        assert_eq!(
            doc,
            full_read(&sem, replica),
            "H1 premise: replica {name}, the two reads agree"
        );
    }
    send_both(&mut a, &mut b, bare(board.delete_card(1)), "a deletes y");
    for (name, replica) in [("a", &a), ("b", &b)] {
        let doc = harness_read(replica);
        assert_eq!(
            refs(&doc, At::Board(0), "blocker"),
            set(&[]),
            "H1: harness replica {name} reads x.blocker -> y after y left the read-out"
        );
        assert_eq!(
            refs(&doc, At::Board(0), "related"),
            set(&[&z]),
            "H1: harness replica {name} reads y in x.related after y left the read-out"
        );
        assert_eq!(
            doc,
            full_read(&sem, replica),
            "H1: harness replica {name} reads other than the full model read of the same tree"
        );
    }
}

/// H1-2. A link to an identity nobody serves (`nobody@1.1`, at `x.related`)
/// is not read by the harness, as it is not by the full read.
#[test]
fn si07_h1_2_the_harness_reads_no_link_to_an_identity_nobody_serves() {
    let board = Board::new();
    let sem = table(&board_descriptor());
    let (mut a, mut b, [_, _, z]) = base(&board, &sem);
    send_both(
        &mut a,
        &mut b,
        link(&board, board.related, "nobody@1.1"),
        "a links an identity nobody serves",
    );
    send_both(&mut a, &mut b, link(&board, board.related, &z), "a links z");
    for (name, replica) in [("a", &a), ("b", &b)] {
        let doc = harness_read(replica);
        assert_eq!(
            refs(&doc, At::Board(0), "related"),
            set(&[&z]),
            "H1: harness replica {name} reads a link to an identity nobody serves"
        );
        assert_eq!(
            doc,
            full_read(&sem, replica),
            "H1: harness replica {name} reads other than the full model read of the same tree"
        );
    }
}
