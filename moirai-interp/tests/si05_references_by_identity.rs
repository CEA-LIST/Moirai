//! SI.05, references by store identity, on the interpreted log in process:
//! claims R1 and R2, the part of R6 a node can show, and the wire contract K2
//! and read-out contract K3 (`si05/mod.rs` restates them).
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.05.md`. Everything here runs
//! two or three real `ModelLog` replicas with delivery under the test's
//! control; the concurrent schedules are run in every delivery order causal
//! broadcast allows ([`interleavings`]), and every replica's identified read
//! is held equal to every other's at the end of each one. Parity with the
//! generated runtime (R3) is the twin suite in Arachne
//! (`generated/simpleuml_crdt/tests/si05_reference_twin*.rs`); two
//! `model_node` processes over HTTP are `si05_references_l3.rs`.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si05_references_by_identity
//! ```

mod si05;

use std::collections::BTreeSet;

use moirai_interp::testing::opened;
use moirai_protocol::replica::IsReplica;
use moirai_semantics::{from_descriptor, metamodel_digest};
use serde_json::{Value, json};

use si05::*;

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// A board with three titled cards `x`, `y`, `z` at `Board.cards[0..3]`,
/// built on replica 0 and delivered to the others.
fn xyz(board: &Board, replicas: &mut [Rep]) {
    let mut ops = vec![
        board.root(),
        board.new_card(0),
        board.new_card(1),
        board.new_card(2),
    ];
    ops.extend(board.title_ops(At::Board(0), "x"));
    ops.extend(board.title_ops(At::Board(1), "y"));
    ops.extend(board.title_ops(At::Board(2), "z"));
    broadcast(replicas, 0, ops, "building the board");
}

/// Every replica serves the same identified document; answers it.
fn agreed(replicas: &[Rep], label: &str) -> Value {
    let first = identified(&replicas[0]);
    for (index, replica) in replicas.iter().enumerate().skip(1) {
        assert_eq!(
            identified(replica),
            first,
            "{label}: replica {index} serves a different document from replica 0"
        );
    }
    first
}

// ---------------------------------------------------------------------------
// K2, the wire
// ---------------------------------------------------------------------------

/// K2: `Link` and `Unlink` are operations a node reads off the wire, and they
/// cross it unchanged.
#[test]
fn si05_k2_link_and_unlink_are_wire_operations() {
    let board = Board::new();
    for verb in ["Link", "Unlink"] {
        let json = board.reference_json(At::Board(1), board.related, verb, "a@4.1");
        let op = wire_op(json.clone()).unwrap_or_else(|why| panic!("{why}"));
        assert_eq!(
            serde_json::to_value(&op).expect("it serializes"),
            json,
            "K2: `{verb}` does not cross the wire unchanged"
        );
    }
}

// ---------------------------------------------------------------------------
// R1, identity
// ---------------------------------------------------------------------------

/// R1 and K3: a reference reads as its target's served identity, in the
/// identified read and in the default read, and never as a title. Renaming
/// the target's identifying attribute (`Card.title`, `iD="true"`) and another
/// attribute (`note`), on the other replica, moves nothing; a new card that
/// takes the old title, or the new one, is not what the reference reads.
#[test]
fn si05_r1_a_reference_holds_its_targets_identity_and_renames_do_not_move_it() {
    let board = Board::new();
    let mut replicas = three(&board_descriptor());
    xyz(&board, &mut replicas);
    let doc = identified(&replicas[0]);
    let (y, z) = (id_at(&doc, At::Board(1)), id_at(&doc, At::Board(2)));

    let link_y = board.link(At::Board(0), board.blocker, &y);
    let m1 = send_wire(&mut replicas[0], link_y, "link x.blocker -> y");
    let m2 = send_wire(
        &mut replicas[0],
        board.link(At::Board(0), board.related, &y),
        "link x.related -> y",
    );
    let m3 = send_wire(
        &mut replicas[0],
        board.link(At::Board(0), board.related, &z),
        "link x.related -> z",
    );
    for message in [m1, m2, m3] {
        deliver_all(&mut replicas, 0, &message);
    }

    let doc = agreed(&replicas, "after the links");
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[&y]),
        "R1: x.blocker reads y's identity"
    );
    assert_eq!(
        refs(&doc, At::Board(0), "related"),
        set(&[&y, &z]),
        "R1: x.related reads y and z"
    );
    for (at, title) in [(0, "x"), (1, "y"), (2, "z")] {
        assert_eq!(title_at(&doc, At::Board(at)), title);
    }
    assert!(
        !refs(&doc, At::Board(0), "blocker").contains("y"),
        "R1: a reference holds an identity, never the title"
    );
    // K3 on the default read: the same identity strings, without `__id`.
    let plain = default_read(&replicas[1]);
    assert_eq!(
        reference_values(plain["cards"][0].get("blocker"), "blocker"),
        set(&[&y]),
        "K3: the default read serves the same identity"
    );
    assert_eq!(
        reference_values(plain["cards"][0].get("related"), "related"),
        set(&[&y, &z]),
        "K3: the default read serves the same identities"
    );

    // Rename y's identifying attribute and its note on replica 1.
    let mut renames = vec![board.rename(At::Board(1), 'r')];
    renames.push(board.type_char(At::Board(1), board.note, 0, 'n'));
    broadcast(&mut replicas, 1, renames, "renaming y");
    // A new card on replica 2 that takes y's old title, and one that takes its new one.
    let mut fresh = vec![board.new_card(3), board.new_card(4)];
    fresh.extend(board.title_ops(At::Board(3), "y"));
    fresh.extend(board.title_ops(At::Board(4), "ry"));
    broadcast(&mut replicas, 2, fresh, "two cards named after y");

    let doc = agreed(&replicas, "after the renames");
    assert_eq!(title_at(&doc, At::Board(1)), "ry", "the rename took");
    assert_eq!(
        id_at(&doc, At::Board(1)),
        y,
        "SI.01: a rename keeps the served identity"
    );
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[&y]),
        "R1: a rename of the target's identifying attribute does not move x.blocker"
    );
    assert_eq!(refs(&doc, At::Board(0), "related"), set(&[&y, &z]));
    let (old_name, new_name) = (id_at(&doc, At::Board(3)), id_at(&doc, At::Board(4)));
    assert!(
        old_name != y && new_name != y,
        "the two new cards have identities of their own"
    );
    assert!(
        !refs(&doc, At::Board(0), "related").contains(&old_name)
            && !refs(&doc, At::Board(0), "related").contains(&new_name),
        "R1: no card is reached by a title"
    );
}

// ---------------------------------------------------------------------------
// R2, deletion
// ---------------------------------------------------------------------------

/// R2: a deletion of the target causally after the link removes it from the
/// read-out, on every replica; a card re-created with the same title at the
/// same place has a new identity and is not linked.
#[test]
fn si05_r2_a_deletion_after_the_link_removes_it_and_a_recreation_does_not_revive_it() {
    let board = Board::new();
    let mut replicas = three(&board_descriptor());
    xyz(&board, &mut replicas);
    let doc = identified(&replicas[0]);
    let (y, z) = (id_at(&doc, At::Board(1)), id_at(&doc, At::Board(2)));
    for op in [
        board.link(At::Board(0), board.blocker, &y),
        board.link(At::Board(0), board.related, &y),
        board.link(At::Board(0), board.related, &z),
    ] {
        let message = send_wire(&mut replicas[1], op, "the links");
        deliver_all(&mut replicas, 1, &message);
    }
    broadcast(&mut replicas, 2, vec![board.delete_card(1)], "delete y");
    let doc = agreed(&replicas, "after the deletion");
    assert_eq!(board_card_ids(&doc).len(), 2, "y is gone");
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[]),
        "R2: x.blocker no longer reads y"
    );
    assert_eq!(
        refs(&doc, At::Board(0), "related"),
        set(&[&z]),
        "R2: x.related reads z alone"
    );

    let mut again = vec![board.new_card(1)];
    again.extend(board.title_ops(At::Board(1), "y"));
    broadcast(&mut replicas, 0, again, "re-create y");
    let doc = agreed(&replicas, "after the re-creation");
    assert_eq!(title_at(&doc, At::Board(1)), "y");
    assert_ne!(
        id_at(&doc, At::Board(1)),
        y,
        "R2: the re-created card has a new identity"
    );
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[]),
        "R2: the re-creation revives nothing"
    );
    assert_eq!(
        refs(&doc, At::Board(0), "related"),
        set(&[&z]),
        "R2: the re-creation revives nothing"
    );
}

/// R2 through an ancestor: deleting a lane deletes its cards, and a link into
/// one of them goes with it.
#[test]
fn si05_r2_deleting_a_lane_removes_the_links_into_its_cards() {
    let board = Board::new();
    let mut replicas = three(&board_descriptor());
    xyz(&board, &mut replicas);
    let mut ops = vec![board.new_lane(0), board.new_lane_card(0, 0)];
    ops.extend(board.title_ops(At::Lane(0, 0), "w"));
    broadcast(&mut replicas, 1, ops, "a lane with card w");
    let doc = identified(&replicas[0]);
    let w = id_at(&doc, At::Lane(0, 0));
    let z = id_at(&doc, At::Board(2));
    for op in [
        board.link(At::Board(0), board.blocker, &w),
        board.link(At::Board(0), board.related, &w),
        board.link(At::Board(0), board.related, &z),
    ] {
        let message = send_wire(&mut replicas[0], op, "links into the lane");
        deliver_all(&mut replicas, 0, &message);
    }
    let doc = agreed(&replicas, "linked");
    assert_eq!(refs(&doc, At::Board(0), "related"), set(&[&w, &z]));

    broadcast(
        &mut replicas,
        2,
        vec![board.delete_lane(0)],
        "delete the lane",
    );
    let doc = agreed(&replicas, "after the lane went");
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[]),
        "R2: the link into the lane's card went with it"
    );
    assert_eq!(refs(&doc, At::Board(0), "related"), set(&[&z]));
}

/// The three replicas of the concurrent schedules below: a board with `x`,
/// `y`, `z` everywhere, then `b` (replica 1) and `c` (replica 2) partitioned
/// from everyone, each sending its own events.
struct Split {
    replicas: [Rep; 3],
    from_b: Vec<Msg>,
    from_c: Vec<Msg>,
    y: String,
    z: String,
}

/// Run `schedule` (which builds the split) in every delivery order: `a`
/// receives the interleavings of `b`'s and `c`'s events, `b` receives `c`'s
/// and `c` receives `b`'s. `check` reads the agreed document at the end of
/// each. Answers how many orders ran.
fn every_order(label: &str, schedule: impl Fn() -> Split, check: impl Fn(&Value, &Split)) -> usize {
    let probe = schedule();
    let orders = interleavings(&[
        (0..probe.from_b.len())
            .map(|i| (1usize, i))
            .collect::<Vec<_>>(),
        (0..probe.from_c.len())
            .map(|i| (2usize, i))
            .collect::<Vec<_>>(),
    ]);
    for order in &orders {
        let mut split = schedule();
        for (origin, index) in order {
            let message = if *origin == 1 {
                &split.from_b[*index]
            } else {
                &split.from_c[*index]
            };
            split.replicas[0].receive(message.clone());
        }
        for message in split.from_c.clone() {
            split.replicas[1].receive(message);
        }
        for message in split.from_b.clone() {
            split.replicas[2].receive(message);
        }
        let doc = agreed(&split.replicas, &format!("{label}, order {order:?}"));
        check(&doc, &split);
    }
    orders.len()
}

/// The base of every split: `x`, `y`, `z` built on `a` and delivered.
fn split_base(board: &Board, links: &[(usize, &str)]) -> ([Rep; 3], String, String) {
    let mut replicas = three(&board_descriptor());
    xyz(board, &mut replicas);
    let doc = identified(&replicas[0]);
    let (y, z) = (id_at(&doc, At::Board(1)), id_at(&doc, At::Board(2)));
    for (feature, which) in links {
        let slot = if *feature == 0 {
            board.blocker
        } else {
            board.related
        };
        let id = if *which == "y" { &y } else { &z };
        let message = send_wire(
            &mut replicas[0],
            board.link(At::Board(0), slot, id),
            "a base link",
        );
        deliver_all(&mut replicas, 0, &message);
    }
    (replicas, y, z)
}

/// R2, the concurrent half: `b` deletes `y` and renames `z`; `c`, which has
/// not seen the deletion, links `x.blocker` and `x.related` to `y` and
/// `x.related` to `z`. In every delivery order, no replica reads a link to
/// `y`, and every replica reads the link to `z`.
#[test]
fn si05_r2_a_link_concurrent_with_the_deletion_is_read_on_no_replica_in_any_order() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, y, z) = split_base(&board, &[]);
        let from_b = vec![
            send(&mut replicas[1], board.delete_card(1), "b deletes y"),
            send(
                &mut replicas[1],
                board.rename(At::Board(1), 'q'),
                "b renames z, now at 1",
            ),
        ];
        let from_c = vec![
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.blocker, &y),
                "c links x.blocker -> y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &y),
                "c links x.related -> y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &z),
                "c links x.related -> z",
            ),
        ];
        Split {
            replicas,
            from_b,
            from_c,
            y,
            z,
        }
    };
    let orders = every_order("R2 concurrent", schedule, |doc, split| {
        assert_eq!(
            board_card_ids(doc),
            vec![id_at(doc, At::Board(0)), split.z.clone()],
            "y is deleted"
        );
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[]),
            "R2: the link to y concurrent with its deletion is not read"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[&split.z]),
            "R2: only z is read"
        );
        assert!(!refs(doc, At::Board(0), "related").contains(&split.y));
    });
    assert_eq!(orders, 10, "C(5,2) interleavings at replica a");
}

/// R2 against the generated baseline, Q1 of the spec's section 1: the link
/// exists and every replica has it; `b` deletes `y` while `c` renames it. The
/// rename keeps `y` (the containment is update-wins, on both paths), and the
/// links the deletion saw stay removed, in every order.
#[test]
fn si05_r2_q1_a_link_the_deletion_saw_stays_removed_when_the_target_survives() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, y, z) = split_base(&board, &[(0, "y"), (1, "y"), (1, "z")]);
        let from_b = vec![send(&mut replicas[1], board.delete_card(1), "b deletes y")];
        let from_c = vec![send(
            &mut replicas[2],
            board.rename(At::Board(1), 'r'),
            "c renames y",
        )];
        Split {
            replicas,
            from_b,
            from_c,
            y,
            z,
        }
    };
    let orders = every_order("R2 Q1", schedule, |doc, split| {
        assert_eq!(
            id_at(doc, At::Board(1)),
            split.y,
            "the concurrent rename keeps y (update-wins)"
        );
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[]),
            "Q1: the link the deletion saw is not read again"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[&split.z]),
            "Q1: only z is read"
        );
    });
    assert_eq!(orders, 2);
}

/// R2 against the generated baseline, Q2: `b` deletes `y` while `c` renames
/// it and links `x.blocker` and `x.related` to it. `y` survives, and the
/// links the deletion did not see are read, in every order.
#[test]
fn si05_r2_q2_a_link_concurrent_with_the_deletion_is_read_when_an_update_keeps_the_target() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, y, z) = split_base(&board, &[]);
        let from_b = vec![send(&mut replicas[1], board.delete_card(1), "b deletes y")];
        let from_c = vec![
            send(
                &mut replicas[2],
                board.rename(At::Board(1), 'r'),
                "c renames y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.blocker, &y),
                "c links x.blocker -> y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &y),
                "c links x.related -> y",
            ),
        ];
        Split {
            replicas,
            from_b,
            from_c,
            y,
            z,
        }
    };
    let orders = every_order("R2 Q2", schedule, |doc, split| {
        assert_eq!(
            id_at(doc, At::Board(1)),
            split.y,
            "the concurrent rename keeps y"
        );
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[&split.y]),
            "Q2: the link the deletion did not see is read"
        );
        assert_eq!(refs(doc, At::Board(0), "related"), set(&[&split.y]));
    });
    assert_eq!(orders, 4);
}

// ---------------------------------------------------------------------------
// Concurrent writes to one reference
// ---------------------------------------------------------------------------

/// A single-valued reference retargeted on two replicas at once keeps exactly
/// one value, the same on every replica, in every order (which one is R3's,
/// matched against the generated graph in the twin suite).
#[test]
fn si05_single_valued_concurrent_retargets_keep_exactly_one_value() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, y, z) = split_base(&board, &[(0, "y")]);
        let doc = identified(&replicas[0]);
        let x = id_at(&doc, At::Board(0));
        let from_b = vec![
            send_wire(
                &mut replicas[1],
                board.unlink(At::Board(0), board.blocker, &y),
                "b unlinks y",
            ),
            send_wire(
                &mut replicas[1],
                board.link(At::Board(0), board.blocker, &z),
                "b links z",
            ),
        ];
        let from_c = vec![
            send_wire(
                &mut replicas[2],
                board.unlink(At::Board(0), board.blocker, &y),
                "c unlinks y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.blocker, &x),
                "c links x itself",
            ),
        ];
        Split {
            replicas,
            from_b,
            from_c,
            y,
            z,
        }
    };
    let orders = every_order("single-valued retarget", schedule, |doc, split| {
        let held = refs(doc, At::Board(0), "blocker");
        let x = id_at(doc, At::Board(0));
        assert_eq!(
            held.len(),
            1,
            "a single-valued reference keeps exactly one value: {held:?}"
        );
        assert!(
            held.contains(&split.z) || held.contains(&x),
            "it is one of the two retargets: {held:?}"
        );
    });
    assert_eq!(orders, 6);
}

/// A many-valued reference keeps every concurrent link, and a link of a
/// target concurrent with its unlink is kept (an unlink removes the links it
/// saw, as a `RemoveArc` makes only the arcs before it redundant).
#[test]
fn si05_many_valued_concurrent_links_are_kept_and_a_relink_survives_a_concurrent_unlink() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, y, z) = split_base(&board, &[(1, "y")]);
        let from_b = vec![
            send_wire(
                &mut replicas[1],
                board.link(At::Board(0), board.related, &z),
                "b links z",
            ),
            send_wire(
                &mut replicas[1],
                board.unlink(At::Board(0), board.related, &y),
                "b unlinks y",
            ),
        ];
        let from_c = vec![
            send_wire(
                &mut replicas[2],
                board.unlink(At::Board(0), board.related, &y),
                "c unlinks y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &y),
                "c links y again",
            ),
        ];
        Split {
            replicas,
            from_b,
            from_c,
            y,
            z,
        }
    };
    let orders = every_order("many-valued", schedule, |doc, split| {
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[&split.y, &split.z]),
            "both links are read: z added by b, y re-linked by c concurrently with b's unlink"
        );
    });
    assert_eq!(orders, 6);
}

/// Convergence where SI.01's served identity is decided by a merge: `a` and
/// `b` each put a new card in `Board.pinned` at once and title it (`p` on `a`,
/// `q` on `b`), so the two creations collapse into one object, and each links
/// `x.related` to the card it created. Every replica reads the same document,
/// in both orders. Only convergence is claimed (spec, F6): whether the link
/// written against the creation that loses resolves is not.
///
/// Amendment A1 (spec, "Round 1 oracle amendment A1"): each writer titles its
/// card before it reads the card's identity. An optional drops a child its
/// write left at the default, as the generated runtime's `OptionLog` does
/// (`option/mod.rs`, ST.02), so a card put there with nothing written into it
/// is not served and has no identity to link.
#[test]
fn si05_two_creations_of_the_pinned_card_converge_with_their_links() {
    let board = Board::new();
    for b_first in [false, true] {
        let mut replicas = three(&board_descriptor());
        xyz(&board, &mut replicas);
        let mut pending: Vec<(usize, Msg)> = Vec::new();
        let mut created: Vec<String> = Vec::new();
        for (from, title) in [(0usize, "p"), (1, "q")] {
            let mut ops = vec![send(&mut replicas[from], board.new_pinned(), "pin a card")];
            for op in board.title_ops(At::Pinned, title) {
                ops.push(send(&mut replicas[from], op, "title the pinned card"));
            }
            let doc = identified(&replicas[from]);
            let pinned = id_at(&doc, At::Pinned);
            ops.push(send_wire(
                &mut replicas[from],
                board.link(At::Board(0), board.related, &pinned),
                "link x.related -> the pinned card",
            ));
            created.push(pinned);
            pending.extend(ops.into_iter().map(|message| (from, message)));
        }
        assert_ne!(
            created[0], created[1],
            "premise: `a` and `b` each created the pinned card, so each was served an identity of its own"
        );
        if b_first {
            pending.reverse();
        }
        for (from, message) in pending {
            deliver_all(&mut replicas, from, &message);
        }
        let doc = agreed(&replicas, &format!("pinned, b first = {b_first}"));
        let served = id_at(&doc, At::Pinned);
        assert!(
            created.contains(&served),
            "premise: the two creations collapse into one pinned card, served as one of them; \
             it serves {served}, the creations were {created:?} (b first = {b_first})"
        );
    }
}

// ---------------------------------------------------------------------------
// R6, versioning
// ---------------------------------------------------------------------------

/// R6: the new rule names itself in the descriptor, so the board on the new
/// rule has a digest of its own, and a node reads it.
#[test]
fn si05_r6_the_new_rule_is_read_and_has_its_own_digest() {
    let new = board_descriptor();
    let today = board_today_descriptor();
    assert_ne!(
        metamodel_digest(&new),
        metamodel_digest(&today),
        "R6: the descriptor on the new rule is a different metamodel id"
    );
    from_descriptor(&new).unwrap_or_else(|error| {
        panic!("R6: a descriptor naming `identityReference` is refused: {error}")
    });
    from_descriptor(&today).expect("today's descriptor is read as before");
}

/// R6: a reference that declares an `eOpposite` keeps today's string rule in
/// a model on the new descriptor: `Lane.board` takes a string `Write` and
/// reads it back as written.
#[test]
fn si05_r6_an_eopposite_reference_keeps_todays_string_rule_on_the_new_descriptor() {
    let board = Board::new();
    let mut replicas = three(&board_descriptor());
    broadcast(
        &mut replicas,
        0,
        vec![
            board.root(),
            board.new_lane(0),
            board.write_lane_board(0, "any text"),
        ],
        "a lane whose board is written as a string",
    );
    let doc = agreed(&replicas, "eOpposite");
    assert_eq!(
        doc["lanes"][0]["board"],
        json!("any text"),
        "R6: Lane.board is still a string register"
    );
}

/// R6, the paired baseline: a model on today's descriptor keeps today's
/// string behaviour. A reference is the string written, a rename or a
/// deletion of the card it names moves nothing, and a many-valued one is a
/// set of strings. Passes before SI.05 and must still pass after it.
#[test]
fn si05_r6_base_a_model_on_todays_descriptor_keeps_string_references() {
    let board = Board::new();
    let (mut a, mut b) = opened("si05-today", &board_today_descriptor());
    let mut ops = vec![board.root(), board.new_card(0), board.new_card(1)];
    ops.extend(board.title_ops(At::Board(0), "x"));
    ops.extend(board.title_ops(At::Board(1), "y"));
    ops.push(board.write_string(At::Board(0), board.blocker, "y"));
    ops.push(board.add_string(At::Board(0), board.related, "y"));
    ops.push(board.add_string(At::Board(0), board.related, "nobody"));
    ops.push(board.rename(At::Board(1), 'r'));
    ops.push(board.delete_card(1));
    for op in ops {
        let message = a
            .send(op.clone())
            .unwrap_or_else(|e| panic!("today's rule refused {op:?}: {e:?}"));
        b.receive(message);
    }
    for replica in [&a, &b] {
        let doc: Value = replica.query(&moirai_protocol::crdt::query::Read::<Value>::new());
        assert_eq!(
            doc["cards"].as_array().map(Vec::len),
            Some(1),
            "y was deleted"
        );
        assert_eq!(
            doc["cards"][0]["blocker"],
            json!("y"),
            "a string reference is the string written"
        );
        assert_eq!(
            reference_values(doc["cards"][0].get("related"), "related"),
            set(&["nobody", "y"]),
            "a many-valued string reference is a set of strings"
        );
    }
}
