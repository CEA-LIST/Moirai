//! SI.05 round 3, oracle only: the behaviour Moirai `be10455` added to the
//! identity references, which no test of rounds 1 and 2 reached.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.05.md`, sections "2. The
//! claim" and "Round 3, written before any rerun". Every expected read-out
//! here is computed by hand from the claim and from the generated runtime it
//! follows, never read off the code under test:
//!
//! - links are not containment. The generated runtime keeps a reference as an
//!   arc of its typed reference graph, and its record carries no field for
//!   it, so an object that holds nothing but links is, for its container, an
//!   object at its default: an optional (`OptionLog`) and a keyed collection
//!   (`UWMapLog`) leave it out, and a link concurrent with the removal of its
//!   source keeps nothing alive (S2, S3, S4);
//! - a link is read only while its target is in the read-out, and only from
//!   a source the read-out holds (K3; the graph reads an arc whose two
//!   vertices are there);
//! - a removal takes away the links it had seen to the objects it removed,
//!   and a creation concurrent with the removal is not removed by it (S1,
//!   ruling R3.1 of the spec);
//! - every replica serves the same document once it has delivered the same
//!   operations, whatever the order (R2), and here every order also ends in
//!   the same document.
//!
//! Each concurrent schedule runs on three real `ModelLog` replicas, in every
//! delivery order causal broadcast allows at every replica ([`every_order`]).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si05_round3_be10455
//! ```

mod si05;

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{class_slot, feature_slot, table};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_protocol::replica::IsReplica;
use moirai_semantics::{ClassSlot, FeatureSlot};
use serde_json::{Value, json};

use si05::*;

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
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

/// Every identity a document serves.
fn served(doc: &Value) -> BTreeSet<String> {
    identities(doc).into_iter().map(|(id, _)| id).collect()
}

/// `base` with `more` added.
fn with(base: &BTreeSet<String>, more: &str) -> BTreeSet<String> {
    let mut out = base.clone();
    out.insert(more.to_string());
    out
}

/// One run of a concurrent schedule: the replicas once every writer has
/// sent, what each replica has still to deliver, the identities the checks
/// name, and every identity the base served.
struct Run {
    replicas: [Rep; 3],
    /// Per replica, the messages it has not delivered yet, as streams: each
    /// stream in the order causal broadcast imposes on it, the streams
    /// concurrent with one another.
    incoming: [Vec<Vec<Msg>>; 3],
    named: BTreeMap<&'static str, String>,
    base: BTreeSet<String>,
}

impl Run {
    fn id(&self, name: &str) -> &str {
        self.named
            .get(name)
            .unwrap_or_else(|| panic!("the schedule names no `{name}`"))
    }
}

/// Run `schedule` in every delivery order: at each replica every
/// interleaving of its incoming streams, in every combination with the
/// interleavings at the other two. `check` reads the agreed document of each
/// run, and every run must serve the same document as the first. Answers how
/// many runs there were.
fn every_order(label: &str, schedule: impl Fn() -> Run, check: impl Fn(&Value, &Run)) -> usize {
    let probe = schedule();
    let per_replica: Vec<Vec<Vec<(usize, usize)>>> = probe
        .incoming
        .iter()
        .map(|streams| {
            let indexed: Vec<Vec<(usize, usize)>> = streams
                .iter()
                .enumerate()
                .map(|(stream, messages)| (0..messages.len()).map(|i| (stream, i)).collect())
                .collect();
            interleavings(&indexed)
        })
        .collect();
    let mut combos: Vec<Vec<usize>> = vec![Vec::new()];
    for options in &per_replica {
        combos = combos
            .into_iter()
            .flat_map(|combo| {
                (0..options.len()).map(move |choice| {
                    let mut next = combo.clone();
                    next.push(choice);
                    next
                })
            })
            .collect();
    }
    let mut first: Option<Value> = None;
    for combo in &combos {
        let mut run = schedule();
        for (replica, &choice) in combo.iter().enumerate() {
            for &(stream, index) in &per_replica[replica][choice] {
                let message = run.incoming[replica][stream][index].clone();
                run.replicas[replica].receive(message);
            }
        }
        let doc = agreed(&run.replicas, &format!("{label}, orders {combo:?}"));
        check(&doc, &run);
        match &first {
            None => first = Some(doc),
            Some(earlier) => assert_eq!(
                &doc, earlier,
                "{label}: orders {combo:?} end in a different document from the first orders"
            ),
        }
    }
    combos.len()
}

// ---------------------------------------------------------------------------
// The board (`si05board.metamodel.json`)
// ---------------------------------------------------------------------------

/// The root and `x`, `y`, `z` titled at `Board.cards[0..3]`; with `lane` an
/// empty lane at `Board.lanes[0]`, with `pinned` a card put in
/// `Board.pinned` and nothing written into it. Built on replica 0 and
/// delivered; answers the replicas with the identities of `x`, `y`, `z` and
/// every identity served.
fn board_base(
    board: &Board,
    lane: bool,
    pinned: bool,
) -> ([Rep; 3], [String; 3], BTreeSet<String>) {
    let mut replicas = three(&board_descriptor());
    let mut ops = vec![
        board.root(),
        board.new_card(0),
        board.new_card(1),
        board.new_card(2),
    ];
    ops.extend(board.title_ops(At::Board(0), "x"));
    ops.extend(board.title_ops(At::Board(1), "y"));
    ops.extend(board.title_ops(At::Board(2), "z"));
    if lane {
        ops.push(board.new_lane(0));
    }
    if pinned {
        ops.push(board.new_pinned());
    }
    broadcast(&mut replicas, 0, ops, "building the board");
    let doc = agreed(&replicas, "the board");
    let ids = [0, 1, 2].map(|i| id_at(&doc, At::Board(i)));
    (replicas, ids, served(&doc))
}

/// `Unset` of `Board.pinned`.
fn unset_pinned(board: &Board) -> ModelOp {
    ModelOp::Instance(InstanceOp::variant(
        board.board,
        InstanceOp::field(board.board_pinned, InstanceOp::unset()),
    ))
}

/// Whether a document serves no card in `Board.pinned`.
fn pinned_absent(doc: &Value) -> bool {
    doc.get("pinned").is_none_or(Value::is_null)
}

// ---------------------------------------------------------------------------
// S1: a creation concurrent with the removal of its container
// ---------------------------------------------------------------------------

/// S1a. `b` creates `C` in the lane; `a` delivers the creation and links
/// `x.blocker` and `x.related` to `C`; `c`, having seen none of it, deletes
/// the lane. The creation is an update of the lane concurrent with its
/// deletion, so the lane stays with `C` in it (the containment is
/// update-wins); the links are concurrent with the deletion, which did not
/// remove `C`, so they are read: a link is read exactly when its target is.
#[test]
fn si05_r3_s1a_a_link_to_a_creation_concurrent_with_the_removal_of_its_lane_is_read() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], base) = board_base(&board, true, false);
        let create = send(
            &mut replicas[1],
            board.new_lane_card(0, 0),
            "b creates C in the lane",
        );
        replicas[0].receive(create.clone());
        let c = id_at(&identified(&replicas[0]), At::Lane(0, 0));
        assert_eq!(
            c,
            id_at(&identified(&replicas[1]), At::Lane(0, 0)),
            "premise: a reads C under the identity b serves it with"
        );
        let link_blocker = send_wire(
            &mut replicas[0],
            board.link(At::Board(0), board.blocker, &c),
            "a links x.blocker -> C",
        );
        let link_related = send_wire(
            &mut replicas[0],
            board.link(At::Board(0), board.related, &c),
            "a links x.related -> C",
        );
        let delete = send(&mut replicas[2], board.delete_lane(0), "c deletes the lane");
        Run {
            replicas,
            incoming: [
                vec![vec![delete.clone()]],
                vec![
                    vec![link_blocker.clone(), link_related.clone()],
                    vec![delete],
                ],
                vec![vec![create, link_blocker, link_related]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("C", c)]),
            base,
        }
    };
    let runs = every_order("S1a", schedule, |doc, run| {
        assert_eq!(
            doc["lanes"].as_array().map(Vec::len),
            Some(1),
            "S1a: the lane stays (update-wins): {doc}"
        );
        assert_eq!(
            id_at(doc, At::Lane(0, 0)),
            run.id("C"),
            "S1a: C stays in the lane under its identity"
        );
        assert_eq!(
            served(doc),
            with(&run.base, run.id("C")),
            "S1a: the base and C are served"
        );
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[run.id("C")]),
            "S1a: the link concurrent with the deletion is read, C being in the read-out"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[run.id("C")]),
            "S1a: the many-valued link too"
        );
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")]
        );
    });
    assert_eq!(runs, 3, "a: 1 order, b: 3 interleavings, c: 1 order");
}

/// S1b, the round-1 review's case (`node.rs:1509` there): a link to the
/// identity a creation is served under, written by a replica that has not
/// delivered the creation (a predicted `<origin>@<seq>.<step>`, which this
/// intake accepts), and seen by a removal of the creation's container that
/// the creation is concurrent with. `b` creates `C` in the lane; `a`, without
/// `C`, links `x.blocker` and `x.related` to `C`'s identity; `c` delivers the
/// links and deletes the lane. The creation keeps the lane and `C`
/// (update-wins). The deletion did not remove `C`, whose creation it had not
/// seen, so it takes no link to `C` away (ruling R3.1), and the links are
/// read with `C` on every replica, whichever of the creation and the deletion
/// it delivered first.
#[test]
fn si05_r3_s1b_a_removal_drops_no_link_to_a_creation_it_had_not_seen_and_all_replicas_agree() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], base) = board_base(&board, true, false);
        let create = send(
            &mut replicas[1],
            board.new_lane_card(0, 0),
            "b creates C in the lane",
        );
        let c = id_at(&identified(&replicas[1]), At::Lane(0, 0));
        assert!(
            card(&identified(&replicas[0]), At::Lane(0, 0)).is_none(),
            "premise: a has not delivered C's creation, so the identity it links is predicted"
        );
        let link_blocker = send_wire(
            &mut replicas[0],
            board.link(At::Board(0), board.blocker, &c),
            "a links x.blocker -> C, predicted",
        );
        let link_related = send_wire(
            &mut replicas[0],
            board.link(At::Board(0), board.related, &c),
            "a links x.related -> C, predicted",
        );
        replicas[2].receive(link_blocker.clone());
        replicas[2].receive(link_related.clone());
        let delete = send(
            &mut replicas[2],
            board.delete_lane(0),
            "c, which has the links and not C, deletes the lane",
        );
        Run {
            replicas,
            incoming: [
                vec![vec![create.clone()], vec![delete.clone()]],
                vec![vec![link_blocker, link_related, delete]],
                vec![vec![create]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("C", c)]),
            base,
        }
    };
    let runs = every_order("S1b", schedule, |doc, run| {
        assert_eq!(
            doc["lanes"].as_array().map(Vec::len),
            Some(1),
            "S1b: the creation concurrent with the deletion keeps the lane: {doc}"
        );
        assert_eq!(
            id_at(doc, At::Lane(0, 0)),
            run.id("C"),
            "S1b: C is in the lane under the identity a linked"
        );
        assert_eq!(
            served(doc),
            with(&run.base, run.id("C")),
            "S1b: the base and C are served"
        );
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[run.id("C")]),
            "S1b (R3.1): a removal that had not seen C's creation takes no link to C away"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[run.id("C")]),
            "S1b (R3.1): the many-valued link too"
        );
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")]
        );
    });
    assert_eq!(runs, 2, "a: 2 interleavings, b: 1 order, c: 1 order");
}

// ---------------------------------------------------------------------------
// S2: an optional source that holds only links
// ---------------------------------------------------------------------------

/// S2a, the round-1 review's case (`node.rs:1399` there) on `Board.pinned`:
/// `a` put a card there and wrote nothing into it, so it is not served (an
/// optional leaves out a child at its default). `b` unsets `Board.pinned`;
/// `c`, concurrently, links that card's `blocker` to `y` and its `related` to
/// `z`. A card holding only links is at its default for the optional, the
/// generated record carrying no field for a reference, so no replica serves
/// a pinned card in any order, and neither link is read.
#[test]
fn si05_r3_s2a_a_pinned_card_holding_only_links_is_served_nowhere_whatever_the_unset_order() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], base) = board_base(&board, false, true);
        assert!(
            pinned_absent(&identified(&replicas[0])),
            "premise: a card put in Board.pinned with nothing written into it is not served"
        );
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let link_blocker = send_wire(
            &mut replicas[2],
            board.link(At::Pinned, board.blocker, &y),
            "c links pinned.blocker -> y",
        );
        let link_related = send_wire(
            &mut replicas[2],
            board.link(At::Pinned, board.related, &z),
            "c links pinned.related -> z",
        );
        assert!(
            pinned_absent(&identified(&replicas[2])),
            "S2a: links alone put no card in Board.pinned, on their writer either"
        );
        Run {
            replicas,
            incoming: [
                vec![
                    vec![unset.clone()],
                    vec![link_blocker.clone(), link_related.clone()],
                ],
                vec![vec![link_blocker, link_related]],
                vec![vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z)]),
            base,
        }
    };
    let runs = every_order("S2a", schedule, |doc, run| {
        assert!(
            pinned_absent(doc),
            "S2a: no replica serves a pinned card that holds only links: {}",
            doc["pinned"]
        );
        assert_eq!(
            served(doc),
            run.base,
            "S2a: nothing is served beyond the base"
        );
        for (index, replica) in run.replicas.iter().enumerate() {
            assert!(
                pinned_absent(&default_read(replica)),
                "S2a (K3): replica {index}'s default read serves no pinned card either"
            );
        }
        assert_eq!(refs(doc, At::Board(0), "blocker"), set(&[]));
        assert_eq!(refs(doc, At::Board(0), "related"), set(&[]));
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")]
        );
    });
    assert_eq!(runs, 3, "a: 3 interleavings, b: 1 order, c: 1 order");
}

/// S2b, the other side of S2a (Q2 for the source): the same, except that `c`
/// titles the card, concurrently with the unset, before it links. The title
/// is a write the unset had not seen, so the card stays (update-wins, ST.02)
/// holding `t`, and the link the unset had not seen is read, on every
/// replica in every order.
#[test]
fn si05_r3_s2b_a_title_written_concurrently_with_the_unset_keeps_the_pinned_card_and_its_link() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], base) = board_base(&board, false, true);
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let mut from_c = Vec::new();
        for op in board.title_ops(At::Pinned, "t") {
            from_c.push(send(&mut replicas[2], op, "c titles the pinned card"));
        }
        from_c.push(send_wire(
            &mut replicas[2],
            board.link(At::Pinned, board.blocker, &y),
            "c links pinned.blocker -> y",
        ));
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], from_c.clone()],
                vec![from_c],
                vec![vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z)]),
            base,
        }
    };
    let runs = every_order("S2b", schedule, |doc, run| {
        assert!(
            !pinned_absent(doc),
            "S2b: the title written concurrently with the unset keeps the pinned card"
        );
        assert_eq!(
            title_at(doc, At::Pinned),
            "t",
            "S2b: the pinned card holds `t`"
        );
        let pinned = id_at(doc, At::Pinned);
        assert!(
            !run.base.contains(&pinned),
            "S2b: the pinned card has an identity of its own"
        );
        assert_eq!(
            served(doc),
            with(&run.base, &pinned),
            "S2b: the base and the pinned card are served"
        );
        assert_eq!(
            refs(doc, At::Pinned, "blocker"),
            set(&[run.id("y")]),
            "S2b: the link the unset had not seen is read while the card is served"
        );
        assert_eq!(refs(doc, At::Pinned, "related"), set(&[]));
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")]
        );
    });
    assert_eq!(runs, 3, "a: 3 interleavings, b: 1 order, c: 1 order");
}

// ---------------------------------------------------------------------------
// The keyed board (`si05keyed.metamodel.json`)
// ---------------------------------------------------------------------------

/// The keyed board: `Board.slots` is a keyed collection of `Card`s (by
/// `Slot.key`), and a `Card` holds `title` (`iD`), the optional counter
/// `rank`, and the identity references `blocker` and `related`. The
/// metamodel is `fixtures/si05/si05keyed.ecore`, and this is what
/// `arachne describe` writes for it (spec, round 3).
const KEYED: &str = include_str!("fixtures/si05/si05keyed.metamodel.json");

/// The keyed board's descriptor, after checking the three facts the tests
/// rest on, read off the JSON alone.
fn keyed_descriptor() -> Value {
    let descriptor: Value = serde_json::from_str(KEYED).expect("the keyed fixture is JSON");
    let slots = &descriptor["classes"]["Board"]["containments"][1];
    assert_eq!(
        (slots["name"].as_str(), &slots["merge"]),
        (
            Some("slots"),
            &json!({"kind": "containment", "shape": {"kind": "keyed", "key": {"kind": "str"}}, "target": "Card"})
        ),
        "premise: Board.slots is a keyed collection of Cards"
    );
    let card = &descriptor["classes"]["Card"];
    assert_eq!(
        (
            card["attributes"][1]["name"].as_str(),
            &card["attributes"][1]["merge"]
        ),
        (
            Some("rank"),
            &json!({"kind": "attribute", "leaf": {"kind": "counter", "num": "i32", "resettable": true}, "shape": {"kind": "optional"}})
        ),
        "premise: Card.rank is an optional resettable counter"
    );
    for (index, many) in [(0, false), (1, true)] {
        assert_eq!(
            card["references"][index]["merge"],
            json!({"kind": "identityReference", "many": many, "target": "Card"}),
            "premise: Card's references are on the identity rule"
        );
    }
    descriptor
}

/// The keyed board's slots, and the operations its tests send.
struct Keyed {
    board: ClassSlot,
    card: ClassSlot,
    cards: FeatureSlot,
    slots: FeatureSlot,
    title: FeatureSlot,
    rank: FeatureSlot,
    blocker: FeatureSlot,
    related: FeatureSlot,
}

impl Keyed {
    fn new() -> Keyed {
        let sem = table(&keyed_descriptor());
        let board = class_slot(&sem, "Board");
        let card = class_slot(&sem, "Card");
        Keyed {
            board,
            card,
            cards: feature_slot(&sem, board, "cards"),
            slots: feature_slot(&sem, board, "slots"),
            title: feature_slot(&sem, card, "title"),
            rank: feature_slot(&sem, card, "rank"),
            blocker: feature_slot(&sem, card, "blocker"),
            related: feature_slot(&sem, card, "related"),
        }
    }

    fn on_board(&self, inner: InstanceOp) -> ModelOp {
        ModelOp::Instance(InstanceOp::variant(self.board, inner))
    }

    /// `inner` addressed to the card at `Board.slots[key]`.
    fn on_entry(&self, key: &str, inner: InstanceOp) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.slots,
            InstanceOp::entry(Scalar::text(key), InstanceOp::variant(self.card, inner)),
        ))
    }

    /// The root, and `x`, `y`, `z` titled at `Board.cards[0..3]`.
    fn base_ops(&self) -> Vec<ModelOp> {
        let mut ops = vec![self.on_board(InstanceOp::New)];
        for (pos, ch) in [(0, 'x'), (1, 'y'), (2, 'z')] {
            ops.push(self.on_board(InstanceOp::field(
                self.cards,
                InstanceOp::insert(pos, InstanceOp::variant(self.card, InstanceOp::New)),
            )));
            ops.push(self.on_board(InstanceOp::field(
                self.cards,
                InstanceOp::at(
                    pos,
                    InstanceOp::variant(
                        self.card,
                        InstanceOp::field(
                            self.title,
                            InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch })),
                        ),
                    ),
                ),
            )));
        }
        ops
    }

    /// `rank` of the card at `key` up by one.
    fn rank_up(&self, key: &str) -> ModelOp {
        self.on_entry(
            key,
            InstanceOp::field(
                self.rank,
                InstanceOp::set(InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1)))),
            ),
        )
    }

    /// A card put at `key` with nothing written into it.
    fn bare(&self, key: &str) -> ModelOp {
        self.on_entry(key, InstanceOp::New)
    }

    /// K2's `Link` at a reference of the card at `key`.
    fn link(&self, key: &str, feature: FeatureSlot, id: &str) -> ModelOp {
        self.on_entry(
            key,
            InstanceOp::field(feature, InstanceOp::Leaf(LeafOp::Link(id.to_string()))),
        )
    }

    /// `Remove` of `key`.
    fn remove(&self, key: &str) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.slots,
            InstanceOp::remove(Scalar::text(key)),
        ))
    }

    /// `Clear` of `Board.slots`.
    fn clear(&self) -> ModelOp {
        self.on_board(InstanceOp::field(self.slots, InstanceOp::clear()))
    }
}

/// Three replicas of a model on the keyed board, its root and `x`, `y`, `z`
/// then `ops` built on replica 0 and delivered; answers them with the
/// identities of `x`, `y`, `z` and every identity served.
fn keyed_base(keyed: &Keyed, ops: Vec<ModelOp>) -> ([Rep; 3], [String; 3], BTreeSet<String>) {
    let mut replicas = three(&keyed_descriptor());
    let mut all = keyed.base_ops();
    all.extend(ops);
    broadcast(&mut replicas, 0, all, "building the keyed board");
    let doc = agreed(&replicas, "the keyed board");
    let ids = [0, 1, 2].map(|i| {
        doc["cards"][i][ID]
            .as_str()
            .unwrap_or_else(|| panic!("no card at Board.cards[{i}] in {doc}"))
            .to_string()
    });
    (replicas, ids, served(&doc))
}

// ---------------------------------------------------------------------------
// S3: a keyed source
// ---------------------------------------------------------------------------

/// S3, the removal of an entry concurrent with links from it. The base: the
/// card at key `k` holds `rank = 1` and a link `related -> z` every replica
/// has. `b` takes `k` away (`Remove` of `k`, or `Clear` of the collection);
/// `c`, concurrently, links `k.blocker` and `k.related` to `y`. The removal
/// resets `rank` and the link it had seen, which leaves the card holding only
/// the links it had not seen. A keyed collection leaves out an entry that
/// reads as a fresh card (`UWMapLog`), and links are no part of what a card
/// holds for its container, so no replica serves `k`, and no link from it is
/// read, in any order.
fn s3_removal_concurrent_with_links_from_the_entry(label: &str, clear: bool) {
    let keyed = Keyed::new();
    let schedule = || {
        let (mut replicas, [x, y, z], _) = keyed_base(&keyed, vec![keyed.rank_up("k")]);
        broadcast(
            &mut replicas,
            0,
            vec![keyed.link("k", keyed.related, &z)],
            "a links k.related -> z",
        );
        let doc = agreed(&replicas, &format!("{label}, the base"));
        let entry = &doc["slots"]["k"];
        assert_eq!(
            (
                &entry["rank"],
                reference_values(entry.get("related"), "related")
            ),
            (&json!(1), set(&[&z])),
            "premise: the entry at k is served, holding rank 1 and a link to z: {doc}"
        );
        let k = entry[ID]
            .as_str()
            .expect("premise: the card at k has an identity")
            .to_string();
        let mut base = served(&doc);
        assert!(base.remove(&k), "premise: the card at k is served");
        let removal = if clear {
            keyed.clear()
        } else {
            keyed.remove("k")
        };
        let removal = send(&mut replicas[1], removal, "b takes k away");
        let link_blocker = send(
            &mut replicas[2],
            keyed.link("k", keyed.blocker, &y),
            "c links k.blocker -> y",
        );
        let link_related = send(
            &mut replicas[2],
            keyed.link("k", keyed.related, &y),
            "c links k.related -> y",
        );
        Run {
            replicas,
            incoming: [
                vec![
                    vec![removal.clone()],
                    vec![link_blocker.clone(), link_related.clone()],
                ],
                vec![vec![link_blocker, link_related]],
                vec![vec![removal]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("k", k)]),
            base,
        }
    };
    let runs = every_order(label, schedule, |doc, run| {
        assert_eq!(
            doc["slots"],
            json!({}),
            "{label}: no replica serves the entry the removal took away, links or not"
        );
        assert_eq!(
            served(doc),
            run.base,
            "{label}: the root, x, y and z are all that is served; the card at k is not"
        );
        for (index, replica) in run.replicas.iter().enumerate() {
            assert_eq!(
                default_read(replica)["slots"],
                json!({}),
                "{label} (K3): replica {index}'s default read serves no entry either"
            );
        }
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")]
        );
    });
    assert_eq!(runs, 3, "a: 3 interleavings, b: 1 order, c: 1 order");
}

#[test]
fn si05_r3_s3_a_key_removed_concurrently_with_links_from_its_entry_is_served_nowhere() {
    s3_removal_concurrent_with_links_from_the_entry("S3 Remove", false);
}

#[test]
fn si05_r3_s3_a_clear_concurrent_with_links_from_an_entry_leaves_no_entry() {
    s3_removal_concurrent_with_links_from_the_entry("S3 Clear", true);
}

// ---------------------------------------------------------------------------
// S4: a link creates no object
// ---------------------------------------------------------------------------

/// S4 on the board. `b` creates a card at `Board.cards[3]` and keeps it to
/// itself for now; `c` links `x.related` to the identity that card is served
/// under (predicted), `x.blocker` to an identity no operation creates, and
/// the `blocker` and `related` of `Board.pinned`, where no card was ever put,
/// to `y` and `z`. Delivered to everyone, the creation to nobody yet: no
/// replica serves an object it did not serve before, nor a pinned card, nor
/// a link to a target it does not serve. Once the creation is delivered, the
/// card is served and so is the link to it (K3); the other links still read
/// nothing.
#[test]
fn si05_r3_s4_a_link_creates_no_object_on_the_board() {
    let board = Board::new();
    let (mut replicas, [x, y, z], base) = board_base(&board, false, false);
    let create = send(
        &mut replicas[1],
        board.new_card(3),
        "b creates a card at Board.cards[3]",
    );
    let p = id_at(&identified(&replicas[1]), At::Board(3));
    let links: Vec<Msg> = [
        (At::Board(0), board.related, p.as_str()),
        (At::Board(0), board.blocker, "nobody@1.1"),
        (At::Pinned, board.blocker, y.as_str()),
        (At::Pinned, board.related, z.as_str()),
    ]
    .into_iter()
    .map(|(at, feature, id)| send_wire(&mut replicas[2], board.link(at, feature, id), "c links"))
    .collect();
    for link in &links {
        deliver_all(&mut replicas, 2, link);
    }
    for index in [0, 2] {
        let doc = identified(&replicas[index]);
        assert_eq!(
            served(&doc),
            base,
            "S4: replica {index} serves no object the links created: {doc}"
        );
        assert!(
            pinned_absent(&doc),
            "S4: a link puts no card in Board.pinned (replica {index})"
        );
        assert_eq!(refs(&doc, At::Board(0), "related"), set(&[]));
        assert_eq!(refs(&doc, At::Board(0), "blocker"), set(&[]));
    }
    let doc = identified(&replicas[1]);
    assert!(
        pinned_absent(&doc),
        "S4: no pinned card on replica 1 either"
    );
    assert_eq!(
        served(&doc),
        with(&base, &p),
        "S4: replica 1 serves its own card and nothing the links created"
    );

    deliver_all(&mut replicas, 1, &create);
    let doc = agreed(&replicas, "S4, after the creation");
    assert_eq!(
        served(&doc),
        with(&base, &p),
        "S4: the card b created is the only object added"
    );
    assert!(pinned_absent(&doc), "S4: still no pinned card");
    assert_eq!(
        refs(&doc, At::Board(0), "related"),
        set(&[&p]),
        "S4 (K3): the link to the card is read once the card is served"
    );
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[]),
        "S4 (K3): a link to an identity nothing serves is not read"
    );
    assert!(
        pinned_absent(&default_read(&replicas[0])),
        "S4 (K3): the default read serves no pinned card"
    );
    assert_eq!(board_card_ids(&doc), vec![x, y, z, p]);
}

/// S4 on the keyed board: `a` put a card at key `m` and wrote nothing into
/// it, so `m` is not served (it reads as a fresh card); `c` links
/// `m.blocker` to `y`, and `n.related` to `z` at a key nobody wrote. No
/// replica serves an entry or any object beyond the base: a link alone never
/// creates an entry.
#[test]
fn si05_r3_s4_a_link_alone_creates_no_keyed_entry() {
    let keyed = Keyed::new();
    let (mut replicas, [x, y, z], base) = keyed_base(&keyed, vec![keyed.bare("m")]);
    assert_eq!(
        identified(&replicas[0])["slots"],
        json!({}),
        "premise: a card put at a key with nothing written into it is not served"
    );
    let links = [
        send(
            &mut replicas[2],
            keyed.link("m", keyed.blocker, &y),
            "c links m.blocker -> y",
        ),
        send(
            &mut replicas[2],
            keyed.link("n", keyed.related, &z),
            "c links n.related -> z",
        ),
    ];
    for link in &links {
        deliver_all(&mut replicas, 2, link);
    }
    let doc = agreed(&replicas, "S4 keyed");
    assert_eq!(
        doc["slots"],
        json!({}),
        "S4: a link alone creates no entry, at a key holding a bare card or at a key nobody wrote"
    );
    assert_eq!(served(&doc), base, "S4: no object is added");
    for (index, replica) in replicas.iter().enumerate() {
        assert_eq!(
            default_read(replica)["slots"],
            json!({}),
            "S4 (K3): replica {index}'s default read serves no entry either"
        );
    }
    assert_eq!(
        board_card_ids(&doc),
        vec![x, y, z],
        "the cards are untouched"
    );
}
