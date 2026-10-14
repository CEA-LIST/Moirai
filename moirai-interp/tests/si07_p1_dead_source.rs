//! SI.07, claim P1 (parity) on the interpreted log: the links from an object
//! a removal reached, when a text keeps that object served.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.07.md`. Under a descriptor
//! with the identity rule, when a removal reaches an object whose creation it
//! had seen, the links from that object that the removal had seen are no
//! longer read, even if the object stays served because a text attribute
//! keeps it (ST.04: its title reads `""`); a link from it concurrent with the
//! removal is read exactly when the generated runtime reads it. The ground
//! truth is the generated runtime's typed reference graph, computed by hand
//! from its rules and held to the generated crates of the same two
//! metamodels by Arachne's `generated/simpleuml_crdt/tests/si07_p1_twin_*.rs`:
//!
//! - every create or update of a record adds the record's vertex; a
//!   `DeleteSubtree` over the optional or the key takes away the vertex
//!   additions it had seen and the arcs it had seen;
//! - a link adds an arc and no vertex;
//! - an arc is read only while both its vertices are in the graph.
//!
//! So a link from a card concurrent with the removal is not read while no
//! write the removal had not seen reaches the card (P1-2), is read when one
//! does (P1-3), and is read again once a later write reaches the card (P1-2,
//! revived). The text keeps the card in the read-out throughout (ST.04 on
//! both paths). Only optional and keyed sources show this: an object of a
//! sequence that a removal reached with no concurrent write is not served on
//! either path, and a link from an object the read-out does not hold is not
//! read (SI.05, R2).
//!
//! Each concurrent schedule runs on three real `ModelLog` replicas in every
//! delivery order causal broadcast allows at every replica, and every run
//! must end with the three replicas serving one identified document, the
//! same in every run.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si07_p1_dead_source
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

/// One run of a concurrent schedule: the replicas once every writer has
/// sent, what each replica has still to deliver (as streams, each in the
/// order causal broadcast imposes on it, the streams concurrent with one
/// another), and the identities the checks name.
struct Run {
    replicas: [Rep; 3],
    incoming: [Vec<Vec<Msg>>; 3],
    named: BTreeMap<&'static str, String>,
}

impl Run {
    fn id(&self, name: &str) -> &str {
        self.named
            .get(name)
            .unwrap_or_else(|| panic!("the schedule names no `{name}`"))
    }
}

/// A later edit sent on the delivered replicas, when a schedule has one.
type Later<'a> = Option<&'a dyn Fn(&mut [Rep; 3])>;

/// Run `schedule` in every delivery order: at each replica every
/// interleaving of its incoming streams, in every combination with the
/// interleavings at the other two. `check` reads the agreed document of each
/// run, which must be the same in every run; then `after`, when given, sends
/// a later edit on the delivered replicas and `check_after` reads the agreed
/// document again. Answers how many runs there were.
fn every_order(
    label: &str,
    schedule: impl Fn() -> Run,
    check: impl Fn(&Value, &Run),
    after: Later<'_>,
    check_after: impl Fn(&Value, &Run),
) -> usize {
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
        if let Some(after) = after {
            after(&mut run.replicas);
            let doc = agreed(
                &run.replicas,
                &format!("{label}, orders {combo:?}, then the later write"),
            );
            check_after(&doc, &run);
        }
    }
    combos.len()
}

// ---------------------------------------------------------------------------
// The board: an optional source, `Board.pinned`
// ---------------------------------------------------------------------------

/// `Unset` of `Board.pinned`.
fn unset_pinned(board: &Board) -> ModelOp {
    ModelOp::Instance(InstanceOp::variant(
        board.board,
        InstanceOp::field(board.board_pinned, InstanceOp::unset()),
    ))
}

/// The base: the root and `x`, `y`, `z` titled at `Board.cards[0..3]`, and a
/// card put in `Board.pinned` and titled `p`. Built on replica 0 and
/// delivered; answers the replicas, the identities of `x`, `y`, `z` and of
/// the pinned card.
fn board_base(board: &Board) -> ([Rep; 3], [String; 3], String) {
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
    ops.push(board.new_pinned());
    ops.extend(board.title_ops(At::Pinned, "p"));
    broadcast(&mut replicas, 0, ops, "building the board");
    let doc = agreed(&replicas, "the board");
    let ids = [0, 1, 2].map(|i| id_at(&doc, At::Board(i)));
    assert_eq!(
        title_at(&doc, At::Pinned),
        "p",
        "premise: the pinned card holds `p`"
    );
    let pinned = id_at(&doc, At::Pinned);
    (replicas, ids, pinned)
}

/// The served identities a board document must hold: the board, `x`, `y`,
/// `z` and the pinned card it serves.
fn board_served(doc: &Value, run: &Run) -> BTreeSet<String> {
    let mut ids = set(&[run.id("x"), run.id("y"), run.id("z")]);
    ids.insert(root_id(doc));
    ids.insert(id_at(doc, At::Pinned));
    ids
}

/// The board's own served identity.
fn root_id(doc: &Value) -> String {
    doc[ID]
        .as_str()
        .unwrap_or_else(|| panic!("the board carries no identity: {doc}"))
        .to_string()
}

/// The default read of every replica holds the pinned card with `title`, and
/// what its two references read there is `blocker` and `related` (K3: the
/// same identity strings as the identified read).
fn default_reads_hold(
    replicas: &[Rep],
    title: &str,
    blocker: &BTreeSet<String>,
    related: &BTreeSet<String>,
    label: &str,
) {
    for (index, replica) in replicas.iter().enumerate() {
        let plain = default_read(replica);
        let pinned = plain
            .get("pinned")
            .filter(|card| !card.is_null())
            .unwrap_or_else(|| {
                panic!("{label}: replica {index}'s default read serves no pinned card: {plain}")
            });
        assert_eq!(
            pinned.get("title").and_then(Value::as_str).unwrap_or(""),
            title,
            "{label}: replica {index}'s default read, the pinned card's title"
        );
        assert_eq!(
            &reference_values(pinned.get("blocker"), "blocker"),
            blocker,
            "{label} (K3): replica {index}'s default read of pinned.blocker"
        );
        assert_eq!(
            &reference_values(pinned.get("related"), "related"),
            related,
            "{label} (K3): replica {index}'s default read of pinned.related"
        );
    }
}

/// P1-1, the links the removal had seen. The pinned card links `blocker -> y`
/// and `related -> y, z`, delivered to all; then `b` unsets `Board.pinned`,
/// delivered to all. The text keeps the card, its title `""`; the unset had
/// seen the card's creation and the three links, so none is read.
#[test]
fn si07_p1_1_links_the_unset_had_seen_are_not_read_from_the_pinned_card_a_text_keeps() {
    let board = Board::new();
    let (mut replicas, [x, y, z], _) = board_base(&board);
    let ops = vec![
        board.link(At::Pinned, board.blocker, &y),
        board.link(At::Pinned, board.related, &y),
        board.link(At::Pinned, board.related, &z),
    ];
    for op in ops {
        let message = send_wire(&mut replicas[0], op, "a links from the pinned card");
        deliver_all(&mut replicas, 0, &message);
    }
    let doc = agreed(&replicas, "P1-1, before the unset");
    assert_eq!(
        (
            refs(&doc, At::Pinned, "blocker"),
            refs(&doc, At::Pinned, "related")
        ),
        (set(&[&y]), set(&[&y, &z])),
        "P1-1 premise: the three links are read before the unset"
    );
    let unset = send(
        &mut replicas[1],
        unset_pinned(&board),
        "b unsets Board.pinned",
    );
    deliver_all(&mut replicas, 1, &unset);
    let doc = agreed(&replicas, "P1-1, after the unset");
    assert_eq!(
        title_at(&doc, At::Pinned),
        "",
        "P1-1: the text keeps the pinned card, its title emptied (ST.04)"
    );
    assert!(
        has_card(&doc, At::Pinned),
        "P1-1: the pinned card is served"
    );
    assert_eq!(
        refs(&doc, At::Pinned, "blocker"),
        set(&[]),
        "P1-1: no blocker the unset had seen"
    );
    assert_eq!(
        refs(&doc, At::Pinned, "related"),
        set(&[]),
        "P1-1: no related the unset had seen"
    );
    let mut expected = set(&[&x, &y, &z]);
    expected.insert(root_id(&doc));
    expected.insert(id_at(&doc, At::Pinned));
    assert_eq!(
        served(&doc),
        expected,
        "P1-1: the board, x, y, z and the pinned card are served"
    );
    default_reads_hold(&replicas, "", &set(&[]), &set(&[]), "P1-1");
}

/// P1-2, the gap of SI.05's ruling R3.2. `b` unsets `Board.pinned`; `c`,
/// concurrently, links the pinned card's `blocker` to `y` and its `related`
/// to `z`. The unset had seen the card's creation and its title; no write it
/// had not seen reaches the card (a link writes nothing into it), so the
/// generated graph holds no vertex for the card once the unset is delivered,
/// and neither link is read, on any replica in any order, while the text
/// keeps the card served with its title `""`. Then `a` types `r` into the
/// card's title, delivered to all: a write the unset had not seen reaches the
/// card, the generated graph holds its vertex again, and the two links, which
/// no removal had seen, are read again.
#[test]
fn si07_p1_2_links_concurrent_with_the_unset_are_read_only_while_a_write_keeps_the_pinned_card() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], _) = board_base(&board);
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let blocker = send_wire(
            &mut replicas[2],
            board.link(At::Pinned, board.blocker, &y),
            "c links pinned.blocker -> y",
        );
        let related = send_wire(
            &mut replicas[2],
            board.link(At::Pinned, board.related, &z),
            "c links pinned.related -> z",
        );
        let on_c = identified(&replicas[2]);
        assert_eq!(
            (
                refs(&on_c, At::Pinned, "blocker"),
                refs(&on_c, At::Pinned, "related")
            ),
            (set(&[&y]), set(&[&z])),
            "P1-2 premise: c reads its own links before it delivers the unset"
        );
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], vec![blocker.clone(), related.clone()]],
                vec![vec![blocker, related]],
                vec![vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z)]),
        }
    };
    let revive = |replicas: &mut [Rep; 3]| {
        let board = Board::new();
        for op in board.title_ops(At::Pinned, "r") {
            let message = send(
                &mut replicas[0],
                op,
                "a types `r` into the pinned card's title",
            );
            deliver_all(replicas, 0, &message);
        }
    };
    let runs = every_order(
        "P1-2",
        schedule,
        |doc, run| {
            assert!(
                has_card(doc, At::Pinned),
                "P1-2: the text keeps the pinned card served"
            );
            assert_eq!(
                title_at(doc, At::Pinned),
                "",
                "P1-2: the pinned card's title is emptied"
            );
            assert_eq!(
                refs(doc, At::Pinned, "blocker"),
                set(&[]),
                "P1-2 (parity): pinned.blocker, linked concurrently with the unset, is not read while no \
                 write the unset had not seen reaches the card; the generated graph holds no vertex for it"
            );
            assert_eq!(
                refs(doc, At::Pinned, "related"),
                set(&[]),
                "P1-2 (parity): pinned.related, the same"
            );
            assert_eq!(
                served(doc),
                board_served(doc, run),
                "P1-2: the board, x, y, z and the pinned card are served"
            );
            assert_eq!(
                board_card_ids(doc),
                vec![run.id("x"), run.id("y"), run.id("z")]
            );
        },
        Some(&revive),
        |doc, run| {
            assert_eq!(
                title_at(doc, At::Pinned),
                "r",
                "P1-2, revived: the pinned card holds `r`"
            );
            assert_eq!(
                refs(doc, At::Pinned, "blocker"),
                set(&[run.id("y")]),
                "P1-2, revived: a write the unset had not seen keeps the card, and the link no removal \
                 had seen is read again"
            );
            assert_eq!(
                refs(doc, At::Pinned, "related"),
                set(&[run.id("z")]),
                "P1-2, revived: pinned.related, the same"
            );
            assert_eq!(served(doc), board_served(doc, run));
        },
    );
    assert_eq!(runs, 3, "a: 3 interleavings, b: 1 order, c: 1 order");
}

/// P1-2 in the default read: the same hidden links, on every replica, after
/// the unset and the links are delivered everywhere (one order suffices, the
/// identified read of every order being held equal above).
#[test]
fn si07_p1_2_the_default_read_hides_the_same_links() {
    let board = Board::new();
    let (mut replicas, [_, y, z], _) = board_base(&board);
    let unset = send(
        &mut replicas[1],
        unset_pinned(&board),
        "b unsets Board.pinned",
    );
    let blocker = send_wire(
        &mut replicas[2],
        board.link(At::Pinned, board.blocker, &y),
        "c links pinned.blocker -> y",
    );
    let related = send_wire(
        &mut replicas[2],
        board.link(At::Pinned, board.related, &z),
        "c links pinned.related -> z",
    );
    deliver_all(&mut replicas, 1, &unset);
    deliver_all(&mut replicas, 2, &blocker);
    deliver_all(&mut replicas, 2, &related);
    agreed(&replicas, "P1-2, default read");
    default_reads_hold(&replicas, "", &set(&[]), &set(&[]), "P1-2, default read");
}

/// P1-3, the other side: `b` unsets `Board.pinned`; `c`, concurrently, links
/// the pinned card's `blocker` to `y` and `related` to `z` and then types `q`
/// into its title. The title is a write the unset had not seen, so the card
/// keeps its vertex in the generated graph and both links are read, on every
/// replica in every order.
#[test]
fn si07_p1_3_a_write_concurrent_with_the_unset_keeps_the_links_from_the_pinned_card_read() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], _) = board_base(&board);
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let mut from_c = vec![
            send_wire(
                &mut replicas[2],
                board.link(At::Pinned, board.blocker, &y),
                "c links pinned.blocker -> y",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Pinned, board.related, &z),
                "c links pinned.related -> z",
            ),
        ];
        from_c.push(send(
            &mut replicas[2],
            board.rename(At::Pinned, 'q'),
            "c types `q` into the pinned title",
        ));
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], from_c.clone()],
                vec![from_c],
                vec![vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z)]),
        }
    };
    let runs = every_order(
        "P1-3",
        schedule,
        |doc, run| {
            assert_eq!(
                title_at(doc, At::Pinned),
                "q",
                "P1-3: the card keeps `q`, typed concurrently with the unset"
            );
            assert_eq!(
                refs(doc, At::Pinned, "blocker"),
                set(&[run.id("y")]),
                "P1-3: pinned.blocker is read, a write the unset had not seen keeping the card"
            );
            assert_eq!(
                refs(doc, At::Pinned, "related"),
                set(&[run.id("z")]),
                "P1-3: pinned.related too"
            );
            assert_eq!(served(doc), board_served(doc, run));
        },
        None,
        |_, _| {},
    );
    assert_eq!(runs, 4, "a: 4 interleavings, b: 1 order, c: 1 order");
}

// ---------------------------------------------------------------------------
// The keyed board: a keyed source, `Board.slots`
// ---------------------------------------------------------------------------

/// The keyed board of `fixtures/si05/si05keyed.ecore` (SI.05 round 3):
/// `Board.slots` is a keyed collection of `Card`s, whose `title` is an
/// optional text (`iD`) and `rank` an optional counter.
const KEYED: &str = include_str!("fixtures/si05/si05keyed.metamodel.json");

fn keyed_descriptor() -> Value {
    serde_json::from_str(KEYED).expect("the keyed fixture is JSON")
}

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

    fn on_entry(&self, key: &str, inner: InstanceOp) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.slots,
            InstanceOp::entry(Scalar::text(key), InstanceOp::variant(self.card, inner)),
        ))
    }

    fn on_listed(&self, pos: usize, inner: InstanceOp) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.cards,
            InstanceOp::at(pos, InstanceOp::variant(self.card, inner)),
        ))
    }

    fn type_char(&self, key: &str, pos: usize, ch: char) -> ModelOp {
        self.on_entry(
            key,
            InstanceOp::field(
                self.title,
                InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })),
            ),
        )
    }

    fn rank_up(&self, key: &str) -> ModelOp {
        self.on_entry(
            key,
            InstanceOp::field(
                self.rank,
                InstanceOp::set(InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1)))),
            ),
        )
    }

    fn link(&self, key: &str, feature: FeatureSlot, id: &str) -> ModelOp {
        self.on_entry(
            key,
            InstanceOp::field(feature, InstanceOp::Leaf(LeafOp::Link(id.to_string()))),
        )
    }

    fn remove(&self, key: &str) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.slots,
            InstanceOp::remove(Scalar::text(key)),
        ))
    }

    fn clear(&self) -> ModelOp {
        self.on_board(InstanceOp::field(self.slots, InstanceOp::clear()))
    }

    /// The root, `x`, `y`, `z` titled at `Board.cards[0..3]`, and the card at
    /// `k` titled `p`.
    fn base_ops(&self) -> Vec<ModelOp> {
        let mut ops = vec![self.on_board(InstanceOp::New)];
        for (pos, ch) in [(0, 'x'), (1, 'y'), (2, 'z')] {
            ops.push(self.on_board(InstanceOp::field(
                self.cards,
                InstanceOp::insert(pos, InstanceOp::variant(self.card, InstanceOp::New)),
            )));
            ops.push(self.on_listed(
                pos,
                InstanceOp::field(
                    self.title,
                    InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch })),
                ),
            ));
        }
        ops.push(self.type_char("k", 0, 'p'));
        ops
    }
}

/// Three replicas of the keyed board with its base built on replica 0 and
/// delivered; answers them with the identities of `x`, `y`, `z`.
fn keyed_base(keyed: &Keyed) -> ([Rep; 3], [String; 3]) {
    let mut replicas = three(&keyed_descriptor());
    broadcast(
        &mut replicas,
        0,
        keyed.base_ops(),
        "building the keyed board",
    );
    let doc = agreed(&replicas, "the keyed board");
    assert_eq!(
        doc["slots"]["k"]["title"],
        json!("p"),
        "premise: the card at k holds `p`"
    );
    let ids = [0, 1, 2].map(|i| {
        doc["cards"][i][ID]
            .as_str()
            .unwrap_or_else(|| panic!("no card at Board.cards[{i}] in {doc}"))
            .to_string()
    });
    (replicas, ids)
}

/// The card at key `k` of a keyed document.
fn at_k(doc: &Value) -> &Value {
    doc.get("slots")
        .and_then(|slots| slots.get("k"))
        .unwrap_or_else(|| panic!("no card at key k in {doc}"))
}

/// P1-4, a key removal and the links it had seen: `k` links `blocker -> y`
/// and `related -> y, z`, delivered; `b` removes `k`, delivered. The emptied
/// title keeps the entry (`""`, which is not a fresh card's absent title on
/// either path); no link is read.
#[test]
fn si07_p1_4_links_a_key_removal_had_seen_are_not_read_from_the_entry_a_text_keeps() {
    let keyed = Keyed::new();
    let (mut replicas, [_, y, z]) = keyed_base(&keyed);
    for op in [
        keyed.link("k", keyed.blocker, &y),
        keyed.link("k", keyed.related, &y),
        keyed.link("k", keyed.related, &z),
    ] {
        let message = send(&mut replicas[0], op, "a links from the card at k");
        deliver_all(&mut replicas, 0, &message);
    }
    let doc = agreed(&replicas, "P1-4, before the removal");
    assert_eq!(
        (
            reference_values(at_k(&doc).get("blocker"), "blocker"),
            reference_values(at_k(&doc).get("related"), "related")
        ),
        (set(&[&y]), set(&[&y, &z])),
        "P1-4 premise: the links are read before the removal"
    );
    let removal = send(&mut replicas[1], keyed.remove("k"), "b removes k");
    deliver_all(&mut replicas, 1, &removal);
    let doc = agreed(&replicas, "P1-4, after the removal");
    let card = at_k(&doc);
    assert_eq!(
        card.get("title"),
        Some(&json!("")),
        "P1-4: the emptied title keeps the entry"
    );
    assert_eq!(
        reference_values(card.get("blocker"), "blocker"),
        set(&[]),
        "P1-4: no blocker the removal had seen"
    );
    assert_eq!(
        reference_values(card.get("related"), "related"),
        set(&[]),
        "P1-4: no related the removal had seen"
    );
}

/// P1-5, the gap at a key: `b` removes `k` (or clears the collection); `c`,
/// concurrently, links the card's `blocker` to `y` and `related` to `z`. No
/// write the removal had not seen reaches the card, so neither link is read
/// while the emptied title keeps the entry served. Then `a` raises the
/// card's rank, delivered to all, and both links are read again.
fn p1_5(clear: bool) {
    let keyed = Keyed::new();
    let label = if clear {
        "P1-5 (Clear)"
    } else {
        "P1-5 (Remove)"
    };
    let schedule = || {
        let (mut replicas, [x, y, z]) = keyed_base(&keyed);
        let removal = send(
            &mut replicas[1],
            if clear {
                keyed.clear()
            } else {
                keyed.remove("k")
            },
            "b takes k away",
        );
        let blocker = send(
            &mut replicas[2],
            keyed.link("k", keyed.blocker, &y),
            "c links k.blocker -> y",
        );
        let related = send(
            &mut replicas[2],
            keyed.link("k", keyed.related, &z),
            "c links k.related -> z",
        );
        Run {
            replicas,
            incoming: [
                vec![
                    vec![removal.clone()],
                    vec![blocker.clone(), related.clone()],
                ],
                vec![vec![blocker, related]],
                vec![vec![removal]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z)]),
        }
    };
    let revive = |replicas: &mut [Rep; 3]| {
        let keyed = Keyed::new();
        let message = send(
            &mut replicas[0],
            keyed.rank_up("k"),
            "a raises the rank of the card at k",
        );
        deliver_all(replicas, 0, &message);
    };
    let runs = every_order(
        label,
        schedule,
        |doc, _| {
            let card = at_k(doc);
            assert_eq!(
                card.get("title"),
                Some(&json!("")),
                "{label}: the emptied title keeps the entry"
            );
            assert_eq!(
                reference_values(card.get("blocker"), "blocker"),
                set(&[]),
                "{label} (parity): k.blocker, linked concurrently with the removal, is not read while no \
                 write the removal had not seen reaches the card"
            );
            assert_eq!(
                reference_values(card.get("related"), "related"),
                set(&[]),
                "{label} (parity): k.related, the same"
            );
        },
        Some(&revive),
        |doc, run| {
            let card = at_k(doc);
            assert_eq!(
                card.get("rank"),
                Some(&json!(1)),
                "{label}, revived: the card holds rank 1"
            );
            assert_eq!(
                reference_values(card.get("blocker"), "blocker"),
                set(&[run.id("y")]),
                "{label}, revived: the link no removal had seen is read again"
            );
            assert_eq!(
                reference_values(card.get("related"), "related"),
                set(&[run.id("z")]),
                "{label}, revived: k.related, the same"
            );
        },
    );
    assert_eq!(runs, 3, "a: 3 interleavings, b: 1 order, c: 1 order");
}

#[test]
fn si07_p1_5_links_concurrent_with_a_key_removal_are_read_only_while_a_write_keeps_the_entry() {
    p1_5(false);
}

#[test]
fn si07_p1_5_links_concurrent_with_a_clear_are_read_only_while_a_write_keeps_the_entry() {
    p1_5(true);
}

/// P1-6, the other side at a key: `b` removes `k`; `c`, concurrently, links
/// `k.blocker` to `y` and `k.related` to `z` and then raises the card's rank,
/// a write the removal had not seen: the links are read in every order.
#[test]
fn si07_p1_6_a_write_concurrent_with_the_key_removal_keeps_the_links_read() {
    let keyed = Keyed::new();
    let schedule = || {
        let (mut replicas, [x, y, z]) = keyed_base(&keyed);
        let removal = send(&mut replicas[1], keyed.remove("k"), "b removes k");
        let from_c = vec![
            send(
                &mut replicas[2],
                keyed.link("k", keyed.blocker, &y),
                "c links k.blocker -> y",
            ),
            send(
                &mut replicas[2],
                keyed.link("k", keyed.related, &z),
                "c links k.related -> z",
            ),
            send(
                &mut replicas[2],
                keyed.rank_up("k"),
                "c raises the rank of the card at k",
            ),
        ];
        Run {
            replicas,
            incoming: [
                vec![vec![removal.clone()], from_c.clone()],
                vec![from_c],
                vec![vec![removal]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z)]),
        }
    };
    let runs = every_order(
        "P1-6",
        schedule,
        |doc, run| {
            let card = at_k(doc);
            assert_eq!(
                card.get("rank"),
                Some(&json!(1)),
                "P1-6: the rank raised concurrently stays"
            );
            assert_eq!(
                reference_values(card.get("blocker"), "blocker"),
                set(&[run.id("y")]),
                "P1-6: k.blocker is read, a write the removal had not seen keeping the card"
            );
            assert_eq!(
                reference_values(card.get("related"), "related"),
                set(&[run.id("z")]),
                "P1-6: k.related too"
            );
        },
        None,
        |_, _| {},
    );
    assert_eq!(runs, 4, "a: 4 interleavings, b: 1 order, c: 1 order");
}
