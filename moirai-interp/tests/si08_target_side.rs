//! SI.08, the target side of SI.07's P1 on the interpreted log: a link TO an
//! object a removal reached, when a text keeps that object served.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.08.md`. Under a descriptor
//! with the identity rule, when a removal (an `Unset` of an optional, a
//! `Remove` of a key, a `Clear` of a keyed collection) reaches an object whose
//! creation it had seen, and the object stays served only because a text
//! attribute keeps it (ST.04: its title reads `""`), a link to that object
//! written concurrently with the removal is read exactly when the generated
//! runtime's typed reference graph reads the arc: not while no write the
//! removal had not seen has reached the object, and read while, or once, such
//! a write has reached it. The links to it that the removal had seen stay
//! hidden (SI.05, R2). The ground truth is the generated runtime, computed by
//! hand from its rules and held to the generated crates of the same two
//! metamodels by Arachne's `generated/simpleuml_crdt/tests/si08_target_twin_*.rs`:
//!
//! - every create or update of a record adds the record's vertex; a
//!   `DeleteSubtree` over the optional or the key takes away the vertex
//!   additions it had seen and the arcs it had seen whose either end lies
//!   below it;
//! - a link adds an arc and no vertex;
//! - an arc is a candidate only while both its vertices are in the graph, and
//!   a single-valued reference keeps the greatest candidate by `FairPolicy`.
//!
//! What is out of the claim (ruling R1 of the spec, observation OB2 of
//! SI.07): a later write whose writer had delivered the removal revives the
//! object under a new served identity (SI.01), so a link written before it
//! names an identity nothing serves; the generated graph, which names the
//! object by its place, reads that arc again. No test here reads a link after
//! such a revival.
//!
//! Each concurrent schedule runs on three real `ModelLog` replicas in every
//! delivery order causal broadcast allows at every replica, and every run
//! must end with the three replicas serving one identified document and one
//! default document, the same in every run, and the number of runs is
//! asserted.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si08_target_side
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

/// Every replica serves the same identified document and the same default
/// document; answers the identified one.
fn agreed(replicas: &[Rep], label: &str) -> Value {
    let first = identified(&replicas[0]);
    let first_plain = default_read(&replicas[0]);
    for (index, replica) in replicas.iter().enumerate().skip(1) {
        assert_eq!(
            identified(replica),
            first,
            "{label}: replica {index} serves a different identified document from replica 0"
        );
        assert_eq!(
            default_read(replica),
            first_plain,
            "{label}: replica {index} serves a different default document from replica 0"
        );
    }
    first
}

/// Every identity a document serves.
fn served(doc: &Value) -> BTreeSet<String> {
    identities(doc).into_iter().map(|(id, _)| id).collect()
}

/// The board's own served identity.
fn root_id(doc: &Value) -> String {
    doc[ID]
        .as_str()
        .unwrap_or_else(|| panic!("the board carries no identity: {doc}"))
        .to_string()
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

/// Run `schedule` in every delivery order: at each replica every
/// interleaving of its incoming streams, in every combination with the
/// interleavings at the other two. `check` reads the agreed identified
/// document of each run and the replicas, and every run must end in the same
/// document as the first. Answers how many runs there were.
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

/// The links of the card at `Board.cards[pos]` in a document: what its
/// `blocker` and its `related` read (K3).
fn links_of(doc: &Value, pos: usize) -> (BTreeSet<String>, BTreeSet<String>) {
    (
        refs(doc, At::Board(pos), "blocker"),
        refs(doc, At::Board(pos), "related"),
    )
}

/// The default read of every replica: the card at `Board.cards[pos]` reads
/// `blocker` and `related` there as the identified read does (K3, the same
/// identity strings in both reads).
fn default_links_hold(
    replicas: &[Rep],
    pos: usize,
    blocker: &BTreeSet<String>,
    related: &BTreeSet<String>,
    label: &str,
) {
    for (index, replica) in replicas.iter().enumerate() {
        let plain = default_read(replica);
        let card = plain
            .get("cards")
            .and_then(|cards| cards.get(pos))
            .filter(|card| !card.is_null())
            .unwrap_or_else(|| {
                panic!(
                    "{label}: replica {index}'s default read holds no Board.cards[{pos}]: {plain}"
                )
            });
        assert_eq!(
            &reference_values(card.get("blocker"), "blocker"),
            blocker,
            "{label} (K3): replica {index}'s default read of Board.cards[{pos}].blocker"
        );
        assert_eq!(
            &reference_values(card.get("related"), "related"),
            related,
            "{label} (K3): replica {index}'s default read of Board.cards[{pos}].related"
        );
    }
}

// ---------------------------------------------------------------------------
// The board: a target in an optional, `Board.pinned`
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
/// delivered; answers the replicas, the identities of `x`, `y`, `z` and the
/// identity the pinned card is served under.
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

/// The pinned card is served, with `title`, under the identity it was
/// created with (`p`): a removal keeps it there, and a write whose writer saw
/// it alive does not move it (SI.01).
fn pinned_holds(doc: &Value, title: &str, p: &str, label: &str) {
    assert!(
        has_card(doc, At::Pinned),
        "{label}: the pinned card is served (the text keeps it, ST.04): {doc}"
    );
    assert_eq!(
        title_at(doc, At::Pinned),
        title,
        "{label}: the pinned card's title"
    );
    assert_eq!(
        id_at(doc, At::Pinned),
        p,
        "{label}: the pinned card is served under the identity the links name"
    );
}

/// The identities a board document must serve: the board, `x`, `y`, `z` and
/// the pinned card.
fn board_served(doc: &Value, run: &Run) -> BTreeSet<String> {
    let mut ids = set(&[run.id("x"), run.id("y"), run.id("z"), run.id("P")]);
    ids.insert(root_id(doc));
    ids
}

/// T1a, the links to the pinned card that the unset had seen. `a` links
/// `x.blocker -> P` and `y.related -> P`, delivered to all; then `b` unsets
/// `Board.pinned`, delivered to all. The text keeps the card (title `""`);
/// the unset had seen the card's creation and the two links, so neither is
/// read (SI.05, R2): the generated `DeleteSubtree` made both arcs redundant.
#[test]
fn si08_t1a_links_to_the_pinned_card_the_unset_had_seen_stay_hidden() {
    let board = Board::new();
    let (mut replicas, [x, y, z], p) = board_base(&board);
    for (at, feature, what) in [
        (At::Board(0), board.blocker, "a links x.blocker -> P"),
        (At::Board(1), board.related, "a links y.related -> P"),
    ] {
        let message = send_wire(&mut replicas[0], board.link(at, feature, &p), what);
        deliver_all(&mut replicas, 0, &message);
    }
    let doc = agreed(&replicas, "T1a, before the unset");
    assert_eq!(
        (
            refs(&doc, At::Board(0), "blocker"),
            refs(&doc, At::Board(1), "related")
        ),
        (set(&[&p]), set(&[&p])),
        "T1a premise: both links to the pinned card are read before the unset"
    );
    let unset = send(
        &mut replicas[1],
        unset_pinned(&board),
        "b unsets Board.pinned",
    );
    deliver_all(&mut replicas, 1, &unset);
    let doc = agreed(&replicas, "T1a, after the unset");
    pinned_holds(&doc, "", &p, "T1a");
    assert_eq!(
        refs(&doc, At::Board(0), "blocker"),
        set(&[]),
        "T1a: x.blocker -> P, which the unset had seen, is not read"
    );
    assert_eq!(
        refs(&doc, At::Board(1), "related"),
        set(&[]),
        "T1a: y.related -> P, which the unset had seen, is not read"
    );
    let mut expected = set(&[&x, &y, &z, &p]);
    expected.insert(root_id(&doc));
    assert_eq!(
        served(&doc),
        expected,
        "T1a: the board, x, y, z and the pinned card are served"
    );
    default_links_hold(&replicas, 0, &set(&[]), &set(&[]), "T1a");
    default_links_hold(&replicas, 1, &set(&[]), &set(&[]), "T1a");
}

/// T1b, the links the unset had seen, with a write the unset had not seen.
/// As T1a up to the links; then `b` unsets `Board.pinned` while `c`,
/// concurrently, types `q` into the card's title. The title keeps the card,
/// its vertex and its identity, but the unset had seen the two links, so the
/// generated graph dropped their arcs for good and neither is read, in every
/// order (SI.05's Q1 on an optional target).
#[test]
fn si08_t1b_links_the_unset_had_seen_stay_hidden_when_a_concurrent_write_keeps_the_card() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], p) = board_base(&board);
        for (at, feature, what) in [
            (At::Board(0), board.blocker, "a links x.blocker -> P"),
            (At::Board(1), board.related, "a links y.related -> P"),
        ] {
            let message = send_wire(&mut replicas[0], board.link(at, feature, &p), what);
            deliver_all(&mut replicas, 0, &message);
        }
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let title = send(
            &mut replicas[2],
            board.rename(At::Pinned, 'q'),
            "c types `q` into the pinned card's title",
        );
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], vec![title.clone()]],
                vec![vec![title]],
                vec![vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("P", p)]),
        }
    };
    let runs = every_order("T1b", schedule, |doc, run| {
        pinned_holds(doc, "q", run.id("P"), "T1b");
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[]),
            "T1b: x.blocker -> P, which the unset had seen, is not read although c's write keeps the card"
        );
        assert_eq!(
            refs(doc, At::Board(1), "related"),
            set(&[]),
            "T1b: y.related -> P, the same"
        );
        assert_eq!(served(doc), board_served(doc, run));
    });
    assert_eq!(runs, 2, "a: 2 interleavings, b: 1 order, c: 1 order");
}

/// T2, the gap (SI.07's OB1). `b` unsets `Board.pinned`; `c`, concurrently,
/// links `x.blocker -> P`, `x.related -> P` and `x.related -> z`. The unset
/// had seen the card's creation and its title, and no write it had not seen
/// reaches the card (a link writes nothing into its target), so the
/// generated graph holds no vertex for the card once the unset is delivered
/// and reads neither arc to it, on any replica in any order, while the text
/// keeps the card served with its title `""` under its first identity. The
/// link to `z` is read: nothing removed `z`.
#[test]
fn si08_t2_links_concurrent_with_the_unset_are_hidden_while_no_unseen_write_reaches_the_pinned_card()
 {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], p) = board_base(&board);
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let from_c = vec![
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.blocker, &p),
                "c links x.blocker -> P",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &p),
                "c links x.related -> P",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &z),
                "c links x.related -> z",
            ),
        ];
        let on_c = identified(&replicas[2]);
        assert_eq!(
            links_of(&on_c, 0),
            (set(&[&p]), set(&[&p, &z])),
            "T2 premise: c reads its own links before it delivers the unset"
        );
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], from_c.clone()],
                vec![from_c],
                vec![vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("P", p)]),
        }
    };
    let runs = every_order("T2", schedule, |doc, run| {
        pinned_holds(doc, "", run.id("P"), "T2");
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[]),
            "T2 (parity): x.blocker -> P, linked concurrently with the unset, is not read while no write \
             the unset had not seen reaches the pinned card; the generated graph holds no vertex for it"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[run.id("z")]),
            "T2 (parity): x.related reads z and not P"
        );
        assert_eq!(
            served(doc),
            board_served(doc, run),
            "T2: the board, x, y, z and the pinned card are served"
        );
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")]
        );
        default_links_hold(
            &run.replicas,
            0,
            &set(&[]),
            &set(&[run.id("z")]),
            "T2 (parity)",
        );
    });
    assert_eq!(runs, 4, "a: 4 interleavings, b: 1 order, c: 1 order");
}

/// T3, a hidden link does not win a single-valued reference. `b` unsets
/// `Board.pinned`; `a`, concurrently, links `x.blocker -> y`; `c`,
/// concurrently, links `x.related -> z` and then `x.blocker -> P`, so its
/// link to `P` carries the greatest Lamport time of the two `x.blocker`
/// links. The generated graph takes the arc to `P` out of the candidates (no
/// vertex) before it keeps the greatest, so `x.blocker` reads `y` on every
/// replica in every order.
#[test]
fn si08_t3_a_hidden_link_to_the_pinned_card_does_not_win_a_single_valued_reference() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], p) = board_base(&board);
        let from_a = send_wire(
            &mut replicas[0],
            board.link(At::Board(0), board.blocker, &y),
            "a links x.blocker -> y",
        );
        let unset = send(
            &mut replicas[1],
            unset_pinned(&board),
            "b unsets Board.pinned",
        );
        let from_c = vec![
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.related, &z),
                "c links x.related -> z",
            ),
            send_wire(
                &mut replicas[2],
                board.link(At::Board(0), board.blocker, &p),
                "c links x.blocker -> P",
            ),
        ];
        assert_eq!(
            refs(&identified(&replicas[2]), At::Board(0), "blocker"),
            set(&[&p]),
            "T3 premise: c reads x.blocker -> P before it delivers anything"
        );
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], from_c.clone()],
                vec![vec![from_a.clone()], from_c],
                vec![vec![from_a], vec![unset]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("P", p)]),
        }
    };
    let runs = every_order("T3", schedule, |doc, run| {
        pinned_holds(doc, "", run.id("P"), "T3");
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[run.id("y")]),
            "T3 (parity): x.blocker reads y: the link to P, greater by FairPolicy, has no target vertex \
             and is no candidate"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[run.id("z")]),
            "T3: x.related reads z"
        );
        assert_eq!(served(doc), board_served(doc, run));
        default_links_hold(
            &run.replicas,
            0,
            &set(&[run.id("y")]),
            &set(&[run.id("z")]),
            "T3 (parity)",
        );
    });
    assert_eq!(
        runs, 18,
        "a: 3 interleavings, b: 3 interleavings, c: 2 interleavings"
    );
}

/// The schedule of T4 and T4-once: `b` unsets `Board.pinned`; `c`,
/// concurrently, links `x.blocker -> P` and `x.related -> P`; `a`,
/// concurrently, types `q` into the pinned card's title. Answers the
/// replicas, the messages (a's title, b's unset, c's two links) and the
/// names.
#[allow(clippy::type_complexity)]
fn t4_sent(board: &Board) -> ([Rep; 3], Msg, Msg, [Msg; 2], BTreeMap<&'static str, String>) {
    let (mut replicas, [x, y, z], p) = board_base(board);
    let title = send(
        &mut replicas[0],
        board.rename(At::Pinned, 'q'),
        "a types `q` into the pinned card's title",
    );
    let unset = send(
        &mut replicas[1],
        unset_pinned(board),
        "b unsets Board.pinned",
    );
    let links = [
        send_wire(
            &mut replicas[2],
            board.link(At::Board(0), board.blocker, &p),
            "c links x.blocker -> P",
        ),
        send_wire(
            &mut replicas[2],
            board.link(At::Board(0), board.related, &p),
            "c links x.related -> P",
        ),
    ];
    (
        replicas,
        title,
        unset,
        links,
        BTreeMap::from([("x", x), ("y", y), ("z", z), ("P", p)]),
    )
}

/// T4, the other side: a write the unset had not seen reaches the card.
/// `a`'s title keeps the card's vertex in the generated graph (its
/// `AddVertex` is concurrent with the `DeleteSubtree`), and `a` saw the card
/// alive, so it keeps its identity (SI.01); both links to it are read, its
/// title is `q`, on every replica in every order.
#[test]
fn si08_t4_a_write_the_unset_had_not_seen_keeps_the_links_to_the_pinned_card_read() {
    let board = Board::new();
    let schedule = || {
        let (replicas, title, unset, links, named) = t4_sent(&board);
        let links = links.to_vec();
        Run {
            replicas,
            incoming: [
                vec![vec![unset.clone()], links.clone()],
                vec![vec![title.clone()], links],
                vec![vec![title], vec![unset]],
            ],
            named,
        }
    };
    let runs = every_order("T4", schedule, |doc, run| {
        pinned_holds(doc, "q", run.id("P"), "T4");
        assert_eq!(
            links_of(doc, 0),
            (set(&[run.id("P")]), set(&[run.id("P")])),
            "T4: x.blocker and x.related read P, kept by a write the unset had not seen"
        );
        assert_eq!(served(doc), board_served(doc, run));
        default_links_hold(
            &run.replicas,
            0,
            &set(&[run.id("P")]),
            &set(&[run.id("P")]),
            "T4",
        );
    });
    assert_eq!(
        runs, 18,
        "a: 3 interleavings, b: 3 interleavings, c: 2 interleavings"
    );
}

/// T4-once, the same schedule read at every step of one order per replica.
/// On `b`, which applied its unset first, `c`'s links arrive while no write
/// the unset had not seen has reached the card, and are not read; once `a`'s
/// title arrives they are read. On `a` and `c`, which hold such a write or no
/// unset, they are read at every step. The generated graph reads the same at
/// every step: on `b` the card has no vertex until `a`'s `AddVertex` arrives.
#[test]
fn si08_t4_once_links_to_the_pinned_card_are_read_once_an_unseen_write_reaches_it() {
    let board = Board::new();
    let (mut replicas, title, unset, [blocker, related], named) = t4_sent(&board);
    let p = named["P"].clone();
    let reads = |replica: &Rep| links_of(&identified(replica), 0);
    let none = (set(&[]), set(&[]));
    let both = (set(&[&p]), set(&[&p]));

    // b: its own unset, then c's links, then a's title.
    assert_eq!(
        reads(&replicas[1]),
        none,
        "T4-once premise: b reads no link"
    );
    replicas[1].receive(blocker.clone());
    replicas[1].receive(related.clone());
    let on_b = identified(&replicas[1]);
    pinned_holds(&on_b, "", &p, "T4-once, b after c's links");
    assert_eq!(
        links_of(&on_b, 0),
        none,
        "T4-once (parity): on b, c's links to P arrive after b's unset and before any write the unset \
         had not seen; they are not read"
    );
    replicas[1].receive(title.clone());
    let on_b = identified(&replicas[1]);
    pinned_holds(&on_b, "q", &p, "T4-once, b after a's title");
    assert_eq!(
        links_of(&on_b, 0),
        both,
        "T4-once: on b, once a's title (a write the unset had not seen) arrives, both links to P are read"
    );

    // a: its own title, then c's links, then b's unset.
    replicas[0].receive(blocker.clone());
    replicas[0].receive(related.clone());
    assert_eq!(
        reads(&replicas[0]),
        both,
        "T4-once: on a, before the unset, both links are read"
    );
    replicas[0].receive(unset.clone());
    let on_a = identified(&replicas[0]);
    pinned_holds(&on_a, "q", &p, "T4-once, a after b's unset");
    assert_eq!(
        links_of(&on_a, 0),
        both,
        "T4-once: on a, the unset arrives after a write it had not seen; both links stay read"
    );

    // c: its own links, then a's title, then b's unset.
    assert_eq!(reads(&replicas[2]), both, "T4-once: c reads its own links");
    replicas[2].receive(title);
    assert_eq!(reads(&replicas[2]), both, "T4-once: c after a's title");
    replicas[2].receive(unset);
    let on_c = identified(&replicas[2]);
    pinned_holds(&on_c, "q", &p, "T4-once, c after b's unset");
    assert_eq!(links_of(&on_c, 0), both, "T4-once: c after b's unset");

    let doc = agreed(&replicas, "T4-once, all delivered");
    assert_eq!(links_of(&doc, 0), both);
}

// ---------------------------------------------------------------------------
// The keyed board: a target at a key, `Board.slots`
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

    /// K2's `Link` at the reference `feature` of the card at
    /// `Board.cards[pos]`.
    fn link_listed(&self, pos: usize, feature: FeatureSlot, id: &str) -> ModelOp {
        self.on_listed(
            pos,
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

    /// `Remove` of `k`, or `Clear` of `Board.slots`.
    fn removal(&self, clear: bool) -> ModelOp {
        if clear {
            self.clear()
        } else {
            self.remove("k")
        }
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

fn removal_name(clear: bool) -> &'static str {
    if clear {
        "Clear of Board.slots"
    } else {
        "Remove of k"
    }
}

/// Three replicas of the keyed board with its base built on replica 0 and
/// delivered; answers them with the identities of `x`, `y`, `z` and of the
/// card at `k`.
fn keyed_base(keyed: &Keyed) -> ([Rep; 3], [String; 3], String) {
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
    let ids = [0, 1, 2].map(|i| id_at(&doc, At::Board(i)));
    let k = doc["slots"]["k"][ID]
        .as_str()
        .unwrap_or_else(|| panic!("the card at k carries no identity: {doc}"))
        .to_string();
    (replicas, ids, k)
}

/// The card at `k` is the only entry, served with `title` and `rank` (absent
/// for `None`) under the identity it was created with.
fn entry_holds(doc: &Value, title: &str, rank: Option<i64>, k: &str, label: &str) {
    let slots = doc
        .get("slots")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("{label}: no Board.slots in {doc}"));
    assert_eq!(
        slots.keys().cloned().collect::<Vec<_>>(),
        vec!["k".to_string()],
        "{label}: the entry at k is served (the text keeps it, ST.04), and no other: {doc}"
    );
    let card = &slots["k"];
    assert_eq!(
        card.get("title").and_then(Value::as_str),
        Some(title),
        "{label}: the title of the card at k"
    );
    assert_eq!(
        card.get("rank").and_then(Value::as_i64),
        rank,
        "{label}: the rank of the card at k"
    );
    assert_eq!(
        card.get(ID).and_then(Value::as_str),
        Some(k),
        "{label}: the card at k is served under the identity the links name"
    );
}

/// The identities a keyed document must serve: the board, `x`, `y`, `z` and
/// the card at `k`.
fn keyed_served(doc: &Value, run: &Run) -> BTreeSet<String> {
    let mut ids = set(&[run.id("x"), run.id("y"), run.id("z"), run.id("K")]);
    ids.insert(root_id(doc));
    ids
}

/// T5, the links to the card at `k` that the removal had seen. `a` links
/// `x.blocker -> K` and `y.related -> K`, delivered to all; then `b` removes
/// `k` (or clears `Board.slots`) while `c`, concurrently, ranks the card up.
/// The rank keeps the entry, its vertex and its identity; the removal had
/// seen the links, so neither is read, in every order, for both removals.
#[test]
fn si08_t5_links_to_a_keyed_entry_the_removal_had_seen_stay_hidden() {
    let keyed = Keyed::new();
    for clear in [false, true] {
        let label = format!("T5 ({})", removal_name(clear));
        let schedule = || {
            let (mut replicas, [x, y, z], k) = keyed_base(&keyed);
            for (pos, feature, what) in [
                (0, keyed.blocker, "a links x.blocker -> K"),
                (1, keyed.related, "a links y.related -> K"),
            ] {
                let message = send(&mut replicas[0], keyed.link_listed(pos, feature, &k), what);
                deliver_all(&mut replicas, 0, &message);
            }
            let doc = identified(&replicas[1]);
            assert_eq!(
                (
                    refs(&doc, At::Board(0), "blocker"),
                    refs(&doc, At::Board(1), "related")
                ),
                (set(&[&k]), set(&[&k])),
                "T5 premise: both links to the card at k are read before the removal"
            );
            let removal = send(&mut replicas[1], keyed.removal(clear), removal_name(clear));
            let rank = send(
                &mut replicas[2],
                keyed.rank_up("k"),
                "c ranks the card at k up",
            );
            Run {
                replicas,
                incoming: [
                    vec![vec![removal.clone()], vec![rank.clone()]],
                    vec![vec![rank]],
                    vec![vec![removal]],
                ],
                named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("K", k)]),
            }
        };
        let runs = every_order(&label, schedule, |doc, run| {
            entry_holds(doc, "", Some(1), run.id("K"), &label);
            assert_eq!(
                refs(doc, At::Board(0), "blocker"),
                set(&[]),
                "{label}: x.blocker -> K, which the removal had seen, is not read although c's rank keeps \
                 the entry"
            );
            assert_eq!(
                refs(doc, At::Board(1), "related"),
                set(&[]),
                "{label}: y.related -> K, the same"
            );
            assert_eq!(served(doc), keyed_served(doc, run));
        });
        assert_eq!(runs, 2, "{label}: a: 2 interleavings, b: 1, c: 1");
    }
}

/// T6, the gap at a key. `b` removes `k` (or clears `Board.slots`); `c`,
/// concurrently, links `x.blocker -> K`, `x.related -> K` and
/// `x.related -> z`. No write the removal had not seen reaches the card, so
/// the generated graph holds no vertex for it and reads neither arc to it,
/// while the emptied title keeps the entry served (`""` is not a fresh
/// card's absent title) under its first identity. The link to `z` is read.
fn t6(clear: bool) {
    let keyed = Keyed::new();
    let label = format!("T6 ({})", removal_name(clear));
    let schedule = || {
        let (mut replicas, [x, y, z], k) = keyed_base(&keyed);
        let removal = send(&mut replicas[1], keyed.removal(clear), removal_name(clear));
        let from_c = vec![
            send(
                &mut replicas[2],
                keyed.link_listed(0, keyed.blocker, &k),
                "c links x.blocker -> K",
            ),
            send(
                &mut replicas[2],
                keyed.link_listed(0, keyed.related, &k),
                "c links x.related -> K",
            ),
            send(
                &mut replicas[2],
                keyed.link_listed(0, keyed.related, &z),
                "c links x.related -> z",
            ),
        ];
        assert_eq!(
            links_of(&identified(&replicas[2]), 0),
            (set(&[&k]), set(&[&k, &z])),
            "{label} premise: c reads its own links before it delivers the removal"
        );
        Run {
            replicas,
            incoming: [
                vec![vec![removal.clone()], from_c.clone()],
                vec![from_c],
                vec![vec![removal]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("K", k)]),
        }
    };
    let runs = every_order(&label, schedule, |doc, run| {
        entry_holds(doc, "", None, run.id("K"), &label);
        assert_eq!(
            refs(doc, At::Board(0), "blocker"),
            set(&[]),
            "{label} (parity): x.blocker -> K, linked concurrently with the removal, is not read while no \
             write the removal had not seen reaches the card at k; the generated graph holds no vertex for it"
        );
        assert_eq!(
            refs(doc, At::Board(0), "related"),
            set(&[run.id("z")]),
            "{label} (parity): x.related reads z and not K"
        );
        assert_eq!(served(doc), keyed_served(doc, run));
        default_links_hold(
            &run.replicas,
            0,
            &set(&[]),
            &set(&[run.id("z")]),
            &format!("{label} (parity)"),
        );
    });
    assert_eq!(runs, 4, "{label}: a: 4 interleavings, b: 1, c: 1");
}

#[test]
fn si08_t6_links_concurrent_with_a_key_removal_are_hidden_while_no_unseen_write_reaches_the_entry()
{
    t6(false);
}

#[test]
fn si08_t6_links_concurrent_with_a_clear_are_hidden_while_no_unseen_write_reaches_the_entry() {
    t6(true);
}

/// T7, the other side at a key: `b` removes `k` (or clears `Board.slots`);
/// `c`, concurrently, links `x.blocker -> K` and `x.related -> K`; `a`,
/// concurrently, ranks the card up. The rank keeps the entry's vertex and its
/// identity; both links are read, on every replica in every order, for both
/// removals.
#[test]
fn si08_t7_a_write_the_removal_had_not_seen_keeps_the_links_to_the_entry_read() {
    let keyed = Keyed::new();
    for clear in [false, true] {
        let label = format!("T7 ({})", removal_name(clear));
        let schedule = || {
            let (mut replicas, [x, y, z], k) = keyed_base(&keyed);
            let rank = send(
                &mut replicas[0],
                keyed.rank_up("k"),
                "a ranks the card at k up",
            );
            let removal = send(&mut replicas[1], keyed.removal(clear), removal_name(clear));
            let links = vec![
                send(
                    &mut replicas[2],
                    keyed.link_listed(0, keyed.blocker, &k),
                    "c links x.blocker -> K",
                ),
                send(
                    &mut replicas[2],
                    keyed.link_listed(0, keyed.related, &k),
                    "c links x.related -> K",
                ),
            ];
            Run {
                replicas,
                incoming: [
                    vec![vec![removal.clone()], links.clone()],
                    vec![vec![rank.clone()], links],
                    vec![vec![rank], vec![removal]],
                ],
                named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("K", k)]),
            }
        };
        let runs = every_order(&label, schedule, |doc, run| {
            entry_holds(doc, "", Some(1), run.id("K"), &label);
            assert_eq!(
                links_of(doc, 0),
                (set(&[run.id("K")]), set(&[run.id("K")])),
                "{label}: x.blocker and x.related read K, kept by a write the removal had not seen"
            );
            assert_eq!(served(doc), keyed_served(doc, run));
            default_links_hold(
                &run.replicas,
                0,
                &set(&[run.id("K")]),
                &set(&[run.id("K")]),
                &label,
            );
        });
        assert_eq!(
            runs, 18,
            "{label}: a: 3 interleavings, b: 3 interleavings, c: 2 interleavings"
        );
    }
}

// ---------------------------------------------------------------------------
// A target in a sequence: the rule is not over-applied
// ---------------------------------------------------------------------------

/// T8, a sequence-held target kept by a concurrent write. `b` deletes `y`
/// from `Board.cards`; `c`, concurrently, links `x.blocker -> y` and
/// `x.related -> y`; `a`, concurrently, types `q` into `y`'s title. The
/// sequence keeps `y` (update-wins), the generated graph holds its vertex
/// again (`a`'s `AddVertex` is concurrent with the `DeleteSubtree`), `a` saw
/// `y` alive so its identity stays, and both links are read on every replica
/// in every order. A removal reached `y`; that alone hides nothing.
#[test]
fn si08_t8_a_sequence_held_target_kept_by_a_concurrent_write_keeps_its_links_read() {
    let board = Board::new();
    let schedule = || {
        let (mut replicas, [x, y, z], p) = board_base(&board);
        let title = send(
            &mut replicas[0],
            board.rename(At::Board(1), 'q'),
            "a types `q` into y's title",
        );
        let delete = send(
            &mut replicas[1],
            board.delete_card(1),
            "b deletes y from Board.cards",
        );
        let links = vec![
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
        Run {
            replicas,
            incoming: [
                vec![vec![delete.clone()], links.clone()],
                vec![vec![title.clone()], links],
                vec![vec![title], vec![delete]],
            ],
            named: BTreeMap::from([("x", x), ("y", y), ("z", z), ("P", p)]),
        }
    };
    let runs = every_order("T8", schedule, |doc, run| {
        assert_eq!(
            board_card_ids(doc),
            vec![run.id("x"), run.id("y"), run.id("z")],
            "T8: the sequence keeps y, under its identity, between x and z"
        );
        assert_eq!(
            title_at(doc, At::Board(1)),
            "q",
            "T8: y holds `q`, typed concurrently with its deletion"
        );
        assert_eq!(
            links_of(doc, 0),
            (set(&[run.id("y")]), set(&[run.id("y")])),
            "T8: x.blocker and x.related read y, kept by a write the deletion had not seen"
        );
        assert_eq!(served(doc), board_served(doc, run));
        default_links_hold(
            &run.replicas,
            0,
            &set(&[run.id("y")]),
            &set(&[run.id("y")]),
            "T8",
        );
    });
    assert_eq!(
        runs, 18,
        "a: 3 interleavings, b: 3 interleavings, c: 2 interleavings"
    );
}
