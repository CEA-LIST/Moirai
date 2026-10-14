//! SI.08 round 2, on the interpreted log: links to and from a card a removal
//! reached from above, where round 1's boards hold no card. The card sits at
//! a single containment whose class has no subclass (`Frame.head`, a
//! `record!` field and not a `union!`), in an optional (`Frame.extra`) or at a
//! key (`Frame.slots["k"]`), inside a `Frame` that sits in an optional
//! (`Board.pinned`) or at a key (`Board.frames["f"]`). The removal is an
//! `Unset` of `Board.pinned`, a `Remove` of `"f"` or a `Clear` of
//! `Board.frames`: it reaches the frame and, from above, the card.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.08.md`, section "Round 2,
//! written before any rerun". Round 1's implementation hides the links to any
//! object of the read-out that holds no write a removal has not seen
//! (`ObjectNode::is_present`), wherever it sits; these schedules hold that
//! wider rule to the generated runtime, computed by hand from its rules and
//! held to the generated crate of the same metamodel by Arachne's
//! `generated/simpleuml_crdt/tests/si08_r2_nested_twin.rs`:
//!
//! - every create or update of a record adds the record's vertex, named by
//!   its place, and a write adds the vertex of every record its path passes
//!   through and of none it does not (`record!`'s `update_typed` at each
//!   level; a record's default expansion adds no vertex for the records it
//!   nests); a link adds an arc and no vertex;
//! - a `DeleteSubtree` over the optional, the key or the map takes away the
//!   vertex additions it had seen below its prefix, the frame's cards
//!   included, and the arcs it had seen with either end below it;
//! - an arc is a candidate only while both its vertices are in the graph,
//!   and a single-valued reference keeps the greatest candidate by
//!   `FairPolicy`.
//!
//! Out of the claim, as ruling R1 of the spec: a later write whose writer had
//! delivered the removal (a revival). No test here sends one.
//!
//! Each concurrent schedule runs on three real `ModelLog` replicas in every
//! delivery order causal broadcast allows at every replica; every run must
//! end with the three replicas serving one identified document and one
//! default document, the same in every run, and the number of runs is
//! asserted.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si08_r2_nested_targets
//! ```

mod si05;

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{class_slot, feature_slot, table};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_protocol::replica::IsReplica;
use moirai_semantics::{ClassSlot, FeatureSlot};
use serde_json::Value;

use si05::{
    ID, Msg, Rep, broadcast, default_read, deliver_all, identified, identities, interleavings,
    reference_values, send, three,
};

/// The nested board of `fixtures/si08/si08nest.ecore`, described by Arachne
/// (`arachne describe`) and assembled a second time by hand from registered
/// fixtures, byte for byte the same (spec, round 2).
const NEST: &str = include_str!("fixtures/si08/si08nest.metamodel.json");

fn nest_descriptor() -> Value {
    serde_json::from_str(NEST).expect("the nested fixture is JSON")
}

// ---------------------------------------------------------------------------
// The metamodel and the edits
// ---------------------------------------------------------------------------

/// Where the frame sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Holder {
    /// `Board.pinned`, an optional containment.
    Pinned,
    /// `Board.frames["f"]`, a keyed collection.
    Keyed,
}

/// The removal that reaches the frame, and with it every card inside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Removal {
    Unset,
    Remove,
    Clear,
}

impl Removal {
    const ALL: [Removal; 3] = [Removal::Unset, Removal::Remove, Removal::Clear];

    fn holder(self) -> Holder {
        match self {
            Removal::Unset => Holder::Pinned,
            Removal::Remove | Removal::Clear => Holder::Keyed,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Removal::Unset => "Unset of Board.pinned",
            Removal::Remove => "Remove of Board.frames[f]",
            Removal::Clear => "Clear of Board.frames",
        }
    }
}

/// Where the card a schedule targets sits inside the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Place {
    /// `Frame.head`, a single containment of a class with no subclass.
    Head,
    /// `Frame.extra`, an optional containment.
    Extra,
    /// `Frame.slots["k"]`, a keyed collection.
    Slot,
}

impl Place {
    const ALL: [Place; 3] = [Place::Head, Place::Extra, Place::Slot];

    /// The name the schedule gives the card.
    fn label(self) -> &'static str {
        match self {
            Place::Head => "H",
            Place::Extra => "E",
            Place::Slot => "S",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Place::Head => "Frame.head",
            Place::Extra => "Frame.extra",
            Place::Slot => "Frame.slots[k]",
        }
    }

    /// The card's title in the base.
    fn letter(self) -> char {
        match self {
            Place::Head => 'h',
            Place::Extra => 'e',
            Place::Slot => 's',
        }
    }
}

/// Where a card sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum At {
    /// `Board.cards[i]`.
    Cards(usize),
    /// A card of the frame.
    Frame(Holder, Place),
}

struct Nest {
    board: ClassSlot,
    frame: ClassSlot,
    card: ClassSlot,
    cards: FeatureSlot,
    pinned: FeatureSlot,
    frames: FeatureSlot,
    label: FeatureSlot,
    head: FeatureSlot,
    extra: FeatureSlot,
    slots: FeatureSlot,
    title: FeatureSlot,
    blocker: FeatureSlot,
    related: FeatureSlot,
}

impl Nest {
    fn new() -> Nest {
        let sem = table(&nest_descriptor());
        let board = class_slot(&sem, "Board");
        let frame = class_slot(&sem, "Frame");
        let card = class_slot(&sem, "Card");
        Nest {
            board,
            frame,
            card,
            cards: feature_slot(&sem, board, "cards"),
            pinned: feature_slot(&sem, board, "pinned"),
            frames: feature_slot(&sem, board, "frames"),
            label: feature_slot(&sem, frame, "label"),
            head: feature_slot(&sem, frame, "head"),
            extra: feature_slot(&sem, frame, "extra"),
            slots: feature_slot(&sem, frame, "slots"),
            title: feature_slot(&sem, card, "title"),
            blocker: feature_slot(&sem, card, "blocker"),
            related: feature_slot(&sem, card, "related"),
        }
    }

    fn on_board(&self, inner: InstanceOp) -> ModelOp {
        ModelOp::Instance(InstanceOp::variant(self.board, inner))
    }

    /// `inner` addressed to the frame at `holder`.
    fn on_frame(&self, holder: Holder, inner: InstanceOp) -> ModelOp {
        let frame = InstanceOp::variant(self.frame, inner);
        self.on_board(match holder {
            Holder::Pinned => InstanceOp::field(self.pinned, InstanceOp::set(frame)),
            Holder::Keyed => {
                InstanceOp::field(self.frames, InstanceOp::entry(Scalar::text("f"), frame))
            }
        })
    }

    /// `inner` addressed to the card at `at`.
    fn on_card(&self, at: At, inner: InstanceOp) -> ModelOp {
        let card = InstanceOp::variant(self.card, inner);
        match at {
            At::Cards(i) => self.on_board(InstanceOp::field(self.cards, InstanceOp::at(i, card))),
            At::Frame(holder, place) => self.on_frame(
                holder,
                match place {
                    Place::Head => InstanceOp::field(self.head, card),
                    Place::Extra => InstanceOp::field(self.extra, InstanceOp::set(card)),
                    Place::Slot => {
                        InstanceOp::field(self.slots, InstanceOp::entry(Scalar::text("k"), card))
                    }
                },
            ),
        }
    }

    /// One character typed at `pos` into the title of the card at `at`.
    fn type_char(&self, at: At, pos: usize, ch: char) -> ModelOp {
        self.on_card(
            at,
            InstanceOp::field(
                self.title,
                InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })),
            ),
        )
    }

    /// One character typed at `pos` into the label of the frame at `holder`.
    fn type_label(&self, holder: Holder, pos: usize, ch: char) -> ModelOp {
        self.on_frame(
            holder,
            InstanceOp::field(
                self.label,
                InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })),
            ),
        )
    }

    /// K2's `Link` at `feature` (`blocker` or `related`) of the card at `at`.
    fn link(&self, at: At, feature: &str, id: &str) -> ModelOp {
        let slot = match feature {
            "blocker" => self.blocker,
            "related" => self.related,
            other => panic!("the card has no reference `{other}`"),
        };
        self.on_card(
            at,
            InstanceOp::field(slot, InstanceOp::Leaf(LeafOp::Link(id.to_string()))),
        )
    }

    fn removal(&self, removal: Removal) -> ModelOp {
        self.on_board(match removal {
            Removal::Unset => InstanceOp::field(self.pinned, InstanceOp::unset()),
            Removal::Remove => {
                InstanceOp::field(self.frames, InstanceOp::remove(Scalar::text("f")))
            }
            Removal::Clear => InstanceOp::field(self.frames, InstanceOp::clear()),
        })
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// Every identity a document serves.
fn served(doc: &Value) -> BTreeSet<String> {
    identities(doc).into_iter().map(|(id, _)| id).collect()
}

/// The frame at `holder` in a document, if it serves one.
fn frame_at(doc: &Value, holder: Holder) -> Option<&Value> {
    let found = match holder {
        Holder::Pinned => doc.get("pinned")?,
        Holder::Keyed => doc.get("frames")?.get("f")?,
    };
    (!found.is_null()).then_some(found)
}

/// The card at `at` in a document, if it serves one.
fn card_at(doc: &Value, at: At) -> Option<&Value> {
    let found = match at {
        At::Cards(i) => doc.get("cards")?.get(i)?,
        At::Frame(holder, place) => {
            let frame = frame_at(doc, holder)?;
            match place {
                Place::Head => frame.get("head")?,
                Place::Extra => frame.get("extra")?,
                Place::Slot => frame.get("slots")?.get("k")?,
            }
        }
    };
    (!found.is_null()).then_some(found)
}

/// A text as served: the string, or `None` when the key is absent.
fn text(value: Option<&Value>, what: &str) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => panic!("{what} reads as {other}, not a text"),
    }
}

/// The identity a served element carries.
fn id_of(value: &Value, what: &str) -> String {
    value
        .get(ID)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{what} carries no identity: {value}"))
        .to_string()
}

/// What `feature` of the card at `at` reads, as a set (K3).
fn refs_at(doc: &Value, at: At, feature: &str, what: &str) -> BTreeSet<String> {
    let card = card_at(doc, at).unwrap_or_else(|| panic!("{what}: no card at {at:?} in {doc}"));
    reference_values(card.get(feature), feature)
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

// ---------------------------------------------------------------------------
// The base and one run
// ---------------------------------------------------------------------------

/// The identities a schedule names: `x`, `y`, `z`, the frame `F`, and its
/// cards `H`, `E` and `S`.
type Named = BTreeMap<&'static str, String>;

/// The base: the root, `x`, `y`, `z` titled at `Board.cards[0..3]`, and a
/// frame at `holder` labelled `l` whose cards hold `h` (`Frame.head`), `e`
/// (`Frame.extra`) and `s` (`Frame.slots["k"]`). Built on replica 0 and
/// delivered; answers the replicas and the identities, read off the
/// identified read.
fn base(nest: &Nest, holder: Holder) -> ([Rep; 3], Named) {
    let mut replicas = three(&nest_descriptor());
    let mut ops = vec![nest.on_board(InstanceOp::New)];
    for (pos, ch) in [(0, 'x'), (1, 'y'), (2, 'z')] {
        ops.push(nest.on_board(InstanceOp::field(
            nest.cards,
            InstanceOp::insert(pos, InstanceOp::variant(nest.card, InstanceOp::New)),
        )));
        ops.push(nest.type_char(At::Cards(pos), 0, ch));
    }
    ops.push(nest.type_label(holder, 0, 'l'));
    for place in Place::ALL {
        ops.push(nest.type_char(At::Frame(holder, place), 0, place.letter()));
    }
    broadcast(&mut replicas, 0, ops, "building the nested board");
    let doc = agreed(&replicas, "the nested board");
    let frame = frame_at(&doc, holder)
        .unwrap_or_else(|| panic!("premise: the frame is served at {holder:?}: {doc}"));
    assert_eq!(
        text(frame.get("label"), "the frame's label"),
        Some("l".to_string()),
        "premise: the frame is labelled `l`"
    );
    let mut named = Named::new();
    for (pos, name) in ["x", "y", "z"].into_iter().enumerate() {
        let card = card_at(&doc, At::Cards(pos)).expect("premise: three cards");
        named.insert(name, id_of(card, name));
    }
    named.insert("F", id_of(frame, "the frame"));
    for place in Place::ALL {
        let card = card_at(&doc, At::Frame(holder, place))
            .unwrap_or_else(|| panic!("premise: a card at {}: {doc}", place.name()));
        assert_eq!(
            text(card.get("title"), place.name()),
            Some(place.letter().to_string()),
            "premise: the card at {} holds `{}`",
            place.name(),
            place.letter()
        );
        named.insert(place.label(), id_of(card, place.name()));
    }
    let mut root = BTreeSet::new();
    root.insert(id_of(&doc, "the board"));
    assert_eq!(
        served(&doc),
        named.values().cloned().chain(root).collect::<BTreeSet<_>>(),
        "premise: the board serves the root, x, y, z, the frame and its three cards"
    );
    (replicas, named)
}

/// One run of a concurrent schedule: the replicas once every writer has
/// sent, what each replica has still to deliver (as streams, each in the
/// order causal broadcast imposes on it, the streams concurrent with one
/// another), and the identities the checks name.
struct Run {
    replicas: [Rep; 3],
    incoming: [Vec<Vec<Msg>>; 3],
    named: Named,
}

/// Run `schedule` in every delivery order: at each replica every
/// interleaving of its incoming streams, in every combination with the
/// interleavings at the other two. `check` reads the agreed identified
/// document of each run and the run, and every run must end in the same
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

/// What one run must serve, by the names of the schedule.
struct Expect {
    /// The frame's label.
    label: &'static str,
    /// The titles of `H`, `E` and `S`.
    titles: [&'static str; 3],
    /// What `x.blocker` and `x.related` read.
    x_blocker: &'static [&'static str],
    x_related: &'static [&'static str],
    /// What the target card's `blocker` and `related` read.
    o_blocker: &'static [&'static str],
    o_related: &'static [&'static str],
}

fn names(named: &Named, which: &[&str]) -> BTreeSet<String> {
    which
        .iter()
        .map(|name| {
            named
                .get(name)
                .unwrap_or_else(|| panic!("the schedule names no `{name}`"))
                .clone()
        })
        .collect()
}

/// The containment of one run: the frame at `holder` with its label, and its
/// three cards with their titles, each under the identity the base gave it
/// (a removal moves no identity, nor does a write whose writer saw the
/// object alive: R4); and the served set, the base's.
fn holds_containment(doc: &Value, named: &Named, holder: Holder, expect: &Expect, label: &str) {
    let frame = frame_at(doc, holder).unwrap_or_else(|| {
        panic!("{label}: the frame is served (its texts keep it, ST.04): {doc}")
    });
    assert_eq!(
        text(frame.get("label"), "the frame's label"),
        Some(expect.label.to_string()),
        "{label}: the frame's label"
    );
    assert_eq!(
        id_of(frame, "the frame"),
        named["F"],
        "{label}: the frame's identity"
    );
    for (place, title) in Place::ALL.into_iter().zip(expect.titles) {
        let card = card_at(doc, At::Frame(holder, place)).unwrap_or_else(|| {
            panic!(
                "{label}: the card at {} is served (its text keeps it, ST.04): {doc}",
                place.name()
            )
        });
        assert_eq!(
            text(card.get("title"), place.name()),
            Some(title.to_string()),
            "{label}: the title of the card at {}",
            place.name()
        );
        assert_eq!(
            id_of(card, place.name()),
            named[place.label()],
            "{label}: the card at {} is served under the identity the links name",
            place.name()
        );
    }
    let mut expected: BTreeSet<String> = named.values().cloned().collect();
    expected.insert(id_of(doc, "the board"));
    assert_eq!(
        served(doc),
        expected,
        "{label}: the root, x, y, z, the frame and its three cards are served, under their identities"
    );
}

/// The links of one run, in the identified read: `x`'s two references, and
/// the target card's (the card at `place`), each against the generated
/// graph's answer.
fn holds_links(
    doc: &Value,
    named: &Named,
    holder: Holder,
    place: Place,
    expect: &Expect,
    label: &str,
) {
    let o = At::Frame(holder, place);
    let x = At::Cards(0);
    let target = place.label();
    assert_eq!(
        refs_at(doc, x, "blocker", label),
        names(named, expect.x_blocker),
        "{label} (parity, to {target}): x.blocker reads what the generated graph reads"
    );
    assert_eq!(
        refs_at(doc, x, "related", label),
        names(named, expect.x_related),
        "{label} (parity, to {target}): x.related reads what the generated graph reads"
    );
    assert_eq!(
        refs_at(doc, o, "blocker", label),
        names(named, expect.o_blocker),
        "{label} (parity, from {target}): {target}.blocker reads what the generated graph reads"
    );
    assert_eq!(
        refs_at(doc, o, "related", label),
        names(named, expect.o_related),
        "{label} (parity, from {target}): {target}.related reads what the generated graph reads"
    );
}

/// The default read of every replica reads the same links as the identified
/// read, as the same identity strings (K3).
fn default_links_hold(
    replicas: &[Rep],
    named: &Named,
    holder: Holder,
    place: Place,
    expect: &Expect,
    label: &str,
) {
    let o = At::Frame(holder, place);
    let x = At::Cards(0);
    for (index, replica) in replicas.iter().enumerate() {
        let plain = default_read(replica);
        let what = format!("{label} (K3, default read of replica {index})");
        assert_eq!(
            (
                refs_at(&plain, x, "blocker", &what),
                refs_at(&plain, x, "related", &what),
                refs_at(&plain, o, "blocker", &what),
                refs_at(&plain, o, "related", &what),
            ),
            (
                names(named, expect.x_blocker),
                names(named, expect.x_related),
                names(named, expect.o_blocker),
                names(named, expect.o_related),
            ),
            "{what}: x.blocker, x.related, {t}.blocker, {t}.related",
            t = place.label()
        );
    }
}

/// Everything one run must hold.
fn holds(doc: &Value, run: &Run, holder: Holder, place: Place, expect: &Expect, label: &str) {
    holds_containment(doc, &run.named, holder, expect, label);
    holds_links(doc, &run.named, holder, place, expect, label);
    default_links_hold(&run.replicas, &run.named, holder, place, expect, label);
}

/// The titles of `H`, `E`, `S` when the card at `place` holds `q` and the
/// others were emptied by the removal.
fn titles_with_q(place: Place) -> [&'static str; 3] {
    match place {
        Place::Head => ["q", "", ""],
        Place::Extra => ["", "q", ""],
        Place::Slot => ["", "", "q"],
    }
}

/// `c`'s four links, concurrent with the removal: to the target card
/// `x.blocker` and `x.related`, a link `x.related -> z` that nothing removes,
/// and from the target card `related -> y`.
fn links_to_and_from(
    nest: &Nest,
    replica: &mut Rep,
    named: &Named,
    at: At,
    what: &str,
) -> Vec<Msg> {
    let target = &named[match at {
        At::Frame(_, place) => place.label(),
        At::Cards(_) => panic!("the target is a card of the frame"),
    }];
    vec![
        send(
            replica,
            nest.link(At::Cards(0), "blocker", target),
            &format!("{what}: x.blocker -> target"),
        ),
        send(
            replica,
            nest.link(At::Cards(0), "related", target),
            &format!("{what}: x.related -> target"),
        ),
        send(
            replica,
            nest.link(At::Cards(0), "related", &named["z"]),
            &format!("{what}: x.related -> z"),
        ),
        send(
            replica,
            nest.link(at, "related", &named["y"]),
            &format!("{what}: target.related -> y"),
        ),
    ]
}

// ---------------------------------------------------------------------------
// N1, the gap: no write the removal had not seen reaches the card
// ---------------------------------------------------------------------------

/// N1. `b` removes the frame (each of the three removals); `c`,
/// concurrently, links `x.blocker` and `x.related` to the card at `place`,
/// `x.related` to `z`, and the card's `related` to `y`. No write the removal
/// had not seen reaches the card (a link writes nothing into its source or
/// its target), so once the removal is delivered the generated graph holds no
/// vertex for it and reads none of the three arcs that touch it, on any
/// replica in any order, while its text keeps it served with title `""`
/// under its first identity. `x.related -> z` is read.
fn n1(place: Place) {
    let nest = Nest::new();
    for removal in Removal::ALL {
        let holder = removal.holder();
        let label = format!("N1 {} ({})", place.label(), removal.name());
        let schedule = || {
            let (mut replicas, named) = base(&nest, holder);
            let remove = send(&mut replicas[1], nest.removal(removal), removal.name());
            let from_c = links_to_and_from(
                &nest,
                &mut replicas[2],
                &named,
                At::Frame(holder, place),
                "c",
            );
            let on_c = identified(&replicas[2]);
            assert_eq!(
                (
                    refs_at(&on_c, At::Cards(0), "related", &label),
                    refs_at(&on_c, At::Frame(holder, place), "related", &label),
                ),
                (names(&named, &[place.label(), "z"]), names(&named, &["y"]),),
                "{label} premise: c reads its own links before it delivers the removal"
            );
            Run {
                replicas,
                incoming: [
                    vec![vec![remove.clone()], from_c.clone()],
                    vec![from_c],
                    vec![vec![remove]],
                ],
                named,
            }
        };
        let expect = Expect {
            label: "",
            titles: ["", "", ""],
            x_blocker: &[],
            x_related: &["z"],
            o_blocker: &[],
            o_related: &[],
        };
        let runs = every_order(&label, schedule, |doc, run| {
            holds(doc, run, holder, place, &expect, &label);
        });
        assert_eq!(
            runs, 5,
            "{label}: a: 5 interleavings, b: 1 order, c: 1 order"
        );
    }
}

#[test]
fn si08_r2_n1_links_to_and_from_a_head_card_concurrent_with_a_removal_above_are_hidden() {
    n1(Place::Head);
}

#[test]
fn si08_r2_n1_links_to_and_from_an_extra_card_concurrent_with_a_removal_above_are_hidden() {
    n1(Place::Extra);
}

#[test]
fn si08_r2_n1_links_to_and_from_a_slot_card_concurrent_with_a_removal_above_are_hidden() {
    n1(Place::Slot);
}

// ---------------------------------------------------------------------------
// N2, the other side: a write the removal had not seen reaches the card
// ---------------------------------------------------------------------------

/// The schedule of N2 and N5: `b` removes the frame; `c`, concurrently, sends
/// N1's four links; `a`, concurrently, types `q` into the target card's
/// title. Answers the replicas, `a`'s title, `b`'s removal, `c`'s links and
/// the names.
#[allow(clippy::type_complexity)]
fn n2_sent(nest: &Nest, removal: Removal, place: Place) -> ([Rep; 3], Msg, Msg, Vec<Msg>, Named) {
    let holder = removal.holder();
    let (mut replicas, named) = base(nest, holder);
    let title = send(
        &mut replicas[0],
        nest.type_char(At::Frame(holder, place), 0, 'q'),
        "a types `q` into the target card's title",
    );
    let remove = send(&mut replicas[1], nest.removal(removal), removal.name());
    let links = links_to_and_from(
        nest,
        &mut replicas[2],
        &named,
        At::Frame(holder, place),
        "c",
    );
    (replicas, title, remove, links, named)
}

/// N2. As N1, and `a`, concurrently with both, types `q` into the card's
/// title. `a`'s write adds the card's vertex (its `AddVertex` is concurrent
/// with the `DeleteSubtree`), and `a` saw the card alive, so it keeps its
/// identity (SI.01): the three arcs that touch the card are read, and its
/// title is `q`, on every replica in every order.
fn n2(place: Place) {
    let nest = Nest::new();
    for removal in Removal::ALL {
        let holder = removal.holder();
        let label = format!("N2 {} ({})", place.label(), removal.name());
        let schedule = || {
            let (replicas, title, remove, links, named) = n2_sent(&nest, removal, place);
            Run {
                replicas,
                incoming: [
                    vec![vec![remove.clone()], links.clone()],
                    vec![vec![title.clone()], links],
                    vec![vec![title], vec![remove]],
                ],
                named,
            }
        };
        let target = place.label();
        let x_related: &'static [&'static str] = match place {
            Place::Head => &["H", "z"],
            Place::Extra => &["E", "z"],
            Place::Slot => &["S", "z"],
        };
        let x_blocker: &'static [&'static str] = match place {
            Place::Head => &["H"],
            Place::Extra => &["E"],
            Place::Slot => &["S"],
        };
        let expect = Expect {
            label: "",
            titles: titles_with_q(place),
            x_blocker,
            x_related,
            o_blocker: &[],
            o_related: &["y"],
        };
        let runs = every_order(&label, schedule, |doc, run| {
            holds(doc, run, holder, place, &expect, &label);
        });
        assert_eq!(
            runs, 50,
            "{label}: a: 5 interleavings, b: 5 interleavings, c: 2 interleavings ({target})"
        );
    }
}

#[test]
fn si08_r2_n2_a_write_the_removal_had_not_seen_keeps_the_links_of_a_head_card_read() {
    n2(Place::Head);
}

#[test]
fn si08_r2_n2_a_write_the_removal_had_not_seen_keeps_the_links_of_an_extra_card_read() {
    n2(Place::Extra);
}

#[test]
fn si08_r2_n2_a_write_the_removal_had_not_seen_keeps_the_links_of_a_slot_card_read() {
    n2(Place::Slot);
}

// ---------------------------------------------------------------------------
// N3, a write the removal had not seen that reaches the frame and not the card
// ---------------------------------------------------------------------------

/// N3. As N2, except that `a` types `q` into the frame's label, a write the
/// removal had not seen whose path passes through the frame and through no
/// card of it. It keeps the frame, but adds no vertex for the card (the
/// frame's `record!` updates its own place; its fields' records are not on
/// the write's path), so the three arcs that touch the card stay hidden, on
/// every replica in every order; the label reads `q`.
fn n3(place: Place) {
    let nest = Nest::new();
    for removal in Removal::ALL {
        let holder = removal.holder();
        let label = format!("N3 {} ({})", place.label(), removal.name());
        let schedule = || {
            let (mut replicas, named) = base(&nest, holder);
            let write = send(
                &mut replicas[0],
                nest.type_label(holder, 0, 'q'),
                "a types `q` into the frame's label",
            );
            let remove = send(&mut replicas[1], nest.removal(removal), removal.name());
            let links = links_to_and_from(
                &nest,
                &mut replicas[2],
                &named,
                At::Frame(holder, place),
                "c",
            );
            Run {
                replicas,
                incoming: [
                    vec![vec![remove.clone()], links.clone()],
                    vec![vec![write.clone()], links],
                    vec![vec![write], vec![remove]],
                ],
                named,
            }
        };
        let expect = Expect {
            label: "q",
            titles: ["", "", ""],
            x_blocker: &[],
            x_related: &["z"],
            o_blocker: &[],
            o_related: &[],
        };
        let runs = every_order(&label, schedule, |doc, run| {
            holds(doc, run, holder, place, &expect, &label);
        });
        assert_eq!(
            runs, 50,
            "{label}: a: 5 interleavings, b: 5 interleavings, c: 2 interleavings"
        );
    }
}

#[test]
fn si08_r2_n3_a_write_into_the_frame_alone_keeps_the_links_of_a_head_card_hidden() {
    n3(Place::Head);
}

#[test]
fn si08_r2_n3_a_write_into_the_frame_alone_keeps_the_links_of_an_extra_card_hidden() {
    n3(Place::Extra);
}

#[test]
fn si08_r2_n3_a_write_into_the_frame_alone_keeps_the_links_of_a_slot_card_hidden() {
    n3(Place::Slot);
}

// ---------------------------------------------------------------------------
// N4, the links the removal had seen
// ---------------------------------------------------------------------------

/// N4. `a` sends N1's four links, delivered to all; then `b` removes the
/// frame while `c`, concurrently, types `q` into the card's title. The title
/// adds the card's vertex again, but the removal had seen the three arcs
/// that touch the card (each has an end below its prefix) and made them
/// redundant for good, so none is read, in every order; `x.related -> z`,
/// whose ends no removal reached, stays read (SI.05, R2; SI.07 on the source
/// side).
fn n4(place: Place) {
    let nest = Nest::new();
    for removal in Removal::ALL {
        let holder = removal.holder();
        let label = format!("N4 {} ({})", place.label(), removal.name());
        let schedule = || {
            let (mut replicas, named) = base(&nest, holder);
            for message in links_to_and_from(
                &nest,
                &mut replicas[0],
                &named,
                At::Frame(holder, place),
                "a",
            ) {
                deliver_all(&mut replicas, 0, &message);
            }
            let before = identified(&replicas[1]);
            assert_eq!(
                (
                    refs_at(&before, At::Cards(0), "blocker", &label),
                    refs_at(&before, At::Cards(0), "related", &label),
                    refs_at(&before, At::Frame(holder, place), "related", &label),
                ),
                (
                    names(&named, &[place.label()]),
                    names(&named, &[place.label(), "z"]),
                    names(&named, &["y"]),
                ),
                "{label} premise: the four links are read before the removal"
            );
            let remove = send(&mut replicas[1], nest.removal(removal), removal.name());
            let title = send(
                &mut replicas[2],
                nest.type_char(At::Frame(holder, place), 0, 'q'),
                "c types `q` into the target card's title",
            );
            Run {
                replicas,
                incoming: [
                    vec![vec![remove.clone()], vec![title.clone()]],
                    vec![vec![title]],
                    vec![vec![remove]],
                ],
                named,
            }
        };
        let expect = Expect {
            label: "",
            titles: titles_with_q(place),
            x_blocker: &[],
            x_related: &["z"],
            o_blocker: &[],
            o_related: &[],
        };
        let runs = every_order(&label, schedule, |doc, run| {
            holds(doc, run, holder, place, &expect, &label);
        });
        assert_eq!(
            runs, 2,
            "{label}: a: 2 interleavings, b: 1 order, c: 1 order"
        );
    }
}

#[test]
fn si08_r2_n4_links_of_a_head_card_the_removal_had_seen_stay_hidden() {
    n4(Place::Head);
}

#[test]
fn si08_r2_n4_links_of_an_extra_card_the_removal_had_seen_stay_hidden() {
    n4(Place::Extra);
}

#[test]
fn si08_r2_n4_links_of_a_slot_card_the_removal_had_seen_stay_hidden() {
    n4(Place::Slot);
}

// ---------------------------------------------------------------------------
// N5, once: N2's schedule read at every step
// ---------------------------------------------------------------------------

/// N5. N2's sends, read at every step of one order per replica. On `b`,
/// which applied its removal first, `c`'s links arrive while no write the
/// removal had not seen has reached the card and are not read; once `a`'s
/// title arrives they are. On `a` and `c`, which hold such a write or no
/// removal, they are read at every step. The generated graph reads the same
/// at every step: on `b` the card has no vertex until `a`'s `AddVertex`
/// arrives.
fn n5(place: Place) {
    let nest = Nest::new();
    for removal in Removal::ALL {
        let holder = removal.holder();
        let label = format!("N5 {} ({})", place.label(), removal.name());
        let (mut replicas, title, remove, links, named) = n2_sent(&nest, removal, place);
        let o = At::Frame(holder, place);
        let reads = |doc: &Value| {
            (
                refs_at(doc, At::Cards(0), "blocker", &label),
                refs_at(doc, At::Cards(0), "related", &label),
                refs_at(doc, o, "related", &label),
            )
        };
        let hidden = (set(&[]), names(&named, &["z"]), set(&[]));
        let read = (
            names(&named, &[place.label()]),
            names(&named, &[place.label(), "z"]),
            names(&named, &["y"]),
        );

        // b: its own removal, then c's links, then a's title.
        for message in &links {
            replicas[1].receive(message.clone());
        }
        let on_b = identified(&replicas[1]);
        assert_eq!(
            reads(&on_b),
            hidden,
            "{label} (parity): on b, c's links arrive after b's removal and before any write it had not \
             seen; none that touches {} is read",
            place.label()
        );
        replicas[1].receive(title.clone());
        assert_eq!(
            reads(&identified(&replicas[1])),
            read,
            "{label}: on b, once a's title (a write the removal had not seen) arrives, the links are read"
        );

        // a: its own title, then c's links, then b's removal.
        for message in &links {
            replicas[0].receive(message.clone());
        }
        assert_eq!(
            reads(&identified(&replicas[0])),
            read,
            "{label}: on a, before the removal"
        );
        replicas[0].receive(remove.clone());
        assert_eq!(
            reads(&identified(&replicas[0])),
            read,
            "{label}: on a, the removal arrives after a write it had not seen; the links stay read"
        );

        // c: its own links, then a's title, then b's removal.
        assert_eq!(
            reads(&identified(&replicas[2])),
            read,
            "{label}: c reads its own links"
        );
        replicas[2].receive(title);
        assert_eq!(
            reads(&identified(&replicas[2])),
            read,
            "{label}: c after a's title"
        );
        replicas[2].receive(remove);
        assert_eq!(
            reads(&identified(&replicas[2])),
            read,
            "{label}: c after b's removal"
        );

        let doc = agreed(&replicas, &format!("{label}, all delivered"));
        assert_eq!(reads(&doc), read, "{label}: all delivered");
        let expect = Expect {
            label: "",
            titles: titles_with_q(place),
            x_blocker: &[],
            x_related: &[],
            o_blocker: &[],
            o_related: &[],
        };
        holds_containment(&doc, &named, holder, &expect, &label);
    }
}

#[test]
fn si08_r2_n5_links_of_a_head_card_are_read_once_an_unseen_write_reaches_it() {
    n5(Place::Head);
}

#[test]
fn si08_r2_n5_links_of_an_extra_card_are_read_once_an_unseen_write_reaches_it() {
    n5(Place::Extra);
}

#[test]
fn si08_r2_n5_links_of_a_slot_card_are_read_once_an_unseen_write_reaches_it() {
    n5(Place::Slot);
}

// ---------------------------------------------------------------------------
// N6, a hidden link does not win a single-valued reference
// ---------------------------------------------------------------------------

/// N6. `b` removes the frame; `a`, concurrently, links `x.blocker -> y`;
/// `c`, concurrently, links `x.related -> z` and then `x.blocker` to the card
/// at `place`, so that link carries the greatest Lamport time of the two
/// `x.blocker` links. The generated graph takes the arc to the card out of
/// the candidates (no vertex) before it keeps the greatest, so `x.blocker`
/// reads `y` on every replica in every order.
fn n6(place: Place) {
    let nest = Nest::new();
    for removal in Removal::ALL {
        let holder = removal.holder();
        let label = format!("N6 {} ({})", place.label(), removal.name());
        let schedule = || {
            let (mut replicas, named) = base(&nest, holder);
            let from_a = send(
                &mut replicas[0],
                nest.link(At::Cards(0), "blocker", &named["y"]),
                "a links x.blocker -> y",
            );
            let remove = send(&mut replicas[1], nest.removal(removal), removal.name());
            let from_c = vec![
                send(
                    &mut replicas[2],
                    nest.link(At::Cards(0), "related", &named["z"]),
                    "c links x.related -> z",
                ),
                send(
                    &mut replicas[2],
                    nest.link(At::Cards(0), "blocker", &named[place.label()]),
                    "c links x.blocker -> the target card",
                ),
            ];
            assert_eq!(
                refs_at(&identified(&replicas[2]), At::Cards(0), "blocker", &label),
                names(&named, &[place.label()]),
                "{label} premise: c reads x.blocker -> the target card before it delivers anything"
            );
            Run {
                replicas,
                incoming: [
                    vec![vec![remove.clone()], from_c.clone()],
                    vec![vec![from_a.clone()], from_c],
                    vec![vec![from_a], vec![remove]],
                ],
                named,
            }
        };
        let expect = Expect {
            label: "",
            titles: ["", "", ""],
            x_blocker: &["y"],
            x_related: &["z"],
            o_blocker: &[],
            o_related: &[],
        };
        let runs = every_order(&label, schedule, |doc, run| {
            holds(doc, run, holder, place, &expect, &label);
        });
        assert_eq!(
            runs, 18,
            "{label}: a: 3 interleavings, b: 3 interleavings, c: 2 interleavings"
        );
    }
}

#[test]
fn si08_r2_n6_a_hidden_link_to_a_head_card_does_not_win_a_single_valued_reference() {
    n6(Place::Head);
}

#[test]
fn si08_r2_n6_a_hidden_link_to_an_extra_card_does_not_win_a_single_valued_reference() {
    n6(Place::Extra);
}

#[test]
fn si08_r2_n6_a_hidden_link_to_a_slot_card_does_not_win_a_single_valued_reference() {
    n6(Place::Slot);
}
