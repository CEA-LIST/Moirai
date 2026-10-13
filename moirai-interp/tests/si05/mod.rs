//! SI.05 oracle, the part its two test files share: the board metamodel, the
//! operations a writer sends on it, and how a served document is read.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.05.md`. The contract these
//! tests read is its section 3, restated here so a reader of the tests does
//! not need the spec open:
//!
//! - K1, the rule. A descriptor names the new rule on a reference entry as
//!   `"merge": {"kind": "identityReference", "many": <bool>, "target": <class>}`.
//!   A reference entry that declares an `eOpposite` keeps
//!   `{"kind": "reference", ...}`, today's string rule, unchanged.
//! - K2, the wire. A writer links and unlinks the object whose served
//!   identity is `<id>` with `{"Leaf": {"Link": "<id>"}}` and
//!   `{"Leaf": {"Unlink": "<id>"}}`, at the end of a path that steps into the
//!   reference feature of the source object (`Field(slot, Leaf(..))`), for a
//!   single-valued and a many-valued reference alike.
//! - K3, the read-out. In the identified read and in the default read alike,
//!   a single-valued identity reference reads as its target's served identity
//!   (the string the target carries under `__id`), and as absent or `null`
//!   when it holds none; a many-valued one reads as an array of identity
//!   strings, each once. A target that is not in the read-out is not read.
//!
//! The board metamodel is `arachne-codegen/tests/fixtures/si05/si05board.ecore`,
//! described by Arachne d81cae8 (`si05board.today.metamodel.json`, every
//! reference on today's rule) and moved to the new rule by the transform of
//! the spec, section 3.1 (`si05board.metamodel.json`):
//!
//! ```text
//! Board  name: text, cards: Card*, lanes: Lane* (eOpposite Lane.board), pinned: Card?
//! Lane   name: text, cards: Card*, board -> Board (eOpposite, today's rule)
//! Card   title: text (iD), note: text, blocker -> Card [0..1], related -> Card [0..*]
//! ```
#![allow(dead_code)]

use std::collections::BTreeSet;

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::eval::read_identified;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{class_slot, feature_slot, install, table};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::{ClassSlot, FeatureSlot, MetamodelSemantics};
use serde_json::{Value, json};

/// The reserved key an element's store identity is served under.
pub const ID: &str = "__id";

/// The board on the new rule: `Card.blocker` and `Card.related` are identity
/// references, `Lane.board` keeps today's rule because it declares an
/// `eOpposite`.
pub const BOARD: &str = include_str!("../fixtures/si05/si05board.metamodel.json");

/// The same board as today's emitter describes it: every reference on
/// today's string rule.
pub const BOARD_TODAY: &str = include_str!("../fixtures/si05/si05board.today.metamodel.json");

pub type Rep = Replica<ModelLog, Tcsb<ModelOp>>;
pub type Msg = EventMessage<ModelOp>;

pub fn board_descriptor() -> Value {
    serde_json::from_str(BOARD).expect("the board fixture is JSON")
}

pub fn board_today_descriptor() -> Value {
    serde_json::from_str(BOARD_TODAY).expect("the board fixture is JSON")
}

/// The board's slots, read from whichever descriptor is given: both name the
/// same classes and features, so the slots are the same.
pub struct Board {
    pub board: ClassSlot,
    pub lane: ClassSlot,
    pub card: ClassSlot,
    pub board_cards: FeatureSlot,
    pub board_lanes: FeatureSlot,
    pub board_pinned: FeatureSlot,
    pub lane_cards: FeatureSlot,
    pub lane_board: FeatureSlot,
    pub title: FeatureSlot,
    pub note: FeatureSlot,
    pub blocker: FeatureSlot,
    pub related: FeatureSlot,
}

impl Board {
    /// Read from today's descriptor, which every build of this crate parses,
    /// so a build that cannot read the new rule still gets its slots and fails
    /// where the rule is used rather than here.
    pub fn new() -> Board {
        let sem: std::sync::Arc<MetamodelSemantics> = table(&board_today_descriptor());
        let board = class_slot(&sem, "Board");
        let lane = class_slot(&sem, "Lane");
        let card = class_slot(&sem, "Card");
        Board {
            board,
            lane,
            card,
            board_cards: feature_slot(&sem, board, "cards"),
            board_lanes: feature_slot(&sem, board, "lanes"),
            board_pinned: feature_slot(&sem, board, "pinned"),
            lane_cards: feature_slot(&sem, lane, "cards"),
            lane_board: feature_slot(&sem, lane, "board"),
            title: feature_slot(&sem, card, "title"),
            note: feature_slot(&sem, card, "note"),
            blocker: feature_slot(&sem, card, "blocker"),
            related: feature_slot(&sem, card, "related"),
        }
    }
}

/// Where a card sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
    /// `Board.cards[i]`.
    Board(usize),
    /// `Board.lanes[l].cards[i]`.
    Lane(usize, usize),
    /// `Board.pinned`.
    Pinned,
}

impl Board {
    fn on_board(&self, inner: InstanceOp) -> ModelOp {
        ModelOp::Instance(InstanceOp::variant(self.board, inner))
    }

    /// Mint the root `Board`.
    pub fn root(&self) -> ModelOp {
        self.on_board(InstanceOp::New)
    }

    /// Insert a new card at `Board.cards[pos]`.
    pub fn new_card(&self, pos: usize) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.board_cards,
            InstanceOp::insert(pos, InstanceOp::variant(self.card, InstanceOp::New)),
        ))
    }

    /// Insert a new lane at `Board.lanes[pos]`.
    pub fn new_lane(&self, pos: usize) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.board_lanes,
            InstanceOp::insert(pos, InstanceOp::variant(self.lane, InstanceOp::New)),
        ))
    }

    /// Insert a new card at `Board.lanes[lane].cards[pos]`.
    pub fn new_lane_card(&self, lane: usize, pos: usize) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.board_lanes,
            InstanceOp::at(
                lane,
                InstanceOp::variant(
                    self.lane,
                    InstanceOp::field(
                        self.lane_cards,
                        InstanceOp::insert(pos, InstanceOp::variant(self.card, InstanceOp::New)),
                    ),
                ),
            ),
        ))
    }

    /// Put a new card in `Board.pinned`.
    pub fn new_pinned(&self) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.board_pinned,
            InstanceOp::set(InstanceOp::variant(self.card, InstanceOp::New)),
        ))
    }

    /// `inner` addressed to the card at `at`.
    pub fn on_card(&self, at: At, inner: InstanceOp) -> ModelOp {
        let card = InstanceOp::variant(self.card, inner);
        self.on_board(match at {
            At::Board(i) => InstanceOp::field(self.board_cards, InstanceOp::at(i, card)),
            At::Lane(l, i) => InstanceOp::field(
                self.board_lanes,
                InstanceOp::at(
                    l,
                    InstanceOp::variant(
                        self.lane,
                        InstanceOp::field(self.lane_cards, InstanceOp::at(i, card)),
                    ),
                ),
            ),
            At::Pinned => InstanceOp::field(self.board_pinned, InstanceOp::set(card)),
        })
    }

    /// One character into an optional text feature of the card at `at`.
    pub fn type_char(&self, at: At, feature: FeatureSlot, pos: usize, ch: char) -> ModelOp {
        self.on_card(
            at,
            InstanceOp::field(
                feature,
                InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })),
            ),
        )
    }

    /// The operations that type `text` into the (empty) title of the card at `at`.
    pub fn title_ops(&self, at: At, text: &str) -> Vec<ModelOp> {
        text.chars()
            .enumerate()
            .map(|(pos, ch)| self.type_char(at, self.title, pos, ch))
            .collect()
    }

    /// Rename: one character in front of the title of the card at `at`.
    pub fn rename(&self, at: At, ch: char) -> ModelOp {
        self.type_char(at, self.title, 0, ch)
    }

    /// Take the card at `Board.cards[pos]` out.
    pub fn delete_card(&self, pos: usize) -> ModelOp {
        self.on_board(InstanceOp::field(self.board_cards, InstanceOp::delete(pos)))
    }

    /// Take the lane at `Board.lanes[pos]` out, with its cards.
    pub fn delete_lane(&self, pos: usize) -> ModelOp {
        self.on_board(InstanceOp::field(self.board_lanes, InstanceOp::delete(pos)))
    }

    /// The K2 wire operation `{"Leaf": {<verb>: <id>}}` at the reference
    /// `feature` of the card at `at`, as JSON. Built from a path with a
    /// placeholder leaf, so the path is the one this crate's own builders
    /// make and only the leaf is spelled by hand.
    pub fn reference_json(&self, at: At, feature: FeatureSlot, verb: &str, id: &str) -> Value {
        let placeholder = self.on_card(
            at,
            InstanceOp::field(feature, InstanceOp::Leaf(LeafOp::Clear)),
        );
        let mut value = serde_json::to_value(&placeholder).expect("an operation serializes");
        let replaced = replace_placeholder(&mut value, &json!({ verb: id }));
        assert!(replaced, "the placeholder leaf was not found in {value}");
        value
    }

    /// `Link` as a [`ModelOp`] this build reads, or why it cannot read it.
    pub fn link(&self, at: At, feature: FeatureSlot, id: &str) -> Result<ModelOp, String> {
        wire_op(self.reference_json(at, feature, "Link", id))
    }

    /// `Unlink` as a [`ModelOp`] this build reads, or why it cannot read it.
    pub fn unlink(&self, at: At, feature: FeatureSlot, id: &str) -> Result<ModelOp, String> {
        wire_op(self.reference_json(at, feature, "Unlink", id))
    }

    /// Today's string rule: write `text` into a single-valued reference.
    pub fn write_string(&self, at: At, feature: FeatureSlot, text: &str) -> ModelOp {
        self.on_card(
            at,
            InstanceOp::field(feature, InstanceOp::Leaf(LeafOp::Write(Scalar::text(text)))),
        )
    }

    /// Today's string rule: add `text` to a many-valued reference.
    pub fn add_string(&self, at: At, feature: FeatureSlot, text: &str) -> ModelOp {
        self.on_card(
            at,
            InstanceOp::field(feature, InstanceOp::Leaf(LeafOp::Add(Scalar::text(text)))),
        )
    }

    /// Today's string rule on `Lane.board`, the reference that declares an
    /// `eOpposite`, of the lane at `Board.lanes[lane]`.
    pub fn write_lane_board(&self, lane: usize, text: &str) -> ModelOp {
        self.on_board(InstanceOp::field(
            self.board_lanes,
            InstanceOp::at(
                lane,
                InstanceOp::variant(
                    self.lane,
                    InstanceOp::field(
                        self.lane_board,
                        InstanceOp::Leaf(LeafOp::Write(Scalar::text(text))),
                    ),
                ),
            ),
        ))
    }
}

/// Swap the first `{"Leaf": "Clear"}` under `value` for `{"Leaf": leaf}`.
fn replace_placeholder(value: &mut Value, leaf: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.len() == 1 && map.get("Leaf") == Some(&json!("Clear")) {
                map.insert("Leaf".to_string(), leaf.clone());
                return true;
            }
            map.values_mut().any(|item| replace_placeholder(item, leaf))
        }
        Value::Array(items) => items.iter_mut().any(|item| replace_placeholder(item, leaf)),
        _ => false,
    }
}

/// A wire operation read the way a node reads `POST /api/model/{id}/op`.
pub fn wire_op(value: Value) -> Result<ModelOp, String> {
    serde_json::from_value::<ModelOp>(value.clone()).map_err(|error| {
        format!("K2: this build does not read the wire operation {value} as a ModelOp: {error}")
    })
}

// ---------------------------------------------------------------------------
// Replicas
// ---------------------------------------------------------------------------

/// Three replicas of one model on `descriptor`, `a` having sent the
/// `Install` and `b` and `c` having received it.
pub fn three(descriptor: &Value) -> [Rep; 3] {
    let (mut a, mut b, mut c) = triplet_log::<ModelLog>();
    let event = a
        .send(install("si05", descriptor))
        .expect("a fresh log takes its first `Install`");
    b.receive(event.clone());
    c.receive(event);
    [a, b, c]
}

/// Send `op` on `replica`, panicking with the refusal: every operation these
/// tests send is one the claim says a writer may send.
pub fn send(replica: &mut Rep, op: ModelOp, what: &str) -> Msg {
    let message = replica
        .send(op.clone())
        .unwrap_or_else(|error| panic!("{what}: the intake refused {op:?}: {error:?}"));
    message
}

/// Send a K2 operation, failing the test with the reason when this build
/// cannot even read it.
pub fn send_wire(replica: &mut Rep, op: Result<ModelOp, String>, what: &str) -> Msg {
    match op {
        Ok(op) => send(replica, op, what),
        Err(why) => panic!("{what}: {why}"),
    }
}

/// Send `ops` on `replicas[from]` and deliver each to every other replica.
pub fn broadcast(replicas: &mut [Rep], from: usize, ops: Vec<ModelOp>, what: &str) {
    for op in ops {
        let message = send(&mut replicas[from], op, what);
        for (index, replica) in replicas.iter_mut().enumerate() {
            if index != from {
                replica.receive(message.clone());
            }
        }
    }
}

/// Deliver `message` to every replica but `from`.
pub fn deliver_all(replicas: &mut [Rep], from: usize, message: &Msg) {
    for (index, replica) in replicas.iter_mut().enumerate() {
        if index != from {
            replica.receive(message.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// The identified read: what `GET /api/model/{id}/state/identified` serves.
pub fn identified(replica: &Rep) -> Value {
    let log = replica.state();
    match log.semantics() {
        Some(sem) => read_identified(sem, log.root()),
        None => Value::Null,
    }
}

/// The default read: what `GET /api/model/{id}/state` serves.
pub fn default_read(replica: &Rep) -> Value {
    replica.query(&Read::<Value>::new())
}

/// The card at `at` in a served document, or `None` when there is none.
pub fn card<'a>(doc: &'a Value, at: At) -> Option<&'a Value> {
    let found = match at {
        At::Board(i) => doc.get("cards")?.get(i)?,
        At::Lane(l, i) => doc.get("lanes")?.get(l)?.get("cards")?.get(i)?,
        At::Pinned => doc.get("pinned")?,
    };
    (!found.is_null()).then_some(found)
}

/// The served identity of the card at `at`.
pub fn id_at(doc: &Value, at: At) -> String {
    card(doc, at)
        .and_then(|card| card.get(ID))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no card with an identity at {at:?} in {doc}"))
        .to_string()
}

/// The title of the card at `at`, `""` when it has none.
pub fn title_at(doc: &Value, at: At) -> String {
    card(doc, at)
        .and_then(|card| card.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// What a reference feature of the card at `at` reads as, as a set of
/// strings: K3's single value or many values, absent and `null` both read as
/// none. A value that is neither a string, an array of strings, `null` nor
/// absent is a failure of K3 and panics.
pub fn refs(doc: &Value, at: At, feature: &str) -> BTreeSet<String> {
    let Some(card) = card(doc, at) else {
        panic!("no card at {at:?} in {doc}");
    };
    reference_values(card.get(feature), feature)
}

/// K3 for one value.
pub fn reference_values(value: Option<&Value>, feature: &str) -> BTreeSet<String> {
    match value {
        None | Some(Value::Null) => BTreeSet::new(),
        Some(Value::String(id)) => [id.clone()].into_iter().collect(),
        Some(Value::Array(items)) => {
            let set: BTreeSet<String> = items
                .iter()
                .map(|item| {
                    item.as_str()
                        .unwrap_or_else(|| panic!("K3: `{feature}` holds a non-string {item}"))
                        .to_string()
                })
                .collect();
            assert_eq!(
                set.len(),
                items.len(),
                "K3: `{feature}` reads one target twice: {items:?}"
            );
            set
        }
        Some(other) => panic!("K3: `{feature}` reads as {other}, neither an identity nor a list"),
    }
}

/// Every served identity in a document, with the title of its card.
pub fn identities(doc: &Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    walk(doc, &mut out);
    out
}

fn walk(value: &Value, out: &mut Vec<(String, String)>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(id)) = map.get(ID) {
                let title = map.get("title").and_then(Value::as_str).unwrap_or("");
                out.push((id.clone(), title.to_string()));
            }
            map.values().for_each(|item| walk(item, out));
        }
        Value::Array(items) => items.iter().for_each(|item| walk(item, out)),
        _ => {}
    }
}

/// Whether the card at `at` exists in `doc`.
pub fn has_card(doc: &Value, at: At) -> bool {
    card(doc, at).is_some()
}

/// The ids of the cards in `Board.cards`, in order.
pub fn board_card_ids(doc: &Value) -> Vec<String> {
    doc.get("cards")
        .and_then(Value::as_array)
        .map(|cards| {
            cards
                .iter()
                .filter_map(|card| card.get(ID).and_then(Value::as_str).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Every interleaving of `streams` that keeps each stream's own order: the
/// delivery orders causal broadcast allows for events of independent origins.
pub fn interleavings<T: Clone>(streams: &[Vec<T>]) -> Vec<Vec<T>> {
    fn go<T: Clone>(
        streams: &[Vec<T>],
        at: &mut Vec<usize>,
        acc: &mut Vec<T>,
        out: &mut Vec<Vec<T>>,
    ) {
        let mut done = true;
        for index in 0..streams.len() {
            if at[index] < streams[index].len() {
                done = false;
                acc.push(streams[index][at[index]].clone());
                at[index] += 1;
                go(streams, at, acc, out);
                at[index] -= 1;
                acc.pop();
            }
        }
        if done {
            out.push(acc.clone());
        }
    }
    let mut out = Vec::new();
    go(
        streams,
        &mut vec![0; streams.len()],
        &mut Vec::new(),
        &mut out,
    );
    out
}
