//! SI.07, claim I1 (intake) on the interpreted log, in process.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.07.md`. Under a descriptor
//! with the identity rule, the local intake refuses a `Link` (K2) whose
//! identity the writer's read-out serves on an object whose class does not
//! conform to the reference's target class (the class itself or one of its
//! subclasses, at any depth), as the generated `typed_graph!` refuses an arc
//! to a vertex of the wrong kind (it has no arc kind for it). A refused link
//! changes nothing. A `Link` to an identity the writer's read-out does not
//! serve keeps SI.05's behaviour and is accepted (ruling R3.1, reading B). A
//! link delivered from a peer is never refused, whatever the receiver serves.
//!
//! Ground truth: the class each identity is served on, read off the fixture
//! documents, and the `supers` of each descriptor's classes, read by hand:
//! the board's `Card.blocker` and `Card.related` point at `Card`, and
//! SimpleUML's `Generalization.general` and `Association.source`/`target` at
//! `Class`, its `Property.ttype` at the abstract `TType`, whose subclasses are
//! `DataType`, `Class` (through `DataType`), `Enumeration` and
//! `PrimitiveType`; `Package` and `Model` are `Classifier`s and
//! `Packageable`s, not `TType`s, and `DataType` is a supertype of `Class`,
//! not a subclass.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --test si07_i1_intake
//! ```

mod si05;

use std::collections::BTreeSet;

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::leaf::LeafOp;
use moirai_interp::testing::{class_slot, feature_slot, install, table};
use moirai_interp::{InstanceOp, ModelOp, Node};
use moirai_protocol::replica::IsReplica;
use moirai_semantics::{ClassSlot, FeatureSlot};
use serde_json::Value;

use si05::*;

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

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

/// The identities the reference `feature` of the card at `Board.cards[pos]`
/// holds in replica `replica`'s log, linked or not read: what the log
/// stored, read through the crate's public tree (`ModelLog::root`,
/// `ObjectNode::fields`, `RefNode::targets`), independent of what the
/// read-out shows.
fn held(replica: &Rep, board: &Board, pos: usize, feature: FeatureSlot) -> BTreeSet<String> {
    let Node::Slot(root) = replica.state().root() else {
        panic!("the model root is not a slot");
    };
    let objects = root.objects();
    let board_object = objects.first().expect("the board root is there");
    let Some(Node::Seq(cards)) = board_object.fields().get(&board.board_cards) else {
        panic!("Board.cards is not a sequence node");
    };
    let order = cards.order();
    let child = cards
        .children()
        .get(&order[pos])
        .unwrap_or_else(|| panic!("no card at Board.cards[{pos}]"));
    let Node::Slot(card) = child else {
        panic!("Board.cards[{pos}] is not a slot");
    };
    let card = card.objects()[0];
    match card.fields().get(&feature) {
        None => BTreeSet::new(),
        Some(Node::Refs(refs)) => refs.targets().map(str::to_string).collect(),
        Some(_) => panic!("the reference is not a reference node"),
    }
}

/// The board with `x`, `y`, `z` titled at `Board.cards[0..3]` and an empty
/// lane at `Board.lanes[0]`, built on replica 0 and delivered; answers the
/// replicas and the identities of `x`, `y`, `z`, the lane and the board.
fn board_with_lane(board: &Board) -> ([Rep; 3], [String; 3], String, String) {
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
    ops.push(board.new_lane(0));
    broadcast(&mut replicas, 0, ops, "building the board");
    let doc = agreed(&replicas, "the board");
    let ids = [0, 1, 2].map(|i| id_at(&doc, At::Board(i)));
    let lane = doc["lanes"][0][ID]
        .as_str()
        .unwrap_or_else(|| panic!("premise: the lane is served with an identity: {doc}"))
        .to_string();
    let root = doc[ID]
        .as_str()
        .unwrap_or_else(|| panic!("premise: the board is served with an identity: {doc}"))
        .to_string();
    (replicas, ids, lane, root)
}

/// I1-1. On replica `a`, which serves the lane and the board: a `Link` of the
/// lane's identity or of the board's at `x.blocker` (`[0..1]`, target `Card`)
/// or at `x.related` (`[0..*]`, target `Card`) is refused, and the model is
/// unchanged after each.
#[test]
fn si07_i1_1_a_link_to_a_served_object_of_another_class_is_refused_and_changes_nothing() {
    let board = Board::new();
    let (mut replicas, _, lane, root) = board_with_lane(&board);
    let before = identified(&replicas[0]);
    for (feature, name) in [(board.blocker, "blocker"), (board.related, "related")] {
        for (target, what) in [(&lane, "the lane"), (&root, "the board")] {
            let op = board
                .link(At::Board(0), feature, target)
                .unwrap_or_else(|why| panic!("{why}"));
            let outcome = replicas[0].send(op);
            assert!(
                outcome.is_err(),
                "I1: a link of {what}'s identity {target} at x.{name}, whose target class is Card, \
                 was accepted at the local intake"
            );
            assert_eq!(
                identified(&replicas[0]),
                before,
                "I1: the refused link of {what} at x.{name} changed the model"
            );
            assert_eq!(
                held(&replicas[0], &board, 0, feature),
                set(&[]),
                "I1: the refused link of {what} at x.{name} was stored"
            );
        }
    }
}

/// I1-2, what stays accepted. On replica `a`: a link of `y` at `x.blocker`
/// and of `z` at `x.related` (served cards, the target class itself); of an
/// identity nobody serves (`nobody@1.1`); and of the identity `w` that `b`
/// serves for a card it created and `a` has not delivered (predicted). All
/// are accepted; once everything is delivered every replica serves one
/// document, in which `x.related` reads `z` and `w` and not `nobody@1.1`.
#[test]
fn si07_i1_2_a_link_to_a_served_card_or_to_an_identity_the_writer_does_not_serve_is_accepted() {
    let board = Board::new();
    let (mut replicas, [_, y, z], _, _) = board_with_lane(&board);
    let create = send(
        &mut replicas[1],
        board.new_card(3),
        "b creates w at Board.cards[3]",
    );
    let w = id_at(&identified(&replicas[1]), At::Board(3));
    assert!(
        !has_card(&identified(&replicas[0]), At::Board(3)),
        "premise: a has not delivered w's creation"
    );
    let mut sent = Vec::new();
    for (feature, target, what) in [
        (board.blocker, y.as_str(), "y at x.blocker"),
        (board.related, z.as_str(), "z at x.related"),
        (
            board.related,
            "nobody@1.1",
            "an identity nobody serves at x.related",
        ),
        (
            board.related,
            w.as_str(),
            "w, which a does not serve yet, at x.related",
        ),
    ] {
        let op = board
            .link(At::Board(0), feature, target)
            .unwrap_or_else(|why| panic!("{why}"));
        match replicas[0].send(op) {
            Ok(message) => sent.push(message),
            Err(why) => panic!("I1: the local intake refused the link of {what}: {why:?}"),
        }
    }
    replicas[0].receive(create.clone());
    replicas[2].receive(create);
    for message in &sent {
        replicas[1].receive(message.clone());
        replicas[2].receive(message.clone());
    }
    let doc = agreed(&replicas, "I1-2");
    assert_eq!(refs(&doc, At::Board(0), "blocker"), set(&[&y]));
    assert_eq!(
        refs(&doc, At::Board(0), "related"),
        set(&[&z, &w]),
        "I1-2: x.related reads z and w, and not the identity nobody serves"
    );
}

/// I1-3, a peer's link is never refused. `a` creates a second lane `m` and
/// does not send it to `b`; `b`, which does not serve `m`, links `x.blocker`
/// to `m`'s identity (accepted: `b` does not serve it). `a` (which serves `m`
/// as a `Lane`) delivers the link; `c` delivers the lane, then the link; `b`
/// then delivers the lane. Every replica's log holds the link, and every
/// replica serves one document.
#[test]
fn si07_i1_3_a_link_delivered_from_a_peer_is_not_refused_whatever_the_receiver_serves() {
    let board = Board::new();
    let (mut replicas, _, _, _) = board_with_lane(&board);
    let lane = send(&mut replicas[0], board.new_lane(1), "a creates the lane m");
    let m = identified(&replicas[0])["lanes"][1][ID]
        .as_str()
        .expect("premise: a serves m with an identity")
        .to_string();
    assert!(
        identified(&replicas[1])["lanes"].as_array().map(Vec::len) == Some(1),
        "premise: b does not serve m"
    );
    let link = match replicas[1].send(
        board
            .link(At::Board(0), board.blocker, &m)
            .unwrap_or_else(|why| panic!("{why}")),
    ) {
        Ok(message) => message,
        Err(why) => panic!("I1-3 premise: b, which does not serve m, could not link it: {why:?}"),
    };
    assert_eq!(
        held(&replicas[1], &board, 0, board.blocker),
        set(&[&m]),
        "b stored its link"
    );
    replicas[0].receive(link.clone());
    replicas[2].receive(lane.clone());
    replicas[2].receive(link);
    replicas[1].receive(lane);
    for (index, replica) in replicas.iter().enumerate() {
        assert_eq!(
            held(replica, &board, 0, board.blocker),
            set(&[&m]),
            "I1: replica {index} did not store the link a peer sent (remote delivery is never refused)"
        );
    }
    agreed(&replicas, "I1-3");
    let plain = default_read(&replicas[0]);
    for (index, replica) in replicas.iter().enumerate().skip(1) {
        assert_eq!(
            default_read(replica),
            plain,
            "I1-3: replica {index}'s default read"
        );
    }
}

// ---------------------------------------------------------------------------
// SimpleUML: conformance through the supertypes
// ---------------------------------------------------------------------------

/// SimpleUML on the identity rule, the same bytes as Arachne's and the
/// editor's SI.05 fixture (sha256 `8c50a7a8...`).
const SIMPLEUML: &str = include_str!("fixtures/si07/simpleuml.identity.metamodel.json");

struct Uml {
    model: ClassSlot,
    class: ClassSlot,
    data_type: ClassSlot,
    primitive: ClassSlot,
    association: ClassSlot,
    package: ClassSlot,
    generalization: ClassSlot,
    property: ClassSlot,
    owned: FeatureSlot,
    generalizations: FeatureSlot,
    attributes: FeatureSlot,
    general: FeatureSlot,
    ttype: FeatureSlot,
    source: FeatureSlot,
    target: FeatureSlot,
}

impl Uml {
    fn new(descriptor: &Value) -> Uml {
        let sem = table(descriptor);
        let model = class_slot(&sem, "Model");
        let class = class_slot(&sem, "Class");
        let generalization = class_slot(&sem, "Generalization");
        let property = class_slot(&sem, "Property");
        let association = class_slot(&sem, "Association");
        Uml {
            model,
            class,
            data_type: class_slot(&sem, "DataType"),
            primitive: class_slot(&sem, "PrimitiveType"),
            association,
            package: class_slot(&sem, "Package"),
            generalization,
            property,
            owned: feature_slot(&sem, model, "ownedElements"),
            generalizations: feature_slot(&sem, class, "generalizations"),
            attributes: feature_slot(&sem, class, "attributes"),
            general: feature_slot(&sem, generalization, "general"),
            ttype: feature_slot(&sem, property, "ttype"),
            source: feature_slot(&sem, association, "source"),
            target: feature_slot(&sem, association, "target"),
        }
    }

    fn on_model(&self, inner: InstanceOp) -> ModelOp {
        ModelOp::Instance(InstanceOp::variant(self.model, inner))
    }

    fn owned(&self, pos: usize, class: ClassSlot, inner: InstanceOp) -> ModelOp {
        self.on_model(InstanceOp::field(
            self.owned,
            InstanceOp::at(pos, InstanceOp::variant(class, inner)),
        ))
    }

    fn create_owned(&self, pos: usize, class: ClassSlot) -> ModelOp {
        self.on_model(InstanceOp::field(
            self.owned,
            InstanceOp::insert(pos, InstanceOp::variant(class, InstanceOp::New)),
        ))
    }

    fn link(&self, from: LinkSite, id: &str) -> ModelOp {
        let leaf = InstanceOp::Leaf(LeafOp::Link(id.to_string()));
        match from {
            LinkSite::General => self.owned(
                0,
                self.class,
                InstanceOp::field(
                    self.generalizations,
                    InstanceOp::at(
                        0,
                        InstanceOp::variant(
                            self.generalization,
                            InstanceOp::field(self.general, leaf),
                        ),
                    ),
                ),
            ),
            LinkSite::Ttype => self.owned(
                0,
                self.class,
                InstanceOp::field(
                    self.attributes,
                    InstanceOp::at(
                        0,
                        InstanceOp::variant(self.property, InstanceOp::field(self.ttype, leaf)),
                    ),
                ),
            ),
            LinkSite::AssociationSource => {
                self.owned(3, self.association, InstanceOp::field(self.source, leaf))
            }
            LinkSite::AssociationTarget => {
                self.owned(3, self.association, InstanceOp::field(self.target, leaf))
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum LinkSite {
    /// `C.generalizations[0].general`, target `Class`.
    General,
    /// `C.attributes[0].ttype`, target `TType`.
    Ttype,
    /// `A.source`, target `Class`.
    AssociationSource,
    /// `A.target`, target `Class`.
    AssociationTarget,
}

/// I1-4. A model `M` holding `ownedElements = [C: Class{generalizations: [G],
/// attributes: [P: Property]}, D: Class, T: PrimitiveType, A: Association,
/// K: Package, DT: DataType]`, built on `a` and delivered. On `a`, each link
/// below is accepted or refused as the target class and its subclasses say,
/// and a refused one changes nothing.
#[test]
fn si07_i1_4_conformance_is_the_target_class_or_a_subclass_at_any_depth() {
    let descriptor: Value = serde_json::from_str(SIMPLEUML).expect("the SimpleUML fixture is JSON");
    let uml = Uml::new(&descriptor);
    let (mut a, mut b, mut c) = triplet_log::<moirai_interp::ModelLog>();
    let event = a
        .send(install("si07-uml", &descriptor))
        .expect("a fresh log takes its Install");
    b.receive(event.clone());
    c.receive(event);
    let mut replicas = [a, b, c];
    let ops = vec![
        uml.on_model(InstanceOp::New),
        uml.create_owned(0, uml.class),
        uml.create_owned(1, uml.class),
        uml.create_owned(2, uml.primitive),
        uml.create_owned(3, uml.association),
        uml.create_owned(4, uml.package),
        uml.create_owned(5, uml.data_type),
        uml.owned(
            0,
            uml.class,
            InstanceOp::field(
                uml.generalizations,
                InstanceOp::insert(0, InstanceOp::variant(uml.generalization, InstanceOp::New)),
            ),
        ),
        uml.owned(
            0,
            uml.class,
            InstanceOp::field(
                uml.attributes,
                InstanceOp::insert(0, InstanceOp::variant(uml.property, InstanceOp::New)),
            ),
        ),
    ];
    broadcast(&mut replicas, 0, ops, "building the SimpleUML model");
    let doc = agreed(&replicas, "the SimpleUML model");
    let id = |value: &Value, what: &str| -> String {
        value[ID]
            .as_str()
            .unwrap_or_else(|| panic!("premise: {what} is served with an identity: {doc}"))
            .to_string()
    };
    let owned = &doc["ownedElements"];
    let m = id(&doc, "M");
    let c_id = id(&owned[0], "C");
    let d = id(&owned[1], "D");
    let t = id(&owned[2], "T");
    let assoc = id(&owned[3], "A");
    let k = id(&owned[4], "K");
    let dt = id(&owned[5], "DT");
    let p = id(&owned[0]["attributes"][0], "P");
    let cases: Vec<(LinkSite, &str, &str, bool)> = vec![
        (LinkSite::General, &d, "D: Class, the target class", true),
        (
            LinkSite::General,
            &dt,
            "DT: DataType, a supertype of Class",
            false,
        ),
        (LinkSite::General, &t, "T: PrimitiveType", false),
        (LinkSite::General, &k, "K: Package", false),
        (
            LinkSite::Ttype,
            &c_id,
            "C: Class, a subclass of TType through DataType",
            true,
        ),
        (
            LinkSite::Ttype,
            &t,
            "T: PrimitiveType, a subclass of TType",
            true,
        ),
        (
            LinkSite::Ttype,
            &dt,
            "DT: DataType, a subclass of TType",
            true,
        ),
        (
            LinkSite::Ttype,
            &k,
            "K: Package, a Classifier and not a TType",
            false,
        ),
        (LinkSite::Ttype, &p, "P: Property", false),
        (LinkSite::Ttype, &m, "M: Model, a Package", false),
        (LinkSite::AssociationSource, &assoc, "A: Association", false),
        (LinkSite::AssociationTarget, &c_id, "C: Class", true),
    ];
    for (from, target, what, accepted) in cases {
        let before = identified(&replicas[0]);
        let outcome = replicas[0].send(uml.link(from, target));
        if accepted {
            assert!(
                outcome.is_ok(),
                "I1: the link of {what} at {from:?} conforms and was refused: {:?}",
                outcome.err()
            );
        } else {
            assert!(
                outcome.is_err(),
                "I1: the link of {what} at {from:?} does not conform and was accepted at the local intake"
            );
            assert_eq!(
                identified(&replicas[0]),
                before,
                "I1: the refused link of {what} at {from:?} changed the model"
            );
        }
    }
}
