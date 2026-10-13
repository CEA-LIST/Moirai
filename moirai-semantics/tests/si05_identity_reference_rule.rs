//! SI.05 (`cea-cdrt-knowledge/spec/items/SI.05.md`), K1 and R6 in the table: `moirai-semantics`
//! reads the new reference rule a descriptor names, `{"kind": "identityReference", "many",
//! "target"}`, keeps reading today's `{"kind": "reference", ...}` as it does, and the table a
//! node serializes (it travels in the log, decision D2) spells the new rule with the same tag.
//!
//! Fixtures: the board metamodel of `moirai-interp/tests/fixtures/si05/`, on the new rule and on
//! today's.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-semantics --test si05_identity_reference_rule
//! ```

use moirai_semantics::{
    ClassSlot, MergeRule, MetamodelSemantics, SemanticsError, from_descriptor, metamodel_digest,
};
use serde_json::{Value, json};

macro_rules! fixture {
    ($file:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../moirai-interp/tests/fixtures/si05/",
            $file
        ))
    };
}

const BOARD: &str = fixture!("si05board.metamodel.json");
const BOARD_TODAY: &str = fixture!("si05board.today.metamodel.json");

fn parse(text: &str) -> Result<MetamodelSemantics, SemanticsError> {
    from_descriptor(&serde_json::from_str::<Value>(text).expect("a fixture is JSON"))
}

fn class(sem: &MetamodelSemantics, name: &str) -> ClassSlot {
    sem.classes
        .iter()
        .find(|c| &*c.name == name)
        .unwrap_or_else(|| panic!("no class {name}"))
        .slot
}

/// The rule of a declared feature, by class and feature name.
fn rule<'a>(sem: &'a MetamodelSemantics, owner: &str, feature: &str) -> &'a MergeRule {
    let slot = class(sem, owner);
    let declared = sem.classes[slot.index()]
        .declared
        .iter()
        .find(|f| &*f.name == feature)
        .unwrap_or_else(|| panic!("{owner} declares no {feature}"));
    &declared.merge
}

/// K1: the new rule is read, for a single-valued and a many-valued reference, and the table
/// spells it `{"kind": "identityReference", "many", "target": <class slot>}`; the reference that
/// declares an `eOpposite` keeps today's rule, `MergeRule::Reference`, in the same table.
#[test]
fn si05_k1_the_identity_rule_is_read_and_spelled_in_the_table() {
    let sem = parse(BOARD)
        .unwrap_or_else(|error| panic!("K1: the board on the new rule is refused: {error}"));
    let card = class(&sem, "Card");
    let board = class(&sem, "Board");
    assert_eq!(
        serde_json::to_value(rule(&sem, "Card", "blocker")).expect("a rule serializes"),
        json!({"kind": "identityReference", "many": false, "target": card.0}),
        "K1: Card.blocker"
    );
    assert_eq!(
        serde_json::to_value(rule(&sem, "Card", "related")).expect("a rule serializes"),
        json!({"kind": "identityReference", "many": true, "target": card.0}),
        "K1: Card.related"
    );
    assert_eq!(
        rule(&sem, "Lane", "board"),
        &MergeRule::Reference {
            many: false,
            target: board
        },
        "K1: a reference that declares an eOpposite keeps today's rule"
    );
    // The table crosses a state transfer as JSON and comes back the same.
    let back: MetamodelSemantics =
        serde_json::from_value(serde_json::to_value(&sem).expect("the table serializes"))
            .expect("and reads back");
    assert_eq!(back, sem, "the table on the new rule round-trips");
}

/// R6: today's descriptor is read exactly as before, every reference on `MergeRule::Reference`,
/// and the two descriptors are two metamodel ids.
#[test]
fn si05_r6_todays_descriptor_is_read_as_before_and_the_digests_differ() {
    let today = parse(BOARD_TODAY).expect("today's board is read");
    let card = class(&today, "Card");
    assert_eq!(
        rule(&today, "Card", "blocker"),
        &MergeRule::Reference {
            many: false,
            target: card
        }
    );
    assert_eq!(
        rule(&today, "Card", "related"),
        &MergeRule::Reference {
            many: true,
            target: card
        }
    );
    let new: Value = serde_json::from_str(BOARD).expect("JSON");
    let old: Value = serde_json::from_str(BOARD_TODAY).expect("JSON");
    assert_ne!(
        metamodel_digest(&new),
        metamodel_digest(&old),
        "R6: a new digest names the new rule"
    );
    let new_sem =
        parse(BOARD).unwrap_or_else(|error| panic!("R6: the new rule is refused: {error}"));
    assert_eq!(
        new_sem.digest,
        metamodel_digest(&new),
        "the table carries the new descriptor's digest"
    );
    assert_ne!(new_sem.digest, today.digest);
}

/// The new rule names its target by class, like every other rule: a target the descriptor does
/// not list is refused, naming it.
#[test]
fn si05_k1_an_identity_reference_to_an_unknown_class_is_refused() {
    let mut descriptor: Value = serde_json::from_str(BOARD).expect("JSON");
    let entries = descriptor["classes"]["Card"]["references"]
        .as_array_mut()
        .expect("references");
    let blocker = entries
        .iter_mut()
        .find(|e| e["name"] == "blocker")
        .expect("blocker");
    blocker["merge"]["target"] = json!("Nowhere");
    match from_descriptor(&descriptor) {
        Err(SemanticsError::UnknownClass { name, .. }) => assert_eq!(name, "Nowhere"),
        other => {
            panic!("an identity reference to an unlisted class is not refused by name: {other:?}")
        }
    }
}
