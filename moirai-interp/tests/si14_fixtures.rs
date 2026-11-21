//! SI.14 premises: what the five new revisions are, what copy (SI.12) says
//! of each, and that every expected document the claim tests hold a rebase
//! to is the document the same operations build when written directly under
//! the revision, which the tree builds today. Green at the base: none of
//! these reads a rebase.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.14.md`; the contract is
//! restated in `si14/mod.rs`.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si14_fixtures -- --test-threads=1
//! ```

mod si14;
#[path = "ec03/side.rs"]
mod side;

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::migrate::migrate;
use moirai_interp::testing::table;
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::{FlagWins, LeafRule, MergeRule, TieBreak};
use serde_json::{Value, json};
use side::triplet_log;

use si14::si12::{
    At, CAR, D1, D2, DOOR, ids_by_label, install, labels, parsed, project, strip_ids,
};
use si14::*;

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

fn identified(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

fn plain(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

/// A rule with its target class named rather than slotted: a revision that
/// removes a class moves the slot of every class sorted after it (D2_NOASSOC
/// moves them all), and a rule is the same rule when it targets the class
/// of the same name.
fn named(sem: &moirai_semantics::MetamodelSemantics, rule: &MergeRule) -> String {
    let class = |slot: &moirai_semantics::ClassSlot| sem.classes[slot.index()].name.to_string();
    match rule {
        MergeRule::Containment { shape, target } => {
            format!(
                "Containment {{ shape: {shape:?}, target: {} }}",
                class(target)
            )
        }
        MergeRule::Reference { many, target } => {
            format!("Reference {{ many: {many}, target: {} }}", class(target))
        }
        MergeRule::IdentityReference { many, target } => {
            format!(
                "IdentityReference {{ many: {many}, target: {} }}",
                class(target)
            )
        }
        other => format!("{other:?}"),
    }
}

/// Every class of a table, and every feature each class can see with the
/// rule it reads by (and that rule with its target named), as
/// `<Class>.<feature>`.
fn shape(text: &str) -> (BTreeSet<String>, BTreeMap<String, (MergeRule, String)>) {
    let sem = table(&parsed(text));
    let mut classes = BTreeSet::new();
    let mut features = BTreeMap::new();
    for class in &sem.classes {
        classes.insert(class.name.to_string());
        for (name, declaring, declared) in &class.visible {
            let rule = sem
                .rule(*declaring, *declared)
                .unwrap_or_else(|| panic!("no rule for {}.{name}", class.name));
            features.insert(format!("{}.{name}", class.name), (*rule, named(&sem, rule)));
        }
    }
    (classes, features)
}

/// F1: the five revisions, their digests, and the one change each makes.
#[test]
fn si14_f1_the_five_revisions_and_the_one_change_each_makes() {
    let (d1_classes, d1_features) = shape(D1);
    let (d2_classes, d2_features) = shape(D2);
    for (name, text) in [
        ("D2_DROP", D2_DROP),
        ("D2_NOASSOC", D2_NOASSOC),
        ("D2_FLAG", D2_FLAG),
        ("D2_KIND", D2_KIND),
        ("D2_LWW", D2_LWW),
    ] {
        let value = parsed(text);
        assert_eq!(
            value["nsURI"],
            parsed(D1)["nsURI"],
            "F1: {name} is a revision of SimpleUML"
        );
        assert_eq!(
            value["rootClasses"],
            parsed(D1)["rootClasses"],
            "F1: {name} keeps the roots"
        );
        println!("SI14-F1: {name} digest {}", si14::si12::digest(text));
    }

    // D2_DROP: class TaggedValue and every visible taggedValue gone, nothing
    // else changed.
    let (classes, features) = shape(D2_DROP);
    let gone: BTreeSet<String> = d1_classes.difference(&classes).cloned().collect();
    assert_eq!(
        gone,
        BTreeSet::from(["TaggedValue".to_string()]),
        "F1: D2_DROP's classes"
    );
    assert!(classes.is_subset(&d1_classes), "F1: D2_DROP adds no class");
    for (feature, rule) in &d1_features {
        let removed = feature.ends_with(".taggedValue") || feature.starts_with("TaggedValue.");
        match features.get(feature) {
            Some(kept) => {
                assert!(!removed, "F1: D2_DROP still has {feature}");
                assert_eq!(kept.1, rule.1, "F1: D2_DROP changes the rule of {feature}");
            }
            None => assert!(removed, "F1: D2_DROP lacks {feature}"),
        }
    }
    assert!(
        features.keys().all(|f| d1_features.contains_key(f)),
        "F1: D2_DROP adds no feature"
    );

    // D2_NOASSOC: class Association and its features gone.
    let (classes, features) = shape(D2_NOASSOC);
    let gone: BTreeSet<String> = d1_classes.difference(&classes).cloned().collect();
    assert_eq!(
        gone,
        BTreeSet::from(["Association".to_string()]),
        "F1: D2_NOASSOC's classes"
    );
    for (feature, rule) in &d1_features {
        match features.get(feature) {
            Some(kept) => assert_eq!(kept.1, rule.1, "F1: D2_NOASSOC changes {feature}"),
            None => assert!(
                feature.starts_with("Association."),
                "F1: D2_NOASSOC lacks {feature}"
            ),
        }
    }
    assert!(
        features.keys().all(|f| d1_features.contains_key(f)),
        "F1: D2_NOASSOC adds no feature"
    );

    // D2_FLAG and D2_KIND: one rule each, on every class that sees it.
    for (name, text, changed, want) in [
        (
            "D2_FLAG",
            D2_FLAG,
            ".abstract",
            "an optional disable-wins flag",
        ),
        (
            "D2_KIND",
            D2_KIND,
            ".owner",
            "a reference by identity, on the Packageable classes",
        ),
    ] {
        let (classes, features) = shape(text);
        assert_eq!(classes, d1_classes, "F1: {name} keeps the classes");
        assert_eq!(
            features.keys().collect::<Vec<_>>(),
            d1_features.keys().collect::<Vec<_>>(),
            "F1: {name} keeps the features"
        );
        let mut differ = Vec::new();
        for (feature, rule) in &d1_features {
            if features[feature].1 != rule.1 {
                differ.push(feature.clone());
                assert!(feature.ends_with(changed), "F1: {name} changes {feature}");
                match (name, &features[feature].0) {
                    (
                        "D2_FLAG",
                        MergeRule::Attribute {
                            leaf:
                                LeafRule::Flag {
                                    wins: FlagWins::Disable,
                                },
                            ..
                        },
                    ) => {}
                    ("D2_KIND", MergeRule::IdentityReference { .. }) => {
                        assert!(
                            matches!(rule.0, MergeRule::Reference { .. }),
                            "F1: D2_KIND's owner was a reference by text"
                        );
                    }
                    (_, other) => panic!("F1: {name} makes {feature} {other:?}, not {want}"),
                }
            }
        }
        assert!(!differ.is_empty(), "F1: {name} changes nothing");
        if name == "D2_FLAG" {
            assert_eq!(differ, vec!["Class.abstract".to_string()], "F1: D2_FLAG");
        }
        println!("SI14-F1: {name} changes {differ:?}");
    }

    // D2_LWW: D2 with the name a last-writer-wins register.
    let (classes, features) = shape(D2_LWW);
    assert_eq!(classes, d2_classes, "F1: D2_LWW keeps D2's classes");
    for (feature, rule) in &d2_features {
        if features[feature].1 != rule.1 {
            assert!(feature.ends_with(".name"), "F1: D2_LWW changes {feature}");
            assert!(
                matches!(
                    (&rule.0, &features[feature].0),
                    (
                        MergeRule::Attribute {
                            leaf: LeafRule::Register {
                                tie: TieBreak::Fair
                            },
                            ..
                        },
                        MergeRule::Attribute {
                            leaf: LeafRule::Register {
                                tie: TieBreak::LastWriterWins
                            },
                            ..
                        }
                    )
                ),
                "F1: D2_LWW makes {feature} {:?}",
                features[feature].0
            );
        }
    }
    assert_eq!(
        features.get("Class.name").map(|rule| rule.0),
        Some(MergeRule::Attribute {
            shape: moirai_semantics::Shape::Optional,
            leaf: LeafRule::Register {
                tie: TieBreak::LastWriterWins
            }
        }),
        "F1: D2_LWW's Class.name"
    );
}

/// F2: what copy (SI.12's `migrate`, unchanged by SI.14) says of each
/// revision. Copy accepts D2_FLAG, which is why a rebase refusal of it names
/// strategy copy (M19); it refuses D2_KIND by the feature, and D2_DROP and
/// D2_NOASSOC by every class and feature they lack, the names `drops`
/// spells (M20).
#[test]
fn si14_f2_what_copy_says_of_each_revision() {
    let (mut a, _b, _c) = triplet_log::<ModelLog>();
    a.send(install(MODEL, D1)).expect("the Install");
    let uml = Uml::new(D1);
    for op in uml.structure() {
        a.send(op).expect("the running example");
    }
    let log = a.state().clone();
    assert!(
        migrate(&log, "si14-copy", D2_FLAG).is_ok(),
        "F2: copy accepts D2_FLAG"
    );
    for (name, text, must_name) in [
        ("D2_KIND", D2_KIND, vec!["Packageable.owner"]),
        ("D2_DROP", D2_DROP, DROPS_TAGGED.to_vec()),
        ("D2_NOASSOC", D2_NOASSOC, DROPS_ASSOC.to_vec()),
    ] {
        match migrate(&log, "si14-copy", text) {
            Ok(_) => panic!("F2: copy accepts {name}"),
            Err(why) => {
                let why = why.to_string();
                for feature in must_name {
                    assert!(
                        why.contains(&format!("`{feature}`")),
                        "F2: copy refuses {name} without naming `{feature}`: {why}"
                    );
                }
                println!("SI14-F2: copy refuses {name}: {why}");
            }
        }
    }
}

use si14::si12::Uml;

/// The running example built directly under `text`, on three replicas, with
/// `then` written on `a` afterwards; answers the three replicas and the
/// labels of the objects before `then`.
fn direct(
    text: &str,
    tagged: bool,
    assoc: bool,
    then: impl Fn(&Uml) -> Vec<ModelOp>,
) -> ([Rep; 3], BTreeMap<String, String>) {
    let uml = Uml::new(text);
    let (a, b, c) = triplet_log::<ModelLog>();
    let mut reps = [a, b, c];
    let write = |reps: &mut [Rep; 3], op: ModelOp| {
        let message = reps[0]
            .send(op)
            .unwrap_or_else(|why| panic!("premise: the direct build was refused: {why}"));
        reps[1].receive(message.clone());
        reps[2].receive(message);
    };
    write(&mut reps, install(MODEL, text));
    for op in structure_under(&uml, tagged, assoc) {
        write(&mut reps, op);
    }
    let ids = ids_by_label(&identified(&reps[0]));
    for op in links_under(&uml, &ids, assoc) {
        write(&mut reps, op);
    }
    let base = labels(&identified(&reps[0]));
    for op in then(&uml) {
        write(&mut reps, op);
    }
    (reps, base)
}

fn assert_reads(reps: &[Rep; 3], base: &BTreeMap<String, String>, want: &Value, what: &str) {
    for (index, replica) in reps.iter().enumerate() {
        let doc = identified(replica);
        assert_eq!(
            project_keep(&doc, base),
            *want,
            "F3: {what}, replica {index}, built directly, does not read as the oracle's document"
        );
        let merged = labels_keep(&doc, base);
        assert_eq!(
            si14::si12::project_with(&plain(replica), &merged),
            strip_ids(want),
            "F3: {what}, replica {index}, default read"
        );
        assert_eq!(replica.state().unresolved(), 0, "F3: {what}");
    }
}

/// F3: the documents the claim tests expect, each built directly under its
/// revision from the same operations: the running example under D2_DROP,
/// D2_NOASSOC and D2_LWW; RB's end (`Doors`, abstract, `persistent`) under
/// D2; RD's end under D2_NOASSOC (`Wheel` after `String`, its stereotype
/// written at `D2`'s position 4).
#[test]
fn si14_f3_the_expected_documents_built_directly_under_each_revision() {
    let (reps, base) = direct(D2_DROP, false, true, |_| Vec::new());
    assert_reads(
        &reps,
        &base,
        &expected_drop(),
        "the running example under D2_DROP",
    );
    let (reps, base) = direct(D2_NOASSOC, true, false, |_| Vec::new());
    assert_reads(
        &reps,
        &base,
        &expected_noassoc(),
        "the running example under D2_NOASSOC",
    );
    let (reps, base) = direct(D2_LWW, true, true, |_| Vec::new());
    assert_reads(
        &reps,
        &base,
        &si14::si12::expected_d2(),
        "the running example under D2_LWW",
    );
    assert_eq!(project(&identified(&reps[0])), si14::si12::expected_d2());

    let (reps, base) = direct(D2, true, true, |uml| {
        vec![
            uml.rename_door("Doors"),
            uml.leaf(DOOR, "abstract", LeafOp::Enable),
            uml.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent"))),
        ]
    });
    let mut want = si14::si12::expected_d2();
    set(&mut want, "Door", "name", json!("Doors"));
    set(&mut want, "Door", "abstract", json!(true));
    add_to_set(&mut want, "Car", "stereotype", &["persistent"]);
    assert_reads(&reps, &base, &want, "RB's end under D2");

    let wheel_at = At::Owned(4, "Class");
    let (reps, base) = direct(D2_NOASSOC, true, false, |uml| {
        let mut ops = vec![uml.insert(At::Root, "ownedElements", 4, "Class")];
        ops.extend(uml.name(wheel_at, "Wheel"));
        ops.push(uml.leaf(wheel_at, "stereotype", LeafOp::Add(Scalar::text("w"))));
        ops
    });
    let mut want = expected_noassoc();
    push_owned(&mut want, wheel("Wheel", &["w"]));
    assert_reads(&reps, &base, &want, "RD's end under D2_NOASSOC");
    println!("SI14-F3: five documents built directly read as the oracle computes them");
}

/// F4: the tie RC registers. Under D2 and under D2_LWW, with members `a`,
/// `b`, `c` on every replica, `a` writes 70 events and then `b` writes
/// `Doors` and `c` writes `Doorx` into `Door`'s name, each at Lamport stamp
/// 71 and neither having seen the other. The fair register keeps `b`'s
/// (FairPolicy: 71 mod 3 = 2 leads the round, so in the sorted members
/// a, b, c `b` ranks 2 and `c` 0, and the greater rank wins); the
/// last-writer-wins register keeps `c`'s (equal stamps, the greater origin
/// id wins). Every replica, both delivery orders.
#[test]
fn si14_f4_an_equal_stamp_tie_at_71_under_the_fair_and_the_lww_register() {
    for (text, want) in [(D2, "Doors"), (D2_LWW, "Doorx")] {
        for b_first in [true, false] {
            let uml = Uml::new(text);
            let (a, b, c) = triplet_log::<ModelLog>();
            let mut reps = [a, b, c];
            let mut ops = vec![
                install(MODEL, text),
                uml.root(),
                uml.insert(At::Root, "ownedElements", 0, "Class"),
                uml.insert(At::Root, "ownedElements", 1, "Class"),
            ];
            ops.extend(uml.name(DOOR, "Door"));
            let mut pad = 0;
            while ops.len() < 70 {
                pad += 1;
                ops.push(uml.leaf(
                    At::Root,
                    "stereotype",
                    LeafOp::Add(Scalar::text(format!("p{pad:02}"))),
                ));
            }
            for op in ops {
                let message = reps[0].send(op).expect("premise: the base");
                reps[1].receive(message.clone());
                reps[2].receive(message);
            }
            let from_b = reps[1].send(uml.rename_door("Doors")).expect("b's write");
            let from_c = reps[2].send(uml.rename_door("Doorx")).expect("c's write");
            assert_eq!(from_b.event().lamport().val(), 71, "F4: b writes at 71");
            assert_eq!(from_c.event().lamport().val(), 71, "F4: c writes at 71");
            reps[1].receive(from_c.clone());
            reps[2].receive(from_b.clone());
            if b_first {
                reps[0].receive(from_b);
                reps[0].receive(from_c);
            } else {
                reps[0].receive(from_c);
                reps[0].receive(from_b);
            }
            for (index, replica) in reps.iter().enumerate() {
                assert_eq!(
                    si14::si12::door_name(&identified(replica)),
                    json!(want),
                    "F4: under {} replica {index} keeps the other write",
                    si14::si12::digest(text)
                );
            }
        }
        println!("SI14-F4: the tie at 71 keeps {want}");
    }
}
