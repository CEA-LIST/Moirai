//! SI.12's premises: the four descriptors are the revisions the spec names,
//! and the running example reads as `expected_d1` under `D1` and, built
//! directly under `D2` by hand, as `expected_d2`. These hold before any
//! migration exists; they are what the claim tests in
//! `si12_copy_migration.rs` and `si12_copy_migration_l3.rs` compare against.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.12.md`.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si12_fixtures -- --test-threads=1
//! ```

mod si12;

use std::collections::{BTreeMap, BTreeSet};

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::MetamodelSemantics;
use serde_json::{Value, json};

use si12::*;

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

fn identified(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

fn plain(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

/// The digests the four fixtures hash to, as `arachne digest` prints them.
const DIGESTS: [(&str, &str); 4] = [
    (
        "D1",
        "5d34af887ace294f008c96403a4e6dd442f374d4e23639ec1620c04a4cf301f3",
    ),
    (
        "D2",
        "f02b1b1fc95a3b9139298ec70618786f4e5373e296c6ea6a8d6f1a44285afb4c",
    ),
    (
        "D2_RENAME",
        "2f84c0d089ff1b6454b5ece0fb1def4516eb8630a561f50c1bb7b05f5983c1e5",
    ),
    (
        "D2_REMOVE",
        "ea1b9b94692c960367c8696820745793dc349598402954a9b57b17f141951341",
    ),
];

/// Every feature of every class of a table, by `<Class>.<feature>` as the
/// class declares it, with its rule spelled with class names for slots.
fn features(sem: &MetamodelSemantics) -> BTreeMap<String, Value> {
    let name_of = |slot: moirai_semantics::ClassSlot| sem.classes[slot.index()].name.to_string();
    let mut out = BTreeMap::new();
    for class in &sem.classes {
        for feature in &class.declared {
            let mut rule = serde_json::to_value(feature.merge).expect("a rule serializes");
            if let Some(target) = rule.get_mut("target") {
                let slot = moirai_semantics::ClassSlot(target.as_u64().expect("a slot") as u16);
                *target = Value::String(name_of(slot));
            }
            out.insert(format!("{}.{}", class.name, feature.name), rule);
        }
    }
    out
}

fn classes(sem: &MetamodelSemantics) -> BTreeSet<String> {
    sem.classes
        .iter()
        .map(|class| class.name.to_string())
        .collect()
}

/// The change from `D1` to `revision`: classes added and removed, features
/// added and removed, and features whose rule changed.
fn change(revision: &str) -> Value {
    let (from, to) = (Uml::new(D1).sem, Uml::new(revision).sem);
    let (old, new) = (features(&from), features(&to));
    let added: Vec<&String> = new.keys().filter(|key| !old.contains_key(*key)).collect();
    let removed: Vec<&String> = old.keys().filter(|key| !new.contains_key(*key)).collect();
    let changed: Vec<Value> = old
        .iter()
        .filter_map(|(key, rule)| match new.get(key) {
            Some(other) if other != rule => {
                Some(json!({"feature": key, "from": rule, "to": other}))
            }
            _ => None,
        })
        .collect();
    let (old_classes, new_classes) = (classes(&from), classes(&to));
    json!({
        "classes_added": new_classes.difference(&old_classes).collect::<Vec<_>>(),
        "classes_removed": old_classes.difference(&new_classes).collect::<Vec<_>>(),
        "added": added,
        "removed": removed,
        "changed": changed,
        "roots_same": from.roots.iter().map(|slot| from.classes[slot.index()].name.to_string()).collect::<Vec<_>>()
            == to.roots.iter().map(|slot| to.classes[slot.index()].name.to_string()).collect::<Vec<_>>(),
        "ns_uri_same": from.ns_uri == to.ns_uri,
    })
}

/// F1. The four fixtures hash to their registered digests, and each
/// revision differs from `D1` by exactly what the spec says.
#[test]
fn si12_f1_the_revisions_are_what_the_spec_names() {
    for ((name, want), text) in DIGESTS.iter().zip([D1, D2, D2_RENAME, D2_REMOVE]) {
        assert_eq!(&digest(text), want, "{name}'s digest");
    }
    let text =
        json!({"kind": "attribute", "shape": {"kind": "optional"}, "leaf": {"kind": "text"}});
    let fair = json!({"kind": "attribute", "shape": {"kind": "optional"}, "leaf": {"kind": "register", "tie": "fair"}});
    assert_eq!(
        change(D2),
        json!({
            "classes_added": ["Comment"],
            "classes_removed": [],
            "added": ["Class.isLeaf", "Comment.body", "ModelElement.ownedComments"],
            "removed": [],
            "changed": [{"feature": "ModelElement.name", "from": text, "to": fair}],
            "roots_same": true,
            "ns_uri_same": true,
        }),
        "D2 against D1"
    );
    let d2 = Uml::new(D2);
    assert_eq!(
        serde_json::to_value(d2.rule("Class", "isLeaf")).unwrap(),
        json!({"kind": "attribute", "shape": {"kind": "single"}, "leaf": {"kind": "flag", "wins": "enable"}}),
        "D2's Class.isLeaf is a single flag"
    );
    assert_eq!(
        serde_json::to_value(d2.rule("Class", "ownedComments")).unwrap(),
        json!({"kind": "containment", "shape": {"kind": "sequence"}, "target": d2.class("Comment").0}),
        "D2's ModelElement.ownedComments is a sequence of Comment"
    );
    assert_eq!(
        change(D2_RENAME),
        json!({
            "classes_added": [], "classes_removed": [],
            "added": ["Class.isAbstract"], "removed": ["Class.abstract"], "changed": [],
            "roots_same": true, "ns_uri_same": true,
        }),
        "D2_RENAME against D1"
    );
    assert_eq!(
        change(D2_REMOVE),
        json!({
            "classes_added": [], "classes_removed": [],
            "added": [], "removed": ["EnumerationLiteral.name"], "changed": [],
            "roots_same": true, "ns_uri_same": true,
        }),
        "D2_REMOVE against D1"
    );
    // The slots the claim's mapping must not copy by position: the class
    // `Comment` and the features `isLeaf` and `ownedComments` sort before
    // features and classes the model writes.
    let d1 = Uml::new(D1);
    assert_ne!(
        d1.slot("Model", "ownedElements"),
        d2.slot("Model", "ownedElements")
    );
    assert_ne!(d1.slot("Class", "name"), d2.slot("Class", "name"));
    assert_ne!(d1.class("Property"), d2.class("Property"));
    println!(
        "SI12-F1: D2 = D1 + Comment, Class.isLeaf, ModelElement.ownedComments, name text -> fair register"
    );
}

/// Build the running example under `text` on three replicas, writing every
/// name by that table's rule.
fn built(text: &str) -> [Rep; 3] {
    let uml = Uml::new(text);
    let (mut a, mut b, mut c) = triplet_log::<ModelLog>();
    let opened = a.send(install("si12-f", text)).expect("an Install");
    b.receive(opened.clone());
    c.receive(opened);
    for op in uml.structure() {
        let message = a
            .send(op.clone())
            .unwrap_or_else(|why| panic!("{op:?}: {why}"));
        b.receive(message.clone());
        c.receive(message);
    }
    let ids = ids_by_label(&identified(&a));
    for op in uml.links(&ids) {
        let message = a
            .send(op.clone())
            .unwrap_or_else(|why| panic!("{op:?}: {why}"));
        b.receive(message.clone());
        c.receive(message);
    }
    [a, b, c]
}

fn check_reads(replicas: &[Rep; 3], expected: &Value, what: &str) {
    for (index, replica) in replicas.iter().enumerate() {
        let doc = identified(replica);
        assert_eq!(
            &project(&doc),
            expected,
            "{what}: replica {index} serves {doc}"
        );
        assert_eq!(
            project_with(&plain(replica), &labels(&doc)),
            strip_ids(expected),
            "{what}: replica {index}'s default read"
        );
        assert_eq!(replica.state().unresolved(), 0, "{what}: replica {index}");
    }
}

/// F2. The running example under `D1` reads `expected_d1` on three replicas.
#[test]
fn si12_f2_the_running_example_under_d1_reads_as_expected() {
    check_reads(
        &built(D1),
        &expected_d1(),
        "F2, the running example under D1",
    );
}

/// F3. The running example built directly under `D2`, every name one
/// register write, reads `expected_d2` on three replicas: the hand
/// expectation of the copy is what `D2` itself makes of the model.
#[test]
fn si12_f3_the_running_example_built_under_d2_reads_as_expected() {
    check_reads(
        &built(D2),
        &expected_d2(),
        "F3, the running example built under D2",
    );
}

/// F4 and F5. The rule change the copy must carry, shown on direct builds:
/// two concurrent renames of `Door`, to `Gate` on `a` and `Hatch` on `b`,
/// with `c` delivering them in either order. Under `D2` every replica reads
/// one of the two names; under `D1` (a text) every replica reads one string
/// holding the characters of both renames.
#[test]
fn si12_f4_f5_two_concurrent_renames_under_d2_and_under_d1() {
    for at_c in interleavings(&[1, 1]) {
        let mut d2 = built(D2);
        let uml2 = Uml::new(D2);
        let gate = d2[0].send(uml2.rename_door("Gate")).expect("a renames");
        let hatch = d2[1].send(uml2.rename_door("Hatch")).expect("b renames");
        d2[0].receive(hatch.clone());
        d2[1].receive(gate.clone());
        let streams = [gate, hatch];
        for stream in &at_c {
            d2[2].receive(streams[*stream].clone());
        }
        let names: Vec<Value> = d2
            .iter()
            .map(|replica| door_name(&identified(replica)))
            .collect();
        assert!(
            names.iter().all(|name| name == &names[0])
                && (names[0] == json!("Gate") || names[0] == json!("Hatch")),
            "F4 (c {at_c:?}): under D2 the renames read {names:?}"
        );

        // Under D1 a rename is the old characters out and the new ones in.
        let mut d1 = built(D1);
        let uml1 = Uml::new(D1);
        let rename = |uml: &Uml, to: &str| -> Vec<ModelOp> {
            let mut ops: Vec<ModelOp> = (0..4)
                .map(|_| {
                    uml.leaf(
                        DOOR,
                        "name",
                        moirai_interp::leaf::LeafOp::DeleteChar { pos: 0 },
                    )
                })
                .collect();
            ops.extend(uml.type_text(DOOR, "name", to));
            ops
        };
        let gate: Vec<_> = rename(&uml1, "Gate")
            .into_iter()
            .map(|op| d1[0].send(op).expect("a renames"))
            .collect();
        let hatch: Vec<_> = rename(&uml1, "Hatch")
            .into_iter()
            .map(|op| d1[1].send(op).expect("b renames"))
            .collect();
        hatch
            .iter()
            .for_each(|message| d1[0].receive(message.clone()));
        gate.iter()
            .for_each(|message| d1[1].receive(message.clone()));
        let streams = [gate, hatch];
        for stream in &at_c {
            streams[*stream]
                .iter()
                .for_each(|message| d1[2].receive(message.clone()));
        }
        let names: Vec<Value> = d1
            .iter()
            .map(|replica| door_name(&identified(replica)))
            .collect();
        let merged = names[0].as_str().unwrap_or_default().to_string();
        let mut chars: Vec<char> = merged.chars().collect();
        chars.sort();
        let mut both: Vec<char> = "GateHatch".chars().collect();
        both.sort();
        assert!(
            names.iter().all(|name| name == &names[0]) && chars == both,
            "F5 (c {at_c:?}): under D1 the renames read {names:?}"
        );
    }
    println!("SI12-F4/F5: D2 keeps one of two concurrent renames, D1 keeps the characters of both");
}
