//! EC.M1l-a (`cea-cdrt-knowledge/spec/items/EC.M1l-a.md`): `moirai-semantics` reads a keyed
//! shape whose key enum is written by name (`{"kind": "enum", "class": "<enum name>"}`, the name
//! exactly as the descriptor's `enums` object keys it, `::` path included) and resolves it to that
//! enum's slot in `enums`; an unknown name, or a key class that is not a string, refuses with an
//! error that names the feature; every descriptor fixture in this workspace reads as it did at
//! b27a4cf (`tests/fixtures/ecm1l_a/reference.tsv`).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 --offline -p moirai-semantics --test ecm1l_a_enum_key_by_name
//! ```

use moirai_semantics::{
    ClassSlot, KeyKind, LeafRule, MergeRule, MetamodelSemantics, SemanticsError, Shape,
    from_descriptor,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Sorted, `enums` is `Alpha` (0), `Color` (1), `sub::Shade` (2); the classes `Holder` and `Part`
/// are slots 0 and 1, so a name resolved among the classes, or a position, lands elsewhere.
fn descriptor(attribute_key: Value, containment_key: Value) -> Value {
    let prov = json!({"ordered": "declared", "unique": "declared",
                      "leaf": "houseDefault", "presence": "declared"});
    json!({
        "formatVersion": 2,
        "package": "ecm1la",
        "nsURI": "http://example.org/ecm1la",
        "rootClasses": ["Holder"],
        "enums": {"Alpha": ["A"], "Color": ["RED", "GREEN"], "sub::Shade": ["DARK"]},
        "classes": {
            "Holder": {
                "abstract": false, "superTypes": [], "references": [],
                "attributes": [{"name": "byColor",
                    "merge": {"kind": "attribute",
                              "shape": {"kind": "keyed", "key": attribute_key},
                              "leaf": {"kind": "text"}},
                    "provenance": prov}],
                "containments": [{"name": "parts",
                    "merge": {"kind": "containment",
                              "shape": {"kind": "keyed", "key": containment_key},
                              "target": "Part"},
                    "provenance": prov}]
            },
            "Part": {"abstract": false, "superTypes": [],
                     "attributes": [], "containments": [], "references": []}
        }
    })
}

fn by_name(name: &str) -> Value {
    json!({"kind": "enum", "class": name})
}

fn str_key() -> Value {
    json!({"kind": "str"})
}

fn rule<'a>(table: &'a MetamodelSemantics, feature: &str) -> &'a MergeRule {
    let holder = table.classes.iter().find(|c| &*c.name == "Holder").unwrap();
    &holder.declared.iter().find(|f| &*f.name == feature).unwrap().merge
}

const NAMES: [(&str, u16); 3] = [("Alpha", 0), ("Color", 1), ("sub::Shade", 2)];

#[test]
fn ecm1l_a_1_an_attribute_map_key_enum_written_by_name_resolves_to_that_enum() {
    for (name, slot) in NAMES {
        let table = from_descriptor(&descriptor(by_name(name), str_key()))
            .unwrap_or_else(|e| panic!("key enum `{name}` by name: refused, {e:?}"));
        assert_eq!(&*table.enums[slot as usize].name, name);
        assert_eq!(
            *rule(&table, "byColor"),
            MergeRule::Attribute {
                shape: Shape::Keyed {
                    key: KeyKind::Enum {
                        class: ClassSlot(slot)
                    }
                },
                leaf: LeafRule::Text,
            },
            "key enum `{name}`"
        );
    }
}

#[test]
fn ecm1l_a_2_a_containment_map_key_enum_written_by_name_resolves_to_that_enum() {
    for (name, slot) in NAMES {
        let table = from_descriptor(&descriptor(str_key(), by_name(name)))
            .unwrap_or_else(|e| panic!("key enum `{name}` by name: refused, {e:?}"));
        let part = table.classes.iter().find(|c| &*c.name == "Part").unwrap().slot;
        assert_eq!(
            *rule(&table, "parts"),
            MergeRule::Containment {
                shape: Shape::Keyed {
                    key: KeyKind::Enum {
                        class: ClassSlot(slot)
                    }
                },
                target: part,
            },
            "key enum `{name}`"
        );
    }
}

/// `Nope` is listed nowhere, `Holder` is a class and not an enum, `Shade` drops its `::` path.
#[test]
fn ecm1l_a_3_a_key_enum_name_the_descriptor_does_not_list_as_an_enum_refuses() {
    for name in ["Nope", "Holder", "Shade"] {
        for (attribute_key, containment_key, at) in [
            (by_name(name), str_key(), "Holder.byColor"),
            (str_key(), by_name(name), "Holder.parts"),
        ] {
            assert_eq!(
                from_descriptor(&descriptor(attribute_key, containment_key)),
                Err(SemanticsError::UnknownClass {
                    at: at.to_string(),
                    name: name.to_string(),
                }),
                "key enum `{name}` at `{at}`"
            );
        }
    }
}

/// An integer is the raw position Arachne wrote before EC.M1k; in range or not, it is not a name.
#[test]
fn ecm1l_a_4_a_key_enum_class_that_is_not_a_name_refuses_naming_the_feature() {
    for class in [json!(0), json!(1), json!(99)] {
        let key = json!({"kind": "enum", "class": class});
        for (attribute_key, containment_key, at) in [
            (key.clone(), str_key(), "Holder.byColor"),
            (str_key(), key.clone(), "Holder.parts"),
        ] {
            match from_descriptor(&descriptor(attribute_key, containment_key)) {
                Ok(_) => panic!("key class {class} at `{at}`: read, not refused"),
                Err(e) => assert!(
                    e.to_string().contains(at),
                    "key class {class}: the refusal does not name `{at}`: {e}"
                ),
            }
        }
    }
}

/// The table travels inside the log (decision D2, `moirai-interp/Cargo.toml` `serde`), so a table
/// with an enum key must come back from its own serde JSON equal.
#[test]
fn ecm1l_a_6_a_table_with_an_enum_key_round_trips_through_its_serde_form() {
    let table = from_descriptor(&descriptor(by_name("sub::Shade"), by_name("Color")))
        .unwrap_or_else(|e| panic!("key enums by name: refused, {e:?}"));
    let back: MetamodelSemantics =
        serde_json::from_str(&serde_json::to_string(&table).unwrap()).unwrap();
    assert_eq!(back, table);
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Every descriptor fixture of the workspace at b27a4cf, read now, equals its read at b27a4cf:
/// the table's serde JSON, or the refusal's `Debug`, by SHA-256, beside the input's SHA-256.
#[test]
fn ecm1l_a_5_every_descriptor_fixture_reads_as_at_b27a4cf() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
    let reference = include_str!("fixtures/ecm1l_a/reference.tsv");
    let mut now = Vec::new();
    for line in reference.lines() {
        let path = line.split('\t').next().unwrap();
        let bytes = std::fs::read(format!("{root}/{path}")).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let read = match from_descriptor(&value) {
            Ok(table) => serde_json::to_string(&table).unwrap(),
            Err(e) => format!("refused {e:?}"),
        };
        now.push(format!("{path}\t{}\t{}", sha(&bytes), sha(read.as_bytes())));
    }
    assert_eq!(reference.lines().count(), 31);
    assert_eq!(
        reference.lines().collect::<Vec<_>>(),
        now,
        "now:\n{}",
        now.join("\n")
    );
}
