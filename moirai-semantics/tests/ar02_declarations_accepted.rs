//! AR.02 (`cea-cdrt-knowledge/spec/items/AR.02.md`), M1: `moirai-semantics` reads a descriptor
//! whose reference entries carry `eOpposite`, `subsets` and `redefines`, and the table it
//! derives is the table it derives from the same descriptor without them. Only the digest, which
//! is taken over the whole descriptor, may differ.
//!
//! Descriptors: SysML and SimpleUML as `arachne describe` writes them from AR.02 on
//! (`sysml.ar02.metamodel.json`, `uml.ar02.metamodel.json`: today's descriptor plus the
//! declarations of the `.ecore`, read with Python's XML parser, see the spec), and every other
//! descriptor fixture of this repository with declarations added by hand to every containment
//! and reference entry, so that no entry of any kind escapes the comparison.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-semantics --test ar02_declarations_accepted -- --nocapture
//! ```

use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

macro_rules! fixture {
    ($file:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../moirai-interp/tests/fixtures/",
            $file
        ))
    };
}

const KEYS: [&str; 3] = ["eOpposite", "subsets", "redefines"];

const SYSML_AR02: &str = fixture!("sysml.ar02.metamodel.json");
const UML_AR02: &str = fixture!("uml.ar02.metamodel.json");
/// `demo/sysml.json`, the SysML descriptor alice serves today.
const SYSML_DIGEST_TODAY: &str = "5168d5f66ef4811bb7e4e2258628dd2051d5fa451c8093479aa3b1304193692e";
const SYSML_DIGEST_AR02: &str = "ebd580696f97d9339c148f5b29db5d0ff7b0928d78fb05efc7c206811405e627";
const UML_DIGEST_TODAY: &str = "12d1a371fd995bce2c44e696e3714d59dae58fe04ff6450382a1e02b0a8a27dd";
const UML_DIGEST_AR02: &str = "21c50dea71938b861233b72116f5ead094fe1bbb3d67bf623864f52c3ead1c3f";

/// Every descriptor fixture that declares none of the three.
const PLAIN: [(&str, &str); 8] = [
    ("bt", fixture!("bt.metamodel.json")),
    ("json", fixture!("json.metamodel.json")),
    ("notation", fixture!("notation.metamodel.json")),
    ("uml", fixture!("uml.metamodel.json")),
    ("st01", fixture!("st01.metamodel.json")),
    ("st02", fixture!("st02.metamodel.json")),
    ("st04", fixture!("st04.metamodel.json")),
    ("st05", fixture!("st05.metamodel.json")),
];

fn parse(text: &str) -> Value {
    serde_json::from_str(text).expect("set-up: the fixture is JSON")
}

/// The descriptor with the three keys taken off every entry.
fn strip(descriptor: &Value) -> Value {
    let mut out = descriptor.clone();
    for (_, body) in out["classes"].as_object_mut().expect("classes").iter_mut() {
        for array in ["attributes", "containments", "references"] {
            if let Some(list) = body[array].as_array_mut() {
                for entry in list {
                    for key in KEYS {
                        entry.as_object_mut().expect("an entry").remove(key);
                    }
                }
            }
        }
    }
    out
}

/// The descriptor with declarations on every containment and reference entry: an opposite
/// named after the entry itself and one `subsets` and one `redefines` naming the entry's own
/// class and name, which is the shape Arachne writes, pointing somewhere that resolves.
fn declare_everywhere(descriptor: &Value) -> (Value, usize) {
    let mut out = descriptor.clone();
    let mut touched = 0;
    for (class, body) in out["classes"].as_object_mut().expect("classes").iter_mut() {
        for array in ["containments", "references"] {
            if let Some(list) = body[array].as_array_mut() {
                for entry in list {
                    let name = entry["name"].as_str().unwrap_or("").to_string();
                    entry["eOpposite"] = json!(name);
                    entry["subsets"] = json!([{"class": class, "feature": name}]);
                    entry["redefines"] = json!([{"class": class, "feature": name}]);
                    touched += 1;
                }
            }
        }
    }
    (out, touched)
}

/// The table with the digest taken out, the one field the three keys may move.
fn without_digest(table: &MetamodelSemantics) -> (MetamodelSemantics, Value) {
    let mut table = table.clone();
    table.digest = String::new();
    let mut value = serde_json::to_value(&table).expect("a table serializes");
    value
        .as_object_mut()
        .expect("a table object")
        .remove("digest");
    (table, value)
}

/// One comparison: `with` must parse, and parse to the table `without` parses to.
fn compare(name: &str, with: &Value, without: &Value, wrong: &mut Vec<String>) {
    let with_table = match from_descriptor(with) {
        Ok(table) => table,
        Err(why) => {
            wrong.push(format!("{name}: refused with the declarations: {why}"));
            return;
        }
    };
    let without_table = from_descriptor(without)
        .unwrap_or_else(|why| panic!("set-up: {name} without the declarations is refused: {why}"));
    let (with_eq, with_value) = without_digest(&with_table);
    let (without_eq, without_value) = without_digest(&without_table);
    let same = with_eq == without_eq && with_value == without_value;
    println!(
        "AR02-M1 {name}: classes {}, features {}, table equal without the digest {same}, \
         digest {} (without {})",
        with_table.classes.len(),
        with_table
            .classes
            .iter()
            .map(|class| class.declared.len())
            .sum::<usize>(),
        with_table.digest,
        without_table.digest
    );
    if !same {
        let differing: Vec<String> = with_table
            .classes
            .iter()
            .zip(&without_table.classes)
            .filter(|(left, right)| left != right)
            .take(5)
            .map(|(left, _)| left.name.to_string())
            .collect();
        wrong.push(format!(
            "{name}: the table differs with the declarations; first differing classes {differing:?}"
        ));
    }
    if with_table.digest != metamodel_digest(with) {
        wrong.push(format!(
            "{name}: the table's digest {} is not the digest of the descriptor it was read from {}",
            with_table.digest,
            metamodel_digest(with)
        ));
    }
}

/// SysML and SimpleUML as Arachne writes them from AR.02 on.
#[test]
fn ar02_m1_the_described_declarations_are_read_and_the_table_is_the_one_without_them() {
    let mut wrong = Vec::new();
    for (name, text, today, ar02) in [
        ("sysml", SYSML_AR02, SYSML_DIGEST_TODAY, SYSML_DIGEST_AR02),
        ("simpleuml", UML_AR02, UML_DIGEST_TODAY, UML_DIGEST_AR02),
    ] {
        let with = parse(text);
        let without = strip(&with);
        assert_eq!(
            (metamodel_digest(&with), metamodel_digest(&without)),
            (ar02.to_string(), today.to_string()),
            "set-up: the {name} fixture is not today's descriptor plus the declarations"
        );
        compare(name, &with, &without, &mut wrong);
    }
    assert!(
        wrong.is_empty(),
        "AR02 claim: the parser does not read the declarations as nothing:\n  {}",
        wrong.join("\n  ")
    );
}

/// Every other descriptor fixture, with declarations on every containment and reference.
#[test]
fn ar02_m1_declarations_on_every_entry_of_every_descriptor_change_no_table() {
    let mut wrong = Vec::new();
    let mut entries = 0;
    for (name, text) in PLAIN {
        let plain = parse(text);
        assert_eq!(
            strip(&plain),
            plain,
            "set-up: {name} already declares one of the three"
        );
        let (with, touched) = declare_everywhere(&plain);
        entries += touched;
        compare(name, &with, &plain, &mut wrong);
    }
    println!(
        "AR02-M1 descriptors {}, entries given declarations {entries}, differing {}",
        PLAIN.len(),
        wrong.len()
    );
    assert!(
        wrong.is_empty(),
        "AR02 claim: declarations on a reference entry changed what the parser derives:\n  {}",
        wrong.join("\n  ")
    );
}
