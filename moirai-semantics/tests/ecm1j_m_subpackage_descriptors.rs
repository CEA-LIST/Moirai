//! EC.M1j-m (`cea-cdrt-knowledge/spec/items/EC.M1j-m.md`): `moirai-semantics` reads, unchanged,
//! the subpackage descriptors `arachne describe` writes since EC.M1j: classifiers keyed by
//! root-relative `::` names, a top-level `packages` map, roots, supertypes, targets and enum
//! leaves into subpackages. EC.M1j's outside gate (`arachne-cli/tests/ecm1j/moirai_gate.rs` on the
//! Arachne ecm1 lane) made committed.
//!
//! Fixtures (`tests/fixtures/ecm1j_m/`), byte copies of EC.M1j's assess run at Arachne 200f6fa:
//! `tree`, `classless_root`, `deptree` (equal to the EC.M1j goldens `*.metamodel.json`) and two
//! corpus descriptors (`corpus_c53`, `corpus_c67`), each beside `<name>.id`, the identity line
//! describe printed.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 --offline -p moirai-semantics --test ecm1j_m_subpackage_descriptors
//! ```

use moirai_semantics::{MergeRule, MetamodelSemantics, from_descriptor};
use serde_json::{Value, json};

macro_rules! fixture {
    ($name:literal) => {
        (
            $name,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/ecm1j_m/",
                $name,
                ".json"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/ecm1j_m/",
                $name,
                ".id"
            )),
        )
    };
}

const FIXTURES: [(&str, &str, &str); 5] = [
    fixture!("tree"),
    fixture!("classless_root"),
    fixture!("deptree"),
    fixture!("corpus_c53"),
    fixture!("corpus_c67"),
];

/// A parsed merge rule as JSON, with every slot replaced by the name it addresses, so that it
/// can be compared with the descriptor's own `merge` object, which spells names.
fn named(s: &MetamodelSemantics, rule: &MergeRule) -> Value {
    fn walk(s: &MetamodelSemantics, v: &mut Value) {
        if let Value::Object(map) = v {
            for (k, x) in map.iter_mut() {
                match (k.as_str(), x.as_u64()) {
                    ("target", Some(n)) => *x = json!(&*s.classes[n as usize].name),
                    ("class", Some(n)) => *x = json!(&*s.enums[n as usize].name),
                    _ => walk(s, x),
                }
            }
        }
    }
    let mut v = serde_json::to_value(rule).unwrap();
    walk(s, &mut v);
    v
}

/// Every difference between the parsed table and the descriptor, read back by name.
fn differences(name: &str, d: &Value, id: &Value) -> Vec<String> {
    let s = match from_descriptor(d) {
        Ok(s) => s,
        Err(e) => return vec![format!("{name}: refused, {e:?}")],
    };
    let mut out = Vec::new();
    let mut check = |what: String, got: Value, want: &Value| {
        if got != *want {
            out.push(format!("{name}: {what}: read {got}, descriptor {want}"));
        }
    };
    check(
        "identity".into(),
        json!({"digest": s.digest, "nsURI": &*s.ns_uri}),
        id,
    );
    let classes: Vec<&str> = s.classes.iter().map(|c| &*c.name).collect();
    let keys: Vec<&str> = d["classes"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    check("classes".into(), json!(classes), &json!(keys));
    let enums: serde_json::Map<String, Value> = s
        .enums
        .iter()
        .map(|e| (e.name.to_string(), json!(e.literals)))
        .collect();
    check("enums".into(), Value::Object(enums), &d["enums"]);
    let roots: Vec<&str> = s
        .roots
        .iter()
        .map(|r| &*s.classes[r.index()].name)
        .collect();
    check("rootClasses".into(), json!(roots), &d["rootClasses"]);
    for c in &s.classes {
        let body = &d["classes"][&*c.name];
        let supers: Vec<&str> = c
            .supers
            .iter()
            .map(|x| &*s.classes[x.index()].name)
            .collect();
        check(
            format!("{}.superTypes", c.name),
            json!(supers),
            &body["superTypes"],
        );
        let mut declared: Vec<&str> = c.declared.iter().map(|f| &*f.name).collect();
        let mut listed: Vec<&str> = ["attributes", "containments", "references"]
            .iter()
            .flat_map(|k| body[*k].as_array().unwrap())
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        declared.sort_unstable();
        listed.sort_unstable();
        check(
            format!("{} features", c.name),
            json!(declared),
            &json!(listed),
        );
        for f in &c.declared {
            let entry = ["attributes", "containments", "references"]
                .iter()
                .flat_map(|k| body[*k].as_array().unwrap())
                .find(|e| e["name"] == *f.name)
                .unwrap();
            check(
                format!("{}.{}.merge", c.name, f.name),
                named(&s, &f.merge),
                &entry["merge"],
            );
        }
    }
    out
}

#[test]
fn moirai_semantics_reads_the_subpackage_descriptors_by_name() {
    let mut failures = Vec::new();
    let (mut sub_roots, mut sub_supers, mut sub_targets, mut sub_enum_leaves) = (0, 0, 0, 0);
    for (name, text, id) in FIXTURES {
        let d: Value = serde_json::from_str(text).unwrap();
        assert!(
            d["packages"].is_object(),
            "{name}: set-up, the fixture carries a packages map"
        );
        failures.extend(differences(name, &d, &serde_json::from_str(id).unwrap()));
        let has = |v: &Value| v.as_str().is_some_and(|n| n.contains("::"));
        sub_roots += d["rootClasses"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| has(r))
            .count();
        for body in d["classes"].as_object().unwrap().values() {
            sub_supers += body["superTypes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|x| has(x))
                .count();
            for k in ["containments", "references"] {
                sub_targets += body[k]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|e| has(&e["merge"]["target"]))
                    .count();
            }
            sub_enum_leaves += body["attributes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| has(&e["merge"]["leaf"]["class"]))
                .count();
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    // Set-up: the fixtures exercise every subpackage reach the claim names.
    assert!(
        sub_roots > 0 && sub_supers > 0 && sub_targets > 0 && sub_enum_leaves > 0,
        "set-up: roots {sub_roots}, supertypes {sub_supers}, targets {sub_targets}, enum leaves {sub_enum_leaves}"
    );

    // The tree fixture by hand, as EC.M1j's gate pinned it.
    let s = from_descriptor(&serde_json::from_str::<Value>(FIXTURES[0].1).unwrap()).unwrap();
    let class = |n: &str| s.classes.iter().find(|c| &*c.name == n).unwrap();
    let name = |slot: moirai_semantics::ClassSlot| &*s.classes[slot.index()].name;
    let roots: Vec<&str> = s.roots.iter().map(|r| name(*r)).collect();
    assert_eq!(roots, ["Shop", "catalog::Inventory"]);
    assert_eq!(name(class("Special").supers[0]), "catalog::Item");
    assert_eq!(name(class("catalog::Item").supers[0]), "Named");
    let rule = |c: &str, f: &str| {
        named(
            &s,
            &class(c)
                .declared
                .iter()
                .find(|x| &*x.name == f)
                .unwrap()
                .merge,
        )
    };
    assert_eq!(rule("Shop", "items")["target"], "catalog::Item");
    assert_eq!(rule("Shop", "tags")["target"], "catalog::detail::Tag");
    assert_eq!(rule("catalog::Inventory", "stock")["target"], "Stock");
    assert_eq!(
        rule("catalog::Item", "color")["leaf"]["class"],
        "catalog::Color"
    );
    eprintln!(
        "ecm1j-m: {} descriptors read; subpackage roots {sub_roots}, supertypes {sub_supers}, targets {sub_targets}, enum leaves {sub_enum_leaves}",
        FIXTURES.len()
    );
}
