//! Reading a format 2 descriptor into a table.
//!
//! Nothing here derives a rule. The descriptor carries the rule Arachne
//! derived; this module resolves names to slots, flattens inheritance once,
//! and refuses what decision D6 puts out of scope.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use smallvec::SmallVec;

use crate::digest::metamodel_digest;
use crate::table::{
    ClassSemantics, ClassSlot, EnumSemantics, FeatureSemantics, FeatureSlot, FlagWins, LeafRule,
    MergeRule, MetamodelSemantics, NumKind, Provenance, Shape, TieBreak, UnsupportedReason,
};

/// The only descriptor layout this crate reads.
const FORMAT_VERSION: u64 = 2;

/// Why a descriptor could not become a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticsError {
    /// The descriptor is not a JSON object at all.
    NotAnObject,
    /// The descriptor is of a format version this crate does not read.
    UnsupportedFormatVersion {
        /// What it declared, `None` when it declared nothing readable.
        found: Option<u64>,
    },
    /// A key is missing or holds the wrong kind of value.
    Malformed {
        /// Where, as `Class.feature` or a key path.
        at: String,
        /// What was wrong.
        reason: String,
    },
    /// A supertype, containment target, reference target, enum or root names
    /// a classifier the descriptor does not list.
    UnknownClass {
        /// Where the name was read.
        at: String,
        /// The name.
        name: String,
    },
    /// A feature carries no `merge` or no `provenance` object.
    MissingRule {
        /// Declaring class.
        class: String,
        /// Feature.
        feature: String,
        /// The absent key.
        key: &'static str,
    },
    /// A feature's rule is one the interpreted path has no node for, and
    /// decision D6 refuses the whole metamodel rather than half-running it.
    Unsupported {
        /// Declaring class.
        class: String,
        /// Feature.
        feature: String,
        /// Which form.
        reason: UnsupportedReason,
    },
    /// More classifiers than a `u16` slot can address.
    TooManyClassifiers {
        /// How many.
        count: usize,
    },
}

impl fmt::Display for SemanticsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SemanticsError::NotAnObject => write!(f, "the descriptor is not a JSON object"),
            SemanticsError::UnsupportedFormatVersion { found: Some(found) } => write!(
                f,
                "the descriptor declares `formatVersion` {found}, and this crate reads \
                 format version {FORMAT_VERSION} only"
            ),
            SemanticsError::UnsupportedFormatVersion { found: None } => write!(
                f,
                "the descriptor declares no readable `formatVersion`, and this crate reads \
                 format version {FORMAT_VERSION} only"
            ),
            SemanticsError::Malformed { at, reason } => write!(f, "`{at}`: {reason}"),
            SemanticsError::UnknownClass { at, name } => write!(
                f,
                "`{at}` names `{name}`, which the descriptor does not list"
            ),
            SemanticsError::MissingRule {
                class,
                feature,
                key,
            } => write!(
                f,
                "`{class}.{feature}`: no `{key}` object; format version {FORMAT_VERSION} carries \
                 the merge rule and its provenance on every feature, and this crate reads them \
                 rather than deriving them"
            ),
            SemanticsError::Unsupported {
                class,
                feature,
                reason,
            } => write!(
                f,
                "`{class}.{feature}` is `{}`, which the interpreted path does not support \
                 (decision D6); this metamodel stays on the generated path",
                reason.as_str()
            ),
            SemanticsError::TooManyClassifiers { count } => write!(
                f,
                "the descriptor lists {count} classifiers, more than the {} a slot addresses",
                u16::MAX
            ),
        }
    }
}

impl std::error::Error for SemanticsError {}

fn malformed(at: impl Into<String>, reason: impl Into<String>) -> SemanticsError {
    SemanticsError::Malformed {
        at: at.into(),
        reason: reason.into(),
    }
}

/// A `merge` object as the descriptor spells it, with targets still by name.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum RawMerge {
    Attribute { shape: Shape, leaf: RawLeaf },
    Containment { shape: Shape, target: String },
    Reference { many: bool, target: String },
    Unsupported { reason: UnsupportedReason },
}

/// A `leaf` object as the descriptor spells it.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum RawLeaf {
    Text,
    Counter {
        num: NumKind,
        resettable: bool,
    },
    Flag {
        wins: FlagWins,
    },
    Register {
        tie: TieBreak,
    },
    Enum {
        /// Absent when the entry names its enum only under the format 1
        /// `enum` key, which holds the same name.
        #[serde(default)]
        class: Option<String>,
        tie: TieBreak,
    },
}

/// One class as pass one leaves it: features parsed, inheritance not yet
/// flattened.
struct Declared<'a> {
    name: &'a str,
    instantiable: bool,
    supers: SmallVec<[ClassSlot; 2]>,
    features: Vec<FeatureSemantics>,
}

/// Reads a format 2 descriptor into a table.
///
/// Fails, naming the class and feature, on a descriptor of another format
/// version, a feature with no rule, a name that resolves to nothing, and on
/// the two forms decision D6 keeps on the generated path: a `keyed`
/// (`uw-map`) feature and a `transparent` one. `json.ecore` needs both, and
/// is refused here on purpose; `bt.ecore` and `SimpleUML.ecore` need neither.
pub fn from_descriptor(descriptor: &Value) -> Result<MetamodelSemantics, SemanticsError> {
    let root = descriptor.as_object().ok_or(SemanticsError::NotAnObject)?;

    let version = root.get("formatVersion").and_then(Value::as_u64);
    if version != Some(FORMAT_VERSION) {
        return Err(SemanticsError::UnsupportedFormatVersion { found: version });
    }

    let ns_uri = required_str(root, "nsURI")?;
    let package = required_str(root, "package")?;

    let declared = root
        .get("classes")
        .and_then(Value::as_object)
        .ok_or_else(|| malformed("descriptor", "no `classes` object"))?;
    let enum_entries: Map<String, Value> = match root.get("enums") {
        Some(Value::Object(entries)) => entries.clone(),
        None | Some(Value::Null) => Map::new(),
        Some(_) => return Err(malformed("descriptor", "`enums` is not an object")),
    };

    // Slots are dense positions by sorted name, sorted here rather than taken
    // from the map's own order so that a `serde_json` built with
    // `preserve_order` could not move them.
    let mut class_names: Vec<&str> = declared.keys().map(String::as_str).collect();
    class_names.sort_unstable();
    let mut enum_names: Vec<&str> = enum_entries.keys().map(String::as_str).collect();
    enum_names.sort_unstable();

    let count = class_names.len() + enum_names.len();
    if count > u16::MAX as usize {
        return Err(SemanticsError::TooManyClassifiers { count });
    }

    let mut class_slots: BTreeMap<&str, ClassSlot> = BTreeMap::new();
    for (index, name) in class_names.iter().enumerate() {
        class_slots.insert(name, ClassSlot(index as u16));
    }
    let mut enum_slots: BTreeMap<&str, ClassSlot> = BTreeMap::new();
    for (index, name) in enum_names.iter().enumerate() {
        if class_slots.contains_key(name) {
            return Err(malformed(
                *name,
                "named both as a class and as an enum, so a slot cannot address it",
            ));
        }
        enum_slots.insert(name, ClassSlot(index as u16));
    }

    let mut enums = Vec::with_capacity(enum_names.len());
    for (index, name) in enum_names.iter().enumerate() {
        let literals = enum_entries[*name]
            .as_array()
            .ok_or_else(|| malformed(*name, "the enum's literals are not an array"))?
            .iter()
            .map(|literal| {
                literal
                    .as_str()
                    .map(Arc::from)
                    .ok_or_else(|| malformed(*name, "an enum literal is not a string"))
            })
            .collect::<Result<Vec<Arc<str>>, _>>()?;
        enums.push(EnumSemantics {
            slot: ClassSlot(index as u16),
            name: Arc::from(*name),
            literals,
        });
    }

    // Pass one: every class's own declarations, with every name resolved.
    let mut classes: Vec<Declared> = Vec::with_capacity(class_names.len());
    for name in &class_names {
        let raw = declared[*name]
            .as_object()
            .ok_or_else(|| malformed(*name, "the class is not an object"))?;
        let instantiable = !raw
            .get("abstract")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let mut supers = SmallVec::new();
        for value in array(raw, "superTypes", name)? {
            let super_name = value
                .as_str()
                .ok_or_else(|| malformed(*name, "a supertype is not a string"))?;
            let slot =
                *class_slots
                    .get(super_name)
                    .ok_or_else(|| SemanticsError::UnknownClass {
                        at: format!("{name}.superTypes"),
                        name: super_name.to_string(),
                    })?;
            supers.push(slot);
        }

        // Attributes, containments and references are one numbering: a class
        // declares each name once, whichever array carries it.
        let mut entries: Vec<(&str, &Value)> = Vec::new();
        for key in ["attributes", "containments", "references"] {
            for entry in array(raw, key, name)? {
                let feature_name = entry
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| malformed(*name, format!("a `{key}` entry has no `name`")))?;
                entries.push((feature_name, entry));
            }
        }
        entries.sort_by(|left, right| left.0.cmp(right.0));
        if entries.len() > u16::MAX as usize {
            return Err(malformed(*name, "more features than a slot addresses"));
        }

        let mut features = Vec::with_capacity(entries.len());
        for (index, (feature_name, entry)) in entries.iter().enumerate() {
            if index > 0 && entries[index - 1].0 == *feature_name {
                return Err(malformed(
                    format!("{name}.{feature_name}"),
                    "declared twice, so its slot is ambiguous",
                ));
            }
            let merge = feature_rule(name, feature_name, entry, &class_slots, &enum_slots)?;
            let provenance = feature_provenance(name, feature_name, entry)?;
            features.push(FeatureSemantics {
                slot: FeatureSlot(index as u16),
                name: Arc::from(*feature_name),
                merge,
                provenance,
            });
        }

        classes.push(Declared {
            name,
            instantiable,
            supers,
            features,
        });
    }

    // Pass two: the supertype closure of every class, itself included. Slots
    // ascend by name, so a `BTreeSet` of them iterates in the same order
    // `conformance.rs`'s closure of names does. A cycle terminates on the
    // insert guard rather than looping.
    let mut closures: Vec<BTreeSet<ClassSlot>> = Vec::with_capacity(classes.len());
    for index in 0..classes.len() {
        let mut closure = BTreeSet::new();
        let mut pending = vec![ClassSlot(index as u16)];
        while let Some(current) = pending.pop() {
            if !closure.insert(current) {
                continue;
            }
            pending.extend(classes[current.index()].supers.iter().copied());
        }
        closures.push(closure);
    }

    // The concrete descendants of each class, self included when it is
    // instantiable: `conformance.rs`'s `family`, by slot. Walking the classes
    // in slot order leaves every list ascending.
    let mut concrete: Vec<Vec<ClassSlot>> = vec![Vec::new(); classes.len()];
    for (index, class) in classes.iter().enumerate() {
        if !class.instantiable {
            continue;
        }
        for member in &closures[index] {
            concrete[member.index()].push(ClassSlot(index as u16));
        }
    }

    let mut table_classes = Vec::with_capacity(classes.len());
    for (index, class) in classes.iter().enumerate() {
        // Supertypes first, so a class's own declaration wins a name, exactly
        // as `conformance.rs` fills its flattened feature table.
        let mut visible: BTreeMap<Arc<str>, (ClassSlot, FeatureSlot)> = BTreeMap::new();
        let own = ClassSlot(index as u16);
        for owner in closures[index]
            .iter()
            .copied()
            .filter(|slot| *slot != own)
            .chain(std::iter::once(own))
        {
            for feature in &classes[owner.index()].features {
                visible.insert(Arc::clone(&feature.name), (owner, feature.slot));
            }
        }

        table_classes.push(ClassSemantics {
            slot: own,
            name: Arc::from(class.name),
            instantiable: class.instantiable,
            supers: class.supers.clone(),
            declared: class.features.clone(),
            visible: visible
                .into_iter()
                .map(|(name, (owner, slot))| (name, owner, slot))
                .collect(),
            concrete: Arc::from(concrete[index].as_slice()),
        });
    }

    // The roots, as declared; every instantiable class when none is, which is
    // what the phase 4 `Schema` does with an empty `rootClasses`.
    let named_roots = match root.get("rootClasses") {
        Some(Value::Array(names)) => names.as_slice(),
        None | Some(Value::Null) => &[][..],
        Some(_) => return Err(malformed("rootClasses", "not an array")),
    };
    let roots: Vec<ClassSlot> = if named_roots.is_empty() {
        table_classes
            .iter()
            .filter(|class| class.instantiable)
            .map(|class| class.slot)
            .collect()
    } else {
        let mut roots = Vec::with_capacity(named_roots.len());
        for value in named_roots {
            let name = value
                .as_str()
                .ok_or_else(|| malformed("rootClasses", "a root class is not a string"))?;
            roots.push(
                *class_slots
                    .get(name)
                    .ok_or_else(|| SemanticsError::UnknownClass {
                        at: "rootClasses".to_string(),
                        name: name.to_string(),
                    })?,
            );
        }
        roots.sort_unstable();
        roots.dedup();
        roots
    };

    // The invariant the whole delivery path rests on: a slot is a position.
    for (index, class) in table_classes.iter().enumerate() {
        assert_eq!(
            class.slot.index(),
            index,
            "class `{}` sits at {index} and calls itself {:?}",
            class.name,
            class.slot
        );
        for (position, feature) in class.declared.iter().enumerate() {
            assert_eq!(
                feature.slot.index(),
                position,
                "`{}.{}` sits at {position} and calls itself {:?}",
                class.name,
                feature.name,
                feature.slot
            );
        }
    }
    for (index, entry) in enums.iter().enumerate() {
        assert_eq!(
            entry.slot.index(),
            index,
            "enum `{}` sits at {index} and calls itself {:?}",
            entry.name,
            entry.slot
        );
    }

    Ok(MetamodelSemantics {
        ns_uri: Arc::from(ns_uri),
        package: Arc::from(package),
        digest: metamodel_digest(descriptor),
        classes: table_classes,
        enums,
        roots,
    })
}

fn required_str<'a>(
    root: &'a Map<String, Value>,
    key: &'static str,
) -> Result<&'a str, SemanticsError> {
    root.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("descriptor", format!("no `{key}` string")))
}

fn array<'a>(
    class: &'a Map<String, Value>,
    key: &'static str,
    name: &str,
) -> Result<&'a [Value], SemanticsError> {
    match class.get(key) {
        Some(Value::Array(entries)) => Ok(entries.as_slice()),
        None | Some(Value::Null) => Ok(&[]),
        Some(_) => Err(malformed(name, format!("`{key}` is not an array"))),
    }
}

/// One feature's rule, with every name resolved to a slot and decision D6
/// applied.
fn feature_rule(
    class: &str,
    feature: &str,
    entry: &Value,
    class_slots: &BTreeMap<&str, ClassSlot>,
    enum_slots: &BTreeMap<&str, ClassSlot>,
) -> Result<MergeRule, SemanticsError> {
    let raw = entry
        .get("merge")
        .ok_or_else(|| SemanticsError::MissingRule {
            class: class.to_string(),
            feature: feature.to_string(),
            key: "merge",
        })?;
    let raw = RawMerge::deserialize(raw).map_err(|err| {
        malformed(
            format!("{class}.{feature}"),
            format!("`merge` is not a rule this crate can read: {err}"),
        )
    })?;

    let at = || format!("{class}.{feature}");
    let class_slot = |name: &str| -> Result<ClassSlot, SemanticsError> {
        class_slots
            .get(name)
            .copied()
            .ok_or_else(|| SemanticsError::UnknownClass {
                at: at(),
                name: name.to_string(),
            })
    };

    match raw {
        RawMerge::Attribute { shape, leaf } => {
            let leaf = match leaf {
                RawLeaf::Text => LeafRule::Text,
                RawLeaf::Counter { num, resettable } => LeafRule::Counter { num, resettable },
                RawLeaf::Flag { wins } => LeafRule::Flag { wins },
                RawLeaf::Register { tie } => LeafRule::Register { tie },
                RawLeaf::Enum { class: named, tie } => {
                    let named = match named
                        .as_deref()
                        .or_else(|| entry.get("enum").and_then(Value::as_str))
                    {
                        Some(named) => named,
                        None => {
                            return Err(malformed(
                                at(),
                                "an `enum` leaf names no enum class, under `merge.leaf.class` \
                                 or under the entry's `enum`",
                            ));
                        }
                    };
                    LeafRule::Enum {
                        class: enum_slots.get(named).copied().ok_or_else(|| {
                            SemanticsError::UnknownClass {
                                at: at(),
                                name: named.to_string(),
                            }
                        })?,
                        tie,
                    }
                }
            };
            Ok(MergeRule::Attribute { shape, leaf })
        }
        RawMerge::Containment { shape, target } => Ok(MergeRule::Containment {
            shape,
            target: class_slot(&target)?,
        }),
        RawMerge::Reference { many, target } => Ok(MergeRule::Reference {
            many,
            target: class_slot(&target)?,
        }),
        RawMerge::Unsupported { reason } if reason.refused_at_parse() => {
            Err(SemanticsError::Unsupported {
                class: class.to_string(),
                feature: feature.to_string(),
                reason,
            })
        }
        RawMerge::Unsupported { reason } => Ok(MergeRule::Unsupported { reason }),
    }
}

/// One feature's provenance. Required, because a facet with no source is
/// what criterion I-A4 is falsified by.
fn feature_provenance(
    class: &str,
    feature: &str,
    entry: &Value,
) -> Result<Provenance, SemanticsError> {
    let raw = entry
        .get("provenance")
        .ok_or_else(|| SemanticsError::MissingRule {
            class: class.to_string(),
            feature: feature.to_string(),
            key: "provenance",
        })?;
    Provenance::deserialize(raw).map_err(|err| {
        malformed(
            format!("{class}.{feature}"),
            format!("`provenance` does not name a source for every facet: {err}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{SemanticsError, from_descriptor};
    use crate::table::{
        ClassSlot, FacetSource, FeatureSlot, FlagWins, LeafRule, MergeRule, MetamodelSemantics,
        NumKind, SetTie, Shape, TieBreak, UnsupportedReason,
    };

    /// A provenance object whose four facets all carry a source. The values
    /// are not what makes any test here pass; that every facet has one is.
    fn provenance(ordered: &str, unique: &str, leaf: &str, presence: &str) -> Value {
        json!({"ordered": ordered, "unique": unique, "leaf": leaf, "presence": presence})
    }

    /// A hand-written format 2 descriptor: three levels of inheritance under
    /// `Node`, two enums, an abstract class with two concrete descendants, a
    /// containment, a plain reference and one feature of every shape the
    /// vocabulary has.
    ///
    /// Hand-written because step 2 has not emitted a real one yet. The real
    /// `bt.ecore` table asserted slot by slot against spec 11 §7 is test
    /// `ip4`, and it lands with the emitter.
    fn fixture() -> Value {
        json!({
            "formatVersion": 2,
            "package": "behaviortree",
            "nsURI": "http://www.example.org/behaviortree",
            "rootClasses": ["Root"],
            "classes": {
                "Node": {
                    "abstract": true,
                    "superTypes": [],
                    "attributes": [{
                        "name": "ID", "kind": "string", "many": false, "required": true,
                        "isId": true, "ordered": null, "unique": null, "annotation": null,
                        "merge": {"kind": "attribute", "shape": {"kind": "single"},
                                  "leaf": {"kind": "text"}},
                        "provenance": provenance(
                            "notApplicable", "notApplicable", "houseDefault", "declared"),
                    }],
                    "containments": [{
                        "name": "children", "target": "Node", "many": true, "required": false,
                        "ordered": null, "unique": null, "annotation": null,
                        "merge": {"kind": "containment", "shape": {"kind": "sequence"},
                                  "target": "Node"},
                        "provenance": provenance(
                            "houseDefault", "notApplicable", "notApplicable", "ecoreDefault"),
                    }],
                    "references": [],
                },
                "Decorator": {
                    "abstract": true,
                    "superTypes": ["Node"],
                    "attributes": [{
                        "name": "label", "kind": "string", "many": false, "required": false,
                        "isId": false, "ordered": null, "unique": null, "annotation": null,
                        "merge": {"kind": "attribute", "shape": {"kind": "optional"},
                                  "leaf": {"kind": "text"}},
                        "provenance": provenance(
                            "notApplicable", "notApplicable", "houseDefault", "declared"),
                    }],
                    "containments": [],
                    "references": [],
                },
                "Inverter": {
                    "abstract": false,
                    "superTypes": ["Decorator"],
                    "attributes": [
                        {
                            "name": "count", "kind": "int", "many": false, "required": true,
                            "isId": false, "ordered": null, "unique": null, "annotation": null,
                            "merge": {"kind": "attribute", "shape": {"kind": "single"},
                                      "leaf": {"kind": "counter", "num": "i32",
                                               "resettable": true}},
                            "provenance": provenance(
                                "notApplicable", "notApplicable", "houseDefault", "declared"),
                        },
                        {
                            "name": "tags", "kind": "string", "many": true, "required": false,
                            "isId": false, "ordered": false, "unique": true, "annotation": null,
                            "merge": {"kind": "attribute", "shape": {"kind": "set", "tie": "aw"},
                                      "leaf": {"kind": "text"}},
                            "provenance": provenance(
                                "declared", "declared", "houseDefault", "declared"),
                        },
                        {
                            "name": "scores", "kind": "float", "many": true, "required": false,
                            "isId": false, "ordered": false, "unique": false, "annotation": null,
                            "merge": {"kind": "attribute", "shape": {"kind": "bag"},
                                      "leaf": {"kind": "counter", "num": "f64",
                                               "resettable": false}},
                            "provenance": provenance(
                                "declared", "declared", "annotation", "declared"),
                        },
                        {
                            "name": "aliases", "kind": "string", "many": true, "required": false,
                            "isId": false, "ordered": true, "unique": true, "annotation": null,
                            "merge": {"kind": "attribute", "shape": {"kind": "orderedSet"},
                                      "leaf": {"kind": "text"}},
                            "provenance": provenance(
                                "declared", "declared", "houseDefault", "declared"),
                        },
                    ],
                    "containments": [],
                    "references": [],
                },
                "Repeater": {
                    "abstract": false,
                    "superTypes": ["Decorator"],
                    "attributes": [
                        {
                            "name": "status", "kind": "enum", "enum": "Status", "many": false,
                            "required": false, "isId": false, "ordered": null, "unique": null,
                            "annotation": null,
                            "merge": {"kind": "attribute", "shape": {"kind": "single"},
                                      "leaf": {"kind": "enum", "class": "Status", "tie": "mv"}},
                            "provenance": provenance(
                                "notApplicable", "notApplicable", "houseDefault", "ecoreDefault"),
                        },
                        {
                            "name": "computed", "kind": "int", "many": false, "required": false,
                            "isId": false, "ordered": null, "unique": null, "annotation": null,
                            "merge": {"kind": "unsupported", "reason": "derived"},
                            "provenance": provenance(
                                "notApplicable", "notApplicable", "notApplicable",
                                "notApplicable"),
                        },
                    ],
                    "containments": [],
                    "references": [],
                },
                "Root": {
                    "abstract": false,
                    "superTypes": [],
                    "attributes": [{
                        "name": "active", "kind": "bool", "many": false, "required": true,
                        "isId": false, "ordered": null, "unique": null, "annotation": null,
                        "merge": {"kind": "attribute", "shape": {"kind": "single"},
                                  "leaf": {"kind": "flag", "wins": "enable"}},
                        "provenance": provenance(
                            "notApplicable", "notApplicable", "houseDefault", "declared"),
                    }],
                    "containments": [{
                        "name": "child", "target": "Node", "many": false, "required": false,
                        "ordered": null, "unique": null, "annotation": null,
                        "merge": {"kind": "containment", "shape": {"kind": "optional"},
                                  "target": "Node"},
                        "provenance": provenance(
                            "notApplicable", "notApplicable", "notApplicable", "declared"),
                    }],
                    "references": [{
                        "name": "entry", "target": "Node", "many": false, "required": false,
                        "ordered": null, "unique": null, "annotation": null,
                        "merge": {"kind": "reference", "many": false, "target": "Node"},
                        "provenance": provenance(
                            "notApplicable", "notApplicable", "notApplicable", "declared"),
                    }],
                },
            },
            "enums": {
                "Status": ["RUNNING", "SUCCESS", "FAILURE"],
                "Colour": ["RED", "BLUE"],
            },
        })
    }

    /// Slots of the fixture, by the sorted-name rule.
    const DECORATOR: ClassSlot = ClassSlot(0);
    const INVERTER: ClassSlot = ClassSlot(1);
    const NODE: ClassSlot = ClassSlot(2);
    const REPEATER: ClassSlot = ClassSlot(3);
    const ROOT: ClassSlot = ClassSlot(4);
    const COLOUR: ClassSlot = ClassSlot(0);
    const STATUS: ClassSlot = ClassSlot(1);

    fn table() -> MetamodelSemantics {
        from_descriptor(&fixture()).expect("the fixture is a readable format 2 descriptor")
    }

    /// Replaces one feature's `merge` object, so a test can put a rule the
    /// parser must refuse where an ordinary one was.
    fn with_merge(class: &str, feature: &str, merge: Value) -> Value {
        let mut descriptor = fixture();
        for key in ["attributes", "containments", "references"] {
            let entries = descriptor["classes"][class][key].as_array_mut().unwrap();
            for entry in entries.iter_mut() {
                if entry["name"] == json!(feature) {
                    entry["merge"] = merge;
                    return descriptor;
                }
            }
        }
        panic!("the fixture has no `{class}.{feature}`");
    }

    /// The table serializes and comes back the same, which is what decision
    /// D2 rests on: the table travels inside `ModelOp::Install` and through
    /// state transfer, so a replica that joins late reads the same policy.
    #[test]
    fn the_table_survives_a_serde_round_trip() {
        let table = table();
        let text = serde_json::to_string(&table).expect("the table serializes");
        let back: MetamodelSemantics = serde_json::from_str(&text).expect("and comes back");
        assert_eq!(back, table);
    }

    /// The invariant `rule` rests on, checked from outside the parser as well
    /// as asserted inside it. This is `ip4` in fixture form; `ip4` proper
    /// runs the same assertions over `bt.ecore` and lands with step 2.
    #[test]
    fn ip4_position_is_the_slot_for_classes_enums_and_features() {
        let table = table();

        let class_names: Vec<&str> = table.classes.iter().map(|c| &*c.name).collect();
        assert_eq!(
            class_names,
            ["Decorator", "Inverter", "Node", "Repeater", "Root"],
            "classes are numbered by sorted name"
        );
        let enum_names: Vec<&str> = table.enums.iter().map(|e| &*e.name).collect();
        assert_eq!(enum_names, ["Colour", "Status"]);

        for (index, class) in table.classes.iter().enumerate() {
            assert_eq!(class.slot, ClassSlot(index as u16));
            for (position, feature) in class.declared.iter().enumerate() {
                assert_eq!(feature.slot, FeatureSlot(position as u16));
            }
        }
        for (index, entry) in table.enums.iter().enumerate() {
            assert_eq!(entry.slot, ClassSlot(index as u16));
        }

        // Features are numbered by sorted name across all three arrays at
        // once, and `ID` sorts before the lowercase names.
        let node = &table.classes[NODE.index()];
        let names: Vec<&str> = node.declared.iter().map(|f| &*f.name).collect();
        assert_eq!(names, ["ID", "children"]);
        let root = &table.classes[ROOT.index()];
        let names: Vec<&str> = root.declared.iter().map(|f| &*f.name).collect();
        assert_eq!(names, ["active", "child", "entry"]);

        assert_eq!(table.roots, vec![ROOT]);
        assert_eq!(table.digest, crate::metamodel_digest(&fixture()));
    }

    /// An enum's slot addresses `enums`, not `classes`. `Status` is enum slot
    /// 1 and there is a wholly unrelated class at class slot 1.
    #[test]
    fn an_enum_leaf_addresses_the_enums_vector() {
        let table = table();
        let MergeRule::Attribute { leaf, .. } = table
            .rule(REPEATER, FeatureSlot(1))
            .copied()
            .expect("`Repeater.status`")
        else {
            panic!("`Repeater.status` is an attribute");
        };
        assert_eq!(
            leaf,
            LeafRule::Enum {
                class: STATUS,
                tie: TieBreak::MultiValue
            }
        );
        assert_eq!(&*table.enums[STATUS.index()].name, "Status");
        assert_eq!(&*table.classes[STATUS.index()].name, "Inverter");
        assert_eq!(&*table.enums[COLOUR.index()].literals[0], "RED");
    }

    /// Three levels: `Inverter` inherits `label` from `Decorator` and `ID`
    /// and `children` from `Node`, each once, each naming the class that
    /// declares it. Flattened here, so no lookup walks a supertype.
    #[test]
    fn visible_holds_each_inherited_feature_once_with_its_declaring_class() {
        let table = table();
        let visible: Vec<(&str, ClassSlot, FeatureSlot)> = table.classes[INVERTER.index()]
            .visible
            .iter()
            .map(|(name, class, feature)| (&**name, *class, *feature))
            .collect();

        assert_eq!(
            visible,
            vec![
                ("ID", NODE, FeatureSlot(0)),
                ("aliases", INVERTER, FeatureSlot(0)),
                ("children", NODE, FeatureSlot(1)),
                ("count", INVERTER, FeatureSlot(1)),
                ("label", DECORATOR, FeatureSlot(0)),
                ("scores", INVERTER, FeatureSlot(2)),
                ("tags", INVERTER, FeatureSlot(3)),
            ]
        );

        // Every visible entry resolves to a rule through its declaring class.
        for (name, class, feature) in &visible {
            assert!(
                table.rule(*class, *feature).is_some(),
                "`{name}` resolves to no rule"
            );
        }
        assert_eq!(
            table.classes[NODE.index()].visible.len(),
            2,
            "a root of the hierarchy sees only its own"
        );
    }

    /// The concrete closure of an abstract class is its instantiable
    /// descendants; a concrete class includes itself.
    #[test]
    fn concrete_is_the_instantiable_descendant_closure() {
        let table = table();
        assert!(!table.classes[NODE.index()].instantiable);
        assert_eq!(&*table.classes[NODE.index()].concrete, [INVERTER, REPEATER]);
        assert_eq!(
            &*table.classes[DECORATOR.index()].concrete,
            [INVERTER, REPEATER]
        );
        assert_eq!(&*table.classes[INVERTER.index()].concrete, [INVERTER]);
        assert_eq!(&*table.classes[ROOT.index()].concrete, [ROOT]);
    }

    /// The delivery path: two bounds-checked indexes, the right variant, and
    /// `None` rather than a panic when either is out of range.
    #[test]
    fn rule_returns_the_declared_variant_and_none_out_of_bounds() {
        let table = table();

        assert_eq!(
            table.rule(NODE, FeatureSlot(0)).copied(),
            Some(MergeRule::Attribute {
                shape: Shape::Single,
                leaf: LeafRule::Text
            })
        );
        assert_eq!(
            table.rule(NODE, FeatureSlot(1)).copied(),
            Some(MergeRule::Containment {
                shape: Shape::Sequence,
                target: NODE
            })
        );
        assert_eq!(
            table.rule(ROOT, FeatureSlot(0)).copied(),
            Some(MergeRule::Attribute {
                shape: Shape::Single,
                leaf: LeafRule::Flag {
                    wins: FlagWins::Enable
                }
            })
        );
        assert_eq!(
            table.rule(ROOT, FeatureSlot(2)).copied(),
            Some(MergeRule::Reference {
                many: false,
                target: NODE
            })
        );
        assert_eq!(
            table.rule(INVERTER, FeatureSlot(3)).copied(),
            Some(MergeRule::Attribute {
                shape: Shape::Set {
                    tie: SetTie::AddWins
                },
                leaf: LeafRule::Text
            })
        );
        assert_eq!(
            table.rule(INVERTER, FeatureSlot(1)).copied(),
            Some(MergeRule::Attribute {
                shape: Shape::Single,
                leaf: LeafRule::Counter {
                    num: NumKind::I32,
                    resettable: true
                }
            })
        );
        assert_eq!(
            table.rule(INVERTER, FeatureSlot(2)).copied(),
            Some(MergeRule::Attribute {
                shape: Shape::Bag,
                leaf: LeafRule::Counter {
                    num: NumKind::F64,
                    resettable: false
                }
            })
        );

        assert_eq!(
            table.rule(ROOT, FeatureSlot(3)),
            None,
            "past the last feature"
        );
        assert_eq!(
            table.rule(ClassSlot(99), FeatureSlot(0)),
            None,
            "past the last class"
        );
        assert_eq!(table.rule(ClassSlot(u16::MAX), FeatureSlot(u16::MAX)), None);
    }

    /// A declared `unique` and `ordered` collection is recorded as it was
    /// declared and merged as a sequence, because that is what the generator
    /// compiles (`attribute.rs:183-198`).
    #[test]
    fn an_ordered_set_is_recorded_and_degrades_to_a_sequence() {
        let table = table();
        let MergeRule::Attribute { shape, .. } = table
            .rule(INVERTER, FeatureSlot(0))
            .copied()
            .expect("`Inverter.aliases`")
        else {
            panic!("`Inverter.aliases` is an attribute");
        };
        assert_eq!(shape, Shape::OrderedSet, "the declaration is not lost");
        assert_eq!(shape.effective(), Shape::Sequence, "and is not merged by");
    }

    /// `derived`, `transient` and `volatile` are recorded and not merged;
    /// only `keyed` and `transparent` cost the whole metamodel.
    #[test]
    fn a_derived_feature_is_recorded_rather_than_refused() {
        let table = table();
        assert_eq!(
            table.rule(REPEATER, FeatureSlot(0)).copied(),
            Some(MergeRule::Unsupported {
                reason: UnsupportedReason::Derived
            })
        );
    }

    /// Every facet of every feature carries a source, which is what criterion
    /// I-A4 asks of the descriptor and what this parser enforces on it.
    #[test]
    fn provenance_is_read_and_not_derived() {
        let table = table();
        let tags = &table.classes[INVERTER.index()].declared[3];
        assert_eq!(&*tags.name, "tags");
        assert_eq!(tags.provenance.ordered, FacetSource::Declared);
        assert_eq!(tags.provenance.unique, FacetSource::Declared);
        assert_eq!(tags.provenance.leaf, FacetSource::HouseDefault);
        assert_eq!(tags.provenance.presence, FacetSource::Declared);

        let children = &table.classes[NODE.index()].declared[1];
        assert_eq!(&*children.name, "children");
        assert_eq!(
            children.provenance.ordered,
            FacetSource::HouseDefault,
            "a silent `ordered` on a multi-valued containment is Arachne's own default"
        );

        let scores = &table.classes[INVERTER.index()].declared[2];
        assert_eq!(scores.provenance.leaf, FacetSource::Annotation);
    }

    /// A format the crate does not read is refused as that, by name, and not
    /// as a hundred confusing missing keys.
    #[test]
    fn a_format_version_1_descriptor_is_refused_by_name() {
        let mut descriptor = fixture();
        descriptor["formatVersion"] = json!(1);
        let error = from_descriptor(&descriptor).expect_err("format 1 carries no rules");
        assert_eq!(
            error,
            SemanticsError::UnsupportedFormatVersion { found: Some(1) }
        );
        let sentence = error.to_string();
        assert!(sentence.contains("formatVersion"), "{sentence}");
        assert!(sentence.contains('1'), "{sentence}");

        descriptor.as_object_mut().unwrap().remove("formatVersion");
        assert_eq!(
            from_descriptor(&descriptor),
            Err(SemanticsError::UnsupportedFormatVersion { found: None })
        );
    }

    /// Decision D6's boundary, in fixture form: a `uw-map` containment is a
    /// form the interpreted node has no node type for, so the whole
    /// metamodel is refused with a sentence naming the feature. `ip5` proper
    /// runs this over `json.metamodel.json` and lands with step 2.
    #[test]
    fn ip5_a_keyed_feature_is_refused_with_a_sentence_naming_it() {
        let descriptor = with_merge(
            "Root",
            "child",
            json!({"kind": "unsupported",
                                                            "reason": "keyed"}),
        );
        let error = from_descriptor(&descriptor).expect_err("D6 refuses a keyed feature");
        assert_eq!(
            error,
            SemanticsError::Unsupported {
                class: "Root".to_string(),
                feature: "child".to_string(),
                reason: UnsupportedReason::Keyed,
            }
        );
        let sentence = error.to_string();
        assert!(sentence.contains("Root.child"), "{sentence}");
        assert!(sentence.contains("keyed"), "{sentence}");
        assert!(sentence.contains("generated path"), "{sentence}");
    }

    /// The other half of D6: a transparent class's feature. This is the one
    /// `json.ecore` trips over.
    #[test]
    fn ip5_a_transparent_feature_is_refused_with_a_sentence_naming_it() {
        let descriptor = with_merge(
            "Inverter",
            "tags",
            json!({"kind": "unsupported", "reason": "transparent"}),
        );
        let error = from_descriptor(&descriptor).expect_err("D6 refuses a transparent feature");
        let sentence = error.to_string();
        assert!(sentence.contains("Inverter.tags"), "{sentence}");
        assert!(sentence.contains("transparent"), "{sentence}");
    }

    /// The rule is read, never derived, so a feature that carries none is a
    /// broken descriptor and not a feature to guess at.
    #[test]
    fn a_feature_with_no_merge_object_is_refused_by_name() {
        let mut descriptor = fixture();
        descriptor["classes"]["Node"]["containments"][0]
            .as_object_mut()
            .unwrap()
            .remove("merge");
        let error = from_descriptor(&descriptor).expect_err("no rule to run");
        assert_eq!(
            error,
            SemanticsError::MissingRule {
                class: "Node".to_string(),
                feature: "children".to_string(),
                key: "merge",
            }
        );
        assert!(error.to_string().contains("Node.children"));
    }

    /// And the same for provenance: a facet with no source falsifies I-A4.
    #[test]
    fn a_feature_with_no_provenance_object_is_refused_by_name() {
        let mut descriptor = fixture();
        descriptor["classes"]["Node"]["attributes"][0]
            .as_object_mut()
            .unwrap()
            .remove("provenance");
        let error = from_descriptor(&descriptor).expect_err("a facet with no source");
        assert_eq!(
            error,
            SemanticsError::MissingRule {
                class: "Node".to_string(),
                feature: "ID".to_string(),
                key: "provenance",
            }
        );
    }

    /// A rule outside the closed vocabulary is refused naming the feature,
    /// rather than parsed into something near it.
    #[test]
    fn a_rule_outside_the_vocabulary_is_refused_naming_the_feature() {
        let descriptor = with_merge(
            "Root",
            "active",
            json!({"kind": "attribute", "shape": {"kind": "single"},
                   "leaf": {"kind": "graph"}}),
        );
        let error = from_descriptor(&descriptor).expect_err("`graph` is not a leaf");
        let SemanticsError::Malformed { at, .. } = &error else {
            panic!("expected a malformed rule, got {error:?}");
        };
        assert_eq!(at, "Root.active");

        let descriptor = with_merge(
            "Root",
            "child",
            json!({"kind": "containment", "shape": {"kind": "sequence"}, "target": "Missing"}),
        );
        assert_eq!(
            from_descriptor(&descriptor),
            Err(SemanticsError::UnknownClass {
                at: "Root.child".to_string(),
                name: "Missing".to_string(),
            })
        );
    }
}
