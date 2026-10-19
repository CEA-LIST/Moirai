//! Copy-migration (SI.12): a model under one descriptor becomes a new model
//! under a revision of it, and the old one is closed.
//!
//! # What a migration writes
//!
//! [`migrate`] plans two things from the source log as the migrating replica
//! holds it. The cut ([`ModelOp::Cut`]), sent on the source log, names the
//! successor and carries the two documents the source serves at that moment;
//! every replica that delivers it serves those from then on and refuses every
//! local write (see [`crate::log`]). The successor's operations, sent on a new
//! log, open it with [`ModelOp::Install`] of the revision and rebuild the
//! cut's document under the revision's table, mapped by name:
//!
//! - a feature both descriptors declare keeps its value: contained objects in
//!   their order, flags, sets, texts, references by text, and references by
//!   identity, each re-pointed at the copy of the object it named;
//! - a feature or class only the revision declares is not written, and reads
//!   its default;
//! - a feature whose rule the revision changes is written once, by the new
//!   rule, with the value the source read for it (a text `Door` becomes one
//!   register value `Door`).
//!
//! # What it refuses
//!
//! A revision is a revision of the same metamodel (the same `nsURI`) that
//! keeps every class and every feature of the source descriptor; one that
//! lacks a feature (a rename or a removal) is refused by the feature's name,
//! `<Class>.<feature>`, whether or not the model holds a value of it, and so
//! is one that changes a feature's kind. A model already cut is refused
//! naming its successor. A value the revision's rule cannot hold as one value
//! (a conflict, say) refuses the migration too, by the feature's name, since
//! the copy would otherwise not be the document the source served.
//!
//! These value refusals go beyond the revision check, and are limits of the
//! copy as built today rather than of the revision: a model is refused when
//! it holds, in a feature both descriptors declare,
//!
//! - a non-empty keyed collection or sequence of scalars (the copy does not
//!   carry those shapes yet; empty ones are carried as their default);
//! - a register or containment that reads a concurrent conflict
//!   (`__conflict`), since the copy holds one value where the source read
//!   several.
//!
//! The running SimpleUML example holds neither.
//!
//! # The size of the cut
//!
//! The cut carries both documents, so it grows with the model (about
//! 1.24 MB at 5,000 objects). A node sends it as one event message; a relay
//! session writes a message longer than the relay's frame bound as chunk
//! frames under it, joined again by the receiver (`moirai-network`'s
//! `relay_transport.rs`, SI.13 D2), so a replica behind a relay hears a cut
//! of any size.
//!
//! # Who sends the successor's operations
//!
//! The plan is written before anyone knows which replica sends it, so a link
//! among the copied objects cannot name its writer: it names the object by
//! the writer's own event as `@<seq>.<step>`, which the log resolves against
//! the event's origin on delivery. The sequence numbers assume what the plan
//! is for: one writer sends every operation of it, in order, on a log it has
//! written nothing else to, so the `n`th operation is its event `n`, and an
//! event's identity is its origin and sequence number with nothing else to
//! tell two events apart.
//!
//! # What an adapter may not send
//!
//! The cut and the own-event link form are the migration's own: the log
//! accepts both from a local writer, because the replica that sends a plan
//! is one. A node's adapter intake must not, or a client could freeze a
//! model with a hand-built cut that skips the revision check and opens no
//! successor, or name a link target `@<seq>.<step>` that SI.07's local
//! check by class cannot resolve, and so get a link of the wrong class past
//! it. [`adapter_refusal`] is that check, for a node to run on every
//! operation an adapter submits.

use std::collections::HashMap;
use std::fmt;

use moirai_semantics::{
    ClassSlot, FeatureSlot, LeafRule, MergeRule, MetamodelSemantics, NumKind, from_descriptor,
    metamodel_digest,
};
use serde_json::{Map, Value};

use crate::eval::{CONFLICT, ECLASS, ID, read, read_identified};
use crate::leaf::{LeafOp, Scalar};
use crate::log::ModelLog;
use crate::node::{LeafSite, Shaped, Site, Target, shaped};
use crate::op::{InstanceOp, ModelOp};

/// A planned migration: the cut, and the successor's operations.
#[derive(Clone, Debug)]
pub struct Migration {
    /// The one operation the migrating replica sends on the source log.
    pub cut: ModelOp,
    /// The operations that open and fill the successor, in the order one
    /// writer sends them: an [`ModelOp::Install`] first, and no other.
    pub ops: Vec<ModelOp>,
}

/// Why a migration was refused. Nothing was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrateRefusal {
    /// The source was cut already.
    Migrated {
        /// Its successor.
        to: String,
    },
    /// The source holds no metamodel yet.
    NoTable,
    /// The revision cannot be read as a table.
    Descriptor(String),
    /// The revision is of another metamodel.
    OtherMetamodel {
        /// The source's `nsURI`.
        from: String,
        /// The revision's.
        to: String,
    },
    /// The revision lacks classes or features of the source descriptor, each
    /// named `<Class>` or `<Class>.<feature>`.
    Lacks(Vec<String>),
    /// The revision changes what kind of feature one is.
    Kind {
        /// `<Class>.<feature>`.
        feature: String,
        /// The source's kind.
        from: &'static str,
        /// The revision's.
        to: &'static str,
    },
    /// The model holds a value the revision cannot hold as the source read
    /// it.
    Value {
        /// `<Class>.<feature>`.
        feature: String,
        /// Why.
        why: String,
    },
}

impl fmt::Display for MigrateRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrateRefusal::Migrated { to } => write!(
                f,
                "this model was migrated to `{to}` already and is read-only: migrate `{to}` \
                 instead"
            ),
            MigrateRefusal::NoTable => write!(f, "this model holds no metamodel yet"),
            MigrateRefusal::Descriptor(why) => {
                write!(f, "the revision cannot be read as a table: {why}")
            }
            MigrateRefusal::OtherMetamodel { from, to } => write!(
                f,
                "the revision is of metamodel `{to}`, and the model is of `{from}`"
            ),
            MigrateRefusal::Lacks(missing) => write!(
                f,
                "the revision lacks {} of the model's metamodel; a rename or a removal is not \
                 a migration this node makes",
                missing
                    .iter()
                    .map(|name| format!("`{name}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            MigrateRefusal::Kind { feature, from, to } => write!(
                f,
                "the revision turns `{feature}` from {from} into {to}, which a copy does not map"
            ),
            MigrateRefusal::Value { feature, why } => {
                write!(
                    f,
                    "the copy cannot hold `{feature}` as the model reads it: {why}"
                )
            }
        }
    }
}

impl std::error::Error for MigrateRefusal {}

/// Plan the migration of `source` to a new model `model_id` under
/// `descriptor`, the revision's text. See the module header.
pub fn migrate(
    source: &ModelLog,
    model_id: &str,
    descriptor: &str,
) -> Result<Migration, MigrateRefusal> {
    if let Some(to) = source.migrated_to() {
        return Err(MigrateRefusal::Migrated { to: to.to_string() });
    }
    let from = source.semantics().ok_or(MigrateRefusal::NoTable)?;
    let parsed: Value = serde_json::from_str(descriptor)
        .map_err(|err| MigrateRefusal::Descriptor(format!("it is not JSON: {err}")))?;
    let to = from_descriptor(&parsed).map_err(|err| MigrateRefusal::Descriptor(err.to_string()))?;
    revision(from, &to)?;

    let identified = read_identified(from, source.root());
    let plain = read(from, source.root());
    let mut copy = Copy {
        sem: &to,
        ops: vec![ModelOp::Install {
            model_id: model_id.to_string(),
            metamodel_id: metamodel_digest(&parsed),
            descriptor: descriptor.to_string(),
        }],
        ids: HashMap::new(),
        links: Vec::new(),
    };
    if let Value::Object(root) = &identified {
        let class = copy.class(root, Target::Roots, "the model root")?;
        copy.object(
            root,
            class,
            Vec::new(),
            InstanceOp::variant(class, InstanceOp::New),
        )?;
    }
    copy.link();
    Ok(Migration {
        cut: ModelOp::Cut {
            migrated_to: model_id.to_string(),
            identified,
            plain,
        },
        ops: copy.ops,
    })
}

/// Why an operation submitted through a node's adapter is refused because it
/// is a migration's own: a cut, or a link or unlink to an own-event target
/// `@<seq>.<step>`. See the module header.
pub fn adapter_refusal(op: &ModelOp) -> Result<(), String> {
    match op {
        ModelOp::Cut { .. } => Err(
            "a cut is written by a migration (`POST /api/model/{id}/migrate`) and by nothing else"
                .to_string(),
        ),
        ModelOp::Instance(op) if crate::log::holds_own_target(op) => Err(
            "a link target `@<seq>.<step>` names an object by its writer's own event, which only \
             a migration's plan does: name the object by its served identity"
                .to_string(),
        ),
        _ => Ok(()),
    }
}

/// What kind of feature a rule makes, for the revision check.
fn kind(rule: &MergeRule) -> &'static str {
    match rule {
        MergeRule::Attribute { .. } => "an attribute",
        MergeRule::Containment { .. } => "a containment",
        MergeRule::Reference { .. } => "a reference by text",
        MergeRule::IdentityReference { .. } => "a reference by identity",
        MergeRule::Unsupported { .. } => "a feature with no rule",
    }
}

/// The revision check: the same metamodel, every class and feature of `from`
/// in `to` under the same name, of the same kind.
fn revision(from: &MetamodelSemantics, to: &MetamodelSemantics) -> Result<(), MigrateRefusal> {
    if from.ns_uri != to.ns_uri {
        return Err(MigrateRefusal::OtherMetamodel {
            from: from.ns_uri.to_string(),
            to: to.ns_uri.to_string(),
        });
    }
    let mut missing = Vec::new();
    for class in &from.classes {
        let Some(revised) = to.classes.iter().find(|other| other.name == class.name) else {
            missing.push(class.name.to_string());
            continue;
        };
        for feature in &class.declared {
            let name = format!("{}.{}", class.name, feature.name);
            let seen = revised
                .visible
                .iter()
                .find(|(visible, _, _)| *visible == feature.name);
            let Some((_, owner, declared)) = seen else {
                missing.push(name);
                continue;
            };
            if let Some(rule) = to.rule(*owner, *declared)
                && kind(rule) != kind(&feature.merge)
            {
                return Err(MigrateRefusal::Kind {
                    feature: name,
                    from: kind(&feature.merge),
                    to: kind(rule),
                });
            }
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(MigrateRefusal::Lacks(missing))
    }
}

/// One step of the path from the model root to an object of the copy.
#[derive(Clone, Copy, Debug)]
enum Step {
    Variant(ClassSlot),
    Field(FeatureSlot),
    At(usize),
    Set,
}

/// `inner` addressed down `path`.
fn wrap(path: &[Step], inner: InstanceOp) -> InstanceOp {
    path.iter().rev().fold(inner, |inner, step| match *step {
        Step::Variant(class) => InstanceOp::variant(class, inner),
        Step::Field(slot) => InstanceOp::field(slot, inner),
        Step::At(pos) => InstanceOp::at(pos, inner),
        Step::Set => InstanceOp::set(inner),
    })
}

/// The copy as it is planned: the operations so far, the identity each
/// copied object will be served under, and the links to write once every
/// object is there.
struct Copy<'a> {
    sem: &'a MetamodelSemantics,
    ops: Vec<ModelOp>,
    /// The source's identity of an object, to the copy's own-event identity.
    ids: HashMap<String, String>,
    /// A reference by identity: the path to its feature, and the source
    /// identities it reads.
    links: Vec<(Vec<Step>, Vec<String>)>,
}

impl Copy<'_> {
    fn push(&mut self, path: &[Step], inner: InstanceOp) {
        self.ops.push(ModelOp::Instance(wrap(path, inner)));
    }

    /// The revision's class an object of the source document is an instance
    /// of, which the place it sits must allow.
    fn class(
        &self,
        object: &Map<String, Value>,
        target: Target,
        place: &str,
    ) -> Result<ClassSlot, MigrateRefusal> {
        let refused = |why: String| MigrateRefusal::Value {
            feature: place.to_string(),
            why,
        };
        let name = object
            .get(ECLASS)
            .and_then(Value::as_str)
            .ok_or_else(|| refused(format!("an element carries no `{ECLASS}`")))?;
        let class = self
            .sem
            .classes
            .iter()
            .find(|class| &*class.name == name)
            .ok_or_else(|| refused(format!("the revision has no class `{name}`")))?;
        if !target.allows(self.sem, class.slot) {
            return Err(refused(format!(
                "the revision does not hold a `{name}` here"
            )));
        }
        Ok(class.slot)
    }

    /// Create the object `object` of `class` by `create`, sent at `parent`,
    /// and write its features; its own path is `parent` and then `below`.
    fn object(
        &mut self,
        object: &Map<String, Value>,
        class: ClassSlot,
        parent: Vec<Step>,
        create: InstanceOp,
    ) -> Result<(), MigrateRefusal> {
        self.push(&parent, create);
        self.placed(object, class, parent)
    }

    /// Note where the object just created sits, and write its features.
    fn placed(
        &mut self,
        object: &Map<String, Value>,
        class: ClassSlot,
        mut path: Vec<Step>,
    ) -> Result<(), MigrateRefusal> {
        path.push(Step::Variant(class));
        // The object is born of the operation just pushed, at the class step
        // it sits below: `ObjectNode::identity`'s `<seq>.<step>`.
        let step = path
            .iter()
            .filter(|step| matches!(step, Step::Variant(_)))
            .count()
            - 1;
        if let Some(Value::String(id)) = object.get(ID) {
            self.ids
                .insert(id.clone(), format!("@{}.{step}", self.ops.len()));
        }
        let sem = self.sem;
        let holder = &sem.classes[class.index()];
        for (index, (name, owner, declared)) in holder.visible.iter().enumerate() {
            let Some(value) = object.get(&**name) else {
                continue;
            };
            let Some(rule) = sem.rule(*owner, *declared) else {
                continue;
            };
            let feature = format!("{}.{name}", sem.classes[owner.index()].name);
            let Ok(shape) = shaped(rule) else {
                continue;
            };
            let mut at = path.clone();
            at.push(Step::Field(FeatureSlot(index as u16)));
            self.feature(&feature, shape, at, value)?;
        }
        Ok(())
    }

    /// Write one feature's value, read off the source, by the revision's
    /// rule, at `at` (the path to the feature).
    fn feature(
        &mut self,
        feature: &str,
        shape: Shaped,
        at: Vec<Step>,
        value: &Value,
    ) -> Result<(), MigrateRefusal> {
        let refused = |why: &str| MigrateRefusal::Value {
            feature: feature.to_string(),
            why: why.to_string(),
        };
        let objects = |value: &Value| -> Result<Vec<Map<String, Value>>, MigrateRefusal> {
            match value {
                Value::Null => Ok(Vec::new()),
                Value::Object(map) if map.contains_key(CONFLICT) => {
                    Err(refused("it holds concurrent objects"))
                }
                Value::Object(map) => Ok(vec![map.clone()]),
                Value::Array(items) => items
                    .iter()
                    .map(|item| match item {
                        Value::Object(map) => Ok(map.clone()),
                        _ => Err(refused("a contained element is not an object")),
                    })
                    .collect(),
                _ => Err(refused("a containment reads as a scalar")),
            }
        };
        match shape {
            Shaped::Bare(Site::Object(target)) => {
                for child in objects(value)? {
                    let class = self.class(&child, target, feature)?;
                    self.object(
                        &child,
                        class,
                        at.clone(),
                        InstanceOp::variant(class, InstanceOp::New),
                    )?;
                }
            }
            Shaped::Optional(Site::Object(target)) => {
                for child in objects(value)? {
                    let class = self.class(&child, target, feature)?;
                    self.push(
                        &at,
                        InstanceOp::set(InstanceOp::variant(class, InstanceOp::New)),
                    );
                    let mut below = at.clone();
                    below.push(Step::Set);
                    self.placed(&child, class, below)?;
                }
            }
            Shaped::Sequence(Site::Object(target)) => {
                for (pos, child) in objects(value)?.iter().enumerate() {
                    let class = self.class(child, target, feature)?;
                    self.push(
                        &at,
                        InstanceOp::insert(pos, InstanceOp::variant(class, InstanceOp::New)),
                    );
                    let mut below = at.clone();
                    below.push(Step::At(pos));
                    self.placed(child, class, below)?;
                }
            }
            Shaped::Bare(Site::Leaf(LeafSite::Scalar(rule))) => {
                for op in self.scalar(feature, rule, value)? {
                    self.push(&at, InstanceOp::Leaf(op));
                }
            }
            Shaped::Optional(Site::Leaf(LeafSite::Scalar(rule))) => {
                for op in self.scalar(feature, rule, value)? {
                    self.push(&at, InstanceOp::set(InstanceOp::Leaf(op)));
                }
            }
            Shaped::Bare(Site::Leaf(LeafSite::Set(_) | LeafSite::Bag)) => {
                for item in elements(value).ok_or_else(|| refused("it is not a collection"))? {
                    let item = scalar(item).ok_or_else(|| refused("an element is not a scalar"))?;
                    self.push(&at, InstanceOp::Leaf(LeafOp::Add(item)));
                }
            }
            Shaped::Bare(Site::Leaf(LeafSite::OrderedSet)) => {
                let items = elements(value).ok_or_else(|| refused("it is not a collection"))?;
                for (pos, item) in items.iter().enumerate() {
                    let item = scalar(item).ok_or_else(|| refused("an element is not a scalar"))?;
                    self.push(&at, InstanceOp::Leaf(LeafOp::Insert { pos, value: item }));
                }
            }
            Shaped::Bare(Site::Refs { .. }) => {
                let targets = match value {
                    Value::Null => Vec::new(),
                    Value::String(id) => vec![id.clone()],
                    Value::Array(items) => items
                        .iter()
                        .map(|item| item.as_str().map(str::to_string))
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| refused("a link is not an identity"))?,
                    _ => return Err(refused("a reference reads as no identity")),
                };
                if !targets.is_empty() {
                    self.links.push((at, targets));
                }
            }
            // Keyed collections, sequences of scalars, and the shapes no rule
            // makes: carried when empty, refused when the model uses them.
            _ => {
                let empty = match value {
                    Value::Null => true,
                    Value::Array(items) => items.is_empty(),
                    Value::Object(map) => map.is_empty(),
                    _ => false,
                };
                if !empty {
                    return Err(refused("a copy does not carry this collection yet"));
                }
            }
        }
        Ok(())
    }

    /// The leaf operations that make a fresh leaf of `rule` read `value`.
    fn scalar(
        &self,
        feature: &str,
        rule: LeafRule,
        value: &Value,
    ) -> Result<Vec<LeafOp>, MigrateRefusal> {
        let refused = |why: String| MigrateRefusal::Value {
            feature: feature.to_string(),
            why,
        };
        if let Value::Object(map) = value
            && map.contains_key(CONFLICT)
        {
            return Err(refused(format!(
                "it holds concurrent values {value}, and a copy holds one"
            )));
        }
        let ops = match (rule, value) {
            (_, Value::Null) => Vec::new(),
            (LeafRule::Text, Value::String(text)) => text
                .chars()
                .enumerate()
                .map(|(pos, ch)| LeafOp::InsertChar { pos, ch })
                .collect(),
            (LeafRule::Counter { num, .. }, Value::Number(number)) => {
                let amount = match num {
                    NumKind::F32 | NumKind::F64 => number.as_f64().map(Scalar::float),
                    _ => number.as_i64().map(Scalar::Int),
                }
                .ok_or_else(|| refused(format!("{number} is not a {num:?} amount")))?;
                if matches!(amount, Scalar::Int(0)) || amount == Scalar::float(0.0) {
                    Vec::new()
                } else {
                    vec![LeafOp::Inc(amount)]
                }
            }
            (LeafRule::Flag { .. }, Value::Bool(true)) => vec![LeafOp::Enable],
            (LeafRule::Flag { .. }, Value::Bool(false)) => Vec::new(),
            (LeafRule::Enum { class, .. }, Value::String(literal)) => {
                let index = self
                    .sem
                    .enums
                    .get(class.index())
                    .and_then(|entry| entry.literals.iter().position(|name| **name == **literal))
                    .ok_or_else(|| refused(format!("the revision has no literal `{literal}`")))?;
                vec![LeafOp::Write(Scalar::Enum(class.0, index as u16))]
            }
            (LeafRule::OptionalRegister { .. }, Value::Array(items)) => match items.as_slice() {
                [] => Vec::new(),
                [item] => vec![LeafOp::Write(
                    scalar(item).ok_or_else(|| refused(format!("{item} is not a scalar")))?,
                )],
                _ => {
                    return Err(refused(format!(
                        "it holds concurrent values {value}, and a copy holds one"
                    )));
                }
            },
            (
                LeafRule::Register { .. } | LeafRule::OptionalRegister { .. },
                Value::Bool(_) | Value::Number(_) | Value::String(_),
            ) => {
                vec![LeafOp::Write(scalar(value).ok_or_else(|| {
                    refused(format!("{value} is not a scalar"))
                })?)]
            }
            (rule, value) => {
                return Err(refused(format!(
                    "{value} is not a value its rule {rule:?} takes"
                )));
            }
        };
        Ok(ops)
    }

    /// The links, now that every object of the copy has its identity: each
    /// names the copy of the object its source named. A link to an object
    /// the copy does not hold is not written, as the source read none.
    fn link(&mut self) {
        for (at, targets) in std::mem::take(&mut self.links) {
            for target in targets {
                if let Some(own) = self.ids.get(&target).cloned() {
                    self.push(&at, InstanceOp::Leaf(LeafOp::Link(own)));
                }
            }
        }
    }
}

/// The elements of a collection read, `[]` for `null`.
fn elements(value: &Value) -> Option<&[Value]> {
    match value {
        Value::Null => Some(&[]),
        Value::Array(items) => Some(items),
        _ => None,
    }
}

/// A JSON scalar as the leaf vocabulary spells it.
fn scalar(value: &Value) -> Option<Scalar> {
    Some(match value {
        Value::Null => Scalar::Null,
        Value::Bool(flag) => Scalar::Bool(*flag),
        Value::Number(number) => match number.as_i64() {
            Some(int) => Scalar::Int(int),
            None => Scalar::float(number.as_f64()?),
        },
        Value::String(text) => Scalar::Str(text.clone()),
        Value::Array(_) | Value::Object(_) => return None,
    })
}
