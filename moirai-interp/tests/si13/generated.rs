//! SI.12's model generator, restated for SI.13's 5,000-object runs.
//!
//! Everything below the constants is `moirai-interp/tests/si12_scale_l3.rs`
//! at `0c90111`, lines 157 to 710 (the generator, the operations it writes
//! under `D1`, the expected documents, `to_d2`, `projected`, `objects`,
//! `at_path`), byte for byte except that every item and field is `pub`.
//! That file is SI.12's registered oracle and a test target, not a module,
//! and its items are private, so it cannot be imported; restating it keeps
//! it unchanged. `SEED`, `CLASSES` and `ASSOCIATIONS` are its lines 129 to
//! 132. `generate(5000)` is therefore the model SI.12 round 4 migrated
//! (`SI12-R4-MODEL`): the same seed `SEED ^ 5000`, the same draws, the same
//! 5,000 objects and 58,060 operations.
#![allow(dead_code)]

use std::collections::BTreeMap;

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{LeafRule, MergeRule, Shape};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};
use serde_json::{Map, Value, json};

use super::si12::{Uml, labels, project};

/// The generator's seed; each size draws from `SEED ^ size`.
pub const SEED: u64 = 0x5112_0003_5112_0003;
/// Classes per package, associations per package.
pub const CLASSES: usize = 8;
pub const ASSOCIATIONS: usize = 2;

/// The SimpleUML classes a generated model instantiates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Model,
    Package,
    PrimitiveType,
    Class,
    Property,
    Generalization,
    Association,
    TaggedValue,
}

impl Kind {
    pub fn class(self) -> &'static str {
        match self {
            Kind::Model => "Model",
            Kind::Package => "Package",
            Kind::PrimitiveType => "PrimitiveType",
            Kind::Class => "Class",
            Kind::Property => "Property",
            Kind::Generalization => "Generalization",
            Kind::Association => "Association",
            Kind::TaggedValue => "TaggedValue",
        }
    }
}

/// One step down from an object: the containment, the position in it, and
/// the class of the object there.
#[derive(Clone, Debug)]
pub struct Step {
    pub feature: &'static str,
    pub pos: usize,
    pub class: &'static str,
}

/// One object of a generated model, as the generator writes it.
#[derive(Clone, Debug)]
pub struct Element {
    pub kind: Kind,
    /// `None` for a generalization, which has no name.
    pub name: Option<String>,
    /// From the root; empty for the root.
    pub path: Vec<Step>,
    pub parent: Option<usize>,
    /// Contained objects, by containment, in order.
    pub children: BTreeMap<&'static str, Vec<usize>>,
    pub is_abstract: bool,
    pub stereotype: Option<&'static str>,
    /// `Packageable.owner`, a reference by text.
    pub owner_text: Option<String>,
    pub is_substitutable: bool,
    /// `TaggedValue.value`.
    pub value: Option<String>,
    /// References by identity, by feature, to the object each names.
    pub refs: BTreeMap<&'static str, usize>,
}

/// A generated model: its objects, the root first, each after its owner.
pub struct Generated {
    pub size: usize,
    pub elements: Vec<Element>,
    /// The classes of each package, in order, by package.
    pub classes_of: BTreeMap<usize, Vec<usize>>,
    /// The primitive type of each package.
    pub type_of: BTreeMap<usize, usize>,
    /// Every property, with the package its class sits in.
    pub properties: Vec<(usize, usize)>,
}

pub fn below(rng: &mut ChaCha8Rng, n: usize) -> usize {
    (rng.next_u64() % n as u64) as usize
}

impl Generated {
    pub fn full(&self) -> bool {
        self.elements.len() >= self.size
    }

    /// One more object, of `kind`, at the end of `parent`'s `feature`.
    pub fn add(
        &mut self,
        parent: usize,
        feature: &'static str,
        kind: Kind,
        name: Option<String>,
    ) -> usize {
        let index = self.elements.len();
        let siblings = self.elements[parent].children.entry(feature).or_default();
        let pos = siblings.len();
        siblings.push(index);
        let mut path = self.elements[parent].path.clone();
        path.push(Step {
            feature,
            pos,
            class: kind.class(),
        });
        self.elements.push(Element {
            kind,
            name,
            path,
            parent: Some(parent),
            children: BTreeMap::new(),
            is_abstract: false,
            stereotype: None,
            owner_text: None,
            is_substitutable: false,
            value: None,
            refs: BTreeMap::new(),
        });
        index
    }

    /// The label `si12::labels` gives the object: its name, or
    /// `<owner label>.<feature>[<i>]`.
    pub fn label(&self, index: usize) -> String {
        let element = &self.elements[index];
        match (&element.name, element.parent, element.path.last()) {
            (Some(name), _, _) => name.clone(),
            (None, Some(parent), Some(step)) => {
                format!("{}.{}[{}]", self.label(parent), step.feature, step.pos)
            }
            _ => unreachable!("the root is named"),
        }
    }

    /// How many objects of each class.
    pub fn census(&self) -> BTreeMap<&'static str, usize> {
        let mut out = BTreeMap::new();
        for element in &self.elements {
            *out.entry(element.kind.class()).or_insert(0) += 1;
        }
        out
    }

    /// The first two classes of the first package, `Class000_0` (abstract,
    /// stereotype `entity`) and `Class000_1` (not abstract): what the
    /// concurrent edits touch.
    pub fn first_classes(&self) -> (usize, usize) {
        let classes = &self.classes_of[&1];
        (classes[0], classes[1])
    }
}

/// The model of `size` objects, from the seed of that size.
pub fn generate(size: usize) -> Generated {
    let mut seed = [0u8; 32];
    for chunk in seed.chunks_mut(8) {
        chunk.copy_from_slice(&(SEED ^ size as u64).to_le_bytes());
    }
    let mut rng = ChaCha8Rng::from_seed(seed);
    let mut g = Generated {
        size,
        elements: vec![Element {
            kind: Kind::Model,
            name: Some("Vehicle".to_string()),
            path: Vec::new(),
            parent: None,
            children: BTreeMap::new(),
            is_abstract: false,
            stereotype: None,
            owner_text: None,
            is_substitutable: false,
            value: None,
            refs: BTreeMap::new(),
        }],
        classes_of: BTreeMap::new(),
        type_of: BTreeMap::new(),
        properties: Vec::new(),
    };
    let mut packages: Vec<usize> = Vec::new();
    'model: for p in 0usize.. {
        if g.full() {
            break;
        }
        let parent = if p % 3 == 2 { packages[p - 1] } else { 0 };
        let package = g.add(
            parent,
            "ownedElements",
            Kind::Package,
            Some(format!("Pkg{p:03}")),
        );
        g.elements[package].owner_text = g.elements[parent].name.clone();
        packages.push(package);
        g.classes_of.insert(package, Vec::new());
        if g.full() {
            break;
        }
        let primitive = g.add(
            package,
            "ownedElements",
            Kind::PrimitiveType,
            Some(format!("Type{p:03}")),
        );
        g.type_of.insert(package, primitive);
        for c in 0..CLASSES {
            if g.full() {
                break 'model;
            }
            let class = g.add(
                package,
                "ownedElements",
                Kind::Class,
                Some(format!("Class{p:03}_{c}")),
            );
            g.classes_of
                .get_mut(&package)
                .expect("opened above")
                .push(class);
            let (abstract_draw, stereotype_draw, owner_draw) =
                (below(&mut rng, 4), below(&mut rng, 4), below(&mut rng, 2));
            let package_name = g.elements[package].name.clone();
            let element = &mut g.elements[class];
            element.is_abstract = c == 0 || (c >= 2 && abstract_draw == 0);
            element.stereotype = (c == 0 || stereotype_draw == 0).then_some("entity");
            element.owner_text = if owner_draw == 0 { package_name } else { None };
            for k in 0..1 + below(&mut rng, 3) {
                if g.full() {
                    break 'model;
                }
                let property = g.add(
                    class,
                    "attributes",
                    Kind::Property,
                    Some(format!("attr{p:03}_{c}_{k}")),
                );
                g.properties.push((property, package));
            }
            if c >= 1 && below(&mut rng, 3) == 0 {
                if g.full() {
                    break 'model;
                }
                let generalization = g.add(class, "generalizations", Kind::Generalization, None);
                let general = g.classes_of[&package][below(&mut rng, c)];
                let element = &mut g.elements[generalization];
                element.is_substitutable = below(&mut rng, 2) == 0;
                element.refs.insert("general", general);
            }
            if below(&mut rng, 5) == 0 {
                if g.full() {
                    break 'model;
                }
                let tagged = g.add(
                    class,
                    "taggedValue",
                    Kind::TaggedValue,
                    Some(format!("tag{p:03}_{c}")),
                );
                g.elements[tagged].value = Some(below(&mut rng, 100).to_string());
            }
        }
        for j in 0..ASSOCIATIONS {
            if g.full() {
                break 'model;
            }
            let association = g.add(
                package,
                "ownedElements",
                Kind::Association,
                Some(format!("assoc{p:03}_{j}")),
            );
            let classes = &g.classes_of[&package];
            let source = classes[below(&mut rng, classes.len())];
            let target = classes[below(&mut rng, classes.len())];
            let element = &mut g.elements[association];
            element.refs.insert("source", source);
            element.refs.insert("target", target);
        }
    }
    // Each property's type, once every object is there: the primitive type
    // of its package or one of its package's classes.
    for (property, package) in g.properties.clone() {
        let mut candidates = vec![g.type_of[&package]];
        candidates.extend(g.classes_of[&package].iter().copied());
        let ttype = candidates[below(&mut rng, candidates.len())];
        g.elements[property].refs.insert("ttype", ttype);
    }
    g
}

// ---------------------------------------------------------------------------
// The generated model's operations under D1, and its expected documents
// ---------------------------------------------------------------------------

/// The class of the object at the end of `path`.
pub fn class_at(path: &[Step]) -> &'static str {
    path.last().map_or("Model", |step| step.class)
}

/// `inner` addressed to the object at `path`, as `si12::Uml::on` addresses
/// one of the running example.
pub fn on(uml: &Uml, path: &[Step], inner: InstanceOp) -> ModelOp {
    let mut op = inner;
    for (index, step) in path.iter().enumerate().rev() {
        let owner = class_at(&path[..index]);
        op = InstanceOp::field(
            uml.slot(owner, step.feature),
            InstanceOp::at(step.pos, InstanceOp::variant(uml.class(step.class), op)),
        );
    }
    ModelOp::Instance(InstanceOp::variant(uml.class("Model"), op))
}

/// Mint the object `step` names, below the object at `path`.
pub fn insert(uml: &Uml, path: &[Step], step: &Step) -> ModelOp {
    on(
        uml,
        path,
        InstanceOp::field(
            uml.slot(class_at(path), step.feature),
            InstanceOp::insert(
                step.pos,
                InstanceOp::variant(uml.class(step.class), InstanceOp::New),
            ),
        ),
    )
}

/// `leaf` at `feature` of the object at `path`, inside the optional when the
/// feature is one, as `si12::Uml::leaf` writes it.
pub fn leaf(uml: &Uml, path: &[Step], feature: &str, leaf: LeafOp) -> ModelOp {
    let class = class_at(path);
    let inner = match uml.rule(class, feature) {
        MergeRule::Attribute {
            shape: Shape::Optional,
            ..
        } => InstanceOp::set(InstanceOp::Leaf(leaf)),
        _ => InstanceOp::Leaf(leaf),
    };
    on(
        uml,
        path,
        InstanceOp::field(uml.slot(class, feature), inner),
    )
}

/// `text` typed into the empty text attribute `feature` at `path`.
pub fn text(uml: &Uml, path: &[Step], feature: &str, text: &str) -> Vec<ModelOp> {
    assert!(
        matches!(
            uml.rule(class_at(path), feature),
            MergeRule::Attribute {
                leaf: LeafRule::Text,
                ..
            }
        ),
        "`{}.{feature}` is not a text attribute under D1",
        class_at(path)
    );
    text.chars()
        .enumerate()
        .map(|(pos, ch)| leaf(uml, path, feature, LeafOp::InsertChar { pos, ch }))
        .collect()
}

impl Generated {
    /// Every object and every attribute, from an empty model, under `D1`:
    /// everything but the identity references.
    pub fn structure(&self, uml: &Uml) -> Vec<ModelOp> {
        let mut ops = vec![ModelOp::Instance(InstanceOp::variant(
            uml.class("Model"),
            InstanceOp::New,
        ))];
        for element in &self.elements {
            let path = &element.path;
            if let Some((step, owner)) = path.split_last() {
                ops.push(insert(uml, owner, step));
            }
            if let Some(name) = &element.name {
                ops.extend(text(uml, path, "name", name));
            }
            if element.is_abstract {
                ops.push(leaf(uml, path, "abstract", LeafOp::Enable));
            }
            if let Some(stereotype) = element.stereotype {
                ops.push(leaf(
                    uml,
                    path,
                    "stereotype",
                    LeafOp::Add(Scalar::text(stereotype)),
                ));
            }
            if let Some(owner) = &element.owner_text {
                ops.push(leaf(
                    uml,
                    path,
                    "owner",
                    LeafOp::Write(Scalar::text(owner.as_str())),
                ));
            }
            if element.is_substitutable {
                ops.push(leaf(uml, path, "isSubstitutable", LeafOp::Enable));
            }
            if let Some(value) = &element.value {
                ops.extend(text(uml, path, "value", value));
            }
        }
        ops
    }

    /// The identity references, to the identities `ids` names by label.
    pub fn links(&self, uml: &Uml, ids: &BTreeMap<String, String>) -> Result<Vec<ModelOp>, String> {
        let mut ops = Vec::new();
        for element in &self.elements {
            for (feature, target) in &element.refs {
                let label = self.label(*target);
                let id = ids
                    .get(&label)
                    .ok_or_else(|| format!("no served object labelled `{label}`"))?;
                ops.push(leaf(uml, &element.path, feature, LeafOp::Link(id.clone())));
            }
        }
        Ok(ops)
    }

    /// The model under `D1`, projected (as `si12::project` projects a served
    /// document): computed from the description by the read-out's rules, as
    /// `si12::expected_d1` is for the running example. An unset optional flag
    /// is absent, an empty containment or set `[]`, a reference by text
    /// nobody wrote `null`, an identity reference `@<label>`.
    pub fn expected_d1(&self) -> Value {
        self.expected(0)
    }

    pub fn expected(&self, index: usize) -> Value {
        let element = &self.elements[index];
        let mut map = Map::new();
        map.insert("__id".to_string(), json!(format!("#{}", self.label(index))));
        map.insert("eClass".to_string(), json!(element.kind.class()));
        let contained = |feature: &str| {
            Value::Array(
                element
                    .children
                    .get(feature)
                    .map(|children| children.iter().map(|child| self.expected(*child)).collect())
                    .unwrap_or_default(),
            )
        };
        let reference = |feature: &str| {
            element
                .refs
                .get(feature)
                .map(|target| json!(format!("@{}", self.label(*target))))
                .unwrap_or(Value::Null)
        };
        let owner = || {
            element
                .owner_text
                .as_ref()
                .map(|owner| json!(owner))
                .unwrap_or(Value::Null)
        };
        match element.kind {
            Kind::Generalization => {
                map.insert("general".to_string(), reference("general"));
                if element.is_substitutable {
                    map.insert("isSubstitutable".to_string(), json!(true));
                }
            }
            Kind::TaggedValue => {
                map.insert("name".to_string(), json!(element.name));
                map.insert("value".to_string(), json!(element.value));
            }
            _ => {
                map.insert("name".to_string(), json!(element.name));
                map.insert(
                    "stereotype".to_string(),
                    json!(element.stereotype.into_iter().collect::<Vec<_>>()),
                );
                map.insert("taggedValue".to_string(), contained("taggedValue"));
                match element.kind {
                    Kind::Model | Kind::Package => {
                        map.insert("ownedElements".to_string(), contained("ownedElements"));
                        map.insert("owner".to_string(), owner());
                    }
                    Kind::PrimitiveType => {
                        map.insert("owner".to_string(), owner());
                    }
                    Kind::Class => {
                        if element.is_abstract {
                            map.insert("abstract".to_string(), json!(true));
                        }
                        map.insert("attributes".to_string(), contained("attributes"));
                        map.insert("generalizations".to_string(), contained("generalizations"));
                        map.insert("owner".to_string(), owner());
                    }
                    Kind::Property => {
                        map.insert("owner".to_string(), Value::Null);
                        map.insert("ttype".to_string(), reference("ttype"));
                    }
                    Kind::Association => {
                        map.insert("owner".to_string(), owner());
                        map.insert("source".to_string(), reference("source"));
                        map.insert("target".to_string(), reference("target"));
                    }
                    Kind::Generalization | Kind::TaggedValue => unreachable!(),
                }
            }
        }
        Value::Object(map)
    }
}

/// A projected document under `D1` mapped to `D2` by hand, from the two
/// `.ecore` files: every `ModelElement` (here `Model`, `Package`, `Class`,
/// `PrimitiveType`, `Association`, `Property`) gains `ownedComments: []`,
/// every `Class` gains `isLeaf: false`; `Generalization` and `TaggedValue`
/// gain nothing; the names read the same strings, now one register value
/// each. `si12::expected_d2` does the same for the running example, which
/// holds no `Package`.
pub fn to_d2(doc: &Value) -> Value {
    match doc {
        Value::Object(map) => {
            let mut out: Map<String, Value> = map
                .iter()
                .map(|(key, value)| (key.clone(), to_d2(value)))
                .collect();
            let class = map.get("eClass").and_then(Value::as_str);
            if matches!(
                class,
                Some("Model" | "Package" | "Class" | "PrimitiveType" | "Association" | "Property")
            ) {
                out.insert("ownedComments".to_string(), json!([]));
            }
            if class == Some("Class") {
                out.insert("isLeaf".to_string(), json!(false));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(to_d2).collect()),
        other => other.clone(),
    }
}

/// `si12::project`, which panics on an identity or a label served twice,
/// turned into an error: a read taken while a frame is half applied may
/// serve a name that is still being typed.
pub fn projected(doc: &Value) -> Result<Value, String> {
    std::panic::catch_unwind(|| project(doc))
        .map_err(|_| "the read serves one identity or one label twice".to_string())
}

/// The number of objects a served document holds: its served identities.
pub fn objects(doc: &Value) -> Result<usize, String> {
    std::panic::catch_unwind(|| labels(doc).len())
        .map_err(|_| "the read serves one identity or one label twice".to_string())
}

/// The object at `path` in a raw document.
pub fn at_path<'a>(doc: &'a Value, path: &[Step]) -> &'a Value {
    path.iter()
        .fold(doc, |value, step| &value[step.feature][step.pos])
}
