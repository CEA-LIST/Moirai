//! SI.12 rounds 3 and 4: copy-migration of generated SimpleUML models of
//! 100, 300 and 1,000 elements on three `model_node` processes over loopback,
//! and, as an observation beyond the claim, of 5,000.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.12.md`, sections "Round 3,
//! written before any rerun" and "Round 4, written before any rerun"; the
//! contract (M1 to M7) is restated in `si12/mod.rs`. Round 2 passed the
//! claim on the paper's running example, ten objects; this file asks the
//! same questions of models of 100 to 1,000 objects, and times the
//! migration while it does. Round 4 adds a second test, the same runs and
//! checks at 5,000 objects, each run stopped at [`RUN_CEILING`].
//!
//! The models. [`generate`] builds, from a fixed seed per size, a SimpleUML
//! model of exactly `size` objects under `D1`: model `Vehicle`, packages
//! (every third one nested in the package before it), and in each package a
//! primitive type, eight classes and two associations; classes hold
//! properties, generalizations and tagged values, some are abstract, some
//! carry the stereotype `entity`, some name their package as owner by text;
//! every property types itself by identity reference to a class or the
//! primitive type of its package, every generalization names an earlier
//! class, every association a source and a target. Every expected document
//! is computed from that description by the read-out's rules, never read off
//! the code under test.
//!
//! Per size and repetition, two runs, each on three fresh nodes (`si12-a`
//! serves `D1` and `D2`, `si12-b` too, `si12-c` serves `D1` alone, so `D2`
//! reaches it only through the copy's own history):
//!
//! - plain: `a` writes the model `m`, which `b` and `c` joined first; `a`
//!   migrates `m` to `D2` while every node is connected; `b` and `c` join the
//!   copy `m'` by id. Timed: the migrate call, the time until every node names
//!   `m'`, and the time until every node serves `m'` equal to `a`'s.
//! - concurrent: as plain up to the migration; then every node pauses every
//!   peer, `b` appends a character to the name of class `Class000_1` and makes
//!   it abstract, `c` adds the stereotype `persistent` to `Class000_0`, `a`
//!   migrates; every node resumes.
//!
//! The checks of a run, as the spec registers them:
//!
//! - K0, the premise and the generator: every node serves `m` as the
//!   generator's expectation under `D1`, in both reads, with exactly `size`
//!   objects.
//! - K1 (M1): the migrate call answers 2xx naming a model id other than
//!   `m`, and `a` lists it under `D2`'s digest.
//! - K2 (M2, M3, M4 on `a` at once; concurrent runs): right after its call
//!   `a` names `m'` with nothing refused, serves `m` as before, refuses an
//!   edit naming `m'`; `b`, cut off, names no migration.
//! - K3 (M2, M5): every node names `m'` and lists as refused exactly the
//!   edits concurrent with the cut (none in a plain run; `si12-b` 1 and 2 and
//!   `si12-c` 1 in a concurrent one). A concurrent run whose nodes do not
//!   take their peers back after the cut fails it as `K3-resume`.
//! - K4 (M4): every node serves `m`, in both reads, exactly as `a` served it
//!   when it cut.
//! - K5 (M3): every node refuses an edit of `m` naming `m'`, on the single
//!   route and (422) on the batch route.
//! - K6 (M6): every node serves one `m'`, equal to `a`'s, which is the
//!   mapping of `m`'s read at the cut and the generator's expectation under
//!   `D2` (added features at their defaults, the names as one register value
//!   each, identity references naming the copies), with `size` objects, in
//!   both reads, listed under `D2`'s digest.
//! - K7 (concurrent runs): the three concurrent edits are in neither `m` nor
//!   `m'` on any node. Since round 4, K7 first waits, up to the convergence
//!   bound, for `b` and `c` to serve a copy holding as many objects as `a`'s,
//!   so that it no longer depends on K6's poll having run, and says "copy not
//!   yet present" when they do not, apart from "serves b's concurrent edit".
//!
//! Every timing here is a loopback resource check (VALIDATION.md section 1),
//! not a performance number. Every size is reported, none is thresholded.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si12_scale_l3 \
//!     -- --ignored --exact \
//!     si12_r3_copy_migration_of_100_300_and_1000_element_models_on_three_nodes \
//!     --test-threads=1 --nocapture
//! ```
//!
//! The binary is `MOIRAI_E2E_INTERP_NODE_BIN` or `target/debug/examples/model_node`;
//! a missing one prints `E2E-SKIP si12-r3: <why>` and returns. A skip is not
//! a pass. `SI12_R3_REPS` (1 to 5) and `SI12_R3_SIZES` (a subset of
//! `100,300,1000`) shorten the schedule for the control arms only; the run
//! then prints `"registered": false` and cannot meet the prediction. The
//! 5,000-object test, `si12_r4_copy_migration_of_a_5000_element_model_on_three_nodes`,
//! reads neither; a bare `--ignored` runs both tests, so name the one meant.
//!
//! A run that stops before its migration keeps its nodes' log tails in its
//! line, and an HTTP error carries its sources (a timeout, a connection the
//! node closed), which round 3's debug-build runs lacked (OB7).

mod si12;

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_network::log_id::LogId;
use moirai_semantics::{LeafRule, MergeRule, Shape};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};
use serde_json::{Map, Value, json};

use si12::{
    D1, D2, Uml, digest, ids_by_label, labels, migrated, not_migrated, project, project_with,
    status_keys, strip_ids,
};

#[path = "nw02/launch.rs"]
mod launch;

/// The registered sizes, in objects.
const SIZES: [usize; 3] = [100, 300, 1_000];
/// The registered repetitions of each case at each size.
const REPS: usize = 5;
/// Round 4's observation beyond the claim: one size, five repetitions of
/// each case.
const R4_SIZE: usize = 5_000;
const R4_REPS: usize = 5;
/// How long one run of the 5,000-object test may take, from before its
/// nodes start. A run that reaches it is stopped at the wait it is in,
/// recorded with what it observed until then, and never retried; the runs
/// of the claim test have no ceiling.
const RUN_CEILING: Duration = Duration::from_secs(40 * 60);
/// The generator's seed; each size draws from `SEED ^ size`.
const SEED: u64 = 0x5112_0003_5112_0003;
/// Classes per package, associations per package.
const CLASSES: usize = 8;
const ASSOCIATIONS: usize = 2;
/// Operations per batch when `a` writes `m`.
const BATCH: usize = 250;
/// `MAX_STATE_TRANSFER_BYTES` in `moirai-network`: the bound the node holds
/// one frame it builds to, and the one the implementation documents for
/// the cut.
const FRAME_BOUND: usize = 768 * 1024;
/// `MAX_FRAME_BYTES` in `moirai-relay`: what a relay reads as one frame.
const RELAY_FRAME: usize = 1024 * 1024;

const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(300);
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(50);
const LOG_ID: &str = "5112c0de5112c0de5112c0de5112c003";

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;

// ---------------------------------------------------------------------------
// The generator
// ---------------------------------------------------------------------------

/// The SimpleUML classes a generated model instantiates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
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
    fn class(self) -> &'static str {
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
struct Step {
    feature: &'static str,
    pos: usize,
    class: &'static str,
}

/// One object of a generated model, as the generator writes it.
#[derive(Clone, Debug)]
struct Element {
    kind: Kind,
    /// `None` for a generalization, which has no name.
    name: Option<String>,
    /// From the root; empty for the root.
    path: Vec<Step>,
    parent: Option<usize>,
    /// Contained objects, by containment, in order.
    children: BTreeMap<&'static str, Vec<usize>>,
    is_abstract: bool,
    stereotype: Option<&'static str>,
    /// `Packageable.owner`, a reference by text.
    owner_text: Option<String>,
    is_substitutable: bool,
    /// `TaggedValue.value`.
    value: Option<String>,
    /// References by identity, by feature, to the object each names.
    refs: BTreeMap<&'static str, usize>,
}

/// A generated model: its objects, the root first, each after its owner.
struct Generated {
    size: usize,
    elements: Vec<Element>,
    /// The classes of each package, in order, by package.
    classes_of: BTreeMap<usize, Vec<usize>>,
    /// The primitive type of each package.
    type_of: BTreeMap<usize, usize>,
    /// Every property, with the package its class sits in.
    properties: Vec<(usize, usize)>,
}

fn below(rng: &mut ChaCha8Rng, n: usize) -> usize {
    (rng.next_u64() % n as u64) as usize
}

impl Generated {
    fn full(&self) -> bool {
        self.elements.len() >= self.size
    }

    /// One more object, of `kind`, at the end of `parent`'s `feature`.
    fn add(
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
    fn label(&self, index: usize) -> String {
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
    fn census(&self) -> BTreeMap<&'static str, usize> {
        let mut out = BTreeMap::new();
        for element in &self.elements {
            *out.entry(element.kind.class()).or_insert(0) += 1;
        }
        out
    }

    /// The first two classes of the first package, `Class000_0` (abstract,
    /// stereotype `entity`) and `Class000_1` (not abstract): what the
    /// concurrent edits touch.
    fn first_classes(&self) -> (usize, usize) {
        let classes = &self.classes_of[&1];
        (classes[0], classes[1])
    }
}

/// The model of `size` objects, from the seed of that size.
fn generate(size: usize) -> Generated {
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
fn class_at(path: &[Step]) -> &'static str {
    path.last().map_or("Model", |step| step.class)
}

/// `inner` addressed to the object at `path`, as `si12::Uml::on` addresses
/// one of the running example.
fn on(uml: &Uml, path: &[Step], inner: InstanceOp) -> ModelOp {
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
fn insert(uml: &Uml, path: &[Step], step: &Step) -> ModelOp {
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
fn leaf(uml: &Uml, path: &[Step], feature: &str, leaf: LeafOp) -> ModelOp {
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
fn text(uml: &Uml, path: &[Step], feature: &str, text: &str) -> Vec<ModelOp> {
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
    fn structure(&self, uml: &Uml) -> Vec<ModelOp> {
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
    fn links(&self, uml: &Uml, ids: &BTreeMap<String, String>) -> Result<Vec<ModelOp>, String> {
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
    fn expected_d1(&self) -> Value {
        self.expected(0)
    }

    fn expected(&self, index: usize) -> Value {
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
fn to_d2(doc: &Value) -> Value {
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
fn projected(doc: &Value) -> Result<Value, String> {
    std::panic::catch_unwind(|| project(doc))
        .map_err(|_| "the read serves one identity or one label twice".to_string())
}

/// The number of objects a served document holds: its served identities.
fn objects(doc: &Value) -> Result<usize, String> {
    std::panic::catch_unwind(|| labels(doc).len())
        .map_err(|_| "the read serves one identity or one label twice".to_string())
}

/// The object at `path` in a raw document.
fn at_path<'a>(doc: &'a Value, path: &[Step]) -> &'a Value {
    path.iter()
        .fold(doc, |value, step| &value[step.feature][step.pos])
}

// ---------------------------------------------------------------------------
// Three nodes
// ---------------------------------------------------------------------------

fn node_binary() -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var("MOIRAI_E2E_INTERP_NODE_BIN") {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "MOIRAI_E2E_INTERP_NODE_BIN points at `{raw}`, which is not a file"
            ))
        };
    }
    let built =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/examples/model_node");
    if built.is_file() {
        Ok(built)
    } else {
        Err(format!(
            "no `model_node` at {}; build it with `cargo build -p moirai-interp --features \
             network --example model_node`",
            built.display()
        ))
    }
}

struct Node {
    id: String,
    http: u16,
    child: Child,
    log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }
}

struct Cluster {
    nodes: Vec<Node>,
    scratch: PathBuf,
    http: reqwest::blocking::Client,
    /// When the run must stop, for a run that has a ceiling.
    ceiling: Option<Instant>,
    /// Whether a wait gave up at the ceiling rather than at its own bound.
    stopped: Cell<bool>,
}

/// An error and every source under it: `reqwest` names the request in its
/// own message and keeps the reason (a timeout, a connection the node
/// closed) in its sources.
fn with_sources(err: &dyn std::error::Error) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
        let _ = fs::remove_dir_all(&self.scratch);
    }
}

impl Cluster {
    /// `si12-a` and `si12-b` serve `D1` and `D2`, `si12-c` serves `D1` alone.
    fn start(binary: &Path, run: &str, ceiling: Option<Instant>) -> Result<Self, String> {
        let scratch =
            std::env::temp_dir().join(format!("moirai-si12-r3-{}-{run}", std::process::id()));
        let _ = fs::remove_dir_all(&scratch);
        let mut specs = Vec::new();
        for (id, served) in [
            ("si12-a", vec![("d1", D1), ("d2", D2)]),
            ("si12-b", vec![("d1", D1), ("d2", D2)]),
            ("si12-c", vec![("d1", D1)]),
        ] {
            let dir = scratch.join(format!("{id}-metamodels"));
            fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
            for (name, text) in served {
                fs::write(dir.join(format!("{name}.json")), text).map_err(|err| err.to_string())?;
            }
            let dir = dir.to_string_lossy().into_owned();
            specs.push(launch::NodeSpec::new(
                id,
                &[("LOG_ID", LOG_ID), ("METAMODEL_DIR", dir.as_str())],
            ));
        }
        let nodes: Vec<Node> = launch::launch(binary, &scratch, &specs)?
            .into_iter()
            .map(|node| Node {
                id: node.id,
                http: node.http,
                child: node.child,
                log: node.log,
            })
            .collect();
        let cluster = Cluster {
            nodes,
            scratch,
            http: reqwest::blocking::Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .map_err(|err| err.to_string())?,
            ceiling,
            stopped: Cell::new(false),
        };
        cluster.await_health()?;
        cluster.await_mesh()?;
        Ok(cluster)
    }

    /// When a wait of `wait` that starts now gives up: after `wait`, or at
    /// the run's ceiling when that comes first.
    fn deadline(&self, wait: Duration) -> Instant {
        let own = Instant::now() + wait;
        self.ceiling.map_or(own, |ceiling| ceiling.min(own))
    }

    /// Whether a wait that gave up at `deadline` gave up at the run's
    /// ceiling; recorded when it did.
    fn at_ceiling(&self, deadline: Instant) -> bool {
        let reached = self.ceiling == Some(deadline);
        if reached {
            self.stopped.set(true);
        }
        reached
    }

    fn tails(&self) -> String {
        self.nodes
            .iter()
            .map(|node| {
                format!(
                    "--- {} (alarms: {})\n{}",
                    node.id,
                    launch::alarms(&node.log),
                    launch::tail(&node.log, 8)
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn get(&self, node: usize, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .http
            .get(self.nodes[node].url(path))
            .send()
            .map_err(|err| {
                format!(
                    "GET {path} on {}: {}",
                    self.nodes[node].id,
                    with_sources(&err)
                )
            })?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| with_sources(&err))?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn post(&self, node: usize, path: &str, body: String) -> Result<(u16, Value), String> {
        let response = self
            .http
            .post(self.nodes[node].url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| {
                format!(
                    "POST {path} on {}: {}",
                    self.nodes[node].id,
                    with_sources(&err)
                )
            })?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| with_sources(&err))?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn await_health(&self) -> Result<(), String> {
        for (index, node) in self.nodes.iter().enumerate() {
            let deadline = Instant::now() + HEALTH_TIMEOUT;
            loop {
                if let Ok((200, body)) = self.get(index, "/api/health") {
                    if body["replica_id"] == json!(node.id) {
                        break;
                    }
                    return Err(format!("the HTTP port of {} answered as {body}", node.id));
                }
                if Instant::now() > deadline {
                    return Err(format!(
                        "{} never answered /api/health:\n{}",
                        node.id,
                        self.tails()
                    ));
                }
                std::thread::sleep(POLL);
            }
        }
        Ok(())
    }

    fn connected(&self, node: usize) -> usize {
        match self.get(node, "/api/peers") {
            Ok((200, peers)) => peers["peers"]
                .as_array()
                .map(|list| {
                    list.iter()
                        .filter(|peer| peer["status"] == Value::String("Connected".into()))
                        .count()
                })
                .unwrap_or(0),
            _ => 0,
        }
    }

    fn await_mesh(&self) -> Result<(), String> {
        let deadline = self.deadline(MESH_TIMEOUT);
        loop {
            if (0..self.nodes.len()).all(|node| self.connected(node) + 1 >= self.nodes.len()) {
                return Ok(());
            }
            if Instant::now() > deadline {
                let when = if self.at_ceiling(deadline) {
                    " before the run's ceiling"
                } else {
                    ""
                };
                return Err(format!("the mesh never formed{when}:\n{}", self.tails()));
            }
            std::thread::sleep(POLL);
        }
    }

    fn control(&self, node: usize, path: &str) -> Result<(), String> {
        let (status, body) = self.post(node, path, String::new())?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!(
                "{path} on {} answered {status}: {body}",
                self.nodes[node].id
            ))
        }
    }

    fn pause_all(&self) -> Result<(), String> {
        (0..self.nodes.len()).try_for_each(|node| self.control(node, "/api/pause-all"))
    }

    fn resume_all(&self) -> Result<(), String> {
        (0..self.nodes.len()).try_for_each(|node| self.control(node, "/api/resume-all"))?;
        self.await_mesh()
    }

    /// Create a model on `node` under `digest`, or join `model` by id
    /// naming it; answers the model id.
    fn register(&self, node: usize, model: Option<&str>, digest: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": digest }),
            None => json!({ "metamodel_id": digest }),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "registering on {} answered {status}: {answer}",
                self.nodes[node].id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    /// Join `model` on `node` by its id alone; a node that hosts it already
    /// (409) is as good.
    fn join_by_id(&self, node: usize, model: &str) -> Result<(), String> {
        let (status, answer) = self.post(
            node,
            "/api/models",
            json!({ "model_id": model }).to_string(),
        )?;
        match status {
            200 | 201 | 409 => Ok(()),
            _ => Err(format!(
                "joining {model} by id on {} answered {status}: {answer}",
                self.nodes[node].id
            )),
        }
    }

    /// Post one operation; answers the node's `OpResult`.
    fn post_op(&self, node: usize, model: &str, op: &ModelOp) -> Result<Value, String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 {
            return Err(format!(
                "{} answered {status} to an operation on {model}: {answer}",
                self.nodes[node].id
            ));
        }
        Ok(answer)
    }

    /// Post operations one by one; each must be applied.
    fn submit(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        for op in ops {
            let answer = self.post_op(node, model, op)?;
            if answer["success"] != Value::Bool(true) {
                return Err(format!(
                    "{} refused an operation on {model}: {answer}",
                    self.nodes[node].id
                ));
            }
        }
        Ok(())
    }

    /// Post operations in batches of [`BATCH`]; each batch must be applied
    /// whole. A batch answered 504 `expired` was applied nowhere, by the
    /// route's contract, and is posted again, at most three times. A run
    /// with a ceiling posts no batch once it is past it.
    fn submit_batches(&self, node: usize, model: &str, ops: &[ModelOp]) -> Result<(), String> {
        let batches = ops.len().div_ceil(BATCH);
        for (index, chunk) in ops.chunks(BATCH).enumerate() {
            let batch = index + 1;
            if self.ceiling.is_some_and(|ceiling| Instant::now() > ceiling) {
                self.stopped.set(true);
                return Err(format!(
                    "the run reached its ceiling while {} was writing {model}, before batch \
                     {batch} of {batches}",
                    self.nodes[node].id
                ));
            }
            let body = json!({ "ops": chunk }).to_string();
            let mut expired = 0;
            loop {
                let (status, answer) = self
                    .post(node, &format!("/api/model/{model}/ops"), body.clone())
                    .map_err(|why| format!("batch {batch} of {batches}: {why}"))?;
                if status == 200 && answer["applied"] == json!(chunk.len()) {
                    break;
                }
                if status == 504 && answer["outcome"] == json!("expired") && expired < 3 {
                    expired += 1;
                    continue;
                }
                return Err(format!(
                    "{} answered batch {batch} of {batches}, {} operations, on {model} with \
                     {status}: {answer}",
                    self.nodes[node].id,
                    chunk.len()
                ));
            }
        }
        Ok(())
    }

    fn read(&self, node: usize, path: &str) -> Result<Value, String> {
        match self.get(node, path)? {
            (200, body) => Ok(body),
            (status, body) => Err(format!(
                "{} answered {path} with {status}: {body}",
                self.nodes[node].id
            )),
        }
    }

    fn identified(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/state/identified"))
    }

    fn plain(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/state"))
    }

    fn metrics(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/metrics"))
    }

    /// M2 over HTTP, the two keys the contract fixes.
    fn migration(&self, node: usize, model: &str) -> Result<Value, String> {
        self.read(node, &format!("/api/model/{model}/migration"))
            .map(|status| status_keys(&status))
    }

    /// Whether `node` lists `model` under `D2`'s digest.
    fn lists_under_d2(&self, node: usize, model: &str) -> Result<(), String> {
        let body = self.read(node, "/api/models")?;
        let listed = body["models"]
            .as_array()
            .ok_or_else(|| format!("no `models` in {body}"))?
            .iter()
            .find(|entry| entry["model_id"] == json!(model))
            .map(|entry| entry["metamodel_id"].clone());
        let digest_of = |value: &Value| match value {
            Value::String(digest) => Some(digest.clone()),
            Value::Object(map) => map
                .get("digest")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        };
        match listed {
            Some(metamodel) if digest_of(&metamodel) == Some(digest(D2)) => Ok(()),
            other => Err(format!(
                "{} lists {model} as {other:?}, not under D2's digest",
                self.nodes[node].id
            )),
        }
    }

    /// Poll until `check` holds on every node, or answer the last reason it
    /// did not.
    fn until(&self, what: &str, check: impl Fn(usize) -> Result<(), String>) -> Result<(), String> {
        let deadline = self.deadline(CONVERGE_TIMEOUT);
        loop {
            let outcome: Result<(), String> = (0..self.nodes.len()).try_for_each(&check);
            match outcome {
                Ok(()) => return Ok(()),
                Err(why) if Instant::now() > deadline => {
                    if self.at_ceiling(deadline) {
                        return Err(format!("{what}, stopped at the run's ceiling: {why}"));
                    }
                    return Err(format!(
                        "{what}, not within {} s: {why}",
                        CONVERGE_TIMEOUT.as_secs()
                    ));
                }
                Err(_) => std::thread::sleep(POLL),
            }
        }
    }

    /// The size a node printed for `model`'s log when it served or refused
    /// a state transfer of it, and which it did.
    fn transfer_line(&self, node: usize, model: &str) -> Option<(usize, &'static str)> {
        let text = fs::read_to_string(&self.nodes[node].log).ok()?;
        text.lines()
            .filter(|line| line.contains(model) && line.contains("state transfer"))
            .find_map(|line| {
                if let Some((_, rest)) = line.split_once("the log is ") {
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    digits.parse().ok().map(|bytes| (bytes, "refused"))
                } else if line.contains("serving a state transfer") {
                    let (_, rest) = line.rsplit_once(": ")?;
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    digits.parse().ok().map(|bytes| (bytes, "served"))
                } else {
                    None
                }
            })
    }
}

// ---------------------------------------------------------------------------
// One run
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Plain,
    Concurrent,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Case::Plain => "plain",
            Case::Concurrent => "concurrent",
        }
    }
}

/// What one run observed: the checks in the order they were made, and the
/// measures.
struct Outcome {
    size: usize,
    case: Case,
    rep: usize,
    /// Why the run stopped before its migration, when it did: it is then
    /// not counted either way.
    setup: Option<String>,
    checks: Vec<(String, bool, String)>,
    measures: Map<String, Value>,
}

impl Outcome {
    fn check(&mut self, name: &str, result: Result<(), String>) {
        let (ok, detail) = match result {
            Ok(()) => (true, String::new()),
            Err(why) => (false, why),
        };
        self.checks.push((name.to_string(), ok, detail));
    }

    fn measure(&mut self, key: &str, value: Value) {
        self.measures.insert(key.to_string(), value);
    }

    fn ms(&mut self, key: &str, since: Instant) {
        self.measure(key, json!(since.elapsed().as_secs_f64() * 1_000.0));
    }

    fn held(&self) -> bool {
        self.setup.is_none() && self.checks.iter().all(|(_, ok, _)| *ok)
    }

    fn line(&self) -> Value {
        let checks: Map<String, Value> = self
            .checks
            .iter()
            .map(|(name, ok, _)| (name.clone(), json!(ok)))
            .collect();
        let failed: Vec<Value> = self
            .checks
            .iter()
            .filter(|(_, ok, _)| !ok)
            .map(|(name, _, detail)| json!({ "check": name, "why": detail }))
            .collect();
        json!({
            "size": self.size,
            "case": self.case.name(),
            "rep": self.rep,
            "completed": self.setup.is_none(),
            "setup_error": self.setup,
            "every_check_held": self.held(),
            "checks": checks,
            "failed": failed,
            "measures": self.measures,
        })
    }
}

/// An edit of `m` every node must refuse once it has the cut: a character
/// into the model's name.
fn probe_edit(uml: &Uml) -> ModelOp {
    leaf(uml, &[], "name", LeafOp::InsertChar { pos: 0, ch: 'x' })
}

/// An edit of `m` refused naming `copy`, on the single and the batch route.
fn refused_edit(
    cluster: &Cluster,
    node: usize,
    model: &str,
    op: &ModelOp,
    copy: &str,
) -> Result<(), String> {
    let answer = cluster.post_op(node, model, op)?;
    let message = answer["message"].as_str().unwrap_or_default();
    if answer["success"] != Value::Bool(false) || !message.contains(copy) {
        return Err(format!(
            "{} answered an edit of the migrated model with {answer}; it must refuse it naming {copy}",
            cluster.nodes[node].id
        ));
    }
    let batch = json!({ "ops": [op] });
    let (status, answer) =
        cluster.post(node, &format!("/api/model/{model}/ops"), batch.to_string())?;
    if status != 422 {
        return Err(format!(
            "{} answered a batch on the migrated model with {status}: {answer}; it must refuse it (422)",
            cluster.nodes[node].id
        ));
    }
    Ok(())
}

/// Everything up to the migration: the nodes, `m` written on `a` and joined
/// by `b` and `c` first, and K0. Answers the cluster and `m`. When it stops
/// once the nodes are up, their log tails go in the run's line: the cluster
/// is dropped on the way out, and its directory with it.
fn set_up(
    binary: &Path,
    g: &Generated,
    d1: &Uml,
    ceiling: Option<Instant>,
    out: &mut Outcome,
) -> Result<(Cluster, String), String> {
    let run = format!("{}-{}-{}", out.size, out.case.name(), out.rep);
    let cluster =
        Cluster::start(binary, &run, ceiling).map_err(|why| format!("the nodes: {why}"))?;
    match write_m(&cluster, g, d1, out) {
        Ok(m) => Ok((cluster, m)),
        Err(why) => {
            out.setup = Some(why.clone());
            record_nodes(&cluster, out);
            Err(why)
        }
    }
}

/// `m` created on `a`, joined by `b` and `c`, written on `a`, and K0.
fn write_m(
    cluster: &Cluster,
    g: &Generated,
    d1: &Uml,
    out: &mut Outcome,
) -> Result<String, String> {
    let m = cluster.register(A, None, &digest(D1))?;
    cluster.register(B, Some(&m), &digest(D1))?;
    cluster.register(C, Some(&m), &digest(D1))?;
    cluster.until("m's Install on every node", |node| {
        match cluster.metrics(node, &m)?["delivered_ops"].as_u64() {
            Some(delivered) if delivered >= 1 => Ok(()),
            other => Err(format!("node {node} delivered {other:?}")),
        }
    })?;
    let structure = g.structure(d1);
    let built = Instant::now();
    cluster.submit_batches(A, &m, &structure)?;
    let ids = ids_by_label(&cluster.identified(A, &m)?);
    let links = g.links(d1, &ids)?;
    cluster.submit_batches(A, &m, &links)?;
    out.measure("m_ops_posted", json!(structure.len() + links.len()));
    let expected = g.expected_d1();
    let expected_plain = strip_ids(&expected);
    cluster.until("m as generated on every node (K0)", |node| {
        let doc = cluster.identified(node, &m)?;
        if projected(&doc)? != expected {
            return Err(format!(
                "node {node} does not serve the generated model yet"
            ));
        }
        if project_with(&cluster.plain(node, &m)?, &labels(&doc)) != expected_plain {
            return Err(format!("node {node}'s default read of m"));
        }
        Ok(())
    })?;
    out.ms("build_ms", built);
    // K0, the count: what every node serves, against the stated size.
    let mut counted = Vec::new();
    for node in [A, B, C] {
        counted.push(objects(&cluster.identified(node, &m)?)?);
    }
    out.measure("elements", json!(counted));
    out.check(
        "K0",
        if counted.iter().all(|n| *n == g.size) {
            Ok(())
        } else {
            Err(format!(
                "the nodes serve {counted:?} objects; the generator states {}",
                g.size
            ))
        },
    );
    let metrics = cluster.metrics(A, &m)?;
    out.measure("m_ops_applied_on_a", metrics["ops_applied"].clone());
    out.measure("m_delivered_on_a", metrics["delivered_ops"].clone());
    Ok(m)
}

/// What a run records last about its nodes, however it ends once they are
/// up: their log tails when it did not hold every check or stopped in
/// set-up, and, for a run with a ceiling, whether the ceiling stopped it.
fn record_nodes(cluster: &Cluster, out: &mut Outcome) {
    if cluster.ceiling.is_some() {
        out.measure("stopped_at_ceiling", json!(cluster.stopped.get()));
    }
    if !out.held() {
        out.measure("node_log_tails", json!(cluster.tails()));
    }
}

/// The migrate call on `a`: K1's answer, and the copy's id, read off the
/// answer or, when the call timed out (504, or no answer), off `a`'s
/// migration status, where a migration still under way shows once it is
/// done; that status is polled for the convergence bound, or until the
/// run's ceiling in a run that has one, and the time until it names the
/// copy is `migrated_ms`. Any other answer is a refusal: nothing was
/// written, and the run stops there.
fn migrate_on_a(cluster: &Cluster, m: &str, out: &mut Outcome) -> Result<String, String> {
    let started = Instant::now();
    let (status, answer) = cluster
        .post(
            A,
            &format!("/api/model/{m}/migrate"),
            json!({ "metamodel_id": digest(D2) }).to_string(),
        )
        .unwrap_or_else(|why| (0, Value::String(why)));
    out.ms("migrate_ms", started);
    out.measure("migrate_status", json!(status));
    let named = answer["model_id"].as_str().map(str::to_string);
    let answered = match (&named, (200..300).contains(&status)) {
        (Some(copy), true) if copy != m && LogId::parse(copy).is_ok() => Ok(()),
        _ => Err(format!("the migrate call answered {status}: {answer}")),
    };
    let copy = match named {
        Some(copy) if answered.is_ok() => copy,
        _ if status != 504 && status != 0 => {
            out.check("K1", answered);
            return Err(format!("the migrate call answered {status}"));
        }
        _ => {
            let deadline = cluster
                .ceiling
                .unwrap_or_else(|| Instant::now() + CONVERGE_TIMEOUT);
            loop {
                if let Ok(status) = cluster.migration(A, m)
                    && let Some(copy) = status["migrated_to"].as_str()
                {
                    out.ms("migrated_ms", started);
                    break copy.to_string();
                }
                if Instant::now() > deadline {
                    out.check("K1", answered);
                    if cluster.at_ceiling(deadline) {
                        return Err(
                            "a named no successor of m before the run's ceiling".to_string()
                        );
                    }
                    return Err("a names no successor of m".to_string());
                }
                std::thread::sleep(POLL);
            }
        }
    };
    out.check(
        "K1",
        answered.and_then(|()| cluster.lists_under_d2(A, &copy)),
    );
    Ok(copy)
}

/// K3: every node names `copy` and lists exactly `refused`, polled until it
/// does. A node that names `copy` and does not list an edit concurrent with
/// the cut that it wrote itself fails at once rather than at the timeout:
/// it delivered that edit before it could deliver the cut, so by M5 the edit
/// is listed from the moment the cut is.
fn reported(cluster: &Cluster, m: &str, copy: &str, refused: &[(&str, u64)]) -> Result<(), String> {
    let expected = migrated(copy, refused);
    let deadline = cluster.deadline(CONVERGE_TIMEOUT);
    loop {
        let mut waiting = None;
        for node in [A, B, C] {
            let status = match cluster.migration(node, m) {
                Ok(status) => status,
                Err(why) => {
                    waiting = Some(why);
                    continue;
                }
            };
            if status == expected {
                continue;
            }
            if status["migrated_to"] == json!(copy) {
                let listed = status["refused"].as_array().cloned().unwrap_or_default();
                if let Some((origin, seq)) = refused.iter().find(|(origin, seq)| {
                    *origin == cluster.nodes[node].id
                        && !listed.contains(&json!({ "origin": origin, "seq": seq }))
                }) {
                    return Err(format!(
                        "node {node} names {copy} and does not list {origin}@{seq}, its own \
                         edit concurrent with the cut: {status}"
                    ));
                }
            }
            waiting = Some(format!("node {node} reads {status}"));
        }
        match waiting {
            None => return Ok(()),
            Some(why) if Instant::now() > deadline => {
                if cluster.at_ceiling(deadline) {
                    return Err(format!(
                        "every node names m' and the refused edits, stopped at the run's \
                         ceiling: {why}"
                    ));
                }
                return Err(format!(
                    "every node names m' and the refused edits, not within {} s: {why}",
                    CONVERGE_TIMEOUT.as_secs()
                ));
            }
            Some(_) => std::thread::sleep(POLL),
        }
    }
}

/// K3 to K6, read once every node names the copy.
#[allow(clippy::too_many_arguments)]
fn after_the_cut(
    cluster: &Cluster,
    g: &Generated,
    d1: &Uml,
    m: &str,
    copy: &str,
    cut: (&Value, &Value),
    refused: &[(&str, u64)],
    out: &mut Outcome,
) {
    let (cut_doc, cut_plain) = cut;
    let started = Instant::now();
    out.check("K3", reported(cluster, m, copy, refused));
    out.ms("k3_ms", started);
    out.check(
        "K4",
        [A, B, C].into_iter().try_for_each(|node| {
            if &cluster.identified(node, m)? != cut_doc {
                return Err(format!(
                    "node {node} serves m otherwise than a served it when it cut"
                ));
            }
            if &cluster.plain(node, m)? != cut_plain {
                return Err(format!(
                    "node {node}'s default read of m moved after the cut"
                ));
            }
            Ok(())
        }),
    );
    let probe = probe_edit(d1);
    out.check(
        "K5",
        [A, B, C]
            .into_iter()
            .try_for_each(|node| refused_edit(cluster, node, m, &probe, copy)),
    );
    let k6 = (|| -> Result<(), String> {
        let first = cluster.identified(A, copy)?;
        let expected = to_d2(&g.expected_d1());
        let mapped = to_d2(&projected(cut_doc)?);
        if projected(&first)? != mapped {
            return Err("a's copy is not m's read at the cut mapped to D2".to_string());
        }
        if mapped != expected {
            return Err("m's read at the cut is not the generated model".to_string());
        }
        if objects(&first)? != g.size {
            return Err(format!("a's copy holds {} objects", objects(&first)?));
        }
        cluster.until("every node serves a's copy (K6)", |node| {
            if cluster.identified(node, copy)? == first {
                Ok(())
            } else {
                Err(format!("node {node} serves another copy"))
            }
        })?;
        let expected_plain = strip_ids(&expected);
        for node in [A, B, C] {
            if project_with(&cluster.plain(node, copy)?, &labels(&first)) != expected_plain {
                return Err(format!("node {node}'s default read of the copy"));
            }
        }
        cluster.until("every node lists m' under D2 (K6)", |node| {
            cluster.lists_under_d2(node, copy)
        })
    })();
    out.check("K6", k6);
}

/// The sizes of what the migration wrote, read off `a`.
fn sizes(cluster: &Cluster, copy: &str, cut: (&Value, &Value), out: &mut Outcome) {
    let (cut_doc, cut_plain) = cut;
    // The cut as 33c932f writes it: the successor's id and the two reads
    // `a` served when it cut, which K4 holds every node to.
    let op = ModelOp::Cut {
        migrated_to: copy.to_string(),
        identified: cut_doc.clone(),
        plain: cut_plain.clone(),
    };
    let cut_bytes = serde_json::to_vec(&op)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    out.measure("cut_op_bytes", json!(cut_bytes));
    out.measure(
        "cut_margin_to_768KiB",
        json!(FRAME_BOUND as i64 - cut_bytes as i64),
    );
    out.measure(
        "cut_margin_to_relay_1MiB",
        json!(RELAY_FRAME as i64 - cut_bytes as i64),
    );
    out.measure(
        "m_identified_bytes",
        json!(serde_json::to_vec(cut_doc).map(|b| b.len()).unwrap_or(0)),
    );
    out.measure(
        "m_plain_bytes",
        json!(serde_json::to_vec(cut_plain).map(|b| b.len()).unwrap_or(0)),
    );
    if let Ok(doc) = cluster.identified(A, copy) {
        out.measure(
            "copy_identified_bytes",
            json!(serde_json::to_vec(&doc).map(|b| b.len()).unwrap_or(0)),
        );
    }
    if let Ok(metrics) = cluster.metrics(A, copy) {
        out.measure("copy_ops_applied_on_a", metrics["ops_applied"].clone());
        out.measure("copy_delivered_on_a", metrics["delivered_ops"].clone());
    }
    match cluster.transfer_line(A, copy) {
        Some((bytes, how)) => {
            out.measure("copy_log_bytes", json!(bytes));
            out.measure("copy_state_transfer", json!(how));
        }
        None => {
            out.measure("copy_log_bytes", Value::Null);
            out.measure("copy_state_transfer", Value::Null);
        }
    }
}

/// When a run that may take `ceiling` from now must stop, recorded in its
/// line as `ceiling_s`.
fn start_ceiling(ceiling: Option<Duration>, out: &mut Outcome) -> Option<Instant> {
    let ceiling = ceiling?;
    out.measure("ceiling_s", json!(ceiling.as_secs()));
    Some(Instant::now() + ceiling)
}

fn plain_run(
    binary: &Path,
    g: &Generated,
    d1: &Uml,
    rep: usize,
    ceiling: Option<Duration>,
) -> Outcome {
    let mut out = Outcome {
        size: g.size,
        case: Case::Plain,
        rep,
        setup: None,
        checks: Vec::new(),
        measures: Map::new(),
    };
    let ceiling = start_ceiling(ceiling, &mut out);
    let (cluster, m) = match set_up(binary, g, d1, ceiling, &mut out) {
        Ok(ready) => ready,
        Err(why) => {
            out.setup = Some(why);
            return out;
        }
    };
    let (cut_doc, cut_plain) = match (cluster.identified(A, &m), cluster.plain(A, &m)) {
        (Ok(doc), Ok(plain)) => (doc, plain),
        (Err(why), _) | (_, Err(why)) => {
            out.setup = Some(format!("a's reads before the cut: {why}"));
            record_nodes(&cluster, &mut out);
            return out;
        }
    };
    let started = Instant::now();
    let copy = match migrate_on_a(&cluster, &m, &mut out) {
        Ok(copy) => copy,
        Err(why) => {
            out.check("K6", Err(format!("no copy to read: {why}")));
            record_nodes(&cluster, &mut out);
            return out;
        }
    };
    let joined = [B, C]
        .into_iter()
        .try_for_each(|node| cluster.join_by_id(node, &copy));
    // The agreement: every node names the copy, then every node serves the
    // copy `a` serves.
    let agreed = (|| -> Result<(), String> {
        joined?;
        let first = cluster.identified(A, &copy)?;
        let deadline = cluster.deadline(CONVERGE_TIMEOUT);
        let mut named_everywhere = false;
        loop {
            if !named_everywhere {
                named_everywhere = [A, B, C].into_iter().all(|node| {
                    cluster
                        .migration(node, &m)
                        .is_ok_and(|status| status["migrated_to"] == json!(copy))
                });
                if named_everywhere {
                    out.ms("cut_everywhere_ms", started);
                }
            }
            if named_everywhere
                && [B, C].into_iter().all(|node| {
                    cluster
                        .identified(node, &copy)
                        .is_ok_and(|doc| doc == first)
                })
            {
                out.ms("agree_ms", started);
                return Ok(());
            }
            if Instant::now() > deadline {
                if cluster.at_ceiling(deadline) {
                    return Err(
                        "the copy was not served equal on every node before the run's ceiling"
                            .to_string(),
                    );
                }
                return Err(format!(
                    "the copy was not served equal on every node within {} s",
                    CONVERGE_TIMEOUT.as_secs()
                ));
            }
            std::thread::sleep(POLL);
        }
    })();
    if let Err(why) = agreed {
        out.check("K6-agree", Err(why));
    }
    after_the_cut(
        &cluster,
        g,
        d1,
        &m,
        &copy,
        (&cut_doc, &cut_plain),
        &[],
        &mut out,
    );
    sizes(&cluster, &copy, (&cut_doc, &cut_plain), &mut out);
    record_nodes(&cluster, &mut out);
    out
}

fn concurrent_run(
    binary: &Path,
    g: &Generated,
    d1: &Uml,
    rep: usize,
    ceiling: Option<Duration>,
) -> Outcome {
    let mut out = Outcome {
        size: g.size,
        case: Case::Concurrent,
        rep,
        setup: None,
        checks: Vec::new(),
        measures: Map::new(),
    };
    let ceiling = start_ceiling(ceiling, &mut out);
    let (cluster, m) = match set_up(binary, g, d1, ceiling, &mut out) {
        Ok(ready) => ready,
        Err(why) => {
            out.setup = Some(why);
            return out;
        }
    };
    let (stereotyped, renamed) = g.first_classes();
    let renamed_path = g.elements[renamed].path.clone();
    let renamed_name = g.elements[renamed].name.clone().unwrap_or_default();
    let stereotyped_path = g.elements[stereotyped].path.clone();
    // The partition, and the edits concurrent with the cut.
    let partitioned = (|| -> Result<(Value, Value), String> {
        cluster.pause_all()?;
        cluster.submit(
            B,
            &m,
            &[
                leaf(
                    d1,
                    &renamed_path,
                    "name",
                    LeafOp::InsertChar {
                        pos: renamed_name.chars().count(),
                        ch: 's',
                    },
                ),
                leaf(d1, &renamed_path, "abstract", LeafOp::Enable),
            ],
        )?;
        cluster.submit(
            C,
            &m,
            &[leaf(
                d1,
                &stereotyped_path,
                "stereotype",
                LeafOp::Add(Scalar::text("persistent")),
            )],
        )?;
        let b_reads = at_path(&cluster.identified(B, &m)?, &renamed_path)["name"].clone();
        if b_reads != json!(format!("{renamed_name}s")) {
            return Err(format!(
                "b reads {b_reads} for its own rename before the merge"
            ));
        }
        Ok((cluster.identified(A, &m)?, cluster.plain(A, &m)?))
    })();
    let (cut_doc, cut_plain) = match partitioned {
        Ok(reads) => reads,
        Err(why) => {
            out.setup = Some(format!("the partition: {why}"));
            record_nodes(&cluster, &mut out);
            return out;
        }
    };
    let copy = match migrate_on_a(&cluster, &m, &mut out) {
        Ok(copy) => copy,
        Err(why) => {
            out.check("K6", Err(format!("no copy to read: {why}")));
            record_nodes(&cluster, &mut out);
            return out;
        }
    };
    let k2 = (|| -> Result<(), String> {
        let status = cluster.migration(A, &m)?;
        if status != migrated(&copy, &[]) {
            return Err(format!("right after its call a reads {status}"));
        }
        if cluster.identified(A, &m)? != cut_doc || cluster.plain(A, &m)? != cut_plain {
            return Err("a's reads of m moved at its own cut".to_string());
        }
        refused_edit(&cluster, A, &m, &probe_edit(d1), &copy)?;
        if cluster.migration(B, &m)? != not_migrated() {
            return Err("b, cut off, names a migration it has not received".to_string());
        }
        Ok(())
    })();
    out.check("K2", k2);
    // From the migrate call on, nothing is set-up any more: nodes that do
    // not take their peers back once the cut is out (a frame one of them
    // cannot read, say) fail the run, and it goes on to its other checks.
    let resumed = Instant::now();
    match cluster.resume_all() {
        Ok(()) => out.ms("resume_ms", resumed),
        Err(why) => out.check("K3-resume", Err(format!("after the cut: {why}"))),
    }
    let joined = [B, C]
        .into_iter()
        .try_for_each(|node| cluster.join_by_id(node, &copy));
    if let Err(why) = joined {
        out.check("K6-join", Err(why));
    }
    after_the_cut(
        &cluster,
        g,
        d1,
        &m,
        &copy,
        (&cut_doc, &cut_plain),
        &[("si12-b", 1), ("si12-b", 2), ("si12-c", 1)],
        &mut out,
    );
    // K7 reads `m'` on `b` and `c` only once each serves a copy as large as
    // `a`'s, and waits for that itself, up to the convergence bound: in
    // round 3 it relied on K6's poll, which a run whose K6 stops earlier
    // skips (record 487c137). A copy that never arrives is "not yet
    // present"; one that arrived and serves an edit concurrent with the cut,
    // or another value, says which.
    let k7 = (|| -> Result<(), String> {
        let whole = objects(&cluster.identified(A, &copy)?)?;
        cluster
            .until("b and c serve a copy as large as a's (K7)", |node| {
                if node == A {
                    return Ok(());
                }
                let served = objects(&cluster.identified(node, &copy)?)?;
                if served == whole {
                    Ok(())
                } else {
                    Err(format!(
                        "node {node} serves {served} of the {whole} objects of a's copy"
                    ))
                }
            })
            .map_err(|why| format!("copy not yet present: {why}"))?;
        let edited = json!(format!("{renamed_name}s"));
        [A, B, C].into_iter().try_for_each(|node| {
            for model in [m.as_str(), copy.as_str()] {
                let doc = cluster.identified(node, model)?;
                let class = at_path(&doc, &renamed_path);
                if class["name"] == edited || class["abstract"] == json!(true) {
                    return Err(format!(
                        "node {node} serves b's concurrent edit in {model}: {}",
                        class["name"]
                    ));
                }
                if class["name"] != json!(renamed_name) {
                    return Err(format!(
                        "node {node} serves {} in {model} for the name of {renamed_name}, \
                         which is neither its name at the cut nor b's concurrent edit",
                        class["name"]
                    ));
                }
                let stereotypes = &at_path(&doc, &stereotyped_path)["stereotype"];
                if stereotypes
                    .as_array()
                    .is_some_and(|items| items.contains(&json!("persistent")))
                {
                    return Err(format!("node {node} serves c's concurrent edit in {model}"));
                }
            }
            Ok(())
        })
    })();
    out.check("K7", k7);
    sizes(&cluster, &copy, (&cut_doc, &cut_plain), &mut out);
    record_nodes(&cluster, &mut out);
    out
}

// ---------------------------------------------------------------------------
// The schedule and the summary
// ---------------------------------------------------------------------------

/// The sizes and repetitions to run, and whether they are the registered
/// ones.
fn schedule() -> (Vec<usize>, usize, bool) {
    let reps = match std::env::var("SI12_R3_REPS") {
        Ok(raw) => raw
            .parse::<usize>()
            .ok()
            .filter(|reps| (1..=REPS).contains(reps))
            .unwrap_or_else(|| panic!("SI12_R3_REPS must be 1 to {REPS}, not `{raw}`")),
        Err(_) => REPS,
    };
    let sizes = match std::env::var("SI12_R3_SIZES") {
        Ok(raw) => raw
            .split(',')
            .map(|size| {
                size.trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|size| SIZES.contains(size))
                    .unwrap_or_else(|| {
                        panic!("SI12_R3_SIZES names sizes among {SIZES:?}, not `{raw}`")
                    })
            })
            .collect(),
        Err(_) => SIZES.to_vec(),
    };
    let registered = reps == REPS && sizes == SIZES.to_vec();
    (sizes, reps, registered)
}

/// Median, least and greatest of what the runs measured under `key`.
fn spread(runs: &[&Outcome], key: &str) -> Value {
    let mut values: Vec<f64> = runs
        .iter()
        .filter_map(|run| run.measures.get(key).and_then(Value::as_f64))
        .collect();
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(f64::total_cmp);
    let n = values.len();
    let median = if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    };
    json!({ "n": n, "median": median, "min": values[0], "max": values[n - 1] })
}

/// The one value every run measured under `key`, or every value when they
/// differ.
fn same(runs: &[&Outcome], key: &str) -> Value {
    let mut values: Vec<Value> = runs
        .iter()
        .filter_map(|run| run.measures.get(key).cloned())
        .collect();
    values.dedup();
    match values.len() {
        0 => Value::Null,
        1 => values.remove(0),
        _ => json!({ "differ": values }),
    }
}

/// Whether a run was stopped at its ceiling.
fn stopped_at_ceiling(run: &Outcome) -> bool {
    run.measures.get("stopped_at_ceiling") == Some(&json!(true))
}

/// The runs of `sizes`, `reps` times each, the sizes interleaved and each
/// size's plain run before its concurrent one, each on three fresh nodes and
/// each stopped at `ceiling` when one is given; every run's line, then a
/// summary per size, printed under `tag`. With a ceiling, two runs in a row
/// stopped at it end the schedule, and the runs not started are counted as
/// such. Panics, as the claim test always did, when a run did not hold
/// every check.
fn evaluate(
    tag: &str,
    binary: &Path,
    sizes: &[usize],
    reps: usize,
    registered: bool,
    ceiling: Option<Duration>,
) {
    println!(
        "{tag}-SCHEDULE {}",
        json!({
            "sizes": sizes,
            "reps": reps,
            "runs": sizes.len() * reps * 2,
            "registered": registered,
            "binary": binary.display().to_string(),
            "ceiling_s": ceiling.map(|ceiling| ceiling.as_secs()),
        })
    );
    let d1 = Uml::new(D1);
    let models: BTreeMap<usize, Generated> =
        sizes.iter().map(|size| (*size, generate(*size))).collect();
    for (size, g) in &models {
        let structure = g.structure(&d1).len();
        let links: usize = g.elements.iter().map(|element| element.refs.len()).sum();
        println!(
            "{tag}-MODEL {}",
            json!({
                "size": size,
                "generated": g.elements.len(),
                "by_class": g.census(),
                "packages_nested": g.elements.iter()
                    .filter(|e| e.kind == Kind::Package && e.path.len() > 1).count(),
                "ops_d1_structure": structure,
                "ops_d1_links": links,
            })
        );
    }
    let mut outcomes: Vec<Outcome> = Vec::new();
    'schedule: for rep in 0..reps {
        for size in sizes {
            let g = &models[size];
            for case in [Case::Plain, Case::Concurrent] {
                if ceiling.is_some()
                    && outcomes.len() >= 2
                    && outcomes[outcomes.len() - 2..]
                        .iter()
                        .all(stopped_at_ceiling)
                {
                    break 'schedule;
                }
                let outcome = match case {
                    Case::Plain => plain_run(binary, g, &d1, rep, ceiling),
                    Case::Concurrent => concurrent_run(binary, g, &d1, rep, ceiling),
                };
                println!("{tag}-RUN {}", outcome.line());
                outcomes.push(outcome);
            }
        }
    }
    for size in sizes {
        let of = |case: Case| -> Vec<&Outcome> {
            outcomes
                .iter()
                .filter(|run| run.size == *size && run.case == case && run.setup.is_none())
                .collect()
        };
        let plain = of(Case::Plain);
        let concurrent = of(Case::Concurrent);
        let all: Vec<&Outcome> = plain.iter().chain(concurrent.iter()).copied().collect();
        println!(
            "{tag}-SUMMARY {}",
            json!({
                "size": size,
                "plain_runs_every_check": plain.iter().filter(|run| run.held()).count(),
                "concurrent_runs_every_check": concurrent.iter().filter(|run| run.held()).count(),
                "migrate_status": same(&all, "migrate_status"),
                "migrate_ms_plain": spread(&plain, "migrate_ms"),
                "migrated_ms": spread(&all, "migrated_ms"),
                "cut_everywhere_ms_plain": spread(&plain, "cut_everywhere_ms"),
                "agree_ms_plain": spread(&plain, "agree_ms"),
                "migrate_ms_concurrent": spread(&concurrent, "migrate_ms"),
                "build_ms": spread(&all, "build_ms"),
                "m_ops_posted": same(&all, "m_ops_posted"),
                "cut_op_bytes": same(&all, "cut_op_bytes"),
                "cut_margin_to_768KiB": same(&all, "cut_margin_to_768KiB"),
                "m_identified_bytes": same(&all, "m_identified_bytes"),
                "copy_identified_bytes": same(&all, "copy_identified_bytes"),
                "copy_ops_applied_on_a": same(&all, "copy_ops_applied_on_a"),
                "copy_log_bytes": same(&plain, "copy_log_bytes"),
                "copy_state_transfer": same(&plain, "copy_state_transfer"),
            })
        );
    }
    let bytes = |size: usize| -> Option<f64> {
        outcomes
            .iter()
            .find(|run| run.size == size && run.setup.is_none())
            .and_then(|run| run.measures.get("cut_op_bytes"))
            .and_then(Value::as_f64)
    };
    if let (Some(small), Some(large)) = (bytes(SIZES[0]), bytes(SIZES[2])) {
        let per_element = (large - small) / (SIZES[2] - SIZES[0]) as f64;
        println!(
            "{tag}-EXTRAPOLATION {}",
            json!({
                "kind": "linear extrapolation from the cut bytes at 100 and 1,000 objects, not a measurement",
                "cut_bytes_per_object": per_element,
                "objects_at_768KiB": SIZES[2] as f64 + (FRAME_BOUND as f64 - large) / per_element,
                "objects_at_relay_1MiB": SIZES[2] as f64 + (RELAY_FRAME as f64 - large) / per_element,
            })
        );
    }
    let completed = outcomes.iter().filter(|run| run.setup.is_none()).count();
    let held = outcomes.iter().filter(|run| run.held()).count();
    let stopped = outcomes
        .iter()
        .filter(|run| stopped_at_ceiling(run))
        .count();
    println!(
        "{tag}-RESULT {}",
        json!({
            "registered": registered,
            "runs": outcomes.len(),
            "not_started": sizes.len() * reps * 2 - outcomes.len(),
            "completed": completed,
            "runs_every_check": held,
            "stopped_at_ceiling": stopped,
        })
    );
    let failures: Vec<String> = outcomes
        .iter()
        .filter(|run| !run.held())
        .map(|run| run.line().to_string())
        .collect();
    assert!(
        failures.is_empty(),
        "{}: {} of {} runs did not hold every check:\n{}",
        tag.to_lowercase(),
        failures.len(),
        outcomes.len(),
        failures.join("\n")
    );
}

/// SI.12 round 3, three nodes, 100 to 1,000 objects.
#[test]
#[ignore = "SI.12 round 3 evaluation: 30 runs on fresh model_node processes; run with --ignored"]
fn si12_r3_copy_migration_of_100_300_and_1000_element_models_on_three_nodes() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si12-r3: {why}");
            return;
        }
    };
    let (sizes, reps, registered) = schedule();
    evaluate("SI12-R3", &binary, &sizes, reps, registered, None);
}

/// SI.12 round 4, three nodes, 5,000 objects: the claim test's runs and
/// checks at a size beyond the claim, an observation with its own predicted
/// outcome (the spec's round 4). Every run is stopped at [`RUN_CEILING`].
#[test]
#[ignore = "SI.12 round 4 observation: 10 runs at 5,000 objects on fresh model_node processes; run with --ignored"]
fn si12_r4_copy_migration_of_a_5000_element_model_on_three_nodes() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP si12-r4: {why}");
            return;
        }
    };
    evaluate(
        "SI12-R4",
        &binary,
        &[R4_SIZE],
        R4_REPS,
        true,
        Some(RUN_CEILING),
    );
}
