//! AR.02 (`cea-cdrt-knowledge/spec/items/AR.02.md`): the declarations of which end is whose
//! (`eOpposite`, `subsets`, `redefines`), which Arachne's descriptor carries from AR.02 on, are
//! carried by the node without changing any merge.
//!
//! - M2, in process: one model opened on the SysML descriptor with the declarations and one on
//!   the same descriptor without them, two replicas each, are handed the same operations; every
//!   operation is accepted or refused alike, and after every one the four replicas read the same
//!   document. The same on SimpleUML.
//! - M3, over the wire: a `model_node` holding the new SysML descriptor lists it under its new
//!   digest and serves it back, value for value, wherever it serves a descriptor
//!   (`GET /api/metamodel`, `GET /api/model/{id}/metamodel`); a node holding none takes it on
//!   `POST /api/metamodels` (what re-posting it on the stack's alice does) and serves it back; a
//!   joiner that holds none joins a model by its id alone and serves the descriptor the model's
//!   history carries, declarations included. Two models, one under the new digest and one under
//!   today's, answer the same operations the same way and serve the same document.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test ar02_declarations_served_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: `E2E-SKIP ar02-m3: <why>` and return, as `e2e_interp.rs` does. A skip is not a
//! pass.
#![cfg(feature = "network")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_crdt::utils::membership::twins_log;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{class_slot, feature_slot, install};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

#[path = "nw02/launch.rs"]
mod launch;

const KEYS: [&str; 3] = ["eOpposite", "subsets", "redefines"];
const SYSML_AR02: &str = include_str!("fixtures/sysml.ar02.metamodel.json");
const UML_AR02: &str = include_str!("fixtures/uml.ar02.metamodel.json");
const SYSML_DIGEST_TODAY: &str = "5168d5f66ef4811bb7e4e2258628dd2051d5fa451c8093479aa3b1304193692e";
const SYSML_DIGEST_AR02: &str = "ebd580696f97d9339c148f5b29db5d0ff7b0928d78fb05efc7c206811405e627";
const UML_DIGEST_AR02: &str = "21c50dea71938b861233b72116f5ead094fe1bbb3d67bf623864f52c3ead1c3f";

const LOG_ID: &str = "a02a02a02a02a02a02a02a02a02a02a0";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

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

// ---------------------------------------------------------------------------
// Operations, by name through the table: the same slots with and without
// ---------------------------------------------------------------------------

struct Ops(MetamodelSemantics);

impl Ops {
    fn new(descriptor: &Value) -> Self {
        Ops(from_descriptor(descriptor).expect("set-up: the descriptor is a table"))
    }

    fn is(&self, class: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::variant(class_slot(&self.0, class), inner)
    }

    fn at(&self, class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        let slot = class_slot(&self.0, class);
        InstanceOp::field(feature_slot(&self.0, slot, feature), inner)
    }
}

fn ch(c: char) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: c })
}

fn write(value: &str) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Write(Scalar::text(value)))
}

/// A SysML package holding a part usage typed and subsetted, and a part definition
/// subclassified: every relationship whose owning end AG.02 asks for, both ends written, the
/// container's opposite written, a flag, an optional name, a refused write to a derived end, and
/// a removal. `(label, operation, accepted?)`; the last is the premise, not the claim.
fn sysml_script(o: &Ops) -> Vec<(&'static str, ModelOp, bool)> {
    let package = |inner| o.is("Package", inner);
    let owned = |pos: usize, inner| {
        package(o.at(
            "Package",
            "ownedRelationship",
            InstanceOp::at(pos, o.is("OwningMembership", inner)),
        ))
    };
    let member = |pos: usize, class: &'static str, inner| {
        owned(
            pos,
            o.at(
                "OwningMembership",
                "ownedRelatedElement",
                InstanceOp::at(0, o.is(class, inner)),
            ),
        )
    };
    let usage_rel = |pos: usize, class: &'static str, inner| {
        member(
            0,
            "PartUsage",
            o.at(
                "PartUsage",
                "ownedRelationship",
                InstanceOp::at(pos, o.is(class, inner)),
            ),
        )
    };
    let definition_rel = |class: &'static str, inner| {
        member(
            1,
            "PartDefinition",
            o.at(
                "PartDefinition",
                "ownedRelationship",
                InstanceOp::at(0, o.is(class, inner)),
            ),
        )
    };
    let op = |instance: InstanceOp| instance.into_model_op();
    vec![
        (
            "root Package, elementId",
            op(package(o.at("Package", "elementId", ch('p')))),
            true,
        ),
        (
            "an OwningMembership in Package.ownedRelationship",
            op(package(o.at(
                "Package",
                "ownedRelationship",
                InstanceOp::insert(0, o.is("OwningMembership", InstanceOp::New)),
            ))),
            true,
        ),
        (
            "a PartUsage in it, elementId",
            op(owned(
                0,
                o.at(
                    "OwningMembership",
                    "ownedRelatedElement",
                    InstanceOp::insert(
                        0,
                        o.is("PartUsage", o.at("PartUsage", "elementId", ch('u'))),
                    ),
                ),
            )),
            true,
        ),
        (
            "PartUsage.declaredName",
            op(member(
                0,
                "PartUsage",
                o.at("PartUsage", "declaredName", InstanceOp::set(ch('e'))),
            )),
            true,
        ),
        (
            "a FeatureTyping in PartUsage.ownedRelationship",
            op(member(
                0,
                "PartUsage",
                o.at(
                    "PartUsage",
                    "ownedRelationship",
                    InstanceOp::insert(0, o.is("FeatureTyping", InstanceOp::New)),
                ),
            )),
            true,
        ),
        (
            "FeatureTyping.typedFeature",
            op(usage_rel(
                0,
                "FeatureTyping",
                o.at("FeatureTyping", "typedFeature", write("u")),
            )),
            true,
        ),
        (
            "FeatureTyping.type",
            op(usage_rel(
                0,
                "FeatureTyping",
                o.at("FeatureTyping", "type", write("d")),
            )),
            true,
        ),
        (
            "FeatureTyping.owningRelatedElement, the containment's opposite",
            op(usage_rel(
                0,
                "FeatureTyping",
                o.at("FeatureTyping", "owningRelatedElement", write("u")),
            )),
            true,
        ),
        (
            "FeatureTyping.isImplied",
            op(usage_rel(
                0,
                "FeatureTyping",
                o.at(
                    "FeatureTyping",
                    "isImplied",
                    InstanceOp::Leaf(LeafOp::Enable),
                ),
            )),
            true,
        ),
        (
            "FeatureTyping.owningFeature, derived: refused",
            op(usage_rel(
                0,
                "FeatureTyping",
                o.at("FeatureTyping", "owningFeature", write("u")),
            )),
            false,
        ),
        (
            "a Subsetting in PartUsage.ownedRelationship",
            op(member(
                0,
                "PartUsage",
                o.at(
                    "PartUsage",
                    "ownedRelationship",
                    InstanceOp::insert(1, o.is("Subsetting", InstanceOp::New)),
                ),
            )),
            true,
        ),
        (
            "Subsetting.subsettingFeature",
            op(usage_rel(
                1,
                "Subsetting",
                o.at("Subsetting", "subsettingFeature", write("u")),
            )),
            true,
        ),
        (
            "Subsetting.subsettedFeature",
            op(usage_rel(
                1,
                "Subsetting",
                o.at("Subsetting", "subsettedFeature", write("v")),
            )),
            true,
        ),
        (
            "a second OwningMembership",
            op(package(o.at(
                "Package",
                "ownedRelationship",
                InstanceOp::insert(1, o.is("OwningMembership", InstanceOp::New)),
            ))),
            true,
        ),
        (
            "a PartDefinition in it, elementId",
            op(owned(
                1,
                o.at(
                    "OwningMembership",
                    "ownedRelatedElement",
                    InstanceOp::insert(
                        0,
                        o.is(
                            "PartDefinition",
                            o.at("PartDefinition", "elementId", ch('d')),
                        ),
                    ),
                ),
            )),
            true,
        ),
        (
            "a Subclassification in PartDefinition.ownedRelationship",
            op(member(
                1,
                "PartDefinition",
                o.at(
                    "PartDefinition",
                    "ownedRelationship",
                    InstanceOp::insert(0, o.is("Subclassification", InstanceOp::New)),
                ),
            )),
            true,
        ),
        (
            "Subclassification.subclassifier",
            op(definition_rel(
                "Subclassification",
                o.at("Subclassification", "subclassifier", write("d")),
            )),
            true,
        ),
        (
            "Subclassification.superclassifier",
            op(definition_rel(
                "Subclassification",
                o.at("Subclassification", "superclassifier", write("e")),
            )),
            true,
        ),
        (
            "Subclassification.owningClassifier, derived: refused",
            op(definition_rel(
                "Subclassification",
                o.at("Subclassification", "owningClassifier", write("d")),
            )),
            false,
        ),
        (
            "the Subsetting taken out",
            op(member(
                0,
                "PartUsage",
                o.at("PartUsage", "ownedRelationship", InstanceOp::delete(1)),
            )),
            true,
        ),
    ]
}

/// A SimpleUML model: a class in a model, a property in the class, both opposites written.
fn uml_script(o: &Ops) -> Vec<(&'static str, ModelOp, bool)> {
    let model = |inner| o.is("Model", inner);
    let class = |inner| {
        model(o.at(
            "Model",
            "ownedElements",
            InstanceOp::at(0, o.is("Class", inner)),
        ))
    };
    let property = |inner| {
        class(o.at(
            "Class",
            "attributes",
            InstanceOp::at(0, o.is("Property", inner)),
        ))
    };
    let op = |instance: InstanceOp| instance.into_model_op();
    vec![
        (
            "root Model, name",
            op(model(o.at("Model", "name", InstanceOp::set(ch('m'))))),
            true,
        ),
        (
            "a Class in Package.ownedElements",
            op(model(o.at(
                "Model",
                "ownedElements",
                InstanceOp::insert(
                    0,
                    o.is("Class", o.at("Class", "name", InstanceOp::set(ch('c')))),
                ),
            ))),
            true,
        ),
        (
            "Packageable.owner on the Class, the containment's opposite",
            op(class(o.at("Class", "owner", write("m")))),
            true,
        ),
        (
            "a Property in DataType.attributes",
            op(class(o.at(
                "Class",
                "attributes",
                InstanceOp::insert(
                    0,
                    o.is(
                        "Property",
                        o.at("Property", "name", InstanceOp::set(ch('p'))),
                    ),
                ),
            ))),
            true,
        ),
        (
            "Property.owner, the containment's opposite",
            op(property(o.at("Property", "owner", write("c")))),
            true,
        ),
        (
            "Property.ttype",
            op(property(o.at("Property", "ttype", write("c")))),
            true,
        ),
        (
            "the Property taken out",
            op(class(o.at("Class", "attributes", InstanceOp::delete(0)))),
            true,
        ),
    ]
}

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;

/// One model opened on two replicas, both holding the table; the refusal, if the log refuses
/// the descriptor.
fn opened(descriptor: &Value) -> Result<(Rep, Rep), String> {
    let (mut a, mut b) = twins_log::<ModelLog>();
    let event = a
        .send(install("ar02", descriptor))
        .map_err(|why| format!("{why:?}"))?;
    b.receive(event);
    Ok((a, b))
}

fn read(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

/// M2: one script on a model with the declarations and on one without; returns the lines of
/// every difference, and the premise failures separately.
fn twin(name: &str, with: &Value, script: Vec<(&'static str, ModelOp, bool)>) -> Vec<String> {
    let without = strip(with);
    assert_ne!(
        metamodel_digest(with),
        metamodel_digest(&without),
        "set-up: the {name} descriptor declares nothing"
    );
    let (mut o1, mut o2) = opened(&without)
        .unwrap_or_else(|why| panic!("set-up: {name} without the declarations is refused: {why}"));
    let (mut w1, mut w2) = match opened(with) {
        Ok(pair) => pair,
        Err(why) => {
            return vec![format!(
                "{name}: a log refuses the descriptor with the declarations: {why}"
            )];
        }
    };
    let mut differences = Vec::new();
    let mut premise = Vec::new();
    let (mut accepted, mut refused) = (0, 0);
    for (label, op, expected) in script {
        let on_with = w1.send(op.clone());
        let on_without = o1.send(op.clone());
        // Each side's event goes to its own twin, whatever the other side did.
        if let Ok(event) = &on_with {
            w2.receive(event.clone());
        }
        if let Ok(event) = &on_without {
            o2.receive(event.clone());
        }
        match (&on_with, &on_without) {
            (Ok(_), Ok(_)) => accepted += 1,
            (Err(why_w), Err(why_o)) => {
                refused += 1;
                if format!("{why_w:?}") != format!("{why_o:?}") {
                    differences.push(format!(
                        "{name} {label}: refused with {why_w:?} with the declarations, {why_o:?} without"
                    ));
                }
            }
            _ => differences.push(format!(
                "{name} {label}: {} with the declarations, {} without",
                if on_with.is_ok() {
                    "accepted"
                } else {
                    "refused"
                },
                if on_without.is_ok() {
                    "accepted"
                } else {
                    "refused"
                }
            )),
        }
        if on_without.is_ok() != expected {
            premise.push(format!(
                "{name} {label}: {} without the declarations, the script expects {}",
                if on_without.is_ok() {
                    "accepted"
                } else {
                    "refused"
                },
                if expected { "accepted" } else { "refused" }
            ));
        }
        let reads = [read(&w1), read(&w2), read(&o1), read(&o2)];
        if reads[0] != reads[2] || reads[1] != reads[3] {
            differences.push(format!(
                "{name} after {label}: with the declarations {} / {}, without {} / {}",
                reads[0], reads[1], reads[2], reads[3]
            ));
        }
        if reads[0] != reads[1] || reads[2] != reads[3] {
            premise.push(format!("{name} after {label}: twins parted: {reads:?}"));
        }
    }
    println!(
        "AR02-M2 {name}: operations accepted {accepted}, refused {refused}, differences {}; \
         document {}",
        differences.len(),
        read(&w1)
    );
    assert!(
        premise.is_empty(),
        "set-up: the script does not run as written:\n  {}",
        premise.join("\n  ")
    );
    differences
}

#[test]
fn ar02_m2_every_operation_does_the_same_with_and_without_the_declarations() {
    let sysml = parse(SYSML_AR02);
    let uml = parse(UML_AR02);
    // The operations are written through the table of the descriptor without the declarations:
    // the slots an editor would compute from today's descriptor.
    let mut differences = twin("sysml", &sysml, sysml_script(&Ops::new(&strip(&sysml))));
    differences.extend(twin("simpleuml", &uml, uml_script(&Ops::new(&strip(&uml)))));
    assert!(
        differences.is_empty(),
        "AR02 claim: the declarations changed what an operation does on a model:\n  {}",
        differences.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// M3: over the wire
// ---------------------------------------------------------------------------

#[test]
fn ar02_m3_a_node_serves_the_declarations_wherever_it_serves_the_descriptor() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP ar02-m3: {why}");
            return;
        }
    };
    let failures = m3(&binary).unwrap_or_else(|why| panic!("ar02-m3: set-up: {why}"));
    assert!(
        failures.is_empty(),
        "AR02 claim: the node does not serve the declarations, or they changed a merge:\n  {}",
        failures.join("\n  ")
    );
}

fn m3(binary: &Path) -> Result<Vec<String>, String> {
    let sysml = parse(SYSML_AR02);
    let before = strip(&sysml);
    let uml = parse(UML_AR02);
    let mut fail = Vec::new();

    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let root = std::env::var_os("AR02_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let scratch = root.join(format!("moirai-ar02-m3-{}-{run}", std::process::id()));
    let holder = scratch.join("metamodels-holder");
    fs::create_dir_all(&holder).map_err(|e| e.to_string())?;
    // The text Arachne writes, served from a file; today's SysML beside it; SimpleUML's.
    fs::write(holder.join("0-sysml.json"), SYSML_AR02).map_err(|e| e.to_string())?;
    fs::write(holder.join("1-sysml-today.json"), format!("{before:#}\n"))
        .map_err(|e| e.to_string())?;
    fs::write(holder.join("2-uml.json"), UML_AR02).map_err(|e| e.to_string())?;
    let holder_dir = holder.to_string_lossy().into_owned();

    let http = Http::new()?;
    let a = spawn_node(
        binary,
        &scratch,
        "a",
        &[("LOG_ID", LOG_ID), ("METAMODEL_DIR", holder_dir.as_str())],
        "",
    )?;
    http.await_health(&a)?;
    let c = spawn_node(
        binary,
        &scratch,
        "c",
        &[("LOG_ID", LOG_ID)],
        &format!("a:127.0.0.1:{}", a.listen),
    )?;
    let b = spawn_node(binary, &scratch, "b", &[("LOG_ID", LOG_ID)], "")?;
    for node in [&c, &b] {
        http.await_health(node)?;
    }
    for node in [&a, &b, &c] {
        http.await_loop(node)?;
    }

    // S1: the listing names the new digest, today's, and SimpleUML's new one.
    let (_, listing) = http.get(&a, "/api/metamodels")?;
    let digests: Vec<&str> = listing["metamodels"]
        .as_array()
        .map(|list| list.iter().filter_map(|m| m["digest"].as_str()).collect())
        .unwrap_or_default();
    println!("AR02-M3 S1 a lists {digests:?}");
    for (what, digest) in [
        ("SysML with the declarations", SYSML_DIGEST_AR02),
        ("SysML of today", SYSML_DIGEST_TODAY),
        ("SimpleUML with its opposites", UML_DIGEST_AR02),
    ] {
        if !digests.contains(&digest) {
            fail.push(format!("S1: a does not list {what} ({digest}): {listing}"));
        }
    }

    // S2: the first descriptor, `GET /api/metamodel`.
    let (status, first) = http.get(&a, "/api/metamodel")?;
    println!(
        "AR02-M3 S2 GET /api/metamodel: {status}, equal to the new SysML descriptor {}",
        first == sysml
    );
    if first != sysml {
        fail.push(format!(
            "S2: GET /api/metamodel answered {status} with digest {}, not the new SysML descriptor",
            metamodel_digest(&first)
        ));
    }

    // S3: a model under each digest serves its own descriptor.
    let m_new = http.register(&a, None, SYSML_DIGEST_AR02);
    let m_old = http.register(&a, None, SYSML_DIGEST_TODAY)?;
    let m_uml = http.register(&a, None, UML_DIGEST_AR02);
    let m_new = match m_new {
        Ok(id) => id,
        Err(why) => {
            fail.push(format!("S3: a model under the new SysML digest: {why}"));
            return Ok(fail);
        }
    };
    for (what, model, want) in [
        ("the new SysML model", Some(m_new.clone()), &sysml),
        ("today's SysML model", Some(m_old.clone()), &before),
        ("the SimpleUML model", m_uml.as_ref().ok().cloned(), &uml),
    ] {
        let Some(model) = model else {
            fail.push(format!("S3: {what} was not registered: {m_uml:?}"));
            continue;
        };
        let (status, served) = http.get(&a, &format!("/api/model/{model}/metamodel"))?;
        println!(
            "AR02-M3 S3 {what} {model}: {status}, equal to its descriptor {}",
            &served == want
        );
        if &served != want {
            fail.push(format!(
                "S3: {what} is served {status} a descriptor of digest {}, not {}",
                metamodel_digest(&served),
                metamodel_digest(want)
            ));
        }
    }

    // S4: the same operations on the new model and on today's, over the wire.
    let ops = Ops::new(&before);
    let mut answered = 0;
    for (label, op, _) in sysml_script(&ops) {
        let body = serde_json::to_string(&op).map_err(|e| e.to_string())?;
        let on_new = http.post(&a, &format!("/api/model/{m_new}/op"), body.clone())?;
        let on_old = http.post(&a, &format!("/api/model/{m_old}/op"), body)?;
        answered += 1;
        if on_new != on_old {
            fail.push(format!(
                "S4 {label}: answered {on_new:?} under the new digest, {on_old:?} under today's"
            ));
        }
    }
    let doc_new = http.state(&a, &m_new)?;
    let doc_old = http.state(&a, &m_old)?;
    println!(
        "AR02-M3 S4 operations {answered}, documents equal {}; {doc_new}",
        doc_new == doc_old
    );
    if doc_new != doc_old {
        fail.push(format!(
            "S4: the documents differ: {doc_new} under the new digest, {doc_old} under today's"
        ));
    }

    // S5: a node holding nothing takes the descriptor on POST, as alice will.
    let (status, answer) = http.post(&b, "/api/metamodels", SYSML_AR02.to_string())?;
    println!(
        "AR02-M3 S5 POST /api/metamodels on b: {status}, digest {}",
        answer["metamodel"]["digest"]
    );
    if status != 201 || answer["metamodel"]["digest"] != json!(SYSML_DIGEST_AR02) {
        fail.push(format!(
            "S5: POST /api/metamodels answered {status}: {answer}"
        ));
    } else {
        let model = http.register(&b, None, SYSML_DIGEST_AR02)?;
        let (status, served) = http.get(&b, &format!("/api/model/{model}/metamodel"))?;
        if served != sysml {
            fail.push(format!(
                "S5: b serves a model under the posted descriptor ({status}) a descriptor of \
                 digest {}",
                metamodel_digest(&served)
            ));
        }
    }

    // S6: a joiner holding nothing joins the new model by id alone and serves its descriptor.
    http.until_seen(&c, &m_new)?;
    http.post(&c, "/api/models", json!({ "model_id": m_new }).to_string())?;
    let joined = wait_until(
        "c to serve the new model's descriptor",
        STEP_TIMEOUT,
        || match http.get(&c, &format!("/api/model/{m_new}/metamodel")) {
            Ok((200, served)) => Ok(Some(served)),
            _ => Ok(None),
        },
    );
    match joined {
        Ok(served) => {
            println!(
                "AR02-M3 S6 c serves the joined model's descriptor, equal to the new SysML \
                 descriptor {}",
                served == sysml
            );
            if served != sysml {
                fail.push(format!(
                    "S6: the joiner serves a descriptor of digest {}",
                    metamodel_digest(&served)
                ));
            }
            if let Err(why) = http.until_serves(&c, &m_new, &doc_new) {
                fail.push(format!("S6: the joiner does not serve a's document: {why}"));
            }
        }
        Err(why) => fail.push(format!("S6: {why}")),
    }
    drop((a, b, c));
    let _ = fs::remove_dir_all(&scratch);
    Ok(fail)
}

// ---------------------------------------------------------------------------
// Nodes, as `ed20_join_without_metamodel_l3.rs` starts and reads them
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
    listen: u16,
    child: Child,
    log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start one node on ports it binds itself (NW.02), its working directory the scratch
/// directory so no `metamodel.json` is picked up by accident, and hand it `peers`.
fn spawn_node(
    binary: &Path,
    scratch: &Path,
    id: &str,
    env: &[(&str, &str)],
    peers: &str,
) -> Result<Node, String> {
    let log = scratch.join(format!("{id}.log"));
    let ports = scratch.join(format!("{id}.ports.json"));
    let peers_file = scratch.join(format!("{id}.peers"));
    let _ = fs::remove_file(&ports);
    let _ = fs::remove_file(&peers_file);
    let out = fs::File::create(&log).map_err(|err| err.to_string())?;
    let err_out = out.try_clone().map_err(|err| err.to_string())?;
    let mut child = Command::new(binary)
        .current_dir(scratch)
        .env("REPLICA_ID", id)
        .env("LISTEN_PORT", "0")
        .env("HTTP_PORT", "0")
        .env("PORTS_FILE", &ports)
        .env("PEERS_FILE", &peers_file)
        .env_remove("PEERS")
        .env_remove("BOOTNODE_URL")
        .env_remove("DASHBOARD_URL")
        .env_remove("METAMODEL_PATH")
        .env_remove("METAMODEL_DIR")
        .env_remove("ADVERTISE_ADDR")
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|err| format!("spawning {id}: {err}"))?;
    let deadline = Instant::now() + launch::PORTS_TIMEOUT;
    let reported = loop {
        if let Some(parsed) = launch::read_ports_file(&ports) {
            let reported = parsed?;
            if reported.replica_id != id || reported.pid != child.id() {
                let _ = child.kill();
                return Err(format!("{id}'s ports file is not its own: {reported:?}"));
            }
            break reported;
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!(
                "{id} exited with {status} before reporting its ports:\n{}",
                launch::tail(&log, 25)
            ));
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err(format!(
                "{id} never reported its ports:\n{}",
                launch::tail(&log, 25)
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    launch::write_atomically(&peers_file, peers)?;
    Ok(Node {
        id: id.to_string(),
        http: reported
            .http_port
            .ok_or_else(|| format!("{id} reported no HTTP port"))?,
        listen: reported.listen_port,
        child,
        log,
    })
}

struct Http(reqwest::blocking::Client);

impl Http {
    fn new() -> Result<Self, String> {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map(Http)
            .map_err(|err| err.to_string())
    }

    fn get(&self, node: &Node, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .0
            .get(node.url(path))
            .send()
            .map_err(|err| format!("GET {path} on {}: {err}", node.id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn post(&self, node: &Node, path: &str, body: String) -> Result<(u16, Value), String> {
        let response = self
            .0
            .post(node.url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| format!("POST {path} on {}: {err}", node.id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn await_health(&self, node: &Node) -> Result<(), String> {
        wait_until(
            &format!("{} to answer /api/health", node.id),
            HEALTH_TIMEOUT,
            || match self.get(node, "/api/health") {
                Ok((200, body)) if body["replica_id"] == json!(node.id) => Ok(Some(())),
                Ok((200, body)) => Err(format!(
                    "the HTTP port {} reported by {} answered as {body}",
                    node.http, node.id
                )),
                _ => Ok(None),
            },
        )
        .map_err(|why| {
            format!(
                "{why}; alarms:\n{}\n{}",
                launch::alarms(&node.log),
                launch::tail(&node.log, 25)
            )
        })
    }

    fn await_loop(&self, node: &Node) -> Result<(), String> {
        wait_until(
            &format!("{} to enter its event loop", node.id),
            STEP_TIMEOUT,
            || {
                Ok(node
                    .log_text()
                    .contains("Entering main event loop")
                    .then_some(()))
            },
        )
    }

    /// Create (`model` none) or join a model under a digest; answers its id.
    fn register(&self, node: &Node, model: Option<&str>, key: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": key }),
            None => json!({ "metamodel_id": key }),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "registering on {} answered {status}: {answer}",
                node.id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    fn state(&self, node: &Node, model: &str) -> Result<Value, String> {
        match self.get(node, &format!("/api/model/{model}/state"))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("{} answered {status}: {body}", node.id)),
        }
    }

    fn until_seen(&self, node: &Node, model: &str) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to list {model} under `seen` (premise)", node.id),
            STEP_TIMEOUT,
            || {
                if let Ok((_, body)) = self.get(node, "/api/models") {
                    last = body;
                }
                Ok(last["seen"]
                    .as_array()
                    .is_some_and(|seen| seen.contains(&json!(model)))
                    .then_some(()))
            },
        )
        .map_err(|why| format!("{why}; it listed {last}"))
    }

    fn until_serves(&self, node: &Node, model: &str, want: &Value) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to serve {want}", node.id),
            STEP_TIMEOUT,
            || {
                if let Ok(state) = self.state(node, model) {
                    last = state;
                }
                Ok((&last == want).then_some(()))
            },
        )
        .map_err(|why| format!("{why}; it served {last}"))
    }
}

fn wait_until<T>(
    what: &str,
    timeout: Duration,
    mut probe: impl FnMut() -> Result<Option<T>, String>,
) -> Result<T, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe()? {
            return Ok(value);
        }
        if Instant::now() > deadline {
            return Err(format!(
                "timed out after {} s waiting for {what}",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(POLL);
    }
}
