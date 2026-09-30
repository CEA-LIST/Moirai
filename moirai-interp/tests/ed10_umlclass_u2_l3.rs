//! ED.10 (`cea-cdrt-knowledge/spec/items/ED.10.md`): the UML class-diagram metamodel Arachne ships
//! as `examples/umlclass.metamodel.json` is hosted by a `model_node`, and the notation plan's U2
//! order system, built on it by operations over the node's HTTP API, reads back value for value
//! on the node that built it and on two replicas that joined it by its id alone.
//!
//! - S1: a node holding no descriptor takes the file's text on `POST /api/metamodels` (201, the
//!   file's digest, nsURI and package).
//! - S2: it lists it on `GET /api/metamodels` and serves it on `GET /api/metamodel`, value for
//!   value.
//! - S3: a model created under its digest gets an id `<32 hex>_umlclass` (NW.07) and is served
//!   the descriptor on `GET /api/model/{id}/metamodel`, value for value.
//! - S4: replica `b`, holding no descriptor, joins the model by its id alone before the build and
//!   serves the descriptor for it.
//! - S5: U2 is built on `a` by operations, one per request; every one is answered 200.
//! - S6: `a`'s document equals `fixtures/ed10_u2.state.json`, U2 written by hand in the
//!   node's canonical read-out, value for value.
//! - S7: `b` serves the same document.
//! - S8: replica `c`, holding no descriptor, joins the model by its id alone after the build and
//!   serves the descriptor and the same document.
//!
//! The operations are derived from the fixture itself: every object is created in its
//! containment at its position, and every value the fixture holds that a fresh object does not
//! (a text, a bound, a literal, a flag set, a reference) is written with the operation section 4
//! of the spec gives its feature. The descriptor gives the slots only; a class, feature or
//! literal it lacks is a claim failure, not a set-up one.
//!
//! The descriptor is read from the Arachne checkout beside this one
//! (`../arachne-model-plane/examples/umlclass.metamodel.json`, the layout Arachne's own
//! manifests assume), or from `ED10_DESCRIPTOR` when set.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test ed10_umlclass_u2_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: `E2E-SKIP ed10-u2: <why>` and return, as `e2e_interp.rs` does. A skip is not a
//! pass. A claim failure panics with `ED10 claim:`; a harness failure with `set-up:`.
#![cfg(feature = "network")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{
    ClassSlot, FeatureSlot, MetamodelSemantics, from_descriptor, metamodel_digest,
};
use serde_json::{Value, json};

#[path = "nw02/launch.rs"]
mod launch;

const U2: &str = include_str!("fixtures/ed10_u2.state.json");
const PACKAGE: &str = "umlclass";
const NS_URI: &str = "http://cea.fr/crdt/umlclass/0.1";
const LOG_ID: &str = "ed10ed10ed10ed10ed10ed10ed10ed10";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

fn descriptor_path() -> PathBuf {
    std::env::var_os("ED10_DESCRIPTOR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../arachne-model-plane/examples/umlclass.metamodel.json")
        })
}

// ---------------------------------------------------------------------------
// The operations, from the fixture: section 4's kind of every feature U2 writes
// ---------------------------------------------------------------------------

/// How section 4 says a feature is written.
#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A many-valued containment: a sequence of objects.
    Contain,
    /// An optional `EString`: text, one character per operation.
    Text,
    /// An optional `EInt` register.
    Int,
    /// An optional enum register, of the named enum.
    Enum(&'static str),
    /// A single `EBoolean` flag, enabled when the fixture says `true`.
    Flag,
    /// A single-valued reference: a register of the target's name.
    Ref,
    /// A many-valued reference: a set of the targets' names.
    Refs,
}

fn kind_of(feature: &str) -> Option<Kind> {
    Some(match feature {
        "ownedComment"
        | "packagedElement"
        | "generalization"
        | "interfaceRealization"
        | "ownedAttribute"
        | "ownedOperation"
        | "ownedLiteral"
        | "ownedParameter"
        | "ownedEnd" => Kind::Contain,
        "name" | "body" | "default" => Kind::Text,
        "lower" | "upper" => Kind::Int,
        "visibility" => Kind::Enum("VisibilityKind"),
        "aggregation" => Kind::Enum("AggregationKind"),
        "direction" => Kind::Enum("ParameterDirectionKind"),
        "isAbstract" | "isStatic" | "isDerived" => Kind::Flag,
        "type" | "general" | "specific" | "contract" | "implementingClassifier" => Kind::Ref,
        "annotatedElement" | "memberEnd" | "navigableOwnedEnd" | "client" | "supplier" => {
            Kind::Refs
        }
        _ => return None,
    })
}

/// Name lookups in the table, each answering why when the descriptor lacks the name.
struct Table(MetamodelSemantics);

impl Table {
    fn class(&self, name: &str) -> Result<ClassSlot, String> {
        self.0
            .classes
            .iter()
            .find(|class| &*class.name == name)
            .map(|class| class.slot)
            .ok_or_else(|| format!("the descriptor has no class `{name}`"))
    }

    fn feature(&self, class: ClassSlot, name: &str) -> Result<FeatureSlot, String> {
        let holder = &self.0.classes[class.index()];
        holder
            .visible
            .iter()
            .position(|(feature, _, _)| &**feature == name)
            .map(|index| FeatureSlot(index as u16))
            .ok_or_else(|| format!("`{}` has no feature `{name}`", holder.name))
    }

    fn literal(&self, enumeration: &str, literal: &str) -> Result<Scalar, String> {
        let found = self
            .0
            .enums
            .iter()
            .find(|e| &*e.name == enumeration)
            .ok_or_else(|| format!("the descriptor has no enum `{enumeration}`"))?;
        let position = found
            .literals
            .iter()
            .position(|l| &**l == literal)
            .ok_or_else(|| format!("`{enumeration}` has no literal `{literal}`"))?;
        Ok(Scalar::Enum(found.slot.index() as u16, position as u16))
    }
}

/// Where an object sits: the root's class, then each step down as the containment's slot on
/// the class above, the position, and the class of the object there.
#[derive(Clone)]
struct Address {
    root: ClassSlot,
    steps: Vec<(FeatureSlot, usize, ClassSlot)>,
}

impl Address {
    /// `inner` applied to the object at this address.
    fn wrap(&self, inner: InstanceOp) -> InstanceOp {
        let mut op = inner;
        for (feature, pos, class) in self.steps.iter().rev() {
            op = InstanceOp::field(
                *feature,
                InstanceOp::at(*pos, InstanceOp::variant(*class, op)),
            );
        }
        InstanceOp::variant(self.root, op)
    }

    fn class(&self) -> ClassSlot {
        self.steps.last().map_or(self.root, |(_, _, class)| *class)
    }
}

/// Every operation that builds `object` at `at`, labelled, in order; a name the descriptor
/// lacks goes to `missing` and its operations are left out.
fn build(
    table: &Table,
    object: &Value,
    at: &Address,
    ops: &mut Vec<(String, ModelOp)>,
    missing: &mut Vec<String>,
) {
    let class = at.class();
    let class_name = object["eClass"].as_str().unwrap_or("?");
    let Some(fields) = object.as_object() else {
        missing.push(format!(
            "set-up: the fixture holds a non-object under {class_name}"
        ));
        return;
    };
    let mut children = Vec::new();
    for (feature, value) in fields {
        if feature == "eClass" {
            continue;
        }
        let Some(kind) = kind_of(feature) else {
            missing.push(format!("set-up: the fixture's `{feature}` has no kind"));
            continue;
        };
        let slot = match table.feature(class, feature) {
            Ok(slot) => slot,
            Err(why) => {
                // Written only when the fixture holds a value a fresh object does not.
                let written = match kind {
                    Kind::Contain | Kind::Refs => value.as_array().is_some_and(|a| !a.is_empty()),
                    Kind::Flag => value == &json!(true),
                    _ => !value.is_null(),
                };
                if written {
                    missing.push(format!("{class_name}: {why}"));
                }
                continue;
            }
        };
        let label = format!("{class_name}.{feature}");
        let mut leaf = |op: InstanceOp| {
            ops.push((
                label.clone(),
                at.wrap(InstanceOp::field(slot, op)).into_model_op(),
            ));
        };
        match kind {
            Kind::Contain => {
                for (pos, child) in value.as_array().into_iter().flatten().enumerate() {
                    children.push((slot, pos, child));
                }
            }
            Kind::Text => {
                for (pos, ch) in value.as_str().unwrap_or("").chars().enumerate() {
                    leaf(InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar {
                        pos,
                        ch,
                    })));
                }
            }
            Kind::Int => {
                if let Some(n) = value.as_i64() {
                    leaf(InstanceOp::set(InstanceOp::Leaf(LeafOp::Write(
                        Scalar::Int(n),
                    ))));
                }
            }
            Kind::Enum(enumeration) => {
                if let Some(literal) = value.as_str() {
                    match table.literal(enumeration, literal) {
                        Ok(scalar) => {
                            leaf(InstanceOp::set(InstanceOp::Leaf(LeafOp::Write(scalar))))
                        }
                        Err(why) => missing.push(format!("{label}: {why}")),
                    }
                }
            }
            Kind::Flag => {
                if value == &json!(true) {
                    leaf(InstanceOp::Leaf(LeafOp::Enable));
                }
            }
            Kind::Ref => {
                if let Some(target) = value.as_str() {
                    leaf(InstanceOp::Leaf(LeafOp::Write(Scalar::text(target))));
                }
            }
            Kind::Refs => {
                for target in value.as_array().into_iter().flatten() {
                    if let Some(target) = target.as_str() {
                        leaf(InstanceOp::Leaf(LeafOp::Add(Scalar::text(target))));
                    }
                }
            }
        }
    }
    for (slot, pos, child) in children {
        let child_class_name = child["eClass"].as_str().unwrap_or("?");
        let child_class = match table.class(child_class_name) {
            Ok(c) => c,
            Err(why) => {
                missing.push(why);
                continue;
            }
        };
        ops.push((
            format!("{class_name} holds a {child_class_name}"),
            at.wrap(InstanceOp::field(
                slot,
                InstanceOp::insert(pos, InstanceOp::variant(child_class, InstanceOp::New)),
            ))
            .into_model_op(),
        ));
        let mut below = at.clone();
        below.steps.push((slot, pos, child_class));
        build(table, child, &below, ops, missing);
    }
}

/// U2's operations, in the order they are sent, or why they cannot be written.
fn u2_operations(descriptor: &Value, u2: &Value) -> (Vec<(String, ModelOp)>, Vec<String>) {
    let table = match from_descriptor(descriptor) {
        Ok(table) => Table(table),
        Err(why) => {
            return (
                Vec::new(),
                vec![format!("the node's parser refuses it: {why}")],
            );
        }
    };
    let mut missing = Vec::new();
    let root = match table.class(u2["eClass"].as_str().unwrap_or("?")) {
        Ok(root) => root,
        Err(why) => return (Vec::new(), vec![why]),
    };
    let mut ops = Vec::new();
    build(
        &table,
        u2,
        &Address {
            root,
            steps: Vec::new(),
        },
        &mut ops,
        &mut missing,
    );
    (ops, missing)
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

#[test]
fn ed10_u2_is_built_on_a_node_and_reads_back_on_two_replicas() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP ed10-u2: {why}");
            return;
        }
    };
    let failures = scenario(&binary).unwrap_or_else(|why| panic!("ed10-u2: set-up: {why}"));
    assert!(
        failures.is_empty(),
        "ED10 claim: the node does not host the class-diagram metamodel, or U2 does not read \
         back value for value:\n  {}",
        failures.join("\n  ")
    );
}

fn scenario(binary: &Path) -> Result<Vec<String>, String> {
    let u2: Value = serde_json::from_str(U2).map_err(|e| format!("the U2 fixture: {e}"))?;
    let path = descriptor_path();
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) => {
            println!("\nED10-U2 no descriptor at {}: {e}", path.display());
            return Ok(vec![format!(
                "no descriptor at {} ({e}): the metamodel is not shipped",
                path.display()
            )]);
        }
    };
    let descriptor: Value = match serde_json::from_str(&text) {
        Ok(d) => d,
        Err(e) => return Ok(vec![format!("{} is not JSON: {e}", path.display())]),
    };
    let digest = metamodel_digest(&descriptor);
    let mut fail = Vec::new();

    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let root = std::env::var_os("ED10_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let scratch = root.join(format!("moirai-ed10-u2-{}-{run}", std::process::id()));
    fs::create_dir_all(&scratch).map_err(|e| e.to_string())?;

    let http = Http::new()?;
    let a = spawn_node(binary, &scratch, "a", &[("LOG_ID", LOG_ID)], "")?;
    http.await_health(&a)?;
    let b = spawn_node(
        binary,
        &scratch,
        "b",
        &[("LOG_ID", LOG_ID)],
        &format!("a:127.0.0.1:{}", a.listen),
    )?;
    http.await_health(&b)?;
    for node in [&a, &b] {
        http.await_loop(node)?;
    }

    // S1: a node holding no descriptor takes the file's text.
    let (status, answer) = http.post(&a, "/api/metamodels", text.clone())?;
    let s1 = status == 201
        && answer["metamodel"]["digest"] == json!(digest)
        && answer["metamodel"]["nsURI"] == json!(NS_URI)
        && answer["metamodel"]["package"] == json!(PACKAGE);
    println!("\nED10-U2 S1 POST /api/metamodels: {status}, {answer}; as expected {s1}");
    if !s1 {
        fail.push(format!(
            "S1: POST /api/metamodels answered {status} {answer}, not 201 with digest {digest}, \
             nsURI {NS_URI}, package {PACKAGE}"
        ));
        drop((a, b));
        let _ = fs::remove_dir_all(&scratch);
        return Ok(fail);
    }

    // S2: listed, and served back value for value.
    let (_, listing) = http.get(&a, "/api/metamodels")?;
    let listed = listing["metamodels"]
        .as_array()
        .is_some_and(|l| l.iter().any(|m| m["digest"] == json!(digest)));
    let (status, first) = http.get(&a, "/api/metamodel")?;
    println!(
        "ED10-U2 S2 listed {listed}; GET /api/metamodel {status}, equal to the file {}",
        first == descriptor
    );
    if !listed {
        fail.push(format!("S2: a does not list {digest}: {listing}"));
    }
    if first != descriptor {
        fail.push(format!(
            "S2: GET /api/metamodel answered {status} a descriptor of digest {}",
            metamodel_digest(&first)
        ));
    }

    // S3: a model under the digest, named for its package, served the descriptor.
    let model = match http.register(&a, None, &digest) {
        Ok(id) => id,
        Err(why) => {
            fail.push(format!("S3: no model is created under {digest}: {why}"));
            drop((a, b));
            let _ = fs::remove_dir_all(&scratch);
            return Ok(fail);
        }
    };
    let named = model.len() == 32 + 1 + PACKAGE.len()
        && model[..32]
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        && model[32..] == format!("_{PACKAGE}");
    let (status, served) = http.get(&a, &format!("/api/model/{model}/metamodel"))?;
    println!(
        "ED10-U2 S3 model {model}, named <32 hex>_{PACKAGE} {named}; its metamodel {status}, \
         equal to the file {}",
        served == descriptor
    );
    if !named {
        fail.push(format!(
            "S3: the model id {model} is not <32 hex>_{PACKAGE}"
        ));
    }
    if served != descriptor {
        fail.push(format!(
            "S3: the model is served ({status}) a descriptor of digest {}",
            metamodel_digest(&served)
        ));
    }

    // S4: b, holding nothing, joins by the id alone before the build.
    http.until_seen(&b, &model)?;
    http.post(&b, "/api/models", json!({ "model_id": model }).to_string())?;
    match http.until_metamodel(&b, &model) {
        Ok(served) => {
            println!(
                "ED10-U2 S4 b joined by id, serves the descriptor, equal to the file {}",
                served == descriptor
            );
            if served != descriptor {
                fail.push(format!(
                    "S4: b serves a descriptor of digest {}",
                    metamodel_digest(&served)
                ));
            }
        }
        Err(why) => fail.push(format!("S4: {why}")),
    }

    // S5: U2, by operations on a.
    let (ops, missing) = u2_operations(&descriptor, &u2);
    for why in &missing {
        fail.push(format!("S5: U2 cannot be written: {why}"));
    }
    let mut refused = Vec::new();
    for (label, op) in &ops {
        let body = serde_json::to_string(op).map_err(|e| e.to_string())?;
        let (status, answer) = http.post(&a, &format!("/api/model/{model}/op"), body)?;
        if status != 200 {
            refused.push(format!("{label}: {status} {answer}"));
        }
    }
    println!(
        "ED10-U2 S5 operations sent {}, accepted {}, refused {}, not writable {}",
        ops.len(),
        ops.len() - refused.len(),
        refused.len(),
        missing.len()
    );
    for line in refused.iter().take(20) {
        println!("ED10-U2 S5 refused {line}");
        fail.push(format!("S5: refused {line}"));
    }
    if refused.len() > 20 {
        fail.push(format!("S5: and {} more refused", refused.len() - 20));
    }

    // S6: a reads U2 back, value for value.
    let doc_a = http.state(&a, &model)?;
    let s6 = doc_a == u2;
    println!("ED10-U2 S6 a's document equal to U2 {s6}");
    if !s6 {
        fail.push(format!(
            "S6: a's document differs from U2: {}",
            differences(&u2, &doc_a, "$").join("; ")
        ));
    }

    // S7: b, which joined before the build, reads the same.
    match http.until_serves(&b, &model, &u2) {
        Ok(()) => println!("ED10-U2 S7 b's document equal to U2 true"),
        Err(why) => {
            println!("ED10-U2 S7 b's document equal to U2 false");
            fail.push(format!("S7: {why}"));
        }
    }

    // S8: c, holding nothing, joins by the id alone after the build.
    let c = spawn_node(
        binary,
        &scratch,
        "c",
        &[("LOG_ID", LOG_ID)],
        &format!("a:127.0.0.1:{}", a.listen),
    )?;
    http.await_health(&c)?;
    http.await_loop(&c)?;
    http.until_seen(&c, &model)?;
    http.post(&c, "/api/models", json!({ "model_id": model }).to_string())?;
    match http.until_metamodel(&c, &model) {
        Ok(served) => {
            let same = served == descriptor;
            let doc = http.until_serves(&c, &model, &u2);
            println!(
                "ED10-U2 S8 c joined by id, descriptor equal to the file {same}, document equal \
                 to U2 {}",
                doc.is_ok()
            );
            if !same {
                fail.push(format!(
                    "S8: c serves a descriptor of digest {}",
                    metamodel_digest(&served)
                ));
            }
            if let Err(why) = doc {
                fail.push(format!("S8: {why}"));
            }
        }
        Err(why) => fail.push(format!("S8: {why}")),
    }
    drop((a, b, c));
    let _ = fs::remove_dir_all(&scratch);
    Ok(fail)
}

/// Where two documents differ, by path, the first twenty.
fn differences(want: &Value, got: &Value, at: &str) -> Vec<String> {
    let mut out = Vec::new();
    match (want, got) {
        (Value::Object(w), Value::Object(g)) => {
            for key in w.keys().chain(g.keys().filter(|k| !w.contains_key(*k))) {
                match (w.get(key), g.get(key)) {
                    (Some(a), Some(b)) => out.extend(differences(a, b, &format!("{at}.{key}"))),
                    (Some(a), None) => out.push(format!("{at}.{key}: {a} expected, absent")),
                    (None, Some(b)) => out.push(format!("{at}.{key}: absent expected, {b}")),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(w), Value::Array(g)) if w.len() == g.len() => {
            for (i, (a, b)) in w.iter().zip(g).enumerate() {
                out.extend(differences(a, b, &format!("{at}[{i}]")));
            }
        }
        _ if want != got => out.push(format!("{at}: {want} expected, {got}")),
        _ => {}
    }
    out.truncate(20);
    out
}

// ---------------------------------------------------------------------------
// Nodes, as `ar02_declarations_served_l3.rs` starts and reads them
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

    fn until_metamodel(&self, node: &Node, model: &str) -> Result<Value, String> {
        wait_until(
            &format!("{} to serve {model}'s descriptor", node.id),
            STEP_TIMEOUT,
            || match self.get(node, &format!("/api/model/{model}/metamodel")) {
                Ok((200, served)) => Ok(Some(served)),
                _ => Ok(None),
            },
        )
    }

    fn until_serves(&self, node: &Node, model: &str, want: &Value) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(&format!("{} to serve U2", node.id), STEP_TIMEOUT, || {
            if let Ok(state) = self.state(node, model) {
                last = state;
            }
            Ok((&last == want).then_some(()))
        })
        .map_err(|why| {
            format!(
                "{why}; it differs: {}",
                differences(want, &last, "$").join("; ")
            )
        })
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
