//! ED.16a (`cea-cdrt-knowledge/spec/items/ED.16a.md`), the level-3 half: a `model_node` serving
//! the notation descriptor Arachne ships (`examples/notation.metamodel.json`) holds several
//! diagrams for one model, each with its own nodes, positions and hidden elements; a view written
//! the way the editor writes one today reads as it reads under the old descriptor; and two
//! replicas, partitioned while one hides a box and the other moves it, read one document after
//! the heal, in which the box is both hidden and moved.
//!
//! - L1, the old view (twin). The operations today's editor sends for a view (`createViewOps`,
//!   `mintViewNodeOps`, one `dropOps`), sent to one node holding the descriptor shipped at
//!   Arachne `dc16103` (`fixtures/notation.metamodel.json`, digest `9d551713…`) and to one holding
//!   the shipped descriptor, read back the same, but for exactly `"diagrams": []` on the root and
//!   `"visible": null` on each node, which the new descriptor adds unwritten.
//! - L2, two diagrams. On `a`: the root diagram (the General View) holds Car and Wheel; a second
//!   diagram in the root's `diagrams`, of kind `interconnection` (`View.type`), framing Car
//!   (`View.element`), named "Car and its parts", holds its own Wheel and Engine at other
//!   positions. `a` and `b` read each Wheel where its own diagram put it.
//! - L3, hide on one. `a` hides Wheel on the second diagram (`Node.visible` false); on both
//!   replicas it is hidden there and unwritten (`null`, shown) on the root.
//! - L4, the concurrent schedule. Both paused (`/api/pause-all`): `a` hides Car on the root while
//!   `b` moves the same node; `a` shows Wheel on the second diagram while `b` hides it; each adds
//!   a diagram at the same position. Resumed, once each has delivered every operation, `a` and
//!   `b` serve one document, in which Car on the root is hidden at `b`'s position, Wheel on the
//!   second diagram holds one of the two values, and the root holds three diagrams, both new ones
//!   among them.
//!
//! The descriptor is read from the Arachne checkout beside this one
//! (`../arachne-model-plane/examples/notation.metamodel.json`), or from `ED16A_DESCRIPTOR`.
//!
//! ```bash
//! MOIRAI_E2E_INTERP_NODE_BIN=/volatile/home/vp242103/.cache/cea-crdt-scratch/bin/model_node-1cf5d55 \
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test ed16a_notation_diagrams_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: `E2E-SKIP ed16a-l3: <why>` and return. A skip is not a pass. A claim failure panics
//! with `ED16A claim:`; a harness failure with `set-up:`.
#![cfg(feature = "network")]

#[path = "st01/cluster.rs"]
mod cluster;

use std::fs;
use std::path::PathBuf;

use cluster::{Cluster, node_binary};
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

const OLD: &str = include_str!("fixtures/notation.metamodel.json");
const OLD_DIGEST: &str = "9d551713bd2d1b1cfd8c38e591ed33a454706305278f0d1c1dc7d3589167506c";
const MODEL_OF_THE_VIEW: &str = "0123456789abcdef0123456789abcdef_umlclass";

fn descriptor_path() -> PathBuf {
    std::env::var_os("ED16A_DESCRIPTOR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../arachne-model-plane/examples/notation.metamodel.json")
        })
}

// ---------------------------------------------------------------------------
// Operations, by name through the descriptor's table, as the editor's encoder does
// ---------------------------------------------------------------------------

/// The table, with lookups that say what the descriptor lacks instead of panicking.
struct Notation(MetamodelSemantics);

/// One step down from a diagram: the containment, the position, the class there.
type Step = (&'static str, usize, &'static str);

impl Notation {
    fn class(&self, name: &str) -> Result<moirai_semantics::ClassSlot, String> {
        self.0
            .classes
            .iter()
            .find(|c| &*c.name == name)
            .map(|c| c.slot)
            .ok_or_else(|| format!("the descriptor has no class `{name}`"))
    }

    fn feature(&self, class: &str, name: &str) -> Result<moirai_semantics::FeatureSlot, String> {
        let slot = self.class(class)?;
        let holder = &self.0.classes[slot.index()];
        holder
            .visible
            .iter()
            .position(|(feature, _, _)| &**feature == name)
            .map(|i| moirai_semantics::FeatureSlot(i as u16))
            .ok_or_else(|| format!("`{class}` has no feature `{name}`"))
    }

    /// `inner` on the object reached from the root `Diagram` by `steps`.
    fn on(&self, steps: &[Step], inner: InstanceOp) -> Result<ModelOp, String> {
        let mut op = inner;
        let mut holders: Vec<&str> = vec!["Diagram"];
        holders.extend(steps.iter().map(|(_, _, class)| *class));
        for (index, (feature, pos, class)) in steps.iter().enumerate().rev() {
            op = InstanceOp::field(
                self.feature(holders[index], feature)?,
                InstanceOp::at(*pos, InstanceOp::variant(self.class(class)?, op)),
            );
        }
        Ok(InstanceOp::variant(self.class("Diagram")?, op).into_model_op())
    }

    /// The feature `feature` of the object at `steps`, written with `leaf`.
    fn write(&self, steps: &[Step], feature: &str, leaf: InstanceOp) -> Result<ModelOp, String> {
        let class = steps.last().map_or("Diagram", |(_, _, class)| *class);
        self.on(
            steps,
            InstanceOp::field(self.feature(class, feature)?, leaf),
        )
    }

    /// A new `class` at `pos` of `containment` of the object at `steps`.
    fn insert(
        &self,
        steps: &[Step],
        containment: &str,
        pos: usize,
        class: &str,
    ) -> Result<ModelOp, String> {
        let holder = steps.last().map_or("Diagram", |(_, _, class)| *class);
        let new = InstanceOp::insert(
            pos,
            InstanceOp::variant(self.class(class)?, InstanceOp::New),
        );
        self.on(
            steps,
            InstanceOp::field(self.feature(holder, containment)?, new),
        )
    }

    fn root(&self) -> Result<ModelOp, String> {
        Ok(InstanceOp::variant(self.class("Diagram")?, InstanceOp::New).into_model_op())
    }
}

fn text(value: &str) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Write(Scalar::text(value)))
}

fn int(value: i64) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Write(Scalar::Int(value)))
}

fn flag(value: bool) -> InstanceOp {
    InstanceOp::Leaf(LeafOp::Write(Scalar::Bool(value)))
}

/// `Diagram.name` is an optional text: one character per operation.
fn name_ops(n: &Notation, steps: &[Step], name: &str) -> Result<Vec<ModelOp>, String> {
    name.chars()
        .enumerate()
        .map(|(pos, ch)| {
            n.write(
                steps,
                "name",
                InstanceOp::set(InstanceOp::Leaf(LeafOp::InsertChar { pos, ch })),
            )
        })
        .collect()
}

/// A `View`'s three strings.
fn view_ops(
    n: &Notation,
    steps: &[Step],
    id: &str,
    kind: &str,
    element: &str,
) -> Result<Vec<ModelOp>, String> {
    Ok(vec![
        n.write(steps, "id", text(id))?,
        n.write(steps, "type", text(kind))?,
        n.write(steps, "element", text(element))?,
    ])
}

/// A `Node` at `pos` of the `children` of the diagram at `diagram`, as `mintViewNodeOps` writes
/// it.
fn node_ops(
    n: &Notation,
    diagram: &[Step],
    pos: usize,
    id: &str,
    element: &str,
    at: (i64, i64),
) -> Result<Vec<ModelOp>, String> {
    let mut ops = vec![n.insert(diagram, "children", pos, "Node")?];
    let mut steps = diagram.to_vec();
    steps.push(("children", pos, "Node"));
    ops.extend(view_ops(n, &steps, id, "PartUsage", element)?);
    ops.push(n.write(&steps, "x", int(at.0))?);
    ops.push(n.write(&steps, "y", int(at.1))?);
    Ok(ops)
}

/// What today's editor sends for a view of one model with two boxes, one of them then dragged.
fn todays_view(n: &Notation) -> Result<Vec<ModelOp>, String> {
    let mut ops = vec![n.root()?];
    ops.extend(view_ops(n, &[], "v-root", "Diagram", MODEL_OF_THE_VIEW)?);
    ops.extend(node_ops(n, &[], 0, "n-car", "e-car", (10, 20))?);
    ops.extend(node_ops(n, &[], 1, "n-wheel", "e-wheel", (200, 20))?);
    ops.push(n.write(&[("children", 0, "Node")], "x", int(15))?);
    ops.push(n.write(&[("children", 0, "Node")], "y", int(25))?);
    Ok(ops)
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// The document with exactly the two features the new descriptor adds taken off where they are
/// unwritten: `"diagrams": []` on a `Diagram`, `"visible": null` on a `Node`. Anything else in
/// them (a diagram held, a node hidden) is left, and so differs.
fn without_unwritten_additions(doc: &Value) -> Value {
    match doc {
        Value::Array(items) => {
            Value::Array(items.iter().map(without_unwritten_additions).collect())
        }
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .filter(|(key, value)| {
                    let class = fields.get("eClass").and_then(Value::as_str);
                    !(class == Some("Diagram")
                        && key.as_str() == "diagrams"
                        && **value == json!([])
                        || class == Some("Node") && key.as_str() == "visible" && value.is_null())
                })
                .map(|(key, value)| (key.clone(), without_unwritten_additions(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn node_at<'a>(diagram: &'a Value, element: &str) -> Option<&'a Value> {
    diagram["children"]
        .as_array()?
        .iter()
        .find(|node| node["element"] == json!(element))
}

fn summary(node: Option<&Value>) -> String {
    match node {
        None => "absent".into(),
        Some(node) => format!("({}, {}) visible {}", node["x"], node["y"], node["visible"]),
    }
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

#[test]
fn ed16a_l3_several_diagrams_per_model_hide_and_move_converge() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP ed16a-l3: {why}");
            return;
        }
    };
    let failures = scenario(&binary).unwrap_or_else(|why| panic!("ed16a-l3: set-up: {why}"));
    assert!(
        failures.is_empty(),
        "ED16A claim: the notation descriptor does not hold several diagrams per model with their \
         own content, or an old view reads differently, or two replicas do not read one view:\n  \
         {}",
        failures.join("\n  ")
    );
}

fn submit_all(cluster: &Cluster, node: usize, model: &str, ops: &[ModelOp]) -> Result<u64, String> {
    for op in ops {
        cluster.submit(node, model, op)?;
    }
    Ok(ops.len() as u64)
}

fn scenario(binary: &PathBuf) -> Result<Vec<String>, String> {
    let mut fail = Vec::new();
    let old: Value = serde_json::from_str(OLD).map_err(|e| format!("the old fixture: {e}"))?;
    if metamodel_digest(&old) != OLD_DIGEST {
        return Err(format!("the old fixture is not {OLD_DIGEST}"));
    }
    let path = descriptor_path();
    let new_text = fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let new: Value =
        serde_json::from_str(&new_text).map_err(|e| format!("{}: {e}", path.display()))?;
    let new_digest = metamodel_digest(&new);
    println!("\nED16A-L3 shipped notation descriptor digest {new_digest}, old {OLD_DIGEST}");
    let old_table = Notation(from_descriptor(&old).map_err(|e| format!("the old table: {e}"))?);
    let new_table = match from_descriptor(&new) {
        Ok(table) => Notation(table),
        Err(why) => {
            return Ok(vec![format!(
                "the node's parser refuses the shipped descriptor: {why}"
            )]);
        }
    };

    // ---- L1: today's view, on the old descriptor and on the new one ----
    let old_doc = {
        let cluster = Cluster::start(binary, &["o"], &[("notation", OLD)])?;
        let model = cluster.register(0, None, OLD_DIGEST)?;
        let ops = todays_view(&old_table)?;
        submit_all(&cluster, 0, &model, &ops)?;
        cluster.until_delivered(0, &model, 1 + ops.len() as u64)?;
        cluster.default_state(0, &model)?
    };
    println!("ED16A-L1 today's view under {OLD_DIGEST}: {old_doc}");

    let cluster = Cluster::start(binary, &["a", "b"], &[("notation", new_text.as_str())])?;
    const A: usize = 0;
    const B: usize = 1;
    {
        let model = cluster.register(A, None, &new_digest)?;
        let ops = match todays_view(&new_table) {
            Ok(ops) => ops,
            Err(why) => {
                fail.push(format!("L1: today's view cannot be written: {why}"));
                return Ok(fail);
            }
        };
        submit_all(&cluster, A, &model, &ops)?;
        cluster.until_delivered(A, &model, 1 + ops.len() as u64)?;
        let new_doc = cluster.default_state(A, &model)?;
        let same = without_unwritten_additions(&new_doc) == old_doc;
        println!("ED16A-L1 today's view under {new_digest}: {new_doc}; same in meaning {same}");
        if !same {
            fail.push(format!(
                "L1: today's view reads {new_doc} under the shipped descriptor, {old_doc} under \
                 {OLD_DIGEST}"
            ));
        }
    }

    // ---- L2: two diagrams of one model, each with its own nodes ----
    let model = cluster.register(A, None, &new_digest)?;
    cluster.register(B, Some(&model), &new_digest)?;
    cluster.until_delivered(B, &model, 1)?;
    let n = &new_table;
    let second: [Step; 1] = [("diagrams", 0, "Diagram")];
    let build = (|| -> Result<Vec<ModelOp>, String> {
        let mut ops = vec![n.root()?];
        ops.extend(view_ops(n, &[], "v-root", "Diagram", MODEL_OF_THE_VIEW)?);
        ops.extend(node_ops(n, &[], 0, "g-car", "e-car", (10, 10))?);
        ops.extend(node_ops(n, &[], 1, "g-wheel", "e-wheel", (10, 200))?);
        ops.push(n.insert(&[], "diagrams", 0, "Diagram")?);
        ops.extend(view_ops(n, &second, "d2", "interconnection", "e-car")?);
        ops.extend(name_ops(n, &second, "Car and its parts")?);
        ops.extend(node_ops(n, &second, 0, "i-wheel", "e-wheel", (300, 40))?);
        ops.extend(node_ops(n, &second, 1, "i-engine", "e-engine", (300, 140))?);
        Ok(ops)
    })();
    let ops = match build {
        Ok(ops) => ops,
        Err(why) => {
            fail.push(format!("L2: two diagrams cannot be written: {why}"));
            return Ok(fail);
        }
    };
    let mut sent = 1 + submit_all(&cluster, A, &model, &ops)?;
    for node in [A, B] {
        cluster.until_delivered(node, &model, sent)?;
    }
    let (on_a, on_b) = (
        cluster.default_state(A, &model)?,
        cluster.default_state(B, &model)?,
    );
    let d2 = &on_a["diagrams"][0];
    println!(
        "ED16A-L2 root Wheel {}, second diagram type {} element {} name {} Wheel {}; a == b {}",
        summary(node_at(&on_a, "e-wheel")),
        d2["type"],
        d2["element"],
        d2["name"],
        summary(node_at(d2, "e-wheel")),
        on_a == on_b
    );
    let at = |node: Option<&Value>, x: i64, y: i64| {
        node.is_some_and(|node| node["x"] == json!(x) && node["y"] == json!(y))
    };
    if !(at(node_at(&on_a, "e-wheel"), 10, 200) && at(node_at(d2, "e-wheel"), 300, 40)) {
        fail.push(format!(
            "L2: Wheel is not at its own place on each diagram: {on_a}"
        ));
    }
    if node_at(&on_a, "e-engine").is_some() || node_at(d2, "e-car").is_some() {
        fail.push(format!(
            "L2: a diagram shows an element only the other holds: {on_a}"
        ));
    }
    if d2["type"] != json!("interconnection")
        || d2["element"] != json!("e-car")
        || d2["name"] != json!("Car and its parts")
        || d2["eClass"] != json!("Diagram")
    {
        fail.push(format!(
            "L2: the second diagram's kind, frame or name do not read back: {d2}"
        ));
    }
    if on_a != on_b {
        fail.push(format!("L2: a reads {on_a}, b reads {on_b}"));
    }

    // ---- L3: hide on one diagram leaves the other ----
    let hide = n
        .write(
            &[("diagrams", 0, "Diagram"), ("children", 0, "Node")],
            "visible",
            flag(false),
        )
        .map_err(|why| format!("Node.visible: {why}"));
    match hide {
        Err(why) => {
            fail.push(format!("L3: {why}"));
            return Ok(fail);
        }
        Ok(op) => {
            cluster.submit(A, &model, &op)?;
            sent += 1;
        }
    }
    for node in [A, B] {
        cluster.until_delivered(node, &model, sent)?;
    }
    let (on_a, on_b) = (
        cluster.default_state(A, &model)?,
        cluster.default_state(B, &model)?,
    );
    let d2 = &on_a["diagrams"][0];
    println!(
        "ED16A-L3 Wheel on the second diagram {}, on the root {}; a == b {}",
        summary(node_at(d2, "e-wheel")),
        summary(node_at(&on_a, "e-wheel")),
        on_a == on_b
    );
    if node_at(d2, "e-wheel").map(|w| &w["visible"]) != Some(&json!(false))
        || !node_at(&on_a, "e-wheel").is_some_and(|w| w["visible"].is_null())
    {
        fail.push(format!(
            "L3: Wheel is not hidden on the second diagram alone: {on_a}"
        ));
    }
    if on_a != on_b {
        fail.push(format!("L3: a reads {on_a}, b reads {on_b}"));
    }

    // ---- L4: partitioned, a hides what b moves; a shows what b hides; both add a diagram ----
    cluster.pause_all(A)?;
    cluster.pause_all(B)?;
    let car: [Step; 1] = [("children", 0, "Node")];
    let wheel2: [Step; 2] = [("diagrams", 0, "Diagram"), ("children", 0, "Node")];
    let added: [Step; 1] = [("diagrams", 1, "Diagram")];
    let on_a_ops = vec![
        n.write(&car, "visible", flag(false))?,
        n.write(&wheel2, "visible", flag(true))?,
        n.insert(&[], "diagrams", 1, "Diagram")?,
        n.write(&added, "id", text("from-a"))?,
    ];
    let on_b_ops = vec![
        n.write(&car, "x", int(111))?,
        n.write(&car, "y", int(222))?,
        n.write(&wheel2, "visible", flag(false))?,
        n.insert(&[], "diagrams", 1, "Diagram")?,
        n.write(&added, "id", text("from-b"))?,
    ];
    sent += submit_all(&cluster, A, &model, &on_a_ops)?;
    sent += submit_all(&cluster, B, &model, &on_b_ops)?;
    cluster.resume_all(A)?;
    cluster.resume_all(B)?;
    for node in [A, B] {
        cluster.until_delivered(node, &model, sent)?;
    }
    let (on_a, on_b) = (
        cluster.default_state(A, &model)?,
        cluster.default_state(B, &model)?,
    );
    let ids: Vec<Value> = on_a["diagrams"]
        .as_array()
        .map(|list| list.iter().map(|d| d["id"].clone()).collect())
        .unwrap_or_default();
    let car_now = node_at(&on_a, "e-car");
    let wheel_now = node_at(&on_a["diagrams"][0], "e-wheel").map(|w| w["visible"].clone());
    println!(
        "ED16A-L4 Car on the root {}; Wheel on the second diagram visible {:?}; diagrams {:?}; \
         a == b {}",
        summary(car_now),
        wheel_now,
        ids,
        on_a == on_b
    );
    if on_a != on_b {
        fail.push(format!("L4: after the heal a reads {on_a}, b reads {on_b}"));
    }
    if !car_now.is_some_and(|c| {
        c["visible"] == json!(false) && c["x"] == json!(111) && c["y"] == json!(222)
    }) {
        fail.push(format!(
            "L4: Car on the root is not both hidden and moved: {}",
            summary(car_now)
        ));
    }
    if !matches!(wheel_now, Some(Value::Bool(_))) {
        fail.push(format!(
            "L4: Wheel's visibility on the second diagram is {wheel_now:?}"
        ));
    }
    if ids.len() != 3
        || ids[0] != json!("d2")
        || !ids.contains(&json!("from-a"))
        || !ids.contains(&json!("from-b"))
    {
        fail.push(format!(
            "L4: the root holds diagrams {ids:?}, not d2 and both added ones"
        ));
    }
    Ok(fail)
}
