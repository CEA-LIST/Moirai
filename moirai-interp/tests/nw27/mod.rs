//! NW.27 fixtures shared by `nw27_typed_decode.rs` and `nw27_model_replication.rs`.
//!
//! Spec: `cea-cdrt-knowledge/spec/items/NW.27.md`. Operations are composed as text from
//! shallow serialized templates, so the oracle never parses or builds a deep value
//! recursively, and their depth is read back by an iterative, quote-aware scanner that
//! shares no code with any decoder under test.
#![allow(dead_code)]

use moirai_interp::InstanceOp;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{attribute, class, containment, descriptor, feature_slot, table};
use serde_json::{Value, json};

/// The decoder limit every site shares.
pub const L: usize = 1024;
/// The deepest operation a node admits over HTTP.
pub const B: usize = 768;
/// The only scratch path the executor may write; every file a test writes goes under it.
pub const WORK: &str = "/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.27/work";

/// The deepest nesting of objects and arrays in `text`, counting the outermost container
/// as 1; brackets inside strings do not count.
pub fn depth(text: &str) -> usize {
    let (mut d, mut high, mut quoted, mut escape) = (0usize, 0, false, false);
    for b in text.bytes() {
        if quoted {
            if escape {
                escape = false
            } else if b == b'\\' {
                escape = true
            } else if b == b'"' {
                quoted = false
            }
        } else {
            match b {
                b'"' => quoted = true,
                b'[' | b'{' => {
                    d += 1;
                    high = high.max(d)
                }
                b']' | b'}' => d = d.checked_sub(1).expect("balanced JSON"),
                _ => (),
            }
        }
    }
    assert_eq!(d, 0, "balanced JSON");
    assert!(!quoted, "closed strings");
    high
}

/// One output line that says "depth" and carries `limit` as a whole number.
pub fn names_depth_and(text: &str, limit: usize) -> bool {
    let limit = limit.to_string();
    text.lines().any(|line| {
        line.to_ascii_lowercase().contains("depth")
            && line
                .split(|c: char| !c.is_ascii_digit())
                .any(|number| number == limit)
    })
}

/// The fixture metamodel `Deep`: one class `Node` holding a counter `count`, a single
/// containment `one` and a sequence containment `many`, both of `Node`. A single step
/// adds 4 JSON levels to an operation and a sequence step adds 7, so every depth from
/// 8 upward that the two can sum to, and in particular 768, 769, 1023, 1031, 1032,
/// 2047 and 2048, is reached exactly.
pub fn deep_descriptor() -> Value {
    descriptor(
        &["Node"],
        vec![(
            "Node",
            class(
                false,
                &[],
                vec![
                    attribute(
                        "count",
                        json!({"kind": "single"}),
                        json!({"kind": "counter", "num": "i32", "resettable": true}),
                    ),
                    containment("one", "Node", "single"),
                    containment("many", "Node", "sequence"),
                ],
            ),
        )],
        json!({}),
    )
}

pub fn deep_descriptor_text() -> String {
    serde_json::to_string(&deep_descriptor()).unwrap()
}

const NEW: &str = "\"New\"";

/// Levels `template` adds around what replaces its one `"New"`.
fn added(template: &str) -> usize {
    assert_eq!(template.matches(NEW).count(), 1, "one placeholder");
    depth(&template.replacen(NEW, "[0]", 1)) - 1
}

/// A `Deep` operation, as JSON text, whose depth is exactly `wanted`: a counter increment
/// at the end of a path of sequence and single steps from the root `Node`. As many
/// sequence steps as the arithmetic allows, so the operation is also the deepest kind for
/// the state it builds.
pub fn deep_op(wanted: usize) -> String {
    let sem = table(&deep_descriptor());
    let node = moirai_interp::testing::class_slot(&sem, "Node");
    let f = |name: &str| feature_slot(&sem, node, name);
    let text = |op: &InstanceOp| serde_json::to_string(op).unwrap();
    let leaf = text(&InstanceOp::field(
        f("count"),
        InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(1))),
    ));
    let single = text(&InstanceOp::field(
        f("one"),
        InstanceOp::variant(node, InstanceOp::New),
    ));
    let sequence = text(&InstanceOp::field(
        f("many"),
        InstanceOp::insert(0, InstanceOp::variant(node, InstanceOp::New)),
    ));
    let root =
        serde_json::to_string(&InstanceOp::variant(node, InstanceOp::New).into_model_op()).unwrap();
    let base = depth(&root.replacen(NEW, &leaf, 1));
    let (s, q) = (added(&single), added(&sequence));
    let span = wanted
        .checked_sub(base)
        .expect("deep enough for the root and leaf");
    let (singles, sequences) = (0..=span / q)
        .rev()
        .find_map(|c| {
            let rest = span - c * q;
            (rest % s == 0).then_some((rest / s, c))
        })
        .unwrap_or_else(|| {
            panic!("fixture: depth {wanted} is not a sum of {s} and {q} past {base}")
        });
    let mut inner = leaf;
    for _ in 0..singles {
        inner = single.replacen(NEW, &inner, 1);
    }
    for _ in 0..sequences {
        inner = sequence.replacen(NEW, &inner, 1);
    }
    let op = root.replacen(NEW, &inner, 1);
    assert_eq!(depth(&op), wanted, "fixture depth");
    op
}

/// A shallow `Deep` operation: increment the root's counter.
pub fn shallow_deep_op() -> String {
    deep_op(8)
}
