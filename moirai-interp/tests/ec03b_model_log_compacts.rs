//! EC.03b: `ip11`'s workload through `Replica<ModelLog>`, so that the log's
//! own `stabilize` is what compacts.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03b.md` (follow-up of
//! EC.03a's record: `ip11_every_rule_compacts_after_stability` runs on the
//! test harness and never reaches `ModelLog::stabilize`). The arms and their
//! operations are `ip11`'s (`moirai-interp/src/node.rs`); only the replicas
//! differ: two `ModelLog` replicas opened on the bench descriptor.

use moirai_interp::testing::{
    bench, bench_descriptor, class_slot, feature_slot, field, object, opened,
};
use moirai_interp::{InstanceOp, LeafOp, ModelOp, Node, Scalar};
use moirai_protocol::replica::IsReplica;

fn on(feature: &str, inner: InstanceOp) -> InstanceOp {
    let sem = bench();
    InstanceOp::field(
        feature_slot(&sem, class_slot(&sem, "Bench"), feature),
        inner,
    )
}

fn leaf(feature: &str, op: LeafOp) -> InstanceOp {
    on(feature, InstanceOp::Leaf(op))
}

fn at_root(inner: InstanceOp) -> ModelOp {
    ModelOp::Instance(InstanceOp::variant(class_slot(&bench(), "Bench"), inner))
}

/// Three writes to one feature, then the causality that makes them stable,
/// and the PO-Log length on either side of it, at the writer.
fn compaction(feature: &str, ops: [InstanceOp; 3]) -> (usize, usize) {
    let sem = bench();
    let (mut a, mut b) = opened("ec03b", &bench_descriptor());
    let len = |log: &moirai_interp::ModelLog| {
        field(&sem, object(log.root()), feature).map_or(0, Node::polog_len)
    };
    let mut events = Vec::new();
    for op in ops {
        events.push(
            a.send(at_root(op))
                .unwrap_or_else(|why| panic!("`{feature}`'s workload is enabled: {why:?}")),
        );
    }
    let before = len(a.state());
    for event in events {
        b.receive(event);
    }
    let tick = b
        .send(at_root(leaf("tick", LeafOp::Inc(Scalar::Int(1)))))
        .expect("the tick is enabled");
    a.receive(tick);
    (before, len(a.state()))
}

#[test]
fn ec03b_m4_every_rule_compacts_through_the_model_log() {
    let sem = bench();
    let item = class_slot(&sem, "Item");
    let count = |n: i64| InstanceOp::Leaf(LeafOp::Inc(Scalar::Int(n)));
    let item_count = |n: i64| {
        InstanceOp::variant(
            item,
            InstanceOp::field(feature_slot(&sem, item, "count"), count(n)),
        )
    };
    let inc = |f: &str| [1, 2, 3].map(|n| leaf(f, LeafOp::Inc(Scalar::Int(n))));
    let flag = |f: &str| [LeafOp::Enable, LeafOp::Disable, LeafOp::Enable].map(|op| leaf(f, op));
    let text =
        |f: &str, op: fn(Scalar) -> LeafOp| ["a", "b", "c"].map(|s| leaf(f, op(Scalar::text(s))));
    let int = |f: &str| [1, 2, 3].map(|n| leaf(f, LeafOp::Write(Scalar::Int(n))));

    let arms: Vec<(&str, [InstanceOp; 3])> = vec![
        ("counter", inc("counter")),
        ("simple", inc("simple")),
        ("flagEw", flag("flagEw")),
        ("flagDw", flag("flagDw")),
        ("regMv", text("regMv", LeafOp::Write)),
        ("regLww", text("regLww", LeafOp::Write)),
        ("regFair", text("regFair", LeafOp::Write)),
        ("regPo", int("regPo")),
        ("regTo", int("regTo")),
        (
            "enumReg",
            [0, 1, 2].map(|v| leaf("enumReg", LeafOp::Write(Scalar::Enum(0, v)))),
        ),
        ("setAw", text("setAw", LeafOp::Add)),
        ("setRw", text("setRw", LeafOp::Add)),
        (
            "bag",
            ["a", "a", "b"].map(|s| leaf("bag", LeafOp::Add(Scalar::text(s)))),
        ),
        (
            "optCounter",
            [1, 2, 3].map(|n| on("optCounter", InstanceOp::set(count(n)))),
        ),
        (
            "seqCounter",
            [
                on("seqCounter", InstanceOp::insert(0, count(1))),
                on("seqCounter", InstanceOp::at(0, count(2))),
                on("seqCounter", InstanceOp::at(0, count(3))),
            ],
        ),
        ("one", [1, 2, 3].map(|n| on("one", item_count(n)))),
        (
            "maybe",
            [1, 2, 3].map(|n| on("maybe", InstanceOp::set(item_count(n)))),
        ),
        (
            "many",
            [
                on("many", InstanceOp::insert(0, item_count(1))),
                on("many", InstanceOp::at(0, item_count(2))),
                on("many", InstanceOp::at(0, item_count(3))),
            ],
        ),
        (
            "refOne",
            ["i1", "i2", "i3"].map(|s| leaf("refOne", LeafOp::Write(Scalar::text(s)))),
        ),
        (
            "refMany",
            ["i1", "i2", "i3"].map(|s| leaf("refMany", LeafOp::Add(Scalar::text(s)))),
        ),
    ];

    let mut failures = Vec::new();
    for (feature, ops) in arms {
        let (before, after) = compaction(feature, ops);
        if before == 0 {
            failures.push(format!("`{feature}` held nothing to compact"));
        } else if after >= before {
            failures.push(format!(
                "`{feature}` did not compact: {before} operations before stability, {after} after"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
