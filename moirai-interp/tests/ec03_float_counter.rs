//! EC.03, listed difference D1: a resettable `f64` counter reads the exact
//! sum of its steps, rounded once, in every delivery order.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03.md`, section 2. Léo's
//! `c00d6c1` accumulates floating-point steps exactly (`bitrep`); the
//! reference `748a185` adds them in delivery order, so there `1e16 + 1 - 1e16`
//! reads 0 on a replica that folds the steps in that order and 1 on one that
//! folds `-1e16` before `1`. JSON numbers are such counters
//! (`json.metamodel.json`, `Number.value`). The value is computed by hand:
//! the exact sum is 1.
//!
//! This test is the candidate's only: it fails on the reference by design.

#[path = "ec03/side.rs"]
mod side;

use std::sync::{Arc, LazyLock};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::{JSON_DESCRIPTOR, class_slot, feature_slot, install, table};
use moirai_interp::{InstanceOp, ModelLog};
use moirai_protocol::{crdt::query::Read, replica::IsReplica};
use moirai_semantics::MetamodelSemantics;
use serde_json::{Value, json};

static SEM: LazyLock<Arc<MetamodelSemantics>> =
    LazyLock::new(|| table(&serde_json::from_str(JSON_DESCRIPTOR).expect("JSON")));

fn on(class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
    let class = class_slot(&SEM, class);
    InstanceOp::variant(
        class,
        InstanceOp::field(feature_slot(&SEM, class, feature), inner),
    )
}

/// Step the number at key `n` of the root object.
fn step(op: LeafOp) -> InstanceOp {
    on(
        "Object",
        "entry",
        InstanceOp::entry(
            Scalar::text("n"),
            on("Number", "value", InstanceOp::Leaf(op)),
        ),
    )
}

#[test]
fn ec03_d1_a_float_counter_reads_the_exact_sum_in_every_delivery_order() {
    let ids = ["a", "b", "c"];
    let mut r: Vec<_> = ids
        .iter()
        .map(|id| side::bootstrap::<ModelLog>(id, &ids))
        .collect();
    let descriptor: Value = serde_json::from_str(JSON_DESCRIPTOR).expect("JSON");
    let opening = r[0]
        .send(install("ec03-d1", &descriptor))
        .expect("an Install");
    r[1].receive(opening.clone());
    r[2].receive(opening);
    let big = r[0]
        .send(step(LeafOp::Inc(Scalar::float(1e16))).into_model_op())
        .expect("a step");
    let one = r[1]
        .send(step(LeafOp::Inc(Scalar::float(1.0))).into_model_op())
        .expect("a step");
    let back = r[2]
        .send(step(LeafOp::Dec(Scalar::float(1e16))).into_model_op())
        .expect("a step");
    // a folds 1e16, 1, -1e16; b folds 1, -1e16, 1e16; c folds -1e16, 1e16, 1.
    r[0].receive(one.clone());
    r[0].receive(back.clone());
    r[1].receive(back);
    r[1].receive(big.clone());
    r[2].receive(big);
    r[2].receive(one);
    for (id, replica) in ids.iter().zip(&r) {
        assert_eq!(
            replica.query(&Read::<Value>::new()),
            json!({"n": 1.0}),
            "replica {id}"
        );
    }
}
