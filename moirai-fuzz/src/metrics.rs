use std::{
    cell::RefCell,
    fmt::Debug,
    time::{Duration, Instant},
};

use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::{event_graph::EventGraph, log::IsLog, po_log::POLog, unstable_state::IsUnstableState},
};
use rand::Rng;

use crate::op_generator::OpGeneratorNested;

// Thread-local flag to control stability behavior during fuzzing
thread_local! {
    static DISABLE_STABILITY: RefCell<bool> = const { RefCell::new(false) };
}

/// Set whether stability should be disabled for replicas created in this thread
pub fn set_disable_stability(disable: bool) {
    DISABLE_STABILITY.with(|flag| {
        *flag.borrow_mut() = disable;
    });
}

/// Get the current stability disable flag
fn get_disable_stability() -> bool {
    DISABLE_STABILITY.with(|flag| *flag.borrow())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct StructureMetrics {
    pub size: usize,
    pub width: usize,
    pub height: usize,
}

impl StructureMetrics {
    pub const fn empty() -> Self {
        Self {
            size: 0,
            width: 0,
            height: 0,
        }
    }

    pub const fn scalar() -> Self {
        Self {
            size: 1,
            width: 1,
            height: 1,
        }
    }

    pub fn object(children: impl IntoIterator<Item = Self>) -> Self {
        let children = children
            .into_iter()
            .filter(|m| m.size > 0)
            .collect::<Vec<_>>();
        if children.is_empty() {
            return Self::empty();
        }

        Self {
            size: children.len(),
            width: children
                .len()
                .max(children.iter().map(|m| m.width).max().unwrap_or(0)),
            height: 1 + children.iter().map(|m| m.height).max().unwrap_or(0),
        }
    }

    pub fn nested_collection(children: impl IntoIterator<Item = Self>) -> Self {
        let children = children
            .into_iter()
            .filter(|m| m.size > 0)
            .collect::<Vec<_>>();
        if children.is_empty() {
            return Self::empty();
        }

        Self {
            size: children.len(),
            width: children
                .len()
                .max(children.iter().map(|m| m.width).max().unwrap_or(0)),
            height: 1 + children.iter().map(|m| m.height).max().unwrap_or(0),
        }
    }

    // case of collection of scalar values (e.g., a set, a bag)
    pub fn collection(len: usize) -> Self {
        Self {
            size: len,
            width: 1,
            height: 1,
        }
    }
}

// TODO: verify the implementations (seems messy and not very accurate, but maybe it's good enough for now)
pub trait FuzzMetrics: IsLog {
    fn structure_metrics(&self) -> StructureMetrics;
}

impl<O> FuzzMetrics for EventGraph<O>
where
    O: moirai_protocol::crdt::pure_crdt::PureCRDT + Clone,
{
    fn structure_metrics(&self) -> StructureMetrics {
        if self.is_default() {
            StructureMetrics::empty()
        } else {
            StructureMetrics::scalar()
        }
    }
}

impl<O, U> FuzzMetrics for POLog<O, U>
where
    O: moirai_protocol::crdt::pure_crdt::PureCRDT + Clone,
    U: IsUnstableState<O> + Default + Debug,
{
    fn structure_metrics(&self) -> StructureMetrics {
        if self.is_default() {
            StructureMetrics::empty()
        } else {
            StructureMetrics::scalar()
        }
    }
}

impl<L> FuzzMetrics for Box<L>
where
    L: FuzzMetrics,
{
    fn structure_metrics(&self) -> StructureMetrics {
        (**self).structure_metrics()
    }
}

/// Wrapper autour d'un IsLog qui mesure le temps passé dans effect()
#[derive(Debug)]
pub struct MetricsLog<L: IsLog> {
    pub inner: L,
    pub total_effect_time: Duration,
    pub effect_call_count: usize,
}

impl<L: IsLog> MetricsLog<L> {
    pub fn new(inner: L) -> Self {
        Self {
            inner,
            total_effect_time: Duration::ZERO,
            effect_call_count: 0,
        }
    }
}

impl<L: IsLog> Default for MetricsLog<L> {
    fn default() -> Self {
        Self::new(L::default())
    }
}

impl<L: IsLog> IsLog for MetricsLog<L> {
    type Value = L::Value;
    type Op = L::Op;

    fn new() -> Self {
        Self::new(L::new())
    }

    fn prepare(op: Self::Op) -> Self::Op {
        L::prepare(op)
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        self.inner.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        let start = Instant::now();
        self.inner.effect(event);
        self.total_effect_time += start.elapsed();
        self.effect_call_count += 1;
    }

    fn stabilize(&mut self, version: &Version) {
        // Only stabilize if stability is not disabled
        if !get_disable_stability() {
            self.inner.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.inner.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.inner.is_default()
    }
}

// Déléguer EvalNested à l'inner log
impl<L, Q> EvalNested<Q> for MetricsLog<L>
where
    L: IsLog + EvalNested<Q>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        self.inner.execute_query(q)
    }
}

impl<L> OpGeneratorNested for MetricsLog<L>
where
    L: IsLog + OpGeneratorNested,
{
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        self.inner.generate(rng)
    }
}

impl<L> FuzzMetrics for MetricsLog<L>
where
    L: IsLog + FuzzMetrics,
{
    fn structure_metrics(&self) -> StructureMetrics {
        self.inner.structure_metrics()
    }
}
