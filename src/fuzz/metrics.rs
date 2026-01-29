use std::{
    cell::RefCell,
    fmt::Debug,
    time::{Duration, Instant},
};

use crate::protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::log::IsLog,
};

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

// Support pour le fuzzer
#[cfg(feature = "fuzz")]
impl<L> crate::fuzz::config::OpGeneratorNested for MetricsLog<L>
where
    L: IsLog + crate::fuzz::config::OpGeneratorNested,
{
    fn generate(&self, rng: &mut impl rand::RngCore) -> Self::Op {
        self.inner.generate(rng)
    }
}
