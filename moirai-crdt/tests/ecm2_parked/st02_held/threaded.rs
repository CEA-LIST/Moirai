//! ST.02, B2 (`cea-cdrt-knowledge/spec/items/ST.02.md`): the compositions of
//! `composition.rs` with a wrapper a user writes between each `OptionLog` and
//! its content, `Threaded<L>`, which implements `IsLog` and `EvalNested` by
//! handing each call to `L`, and runs `L`'s `effect`, `stabilize` and
//! `redundant_by_parent` on a thread of its own (a scoped thread, joined
//! before the call returns). Built and run by `../st02_wrapper_probe.rs` in a
//! crate of its own; exits 0 when every replica serves the rule's document
//! on every schedule, 1 otherwise.

#[macro_use]
#[path = "composition.rs"]
mod composition;

use composition::*;
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};

/// A wrapper that hands every call to what it wraps, the ones that change it
/// on another thread.
#[derive(Debug, Clone, Default)]
pub struct Threaded<L>(L);

fn elsewhere<R: Send>(work: impl FnOnce() -> R + Send) -> R {
    std::thread::scope(|scope| scope.spawn(work).join().expect("the wrapped call panicked"))
}

impl<L: IsLog + Send> IsLog for Threaded<L>
where
    L::Op: Send,
{
    type Command = L::Command;
    type Op = L::Op;
    type Rejection = L::Rejection;

    const STABILIZE_MOVES_OPERATIONS: bool = L::STABILIZE_MOVES_OPERATIONS;

    fn new() -> Self {
        Threaded(L::new())
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        self.0.prepare(command)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.0.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        let inner = &mut self.0;
        elsewhere(move || inner.effect(event, ctx))
    }

    fn stabilize(&mut self, version: &Version) {
        let inner = &mut self.0;
        elsewhere(move || inner.stabilize(version))
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        let inner = &mut self.0;
        elsewhere(move || inner.redundant_by_parent(version, conservative))
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

impl<L, Q> EvalNested<Q> for Threaded<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q> + Send,
    L::Op: Send,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

composition!(threaded, Threaded, same, Id, id_ref);

fn main() {
    let (failures, runs) = threaded::run(false);
    println!(
        "ST02-B2 Threaded: runs {runs}, replicas serving another value {} (ST.01 Flat {}, \
         ST.01 Outer {}, claim A {})",
        failures.len(),
        by_place(&failures, "St01Flat"),
        by_place(&failures, "St01Outer"),
        by_place(&failures, "ClaimA")
    );
    for line in failures.iter().take(4) {
        println!("ST02-B2 {line}");
    }
    std::process::exit(if failures.is_empty() { 0 } else { 1 });
}
