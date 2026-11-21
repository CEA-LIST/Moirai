//! ST.02, B2 (`cea-cdrt-knowledge/spec/items/ST.02.md`): the compositions of
//! `composition.rs` with a wrapper a user writes between each `OptionLog` and
//! its content, `Wrapped<L>`, which implements `IsLog` (every method, the
//! provided ones included) and `EvalNested` by handing each call to `L`.
//! Built and run by `../st02_wrapper_probe.rs` in a crate of its own; exits 0
//! when every replica serves the rule's document on every schedule, 1
//! otherwise.

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

/// A wrapper that hands every call to what it wraps.
#[derive(Debug, Clone, Default)]
pub struct Wrapped<L>(L);

impl<L: IsLog> IsLog for Wrapped<L> {
    type Command = L::Command;
    type Op = L::Op;
    type Rejection = L::Rejection;

    const STABILIZE_MOVES_OPERATIONS: bool = L::STABILIZE_MOVES_OPERATIONS;

    fn new() -> Self {
        Wrapped(L::new())
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        self.0.prepare(command)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.0.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        self.0.effect(event, ctx)
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.stabilize(version)
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative)
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

impl<L, Q> EvalNested<Q> for Wrapped<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

composition!(wrapped, Wrapped, same, Id, id_ref);

fn main() {
    let mut failures = Vec::new();
    for threads in [false, true] {
        let (found, runs) = wrapped::run(threads);
        println!(
            "ST02-B2 Wrapped, threads {threads}: runs {runs}, replicas serving another value {} \
             (ST.01 Flat {}, ST.01 Outer {}, claim A {})",
            found.len(),
            by_place(&found, "St01Flat"),
            by_place(&found, "St01Outer"),
            by_place(&found, "ClaimA")
        );
        failures.extend(found);
    }
    for line in failures.iter().take(4) {
        println!("ST02-B2 {line}");
    }
    std::process::exit(if failures.is_empty() { 0 } else { 1 });
}
