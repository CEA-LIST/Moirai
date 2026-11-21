//! ST.06 (`cea-cdrt-knowledge/spec/items/ST.06.md`): a wrapper a user writes
//! between an `OptionLog` and its content, `Relay<L>`, which implements
//! `IsLog` (every method, the provided ones included) and `EvalNested` by
//! handing each call to `L`. What it does with a removal an optional hands
//! down (`OptionalContent`) is left to the binary that uses it:
//! `empty_impl.rs` writes the impl with no body, `explicit_body.rs` states
//! one that forwards the removal to `L`. Built by
//! `../st06_optional_content_required.rs` in a crate of its own.

use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};

/// A wrapper that hands every `IsLog` and `EvalNested` call to what it wraps.
#[derive(Debug, Clone, Default)]
pub struct Relay<L>(pub L);

impl<L: IsLog> IsLog for Relay<L> {
    type Command = L::Command;
    type Op = L::Op;
    type Rejection = L::Rejection;

    const STABILIZE_MOVES_OPERATIONS: bool = L::STABILIZE_MOVES_OPERATIONS;

    fn new() -> Self {
        Relay(L::new())
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

impl<L, Q> EvalNested<Q> for Relay<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.0.execute_query(q)
    }
}
