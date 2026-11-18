//! EC.03c's shim on the reference, `748a185`: copied over `ec03c/side.rs` when
//! the oracle files run there (spec/items/EC.03c.md, section 8). The set log
//! is `748a185`'s `nw15_common/nodes.rs` block, moved here unchanged.
#![allow(dead_code)]

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::clock::version_vector::Version;
use moirai_protocol::crdt::eval::EvalNested;
use moirai_protocol::crdt::query::{QueryOperation, Read};
use moirai_protocol::event::Event;
pub use moirai_protocol::log_id::LogId;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::effect_context::EffectContext;
use moirai_protocol::state::log::IsLog;
use moirai_protocol::state::po_log::VecLog;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub type Op = AWSet<String>;

/// An add-wins set of strings: `VecLog<AWSet<String>>`, wrapped so this
/// crate may implement `QueryableLog` for it, and serialisable so that it
/// can be transferred to a late joiner.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SetLog(VecLog<AWSet<String>>);

impl IsLog for SetLog {
    type Command = <VecLog<AWSet<String>> as IsLog>::Command;
    type Op = <VecLog<AWSet<String>> as IsLog>::Op;
    type Rejection = <VecLog<AWSet<String>> as IsLog>::Rejection;

    const STABILIZE_MOVES_OPERATIONS: bool =
        <VecLog<AWSet<String>> as IsLog>::STABILIZE_MOVES_OPERATIONS;

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

impl<Q> EvalNested<Q> for SetLog
where
    VecLog<AWSet<String>>: EvalNested<Q>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

impl QueryableLog for SetLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<Self::Op>>) -> Value {
        let value = replica.query(&Read::new());
        serde_json::to_value(&value)
            .unwrap_or_else(|e| json!({ "error": format!("cannot serialize the set: {e}") }))
    }
}
