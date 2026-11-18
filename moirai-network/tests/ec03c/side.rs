//! EC.03c's shim (spec/items/EC.03c.md): what the network tests need that
//! Léo's trait API names differently from `748a185`'s. `side_748a185.rs` is
//! the same on the reference. The log id lives in `moirai-network`.
#![allow(dead_code)]

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::clock::causal_frontier::CausalFrontier;
use moirai_protocol::clock::version_vector::Version;
use moirai_protocol::crdt::eval::EvalNested;
use moirai_protocol::crdt::query::{QueryOperation, Read};
use moirai_protocol::event::Event;
use moirai_protocol::replica::{IsReplica, Replica, ReplicaId};
use moirai_protocol::state::effect_context::EffectContext;
use moirai_protocol::state::log::IsLog;
use moirai_protocol::state::po_log::VecLog;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub use moirai_network::log_id::LogId;

type Inner = VecLog<AWSet<String>>;

pub type Op = <Inner as IsLog>::Update;

/// `748a185`'s add-wins set of strings, on Léo's `IsLog`: every method
/// forwards to the `VecLog`, the transfer hooks included.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SetLog(Inner);

impl IsLog for SetLog {
    type Command = <Inner as IsLog>::Command;
    type Update = <Inner as IsLog>::Update;
    type Rejection = <Inner as IsLog>::Rejection;

    fn hosted_by(&mut self, id: &ReplicaId) {
        self.0.hosted_by(id)
    }

    fn adopted_from(&mut self, donor: &Version) {
        self.0.adopted_from(donor)
    }

    fn prepare(&self, command: Self::Command) -> Self::Update {
        self.0.prepare(command)
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        self.0.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        self.0.effect(event, ctx)
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.0.stabilize(frontier)
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
    Inner: EvalNested<Q>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

impl QueryableLog for SetLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<Self::Update>>) -> Value {
        let value = replica.query(&Read::new());
        serde_json::to_value(&value)
            .unwrap_or_else(|e| json!({ "error": format!("cannot serialize the set: {e}") }))
    }
}
