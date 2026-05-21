use crate::{
    clock::version_vector::Version,
    crdt::pure_crdt::PureCRDT,
    event::Event,
    state::{effect_context::EffectContext, log::IsLog, unstable_state::event_graph::EventGraph},
};

#[derive(Debug, Clone)]
pub struct GraphLog<O> {
    graph: EventGraph<O>,
}

impl<O> IsLog for GraphLog<O>
where
    O: PureCRDT + Clone,
{
    type Value = <O as PureCRDT>::Value;
    type Op = O;

    fn new() -> Self {
        const {
            debug_assert!(O::DISABLE_R_WHEN_NOT_R && O::DISABLE_R_WHEN_R && O::DISABLE_STABILIZE);
        }
        Self {
            graph: Default::default(),
        }
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        self.graph.effect(event, ctx);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.graph.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.graph.is_default()
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        self.graph.is_enabled(op)
    }

    fn stabilize(&mut self, version: &Version) {
        self.graph.stabilize(version);
    }
}

impl<O> Default for GraphLog<O> {
    fn default() -> Self {
        Self {
            graph: Default::default(),
        }
    }
}
