use crate::{
    clock::version_vector::Version,
    commitment::commit_op::CommitOp,
    crdt::sequential::SequentialDataType,
    event::Event,
    state::{
        effect_context::EffectContext, event_graph::EventGraph, log::IsLog,
        unstable_state::IsUnstableCore,
    },
};

#[derive(Debug, Clone)]
pub struct CommitLog<A>
where
    A: SequentialDataType,
{
    commited: A,
    unstable: EventGraph<CommitOp<A::Update>>,
}

impl<A> Default for CommitLog<A>
where
    A: SequentialDataType,
{
    fn default() -> Self {
        Self {
            commited: A::default(),
            unstable: EventGraph::default(),
        }
    }
}

impl<A> IsLog for CommitLog<A>
where
    A: SequentialDataType,
{
    type Value = A::Value;
    type Op = CommitOp<A::Update>;
    type Rejection = A::Rejection;

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        //   let state = self.materialize()?;
        //   state.is_enabled(&op.update)
        todo!()
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        self.unstable.append(event);
        // commitment protocol updates here
    }

    fn stabilize(&mut self, version: &Version) {
        todo!()
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        todo!()
    }

    fn is_default(&self) -> bool {
        todo!()
    }
}

impl<A> CommitLog<A>
where
    A: SequentialDataType,
{
    pub fn unstable(&self) -> &EventGraph<CommitOp<A::Update>> {
        &self.unstable
    }

    pub fn unstable_mut(&mut self) -> &mut EventGraph<CommitOp<A::Update>> {
        &mut self.unstable
    }
}
