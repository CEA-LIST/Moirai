#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_protocol::{
    crdt::redundancy::RedundancyRelation, event::tagged_op::TaggedOp,
    state::stable_state::IsStableState,
};

use crate::counter::resettable_counter::{Counter as ResettableCounter, CounterValue};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct CounterStable<V>(V);

impl<V> CounterStable<V> {
    pub fn into_inner(self) -> V {
        self.0
    }
    pub fn as_inner(&self) -> &V {
        &self.0
    }
    pub fn as_inner_mut(&mut self) -> &mut V {
        &mut self.0
    }
}

impl<V> IsStableState<ResettableCounter<V>> for CounterStable<V::Accumulator>
where
    V: CounterValue,
{
    fn is_default(&self) -> bool {
        V::value(&self.0) == V::default()
    }

    fn apply(&mut self, value: ResettableCounter<V>) {
        match value {
            ResettableCounter::Inc(v) => V::increment(&mut self.0, v),
            ResettableCounter::Dec(v) => V::decrement(&mut self.0, v),
            ResettableCounter::Reset => unreachable!(),
        }
    }

    fn clear(&mut self) {
        self.0 = V::Accumulator::default();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<ResettableCounter<V>>,
        tagged_op: &TaggedOp<ResettableCounter<V>>,
    ) {
        if let ResettableCounter::Reset = tagged_op.op() {
            <Self as IsStableState<ResettableCounter<V>>>::clear(self)
        }
    }
}
