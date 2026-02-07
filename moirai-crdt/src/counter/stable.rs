use moirai_protocol::crdt::redundancy::RedundancyRelation;
use moirai_protocol::event::tagged_op::TaggedOp;
use moirai_protocol::state::stable_state::IsStableState;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, SubAssign};

use crate::counter::resettable_counter::Counter as ResettableCounter;
use crate::counter::simple_counter::Counter as SimpleCounter;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

impl<V> IsStableState<ResettableCounter<V>> for CounterStable<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn is_default(&self) -> bool {
        self.0 == V::default()
    }

    fn apply(&mut self, value: ResettableCounter<V>) {
        match value {
            ResettableCounter::Inc(v) => self.0 += v,
            ResettableCounter::Dec(v) => self.0 -= v,
            ResettableCounter::Reset => unreachable!(),
        }
    }

    fn clear(&mut self) {
        self.0 = V::default();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<ResettableCounter<V>>,
        tagged_op: &TaggedOp<ResettableCounter<V>>,
    ) {
        if let ResettableCounter::Reset = tagged_op.op() {
            <CounterStable<V> as IsStableState<ResettableCounter<V>>>::clear(self)
        }
    }
}

impl<V> IsStableState<SimpleCounter<V>> for CounterStable<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn is_default(&self) -> bool {
        self.0 == V::default()
    }

    fn apply(&mut self, value: SimpleCounter<V>) {
        match value {
            SimpleCounter::Inc(v) => self.0 += v,
            SimpleCounter::Dec(v) => self.0 -= v,
        }
    }

    fn clear(&mut self) {
        self.0 = V::default();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<SimpleCounter<V>>,
        _tagged_op: &TaggedOp<SimpleCounter<V>>,
    ) {
    }
}
