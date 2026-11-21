//! The counter without reset, `748a185`'s `moirai-crdt` `simple_counter.rs`
//! on Léo's trait API: steps fold in delivery order, into a plain sum.
//!
//! Léo's `Counter<V>` is resettable and sums floats exactly; this one keeps
//! the sum `748a185` keeps, so `LeafRule::Counter { resettable: false }`
//! reads as it did there.

use std::{convert::Infallible, marker::PhantomData};

use moirai_crdt::counter::{CounterValue, update::CounterUpdate};
use moirai_protocol::{
    crdt::{
        eval::Eval,
        query::Read,
        replicated_data_type::{
            CausalCompaction, CausalReset, CausalStabilize, Precondition, ReplicatedDataType,
            Sequential,
        },
    },
    state::unstable_state::IsUnstableCore,
};

/// A counter that only steps up and down.
#[derive(Clone, Debug)]
pub struct SimpleCounter<V>(PhantomData<V>);

/// Its stable sum, added to in the order operations stabilize.
#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SimpleStable<V>(V);

impl<V: CounterValue> Sequential for SimpleStable<V> {
    type Update = CounterUpdate<V>;

    fn apply(&mut self, update: &Self::Update) {
        step(&mut self.0, update);
    }

    fn is_default(&self) -> bool {
        self.0 == V::default()
    }
}

/// `Reset` is never built for this counter (`leaf::simple_counter_op`).
fn step<V: CounterValue>(sum: &mut V, update: &CounterUpdate<V>) {
    match update {
        CounterUpdate::Inc(v) => *sum += *v,
        CounterUpdate::Dec(v) => *sum -= *v,
        CounterUpdate::Reset => unreachable!("a counter without reset takes no reset"),
    }
}

impl<V: CounterValue> ReplicatedDataType for SimpleCounter<V> {
    type Update = CounterUpdate<V>;
    type Stable = SimpleStable<V>;
    type Rejection = Infallible;
}

impl<V: CounterValue, U> Precondition<U> for SimpleCounter<V> {
    fn is_enabled(_: &Self::Stable, _: &U, _: &Self::Update) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<V: CounterValue, U: IsUnstableCore<CounterUpdate<V>>> CausalCompaction<U>
    for SimpleCounter<V>
{
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
}

impl<V: CounterValue, U> CausalStabilize<U> for SimpleCounter<V> {}

impl<V: CounterValue, U> CausalReset<U> for SimpleCounter<V> {}

impl<V: CounterValue, U: IsUnstableCore<CounterUpdate<V>>> Eval<SimpleStable<V>, U, Read<V>>
    for SimpleCounter<V>
{
    fn execute_query(stable: &SimpleStable<V>, unstable: &U, _q: &Read<V>) -> V {
        let mut sum = stable.0;
        for op in unstable.iter() {
            step(&mut sum, op.op());
        }
        sum
    }
}
