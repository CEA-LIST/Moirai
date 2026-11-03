use moirai_protocol::{
    crdt::replicated_data_type::{ReplicatedDataType, Stable},
    state::unstable_state::IsUnstableCore,
};
use rand::Rng;

/// Generates valid and enabled updates for a replicated data type
/// given the current state of its log.
pub trait OpGenerator<U>: ReplicatedDataType
where
    U: IsUnstableCore<Self::Update>,
{
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Stable<Self>,
        unstable: &U,
    ) -> Self::Update;
}
