use moirai_protocol::{
    crdt::replicated_data_type::ReplicatedDataType, state::unstable_state::IsUnstableCore,
};
use rand::Rng;

/// Generates valid and enabled updates for a replicated data type
/// given the current state of its log.
pub trait OpGenerator<U>: ReplicatedDataType
where
    U: IsUnstableCore<Self>,
{
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Self;
}
