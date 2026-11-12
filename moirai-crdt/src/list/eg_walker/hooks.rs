use std::fmt::Debug;

use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    data_type::replicated::{Reset, Stabilization},
    state::unstable_state::CausalReplay,
};

use super::{List, ListRejection, ListUpdate, StableList, replay::replay};

/// Replay helpers shared by validation, reset, and stabilization.
pub(super) trait EgWalkerHooks<V> {
    fn validate(&self, op: &ListUpdate<V>, stable: &[V]) -> Result<(), ListRejection>;
    fn conservative_reset(&self, version: &Version, stable: &[V]) -> Reset<ListUpdate<V>>;
    fn stabilize(&self, frontier: &CausalFrontier, stable: &[V]) -> Stabilization<StableList<V>>;
}

impl<V, U> EgWalkerHooks<V> for U
where
    V: Clone + Debug,
    U: CausalReplay<ListUpdate<V>>,
{
    fn validate(&self, op: &ListUpdate<V>, stable: &[V]) -> Result<(), ListRejection> {
        let len = List::visible_len(stable, self);
        match op {
            ListUpdate::Insert { pos, .. } => (*pos <= len)
                .then_some(())
                .ok_or(ListRejection::OutOfBounds { pos: *pos, len }),
            ListUpdate::Update { pos } | ListUpdate::Delete { pos } => (*pos < len)
                .then_some(())
                .ok_or(ListRejection::OutOfBounds { pos: *pos, len }),
            ListUpdate::DeleteRange { start, len: count } => start
                .checked_add(*count)
                .filter(|end| *end <= len)
                .map(|_| ())
                .ok_or_else(|| ListRejection::OutOfBounds {
                    pos: start.saturating_add(*count),
                    len,
                }),
        }
    }

    fn conservative_reset(&self, version: &Version, stable: &[V]) -> Reset<ListUpdate<V>> {
        let len = replay(stable, self, self.predecessors(version)).visible_len();
        Reset::Inject(vec![ListUpdate::DeleteRange { start: 0, len }])
    }

    fn stabilize(&self, frontier: &CausalFrontier, stable: &[V]) -> Stabilization<StableList<V>> {
        let Some(cut) = self.greatest_critical_frontier(frontier) else {
            return Stabilization::None;
        };
        let prefix = self.iter().take_while(|event| cut.contains(event.id()));
        let state = replay(stable, self, prefix).to_list();
        Stabilization::Critical {
            frontier: cut,
            stable: state.into(),
        }
    }
}
