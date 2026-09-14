mod causal_cursor;
mod document;
mod item;
mod presence_state;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::CausalOpGenerator, value_generator::ValueGenerator};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::Eval,
        query::{QueryOperation, Read},
        redundancy::RedundancyRelation,
        replicated_data_type::{CausalReset, ReplicatedDataType, UsesUnstableService},
    },
    event::{id::EventId, tagged_op::TaggedOp},
    state::{
        cache::IncrementalCache,
        stable_state::IsStableState,
        unstable_state::{CausalReplay, IsUnstableCore},
    },
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

use crate::list::eg_walker::{
    causal_cursor::transition,
    document::Document,
    item::{ItemId, LifeDot},
};

// Single-character, position-based, pure op-based CRDT operations
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum List<V> {
    /// Insert `content` at the visible position observed by the issuing replica.
    Insert { content: V, pos: usize },
    /// Delete the visible item at `pos` in the issuing replica's parent version.
    Delete { pos: usize },
    /// Delete `len` visible items starting at `start`.
    DeleteRange { start: usize, len: usize },
    /// Mark the item at `pos` as freshly alive without changing its payload.
    ///
    /// This operation models update-wins behavior for nested values: a concurrent
    /// delete can only remove life dots it observed, so an unseen update dot keeps
    /// the item visible.
    Update { pos: usize },
}

#[derive(Clone, Debug)]
struct DeleteEffect {
    item_id: ItemId,
    // A delete removes exactly the dots that were visible in its parent context.
    // Capturing them once avoids any reachability query during advance/retreat.
    removed_dots: Vec<LifeDot>,
}

impl<V> List<V>
where
    V: Clone + Debug,
{
    pub fn insert(content: V, pos: usize) -> Self {
        Self::Insert { content, pos }
    }

    pub fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    pub fn delete_range(start: usize, len: usize) -> Self {
        Self::DeleteRange { start, len }
    }

    pub fn update(pos: usize) -> Self {
        Self::Update { pos }
    }

    /// Move the prepare view backwards across `event_id`.
    ///
    /// During replay, consecutive events can have different parent versions. To
    /// interpret the next event, EgWalker first retreats prepare-state effects
    /// that are present in the current version but absent from the next event's
    /// parents. The effect view is intentionally untouched.
    fn retreat<U>(doc: &mut Document<V>, state: &U, event_id: &EventId)
    where
        U: IsUnstableCore<List<V>>,
    {
        match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => {
                let item_id = ItemId::event(event_id.clone());
                let life_dot = LifeDot::event(event_id.clone());
                if let Some(item_idx) = doc.position_of(&item_id) {
                    let item = doc.item_mut(item_idx).unwrap();
                    item.retreat_insert(&life_dot);
                }
            }
            List::Update { .. } => {
                let target = doc.update_targets.get(event_id).unwrap();
                if let Some(item_idx) = doc.position_of(target) {
                    let item = doc.item_mut(item_idx).unwrap();
                    item.retreat_update(&LifeDot::event(event_id.clone()));
                }
            }
            List::DeleteRange { .. } | List::Delete { .. } => {
                let targets = Arc::clone(doc.delete_targets.get(event_id).unwrap());

                for effect in targets.iter() {
                    if let Some(item_idx) = doc.position_of(&effect.item_id) {
                        let item = doc.item_mut(item_idx).unwrap();
                        item.retreat_delete(&effect.removed_dots);
                    }
                }
            }
        }
    }

    /// Move the prepare view forwards across `event_id`.
    ///
    /// This is the inverse of `retreat`: it reapplies prepare-state effects that
    /// are needed to reach the next event's parent version. The effect view remains
    /// the final accumulated replay result.
    fn advance<U>(doc: &mut Document<V>, state: &U, event_id: &EventId)
    where
        U: IsUnstableCore<List<V>>,
    {
        match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => {
                let item_id = ItemId::event(event_id.clone());
                let life_dot = LifeDot::event(event_id.clone());
                if let Some(item_idx) = doc.position_of(&item_id) {
                    let item = doc.item_mut(item_idx).unwrap();
                    item.advance_insert(life_dot);
                }
            }
            List::Update { .. } => {
                let target = doc.update_targets.get(event_id).unwrap();
                if let Some(item_idx) = doc.position_of(target) {
                    let item = doc.item_mut(item_idx).unwrap();
                    item.advance_update(LifeDot::event(event_id.clone()));
                }
            }
            List::DeleteRange { .. } | List::Delete { .. } => {
                let targets = Arc::clone(doc.delete_targets.get(event_id).unwrap());

                for effect in targets.iter() {
                    if let Some(item_idx) = doc.position_of(&effect.item_id) {
                        let item = doc.item_mut(item_idx).unwrap();
                        item.advance_delete(&effect.removed_dots);
                    }
                }
            }
        }
    }

    fn delete_visible_item(doc: &mut Document<V>, pos: usize) -> Option<DeleteEffect> {
        let idx = doc.visible_item_at(pos)?;
        let item = doc.item_mut(idx).unwrap();
        let removed_dots = item.apply_delete();
        Some(DeleteEffect {
            item_id: item.id.clone(),
            removed_dots,
        })
    }

    /// Apply one event after the prepare view has been moved to its parent version.
    ///
    /// Positional indices are interpreted against `prepare`. The operation also
    /// updates `effect`, which is the state eventually materialized by reads.
    fn apply(doc: &mut Document<V>, tagged_op: &TaggedOp<List<V>>) {
        match tagged_op.op() {
            List::Delete { pos } => {
                let Some(effect) = Self::delete_visible_item(doc, *pos) else {
                    debug_assert!(false, "No visible item found at position {pos}");
                    return;
                };
                doc.delete_targets
                    .insert(tagged_op.id().clone(), Arc::from([effect]));
            }
            List::DeleteRange { start, len } => {
                let mut effects = Vec::new();

                for _ in 0..*len {
                    let Some(effect) = Self::delete_visible_item(doc, *start) else {
                        return;
                    };
                    effects.push(effect);
                }

                doc.delete_targets
                    .insert(tagged_op.id().clone(), effects.into());
            }
            List::Update { pos } => {
                let Some(idx) = doc.visible_item_at(*pos) else {
                    debug_assert!(false, "No visible item found at position {pos}");
                    return;
                };

                let item_id = {
                    let item = doc.item_mut(idx).unwrap();
                    // Updating an existing element adds a fresh life dot for the same identity.
                    // If concurrent with a delete, that dot is not part of the delete effect.
                    let update_dot = LifeDot::event(tagged_op.id().clone());
                    item.apply_update(update_dot);
                    item.id.clone()
                };
                doc.update_targets.insert(tagged_op.id().clone(), item_id);
            }
            List::Insert { content, pos } => {
                doc.insert_event(tagged_op.id().clone(), content.clone(), *pos);
            }
        }
    }

    fn replay_document<'a, U, I>(stable: &'a [V], unstable: &'a U, events: I) -> Document<'a, V>
    where
        U: CausalReplay<List<V>> + 'a,
        I: IntoIterator<Item = &'a TaggedOp<List<V>>>,
        V: 'a,
    {
        let mut document = Document::new(stable);

        for tagged_op in events {
            let parents = unstable.direct_predecessors(tagged_op.id());
            let transition = transition(unstable, document.prepared_head.as_ref(), &parents);

            for event_id in transition.retreat {
                Self::retreat(&mut document, unstable, &event_id);
            }

            for event_id in transition.advance {
                Self::advance(&mut document, unstable, &event_id);
            }

            Self::apply(&mut document, tagged_op);
            document.prepared_head = Some(tagged_op.id().clone());
        }

        document
    }

    /// Replay an event stream and materialize its final effect view.
    fn replay<'a, U, I>(stable: &'a [V], unstable: &'a U, events: I) -> Vec<V>
    where
        U: CausalReplay<List<V>> + 'a,
        I: IntoIterator<Item = &'a TaggedOp<List<V>>>,
        V: 'a,
    {
        Self::replay_document(stable, unstable, events).materialize()
    }

    fn visible_len<U>(stable: &[V], unstable: &U) -> usize
    where
        U: CausalReplay<List<V>>,
    {
        Self::replay_document(stable, unstable, unstable.iter()).visible_len()
    }
}

#[derive(Clone, Debug)]
pub enum ListRejection {
    /// The operation refers to a visible position outside the current read state.
    OutOfBounds { pos: usize, len: usize },
}

impl Display for ListRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListRejection::OutOfBounds { pos, len } => {
                write!(
                    f,
                    "Position {pos} is out of bounds for document of length {len}"
                )
            }
        }
    }
}

impl<V> ReplicatedDataType for List<V>
where
    V: Debug + Clone,
{
    type StableState = Vec<V>;
    type Rejection = ListRejection;

    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_STABILIZE: bool = true;
}

impl<V, U> UsesUnstableService<U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    /// Validate positional operations against the current visible document.
    ///
    /// EgWalker operations store user-facing positions, so enablement is checked by
    /// reading the current state and comparing the requested position with its
    /// length.
    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Result<(), Self::Rejection> {
        let state_len = Self::visible_len(stable, unstable);
        match op {
            List::Insert { pos, .. } => {
                (*pos <= state_len)
                    .then_some(())
                    .ok_or(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: state_len,
                    })
            }
            List::Update { pos } | List::Delete { pos } => {
                (*pos < state_len)
                    .then_some(())
                    .ok_or(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: state_len,
                    })
            }
            List::DeleteRange { start, len } => start
                .checked_add(*len)
                .filter(|end| *end <= state_len)
                .map(|_| ())
                .ok_or(ListRejection::OutOfBounds {
                    pos: start.saturating_add(*len),
                    len: state_len,
                }),
        }
    }

    fn causal_reset(
        version: &Version,
        conservative: bool,
        stable: &Self::StableState,
        unstable: &U,
    ) -> CausalReset<Self> {
        if !conservative {
            return CausalReset::Prune;
        }
        let predecessors = unstable.predecessors(version);
        let len = Self::replay_document(stable, unstable, predecessors).visible_len();
        CausalReset::Inject(vec![List::DeleteRange { start: 0, len }])
    }
}

impl<V, U> Eval<Read<Vec<V>>, U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn execute_query(_q: &Read<Vec<V>>, stable: &Self::StableState, unstable: &U) -> Vec<V> {
        Self::replay(stable, unstable, unstable.iter())
    }
}

/// Convenience read for character lists.
impl<U> Eval<Read<String>, U> for List<char>
where
    U: CausalReplay<Self>,
{
    fn execute_query(_q: &Read<String>, stable: &Self::StableState, unstable: &U) -> String {
        let chars: Vec<char> = Self::execute_query(&Read::new(), stable, unstable);
        chars.into_iter().collect()
    }
}

impl<V> IncrementalCache<List<V>> for Vec<V>
where
    V: Clone,
{
    fn apply(&mut self, op: &List<V>) {
        match op {
            List::Insert { content, pos } => {
                self.insert(*pos, content.clone());
            }
            List::Delete { pos } => {
                self.remove(*pos);
            }
            List::DeleteRange { start, len } => {
                self.drain(*start..*start + *len);
            }
            List::Update { .. } => {}
        }
    }
}

/// Read the list at a historical version.
///
/// The unstable log supplies the predecessor events for the requested version,
/// and the same replay algorithm is used on that restricted stream.
pub struct ReadAt<V> {
    version: Version,
    _marker: std::marker::PhantomData<V>,
}

impl<V> ReadAt<V> {
    pub fn new(version: &Version) -> Self {
        Self {
            version: version.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V> QueryOperation for ReadAt<V> {
    type Response = V;
}

impl<V, U> Eval<ReadAt<Vec<V>>, U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn execute_query(q: &ReadAt<Vec<V>>, stable: &Self::StableState, unstable: &U) -> Vec<V> {
        let predecessors = unstable.predecessors(&q.version);
        Self::replay(stable, unstable, predecessors)
    }
}

impl<V> IsStableState<List<V>> for Vec<V>
where
    V: Debug + Clone,
{
    /// The stable state is the already-materialized snapshot used as replay baseline.
    fn is_default(&self) -> bool {
        self.is_empty()
    }

    /// Apply an operation to stable state using sequential list semantics.
    ///
    /// When a stable snapshot is built or supplied, it stores the plain list value.
    /// Unstable replay then starts from this state and interprets only the remaining
    /// events.
    fn apply(&mut self, value: List<V>) {
        match value {
            List::Insert { content, pos } => self.insert(pos, content),
            List::Delete { pos } => {
                self.remove(pos);
            }
            List::DeleteRange { start, len } => {
                self.drain(start..start + len);
            }
            List::Update { .. } => {}
        }
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }

    /// Redundant-operation pruning for EgWalker stable state is still pending.
    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<List<V>>,
        _tagged_op: &TaggedOp<List<V>>,
    ) {
    }
}

#[cfg(feature = "fuzz")]
impl<V> CausalOpGenerator for List<V>
where
    V: ValueGenerator + Debug + Clone,
{
    type Config = ();

    fn generate_causal(
        rng: &mut impl Rng,
        _config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl CausalReplay<Self>,
    ) -> Self {
        // Fuzzing generates only user operations that are enabled in the current
        // visible document, because out-of-bounds operations are rejected before
        // they enter the log.
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Insert,
            Delete,
            DeleteRange,
        }

        let list = Self::execute_query(&Read::new(), stable, unstable);

        let choice = if list.is_empty() {
            &Choice::Insert
        } else if list.len() < 3 {
            let dist = WeightedIndex::new([3, 2]).unwrap();
            &[Choice::Insert, Choice::Delete][dist.sample(rng)]
        } else {
            let dist = WeightedIndex::new([7, 2, 1]).unwrap();
            &[Choice::Insert, Choice::Delete, Choice::DeleteRange][dist.sample(rng)]
        };

        match choice {
            Choice::Insert => {
                use moirai_fuzz::value_generator::ValueGenerator;

                let pos = rng.random_range(0..=list.len());
                let c = V::generate(rng, &<V as ValueGenerator>::Config::default());
                List::insert(c, pos)
            }
            Choice::Delete => {
                let pos = rng.random_range(0..list.len());
                List::delete(pos)
            }
            Choice::DeleteRange => {
                let start = rng.random_range(0..list.len());
                let max_len = list.len() - start;
                let len = if max_len == 0 {
                    0
                } else {
                    rng.random_range(1..=max_len)
                };
                List::delete_range(start, len)
            }
        }
    }
}

#[cfg(test)]
mod tests;
