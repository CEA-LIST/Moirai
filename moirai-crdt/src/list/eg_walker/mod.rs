//! EgWalker list CRDT replay.
//!
//! Operations are generated against positional list states, but concurrent events
//! may observe different positions. EgWalker resolves this by replaying the event
//! graph in topological order while maintaining two views of the same internal
//! item sequence:
//!
//! - the prepare view, which is moved to each event's parent version before the
//!   event is interpreted;
//! - the effect view, which accumulates the transformed operations that determine
//!   the value returned by reads.
//!
//! The current implementation also accepts a stable list snapshot. Stable content
//! is represented as compressed placeholder ranges and only materialized as items
//! when an unstable operation targets a stable element directly.

mod document;
mod item;
mod presence_state;

use std::{
    collections::{BTreeSet, BinaryHeap},
    fmt::{Debug, Display},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::OpGenerator, value_generator::ValueGenerator};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::Eval,
        pure_crdt::{CausalReset, PureCRDT},
        query::{QueryOperation, Read},
        redundancy::RedundancyRelation,
    },
    event::{id::EventId, tagged_op::TaggedOp},
    state::{
        stable_state::IsStableState,
        unstable_state::{CausalReplay, IsUnstableCore},
    },
    utils::intern_str::{InternalizeOp, Interner},
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

use crate::{
    HashMap,
    list::eg_walker::{
        document::{Document, Record},
        item::{Item, ItemId, LifeDot},
    },
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

impl<V> InternalizeOp for List<V> {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[derive(Clone, Debug)]
struct DeleteEffect {
    item_id: ItemId,
    // A delete removes exactly the dots that were visible in its parent context.
    // Capturing them once avoids any reachability query during advance/retreat.
    removed_dots: BTreeSet<LifeDot>,
}

#[derive(Clone, Debug)]
enum DeleteTarget {
    /// A single-position delete and the dots it removed.
    Single(DeleteEffect),
    /// A range delete represented as one delete effect per touched item.
    Range(Vec<DeleteEffect>),
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

    /// Find where to insert a new item in the current prepared document.
    ///
    /// `target_pos` is expressed in visible prepare-state positions. Stable ranges
    /// count as visible content, so inserting inside one only splits the range at
    /// the insertion boundary.
    /// Stable ranges are split at the insertion boundary, but their elements remain placeholders.
    fn find_insert_position(doc: &mut Document<V>, target_pos: usize) -> usize {
        let mut cur_pos = 0usize;
        let mut idx = 0usize;

        while idx < doc.records.len() {
            if cur_pos == target_pos {
                return idx;
            }

            match &doc.records[idx] {
                Record::StableRange { start, end } => {
                    let len = end - start;
                    if cur_pos + len >= target_pos {
                        return doc.split_stable_range_at_boundary(idx, target_pos - cur_pos);
                    }
                    cur_pos += len;
                }
                Record::Item(item) if item.prepare.is_visible() => {
                    cur_pos += 1;
                }
                Record::Item(_) => {}
            }
            idx += 1;
        }

        idx
    }

    /// Find the concrete record for the visible item at `target_pos`.
    ///
    /// Deletes and updates need item-level dot state. If the target lies inside a
    /// stable range, this materializes exactly that stable element and leaves the
    /// rest of the range compressed.
    fn find_visible_item(doc: &mut Document<V>, target_pos: usize) -> Option<usize> {
        let mut cur_pos = 0usize;
        let mut idx = 0usize;

        while idx < doc.records.len() {
            match &doc.records[idx] {
                Record::StableRange { start, end } => {
                    let len = end - start;
                    if target_pos < cur_pos + len {
                        return Some(doc.isolate_stable_item(idx, target_pos - cur_pos));
                    }
                    cur_pos += len;
                }
                Record::Item(item) if item.prepare.is_visible() => {
                    if cur_pos == target_pos {
                        return Some(idx);
                    }
                    cur_pos += 1;
                }
                Record::Item(_) => {}
            }
            idx += 1;
        }

        None
    }

    /// Nearest integrated item before `idx`, used as the left insertion origin.
    fn previous_integrated_id(doc: &Document<V>, idx: usize) -> Option<ItemId> {
        doc.records[..idx]
            .iter()
            .rev()
            .find(|record| record.is_integrated())
            .and_then(Record::last_id)
    }

    /// Nearest integrated item at or after `idx`, used as the right insertion origin.
    fn next_integrated_id(doc: &Document<V>, idx: usize) -> Option<ItemId> {
        doc.records[idx..]
            .iter()
            .find(|record| record.is_integrated())
            .and_then(Record::first_id)
    }

    /// Insert a new item into the CRDT sequence.
    ///
    /// The visible position gives an initial location, but concurrent insertions
    /// at the same position must be ordered deterministically. The origin-left and
    /// origin-right anchors restrict the scan to the insertion window; the local
    /// item id tie-breaker is the current deterministic ordering rule.
    fn integrate(doc: &mut Document<V>, new_item: Item<V>, mut idx: usize) {
        let mut scan_idx = idx;

        // If origin_left is None, we'll pretend there's an item at position -1 which we were inserted to the right of.
        let left = new_item
            .origin_left
            .as_ref()
            .and_then(|id| doc.position_of(id))
            .map(|idx| idx as isize)
            .unwrap_or(-1);
        let right = if let Some(e) = &new_item.origin_right {
            doc.position_of(e).expect("Could not find item by id")
        } else {
            doc.records.len()
        };

        let mut scanning = false;

        while scan_idx < right {
            let other = match &doc.records[scan_idx] {
                Record::Item(item) => item,
                Record::StableRange { .. } => break,
            };

            // Only not-yet-integrated items participate in the Eg-Walker insertion walk.
            if other.prepare.is_integrated() {
                break;
            }

            let oleft = if let Some(ol) = &other.origin_left {
                doc.position_of(ol).expect("Could not find item by id") as isize
            } else {
                -1
            };

            let oright = if let Some(or) = &other.origin_right {
                doc.position_of(or).expect("Could not find item by id")
            } else {
                doc.records.len()
            };

            // TODO: use Fair Tag
            if oleft < left || (oleft == left && oright == right && new_item.id < other.id) {
                break;
            }
            if oleft == left {
                scanning = oright < right;
            }
            scan_idx += 1;

            if !scanning {
                idx = scan_idx;
            }
        }

        doc.records.insert(idx, Record::Item(new_item));
        doc.rebuild_index();
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
                    item.prepare.remove_life_dot(&life_dot);
                    item.prepare.inserted = false;
                }
            }
            List::Update { .. } => {
                let target = doc.update_targets.get(event_id).unwrap();
                if let Some(item_idx) = doc.position_of(target) {
                    let item = doc.item_mut(item_idx).unwrap();
                    item.prepare
                        .remove_life_dot(&LifeDot::event(event_id.clone()));
                }
            }
            List::DeleteRange { .. } | List::Delete { .. } => {
                let targets: Vec<DeleteEffect> = doc
                    .delete_targets
                    .get(event_id)
                    .map(|t| match t {
                        DeleteTarget::Single(effect) => vec![effect.clone()],
                        DeleteTarget::Range(effects) => effects.clone(),
                    })
                    .unwrap();

                for effect in &targets {
                    if let Some(item_idx) = doc.position_of(&effect.item_id) {
                        let item = doc.item_mut(item_idx).unwrap();
                        for dot in &effect.removed_dots {
                            item.prepare.undo_delete(dot);
                        }
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
                    item.prepare.inserted = true;
                    item.prepare.add_life_dot(life_dot);
                }
            }
            List::Update { .. } => {
                let target = doc.update_targets.get(event_id).unwrap();
                if let Some(item_idx) = doc.position_of(target) {
                    let item = doc.item_mut(item_idx).unwrap();
                    item.prepare.add_life_dot(LifeDot::event(event_id.clone()));
                }
            }
            List::DeleteRange { .. } | List::Delete { .. } => {
                let targets: Vec<DeleteEffect> = doc
                    .delete_targets
                    .get(event_id)
                    .map(|t| match t {
                        DeleteTarget::Single(effect) => vec![effect.clone()],
                        DeleteTarget::Range(effects) => effects.clone(),
                    })
                    .unwrap();

                for effect in &targets {
                    if let Some(item_idx) = doc.position_of(&effect.item_id) {
                        let item = doc.item_mut(item_idx).unwrap();
                        for dot in &effect.removed_dots {
                            item.prepare.record_delete(dot);
                        }
                    }
                }
            }
        }
    }

    /// Apply one event after the prepare view has been moved to its parent version.
    ///
    /// Positional indices are interpreted against `prepare`. The operation also
    /// updates `effect`, which is the state eventually materialized by reads.
    fn apply(doc: &mut Document<V>, tagged_op: &TaggedOp<List<V>>) {
        match tagged_op.op() {
            List::Delete { pos } => {
                let Some(idx) = Self::find_visible_item(doc, *pos) else {
                    debug_assert!(false, "No visible item found at position {pos}");
                    return;
                };

                let (item_id, removed_dots) = {
                    let item = doc.item_mut(idx).unwrap();
                    // The delete only removes dots that are visible in its prepared parent context.
                    #[allow(clippy::mutable_key_type)]
                    let removed_dots = item.prepare.visible_life_dots();
                    for dot in &removed_dots {
                        item.effect.remove_life_dot(dot);
                    }
                    for dot in &removed_dots {
                        item.prepare.record_delete(dot);
                    }
                    (item.id.clone(), removed_dots)
                };
                doc.delete_targets.insert(
                    tagged_op.id().clone(),
                    DeleteTarget::Single(DeleteEffect {
                        item_id,
                        removed_dots,
                    }),
                );
            }
            List::DeleteRange { start, len } => {
                let mut pos = 0usize;
                let mut effects = Vec::new();

                while pos < *len {
                    let Some(idx) = Self::find_visible_item(doc, *start) else {
                        return;
                    };

                    effects.push({
                        let item = doc.item_mut(idx).unwrap();
                        #[allow(clippy::mutable_key_type)]
                        let removed_dots = item.prepare.visible_life_dots();
                        for dot in &removed_dots {
                            item.effect.remove_life_dot(dot);
                        }
                        for dot in &removed_dots {
                            item.prepare.record_delete(dot);
                        }
                        DeleteEffect {
                            item_id: item.id.clone(),
                            removed_dots,
                        }
                    });
                    pos += 1;
                }

                doc.delete_targets
                    .insert(tagged_op.id().clone(), DeleteTarget::Range(effects));
            }
            List::Update { pos } => {
                let Some(idx) = Self::find_visible_item(doc, *pos) else {
                    debug_assert!(false, "No visible item found at position {pos}");
                    return;
                };

                let item_id = {
                    let item = doc.item_mut(idx).unwrap();
                    // Updating an existing element adds a fresh life dot for the same identity.
                    // If concurrent with a delete, that dot is not part of the delete effect.
                    let update_dot = LifeDot::event(tagged_op.id().clone());
                    item.effect.add_life_dot(update_dot.clone());
                    item.prepare.add_life_dot(update_dot);
                    item.id.clone()
                };
                doc.update_targets.insert(tagged_op.id().clone(), item_id);
            }
            List::Insert { content, pos } => {
                let idx = Self::find_insert_position(doc, *pos);

                debug_assert!(
                    idx == 0 || doc.records[idx - 1].is_integrated(),
                    "Item to the left is not integrated"
                );

                let origin_left = Self::previous_integrated_id(doc, idx);
                let origin_right = Self::next_integrated_id(doc, idx);

                let item = Item::new_event(
                    tagged_op.id().clone(),
                    origin_left,
                    origin_right,
                    content.clone(),
                );
                Self::integrate(doc, item, idx)
            }
        }
    }

    /// Compute how to move the prepare view from `current_version` to `parents`.
    ///
    /// The returned `a_only` events must be retreated, and `b_only` events must be
    /// advanced. The search walks ancestors from both frontiers until all remaining
    /// queued events are shared ancestors.
    fn diff<U>(
        state: &U,
        current_version: &Option<EventId>,
        parents: &[EventId],
    ) -> (Vec<EventId>, Vec<EventId>)
    where
        U: CausalReplay<List<V>>,
    {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum DiffFlag {
            A,
            B,
            Shared,
        }

        #[allow(clippy::mutable_key_type)]
        let mut flags: HashMap<EventId, DiffFlag> = HashMap::default();
        // Process newer events first so frontiers converge toward their nearest
        // common ancestors.
        let mut queue: BinaryHeap<(usize, EventId)> = BinaryHeap::new();
        let mut num_shared = 0usize;

        #[allow(clippy::mutable_key_type)]
        fn enq(
            flags: &mut HashMap<EventId, DiffFlag>,
            queue: &mut BinaryHeap<(usize, EventId)>,
            num_shared: &mut usize,
            event_id: EventId,
            event_idx: usize,
            flag: DiffFlag,
        ) {
            let prev = flags.get(&event_id).copied();
            match prev {
                None => {
                    queue.push((event_idx, event_id.clone()));
                    if flag == DiffFlag::Shared {
                        *num_shared += 1;
                    }
                    flags.insert(event_id, flag);
                }
                Some(old_flag) => {
                    if flag != old_flag && old_flag != DiffFlag::Shared {
                        flags.insert(event_id, DiffFlag::Shared);
                        *num_shared += 1;
                    }
                }
            }
        }

        if let Some(id) = current_version {
            let event_idx = state.delivery_order(id).unwrap();
            enq(
                &mut flags,
                &mut queue,
                &mut num_shared,
                id.clone(),
                event_idx,
                DiffFlag::A,
            );
        }
        for p in parents.iter() {
            let event_idx = state.delivery_order(p).unwrap();
            enq(
                &mut flags,
                &mut queue,
                &mut num_shared,
                p.clone(),
                event_idx,
                DiffFlag::B,
            );
        }

        let mut a_only = Vec::new();
        let mut b_only = Vec::new();

        while queue.len() > num_shared {
            let (_, id) = queue.pop().unwrap();
            let flag = flags.get(&id).copied().unwrap();
            match flag {
                DiffFlag::Shared => {
                    num_shared -= 1;
                }
                DiffFlag::A => a_only.push(id.clone()),
                DiffFlag::B => b_only.push(id.clone()),
            }

            for parent in state.parents(&id).iter() {
                let event_idx = state.delivery_order(parent).unwrap();
                enq(
                    &mut flags,
                    &mut queue,
                    &mut num_shared,
                    parent.clone(),
                    event_idx,
                    flag,
                );
            }
        }

        (a_only, b_only)
    }

    /// Replay a topologically ordered event stream over an optional stable baseline.
    ///
    /// `events` can be the whole unstable log for `Read`, or a predecessor stream
    /// for `ReadAt`. The document keeps its prepare view at the parent version of
    /// each event, applies the event, and finally materializes the effect view.
    fn replay<'a, U, I>(stable: &'a [V], unstable: &'a U, events: I) -> Vec<V>
    where
        U: CausalReplay<List<V>> + 'a,
        I: IntoIterator<Item = &'a TaggedOp<List<V>>>,
        V: 'a,
    {
        let mut document = Document::new(stable);

        for tagged_op in events {
            let parents = unstable.parents(tagged_op.id());
            let (a_only, b_only) = Self::diff(unstable, &document.current_version, &parents);

            for event_id in a_only {
                Self::retreat(&mut document, unstable, &event_id);
            }

            for event_id in b_only {
                Self::advance(&mut document, unstable, &event_id);
            }

            Self::apply(&mut document, tagged_op);
            document.current_version = Some(tagged_op.id().clone());
        }

        document.materialize()
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

impl<V> PureCRDT for List<V>
where
    V: Debug + Clone,
{
    type Value = Vec<V>;
    type StableState = Vec<V>;
    type Rejection = ListRejection;

    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_STABILIZE: bool = true;

    /// Validate positional operations against the current visible document.
    ///
    /// EgWalker operations store user-facing positions, so enablement is checked by
    /// reading the current state and comparing the requested position with its
    /// length.
    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &impl CausalReplay<Self>,
    ) -> Result<(), Self::Rejection> {
        let state = Self::execute_query(Read::new(), stable, unstable);
        match op {
            List::Insert { pos, .. } => {
                (*pos <= state.len())
                    .then_some(())
                    .ok_or(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: state.len(),
                    })
            }
            List::Update { pos } | List::Delete { pos } => (*pos < state.len())
                .then_some(())
                .ok_or(ListRejection::OutOfBounds {
                    pos: *pos,
                    len: state.len(),
                }),
            List::DeleteRange { start, len } => ((*start + *len) <= state.len())
                .then_some(())
                .ok_or(ListRejection::OutOfBounds {
                    pos: *start + *len,
                    len: state.len(),
                }),
        }
    }

    fn causal_reset(
        version: &Version,
        conservative: bool,
        stable: &Self::StableState,
        unstable: &impl CausalReplay<Self>,
    ) -> CausalReset<Self> {
        if !conservative {
            return CausalReset::Prune;
        }
        let state = Self::execute_query(ReadAt::new(version), stable, unstable);
        CausalReset::Inject(vec![List::DeleteRange {
            start: 0,
            len: state.len(),
        }])
    }
}

/// Normal read: replay all unstable events on top of the stable list snapshot.
impl<V, U> Eval<Read<<Self as PureCRDT>::Value>, U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Vec<V> {
        Self::replay(stable, unstable, unstable.iter())
    }
}

/// Convenience read for character lists.
impl<U> Eval<Read<String>, U> for List<char>
where
    U: CausalReplay<Self>,
{
    fn execute_query(_q: Read<String>, stable: &Self::StableState, unstable: &U) -> String {
        let chars: Vec<char> = Self::execute_query(Read::new(), stable, unstable);
        chars.into_iter().collect()
    }
}

/// Read the list at a historical version.
///
/// The unstable log supplies the predecessor events for the requested version,
/// and the same replay algorithm is used on that restricted stream.
pub struct ReadAt<'a, V> {
    version: &'a Version,
    _marker: std::marker::PhantomData<V>,
}

impl<'a, V> ReadAt<'a, V> {
    pub fn new(version: &'a Version) -> Self {
        Self {
            version,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, V> QueryOperation for ReadAt<'a, V> {
    type Response = V;
}

impl<'a, V, U> Eval<ReadAt<'a, <Self as PureCRDT>::Value>, U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn execute_query(
        q: ReadAt<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Vec<V> {
        let predecessors = unstable.predecessors(q.version);
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
        todo!()
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGenerator for List<V>
where
    V: ValueGenerator + Debug + Clone,
{
    type Config = ();

    fn generate(
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

        let list = Self::execute_query(Read::new(), stable, unstable);

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
mod tests {
    use moirai_protocol::{
        broadcast::tcsb::Tcsb,
        replica::{IsReplica, Replica},
        state::graph_log::GraphLog,
    };

    use super::*;
    use crate::utils::membership::{triplet_log, twins_log};

    type ListReplica = Replica<GraphLog<List<char>>, Tcsb<List<char>>>;

    fn stable_twins(stable: Vec<char>) -> (ListReplica, ListReplica) {
        let replica_a = Replica::bootstrap_with_state(
            "a".to_string(),
            &["a", "b"],
            GraphLog::<List<char>>::from_stable(stable.clone()),
        );
        let replica_b = Replica::bootstrap_with_state(
            "b".to_string(),
            &["a", "b"],
            GraphLog::<List<char>>::from_stable(stable),
        );
        (replica_a, replica_b)
    }

    #[test]
    fn simple_insertion_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        assert_eq!(&replica_a.query(Read::<String>::new()), "A");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn starts_from_stable_document() {
        let (replica_a, replica_b) = stable_twins(vec!['a', 'b', 'c']);

        assert_eq!(replica_a.query(Read::<String>::new()), "abc");
        assert_eq!(replica_b.query(Read::<String>::new()), "abc");
    }

    #[test]
    fn inserts_into_stable_document() {
        let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

        let event = replica_a.send(List::insert('X', 1)).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::<String>::new()), "aXbc");
        assert_eq!(replica_b.query(Read::<String>::new()), "aXbc");
    }

    #[test]
    fn deletes_from_stable_document() {
        let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

        let event = replica_a.send(List::delete(1)).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::<String>::new()), "ac");
        assert_eq!(replica_b.query(Read::<String>::new()), "ac");
    }

    #[test]
    fn delete_range_from_stable_document() {
        let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c', 'd']);

        let event = replica_a.send(List::delete_range(1, 2)).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::<String>::new()), "ad");
        assert_eq!(replica_b.query(Read::<String>::new()), "ad");
    }

    #[test]
    fn read_at_uses_stable_document() {
        let (mut replica_a, _) = stable_twins(vec!['a', 'b', 'c']);

        let insert = replica_a.send(List::insert('X', 1)).unwrap();
        let insert_version = insert.event().version().clone();
        replica_a.send(List::delete(1)).unwrap();

        assert_eq!(
            replica_a.query(ReadAt::<Vec<char>>::new(&insert_version)),
            vec!['a', 'X', 'b', 'c']
        );
        assert_eq!(replica_a.query(Read::<String>::new()), "abc");
    }

    #[test]
    fn concurrent_insertions_into_stable_document_converge() {
        let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

        let event_a = replica_a.send(List::insert('X', 1)).unwrap();
        let event_b = replica_b.send(List::insert('Y', 1)).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let a = replica_a.query(Read::<String>::new());
        let b = replica_b.query(Read::<String>::new());
        assert_eq!(a, b);
        assert!(a == "aXYbc" || a == "aYXbc", "unexpected result: {a}");
    }

    #[test]
    fn stable_update_wins_over_concurrent_delete() {
        let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

        let event_a = replica_a.send(List::delete(1)).unwrap();
        let event_b = replica_b.send(List::update(1)).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::<String>::new()), "abc");
        assert_eq!(replica_b.query(Read::<String>::new()), "abc");
    }

    #[test]
    fn concurrent_insertions_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('H', 0)).unwrap();
        replica_b.receive(e1);
        assert_eq!(&replica_a.query(Read::<String>::new()), "H");

        let e2a = replica_a.send(List::insert('e', 1)).unwrap();
        let e2b = replica_b.send(List::insert('i', 1)).unwrap();
        replica_b.receive(e2a);
        replica_a.receive(e2b);

        let res_b = replica_b.query(Read::<String>::new());
        assert!(
            res_b == "Hei" || res_b == "Hie",
            "Unexpected order: {}",
            res_b
        );
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('H', 0)).unwrap();
        let e2 = replica_b.send(List::insert('i', 0)).unwrap();
        replica_a.receive(e2);
        replica_b.receive(e1);

        let res_a = replica_a.query(Read::<String>::new());
        assert!(
            res_a == "Hi" || res_a == "iH",
            "Unexpected order: {}",
            res_a
        );
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn delete_operation_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        let e2 = replica_a.send(List::delete(0)).unwrap();
        replica_b.receive(e2);

        assert_eq!(&replica_a.query(Read::<String>::new()), "");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn conc_delete_ins_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        let edel = replica_a.send(List::delete(0)).unwrap();
        let eins = replica_b.send(List::insert('B', 1)).unwrap(); // Insert to the right of 'A' in B's view
        replica_a.receive(eins);
        replica_b.receive(edel);

        assert_eq!(&replica_a.query(Read::<String>::new()), "B");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn sequential_conc_operations_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('H', 0)).unwrap();
        replica_b.receive(e1);
        assert_eq!(&replica_a.query(Read::<String>::new()), "H");

        let e2a = replica_a.send(List::insert('e', 1)).unwrap();
        let e2b = replica_b.send(List::insert('i', 1)).unwrap();
        replica_b.receive(e2a);
        replica_a.receive(e2b);
        assert!(
            replica_b.query(Read::<String>::new()) == "Hei"
                || replica_b.query(Read::<String>::new()) == "Hie"
        );

        // Insert a space between e and i from A's perspective (which will be position 2 if e<i)
        let e3 = replica_a.send(List::insert(' ', 2)).unwrap();
        replica_b.receive(e3);
        let res = replica_a.query(Read::<String>::new());
        // Depending on tie-breaker, expected is either "He i" or "Hi e". We accept either space between letters.
        assert!(res == "He i" || res == "Hi e", "Unexpected result: {}", res);
    }

    #[test]
    fn in_paper() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        // e1: Insert(0, 'h')
        let e1 = replica_a.send(List::insert('h', 0)).unwrap();
        replica_b.receive(e1.clone());

        // e2: Insert(1, 'i')
        let e2 = replica_a.send(List::insert('i', 1)).unwrap();
        replica_b.receive(e2.clone());

        // Branch: Replica A will capitalize 'H', Replica B will change to 'hey'
        // e3: Insert(0, 'H') depends on e1,e2
        let e3 = replica_a.send(List::insert('H', 0)).unwrap();

        // e4: Delete(1) (remove lowercase 'h') depends on e3
        let e4 = replica_a.send(List::delete(1)).unwrap();

        let e4_version = e4.event().version();
        assert_eq!(&replica_a.query(Read::<String>::new()), "Hi");

        // e5: Delete(1) (remove 'i') on other branch
        let e5 = replica_b.send(List::delete(1)).unwrap();

        // e6: Insert(1, 'e')
        let e6 = replica_b.send(List::insert('e', 1)).unwrap();

        // e7: Insert(2, 'y')
        let e7 = replica_b.send(List::insert('y', 2)).unwrap();

        replica_b.receive(e3.clone());
        replica_b.receive(e4.clone());
        replica_a.receive(e5.clone());
        replica_a.receive(e6.clone());
        replica_a.receive(e7.clone());
        // Merge both replicas so they see all events before e8
        // At this point both should be "Hey"

        // e8: Insert(3, '!')
        let e8 = replica_b.send(List::insert('!', 3)).unwrap();
        replica_a.receive(e8.clone());

        // Final result should be "Hey!"
        assert_eq!(replica_a.query(ReadAt::new(e4_version)), vec!['H', 'i']);
        assert_eq!(&replica_a.query(Read::<String>::new()), "Hey!");
        assert_eq!(
            &replica_a.query(Read::<String>::new()),
            &replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn delete_range_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        let e2 = replica_a.send(List::insert('B', 1)).unwrap();
        let e3 = replica_a.send(List::insert('C', 2)).unwrap();
        replica_b.receive(e1);
        replica_b.receive(e2);
        replica_b.receive(e3);
        assert_eq!(replica_a.query(Read::<String>::new()), "ABC");

        let e4 = replica_a.send(List::delete_range(0, 2)).unwrap();
        replica_b.receive(e4);
        assert_eq!(replica_a.query(Read::<String>::new()), "C");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn delete_range_egwalker_2() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let event_a = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::<String>::new()), "A");
        assert_eq!(replica_b.query(Read::<String>::new()), "A");

        let event_b = replica_b.send(List::insert('B', 0)).unwrap();
        assert_eq!(replica_b.query(Read::<String>::new()), "BA");
        let event_b_2 = replica_b.send(List::delete_range(0, 2)).unwrap();

        assert_eq!(replica_b.query(Read::<String>::new()), "");

        let event_a_2 = replica_a.send(List::delete_range(0, 1)).unwrap();

        assert_eq!(replica_a.query(Read::<String>::new()), "");

        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);
        assert_eq!(replica_a.query(Read::<String>::new()), "");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn delete_range_egwalker_3() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<GraphLog<List<char>>>();

        let event_a = replica_a.send(List::insert('4', 0)).unwrap();
        replica_c.receive(event_a.clone());

        let event_c = replica_c.send(List::insert('U', 0)).unwrap();
        let event_c_1 = replica_c.send(List::delete_range(0, 2)).unwrap();

        replica_b.receive(event_c.clone());
        replica_b.receive(event_a.clone());
        let event_b = replica_b.send(List::insert('y', 1)).unwrap();

        replica_a.receive(event_c);
        replica_a.receive(event_b.clone());
        replica_c.receive(event_b);
        replica_b.receive(event_c_1.clone());
        replica_a.receive(event_c_1);

        assert_eq!(replica_a.query(Read::<String>::new()), "y");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_c.query(Read::<String>::new())
        );
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn update_delete() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        let e2 = replica_b.send(List::update(0)).unwrap();

        let e3 = replica_a.send(List::delete(0)).unwrap();
        replica_b.receive(e3);
        replica_a.receive(e2);

        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    /// digraph {
    ///     0 [ label="[Insert { content: 'a', pos: 0 }@(0:1)]"]
    ///     1 [ label="[Delete { pos: 0 }@(0:2)]"]
    ///     2 [ label="[Insert { content: '6', pos: 0 }@(1:1)]"]
    ///     3 [ label="[Delete { pos: 0 }@(0:3)]"]
    ///     0 -> 1 [ ]  1 -> 3 [ ]  2 -> 3 [ ]
    /// }
    #[test]
    fn regression_1() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();
        let a1 = replica_a.send(List::insert('a', 0)).unwrap();
        let a2 = replica_a.send(List::delete(0)).unwrap();
        let b1 = replica_b.send(List::insert('6', 0)).unwrap();
        replica_a.receive(b1);
        replica_b.receive(a1);
        replica_b.receive(a2);

        let a3 = replica_a.send(List::delete(0)).unwrap();
        replica_b.receive(a3);

        let state_a = replica_a.query(Read::<String>::new());
        let state_b = replica_b.query(Read::<String>::new());
        let result = String::new();

        assert_eq!(state_a, result);
        assert_eq!(state_b, result);
    }

    /// digraph {
    ///     0 [ label="[Insert { content: 'N', pos: 0 }@(1:1)]"]
    ///     1 [ label="[Insert { content: 'r', pos: 1 }@(1:2)]"]
    ///     2 [ label="[Delete { pos: 0 }@(0:1)]"]
    ///     3 [ label="[Delete { pos: 0 }@(1:3)]"]
    ///     4 [ label="[Insert { content: 'Y', pos: 0 }@(0:2)]"]
    ///     5 [ label="[Insert { content: 'x', pos: 1 }@(1:4)]"]
    ///     0 -> 1 [ ]  1 -> 2 [ ]  1 -> 3 [ ]  2 -> 4 [ ]  3 -> 4 [ ]  3 -> 5 [ ]
    /// }
    #[test]
    fn regression_2() {
        let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

        let b1 = replica_b.send(List::insert('N', 0)).unwrap();
        let b2 = replica_b.send(List::insert('r', 1)).unwrap();

        replica_a.receive(b1);
        replica_a.receive(b2);

        let a1 = replica_a.send(List::delete(0)).unwrap();
        let b3 = replica_b.send(List::delete(0)).unwrap();

        replica_a.receive(b3);
        let a2 = replica_a.send(List::insert('Y', 0)).unwrap();
        let b4 = replica_b.send(List::insert('x', 1)).unwrap();

        replica_a.receive(b4);
        replica_b.receive(a1);
        replica_b.receive(a2);

        let state_a = replica_a.query(Read::<String>::new());
        let state_b = replica_b.query(Read::<String>::new());
        let result = String::from("Yrx");

        assert_eq!(state_a, result);
        assert_eq!(state_b, result);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_list() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run = RunConfig::new(0.5, 8, 1_000, None, None, true, false);
        let runs = vec![run; 1];

        let config =
            FuzzerConfig::<GraphLog<List<char>>>::new("list", runs, true, |a, b| a == b, false);

        fuzzer::<GraphLog<List<char>>>(config);
    }
}
