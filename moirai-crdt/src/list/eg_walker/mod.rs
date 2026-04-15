mod document;
mod presence_state;

use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fmt::Debug,
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
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
    utils::intern_str::{InternalizeOp, Interner},
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

use crate::{
    HashMap,
    list::eg_walker::{document::Document, presence_state::PresenceState},
};

// Single-character, position-based, pure op-based CRDT operations
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum List<V> {
    Insert { content: V, pos: usize },
    Delete { pos: usize },
    DeleteRange { start: usize, len: usize },
    Update { pos: usize },
}

impl<V> InternalizeOp for List<V> {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[derive(Clone, Debug)]
struct Item<V> {
    pub id: EventId,
    /// Event id of the character the user saw when inserting this new op.
    /// The fields from the CRDT that determines insertion order.
    pub origin_left: Option<EventId>,
    pub origin_right: Option<EventId>,
    pub content: V,
    /// Active life dots in the final replayed version.
    /// This is the state that materializes the visible list returned to the user.
    pub effect_live_dots: BTreeSet<EventId>,
    /// State in the prepared parent context used while Eg-Walker jumps across the event graph.
    pub presence: PresenceState,
}

#[derive(Clone, Debug)]
struct DeleteEffect {
    item_id: EventId,
    // A delete removes exactly the dots that were visible in its parent context.
    // Capturing them once avoids any reachability query during advance/retreat.
    removed_dots: BTreeSet<EventId>,
}

#[derive(Clone, Debug)]
enum DeleteTarget {
    Single(DeleteEffect),
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

    /// Where to insert the new item so that it appears at target_pos in the current visible document
    fn find_by_current_pos(items: &[Item<V>], target_pos: usize) -> usize {
        let mut cur_pos = 0usize;
        let mut idx = 0usize;

        while cur_pos < target_pos {
            debug_assert!(
                idx < items.len(),
                "Target position {target_pos} is out of bounds for current document"
            );

            let item = &items[idx];
            if item.presence.is_visible() {
                cur_pos += 1;
            }
            idx += 1;
        }

        idx
    }

    fn integrate(doc: &mut Document<V>, new_item: Item<V>, mut idx: usize) {
        let mut scan_idx = idx;

        // If origin_left is None, we'll pretend there's an item at position -1 which we were inserted to the right of.
        let left = scan_idx as isize - 1;
        let right = if let Some(e) = &new_item.origin_right {
            *doc.items_by_idx
                .get(e)
                .expect("Could not find item by NodeIndex")
        } else {
            doc.items.len()
        };

        let mut scanning = false;

        while scan_idx < right {
            let other = &doc.items[scan_idx];

            // Only not-yet-integrated items participate in the Eg-Walker insertion walk.
            if other.presence.is_integrated() {
                break;
            }

            let oleft = if let Some(ol) = &other.origin_left {
                *doc.items_by_idx
                    .get(ol)
                    .expect("Could not find item by NodeIndex") as isize
            } else {
                -1
            };

            let oright = if let Some(or) = &other.origin_right {
                *doc.items_by_idx
                    .get(or)
                    .expect("Could not find item by NodeIndex")
            } else {
                doc.items.len()
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

        doc.items.insert(idx, new_item);

        // Update items_by_nx mapping for all shifted items at and after idx
        for i in idx..doc.items.len() {
            let n = doc.items[i].id.clone();
            doc.items_by_idx.insert(n, i);
        }
    }

    fn retreat(doc: &mut Document<V>, state: &impl IsUnstableState<List<V>>, event_id: &EventId) {
        match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => {
                if let Some(&item_idx) = doc.items_by_idx.get(event_id) {
                    let item = &mut doc.items[item_idx];
                    item.presence.born_dots.remove(event_id);
                    item.presence.inserted = false;
                }
            }
            List::Update { .. } => {
                let target = doc.update_targets.get(event_id).unwrap();
                if let Some(&item_idx) = doc.items_by_idx.get(target) {
                    let item = &mut doc.items[item_idx];
                    item.presence.born_dots.remove(event_id);
                }
            }
            List::DeleteRange { .. } | List::Delete { .. } => {
                let targets: Vec<&DeleteEffect> = doc
                    .delete_targets
                    .get(event_id)
                    .map(|t| match t {
                        DeleteTarget::Single(effect) => vec![effect],
                        DeleteTarget::Range(effects) => effects.iter().collect(),
                    })
                    .unwrap();

                for effect in targets {
                    if let Some(&item_idx) = doc.items_by_idx.get(&effect.item_id) {
                        let item = &mut doc.items[item_idx];
                        for dot in &effect.removed_dots {
                            item.presence.remove_deleted(dot);
                        }
                    }
                }
            }
        }
    }

    fn advance(doc: &mut Document<V>, state: &impl IsUnstableState<List<V>>, event_id: &EventId) {
        match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => {
                if let Some(&item_idx) = doc.items_by_idx.get(event_id) {
                    let item = &mut doc.items[item_idx];
                    item.presence.inserted = true;
                    item.presence.born_dots.insert(event_id.clone());
                }
            }
            List::Update { .. } => {
                let target = doc.update_targets.get(event_id).unwrap();
                if let Some(&item_idx) = doc.items_by_idx.get(target) {
                    let item = &mut doc.items[item_idx];
                    item.presence.born_dots.insert(event_id.clone());
                }
            }
            List::DeleteRange { .. } | List::Delete { .. } => {
                let targets: Vec<&DeleteEffect> = doc
                    .delete_targets
                    .get(event_id)
                    .map(|t| match t {
                        DeleteTarget::Single(effect) => vec![effect],
                        DeleteTarget::Range(effects) => effects.iter().collect(),
                    })
                    .unwrap();

                for effect in targets {
                    if let Some(&item_idx) = doc.items_by_idx.get(&effect.item_id) {
                        let item = &mut doc.items[item_idx];
                        for dot in &effect.removed_dots {
                            item.presence.add_deleted(dot);
                        }
                    }
                }
            }
        }
    }

    fn apply(doc: &mut Document<V>, tagged_op: &TaggedOp<List<V>>) {
        match tagged_op.op() {
            List::Delete { pos } => {
                let mut idx = Self::find_by_current_pos(&doc.items, *pos);

                while idx < doc.items.len() && !doc.items[idx].presence.is_visible() {
                    idx += 1;
                }

                debug_assert!(
                    idx < doc.items.len(),
                    "No visible item found at position {pos}"
                );

                let item = &mut doc.items[idx];
                // The delete only removes dots that are visible in its prepared parent context.
                #[allow(clippy::mutable_key_type)]
                let removed_dots = item.presence.visible_dots();
                for dot in &removed_dots {
                    item.effect_live_dots.remove(dot);
                }
                for dot in &removed_dots {
                    item.presence.add_deleted(dot);
                }
                doc.delete_targets.insert(
                    tagged_op.id().clone(),
                    DeleteTarget::Single(DeleteEffect {
                        item_id: item.id.clone(),
                        removed_dots,
                    }),
                );
            }
            List::DeleteRange { start, len } => {
                let mut idx = Self::find_by_current_pos(&doc.items, *start);
                let mut pos = 0usize;
                let mut effects = Vec::new();

                while pos < *len {
                    while idx < doc.items.len() && !doc.items[idx].presence.is_visible() {
                        idx += 1;
                    }

                    if idx >= doc.items.len() {
                        return;
                    }

                    let item = &mut doc.items[idx];
                    #[allow(clippy::mutable_key_type)]
                    let removed_dots = item.presence.visible_dots();
                    for dot in &removed_dots {
                        item.effect_live_dots.remove(dot);
                    }
                    for dot in &removed_dots {
                        item.presence.add_deleted(dot);
                    }
                    effects.push(DeleteEffect {
                        item_id: item.id.clone(),
                        removed_dots,
                    });
                    pos += 1;
                }

                doc.delete_targets
                    .insert(tagged_op.id().clone(), DeleteTarget::Range(effects));
            }
            List::Update { pos } => {
                let mut idx = Self::find_by_current_pos(&doc.items, *pos);

                while idx < doc.items.len() && !doc.items[idx].presence.is_visible() {
                    idx += 1;
                }

                debug_assert!(
                    idx < doc.items.len(),
                    "No visible item found at position {pos}"
                );

                let item = &mut doc.items[idx];
                // Updating an existing element adds a fresh life dot for the same identity.
                // If concurrent with a delete, that dot is not part of the delete effect.
                item.effect_live_dots.insert(tagged_op.id().clone());
                item.presence.born_dots.insert(tagged_op.id().clone());
                doc.update_targets
                    .insert(tagged_op.id().clone(), item.id.clone());
            }
            List::Insert { content, pos } => {
                let idx = Self::find_by_current_pos(&doc.items, *pos);

                // if idx >= 1 && !&doc.items[idx - 1].presence.is_integrated() {
                //     panic!("Item to the left is not inserted! What!"); // OLDCODE behavior retained
                // }

                let origin_left = if idx == 0 {
                    None
                } else {
                    Some(doc.items[idx - 1].id.clone())
                };

                let mut origin_right = None;
                for i in idx..doc.items.len() {
                    if doc.items[i].presence.is_integrated() {
                        origin_right = Some(doc.items[i].id.clone());
                        break;
                    }
                }

                let item = Item {
                    id: tagged_op.id().clone(),
                    origin_left,
                    origin_right,
                    content: content.clone(),
                    effect_live_dots: BTreeSet::from([tagged_op.id().clone()]),
                    presence: PresenceState {
                        inserted: true,
                        born_dots: BTreeSet::from([tagged_op.id().clone()]),
                        deleted_dots: BTreeMap::new(),
                    },
                };

                Self::integrate(doc, item, idx)
            }
        }
    }

    fn materialize(doc: &Document<V>) -> Vec<V> {
        // Queries read the final replayed effect state, not the transient prepared state
        // that Eg-Walker uses while moving between parent versions.
        doc.items
            .iter()
            .filter(|item| !item.effect_live_dots.is_empty())
            .map(|item| item.content.clone())
            .collect()
    }

    fn diff(
        state: &impl IsUnstableState<List<V>>,
        current_version: &Option<EventId>,
        parents: &[EventId],
    ) -> (Vec<EventId>, Vec<EventId>) {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum DiffFlag {
            A,
            B,
            Shared,
        }

        #[allow(clippy::mutable_key_type)]
        let mut flags: HashMap<EventId, DiffFlag> = HashMap::default();
        // PriorityQueue: prioritizing higher NodeIndex first
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
            let event_idx = state.delivery_order(id);
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
            let event_idx = state.delivery_order(p);
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
                let event_idx = state.delivery_order(parent);
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
}

impl<V> PureCRDT for List<V>
where
    V: Debug + Clone,
{
    type Value = Vec<V>;
    type StableState = Vec<V>;

    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_STABILIZE: bool = true;

    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> bool {
        let state = Self::execute_query(Read::new(), stable, unstable);
        match op {
            List::Insert { pos, .. } => *pos <= state.len(),
            List::Delete { pos } => *pos < state.len(),
            List::Update { pos } => *pos < state.len(),
            List::DeleteRange { start, len } => (*start + *len) <= state.len(),
        }
    }

    fn causal_reset(
        version: &Version,
        conservative: bool,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> CausalReset<Self> {
        if !conservative {
            panic!("EventGraph::redundant_by_parent non-conservative is not implemented");
        }
        let state = Self::execute_query(ReadAt::new(version), stable, unstable);
        let op = List::DeleteRange {
            start: 0,
            len: state.len(),
        };
        CausalReset::Inject(vec![op])
    }
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for List<V>
where
    V: Debug + Clone,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Vec<V> {
        let mut document = Document::default();

        for tagged_op in unstable.iter() {
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

        let mut result = stable.clone();
        result.extend(Self::materialize(&document));
        result
    }
}

impl Eval<Read<String>> for List<char> {
    fn execute_query(
        _q: Read<String>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> String {
        let chars: Vec<char> = Self::execute_query(Read::new(), stable, unstable);
        chars.into_iter().collect()
    }
}

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

// TODO: merge with the classical Read
impl<'a, V> Eval<ReadAt<'a, <Self as PureCRDT>::Value>> for List<V>
where
    V: Debug + Clone,
{
    // TODO: add a stable state containing the snapshot
    fn execute_query(
        q: ReadAt<<Self as PureCRDT>::Value>,
        _stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Vec<V> {
        let mut document = Document::default();

        for tagged_op in unstable.iter() {
            // TODO: optimize by stopping when we pass the version
            if !tagged_op.id().is_predecessor_of(q.version) {
                continue;
            }

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

        Self::materialize(&document)
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
        unstable: &impl IsUnstableState<Self>,
    ) -> Self {
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

// TODO: implement the stable state for Eg-Walker.
// Idea: compute the state at the stable version (ReadAtVersion)
// Then flush this state (a Vec<V>) into the stable state.
// Remove all causally stable updates from the unstable state (keep the nodes but remove the ops for updates that are parent of unstable updates).
impl<V> IsStableState<List<V>> for Vec<V>
where
    V: Debug + Clone,
{
    fn is_default(&self) -> bool {
        self.is_empty()
    }

    fn apply(&mut self, _value: List<V>) {
        todo!()
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<List<V>>,
        _tagged_op: &TaggedOp<List<V>>,
    ) {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{replica::IsReplica, state::event_graph::EventGraph};

    use super::*;
    use crate::utils::membership::{triplet_log, twins_log};

    #[test]
    fn simple_insertion_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        assert_eq!(&replica_a.query(Read::<String>::new()), "A");
        assert_eq!(
            replica_a.query(Read::<String>::new()),
            replica_b.query(Read::<String>::new())
        );
    }

    #[test]
    fn concurrent_insertions_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();
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
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

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

        let run_1 = RunConfig::new(0.6, 8, 2_000, None, None, false, false);
        // let run_2 = RunConfig::new(0.6, 8, 2_000, None, None, false, false);
        // let run_3 = RunConfig::new(0.6, 8, 3_000, None, None, false, false);
        let runs = vec![run_1]; //, run_2, run_3];
        // let run = RunConfig::new(0.6, 8, 100, None, None, false, false);
        // let runs = vec![run; 10_000];

        let config =
            FuzzerConfig::<EventGraph<List<char>>>::new(
                "list",
                runs,
                true,
                |a, b| a == b,
                true,
                None,
            );

        fuzzer::<EventGraph<List<char>>>(config);
    }
}
