use std::{
    collections::BinaryHeap,
    fmt::{Debug, Display, Formatter, Result},
};

#[cfg(feature = "fuzz")]
use rand::RngCore;

#[cfg(feature = "fuzz")]
use crate::fuzz::{config::OpGenerator, value_generator::ValueGenerator};
use crate::{
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::Eval,
            pure_crdt::PureCRDT,
            query::{QueryOperation, Read},
            redundancy::RedundancyRelation,
        },
        event::{id::EventId, tagged_op::TaggedOp},
        state::{stable_state::IsStableState, unstable_state::IsUnstableState},
    },
    HashMap,
};

// TODO: use Fair Tag

// Single-character, position-based, pure op-based CRDT operations
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum List<V> {
    Insert { content: V, pos: usize },
    Delete { pos: usize },
    DeleteRange { start: usize, len: usize },
    Update { pos: usize },
}

#[derive(Clone, Debug)]
enum PrepareState {
    /// \>0
    Deleted(u8), // number of deletes applied
    /// 0
    Inserted,
    /// -1
    NotYetInserted,
}

impl PrepareState {
    fn advance_state(&self) -> Self {
        match self {
            PrepareState::Deleted(x) => PrepareState::Deleted(x + 1),
            PrepareState::Inserted => PrepareState::Deleted(1),
            PrepareState::NotYetInserted => PrepareState::Inserted,
        }
    }

    fn retreat_state(&self) -> Self {
        match self {
            PrepareState::Deleted(x) if *x >= 2 => PrepareState::Deleted(x - 1),
            PrepareState::Deleted(1) => PrepareState::Inserted,
            PrepareState::Deleted(_) => unreachable!(),
            PrepareState::Inserted => PrepareState::NotYetInserted,
            PrepareState::NotYetInserted => PrepareState::NotYetInserted,
        }
    }
}

#[derive(Clone, Debug)]
struct Item {
    id: EventId,
    /// Event id of the character the user saw when inserting this new op.
    /// The fields from the CRDT that determines insertion order.
    origin_left: Option<EventId>,
    origin_right: Option<EventId>,
    /// State at effect version. Either inserted or inserted-and-subsequently-deleted.
    ever_deleted: bool,
    /// State at prepare version (affected by retreat / advance)
    prepare_state: PrepareState, // NOT_YET_INSERTED, INSERTED, >0 for number of deletes applied
}

#[derive(Clone, Debug)]
enum DeleteTarget {
    Single(EventId),
    Range(Vec<EventId>),
}

#[derive(Default, Debug)]
struct Document {
    items: Vec<Item>,
    /// Last processed event
    current_version: Option<EventId>,
    /// Key = delete op id, Value = target insert op id
    delete_targets: HashMap<EventId, DeleteTarget>,
    /// map of the event id to the current value position in vector of items
    items_by_idx: HashMap<EventId, usize>,
    /// map of event id to the index of the mutate entry
    mutations: HashMap<EventId, EventId>,
}

impl Display for Document {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        writeln!(
            f,
            "items by idx: {}",
            self.items_by_idx
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        writeln!(f, "items:")?;
        for item in &self.items {
            write!(f, "    - {}", item.id)?;
            write!(f, " [")?;
            if let Some(ol) = &item.origin_left {
                write!(f, " L:{}", ol)?;
            } else {
                write!(f, " L:None")?;
            }
            if let Some(or) = &item.origin_right {
                write!(f, " R:{}", or)?;
            } else {
                write!(f, " R:None")?;
            }
            write!(f, " | EverDeleted: {}", item.ever_deleted)?;
            write!(f, " | State: {:?}", item.prepare_state)?;
            writeln!(f, " ]")?;
        }
        // delete targets
        writeln!(
            f,
            "delete targets: {}",
            self.delete_targets
                .iter()
                .map(|(k, v)| format!("{}: {:?}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        Ok(())
    }
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

    /// Where to insert the new item so that it appears at target_pos in the current visible document
    /// # Returns
    /// First: index in the document
    /// Second: index in the snapshot
    fn find_by_current_pos(items: &[Item], target_pos: usize) -> (usize, usize) {
        let mut cur_pos = 0usize;
        let mut end_pos = 0usize;
        let mut idx = 0usize;

        while cur_pos < target_pos {
            if idx >= items.len() {
                panic!("Past end of items list");
            }

            let item = &items[idx];
            if matches!(item.prepare_state, PrepareState::Inserted) {
                cur_pos += 1;
            }
            if !item.ever_deleted {
                end_pos += 1;
            }
            idx += 1;
        }

        (idx, end_pos)
    }

    /// Add a new value to the snapshot
    fn integrate(
        doc: &mut Document,
        new_item: Item,
        mut idx: usize,
        mut end_pos: usize,
        snapshot: &mut Vec<V>,
        content: V,
    ) {
        let mut scan_idx = idx;
        let mut scan_end_pos = end_pos;

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

            if !matches!(other.prepare_state, PrepareState::NotYetInserted) {
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

            if oleft < left || (oleft == left && oright == right && new_item.id < other.id) {
                break;
            }
            if oleft == left {
                scanning = oright < right;
            }

            if !other.ever_deleted {
                scan_end_pos += 1;
            }
            scan_idx += 1;

            if !scanning {
                idx = scan_idx;
                end_pos = scan_end_pos;
            }
        }

        doc.items.insert(idx, new_item);

        // Update items_by_nx mapping for all shifted items at and after idx
        for i in idx..doc.items.len() {
            let n = doc.items[i].id.clone();
            doc.items_by_idx.insert(n, i);
        }
        // Update snapshot with content
        snapshot.insert(end_pos, content);
    }

    fn retreat(doc: &mut Document, state: &impl IsUnstableState<List<V>>, event_id: &EventId) {
        // For inserts, target is the item itself; for deletes, target is the item which was deleted
        let targets: Vec<&EventId> = match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => vec![event_id],
            List::DeleteRange { .. } | List::Delete { .. } => doc
                .delete_targets
                .get(event_id)
                .map(|t| match t {
                    DeleteTarget::Single(eid) => vec![eid],
                    DeleteTarget::Range(eids) => eids.iter().collect(),
                })
                .unwrap(),
            List::Update { .. } => return,
        };

        for target in targets {
            if let Some(&item_idx) = doc.items_by_idx.get(target) {
                let item = &mut doc.items[item_idx];
                item.prepare_state = PrepareState::retreat_state(&item.prepare_state);
            }
        }
    }

    fn advance(doc: &mut Document, state: &impl IsUnstableState<List<V>>, event_id: &EventId) {
        let targets: Vec<&EventId> = match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => vec![event_id],
            List::DeleteRange { .. } | List::Delete { .. } => doc
                .delete_targets
                .get(event_id)
                .map(|t| match t {
                    DeleteTarget::Single(eid) => vec![eid],
                    DeleteTarget::Range(eids) => eids.iter().collect(),
                })
                .unwrap(),
            List::Update { .. } => return,
        };

        for target in targets {
            if let Some(&item_idx) = doc.items_by_idx.get(target) {
                let item = &mut doc.items[item_idx];
                item.prepare_state = PrepareState::advance_state(&item.prepare_state);
            }
        }
    }

    fn apply(doc: &mut Document, tagged_op: &TaggedOp<List<V>>, snapshot: &mut Vec<V>) {
        match tagged_op.op() {
            List::Delete { pos } => {
                let (mut idx, mut end_pos) = Self::find_by_current_pos(&doc.items, *pos);

                while idx < doc.items.len()
                    && matches!(
                        doc.items[idx].prepare_state,
                        PrepareState::NotYetInserted | PrepareState::Deleted(_)
                    )
                {
                    if !doc.items[idx].ever_deleted {
                        end_pos += 1;
                    }
                    idx += 1;
                }

                debug_assert!(
                    idx < doc.items.len(),
                    "No `INSERTED` item found at position {pos}"
                );

                let item = &mut doc.items[idx];
                if !item.ever_deleted {
                    item.ever_deleted = true;
                    // TODO: O(n)
                    snapshot.remove(end_pos);
                }
                item.prepare_state = PrepareState::Deleted(1);
                doc.delete_targets.insert(
                    tagged_op.id().clone(),
                    DeleteTarget::Single(item.id.clone()),
                );
            }
            List::DeleteRange { start, len } => {
                let (mut idx, mut end_pos) = Self::find_by_current_pos(&doc.items, *start);
                let mut pos = 0usize;
                let mut deleted_ids = Vec::new();

                while pos < *len {
                    while idx < doc.items.len()
                        && matches!(
                            doc.items[idx].prepare_state,
                            PrepareState::NotYetInserted | PrepareState::Deleted(_)
                        )
                    {
                        if !doc.items[idx].ever_deleted {
                            end_pos += 1;
                        }
                        idx += 1;
                    }

                    if idx >= doc.items.len() {
                        return;
                    }

                    let item = &mut doc.items[idx];
                    if !item.ever_deleted {
                        item.ever_deleted = true;
                        // TODO: O(n). Optimize with drain
                        snapshot.remove(end_pos);
                    }
                    item.prepare_state = PrepareState::Deleted(1);
                    deleted_ids.push(item.id.clone());
                    pos += 1;
                }

                doc.delete_targets
                    .insert(tagged_op.id().clone(), DeleteTarget::Range(deleted_ids));
            }
            List::Update { pos } => {
                let (mut idx, _) = Self::find_by_current_pos(&doc.items, *pos);

                while idx < doc.items.len()
                    && matches!(
                        doc.items[idx].prepare_state,
                        PrepareState::NotYetInserted | PrepareState::Deleted(_)
                    )
                {
                    idx += 1;
                }

                debug_assert!(
                    idx < doc.items.len(),
                    "No `INSERTED` item found at position {pos}"
                );

                doc.mutations
                    .insert(tagged_op.id().clone(), doc.items[idx].id.clone());
            }
            List::Insert { content, pos } => {
                let (idx, end_pos) = Self::find_by_current_pos(&doc.items, *pos);

                if idx >= 1
                    && matches!(
                        doc.items[idx - 1].prepare_state,
                        PrepareState::NotYetInserted | PrepareState::Deleted(_)
                    )
                {
                    panic!("Item to the left is not inserted! What!"); // OLDCODE behavior retained
                }

                let origin_left = if idx == 0 {
                    None
                } else {
                    Some(doc.items[idx - 1].id.clone())
                };

                let mut origin_right = None;
                for i in idx..doc.items.len() {
                    if matches!(
                        doc.items[i].prepare_state,
                        PrepareState::Inserted | PrepareState::Deleted(_)
                    ) {
                        origin_right = Some(doc.items[i].id.clone());
                        break;
                    }
                }

                let item = Item {
                    id: tagged_op.id().clone(),
                    origin_left,
                    origin_right,
                    ever_deleted: false,
                    prepare_state: PrepareState::Inserted,
                };

                Self::integrate(doc, item, idx, end_pos, snapshot, content.clone())
            }
        }
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

    fn apply(&mut self, value: List<V>) {
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
        let mut snapshot: Vec<V> = stable.clone();

        for tagged_op in unstable.iter() {
            let parents = unstable.parents(tagged_op.id());
            let (a_only, b_only) = Self::diff(unstable, &document.current_version, &parents);

            for event_id in a_only {
                Self::retreat(&mut document, unstable, &event_id);
            }

            for event_id in b_only {
                Self::advance(&mut document, unstable, &event_id);
            }

            Self::apply(&mut document, tagged_op, &mut snapshot);

            document.current_version = Some(tagged_op.id().clone());
        }

        snapshot.into_iter().collect()
    }
}

pub struct MutationTarget {
    target: EventId,
}

impl MutationTarget {
    pub fn new(target: EventId) -> Self {
        Self { target }
    }
}

impl QueryOperation for MutationTarget {
    type Response = EventId;
}

impl<V> Eval<MutationTarget> for List<V>
where
    V: Debug + Clone,
{
    fn execute_query(
        q: MutationTarget,
        _stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> EventId {
        let mut document = Document::default();
        let mut snapshot: Vec<V> = Vec::new();

        for tagged_op in unstable.iter() {
            let parents = unstable.parents(tagged_op.id());
            let (a_only, b_only) = Self::diff(unstable, &document.current_version, &parents);

            for event_id in a_only {
                Self::retreat(&mut document, unstable, &event_id);
            }

            for event_id in b_only {
                Self::advance(&mut document, unstable, &event_id);
            }

            Self::apply(&mut document, tagged_op, &mut snapshot);

            document.current_version = Some(tagged_op.id().clone());

            if let Some(event_id) = document.mutations.get(&q.target) {
                return event_id.clone();
            }
        }

        panic!("Mutation target not found in document");
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
        let mut snapshot: Vec<V> = Vec::new();

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

            Self::apply(&mut document, tagged_op, &mut snapshot);

            document.current_version = Some(tagged_op.id().clone());
        }

        snapshot.into_iter().collect()
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGenerator for List<V>
where
    V: ValueGenerator + Debug + Clone,
{
    type Config = ();

    fn generate(
        rng: &mut impl RngCore,
        _config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        use rand::{
            distr::{weighted::WeightedIndex, Distribution},
            Rng,
        };

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
    use super::*;
    use crate::{
        crdt::test_util::{triplet_log, twins_log},
        protocol::{replica::IsReplica, state::event_graph::EventGraph},
    };

    fn to_string(vec: &[char]) -> String {
        vec.iter().collect()
    }

    #[test]
    fn simple_insertion_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        assert_eq!(to_string(&replica_a.query(Read::new())), "A");
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_insertions_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let e1 = replica_a.send(List::insert('H', 0)).unwrap();
        replica_b.receive(e1);
        assert_eq!(to_string(&replica_a.query(Read::new())), "H");

        let e2a = replica_a.send(List::insert('e', 1)).unwrap();
        let e2b = replica_b.send(List::insert('i', 1)).unwrap();
        replica_b.receive(e2a);
        replica_a.receive(e2b);

        let res_b = to_string(&replica_b.query(Read::new()));
        assert!(
            res_b == "Hei" || res_b == "Hie",
            "Unexpected order: {}",
            res_b
        );
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_insert() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let e1 = replica_a.send(List::insert('H', 0)).unwrap();
        let e2 = replica_b.send(List::insert('i', 0)).unwrap();
        replica_a.receive(e2);
        replica_b.receive(e1);

        let res_a = to_string(&replica_a.query(Read::new()));
        assert!(
            res_a == "Hi" || res_a == "iH",
            "Unexpected order: {}",
            res_a
        );
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn delete_operation_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let e1 = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(e1);

        let e2 = replica_a.send(List::delete(0)).unwrap();
        replica_b.receive(e2);

        assert_eq!(to_string(&replica_a.query(Read::new())), "");
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_b.query(Read::new()))
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

        assert_eq!(to_string(&replica_a.query(Read::new())), "B");
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_b.query(Read::new()))
        );
    }

    #[test]
    fn sequential_conc_operations_egwalker() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let e1 = replica_a.send(List::insert('H', 0)).unwrap();
        replica_b.receive(e1);
        assert_eq!(to_string(&replica_a.query(Read::new())), "H");

        let e2a = replica_a.send(List::insert('e', 1)).unwrap();
        let e2b = replica_b.send(List::insert('i', 1)).unwrap();
        replica_b.receive(e2a);
        replica_a.receive(e2b);
        assert!(
            to_string(&replica_b.query(Read::new())) == "Hei"
                || to_string(&replica_b.query(Read::new())) == "Hie"
        );

        // Insert a space between e and i from A's perspective (which will be position 2 if e<i)
        let e3 = replica_a.send(List::insert(' ', 2)).unwrap();
        replica_b.receive(e3);
        let res = to_string(&replica_a.query(Read::new()));
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
        assert_eq!(to_string(&replica_a.query(Read::new())), "Hi");

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
        assert_eq!(to_string(&replica_a.query(ReadAt::new(e4_version))), "Hi");
        assert_eq!(to_string(&replica_a.query(Read::new())), "Hey!");
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_b.query(Read::new()))
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
        assert_eq!(to_string(&replica_a.query(Read::new())), "ABC");

        let e4 = replica_a.send(List::delete_range(0, 2)).unwrap();
        replica_b.receive(e4);
        assert_eq!(to_string(&replica_a.query(Read::new())), "C");
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_b.query(Read::new()))
        );
    }

    #[test]
    fn delete_range_egwalker_2() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<List<char>>>();

        let event_a = replica_a.send(List::insert('A', 0)).unwrap();
        replica_b.receive(event_a);

        assert_eq!(to_string(&replica_a.query(Read::new())), "A");
        assert_eq!(to_string(&replica_b.query(Read::new())), "A");

        let event_b = replica_b.send(List::insert('B', 0)).unwrap();
        assert_eq!(to_string(&replica_b.query(Read::new())), "BA");
        let event_b_2 = replica_b.send(List::delete_range(0, 2)).unwrap();

        assert_eq!(to_string(&replica_b.query(Read::new())), "");

        let event_a_2 = replica_a.send(List::delete_range(0, 1)).unwrap();

        assert_eq!(to_string(&replica_a.query(Read::new())), "");

        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);
        assert_eq!(to_string(&replica_a.query(Read::new())), "");
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_b.query(Read::new()))
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

        assert_eq!(to_string(&replica_a.query(Read::new())), "y");
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_c.query(Read::new()))
        );
        assert_eq!(
            to_string(&replica_a.query(Read::new())),
            to_string(&replica_b.query(Read::new()))
        );
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_list() {
        use crate::fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run_1 = RunConfig::new(0.4, 4, 1_000, None, None, false, false);
        // let run_2 = RunConfig::new(0.4, 4, 200, None, None, false, false);
        // let run_3 = RunConfig::new(0.4, 4, 300, None, None, false, false);
        // let run_4 = RunConfig::new(0.4, 4, 400, None, None, false, false);
        // let run_5 = RunConfig::new(0.4, 4, 500, None, None, false, false);
        // let run_6 = RunConfig::new(0.4, 4, 600, None, None, false, false);
        // let run_7 = RunConfig::new(0.4, 4, 700, None, None, false, false);
        // let run_8 = RunConfig::new(0.4, 4, 800, None, None, false, false);
        // let run_9 = RunConfig::new(0.4, 4, 900, None, None, false, false);
        // let run_10 = RunConfig::new(0.4, 4, 1_000, None, None, false, false);
        let runs = vec![
            run_1, // run_2, run_3, run_4, run_5, run_6, run_7, run_8, run_9, // run_10,
        ];

        let config =
            FuzzerConfig::<EventGraph<List<char>>>::new("list", runs, true, |a, b| a == b, true);

        fuzzer::<EventGraph<List<char>>>(config);
    }
}
