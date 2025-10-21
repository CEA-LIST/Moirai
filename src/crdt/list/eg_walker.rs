use std::{
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
};

use crate::protocol::{
    crdt::pure_crdt::{Eval, PureCRDT, QueryOperation, Read},
    event::{id::EventId, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
};

// Single-character, position-based, pure op-based CRDT operations
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum List<V> {
    Insert { content: V, pos: usize },
    Delete { pos: usize },
}

#[derive(Clone, Debug)]
enum State {
    /// \>0
    Deleted(u8), // number of deletes applied
    /// 0
    Inserted,
    /// -1
    NotYetInserted,
}

#[derive(Clone, Debug)]
struct Item {
    id: EventId,
    origin_left: Option<EventId>, // Event id of the character the user saw when inserting this new op
    origin_right: Option<EventId>,
    deleted: bool,
    cur_state: State, // NOT_YET_INSERTED, INSERTED, >0 for number of deletes applied
}

#[derive(Default)]
struct Document {
    items: Vec<Item>,
    /// Last processed event
    current_version: Option<EventId>,
    /// Key = delete op id, Value = target insert op id
    del_targets: HashMap<EventId, EventId>,
    /// map of the event id to the current value position in vector of items
    items_by_nx: HashMap<EventId, usize>,
}

impl<V> List<V>
where
    V: Clone,
{
    pub fn insert(content: V, pos: usize) -> Self {
        Self::Insert { content, pos }
    }

    pub fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    /// First: index in the document, second: index in the snapshot
    /// Where to insert the new item so that it appears at target_pos in the current visible document
    fn find_by_current_pos(items: &[Item], target_pos: usize) -> (usize, usize) {
        let mut cur_pos = 0usize;
        let mut end_pos = 0usize;
        let mut idx = 0usize;

        while cur_pos < target_pos {
            if idx >= items.len() {
                panic!("Past end of items list");
            }

            let item = &items[idx];
            if matches!(item.cur_state, State::Inserted) {
                cur_pos += 1;
            }
            if !item.deleted {
                end_pos += 1;
            }
            idx += 1;
        }

        (idx, end_pos)
    }

    /// Search for the item corresponding to the event_id
    fn index_of_node(items: &[Item], event_id: &EventId) -> usize {
        items
            .iter()
            .position(|it| it.id == *event_id)
            .expect("Could not find item by NodeIndex")
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
        let right = if let Some(r) = &new_item.origin_right {
            Self::index_of_node(&doc.items, r)
        } else {
            doc.items.len()
        };

        let mut scanning = false;

        while scan_idx < right {
            let other = &doc.items[scan_idx];

            if !matches!(other.cur_state, State::NotYetInserted) {
                break;
            }

            let oleft = if let Some(ol) = &other.origin_left {
                Self::index_of_node(&doc.items, ol) as isize
            } else {
                -1
            };

            let oright = if let Some(or) = &other.origin_right {
                Self::index_of_node(&doc.items, or)
            } else {
                doc.items.len()
            };

            if oleft < left || (oleft == left && oright == right && new_item.id < other.id) {
                break;
            }
            if oleft == left {
                scanning = oright < right;
            }

            if !other.deleted {
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
            doc.items_by_nx.insert(n, i);
        }
        // Update snapshot with content
        snapshot.insert(end_pos, content);
    }

    fn retreat(doc: &mut Document, state: &impl IsUnstableState<List<V>>, event_id: &EventId) {
        // For inserts, target is the item itself; for deletes, target is the item which was deleted
        let target = match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => event_id,
            List::Delete { .. } => &doc.del_targets[event_id],
        };

        if let Some(&item_idx) = doc.items_by_nx.get(target) {
            let item = &mut doc.items[item_idx];
            item.cur_state = match item.cur_state {
                State::Deleted(x) if x >= 2 => State::Deleted(x - 1),
                State::Deleted(1) => State::Inserted,
                State::Deleted(_) => unreachable!(),
                State::Inserted => State::NotYetInserted,
                State::NotYetInserted => State::NotYetInserted,
            }
        }
    }

    fn advance(doc: &mut Document, state: &impl IsUnstableState<List<V>>, event_id: &EventId) {
        let target = match &state.get(event_id).unwrap().op() {
            List::Insert { .. } => event_id,
            List::Delete { .. } => &doc.del_targets[event_id],
        };

        if let Some(&item_idx) = doc.items_by_nx.get(target) {
            let item = &mut doc.items[item_idx];
            item.cur_state = match item.cur_state {
                State::Deleted(x) => State::Deleted(x + 1),
                State::Inserted => State::Deleted(1),
                State::NotYetInserted => State::Inserted,
            }
        }
    }

    fn apply(doc: &mut Document, tagged_op: &TaggedOp<List<V>>, snapshot: &mut Vec<V>) {
        match tagged_op.op() {
            List::Delete { pos } => {
                let (mut idx, mut end_pos) = Self::find_by_current_pos(&doc.items, *pos);

                while idx < doc.items.len()
                    && matches!(
                        doc.items[idx].cur_state,
                        State::NotYetInserted | State::Deleted(_)
                    )
                {
                    if !doc.items[idx].deleted {
                        end_pos += 1;
                    }
                    idx += 1;
                }

                if idx >= doc.items.len() {
                    panic!("No INSERTED item found at position {pos}");
                }

                {
                    let item = &mut doc.items[idx];
                    if !item.deleted {
                        item.deleted = true;
                        snapshot.remove(end_pos);
                    }
                    item.cur_state = State::Deleted(1);
                    doc.del_targets
                        .insert(tagged_op.id().clone(), item.id.clone());
                }
            }
            List::Insert { content, pos } => {
                let (idx, end_pos) = Self::find_by_current_pos(&doc.items, *pos);

                if idx >= 1
                    && matches!(
                        doc.items[idx - 1].cur_state,
                        State::NotYetInserted | State::Deleted(_)
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
                    if matches!(doc.items[i].cur_state, State::Inserted | State::Deleted(_)) {
                        origin_right = Some(doc.items[i].id.clone());
                        break;
                    }
                }

                let item = Item {
                    id: tagged_op.id().clone(),
                    origin_left,
                    origin_right,
                    deleted: false,
                    cur_state: State::Inserted,
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
        let mut flags: HashMap<EventId, DiffFlag> = HashMap::new();
        // Match TS PriorityQueue<Lv> by prioritizing higher NodeIndex first
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
    type StableState = Vec<Self>;

    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_STABILIZE: bool = true;

    // fn is_enabled(op: &Self, state: impl Fn() -> Self::Value) -> bool {
    //     let eval = state();
    //     match op {
    //         List::Insert { pos, .. } => *pos <= eval.len(),
    //         List::Delete { pos } => *pos < eval.len(),
    //     }
    // }
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for List<V>
where
    V: Debug + Clone,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        _stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
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
        }

        snapshot.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crdt::test_util::twins_log,
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
        // replica_b.receive(e3.clone());

        // e4: Delete(1) (remove lowercase 'h') depends on e3
        let e4 = replica_a.send(List::delete(1)).unwrap();
        // replica_b.receive(e4.clone());

        // e5: Delete(1) (remove 'i') on other branch
        let e5 = replica_b.send(List::delete(1)).unwrap();
        // replica_a.receive(e5.clone());

        // e6: Insert(1, 'e')
        let e6 = replica_b.send(List::insert('e', 1)).unwrap();
        // replica_a.receive(e6.clone());

        // e7: Insert(2, 'y')
        let e7 = replica_b.send(List::insert('y', 2)).unwrap();

        replica_b.receive(e3.clone());
        replica_b.receive(e4.clone());
        replica_a.receive(e5.clone());
        replica_a.receive(e6.clone());
        replica_a.receive(e7.clone());
        // Merge both replicas so they see all events before e8
        // At this point both should be "Hey"
        // assert_eq!(replica_a.query(Read::new()), "Hey");
        // assert_eq!(replica_a.query(Read::new()));

        // e8: Insert(3, '!')
        let e8 = replica_b.send(List::insert('!', 3)).unwrap();
        replica_a.receive(e8.clone());

        // Final result should be "Hey!"
        assert_eq!(to_string(&replica_a.query(Read::new())), "Hey!");
        // assert_eq!(to_string(&replica_a.query(Read::new())));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_eg_walker() {
        use crate::{
            // crdt::test_util::init_tracing,
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
        };

        // init_tracing();

        let binding = [
            List::insert('A', 0),
            List::insert('B', 0),
            List::insert('C', 0),
            List::delete(0),
            List::insert('D', 1),
            List::insert('E', 1),
            List::insert('F', 1),
            List::delete(1),
            List::insert('G', 2),
            List::insert('H', 2),
            List::insert('I', 2),
            List::delete(2),
            List::insert('J', 3),
            List::insert('K', 3),
            List::insert('L', 3),
            List::delete(3),
        ];
        let ops: OpConfig<List<char>> = OpConfig::Uniform(&binding);

        let run = RunConfig::new(0.4, 8, 100, None, None);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<EventGraph<List<char>>>::new(
            "uw_multidigraph",
            runs,
            ops,
            true,
            |a, b| a == b,
            None,
        );

        fuzzer::<EventGraph<List<char>>>(config);
    }
}
