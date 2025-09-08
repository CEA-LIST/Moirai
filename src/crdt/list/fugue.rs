use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    event::{id::EventId, tagged_op::TaggedOp},
};
use std::fmt::Debug;

/// A character item in the document
#[derive(Clone, Debug)]
pub struct Item {
    /// The character content (single character)
    pub content: char,
    /// Unique identifier for this item
    pub id: EventId,
    /// Reference to the item to the left (null if at start)
    pub origin_left: Option<EventId>,
    /// Reference to the item to the right (null if at end)
    pub origin_right: Option<EventId>,
    /// Whether this item is deleted
    pub deleted: bool,
}

impl Item {
    pub fn new(
        content: char,
        id: EventId,
        origin_left: Option<EventId>,
        origin_right: Option<EventId>,
    ) -> Self {
        Self {
            content,
            id,
            origin_left,
            origin_right,
            deleted: false,
        }
    }
}

/// Operations for the Fugue text CRDT
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FugueTextOp {
    /// Insert a character with metadata
    Insert {
        content: char,
        origin_left: Option<EventId>,
        origin_right: Option<EventId>,
    },
    /// Delete an item by its ID
    Delete { id: EventId },
}

/// Document state for the Fugue text CRDT
#[derive(Clone, Debug, Default)]
pub struct FugueDocument {
    /// All items in the document (including deleted ones)
    pub content: Vec<Item>,
}

impl FugueDocument {
    pub fn new() -> Self {
        Self {
            content: Vec::new(),
        }
    }

    /// Get the visible text content
    pub fn get_content(&self) -> String {
        let mut content = String::new();
        for item in &self.content {
            if !item.deleted {
                content.push(item.content);
            }
        }
        content
    }

    /// Find the index of an item by its ID
    pub fn find_item_by_id(&self, id: &EventId) -> Option<usize> {
        for (i, item) in self.content.iter().enumerate() {
            if item.id == *id {
                return Some(i);
            }
        }
        None
    }

    /// Integrate a new item into the document (following Fugue logic)
    pub fn integrate(&mut self, new_item: Item) {
        // Find insertion position
        let left_idx: isize = if let Some(ref origin_left) = new_item.origin_left {
            self.find_item_by_id(origin_left).unwrap_or(0) as isize
        } else {
            -1
        };

        let right_idx = if let Some(ref origin_right) = new_item.origin_right {
            self.find_item_by_id(origin_right)
                .unwrap_or(self.content.len())
        } else {
            self.content.len()
        };

        let mut dest_idx = if left_idx == -1 {
            0
        } else {
            (left_idx + 1) as usize
        };
        let mut scanning = false;

        // Scan forward to find the right insertion position
        for i in dest_idx.. {
            if !scanning {
                dest_idx = i;
            }

            // If we reach the end of the document, just insert
            if i == self.content.len() {
                break;
            }

            // If we reach the right boundary, no ambiguity - insert here
            if i == right_idx {
                break;
            }

            let other = &self.content[i];
            let other_left_idx = if let Some(ref other_origin_left) = other.origin_left {
                self.find_item_by_id(other_origin_left).unwrap_or(0)
            } else {
                0
            };

            let other_right_idx = if let Some(ref other_origin_right) = other.origin_right {
                self.find_item_by_id(other_origin_right)
                    .unwrap_or(self.content.len())
            } else {
                self.content.len()
            };

            // Fugue ordering logic
            if other_left_idx < left_idx as usize
                || (other_left_idx == left_idx as usize
                    && other_right_idx == right_idx
                    && new_item.id.origin_id() < other.id.origin_id())
            {
                break;
            }

            if other_left_idx == left_idx as usize {
                scanning = other_right_idx < right_idx;
            }
        }

        // Ensure dest_idx is within bounds before inserting
        //dest_idx = dest_idx.min(self.content.len());
        // Insert the item
        self.content.insert(dest_idx, new_item);
    }

    /// Apply a delete operation
    pub fn apply_delete(&mut self, id: &EventId) {
        if let Some(idx) = self.find_item_by_id(id) {
            self.content[idx].deleted = true;
        }
    }
}

impl PureCRDT for FugueTextOp {
    // Inspired by Joseph Gentle's implementation:
    // - Fugue Max: https://github.com/josephg/crdt-from-scratch
    type Value = String;
    type StableState = Vec<Self>;

    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_STABILIZE: bool = true;

    fn eval<'a>(
        _stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        Self: 'a,
    {
        // Create a working copy of the stable document
        // let mut doc = stable.clone();
        let mut doc = FugueDocument::new();
        // Loop through all events in the event graph (causally ordered)
        for tagged_op in unstable {
            match tagged_op.op().clone() {
                FugueTextOp::Insert {
                    content,
                    origin_left,
                    origin_right,
                } => {
                    // Create item with ID from the dotted notation
                    let item = Item::new(
                        content,
                        tagged_op.id().clone(),
                        origin_left.clone(),
                        origin_right.clone(),
                    );
                    // Integrate the item if it can be inserted now
                    doc.integrate(item);
                }
                FugueTextOp::Delete { id } => {
                    // Apply delete operation
                    doc.apply_delete(&id);
                }
            }
        }

        // Return the final text content
        doc.get_content()
    }
}

impl FugueTextOp {
    pub fn insert(
        content: char,
        origin_left: Option<EventId>,
        origin_right: Option<EventId>,
    ) -> Self {
        Self::Insert {
            content,
            origin_left,
            origin_right,
        }
    }

    pub fn delete(id: EventId) -> Self {
        Self::Delete { id }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::test_util::twins,
        protocol::{event::Event, replica::IsReplica},
    };

    use super::*;

    fn extract_item_id_from_event(event: &Event<FugueTextOp>) -> Option<EventId> {
        match &event.op() {
            FugueTextOp::Insert { .. } => {
                // Extract from the event's metadata
                Some(event.id().clone())
            }
            FugueTextOp::Delete { id } => Some(id.clone()),
        }
    }

    #[test]
    fn test_simple_insertion_crdt() {
        let (mut replica_a, mut replica_b) = twins::<FugueTextOp>();

        // Create insert operation
        let event = replica_a.send(FugueTextOp::insert('A', None, None));
        replica_b.receive(event);

        // Evaluate
        assert_eq!(replica_a.query(), "A");
        assert_eq!(replica_b.query(), "A");
    }

    #[test]
    fn test_concurrent_insertions_crdt() {
        let (mut replica_a, mut replica_b) = twins::<FugueTextOp>();

        let event1 = replica_a.send(FugueTextOp::insert('H', None, None));
        let id1 = extract_item_id_from_event(&event1).unwrap();

        replica_b.receive(event1);
        let result = replica_a.query();
        assert_eq!(result, "H");
        let event2a = replica_a.send(FugueTextOp::insert('e', Some(id1.clone()), None));
        let event2b = replica_b.send(FugueTextOp::insert('i', Some(id1.clone()), None));
        replica_b.receive(event2a);
        replica_a.receive(event2b);
        let result2b = replica_b.query();
        assert_eq!(result2b, "Hei");
        assert!(replica_a.query() == replica_b.query());
    }

    #[test]
    fn test_delete_operation_crdt() {
        let (mut replica_a, mut replica_b) = twins::<FugueTextOp>();

        // Insert 'A'
        let event1 = replica_a.send(FugueTextOp::insert('A', None, None));
        let id1 = extract_item_id_from_event(&event1).unwrap();
        replica_b.receive(event1);

        // Delete 'A'

        let event2a = replica_a.send(FugueTextOp::delete(id1));

        replica_b.receive(event2a);

        // Evaluate
        let result = replica_a.query();
        assert_eq!(result, "");
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn test_conc_delete_ins_crdt() {
        let (mut replica_a, mut replica_b) = twins::<FugueTextOp>();

        // Insert 'A'
        let event1 = replica_a.send(FugueTextOp::insert('A', None, None));
        let id1 = extract_item_id_from_event(&event1).unwrap();
        replica_b.receive(event1);

        // Delete 'A'

        let event2a = replica_a.send(FugueTextOp::delete(id1.clone()));
        let event2b = replica_b.send(FugueTextOp::insert('B', None, Some(id1.clone())));
        replica_a.receive(event2b);
        replica_b.receive(event2a);

        // Evaluate
        let result = replica_a.query();
        assert_eq!(result, "B");
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn test_sequential_conc_operations_crdt() {
        let (mut replica_a, mut replica_b) = twins::<FugueTextOp>();

        let event1 = replica_a.send(FugueTextOp::insert('H', None, None));
        let id1 = extract_item_id_from_event(&event1).unwrap();

        replica_b.receive(event1);
        let result = replica_a.query();
        assert_eq!(result, "H");
        // let id1 = EventId::new("a".to_string(), 0);
        let event2a = replica_a.send(FugueTextOp::insert('e', Some(id1.clone()), None));
        let id2a = extract_item_id_from_event(&event2a);
        let event2b = replica_b.send(FugueTextOp::insert('i', Some(id1.clone()), None));
        let id2b = extract_item_id_from_event(&event2b);
        replica_b.receive(event2a);
        replica_a.receive(event2b);
        let result2b = replica_b.query();
        assert_eq!(result2b, "Hei");

        let event3a = replica_a.send(FugueTextOp::insert(' ', id2a, id2b.clone()));
        let id3a = extract_item_id_from_event(&event3a).unwrap();
        let event4a = replica_a.send(FugueTextOp::insert('h', Some(id3a.clone()), id2b.clone()));

        replica_b.receive(event3a);
        replica_b.receive(event4a);
        //let result = replica_a.query();
        assert!(replica_a.query() == replica_b.query());
        //assert_eq!(result, "He i");
    }
}
