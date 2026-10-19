use moirai_protocol::event::id::EventId;

use crate::{
    HashMap,
    list::eg_walker::item::{Item, ItemId},
};

#[derive(Clone, Copy)]
enum PositionKind {
    /// A gap before, between, or after visible items; no item is materialized.
    Boundary,
    /// A visible item, materialized if it is still inside a stable range.
    Item,
}

#[derive(Debug)]
enum Record<V> {
    /// Contiguous part of the stable state that has not been touched by unstable ops.
    ///
    /// This is the placeholder used by the partial-replay design: stable content
    /// can be skipped as a range until an insert/delete/update needs to address a
    /// concrete stable element.
    StableRange { start: usize, end: usize },
    /// Concrete unstable item, or a stable item that has been isolated from a range.
    Item(Item<V>),
}

impl<V> Record<V> {
    fn prepared_len(&self) -> usize {
        match self {
            Self::StableRange { start, end } => end - start,
            Self::Item(item) => usize::from(item.prepare.is_visible()),
        }
    }

    /// Can this record be used as an insertion anchor?
    ///
    /// Retreated insertions are still in the sequence but are not integrated in the
    /// prepared version, so new insertions must not anchor on them.
    pub fn is_integrated(&self) -> bool {
        match self {
            Self::StableRange { .. } => true,
            Self::Item(item) => item.prepare.is_integrated(),
        }
    }

    /// First item identity represented by this record.
    ///
    /// For stable ranges this is the first stable index in the range; for concrete
    /// items it is the item's own identity. Empty ranges do not have an identity.
    pub fn first_id(&self) -> Option<ItemId> {
        match self {
            Self::StableRange { start, end } if start < end => Some(ItemId::stable(*start)),
            Self::StableRange { .. } => None,
            Self::Item(item) => Some(item.id.clone()),
        }
    }

    /// Last item identity represented by this record.
    pub fn last_id(&self) -> Option<ItemId> {
        match self {
            Self::StableRange { start, end } if start < end => Some(ItemId::stable(end - 1)),
            Self::StableRange { .. } => None,
            Self::Item(item) => Some(item.id.clone()),
        }
    }
}

#[derive(Debug)]
pub struct Document<'a, V> {
    /// Stable baseline stored outside the unstable event graph.
    stable: &'a [V],
    /// Replay sequence mixing compressed stable ranges and concrete item records.
    records: Vec<Record<V>>,
    /// Concrete item identity to record index.
    ///
    /// Stable items that are still inside a `StableRange` are located by scanning
    /// ranges in `position_of`; only materialized `Item` records live in this map.
    items_by_idx: HashMap<ItemId, usize>,
}

impl<'a, V> Document<'a, V> {
    /// Start replay from a stable document snapshot.
    ///
    /// The snapshot is represented as one compressed range, so reading a stable
    /// document does not allocate one EgWalker item per stable element.
    pub fn new(stable: &'a [V]) -> Self {
        let mut document = Self {
            stable,
            records: Vec::new(),
            items_by_idx: HashMap::default(),
        };
        if !stable.is_empty() {
            document.records.push(Record::StableRange {
                start: 0,
                end: stable.len(),
            });
        }
        document
    }

    /// Rebuild the item-to-record index after a splice or insertion.
    fn rebuild_index(&mut self) {
        self.items_by_idx.clear();
        for (idx, record) in self.records.iter().enumerate() {
            if let Record::Item(item) = record {
                self.items_by_idx.insert(item.id.clone(), idx);
            }
        }
    }

    /// Find the current record containing an item identity.
    ///
    /// Materialized items are looked up through `items_by_idx`. Stable identities
    /// may still be compressed in a range, so they also need a range lookup.
    fn position_of(&self, id: &ItemId) -> Option<usize> {
        if let Some(idx) = self.items_by_idx.get(id) {
            return Some(*idx);
        }

        match id.stable_index() {
            Some(stable_idx) => self
                .records
                .iter()
                .enumerate()
                .find_map(|(record_idx, record)| match record {
                    Record::StableRange { start, end }
                        if *start <= stable_idx && stable_idx < *end =>
                    {
                        Some(record_idx)
                    }
                    _ => None,
                }),
            None => None,
        }
    }

    /// Borrow the concrete item at `idx`, if the record is materialized.
    fn item_mut(&mut self, idx: usize) -> Option<&mut Item<V>> {
        match self.records.get_mut(idx) {
            Some(Record::Item(item)) => Some(item),
            _ => None,
        }
    }

    /// Journaled targets are always concrete items and remain in the sequence.
    pub fn item_by_id_mut(&mut self, id: &ItemId) -> &mut Item<V> {
        let idx = *self
            .items_by_idx
            .get(id)
            .unwrap_or_else(|| panic!("Eg-Walker invariant: missing journal target {id:?}"));
        self.item_mut(idx)
            .expect("journal target must be materialized")
    }

    pub fn prepared_len(&self) -> usize {
        self.records.iter().map(Record::prepared_len).sum()
    }

    fn previous_integrated_id(&self, idx: usize) -> Option<ItemId> {
        self.records[..idx]
            .iter()
            .rev()
            .find(|record| record.is_integrated())
            .and_then(Record::last_id)
    }

    fn next_integrated_id(&self, idx: usize) -> Option<ItemId> {
        self.records[idx..]
            .iter()
            .find(|record| record.is_integrated())
            .and_then(Record::first_id)
    }

    /// Integrate an item into its deterministic Eg-Walker insertion window.
    fn integrate(&mut self, new_item: Item<V>, mut idx: usize) {
        let mut scan_idx = idx;
        let left = new_item
            .origin_left
            .as_ref()
            .and_then(|id| self.position_of(id))
            .map(|idx| idx as isize)
            .unwrap_or(-1);
        let right = new_item
            .origin_right
            .as_ref()
            .map_or(self.records.len(), |id| {
                self.position_of(id).expect("could not find right origin")
            });
        let mut scanning = false;

        while scan_idx < right {
            let other = match &self.records[scan_idx] {
                Record::Item(item) => item,
                Record::StableRange { .. } => break,
            };

            if other.prepare.is_integrated() {
                break;
            }

            let other_left = other.origin_left.as_ref().map_or(-1, |id| {
                self.position_of(id).expect("could not find left origin") as isize
            });
            let other_right = other
                .origin_right
                .as_ref()
                .map_or(self.records.len(), |id| {
                    self.position_of(id).expect("could not find right origin")
                });

            if other_left < left
                || (other_left == left && other_right == right && new_item.id < other.id)
            {
                break;
            }
            if other_left == left {
                scanning = other_right < right;
            }
            scan_idx += 1;
            if !scanning {
                idx = scan_idx;
            }
        }

        self.records.insert(idx, Record::Item(new_item));
        self.rebuild_index();
    }
}

impl<'a, V> Document<'a, V>
where
    V: Clone,
{
    /// Find a prepared position. Boundaries stop before following invisible
    /// records; item lookup skips them. That distinction preserves insert anchors.
    fn locate(&mut self, mut pos: usize, kind: PositionKind) -> Option<usize> {
        for idx in 0..self.records.len() {
            let len = self.records[idx].prepared_len();
            if pos < len || (matches!(kind, PositionKind::Boundary) && pos == len) {
                return Some(self.split_stable_range(idx, pos, kind));
            }
            pos -= len;
        }
        (matches!(kind, PositionKind::Boundary) && pos == 0).then_some(self.records.len())
    }

    /// Find and materialize a visible item in the prepared version.
    pub fn visible_item_at(&mut self, pos: usize) -> Option<&mut Item<V>> {
        let idx = self.locate(pos, PositionKind::Item)?;
        self.item_mut(idx)
    }

    /// Insert an event-backed item at a visible position.
    pub fn insert_event(&mut self, event_id: EventId, content: V, target_pos: usize) {
        let idx = self
            .locate(target_pos, PositionKind::Boundary)
            .expect("insert position must be within the prepared document");

        debug_assert!(
            idx == 0 || self.records[idx - 1].is_integrated(),
            "item to the left is not integrated"
        );

        let item = Item::new_event(
            event_id,
            self.previous_integrated_id(idx),
            self.next_integrated_id(idx),
            content,
        );
        self.integrate(item, idx);
    }

    /// Split only as much stable content as needed: a boundary creates two
    /// ranges, while an item creates up to two ranges around a concrete record.
    fn split_stable_range(&mut self, idx: usize, offset: usize, kind: PositionKind) -> usize {
        let (start, end) = match self.records[idx] {
            Record::StableRange { start, end } => (start, end),
            Record::Item(_) => return idx + offset,
        };
        let stable_idx = start + offset;
        if matches!(kind, PositionKind::Boundary) && (offset == 0 || stable_idx == end) {
            return idx + usize::from(offset > 0);
        }

        let mut replacement = Vec::with_capacity(3);
        if start < stable_idx {
            replacement.push(Record::StableRange {
                start,
                end: stable_idx,
            });
        }
        let result_idx = idx + replacement.len();
        let mut right_start = stable_idx;
        if matches!(kind, PositionKind::Item) {
            replacement.push(Record::Item(Item::new_stable(
                stable_idx,
                self.stable[stable_idx].clone(),
            )));
            right_start += 1;
        }
        if right_start < end {
            replacement.push(Record::StableRange {
                start: right_start,
                end,
            });
        }
        self.records.splice(idx..=idx, replacement);
        self.rebuild_index();
        result_idx
    }

    /// Convert the replay document into the user-visible list value.
    ///
    /// Untouched stable ranges are copied directly. Concrete items are included
    /// only when their effect state still has at least one live dot.
    pub fn materialize(&self) -> Vec<V> {
        let mut value = Vec::new();
        for record in &self.records {
            match record {
                Record::StableRange { start, end } => {
                    value.extend_from_slice(&self.stable[*start..*end]);
                }
                Record::Item(item) => {
                    if item.effect.is_visible() {
                        value.push(item.content.clone());
                    }
                }
            }
        }
        value
    }

    pub fn visible_len(&self) -> usize {
        self.records
            .iter()
            .map(|record| match record {
                Record::StableRange { start, end } => end - start,
                Record::Item(item) => usize::from(item.effect.is_visible()),
            })
            .sum()
    }
}
