use std::fmt::{Display, Formatter};

use moirai_protocol::event::id::EventId;

use crate::{
    HashMap,
    list::eg_walker::{
        DeleteTarget,
        item::{Item, ItemId},
    },
};

#[derive(Debug)]
pub enum Record<V> {
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
    pub stable: &'a [V],
    /// Replay sequence mixing compressed stable ranges and concrete item records.
    pub records: Vec<Record<V>>,
    /// Last processed event in the topological replay.
    pub current_version: Option<EventId>,
    /// Key = update op id, value = item updated by that op.
    pub update_targets: HashMap<EventId, ItemId>,
    /// Key = delete op id, value = item/dots removed by that op.
    pub delete_targets: HashMap<EventId, DeleteTarget>,
    /// Concrete item identity to record index.
    ///
    /// Stable items that are still inside a `StableRange` are located by scanning
    /// ranges in `position_of`; only materialized `Item` records live in this map.
    pub items_by_idx: HashMap<ItemId, usize>,
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
            current_version: None,
            update_targets: HashMap::default(),
            delete_targets: HashMap::default(),
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
    pub fn rebuild_index(&mut self) {
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
    pub fn position_of(&self, id: &ItemId) -> Option<usize> {
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
    pub fn item_mut(&mut self, idx: usize) -> Option<&mut Item<V>> {
        match self.records.get_mut(idx) {
            Some(Record::Item(item)) => Some(item),
            _ => None,
        }
    }

    /// Split a stable range at a visible-position boundary.
    ///
    /// This is enough for insertion: inserting at a position inside stable content
    /// only needs a boundary between two ranges, not a materialized stable item.
    pub fn split_stable_range_at_boundary(&mut self, idx: usize, offset: usize) -> usize {
        let (start, end) = match self.records[idx] {
            Record::StableRange { start, end } => (start, end),
            Record::Item(_) => return idx,
        };

        let len = end - start;
        debug_assert!(offset <= len);

        if offset == 0 {
            return idx;
        }
        if offset == len {
            return idx + 1;
        }

        self.records.splice(
            idx..=idx,
            [
                Record::StableRange {
                    start,
                    end: start + offset,
                },
                Record::StableRange {
                    start: start + offset,
                    end,
                },
            ],
        );
        self.rebuild_index();
        idx + 1
    }
}

impl<'a, V> Document<'a, V>
where
    V: Clone,
{
    /// Replace one stable element inside a range by a concrete `Item`.
    ///
    /// Deletes and updates need item-level prepare/effect state, so a stable
    /// element is isolated lazily the first time an unstable op targets it.
    pub fn isolate_stable_item(&mut self, idx: usize, offset: usize) -> usize {
        let (start, end) = match self.records[idx] {
            Record::StableRange { start, end } => (start, end),
            Record::Item(_) => return idx,
        };

        let stable_idx = start + offset;
        debug_assert!(stable_idx < end);

        let mut replacement = Vec::new();
        if start < stable_idx {
            replacement.push(Record::StableRange {
                start,
                end: stable_idx,
            });
        }
        replacement.push(Record::Item(Item::new_stable(
            stable_idx,
            self.stable[stable_idx].clone(),
        )));
        let item_idx = idx + usize::from(start < stable_idx);
        if stable_idx + 1 < end {
            replacement.push(Record::StableRange {
                start: stable_idx + 1,
                end,
            });
        }

        self.records.splice(idx..=idx, replacement);
        self.rebuild_index();
        item_idx
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
                Record::Item(item) if item.effect.is_visible() => {
                    value.push(item.content.clone());
                }
                Record::Item(_) => {}
            }
        }
        value
    }
}

impl<V> Display for Document<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(
            f,
            "items by idx: {}",
            self.items_by_idx
                .iter()
                .map(|(k, v)| format!("{:?}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        writeln!(f, "records:")?;
        for record in &self.records {
            match record {
                Record::StableRange { start, end } => {
                    writeln!(f, "    - StableRange [{start}, {end})")?;
                }
                Record::Item(item) => {
                    write!(f, "    - {:?}", item.id)?;
                    write!(f, " [")?;
                    if let Some(ol) = &item.origin_left {
                        write!(f, " L:{:?}", ol)?;
                    } else {
                        write!(f, " L:None")?;
                    }
                    if let Some(or) = &item.origin_right {
                        write!(f, " R:{:?}", or)?;
                    } else {
                        write!(f, " R:None")?;
                    }
                    write!(f, " | EffectLiveDots: {:?}", item.effect.live_dots)?;
                    write!(f, " | PrepareInserted: {}", item.prepare.inserted)?;
                    write!(f, " | PrepareLifeDots: {:?}", item.prepare.life_dots)?;
                    write!(
                        f,
                        " | PrepareDeleteCounts: {:?}",
                        item.prepare.delete_counts
                    )?;
                    writeln!(f, " ]")?;
                }
            }
        }
        writeln!(
            f,
            "update targets: {}",
            self.update_targets
                .iter()
                .map(|(k, v)| format!("{}: {:?}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
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
