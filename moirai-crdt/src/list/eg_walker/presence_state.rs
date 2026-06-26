use std::collections::{BTreeMap, BTreeSet};

use crate::list::eg_walker::item::LifeDot;

#[derive(Clone, Debug, Default)]
pub struct PreparePresence {
    /// Whether the insertion event is present in the currently prepared version.
    ///
    /// Retreating an insert turns this off; advancing it turns it back on. Updates
    /// and deletes are represented by the dot sets below.
    pub inserted: bool,
    /// Add/update dots currently present in the prepared causal context.
    ///
    /// The prepared context is the document version in which the operation being
    /// applied must be interpreted. EgWalker mutates this set when it retreats
    /// from the previous event's parents and advances to the next event's parents.
    pub life_dots: BTreeSet<LifeDot>,
    /// Deletes are counted per dot. Concurrent deletes may target the same dot, and
    /// retreat/advance must undo them one occurrence at a time.
    pub delete_counts: BTreeMap<LifeDot, u32>,
}

impl PreparePresence {
    pub fn new(dot: LifeDot) -> Self {
        Self {
            inserted: true,
            life_dots: BTreeSet::from([dot]),
            delete_counts: BTreeMap::new(),
        }
    }

    pub fn is_integrated(&self) -> bool {
        self.inserted
    }

    /// Is the item visible in the currently prepared version?
    ///
    /// An item is visible if its insertion has not been retreated and at least one
    /// prepared life dot is not cancelled by a prepared delete.
    pub fn is_visible(&self) -> bool {
        self.inserted
            && self
                .life_dots
                .iter()
                .any(|dot| self.delete_counts.get(dot).copied().unwrap_or(0) == 0)
    }

    #[allow(clippy::mutable_key_type)]
    pub fn visible_life_dots(&self) -> BTreeSet<LifeDot> {
        // Update-wins semantics is expressed here: a delete can only remove dots
        // visible in its own parent context. A concurrent update dot is not visible
        // to that delete, so it survives in the effect state.
        self.life_dots
            .iter()
            .filter(|dot| self.delete_counts.get(*dot).copied().unwrap_or(0) == 0)
            .cloned()
            .collect()
    }

    pub fn add_life_dot(&mut self, dot: LifeDot) {
        self.life_dots.insert(dot);
    }

    pub fn remove_life_dot(&mut self, dot: &LifeDot) {
        self.life_dots.remove(dot);
    }

    pub fn record_delete(&mut self, dot: &LifeDot) {
        *self.delete_counts.entry(dot.clone()).or_insert(0) += 1;
    }

    pub fn undo_delete(&mut self, dot: &LifeDot) {
        match self.delete_counts.get_mut(dot) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.delete_counts.remove(dot);
            }
            None => {}
        }
    }
}
