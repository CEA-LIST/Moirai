use std::collections::BTreeSet;

use moirai_protocol::event::id::EventId;

use crate::list::eg_walker::presence_state::PreparePresence;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ItemKey {
    /// Character that belongs to the stable baseline materialized outside the event graph.
    Stable(usize),
    /// Character or life dot introduced by an unstable event.
    Event(EventId),
}

/// Identity of the list item used for anchoring and lookup.
///
/// This identifies a physical record in the EgWalker sequence. It is used for
/// insertion origins, update/delete targets, and the index map. It is deliberately
/// distinct from `LifeDot`: an item keeps the same identity while updates may add
/// more life dots to its visibility state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ItemId(ItemKey);

impl ItemId {
    pub fn stable(index: usize) -> Self {
        Self(ItemKey::Stable(index))
    }

    pub fn event(event_id: EventId) -> Self {
        Self(ItemKey::Event(event_id))
    }

    pub fn stable_index(&self) -> Option<usize> {
        match self {
            Self(ItemKey::Stable(index)) => Some(*index),
            Self(ItemKey::Event(_)) => None,
        }
    }
}

/// Add/update dot used by the prepare and effect visibility states.
///
/// A dot represents one reason for an item to be alive. Inserts create the first
/// dot for a new item; updates create additional dots for the same item. Deletes
/// remove only the dots that were visible in the delete operation's parent
/// context, which gives the list its update/add-wins behavior.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LifeDot(ItemKey);

impl LifeDot {
    pub fn stable(index: usize) -> Self {
        Self(ItemKey::Stable(index))
    }

    pub fn event(event_id: EventId) -> Self {
        Self(ItemKey::Event(event_id))
    }
}

#[derive(Clone, Debug, Default)]
pub struct EffectPresence {
    /// Life dots that remain visible in the final replayed version.
    ///
    /// Unlike `PreparePresence`, this state is not retreated or advanced while
    /// traversing the event graph. It accumulates the transformed effect that will
    /// be materialized as the read result.
    pub live_dots: BTreeSet<LifeDot>,
}

impl EffectPresence {
    pub fn new(dot: LifeDot) -> Self {
        Self {
            live_dots: BTreeSet::from([dot]),
        }
    }

    pub fn is_visible(&self) -> bool {
        !self.live_dots.is_empty()
    }

    pub fn add_life_dot(&mut self, dot: LifeDot) {
        self.live_dots.insert(dot);
    }

    pub fn remove_life_dot(&mut self, dot: &LifeDot) {
        self.live_dots.remove(dot);
    }
}

#[derive(Clone, Debug)]
pub struct Item<V> {
    /// Stable identity of the physical item record.
    pub id: ItemId,
    /// Integrated item immediately to the left when this insert was generated.
    pub origin_left: Option<ItemId>,
    /// Integrated item immediately to the right when this insert was generated.
    pub origin_right: Option<ItemId>,
    /// The user payload stored in this list cell.
    pub content: V,
    /// Final replayed visibility state. This is what materializes the value returned to the user.
    pub effect: EffectPresence,
    /// Prepared-parent visibility state used while Eg-Walker jumps across the event graph.
    pub prepare: PreparePresence,
}

impl<V> Item<V> {
    /// Create an item introduced by an insert event in the unstable log.
    pub fn new_event(
        id: EventId,
        origin_left: Option<ItemId>,
        origin_right: Option<ItemId>,
        content: V,
    ) -> Self {
        let item_id = ItemId::event(id.clone());
        let dot = LifeDot::event(id);
        Self {
            id: item_id,
            origin_left,
            origin_right,
            content,
            effect: EffectPresence::new(dot.clone()),
            prepare: PreparePresence::new(dot),
        }
    }

    /// Materialize one element of the stable baseline as a normal item record.
    ///
    /// Stable elements usually stay compressed in a `StableRange`. We only create
    /// an `Item` for one of them when a later unstable operation must address it
    /// directly, for example deleting or updating a stable character.
    pub fn new_stable(index: usize, content: V) -> Self {
        let item_id = ItemId::stable(index);
        let dot = LifeDot::stable(index);
        Self {
            id: item_id,
            origin_left: (index > 0).then_some(ItemId::stable(index - 1)),
            origin_right: None,
            content,
            effect: EffectPresence::new(dot.clone()),
            prepare: PreparePresence::new(dot),
        }
    }
}
