use std::collections::BTreeSet;

use moirai_protocol::event::id::EventId;

use crate::list::eg_walker::presence_state::PresenceState;

#[derive(Clone, Debug)]
pub struct Item<V> {
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

impl<V> Item<V> {
    pub fn new(
        id: EventId,
        origin_left: Option<EventId>,
        origin_right: Option<EventId>,
        content: V,
    ) -> Self {
        Self {
            id: id.clone(),
            origin_left,
            origin_right,
            content,
            effect_live_dots: BTreeSet::from([id.clone()]),
            presence: PresenceState::new(&id),
        }
    }
}
