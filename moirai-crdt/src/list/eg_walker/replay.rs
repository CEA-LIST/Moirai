use std::fmt::Debug;

use moirai_protocol::{
    event::{id::EventId, tagged_op::TaggedOp},
    state::unstable_state::CausalReplay,
};

use super::{
    List,
    causal_cursor::transition,
    document::Document,
    item::{ItemId, LifeDot},
};
use crate::HashMap;

/// Targets are resolved once in the operation's parent version, then reused
/// whenever replay crosses that event again.
enum AppliedEffect {
    Insert(ItemId),
    Update(ItemId),
    Delete(Vec<DeleteEffect>),
}

struct DeleteEffect {
    item_id: ItemId,
    removed_dots: Vec<LifeDot>,
}

#[derive(Clone, Copy)]
enum Direction {
    Advance,
    Retreat,
}

impl AppliedEffect {
    /// Apply to both views after preparing the operation's parent version.
    fn apply<V: Clone>(doc: &mut Document<V>, event: &TaggedOp<List<V>>) -> Self {
        match event.op() {
            List::Insert { content, pos } => {
                doc.insert_event(event.id().clone(), content.clone(), *pos);
                Self::Insert(ItemId::event(event.id().clone()))
            }
            List::Update { pos } => {
                let item = doc
                    .visible_item_at(*pos)
                    .expect("Eg-Walker invariant: update target must exist");
                item.apply_update(LifeDot::event(event.id().clone()));
                Self::Update(item.id.clone())
            }
            List::Delete { pos } => Self::delete(doc, *pos, 1),
            List::DeleteRange { start, len } => Self::delete(doc, *start, *len),
        }
    }

    fn delete<V: Clone>(doc: &mut Document<V>, start: usize, len: usize) -> Self {
        let end = start
            .checked_add(len)
            .expect("Eg-Walker invariant: delete range overflow");
        assert!(
            end <= doc.prepared_len(),
            "Eg-Walker invariant: delete range exceeds prepared document"
        );
        // Resolve the entire range before changing visibility, so every target
        // uses the same parent version and no partial deletion can be journaled.
        let targets: Vec<_> = (start..end)
            .map(|pos| {
                let item = doc
                    .visible_item_at(pos)
                    .expect("Eg-Walker invariant: delete target must exist");
                DeleteEffect {
                    item_id: item.id.clone(),
                    removed_dots: item.prepare.visible_life_dots().cloned().collect(),
                }
            })
            .collect();
        for target in &targets {
            doc.item_by_id_mut(&target.item_id)
                .apply_delete(&target.removed_dots);
        }
        Self::Delete(targets)
    }

    /// Advance and retreat change only prepare state; the accumulated effect
    /// view is never undone. Concurrent deletes are reversed one count at a time.
    fn move_prepare<V>(&self, doc: &mut Document<V>, id: &EventId, direction: Direction) {
        match self {
            Self::Insert(item_id) | Self::Update(item_id) => {
                let prepare = &mut doc.item_by_id_mut(item_id).prepare;
                if matches!(self, Self::Insert(_)) {
                    prepare.inserted = matches!(direction, Direction::Advance);
                }
                let dot = LifeDot::event(id.clone());
                match direction {
                    Direction::Advance => prepare.add_life_dot(dot),
                    Direction::Retreat => prepare.remove_life_dot(&dot),
                }
            }
            Self::Delete(targets) => {
                for target in targets {
                    let prepare = &mut doc.item_by_id_mut(&target.item_id).prepare;
                    for dot in &target.removed_dots {
                        match direction {
                            Direction::Advance => prepare.record_delete(dot),
                            Direction::Retreat => prepare.undo_delete(dot),
                        }
                    }
                }
            }
        }
    }
}

/// Replay a causally closed stream in topological order. Cursor and journal are
/// local to this replay; Document owns only the sequence and its visibility.
pub(super) fn replay_document<'a, V, U, I>(
    stable: &'a [V],
    unstable: &'a U,
    events: I,
) -> Document<'a, V>
where
    V: Clone + Debug + 'a,
    U: CausalReplay<List<V>> + 'a,
    I: IntoIterator<Item = &'a TaggedOp<List<V>>>,
{
    let mut document = Document::new(stable);
    let mut prepared_head = None;
    #[allow(clippy::mutable_key_type)]
    let mut effects = HashMap::<EventId, AppliedEffect>::default();

    for event in events {
        let parents = unstable.direct_predecessors(event.id());
        let transition = transition(unstable, prepared_head, &parents);
        // The cursor returns newest first. Undo children before parents, and
        // redo parents before children.
        for id in transition.retreat {
            effects
                .get(&id)
                .unwrap_or_else(|| panic!("Eg-Walker invariant: missing applied effect for {id}"))
                .move_prepare(&mut document, &id, Direction::Retreat);
        }
        for id in transition.advance.into_iter().rev() {
            effects
                .get(&id)
                .unwrap_or_else(|| panic!("Eg-Walker invariant: missing applied effect for {id}"))
                .move_prepare(&mut document, &id, Direction::Advance);
        }
        let effect = AppliedEffect::apply(&mut document, event);
        effects.insert(event.id().clone(), effect);
        prepared_head = Some(event.id());
    }
    document
}
