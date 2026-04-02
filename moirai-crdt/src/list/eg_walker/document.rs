use std::fmt::{Display, Formatter};

use moirai_protocol::event::id::EventId;

use crate::{
    HashMap,
    list::eg_walker::{DeleteTarget, Item},
};

#[derive(Debug)]
pub struct Document<V> {
    pub items: Vec<Item<V>>,
    /// Last processed event
    pub current_version: Option<EventId>,
    /// Key = update op id, Value = target insert op id
    pub update_targets: HashMap<EventId, EventId>,
    /// Key = delete op id, Value = target insert op id
    pub delete_targets: HashMap<EventId, DeleteTarget>,
    /// map of the event id to the current value position in vector of items
    pub items_by_idx: HashMap<EventId, usize>,
}

impl<V> Default for Document<V> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            current_version: None,
            update_targets: HashMap::default(),
            delete_targets: HashMap::default(),
            items_by_idx: HashMap::default(),
        }
    }
}

impl<V> Display for Document<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
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
            write!(f, " | EffectDots: {:?}", item.effect_live_dots)?;
            write!(f, " | Inserted: {}", item.presence.inserted)?;
            write!(f, " | BornDots: {:?}", item.presence.born_dots)?;
            write!(f, " | DeletedDots: {:?}", item.presence.deleted_dots)?;
            writeln!(f, " ]")?;
        }
        writeln!(
            f,
            "update targets: {}",
            self.update_targets
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
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
