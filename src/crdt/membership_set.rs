use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::po_log::POLog;
use crate::protocol::pure_crdt::PureCRDT;
use camino::Utf8Path;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MSet<V> {
    Add(V),
    Remove(V),
}

impl MSet<String> {
    pub fn add(s: &str) -> Self {
        MSet::Add(s.to_string())
    }

    pub fn remove(s: &str) -> Self {
        MSet::Remove(s.to_string())
    }
}

impl<V> PureCRDT for MSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;

    fn r(_: &Event<Self>, _: &POLog<Self>) -> bool {
        false
    }

    fn r_zero(_: &Event<Self>, _: &Event<Self>) -> bool {
        false
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(metadata: &Metadata, state: &mut POLog<Self>) {
        let op = state.unstable.get(metadata).unwrap();

        let is_stable_or_unstable = |v: &V| {
            state.stable.iter().any(|o| match o.as_ref() {
                MSet::Add(v2) | MSet::Remove(v2) => v == v2,
            }) || state.unstable.iter().any(|(t, o)| match o.as_ref() {
                MSet::Add(v2) | MSet::Remove(v2) => v == v2 && metadata.clock != t.clock,
            })
        };

        let to_remove = match op.as_ref() {
            MSet::Add(v) => is_stable_or_unstable(v),
            MSet::Remove(v) => !state
                .stable
                .iter()
                .any(|o| matches!(o.as_ref(), MSet::Add(v2) if v == v2))
                && !state.unstable.iter().any(
                    |(t, o)| matches!(o.as_ref(), MSet::Add(v2) if v == v2 && metadata.clock != t.clock),
                ),
        };

        if let MSet::Add(v) = op.as_ref() {
            if let Some(i) = state
                .stable
                .iter()
                .position(|o| matches!(o.as_ref(), MSet::Remove(v2) if v == v2))
            {
                state.stable.remove(i);
            }
        }

        if to_remove {
            state.unstable.remove(metadata);
        }
    }

    fn eval(state: &POLog<Self>, _: &Utf8Path) -> Self::Value {
        let mut set = Self::Value::new();
        for o in &state.stable {
            if let MSet::Add(v) = o.as_ref() {
                if state.stable.iter().all(|e| {
                    if let MSet::Remove(v2) = e.as_ref() {
                        v != v2
                    } else {
                        true
                    }
                }) {
                    set.insert(v.clone());
                }
            }
        }
        set
    }
}

impl<V> Display for MSet<V>
where
    V: Debug + Display + Clone + Hash + Eq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MSet::Add(v) => write!(f, "Add({})", v),
            MSet::Remove(v) => write!(f, "Remove({})", v),
        }
    }
}
