//! The add-wins set of strings, as the generated path's awset_node hosts it.

use std::collections::HashSet;

use moirai_crdt::set::aw_set::AWSet;
use moirai_protocol::{data_type::query::Read, replica::IsReplica, state::po_log::VecLog};
use serde_json::json;

use crate::Factory;

pub type Log = VecLog<AWSet<String>>;

/// Ops are `{"Add": s}`, `{"Remove": s}` and `"Clear"`; the state reads as a
/// sorted JSON array of strings.
pub fn factory() -> Factory<Log> {
    Factory {
        read: |replica| {
            let mut set: Vec<String> = replica
                .query(&Read::<HashSet<String, _>>::new())
                .into_iter()
                .collect();
            set.sort();
            json!(set)
        },
    }
}
