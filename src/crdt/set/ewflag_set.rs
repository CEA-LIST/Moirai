use std::{fmt::Debug, hash::Hash};

use crate::{
    crdt::{
        flag::ew_flag::EWFlag,
        map::uw_map::{UWMap, UWMapLog},
    },
    protocol::state::po_log::VecLog,
    HashMap, HashSet,
};

pub type EWFlagSet<T> = UWMapLog<T, VecLog<EWFlag>>;
pub struct Map<T>(UWMap<T, EWFlag>);

impl<T> Map<T>
where
    T: Clone + Hash + Debug + Eq,
{
    pub fn add(key: T) -> UWMap<T, EWFlag> {
        UWMap::Update(key, EWFlag::Enable)
    }

    pub fn remove(key: T) -> UWMap<T, EWFlag> {
        UWMap::Update(key, EWFlag::Disable)
    }
}

pub struct MapVal<T>(HashMap<T, bool>);

impl<T> MapVal<T>
where
    T: Clone + Hash + Debug + Eq,
{
    pub fn to_set(&self) -> HashSet<T> {
        self.0
            .iter()
            .filter_map(|(k, v)| if *v { Some(k.clone()) } else { None })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crdt::test_util::twins_log,
        protocol::{crdt::query::Read, replica::IsReplica},
    };

    #[test]
    fn test_ewflag_set() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSet<&str>>();
        let event_a = replica_a.send(Map::<&str>::add("a")).unwrap();
        let event_b = replica_b.send(Map::<&str>::add("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(Map::<&str>::remove("a")).unwrap();
        let event_b = replica_b.send(Map::<&str>::add("c")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        MapVal(replica_a.query(Read::new())).to_set();
        MapVal(replica_b.query(Read::new())).to_set();
    }
}
