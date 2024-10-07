use std::{collections::HashMap, fmt::Debug, sync::Arc};

use camino::{Utf8Path, Utf8PathBuf};
use radix_trie::TrieCommon;

use crate::protocol::{
    event::Event, metadata::Metadata, pathbuf_key::PathBufKey, po_log::POLog, pure_crdt::PureCRDT,
};

#[derive(Clone, Debug)]
pub enum UWMap<O>
where
    O: PureCRDT + Debug,
{
    Update(String, O),
    Remove(String),
}

impl<O> PureCRDT for UWMap<O>
where
    O: PureCRDT + Debug,
{
    type Value = HashMap<String, O::Value>;

    fn r(event: &Event<Self>, _state: &POLog<Self>) -> bool {
        matches!(event.op, UWMap::Remove(_))
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        old_event.metadata.clock < new_event.metadata.clock
            && match (&old_event.op, &new_event.op) {
                (UWMap::Update(_, _), UWMap::Update(_, _)) => false,
                (UWMap::Remove(key1), UWMap::Remove(key2))
                | (UWMap::Update(key1, _), UWMap::Remove(key2))
                | (UWMap::Remove(key1), UWMap::Update(key2, _)) => key1 == key2,
            }
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>, path: &Utf8Path) -> Self::Value {
        let mut map = Self::Value::new();
        let ops_by_path = state.path_trie.subtrie(&PathBufKey::new(path));
        if ops_by_path.is_none() {
            return map;
        }
        let mut logs_by_path: HashMap<String, POLog<O>> = HashMap::new();
        for weak_ops in ops_by_path.unwrap().values() {
            let mut key: Option<String> = None;
            for weak_op in weak_ops {
                if let Some(rc_op) = weak_op.upgrade() {
                    if let UWMap::Update(k, v) = rc_op.as_ref() {
                        let log = logs_by_path.entry(k.to_string()).or_default();
                        log.new_stable(Arc::new(v.clone()));
                        key = Some(k.to_string());
                    }
                }
            }
            if let Some(k) = key {
                let log = logs_by_path.get_mut(&k).unwrap();
                map.insert(k.clone(), O::eval(log, Utf8Path::new(&k)));
            }
        }
        map
    }

    fn to_path(op: &Self) -> Utf8PathBuf {
        match op {
            UWMap::Update(k, v) => Utf8PathBuf::from(k).join(O::to_path(v)),
            UWMap::Remove(k) => Utf8PathBuf::from(k),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::crdt::{counter::Counter, duet::Duet, test_util::twins, uw_map::UWMap};

    #[test_log::test]
    fn simple_aw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMap<Counter<i32>>>();

        let event = tcsb_a.tc_bcast_op(UWMap::Update("a".to_string(), Counter::Dec(5)));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(UWMap::Update("b".to_string(), Counter::Inc(5)));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(UWMap::Update("a".to_string(), Counter::Inc(15)));
        tcsb_b.tc_deliver_op(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), 10);
        map.insert(String::from("b"), 5);
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMap<Duet<Counter<i32>, Counter<i32>>>>();

        let event = tcsb_a.tc_bcast_op(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.tc_deliver_op(event);

        let event =
            tcsb_a.tc_bcast_op(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(UWMap::Update(
            "b".to_string(),
            Duet::Second(Counter::Dec(7)),
        ));
        tcsb_b.tc_deliver_op(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (25, 0));
        map.insert(String::from("b"), (5, -7));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_concurrent_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMap<Duet<Counter<i32>, Counter<i32>>>>();

        let event = tcsb_a.tc_bcast_op(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(15)),
        ));
        tcsb_b.tc_deliver_op(event);

        let event =
            tcsb_a.tc_bcast_op(UWMap::Update("b".to_string(), Duet::First(Counter::Inc(5))));
        tcsb_b.tc_deliver_op(event);

        let event_a = tcsb_a.tc_bcast_op(UWMap::Update(
            "a".to_string(),
            Duet::First(Counter::Inc(10)),
        ));
        let event_b = tcsb_b.tc_bcast_op(UWMap::Remove("a".to_string()));
        tcsb_b.tc_deliver_op(event_a);
        tcsb_a.tc_deliver_op(event_b);

        let event = tcsb_a.tc_bcast_op(UWMap::Update(
            "b".to_string(),
            Duet::Second(Counter::Dec(7)),
        ));
        tcsb_b.tc_deliver_op(event);

        let event = tcsb_a.tc_bcast_op(UWMap::Remove("b".to_string()));
        tcsb_b.tc_deliver_op(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (10, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }
}
