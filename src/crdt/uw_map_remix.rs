use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    rc::Rc,
};

use radix_trie::TrieCommon;

use crate::protocol::{event::Event, metadata::Metadata, po_log::POLog, pure_crdt::PureCRDT};

#[derive(Clone, Debug)]
pub enum UWMap<O>
where
    O: PureCRDT + Debug,
{
    Update(&'static str, O),
    Remove(&'static str),
}

impl<O> PureCRDT for UWMap<O>
where
    O: PureCRDT + Debug,
{
    type Value = HashMap<String, O::Value>;

    fn r(event: &Event<Self>, state: &POLog<Self>) -> bool {
        match event.op {
            UWMap::Remove(key) => state
                .unstable
                .iter()
                .any(|(metadata, op)| match op.as_ref() {
                    UWMap::Update(k, _) => *k == key && !(metadata.vc < event.metadata.vc),
                    _ => false,
                }),
            _ => false,
        }
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        match (&old_event.op, &new_event.op) {
            (UWMap::Remove(key2), UWMap::Update(key1, _)) => {
                key1 == key2
                    && match PartialOrd::partial_cmp(&old_event.metadata.vc, &new_event.metadata.vc)
                    {
                        Some(Ordering::Less) => true,
                        Some(Ordering::Less) => false,
                        None => true,
                        _ => false,
                    }
            }
            (UWMap::Update(..), UWMap::Update(..)) | (UWMap::Update(..), UWMap::Remove(..)) => {
                false
            }
            (UWMap::Remove(key1), UWMap::Remove(key2)) => {
                key1 == key2 && old_event.metadata.vc < new_event.metadata.vc
            }
        }
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(metadata: &Metadata, state: &mut POLog<Self>) {
        let op = state.unstable.get(metadata).unwrap();
        if let UWMap::Remove(key) = op.as_ref() {
            state.stable.retain(|o| match o.as_ref() {
                UWMap::Update(k, _) => k != key,
                _ => true,
            });
        }
    }

    fn eval(state: &POLog<Self>, path: &Path) -> Self::Value {
        let mut map = Self::Value::new();
        let ops_by_path = state.path_trie.subtrie(path);
        if ops_by_path.is_none() {
            return map;
        }
        let mut logs_by_path: HashMap<&str, POLog<O>> = HashMap::new();
        for (p, weak_ops) in ops_by_path.unwrap().iter() {
            // let key = p.parent().unwrap_or(Path::new(""));
            // let key = if key == PathBuf::from("") { p } else { key };
            for weak_op in weak_ops {
                if let Some(rc_op) = weak_op.upgrade() {
                    match rc_op.as_ref() {
                        UWMap::Update(k, v) => {
                            // assert_eq!(k, &key.to_str().unwrap_or(""));
                            let log = logs_by_path.entry(k).or_default();
                            log.new_stable(Rc::new(v.clone()));
                        }
                        UWMap::Remove(k) => {
                            // assert_eq!(k, &key.to_str().unwrap_or(""));
                            logs_by_path.remove(k);
                            break;
                        }
                    }
                }
            }
        }
        for (p, log) in &logs_by_path {
            map.insert(String::from(*p), O::eval(&log, &PathBuf::from(p)));
        }
        map
    }

    fn to_path(op: &Self) -> PathBuf {
        match op {
            UWMap::Update(k, v) => PathBuf::from(k).join(O::to_path(v)),
            UWMap::Remove(k) => PathBuf::from(k),
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

        let event = tcsb_a.tc_bcast(UWMap::Update("a", Counter::Dec(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b", Counter::Inc(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("a", Counter::Inc(15)));
        tcsb_b.tc_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), 10);
        map.insert(String::from("b"), 5);
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMap<Duet<Counter<i32>, Counter<i32>>>>();

        let event = tcsb_a.tc_bcast(UWMap::Update("a", Duet::First(Counter::Inc(15))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b", Duet::First(Counter::Inc(5))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("a", Duet::First(Counter::Inc(10))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b", Duet::Second(Counter::Dec(7))));
        tcsb_b.tc_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (25, 0));
        map.insert(String::from("b"), (5, -7));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    #[test_log::test]
    fn aw_map_concurrent_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<UWMap<Duet<Counter<i32>, Counter<i32>>>>();

        let event = tcsb_a.tc_bcast(UWMap::Update("a", Duet::First(Counter::Inc(15))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Update("b", Duet::First(Counter::Inc(5))));
        tcsb_b.tc_deliver(event);

        let event_a = tcsb_a.tc_bcast(UWMap::Update("a", Duet::First(Counter::Inc(10))));
        let event_b = tcsb_b.tc_bcast(UWMap::Remove("a"));
        tcsb_b.tc_deliver(event_a);
        tcsb_a.tc_deliver(event_b);

        let event = tcsb_a.tc_bcast(UWMap::Update("b", Duet::Second(Counter::Dec(7))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(UWMap::Remove("b"));
        tcsb_b.tc_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (25, 0));
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }
}
