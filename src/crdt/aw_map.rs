use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    rc::Rc,
};

use radix_trie::TrieCommon;

use crate::protocol::{event::Event, metadata::Metadata, po_log::POLog, pure_crdt::PureCRDT};

#[derive(Clone, Debug)]
pub enum AWMap<O>
where
    O: PureCRDT,
{
    Insert(&'static str, O),
    Remove(&'static str),
}

impl<O> PureCRDT for AWMap<O>
where
    O: PureCRDT,
{
    type Value = HashMap<&'static str, O::Value>;

    fn r(_event: &Event<Self>, _state: &POLog<Self>) -> bool {
        false
    }

    fn r_zero(_old_event: &Event<Self>, _new_event: &Event<Self>) -> bool {
        false
    }

    fn r_one(_old_event: &Event<Self>, _new_event: &Event<Self>) -> bool {
        false
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>, path: &Path) -> Self::Value {
        let mut map = Self::Value::new();
        let ops_by_path = state.path_trie.subtrie(path);
        if ops_by_path.is_none() {
            return map;
        }
        for (k, v) in ops_by_path.unwrap().iter() {
            let mut new_log: POLog<O> = POLog::new();
            let mut key: &str = "";
            for weak_op in v {
                if let Some(rc_op) = weak_op.upgrade() {
                    if let AWMap::Insert(k, v) = rc_op.as_ref() {
                        key = k;
                        new_log.new_stable(Rc::new(v.clone()));
                    }
                }
            }
            if !new_log.is_empty() {
                map.insert(key, O::eval(&new_log, k));
            }
        }
        map
    }

    fn to_path(op: &Self) -> PathBuf {
        match op {
            AWMap::Insert(k, v) => PathBuf::from(k).join(O::to_path(v)),
            AWMap::Remove(k) => PathBuf::from(k),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{aw_map::AWMap, counter::Counter, test_util::twins};

    #[test_log::test]
    fn simple_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWMap<Counter<i32>>>();

        let event = tcsb_a.tc_bcast(AWMap::Insert("first", Counter::Dec(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("second", Counter::Inc(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("first", Counter::Inc(15)));
        tcsb_b.tc_deliver(event);

        println!("{:?}", tcsb_a.eval());
    }
}
