use std::{
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    rc::Rc,
};

use radix_trie::TrieCommon;

use crate::protocol::{event::Event, metadata::Metadata, po_log::POLog, pure_crdt::PureCRDT};

#[derive(Clone, Debug)]
pub enum AWMap<O>
where
    O: PureCRDT + Debug,
{
    Insert(&'static str, O),
    Remove(&'static str),
}

impl<O> PureCRDT for AWMap<O>
where
    O: PureCRDT + Debug,
{
    type Value = HashMap<String, O::Value>;

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
        let mut sub_logs: HashMap<&Path, POLog<O>> = HashMap::new();
        for (k, v) in ops_by_path.unwrap().iter() {
            let key = k.parent().unwrap_or(Path::new(""));
            let log = sub_logs.entry(key).or_default();
            for weak_op in v {
                if let Some(rc_op) = weak_op.upgrade() {
                    if let AWMap::Insert(_, v) = rc_op.as_ref() {
                        log.new_stable(Rc::new(v.clone()));
                    }
                }
            }
        }
        for (p, log) in sub_logs {
            map.insert(String::from(p.to_str().unwrap_or("")), O::eval(&log, p));
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
    use std::collections::HashMap;

    use crate::crdt::{aw_map::AWMap, counter::Counter, duet::Duet, test_util::twins};

    #[test_log::test]
    fn simple_aw_map() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWMap<Counter<i32>>>();

        let event = tcsb_a.tc_bcast(AWMap::Insert("a", Counter::Dec(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("b", Counter::Inc(5)));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("a", Counter::Inc(15)));
        tcsb_b.tc_deliver(event);

        println!("{:?}", tcsb_a.eval());
    }

    #[test_log::test]
    fn aw_map_duet_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWMap<Duet<Counter<i32>, Counter<i32>>>>();

        let event = tcsb_a.tc_bcast(AWMap::Insert("a", Duet::First(Counter::Inc(15))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("b", Duet::First(Counter::Inc(5))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("a", Duet::First(Counter::Inc(10))));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(AWMap::Insert("b", Duet::Second(Counter::Dec(7))));
        tcsb_b.tc_deliver(event);

        let mut map = HashMap::new();
        map.insert(String::from("a"), (25, 0));
        map.insert(String::from("b"), (5, -7));
        println!("{:?}", tcsb_a.eval());
        assert_eq!(map, tcsb_a.eval());
        assert_eq!(map, tcsb_b.eval());
    }

    // #[derive(Debug)]
    // pub enum Bar {
    //     Patate,
    //     Test,
    // }

    // #[derive(Debug)]
    // pub enum Foo {
    //     Boz(Bar),
    //     Giz(Bar),
    // }

    // #[test_log::test]
    // fn patate() {
    //     let test = Foo::Boz(Bar::Patate);
    //     let bar = match test {
    //         Foo::Boz(ref v) => v,
    //         Foo::Giz(ref v) => v,
    //     };
    //     println!("{:?}", bar);
    //     println!("{:?}", test);
    // }
}
