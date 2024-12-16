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

    fn r(event: &Event<Self>, state: &POLog<Self>) -> bool {
        // let order =
        false
    }

    fn r_zero(_: &Event<Self>, _: &Event<Self>) -> bool {
        false
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        // old_event.metadata.clock < new_event.metadata.clock
        //     && !old_event.metadata.clock.is_empty()
        //     && match (&old_event.op, &new_event.op) {
        //         (MSet::Add(v1), MSet::Add(v2))
        //         | (MSet::Remove(v1), MSet::Remove(v2))
        //         | (MSet::Add(v1), MSet::Remove(v2))
        //         | (MSet::Remove(v1), MSet::Add(v2)) => v1 == v2,
        //     }
        false
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

#[cfg(test)]
mod tests {
    // use std::collections::HashSet;

    // use crate::crdt::{
    //     membership_set::MSet,
    //     test_util::{triplet, twins},
    // };

    // #[test_log::test]
    // fn rw_set_multiple_operations_triplet() {
    //     let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<MSet<&str>>();

    //     let event_a = tcsb_a.tc_bcast_op(MSet::Add("a"));
    //     let event_a_2 = tcsb_a.tc_bcast_op(MSet::Add("b"));
    //     let event_a_3 = tcsb_a.tc_bcast_op(MSet::Add("c"));
    //     let event_a_4 = tcsb_a.tc_bcast_op(MSet::Remove("p"));
    //     tcsb_b.tc_deliver_op(event_a.clone());
    //     tcsb_b.tc_deliver_op(event_a_2.clone());
    //     tcsb_b.tc_deliver_op(event_a_3.clone());
    //     tcsb_b.tc_deliver_op(event_a_4.clone());
    //     tcsb_c.tc_deliver_op(event_a);
    //     tcsb_c.tc_deliver_op(event_a_2);
    //     tcsb_c.tc_deliver_op(event_a_3);
    //     tcsb_c.tc_deliver_op(event_a_4);

    //     let result = HashSet::from([]);
    //     assert_eq!(tcsb_b.eval(), result);
    //     assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    //     assert_eq!(tcsb_a.eval(), tcsb_c.eval());

    //     let event_a = tcsb_a.tc_bcast_op(MSet::Remove("a"));
    //     tcsb_b.tc_deliver_op(event_a.clone());
    //     tcsb_c.tc_deliver_op(event_a);

    //     let result = HashSet::from(["b", "c"]);
    //     assert_eq!(tcsb_b.eval(), result);
    //     assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    //     assert_eq!(tcsb_a.eval(), tcsb_c.eval());

    //     let event_b = tcsb_b.tc_bcast_op(MSet::Remove("a"));
    //     tcsb_a.tc_deliver_op(event_b.clone());
    //     tcsb_c.tc_deliver_op(event_b);

    //     let result = HashSet::from(["c", "b"]);
    //     assert_eq!(tcsb_b.eval(), result);
    //     assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    //     assert_eq!(tcsb_a.eval(), tcsb_c.eval());

    //     let event_a = tcsb_a.tc_bcast_op(MSet::Add("a"));
    //     tcsb_b.tc_deliver_op(event_a.clone());
    //     tcsb_c.tc_deliver_op(event_a);

    //     let result = HashSet::from(["a", "b", "c"]);
    //     assert_eq!(tcsb_b.eval(), result);
    //     assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    //     assert_eq!(tcsb_a.eval(), tcsb_c.eval());
    // }

    use std::collections::HashSet;

    use crate::crdt::{
        membership_set::MSet,
        test_util::{triplet, twins},
    };

    #[test_log::test]
    fn stable_op() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MSet<&str>>();

        let event_a = tcsb_a.tc_bcast_op(MSet::Add("a"));
        tcsb_b.tc_deliver_op(event_a);

        assert_eq!(tcsb_b.eval(), HashSet::from(["a"]));
        assert_eq!(tcsb_a.eval(), HashSet::from([]));
    }

    #[test_log::test]
    fn stable_op_rmv() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MSet<&str>>();

        let event_a = tcsb_a.tc_bcast_op(MSet::Add("a"));
        let event_a_2 = tcsb_a.tc_bcast_op(MSet::Remove("a"));
        tcsb_b.tc_deliver_op(event_a);
        tcsb_b.tc_deliver_op(event_a_2);

        assert_eq!(tcsb_b.eval(), HashSet::from([]));
        assert_eq!(tcsb_a.eval(), HashSet::from([]));
    }

    #[test_log::test]
    fn stable_op_rmv_add_concurrent() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MSet<&str>>();

        // let event_a = tcsb_a.tc_bcast_op(MSet::Add("a"));
        // let event_b = tcsb_b.tc_bcast_op(MSet::Remove("a"));
        // tcsb_b.tc_deliver_op(event_a);
        // tcsb_a.tc_deliver_op(event_b);

        // assert_eq!(tcsb_b.eval(), HashSet::from([]));
        // assert_eq!(tcsb_a.eval(), HashSet::from([]));

        let event_a = tcsb_a.tc_bcast_op(MSet::Add("c"));
        let event_b = tcsb_b.tc_bcast_op(MSet::Add("c"));
        tcsb_b.tc_deliver_op(event_a);
        tcsb_a.tc_deliver_op(event_b);

        println!("ltm B {}", tcsb_b.ltm);
        println!("POLOG B: {:?}", tcsb_b.state);

        println!("ltm A {}", tcsb_a.ltm);
        println!("POLOG A: {:?}", tcsb_a.state);

        assert_eq!(tcsb_b.eval(), HashSet::from(["c"]));
        assert_eq!(tcsb_a.eval(), HashSet::from(["c"]));
    }

    #[test_log::test]
    fn add_then_rmv_concurrent_add() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<MSet<&str>>();

        let event_a = tcsb_a.tc_bcast_op(MSet::Add("a"));
        tcsb_b.tc_deliver_op(event_a);

        let event_b = tcsb_b.tc_bcast_op(MSet::Remove("a"));
        tcsb_a.tc_deliver_op(event_b);

        let event_c = tcsb_c.tc_bcast_op(MSet::Add("a"));
        tcsb_a.tc_deliver_op(event_c.clone());
        tcsb_b.tc_deliver_op(event_c);

        assert_eq!(tcsb_b.eval(), HashSet::from([]));
        assert_eq!(tcsb_c.eval(), HashSet::from([]));
        assert_eq!(tcsb_a.eval(), HashSet::from([]));
    }
}
