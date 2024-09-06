use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::po_log::POLog;
use crate::protocol::pure_crdt::PureCRDT;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;

#[derive(Clone, Debug)]
pub enum MembershipSet<V> {
    Add(V),
    Remove(V),
}

impl<V> PureCRDT for MembershipSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;

    fn r(_: &Event<Self>, _: &POLog<Self>) -> bool {
        false
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        let b = old_event.metadata.vc < new_event.metadata.vc
            && match (&old_event.op, &new_event.op) {
                (MembershipSet::Add(v1), MembershipSet::Add(v2))
                | (MembershipSet::Remove(v1), MembershipSet::Remove(v2))
                | (MembershipSet::Add(v1), MembershipSet::Remove(v2))
                | (MembershipSet::Remove(v1), MembershipSet::Add(v2)) => v1 == v2,
            };
        println!("r_zero: {:?} {:?} -> {}", old_event.op, new_event.op, b);
        b
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {
        // let op = state.unstable.get(metadata).unwrap();
        // match op.as_ref() {
        //     MembershipSet::Remove(v) => {
        //         state.stable.retain(|o| match o.as_ref() {
        //             MembershipSet::Add(v2) => v != v2,
        //             _ => true,
        //         });
        //         state.unstable.remove(metadata);
        //     }
        //     _ => {}
        // }
    }

    fn eval(state: &POLog<Self>, _: &Path) -> Self::Value {
        let mut set = Self::Value::new();
        for o in state.stable.iter() {
            if let MembershipSet::Add(v) = o.as_ref() {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{membership_set::MembershipSet, test_util::twins};

    #[test_log::test]
    fn simple_membership_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MembershipSet<&str>>();

        let event_a = tcsb_a.tc_bcast(MembershipSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(MembershipSet::Add("a"));

        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        assert_eq!(tcsb_a.eval(), vec!["a"].into_iter().collect());
        assert_eq!(tcsb_b.eval(), tcsb_a.eval());
    }

    #[test_log::test]
    fn concurrent_add_remove_membership_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MembershipSet<&str>>();

        let event_a = tcsb_a.tc_bcast(MembershipSet::Remove("a"));
        let event_b = tcsb_b.tc_bcast(MembershipSet::Add("a"));

        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        println!("{:?}", tcsb_a.state.unstable);
        println!("{:?}", tcsb_b.state.unstable);

        assert_eq!(tcsb_b.eval(), vec!["a"].into_iter().collect());
        assert_eq!(tcsb_b.eval(), tcsb_a.eval());
    }

    #[test_log::test]
    fn concurrent_add_remove_2_membership_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MembershipSet<&str>>();

        let event = tcsb_a.tc_bcast(MembershipSet::Add("a"));
        tcsb_b.tc_deliver(event);

        let event_a = tcsb_a.tc_bcast(MembershipSet::Remove("a"));
        let event_b = tcsb_b.tc_bcast(MembershipSet::Add("a"));

        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        assert_eq!(tcsb_a.eval(), vec!["a"].into_iter().collect());
        assert_eq!(tcsb_b.eval(), tcsb_a.eval());
    }
}
