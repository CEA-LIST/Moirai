pub mod aw_set;
pub mod counter;
pub mod duet;
pub mod graph;
pub mod membership_set;
pub mod mv_register;
pub mod rw_set;
pub mod uw_map;

pub mod test_util {
    use crate::protocol::{pure_crdt::PureCRDT, tcsb::Tcsb};
    use std::fmt::Debug;

    use super::membership_set::MSet;

    pub type Twins<O> = (Tcsb<O>, Tcsb<O>);
    pub type Triplet<O> = (Tcsb<O>, Tcsb<O>, Tcsb<O>);

    pub fn twins<O: PureCRDT + Clone + Debug>() -> Twins<O> {
        let mut tcsb_a = Tcsb::<O>::new_with_trace("a");
        let mut tcsb_b = Tcsb::<O>::new_with_trace("b");

        let event_a = tcsb_a.tc_bcast_membership(MSet::add("b"));
        let event_b = tcsb_b.tc_bcast_membership(MSet::add("a"));

        tcsb_b.tc_deliver_membership(event_a);
        tcsb_a.tc_deliver_membership(event_b);

        (tcsb_a, tcsb_b)
    }

    pub fn triplet<O: PureCRDT + Clone + Debug>() -> Triplet<O> {
        let mut tcsb_a = Tcsb::<O>::new("a");
        let mut tcsb_b = Tcsb::<O>::new("b");
        let mut tcsb_c = Tcsb::<O>::new("c");

        let event_a = tcsb_a.tc_bcast_membership(MSet::add("b"));
        let event_b = tcsb_b.tc_bcast_membership(MSet::add("a"));

        tcsb_b.tc_deliver_membership(event_a);
        tcsb_a.tc_deliver_membership(event_b);

        let event_b = tcsb_b.tc_bcast_membership(MSet::add("c"));
        tcsb_a.tc_deliver_membership(event_b);

        let event_a = tcsb_a.tc_bcast_membership(MSet::add("c"));
        tcsb_b.tc_deliver_membership(event_a);

        tcsb_c.group_membership = tcsb_b.group_membership.clone();
        tcsb_c.lsv = tcsb_b.lsv.clone();
        tcsb_c.ltm = tcsb_b.ltm.clone();

        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
        assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
        assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);
        (tcsb_a, tcsb_b, tcsb_c)
    }
}
