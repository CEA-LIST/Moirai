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
    pub type Triplets<O> = (Tcsb<O>, Tcsb<O>, Tcsb<O>);

    pub fn twins<O: PureCRDT + Clone + Debug>() -> Twins<O> {
        let mut tcsb_a = Tcsb::<O>::new("a");
        let mut tcsb_b = Tcsb::<O>::new("b");

        let event_a = tcsb_a.tc_bcast_gms(MSet::Add("b"));
        let event_b = tcsb_b.tc_bcast_gms(MSet::Add("a"));

        tcsb_b.tc_deliver_gms(event_a);
        tcsb_a.tc_deliver_gms(event_b);

        (tcsb_a, tcsb_b)
    }

    pub fn triplets<O: PureCRDT + Clone + Debug>() -> Triplets<O> {
        let mut tcsb_a = Tcsb::<O>::new("a");
        let mut tcsb_b = Tcsb::<O>::new("b");
        let mut tcsb_c = Tcsb::<O>::new("c");

        let event_a = tcsb_a.tc_bcast_gms(MSet::Add("b"));
        let event_b = tcsb_b.tc_bcast_gms(MSet::Add("a"));

        tcsb_b.tc_deliver_gms(event_a);
        tcsb_a.tc_deliver_gms(event_b);

        let event_b = tcsb_b.tc_bcast_gms(MSet::Add("c"));
        tcsb_a.tc_deliver_gms(event_b);

        let event_a = tcsb_a.tc_bcast_gms(MSet::Add("c"));
        tcsb_b.tc_deliver_gms(event_a);

        tcsb_c.gms = tcsb_b.gms.clone();
        tcsb_c.lsv = tcsb_b.lsv.clone();
        tcsb_c.ltm = tcsb_b.ltm.clone();

        assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
        assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
        assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);

        (tcsb_a, tcsb_b, tcsb_c)
    }
}
