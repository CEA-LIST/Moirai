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

    pub fn twins<O: PureCRDT + Clone + Debug>() -> Twins<O> {
        let mut tcsb_a = Tcsb::<O>::new("a");
        let mut tcsb_b = Tcsb::<O>::new("b");

        let event_a = tcsb_a.tc_bcast_gms(MSet::Add("b"));
        let event_b = tcsb_b.tc_bcast_gms(MSet::Add("a"));

        tcsb_b.tc_deliver_gms(event_a);
        tcsb_a.tc_deliver_gms(event_b);

        (tcsb_a, tcsb_b)
    }
}
