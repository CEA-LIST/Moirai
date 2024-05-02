pub mod aw_set;
pub mod counter;
pub mod graph;
pub mod mv_register;

#[cfg(test)]
pub mod test_util {
    use crate::protocol::{
        event::Message,
        membership::{Membership, Welcome},
        pure_crdt::PureCRDT,
        tcsb::Tcsb,
    };
    use std::fmt::Debug;

    pub fn twins<O: PureCRDT + Clone + Debug>(
    ) -> (Tcsb<&'static str, u64, O>, Tcsb<&'static str, u64, O>) {
        let mut tcsb_a = Tcsb::<&str, u64, O>::new("a");
        let mut tcsb_b = Tcsb::<&str, u64, O>::new("b");

        let event = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
        tcsb_b.tc_deliver(event);

        let welcome = Welcome::new(&tcsb_b);
        let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
        tcsb_a.tc_deliver(event);

        (tcsb_a, tcsb_b)
    }
}
