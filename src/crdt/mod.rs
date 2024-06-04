pub mod aw_set;
pub mod counter;
pub mod graph;
pub mod mv_register;

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

    pub fn triplets<O: PureCRDT + Clone + Debug>() -> (
        Tcsb<&'static str, u64, O>,
        Tcsb<&'static str, u64, O>,
        Tcsb<&'static str, u64, O>,
    ) {
        let mut tcsb_a = Tcsb::<&str, u64, O>::new("a");
        let mut tcsb_b = Tcsb::<&str, u64, O>::new("b");
        let mut tcsb_c = Tcsb::<&str, u64, O>::new("c");

        let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
        let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Join));

        tcsb_b.tc_deliver(event_a.clone());
        tcsb_b.tc_deliver(event_c.clone());
        tcsb_a.tc_deliver(event_c);
        tcsb_c.tc_deliver(event_a);

        let welcome = Welcome::new(&tcsb_b);
        let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
        tcsb_a.tc_deliver(event.clone());
        tcsb_c.tc_deliver(event);

        assert_eq!(tcsb_a.ltm.len(), 3);
        assert_eq!(tcsb_b.ltm.len(), 3);
        assert_eq!(tcsb_c.ltm.len(), 3);

        (tcsb_a, tcsb_b, tcsb_c)
    }
}
