use camino::Utf8Path;

use crate::protocol::metadata::Metadata;
use crate::protocol::po_log::POLog;
use crate::protocol::{event::Event, pure_crdt::PureCRDT};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum MVRegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for MVRegister<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = Vec<V>;

    fn r(event: &Event<Self>, _: &POLog<Self>) -> bool {
        matches!(event.op, MVRegister::Clear)
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        old_event.metadata.clock < new_event.metadata.clock
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(_: &Metadata, _: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>, _: &Utf8Path) -> Self::Value {
        let mut vec = Self::Value::new();
        for op in state.iter() {
            if let MVRegister::Write(v) = op.as_ref() {
                vec.push(v.clone());
            }
        }
        vec
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{
        mv_register::MVRegister,
        test_util::{triplet, twins},
    };

    #[test_log::test]
    fn simple_mv_register() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast_op(MVRegister::Write("a"));
        tcsb_b.tc_deliver_op(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast_op(MVRegister::Write("b"));
        tcsb_a.tc_deliver_op(event);

        assert_eq!(tcsb_a.state.stable.len(), 1);

        let result = vec!["b"];
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_mv_register() {
        let (mut tcsb_a, mut tcsb_b) = twins::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast_op(MVRegister::Write("c"));
        tcsb_b.tc_deliver_op(event);

        assert_eq!(tcsb_a.eval(), vec!["c"]);
        assert_eq!(tcsb_b.eval(), vec!["c"]);

        let event = tcsb_b.tc_bcast_op(MVRegister::Write("d"));
        tcsb_a.tc_deliver_op(event);

        assert_eq!(tcsb_a.eval(), vec!["d"]);
        assert_eq!(tcsb_b.eval(), vec!["d"]);

        let event_a = tcsb_a.tc_bcast_op(MVRegister::Write("a"));
        let event_b = tcsb_b.tc_bcast_op(MVRegister::Write("b"));
        tcsb_b.tc_deliver_op(event_a);
        tcsb_a.tc_deliver_op(event_b);

        let result = vec!["b", "a"];
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn multiple_concurrent_mv_register() {
        let (mut tcsb_a, mut tcsb_b, _tcsb_c) = triplet::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast_op(MVRegister::Write("c"));
        tcsb_b.tc_deliver_op(event);

        assert_eq!(tcsb_a.eval(), vec!["c"]);
        assert_eq!(tcsb_b.eval(), vec!["c"]);

        let event = tcsb_b.tc_bcast_op(MVRegister::Write("d"));
        tcsb_a.tc_deliver_op(event);

        assert_eq!(tcsb_a.eval(), vec!["d"]);
        assert_eq!(tcsb_b.eval(), vec!["d"]);

        let event_a = tcsb_a.tc_bcast_op(MVRegister::Write("a"));
        let event_aa = tcsb_a.tc_bcast_op(MVRegister::Write("aa"));

        let event_b = tcsb_b.tc_bcast_op(MVRegister::Write("b"));

        tcsb_a.tc_deliver_op(event_b);
        tcsb_b.tc_deliver_op(event_a);
        tcsb_b.tc_deliver_op(event_aa);

        println!("A: {:?}", tcsb_a.converging_members);
        println!("B: {:?}", tcsb_b.converging_members);

        let result = vec!["b", "aa"];
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_b.eval(), result);
    }
}
