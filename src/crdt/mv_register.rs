use std::{cmp::Ordering, fmt::Debug};

use crate::{
    clocks::dependency_clock::DependencyClock,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum MVRegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for MVRegister<V>
where
    V: Debug + Clone,
{
    type Value = Vec<V>;

    fn r(new_op: &Self, _order: Option<Ordering>, _old_op: &Self) -> bool {
        matches!(new_op, MVRegister::Clear)
    }

    fn r_zero(_old_op: &Self, order: Option<Ordering>, _new_op: &Self) -> bool {
        order == Some(Ordering::Less)
    }

    fn r_one(old_op: &Self, order: Option<Ordering>, new_op: &Self) -> bool {
        Self::r_zero(old_op, order, new_op)
    }

    fn stabilize(_metadata: &DependencyClock, _state: &mut EventGraph<Self>) {}

    fn eval(ops: &[Self]) -> Self::Value {
        let mut vec = Self::Value::new();
        for o in ops {
            if let MVRegister::Write(v) = o {
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
        test_util::{triplet_graph, twins_graph},
    };

    #[test_log::test]
    fn simple_mv_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast(MVRegister::Write("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast(MVRegister::Write("b"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 1);

        let result = vec!["b"];
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_mv_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast(MVRegister::Write("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), vec!["c"]);
        assert_eq!(tcsb_b.eval(), vec!["c"]);

        let event = tcsb_b.tc_bcast(MVRegister::Write("d"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.eval(), vec!["d"]);
        assert_eq!(tcsb_b.eval(), vec!["d"]);

        let event_a = tcsb_a.tc_bcast(MVRegister::Write("a"));
        let event_b = tcsb_b.tc_bcast(MVRegister::Write("b"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        let mut result = vec!["b", "a"];
        result.sort();
        let mut eval_a = tcsb_a.eval();
        eval_a.sort();
        let mut eval_b = tcsb_b.eval();
        eval_b.sort();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test_log::test]
    fn multiple_concurrent_mv_register() {
        let (mut tcsb_a, mut tcsb_b, _tcsb_c) = triplet_graph::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast(MVRegister::Write("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), vec!["c"]);
        assert_eq!(tcsb_b.eval(), vec!["c"]);

        let event = tcsb_b.tc_bcast(MVRegister::Write("d"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.eval(), vec!["d"]);
        assert_eq!(tcsb_b.eval(), vec!["d"]);

        let event_a = tcsb_a.tc_bcast(MVRegister::Write("a"));
        let event_aa = tcsb_a.tc_bcast(MVRegister::Write("aa"));

        let event_b = tcsb_b.tc_bcast(MVRegister::Write("b"));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);
        tcsb_b.try_deliver(event_aa);

        let mut result = vec!["b", "aa"];
        result.sort();
        let mut eval_a = tcsb_a.eval();
        eval_a.sort();
        let mut eval_b = tcsb_b.eval();
        eval_b.sort();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }
}
