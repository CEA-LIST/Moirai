use std::fmt::Debug;

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum TORegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for TORegister<V>
where
    V: Debug + Clone + Eq + Default + PartialOrd + Ord,
{
    type Value = V;
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, TORegister::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        _new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        new_dot: &Dot,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_dot, is_conc, new_op, new_dot)
    }

    fn eval(stable: &Self::Stable, ops: &[Self]) -> Self::Value {
        let mut set = Self::Value::default();
        for o in stable.iter().chain(ops.iter()) {
            if let TORegister::Write(v) = o {
                if v > &set {
                    set = v.clone();
                }
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{
        register::to_register::TORegister,
        test_util::{triplet_graph, twins_graph},
    };

    #[test_log::test]
    fn simple_to_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<TORegister<&str>>();

        let event = tcsb_a.tc_bcast(TORegister::Write("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), "a");
        assert_eq!(tcsb_b.eval(), "a");

        let event = tcsb_b.tc_bcast(TORegister::Write("b"));
        tcsb_a.try_deliver(event);

        let result = "b";
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_to_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<TORegister<&str>>();

        let event = tcsb_a.tc_bcast(TORegister::Write("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), "c");
        assert_eq!(tcsb_b.eval(), "c");

        let event = tcsb_b.tc_bcast(TORegister::Write("d"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.eval(), "d");
        assert_eq!(tcsb_b.eval(), "d");

        let event_a = tcsb_a.tc_bcast(TORegister::Write("a"));
        let event_b = tcsb_b.tc_bcast(TORegister::Write("b"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        let result = "b";
        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test_log::test]
    fn multiple_concurrent_to_register() {
        let (mut tcsb_a, mut tcsb_b, _tcsb_c) = triplet_graph::<TORegister<&str>>();

        let event = tcsb_a.tc_bcast(TORegister::Write("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), "c");
        assert_eq!(tcsb_b.eval(), "c");

        let event = tcsb_b.tc_bcast(TORegister::Write("d"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.eval(), "d");
        assert_eq!(tcsb_b.eval(), "d");

        let event_a = tcsb_a.tc_bcast(TORegister::Write("a"));
        let event_aa = tcsb_a.tc_bcast(TORegister::Write("aa"));

        let event_b = tcsb_b.tc_bcast(TORegister::Write("b"));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);
        tcsb_b.try_deliver(event_aa);

        let result = "b";
        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test_log::test]
    fn to_register_instability() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<TORegister<u32>>();

        let event_a_1 = tcsb_a.tc_bcast(TORegister::Write(4));
        assert_eq!(tcsb_a.eval(), 4);
        let event_b_1 = tcsb_b.tc_bcast(TORegister::Write(5));
        assert_eq!(tcsb_b.eval(), 5);
        tcsb_a.try_deliver(event_b_1);
        assert_eq!(tcsb_a.eval(), 5);

        let event_b_2 = tcsb_b.tc_bcast(TORegister::Write(2));
        assert_eq!(tcsb_b.eval(), 2);
        tcsb_a.try_deliver(event_b_2);
        tcsb_b.try_deliver(event_a_1);

        assert_eq!(tcsb_a.eval(), 4);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        convergence_checker::<EventGraph<TORegister<&str>>>(
            &[
                TORegister::Write("a"),
                TORegister::Write("b"),
                TORegister::Clear,
            ],
            "b",
            |a, b| a == b,
        );
    }
}
