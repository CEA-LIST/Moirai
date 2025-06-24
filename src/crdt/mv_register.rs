use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum MVRegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = HashSet<V>;
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, MVRegister::Clear)
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
            if let MVRegister::Write(v) = o {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::crdt::{
        mv_register::MVRegister,
        test_util::{triplet_graph, twins_graph},
    };

    #[test_log::test]
    fn simple_mv_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast(MVRegister::Write("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from(["a"]));
        assert_eq!(tcsb_b.eval(), HashSet::from(["a"]));

        let event = tcsb_b.tc_bcast(MVRegister::Write("b"));
        tcsb_a.try_deliver(event);

        let result = HashSet::from(["b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_mv_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast(MVRegister::Write("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from(["c"]));
        assert_eq!(tcsb_b.eval(), HashSet::from(["c"]));

        let event = tcsb_b.tc_bcast(MVRegister::Write("d"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from(["d"]));
        assert_eq!(tcsb_b.eval(), HashSet::from(["d"]));

        let event_a = tcsb_a.tc_bcast(MVRegister::Write("a"));
        let event_b = tcsb_b.tc_bcast(MVRegister::Write("b"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        let result = HashSet::from(["b", "a"]);
        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test_log::test]
    fn multiple_concurrent_mv_register() {
        let (mut tcsb_a, mut tcsb_b, _tcsb_c) = triplet_graph::<MVRegister<&str>>();

        let event = tcsb_a.tc_bcast(MVRegister::Write("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from(["c"]));
        assert_eq!(tcsb_b.eval(), HashSet::from(["c"]));

        let event = tcsb_b.tc_bcast(MVRegister::Write("d"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from(["d"]));
        assert_eq!(tcsb_b.eval(), HashSet::from(["d"]));

        let event_a = tcsb_a.tc_bcast(MVRegister::Write("a"));
        let event_aa = tcsb_a.tc_bcast(MVRegister::Write("aa"));

        let event_b = tcsb_b.tc_bcast(MVRegister::Write("b"));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);
        tcsb_b.try_deliver(event_aa);

        let result = HashSet::from(["aa", "b"]);
        let eval_a = tcsb_a.eval();
        let eval_b = tcsb_b.eval();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        convergence_checker::<EventGraph<MVRegister<&str>>>(
            &[
                MVRegister::Write("a"),
                MVRegister::Write("b"),
                MVRegister::Clear,
            ],
            HashSet::from(["a", "b"]),
        );
    }
}
