use crate::protocol::{crdt::pure_crdt::PureCRDT, event::tagged_op::TaggedOp};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub enum TORegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for TORegister<V>
where
    V: Debug + Default + PartialOrd + Ord + Clone,
{
    type Value = V;
    type StableState = Vec<Self>;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), TORegister::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&crate::protocol::event::tag::Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&crate::protocol::event::tag::Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
    }

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        Self: 'a,
    {
        let mut val = Self::Value::default();
        for o in stable.iter().chain(unstable.map(|to| to.op())) {
            if let TORegister::Write(v) = o {
                if v > &val {
                    val = v.clone();
                }
            }
        }
        val
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::crdt::{
//         register::to_register::TORegister,
//         test_util::{triplet_graph, twins_graph},
//     };

//     #[test_log::test]
//     fn simple_to_register() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<TORegister<&str>>();

//         let event = tcsb_a.tc_bcast(TORegister::Write("a"));
//         tcsb_b.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), "a");
//         assert_eq!(tcsb_b.eval(), "a");

//         let event = tcsb_b.tc_bcast(TORegister::Write("b"));
//         tcsb_a.try_deliver(event);

//         let result = "b";
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn concurrent_to_register() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<TORegister<&str>>();

//         let event = tcsb_a.tc_bcast(TORegister::Write("c"));
//         tcsb_b.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), "c");
//         assert_eq!(tcsb_b.eval(), "c");

//         let event = tcsb_b.tc_bcast(TORegister::Write("d"));
//         tcsb_a.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), "d");
//         assert_eq!(tcsb_b.eval(), "d");

//         let event_a = tcsb_a.tc_bcast(TORegister::Write("a"));
//         let event_b = tcsb_b.tc_bcast(TORegister::Write("b"));
//         tcsb_b.try_deliver(event_a);
//         tcsb_a.try_deliver(event_b);

//         let result = "b";
//         let eval_a = tcsb_a.eval();
//         let eval_b = tcsb_b.eval();
//         assert_eq!(eval_a, result);
//         assert_eq!(eval_a, eval_b);
//     }

//     #[test_log::test]
//     fn multiple_concurrent_to_register() {
//         let (mut tcsb_a, mut tcsb_b, _tcsb_c) = triplet_graph::<TORegister<&str>>();

//         let event = tcsb_a.tc_bcast(TORegister::Write("c"));
//         tcsb_b.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), "c");
//         assert_eq!(tcsb_b.eval(), "c");

//         let event = tcsb_b.tc_bcast(TORegister::Write("d"));
//         tcsb_a.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), "d");
//         assert_eq!(tcsb_b.eval(), "d");

//         let event_a = tcsb_a.tc_bcast(TORegister::Write("a"));
//         let event_aa = tcsb_a.tc_bcast(TORegister::Write("aa"));

//         let event_b = tcsb_b.tc_bcast(TORegister::Write("b"));

//         tcsb_a.try_deliver(event_b);
//         tcsb_b.try_deliver(event_a);
//         tcsb_b.try_deliver(event_aa);

//         let result = "b";
//         let eval_a = tcsb_a.eval();
//         let eval_b = tcsb_b.eval();
//         assert_eq!(eval_a, result);
//         assert_eq!(eval_a, eval_b);
//     }

//     #[test_log::test]
//     fn to_register_instability() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<TORegister<u32>>();

//         let event_a_1 = tcsb_a.tc_bcast(TORegister::Write(4));
//         assert_eq!(tcsb_a.eval(), 4);
//         let event_b_1 = tcsb_b.tc_bcast(TORegister::Write(5));
//         assert_eq!(tcsb_b.eval(), 5);
//         tcsb_a.try_deliver(event_b_1);
//         assert_eq!(tcsb_a.eval(), 5);

//         let event_b_2 = tcsb_b.tc_bcast(TORegister::Write(2));
//         assert_eq!(tcsb_b.eval(), 2);
//         tcsb_a.try_deliver(event_b_2);
//         tcsb_b.try_deliver(event_a_1);

//         assert_eq!(tcsb_a.eval(), 4);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[cfg(feature = "utils")]
//     #[test_log::test]
//     fn convergence_check() {
//         use crate::{
//             protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
//         };

//         convergence_checker::<EventGraph<TORegister<&str>>>(
//             &[
//                 TORegister::Write("a"),
//                 TORegister::Write("b"),
//                 TORegister::Clear,
//             ],
//             "b",
//             |a, b| a == b,
//         );
//     }
// }
