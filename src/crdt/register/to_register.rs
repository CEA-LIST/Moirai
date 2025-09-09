use crate::protocol::{
    crdt::pure_crdt::PureCRDT, event::tagged_op::TaggedOp, state::unstable_state::IsUnstableState,
};
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

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut val = Self::Value::default();
        for o in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            if let TORegister::Write(v) = o {
                if v > &val {
                    val = v.clone();
                }
            }
        }
        val
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            register::to_register::TORegister,
            test_util::{triplet, twins},
        },
        protocol::replica::IsReplica,
    };

    #[test]
    fn simple_to_register() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<&str>>();

        let event = replica_a.send(TORegister::Write("a"));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), "a");
        assert_eq!(replica_b.query(), "a");

        let event = replica_b.send(TORegister::Write("b"));
        replica_a.receive(event);

        let result = "b";
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_to_register() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<&str>>();

        let event = replica_a.send(TORegister::Write("c"));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), "c");
        assert_eq!(replica_b.query(), "c");

        let event = replica_b.send(TORegister::Write("d"));
        replica_a.receive(event);

        assert_eq!(replica_a.query(), "d");
        assert_eq!(replica_b.query(), "d");

        let event_a = replica_a.send(TORegister::Write("a"));
        let event_b = replica_b.send(TORegister::Write("b"));
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = "b";
        let eval_a = replica_a.query();
        let eval_b = replica_b.query();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_to_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<TORegister<&str>>();

        let event = replica_a.send(TORegister::Write("c"));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), "c");
        assert_eq!(replica_b.query(), "c");

        let event = replica_b.send(TORegister::Write("d"));
        replica_a.receive(event);

        assert_eq!(replica_a.query(), "d");
        assert_eq!(replica_b.query(), "d");

        let event_a = replica_a.send(TORegister::Write("a"));
        let event_aa = replica_a.send(TORegister::Write("aa"));

        let event_b = replica_b.send(TORegister::Write("b"));

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = "b";
        let eval_a = replica_a.query();
        let eval_b = replica_b.query();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn to_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<u32>>();

        let event_a_1 = replica_a.send(TORegister::Write(4));
        assert_eq!(replica_a.query(), 4);
        let event_b_1 = replica_b.send(TORegister::Write(5));
        assert_eq!(replica_b.query(), 5);
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(), 5);

        let event_b_2 = replica_b.send(TORegister::Write(2));
        assert_eq!(replica_b.query(), 2);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(), 4);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    // #[cfg(feature = "utils")]
    // #[test]
    // fn convergence_check() {
    //     use crate::{
    //         protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
    //     };

    //     convergence_checker::<EventGraph<TORegister<&str>>>(
    //         &[
    //             TORegister::Write("a"),
    //             TORegister::Write("b"),
    //             TORegister::Clear,
    //         ],
    //         "b",
    //         |a, b| a == b,
    //     );
    // }
}
