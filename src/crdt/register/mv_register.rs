use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
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
    type StableState = Vec<Self>;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), MVRegister::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
    }

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut set = Self::Value::default();
        for o in stable.iter().chain(unstable.iter().map(|t| t.op())) {
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

    use crate::{
        crdt::{
            register::mv_register::MVRegister,
            test_util::{triplet, twins},
        },
        protocol::replica::IsReplica,
    };

    #[test]
    fn simple_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("a"));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), HashSet::from(["a"]));
        assert_eq!(replica_b.query(), HashSet::from(["a"]));

        let event = replica_b.send(MVRegister::Write("b"));
        replica_a.receive(event);

        let result = HashSet::from(["b"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("c"));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), HashSet::from(["c"]));
        assert_eq!(replica_b.query(), HashSet::from(["c"]));

        let event = replica_b.send(MVRegister::Write("d"));
        replica_a.receive(event);

        assert_eq!(replica_a.query(), HashSet::from(["d"]));
        assert_eq!(replica_b.query(), HashSet::from(["d"]));

        let event_a = replica_a.send(MVRegister::Write("a"));
        let event_b = replica_b.send(MVRegister::Write("b"));
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = HashSet::from(["b", "a"]);
        let eval_a = replica_a.query();
        let eval_b = replica_b.query();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_mv_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("c"));
        replica_b.receive(event);

        assert_eq!(replica_a.query(), HashSet::from(["c"]));
        assert_eq!(replica_b.query(), HashSet::from(["c"]));

        let event = replica_b.send(MVRegister::Write("d"));
        replica_a.receive(event);

        assert_eq!(replica_a.query(), HashSet::from(["d"]));
        assert_eq!(replica_b.query(), HashSet::from(["d"]));

        let event_a = replica_a.send(MVRegister::Write("a"));
        let event_aa = replica_a.send(MVRegister::Write("aa"));

        let event_b = replica_b.send(MVRegister::Write("b"));

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = HashSet::from(["aa", "b"]);
        let eval_a = replica_a.query();
        let eval_b = replica_b.query();
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn mv_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<u32>>();

        let event_a_1 = replica_a.send(MVRegister::Write(4));
        assert_eq!(replica_a.query(), HashSet::from([4]));
        let event_b_1 = replica_b.send(MVRegister::Write(5));
        assert_eq!(replica_b.query(), HashSet::from([5]));
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(), HashSet::from([4, 5]));

        let event_b_2 = replica_b.send(MVRegister::Write(2));
        assert_eq!(replica_b.query(), HashSet::from([2]));
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(), HashSet::from([4, 2]));
        assert_eq!(replica_a.query(), replica_b.query());
    }

    // #[cfg(feature = "utils")]
    // #[test]
    // fn convergence_check() {
    //     use crate::{
    //         crdt::register::mv_register::MVRegister, protocol::event_graph::EventGraph,
    //         utils::convergence_checker::convergence_checker,
    //     };

    //     convergence_checker::<EventGraph<MVRegister<&str>>>(
    //         &[
    //             MVRegister::Write("a"),
    //             MVRegister::Write("b"),
    //             MVRegister::Clear,
    //         ],
    //         HashSet::from(["a", "b"]),
    //         HashSet::eq,
    //     );
    // }
}
