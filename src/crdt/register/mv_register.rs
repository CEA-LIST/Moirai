use std::{fmt::Debug, hash::Hash};

use crate::{
    protocol::{
        crdt::{
            eval::Eval,
            pure_crdt::PureCRDT,
            query::{QueryOperation, Read},
        },
        event::{tag::Tag, tagged_op::TaggedOp},
        state::unstable_state::IsUnstableState,
    },
    HashSet,
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
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash + Default,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &<MVRegister<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut set = HashSet::<V>::default();
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
    use crate::{
        crdt::{
            register::mv_register::MVRegister,
            test_util::{triplet, twins},
        },
        protocol::{crdt::query::Read, replica::IsReplica},
        set_from_slice, HashSet,
    };

    #[test]
    fn simple_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(Read::new()),
            HashSet::from_iter(["a"].iter().cloned())
        );
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["a"]));

        let event = replica_b.send(MVRegister::Write("b")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["c"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["c"]));

        let event = replica_b.send(MVRegister::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["d"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["d"]));

        let event_a = replica_a.send(MVRegister::Write("a")).unwrap();
        let event_b = replica_b.send(MVRegister::Write("b")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = set_from_slice(&["b", "a"]);
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_mv_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["c"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["c"]));

        let event = replica_b.send(MVRegister::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["d"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["d"]));

        let event_a = replica_a.send(MVRegister::Write("a")).unwrap();
        let event_aa = replica_a.send(MVRegister::Write("aa")).unwrap();

        let event_b = replica_b.send(MVRegister::Write("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = set_from_slice(&["aa", "b"]);
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn mv_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<u32>>();

        let event_a_1 = replica_a.send(MVRegister::Write(4)).unwrap();
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[4]));
        let event_b_1 = replica_b.send(MVRegister::Write(5)).unwrap();
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&[5]));
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[4, 5]));

        let event_b_2 = replica_b.send(MVRegister::Write(2)).unwrap();
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&[2]));
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[4, 2]));
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
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
    //         set_from_slice(&["a", "b"]),
    //         HashSet::eq,
    //     );
    // }
}
