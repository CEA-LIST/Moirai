use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use crate::{
    protocol::{
        crdt::{
            eval::Eval,
            pure_crdt::PureCRDT,
            query::{QueryOperation, Read},
        },
        event::tagged_op::TaggedOp,
        state::unstable_state::IsUnstableState,
    },
    HashSet,
};

#[derive(Clone, Debug)]
pub enum PORegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for PORegister<V>
where
    V: Debug + Default + PartialOrd + Clone + Eq + PartialEq + Hash,
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
        matches!(new_tagged_op.op(), PORegister::Clear)
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
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for PORegister<V>
where
    V: Debug + Default + PartialOrd + Clone + Eq + PartialEq + Hash,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &<PORegister<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        // The set can contain only incomparable values
        let mut set = HashSet::<V>::default();
        for o in stable.iter().chain(unstable.iter().map(|to| to.op())) {
            if let PORegister::Write(v) = o {
                // We add the value if there is no v' in the set that is superior to v
                // We remove any v' in the set that is inferior to v
                if !set.iter().any(|v2| v2 > v) {
                    set.retain(|v2| !matches!(v2.partial_cmp(v), Some(Ordering::Less)));
                    set.insert(v.clone());
                }
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::{
        crdt::{register::po_register::PORegister, test_util::twins},
        protocol::{crdt::query::Read, replica::IsReplica},
        set_from_slice,
    };

    #[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
    pub enum Family {
        Parent(u32), // Age
        #[default]
        Child,
    }

    impl PartialOrd for Family {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            match (self, other) {
                (Family::Parent(age1), Family::Parent(age2)) => {
                    if age1 == age2 {
                        Some(Ordering::Equal)
                    } else {
                        None
                    }
                }
                (Family::Parent(_), Family::Child) => Some(Ordering::Greater),
                (Family::Child, Family::Parent(_)) => Some(Ordering::Less),
                (Family::Child, Family::Child) => None,
            }
        }
    }

    #[test]
    fn simple_po_register() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event = replica_a.send(PORegister::Write(Family::Child)).unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Child])
        );
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Child])
        );

        let event = replica_b
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&[Family::Parent(20)]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn simple_po_register_2() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event = replica_a
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );

        let event = replica_b.send(PORegister::Write(Family::Child)).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&[Family::Child]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_po_register() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event_a = replica_a
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        let event_b = replica_b
            .send(PORegister::Write(Family::Parent(21)))
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&[Family::Parent(20), Family::Parent(21)]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn po_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event_a_1 = replica_a.send(PORegister::Write(Family::Child)).unwrap();
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Child])
        );
        let event_b_1 = replica_b
            .send(PORegister::Write(Family::Parent(42)))
            .unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(42)])
        );
        replica_a.receive(event_b_1);
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(42)])
        );

        let event_b_2 = replica_b
            .send(PORegister::Write(Family::Parent(21)))
            .unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(21)])
        );
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(21)])
        );
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn po_register_instability_2() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event_a_1 = replica_a
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );
        let event_b_1 = replica_b
            .send(PORegister::Write(Family::Parent(42)))
            .unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(42)])
        );
        replica_a.receive(event_b_1);
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(42), Family::Parent(20)])
        );

        let event_b_2 = replica_b.send(PORegister::Write(Family::Child)).unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Child])
        );
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    // #[cfg(feature = "utils")]
    // #[test]
    // fn convergence_check() {
    //     use crate::{
    //         protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
    //     };

    //     convergence_checker::<EventGraph<PORegister<Family>>>(
    //         &[
    //             PORegister::Write(Family::Child),
    //             PORegister::Write(Family::Parent(30)),
    //             PORegister::Write(Family::Parent(40)),
    //             PORegister::Clear,
    //         ],
    //         set_from_slice(&[Family::Parent(30), Family::Parent(40)]),
    //         HashSet::eq,
    //     );
    // }
}
