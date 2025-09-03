use crate::protocol::{crdt::pure_crdt::PureCRDT, event::tagged_op::TaggedOp};
use std::{collections::HashSet, fmt::Debug, hash::Hash};

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

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        Self: 'a,
    {
        // The set can contain only incomparable values
        let mut set = Self::Value::default();
        for o in stable.iter().chain(unstable.map(|to| to.op())) {
            if let PORegister::Write(v) = o {
                // We add the value if there is no v' in the set that is superior to v
                // We remove any v' in the set that is inferior to v
                if !set.iter().any(|v2| v2 > v) {
                    set.retain(|v2| v2 >= v);
                    set.insert(v.clone());
                }
            }
        }
        set
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{cmp::Ordering, collections::HashSet};

//     use crate::crdt::{register::po_register::PORegister, test_util::twins_graph};

//     #[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
//     pub enum Family {
//         // Age
//         Parent(u32),
//         #[default]
//         Child,
//     }

//     impl PartialOrd for Family {
//         fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//             match (self, other) {
//                 (Family::Parent(age1), Family::Parent(age2)) => {
//                     if age1 == age2 {
//                         Some(Ordering::Equal)
//                     } else {
//                         None
//                     }
//                 }
//                 (Family::Parent(_), Family::Child) => Some(Ordering::Greater),
//                 (Family::Child, Family::Parent(_)) => Some(Ordering::Less),
//                 (Family::Child, Family::Child) => None,
//             }
//         }
//     }

//     #[test_log::test]
//     fn simple_po_register() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

//         let event = tcsb_a.tc_bcast(PORegister::Write(Family::Child));
//         tcsb_b.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Child]));
//         assert_eq!(tcsb_b.eval(), HashSet::from([Family::Child]));

//         let event = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(20)));
//         tcsb_a.try_deliver(event);

//         let result = HashSet::from([Family::Parent(20)]);
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn simple_po_register_2() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

//         let event = tcsb_a.tc_bcast(PORegister::Write(Family::Parent(20)));
//         tcsb_b.try_deliver(event);

//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(20)]));
//         assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(20)]));

//         let event = tcsb_b.tc_bcast(PORegister::Write(Family::Child));
//         tcsb_a.try_deliver(event);

//         let result = HashSet::from([Family::Child]);
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn concurrent_po_register() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

//         let event_a = tcsb_a.tc_bcast(PORegister::Write(Family::Parent(20)));
//         let event_b = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(21)));
//         tcsb_a.try_deliver(event_b);
//         tcsb_b.try_deliver(event_a);

//         let result = HashSet::from([Family::Parent(20), Family::Parent(21)]);
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn po_register_instability() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

//         let event_a_1 = tcsb_a.tc_bcast(PORegister::Write(Family::Child));
//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Child]));
//         let event_b_1 = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(42)));
//         assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(42)]));
//         tcsb_a.try_deliver(event_b_1);
//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(42)]));

//         let event_b_2 = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(21)));
//         assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(21)]));
//         tcsb_a.try_deliver(event_b_2);
//         tcsb_b.try_deliver(event_a_1);

//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(21)]));
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn po_register_instability_2() {
//         let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

//         let event_a_1 = tcsb_a.tc_bcast(PORegister::Write(Family::Parent(20)));
//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(20)]));
//         let event_b_1 = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(42)));
//         assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(42)]));
//         tcsb_a.try_deliver(event_b_1);
//         assert_eq!(
//             tcsb_a.eval(),
//             HashSet::from([Family::Parent(42), Family::Parent(20)])
//         );

//         let event_b_2 = tcsb_b.tc_bcast(PORegister::Write(Family::Child));
//         assert_eq!(tcsb_b.eval(), HashSet::from([Family::Child]));
//         tcsb_a.try_deliver(event_b_2);
//         tcsb_b.try_deliver(event_a_1);

//         assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(20)]));
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[cfg(feature = "utils")]
//     #[test_log::test]
//     fn convergence_check() {
//         use crate::{
//             protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
//         };

//         convergence_checker::<EventGraph<PORegister<Family>>>(
//             &[
//                 PORegister::Write(Family::Child),
//                 PORegister::Write(Family::Parent(30)),
//                 PORegister::Write(Family::Parent(40)),
//                 PORegister::Clear,
//             ],
//             HashSet::from([Family::Parent(30), Family::Parent(40)]),
//             HashSet::eq,
//         );
//     }
// }
