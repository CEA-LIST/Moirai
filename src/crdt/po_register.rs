use std::{cmp::Ordering, collections::HashSet, fmt::Debug, hash::Hash};

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum PORegister<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for PORegister<V>
where
    V: Debug + Clone + Eq + Hash + PartialOrd,
{
    type Value = HashSet<V>;
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, PORegister::Clear)
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
            if let PORegister::Write(v) = o {
                let mut keep = true;
                set.retain(|existing| {
                    match v.partial_cmp(existing) {
                        Some(Ordering::Greater) => {
                            // If greater, remove existing
                            false
                        }
                        Some(Ordering::Less) => {
                            // If less, keep existing
                            keep = false;
                            true
                        }
                        _ => {
                            // If equal, keep existing
                            true
                        }
                    }
                });
                if keep {
                    set.insert(v.clone());
                }
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::HashSet};

    use crate::crdt::{po_register::PORegister, test_util::twins_graph};

    #[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
    pub enum Family {
        // Age
        Parent(u32),
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

    #[test_log::test]
    fn simple_po_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

        let event = tcsb_a.tc_bcast(PORegister::Write(Family::Child));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Child]));
        assert_eq!(tcsb_b.eval(), HashSet::from([Family::Child]));

        let event = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(20)));
        tcsb_a.try_deliver(event);

        let result = HashSet::from([Family::Parent(20)]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn simple_po_register_2() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

        let event = tcsb_a.tc_bcast(PORegister::Write(Family::Parent(20)));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(20)]));
        assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(20)]));

        let event = tcsb_b.tc_bcast(PORegister::Write(Family::Child));
        tcsb_a.try_deliver(event);

        let result = HashSet::from([Family::Child]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_po_register() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

        let event_a = tcsb_a.tc_bcast(PORegister::Write(Family::Parent(20)));
        let event_b = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(21)));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let result = HashSet::from([Family::Parent(20), Family::Parent(21)]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn po_register_instability() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

        let event_a_1 = tcsb_a.tc_bcast(PORegister::Write(Family::Child));
        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Child]));
        let event_b_1 = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(42)));
        assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(42)]));
        tcsb_a.try_deliver(event_b_1);
        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(42)]));

        let event_b_2 = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(21)));
        assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(21)]));
        tcsb_a.try_deliver(event_b_2);
        tcsb_b.try_deliver(event_a_1);

        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(21)]));
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn po_register_instability_2() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<PORegister<Family>>();

        let event_a_1 = tcsb_a.tc_bcast(PORegister::Write(Family::Parent(20)));
        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(20)]));
        let event_b_1 = tcsb_b.tc_bcast(PORegister::Write(Family::Parent(42)));
        assert_eq!(tcsb_b.eval(), HashSet::from([Family::Parent(42)]));
        tcsb_a.try_deliver(event_b_1);
        assert_eq!(
            tcsb_a.eval(),
            HashSet::from([Family::Parent(42), Family::Parent(20)])
        );

        let event_b_2 = tcsb_b.tc_bcast(PORegister::Write(Family::Child));
        assert_eq!(tcsb_b.eval(), HashSet::from([Family::Child]));
        tcsb_a.try_deliver(event_b_2);
        tcsb_b.try_deliver(event_a_1);

        assert_eq!(tcsb_a.eval(), HashSet::from([Family::Parent(20)]));
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        convergence_checker::<EventGraph<PORegister<Family>>>(
            &[
                PORegister::Write(Family::Child),
                PORegister::Write(Family::Parent(30)),
                PORegister::Write(Family::Parent(40)),
                PORegister::Clear,
            ],
            HashSet::from([Family::Parent(30), Family::Parent(40)]),
            HashSet::eq,
        );
    }
}
