use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT, stable::Stable},
};

#[derive(Clone, Debug, PartialEq)]
pub enum RWSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

impl<V> Stable<RWSet<V>> for (HashSet<V>, Vec<RWSet<V>>)
where
    V: Clone + Eq + Hash + Debug,
{
    fn is_default(&self) -> bool {
        (HashSet::default(), Vec::default()) == *self
    }

    fn apply_redundant(
        &mut self,
        _rdnt: fn(&RWSet<V>, Option<&Dot>, bool, &RWSet<V>, &Dot) -> bool,
        op: &RWSet<V>,
        _dot: &Dot,
    ) {
        match op {
            RWSet::Add(v) => {
                self.0.remove(v);
            }
            RWSet::Remove(v) => {
                self.0.remove(v);
                self.1.retain(|o| matches!(o, RWSet::Remove(v2) if v != v2));
            }
            RWSet::Clear => {
                self.0.clear();
            }
        }
    }

    fn apply(&mut self, value: RWSet<V>) {
        match value {
            RWSet::Add(v) => {
                self.0.insert(v);
            }
            RWSet::Remove(_) => {
                self.1.push(value);
            }
            _ => {}
        }
    }
}

impl<V> PureCRDT for RWSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;
    type Stable = (HashSet<V>, Vec<RWSet<V>>);

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, RWSet::Clear)
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        !is_conc
            && (matches!(new_op, RWSet::Clear)
                || match (&old_op, &new_op) {
                    (RWSet::Add(v1), RWSet::Add(v2))
                    | (RWSet::Remove(v1), RWSet::Remove(v2))
                    | (RWSet::Add(v1), RWSet::Remove(v2))
                    | (RWSet::Remove(v1), RWSet::Add(v2)) => v1 == v2,
                    _ => false,
                })
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

    fn stabilize(dot: &Dot, state: &mut EventGraph<Self>) {
        //Get the op
        let op = state.get_op(dot).unwrap();

        let is_stable_or_unstable = |v: &V| {
            // Is there an already stable op (add or rmv) with the same value?
            state.stable.1
                .iter()
                .any(|o| match o {
                    RWSet::Remove(v2) => v == v2,
                    _ => false,
                })
                || state.stable.0.contains(v)
            // Is there another unstable op (add or rmv, not the current op) with the same value?
            || state.unstable.node_indices().zip(state.unstable.node_weights()).any(|(idx, op)| {
				let other_dot = state.dot_index_map.nx_to_dot(&idx).unwrap();
				match &op.0 {
					RWSet::Add(v2) | RWSet::Remove(v2) => v == v2 && other_dot != dot,
					_ => false,
				}
			})
        };

        // Should we remove the op?
        let to_remove = match &op {
            // If it's a 'add' op, remove it if another operation with the same value exists
            RWSet::Add(v) => is_stable_or_unstable(v),
            // If it's a 'remove' op, remove it if there is no 'add' op with the same value
            RWSet::Remove(v) => {
                !state
                    .stable
                    .1
                    .iter()
                    .any(|o| matches!(o, RWSet::Add(v2) if v == v2))
                    && !state
                        .unstable
                        .node_indices()
                        .zip(state.unstable.node_weights())
                        .any(|(idx, op)| {
                            let other_dot = state.dot_index_map.nx_to_dot(&idx).unwrap();
                            matches!(&op.0, RWSet::Add(v2) if v == v2 && dot != other_dot)
                        })
            }
            RWSet::Clear => true,
        };

        // If it's a 'add' op and there exists a stable remove op with the same value, remove it
        if let RWSet::Add(v) = op {
            if let Some(i) = state
                .stable
                .1
                .iter()
                .position(|o| matches!(o, RWSet::Remove(v2) if v == *v2))
            {
                state.stable.1.remove(i);
            }
        }

        if to_remove {
            state.remove_dot(dot);
        }
    }

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut set = stable.0.clone();
        for o in stable.1.iter().chain(unstable.iter()) {
            if let RWSet::Add(v) = o {
                if stable.1.iter().chain(unstable.iter()).all(|e| {
                    if let RWSet::Remove(v2) = e {
                        v != v2
                    } else {
                        true
                    }
                }) {
                    set.insert(v.clone());
                }
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::crdt::{rw_set::RWSet, test_util::twins_graph};

    #[test_log::test]
    fn clear_rw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<RWSet<&str>>();

        let event = tcsb_a.tc_bcast(RWSet::Add("a"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(RWSet::Add("b"));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(RWSet::Clear);
        tcsb_b.try_deliver(event);

        let result = HashSet::new();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    // Following tests are reproduction of same simulation in Figure 18 of the “Pure Operation-Based CRDTs” paper.

    #[test_log::test]
    fn case_one() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<RWSet<&str>>();
        let event = tcsb_a.tc_bcast(RWSet::Add("a"));
        tcsb_b.try_deliver(event);

        let result = HashSet::from(["a"]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn case_two() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<RWSet<&str>>();

        let event_a = tcsb_a.tc_bcast(RWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(RWSet::Add("a"));

        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        assert_eq!(tcsb_a.state.unstable.node_count(), 1);
        assert_eq!(tcsb_b.state.unstable.node_count(), 1);

        let result = HashSet::from(["a"]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn case_three() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<RWSet<&str>>();

        let event_a = tcsb_a.tc_bcast(RWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(RWSet::Remove("a"));
        let event_a_2 = tcsb_a.tc_bcast(RWSet::Remove("a"));

        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a_2);

        let result = HashSet::from([]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn case_five() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<RWSet<&str>>();
        let event = tcsb_a.tc_bcast(RWSet::Remove("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.state.unstable.node_count(), 1);
        assert_eq!(tcsb_b.state.unstable.node_count(), 0);

        let result = HashSet::from([]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_remove() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<RWSet<&str>>();

        let event_b = tcsb_b.tc_bcast(RWSet::Remove("a"));
        let event_a = tcsb_a.tc_bcast(RWSet::Add("a"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        let result = HashSet::from([]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        convergence_checker::<EventGraph<RWSet<&str>>>(
            &[RWSet::Add("a"), RWSet::Remove("a"), RWSet::Clear],
            HashSet::new(),
        );
    }
}
