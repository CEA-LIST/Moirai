use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::po_log::POLog;
use crate::protocol::pure_crdt::PureCRDT;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum RWSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

impl<V> PureCRDT for RWSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;

    fn r(event: &Event<Self>, _state: &POLog<Self>) -> bool {
        matches!(event.op, RWSet::Clear)
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        old_event.metadata.clock < new_event.metadata.clock
            && (matches!(new_event.op, RWSet::Clear)
                || match (&old_event.op, &new_event.op) {
                    (RWSet::Add(v1), RWSet::Add(v2))
                    | (RWSet::Remove(v1), RWSet::Remove(v2))
                    | (RWSet::Add(v1), RWSet::Remove(v2))
                    | (RWSet::Remove(v1), RWSet::Add(v2)) => v1 == v2,
                    _ => false,
                })
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(metadata: &Metadata, state: &mut POLog<Self>) {
        // Get the op
        let op = state.unstable.get(metadata).unwrap();

        let is_stable_or_unstable = |v: &V| {
            // Is there an already stable op (add or rmv) with the same value?
            state.stable.iter().any(|o| match o.as_ref() {
                RWSet::Add(v2) | RWSet::Remove(v2) => v == v2,
                _ => false,
            })
            // Is there another unstable op (add or rmv, not the current op) with the same value?
            || state.unstable.iter().any(|(t, o)| match o.as_ref() {
                RWSet::Add(v2) | RWSet::Remove(v2) => v == v2 && metadata.clock != t.clock,
                _ => false,
            })
        };

        // Should we remove the op?
        let to_remove = match op.as_ref() {
            // If it's a 'add' op, remove it if another operation with the same value exists
            RWSet::Add(v) => is_stable_or_unstable(v),
            // If it's a 'remove' op, remove it if there is no 'add' op with the same value
            RWSet::Remove(v) => !state
                .stable
                .iter()
                .any(|o| matches!(o.as_ref(), RWSet::Add(v2) if v == v2))
                && !state.unstable.iter().any(
                    |(t, o)| matches!(o.as_ref(), RWSet::Add(v2) if v == v2 && metadata.clock != t.clock),
                ),
            RWSet::Clear => true,
        };

        // If it's a 'add' op and there exists a stable remove op with the same value, remove it
        if let RWSet::Add(v) = op.as_ref() {
            if let Some(i) = state
                .stable
                .iter()
                .position(|o| matches!(o.as_ref(), RWSet::Remove(v2) if v == v2))
            {
                state.stable.remove(i);
            }
        }

        if to_remove {
            state.unstable.remove(metadata);
        }
    }

    fn eval(state: &POLog<Self>) -> Self::Value {
        let mut set = Self::Value::new();
        for o in state.iter() {
            if let RWSet::Add(v) = o.as_ref() {
                if state.iter().all(|e| {
                    if let RWSet::Remove(v2) = e.as_ref() {
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

    use crate::crdt::{rw_set::RWSet, test_util::twins};

    #[test_log::test]
    fn clear_rw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<RWSet<&str>>();

        let event = tcsb_a.tc_bcast(RWSet::Add("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast(RWSet::Add("b"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 2);

        let event = tcsb_a.tc_bcast(RWSet::Clear);
        tcsb_b.try_deliver(event);

        let result = HashSet::new();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    // Following tests are reproduction of same simulation in Figure 18 of the “Pure Operation-Based CRDTs” paper.

    #[test_log::test]
    fn case_one() {
        let (mut tcsb_a, mut tcsb_b) = twins::<RWSet<&str>>();
        let event = tcsb_a.tc_bcast(RWSet::Add("a"));
        tcsb_b.try_deliver(event);

        let result = HashSet::from(["a"]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn case_two() {
        let (mut tcsb_a, mut tcsb_b) = twins::<RWSet<&str>>();
        let event_a = tcsb_a.tc_bcast(RWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(RWSet::Add("a"));

        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        assert_eq!(tcsb_a.state.stable.len(), 0);
        assert_eq!(tcsb_a.state.unstable.len(), 1);
        assert_eq!(tcsb_b.state.stable.len(), 0);
        assert_eq!(tcsb_b.state.unstable.len(), 1);

        let result = HashSet::from(["a"]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn case_three() {
        let (mut tcsb_a, mut tcsb_b) = twins::<RWSet<&str>>();

        let event_a = tcsb_a.tc_bcast(RWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(RWSet::Remove("a"));
        let event_a_2 = tcsb_a.tc_bcast(RWSet::Remove("a"));

        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a_2);

        assert_eq!(tcsb_a.state.stable.len(), 0);
        assert_eq!(tcsb_a.state.unstable.len(), 1);
        assert_eq!(tcsb_b.state.stable.len(), 0);
        assert_eq!(tcsb_b.state.unstable.len(), 1);

        let result = HashSet::from([]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn case_five() {
        let (mut tcsb_a, mut tcsb_b) = twins::<RWSet<&str>>();
        let event = tcsb_a.tc_bcast(RWSet::Remove("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 0);
        assert_eq!(tcsb_a.state.unstable.len(), 1);
        assert_eq!(tcsb_b.state.stable.len(), 0);
        assert_eq!(tcsb_b.state.unstable.len(), 0);

        let result = HashSet::from([]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_remove() {
        let (mut tcsb_a, mut tcsb_b) = twins::<RWSet<&str>>();

        let event_b = tcsb_b.tc_bcast(RWSet::Remove("a"));
        let event_a = tcsb_a.tc_bcast(RWSet::Add("a"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        let result = HashSet::from([]);
        assert_eq!(tcsb_b.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
