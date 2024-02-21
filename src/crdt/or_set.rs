use crate::trcb::{Event, OpRules};
use std::{collections::HashSet, fmt::Debug, hash::Hash};

#[derive(Clone, Debug)]
pub enum Operation<V> {
    Add(V),
    Remove(V),
}

impl<V> OpRules<&str, u32> for Operation<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = HashSet<V>;

    fn obsolete(is_obsolete: &Event<&str, u32, Self>, other: &Event<&str, u32, Self>) -> bool {
        match (&is_obsolete.op, &other.op) {
            (Operation::Remove(_), _) => true,
            (Operation::Add(v1), Operation::Add(v2))
            | (Operation::Add(v1), Operation::Remove(v2)) => is_obsolete.vc < other.vc && v1 == v2,
        }
    }

    fn eval(unstable_events: &[Event<&str, u32, Self>], stable_events: &[Self]) -> Self::Value {
        let mut set = Self::Value::new();
        // No "remove" operation can be in the stable set
        for op in stable_events {
            if let Operation::Add(v) = op {
                set.insert(v.clone());
            }
        }
        for event in unstable_events {
            if let Operation::Add(v) = &event.op {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{crdt::or_set::Operation, trcb::Trcb};

    #[test]
    fn test_or_set() {
        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new("A");
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new("B");

        trcb_a.new_peer(&"B");
        trcb_b.new_peer(&"A");

        let event_a = trcb_a.tc_bcast(Operation::Add("A"));
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.lvv.get(&"A"), Some(1));
        assert_eq!(trcb_b.lvv.get(&"A"), Some(1));

        let event_b = trcb_b.tc_bcast(Operation::Add("B"));
        trcb_a.tc_deliver(event_b);

        assert_eq!(trcb_a.lvv.get(&"B"), Some(1));
        assert_eq!(trcb_a.lvv.get(&"B"), Some(1));

        let event_a = trcb_a.tc_bcast(Operation::Remove("A"));
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.lvv.get(&"A"), Some(2));

        let event_b = trcb_b.tc_bcast(Operation::Add("C"));
        trcb_a.tc_deliver(event_b);

        assert_eq!(trcb_a.lvv.get(&"B"), Some(2));
        assert_eq!(trcb_a.eval(), trcb_b.eval(),);
    }

    #[test]
    fn test_concurrent_remove() {
        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new("A");
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new("B");

        trcb_a.new_peer(&"B");
        trcb_b.new_peer(&"A");

        let event_a = trcb_a.tc_bcast(Operation::Add("A"));
        trcb_b.tc_deliver(event_a);
        let event_b = trcb_b.tc_bcast(Operation::Add("B"));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Operation::Remove("A"));
        let event_b = trcb_b.tc_bcast(Operation::Add("A"));
        trcb_a.tc_deliver(event_b);
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), HashSet::from(["A", "B"]));
        assert_eq!(trcb_b.eval(), trcb_a.eval());
    }
}
