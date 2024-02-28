use crate::trcb::{Event, OpRules};
use std::{cmp::Ordering, fmt::Debug, hash::Hash};

#[derive(Clone, Debug)]
pub struct Operation<V>(pub V);

impl<V> OpRules<&str, u32> for Operation<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = Vec<V>;

    fn obsolete(is_obsolete: &Event<&str, u32, Self>, other: &Event<&str, u32, Self>) -> bool {
        let cmp = is_obsolete.vc.partial_cmp(&other.vc);
        match cmp {
            Some(ord) => match ord {
                Ordering::Less => true,
                Ordering::Equal | Ordering::Greater => false,
            },
            None => false,
        }
    }

    fn eval(unstable_events: &[Event<&str, u32, Self>], stable_events: &[Self]) -> Self::Value {
        let mut set = Self::Value::new();
        for op in stable_events {
            set.push(op.0.clone());
        }
        for event in unstable_events {
            set.push(event.op.0.clone());
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use crate::{crdt::mv_register::Operation, trcb::Trcb};

    #[test_log::test]
    fn test_mv_register_concurrent() {
        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new("A");
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new("B");

        trcb_a.new_peer(&"B");
        trcb_b.new_peer(&"A");

        let event_a = trcb_a.tc_bcast(Operation("A"));
        let event_b = trcb_b.tc_bcast(Operation("B"));

        trcb_a.tc_deliver(event_b);
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), vec!["A", "B"]);
        assert_eq!(trcb_b.eval(), trcb_b.eval());
    }

    #[test_log::test]
    fn test_mv_register() {
        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new("A");
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new("B");

        trcb_a.new_peer(&"B");
        trcb_b.new_peer(&"A");

        let event_a = trcb_a.tc_bcast(Operation("A"));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Operation("B"));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Operation("C"));
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), vec!["C"]);
        assert_eq!(trcb_b.eval(), trcb_a.eval());
    }
}
