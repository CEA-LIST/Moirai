use crate::protocol::{
    event::{Event, Message},
    op_rules::OpRules,
};
use serde::Serialize;
use std::{
    collections::HashSet,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign},
};

#[derive(Clone, Debug)]
pub enum Op<V> {
    Add(V),
    Remove(V),
}

impl<V> OpRules for Op<V>
where
    V: Debug + Clone + Eq + Hash + Serialize,
{
    type Value = HashSet<V>;

    fn obsolete<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        is_obsolete: &Event<K, C, Self>,
        other: &Event<K, C, Self>,
    ) -> bool {
        match (&is_obsolete.message, &other.message) {
            (_, Message::Signal(_)) => false,
            (Message::Signal(_), _) => false,
            (Message::Op(Op::Remove(_)), _) => true,
            (Message::Op(Op::Add(v1)), Message::Op(Op::Add(v2)))
            | (Message::Op(Op::Add(v1)), Message::Op(Op::Remove(v2))) => {
                is_obsolete.vc < other.vc && v1 == v2
            }
        }
    }

    fn eval<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        unstable_events: &[Event<K, C, Self>],
        stable_events: &[Self],
    ) -> Self::Value {
        let mut set = Self::Value::new();
        // No "remove" operation can be in the stable set
        for op in stable_events {
            if let Op::Add(v) = op {
                set.insert(v.clone());
            }
        }
        for event in unstable_events {
            if let Message::Op(Op::Add(v)) = &event.message {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::or_set::Op,
        protocol::{
            event::{Message, Signal},
            trcb::Trcb,
        },
    };
    use std::collections::HashSet;
    use uuid::Uuid;

    #[test_log::test]
    fn test_or_set() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Op<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Op<&str>>::new(id_b.as_str());

        let event_a = trcb_a.tc_bcast(Message::Signal(Signal::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Signal(Signal::Join));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op::Add("A")));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Op(Op::Add("B")));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op::Remove("A")));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Op(Op::Add("C")));
        trcb_a.tc_deliver(event_b);

        assert_eq!(trcb_a.eval(), trcb_b.eval(),);
    }

    #[test_log::test]
    fn test_concurrent_remove() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Op<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Op<&str>>::new(id_b.as_str());

        let event_a = trcb_a.tc_bcast(Message::Signal(Signal::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Signal(Signal::Join));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op::Add("A")));
        trcb_b.tc_deliver(event_a);
        let event_b = trcb_b.tc_bcast(Message::Op(Op::Add("B")));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op::Remove("A")));
        let event_b = trcb_b.tc_bcast(Message::Op(Op::Add("A")));
        trcb_a.tc_deliver(event_b);
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), HashSet::from(["A", "B"]));
        assert_eq!(trcb_b.eval(), trcb_a.eval());
    }
}
