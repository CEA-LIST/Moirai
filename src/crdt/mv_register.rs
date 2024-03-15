use crate::protocol::{
    event::{Event, Message},
    op_rules::OpRules,
};
use serde::Serialize;
use std::{
    cmp::Ordering,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign},
};

#[derive(Clone, Debug)]
pub struct Op<V>(pub V);

impl<V> OpRules for Op<V>
where
    V: Debug + Clone + Eq + Hash + Serialize,
{
    type Value = Vec<V>;

    fn obsolete<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        is_obsolete: &Event<K, C, Self>,
        other: &Event<K, C, Self>,
    ) -> bool {
        let cmp = is_obsolete.vc.partial_cmp(&other.vc);
        match cmp {
            Some(ord) => match ord {
                Ordering::Less => true,
                Ordering::Equal | Ordering::Greater => false,
            },
            None => false,
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
        for op in stable_events {
            set.push(op.0.clone());
        }
        for event in unstable_events {
            if let Message::Op(op) = &event.message {
                set.push(op.0.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::mv_register::Op,
        protocol::{
            event::{Message, Signal},
            trcb::Trcb,
        },
    };
    use uuid::Uuid;

    #[test_log::test]
    fn test_mv_register_concurrent() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Op<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Op<&str>>::new(id_b.as_str());

        let event_a = trcb_a.tc_bcast(Message::Signal(Signal::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Signal(Signal::Join));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op("A")));
        let event_b = trcb_b.tc_bcast(Message::Op(Op("B")));

        trcb_a.tc_deliver(event_b);
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), vec!["A", "B"]);
        assert_eq!(trcb_b.eval(), trcb_b.eval());
    }

    #[test_log::test]
    fn test_mv_register() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Op<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Op<&str>>::new(id_b.as_str());

        let event_a = trcb_a.tc_bcast(Message::Signal(Signal::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Signal(Signal::Join));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op("A")));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Op(Op("B")));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op("C")));
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), vec!["C"]);
        assert_eq!(trcb_b.eval(), trcb_a.eval());
    }
}
