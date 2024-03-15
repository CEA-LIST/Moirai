use serde::Serialize;
use std::hash::Hash;
use std::ops::AddAssign;
use std::{fmt::Debug, ops::Add};

use crate::protocol::event::{Event, Message};
use crate::protocol::op_rules::OpRules;

#[derive(Clone, Debug)]
pub struct Op<V>(pub V);

impl<V> OpRules for Op<V>
where
    V: Add<V, Output = V> + Debug + Clone + Default + Serialize,
{
    type Value = V;

    fn obsolete<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        _: &Event<K, C, Self>,
        _: &Event<K, C, Self>,
    ) -> bool {
        false
    }

    fn eval<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        unstable_events: &[Event<K, C, Self>],
        stable_events: &[Self],
    ) -> Self::Value {
        stable_events
            .iter()
            .map(|event| event.0.clone())
            .fold(V::default(), |acc, x| acc + x)
            + unstable_events
                .iter()
                .filter_map(|event| match &event.message {
                    Message::Op(op) => Some(op.0.clone()),
                    Message::Signal(_) => None,
                })
                .fold(V::default(), |acc, x| acc + x)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::counter::Op,
        protocol::{
            event::{Message, Signal},
            trcb::Trcb,
        },
    };
    use uuid::Uuid;

    #[test_log::test]
    fn test_counter() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Op<i32>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Op<i32>>::new(id_b.as_str());

        let event_a = trcb_a.tc_bcast(Message::Signal(Signal::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Signal(Signal::Join));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Message::Op(Op(12)));
        let event_b = trcb_b.tc_bcast(Message::Op(Op(5)));

        trcb_a.tc_deliver(event_b);
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), 17);
        assert_eq!(trcb_b.eval(), trcb_b.eval());
    }
}
