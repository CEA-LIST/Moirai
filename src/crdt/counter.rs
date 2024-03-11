use std::{fmt::Debug, ops::Add};

use crate::trcb::{Event, OpRules};

#[derive(Clone, Debug)]
pub struct Operation<V>(pub V);

impl<V> OpRules<&str, u32> for Operation<V>
where
    V: Add<V, Output = V> + Debug + Clone + Default,
{
    type Value = V;

    fn obsolete(_: &Event<&str, u32, Self>, _: &Event<&str, u32, Self>) -> bool {
        false
    }

    fn eval(unstable_events: &[Event<&str, u32, Self>], stable_events: &[Self]) -> Self::Value {
        stable_events
            .iter()
            .map(|event| event.0.clone())
            .fold(V::default(), |acc, x| acc + x)
            + unstable_events
                .iter()
                .map(|event| event.op.0.clone())
                .fold(V::default(), |acc, x| acc + x)
    }
}

#[cfg(test)]
mod tests {
    use crate::{crdt::counter::Operation, trcb::Trcb};
    use uuid::Uuid;

    #[test_log::test]
    fn test_counter() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Operation<i32>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Operation<i32>>::new(id_b.as_str());

        trcb_a.new_peer(&id_b.as_str());
        trcb_b.new_peer(&id_a.as_str());

        let event_a = trcb_a.tc_bcast(Operation(12));
        let event_b = trcb_b.tc_bcast(Operation(5));

        trcb_a.tc_deliver(event_b);
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), 17);
        assert_eq!(trcb_b.eval(), trcb_b.eval());
    }
}
