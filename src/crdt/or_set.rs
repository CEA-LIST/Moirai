use crate::protocol::{event::OpEvent, op_rules::OpRules};
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
        is_obsolete: &OpEvent<K, C, Self>,
        other: &OpEvent<K, C, Self>,
    ) -> bool {
        match (&is_obsolete.op, &other.op) {
            (Op::Remove(_), _) => true,
            (Op::Add(v1), Op::Add(v2)) | (Op::Add(v1), Op::Remove(v2)) => {
                is_obsolete.metadata.vc < other.metadata.vc && v1 == v2
            }
        }
    }

    fn eval<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        unstable_events: &[&OpEvent<K, C, Self>],
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
            if let Op::Add(v) = &event.op {
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
            event::{Message, ProtocolCmd},
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

        let event_a = trcb_a.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
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

        let event_a = trcb_a.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
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
