use crate::trcb::{Event, OpRules};
use std::{cmp::Ordering, fmt::Debug, hash::Hash};

#[derive(Clone, Debug)]
pub struct Operation<V>(pub V);

impl<V> OpRules<&str, u32> for Operation<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = V;

    fn obsolete(is_obsolete: &Event<&str, u32, Self>, other: &Event<&str, u32, Self>) -> bool {
        let cmp = is_obsolete.vc.partial_cmp(&other.vc);
        match cmp {
            Some(ord) => match ord {
                Ordering::Less => true,
                Ordering::Equal => false,
                Ordering::Greater => false,
            },
            None => {
                println!("Concurrent events");
                match is_obsolete.wc.cmp(&other.wc) {
                    Ordering::Less => {
                        println!("LESS");
                        true
                    }
                    Ordering::Equal => {
                        println!("EQUAL");
                        is_obsolete.origin < other.origin
                    }
                    Ordering::Greater => {
                        println!("GREATER");
                        false
                    }
                }
            }
        }
    }

    fn eval(unstable_events: &[Event<&str, u32, Self>], stable_events: &[Self]) -> Self::Value {
        let mut value = None;
        for event in unstable_events {
            value = Some(event.op.0.clone());
        }
        for event in stable_events {
            value = Some(event.0.clone());
        }
        value.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{crdt::lww_register::Operation, trcb::Trcb};
    use uuid::Uuid;

    #[test_log::test]
    fn test_lww_register() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new(id_b.as_str());

        trcb_a.new_peer(&id_b.as_str());
        trcb_b.new_peer(&id_a.as_str());

        let event_a = trcb_a.tc_bcast(Operation("A"));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Operation("B"));
        trcb_a.tc_deliver(event_b);

        let event_a = trcb_a.tc_bcast(Operation("C"));
        trcb_b.tc_deliver(event_a);

        assert_eq!(trcb_a.eval(), trcb_b.eval());
        assert_eq!(trcb_a.eval(), "C");
    }

    #[test_log::test]
    fn test_concurrent_lww_register() {
        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new(id_b.as_str());

        trcb_a.new_peer(&id_b.as_str());
        trcb_b.new_peer(&id_a.as_str());

        let event_a = trcb_a.tc_bcast(Operation("A"));
        println!("{}", event_a.wc < event_a.wc);
        let event_b = trcb_b.tc_bcast(Operation("B"));
        println!("DELIVERING");
        trcb_a.tc_deliver(event_b.clone());
        trcb_b.tc_deliver(event_a.clone());

        assert_eq!(trcb_a.eval(), trcb_b.eval());

        if event_a.wc < event_a.wc {
            assert_eq!(trcb_a.eval(), "B");
        } else if event_a.wc > event_a.wc {
            assert_eq!(trcb_a.eval(), "A");
        } else {
            if event_a.origin < event_b.origin {
                assert_eq!(trcb_a.eval(), "B");
            } else {
                assert_eq!(trcb_a.eval(), "A");
            }
        }
    }
}
