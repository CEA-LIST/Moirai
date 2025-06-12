use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};
use std::fmt::Debug;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum LWWRegister<V> {
    Write(V),
    Clear,
}

impl<V: Default + Debug + Clone> PureCRDT for LWWRegister<V> {
    type Value = V;
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self) -> bool {
        matches!(new_op, LWWRegister::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _is_conc: bool,
        order: bool,
        _new_op: &Self,
    ) -> bool {
        !order
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        is_conc: bool,
        order: bool,
        new_op: &Self,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, is_conc, order, new_op)
    }

    fn stabilize(_dot: &Dot, _state: &mut EventGraph<Self>) {}

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut value = V::default();
        for op in stable.iter().chain(unstable.iter()) {
            match op {
                LWWRegister::Write(v) => value = v.clone(),
                LWWRegister::Clear => value = V::default(),
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{lww_register::LWWRegister, test_util::twins};
    use crate::protocol::event_graph::EventGraph;

    #[test_log::test]
    pub fn simple_lww_register() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<LWWRegister<String>>>();

        let event = tcsb_a.tc_bcast(LWWRegister::Write("Hello".to_string()));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(LWWRegister::Clear);
        tcsb_b.try_deliver(event);

        let result = String::default();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    pub fn lww_register_with_write() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<LWWRegister<String>>>();

        let event = tcsb_a.tc_bcast(LWWRegister::Write("Hello".to_string()));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(LWWRegister::Write("World".to_string()));
        tcsb_b.try_deliver(event);

        let result = "World".to_string();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    pub fn lww_register_concurrent_writes() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<LWWRegister<String>>>();

        let event_a = tcsb_a.tc_bcast(LWWRegister::Write("Hello".to_string()));
        let event_b = tcsb_b.tc_bcast(LWWRegister::Write("World".to_string()));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert!(tcsb_a.eval() == "World");
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
