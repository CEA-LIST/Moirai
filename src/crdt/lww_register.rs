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

    fn redundant_itself(new_op: &Self, new_dot: &Dot, state: &EventGraph<Self>) -> bool {
        if matches!(new_op, LWWRegister::Clear) {
            true
        } else {
            let predecessors = state.causal_predecessors(new_dot);
            let is_not_redundant = state.non_tombstones.iter().any(|nx| {
                // Create a total order for the operations
                // true if old_op > new_op, false otherwise
                // if conc, we compare on the lexicographic order of process ids
                !predecessors.contains(nx) && state.dot_index_map.nx_to_dot(nx).unwrap().origin() > new_dot.origin()
            });
            !is_not_redundant
        }
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        !is_conc && matches!(new_op, LWWRegister::Clear)
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        old_dot: Option<&Dot>,
        is_conc: bool,
        _new_op: &Self,
        new_dot: &Dot,
    ) -> bool {
        !is_conc || old_dot.unwrap().origin() < new_dot.origin()
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
    use crate::crdt::test_util::triplet;
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
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<EventGraph<LWWRegister<String>>>();

        let event_a = tcsb_a.tc_bcast(LWWRegister::Write("Hello".to_string()));
        let event_b = tcsb_b.tc_bcast(LWWRegister::Write("World".to_string()));

        tcsb_a.try_deliver(event_b.clone());
        tcsb_b.try_deliver(event_a.clone());
        tcsb_c.try_deliver(event_a);
        tcsb_c.try_deliver(event_b);

        assert!(tcsb_a.eval() == "World");
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
        assert_eq!(tcsb_a.eval(), tcsb_c.eval());
    }
}
