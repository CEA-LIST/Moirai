use std::fmt::Debug;

use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    event::{tag::Tag, tagged_op::TaggedOp},
};

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum LWWRegister<V> {
    Write(V),
}

impl<V: Default + Debug + Clone> PureCRDT for LWWRegister<V> {
    type Value = V;
    type StableState = Vec<Self>;
    const DISABLE_R_WHEN_R: bool = true;

    /// a -> b => Lamport(a) < Lamport(b)
    /// Lamport(a) < Lamport(b) => a -> b || a conc b
    /// Because of the causal broadcast, new_op can only be concurrent or causally after old_op.
    /// The new op is redundant if there is an old op that is concurrent to it and has a higher origin identifier.
    /// i.e. (t, o) R s = \exists (t', o') \in s : t ≮ t' \land t.id < t'.id
    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        mut unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        unstable.any(|old_tagged_op| new_tagged_op.tag() < old_tagged_op.tag())
    }

    /// (t, o) R (t', o') = t < t' || (t == t' && t.id < t'.id)
    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        old_tag: Option<&Tag>,
        _is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        if let Some(old_tag) = old_tag {
            new_tagged_op.tag() > old_tag
        } else {
            true
        }
    }

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        V: 'a,
    {
        let mut value = V::default();
        for op in stable.iter().chain(unstable.map(|t| t.op())) {
            match op {
                LWWRegister::Write(v) => value = v.clone(),
            }
        }
        value
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::{
//         crdt::{
//             register::lww_register::LWWRegister,
//             test_util::{triplet, twins},
//         },
//         protocol::event_graph::EventGraph,
//     };

//     #[test_log::test]
//     pub fn lww_register_with_write() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<LWWRegister<String>>>();

//         let event = tcsb_a.tc_bcast(LWWRegister::Write("Hello".to_string()));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(LWWRegister::Write("World".to_string()));
//         tcsb_b.try_deliver(event);

//         let result = "World".to_string();
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     pub fn lww_register_concurrent_writes() {
//         let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<EventGraph<LWWRegister<String>>>();

//         let event_a = tcsb_a.tc_bcast(LWWRegister::Write("Hello".to_string()));
//         assert!(tcsb_a.eval() == "Hello");
//         let event_b = tcsb_b.tc_bcast(LWWRegister::Write("World".to_string()));
//         assert!(tcsb_b.eval() == "World");

//         tcsb_a.try_deliver(event_b.clone());
//         tcsb_b.try_deliver(event_a.clone());
//         tcsb_c.try_deliver(event_a);
//         tcsb_c.try_deliver(event_b);

//         assert!(tcsb_a.eval() == "World");
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//         assert_eq!(tcsb_a.eval(), tcsb_c.eval());
//     }

//     #[test_log::test]
//     pub fn lww_register_more_concurrent() {
//         let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<EventGraph<LWWRegister<String>>>();

//         let event_c_1 = tcsb_c.tc_bcast(LWWRegister::Write("x".to_string()));
//         tcsb_a.try_deliver(event_c_1.clone());

//         let event_a_1 = tcsb_a.tc_bcast(LWWRegister::Write("y".to_string()));

//         let event_b_1 = tcsb_b.tc_bcast(LWWRegister::Write("z".to_string()));
//         tcsb_c.try_deliver(event_b_1.clone());

//         tcsb_b.try_deliver(event_c_1.clone());
//         tcsb_b.try_deliver(event_a_1.clone());

//         tcsb_c.try_deliver(event_a_1.clone());
//         tcsb_a.try_deliver(event_b_1);

//         assert_eq!(tcsb_a.eval(), "y".to_string());
//         assert_eq!(tcsb_b.eval(), "y".to_string());
//         assert_eq!(tcsb_c.eval(), "y".to_string());
//     }

//     #[cfg(feature = "op_weaver")]
//     #[test_log::test]
//     fn generate_lww_register_convergence() {
//         use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

//         let ops = vec![
//             LWWRegister::Write("w".to_string()),
//             LWWRegister::Write("x".to_string()),
//             LWWRegister::Write("y".to_string()),
//             LWWRegister::Write("z".to_string()),
//             LWWRegister::Write("u".to_string()),
//             LWWRegister::Write("v".to_string()),
//         ];

//         let config = EventGraphConfig {
//             name: "lww_register",
//             num_replicas: 8,
//             num_operations: 10_000,
//             operations: &ops,
//             final_sync: true,
//             churn_rate: 0.3,
//             reachability: None,
//             compare: |a: &String, b: &String| a == b,
//             record_results: true,
//             seed: None,
//             witness_graph: false,
//             concurrency_score: false,
//         };

//         op_weaver::<EventGraph<LWWRegister<String>>>(config);
//     }
// }
