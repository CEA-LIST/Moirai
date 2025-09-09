use std::fmt::Debug;

use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
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
        unstable.any(|old_tagged_op| {
            let cmp = new_tagged_op.tag() < old_tagged_op.tag();
            tracing::info!(
                "compare {} < {}: result {}. if true, then new is redundant.",
                new_tagged_op,
                old_tagged_op,
                cmp
            );
            cmp
        })
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

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut value = V::default();
        for op in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            match op {
                LWWRegister::Write(v) => value = v.clone(),
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            register::lww_register::LWWRegister,
            test_util::{triplet, twins},
        },
        protocol::replica::IsReplica,
    };

    #[test]
    pub fn lww_register_with_write() {
        let (mut replica_a, mut replica_b) = twins::<LWWRegister<String>>();

        let event = replica_a.send(LWWRegister::Write("Hello".to_string()));
        replica_b.receive(event);

        let event = replica_a.send(LWWRegister::Write("World".to_string()));
        replica_b.receive(event);

        let result = "World".to_string();
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    pub fn lww_register_concurrent_writes() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<LWWRegister<String>>();

        let event_a = replica_a.send(LWWRegister::Write("Hello".to_string()));
        assert!(replica_a.query() == "Hello");
        let event_b = replica_b.send(LWWRegister::Write("World".to_string()));
        assert!(replica_b.query() == "World");

        replica_a.receive(event_b.clone());
        assert_eq!(replica_a.query(), "World");
        replica_b.receive(event_a.clone());
        assert_eq!(replica_b.query(), "World");
        replica_c.receive(event_a);
        assert_eq!(replica_c.query(), "Hello");
        replica_c.receive(event_b);
        assert_eq!(replica_c.query(), "World");
    }

    #[test]
    pub fn lww_register_more_concurrent() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<LWWRegister<String>>();

        let event_c_1 = replica_c.send(LWWRegister::Write("x".to_string()));
        replica_a.receive(event_c_1.clone());

        let event_a_1 = replica_a.send(LWWRegister::Write("y".to_string()));

        let event_b_1 = replica_b.send(LWWRegister::Write("z".to_string()));
        replica_c.receive(event_b_1.clone());

        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_a_1.clone());

        replica_c.receive(event_a_1.clone());
        replica_a.receive(event_b_1);

        assert_eq!(replica_a.query(), "y".to_string());
        assert_eq!(replica_b.query(), "y".to_string());
        assert_eq!(replica_c.query(), "y".to_string());
    }

    //     #[cfg(feature = "op_weaver")]
    //     #[test]
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
}
