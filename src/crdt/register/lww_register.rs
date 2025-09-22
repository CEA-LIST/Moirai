use std::fmt::Debug;

use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    event::{
        tag::{Lww, Tag},
        tagged_op::TaggedOp,
    },
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

    /// # Last-Writer-Wins (LWW) Register
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
        unstable.any(|old_tagged_op| Lww(new_tagged_op.tag()) < Lww(old_tagged_op.tag()))
    }

    /// # Last-Writer-Wins (LWW) Register
    /// (t, o) R (t', o') = t < t' || (t == t' && t.id < t'.id)
    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        old_tag: Option<&Tag>,
        _is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        if let Some(old_tag) = old_tag {
            Lww(new_tagged_op.tag()) > Lww(old_tag)
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

        let event = replica_a.send(LWWRegister::Write("Hello".to_string())).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(LWWRegister::Write("World".to_string())).unwrap();
        replica_b.receive(event);

        let result = "World".to_string();
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    pub fn lww_register_concurrent_writes() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<LWWRegister<String>>();

        let event_a = replica_a.send(LWWRegister::Write("Hello".to_string())).unwrap();
        assert!(replica_a.query() == "Hello");
        let event_b = replica_b.send(LWWRegister::Write("World".to_string())).unwrap();
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

        let event_c_1 = replica_c.send(LWWRegister::Write("x".to_string())).unwrap();
        replica_a.receive(event_c_1.clone());

        let event_a_1 = replica_a.send(LWWRegister::Write("y".to_string())).unwrap();

        let event_b_1 = replica_b.send(LWWRegister::Write("z".to_string())).unwrap();
        replica_c.receive(event_b_1.clone());

        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_a_1.clone());

        replica_c.receive(event_a_1.clone());
        replica_a.receive(event_b_1);

        assert_eq!(replica_a.query(), "y".to_string());
        assert_eq!(replica_b.query(), "y".to_string());
        assert_eq!(replica_c.query(), "y".to_string());
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_lww_register() {
        // init_tracing();

        use crate::{
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        let ops = OpConfig::Uniform(&[
            LWWRegister::Write("w"),
            LWWRegister::Write("x"),
            LWWRegister::Write("y"),
            LWWRegister::Write("z"),
            LWWRegister::Write("u"),
            LWWRegister::Write("v"),
        ]);

        let run = RunConfig::new(0.4, 8, 10_000, None, None);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<LWWRegister<&str>>>::new(
            "lww_register",
            runs,
            ops,
            true,
            |a, b| a == b,
            None,
        );

        fuzzer::<VecLog<LWWRegister<&str>>>(config);
    }
}
