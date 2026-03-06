use std::{fmt::Debug, marker::PhantomData};

use moirai_protocol::{
    crdt::{
        eval::Eval,
        policy::Policy,
        pure_crdt::PureCRDT,
        query::{QueryOperation, Read},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
};

use crate::policy::{FairPolicy, LwwPolicy};

pub type LwwRegister<V> = Register<V, LwwPolicy>;
pub type FairRegister<V> = Register<V, FairPolicy>;

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum Register<V, P> {
    Write(V),
    // TODO: find a better design pattern
    __Marker(std::convert::Infallible, PhantomData<P>),
}

impl<V, P> PureCRDT for Register<V, P>
where
    V: Default + Debug + Clone,
    P: Policy,
{
    type Value = V;
    type StableState = Vec<Self>;
    const DISABLE_R_WHEN_R: bool = true;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        mut unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        unstable.any(|old_tagged_op| {
            P::compare(new_tagged_op.tag(), old_tagged_op.tag()) == std::cmp::Ordering::Less
        })
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        old_tag: Option<&Tag>,
        _is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        if let Some(old_tag) = old_tag {
            P::compare(new_tagged_op.tag(), old_tag) == std::cmp::Ordering::Greater
        } else {
            true
        }
    }
}

impl<V, P> Eval<Read<<Self as PureCRDT>::Value>> for Register<V, P>
where
    V: Default + Debug + Clone,
    P: Policy,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &<Register<V, P> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut value = V::default();
        for op in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            match op {
                Register::Write(v) => value = v.clone(),
                _ => unreachable!(),
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{
        policy::LwwPolicy,
        register::unique_register::Register,
        utils::membership::{triplet, twins},
    };

    #[test]
    pub fn lww_register_with_write() {
        let (mut replica_a, mut replica_b) = twins::<Register<String, LwwPolicy>>();

        let event = replica_a
            .send(Register::Write("Hello".to_string()))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(Register::Write("World".to_string()))
            .unwrap();
        replica_b.receive(event);

        let result = "World".to_string();
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    pub fn lww_register_concurrent_writes() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet::<Register<String, LwwPolicy>>();

        let event_a = replica_a
            .send(Register::Write("Hello".to_string()))
            .unwrap();
        assert!(replica_a.query(Read::new()) == "Hello");
        let event_b = replica_b
            .send(Register::Write("World".to_string()))
            .unwrap();
        assert!(replica_b.query(Read::new()) == "World");

        replica_a.receive(event_b.clone());
        assert_eq!(replica_a.query(Read::new()), "World");
        replica_b.receive(event_a.clone());
        assert_eq!(replica_b.query(Read::new()), "World");
        replica_c.receive(event_a);
        assert_eq!(replica_c.query(Read::new()), "Hello");
        replica_c.receive(event_b);
        assert_eq!(replica_c.query(Read::new()), "World");
    }

    #[test]
    pub fn lww_register_more_concurrent() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet::<Register<String, LwwPolicy>>();

        let event_c_1 = replica_c.send(Register::Write("x".to_string())).unwrap();
        replica_a.receive(event_c_1.clone());

        let event_a_1 = replica_a.send(Register::Write("y".to_string())).unwrap();

        let event_b_1 = replica_b.send(Register::Write("z".to_string())).unwrap();
        replica_c.receive(event_b_1.clone());

        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_a_1.clone());

        replica_c.receive(event_a_1.clone());
        replica_a.receive(event_b_1);

        assert_eq!(replica_a.query(Read::new()), "y".to_string());
        assert_eq!(replica_b.query(Read::new()), "y".to_string());
        assert_eq!(replica_c.query(Read::new()), "y".to_string());
    }

    // TODO: fuzzer
}
