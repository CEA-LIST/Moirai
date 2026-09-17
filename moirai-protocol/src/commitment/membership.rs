use std::fmt::Display;

use crate::{
    crdt::{
        eval::Eval,
        query::{QueryOperation, Read},
        redundancy::RedundancyRelation,
        replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    },
    event::tagged_op::TaggedOp,
    replica::ReplicaIdOwned,
    state::{
        stable_state::IsStableState,
        unstable_state::{IsUnstableCore, IsUnstablePrune},
    },
    utils::hashmap::HashSet,
};

#[derive(Debug, Clone)]
pub enum Membership {
    Join(ReplicaIdOwned),
    Leave(ReplicaIdOwned),
}

#[derive(Debug, Clone)]
pub enum MembershipRejection {
    AlreadyJoined(ReplicaIdOwned),
    NotJoined(ReplicaIdOwned),
}

impl Display for MembershipRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MembershipRejection::AlreadyJoined(id) => write!(f, "{} is already joined", id),
            MembershipRejection::NotJoined(id) => write!(f, "{} is not joined", id),
        }
    }
}

impl ReplicatedDataType for Membership {
    type StableState = HashSet<ReplicaIdOwned>;
    type Rejection = MembershipRejection;

    const DISABLE_R_WHEN_R: bool = false;
    const DISABLE_R_WHEN_NOT_R: bool = false;
    const DISABLE_STABILIZE: bool = false;

    fn redundant_itself<'a>(
        _new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        false
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&crate::event::tag::Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&crate::event::tag::Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn stabilize(
        _tagged_op: &TaggedOp<Self>,
        _stable: &mut Self::StableState,
        _unstable: &mut impl IsUnstablePrune<Self>,
    ) {
    }

    fn eval<Q, U>(q: &Q, stable: &Self::StableState, unstable: &U) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<Q, U>,
    {
        Self::execute_query(q, stable, unstable)
    }
}

impl<U> UsesUnstableService<U> for Membership
where
    U: IsUnstableCore<Self>,
{
    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Result<(), Self::Rejection> {
        let members: HashSet<ReplicaIdOwned> = Self::execute_query(&Read::new(), stable, unstable);

        match op {
            Membership::Join(id) if members.contains(id) => {
                Err(MembershipRejection::AlreadyJoined(id.clone()))
            }
            Membership::Leave(id) if !members.contains(id) => {
                Err(MembershipRejection::NotJoined(id.clone()))
            }
            _ => Ok(()),
        }
    }
}

impl IsStableState<Membership> for HashSet<ReplicaIdOwned> {
    fn is_default(&self) -> bool {
        self == &HashSet::default()
    }

    fn apply(&mut self, value: Membership) {
        match value {
            Membership::Join(replica_id) => {
                self.insert(replica_id);
            }
            Membership::Leave(replica_id) => {
                self.remove(&replica_id);
            }
        }
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<Membership>,
        _tagged_op: &TaggedOp<Membership>,
    ) {
    }
}

impl<U> Eval<Read<HashSet<ReplicaIdOwned>>, U> for Membership
where
    U: IsUnstableCore<Self>,
{
    fn execute_query(
        _q: &Read<HashSet<ReplicaIdOwned>>,
        stable: &Self::StableState,
        unstable: &U,
    ) -> <Read<HashSet<ReplicaIdOwned>> as QueryOperation>::Response {
        let mut members = stable.clone();

        for tagged_op in unstable.iter() {
            if let Membership::Join(id) = tagged_op.op() {
                members.insert(id.clone());
            }
        }

        members
    }
}
