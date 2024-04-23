use crate::clocks::vector_clock::VectorClock;

use super::{
    metadata::Metadata,
    pure_crdt::PureCRDT,
    utils::{Incrementable, Keyable},
};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub enum Message<K, O>
where
    K: Keyable,
    O: PureCRDT,
{
    Op(O),
    Membership(Membership<K>),
}

#[derive(Clone, Debug)]
pub enum Membership<K>
where
    K: Keyable,
{
    Join,
    Leave,
    KickOut(K),
}

#[derive(Clone, Debug)]
pub struct OpEvent<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub op: O,
    pub metadata: Metadata<K, C>,
}

impl<K, C, O> OpEvent<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub fn new(op: O, metadata: Metadata<K, C>) -> Self {
        Self { op, metadata }
    }
}

#[derive(Clone)]
pub struct MembershipEvent<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    pub cmd: Membership<K>,
    pub metadata: Metadata<K, C>,
}

impl<K, C> MembershipEvent<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    pub fn new(cmd: Membership<K>, metadata: Metadata<K, C>) -> Self {
        Self { cmd, metadata }
    }
}

#[derive(Clone)]
pub enum Event<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT,
{
    OpEvent(OpEvent<K, C, O>),
    MembershipEvent(MembershipEvent<K, C>),
}

impl<K, C, O> Event<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT,
{
    pub fn new(message: Message<K, O>, vc: VectorClock<K, C>, origin: K) -> Self {
        let metadata = Metadata::new(vc, origin);
        match message {
            Message::Op(op) => Event::OpEvent(OpEvent::new(op, metadata)),
            Message::Membership(membership) => {
                Event::MembershipEvent(MembershipEvent::new(membership, metadata))
            }
        }
    }

    pub fn metadata(&self) -> &Metadata<K, C> {
        match self {
            Event::OpEvent(op_event) => &op_event.metadata,
            Event::MembershipEvent(membership_event) => &membership_event.metadata,
        }
    }
}
