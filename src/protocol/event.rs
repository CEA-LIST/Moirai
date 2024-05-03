use crate::clocks::vector_clock::VectorClock;

use super::{
    membership::{Membership, MembershipEvent},
    metadata::Metadata,
    pure_crdt::PureCRDT,
    utils::{Incrementable, Keyable},
};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub enum Message<K, C, O>
where
    K: Keyable + Debug + Clone,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    Op(O),
    Membership(Membership<K, C, O>),
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

#[derive(Clone, Debug)]
pub enum Event<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT,
{
    OpEvent(OpEvent<K, C, O>),
    MembershipEvent(MembershipEvent<K, C, O>),
}

impl<K, C, O> Event<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT,
{
    pub fn new(message: Message<K, C, O>, vc: VectorClock<K, C>, origin: K) -> Self {
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

    pub fn metadata_mut(&mut self) -> &mut Metadata<K, C> {
        match self {
            Event::OpEvent(op_event) => &mut op_event.metadata,
            Event::MembershipEvent(membership_event) => &mut membership_event.metadata,
        }
    }
}
