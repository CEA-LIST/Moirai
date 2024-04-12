use crate::clocks::vector_clock::VectorClock;
use serde::{Deserialize, Serialize};

use super::op_rules::OpRules;
use std::hash::Hash;
use std::ops::Add;
use std::{fmt::Debug, ops::AddAssign};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum Event<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    OpEvent(OpEvent<K, C, O>),
    ProtocolEvent(ProtocolEvent<K, C>),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct ProtocolEvent<K, C>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
{
    pub cmd: ProtocolCmd<K>,
    pub metadata: Metadata<K, C>,
}

impl<K, C> ProtocolEvent<K, C>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
{
    pub fn obsolete(is_obsolete: &Self, other: &Self) -> bool {
        match (&is_obsolete.cmd, &other.cmd) {
            (ProtocolCmd::Join, ProtocolCmd::Join) => false,
            (ProtocolCmd::Join, ProtocolCmd::Leave) => false,
            (ProtocolCmd::Join, ProtocolCmd::KickOut(_)) => false,
            (ProtocolCmd::Leave, ProtocolCmd::Join) => false,
            (ProtocolCmd::Leave, ProtocolCmd::Leave) => false,
            (ProtocolCmd::Leave, ProtocolCmd::KickOut(_)) => false,
            (ProtocolCmd::KickOut(_), ProtocolCmd::Join) => false,
            (ProtocolCmd::KickOut(_), ProtocolCmd::Leave) => false,
            (ProtocolCmd::KickOut(_), ProtocolCmd::KickOut(_)) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct OpEvent<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    pub op: O,
    pub metadata: Metadata<K, C>,
}

/// Raw event body
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Message<K, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    Op(O),
    ProtocolCmd(ProtocolCmd<K>),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ProtocolCmd<K>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
{
    Join,
    Leave,
    KickOut(K),
}

impl<K, C, O> Event<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    pub fn new(message: Message<K, O>, vc: VectorClock<K, C>, origin: K) -> Self {
        let metadata = Metadata {
            vc,
            origin,
            wc: Metadata::<K, C>::since_the_epoch(),
        };
        match message {
            Message::Op(op) => Self::OpEvent(OpEvent { op, metadata }),
            Message::ProtocolCmd(cmd) => Self::ProtocolEvent(ProtocolEvent { cmd, metadata }),
        }
    }

    pub fn new_op(op: O, vc: VectorClock<K, C>, origin: K) -> OpEvent<K, C, O> {
        let metadata = Metadata {
            vc,
            origin,
            wc: Metadata::<K, C>::since_the_epoch(),
        };
        OpEvent { op, metadata }
    }

    pub fn metadata(&self) -> &Metadata<K, C> {
        match self {
            Self::OpEvent(op_event) => &op_event.metadata,
            Self::ProtocolEvent(protocol_event) => &protocol_event.metadata,
        }
    }

    pub fn message(&self) -> Message<K, O> {
        match self {
            Self::OpEvent(op_event) => Message::Op(op_event.op.clone()),
            Self::ProtocolEvent(protocol_event) => Message::ProtocolCmd(protocol_event.cmd.clone()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct Metadata<K, C>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
{
    pub vc: VectorClock<K, C>,
    pub wc: u128,
    pub origin: K,
}

impl<K, C> Metadata<K, C>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
{
    fn since_the_epoch() -> u128 {
        #[cfg(feature = "wasm")]
        return web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        #[cfg(not(feature = "wasm"))]
        return std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
    }
}
