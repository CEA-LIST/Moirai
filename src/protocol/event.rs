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

    pub fn origin(&self) -> &K {
        match self {
            Event::OpEvent(op) => &op.metadata.origin,
            Event::ProtocolEvent(cmd) => &cmd.metadata.origin,
        }
    }

    pub fn vc(&self) -> &VectorClock<K, C> {
        match self {
            Event::OpEvent(op) => &op.metadata.vc,
            Event::ProtocolEvent(cmd) => &cmd.metadata.vc,
        }
    }

    pub fn message(&self) -> String {
        match &self {
            Event::OpEvent(op) => format!("{:?}", op.op),
            Event::ProtocolEvent(cmd) => format!("{:?}", cmd.cmd),
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
