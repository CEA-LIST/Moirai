use crate::clocks::vector_clock::VectorClock;

use super::op_rules::OpRules;
use std::hash::Hash;
use std::ops::Add;
use std::{fmt::Debug, ops::AddAssign};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Signal {
    Join,
    Leave,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<O>
where
    O: Clone + Debug + OpRules,
{
    Op(O),
    Signal(Signal),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Event<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    pub vc: VectorClock<K, C>,
    pub message: Message<O>,
    pub wc: u128,
    pub origin: K,
}

impl<K, C, O> Event<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    pub fn new(vc: VectorClock<K, C>, message: Message<O>, origin: K) -> Self {
        Self {
            vc,
            message,
            origin,
            wc: Self::since_the_epoch(),
        }
    }

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
