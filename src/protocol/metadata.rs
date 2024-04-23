use crate::clocks::vector_clock::VectorClock;

use super::utils::{Incrementable, Keyable};
use std::{cmp::Ordering, fmt::Debug};

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Metadata<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    pub vc: VectorClock<K, C>,
    pub wc: u128,
    pub origin: K,
}

impl<K, C> Default for Metadata<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    fn default() -> Self {
        Self {
            vc: VectorClock::default(),
            wc: 0,
            origin: K::default(),
        }
    }
}

impl<K, C> Metadata<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    pub fn new(vc: VectorClock<K, C>, origin: K) -> Self {
        Self {
            vc,
            wc: Self::since_the_epoch(),
            origin,
        }
    }

    pub fn since_the_epoch() -> u128 {
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

impl<K, C> Ord for Metadata<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let clock_cmp: Option<Ordering> = self.vc.partial_cmp(&other.vc);
        match clock_cmp {
            Some(Ordering::Equal) | None => match other.wc.cmp(&self.wc) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => other.origin.cmp(&self.origin),
                Ordering::Greater => Ordering::Greater,
            },
            Some(Ordering::Less) => Ordering::Less,
            Some(Ordering::Greater) => Ordering::Greater,
        }
    }
}

impl<K, C> PartialOrd for Metadata<K, C>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
