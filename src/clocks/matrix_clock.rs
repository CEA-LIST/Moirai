use super::vector_clock::VectorClock;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result},
    hash::Hash,
    ops::{Add, AddAssign},
};

#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct MatrixClock<K, C>
where
    K: PartialOrd + Hash + Clone + Eq + Ord,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
{
    clock: HashMap<K, VectorClock<K, C>>,
}

impl<K, C> MatrixClock<K, C>
where
    K: PartialOrd + Hash + Clone + Eq + Ord + Display,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug + Display,
{
    pub fn new(keys: &[K]) -> MatrixClock<K, C> {
        let mut clock = HashMap::new();
        for k in keys {
            clock.insert(
                k.clone(),
                VectorClock::from(keys, &vec![C::default(); keys.len()]),
            );
        }
        MatrixClock { clock }
    }

    pub fn get(&self, key: &K) -> Option<&VectorClock<K, C>> {
        self.clock.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut VectorClock<K, C>> {
        self.clock.get_mut(key)
    }

    /// Add a new key to the matrix clock, set its vector clock to the initial value
    /// Add the key in the vector clocks of all other keys
    pub fn add_key(&mut self, key: K) {
        let keys: Vec<K> = self.clock.keys().cloned().collect();
        let vc = VectorClock::from(&keys, &vec![C::default(); keys.len()]);
        self.clock.insert(key.clone(), vc);
        for vc in self.clock.values_mut() {
            vc.increment(&key.clone());
        }
        assert!(self.is_square());
    }

    pub fn remove_key(&mut self, key: &K) {
        self.clock.remove(key);
        for vc in self.clock.values_mut() {
            vc.remove(key);
        }
        assert!(self.is_square());
    }

    pub fn update(&mut self, key: &K, vc: &VectorClock<K, C>) {
        self.clock
            .entry(key.clone())
            .and_modify(|vc2| vc2.merge(vc));
        assert!(self.is_square());
    }

    pub fn from(keys: &[K], vcs: &[VectorClock<K, C>]) -> MatrixClock<K, C> {
        let mut clock = HashMap::new();
        for (k, vc) in keys.iter().zip(vcs.iter()) {
            clock.insert(k.clone(), vc.clone());
        }
        MatrixClock { clock }
    }

    pub fn clear(&mut self) {
        self.clock.clear();
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    pub fn svv(&self, ignore: &[K]) -> VectorClock<K, C> {
        let mut svv = VectorClock::default();
        for (k, vc) in &self.clock {
            if !ignore.contains(k) {
                svv = svv.min(vc);
            }
        }
        svv
    }

    pub fn merge(&mut self, other: &MatrixClock<K, C>) {
        for (k, vc1) in &(other.clock) {
            self.clock
                .entry(k.clone())
                .and_modify(|vc2| vc2.merge(vc1))
                .or_insert_with(|| vc1.clone());
        }
        assert!(self.is_square());
    }

    /// Update the given key in the matrix clock with the value of the other keys
    pub fn most_update(&mut self, key: &K) {
        let keys: Vec<K> = self.clock.keys().cloned().collect();
        let mut vc = VectorClock::from(&keys, &vec![C::default(); keys.len()]);
        for k in &keys {
            if k != key {
                vc.merge(self.get(k).unwrap());
            }
        }
        self.update(key, &vc);
        assert!(self.is_square());
    }

    /// Check if the matrix clock is square
    pub fn is_square(&self) -> bool {
        let n = self.clock.len();
        self.clock.values().all(|vc| vc.len() == n)
    }

    pub fn keys(&self) -> Vec<K> {
        let mut keys: Vec<K> = self.clock.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub fn len(&self) -> usize {
        self.clock.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clock.is_empty()
    }
}

impl<K, C> Display for MatrixClock<K, C>
where
    K: PartialOrd + Hash + Clone + Eq + Display + Ord,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Display + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut sorted_keys: Vec<_> = self.clock.keys().collect();
        sorted_keys.sort();

        let s = sorted_keys.iter().fold(String::from("{\n"), |acc, k| {
            let v = self.clock.get(k).unwrap();
            acc + &format!("  {}: {}\n", k, v)
        });
        write!(f, "{}}}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn test_new() {
        let mc = MatrixClock::<&str, i32>::new(&["A", "B", "C"]);
        assert_eq!(mc.clock.len(), 3);
        assert_eq!(
            mc.get(&"A"),
            Some(&VectorClock::from(&["A", "B", "C"], &[0, 0, 0]))
        );
    }

    #[test_log::test]
    fn test_svv() {
        let mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from(&["A", "B"], &[10, 2]),
                VectorClock::from(&["A", "B"], &[8, 6]),
            ],
        );
        assert_eq!(mc.svv(&[]), VectorClock::from(&["A", "B"], &[8, 2]));
    }

    #[test_log::test]
    fn test_merge() {
        let mut mc1 = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from(&["A", "B"], &[10, 2]),
                VectorClock::from(&["A", "B"], &[8, 6]),
            ],
        );
        let mc2 = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from(&["A", "B"], &[9, 3]),
                VectorClock::from(&["A", "B"], &[7, 7]),
            ],
        );
        mc1.merge(&mc2);
        assert_eq!(
            mc1,
            MatrixClock::from(
                &["A", "B"],
                &[
                    VectorClock::from(&["A", "B"], &[10, 3]),
                    VectorClock::from(&["A", "B"], &[8, 7]),
                ]
            )
        );
    }

    #[test_log::test]
    fn test_svv_ignore() {
        let mc = MatrixClock::from(
            &["A", "B", "C"],
            &[
                VectorClock::from(&["A", "B", "C"], &[2, 6, 1]),
                VectorClock::from(&["A", "B", "C"], &[2, 5, 2]),
                VectorClock::from(&["A", "B", "C"], &[1, 4, 11]),
            ],
        );
        assert_eq!(
            mc.svv(&["C"]),
            VectorClock::from(&["A", "B", "C"], &[2, 5, 1]),
        );
    }

    #[test_log::test]
    fn test_display() {
        let mc = MatrixClock::from(
            &["A", "B", "C"],
            &[
                VectorClock::from(&["A", "B", "C"], &[0, 1, 1]),
                VectorClock::from(&["A", "B", "C"], &[1, 0, 1]),
                VectorClock::from(&["A", "B", "C"], &[1, 1, 0]),
            ],
        );
        assert_eq!(
            format!("{}", mc),
            "{\n  A: { A: 0, B: 1, C: 1 }\n  B: { A: 1, B: 0, C: 1 }\n  C: { A: 1, B: 1, C: 0 }\n}"
        );
    }

    #[test_log::test]
    fn test_add_key() {
        let mut mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from(&["A", "B"], &[10, 2]),
                VectorClock::from(&["A", "B"], &[8, 6]),
            ],
        );
        mc.add_key("C");
        assert_eq!(
            mc,
            MatrixClock::from(
                &["A", "B", "C"],
                &[
                    VectorClock::from(&["A", "B", "C"], &[10, 2, 0]),
                    VectorClock::from(&["A", "B", "C"], &[8, 6, 0]),
                    VectorClock::from(&["A", "B", "C"], &[0, 0, 0]),
                ]
            )
        );
    }

    #[test_log::test]
    fn test_keys() {
        let mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from(&["A", "B"], &[10, 2]),
                VectorClock::from(&["A", "B"], &[8, 6]),
            ],
        );
        assert!((mc.keys() == &["A", "B"]) || (mc.keys() == ["B", "A"]));
    }
}
