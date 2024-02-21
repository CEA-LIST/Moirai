use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result},
    hash::Hash,
    ops::{Add, AddAssign},
};

use super::vector_clock::VectorClock;

/// The matrix must ALWAYS be square
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MatrixClock<K, T>
where
    K: Hash + Clone + Eq,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    clock: HashMap<K, VectorClock<K, T>>,
}

impl<K, T> MatrixClock<K, T>
where
    K: Hash + Clone + Eq,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    pub fn new(keys: &[K]) -> MatrixClock<K, T> {
        let mut clock = HashMap::new();
        for k in keys {
            clock.insert(
                k.clone(),
                VectorClock::from(keys, &vec![T::default(); keys.len()]),
            );
        }
        MatrixClock { clock }
    }

    pub fn get(&self, key: &K) -> Option<VectorClock<K, T>> {
        self.clock.get(key).cloned()
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut VectorClock<K, T>> {
        self.clock.get_mut(key)
    }

    /// Add a new key to the matrix clock, set its vector clock to the initial value
    pub fn add_key(&mut self, key: K) {
        let mut keys = self.clock.keys().cloned().collect::<Vec<_>>();
        keys.extend([key.clone()]);
        self.clock.insert(
            key,
            VectorClock::from(&keys, &vec![T::default(); keys.len()]),
        );
    }

    pub fn update(&mut self, key: &K, vc: &VectorClock<K, T>) {
        self.clock
            .entry(key.clone())
            .and_modify(|vc2| vc2.merge(vc));
    }

    pub fn from(keys: &[K], vcs: &[VectorClock<K, T>]) -> MatrixClock<K, T> {
        let mut clock = HashMap::new();
        for (k, vc) in keys.iter().zip(vcs.iter()) {
            clock.insert(k.clone(), vc.clone());
        }
        MatrixClock { clock }
    }

    pub fn min(&self) -> VectorClock<K, T> {
        let mut min_vc = self.clock.values().next().unwrap().clone();
        for vc in self.clock.values() {
            min_vc = min_vc.min(vc);
        }
        min_vc
    }

    pub fn merge(&mut self, other: &MatrixClock<K, T>) {
        for (k, vc1) in &(other.clock) {
            self.clock
                .entry(k.clone())
                .and_modify(|vc2| vc2.merge(vc1))
                .or_insert_with(|| vc1.clone());
        }
    }
}

impl<K, T> Display for MatrixClock<K, T>
where
    K: Hash + Clone + Eq + Display + Ord,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Display + Debug,
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

    #[test]
    fn test_new() {
        let mc = MatrixClock::<&str, i32>::new(&["A", "B", "C"]);
        assert_eq!(mc.clock.len(), 3);
        assert_eq!(
            mc.get(&"A"),
            Some(VectorClock::from(&["A", "B", "C"], &[0, 0, 0]))
        );
    }

    #[test]
    fn test_min() {
        let mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from(&["A", "B"], &[10, 2]),
                VectorClock::from(&["A", "B"], &[8, 6]),
            ],
        );
        assert_eq!(mc.min(), VectorClock::from(&["A", "B"], &[8, 2]));
    }

    #[test]
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

    #[test]
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
}
