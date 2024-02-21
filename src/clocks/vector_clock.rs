// Inspired by https://gitlab.com/liberecofr/vclock

use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result},
    hash::Hash,
    ops::{Add, AddAssign},
};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct VectorClock<K = usize, T = usize>
where
    K: Eq + Hash + Clone,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    clock: HashMap<K, T>,
}

impl<K, T> VectorClock<K, T>
where
    K: Hash + Clone + Eq,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    /// Create a new VectorClock with a single key and an initial value
    pub fn new(key: K) -> VectorClock<K, T> {
        let mut clock = HashMap::new();
        clock.insert(key, T::default());
        VectorClock { clock }
    }

    /// Get the value of a key
    pub fn get(&self, key: &K) -> Option<T> {
        self.clock.get(key).cloned()
    }

    /// Increment the value of a key
    pub fn increment(&mut self, key: &K) {
        let value = match self.clock.get(key) {
            Some(v) => v.clone() + T::from(1),
            None => T::default(),
        };
        self.clock.insert(key.clone(), value);
    }

    /// Take the max of the two clocks
    pub fn merge(&mut self, other: &VectorClock<K, T>) {
        for (k, v) in &(other.clock) {
            if match self.clock.get(k) {
                Some(v2) => v2 < v,
                None => true,
            } {
                self.clock.insert(k.clone(), v.clone());
            }
        }
    }

    /// Create a VectorClock from two slices
    /// The first slice is the keys and the second slice is the values
    /// The two slices must have the same length
    pub fn from(key: &[K], value: &[T]) -> VectorClock<K, T> {
        if key.len() != value.len() {
            panic!("The two slices must have the same length");
        }
        let mut clock = HashMap::new();
        for (k, v) in key.iter().zip(value.iter()) {
            clock.insert(k.clone(), v.clone());
        }
        VectorClock { clock }
    }

    /// Take the min of the two clocks
    /// The min of two clocks is the clock that has the min value for each key
    /// It represents the number of events that have been observed by both clocks
    pub fn min(&self, other: &VectorClock<K, T>) -> VectorClock<K, T> {
        let mut result = VectorClock::default();
        for (k, v) in &(other.clock) {
            if match self.clock.get(k) {
                Some(v2) => v2 > v,
                None => true,
            } {
                result.clock.insert(k.clone(), v.clone());
            } else {
                result.clock.insert(k.clone(), self.clock[k].clone());
            }
        }
        result
    }
}

impl<K, T> Default for VectorClock<K, T>
where
    K: Eq + Hash + Clone,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    fn default() -> VectorClock<K, T> {
        VectorClock {
            clock: HashMap::<K, T>::new(),
        }
    }
}

impl<K, T> Display for VectorClock<K, T>
where
    K: Eq + Hash + Clone + Ord + Display,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug + Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut keys: Vec<&K> = self.clock.keys().collect();
        keys.sort(); // Sort the keys
        let result = keys
            .iter()
            .map(|key| format!("{}: {}", key, self.clock[key]))
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{{ {} }}", result)
    }
}

impl<K, T> PartialOrd for VectorClock<K, T>
where
    K: Hash + Clone + Eq,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut has_less: bool = false;
        let mut has_greater: bool = false;

        for (k, v) in &(self.clock) {
            match other.clock.get(k) {
                Some(other_v) => {
                    if v > other_v {
                        if !has_less {
                            has_greater = true;
                        } else {
                            return None;
                        }
                    }
                    if v < other_v {
                        if !has_greater {
                            has_less = true;
                        } else {
                            return None;
                        }
                    }
                }
                None => {
                    if !has_less {
                        has_greater = true;
                    } else {
                        return None;
                    }
                }
            }
        }

        for (k, v) in &(other.clock) {
            match self.clock.get(k) {
                Some(self_v) => {
                    if v > self_v {
                        if !has_greater {
                            has_less = true;
                        } else {
                            return None;
                        }
                    }
                    if v < self_v {
                        if !has_less {
                            has_greater = true;
                        } else {
                            return None;
                        }
                    }
                }
                None => {
                    if !has_greater {
                        has_less = true;
                    } else {
                        return None;
                    }
                }
            }
        }
        if has_less && !has_greater {
            return Some(Ordering::Less);
        }
        if has_greater && !has_less {
            return Some(Ordering::Greater);
        }
        if has_less && has_greater {
            // Normally this should be useless as there are shortcuts
            // before setting has_greater or has_less. But better be safe than sorry.
            return None;
        }
        Some(Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let clock = VectorClock::<i32, i32>::new(0);
        assert_eq!(clock.get(&0), Some(0));
    }

    #[test]
    fn test_increment() {
        let mut clock = VectorClock::new("A");
        clock.increment(&"A");
        clock.increment(&"A");
        assert_eq!(clock.get(&"A"), Some(2));
    }

    #[test]
    fn test_merge() {
        let mut clock1 = VectorClock::new("A");
        clock1.increment(&"A");
        let mut clock2 = VectorClock::new("B");
        clock2.increment(&"B");
        clock2.increment(&"A");
        clock2.increment(&"A");
        clock2.increment(&"A");

        clock1.merge(&clock2);
        assert_eq!(clock1.get(&"A"), Some(2));
        assert_eq!(clock1.get(&"B"), Some(1));
    }

    #[test]
    fn test_concurrent_clocks() {
        let mut clock: VectorClock<&str, i32> = VectorClock::new(&"A");
        clock.increment(&"B");
        clock.increment(&"A");
        let mut clock2: VectorClock<&str, i32> = VectorClock::new(&"B");
        clock2.increment(&"B");
        clock2.increment(&"A");
        assert_eq!(clock2.partial_cmp(&clock), None);
    }

    #[test]
    fn test_display() {
        let mut clock: VectorClock<&str, i32> = VectorClock::new("A");
        clock.increment(&"A");
        clock.increment(&"B");
        clock.increment(&"B");
        assert_eq!(String::from("{ A: 1, B: 1 }"), clock.to_string());
    }

    #[test]
    fn test_min() {
        let mut clock1: VectorClock<&str, i32> = VectorClock::new("A");
        clock1.increment(&"A");
        clock1.increment(&"A");
        clock1.increment(&"A");
        clock1.increment(&"A");
        clock1.increment(&"B");
        let mut clock2: VectorClock<&str, i32> = VectorClock::new("B");
        clock2.increment(&"B");
        clock2.increment(&"A");
        clock2.increment(&"A");
        clock2.increment(&"A");
        let clock3 = clock1.min(&clock2);
        assert_eq!(clock3.get(&"A"), Some(2));
        assert_eq!(clock3.get(&"B"), Some(0));
    }
}
