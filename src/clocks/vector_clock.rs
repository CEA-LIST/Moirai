// // Inspired by https://gitlab.com/liberecofr/vclock

// use std::{
//     cmp::Ordering,
//     collections::HashMap,
//     fmt::{Debug, Display, Formatter, Result},
//     hash::Hash,
//     ops::{Add, AddAssign},
// };

// #[cfg(feature = "serde")]
// use serde::{Deserialize, Serialize};

// #[derive(Debug, Eq, PartialEq, Clone)]
// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// pub struct VectorClock<K = usize, C = usize>
// where
//     K: PartialOrd + Hash + Clone + Eq + Ord,
//     C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
// {
//     clock: HashMap<K, C>,
// }

// impl<K, C> VectorClock<K, C>
// where
//     K: PartialOrd + Hash + Clone + Eq + Ord,
//     C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
// {
//     /// Create a new VectorClock with a single key and an initial value
//     pub fn new(key: K) -> VectorClock<K, C> {
//         let mut clock = HashMap::new();
//         clock.insert(key, C::default());
//         VectorClock { clock }
//     }

//     /// Get the value of a key
//     pub fn get(&self, key: &K) -> Option<C> {
//         self.clock.get(key).cloned()
//     }

//     /// Get a mutable reference to the value of a key
//     pub fn get_mut(&mut self, key: &K) -> Option<&mut C> {
//         self.clock.get_mut(key)
//     }

//     pub fn iter(&self) -> impl Iterator<Item = (&K, &C)> {
//         self.clock.iter()
//     }

//     /// Increment the value of a key
//     pub fn increment(&mut self, key: &K) {
//         let value = match self.clock.get(key) {
//             Some(v) => v.clone() + C::from(1),
//             None => C::default(),
//         };
//         self.clock.insert(key.clone(), value);
//     }

//     /// Insert a key with a value
//     pub fn insert(&mut self, key: K, value: C) {
//         self.clock.insert(key, value);
//     }

//     pub fn remove(&mut self, key: &K) -> Option<C> {
//         self.clock.remove(key)
//     }

//     /// Take the max of the two clocks
//     pub fn merge(&mut self, other: &VectorClock<K, C>) {
//         for (k, v) in &(other.clock) {
//             if match self.clock.get(k) {
//                 Some(v2) => v2 < v,
//                 None => true,
//             } {
//                 self.clock.insert(k.clone(), v.clone());
//             }
//         }
//     }

//     /// Create a VectorClock from two slices
//     /// The first slice is the keys and the second slice is the values
//     /// The two slices must have the same length
//     pub fn from_key_value(keys: &[K], values: &[C]) -> VectorClock<K, C> {
//         if keys.len() != values.len() {
//             panic!("The two slices must have the same length");
//         }
//         let mut clock = HashMap::new();
//         for (k, v) in keys.iter().zip(values.iter()) {
//             clock.insert(k.clone(), v.clone());
//         }
//         VectorClock { clock }
//     }

//     /// Take the min of the two clocks
//     /// The min of two clocks is the clock that has the min value for each key
//     /// It represents the number of events that have been observed by both clocks
//     pub fn min(&self, other: &VectorClock<K, C>) -> VectorClock<K, C> {
//         let mut result = VectorClock::default();
//         for (k, v) in &(other.clock) {
//             if match self.clock.get(k) {
//                 Some(v2) => v2 > v,
//                 None => true,
//             } {
//                 result.clock.insert(k.clone(), v.clone());
//             } else {
//                 result.clock.insert(k.clone(), self.clock[k].clone());
//             }
//         }
//         result
//     }

//     /// Take the difference of the two clocks
//     /// The difference of two clocks is the clock that has the keys that are ONLY in the first clock
//     pub fn left_difference(&self, other: &VectorClock<K, C>) -> VectorClock<K, C> {
//         let mut result = VectorClock::default();
//         for (k, v) in &(self.clock) {
//             if !other.clock.contains_key(k) {
//                 result.clock.insert(k.clone(), v.clone());
//             }
//         }
//         result
//     }

//     /// Check if the clock contains a key
//     pub fn contains(&self, key: &K) -> bool {
//         self.clock.contains_key(key)
//     }

//     pub fn keys(&self) -> Vec<K> {
//         let mut keys: Vec<K> = self.clock.keys().cloned().collect();
//         keys.sort();
//         keys
//     }

//     pub fn len(&self) -> usize {
//         self.clock.len()
//     }

//     pub fn bot() -> VectorClock<K, C> {
//         Self::default()
//     }

//     pub fn is_empty(&self) -> bool {
//         self.clock.is_empty()
//     }
// }

// impl<K, C> Default for VectorClock<K, C>
// where
//     K: PartialOrd + Hash + Clone + Eq + Ord,
//     C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
// {
//     fn default() -> VectorClock<K, C> {
//         VectorClock {
//             clock: HashMap::<K, C>::new(),
//         }
//     }
// }

// impl<K, C> Display for VectorClock<K, C>
// where
//     K: Eq + Hash + Clone + Ord + Display,
//     C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug + Display,
// {
//     fn fmt(&self, f: &mut Formatter<'_>) -> Result {
//         let mut keys: Vec<&K> = self.clock.keys().collect();
//         keys.sort();
//         let result = keys
//             .iter()
//             .map(|key| format!("{}: {}", key, self.clock[key]))
//             .collect::<Vec<String>>()
//             .join(", ");
//         write!(f, "{{ {} }}", result)
//     }
// }

// impl<K, C> PartialOrd for VectorClock<K, C>
// where
//     K: PartialOrd + Hash + Clone + Eq + Ord + Debug,
//     C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
// {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         assert!(self.clock.len() == other.clock.len() || self.clock.is_empty(), "Clocks must have the same length or the first clock must be empty. Clock 1: {:?}, Clock 2: {:?}", self, other);
//         let mut has_less: bool = false;
//         let mut has_greater: bool = false;

//         for (k, v) in &(self.clock) {
//             match other.clock.get(k) {
//                 Some(other_v) => {
//                     if v > other_v {
//                         if !has_less {
//                             has_greater = true;
//                         } else {
//                             return None;
//                         }
//                     }
//                     if v < other_v {
//                         if !has_greater {
//                             has_less = true;
//                         } else {
//                             return None;
//                         }
//                     }
//                 }
//                 None => {
//                     if !has_less {
//                         has_greater = true;
//                     } else {
//                         return None;
//                     }
//                 }
//             }
//         }

//         for (k, v) in &(other.clock) {
//             match self.clock.get(k) {
//                 Some(self_v) => {
//                     if v > self_v {
//                         if !has_greater {
//                             has_less = true;
//                         } else {
//                             return None;
//                         }
//                     }
//                     if v < self_v {
//                         if !has_less {
//                             has_greater = true;
//                         } else {
//                             return None;
//                         }
//                     }
//                 }
//                 None => {
//                     if !has_greater {
//                         has_less = true;
//                     } else {
//                         return None;
//                     }
//                 }
//             }
//         }
//         if has_less && !has_greater {
//             return Some(Ordering::Less);
//         }
//         if has_greater && !has_less {
//             return Some(Ordering::Greater);
//         }
//         if has_less && has_greater {
//             // Normally this should be useless as there are shortcuts
//             // before setting has_greater or has_less. But better be safe than sorry.
//             return None;
//         }
//         Some(Ordering::Equal)
//     }
// }

// // impl<K, C> PartialOrd for VectorClock<K, C>
// // where
// //     K: PartialOrd + Hash + Clone + Eq + Ord,
// //     C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
// // {
// //     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
// //         let mut less = false;
// //         let mut greater = false;

// //         // Collect all unique keys from both clocks
// //         let all_keys: std::collections::HashSet<_> =
// //             self.keys().into_iter().chain(other.keys()).collect();

// //         for key in all_keys {
// //             let v1 = self.get(&key).unwrap_or_default();
// //             let v2 = other.get(&key).unwrap_or_default();

// //             match v1.cmp(&v2) {
// //                 Ordering::Less => less = true,
// //                 Ordering::Greater => greater = true,
// //                 _ => (),
// //             }

// //             // If both less and greater are true, the clocks are concurrent
// //             if less && greater {
// //                 return None;
// //             }
// //         }

// //         if less {
// //             Some(Ordering::Less)
// //         } else if greater {
// //             Some(Ordering::Greater)
// //         } else {
// //             Some(Ordering::Equal)
// //         }
// //     }
// // }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test_log::test]
//     fn new() {
//         let clock = VectorClock::<i32, i32>::new(0);
//         assert_eq!(clock.get(&0), Some(0));
//     }

//     #[test_log::test]
//     fn increment() {
//         let mut clock = VectorClock::new("A");
//         clock.increment(&"A");
//         clock.increment(&"A");
//         assert_eq!(clock.get(&"A"), Some(2));
//     }

//     #[test_log::test]
//     fn merge() {
//         let mut clock1 = VectorClock::new("A");
//         clock1.increment(&"A");
//         let mut clock2 = VectorClock::new("B");
//         clock2.increment(&"B");
//         clock2.increment(&"A");
//         clock2.increment(&"A");
//         clock2.increment(&"A");

//         clock1.merge(&clock2);
//         assert_eq!(clock1.get(&"A"), Some(2));
//         assert_eq!(clock1.get(&"B"), Some(1));
//     }

//     #[test_log::test]
//     fn merge_2() {
//         let mut clock1 = VectorClock::new("A");
//         let clock2 = VectorClock::new("B");
//         clock1.merge(&clock2);
//         assert_eq!(clock1.get(&"A"), Some(0));
//         assert_eq!(clock1.get(&"B"), Some(0));
//     }

//     #[test_log::test]
//     fn merge_3() {
//         let mut clock1 = VectorClock::from_key_value(&["A", "B", "C", "D"], &[1, 1, 0, 1]);
//         let clock2 = VectorClock::from_key_value(&["A", "B", "C", "D"], &[0, 0, 2, 0]);
//         clock1.merge(&clock2);
//         assert_eq!(clock1.get(&"C"), Some(2));
//     }

//     #[test_log::test]
//     fn concurrent_clocks() {
//         let mut clock: VectorClock<&str, i32> = VectorClock::new("A");
//         clock.increment(&"B");
//         clock.increment(&"A");
//         let mut clock2: VectorClock<&str, i32> = VectorClock::new("B");
//         clock2.increment(&"B");
//         clock2.increment(&"A");
//         assert_eq!(clock2.partial_cmp(&clock), None);
//     }

//     #[test_log::test]
//     fn display() {
//         let mut clock: VectorClock<&str, i32> = VectorClock::new("A");
//         clock.increment(&"A");
//         clock.increment(&"B");
//         clock.increment(&"B");
//         assert_eq!(String::from("{ A: 1, B: 1 }"), clock.to_string());
//     }

//     #[test_log::test]
//     fn min() {
//         let mut clock1: VectorClock<&str, i32> = VectorClock::new("A");
//         clock1.increment(&"A");
//         clock1.increment(&"A");
//         clock1.increment(&"A");
//         clock1.increment(&"A");
//         clock1.increment(&"B");
//         let mut clock2: VectorClock<&str, i32> = VectorClock::new("B");
//         clock2.increment(&"B");
//         clock2.increment(&"A");
//         clock2.increment(&"A");
//         clock2.increment(&"A");
//         let clock3 = clock1.min(&clock2);
//         assert_eq!(clock3.get(&"A"), Some(2));
//         assert_eq!(clock3.get(&"B"), Some(0));
//     }

//     #[test_log::test]
//     fn lsv() {
//         let mut lsv = VectorClock::from_key_value(&["A", "B"], &[12, 16]);
//         let ltm = VectorClock::from_key_value(&["A", "B", "C"], &[0, 0, 0]);
//         lsv.merge(&ltm);
//         let merge = VectorClock::from_key_value(&["A", "B", "C"], &[12, 16, 0]);
//         assert_eq!(merge, lsv);
//     }

//     #[test_log::test]
//     fn left_difference() {
//         let clock = VectorClock::from_key_value(&["a", "b", "c"], &[4, 2, 1]);
//         let ext = VectorClock::from_key_value(&["d", "e", "b"], &[2, 51, 4]);

//         assert_eq!(
//             ext.left_difference(&clock),
//             VectorClock::from_key_value(&["d", "e"], &[2, 51])
//         );
//     }
// }
