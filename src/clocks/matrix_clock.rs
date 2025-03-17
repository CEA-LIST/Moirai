use crate::protocol::membership::View;

use super::{
    clock::{self, Clock},
    dependency_clock::DependencyClock,
    vector_clock::VectorClock,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result},
    hash::Hash,
    ops::{Add, AddAssign},
    rc::Rc,
};

#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct MatrixClock {
    clock: HashMap<usize, DependencyClock>,
    view: Rc<View>,
}

impl MatrixClock {
    pub fn new(view: &Rc<View>) -> Self {
        Self {
            clock: view
                .members
                .iter()
                .enumerate()
                .map(|(i, _)| (i, DependencyClock::new(&view, &view.members[i])))
                .collect(),
            view: Rc::clone(view),
        }
    }

    pub fn change_view(&mut self, view: &Rc<View>) {
        for m in &self.view.members {
            if !view.members.contains(m) {
                self.clock
                    .remove(&self.view.members.iter().position(|x| x == m).unwrap());
            }
        }
        for m in &view.members {
            if !self.view.members.contains(m) {
                self.clock.insert(
                    view.members.iter().position(|x| x == m).unwrap(),
                    DependencyClock::new(&view, m),
                );
            }
        }
        self.view = Rc::clone(view);
        for (_, d) in &mut self.clock {
            let mut to_remove = vec![];
            for (o, _) in d.iter() {
                if !view.members.contains(&self.view.members[*o]) {
                    to_remove.push(o);
                }
            }
            for o in &to_remove {
                d.clock.remove(o);
            }
        }
        assert!(self.is_square());
    }

    pub fn merge_clock(&mut self, member: &str, clock: &DependencyClock) {
        let i = self.view.members.iter().position(|m| m == member).unwrap();
        self.clock.get_mut(&i).unwrap().merge(clock);
        assert!(self.is_square());
    }

    pub fn clear(&mut self) {
        self.clock.clear();
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    pub fn svv(&self, ignore: &[&str]) -> DependencyClock {
        let mut svv = DependencyClock::new(&self.view, "");
        for (o, d) in &self.clock {
            if !ignore.contains(&self.view.members[*o].as_str()) {
                svv = Clock::min(&svv, &d);
            }
        }
        for (o, c) in &svv.clock {
            if self.clock[&o].get(&svv.view.members[*o]) == *c {
                svv.origin = *o;
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
        let mut vc = VectorClock::from_key_value(&keys, &vec![C::default(); keys.len()]);
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
    fn new() {
        let mc = MatrixClock::<&str, i32>::new(&["A", "B", "C"]);
        assert_eq!(mc.clock.len(), 3);
        assert_eq!(
            mc.get(&"A"),
            Some(&VectorClock::from_key_value(&["A", "B", "C"], &[0, 0, 0]))
        );
    }

    #[test_log::test]
    fn svv() {
        let mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from_key_value(&["A", "B"], &[10, 2]),
                VectorClock::from_key_value(&["A", "B"], &[8, 6]),
            ],
        );
        assert_eq!(
            mc.svv(&[]),
            VectorClock::from_key_value(&["A", "B"], &[8, 2])
        );
    }

    #[test_log::test]
    fn merge() {
        let mut mc1 = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from_key_value(&["A", "B"], &[10, 2]),
                VectorClock::from_key_value(&["A", "B"], &[8, 6]),
            ],
        );
        let mc2 = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from_key_value(&["A", "B"], &[9, 3]),
                VectorClock::from_key_value(&["A", "B"], &[7, 7]),
            ],
        );
        mc1.merge(&mc2);
        assert_eq!(
            mc1,
            MatrixClock::from(
                &["A", "B"],
                &[
                    VectorClock::from_key_value(&["A", "B"], &[10, 3]),
                    VectorClock::from_key_value(&["A", "B"], &[8, 7]),
                ]
            )
        );
    }

    #[test_log::test]
    fn svv_ignore() {
        let mc = MatrixClock::from(
            &["A", "B", "C"],
            &[
                VectorClock::from_key_value(&["A", "B", "C"], &[2, 6, 1]),
                VectorClock::from_key_value(&["A", "B", "C"], &[2, 5, 2]),
                VectorClock::from_key_value(&["A", "B", "C"], &[1, 4, 11]),
            ],
        );
        assert_eq!(
            mc.svv(&[&"C"]),
            VectorClock::from_key_value(&["A", "B", "C"], &[2, 5, 1]),
        );
    }

    #[test_log::test]
    fn display() {
        let mc = MatrixClock::from(
            &["A", "B", "C"],
            &[
                VectorClock::from_key_value(&["A", "B", "C"], &[0, 1, 1]),
                VectorClock::from_key_value(&["A", "B", "C"], &[1, 0, 1]),
                VectorClock::from_key_value(&["A", "B", "C"], &[1, 1, 0]),
            ],
        );
        assert_eq!(
            format!("{}", mc),
            "{\n  A: { A: 0, B: 1, C: 1 }\n  B: { A: 1, B: 0, C: 1 }\n  C: { A: 1, B: 1, C: 0 }\n}"
        );
    }

    #[test_log::test]
    fn add_key() {
        let mut mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from_key_value(&["A", "B"], &[10, 2]),
                VectorClock::from_key_value(&["A", "B"], &[8, 6]),
            ],
        );
        mc.add_key("C");
        assert_eq!(
            mc,
            MatrixClock::from(
                &["A", "B", "C"],
                &[
                    VectorClock::from_key_value(&["A", "B", "C"], &[10, 2, 0]),
                    VectorClock::from_key_value(&["A", "B", "C"], &[8, 6, 0]),
                    VectorClock::from_key_value(&["A", "B", "C"], &[0, 0, 0]),
                ]
            )
        );
    }

    #[test_log::test]
    fn keys() {
        let mc = MatrixClock::from(
            &["A", "B"],
            &[
                VectorClock::from_key_value(&["A", "B"], &[10, 2]),
                VectorClock::from_key_value(&["A", "B"], &[8, 6]),
            ],
        );
        assert!((mc.keys() == ["A", "B"]) || (mc.keys() == ["B", "A"]));
    }
}
