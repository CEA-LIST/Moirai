use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result},
    rc::Rc,
};

use super::{clock::Clock, dependency_clock::DependencyClock};
use crate::protocol::membership::ViewData;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use tsify::Tsify;

#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]

pub struct MatrixClock {
    clock: HashMap<usize, DependencyClock>,
    view: Rc<ViewData>,
}

impl MatrixClock {
    pub fn new(view: &Rc<ViewData>) -> Self {
        Self {
            clock: view
                .members
                .iter()
                .enumerate()
                .map(|(i, _)| (i, DependencyClock::new(view, &view.members[i])))
                .collect(),
            view: Rc::clone(view),
        }
    }

    pub fn change_view(&mut self, new_view: &Rc<ViewData>) {
        let mut new_matrix_clock = MatrixClock::new(new_view);

        for (i, new_d) in new_matrix_clock.clock.iter_mut() {
            for (j, c) in new_d.clock.iter_mut() {
                let i_member = &new_matrix_clock.view.members[*i];
                let j_member = &new_matrix_clock.view.members[*j];
                if let Some(old_d) = self.get(i_member) {
                    if let Some(old_c) = old_d.get(j_member) {
                        *c = old_c;
                    }
                }
            }
        }

        *self = new_matrix_clock;

        assert!(self.is_valid());
    }

    pub fn members(&self) -> &Vec<String> {
        &self.view.members
    }

    /// Can panic if the member is not found
    pub fn dot(&self, member: &str) -> usize {
        self.clock
            .get(&self.view.members.iter().position(|m| m == member).unwrap())
            .unwrap()
            .dot()
    }

    pub fn get(&self, member: &str) -> Option<&DependencyClock> {
        let i = &self.view.members.iter().position(|m| m == member);
        if let Some(i) = i {
            return self.clock.get(i);
        }
        None
    }

    pub fn get_mut(&mut self, member: &str) -> Option<&mut DependencyClock> {
        let i = &self.view.members.iter().position(|m| m == member);
        if let Some(i) = i {
            return self.clock.get_mut(i);
        }
        None
    }

    pub fn merge_clock(&mut self, member: &str, clock: &DependencyClock) {
        let i = self.view.members.iter().position(|m| m == member).unwrap();
        self.clock.get_mut(&i).unwrap().merge(clock);
        assert!(self.is_valid());
    }

    pub fn clear(&mut self) {
        self.clock.clear();
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    pub fn svv(&self, id: &str, ignore: &[&String]) -> DependencyClock {
        let mut svv = self
            .get(id)
            .unwrap_or_else(|| panic!("Member {} not found", id))
            .clone();
        for (o, d) in &self.clock {
            if !ignore.contains(&&self.view.members[*o]) {
                svv = Clock::min(&svv, d);
            }
        }
        // TODO
        // for (o, c) in &svv.clock {
        //     if self.clock[o].get(&svv.view.members[*o]) == *c {
        //         svv.origin = Some(*o);
        //     }
        // }
        svv.origin = None;
        svv
    }

    pub fn merge(&mut self, other: &MatrixClock) {
        for (k, vc1) in &(other.clock) {
            self.clock
                .entry(*k)
                .and_modify(|vc2| vc2.merge(vc1))
                .or_insert_with(|| vc1.clone());
        }
        assert!(self.is_valid());
    }

    /// Update the given key in the matrix clock with the value of the other keys
    pub fn most_update(&mut self, key: &str) {
        let i = self.view.members.iter().position(|m| m == key).unwrap();
        let mut max = DependencyClock::new(&self.view, key);
        for d in self.clock.values() {
            max.merge(d);
        }
        self.clock.get_mut(&i).unwrap().merge(&max);
        assert!(self.is_valid());
    }

    /// Check if the matrix clock is square
    pub fn is_square(&self) -> bool {
        let n = self.clock.len();
        self.clock.values().all(|d| d.clock.len() == n)
    }

    pub fn len(&self) -> usize {
        self.clock.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clock.is_empty()
    }

    pub fn build(view: &Rc<ViewData>, clocks: &[&[usize]]) -> MatrixClock {
        assert!(clocks.len() == view.members.len());
        for c in clocks {
            assert!(c.len() == view.members.len());
        }
        let mut matrix = MatrixClock::new(view);
        for (i, c) in clocks.iter().enumerate() {
            let origin = &view.members[i];
            let dc = DependencyClock::build(view, Some(origin), c);
            matrix.clock.insert(i, dc);
        }
        matrix
    }

    /// Check if the matrix clock is valid. A matrix clock is valid if it:
    /// - is square
    /// - no clock i has an entry j greater than the entry j of clock j
    ///
    /// Returns true if the matrix clock is valid
    pub fn is_valid(&self) -> bool {
        self.is_square()
            && self.clock.iter().all(|(_, d)| {
                d.clock
                    .iter()
                    .all(|(j, c)| self.clock[j].get(&self.view.members[*j]).unwrap() >= *c)
            })
    }
}

impl Display for MatrixClock {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        writeln!(f, "{{")?;
        for (i, m) in self.view.members.iter().enumerate() {
            writeln!(f, "  {}: {}", m, self.clock.get(&i).unwrap())?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view_ab() -> Rc<ViewData> {
        let view = Rc::new(ViewData {
            members: vec!["A".to_string(), "B".to_string()],
            id: 0,
        });
        view
    }

    fn view_abc() -> Rc<ViewData> {
        let view = Rc::new(ViewData {
            members: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            id: 0,
        });
        view
    }

    #[test_log::test]
    fn new() {
        let mc = MatrixClock::build(&view_abc(), &[&[0, 0, 0], &[0, 0, 0], &[0, 0, 0]]);
        assert_eq!(mc.clock.len(), 3);
        assert_eq!(
            mc.get(&"A"),
            Some(&DependencyClock::build(&view_abc(), Some(&"A"), &[0, 0, 0]))
        );
    }

    #[test_log::test]
    fn svv() {
        let m = MatrixClock::build(&view_ab(), &[&[10, 2], &[8, 6]]);
        assert_eq!(
            m.svv(&"A", &[]),
            DependencyClock::build(&view_ab(), None, &[8, 2])
        );
    }

    #[test_log::test]
    fn merge() {
        let mut mc1 = MatrixClock::build(&view_ab(), &[&[10, 2], &[8, 6]]);
        let mc2 = MatrixClock::build(&view_ab(), &[&[9, 3], &[7, 7]]);
        mc1.merge(&mc2);
        assert_eq!(mc1, MatrixClock::build(&view_ab(), &[&[10, 3], &[8, 7]]));
    }

    #[test_log::test]
    fn svv_ignore() {
        let mc = MatrixClock::build(&view_abc(), &[&[2, 6, 1], &[2, 5, 2], &[1, 4, 11]]);
        assert_eq!(
            mc.svv(&"A", &[&"C".to_string()]),
            DependencyClock::build(&view_abc(), None, &[2, 5, 1])
        );
    }

    #[test_log::test]
    fn display() {
        let mc = MatrixClock::build(&view_abc(), &[&[0, 1, 1], &[1, 0, 1], &[1, 1, 0]]);
        assert_eq!(
            format!("{}", mc),
            "{\n  A: { A: 0, B: 1, C: 1 }@A\n  B: { A: 1, B: 0, C: 1 }@B\n  C: { A: 1, B: 1, C: 0 }@C\n}"
        );
    }

    #[test_log::test]
    fn change_view() {
        let mut mc = MatrixClock::build(&view_ab(), &[&[10, 2], &[8, 6]]);
        let view_data = ViewData {
            members: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            id: 1,
        };
        let rc_view_data = &Rc::new(view_data);
        mc.change_view(rc_view_data);
        assert_eq!(
            mc,
            MatrixClock::build(rc_view_data, &[&[10, 2, 0], &[8, 6, 0], &[0, 0, 0]])
        );
    }

    #[test_log::test]
    fn change_view_complex() {
        let view_data_0 = ViewData {
            members: vec!["B".to_string(), "C".to_string(), "D".to_string()],
            id: 0,
        };
        let view_data_0_rc = Rc::new(view_data_0);
        let mut mc = MatrixClock::build(&view_data_0_rc, &[&[10, 2, 3], &[8, 6, 4], &[9, 0, 4]]);
        let view_data_1 = ViewData {
            members: vec![
                "E".to_string(),
                "A".to_string(),
                "C".to_string(),
                "D".to_string(),
            ],
            id: 1,
        };
        let view_data_1_rc = Rc::new(view_data_1);
        mc.change_view(&view_data_1_rc);
        let test = MatrixClock::build(
            &view_data_1_rc,
            &[&[0, 0, 0, 0], &[0, 0, 0, 0], &[0, 0, 6, 4], &[0, 0, 0, 4]],
        );
        assert_eq!(test, mc);
    }
}
