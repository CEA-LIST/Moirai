use super::clock::{Clock, ClockState, Full};
use crate::protocol::membership::ViewData;
#[cfg(feature = "utils")]
use deepsize::DeepSizeOf;
use log::error;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result},
    rc::Rc,
};
#[cfg(feature = "serde")]
use tsify::Tsify;

#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct MatrixClock {
    clock: HashMap<usize, Clock<Full>>,
    view: Rc<ViewData>,
    id: usize,
}

impl MatrixClock {
    pub fn new(view: &Rc<ViewData>, id: usize) -> Self {
        Self {
            clock: view
                .members
                .iter()
                .enumerate()
                .map(|(i, _)| (i, Clock::<Full>::new(view, Some(&view.members[i]))))
                .collect(),
            view: Rc::clone(view),
            id,
        }
    }

    pub fn change_view(&mut self, new_view: &Rc<ViewData>, id: usize) {
        let mut new_matrix_clock = MatrixClock::new(new_view, id);

        // TODO: resetting the clocks.
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

        debug_assert!(self.is_valid());
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

    pub fn get(&self, member: &str) -> Option<&Clock<Full>> {
        let i = &self.view.members.iter().position(|m| m == member);
        if let Some(i) = i {
            return self.clock.get(i);
        }
        None
    }

    pub fn get_mut(&mut self, member: &str) -> Option<&mut Clock<Full>> {
        let i = &self.view.members.iter().position(|m| m == member);
        if let Some(i) = i {
            return self.clock.get_mut(i);
        }
        None
    }

    pub fn get_by_idx(&self, index: usize) -> Option<&Clock<Full>> {
        self.clock.get(&index)
    }

    pub fn merge_clock<S: ClockState>(&mut self, clock: &Clock<S>) {
        let i = clock.origin.unwrap();
        self.clock.get_mut(&i).unwrap().merge(clock);
        debug_assert!(self.is_valid());
    }

    pub fn clear(&mut self) {
        self.clock.clear();
        debug_assert!(self.is_valid());
    }

    pub fn set_id(&mut self, id: usize) {
        self.id = id;
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    pub fn svv(&self, ignore: &[&String]) -> Clock<Full> {
        let mut svv = self.clock[&self.id].clone();
        for (o, d) in &self.clock {
            if !ignore.contains(&&self.view.members[*o]) {
                svv = Clock::<Full>::min(&svv, d);
            }
        }
        svv.origin = None;
        svv
    }

    /// Incrementally update the Stable Version Vector (SVV) given a new clock and the last SVV.
    /// Only recomputes columns that have changed.
    pub fn incremental_svv<S: ClockState>(
        &self,
        new_clock: &Clock<S>,
        lsv: &Clock<Full>,
        ignore: &[&String],
    ) -> Clock<Full> {
        match self.len() {
            1 => return self.origin_clock().clone(),
            2 => {
                let pos = if self.id == 0 { 1 } else { 0 };
                return self.clock.get(&pos).unwrap().clone();
            }
            _ => {}
        }

        let mut new_lsv = lsv.clone();
        let origin = new_clock.origin.expect("Clock must have an origin");

        for (&o, &new_val) in &new_clock.clock {
            let lsv_val = lsv.clock.get(&o).copied().unwrap_or(0);
            if new_val > lsv_val && o != origin {
                let min = self
                    .clock
                    .values()
                    .filter(|_| !ignore.contains(&&self.view.members[o]))
                    .filter_map(|c| c.clock.get(&o))
                    .min()
                    .copied()
                    .unwrap_or(lsv_val);
                new_lsv.clock.insert(o, min);
            }
        }

        new_lsv
    }

    pub fn merge(&mut self, other: &MatrixClock) {
        for (k, vc1) in &(other.clock) {
            self.clock
                .entry(*k)
                .and_modify(|vc2| vc2.merge(vc1))
                .or_insert_with(|| vc1.clone());
        }
        debug_assert!(self.is_valid());
    }

    /// Update the given key in the matrix clock with the value of the other keys
    pub fn most_update(&mut self, key: &str) {
        let i = self.view.member_pos(key).unwrap();
        let mut max = Clock::<Full>::new(&self.view, Some(key));
        for d in self.clock.values() {
            max.merge(d);
        }
        self.clock.get_mut(&i).unwrap().merge(&max);
        debug_assert!(self.is_valid());
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

    pub fn origin_clock(&self) -> &Clock<Full> {
        &self.clock[&self.id]
    }

    pub fn origin_clock_mut(&mut self) -> &mut Clock<Full> {
        self.clock
            .get_mut(&self.id)
            .expect("Origin clock not found")
    }

    pub fn build(view: &Rc<ViewData>, id: usize, clocks: &[&[usize]]) -> MatrixClock {
        assert!(clocks.len() == view.members.len());
        for c in clocks {
            assert!(c.len() == view.members.len());
        }
        let mut matrix = MatrixClock::new(view, id);
        for (i, c) in clocks.iter().enumerate() {
            let origin = &view.members[i];
            let dc = Clock::build(view, Some(origin), c);
            matrix.clock.insert(i, dc);
        }
        matrix
    }

    /// Check if the matrix clock is valid. A matrix clock is valid if it:
    /// - is square
    /// - no clock i has an entry j greater than the entry j of clock j
    /// - every entry i of the origin clock is equal or greater to the entry i of the clock i
    ///
    /// Returns true if the matrix clock is valid
    pub fn is_valid(&self) -> bool {
        let is_square = self.is_square();
        let valid_entries = self.clock.iter().all(|(_, d)| {
            d.clock
                .iter()
                .all(|(j, c)| self.clock[j].get(&self.view.members[*j]).unwrap() >= *c)
        });
        let valid_origin_entries = self.origin_clock().clock.iter().all(|(o, c)| {
            let cmp = *c
                >= self
                    .clock
                    .get(o)
                    .unwrap()
                    .get(&self.view.members[*o])
                    .unwrap();
            if !cmp {
                error!(
                    "Origin clock entry {} with value {} is less than clock entry {} with value {}",
                    self.view.members[*o],
                    c,
                    self.view.members[*o],
                    self.clock[o].get(&self.view.members[*o]).unwrap()
                );
            }
            cmp
        });
        if !is_square {
            error!("Matrix clock is not square");
        }
        if !valid_entries {
            error!("Matrix clock has invalid entries: {}", self);
        }
        if !valid_origin_entries {
            error!("Matrix clock has invalid origin entries");
        }
        is_square && valid_entries && valid_origin_entries
    }

    pub fn clock(&self) -> &HashMap<usize, Clock<Full>> {
        &self.clock
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
        Rc::new(ViewData {
            members: vec!["A".to_string(), "B".to_string()],
            id: 0,
        })
    }

    fn view_abc() -> Rc<ViewData> {
        Rc::new(ViewData {
            members: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            id: 0,
        })
    }

    #[test_log::test]
    fn new() {
        let mc = MatrixClock::build(&view_abc(), 0, &[&[0, 0, 0], &[0, 0, 0], &[0, 0, 0]]);
        assert_eq!(mc.clock.len(), 3);
        assert_eq!(
            mc.get("A"),
            Some(&Clock::build(&view_abc(), Some("A"), &[0, 0, 0]))
        );
    }

    #[test_log::test]
    fn svv() {
        let m = MatrixClock::build(&view_ab(), 0, &[&[10, 2], &[8, 6]]);
        assert_eq!(m.svv(&[]), Clock::build(&view_ab(), None, &[8, 2]));
    }

    #[test_log::test]
    fn incremental_svv() {
        let mut lsv = Clock::build(&view_ab(), None, &[0, 0]);
        let mut m = MatrixClock::build(&view_ab(), 0, &[&[0, 0], &[0, 0]]);
        let c_1: Clock<Full> = Clock::build(&view_ab(), Some("A"), &[1, 0]);
        m.merge_clock(&c_1);
        assert_eq!(
            m.incremental_svv(&c_1, &mut lsv, &[]),
            Clock::build(&view_ab(), None, &[0, 0])
        );
        let c_2: Clock<Full> = Clock::build(&view_ab(), Some("B"), &[0, 1]);
        m.get_mut("A").unwrap().merge(&c_2);
        m.merge_clock(&c_2);
        assert_eq!(
            m.incremental_svv(&c_2, &mut lsv, &[]),
            Clock::build(&view_ab(), None, &[0, 1])
        );
        let c_3: Clock<Full> = Clock::build(&view_ab(), Some("A"), &[2, 1]);
        m.get_mut("A").unwrap().merge(&c_3);
        m.merge_clock(&c_3);
        assert_eq!(
            m.incremental_svv(&c_3, &mut lsv, &[]),
            Clock::build(&view_ab(), None, &[0, 1])
        );
        let c_4: Clock<Full> = Clock::build(&view_ab(), Some("B"), &[2, 2]);
        m.get_mut("A").unwrap().merge(&c_4);
        m.merge_clock(&c_4);
        assert_eq!(
            m.incremental_svv(&c_4, &mut lsv, &[]),
            Clock::build(&view_ab(), None, &[2, 2])
        );
    }

    #[test_log::test]
    fn merge() {
        let mut mc1 = MatrixClock::build(&view_ab(), 0, &[&[10, 6], &[8, 6]]);
        let mc2 = MatrixClock::build(&view_ab(), 0, &[&[7, 13], &[1, 13]]);
        mc1.merge(&mc2);
        assert_eq!(
            mc1,
            MatrixClock::build(&view_ab(), 0, &[&[10, 13], &[8, 13]])
        );
    }

    #[test_log::test]
    fn svv_ignore() {
        let mc = MatrixClock::build(&view_abc(), 0, &[&[2, 6, 1], &[2, 5, 2], &[1, 4, 11]]);
        assert_eq!(
            mc.svv(&[&"C".to_string()]),
            Clock::build(&view_abc(), None, &[2, 5, 1])
        );
    }

    #[test_log::test]
    fn display() {
        let mc = MatrixClock::build(&view_abc(), 0, &[&[0, 1, 1], &[1, 0, 1], &[1, 1, 0]]);
        assert_eq!(
            format!("{}", mc),
            "{\n  A: { A: 0, B: 1, C: 1 }@A\n  B: { A: 1, B: 0, C: 1 }@B\n  C: { A: 1, B: 1, C: 0 }@C\n}"
        );
    }

    #[test_log::test]
    fn change_view() {
        let mut mc = MatrixClock::build(&view_ab(), 0, &[&[10, 6], &[8, 6]]);
        let view_data = ViewData {
            members: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            id: 1,
        };
        let rc_view_data = &Rc::new(view_data);
        mc.change_view(rc_view_data, 0);
        assert_eq!(
            mc,
            MatrixClock::build(rc_view_data, 0, &[&[10, 6, 0], &[8, 6, 0], &[0, 0, 0]])
        );
    }

    #[test_log::test]
    fn change_view_complex() {
        let view_data_0 = ViewData {
            members: vec!["B".to_string(), "C".to_string(), "D".to_string()],
            id: 0,
        };
        let view_data_0_rc = Rc::new(view_data_0);
        let mut mc = MatrixClock::build(&view_data_0_rc, 1, &[&[10, 6, 4], &[8, 6, 4], &[9, 0, 4]]);
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
        mc.change_view(&view_data_1_rc, 2);
        let test = MatrixClock::build(
            &view_data_1_rc,
            2,
            &[&[0, 0, 0, 0], &[0, 0, 0, 0], &[0, 0, 6, 4], &[0, 0, 0, 4]],
        );
        assert_eq!(test, mc);
    }
}
