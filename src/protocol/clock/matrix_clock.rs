use std::{collections::HashMap, fmt::Debug};

// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::protocol::membership::{ReplicaId, ReplicaIdx, View};
use crate::{protocol::clock::version_vector::Version, utils::mut_owner::Reader};

/// A matrix clock is a generalization of a vector clock. It is a square matrix of positive integers.
/// Each row represents the last vector clock known by the local replica from each member of the view.
/// The column-wise maximum is the clock of the local replica. The column-wise minimum is the stable version vector (SVV).
// #[cfg_attr(feature = "utils", derive(DeepSizeOf))]
#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct MatrixClock {
    entries: HashMap<ReplicaIdx, Version>,
    origin_idx: ReplicaIdx,
    view: Reader<View>,
}

impl MatrixClock {
    pub fn new(view: &Reader<View>, origin_idx: ReplicaIdx) -> Self {
        let entries = view
            .borrow()
            .members()
            .map(|(idx, _)| (*idx, Version::new(view, *idx)))
            .collect();
        Self {
            entries,
            view: Reader::clone(view),
            origin_idx,
        }
    }

    pub fn origin_version(&self) -> &Version {
        self.entries.get(&self.origin_idx).unwrap()
    }

    pub fn origin_version_mut(&mut self) -> &mut Version {
        self.entries.get_mut(&self.origin_idx).unwrap()
    }

    pub fn get_by_id(&self, id: &ReplicaId) -> Option<&Version> {
        self.view
            .borrow()
            .get_idx(id)
            .and_then(|idx| self.get_by_idx(idx))
    }

    pub fn get_by_idx(&self, idx: ReplicaIdx) -> Option<&Version> {
        self.entries.get(&idx)
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    ///
    /// # Complexity
    /// Runs in `O(n^2)` time complexity
    pub fn column_wise_min(&self) -> Version {
        let mut min_clock = Version::new(&self.view, self.origin_idx);

        for row in self.entries.values() {
            for (idx, seq) in row.iter() {
                if let Some(min_seq) = min_clock.seq_by_idx(*idx) {
                    if *seq < min_seq {
                        min_clock.set_by_idx(*idx, *seq);
                    }
                } else {
                    min_clock.set_by_idx(*idx, *seq);
                }
            }
        }

        min_clock
    }

    /// Check if the matrix clock is square
    /// # Complexity
    /// `O(n)`
    fn is_square(&self) -> bool {
        let n = self.entries.len();
        self.entries.values().all(|c| c.len() == n)
    }

    /// Check that no clock i has an entry j greater than the entry j of clock j
    /// # Complexity
    /// `O(n^2)`
    fn diagonal(&self) -> bool {
        for (i, version) in self.entries.iter() {
            for (j, seq) in version.iter() {
                if j != i && *seq > self.entries[j].origin_seq() {
                    return false;
                }
            }
        }
        true
    }

    /// Check that every entry i of the origin clock is equal or greater than the entry i of the clock i
    /// # Complexity
    /// `O(n^2)`
    fn dominate(&self) -> bool {
        let origin_ver = self.origin_version();
        for ver in self.entries.values() {
            for (idx, seq) in ver.iter() {
                if origin_ver.seq_by_idx(*idx).unwrap_or(0) < *seq {
                    return false;
                }
            }
        }
        true
    }

    /// Check if the matrix clock is valid. A matrix clock is valid if it:
    /// - is square
    /// - no clock i has an entry j greater than the entry j of clock j
    /// - every entry i of the origin clock is equal or greater than the entry i of the clock i
    ///
    /// Returns true if the matrix clock is valid
    /// # Complexity
    /// `O(n^2)`
    pub fn is_valid(&self) -> bool {
        let is_square = self.is_square();
        let diagonal = self.diagonal();
        let dominate = self.dominate();

        is_square && diagonal && dominate
    }
}

// impl Display for MatrixClock {
//     fn fmt(&self, f: &mut Formatter<'_>) -> Result {
//         writeln!(f, "{{")?;
//         for (i, m) in self.view.members().iter().enumerate() {
//             writeln!(f, "  {}: {}", m, self.clock.get(&i).unwrap())?;
//         }
//         write!(f, "}}")
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     fn view_ab() -> Rc<ViewData> {
//         Rc::new(ViewData {
//             members: vec!["A".to_string(), "B".to_string()],
//             id: 0,
//         })
//     }

//     fn view_abc() -> Rc<ViewData> {
//         Rc::new(ViewData {
//             members: vec!["A".to_string(), "B".to_string(), "C".to_string()],
//             id: 0,
//         })
//     }

//     #[test_log::test]
//     fn new() {
//         let mc = MatrixClock::build(&view_abc(), 0, &[&[0, 0, 0], &[0, 0, 0], &[0, 0, 0]]);
//         assert_eq!(mc.clock.len(), 3);
//         assert_eq!(
//             mc.get("A"),
//             Some(&Version::build(&view_abc(), Some("A"), &[0, 0, 0]))
//         );
//     }

//     #[test_log::test]
//     fn svv() {
//         let m = MatrixClock::build(&view_ab(), 0, &[&[10, 2], &[8, 6]]);
//         assert_eq!(m.svv(&[]), Version::build(&view_ab(), None, &[8, 2]));
//     }

//     #[test_log::test]
//     fn incremental_svv() {
//         let mut lsv = Version::build(&view_ab(), None, &[0, 0]);
//         let mut m = MatrixClock::build(&view_ab(), 0, &[&[0, 0], &[0, 0]]);
//         let c_1: Version = Version::build(&view_ab(), Some("A"), &[1, 0]);
//         m.merge_clock(&c_1);
//         assert_eq!(
//             m.incremental_svv(&c_1, &mut lsv, &[]),
//             Version::build(&view_ab(), None, &[0, 0])
//         );
//         let c_2: Version = Version::build(&view_ab(), Some("B"), &[0, 1]);
//         m.get_mut("A").unwrap().merge(&c_2);
//         m.merge_clock(&c_2);
//         assert_eq!(
//             m.incremental_svv(&c_2, &mut lsv, &[]),
//             Version::build(&view_ab(), None, &[0, 1])
//         );
//         let c_3: Version = Version::build(&view_ab(), Some("A"), &[2, 1]);
//         m.get_mut("A").unwrap().merge(&c_3);
//         m.merge_clock(&c_3);
//         assert_eq!(
//             m.incremental_svv(&c_3, &mut lsv, &[]),
//             Version::build(&view_ab(), None, &[0, 1])
//         );
//         let c_4: Version = Version::build(&view_ab(), Some("B"), &[2, 2]);
//         m.get_mut("A").unwrap().merge(&c_4);
//         m.merge_clock(&c_4);
//         assert_eq!(
//             m.incremental_svv(&c_4, &mut lsv, &[]),
//             Version::build(&view_ab(), None, &[2, 2])
//         );
//     }

//     #[test_log::test]
//     fn merge() {
//         let mut mc1 = MatrixClock::build(&view_ab(), 0, &[&[10, 6], &[8, 6]]);
//         let mc2 = MatrixClock::build(&view_ab(), 0, &[&[7, 13], &[1, 13]]);
//         mc1.merge(&mc2);
//         assert_eq!(
//             mc1,
//             MatrixClock::build(&view_ab(), 0, &[&[10, 13], &[8, 13]])
//         );
//     }

//     #[test_log::test]
//     fn svv_ignore() {
//         let mc = MatrixClock::build(&view_abc(), 0, &[&[2, 6, 1], &[2, 5, 2], &[1, 4, 11]]);
//         assert_eq!(
//             mc.svv(&[&"C".to_string()]),
//             Version::build(&view_abc(), None, &[2, 5, 1])
//         );
//     }

//     #[test_log::test]
//     fn display() {
//         let mc = MatrixClock::build(&view_abc(), 0, &[&[0, 1, 1], &[1, 0, 1], &[1, 1, 0]]);
//         assert_eq!(
//             format!("{}", mc),
//             "{\n  A: { A: 0, B: 1, C: 1 }@A\n  B: { A: 1, B: 0, C: 1 }@B\n  C: { A: 1, B: 1, C: 0 }@C\n}"
//         );
//     }

//     #[test_log::test]
//     fn change_view() {
//         let mut mc = MatrixClock::build(&view_ab(), 0, &[&[10, 6], &[8, 6]]);
//         let view_data = ViewData {
//             members: vec!["A".to_string(), "B".to_string(), "C".to_string()],
//             id: 1,
//         };
//         let rc_view_data = &Rc::new(view_data);
//         mc.change_view(rc_view_data, 0);
//         assert_eq!(
//             mc,
//             MatrixClock::build(rc_view_data, 0, &[&[10, 6, 0], &[8, 6, 0], &[0, 0, 0]])
//         );
//     }

//     #[test_log::test]
//     fn change_view_complex() {
//         let view_data_0 = ViewData {
//             members: vec!["B".to_string(), "C".to_string(), "D".to_string()],
//             id: 0,
//         };
//         let view_data_0_rc = Rc::new(view_data_0);
//         let mut mc = MatrixClock::build(&view_data_0_rc, 1, &[&[10, 6, 4], &[8, 6, 4], &[9, 0, 4]]);
//         let view_data_1 = ViewData {
//             members: vec![
//                 "E".to_string(),
//                 "A".to_string(),
//                 "C".to_string(),
//                 "D".to_string(),
//             ],
//             id: 1,
//         };
//         let view_data_1_rc = Rc::new(view_data_1);
//         mc.change_view(&view_data_1_rc, 2);
//         let test = MatrixClock::build(
//             &view_data_1_rc,
//             2,
//             &[&[0, 0, 0, 0], &[0, 0, 0, 0], &[0, 0, 6, 4], &[0, 0, 0, 4]],
//         );
//         assert_eq!(test, mc);
//     }
// }
