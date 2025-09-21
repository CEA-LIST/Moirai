use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
};

// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use tracing::error;
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    protocol::{
        clock::version_vector::Version,
        membership::{view::View, ReplicaId, ReplicaIdx},
    },
    utils::mut_owner::Reader,
};

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
        let matrix = Self {
            entries,
            view: Reader::clone(view),
            origin_idx,
        };
        debug_assert!(matrix.is_valid());
        matrix
    }

    pub fn origin_version(&self) -> &Version {
        self.entries.get(&self.origin_idx).unwrap()
    }

    pub fn origin_version_mut(&mut self) -> &mut Version {
        self.entries.get_mut(&self.origin_idx).unwrap()
    }

    pub fn version_by_id(&self, id: &ReplicaId) -> Option<&Version> {
        self.view
            .borrow()
            .get_idx(id)
            .and_then(|idx| self.version_by_idx(idx))
    }

    fn version_by_idx(&self, idx: ReplicaIdx) -> Option<&Version> {
        self.entries.get(&idx)
    }

    pub fn set_by_id(&mut self, id: &ReplicaId, version: &Version) {
        let idx_option = self.view.borrow().get_idx(id);
        if let Some(idx) = idx_option {
            self.set_by_idx(idx, version);
        }
    }

    fn set_by_idx(&mut self, idx: ReplicaIdx, version: &Version) {
        self.entries.insert(idx, version.clone());
    }

    /// Change the view of the matrix clock.
    /// # Invariant
    /// The previous indices must be preserved.
    pub fn change_view(&mut self, new_view: &Reader<View>) {
        self.view = Reader::clone(new_view);
        debug_assert!(self.is_valid());
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    ///
    /// # Complexity
    /// Runs in `O(n^2)` time complexity
    pub fn column_wise_min(&self) -> Version {
        let mut min_clock = Version::new(&self.view, self.origin_idx);

        for (_, col_id) in self.view.borrow().members() {
            let mut min = usize::MAX;
            for (_, row_id) in self.view.borrow().members() {
                let seq = self
                    .version_by_id(row_id)
                    .and_then(|v| v.seq_by_id(col_id))
                    .unwrap_or(0);
                if seq < min {
                    min = seq;
                }
            }
            min_clock.set_by_id(col_id, min);
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
            for event_id in version.iter() {
                if event_id.origin_idx() != *i
                    && event_id.seq() > self.entries[&event_id.origin_idx()].origin_seq()
                {
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
            for event_id in ver.iter() {
                if origin_ver.seq_by_idx(event_id.origin_idx()).unwrap_or(0) < event_id.seq() {
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

        if !is_square {
            error!("Matrix clock is not square");
        }
        if !diagonal {
            error!("Matrix clock is not diagonal");
        }
        if !dominate {
            error!("Matrix clock does not dominate");
        }

        is_square && diagonal && dominate
    }

    #[cfg(test)]
    fn build(view: &Reader<View>, origin_idx: ReplicaIdx, values: &[&[usize]]) -> Self {
        let mut mc = MatrixClock::new(view, origin_idx);
        for (idx, val) in values.iter().enumerate() {
            let version = Version::build(view, idx, val);
            mc.set_by_idx(idx, &version);
        }
        mc
    }
}

impl Display for MatrixClock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{{")?;
        for (_, id) in self.view.borrow().members() {
            let version = self.version_by_id(id).unwrap();
            writeln!(f, "  {id}: {version}")?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        protocol::{
            clock::{matrix_clock::MatrixClock, version_vector::Version},
            membership::view::View,
        },
        utils::mut_owner::MutOwner,
    };

    fn view() -> MutOwner<View> {
        let mut view = View::new(&"a".to_string());
        view.add(&"b".to_string());
        view.add(&"c".to_string());
        MutOwner::new(view)
    }

    #[test]
    fn new() {
        let view = view().as_reader();
        let mc = MatrixClock::new(&view, 0);
        assert_eq!(mc.entries.len(), 3);
        assert_eq!(
            mc.version_by_id(&"a".to_string()),
            Some(&Version::new(&view, 0))
        );
    }

    #[test]
    fn column_wise_min() {
        let view = view().as_reader();
        let mc = MatrixClock::build(&view, 0, &[&[10, 6, 5], &[8, 6, 3], &[9, 4, 5]]);
        assert_eq!(mc.column_wise_min(), Version::build(&view, 0, &[8, 4, 3]));
    }

    //     #[test]
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

    //     #[test]
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
}
