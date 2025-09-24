use std::fmt::{Debug, Display, Formatter};

// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use tracing::error;
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    protocol::clock::version_vector::Version,
    utils::intern_str::{ReplicaIdx, Resolver},
};

#[derive(Debug, PartialEq)]
struct ReplicaMap(Vec<Version>);

impl ReplicaMap {
    fn get(&self, idx: ReplicaIdx) -> Option<&Version> {
        self.0.get(idx.0)
    }

    fn get_mut(&mut self, idx: ReplicaIdx) -> Option<&mut Version> {
        self.0.get_mut(idx.0)
    }
}

/// A matrix clock is a generalization of a vector clock. It is a square matrix of positive integers.
/// Each row represents the last vector clock known by the local replica from each member of the view.
/// The column-wise maximum is the clock of the local replica. The column-wise minimum is the stable version vector (SVV).
// #[cfg_attr(feature = "utils", derive(DeepSizeOf))]
#[derive(Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct MatrixClock {
    entries: ReplicaMap,
    origin_idx: ReplicaIdx,
    resolver: Resolver,
}

impl MatrixClock {
    pub fn new(origin_idx: ReplicaIdx, resolver: Resolver) -> Self {
        let mut entries = Vec::with_capacity(resolver.len());
        for i in 0..resolver.len() {
            entries.push(Version::new(ReplicaIdx(i), resolver.clone()));
        }

        let matrix = Self {
            entries: ReplicaMap(entries),
            origin_idx,
            resolver,
        };
        debug_assert!(matrix.is_valid());
        matrix
    }

    pub fn origin_version(&self) -> &Version {
        self.entries.get(self.origin_idx).unwrap()
    }

    pub fn origin_version_mut(&mut self) -> &mut Version {
        self.entries.get_mut(self.origin_idx).unwrap()
    }

    pub fn version_by_idx(&self, idx: ReplicaIdx) -> Option<&Version> {
        self.entries.get(idx)
    }

    pub fn set_by_idx(&mut self, idx: ReplicaIdx, version: Version) {
        *self.entries.get_mut(idx).unwrap() = version;
    }

    pub fn add_replica(&mut self, idx: ReplicaIdx) {
        if idx.0 == self.entries.0.len() {
            let version = Version::new(idx, self.resolver.clone());
            self.entries.0.push(version);
        } else {
            panic!("Big issue");
        }
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the POLog that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    ///
    /// # Complexity
    /// Runs in `O(n^2)` time complexity
    pub fn column_wise_min(&self) -> Version {
        let mut iter = self.entries.0.iter();
        let mut min_clock = iter.next().unwrap().clone();

        for ver in iter {
            min_clock.meet(ver);
        }

        // for (_, col_id) in self.view.borrow().members() {
        //     let mut min = usize::MAX;
        //     for (_, row_id) in self.view.borrow().members() {
        //         let seq = self
        //             .version_by_id(row_id)
        //             .and_then(|v| v.seq_by_id(col_id))
        //             .unwrap_or(0);
        //         if seq < min {
        //             min = seq;
        //         }
        //     }
        //     min_clock.set_by_id(col_id, min);
        // }

        min_clock
    }

    /// Check if the matrix clock is square
    /// # Complexity
    /// `O(n)`
    fn is_square(&self) -> bool {
        // TODO: change
        // let n = self.entries.len();
        // self.entries.values().all(|c| c.len() == n)
        true
    }

    /// Check that no clock i has an entry j greater than the entry j of clock j
    /// # Complexity
    /// `O(n^2)`
    fn diagonal(&self) -> bool {
        // TODO: change

        for (i, version) in self.entries.0.iter().enumerate() {
            for (idx, seq) in version.iter() {
                if idx.0 != i && seq > self.entries.0[idx.0].origin_seq() {
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
        for ver in self.entries.0.iter() {
            for (idx, seq) in ver.iter() {
                if origin_ver.seq_by_idx(idx) < seq {
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
    fn build(resolver: Resolver, origin_idx: ReplicaIdx, values: &[&[usize]]) -> Self {
        let mut mc = MatrixClock::new(origin_idx, resolver.clone());
        for (idx, val) in values.iter().enumerate() {
            let version = Version::build(resolver.clone(), ReplicaIdx(idx), val);
            mc.set_by_idx(ReplicaIdx(idx), version);
        }
        mc
    }
}

impl Display for MatrixClock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{{")?;
        for (idx, version) in self.entries.0.iter().enumerate() {
            let id = self.resolver.resolve(ReplicaIdx(idx)).unwrap();
            writeln!(f, "  {id}: {version}")?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        protocol::clock::{matrix_clock::MatrixClock, version_vector::Version},
        utils::intern_str::{Interner, ReplicaIdx},
    };

    #[test]
    fn column_wise_min() {
        let mut interner = Interner::new();
        interner.intern("A");
        interner.intern("B");
        interner.intern("C");

        let resolver = interner.resolver();

        let mc = MatrixClock::build(
            resolver.clone(),
            ReplicaIdx(0),
            &[&[10, 6, 5], &[8, 6, 3], &[9, 4, 5]],
        );
        assert_eq!(
            mc.column_wise_min(),
            Version::build(resolver.clone(), ReplicaIdx(0), &[8, 4, 3])
        );
    }
}
