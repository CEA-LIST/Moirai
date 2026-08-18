use std::fmt::{Debug, Display, Formatter};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    clock::version_vector::{Seq, Version},
    replica::ReplicaIdx,
    utils::intern_str::Resolver,
};

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
struct ReplicaMap(Vec<Version>);

impl ReplicaMap {
    fn get(&self, idx: ReplicaIdx) -> Option<&Version> {
        self.0.get(idx.0)
    }

    fn get_mut(&mut self, idx: ReplicaIdx) -> Option<&mut Version> {
        self.0.get_mut(idx.0)
    }
}

/// A matrix clock is a generalization in 2 dimensions of a vector clock. It is a square matrix of positive integers.
/// Each row represents the last vector clock known by the local replica from each member of the view.
/// The column-wise maximum is the clock of the local replica. The column-wise minimum is the stable version vector (SVV).
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
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

    /// Index of the replica this clock belongs to.
    pub fn origin_idx(&self) -> ReplicaIdx {
        self.origin_idx
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

    pub fn set_by_idx_incremental(&mut self, idx: ReplicaIdx, version: Version) -> Vec<ReplicaIdx> {
        let entry = self.entries.get_mut(idx).unwrap();
        let mut updated_columns = Vec::new();
        for (col_idx, seq) in version.iter() {
            if seq > entry.seq_by_idx(col_idx) {
                entry.set_by_idx(col_idx, seq);
                updated_columns.push(col_idx);
            }
        }
        updated_columns
    }

    pub fn add_replica(&mut self, idx: ReplicaIdx) {
        debug_assert!(idx.0 == self.entries.0.len());
        let version = Version::new(idx, self.resolver.clone());
        self.entries.0.push(version);
    }

    /// At each node i, the Stable Version Vector at i (SVVi) is the pointwise minimum of all version vectors in the LTM.
    /// Each operation in the PO-Log that causally precedes (happend-before) the SVV is considered stable and removed
    /// from the POLog, to be added to the sequential data type.
    ///
    /// # Complexity
    /// Runs in `O(n^2)` time complexity
    #[deprecated]
    pub fn column_wise_min(&self) -> Version {
        let mut iter = self.entries.0.iter();
        let mut min_clock = iter.next().unwrap().clone();

        for ver in iter {
            min_clock.meet(ver);
        }

        min_clock
    }

    /// Incremental SVV recomputation that only rescans columns whose value can advance
    /// relative to the provided `last_svv`. It stops a column scan early as soon as a value
    /// less than or equal to the previous minimum is found, since the minimum then cannot grow.
    ///
    /// # The result never goes below `last_svv`
    ///
    /// Which is what "can advance" above means, and what the rest of the system
    /// reads this frontier as. [`add_replica`](Self::add_replica) seeds a member
    /// that appears mid-session with a row of zeros, and a row stays at zero
    /// until that member *originates* an operation — so a joiner that adopts a
    /// snapshot and then never writes would otherwise pull every column back to
    /// zero. Measured: a settled three-member session went from a stable prefix
    /// of 615 to 0 in three seconds when a fourth member joined and stayed
    /// quiet.
    ///
    /// A retreat is not a safely conservative answer, it is a false one. Once a
    /// version has been reported stable, `Tcsb::prune_outbox` has deleted the
    /// events at or below it and the CRDT log has folded them into its
    /// tag-free stable state; nothing can put them back. A frontier that then
    /// sits below them breaks the invariant `Tcsb::snapshot` transfers on —
    /// that the whole outbox *is* the suffix above the stable version — and a
    /// joiner would be handed a compacted state alongside a suffix that does
    /// not start where the state ends.
    ///
    /// Holding the column instead degrades to the silent-member case: the
    /// frontier stops advancing until the new member speaks, and everything
    /// already compacted stays compacted.
    pub fn column_wise_min_incremental(
        &self,
        last_svv: &Version,
        updated_columns: &[ReplicaIdx],
    ) -> Version {
        let mut svv = last_svv.clone();

        for col_idx in updated_columns {
            let previous = svv.seq_by_idx(*col_idx);
            let mut min_value = Seq::MAX;
            for ver in self.entries.0.iter() {
                let entry = ver.seq_by_idx(*col_idx);
                if entry <= previous {
                    // Cannot advance this column's minimum. The comparison used
                    // to be `==`, so a row strictly *below* the baseline fell
                    // through to the running minimum instead of stopping the
                    // scan — which both let the frontier retreat and made the
                    // answer depend on the order the rows happened to be in.
                    min_value = previous;
                    break;
                }
                if entry < min_value {
                    min_value = entry;
                }
            }
            svv.set_by_idx(*col_idx, min_value);
        }

        svv
    }

    /// Check if the matrix clock is square
    /// # Complexity
    /// `O(n)`
    fn is_square(&self) -> bool {
        let n = self.entries.0.len();
        self.entries.0.iter().all(|c| c.len() == n)
    }

    /// Check that no clock i has an entry j greater than the entry j of clock j
    /// # Complexity
    /// `O(n^2)`
    fn diagonal(&self) -> bool {
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
        clock::{matrix_clock::MatrixClock, version_vector::Version},
        replica::ReplicaIdx,
        utils::intern_str::Interner,
    };

    #[test]
    #[allow(deprecated)]
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

    #[test]
    #[allow(deprecated)]
    fn column_wise_min_incremental_advances_only_changed_columns() {
        let mut interner = Interner::new();
        interner.intern("A");
        interner.intern("B");
        interner.intern("C");

        let resolver = interner.resolver();

        // Initial matrix and baseline SVV
        let baseline_mc = MatrixClock::build(
            resolver.clone(),
            ReplicaIdx(0),
            &[&[5, 11, 1], &[4, 11, 0], &[1, 8, 2]],
        );
        let baseline_svv = baseline_mc.column_wise_min();
        assert_eq!(
            baseline_svv,
            Version::build(resolver.clone(), ReplicaIdx(0), &[1, 8, 0])
        );

        // Row 2 is updated
        // Only columns 0, 1 are affected
        let mut updated_mc = baseline_mc;
        updated_mc.set_by_idx(
            ReplicaIdx(2),
            Version::build(resolver.clone(), ReplicaIdx(2), &[3, 10, 2]),
        );

        let incremental =
            updated_mc.column_wise_min_incremental(&baseline_svv, &[ReplicaIdx(0), ReplicaIdx(1)]);
        let full = updated_mc.column_wise_min();

        assert_eq!(incremental, full);
        assert_eq!(
            incremental,
            Version::build(resolver.clone(), ReplicaIdx(0), &[3, 10, 0])
        );
    }

    /// A member that appears mid-session and then says nothing must not pull
    /// the stable frontier back over operations that have already been
    /// compacted away. See `column_wise_min_incremental`.
    #[test]
    fn column_wise_min_incremental_holds_behind_a_silent_new_member() {
        let mut interner = Interner::new();
        interner.intern("A");
        interner.intern("B");

        let mut mc = MatrixClock::build(
            interner.resolver().clone(),
            ReplicaIdx(0),
            &[&[4, 4], &[4, 4]],
        );

        // C joins and never originates an operation, so `add_replica` leaves it
        // on a row of zeros indefinitely.
        let (c, _) = interner.intern("C");
        mc.add_replica(c);
        let resolver = interner.resolver();

        // A and B keep writing, which is what puts their columns up for rescan.
        mc.set_by_idx(
            ReplicaIdx(0),
            Version::build(resolver.clone(), ReplicaIdx(0), &[5, 5]),
        );
        mc.set_by_idx(
            ReplicaIdx(1),
            Version::build(resolver.clone(), ReplicaIdx(1), &[5, 5]),
        );

        let baseline = Version::build(resolver.clone(), ReplicaIdx(0), &[4, 4]);
        let svv = mc.column_wise_min_incremental(&baseline, &[ReplicaIdx(0), ReplicaIdx(1)]);

        // Held where it was. The unconstrained minimum here is C's row of
        // zeros, which is what used to be returned.
        assert_eq!(svv, baseline);
    }
}
