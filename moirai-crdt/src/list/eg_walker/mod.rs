//! Positional list replay with update-wins semantics.
//!
//! `GraphLog::stabilize` folds the largest causally stable critical prefix into
//! the snapshot: every retained event must observe the entire discarded prefix.
//! `ReadAt` returns `HistoryStabilized` for versions before or concurrent with the
//! retained prefix. Save and restore its frontier with the snapshot.

mod causal_cursor;
mod document;
mod hooks;
mod item;
mod presence_state;
mod replay;

use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Formatter},
    marker::PhantomData,
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use hooks::EgWalkerHooks;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::op_generator::OpGenerator, generator::value_generator::ValueGenerator,
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    data_type::{
        query::{QueryOperation, Read},
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, ReplicatedDataType,
            Reset, Stabilization,
        },
        sequential::SequentialDataType,
    },
    state::{
        cache::IncrementalCache,
        unstable_state::{CausalReplay, IsUnstableCore},
    },
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};
use replay::replay;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::list::update::ListUpdate;

/// Eg-Walker positional list semantics.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct List<V>(PhantomData<V>);

impl<V: Clone + Debug> List<V> {
    fn visible_len<U: CausalReplay<ListUpdate<V>>>(stable: &[V], unstable: &U) -> usize {
        replay(stable, unstable, unstable.iter()).visible_len()
    }
}

impl<V> ListUpdate<V> {
    /// Apply an already-sequential operation to a snapshot or cache.
    /// Concurrent operations must go through causal replay first.
    fn apply_sequential(self, value: &mut Vec<V>) {
        match self {
            Self::Insert { content, pos } => value.insert(pos, content),
            Self::Delete { pos } => {
                value.remove(pos);
            }
            Self::DeleteRange { start, len } => {
                value.drain(start..start + len);
            }
            Self::Update { .. } => {}
        }
    }
}

#[derive(Clone, Debug)]
pub enum ListRejection {
    /// The operation refers to a visible position outside the current read state.
    OutOfBounds { pos: usize, len: usize },
    /// The storage does not provide the causal services required for replay.
    CausalReplayUnavailable,
}

impl Display for ListRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfBounds { pos, len } => {
                write!(
                    f,
                    "Position {pos} is out of bounds for document of length {len}"
                )
            }
            Self::CausalReplayUnavailable => {
                write!(f, "Eg-Walker requires causal replay services")
            }
        }
    }
}

impl<U, V> Precondition<U> for List<V>
where
    U: CausalReplay<Self::Update>,
    V: Debug + Clone,
{
    fn is_enabled(
        stable: &Self::Stable,
        unstable: &U,
        update: &Self::Update,
    ) -> Result<(), Self::Rejection> {
        unstable.validate(update, stable)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StableList<V>(Vec<V>);

impl<V> Default for StableList<V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<V> From<Vec<V>> for StableList<V> {
    fn from(value: Vec<V>) -> Self {
        Self(value)
    }
}

impl<V> std::ops::Deref for StableList<V> {
    type Target = [V];
    fn deref(&self) -> &[V] {
        &self.0
    }
}

impl<V> SequentialDataType for StableList<V>
where
    V: Debug + Clone,
{
    type Update = ListUpdate<V>;
    type Rejection = ListRejection;

    fn is_default(&self) -> bool {
        self.0.is_empty()
    }

    fn apply(&mut self, update: &Self::Update) {
        update.clone().apply_sequential(&mut self.0);
    }

    fn is_enabled(&self, update: &Self::Update) -> Result<(), Self::Rejection> {
        match update {
            ListUpdate::Insert { pos, .. } => {
                if *pos > self.0.len() {
                    return Err(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: self.0.len(),
                    });
                }
            }
            ListUpdate::Delete { pos } => {
                if *pos >= self.0.len() {
                    return Err(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: self.0.len(),
                    });
                }
            }
            ListUpdate::DeleteRange { start, len } => {
                if *start + *len > self.0.len() {
                    return Err(ListRejection::OutOfBounds {
                        pos: *start + *len,
                        len: self.0.len(),
                    });
                }
            }
            ListUpdate::Update { pos, .. } => {
                if *pos >= self.0.len() {
                    return Err(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: self.0.len(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl<V> ReplicatedDataType for List<V>
where
    V: Debug + Clone,
{
    type Stable = StableList<V>;
    type Rejection = ListRejection;
    type Update = ListUpdate<V>;
}

impl<U, V> CausalCompaction<U> for List<V>
where
    V: Debug + Clone,
    U: IsUnstableCore<ListUpdate<V>>,
{
    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
}

impl<U, V> CausalReset<U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self::Update>,
{
    fn reset(
        version: &Version,
        conservative: bool,
        stable: &Self::Stable,
        unstable: &U,
    ) -> Reset<Self::Update> {
        if !conservative {
            return Reset::Prune;
        }

        unstable.conservative_reset(version, stable)
    }
}

impl<U, V> CausalStabilize<U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self::Update>,
{
    fn causal_stabilize(
        frontier: &CausalFrontier,
        stable: &Self::Stable,
        unstable: &U,
    ) -> Stabilization<Self::Stable> {
        unstable.stabilize(frontier, stable)
    }
}

impl<V, U> Eval<U, Read<Vec<V>>> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<ListUpdate<V>>,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        _q: &Read<Vec<V>>,
    ) -> Vec<V> {
        replay(&stable.0, unstable, unstable.iter()).to_list()
    }
}

/// Convenience read for character lists.
impl<U> Eval<U, Read<String>> for List<char>
where
    U: CausalReplay<ListUpdate<char>>,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        _q: &Read<String>,
    ) -> String {
        let chars: Vec<char> = Self::execute_query(stable, unstable, &Read::new());
        chars.into_iter().collect()
    }
}

impl<V> IncrementalCache<ListUpdate<V>> for Vec<V>
where
    V: Clone,
{
    fn apply(&mut self, op: &ListUpdate<V>) {
        op.clone().apply_sequential(self);
    }
}

/// Read the list at a historical version.
pub struct ReadAt<V> {
    version: Version,
    _marker: std::marker::PhantomData<V>,
}

impl<V> ReadAt<V> {
    pub fn new(version: &Version) -> Self {
        Self {
            version: version.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V> QueryOperation for ReadAt<V> {
    type Response = Result<V, HistoryStabilized>;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "ReadAt"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        vec![format!("version={}", self.version)]
    }
}

impl<V, U> Eval<U, ReadAt<Vec<V>>> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<ListUpdate<V>>,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        q: &ReadAt<Vec<V>>,
    ) -> Result<Vec<V>, HistoryStabilized> {
        if let Some(roots) = unstable.roots() {
            let frontier = CausalFrontier::from(&q.version);
            match &frontier.partial_cmp(roots) {
                Some(Ordering::Less) | None => {
                    return Err(HistoryStabilized {
                        frontier: roots.clone(),
                    });
                }
                _ => {}
            }
        }
        let predecessors = unstable.predecessors(&q.version);
        Ok(replay(&stable.0, unstable, predecessors).to_list())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryStabilized {
    pub frontier: CausalFrontier,
}

impl Display for HistoryStabilized {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "history before or concurrent with frontier {} has been pruned.",
            self.frontier
        )
    }
}

impl std::error::Error for HistoryStabilized {}

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for List<V>
where
    U: CausalReplay<Self::Update>,
    V: ValueGenerator + Debug + Clone,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        stable: &Self::Stable,
        unstable: &U,
    ) -> Self::Update {
        // Fuzzing generates only user operations that are enabled in the current
        // visible document, because out-of-bounds operations are rejected before
        // they enter the log.
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Insert,
            Delete,
            DeleteRange,
        }

        let list = Self::execute_query(stable, unstable, &Read::new());

        let choice = if list.is_empty() {
            &Choice::Insert
        } else if list.len() < 3 {
            let dist = WeightedIndex::new([3, 2]).unwrap();
            &[Choice::Insert, Choice::Delete][dist.sample(rng)]
        } else {
            let dist = WeightedIndex::new([7, 2, 1]).unwrap();
            &[Choice::Insert, Choice::Delete, Choice::DeleteRange][dist.sample(rng)]
        };

        match choice {
            Choice::Insert => {
                use moirai_fuzz::generator::value_generator::ValueGenerator;

                let pos = rng.random_range(0..=list.len());
                let c = V::generate(rng, &<V as ValueGenerator>::Config::default());
                ListUpdate::insert(c, pos)
            }
            Choice::Delete => {
                let pos = rng.random_range(0..list.len());
                ListUpdate::delete(pos)
            }
            Choice::DeleteRange => {
                let start = rng.random_range(0..list.len());
                let max_len = list.len() - start;
                let len = if max_len == 0 {
                    0
                } else {
                    rng.random_range(1..=max_len)
                };
                ListUpdate::delete_range(start, len)
            }
        }
    }
}

#[cfg(test)]
mod tests;
