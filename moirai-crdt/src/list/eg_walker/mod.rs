//! Positional list replay with update-wins membership.
//!
//! `GraphLog::stabilize` folds the largest causally stable critical prefix into
//! the snapshot: every retained event must observe the entire discarded prefix.
//! `ReadAt` returns `HistoryCompacted` for versions before or concurrent with the
//! resulting retention boundary. Save and restore that boundary with the snapshot.

mod causal_cursor;
mod document;
mod item;
mod presence_state;
mod replay;

use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::op_generator::OpGenerator, generator::value_generator::ValueGenerator,
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::Eval,
        query::{QueryOperation, Read},
        redundancy::RedundancyRelation,
        replicated_data_type::{CausalReset, ReplicatedDataType, UsesUnstableService},
    },
    event::tagged_op::TaggedOp,
    state::{cache::IncrementalCache, stable_state::IsStableState, unstable_state::CausalReplay},
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

use replay::replay_document;

// Single-character, position-based, pure op-based CRDT operations
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum List<V> {
    /// Insert `content` at the visible position observed by the issuing replica.
    Insert { content: V, pos: usize },
    /// Delete the visible item at `pos` in the issuing replica's parent version.
    Delete { pos: usize },
    /// Delete `len` visible items starting at `start`.
    DeleteRange { start: usize, len: usize },
    /// Mark the item at `pos` as freshly alive without changing its payload.
    ///
    /// This operation models update-wins behavior for nested values: a concurrent
    /// delete can only remove life dots it observed, so an unseen update dot keeps
    /// the item visible.
    Update { pos: usize },
}

impl<V> List<V>
where
    V: Clone + Debug,
{
    pub fn insert(content: V, pos: usize) -> Self {
        Self::Insert { content, pos }
    }

    pub fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    pub fn delete_range(start: usize, len: usize) -> Self {
        Self::DeleteRange { start, len }
    }

    pub fn update(pos: usize) -> Self {
        Self::Update { pos }
    }

    fn visible_len<U>(stable: &[V], unstable: &U) -> usize
    where
        U: CausalReplay<List<V>>,
    {
        replay_document(stable, unstable, unstable.iter()).visible_len()
    }
}

impl<V> List<V> {
    /// Apply an already-sequential operation to a materialized snapshot or cache.
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
}

impl Display for ListRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListRejection::OutOfBounds { pos, len } => {
                write!(
                    f,
                    "Position {pos} is out of bounds for document of length {len}"
                )
            }
        }
    }
}

impl<V> ReplicatedDataType for List<V>
where
    V: Debug + Clone,
{
    type StableState = Vec<V>;
    type Rejection = ListRejection;

    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;
    // Per-operation folding is unsafe for positional operations. GraphLog uses
    // the whole-prefix checkpoint hook instead.
    const DISABLE_STABILIZE: bool = true;
}

impl<V, U> UsesUnstableService<U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn checkpoint(
        stable: &Self::StableState,
        unstable: &U,
        prefix_len: usize,
    ) -> Option<Self::StableState> {
        Some(replay_document(stable, unstable, unstable.iter().take(prefix_len)).materialize())
    }

    /// Validate positional operations against the current visible document.
    ///
    /// EgWalker operations store user-facing positions, so enablement is checked by
    /// reading the current state and comparing the requested position with its
    /// length.
    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Result<(), Self::Rejection> {
        let state_len = Self::visible_len(stable, unstable);
        match op {
            List::Insert { pos, .. } => {
                (*pos <= state_len)
                    .then_some(())
                    .ok_or(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: state_len,
                    })
            }
            List::Update { pos } | List::Delete { pos } => {
                (*pos < state_len)
                    .then_some(())
                    .ok_or(ListRejection::OutOfBounds {
                        pos: *pos,
                        len: state_len,
                    })
            }
            List::DeleteRange { start, len } => start
                .checked_add(*len)
                .filter(|end| *end <= state_len)
                .map(|_| ())
                .ok_or(ListRejection::OutOfBounds {
                    pos: start.saturating_add(*len),
                    len: state_len,
                }),
        }
    }

    fn causal_reset(
        version: &Version,
        conservative: bool,
        stable: &Self::StableState,
        unstable: &U,
    ) -> CausalReset<Self> {
        if !conservative {
            return CausalReset::Prune;
        }
        check_retained(version, unstable.retention_boundary())
            .expect("Eg-Walker invariant: causal reset predates retained history");
        let predecessors = unstable.predecessors(version);
        let len = replay_document(stable, unstable, predecessors).visible_len();
        CausalReset::Inject(vec![List::DeleteRange { start: 0, len }])
    }
}

impl<V, U> Eval<Read<Vec<V>>, U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn execute_query(_q: &Read<Vec<V>>, stable: &Self::StableState, unstable: &U) -> Vec<V> {
        replay_document(stable, unstable, unstable.iter()).materialize()
    }
}

/// Convenience read for character lists.
impl<U> Eval<Read<String>, U> for List<char>
where
    U: CausalReplay<Self>,
{
    fn execute_query(_q: &Read<String>, stable: &Self::StableState, unstable: &U) -> String {
        let chars: Vec<char> = Self::execute_query(&Read::new(), stable, unstable);
        chars.into_iter().collect()
    }
}

impl<V> IncrementalCache<List<V>> for Vec<V>
where
    V: Clone,
{
    fn apply(&mut self, op: &List<V>) {
        op.clone().apply_sequential(self);
    }
}

/// Read the list at a historical version.
///
/// The unstable log supplies the predecessor events for the requested version,
/// and the same replay algorithm is used on that restricted stream.
/// Returns `HistoryCompacted` if the version does not include the checkpoint.
/// Versions must use this replica's local index mapping.
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
    type Response = Result<V, HistoryCompacted>;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "ReadAt"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        vec![format!("version={}", self.version)]
    }
}

impl<V, U> Eval<ReadAt<Vec<V>>, U> for List<V>
where
    V: Debug + Clone,
    U: CausalReplay<Self>,
{
    fn execute_query(
        q: &ReadAt<Vec<V>>,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Result<Vec<V>, HistoryCompacted> {
        check_retained(&q.version, unstable.retention_boundary())?;
        let predecessors = unstable.predecessors(&q.version);
        Ok(replay_document(stable, unstable, predecessors).materialize())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryCompacted {
    pub boundary: CausalFrontier,
}

impl Display for HistoryCompacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "history before or concurrent with checkpoint {} has been compacted",
            self.boundary
        )
    }
}

impl std::error::Error for HistoryCompacted {}

fn check_retained(
    version: &Version,
    boundary: Option<&CausalFrontier>,
) -> Result<(), HistoryCompacted> {
    if let Some(boundary) = boundary
        && !version.includes_frontier(boundary)
    {
        return Err(HistoryCompacted {
            boundary: boundary.clone(),
        });
    }
    Ok(())
}

impl<V> IsStableState<List<V>> for Vec<V>
where
    V: Debug + Clone,
{
    /// The stable state is the already-materialized snapshot used as replay baseline.
    fn is_default(&self) -> bool {
        self.is_empty()
    }

    /// Apply an operation to stable state using sequential list semantics.
    ///
    /// When a stable snapshot is built or supplied, it stores the plain list value.
    /// Unstable replay then starts from this state and interprets only the remaining
    /// events.
    fn apply(&mut self, value: List<V>) {
        value.apply_sequential(self);
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }

    /// Redundant-operation pruning for EgWalker stable state is still pending.
    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<List<V>>,
        _tagged_op: &TaggedOp<List<V>>,
    ) {
    }
}

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for List<V>
where
    U: CausalReplay<Self>,
    V: ValueGenerator + Debug + Clone,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        stable: &Self::StableState,
        unstable: &U,
    ) -> Self {
        // Fuzzing generates only user operations that are enabled in the current
        // visible document, because out-of-bounds operations are rejected before
        // they enter the log.
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Insert,
            Delete,
            DeleteRange,
        }

        let list = Self::execute_query(&Read::new(), stable, unstable);

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
                List::insert(c, pos)
            }
            Choice::Delete => {
                let pos = rng.random_range(0..list.len());
                List::delete(pos)
            }
            Choice::DeleteRange => {
                let start = rng.random_range(0..list.len());
                let max_len = list.len() - start;
                let len = if max_len == 0 {
                    0
                } else {
                    rng.random_range(1..=max_len)
                };
                List::delete_range(start, len)
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod flushing_tests;
