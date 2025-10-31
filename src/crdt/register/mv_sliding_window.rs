/// I’ve been exploring a new (useless?) CRDT that generalizes the familiar register data type along two orthogonal dimensions: concurrency and history.
/// A sequential register holds a single value (the most recent write).
/// Extending this structure along the history axis yields the k-sliding window, a register that retains and returns the k most recent writes.
/// Extending it instead along the concurrency axis gives the Multi-Value Register (MV-Register), which captures the latest concurrent writes (the maximal events in the causal history, i.e., the causal frontier).
/// The next step is to combine both extensions: the Multi-Value Sliding Window (MV-SW).
/// his register returns the most recent writes from the causal frontier, together with their causal predecessors up to a depth of k.
/// Interestingly, when k is sufficiently large, the MV-SW returns the concurrent history itself.
/// The MV-Register can be seen as a MV-SW of depth 1
/// Dimension of a register: 0.
/// Dimension of a k-sliding window: 1 (history, ordered).
/// Dimension of a MV-Register: 1 (concurrency, unordered).
/// Dimension of a MV-Sliding Window: 2 (concurrency + history, partially ordered).
use std::{fmt::Debug, hash::Hash};

use petgraph::graph::DiGraph;

use crate::{
    protocol::{
        crdt::{
            eval::Eval,
            pure_crdt::PureCRDT,
            query::{QueryOperation, Read},
        },
        event::{tag::Tag, tagged_op::TaggedOp},
        state::unstable_state::IsUnstableState,
    },
    HashMap,
};

#[derive(Clone, Debug)]
pub enum MVSlidingWindow<V> {
    Write(V),
}

impl<V> MVSlidingWindow<V> {
    const K: i32 = 1;
}

impl<V> PureCRDT for MVSlidingWindow<V>
where
    V: Debug + Clone + Eq + Hash,
{
    const DISABLE_STABILIZE: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
    const DISABLE_R_WHEN_R: bool = true;

    type Value = DiGraph<V, ()>;
    type StableState = Vec<Self>;

    // TODO: establish redundancy rules

    fn redundant_itself<'a>(
        _new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        false
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for MVSlidingWindow<V>
where
    V: Debug + Clone + Eq + Hash + Default,
{
    /// Returns the $k$-depth causal history preceding the heads as a directed graph.
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        _stable: &<MVSlidingWindow<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut graph = DiGraph::new();
        // Max depth
        let k = MVSlidingWindow::<V>::K;
        let heads = unstable.frontier();
        let mut node_map: HashMap<String, petgraph::graph::NodeIndex> = HashMap::default();

        for head in heads {
            let mut to_visit = vec![(head.clone(), 0)];
            while let Some((current, depth)) = to_visit.pop() {
                if depth >= k {
                    continue;
                }
                let op = match current.op() {
                    MVSlidingWindow::Write(v) => v.clone(),
                };
                let _ = *node_map
                    .entry(current.id().to_string())
                    .or_insert_with(|| graph.add_node(op.clone()));

                for parent_id in unstable.parents(current.id()).iter() {
                    if let Some(parent) = unstable.get(parent_id) {
                        to_visit.push((parent.clone(), depth + 1));
                    }
                }
            }
        }
        graph
    }
}

#[cfg(test)]
mod tests {
    use petgraph::dot::Config;

    use crate::{
        crdt::{register::mv_sliding_window::MVSlidingWindow, test_util::twins_log},
        protocol::{crdt::query::Read, replica::IsReplica, state::event_graph::EventGraph},
    };

    #[test]
    fn simple_mv_sliding_window() {
        let (mut replica_a, mut replica_b) = twins_log::<EventGraph<MVSlidingWindow<&str>>>();

        let e1 = replica_a.send(MVSlidingWindow::Write("A1")).unwrap();
        let e2 = replica_b.send(MVSlidingWindow::Write("A2")).unwrap();

        replica_b.receive(e1);
        replica_a.receive(e2);

        let e3 = replica_a.send(MVSlidingWindow::Write("A3")).unwrap();
        let e4 = replica_b.send(MVSlidingWindow::Write("A4")).unwrap();
        replica_b.receive(e3);
        replica_a.receive(e4);

        let e5 = replica_a.send(MVSlidingWindow::Write("A5")).unwrap();
        let e6 = replica_b.send(MVSlidingWindow::Write("A6")).unwrap();

        replica_b.receive(e5);
        replica_a.receive(e6);

        println!(
            "Replica A: {:?}",
            petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[Config::EdgeNoLabel])
        );
        println!(
            "Replica B: {:?}",
            petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[Config::EdgeNoLabel])
        );
        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }
}
