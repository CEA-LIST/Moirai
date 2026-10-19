use super::*;
use crate::{
    broadcast::internalizer::{Interner, Resolver},
    event::lamport::Lamport,
};

fn resolver(n: usize) -> Resolver {
    let mut interner = Interner::new();
    for i in 0..n {
        interner.intern(&format!("r{i}").into());
    }
    interner.resolver().clone()
}

fn event(resolver: &Resolver, origin: usize, clock: &[usize]) -> Event<()> {
    let mut version = Version::new(ReplicaIdx(origin), resolver.clone());
    for (i, &seq) in clock.iter().enumerate() {
        version.set_by_idx(ReplicaIdx(i), seq);
    }
    Event::new(
        EventId::from(&version),
        Lamport::from(&version),
        (),
        version,
    )
}

fn assert_replay_order(graph: &EventGraph<()>) {
    let events: Vec<_> = graph.iter().collect();
    assert_eq!(
        graph
            .graph
            .node_indices()
            .map(|idx| idx.index())
            .collect::<Vec<_>>(),
        (0..events.len()).collect::<Vec<_>>()
    );
    for (rank, event) in events.iter().enumerate() {
        assert_eq!(graph.delivery_order(event.id()), Some(rank));
        for parent in graph.direct_predecessors(event.id()) {
            assert!(graph.delivery_order(&parent).unwrap() < rank);
        }
    }
}

#[test]
fn pruning_rebuilds_order_and_heads_before_later_appends() {
    let r = resolver(2);
    let a = event(&r, 0, &[1, 0]);
    let b = event(&r, 1, &[0, 1]);
    let c = event(&r, 1, &[1, 2]);
    let d = event(&r, 1, &[1, 3]);
    let mut graph = EventGraph::default();
    for e in [&a, &b, &c] {
        graph.append(e.clone());
    }
    graph.remove(a.id());
    graph.append(d.clone());
    assert_replay_order(&graph);
    assert_eq!(graph.predecessors(d.version()).len(), 3);
    graph.retain(|e| e.id() != c.id() && e.id() != d.id());
    assert_eq!(graph.heads(), &HashSet::from_iter([b.id().clone()]));
    assert_replay_order(&graph);
}

#[test]
fn critical_cut_detection_matches_reachability_for_all_five_node_dags() {
    let r = resolver(5);
    // Every DAG admits a topological labeling. Enumerate all forward-edge
    // subsets and compare the boundary test with the all-pairs definition.
    for mask in 0..(1 << 10) {
        let mut ancestors = [[false; 5]; 5];
        let mut edge = 0;
        let mut graph = EventGraph::default();
        for child in 0..5 {
            for parent in 0..child {
                if mask & (1 << edge) != 0 {
                    ancestors[child][parent] = true;
                    for earlier in 0..parent {
                        ancestors[child][earlier] |= ancestors[parent][earlier];
                    }
                }
                edge += 1;
            }
            let mut clock = [0; 5];
            for parent in 0..child {
                clock[parent] = usize::from(ancestors[child][parent]);
            }
            clock[child] = 1;
            graph.append(event(&r, child, &clock));
        }
        for cut in 0..=5 {
            let critical = (0..cut).all(|p| (cut..5).all(|c| ancestors[c][p]));
            assert_eq!(
                graph.is_critical_prefix(cut),
                critical,
                "mask={mask}, cut={cut}"
            );
            let mut stable = CausalFrontier::new(r.clone());
            for i in 0..cut {
                stable.set_by_idx(ReplicaIdx(i), 1);
            }
            let expected = (0..=cut)
                .filter(|&k| (0..k).all(|p| (k..5).all(|c| ancestors[c][p])))
                .max()
                .unwrap();
            assert_eq!(graph.stable_critical_prefix(&stable), expected);
            if cut > 0 && critical {
                let suffix = graph.after_checkpoint(cut);
                assert_replay_order(&suffix);
                assert_eq!(suffix.len(), 5 - cut);
            }
        }
    }
}
