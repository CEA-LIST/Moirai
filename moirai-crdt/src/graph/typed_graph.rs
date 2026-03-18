use std::{fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use moirai_fuzz::value_generator::{NumberConfig, ValueGenerator};
use moirai_macros::typed_graph;
#[cfg(feature = "fuzz")]
use rand::Rng;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Foo(u32);

#[cfg(feature = "fuzz")]
impl ValueGenerator for Foo {
    type Config = ();

    fn generate(rng: &mut impl Rng, _config: &Self::Config) -> Self {
        Foo(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Bar(u32);

#[cfg(feature = "fuzz")]
impl ValueGenerator for Bar {
    type Config = ();

    fn generate(rng: &mut impl Rng, _config: &Self::Config) -> Self {
        Bar(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Baz(u32);

#[cfg(feature = "fuzz")]
impl ValueGenerator for Baz {
    type Config = ();

    fn generate(rng: &mut impl Rng, _config: &Self::Config) -> Self {
        Baz(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FooBarEdge;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BarBazEdge;

typed_graph! {
    graph: MyTypedGraph,
    vertex: MyVertex,
    edge: MyEdge,
    arcs_type: MyArcs,
    vertices { Foo, Bar, Baz },
    connections {
        FooToBar: Foo -> Bar (FooBarEdge) [0, 1],
        BarToBaz: Bar -> Baz (BarBazEdge) [1, 1],
        FooToBaz: Foo -> Baz (FooBarEdge) [0, *],
    }
}

#[cfg(test)]
mod tests {
    use moirai_macros::typed_graph::Arc;
    #[cfg(feature = "fuzz")]
    use moirai_protocol::{crdt::policy::Policy, state::unstable_state::IsUnstableState};
    use moirai_protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog};

    use super::*;
    use crate::{graph::typed_graph::MyTypedGraph, policy::LwwPolicy, utils::membership::twins};

    fn assert_convergence<R: IsReplica<VecLog<MyTypedGraph<LwwPolicy>>>>(
        replica_a: &R,
        replica_b: &R,
    ) {
        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some(),
            "Replicas did not converge!\nA: {:?}\nB: {:?}",
            petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[]),
            petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[]),
        );
    }

    #[test]
    fn add_delete_vertex() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let init = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_b.receive(init);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_eq!(replica_b.query(Read::new()).node_count(), 1);

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        let e2 = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_b.receive(e1);
        replica_a.receive(e2);

        // assert_convergence(&replica_a, &replica_b);
        assert_eq!(replica_b.query(Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
    }

    #[test]
    fn no_upper_bound() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_b.receive(e1);

        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Baz(Baz(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let e3 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Baz(Baz(2)),
            })
            .unwrap();
        replica_a.receive(e3);

        let e4 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Baz(Baz(3)),
            })
            .unwrap();
        replica_a.receive(e4);

        let e5 = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBaz(Arc {
                source: Foo(1),
                target: Baz(1),
                kind: FooBarEdge,
            })))
            .unwrap();
        replica_b.receive(e5);

        let e6 = replica_b
            .send(MyTypedGraph::AddArc(MyArcs::FooToBaz(Arc {
                source: Foo(1),
                target: Baz(2),
                kind: FooBarEdge,
            })))
            .unwrap();
        replica_a.receive(e6);

        let e7 = replica_b
            .send(MyTypedGraph::AddArc(MyArcs::FooToBaz(Arc {
                source: Foo(1),
                target: Baz(3),
                kind: FooBarEdge,
            })))
            .unwrap();
        replica_a.receive(e7);

        assert_convergence(&replica_a, &replica_b);
        assert_eq!(replica_a.query(Read::new()).node_count(), 4);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 3);
    }

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_b.receive(e1);

        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let e3 = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(1),
                target: Bar(1),
                kind: FooBarEdge,
            })))
            .unwrap();
        replica_b.receive(e3);

        assert_convergence(&replica_a, &replica_b);
        assert_eq!(replica_a.query(Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 1);
    }

    #[test]
    fn concurrent_add_same_vertex() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let event_a = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        let event_b = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn remove_vertex_cascades_arcs() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_b.receive(e1);
        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let e3 = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(1),
                target: Bar(1),
                kind: FooBarEdge,
            })))
            .unwrap();
        replica_b.receive(e3);

        let e4 = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyVertex::Bar(Bar(1)),
            })
            .unwrap();
        replica_a.receive(e4);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 0);
        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn concurrent_add_arc_vs_remove_vertex() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(1)),
            })
            .unwrap();
        replica_b.receive(e1);
        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let event_a = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(1),
                target: Bar(1),
                kind: FooBarEdge,
            })))
            .unwrap();
        let event_b = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyVertex::Bar(Bar(1)),
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn concurrent_add_same_plus_remove() {
        // this test reproduce this trace:
        // digraph {  0 [ label="[AddVertex { id: User(User(13)) }@(1:1)]"]  1 [ label="[AddVertex { id: User(User(19)) }@(1:2)]"]  2 [ label="[AddVertex { id: Database(Database(7)) }@(0:1)]"]  3 [ label="[AddArc(UserToDb(Arc { source: User(13), target: Database(7), kind: UserToDbConnection }))@(1:3)]"]  4 [ label="[RemoveArc(UserToDb(Arc { source: User(13), target: Database(7), kind: UserToDbConnection }))@(1:4)]"]  5 [ label="[AddArc(UserToDb(Arc { source: User(13), target: Database(7), kind: UserToDbConnection }))@(0:2)]"]  1 -> 0 [ ]  2 -> 0 [ ]  3 -> 2 [ ]  3 -> 1 [ ]  4 -> 3 [ ]  5 -> 2 [ ]  5 -> 1 [ ]}
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e_b_1 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(13)),
            })
            .unwrap();

        replica_a.receive(e_b_1);

        let e_b_2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(19)),
            })
            .unwrap();

        let e_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(7)),
            })
            .unwrap();

        replica_a.receive(e_b_2);
        replica_b.receive(e_a_1);

        let e_a_2 = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(13),
                target: Bar(7),
                kind: FooBarEdge,
            })))
            .unwrap();

        let e_b_3 = replica_b
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(13),
                target: Bar(7),
                kind: FooBarEdge,
            })))
            .unwrap();

        let e_b_4 = replica_b
            .send(MyTypedGraph::RemoveArc(MyArcs::FooToBar(Arc {
                source: Foo(13),
                target: Bar(7),
                kind: FooBarEdge,
            })))
            .unwrap();

        replica_b.receive(e_a_2);
        replica_a.receive(e_b_3);
        replica_a.receive(e_b_4);

        let graph_a = replica_a.query(Read::new());
        let graph_b = replica_b.query(Read::new());

        assert_eq!(graph_a.node_count(), 3);
        assert_eq!(graph_b.node_count(), 3);
        assert_eq!(graph_b.edge_count(), 1);
        assert_eq!(graph_a.edge_count(), 1);

        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn concurrent_add_violates_max_constraint() {
        // this test reproduce this trace:
        // digraph {  0 [ label="[AddVertex { id: User(User(15)) }@(1:1)]"]  1 [ label="[AddVertex { id: Database(Database(4)) }@(1:2)]"]  2 [ label="[AddArc(UserToDb(Arc { source: User(15), target: Database(4), kind: UserToDbConnection }))@(1:3)]"]  3 [ label="[AddVertex { id: Database(Database(17)) }@(0:1)]"]  4 [ label="[AddArc(UserToDb(Arc { source: User(15), target: Database(17), kind: UserToDbConnection }))@(0:2)]"]  0 -> 1 [ ]  1 -> 2 [ ]  0 -> 3 [ ]  3 -> 4 [ ]  0 -> 4 [ ]}
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e_b_1 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(15)),
            })
            .unwrap();

        replica_a.receive(e_b_1);

        let e_b_2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(4)),
            })
            .unwrap();

        let e_b_3 = replica_b
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(15),
                target: Bar(4),
                kind: FooBarEdge,
            })))
            .unwrap();

        let e_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(17)),
            })
            .unwrap();

        let e_a_2 = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(15),
                target: Bar(17),
                kind: FooBarEdge,
            })))
            .unwrap();

        replica_a.receive(e_b_2);
        replica_a.receive(e_b_3);
        replica_b.receive(e_a_1);
        replica_b.receive(e_a_2);

        let graph_a = replica_a.query(Read::new());
        let graph_b = replica_b.query(Read::new());

        assert_eq!(graph_a.node_count(), 3);
        assert_eq!(graph_b.node_count(), 3);
        assert_eq!(graph_a.edge_count(), 1);
        assert_eq!(graph_b.edge_count(), 1);

        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn concurrent_add_arc_plus_remove_vertex() {
        // this test reproduce this trace:
        // digraph {  0 [ label="[AddVertex { id: User(User(17)) }@(0:1)]"]  1 [ label="[AddVertex { id: Database(Database(3)) }@(0:2)]"]  2 [ label="[AddVertex { id: Database(Database(11)) }@(1:1)]"]  3 [ label="[AddArc(UserToDb(Arc { source: User(17), target: Database(3), kind: UserToDbConnection }))@(0:3)]"]  4 [ label="[AddArc(UserToDb(Arc { source: User(17), target: Database(11), kind: UserToDbConnection }))@(1:2)]"]  5 [ label="[RemoveVertex { id: Database(Database(11)) }@(1:3)]"]  0 -> 1 [ ]  0 -> 2 [ ]  1 -> 3 [ ]  2 -> 4 [ ]  0 -> 4 [ ]  4 -> 5 [ ]  0 -> 5 [ ]}
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Foo(Foo(17)),
            })
            .unwrap();
        replica_b.receive(e_a_1);
        let e_a_2 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(3)),
            })
            .unwrap();
        let e_a_3 = replica_a
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(17),
                target: Bar(3),
                kind: FooBarEdge,
            })))
            .unwrap();

        let b_e_1 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyVertex::Bar(Bar(11)),
            })
            .unwrap();
        let b_e_2 = replica_b
            .send(MyTypedGraph::AddArc(MyArcs::FooToBar(Arc {
                source: Foo(17),
                target: Bar(11),
                kind: FooBarEdge,
            })))
            .unwrap();
        let b_e_3 = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyVertex::Bar(Bar(11)),
            })
            .unwrap();

        replica_a.receive(b_e_1);
        replica_a.receive(b_e_2);
        replica_a.receive(b_e_3);
        replica_b.receive(e_a_2);
        replica_b.receive(e_a_3);

        let graph_a = replica_a.query(Read::new());
        let graph_b = replica_b.query(Read::new());

        assert_eq!(graph_a.node_count(), 2);
        assert_eq!(graph_b.node_count(), 2);
        assert_eq!(graph_a.edge_count(), 1);
        assert_eq!(graph_b.edge_count(), 1);

        assert_convergence(&replica_a, &replica_b);
    }

    #[cfg(feature = "fuzz")]
    impl<P> moirai_fuzz::op_generator::OpGenerator for MyTypedGraph<P>
    where
        P: Policy,
    {
        type Config = ();

        fn generate(
            rng: &mut impl rand::Rng,
            _config: &Self::Config,
            stable: &Self::StableState,
            unstable: &impl IsUnstableState<Self>,
        ) -> Self {
            use moirai_protocol::crdt::eval::Eval;
            use moirai_protocol::crdt::query::Read;
            use rand::distr::{Distribution, weighted::WeightedIndex};
            use rand::seq::IndexedRandom;

            enum Choice {
                AddVertex,
                RemoveVertex,
                AddArc,
                RemoveArc,
            }

            let graph = Self::execute_query(Read::new(), stable, unstable);
            let constraints = compute_arc_constraints(&graph);
            let existing_vertices: Vec<_> = graph.node_weights().cloned().collect();

            let choice = if graph.node_count() < 2 {
                &Choice::AddVertex
            } else if graph.edge_count() == 0 {
                if constraints.addable.is_empty() {
                    let dist = WeightedIndex::new([2, 1]).unwrap();
                    &[Choice::AddVertex, Choice::RemoveVertex][dist.sample(rng)]
                } else {
                    let dist = WeightedIndex::new([2, 1, 3]).unwrap();
                    &[Choice::AddVertex, Choice::RemoveVertex, Choice::AddArc][dist.sample(rng)]
                }
            } else if constraints.removable.is_empty() && constraints.addable.is_empty() {
                let dist = WeightedIndex::new([2, 1]).unwrap();
                &[Choice::AddVertex, Choice::RemoveVertex][dist.sample(rng)]
            } else if !constraints.removable.is_empty() && constraints.addable.is_empty() {
                let dist = WeightedIndex::new([2, 1, 2]).unwrap();
                &[Choice::AddVertex, Choice::RemoveVertex, Choice::RemoveArc][dist.sample(rng)]
            } else if constraints.removable.is_empty() && !constraints.addable.is_empty() {
                let dist = WeightedIndex::new([2, 1, 3]).unwrap();
                &[Choice::AddVertex, Choice::RemoveVertex, Choice::AddArc][dist.sample(rng)]
            } else {
                let dist = WeightedIndex::new([2, 1, 3, 2]).unwrap();
                &[
                    Choice::AddVertex,
                    Choice::RemoveVertex,
                    Choice::AddArc,
                    Choice::RemoveArc,
                ][dist.sample(rng)]
            };

            match choice {
                Choice::AddVertex => MyTypedGraph::AddVertex {
                    id: <MyVertex as moirai_fuzz::value_generator::ValueGenerator>::generate(
                        rng,
                        &(),
                    ),
                },
                Choice::RemoveVertex => {
                    let vertex = existing_vertices.choose(rng).unwrap().clone();
                    MyTypedGraph::RemoveVertex { id: vertex }
                }
                Choice::AddArc => {
                    MyTypedGraph::AddArc(constraints.addable.choose(rng).unwrap().clone())
                }
                Choice::RemoveArc => {
                    MyTypedGraph::RemoveArc(constraints.removable.choose(rng).unwrap().clone())
                }
            }
        }
    }

    // TODO: implement ValueGenerator for each vertex type, not just the wrapper enum

    #[cfg(feature = "fuzz")]
    impl moirai_fuzz::value_generator::ValueGenerator for MyVertex {
        type Config = ();

        fn generate(rng: &mut impl Rng, _config: &Self::Config) -> Self {
            use rand::prelude::IndexedRandom;

            enum Choice {
                Foo,
                Bar,
                Baz,
            }
            let choices = [Choice::Foo, Choice::Bar, Choice::Baz];
            let choice = choices.choose(rng).unwrap();
            match choice {
                Choice::Foo => MyVertex::Foo(Foo::generate(rng, &())),
                Choice::Bar => MyVertex::Bar(Bar::generate(rng, &())),
                Choice::Baz => MyVertex::Baz(Baz::generate(rng, &())),
            }
        }
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_typed_graph() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run_1 = RunConfig::new(0.4, 8, 100, None, None, true, false);
        let runs = vec![run_1; 10_000];

        let config = FuzzerConfig::<VecLog<MyTypedGraph<LwwPolicy>>>::new(
            "typed_graph",
            runs,
            true,
            |a, b| {
                let node = a.node_count() == b.node_count();
                let edge = a.edge_count() == b.edge_count();
                let is_valid = validate_schema(&a);

                let is_valid = match is_valid {
                    Ok(_) => true,
                    Err(violations) => {
                        if violations
                            .iter()
                            .all(|v| matches!(v, SchemaViolation::BelowMin { .. }))
                        {
                            true
                        } else {
                            println!("Schema violations: {:?}", violations);
                            false
                        }
                    }
                };
                node && edge && is_valid
            },
            false,
        );

        fuzzer::<VecLog<MyTypedGraph<LwwPolicy>>>(config);
    }
}
