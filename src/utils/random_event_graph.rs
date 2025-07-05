use std::{fmt::Debug, fs::File, io::Write, time::Instant};

use rand::{
    seq::{IndexedRandom, IteratorRandom},
    Rng,
};

use crate::{
    crdt::test_util::n_members,
    protocol::{log::Log, pulling::Since, tcsb::Tcsb},
};

/// Configuration for random event graph generation in a partially connected distributed system.
pub struct EventGraphConfig<'a, Op> {
    pub n_replicas: usize,
    pub total_operations: usize,
    // TODO: associate a probability with each operation
    pub ops: &'a [Op],
    pub final_sync: bool,
    pub churn_rate: f64,
    pub reachability: Option<Vec<Vec<bool>>>,
    pub log_timing_csv: bool, // NEW FIELD: enable CSV logging
}

impl<'a, Op> Default for EventGraphConfig<'a, Op> {
    fn default() -> Self {
        Self {
            n_replicas: 4,
            total_operations: 100,
            ops: &[],
            final_sync: true,
            churn_rate: 0.0,
            reachability: None,
            log_timing_csv: false,
        }
    }
}

// TODO: An offline replica should be able to perform operations, but not broadcast them.
pub fn generate_event_graph<L>(config: EventGraphConfig<'_, L::Op>) -> Vec<Tcsb<L>>
where
    L: Log,
    L::Op: Clone + Debug,
    L::Value: Debug,
{
    let mut rng = rand::rng();
    let mut tcsbs: Vec<Tcsb<L>> = n_members::<L>(config.n_replicas);
    let reachability = config
        .reachability
        .unwrap_or_else(|| vec![vec![true; config.n_replicas]; config.n_replicas]);

    let mut local_ops_issued = 0;
    let mut online = vec![true; config.n_replicas];

    // Timing logs: one Vec per replica
    let mut timing_logs: Vec<Vec<(String, u128)>> = vec![Vec::new(); config.n_replicas];

    while local_ops_issued < config.total_operations {
        for online_flag in &mut online {
            *online_flag = rng.random::<f64>() >= config.churn_rate;
        }

        if let Some(replica_idx) = (0..config.n_replicas)
            .filter(|&i| online[i])
            .choose(&mut rng)
        {
            let op = config
                .ops
                .choose(&mut rng)
                .expect("`ops` slice cannot be empty")
                .clone();

            let start = Instant::now();
            let _ = tcsbs[replica_idx].tc_bcast(op);
            let duration = start.elapsed().as_micros();

            if config.log_timing_csv {
                timing_logs[replica_idx].push(("tc_bcast".to_string(), duration));
            }

            local_ops_issued += 1;

            for j in 0..config.n_replicas {
                if j != replica_idx && online[j] && reachability[replica_idx][j] {
                    let since = Since::new_from(&tcsbs[j]);
                    let batch = tcsbs[replica_idx].events_since(&since);

                    let batch_size: u128 = if let Ok(ref ok_batch) = batch {
                        ok_batch.events.len() as u128
                    } else {
                        0
                    };

                    let start = Instant::now();
                    tcsbs[j].deliver_batch(batch);
                    let duration = start.elapsed().as_micros();

                    if config.log_timing_csv && batch_size > 0 {
                        timing_logs[j].push(("deliver_batch".to_string(), duration / batch_size));
                    }
                }
            }
        }
    }

    if config.final_sync {
        for i in 0..config.n_replicas {
            for j in 0..config.n_replicas {
                if i != j && reachability[i][j] {
                    let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[j]));

                    let batch_size: u128 = if let Ok(ref ok_batch) = batch {
                        ok_batch.events.len() as u128
                    } else {
                        0
                    };

                    let start = Instant::now();
                    tcsbs[j].deliver_batch(batch);
                    let duration = start.elapsed().as_micros();

                    if config.log_timing_csv && batch_size > 0 {
                        timing_logs[j].push(("deliver_batch".to_string(), duration / batch_size));
                    }
                }
            }
        }
    }

    // Export to CSV if enabled
    if config.log_timing_csv {
        for (i, log) in timing_logs.into_iter().enumerate() {
            let filename = format!("replica_{i}_timings.csv");
            let mut file = File::create(&filename)
                .unwrap_or_else(|e| panic!("Failed to create {filename}: {e}"));
            writeln!(file, "operation,time_per_event_micros").unwrap();
            for (op, time) in log {
                writeln!(file, "{op},{time}").unwrap();
            }
        }
    }
    tcsbs
}

#[cfg(feature = "utils")]
#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use petgraph::graph::DiGraph;

    use super::*;
    use crate::{
        crdt::{
            aw_set::AWSet,
            class_diagram::{
                export_fancy_class_diagram, Class, ClassDiagram, ClassDiagramCrdt, Feature,
                Operation, PrimitiveType, Relation, RelationType,
            },
            lww_register::LWWRegister,
            multidigraph::Graph,
            mv_register::MVRegister,
            resettable_counter::Counter,
            to_register::TORegister,
            uw_map::{UWMap, UWMapLog},
            uw_multigraph::{UWGraph, UWGraphLog},
        },
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn folie() {
        for _ in 0..1_000 {
            // generate_uw_multigraph_convergence();
            // generate_class_diagram();
            // generate_uw_map_convergence();
            // generate_lww_register_convergence();
            // generate_deeply_nested_uw_map_convergence();
            // generate_ewflag_convergence();
            generate_graph_convergence();
        }
    }

    #[test_log::test]
    fn generate_ewflag_convergence() {
        use crate::crdt::ew_flag::EWFlag;

        let ops = vec![EWFlag::Enable, EWFlag::Disable, EWFlag::Clear];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 100,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<EventGraph<EWFlag>>(config);

        // All replicas' eval() should match
        let mut reference_val: bool = false;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
            }
            assert_eq!(tcsb.eval(), reference_val, "Replica {} did not converge", i);
        }
    }

    #[test_log::test]
    fn generate_aw_set_convergence() {
        let ops = vec![
            AWSet::Add("a"),
            AWSet::Add("b"),
            AWSet::Add("c"),
            AWSet::Add("d"),
            AWSet::Clear,
            AWSet::Remove("a"),
            AWSet::Remove("b"),
            AWSet::Remove("d"),
            AWSet::Remove("c"),
        ];

        let config = EventGraphConfig {
            n_replicas: 8,
            total_operations: 10_000,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<EventGraph<AWSet<&str>>>(config);

        // All replicas' eval() should match
        let mut reference: HashSet<&str> = HashSet::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert_eq!(
                tcsb.eval(),
                reference,
                "Replica {} did not converge with the reference.",
                tcsb.id
            );
        }
    }

    #[test_log::test]
    fn generate_counter_convergence() {
        let ops: Vec<Counter<isize>> = vec![Counter::Inc(1), Counter::Dec(1), Counter::Reset];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 10_000,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.7,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<EventGraph<Counter<isize>>>(config);

        // All replicas' eval() should match
        let mut reference_val: isize = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
            }
            assert_eq!(tcsb.eval(), reference_val);
        }
    }

    #[test_log::test]
    fn generate_uw_map_convergence() {
        let ops = vec![
            UWMap::Update("a".to_string(), MVRegister::Write(1)),
            UWMap::Update("a".to_string(), MVRegister::Write(2)),
            UWMap::Update("a".to_string(), MVRegister::Write(3)),
            UWMap::Remove("a".to_string()),
            UWMap::Update("b".to_string(), MVRegister::Write(5)),
            UWMap::Update("b".to_string(), MVRegister::Write(6)),
            UWMap::Update("b".to_string(), MVRegister::Write(7)),
            UWMap::Remove("b".to_string()),
            UWMap::Update("c".to_string(), MVRegister::Write(10)),
            UWMap::Update("c".to_string(), MVRegister::Write(20)),
            UWMap::Update("c".to_string(), MVRegister::Write(30)),
            UWMap::Remove("c".to_string()),
        ];

        let config = EventGraphConfig {
            n_replicas: 8,
            total_operations: 100,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<UWMapLog<String, EventGraph<MVRegister<i32>>>>(config);

        // All replicas' eval() should match
        let mut reference_val: HashMap<String, HashSet<i32>> = HashMap::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert_eq!(
                tcsb.eval(),
                reference_val,
                "Replica {} did not converge with the reference.",
                tcsb.id,
            );
            println!("Replica {}: {:?}", tcsb.id, tcsb.eval(),)
        }
    }

    #[test_log::test]
    fn generate_class_diagram() {
        let ops = vec![
            UWGraph::UpdateVertex("Car", Class::Name(MVRegister::Write("Car".to_string()))),
            UWGraph::UpdateVertex(
                "Car",
                Class::Operations(UWMap::Update(
                    "start".to_string(),
                    Operation::ReturnType(MVRegister::Write(PrimitiveType::Void)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Car",
                Class::Operations(UWMap::Update(
                    "start".to_string(),
                    Operation::Parameters(UWMap::Update(
                        "driver".to_string(),
                        MVRegister::Write(PrimitiveType::String),
                    )),
                )),
            ),
            UWGraph::UpdateVertex(
                "Wheel",
                Class::Features(UWMap::Update(
                    "brand".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Engine",
                Class::Features(UWMap::Update(
                    "horsepower".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Driver",
                Class::Features(UWMap::Update(
                    "name".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Car",
                Class::Features(UWMap::Update(
                    "wheels".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Car",
                Class::Features(UWMap::Update(
                    "engine".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Driver",
                Class::Features(UWMap::Update(
                    "age".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::Number)),
                )),
            ),
            UWGraph::UpdateVertex(
                "Driver",
                Class::Features(UWMap::Update(
                    "license".to_string(),
                    Feature::Typ(MVRegister::Write(PrimitiveType::String)),
                )),
            ),
            UWGraph::RemoveVertex("Wheel"),
            UWGraph::RemoveVertex("Engine"),
            UWGraph::RemoveVertex("Driver"),
            UWGraph::UpdateArc(
                "Car",
                "Wheel",
                "arc1",
                Relation::Label(MVRegister::Write("has".to_string())),
            ),
            UWGraph::UpdateArc(
                "Car",
                "Wheel",
                "arc1",
                Relation::Label(MVRegister::Write("wheelcar".to_string())),
            ),
            UWGraph::UpdateArc(
                "Wheel",
                "Car",
                "arc2",
                Relation::RelationType(TORegister::Write(RelationType::Composes)),
            ),
            UWGraph::UpdateArc(
                "Car",
                "Wheel",
                "arc1",
                Relation::RelationType(TORegister::Write(RelationType::Extends)),
            ),
            UWGraph::UpdateArc(
                "Wheel",
                "Car",
                "arc2",
                Relation::RelationType(TORegister::Write(RelationType::Implements)),
            ),
            UWGraph::UpdateArc(
                "Car",
                "Engine",
                "arc3",
                Relation::Label(MVRegister::Write("has".to_string())),
            ),
            UWGraph::UpdateArc(
                "Driver",
                "Car",
                "arc4",
                Relation::Label(MVRegister::Write("drives".to_string())),
            ),
            UWGraph::UpdateArc(
                "Car",
                "Driver",
                "arc5",
                Relation::Label(MVRegister::Write("owned_by".to_string())),
            ),
        ];

        let config = EventGraphConfig {
            n_replicas: 8,
            total_operations: 100,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.2,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<ClassDiagramCrdt>(config);

        // All replicas' eval() should match
        let mut reference_val: ClassDiagram = DiGraph::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            let new_eval = tcsb.eval();
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert!(
                petgraph::algo::is_isomorphic(&new_eval, &reference_val),
                "Replica {} did not converge with the reference. Reference: {:?}, Replica: {:?}",
                tcsb.id,
                petgraph::dot::Dot::with_config(&new_eval, &[]),
                petgraph::dot::Dot::with_config(&reference_val, &[]),
            );
            println!(
                "Replica {}: {}",
                tcsb.id,
                export_fancy_class_diagram(&new_eval)
            );
        }
    }

    #[test_log::test]
    fn generate_graph_convergence() {
        let ops: Vec<Graph<&'static str, u8>> = vec![
            Graph::AddVertex("a"),
            Graph::AddVertex("b"),
            Graph::AddVertex("c"),
            Graph::RemoveVertex("a"),
            Graph::RemoveVertex("b"),
            Graph::RemoveVertex("c"),
            Graph::AddArc("a", "b", 1),
            Graph::AddArc("a", "b", 2),
            Graph::AddArc("b", "c", 1),
            Graph::AddArc("b", "c", 2),
            Graph::AddArc("c", "a", 1),
            Graph::AddArc("c", "a", 2),
            Graph::RemoveArc("a", "b", 1),
            Graph::RemoveArc("a", "b", 2),
            Graph::RemoveArc("b", "c", 1),
            Graph::RemoveArc("b", "c", 2),
            Graph::RemoveArc("c", "a", 1),
            Graph::RemoveArc("c", "a", 2),
        ];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 12,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.6,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<EventGraph<Graph<&str, u8>>>(config);

        // All replicas' eval() should match
        let mut reference_val: DiGraph<&str, u8> = DiGraph::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            let new_eval = tcsb.eval();
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert!(
                petgraph::algo::is_isomorphic(&new_eval, &reference_val),
                "Replica {} did not converge with the reference. Reference: {}, Replica: {}",
                tcsb.id,
                petgraph::dot::Dot::with_config(&new_eval, &[]),
                petgraph::dot::Dot::with_config(&reference_val, &[]),
            );
        }
    }

    #[test_log::test]
    fn generate_uw_multigraph_convergence() {
        let ops = vec![
            UWGraph::UpdateVertex("a", LWWRegister::Write("vertex_a")),
            UWGraph::UpdateVertex("b", LWWRegister::Write("vertex_b")),
            UWGraph::UpdateVertex("c", LWWRegister::Write("vertex_c")),
            UWGraph::UpdateVertex("d", LWWRegister::Write("vertex_d")),
            UWGraph::UpdateVertex("e", LWWRegister::Write("vertex_e")),
            UWGraph::RemoveVertex("a"),
            UWGraph::RemoveVertex("b"),
            UWGraph::RemoveVertex("c"),
            UWGraph::RemoveVertex("d"),
            UWGraph::RemoveVertex("e"),
            UWGraph::UpdateArc("a", "b", 1, Counter::Inc(1)),
            UWGraph::UpdateArc("a", "a", 1, Counter::Inc(13)),
            UWGraph::UpdateArc("a", "a", 1, Counter::Dec(3)),
            UWGraph::UpdateArc("a", "b", 2, Counter::Dec(2)),
            UWGraph::UpdateArc("a", "b", 2, Counter::Inc(7)),
            UWGraph::UpdateArc("b", "c", 1, Counter::Dec(5)),
            UWGraph::UpdateArc("c", "d", 1, Counter::Inc(3)),
            UWGraph::UpdateArc("d", "e", 1, Counter::Dec(2)),
            UWGraph::UpdateArc("e", "a", 1, Counter::Inc(4)),
            UWGraph::RemoveArc("a", "b", 1),
            UWGraph::RemoveArc("a", "b", 2),
            UWGraph::RemoveArc("b", "c", 1),
            UWGraph::RemoveArc("c", "d", 1),
            UWGraph::RemoveArc("d", "e", 1),
            UWGraph::RemoveArc("e", "a", 1),
        ];

        let config = EventGraphConfig {
            n_replicas: 8,
            total_operations: 100,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.2,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<
            UWGraphLog<&str, u8, EventGraph<LWWRegister<&str>>, EventGraph<Counter<i32>>>,
        >(config);

        // All replicas' eval() should match
        let mut reference_val: DiGraph<&str, i32> = DiGraph::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            let new_eval = tcsb.eval();
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert!(
                petgraph::algo::is_isomorphic(&new_eval, &reference_val),
                "Replica {} did not converge with the reference. Reference: {}, Replica: {}",
                tcsb.id,
                petgraph::dot::Dot::with_config(&new_eval, &[]),
                petgraph::dot::Dot::with_config(&reference_val, &[]),
            );
            println!(
                "Replica {}: {}",
                tcsb.id,
                petgraph::dot::Dot::with_config(&new_eval, &[])
            );
        }
    }

    #[test_log::test]
    fn generate_deeply_nested_uw_map_convergence() {
        let ops = vec![
            UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Inc(2))),
            UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Dec(3))),
            UWMap::Update("a".to_string(), UWMap::Update(1, Counter::Reset)),
            UWMap::Update("a".to_string(), UWMap::Remove(1)),
            UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Inc(5))),
            UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Dec(1))),
            UWMap::Update("b".to_string(), UWMap::Update(2, Counter::Reset)),
            UWMap::Update("b".to_string(), UWMap::Remove(2)),
            UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Inc(10))),
            UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Dec(2))),
            UWMap::Update("c".to_string(), UWMap::Update(3, Counter::Reset)),
            UWMap::Update("c".to_string(), UWMap::Remove(3)),
            UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Inc(7))),
            UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Dec(4))),
            UWMap::Update("d".to_string(), UWMap::Update(4, Counter::Reset)),
            UWMap::Update("d".to_string(), UWMap::Remove(4)),
            UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Inc(3))),
            UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Dec(1))),
            UWMap::Update("e".to_string(), UWMap::Update(5, Counter::Reset)),
            UWMap::Update("e".to_string(), UWMap::Remove(5)),
            UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Inc(2))),
            UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Dec(2))),
            UWMap::Update("a".to_string(), UWMap::Update(6, Counter::Reset)),
            UWMap::Update("a".to_string(), UWMap::Remove(6)),
        ];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 40,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<UWMapLog<String, UWMapLog<i32, EventGraph<Counter<i32>>>>>(
            config,
        );

        // All replicas' eval() should match
        let mut reference_val: HashMap<String, HashMap<i32, i32>> = HashMap::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert_eq!(
                tcsb.eval(),
                reference_val,
                "Replica {} did not converge with the reference.",
                i,
            );
        }
    }

    #[test_log::test]
    fn generate_lww_register_convergence() {
        let ops = vec![
            LWWRegister::Write("w".to_string()),
            LWWRegister::Write("x".to_string()),
            LWWRegister::Write("y".to_string()),
            LWWRegister::Write("z".to_string()),
            LWWRegister::Write("u".to_string()),
            LWWRegister::Write("v".to_string()),
        ];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 50,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.2,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<EventGraph<LWWRegister<String>>>(config);

        // All replicas' eval() should match
        let mut reference_val: String = String::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference_val = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            assert_eq!(tcsb.my_clock().sum(), event_sum);
            assert_ne!(tcsb.eval(), String::default());
            assert_eq!(
                tcsb.eval(),
                reference_val,
                "Replica '{}' did not converge with the reference '{}'. Ref val: '{}'",
                tcsb.id,
                tcsbs[0].id,
                reference_val
            );
        }
    }
}
