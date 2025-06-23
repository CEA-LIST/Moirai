use crate::{
    crdt::test_util::n_members,
    protocol::{log::Log, pulling::Since, tcsb::Tcsb},
};
use rand::{
    seq::{IndexedRandom, IteratorRandom},
    Rng,
};
use std::{fmt::Debug, fs::File, io::Write, time::Instant};

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
    use petgraph::graph::DiGraph;

    use super::*;
    use std::collections::{HashMap, HashSet};

    use crate::crdt::aw_map::{AWMap, AWMapLog};
    use crate::crdt::aw_multigraph::AWGraph;
    use crate::crdt::aw_set::AWSet;
    use crate::crdt::lww_register::LWWRegister;
    use crate::crdt::resettable_counter::Counter;
    use crate::crdt::uw_multigraph::{UWGraph, UWGraphLog};
    use crate::protocol::event_graph::EventGraph;

    #[test_log::test]
    fn folie() {
        for _ in 0..100 {
            generate_uw_multigraph_convergence();
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
            n_replicas: 32,
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
        let ops = vec![Counter::Inc(1), Counter::Dec(1), Counter::Reset];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 100,
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
    fn generate_aw_map_convergence() {
        let ops = vec![
            AWMap::Update("a".to_string(), Counter::Inc(2)),
            AWMap::Update("a".to_string(), Counter::Dec(3)),
            AWMap::Update("a".to_string(), Counter::Reset),
            AWMap::Remove("a".to_string()),
            AWMap::Update("b".to_string(), Counter::Inc(5)),
            AWMap::Update("b".to_string(), Counter::Dec(1)),
            AWMap::Update("b".to_string(), Counter::Reset),
            AWMap::Remove("b".to_string()),
            AWMap::Update("c".to_string(), Counter::Inc(10)),
            AWMap::Update("c".to_string(), Counter::Dec(2)),
            AWMap::Update("c".to_string(), Counter::Reset),
            AWMap::Remove("c".to_string()),
        ];

        let config = EventGraphConfig {
            n_replicas: 5,
            total_operations: 4,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            log_timing_csv: false,
        };

        let tcsbs = generate_event_graph::<AWMapLog<String, EventGraph<Counter<i32>>>>(config);

        // All replicas' eval() should match
        let mut reference_val: HashMap<String, i32> = HashMap::new();
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
        }
    }

    #[test_log::test]
    fn generate_aw_graph_convergence() {
        let ops = vec![
            AWGraph::AddVertex("a"),
            AWGraph::AddVertex("b"),
            AWGraph::AddVertex("c"),
            AWGraph::RemoveVertex("a"),
            AWGraph::RemoveVertex("b"),
            AWGraph::RemoveVertex("c"),
            AWGraph::AddArc("a", "b", 1),
            AWGraph::AddArc("a", "b", 2),
            AWGraph::AddArc("b", "c", 1),
            AWGraph::AddArc("b", "c", 2),
            AWGraph::AddArc("c", "a", 1),
            AWGraph::AddArc("c", "a", 2),
            AWGraph::RemoveArc("a", "b", 1),
            AWGraph::RemoveArc("a", "b", 2),
            AWGraph::RemoveArc("b", "c", 1),
            AWGraph::RemoveArc("b", "c", 2),
            AWGraph::RemoveArc("c", "a", 1),
            AWGraph::RemoveArc("c", "a", 2),
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

        let tcsbs = generate_event_graph::<EventGraph<AWGraph<&str, u8>>>(config);

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
    fn generate_deeply_nested_aw_map_convergence() {
        let ops = vec![
            AWMap::Update("a".to_string(), AWMap::Update(1, Counter::Inc(2))),
            AWMap::Update("a".to_string(), AWMap::Update(1, Counter::Dec(3))),
            AWMap::Update("a".to_string(), AWMap::Update(1, Counter::Reset)),
            AWMap::Update("a".to_string(), AWMap::Remove(1)),
            AWMap::Update("b".to_string(), AWMap::Update(2, Counter::Inc(5))),
            AWMap::Update("b".to_string(), AWMap::Update(2, Counter::Dec(1))),
            AWMap::Update("b".to_string(), AWMap::Update(2, Counter::Reset)),
            AWMap::Update("b".to_string(), AWMap::Remove(2)),
            AWMap::Update("c".to_string(), AWMap::Update(3, Counter::Inc(10))),
            AWMap::Update("c".to_string(), AWMap::Update(3, Counter::Dec(2))),
            AWMap::Update("c".to_string(), AWMap::Update(3, Counter::Reset)),
            AWMap::Update("c".to_string(), AWMap::Remove(3)),
            AWMap::Update("d".to_string(), AWMap::Update(4, Counter::Inc(7))),
            AWMap::Update("d".to_string(), AWMap::Update(4, Counter::Dec(4))),
            AWMap::Update("d".to_string(), AWMap::Update(4, Counter::Reset)),
            AWMap::Update("d".to_string(), AWMap::Remove(4)),
            AWMap::Update("e".to_string(), AWMap::Update(5, Counter::Inc(3))),
            AWMap::Update("e".to_string(), AWMap::Update(5, Counter::Dec(1))),
            AWMap::Update("e".to_string(), AWMap::Update(5, Counter::Reset)),
            AWMap::Update("e".to_string(), AWMap::Remove(5)),
            AWMap::Update("a".to_string(), AWMap::Update(6, Counter::Inc(2))),
            AWMap::Update("a".to_string(), AWMap::Update(6, Counter::Dec(2))),
            AWMap::Update("a".to_string(), AWMap::Update(6, Counter::Reset)),
            AWMap::Update("a".to_string(), AWMap::Remove(6)),
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

        let tcsbs = generate_event_graph::<AWMapLog<String, AWMapLog<i32, EventGraph<Counter<i32>>>>>(
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
