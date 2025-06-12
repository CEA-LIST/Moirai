use crate::{
    crdt::test_util::n_members,
    protocol::{log::Log, pulling::Since, tcsb::Tcsb},
};
use rand::{
    seq::{IndexedRandom, IteratorRandom},
    Rng,
};
use std::fmt::Debug;

/// Configuration for random event graph generation in a partially connected distributed system.
/// If `churn_rate` is set to 0, `final_sync` is false and `reachability` is None,
/// the graph will be fully connected and all replicas will be online throughout the simulation.
/// Thus, they will converge to the same state after all operations are applied.
pub struct EventGraphConfig<'a, Op> {
    pub n_replicas: usize,
    pub total_operations: usize,
    pub ops: &'a [Op],
    pub final_sync: bool,
    /// The probability that a replica switch offline/online at each operation.
    pub churn_rate: f64,
    pub reachability: Option<Vec<Vec<bool>>>, // Optional static reachability matrix
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
        }
    }
}

pub fn generate_event_graph<L>(config: EventGraphConfig<'_, L::Op>) -> Vec<Tcsb<L>>
where
    L: Log,
    L::Op: Clone + Debug,
    L::Value: PartialEq + Debug,
{
    let mut rng = rand::rng();
    let mut tcsbs: Vec<Tcsb<L>> = n_members::<L>(config.n_replicas);
    let reachability = config
        .reachability
        .unwrap_or_else(|| vec![vec![true; config.n_replicas]; config.n_replicas]);

    let mut local_ops_issued = 0;
    let mut online = vec![true; config.n_replicas];

    while local_ops_issued < config.total_operations {
        // Churn simulation: determine which replicas are online.
        for online_flag in &mut online {
            *online_flag = rng.random::<f64>() >= config.churn_rate;
        }

        // Broadcast step
        if let Some(replica_idx) = (0..config.n_replicas)
            .filter(|&i| online[i])
            .choose(&mut rng)
        {
            let op = config
                .ops
                .choose(&mut rng)
                .expect("`ops` slice cannot be empty")
                .clone();
            let _ = tcsbs[replica_idx].tc_bcast(op);
            local_ops_issued += 1;

            // Sync step from replica_idx to all others that are online and  reachable
            for j in 0..config.n_replicas {
                if j != replica_idx && online[j] && reachability[replica_idx][j] {
                    let since = Since::new_from(&tcsbs[j]);
                    let batch = tcsbs[replica_idx].events_since(&since);
                    tcsbs[j].deliver_batch(batch);
                }
            }
        }
    }

    // Final all-to-all sync (if enabled)
    if config.final_sync {
        for i in 0..config.n_replicas {
            for j in 0..config.n_replicas {
                if i != j && reachability[i][j] {
                    let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[j]));
                    tcsbs[j].deliver_batch(batch);
                }
            }
        }
    }

    tcsbs
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    use crate::crdt::aw_map::{AWMap, AWMapLog};
    use crate::crdt::aw_set::AWSet;
    use crate::crdt::resettable_counter::Counter;
    use crate::protocol::event_graph::EventGraph;

    #[test_log::test]
    fn folie() {
        for _ in 0..1_000 {
            generate_deeply_nested_aw_map_convergence();
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
            n_replicas: 20,
            total_operations: 30,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.2,
            reachability: None,
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
            total_operations: 40,
            ops: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
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
                i,
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
            // More deeply nested operations for better coverage
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
}
