use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    crdt::test_util::bootstrap_n,
    fuzz::config::{FuzzerConfig, OpConfig, RunConfig},
    protocol::{
        broadcast::tcsb::Tcsb, membership::ReplicaIdx, replica::IsReplica, state::log::IsLog,
    },
};

pub mod config;
pub mod convergence_checker;

pub fn fuzzer<L: IsLog>(config: FuzzerConfig<L>) {
    for run_config in config.runs {
        runner::<L>(
            run_config,
            &config.operations,
            config.final_merge,
            config.compare,
        );
    }
}

pub fn runner<L: IsLog>(
    config: RunConfig,
    operations: &OpConfig<L::Op>,
    final_merge: bool,
    compare: fn(&L::Value, &L::Value) -> bool,
) {
    let mut rng = if let Some(seed) = config.seed {
        ChaCha8Rng::from_seed(seed)
    } else {
        ChaCha8Rng::from_os_rng()
    };
    let mut replicas = bootstrap_n::<L, Tcsb<L::Op>>(config.num_replicas);
    let reachability = config.reachability.unwrap_or_else(|| {
        vec![vec![true; config.num_replicas.into()]; config.num_replicas.into()]
    });
    let mut online = vec![true; config.num_replicas.into()];
    let mut count_ops = 0;

    let mut time_to_deliver: HashMap<ReplicaIdx, Duration> = HashMap::new();
    let mut _time_to_eval: HashMap<ReplicaIdx, Duration> = HashMap::new();

    // Main loop
    while count_ops < config.num_operations {
        // Randomly select a replica
        let replica_idx = (0..config.num_replicas).choose(&mut rng).unwrap() as usize;
        // If the replica is online, deliver any pending events from other replicas
        if online[replica_idx] {
            for other_idx in 0..config.num_replicas.into() {
                if other_idx != replica_idx
                    && online[other_idx]
                    && reachability[replica_idx][other_idx]
                {
                    let since = replicas[replica_idx].since();
                    let batch = replicas[other_idx].pull(since);
                    let start = Instant::now();
                    replicas[replica_idx].receive_batch(batch);
                    let duration = start.elapsed();
                    time_to_deliver
                        .entry(replica_idx)
                        .and_modify(|d| *d += duration)
                        .or_insert(duration);
                }
            }
        }

        // Choose an operation
        let op = operations.choose(&mut rng);
        count_ops += 1;

        // Send the operation
        let start = Instant::now();
        let event = replicas[replica_idx].send(op.clone());
        let duration = start.elapsed();
        time_to_deliver
            .entry(replica_idx)
            .and_modify(|d| *d += duration)
            .or_insert(duration);

        if online[replica_idx] {
            for other_idx in 0..config.num_replicas.into() {
                if other_idx != replica_idx
                    && online[other_idx]
                    && reachability[replica_idx][other_idx]
                {
                    let start = Instant::now();
                    replicas[other_idx].receive(event.clone());
                    let duration = start.elapsed();
                    time_to_deliver
                        .entry(other_idx)
                        .and_modify(|d| *d += duration)
                        .or_insert(duration);
                }
            }
        }

        // Randomly decide whether the replicas go offline or not
        for online_flag in &mut online {
            *online_flag = rng.random_bool(1.0 - config.churn_rate);
        }
    }

    // Final convergence phase
    if final_merge {
        for i in 0..config.num_replicas.into() {
            for j in 0..config.num_replicas.into() {
                if i != j {
                    let since = replicas[i].since();
                    let batch = replicas[j].pull(since);
                    let start = Instant::now();
                    replicas[i].receive_batch(batch);
                    let duration = start.elapsed();
                    time_to_deliver
                        .entry(i)
                        .and_modify(|d| *d += duration)
                        .or_insert(duration);
                }
            }
        }
    }

    // Check convergence
    let first_value = replicas[0].query();
    let num_delivered_events = replicas[0].num_delivered_events();
    for (i, replica) in replicas.iter().enumerate().skip(1) {
        let replica_delivered_events = replica.num_delivered_events();
        if num_delivered_events != replica_delivered_events {
            panic!(
                "Replica {} and {} have delivered a different number of events: {num_delivered_events} vs {replica_delivered_events}",
                replicas[0].id(),
                replica.id()
            );
        }
        let value = replica.query();
        if !compare(&first_value, &value) {
            panic!("Replicas 0 and {i} diverged: {first_value:?} vs {value:?}");
        }
    }

    println!("All replicas converged to the same state: {first_value:?}");
    for (idx, duration) in time_to_deliver.iter() {
        println!(
            "Replica {} total time to deliver: {:?} ms",
            replicas[*idx].id(),
            duration.as_millis()
        );
    }

    println!(
        "Average time to deliver per operation: {} ms",
        time_to_deliver
            .values()
            .map(|d| d.as_millis())
            .sum::<u128>() as f64
            / count_ops as f64
    );
    println!(
        "Average op/sec: {}",
        (count_ops as f64 * 1000.0)
            / time_to_deliver
                .values()
                .map(|d| d.as_millis())
                .sum::<u128>() as f64
    );
}
