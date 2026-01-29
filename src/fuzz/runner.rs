// TODO: add information about the max number of events between two stabilizations
// TODO: add information about the shape of the execution graph (height, width, etc.)

use log::{debug, info, warn};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};
use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    crdt::test_util::bootstrap_n,
    fuzz::{
        config::{OpGeneratorNested, RunConfig},
        metrics::{set_disable_stability, MetricsLog},
        utils::{clean_dot_output, format_string, seed_to_hex},
    },
    protocol::{
        broadcast::tcsb::{IsTcsbFuzz, Tcsb},
        crdt::{eval::EvalNested, query::Read},
        replica::{IsReplica, ReplicaIdx},
        state::{event_graph::EventGraph, log::IsLog, unstable_state::IsUnstableState},
    },
    HashMap,
};

/// Internal structure to hold run results before serialization
pub struct RunData {
    /// Configuration used for this run
    pub config: RunConfig,
    /// Seed used for this run
    pub used_seed: [u8; 32],
    /// Final value observed after convergence
    pub first_value: String,
    /// Total time taken to deliver all ops, per replica
    pub total_time_to_deliver_per_replica: HashMap<ReplicaIdx, Duration>,
    /// Total time spent in effect() per replica
    pub total_time_in_effect_per_replica: HashMap<ReplicaIdx, Duration>,
    /// Execution graph in DOT format (if generated)
    pub execution_graph_dot: Option<String>,
}

pub fn runner<L>(
    config: RunConfig,
    final_merge: bool,
    compare: fn(&L::Value, &L::Value) -> bool,
) -> RunData
where
    L: IsLog + OpGeneratorNested + EvalNested<Read<<L as IsLog>::Value>>,
{
    // Capture or generate the seed
    let used_seed = config.seed.unwrap_or_else(|| {
        let mut rng = ChaCha8Rng::from_os_rng();
        let mut seed = [0u8; 32];
        rng.fill(&mut seed);
        seed
    });

    info!("🎲 Using seed: {}", seed_to_hex(&used_seed));

    // Set disable_stability flag based on run configuration
    set_disable_stability(config.disable_stability);

    let mut rng = ChaCha8Rng::from_seed(used_seed);

    let mut replicas = bootstrap_n::<MetricsLog<L>, Tcsb<L::Op>>(config.num_replicas);
    let reachability = config.reachability.clone().unwrap_or_else(|| {
        vec![vec![true; config.num_replicas.into()]; config.num_replicas.into()]
    });
    // `online[i]` indicates whether replica i is online.
    let mut online = vec![true; config.num_replicas.into()];
    let mut count_ops = 0;
    let mut total_time_to_deliver_per_replica: HashMap<ReplicaIdx, Duration> = HashMap::default();

    // Create execution graph if requested
    let mut execution_graph: Option<EventGraph<L::Op>> = if config.generate_execution_graph {
        Some(EventGraph::default())
    } else {
        None
    };

    // Create a progress bar with indicatif
    let pb = ProgressBar::new(config.num_operations as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ops ({percent}%) ETA: {eta_precise}"
            )
            .unwrap()
            .progress_chars("█▓▒░ ")
    );
    pb.set_message("Fuzzing in progress...");

    // Main loop
    while count_ops < config.num_operations {
        // Randomly select a replica
        let replica_idx = (0..config.num_replicas).choose(&mut rng).unwrap() as usize;
        // Determine if the replica is online based on churn rate
        online[replica_idx] = !rng.random_bool(config.churn_rate);

        // If the replica is online, deliver any pending events from other online replicas
        if online[replica_idx] {
            for other_idx in 0..config.num_replicas.into() {
                if other_idx != replica_idx
                    && online[other_idx]
                    && reachability[replica_idx][other_idx]
                {
                    let since = replicas[replica_idx].since();
                    let batch = replicas[other_idx].pull(since);
                    timed(
                        ReplicaIdx(replica_idx),
                        &mut total_time_to_deliver_per_replica,
                        || replicas[replica_idx].receive_batch(batch),
                    );
                }
            }
        }

        // Send the operation
        let op = replicas[replica_idx].state().generate(&mut rng);
        count_ops += 1;

        // Update progress bar
        pb.inc(1);

        let msg = timed(
            ReplicaIdx(replica_idx),
            &mut total_time_to_deliver_per_replica,
            || replicas[replica_idx].send(op.clone()).unwrap(),
        );

        // Add event to execution graph if enabled
        if let Some(ref mut graph) = execution_graph {
            let event = msg.event().clone();
            graph.append(event);
        }

        if online[replica_idx] {
            for other_idx in 0..config.num_replicas.into() {
                if other_idx != replica_idx
                    && online[other_idx]
                    && reachability[replica_idx][other_idx]
                {
                    timed(
                        ReplicaIdx(other_idx),
                        &mut total_time_to_deliver_per_replica,
                        || replicas[other_idx].receive(msg.clone()),
                    );
                }
            }
        }
    }

    pb.finish_with_message("All operations completed ✓");

    // Final convergence phase
    if final_merge {
        let total_merges = (config.num_replicas as usize) * (config.num_replicas as usize - 1);
        let merge_pb = ProgressBar::new(total_merges as u64);
        merge_pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} merges ({percent}%) {msg} ETA: {eta_precise}"
                )
                .unwrap()
                .progress_chars("█▓▒░ ")
        );
        merge_pb.set_message("Final convergence...");

        for i in 0..config.num_replicas.into() {
            for j in 0..config.num_replicas.into() {
                if i != j {
                    let since = replicas[i].since();
                    let msg = replicas[j].pull(since);

                    timed(
                        ReplicaIdx(i),
                        &mut total_time_to_deliver_per_replica,
                        || replicas[i].receive_batch(msg),
                    );

                    merge_pb.inc(1);
                }
            }
        }
        merge_pb.finish_with_message("Convergence completed ✓");
    }

    // Check convergence
    let check_pb = ProgressBar::new((config.num_replicas as usize - 1) as u64);
    check_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {pos}/{len} replicas ({percent}%) {msg}"
            )
            .unwrap()
            .progress_chars("█▓▒░ ")
    );
    check_pb.set_message("Checking convergence...");

    let first_value = replicas[0].query(Read::new());
    let val = format_string(&first_value);
    let num_delivered_events = replicas[0].num_delivered_events();

    for (idx, r) in replicas.iter().enumerate().skip(1) {
        let replica_delivered_events = r.num_delivered_events();
        if num_delivered_events != replica_delivered_events {
            check_pb.finish_and_clear();
            panic!(
                "Replica {} and {} have delivered a different number of events: {num_delivered_events} vs {replica_delivered_events}",
                replicas[0].id(),
                r.id()
            );
        }
        let value = r.query(Read::new());
        if !compare(&first_value, &value) {
            check_pb.finish_and_clear();
            if let Some(ref graph) = execution_graph {
                warn!(
                    "Execution graph at divergence:\n{}",
                    clean_dot_output(&graph.to_dot())
                );
            }
            panic!("Replicas 0 and {idx} diverged: {val} vs {value:?}");
        }
        check_pb.inc(1);
    }

    check_pb.finish_with_message("Convergence verified ✓");
    debug!("Run completed");

    let total_time_in_effect_per_replica: HashMap<ReplicaIdx, Duration> = replicas
        .iter()
        .enumerate()
        .map(|(idx, replica)| (ReplicaIdx(idx), replica.state().total_effect_time))
        .collect();

    // Generate DOT format for the execution graph if it was created
    let mut execution_graph_dot = None;

    if let Some(graph) = execution_graph {
        let output = graph.to_dot();
        execution_graph_dot = Some(clean_dot_output(&output));
    }

    for replica in &replicas {
        debug!(
            "Replica {}: time_matrix_clock_full: {} ms",
            replica.id(),
            replica.tcsb().time_matrix_clock_full().as_millis()
        );
        debug!(
            "Replica {}: time_matrix_clock_incremental: {} ms",
            replica.id(),
            replica.tcsb().time_matrix_clock_incremental().as_millis()
        );
    }

    // Return the run data
    RunData {
        config,
        used_seed,
        first_value: format_string(&first_value),
        total_time_to_deliver_per_replica,
        total_time_in_effect_per_replica,
        execution_graph_dot,
    }
}

fn timed<F, R>(replica_idx: ReplicaIdx, recorder: &mut HashMap<ReplicaIdx, Duration>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed();
    recorder
        .entry(replica_idx)
        .and_modify(|d| *d += elapsed)
        .or_insert(elapsed);
    result
}
