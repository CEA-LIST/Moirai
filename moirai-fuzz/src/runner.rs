use std::fmt::Debug;

use deepsize::DeepSizeOf;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use moirai_protocol::{
    crdt::{eval::EvalNested, query::QueryOperation},
    replica::IsReplica,
    state::log::IsLog,
};
use rand::{RngExt, SeedableRng, seq::IteratorRandom};
use rand_chacha::ChaCha8Rng;

use crate::{
    config::{OracleDriver, Predicate, RunConfig},
    execution_graph::ExecutionGraph,
    generator::command_generator::CommandGenerator,
    metrics::Phase,
    observers::{footprint::LogFootprint, recorder::ReplicaRecorders},
    utils::{
        boostrap::bootstrap_n,
        format::{clean_dot_output, seed_to_hex},
    },
};

/// Internal structure to hold run results before serialization
#[derive(Debug)]
pub struct RunCapture<O> {
    /// Configuration used for this run
    pub config: RunConfig,
    /// Seed used for this run
    pub seed: [u8; 32],
    /// Final value observed after convergence
    pub final_value: Option<String>,
    pub measurements: Vec<ReplicaRecorders>,
    pub execution_graph: Option<ExecutionGraph<O>>,
}

/// Run the fuzzing test with the given configuration and return the results
pub fn runner<L, Q>(
    config: RunConfig,
    final_merge: bool,
    predicate: &Predicate<Q>,
    oracle_driver: Option<&OracleDriver<L>>,
    display: bool,
) -> RunCapture<L::Op>
where
    Q: QueryOperation,
    <Q as QueryOperation>::Response: Debug,
    L: IsLog + CommandGenerator + EvalNested<Q> + DeepSizeOf + LogFootprint,
    L::Op: DeepSizeOf,
{
    // Capture or generate the seed
    let used_seed = config.seed.unwrap_or_else(|| {
        let rng: ChaCha8Rng = rand::make_rng();
        rng.get_seed()
    });

    info!("🎲 Using seed: {}", seed_to_hex(&used_seed));

    let mut rng = ChaCha8Rng::from_seed(used_seed);

    let mut replicas = bootstrap_n::<L>(
        config.num_replicas,
        &config.metrics,
        !config.disable_stability,
        used_seed,
    );

    let reachability = config.reachability.clone().unwrap_or_else(|| {
        vec![vec![true; config.num_replicas.into()]; config.num_replicas.into()]
    });
    // `online[i]` indicates whether replica i is online.
    let mut online = vec![true; config.num_replicas.into()];
    let mut count_ops = 0;

    // Create execution graph if requested
    let mut execution_graph: Option<ExecutionGraph<L::Op>> = if config.execution_graph {
        Some(ExecutionGraph::new())
    } else {
        None
    };

    // Create a progress bar with indicatif
    let pb = ProgressBar::new(config.num_operations as u64);
    if !display {
        pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }
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
        // Select a replica unformly at random
        let replica_idx = (0..config.num_replicas).choose(&mut rng).unwrap() as usize;
        // Advance the selected replica's online/offline Markov state.
        online[replica_idx] = if online[replica_idx] {
            !rng.random_bool(config.churn.p_disconnect.val())
        } else {
            rng.random_bool(config.churn.p_reconnect.val())
        };

        // If the replica is online, deliver any pending events from other online replicas
        if online[replica_idx] {
            for other_idx in 0..config.num_replicas.into() {
                if other_idx != replica_idx
                    && online[other_idx]
                    && reachability[replica_idx][other_idx]
                {
                    let since = replicas[replica_idx].since();
                    let batch = replicas[other_idx].pull(since);
                    replicas[replica_idx].receive_batch(batch);
                }
            }
        }

        // If a CommitLog is used
        if let Some(driver) = oracle_driver {
            let leader_idx = select_omega_leader(replica_idx, &online, &reachability);
            let leader = replicas[leader_idx].id().to_owned();
            driver.set_leader(replicas[replica_idx].state_mut(), &leader);
        }

        // Generate a client command. `send` then prepares the replicated payload, including any
        // protocol metadata configured by the oracle driver above.
        let command = replicas[replica_idx].state().generate_command(&mut rng);
        count_ops += 1;

        // Update progress bar
        pb.inc(1);

        //* SEND
        let msg = replicas[replica_idx].send(command).unwrap();

        // Add event to execution graph if enabled
        if let Some(ref mut graph) = execution_graph {
            let event = msg.event().clone();
            graph.append(&event);
        }

        if online[replica_idx] {
            for other_idx in 0..config.num_replicas.into() {
                if other_idx != replica_idx
                    && online[other_idx]
                    && reachability[replica_idx][other_idx]
                {
                    //* RECEIVE
                    replicas[other_idx].receive(msg.clone());
                }
            }
        }
    }

    pb.finish_with_message("All operations completed ✓");

    let mut final_value = None;

    // Final convergence phase
    if final_merge {
        for replica in &mut replicas {
            replica.set_phase(Phase::Quiescence);
        }
        let total_merges = (config.num_replicas as usize) * (config.num_replicas as usize - 1);
        let merge_pb = ProgressBar::new(total_merges as u64);
        if !display {
            merge_pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        }
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
                    //* RECEIVE BATCH
                    replicas[i].receive_batch(msg);

                    merge_pb.inc(1);
                }
            }
        }
        let event_count = replicas[0].delivered_count();
        assert_eq!(
            event_count, config.num_operations,
            "Replica 0 has a different number of events after final merge: {} vs {}",
            event_count, config.num_operations
        );
        for replica in &replicas {
            let replica_event_count = replica.delivered_count();
            if replica_event_count != event_count {
                panic!(
                    "Replica {} has a different number of events after final merge: {} vs {}",
                    replica.id(),
                    replica_event_count,
                    event_count
                );
            }
        }
        merge_pb.finish_with_message("Convergence completed ✓");

        // Check convergence
        for replica in &mut replicas {
            replica.set_phase(Phase::Convergence);
        }
        let check_pb = ProgressBar::new((config.num_replicas as usize - 1) as u64);
        if !display {
            check_pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        }
        check_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {pos}/{len} replicas ({percent}%) {msg}"
            )
            .unwrap()
            .progress_chars("█▓▒░ ")
    );
        check_pb.set_message("Checking convergence...");

        let first_value = replicas[0].query(&predicate.query);

        let num_delivered_events = replicas[0].delivered_count();
        let first_replica_id = replicas[0].id().clone();

        for (idx, r) in replicas.iter_mut().enumerate().skip(1) {
            let replica_delivered_events = r.delivered_count();
            if num_delivered_events != replica_delivered_events {
                check_pb.finish_and_clear();
                panic!(
                    "Replica {} and {} have delivered a different number of events: {num_delivered_events} vs {replica_delivered_events}",
                    first_replica_id,
                    r.id()
                );
            }
            let value = r.query(&predicate.query);
            if !(predicate.cmp)(&first_value, &value) {
                check_pb.finish_and_clear();
                if let Some(ref mut graph) = execution_graph {
                    graph.reduce(display);
                    warn!(
                        "Execution graph at divergence:\n{}",
                        clean_dot_output(&graph.to_dot())
                    );
                }
                panic!("Replicas 0 and {idx} diverged: {first_value:?} vs {value:?}");
            }
            check_pb.inc(1);
        }

        final_value = Some(format!("{:?}", first_value));

        check_pb.finish_with_message("Convergence verified ✓");
    }
    debug!("Run completed");

    for replica in &mut replicas {
        replica.record_terminal_sizes();
    }

    RunCapture {
        config,
        seed: used_seed,
        final_value,
        measurements: replicas
            .into_iter()
            .map(|replica| replica.measurements())
            .collect(),
        execution_graph,
    }
}

/// Select the omega leader for the given replica index, based on the online status and reachability of other replicas.
///
/// If the selected replica is offline, it will return itself as the leader. Otherwise,
/// it wille return the lowest-indexed online replica that is reachable from the selected replica.
fn select_omega_leader(selected: usize, online: &[bool], reachability: &[Vec<bool>]) -> usize {
    if !online[selected] {
        return selected;
    }

    (0..online.len())
        .filter(|&candidate| online[candidate] && reachability[selected][candidate])
        .min_by(|&left, &right| left.cmp(&right))
        .unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn omega_leader_selection_everyone_online() {
        let online = vec![true, true, true, true];
        let reachability = vec![
            vec![true, true, true, true],
            vec![true, true, true, true],
            vec![true, true, true, true],
            vec![true, true, true, true],
        ];

        let leaders = (0..online.len())
            .map(|selected| super::select_omega_leader(selected, &online, &reachability))
            .collect::<Vec<usize>>();

        assert_eq!(leaders, vec![0, 0, 0, 0]);
    }

    #[test]
    fn omega_leader_selection_one_offline() {
        let online = vec![true, true, false, true];
        let reachability = vec![
            vec![true, true, true, true],
            vec![true, true, true, true],
            vec![true, true, true, true],
            vec![true, true, true, true],
        ];

        let leaders = (0..online.len())
            .map(|selected| super::select_omega_leader(selected, &online, &reachability))
            .collect::<Vec<usize>>();

        assert_eq!(leaders, vec![0, 0, 2, 0]);
    }
}
