use std::time::{Duration, Instant};

use comfy_table::{
    presets::UTF8_FULL, Attribute, Cell, CellAlignment, Color, ContentArrangement, Table,
};
use indicatif::{ProgressBar, ProgressStyle};
use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    crdt::test_util::bootstrap_n,
    fuzz::{
        config::{FuzzerConfig, OpConfig, RunConfig},
        utils::{format_number, format_string},
    },
    protocol::{
        broadcast::tcsb::Tcsb,
        crdt::{eval::EvalNested, query::Read},
        membership::ReplicaIdx,
        replica::IsReplica,
        state::log::IsLog,
    },
    HashMap,
};

// TODO: add information about the max number of events between two stabilizations

pub mod config;
pub mod convergence_checker;
mod utils;

pub fn fuzzer<L>(config: FuzzerConfig<L>)
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    for run_config in config.runs {
        runner::<L>(
            run_config,
            &config.operations,
            config.final_merge,
            config.compare,
        );
    }
}

pub fn runner<L>(
    config: RunConfig,
    operations: &OpConfig<L::Op>,
    final_merge: bool,
    compare: fn(&L::Value, &L::Value) -> bool,
) where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    // Afficher les informations de configuration avec comfy-table
    let mut config_table = Table::new();
    config_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Fuzzer Configuration")
            .add_attribute(Attribute::Bold)
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Center)]);

    config_table.add_row(vec![
        Cell::new("Parameter").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    config_table.add_row(vec!["Replicas", &format!("{}", config.num_replicas)]);

    config_table.add_row(vec![
        "Operations",
        &format_number(config.num_operations as f64),
    ]);

    config_table.add_row(vec![
        "Churn rate",
        &format!("{:.1}%", config.churn_rate * 100.0),
    ]);

    config_table.add_row(vec!["Final merge", if final_merge { "Yes" } else { "No" }]);

    if let Some(seed) = config.seed {
        config_table.add_row(vec![
            "Seed",
            &format!(
                "0x{:016X}",
                u64::from_le_bytes(seed[0..8].try_into().unwrap())
            ),
        ]);
    } else {
        config_table.add_row(vec!["Seed", "Random"]);
    }

    println!("{config_table}");
    println!();

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

    let mut time_to_deliver: HashMap<ReplicaIdx, Duration> = HashMap::default();
    let mut _time_to_eval: HashMap<ReplicaIdx, Duration> = HashMap::default();

    // Créer une barre de progression avec indicatif
    let pb = ProgressBar::new(config.num_operations as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ops ({percent}%) {msg} ETA: {eta_precise}"
            )
            .unwrap()
            .progress_chars("█▓▒░ ")
    );
    pb.set_message("Fuzzing in progress...");

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

        // Send the operation
        let op = operations.choose(&mut rng);
        count_ops += 1;

        // Mettre à jour la barre de progression
        pb.inc(1);
        if count_ops % 100 == 0 {
            // Mise à jour du message toutes les 100 opérations
            let avg_time_ms = if !time_to_deliver.is_empty() {
                time_to_deliver
                    .values()
                    .map(|d| d.as_millis())
                    .sum::<u128>() as f64
                    / count_ops as f64
            } else {
                0.0
            };
            pb.set_message(format!("Avg: {avg_time_ms:.3}ms/op"));
        }

        let start = Instant::now();
        let msg = replicas[replica_idx].send(op.clone()).unwrap();
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
                    replicas[other_idx].receive(msg.clone());
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

    // Finaliser la barre de progression principale
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
                    let batch = replicas[j].pull(since);
                    let start = Instant::now();
                    replicas[i].receive_batch(batch);
                    let duration = start.elapsed();
                    time_to_deliver
                        .entry(i)
                        .and_modify(|d| *d += duration)
                        .or_insert(duration);
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
            panic!("Replicas 0 and {idx} diverged: {val} vs {value:?}");
        }
        check_pb.inc(1);
    }

    check_pb.finish_with_message("Convergence verified ✓");
    println!();

    // Calculer les statistiques
    let total_time_ms = time_to_deliver
        .values()
        .map(|d| d.as_millis())
        .sum::<u128>();
    let avg_time_per_op = total_time_ms as f64 / count_ops as f64;
    let ops_per_sec = (count_ops as f64 * 1000.0) / total_time_ms as f64;

    // Table des performances par réplica
    let mut replica_table = Table::new();
    replica_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Replica Performance")
            .add_attribute(Attribute::Bold)
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center)]);

    replica_table.add_row(vec![
        Cell::new("Replica").add_attribute(Attribute::Bold),
        Cell::new("Total Time (ms)").add_attribute(Attribute::Bold),
        Cell::new("Percentage").add_attribute(Attribute::Bold),
    ]);

    for (idx, duration) in time_to_deliver.iter() {
        let percentage = (duration.as_millis() as f64 / total_time_ms as f64) * 100.0;
        replica_table.add_row(vec![
            &format!("Replica {idx}"),
            &format_number(duration.as_millis() as f64),
            &format!("{percentage:.1}%"),
        ]);
    }

    println!("{replica_table}");
    println!();

    // Table des résultats principaux
    let mut results_table = Table::new();
    results_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Fuzzing Results")
            .add_attribute(Attribute::Bold)
            .fg(Color::Green)
            .set_alignment(CellAlignment::Center)]);

    results_table.add_row(vec![
        Cell::new("Metric").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    results_table.add_row(vec!["Convergence", "✓ Success"]);

    results_table.add_row(vec!["Final state", &format_string(&first_value)]);

    results_table.add_row(vec![
        "Delivered events",
        &format_number(num_delivered_events as f64),
    ]);

    results_table.add_row(vec!["Total operations", &format_number(count_ops as f64)]);

    results_table.add_row(vec![
        "Total time",
        &format!("{} ms", format_number(total_time_ms as f64)),
    ]);

    results_table.add_row(vec![
        "Avg per operation",
        &format!("{avg_time_per_op:.3} ms"),
    ]);

    results_table.add_row(vec![
        "Throughput",
        &format!("{} ops/sec", format_number(ops_per_sec)),
    ]);

    println!("{results_table}");
}
