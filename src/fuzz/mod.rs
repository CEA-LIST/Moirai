use core::panic;
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

use comfy_table::{
    presets::UTF8_FULL, Attribute, Cell, CellAlignment, Color, ContentArrangement, Table,
};
use indicatif::{ProgressBar, ProgressStyle};
use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    crdt::test_util::bootstrap_n,
    fuzz::{
        config::{FuzzerConfig, OpGeneratorNested, RunConfig},
        utils::{clean_dot_output, format_number, format_string, get_git_branch, get_git_commit},
    },
    protocol::{
        broadcast::{
            message::{kind, Message},
            tcsb::Tcsb,
        },
        crdt::{eval::EvalNested, query::Read},
        membership::ReplicaIdx,
        replica::IsReplica,
        state::{event_graph::EventGraph, log::IsLog, unstable_state::IsUnstableState},
    },
    HashMap,
};

// TODO: add information about the max number of events between two stabilizations
// TODO: add information about the shape of the execution graph (height, width, etc.)

pub mod config;
mod utils;
pub mod value_generator;

/// Internal structure to hold run results before serialization
pub struct RunData {
    run_config: config::RunConfig,
    used_seed: [u8; 32],
    first_value: String,
    num_delivered_events: usize,
    count_ops: usize,
    total_time_ms: u128,
    avg_time_per_op: f64,
    ops_per_sec: f64,
    time_to_deliver: HashMap<ReplicaIdx, Duration>,
    execution_graph_dot: Option<String>,
}

pub fn fuzzer<L>(config: FuzzerConfig<L>)
where
    L: IsLog + OpGeneratorNested + EvalNested<Read<<L as IsLog>::Value>>,
{
    let mut run_data_list = Vec::new();

    for (run_idx, run_config) in config.runs.into_iter().enumerate() {
        let run_data = runner::<L>(run_config, config.final_merge, config.compare);

        if config.save_execution {
            run_data_list.push((run_idx + 1, run_data));
        }
    }

    // Display summary across all runs
    if run_data_list.len() > 1 {
        display_summary(&run_data_list);
    }

    // Save all runs at the end
    if config.save_execution && !run_data_list.is_empty() {
        if let Err(e) = save_execution_record(config.name, config.final_merge, run_data_list) {
            eprintln!("Failed to save execution record: {e}");
        }
    }
}

fn runner<L>(
    run_config: RunConfig,
    final_merge: bool,
    compare: fn(&L::Value, &L::Value) -> bool,
) -> RunData
where
    L: IsLog + OpGeneratorNested + EvalNested<Read<<L as IsLog>::Value>>,
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

    config_table.add_row(vec!["Replicas", &format!("{}", run_config.num_replicas)]);

    config_table.add_row(vec![
        "Operations",
        &format_number(run_config.num_operations as f64),
    ]);

    config_table.add_row(vec![
        "Churn rate",
        &format!("{:.1}%", run_config.churn_rate * 100.0),
    ]);

    config_table.add_row(vec!["Final merge", if final_merge { "Yes" } else { "No" }]);

    if let Some(seed) = run_config.seed {
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

    // Capture or generate the seed
    let used_seed = run_config.seed.unwrap_or_else(|| {
        let mut rng = ChaCha8Rng::from_os_rng();
        let mut seed = [0u8; 32];
        rng.fill(&mut seed);
        seed
    });

    println!("\x1b[1;34m🎲 Using seed: {:?}\x1b[0m", used_seed);

    let mut rng = ChaCha8Rng::from_seed(used_seed);
    let mut replicas = bootstrap_n::<L, Tcsb<L::Op>>(run_config.num_replicas);
    let reachability = run_config.reachability.clone().unwrap_or_else(|| {
        vec![vec![true; run_config.num_replicas.into()]; run_config.num_replicas.into()]
    });
    // `online[i]` indique si la réplique i est joignable à cette étape.
    // L'état initial est "toutes en ligne"; `churn_rate` contrôle désormais
    // directement la probabilité d'être hors-ligne (et non plus de changer d'état).
    let mut online = vec![true; run_config.num_replicas.into()];
    let mut count_ops = 0;

    let mut time_to_deliver: HashMap<ReplicaIdx, Duration> = HashMap::default();
    let mut _time_to_eval: HashMap<ReplicaIdx, Duration> = HashMap::default();

    // Create execution graph if requested
    let mut execution_graph: Option<EventGraph<L::Op>> = if run_config.generate_execution_graph {
        Some(EventGraph::default())
    } else {
        None
    };

    // Créer une barre de progression avec indicatif
    let pb = ProgressBar::new(run_config.num_operations as u64);
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
    while count_ops < run_config.num_operations {
        // Randomly select a replica
        let replica_idx = (0..run_config.num_replicas).choose(&mut rng).unwrap() as usize;

        online[replica_idx] = !rng.random_bool(run_config.churn_rate);

        if run_config.churn_rate == 0.0 {
            assert_eq!(online, vec![true; run_config.num_replicas.into()]);
        }

        // If the replica is online, deliver any pending events from other online replicas
        if online[replica_idx] {
            for other_idx in 0..run_config.num_replicas.into() {
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
        let op = replicas[replica_idx].state().generate(&mut rng);
        // println!(
        //     "\x1b[1;33m📝 Replica {} generated operation: {:?}\x1b[0m",
        //     replica_idx, op
        // );
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
        let msg = replicas[replica_idx].send(op.clone());
        let msg: Message<<L as IsLog>::Op, kind::Event> = match msg {
            Some(m) => {
                // println!(
                //     "\x1b[1;32m📤 Operation version: {}\x1b[0m",
                //     m.event().version()
                // );
                m
            }
            None => {
                panic!("Replica {} failed to send operation: {:?}", replica_idx, op);
            }
        };
        let duration = start.elapsed();
        time_to_deliver
            .entry(replica_idx)
            .and_modify(|d| *d += duration)
            .or_insert(duration);

        // Add event to execution graph if enabled
        if let Some(ref mut graph) = execution_graph {
            let event = msg.event().clone();
            graph.append(event);
        }

        if online[replica_idx] {
            for other_idx in 0..run_config.num_replicas.into() {
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
    }

    // Finaliser la barre de progression principale
    pb.finish_with_message("All operations completed ✓");

    // Final convergence phase
    if final_merge {
        let total_merges =
            (run_config.num_replicas as usize) * (run_config.num_replicas as usize - 1);
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

        for i in 0..run_config.num_replicas.into() {
            for j in 0..run_config.num_replicas.into() {
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
    let check_pb = ProgressBar::new((run_config.num_replicas as usize - 1) as u64);
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
                println!(
                    "Execution graph at divergence:\n{}",
                    clean_dot_output(&graph.to_dot())
                );
            }
            for (replica_idx, replica) in replicas.iter().enumerate() {
                println!("Replica {} log: {:#?}", replica_idx, replica.state());
            }
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

    // Generate DOT format for the execution graph if it was created
    let mut execution_graph_dot = None;

    if let Some(graph) = execution_graph {
        let output = graph.to_dot();
        execution_graph_dot = Some(clean_dot_output(&output));

        println!("\n\x1b[1;35m🗺️  Execution Graph (DOT format):\x1b[0m\n{output}");
    }

    // Return the run data
    RunData {
        run_config,
        used_seed,
        first_value: format_string(&first_value),
        num_delivered_events,
        count_ops,
        total_time_ms,
        avg_time_per_op,
        ops_per_sec,
        time_to_deliver,
        execution_graph_dot,
    }
}

/// Display summary statistics across all runs
fn display_summary(run_data_list: &[(usize, RunData)]) {
    println!("\n");
    println!("═══════════════════════════════════════════════════════════════");

    let mut summary_table = Table::new();
    summary_table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Execution Summary")
            .add_attribute(Attribute::Bold)
            .fg(Color::Magenta)
            .set_alignment(CellAlignment::Center)]);

    summary_table.add_row(vec![
        Cell::new("Metric").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    let total_runs = run_data_list.len();
    let all_converged = true; // All runs that completed converged
    let total_operations: usize = run_data_list.iter().map(|(_, d)| d.count_ops).sum();
    let total_time_ms: u128 = run_data_list.iter().map(|(_, d)| d.total_time_ms).sum();

    let throughputs: Vec<f64> = run_data_list.iter().map(|(_, d)| d.ops_per_sec).collect();
    let mean_throughput = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
    let min_throughput = throughputs.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_throughput = throughputs
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);

    let time_per_ops: Vec<f64> = run_data_list
        .iter()
        .map(|(_, d)| d.avg_time_per_op)
        .collect();
    let mean_time_per_op = time_per_ops.iter().sum::<f64>() / time_per_ops.len() as f64;

    summary_table.add_row(vec!["Total runs", &format_number(total_runs as f64)]);

    summary_table.add_row(vec![
        "All converged",
        if all_converged { "✓ Yes" } else { "✗ No" },
    ]);

    summary_table.add_row(vec![
        "Total operations",
        &format_number(total_operations as f64),
    ]);

    summary_table.add_row(vec![
        "Total time",
        &format!("{} ms", format_number(total_time_ms as f64)),
    ]);

    summary_table.add_row(vec![
        "Mean throughput",
        &format!("{} ops/sec", format_number(mean_throughput)),
    ]);

    summary_table.add_row(vec![
        "Min throughput",
        &format!("{} ops/sec", format_number(min_throughput)),
    ]);

    summary_table.add_row(vec![
        "Max throughput",
        &format!("{} ops/sec", format_number(max_throughput)),
    ]);

    summary_table.add_row(vec![
        "Mean time per op",
        &format!("{:.3} ms", mean_time_per_op),
    ]);

    println!("{summary_table}");
}

/// Save execution record with multiple runs to JSON file in logs/ directory
fn save_execution_record(
    test_name: &str,
    final_merge: bool,
    run_data_list: Vec<(usize, RunData)>,
) -> Result<(), Box<dyn std::error::Error>> {
    use chrono::Local;
    use config::{ExecutionRecord, RunParameters, RunRecord, RunResults};

    // Get current date and timestamp
    let now = Local::now();
    let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();

    // Get git information
    let git_commit = get_git_commit();
    let git_branch = get_git_branch();

    // Build filename: {date}_{branch}_{commit}.json
    let mut filename_parts = vec![date.clone()];

    if let Some(ref branch) = git_branch {
        // Sanitize branch name for filesystem (replace slashes, etc.)
        let safe_branch = branch.replace('/', "_");
        filename_parts.push(safe_branch);
    }

    if let Some(ref commit) = git_commit {
        // Use short commit hash (first 8 chars)
        let short_commit = &commit[..8.min(commit.len())];
        filename_parts.push(short_commit.to_string());
    }

    let filename = format!("{}.json", filename_parts.join("_"));

    // Create directory structure: logs/{test_name}/
    let logs_dir = PathBuf::from("logs").join(test_name);
    fs::create_dir_all(&logs_dir)?;

    // Build the full path
    let filepath = logs_dir.join(filename);

    // Convert each run data to RunRecord
    let runs: Vec<RunRecord> = run_data_list
        .into_iter()
        .map(|(run_number, data)| {
            // Convert seed to hex string
            let seed_hex = format!(
                "0x{:016X}",
                u64::from_le_bytes(data.used_seed[0..8].try_into().unwrap())
            );

            // Collect replica times
            let replica_times: HashMap<usize, u128> = data
                .time_to_deliver
                .iter()
                .map(|(idx, duration)| (*idx, duration.as_millis()))
                .collect();

            RunRecord {
                run_number,
                parameters: RunParameters {
                    num_replicas: data.run_config.num_replicas,
                    num_operations: data.run_config.num_operations,
                    churn_rate: data.run_config.churn_rate,
                    seed: seed_hex,
                    generate_execution_graph: data.run_config.generate_execution_graph,
                },
                results: RunResults {
                    convergence: true,
                    final_state: data.first_value,
                    delivered_events: data.num_delivered_events,
                    total_operations: data.count_ops,
                    total_time_ms: data.total_time_ms,
                    avg_time_per_op_ms: data.avg_time_per_op,
                    throughput_ops_per_sec: data.ops_per_sec,
                    replica_times_ms: replica_times,
                    execution_graph_dot: data.execution_graph_dot,
                },
            }
        })
        .collect();

    // Calculate summary statistics across all runs
    let total_runs = runs.len();
    let all_converged = runs.iter().all(|r| r.results.convergence);
    let total_operations: usize = runs.iter().map(|r| r.results.total_operations).sum();
    let total_time_ms: u128 = runs.iter().map(|r| r.results.total_time_ms).sum();

    let throughputs: Vec<f64> = runs
        .iter()
        .map(|r| r.results.throughput_ops_per_sec)
        .collect();
    let mean_throughput = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
    let min_throughput = throughputs.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_throughput = throughputs
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);

    let time_per_ops: Vec<f64> = runs.iter().map(|r| r.results.avg_time_per_op_ms).collect();
    let mean_time_per_op = time_per_ops.iter().sum::<f64>() / time_per_ops.len() as f64;

    let summary = config::ExecutionSummary {
        total_runs,
        total_operations,
        total_time_ms,
        mean_throughput_ops_per_sec: mean_throughput,
        min_throughput_ops_per_sec: min_throughput,
        max_throughput_ops_per_sec: max_throughput,
        mean_time_per_op_ms: mean_time_per_op,
        all_converged,
    };

    // Create execution record
    let record = ExecutionRecord {
        name: test_name.to_string(),
        timestamp: now.to_rfc3339(),
        git_commit,
        git_branch,
        final_merge,
        summary,
        runs,
    };

    // Save to JSON file
    let json = serde_json::to_string_pretty(&record)?;
    fs::write(&filepath, json)?;

    println!("\n✓ Execution saved to: {}", filepath.display());

    Ok(())
}
