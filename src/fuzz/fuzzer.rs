// TODO: add information about the max number of events between two stabilizations
// TODO: add information about the shape of the execution graph (height, width, etc.)

use serde::{Deserialize, Serialize};

use crate::{
    fuzz::{
        config::{FuzzerConfig, OpGeneratorNested, RunConfig},
        display::{display_config_table, display_run_results, display_summary},
        runner::{runner, RunData},
        serialize::save_execution_record,
        utils::seed_to_hex,
    },
    protocol::{
        crdt::{eval::EvalNested, query::Read},
        state::log::IsLog,
    },
};

pub fn fuzzer<L>(config: FuzzerConfig<L>)
where
    L: IsLog + OpGeneratorNested + EvalNested<Read<<L as IsLog>::Value>>,
{
    let mut run_results_list: Vec<(usize, (RunResults, RunConfig))> = Vec::new();

    for (run_idx, run_config) in config.runs.into_iter().enumerate() {
        println!("\n");

        // Run configuration display
        let config_table = display_config_table(&run_config, config.final_merge);

        println!("{config_table}");

        let run_data = runner::<L>(run_config, config.final_merge, config.compare);
        let results = run_results(&run_data);

        println!("\n");

        let run_table = display_run_results(run_idx + 1, &results);

        println!("{run_table}");

        if config.save_execution {
            run_results_list.push((run_idx + 1, (results, run_data.config)));
        }
    }

    let maybe_summary = execution_summary(&run_results_list);

    // Display summary across all runs
    if let Some(ref execution_summary) = maybe_summary {
        let summary_table = display_summary(execution_summary);
        println!("{summary_table}");
    }

    // Save all runs at the end
    if config.save_execution && !run_results_list.is_empty() {
        if let Err(e) = save_execution_record(
            config.name,
            config.final_merge,
            run_results_list,
            maybe_summary,
        ) {
            eprintln!("Failed to save execution record: {e}");
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunResults {
    /// Snapshot of the final converged state
    pub final_state: String,
    /// Average time to deliver an operation (in milliseconds), across all replicas
    pub avg_time_per_op_ms: f64,
    /// Average number of operations delivered/second, across all replicas
    pub avg_throughput_ops_per_sec: f64,
    /// Total time to deliver all operations (in milliseconds), for each replica. Index i = replica i
    pub total_deliver_ms_per_replica: Vec<u128>,
    /// Total time spent in effect() per replica (in milliseconds). Index i = replica i
    pub total_effect_ms_per_replica: Vec<u128>,
    /// Average time per effect() call (in milliseconds), across all replicas
    pub avg_effect_ms: f64,
    /// Execution graph in GraphViz DOT format (if generated)
    pub execution_graph_dot: Option<String>,
    /// Seed
    pub used_seed: String,
}

fn run_results(run_data: &RunData) -> RunResults {
    let avg_time_per_op_ms = run_data
        .total_time_to_deliver_per_replica
        .values()
        .map(|d| d.as_millis() as f64 / run_data.config.num_operations as f64)
        .sum::<f64>()
        / run_data.config.num_replicas as f64;

    let avg_throughput_ops_per_sec = run_data
        .total_time_to_deliver_per_replica
        .values()
        .map(|d| {
            if d.as_secs_f64() > 0.0 {
                run_data.config.num_operations as f64 / d.as_secs_f64()
            } else {
                0.0
            }
        })
        .sum::<f64>()
        / run_data.config.num_replicas as f64;

    let total_deliver_ms_per_replica = {
        let mut vec = vec![0u128; run_data.config.num_replicas as usize];
        for (idx, duration) in run_data.total_time_to_deliver_per_replica.iter() {
            vec[idx.0] = duration.as_millis();
        }
        vec
    };

    let total_effect_ms_per_replica = {
        let mut vec = vec![0u128; run_data.config.num_replicas as usize];
        for (idx, duration) in run_data.total_time_in_effect_per_replica.iter() {
            vec[idx.0] = duration.as_millis();
        }
        vec
    };

    let avg_effect_ms = run_data
        .total_time_in_effect_per_replica
        .values()
        .map(|d| d.as_millis() as f64)
        .sum::<f64>()
        / (run_data.config.num_replicas as f64);

    RunResults {
        final_state: run_data.first_value.clone(),
        avg_time_per_op_ms,
        avg_throughput_ops_per_sec,
        total_deliver_ms_per_replica,
        total_effect_ms_per_replica,
        avg_effect_ms,
        execution_graph_dot: run_data.execution_graph_dot.clone(),
        used_seed: seed_to_hex(&run_data.used_seed),
    }
}

/// Aggregated statistics across all runs
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionSummary {
    /// Number of runs executed
    pub total_runs: usize,
    /// Average number of operations delivered/second, across all runs, across all replicas
    pub avg_runs_per_replica_throughput_ops_per_sec: f64,
    /// Minimum number of operations delivered/second, across all runs, across all replicas
    pub min_runs_per_replica_throughput_ops_per_sec: f64,
    /// Maximum number of operations delivered/second, across all runs, across all replicas
    pub max_runs_per_replica_throughput_ops_per_sec: f64,
    /// Average time to deliver an operation (in milliseconds), across all runs, across all replicas
    pub avg_time_per_op_ms: f64,
}

fn execution_summary(
    run_results_list: &[(usize, (RunResults, RunConfig))],
) -> Option<ExecutionSummary> {
    if run_results_list.len() < 2 {
        return None;
    }

    let total_runs = run_results_list.len();

    Some(ExecutionSummary {
        total_runs,
        avg_runs_per_replica_throughput_ops_per_sec: 0.0,
        min_runs_per_replica_throughput_ops_per_sec: 0.0,
        max_runs_per_replica_throughput_ops_per_sec: 0.0,
        avg_time_per_op_ms: 0.0,
    })
}
