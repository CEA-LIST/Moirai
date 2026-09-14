// TODO: add information about the max number of events between two stabilizations
// TODO: add information about the shape of the execution graph (height, width, etc.)

use std::fmt::Debug;

use log::{debug, info, warn};
use moirai_protocol::{
    crdt::{eval::EvalNested, query::QueryOperation},
    state::log::IsLog,
};
use serde::{Deserialize, Serialize};

use crate::{
    config::{FuzzerConfig, RunConfig},
    display::{display_config_table, display_run_results, display_summary},
    op_generator::CommandGenerator,
    runner::{RunData, runner},
    serialize::save_execution_record,
    utils::format::seed_to_hex,
};

pub fn fuzzer<L, Q>(config: FuzzerConfig<L, Q>)
where
    L: IsLog + CommandGenerator + EvalNested<Q> + deepsize::DeepSizeOf,
    L::Op: deepsize::DeepSizeOf,
    Q: QueryOperation,
    <Q as QueryOperation>::Response: Debug,
{
    let _ = env_logger::builder()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(buf, "{}", record.args())
        })
        .try_init();
    let FuzzerConfig {
        name,
        runs,
        final_merge,
        predicate,
        save_execution,
        oracle_driver,
    } = config;

    if oracle_driver.is_some() {
        assert!(
            runs.iter().all(|run| run.churn_rate < 1.0),
            "Omega fuzzing requires a churn rate below 1.0 so that an online leader can eventually be selected"
        );
    }

    let mut run_results_list: Vec<(usize, (RunResults, RunConfig))> = Vec::new();

    for (run_idx, run_config) in runs.into_iter().enumerate() {
        debug!("Starting run {}", run_idx + 1);

        // Run configuration display
        let config_table = display_config_table(&run_config, final_merge);

        info!("{}", config_table);

        let run_data = runner::<L, Q>(run_config, final_merge, &predicate, oracle_driver.as_ref());
        let results = run_results(&run_data);

        debug!("Run {} completed", run_idx + 1);

        let run_table = display_run_results(run_idx + 1, &results);

        info!("{}", run_table);

        if save_execution {
            run_results_list.push((run_idx + 1, (results, run_data.config)));
        }
    }

    let maybe_summary = execution_summary(&run_results_list);

    // Display summary across all runs
    if let Some(ref execution_summary) = maybe_summary {
        let summary_table = display_summary(execution_summary);
        info!("{}", summary_table);
    }

    // Save all runs at the end
    if save_execution
        && !run_results_list.is_empty()
        && let Err(e) = save_execution_record(name, final_merge, run_results_list, maybe_summary)
    {
        warn!("Failed to save execution record: {e}");
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
    /// Inter-replica concurrency ratio (if execution graph was generated)
    pub inter_replica_concurrency_ratio: Option<f64>,
    /// Seed
    pub used_seed: String,
    /// Optional retained-memory timeline and sampled peaks, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_usage: Option<crate::memory::MemoryUsage>,
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
        .map(|duration| run_data.config.num_operations as f64 / (duration.as_secs_f64()))
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
        inter_replica_concurrency_ratio: run_data.inter_replica_concurrency_ratio,
        used_seed: seed_to_hex(&run_data.used_seed),
        memory_usage: run_data.memory_usage.clone(),
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

#[cfg(test)]
mod memory_tests {
    use super::*;
    use crate::{config::Predicate, memory::MemoryPhase};
    use moirai_protocol::{
        clock::version_vector::Version, crdt::query::Read, event::Event,
        state::effect_context::EffectContext,
    };

    #[derive(Default, deepsize::DeepSizeOf)]
    struct SumLog(Vec<u64>);

    impl IsLog for SumLog {
        type Command = u64;
        type Op = u64;
        type Rejection = std::convert::Infallible;

        fn prepare(&self, command: u64) -> u64 {
            command
        }
        fn effect(&mut self, event: Event<u64>, _: &mut EffectContext<'_>) {
            self.0.push(*event.op());
        }
        fn stabilize(&mut self, _: &Version) {}
        fn redundant_by_parent(&mut self, _: &Version, _: bool) {
            self.0.clear();
        }
        fn is_default(&self) -> bool {
            self.0.is_empty()
        }
    }

    impl EvalNested<Read<u64>> for SumLog {
        fn execute_query(&self, _: &Read<u64>) -> u64 {
            self.0.iter().sum()
        }
    }

    impl CommandGenerator for SumLog {
        fn generate_command(&self, _: &mut impl rand::Rng) -> u64 {
            1
        }
    }

    #[test]
    fn memory_timeline_covers_operations_merges_and_queries_and_roundtrips() {
        let config =
            RunConfig::new(0.5, 2, 5, None, Some([7; 32]), false, false).with_memory_sampling(2);
        let data = runner::<SumLog, _>(
            config,
            true,
            &Predicate::new(Read::new(), |a, b| a == b),
            None,
        );
        let results = run_results(&data);
        let memory = results.memory_usage.as_ref().unwrap();
        let points: Vec<_> = memory
            .samples
            .iter()
            .map(|s| (s.issued_operations, s.phase))
            .collect();
        assert_eq!(
            points,
            vec![
                (0, MemoryPhase::Initial),
                (2, MemoryPhase::Operations),
                (4, MemoryPhase::Operations),
                (5, MemoryPhase::Operations),
                (5, MemoryPhase::FinalMerge),
                (5, MemoryPhase::FinalMerge),
                (5, MemoryPhase::FinalQueries),
            ]
        );
        assert_eq!(
            memory.final_bytes_per_replica,
            memory.samples.last().unwrap().bytes_per_replica
        );
        for i in 0..2 {
            assert_eq!(
                memory.sampled_peak_bytes_per_replica[i],
                memory
                    .samples
                    .iter()
                    .map(|s| s.bytes_per_replica[i])
                    .max()
                    .unwrap()
            );
            assert!(memory.final_bytes_per_replica[i] > memory.samples[0].bytes_per_replica[i]);
        }
        let mut json = serde_json::to_value(&results).unwrap();
        let decoded: RunResults = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(decoded.memory_usage, results.memory_usage);
        json.as_object_mut().unwrap().remove("memory_usage");
        assert!(
            serde_json::from_value::<RunResults>(json)
                .unwrap()
                .memory_usage
                .is_none()
        );
    }

    #[test]
    fn memory_sampling_is_optional() {
        let config = RunConfig::new(0.0, 2, 3, None, Some([7; 32]), false, false);
        let data = runner::<SumLog, _>(
            config,
            false,
            &Predicate::new(Read::new(), |a, b| a == b),
            None,
        );
        assert!(run_results(&data).memory_usage.is_none());
    }

    #[test]
    #[should_panic(expected = "Memory sample interval must be greater than 0")]
    fn zero_memory_sample_interval_is_rejected() {
        RunConfig::new(0.0, 2, 3, None, None, false, false).with_memory_sampling(0);
    }
}
