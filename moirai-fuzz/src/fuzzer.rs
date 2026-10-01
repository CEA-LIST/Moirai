use std::{fmt::Debug, time::Duration};

use deepsize::DeepSizeOf;
use log::debug;
use moirai_protocol::{
    crdt::{eval::EvalNested, query::QueryOperation},
    state::log::IsLog,
};
use serde::{Deserialize, Serialize};

pub use crate::serialize::{ExecutionMetadata, ExecutionReport, RunRecord};
use crate::{
    config::{CallSampling, FuzzerConfig, RunConfig},
    display::{display_config_table, display_run_results, display_summary},
    generator::command_generator::CommandGenerator,
    observers::{
        footprint::{FootprintNode, LogFootprint},
        recorder::{
            EffectRecorder, LogSizeRecorder, QueryRecorder, ReplicaRecorders, ReplicaSizeRecorder,
            ResponseRecorder, StabilityRecorder,
        },
    },
    runner::{RunCapture, runner},
    serialize::{SaveTarget, save_record},
    utils::{
        format::{clean_dot_output, format_string_ellipsis},
        option_duration_nanos,
    },
};

/// Execute runs and return structured results, whether or not persistence is enabled.
/// This library never installs or reconfigures a global logger.
pub fn fuzzer<L, Q>(config: FuzzerConfig<L, Q>) -> ExecutionReport
where
    L: IsLog + CommandGenerator + EvalNested<Q> + DeepSizeOf + LogFootprint,
    L::Op: DeepSizeOf,
    Q: QueryOperation,
    <Q as QueryOperation>::Response: Debug,
{
    let FuzzerConfig {
        name,
        runs,
        final_merge,
        predicate,
        save,
        display,
        oracle_driver,
    } = config;

    if oracle_driver.is_some() {
        assert!(
            runs.iter().all(|run| run.churn.p_reconnect.val() > 0.0),
            "Omega fuzzing requires that an online leader can eventually be selected"
        );
    }

    let target = SaveTarget::new(name);
    let mut report = target.report(name, final_merge);

    for (run_idx, run_config) in runs.into_iter().enumerate() {
        debug!("Starting run {}", run_idx + 1);

        // Run configuration display
        if display {
            println!("{}", display_config_table(&run_config, final_merge));
        }

        let run_data = runner::<L, Q>(
            run_config,
            final_merge,
            &predicate,
            oracle_driver.as_ref(),
            display,
        );

        let results = run_report(run_data);

        debug!("Run {} completed", run_idx + 1);

        if display {
            for run_table in display_run_results(run_idx + 1, &results) {
                println!("{run_table}");
            }
        }

        report.runs.push(RunRecord {
            run_number: run_idx + 1,
            results,
        });
    }

    report.summary = execution_summary(&report.runs);

    // Display summary across all runs
    if display && let Some(ref summary) = report.summary {
        println!("{}", display_summary(summary));
    }

    // Save all runs at the end
    if let Some(mode) = save
        && let Err(e) = save_record(&report, &target, mode)
    {
        eprintln!("Failed to save execution record: {e}");
    }
    report
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicaStats {
    /// Recorded sample counts (zero means enabled but no calls were selected).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_sample_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_sample_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_sample_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_size_sample_count: Option<usize>,
    /// Snapshot count: an effect and its following stability may contribute two snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_size_sample_count: Option<usize>,
    //* STABILITY */
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability_progress_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg_events_between_stability: Option<f64>,
    //* EFFECT TIME */
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_effect_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_effect_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "p99_effect_time_ns",
        with = "option_duration_nanos"
    )]
    pub p99_effect_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "total_effect_time_ns",
        with = "option_duration_nanos"
    )]
    pub total_effect_time: Option<Duration>,
    //* RESPONSE TIME */
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_response_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_response_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "p99_response_time_ns",
        with = "option_duration_nanos"
    )]
    pub p99_response_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "total_response_time_ns",
        with = "option_duration_nanos"
    )]
    pub total_response_time: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_calls_per_sec: Option<f64>,
    //* QUERY TIME */
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_query_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_query_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "p99_query_time_ns",
        with = "option_duration_nanos"
    )]
    pub p99_query_time: Option<Duration>,
    //* REPLICA SIZE */
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled_peak_replica_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_replica_size: Option<usize>,
    //* LOG SIZE */
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled_peak_log_size: Option<FootprintNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_log_size: Option<FootprintNode>,
}

impl ReplicaStats {
    pub fn new() -> Self {
        Self {
            response_sample_count: None,
            effect_sample_count: None,
            query_sample_count: None,
            replica_size_sample_count: None,
            log_size_sample_count: None,
            stability_progress_count: None,
            avg_events_between_stability: None,
            avg_effect_time: None,
            p99_effect_time: None,
            total_effect_time: None,
            avg_response_time: None,
            p99_response_time: None,
            total_response_time: None,
            throughput_calls_per_sec: None,
            avg_query_time: None,
            p99_query_time: None,
            sampled_peak_replica_size: None,
            final_replica_size: None,
            sampled_peak_log_size: None,
            final_log_size: None,
        }
    }

    pub fn from_recorders(recorders: &ReplicaRecorders) -> Self {
        let mut stats = Self::new();

        stats.response_sample_count = recorders
            .response_recorder
            .config
            .as_ref()
            .map(|_| recorders.response_recorder.time.len());
        stats.effect_sample_count = recorders
            .effect_recorder
            .config
            .as_ref()
            .map(|_| recorders.effect_recorder.time.len());
        stats.query_sample_count = recorders
            .query_recorder
            .config
            .as_ref()
            .map(|_| recorders.query_recorder.time.len());
        stats.replica_size_sample_count = recorders
            .replica_size_recorder
            .config
            .as_ref()
            .map(|_| recorders.replica_size_recorder.size.len());
        stats.log_size_sample_count = recorders
            .log_size_recorder
            .config
            .as_ref()
            .map(|_| recorders.log_size_recorder.size.len());

        stats.stability_progress_count(&recorders.stability_recorder);
        stats.avg_events_between_stability(&recorders.stability_recorder);
        stats.avg_effect_time(&recorders.effect_recorder);
        stats.p99_effect_time(&recorders.effect_recorder);
        stats.total_effect_time(&recorders.effect_recorder);
        stats.avg_response_time(&recorders.response_recorder);
        stats.p99_response_time(&recorders.response_recorder);
        stats.total_response_time(&recorders.response_recorder);
        stats.throughput(&recorders.response_recorder);
        stats.avg_query_time(&recorders.query_recorder);
        stats.p99_query_time(&recorders.query_recorder);
        stats.sampled_peak_replica_size(&recorders.replica_size_recorder);
        stats.final_replica_size(&recorders.replica_size_recorder);
        stats.sampled_peak_log_size(&recorders.log_size_recorder);
        stats.final_log_size(&recorders.log_size_recorder);

        stats
    }

    pub fn stability_progress_count(&mut self, stability_recorder: &StabilityRecorder) {
        if stability_recorder.enabled {
            self.stability_progress_count = Some(stability_recorder.progress.len());
        }
    }

    pub fn avg_events_between_stability(&mut self, stability_recorder: &StabilityRecorder) {
        if stability_recorder.enabled && !stability_recorder.progress.is_empty() {
            let mut previous_frontier_sum = 0;
            let total_events = stability_recorder
                .progress
                .iter()
                .map(|sample| {
                    let frontier = sample.frontier();
                    let frontier_sum = frontier.sum();
                    let interval = frontier_sum - previous_frontier_sum;
                    previous_frontier_sum = frontier_sum;
                    interval
                })
                .sum::<usize>();

            self.avg_events_between_stability =
                Some(total_events as f64 / stability_recorder.progress.len() as f64);
        }
    }

    pub fn avg_effect_time(&mut self, effect_recorder: &EffectRecorder) {
        if effect_recorder.config.is_some() && !effect_recorder.time.is_empty() {
            let total = effect_recorder
                .time
                .iter()
                .map(|effect| effect.duration())
                .sum::<Duration>();
            self.avg_effect_time = average_duration(total, effect_recorder.time.len());
        }
    }

    pub fn p99_effect_time(&mut self, effect_recorder: &EffectRecorder) {
        if effect_recorder.config.is_some() && !effect_recorder.time.is_empty() {
            let mut durations = effect_recorder
                .time
                .iter()
                .map(|effect| effect.duration())
                .collect::<Vec<_>>();
            durations.sort_unstable();

            let index = (durations.len() as f64 * 0.99).ceil() as usize - 1;
            self.p99_effect_time = Some(durations[index]);
        }
    }

    pub fn total_effect_time(&mut self, effect_recorder: &EffectRecorder) {
        if is_exhaustive(&effect_recorder.config) {
            self.total_effect_time = Some(
                effect_recorder
                    .time
                    .iter()
                    .map(|effect| effect.duration())
                    .sum(),
            );
        }
    }

    pub fn avg_response_time(&mut self, response_recorder: &ResponseRecorder) {
        if response_recorder.config.is_some() && !response_recorder.time.is_empty() {
            let total = response_recorder
                .time
                .iter()
                .map(|response| response.duration())
                .sum::<Duration>();
            self.avg_response_time = average_duration(total, response_recorder.time.len());
        }
    }

    pub fn p99_response_time(&mut self, response_recorder: &ResponseRecorder) {
        if response_recorder.config.is_some() && !response_recorder.time.is_empty() {
            let mut durations = response_recorder
                .time
                .iter()
                .map(|response| response.duration())
                .collect::<Vec<_>>();
            durations.sort_unstable();

            let index = (durations.len() as f64 * 0.99).ceil() as usize - 1;
            self.p99_response_time = Some(durations[index]);
        }
    }

    pub fn total_response_time(&mut self, response_recorder: &ResponseRecorder) {
        if is_exhaustive(&response_recorder.config) {
            self.total_response_time = Some(
                response_recorder
                    .time
                    .iter()
                    .map(|response| response.duration())
                    .sum(),
            );
        }
    }

    pub fn throughput(&mut self, response_recorder: &ResponseRecorder) {
        if is_exhaustive(&response_recorder.config) && !response_recorder.time.is_empty() {
            let total_response_time = response_recorder
                .time
                .iter()
                .map(|response| response.duration())
                .sum::<Duration>();

            if !total_response_time.is_zero() {
                self.throughput_calls_per_sec =
                    Some(response_recorder.time.len() as f64 / total_response_time.as_secs_f64());
            }
        }
    }

    pub fn avg_query_time(&mut self, query_recorder: &QueryRecorder) {
        if query_recorder.config.is_some() && !query_recorder.time.is_empty() {
            let total = query_recorder
                .time
                .iter()
                .map(|query| query.duration())
                .sum::<Duration>();
            self.avg_query_time = average_duration(total, query_recorder.time.len());
        }
    }

    pub fn p99_query_time(&mut self, query_recorder: &QueryRecorder) {
        if query_recorder.config.is_some() && !query_recorder.time.is_empty() {
            let mut durations = query_recorder
                .time
                .iter()
                .map(|query| query.duration())
                .collect::<Vec<_>>();
            durations.sort_unstable();

            let index = (durations.len() as f64 * 0.99).ceil() as usize - 1;
            self.p99_query_time = Some(durations[index]);
        }
    }

    pub fn sampled_peak_replica_size(&mut self, replica_size_recorder: &ReplicaSizeRecorder) {
        if replica_size_recorder.config.is_some() {
            self.sampled_peak_replica_size = replica_size_recorder
                .size
                .iter()
                .map(|sample| sample.bytes)
                .max();
        }
    }

    pub fn final_replica_size(&mut self, replica_size_recorder: &ReplicaSizeRecorder) {
        if replica_size_recorder.config.is_some() {
            self.final_replica_size = replica_size_recorder.final_size;
        }
    }

    pub fn sampled_peak_log_size(&mut self, log_size_recorder: &LogSizeRecorder) {
        if log_size_recorder.config.is_some() {
            self.sampled_peak_log_size = log_size_recorder
                .size
                .iter()
                .max_by_key(|sample| sample.footprint.inclusive_bytes)
                .map(|sample| sample.footprint.clone());
        }
    }

    pub fn final_log_size(&mut self, log_size_recorder: &LogSizeRecorder) {
        if log_size_recorder.config.is_some() {
            self.final_log_size = log_size_recorder.final_size.clone();
        }
    }
}

fn is_exhaustive(config: &Option<CallSampling>) -> bool {
    config.as_ref().is_some_and(CallSampling::is_exhaustive)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RunStats {
    pub response_sample_count: usize,
    /// Pooled mean: every recorded response call has equal weight.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_response_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_response_time: Option<Duration>,
    /// Equal weight per replica with at least one response sample, not per call.
    pub response_replica_count: usize,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_replica_response_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_replica_response_time: Option<Duration>,
    pub effect_sample_count: usize,
    /// Pooled mean: every recorded effect has equal weight.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_effect_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_effect_time: Option<Duration>,
    pub effect_replica_count: usize,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "avg_replica_effect_time_ns",
        with = "option_duration_nanos"
    )]
    pub avg_replica_effect_time: Option<Duration>,
}

impl RunStats {
    pub fn from_recorders(replica_recorders: &[ReplicaRecorders]) -> Self {
        let response_sample_count = replica_recorders
            .iter()
            .map(|recorders| recorders.response_recorder.time.len())
            .sum();
        let total_response_time = replica_recorders
            .iter()
            .flat_map(|recorders| &recorders.response_recorder.time)
            .map(|sample| sample.duration())
            .sum();

        let effect_sample_count = replica_recorders
            .iter()
            .map(|recorders| recorders.effect_recorder.time.len())
            .sum();
        let total_effect_time = replica_recorders
            .iter()
            .flat_map(|recorders| &recorders.effect_recorder.time)
            .map(|sample| sample.duration())
            .sum();

        let response_means = replica_recorders
            .iter()
            .filter_map(|r| {
                average_duration(
                    r.response_recorder.time.iter().map(|s| s.duration()).sum(),
                    r.response_recorder.time.len(),
                )
            })
            .collect::<Vec<_>>();
        let effect_means = replica_recorders
            .iter()
            .filter_map(|r| {
                average_duration(
                    r.effect_recorder.time.iter().map(|s| s.duration()).sum(),
                    r.effect_recorder.time.len(),
                )
            })
            .collect::<Vec<_>>();

        Self {
            response_sample_count,
            avg_response_time: average_duration(total_response_time, response_sample_count),
            response_replica_count: response_means.len(),
            avg_replica_response_time: average_duration(
                response_means.iter().copied().sum(),
                response_means.len(),
            ),
            effect_sample_count,
            avg_effect_time: average_duration(total_effect_time, effect_sample_count),
            effect_replica_count: effect_means.len(),
            avg_replica_effect_time: average_duration(
                effect_means.iter().copied().sum(),
                effect_means.len(),
            ),
        }
    }
}

fn average_duration(total: Duration, sample_count: usize) -> Option<Duration> {
    if sample_count == 0 {
        return None;
    }

    let nanos = total.as_nanos() / sample_count as u128;
    Some(Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    ))
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunReport {
    pub config: RunConfig,
    /// Snapshot of the returned value of executing the query provided by the user to the first replica after all updates have been delivered everywhere.
    /// This assume that all replicas have converged to the same state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_value: Option<String>,
    pub run_stats: RunStats,
    pub measurements: Vec<ReplicaStats>,
    /// Raw captures are available to the caller regardless of saving.
    /// Summary exports omit them; Full exports include them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorders: Option<Vec<ReplicaRecorders>>,
    /// Execution graph in GraphViz DOT format (if generated)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_graph_dot: Option<String>,
    /// Inter-replica concurrency ratio (if execution graph was generated)
    /// # Calculation
    /// Cross-origin pairs total `sum_{r < s}(n_r * n_s)`, where `n_r` is the
    /// number of sends from replica r. Each event's non-origin version-vector
    /// components count its cross-origin causal predecessors. Subtract their
    /// sum from the total and divide by the total to obtain the concurrent
    /// fraction. The result is zero when there are no cross-origin pairs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_replica_concurrency_ratio: Option<f64>,
}

fn run_report<O>(mut capture: RunCapture<O>) -> RunReport
where
    O: Debug + Clone,
{
    let run_stats = RunStats::from_recorders(&capture.measurements);
    let measurements = capture
        .measurements
        .iter()
        .map(ReplicaStats::from_recorders)
        .collect();

    let (execution_graph_dot, inter_replica_concurrency_ratio) =
        if let Some(graph) = capture.execution_graph.take() {
            let ratio = graph.inter_replica_concurrency_ratio().clamp(0.0, 1.0);
            let dot = clean_dot_output(&graph.to_dot());
            (Some(dot), Some(ratio))
        } else {
            (None, None)
        };

    let mut config = capture.config;
    config.seed = Some(capture.seed);

    RunReport {
        config,
        final_value: capture
            .final_value
            .map(|v| format_string_ellipsis(&v, Some(120))),
        run_stats,
        measurements,
        recorders: Some(capture.measurements),
        execution_graph_dot,
        inter_replica_concurrency_ratio,
    }
}

/// Aggregated statistics across all runs
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionSummary {
    /// Number of runs executed
    pub total_runs: usize,
}

fn execution_summary(run_results_list: &[RunRecord]) -> Option<ExecutionSummary> {
    if run_results_list.len() < 2 {
        return None;
    }

    let total_runs = run_results_list.len();

    Some(ExecutionSummary { total_runs })
}
