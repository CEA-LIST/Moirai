use std::{fs, path::PathBuf};

use chrono::Local;
use serde::{Deserialize, Serialize};

use crate::fuzz::{
    config::RunConfig,
    fuzzer::{ExecutionSummary, RunResults},
    utils::{get_git_branch, get_git_commit},
};

/// Structure to save execution results to JSON (contains all runs)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionRecord {
    /// Name of the test
    pub name: String,
    /// Date and time of execution start (ISO 8601 format)
    pub timestamp: String,
    /// Git commit hash (if available)
    pub git_commit: Option<String>,
    /// Git branch name (if available)
    pub git_branch: Option<String>,
    /// Whether final merge was performed
    pub final_merge: bool,
    /// Aggregated summary statistics across all runs
    pub summary: Option<ExecutionSummary>,
    /// Results from all runs
    pub runs: Vec<RunRecord>,
}

/// Record for a single run within an execution
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunRecord {
    /// Run number (1-indexed)
    pub run_number: usize,
    /// Input parameters for this run
    pub parameters: RunParameters,
    /// Execution results for this run
    pub results: RunResults,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunParameters {
    pub num_replicas: u8,
    pub num_operations: usize,
    pub churn_rate: f64,
}

/// Save execution record with multiple runs to JSON file in logs/ directory
pub fn save_execution_record(
    test_name: &str,
    final_merge: bool,
    run_data_list: Vec<(usize, (RunResults, RunConfig))>,
    maybe_summary: Option<ExecutionSummary>,
) -> Result<(), Box<dyn std::error::Error>> {
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
        .map(|(run_number, data)| RunRecord {
            run_number,
            parameters: RunParameters {
                num_replicas: data.1.num_replicas,
                num_operations: data.1.num_operations,
                churn_rate: data.1.churn_rate,
            },
            results: RunResults {
                final_state: data.0.final_state,
                avg_time_per_op_ms: data.0.avg_time_per_op_ms,
                avg_throughput_ops_per_sec: data.0.avg_throughput_ops_per_sec,
                total_deliver_ms_per_replica: data.0.total_deliver_ms_per_replica,
                total_effect_ms_per_replica: data.0.total_effect_ms_per_replica,
                avg_effect_ms: data.0.avg_effect_ms,
                execution_graph_dot: data.0.execution_graph_dot,
                used_seed: data.0.used_seed,
            },
        })
        .collect();

    // Create execution record
    let record = ExecutionRecord {
        name: test_name.to_string(),
        timestamp: now.to_rfc3339(),
        git_commit,
        git_branch,
        final_merge,
        summary: maybe_summary,
        runs,
    };

    // Save to JSON file
    let json = serde_json::to_string_pretty(&record)?;
    fs::write(&filepath, json)?;

    println!("\n✓ Execution saved to: {}", filepath.display());

    Ok(())
}
