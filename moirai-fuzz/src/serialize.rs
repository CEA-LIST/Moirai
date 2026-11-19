use std::{
    fs,
    path::{Path, PathBuf},
};

use chrono::Local;
use log::info;
use serde::{Deserialize, Serialize};

use crate::{
    config::{RunConfig, SaveMode},
    fuzzer::{ExecutionSummary, ReplicaStats, RunReport, RunStats},
    observers::recorder::ReplicaRecorders,
    utils::git::{get_git_branch, get_git_commit},
};

/// Metadata shared by the execution report and its borrowed persistence view.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionMetadata {
    /// Name of the test
    pub name: String,
    /// Date and time of execution start (ISO 8601 format)
    pub timestamp: String,
    /// Git commit hash (if available)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    /// Git branch name (if available)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_branch: Option<String>,
    /// Whether final merge was performed
    pub final_merge: bool,
}

/// Structure to save execution results to JSON (contains all runs)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionReport {
    /// Execution identity and settings, kept at the top level in JSON.
    #[serde(flatten)]
    pub metadata: ExecutionMetadata,
    /// Aggregated summary statistics across all runs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<ExecutionSummary>,
    /// Results from all runs
    pub runs: Vec<RunRecord>,
}

impl ExecutionReport {
    /// Serialize a borrowed view of the report, independently of execution or disk I/O.
    /// Summary contains derived statistics; Full also contains raw recorder samples.
    pub fn to_json(&self, mode: SaveMode) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&ExecutionExport::new(self, mode))
    }
}

/// Record for a single run within an execution
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunRecord {
    /// Run number (1-indexed)
    pub run_number: usize,
    /// Execution results for this run
    pub results: RunReport,
}

pub struct SaveTarget {
    timestamp: String,
    git_commit: Option<String>,
    git_branch: Option<String>,
    directory: PathBuf,
    /// Filename stem (without extension) for the JSON file, e.g. "2023-08-15-12-30-00_feature_branch_abcdef12"
    stem: String,
}

impl SaveTarget {
    pub fn new(test_name: &str) -> Self {
        let now = Local::now();
        let git_commit = get_git_commit();
        let git_branch = get_git_branch();
        let mut filename_parts = vec![now.format("%Y-%m-%d-%H-%M-%S").to_string()];

        if let Some(branch) = git_branch.as_ref() {
            filename_parts.push(branch.replace('/', "_"));
        }
        if let Some(commit) = git_commit.as_ref() {
            filename_parts.push(commit[..8.min(commit.len())].to_owned());
        }

        Self {
            timestamp: now.to_rfc3339(),
            git_commit,
            git_branch,
            directory: PathBuf::from("bench-results")
                .join(test_name)
                .join(now.format("%Y-%m-%d-%H-%M-%S").to_string()),
            stem: filename_parts.join("_"),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn stem(&self) -> &str {
        &self.stem
    }

    pub fn report(&self, name: &str, final_merge: bool) -> ExecutionReport {
        ExecutionReport {
            metadata: ExecutionMetadata {
                name: name.to_owned(),
                timestamp: self.timestamp.clone(),
                git_commit: self.git_commit.clone(),
                git_branch: self.git_branch.clone(),
                final_merge,
            },
            summary: None,
            runs: Vec::new(),
        }
    }
}

/// Save all runs beside their charts in bench-results/<name>/<YYYY-MM-DD-HH-SS-MM>/.
pub fn save_record(
    record: &ExecutionReport,
    save_target: &SaveTarget,
    mode: SaveMode,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(save_target.directory())?;
    let filepath = save_target
        .directory()
        .join(format!("{}.json", save_target.stem()));
    // Save to JSON file
    let json = record.to_json(mode)?;
    fs::write(&filepath, json)?;

    info!("✓ Execution saved to: {}", filepath.display());

    Ok(())
}

/// Borrowed persistence views: Summary never clones or serializes raw captures.
#[derive(Serialize)]
struct ExecutionExport<'a> {
    #[serde(flatten)]
    metadata: &'a ExecutionMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: &'a Option<ExecutionSummary>,
    runs: Vec<RunExport<'a>>,
}

#[derive(Serialize)]
struct RunExport<'a> {
    run_number: usize,
    results: RunReportExport<'a>,
}

#[derive(Serialize)]
struct RunReportExport<'a> {
    config: &'a RunConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_value: &'a Option<String>,
    #[serde(skip_serializing_if = "RunStats::is_redundant")]
    run_stats: &'a RunStats,
    measurements: &'a [ReplicaStats],
    #[serde(skip_serializing_if = "Option::is_none")]
    recorders: Option<&'a [ReplicaRecorders]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_graph_dot: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inter_replica_concurrency_ratio: &'a Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avg_concurrent_updates: &'a Option<f64>,
}

impl<'a> ExecutionExport<'a> {
    fn new(report: &'a ExecutionReport, mode: SaveMode) -> Self {
        Self {
            metadata: &report.metadata,
            summary: &report.summary,
            runs: report
                .runs
                .iter()
                .map(|run| {
                    let r = &run.results;
                    RunExport {
                        run_number: run.run_number,
                        results: RunReportExport {
                            config: &r.config,
                            final_value: &r.final_value,
                            run_stats: &r.run_stats,
                            measurements: &r.measurements,
                            recorders: if mode == SaveMode::Full {
                                r.recorders.as_deref()
                            } else {
                                None
                            },
                            execution_graph_dot: &r.execution_graph_dot,
                            inter_replica_concurrency_ratio: &r.inter_replica_concurrency_ratio,
                            avg_concurrent_updates: &r.avg_concurrent_updates,
                        },
                    }
                })
                .collect(),
        }
    }
}
