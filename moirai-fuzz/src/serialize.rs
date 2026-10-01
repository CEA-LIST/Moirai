use std::{
    fs,
    path::{Path, PathBuf},
};

use chrono::Local;
use log::info;
use serde::{Deserialize, Serialize};

use crate::{
    ReplicaRecorders,
    config::{RunConfig, SaveMode},
    fuzzer::{ExecutionSummary, ReplicaStats, RunReport, RunStats},
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
                .join(now.format("%Y-%m-%d").to_string()),
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

/// Save all runs beside their charts in bench-results/<name>/<YYYY-MM-DD>/.
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
    run_stats: &'a RunStats,
    measurements: &'a [ReplicaStats],
    #[serde(skip_serializing_if = "Option::is_none")]
    recorders: Option<&'a [ReplicaRecorders]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_graph_dot: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inter_replica_concurrency_ratio: &'a Option<f64>,
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
                        },
                    }
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use moirai_protocol::{event::id::ResolvedEventId, replica::ReplicaId};
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        EffectRecorder,
        config::{CallSampling, ChurnConfig, RunConfig},
        fuzzer::{ReplicaStats, RunStats},
        metrics::EffectSample,
    };

    #[test]
    fn save_target_uses_the_execution_start_date_for_the_artifact_directory() {
        let target = SaveTarget::new("aw_set");
        let started = chrono::DateTime::parse_from_rfc3339(&target.timestamp).unwrap();
        let date = started.format("%Y-%m-%d").to_string();
        assert_eq!(
            target.directory(),
            Path::new("bench-results").join("aw_set").join(&date)
        );
        assert!(target.stem().starts_with(&format!("{date}-")));

        let report = target.report("aw_set", false);
        assert_eq!(report.metadata.name, "aw_set");
        assert_eq!(report.metadata.timestamp, target.timestamp);
        assert_eq!(report.metadata.git_commit, target.git_commit);
        assert_eq!(report.metadata.git_branch, target.git_branch);
        assert!(!report.metadata.final_merge);
    }

    fn report_with_metrics() -> ExecutionReport {
        let mut replica_stats = ReplicaStats::new();
        replica_stats.avg_effect_time = Some(Duration::new(1, 88));
        let report = RunReport {
            config: RunConfig::new(ChurnConfig::new(0.4, 0.6), 2, 1, None, Some([0xAB; 32])),
            final_value: None,
            run_stats: RunStats {
                avg_response_time: Some(Duration::from_nanos(1_088)),
                ..RunStats::default()
            },
            measurements: vec![replica_stats],
            recorders: None,
            execution_graph_dot: None,
            inter_replica_concurrency_ratio: None,
        };
        ExecutionReport {
            metadata: ExecutionMetadata {
                name: "test".to_owned(),
                timestamp: "1970-01-01T00:00:00Z".to_owned(),
                git_commit: None,
                git_branch: None,
                final_merge: true,
            },
            summary: None,
            runs: vec![RunRecord {
                run_number: 1,
                results: report,
            }],
        }
    }

    #[test]
    fn serialized_execution_record_contains_no_null_values() {
        let record = report_with_metrics();
        let value = serde_json::to_value(&record).unwrap();
        assert!(!contains_null(&value));
        assert!(value.get("metadata").is_none());
        assert!(value.get("git_commit").is_none());
        assert!(value.get("git_branch").is_none());
        for mode in [SaveMode::Summary, SaveMode::Full] {
            let exported: Value = serde_json::from_str(&record.to_json(mode).unwrap()).unwrap();
            assert_eq!(exported, value);
        }
        assert_eq!(
            value["runs"][0]["results"]["config"]["seed"],
            Value::String(format!("0x{}", "AB".repeat(32)))
        );
        assert!(value["runs"][0]["results"].get("seed").is_none());
        assert_eq!(
            value["runs"][0]["results"]["run_stats"]["avg_response_time_ns"],
            Value::from(1_088)
        );
        assert_eq!(
            value["runs"][0]["results"]["measurements"][0]["avg_effect_time_ns"],
            Value::from(1_000_000_088_u64)
        );

        let decoded: ExecutionReport = serde_json::from_value(value).unwrap();
        assert_eq!(
            decoded.runs[0].results.run_stats.avg_response_time,
            Some(Duration::from_nanos(1_088))
        );
        assert_eq!(
            decoded.runs[0].results.measurements[0].avg_effect_time,
            Some(Duration::new(1, 88))
        );
        assert_eq!(decoded.runs[0].results.config.seed, Some([0xAB; 32]));
        assert_eq!(decoded.metadata, record.metadata);
    }

    #[test]
    fn execution_exports_borrow_metadata_and_preserve_flat_json() {
        let mut report = report_with_metrics();
        report.metadata.git_commit = Some("0123456789abcdef".to_owned());
        report.metadata.git_branch = Some("feature/metrics".to_owned());
        report.metadata.final_merge = false;
        let expected_metadata = json!({
            "name": "test",
            "timestamp": "1970-01-01T00:00:00Z",
            "git_commit": "0123456789abcdef",
            "git_branch": "feature/metrics",
            "final_merge": false
        });

        for mode in [SaveMode::Summary, SaveMode::Full] {
            let view = ExecutionExport::new(&report, mode);
            assert!(std::ptr::eq(view.metadata, &report.metadata));
            let json = report.to_json(mode).unwrap();
            let value: Value = serde_json::from_str(&json).unwrap();
            assert!(value.get("metadata").is_none());
            assert_eq!(value.as_object().unwrap().len(), 6);
            for (key, expected) in expected_metadata.as_object().unwrap() {
                assert_eq!(&value[key], expected);
            }
            assert_eq!(json.matches("\"git_commit\"").count(), 1);
            assert_eq!(json.matches("\"git_branch\"").count(), 1);
            let decoded: ExecutionReport = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(decoded.metadata, report.metadata);
            assert_eq!(serde_json::to_value(decoded).unwrap(), value);
        }
    }

    #[test]
    fn summary_omits_recorders_while_full_borrows_and_preserves_them() {
        let mut report = report_with_metrics();
        let mut effect_recorder = EffectRecorder::new(Some(CallSampling::All));
        let effect_id = effect_recorder.inc();
        effect_recorder.time.push(EffectSample::new(
            effect_id,
            Duration::from_nanos(900),
            ResolvedEventId::new(ReplicaId::from("replica-a"), 1),
        ));
        report.runs[0].results.recorders = Some(vec![ReplicaRecorders {
            delivered_event_count: 1,
            effect_recorder,
            ..ReplicaRecorders::default()
        }]);

        let summary_view = ExecutionExport::new(&report, SaveMode::Summary);
        assert!(summary_view.runs[0].results.recorders.is_none());
        let full_view = ExecutionExport::new(&report, SaveMode::Full);
        assert!(std::ptr::eq(
            full_view.runs[0].results.recorders.unwrap(),
            report.runs[0].results.recorders.as_deref().unwrap()
        ));

        let full: Value = serde_json::from_str(&report.to_json(SaveMode::Full).unwrap()).unwrap();
        let summary: Value =
            serde_json::from_str(&report.to_json(SaveMode::Summary).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(&report).unwrap(), full);
        let mut expected_summary = full.clone();
        expected_summary["runs"][0]["results"]
            .as_object_mut()
            .unwrap()
            .remove("recorders");
        assert_eq!(summary, expected_summary);

        let decoded_full: ExecutionReport = serde_json::from_value(full).unwrap();
        let decoded_recorders = decoded_full.runs[0].results.recorders.as_ref().unwrap();
        assert_eq!(decoded_recorders[0].effect_recorder.time.len(), 1);
        assert_eq!(
            decoded_recorders[0].effect_recorder.time[0].duration(),
            Duration::from_nanos(900)
        );
        let decoded_summary: ExecutionReport = serde_json::from_value(summary).unwrap();
        assert!(decoded_summary.runs[0].results.recorders.is_none());
        assert_eq!(decoded_summary.metadata, report.metadata);
    }

    fn contains_null(value: &Value) -> bool {
        match value {
            Value::Null => true,
            Value::Array(values) => values.iter().any(contains_null),
            Value::Object(values) => values.values().any(contains_null),
            _ => false,
        }
    }
}
