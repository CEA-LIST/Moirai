use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    state::{log::IsLog, unstable_state::IsUnstableState},
};
#[cfg(feature = "fuzz")]
use crate::HashMap;

pub struct FuzzerConfig<'a, L>
where
    L: IsLog,
{
    /// Name of the simulation, used for logging
    pub name: &'a str,
    pub runs: Vec<RunConfig>,
    /// Whether to perform a final merge after all operations are issued
    pub final_merge: bool,
    /// Comparison function to check if the replicas converge
    pub compare: fn(&L::Value, &L::Value) -> bool,
    /// Whether to save the execution results to a JSON file in logs/
    pub save_execution: bool,
}

impl<'a, L> FuzzerConfig<'a, L>
where
    L: IsLog,
{
    pub fn new(
        name: &'a str,
        runs: Vec<RunConfig>,
        final_merge: bool,
        compare: fn(&L::Value, &L::Value) -> bool,
        save_execution: bool,
    ) -> Self {
        assert!(
            !runs.is_empty(),
            "At least one run configuration must be provided"
        );
        Self {
            name,
            runs,
            final_merge,
            compare,
            save_execution,
        }
    }
}

pub trait OpGenerator: PureCRDT {
    type Config: Default;

    fn generate(
        rng: &mut impl RngCore,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Self;
}

pub trait OpGeneratorNested: IsLog {
    fn generate(&self, rng: &mut impl RngCore) -> Self::Op;
}

#[derive(Clone)]
pub struct RunConfig {
    /// Churn rate defines the probability of a replica going offline after each operation
    pub churn_rate: f64,
    /// Number of replicas in the system
    pub num_replicas: u8,
    /// Total number of operations to be issued
    pub num_operations: usize,
    /// Optional reachability matrix to define which replicas can communicate with each other
    pub reachability: Option<Vec<Vec<bool>>>,
    /// Seed for the random number generator
    pub seed: Option<[u8; 32]>,
    /// Whether to generate an execution graph in GraphViz format
    pub generate_execution_graph: bool,
}

impl RunConfig {
    pub fn new(
        churn_rate: f64,
        num_replicas: u8,
        num_operations: usize,
        reachability: Option<Vec<Vec<bool>>>,
        seed: Option<[u8; 32]>,
        generate_execution_graph: bool,
    ) -> Self {
        assert!(
            (0.0..=1.0).contains(&churn_rate),
            "Churn rate must be between 0 and 1"
        );
        assert!(
            num_replicas > 1,
            "Number of replicas must be greater than 1"
        );
        assert!(
            num_operations > 0,
            "Number of operations must be greater than 0"
        );
        if let Some(matrix) = &reachability {
            assert!(
                matrix.len() == num_replicas as usize,
                "Reachability matrix must have size equal to number of replicas"
            );
            for row in matrix {
                assert!(
                    row.len() == num_replicas as usize,
                    "Each row in reachability matrix must have size equal to number of replicas"
                );
            }
        }
        Self {
            churn_rate,
            num_replicas,
            num_operations,
            reachability,
            seed,
            generate_execution_graph,
        }
    }
}

/// Structure to save execution results to JSON (contains all runs)
#[cfg(feature = "fuzz")]
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
    pub summary: ExecutionSummary,
    /// Results from all runs
    pub runs: Vec<RunRecord>,
}

/// Aggregated statistics across all runs
#[cfg(feature = "fuzz")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionSummary {
    pub total_runs: usize,
    pub total_operations: usize,
    pub total_time_ms: u128,
    pub mean_throughput_ops_per_sec: f64,
    pub min_throughput_ops_per_sec: f64,
    pub max_throughput_ops_per_sec: f64,
    pub mean_time_per_op_ms: f64,
    pub all_converged: bool,
}

/// Record for a single run within an execution
#[cfg(feature = "fuzz")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunRecord {
    /// Run number (1-indexed)
    pub run_number: usize,
    /// Input parameters for this run
    pub parameters: RunParameters,
    /// Execution results for this run
    pub results: RunResults,
}

#[cfg(feature = "fuzz")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunParameters {
    pub num_replicas: u8,
    pub num_operations: usize,
    pub churn_rate: f64,
    pub seed: String,
    pub generate_execution_graph: bool,
}

#[cfg(feature = "fuzz")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RunResults {
    pub convergence: bool,
    pub final_state: String,
    pub delivered_events: usize,
    pub total_operations: usize,
    pub total_time_ms: u128,
    pub avg_time_per_op_ms: f64,
    pub throughput_ops_per_sec: f64,
    pub replica_times_ms: HashMap<usize, u128>,
    /// Execution graph in GraphViz DOT format (if generated)
    pub execution_graph_dot: Option<String>,
}
