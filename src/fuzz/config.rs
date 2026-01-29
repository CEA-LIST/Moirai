use rand::RngCore;

use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    state::{log::IsLog, unstable_state::IsUnstableState},
};

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
    /// Whether to save the execution results to a JSON file in bench-results/
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

#[derive(Clone, Debug)]
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
    /// Whether to disable stability (stabilize() will never be called)
    pub disable_stability: bool,
}

impl RunConfig {
    pub fn new(
        churn_rate: f64,
        num_replicas: u8,
        num_operations: usize,
        reachability: Option<Vec<Vec<bool>>>,
        seed: Option<[u8; 32]>,
        generate_execution_graph: bool,
        disable_stability: bool,
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
            // Ensure that a process is always reachable to itself
            for (i, item) in matrix.iter().enumerate().take(num_replicas as usize) {
                assert!(
                    item[i],
                    "Each replica must be reachable to itself in the reachability matrix"
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
            disable_stability,
        }
    }
}
