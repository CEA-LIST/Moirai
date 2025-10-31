use std::path::Path;

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
    /// Set of operations to be performed by the replicas
    pub op_config: OpConfig,
    /// Whether to perform a final merge after all operations are issued
    pub final_merge: bool,
    /// Comparison function to check if the replicas converge
    pub compare: fn(&L::Value, &L::Value) -> bool,
    /// Whether to log the results to a file
    pub record_results: Option<RecorderConfig<'a>>,
}

impl<'a, L> FuzzerConfig<'a, L>
where
    L: IsLog,
{
    pub fn new(
        name: &'a str,
        runs: Vec<RunConfig>,
        op_config: OpConfig,
        final_merge: bool,
        compare: fn(&L::Value, &L::Value) -> bool,
        record_results: Option<RecorderConfig<'a>>,
    ) -> Self {
        assert!(
            !runs.is_empty(),
            "At least one run configuration must be provided"
        );
        Self {
            name,
            runs,
            op_config,
            final_merge,
            compare,
            record_results,
        }
    }
}

// pub enum OpConfig<'a, O> {
//     Uniform(&'a [O]),
//     Probabilistic(&'a [(O, f64)]),
// }

// impl<'a, O> OpConfig<'a, O> {
//     pub fn random(ops: &'a [O]) -> Self {
//         assert!(!ops.is_empty(), "Operation list cannot be empty");
//         Self::Uniform(ops)
//     }

//     pub fn probabilistic(ops: &'a [(O, f64)]) -> Self {
//         assert!(!ops.is_empty(), "Operation list cannot be empty");
//         let total_prob: f64 = ops.iter().map(|(_, p)| p).sum();
//         assert!(
//             (total_prob - 1.0).abs() < f64::EPSILON,
//             "Total probability must sum to 1.0"
//         );
//         Self::Probabilistic(ops)
//     }

//     pub fn choose(&self, rng: &mut impl rand::Rng) -> O
//     where
//         O: Clone,
//     {
//         match self {
//             OpConfig::Uniform(ops) => ops.iter().choose(rng).unwrap().clone(),
//             OpConfig::Probabilistic(ops) => {
//                 let weights: Vec<f64> = ops.iter().map(|(_, p)| *p).collect();
//                 let dist = WeightedIndex::new(&weights).ok().unwrap(); // returns None if weights are invalid
//                 let index = dist.sample(rng);
//                 ops[index].0.clone()
//             }
//         }
//     }
// }

pub struct OpConfig {
    pub max_elements: usize,
}

pub trait OpGenerator: PureCRDT {
    fn generate(
        rng: &mut impl RngCore,
        config: &OpConfig,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Self;
}

pub trait OpGeneratorNested: IsLog {
    fn generate(&self, rng: &mut impl RngCore, config: &OpConfig) -> Self::Op;
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
}

impl RunConfig {
    pub fn new(
        churn_rate: f64,
        num_replicas: u8,
        num_operations: usize,
        reachability: Option<Vec<Vec<bool>>>,
        seed: Option<[u8; 32]>,
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
        }
    }
}

pub struct RecorderConfig<'a> {
    #[allow(dead_code)]
    file_path: &'a Path,
    #[allow(dead_code)]
    execution_graph: Option<ExecutionGraphConfig>,
}

impl<'a> RecorderConfig<'a> {
    pub fn new(file_path: &'a Path, execution_graph: Option<ExecutionGraphConfig>) -> Self {
        Self {
            file_path,
            execution_graph,
        }
    }
}

pub struct ExecutionGraphConfig {
    pub concurrency_score: bool,
}

impl ExecutionGraphConfig {
    pub fn new(concurrency_score: bool) -> Self {
        Self { concurrency_score }
    }
}
