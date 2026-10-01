use std::num::NonZeroUsize;

use moirai_protocol::{
    commitment::commit_log::CommitmentLog, crdt::query::QueryOperation, replica::ReplicaId,
    state::log::IsLog,
};
use serde::{Deserialize, Serialize};

use crate::utils::seed_hex;

/// Connects the generic fuzzer to a log-specific leader oracle.
pub struct OracleDriver<L>
where
    L: IsLog,
{
    set_leader: fn(&mut L, &ReplicaId),
}

impl<L> OracleDriver<L>
where
    L: IsLog,
{
    pub(crate) fn set_leader(&self, log: &mut L, leader: &ReplicaId) {
        (self.set_leader)(log, leader);
    }
}

/// Query predicate used to check if the replicas converge after all operations are issued.
/// Given a query and a comparison function, the predicate will be evaluated on the query results of all replicas.
#[derive(Clone, Debug)]
pub struct Predicate<Q>
where
    Q: QueryOperation,
{
    pub query: Q,
    pub cmp: fn(&Q::Response, &Q::Response) -> bool,
}

impl<Q> Predicate<Q>
where
    Q: QueryOperation,
{
    pub fn new(query: Q, cmp: fn(&Q::Response, &Q::Response) -> bool) -> Self {
        Self { query, cmp }
    }
}

/// Churn rate defines the probability of a replica going offline/online during the simulation.
/// It's a two-state Markov chain with the following transition probabilities:
/// - p_disconnect: probability of a replica going offline (from online to offline)
/// - p_reconnect: probability of a replica going online (from offline to online)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChurnConfig {
    pub p_disconnect: Probability,
    pub p_reconnect: Probability,
}

impl ChurnConfig {
    pub fn new(p_disconnect: f64, p_reconnect: f64) -> Self {
        let p_disconnect = Probability::new(p_disconnect).unwrap();
        let p_reconnect = Probability::new(p_reconnect).unwrap();
        Self {
            p_disconnect,
            p_reconnect,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaveMode {
    Summary,
    Full,
}

/// Fuzzer configuration for a set of runs with different parameters.
pub struct FuzzerConfig<'a, L, Q>
where
    L: IsLog,
    Q: QueryOperation,
{
    /// Name of the simulation, used for logging
    pub name: &'a str,
    /// List of run configurations to be executed
    pub runs: Vec<RunConfig>,
    /// Whether to perform a final merge after all operations are issued
    pub final_merge: bool,
    /// Comparison function to check if the replicas converge
    pub predicate: Predicate<Q>,
    /// Whether to save the execution results to a JSON file in bench-results/
    pub save: Option<SaveMode>,
    /// Print tables and progress bars. Disable this when the caller owns presentation.
    pub display: bool,
    /// Optional adapter used to update a protocol oracle immediately before command preparation.
    pub oracle_driver: Option<OracleDriver<L>>,
}

impl<'a, L, Q> FuzzerConfig<'a, L, Q>
where
    L: IsLog,
    Q: QueryOperation,
{
    pub fn new(name: &'a str, runs: Vec<RunConfig>, predicate: Predicate<Q>) -> Self {
        assert!(
            !runs.is_empty(),
            "At least one run configuration must be provided"
        );
        Self {
            name,
            runs,
            predicate,
            final_merge: true,
            save: None,
            display: true,
            oracle_driver: None,
        }
    }

    pub fn with_oracle_driver(mut self, driver: OracleDriver<L>) -> Self {
        self.oracle_driver = Some(driver);
        self
    }

    pub fn without_final_merge(mut self) -> Self {
        self.final_merge = false;
        self
    }

    pub fn save(mut self, config: SaveMode) -> Self {
        self.save = Some(config);
        self
    }

    pub fn without_display(mut self) -> Self {
        self.display = false;
        self
    }
}

impl<'a, L, Q> FuzzerConfig<'a, CommitmentLog<L>, Q>
where
    L: IsLog,
    Q: QueryOperation,
{
    /// Drive the commitment log's Omega oracle from the simulated network state.
    pub fn with_omega_oracle(mut self) -> Self {
        self.oracle_driver = Some(OracleDriver {
            set_leader: |log, leader| log.oracle_mut().set_leader(leader.clone()),
        });
        self
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(try_from = "f64")]
pub struct Probability(f64);

impl TryFrom<f64> for Probability {
    type Error = String;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Probability {
    pub fn new(value: f64) -> Result<Self, String> {
        if value.is_finite() && (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(format!(
                "Probability value must be in the range [0.0, 1.0], got {}",
                value
            ))
        }
    }

    pub fn val(&self) -> f64 {
        self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CallSampling {
    /// Record every eligible call.
    All,
    /// Record one call every `interval` eligible calls.
    Every { interval: NonZeroUsize },
}

impl CallSampling {
    pub fn is_exhaustive(&self) -> bool {
        match self {
            Self::All => true,
            Self::Every { interval } => interval.get() == 1,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicaSampling {
    /// Observe every replica.
    All,
    /// Select exactly this many replicas uniformly without replacement.
    Count(NonZeroUsize),
    /// Explicit replica indices, for targeted experiments (e.g. replica 0).
    Indices(Vec<usize>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsSampling {
    pub calls: CallSampling,
    pub replicas: ReplicaSampling,
}

impl MetricsSampling {
    pub fn new(calls: CallSampling, replicas: ReplicaSampling) -> Self {
        Self { calls, replicas }
    }

    pub fn all() -> Self {
        Self {
            calls: CallSampling::All,
            replicas: ReplicaSampling::All,
        }
    }

    pub fn every_call(interval: NonZeroUsize) -> Self {
        Self {
            calls: CallSampling::Every { interval },
            replicas: ReplicaSampling::All,
        }
    }

    pub fn every_replica(count: NonZeroUsize) -> Self {
        Self {
            calls: CallSampling::All,
            replicas: ReplicaSampling::Count(count),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_time: Option<MetricsSampling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_time: Option<MetricsSampling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_time: Option<MetricsSampling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_size: Option<MetricsSampling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_size: Option<MetricsSampling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_stability: Option<ReplicaSampling>,
}

/// Configuration for a single run of the fuzzer, with specific parameters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunConfig {
    /// Churn rate defines the probability of a replica going offline/online
    pub churn: ChurnConfig,
    /// Number of replicas in the system
    pub num_replicas: u8,
    /// Total number of operations to be issued
    pub num_operations: usize,
    /// Optional reachability matrix to define which replicas can communicate with each other
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reachability: Option<Vec<Vec<bool>>>,
    /// Seed for the random number generator
    #[serde(default, skip_serializing_if = "Option::is_none", with = "seed_hex")]
    pub seed: Option<[u8; 32]>,
    /// Whether to generate an execution graph in GraphViz format
    pub execution_graph: bool,
    /// Whether to disable stability (`stabilize()` will never be called)
    pub disable_stability: bool,
    /// Metrics configuration for this run
    pub metrics: MetricsConfig,
}

impl RunConfig {
    pub fn new(
        churn: ChurnConfig,
        num_replicas: u8,
        num_operations: usize,
        reachability: Option<Vec<Vec<bool>>>,
        seed: Option<[u8; 32]>,
    ) -> Self {
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
            churn,
            num_replicas,
            num_operations,
            reachability,
            seed,
            execution_graph: false,
            disable_stability: false,
            metrics: MetricsConfig {
                response_time: None,
                effect_time: None,
                query_time: None,
                log_size: None,
                replica_size: None,
                track_stability: None,
            },
        }
    }

    pub fn with_execution_graph(mut self) -> Self {
        self.execution_graph = true;
        self
    }

    pub fn with_stability(mut self) -> Self {
        self.disable_stability = false;
        self
    }

    pub fn without_stability(mut self) -> Self {
        self.disable_stability = true;
        self
    }

    pub fn with_effect_time(mut self, sampling: MetricsSampling) -> Self {
        self.metrics.effect_time = Some(sampling);
        self
    }

    pub fn with_response_time(mut self, sampling: MetricsSampling) -> Self {
        self.metrics.response_time = Some(sampling);
        self
    }

    pub fn with_query_time(mut self, sampling: MetricsSampling) -> Self {
        self.metrics.query_time = Some(sampling);
        self
    }

    pub fn with_log_size(mut self, sampling: MetricsSampling) -> Self {
        self.metrics.log_size = Some(sampling);
        self
    }

    pub fn with_replica_size(mut self, sampling: MetricsSampling) -> Self {
        self.metrics.replica_size = Some(sampling);
        self
    }

    pub fn with_track_stability(mut self, sampling: ReplicaSampling) -> Self {
        self.metrics.track_stability = Some(sampling);
        self.disable_stability = false;
        self
    }
}
