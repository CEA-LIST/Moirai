use std::collections::BTreeSet;

use moirai_protocol::{
    replica::{ReplicaId, ReplicaIdx},
    state::log::IsLog,
};
use rand::seq::SliceRandom;
use rand_chacha::ChaCha8Rng;

use crate::{
    config::{CallSampling, MetricsConfig, MetricsSampling, ReplicaSampling},
    observers::{
        observer_log::{CommitmentAccessor, LogObserver},
        recorder::{
            EffectRecorder, LogSizeRecorder, QueryRecorder, ReplicaSizeRecorder, ResponseRecorder,
            StabilityRecorder, sampling_rng,
        },
        replica_observer::ReplicaObserver,
    },
};

#[derive(Clone, Default)]
struct ReplicaMetricPlan {
    response_time: Option<CallSampling>,
    effect_time: Option<CallSampling>,
    query_time: Option<CallSampling>,
    log_size: Option<CallSampling>,
    replica_size: Option<CallSampling>,
    track_stability: bool,
}

/// Bootstrap a set of replicas with the given number of members.
pub fn bootstrap_n<L>(
    n: u8,
    metrics_config: &MetricsConfig,
    with_stability: bool,
    seed: [u8; 32],
    commitment_accessor: Option<CommitmentAccessor<L>>,
) -> Vec<ReplicaObserver<LogObserver<L>>>
where
    L: IsLog + crate::observers::footprint::LogFootprint,
{
    let metrics_configs = resolve_metrics_configs(metrics_config, n as usize, seed);
    let mut replicas = Vec::with_capacity(n as usize);
    let member_ids = (0..n).map(|j| ReplicaId(j.to_string())).collect::<Vec<_>>();
    let members = member_ids.iter().collect::<Vec<_>>();

    for (id, config) in member_ids.iter().cloned().zip(metrics_configs) {
        let replica = ReplicaObserver::<LogObserver<L>>::bootstrap(
            id,
            &members,
            (
                ResponseRecorder::new(config.response_time),
                QueryRecorder::new(config.query_time),
                ReplicaSizeRecorder::new(config.replica_size),
                EffectRecorder::new(config.effect_time),
                StabilityRecorder::new(config.track_stability),
                LogSizeRecorder::new(config.log_size),
            ),
            with_stability,
            commitment_accessor,
        );
        replicas.push(replica);
    }
    replicas
}

fn resolve_metrics_configs(
    metrics_config: &MetricsConfig,
    num_replicas: usize,
    seed: [u8; 32],
) -> Vec<ReplicaMetricPlan> {
    let mut configs = vec![ReplicaMetricPlan::default(); num_replicas];

    // Response time
    assign_metric(
        &mut configs,
        metrics_config.response_time.as_ref(),
        &mut sampling_rng(seed, 1),
        |config, sampling| config.response_time = Some(sampling),
    );
    // Effect time
    assign_metric(
        &mut configs,
        metrics_config.effect_time.as_ref(),
        &mut sampling_rng(seed, 2),
        |config, sampling| config.effect_time = Some(sampling),
    );
    // Query time
    assign_metric(
        &mut configs,
        metrics_config.query_time.as_ref(),
        &mut sampling_rng(seed, 3),
        |config, sampling| config.query_time = Some(sampling),
    );
    // Inclusive log footprint tree
    assign_metric(
        &mut configs,
        metrics_config.log_size.as_ref(),
        &mut sampling_rng(seed, 4),
        |config, sampling| config.log_size = Some(sampling),
    );
    // Whole replica, excluding observer-owned metrics
    assign_metric(
        &mut configs,
        metrics_config.replica_size.as_ref(),
        &mut sampling_rng(seed, 5),
        |config, sampling| config.replica_size = Some(sampling),
    );
    // Stability progress
    assign_replica_metric(
        &mut configs,
        metrics_config.track_stability.as_ref(),
        &mut sampling_rng(seed, 6),
        |config| config.track_stability = true,
    );

    configs
}

fn assign_metric(
    configs: &mut [ReplicaMetricPlan],
    sampling: Option<&MetricsSampling>,
    rng: &mut ChaCha8Rng,
    mut assign: impl FnMut(&mut ReplicaMetricPlan, CallSampling),
) {
    let Some(sampling) = sampling else {
        return;
    };

    for replica in select_replicas(&sampling.replicas, configs.len(), rng) {
        assign(&mut configs[replica.0], sampling.calls.clone());
    }
}

fn assign_replica_metric(
    configs: &mut [ReplicaMetricPlan],
    sampling: Option<&ReplicaSampling>,
    rng: &mut ChaCha8Rng,
    mut assign: impl FnMut(&mut ReplicaMetricPlan),
) {
    let Some(sampling) = sampling else {
        return;
    };

    for replica in select_replicas(sampling, configs.len(), rng) {
        assign(&mut configs[replica.0]);
    }
}

fn select_replicas(
    sampling: &ReplicaSampling,
    num_replicas: usize,
    rng: &mut ChaCha8Rng,
) -> BTreeSet<ReplicaIdx> {
    let selected_count = match sampling {
        ReplicaSampling::All => num_replicas,
        ReplicaSampling::Count(count) => {
            let count = count.get();
            assert!(
                count <= num_replicas,
                "Cannot observe {count} replicas when the run only has {num_replicas} replicas"
            );
            count
        }
        ReplicaSampling::Indices(indices) => {
            assert!(
                indices.iter().all(|&idx| idx < num_replicas),
                "Observed replica index is out of bounds"
            );
            let selected = indices
                .iter()
                .copied()
                .map(ReplicaIdx)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                selected.len(),
                indices.len(),
                "Observed replica indices must be unique"
            );
            return selected;
        }
    };

    let mut indices = (0..num_replicas).collect::<Vec<_>>();
    indices.shuffle(rng);
    indices
        .into_iter()
        .take(selected_count)
        .map(ReplicaIdx)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    #[test]
    fn selections_are_reproducible_and_not_prefixes() {
        let sampling = ReplicaSampling::Count(NonZeroUsize::new(3).unwrap());
        let a = select_replicas(&sampling, 20, &mut sampling_rng([7; 32], 1));
        let b = select_replicas(&sampling, 20, &mut sampling_rng([7; 32], 1));
        assert_eq!(a, b);
        assert_eq!(a.len(), 3);
        assert_ne!(a, [ReplicaIdx(0), ReplicaIdx(1), ReplicaIdx(2)].into());
    }
}
