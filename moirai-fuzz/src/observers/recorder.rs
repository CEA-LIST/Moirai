use std::time::{Duration, Instant};

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

use crate::{
    FootprintNode,
    config::CallSampling,
    metrics::{
        EffectSample, LogSizeSample, Phase, QuerySample, ReplicaSizeSample, ResponseSample,
        StabilitySample,
    },
};

fn should_record(sampling: &CallSampling, counter: usize) -> bool {
    match sampling {
        CallSampling::All => true,
        CallSampling::Every { interval } => counter % interval.get() == 0,
    }
}

/// Sampling has its own deterministic streams; enabling metrics must not change the workload.
pub(crate) fn sampling_rng(seed: [u8; 32], stream: u64) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::from_seed(seed);
    rng.set_stream(stream);
    rng
}

pub fn time<F, R>(f: F) -> (Duration, R)
where
    F: FnOnce() -> R,
{
    let start = Instant::now();
    let response = f();
    (start.elapsed(), response)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicaRecorders {
    pub delivered_event_count: usize,
    #[serde(skip_serializing_if = "ResponseRecorder::disabled", default)]
    pub response_recorder: ResponseRecorder,
    #[serde(skip_serializing_if = "QueryRecorder::disabled", default)]
    pub query_recorder: QueryRecorder,
    #[serde(skip_serializing_if = "ReplicaSizeRecorder::disabled", default)]
    pub replica_size_recorder: ReplicaSizeRecorder,
    #[serde(skip_serializing_if = "StabilityRecorder::disabled", default)]
    pub stability_recorder: StabilityRecorder,
    #[serde(skip_serializing_if = "EffectRecorder::disabled", default)]
    pub effect_recorder: EffectRecorder,
    #[serde(skip_serializing_if = "LogSizeRecorder::disabled", default)]
    pub log_size_recorder: LogSizeRecorder,
}

macro_rules! time_recorder {
    ($name:ident, $sample:ty) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct $name {
            count: usize,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub config: Option<CallSampling>,
            pub phase: Phase,
            pub time: Vec<$sample>,
        }

        impl $name {
            pub fn new(config: Option<CallSampling>) -> Self {
                Self {
                    config,
                    time: Vec::new(),
                    count: 0,
                    phase: Phase::Execution,
                }
            }

            pub fn inc(&mut self) -> usize {
                self.count += 1;
                self.count
            }

            pub fn count(&self) -> usize {
                self.count
            }

            pub fn next_call(&mut self) -> (usize, bool) {
                let count = self.inc();
                let selected = self
                    .config
                    .as_ref()
                    .is_some_and(|sampling| should_record(sampling, count));
                (count, selected)
            }

            fn disabled(&self) -> bool {
                self.config.is_none()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new(None)
            }
        }
    };
}

time_recorder!(ResponseRecorder, ResponseSample);
time_recorder!(QueryRecorder, QuerySample);
time_recorder!(EffectRecorder, EffectSample);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaSizeRecorder {
    count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<CallSampling>,
    pub phase: Phase,
    pub size: Vec<ReplicaSizeSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_size: Option<usize>,
}

impl ReplicaSizeRecorder {
    pub fn new(config: Option<CallSampling>) -> Self {
        Self {
            config,
            size: Vec::new(),
            final_size: None,
            count: 0,
            phase: Phase::Execution,
        }
    }

    pub fn next_call(&mut self) -> (usize, bool) {
        self.count += 1;
        let selected = self
            .config
            .as_ref()
            .is_some_and(|sampling| should_record(sampling, self.count));
        (self.count, selected)
    }

    fn disabled(&self) -> bool {
        self.config.is_none()
    }
}

impl Default for ReplicaSizeRecorder {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StabilityRecorder {
    /// Is the recorder enabled? If not, it will not record any stability samples.
    pub enabled: bool,
    pub phase: Phase,
    #[serde(deserialize_with = "deserialize_progress")]
    pub progress: Vec<StabilitySample>,
}

// TODO: is this trash?
/// Frontiers in one recorder must share a resolver for clock comparisons after loading.
/// Re-intern all resolved wire origins into one canonical view, outside the measured run.
fn deserialize_progress<'de, D>(deserializer: D) -> Result<Vec<StabilitySample>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use std::collections::BTreeSet;

    use moirai_protocol::{
        broadcast::internalizer::Interner, clock::causal_frontier::CausalFrontier,
    };

    let samples = Vec::<StabilitySample>::deserialize(deserializer)?;
    let origins = samples
        .iter()
        .flat_map(|sample| {
            sample
                .frontier()
                .iter()
                .map(|(idx, _)| sample.frontier().resolver().resolve(idx).unwrap().clone())
        })
        .collect::<BTreeSet<_>>();
    let mut interner = Interner::new();
    for origin in origins {
        interner.intern(&origin);
    }
    Ok(samples
        .into_iter()
        .map(|sample| {
            let mut frontier = CausalFrontier::new(interner.resolver().clone());
            for (idx, seq) in sample.frontier().iter() {
                let origin = sample.frontier().resolver().resolve(idx).unwrap();
                frontier.set_by_idx(interner.get(origin).unwrap(), seq);
            }
            StabilitySample::new(sample.after_effect_id(), frontier).with_phase(sample.phase())
        })
        .collect())
}

impl StabilityRecorder {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Self::default()
        }
    }

    fn disabled(&self) -> bool {
        !self.enabled
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSizeRecorder {
    /// Eligible effects, not the number of effect/stability snapshots.
    count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<CallSampling>,
    pub phase: Phase,
    pub size: Vec<LogSizeSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_size: Option<FootprintNode>,
    #[serde(skip)]
    pub(crate) current_effect_selected: bool,
}

impl LogSizeRecorder {
    pub fn new(config: Option<CallSampling>) -> Self {
        Self {
            config,
            size: Vec::new(),
            final_size: None,
            count: 0,
            phase: Phase::Execution,
            current_effect_selected: false,
        }
    }

    /// Draw once per effect. A following stability snapshot reuses this decision and ID.
    pub(crate) fn observe_effect(&mut self, effect_id: usize) {
        self.count = effect_id;
        self.current_effect_selected = self
            .config
            .as_ref()
            .is_some_and(|sampling| should_record(sampling, effect_id));
    }

    fn disabled(&self) -> bool {
        self.config.is_none()
    }
}

impl Default for LogSizeRecorder {
    fn default() -> Self {
        Self::new(None)
    }
}
