use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};

use crate::{
    metrics::{DeliveryPhase, EffectSample, LogSizeSample, StabilitySample},
    observers::{
        footprint::{FootprintNode, LogFootprint},
        recorder::{EffectRecorder, LogSizeRecorder, StabilityRecorder, time},
    },
};

/// A wrapper log to record metrics about the inner log
pub struct LogObserver<L> {
    /// The inner log being observed
    inner: L,
    pub stability_recorder: StabilityRecorder,
    pub effect_recorder: EffectRecorder,
    pub log_size_recorder: LogSizeRecorder,
    /// Whether to call stabilize on the inner log
    with_stability: bool,
}

impl<L> Default for LogObserver<L>
where
    L: IsLog,
{
    fn default() -> Self {
        LogObserver {
            inner: L::default(),
            with_stability: true,
            stability_recorder: StabilityRecorder::default(),
            effect_recorder: EffectRecorder::default(),
            log_size_recorder: LogSizeRecorder::default(),
        }
    }
}

impl<L: IsLog + LogFootprint> IsLog for LogObserver<L> {
    type Command = L::Command;
    type Op = L::Op;
    type Rejection = L::Rejection;

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.inner.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        let (cnt, should_record) = self.effect_recorder.next_call();

        if should_record {
            let event_id = event.id().into();
            let (elapsed, _) = time(|| self.inner.effect(event, ctx));

            self.effect_recorder.time.push(
                EffectSample::new(cnt, elapsed, event_id).with_phase(self.effect_recorder.phase),
            );
        } else {
            self.inner.effect(event, ctx);
        }

        self.log_size_recorder.observe_effect(cnt);
        self.record_size(DeliveryPhase::Effect);
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        if self.stability_recorder.enabled {
            let after_effect_id = self.effect_recorder.count();
            debug_assert!(
                after_effect_id > 0,
                "stability cannot progress before the first effect"
            );
            self.stability_recorder.progress.push(
                StabilitySample::new(after_effect_id, frontier.clone())
                    .with_phase(self.stability_recorder.phase),
            );
        }

        if self.with_stability {
            self.inner.stabilize(frontier);
        }

        self.record_size(DeliveryPhase::Effect);
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        self.inner.prepare(command)
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.inner.redundant_by_parent(version, conservative)
    }

    fn is_default(&self) -> bool {
        self.inner.is_default()
    }
}

impl<L> LogObserver<L> {
    pub fn new(
        inner: L,
        stability_recorder: StabilityRecorder,
        effect_recorder: EffectRecorder,
        log_size_recorder: LogSizeRecorder,
        with_stability: bool,
    ) -> Self {
        LogObserver {
            inner,
            stability_recorder,
            effect_recorder,
            log_size_recorder,
            with_stability,
        }
    }

    pub fn inner(&self) -> &L {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut L {
        &mut self.inner
    }
}

impl<L> LogObserver<L>
where
    L: LogFootprint,
{
    /// Record the inclusive footprint tree after a log mutation call.
    fn record_size(&mut self, delivery_phase: DeliveryPhase) {
        if self.log_size_recorder.current_effect_selected {
            let footprint = self.inner.footprint();
            self.log_size_recorder.size.push(LogSizeSample {
                effect_id: self.effect_recorder.count(),
                delivery_phase,
                footprint,
                phase: self.log_size_recorder.phase,
            });
        }
    }
}

impl<L> LogFootprint for LogObserver<L>
where
    L: LogFootprint,
{
    fn footprint(&self) -> FootprintNode {
        self.inner.footprint()
    }
}

impl<Q, L> EvalNested<Q> for LogObserver<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q> + LogFootprint,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.inner.execute_query(q)
    }
}
