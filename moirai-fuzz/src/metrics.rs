use std::time::Duration;

use moirai_protocol::{
    clock::causal_frontier::CausalFrontier,
    event::id::{EventId, ResolvedEventId},
};
use serde::{Deserialize, Serialize};

use crate::{observers::footprint::FootprintNode, utils::duration_nanos};

/// Phase of the runner when a measurement was taken. Combined statistics include all phases.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    #[default]
    Execution,
    /// The system is becoming quiescent, i.e., no more update events are being generated and everyone is pulling for the messages they have missed.
    Quiescence,
    /// The system has reached a stable state, i.e., all messages have been delivered and all replicas are in the same state.
    Convergence,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum CommCall {
    Send,
    Receive,
    ReceiveBatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseSample {
    call_id: usize,
    #[serde(rename = "duration_ns", with = "duration_nanos")]
    duration: Duration,
    event_ids: Vec<ResolvedEventId>,
    primitive: CommCall,
    phase: Phase,
}

impl ResponseSample {
    pub fn new(
        call_id: usize,
        duration: Duration,
        event_ids: Vec<EventId>,
        primitive: CommCall,
    ) -> Self {
        ResponseSample {
            call_id,
            duration,
            event_ids: event_ids.into_iter().map(ResolvedEventId::from).collect(),
            primitive,
            phase: Phase::Execution,
        }
    }

    pub fn from_send(call_id: usize, duration: Duration, event_id: EventId) -> Self {
        ResponseSample::new(call_id, duration, vec![event_id], CommCall::Send)
    }

    pub fn from_receive(call_id: usize, duration: Duration, event_id: EventId) -> Self {
        ResponseSample::new(call_id, duration, vec![event_id], CommCall::Receive)
    }

    pub fn from_batch(call_id: usize, duration: Duration, event_ids: Vec<EventId>) -> Self {
        ResponseSample::new(call_id, duration, event_ids, CommCall::ReceiveBatch)
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }

    pub fn call_id(&self) -> usize {
        self.call_id
    }

    pub fn event_ids(&self) -> &[ResolvedEventId] {
        &self.event_ids
    }

    pub fn primitive(&self) -> CommCall {
        self.primitive
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectSample {
    effect_id: usize,
    #[serde(rename = "duration_ns", with = "duration_nanos")]
    duration: Duration,
    event_id: ResolvedEventId,
    phase: Phase,
}

impl EffectSample {
    pub fn new(effect_id: usize, duration: Duration, event_id: ResolvedEventId) -> Self {
        EffectSample {
            effect_id,
            duration,
            event_id,
            phase: Phase::Execution,
        }
    }

    pub fn effect_id(&self) -> usize {
        self.effect_id
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }

    pub fn event_id(&self) -> &ResolvedEventId {
        &self.event_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StabilitySample {
    after_effect_id: usize,
    #[serde(with = "causal_frontier")]
    frontier: CausalFrontier,
    phase: Phase,
}

impl StabilitySample {
    pub fn new(after_effect_id: usize, frontier: CausalFrontier) -> Self {
        Self {
            after_effect_id,
            frontier,
            phase: Phase::Execution,
        }
    }

    pub fn after_effect_id(&self) -> usize {
        self.after_effect_id
    }

    pub fn frontier(&self) -> &CausalFrontier {
        &self.frontier
    }
}

/// A majority-backed commitment frontier, distinct from causal stability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitmentSample {
    after_effect_id: usize,
    #[serde(with = "causal_frontier")]
    frontier: CausalFrontier,
    phase: Phase,
}

impl CommitmentSample {
    pub fn new(after_effect_id: usize, frontier: CausalFrontier) -> Self {
        Self {
            after_effect_id,
            frontier,
            phase: Phase::Execution,
        }
    }

    pub fn after_effect_id(&self) -> usize {
        self.after_effect_id
    }

    pub fn frontier(&self) -> &CausalFrontier {
        &self.frontier
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySample {
    call_id: usize,
    #[serde(rename = "duration_ns", with = "duration_nanos")]
    duration: Duration,
    query_name: String,
    query_args: Vec<String>,
    phase: Phase,
}

impl QuerySample {
    pub fn new(
        call_id: usize,
        duration: Duration,
        query_name: &'static str,
        query_args: Vec<String>,
    ) -> Self {
        QuerySample {
            call_id,
            duration,
            query_name: query_name.to_owned(),
            query_args,
            phase: Phase::Execution,
        }
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaSizeSample {
    pub call_id: usize,
    pub bytes: usize,
    pub phase: Phase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPhase {
    Effect,
    Stability,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSizeSample {
    pub effect_id: usize,
    pub delivery_phase: DeliveryPhase,
    pub footprint: FootprintNode,
    pub phase: Phase,
}

macro_rules! phased_sample {
    ($($sample:ty),*) => { $(
        impl $sample {
            pub fn with_phase(mut self, phase: Phase) -> Self { self.phase = phase; self }
            pub fn phase(&self) -> Phase { self.phase }
        }
    )* };
}

phased_sample!(
    ResponseSample,
    EffectSample,
    QuerySample,
    StabilitySample,
    CommitmentSample
);

mod causal_frontier {
    use std::collections::BTreeMap;

    use moirai_protocol::{broadcast::internalizer::Interner, replica::ReplicaId};
    use serde::{Deserializer, Serializer};

    use super::*;

    pub fn serialize<S: Serializer>(
        frontier: &CausalFrontier,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        frontier
            .iter()
            .map(|(idx, seq)| (frontier.resolver().resolve(idx).unwrap().to_string(), seq))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<CausalFrontier, D::Error> {
        let entries = BTreeMap::<String, usize>::deserialize(deserializer)?;
        let mut interner = Interner::new();
        let entries = entries
            .into_iter()
            .map(|(id, seq)| (interner.intern(&ReplicaId(id)).0, seq))
            .collect::<Vec<_>>();
        let mut frontier = CausalFrontier::new(interner.resolver().clone());
        for (idx, seq) in entries {
            frontier.set_by_idx(idx, seq);
        }
        Ok(frontier)
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{broadcast::internalizer::Interner, replica::ReplicaId};
    use serde_json::json;

    use super::*;

    #[test]
    fn response_sample_json_uses_resolved_ids_across_local_mappings() {
        let origin = ReplicaId::from("replica-a");
        let mut sender = Interner::new();
        let (sender_idx, _) = sender.intern(&origin);
        let mut receiver = Interner::new();
        receiver.intern(&ReplicaId::from("replica-b"));
        let (receiver_idx, _) = receiver.intern(&origin);
        assert_ne!(sender_idx, receiver_idx);

        let sample = ResponseSample::from_batch(
            3,
            Duration::from_nanos(600),
            vec![
                EventId::new(sender_idx, 7, sender.resolver().clone()),
                EventId::new(receiver_idx, 7, receiver.resolver().clone()).with_disambiguator(0),
            ],
        );
        let value = serde_json::to_value(&sample).unwrap();
        assert_eq!(
            value,
            json!({
                "call_id": 3,
                "duration_ns": 600,
                "event_ids": [
                    { "origin_id": "replica-a", "seq": 7 },
                    { "origin_id": "replica-a", "seq": 7, "disambiguator": 0 }
                ],
                "primitive": "ReceiveBatch",
                "phase": "execution"
            })
        );

        let decoded: ResponseSample = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.event_ids(), sample.event_ids());
        assert_eq!(decoded.duration(), sample.duration());
        assert_eq!(decoded.call_id(), sample.call_id());
        assert!(matches!(decoded.primitive(), CommCall::ReceiveBatch));
        assert_eq!(decoded.phase(), sample.phase());
    }

    #[test]
    fn effect_sample_json_preserves_resolved_id() {
        let sample = EffectSample::new(
            4,
            Duration::from_nanos(900),
            ResolvedEventId::new_with_disambiguator(ReplicaId::from("replica-a"), 7, Some(42)),
        );
        let value = serde_json::to_value(&sample).unwrap();
        assert_eq!(
            value,
            json!({
                "effect_id": 4,
                "duration_ns": 900,
                "event_id": { "origin_id": "replica-a", "seq": 7, "disambiguator": 42 },
                "phase": "execution"
            })
        );

        let decoded: EffectSample = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.event_id(), sample.event_id());
        assert_eq!(decoded.duration(), sample.duration());
        assert_eq!(decoded.effect_id(), sample.effect_id());
        assert_eq!(decoded.phase(), sample.phase());
    }

    #[test]
    fn empty_batch_response_sample_json_round_trip() {
        let sample =
            ResponseSample::from_batch(5, Duration::ZERO, vec![]).with_phase(Phase::Quiescence);
        let value = serde_json::to_value(&sample).unwrap();
        assert_eq!(value["event_ids"], json!([]));
        assert_eq!(value["phase"], "quiescence");
        let decoded: ResponseSample = serde_json::from_value(value).unwrap();
        assert!(decoded.event_ids().is_empty());
        assert_eq!(decoded.duration(), Duration::ZERO);
        assert_eq!(decoded.phase(), Phase::Quiescence);
    }
}
