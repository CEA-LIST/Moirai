use std::fmt::Debug;

use deepsize::DeepSizeOf;
use moirai_protocol::{
    broadcast::{
        internalizer::Interner,
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::{IsTcsb, Tcsb},
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    replica::{IsReplica, Replica, ReplicaId},
    state::log::IsLog,
};

use crate::{
    metrics::{Phase, QuerySample, ReplicaSizeSample, ResponseSample},
    observers::{
        footprint::LogFootprint,
        observer_log::LogObserver,
        recorder::{
            EffectRecorder, LogSizeRecorder, QueryRecorder, ReplicaRecorders, ReplicaSizeRecorder,
            ResponseRecorder, StabilityRecorder, time,
        },
    },
};

/// A wrapper around a replica that records metrics about its size and response times.
pub struct ReplicaObserver<L>
where
    L: IsLog,
{
    replica: Replica<L, Tcsb<L::Op>>,
    response_recorder: ResponseRecorder,
    query_recorder: QueryRecorder,
    replica_size_recorder: ReplicaSizeRecorder,
}

impl<L> IsReplica<LogObserver<L>> for ReplicaObserver<LogObserver<L>>
where
    L: IsLog + DeepSizeOf + LogFootprint,
    L::Op: DeepSizeOf,
{
    type Command = L::Command;
    type Payload = L::Op;

    fn id(&self) -> &ReplicaId {
        self.replica.id()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Payload {
        self.replica.prepare(cmd)
    }

    fn receive(&mut self, message: EventMessage<Self::Payload>) {
        let (cnt, should_record) = self.response_recorder.next_call();

        if should_record {
            let event_id = message.event().id().clone();
            let (d, _) = time(|| self.replica.receive(message));
            self.response_recorder.time.push(
                ResponseSample::from_receive(cnt, d, event_id)
                    .with_phase(self.response_recorder.phase),
            );
        } else {
            self.replica.receive(message);
        }

        self.record_replica_size();
    }

    fn receive_batch(&mut self, message: BatchMessage<Self::Payload>) {
        let (cnt, should_record) = self.response_recorder.next_call();

        if should_record {
            let event_ids = message
                .batch()
                .events()
                .iter()
                .map(|e| e.id().clone())
                .collect::<Vec<_>>();

            let (d, _) = time(|| self.replica.receive_batch(message));

            self.response_recorder.time.push(
                ResponseSample::from_batch(cnt, d, event_ids)
                    .with_phase(self.response_recorder.phase),
            );
        } else {
            self.replica.receive_batch(message);
        }

        self.record_replica_size();
    }

    fn since(&self) -> SinceMessage {
        self.replica.since()
    }

    fn send(
        &mut self,
        cmd: Self::Command,
    ) -> Result<EventMessage<Self::Payload>, <LogObserver<L> as IsLog>::Rejection> {
        let (cnt, should_record) = self.response_recorder.next_call();

        let msg = if should_record {
            let (d, msg) = time(|| self.replica.send(cmd));

            if let Ok(ref event_msg) = msg {
                let event_id = event_msg.event().id().clone();
                self.response_recorder.time.push(
                    ResponseSample::from_send(cnt, d, event_id)
                        .with_phase(self.response_recorder.phase),
                );
            }

            msg
        } else {
            self.replica.send(cmd)
        };

        self.record_replica_size();
        msg
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload> {
        self.replica.pull(since)
    }

    fn query<Q>(&self, q: &Q) -> Q::Response
    where
        Q: QueryOperation,
        LogObserver<L>: EvalNested<Q>,
    {
        self.replica.query(q)
    }
}

impl<L> ReplicaObserver<LogObserver<L>>
where
    L: IsLog + LogFootprint,
{
    pub fn bootstrap<M>(
        id: ReplicaId,
        members: &[M],
        response_recorder: ResponseRecorder,
        query_recorder: QueryRecorder,
        replica_size_recorder: ReplicaSizeRecorder,
        effect_recorder: EffectRecorder,
        stability_recorder: StabilityRecorder,
        log_size_recorder: LogSizeRecorder,
        with_stability: bool,
    ) -> Self
    where
        M: AsRef<str> + Debug,
    {
        assert!(
            members.iter().any(|member| member.as_ref() == id.as_ref()),
            "Bootstrap replica ID {} must be included in members list {:?}",
            id,
            members
        );
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id);

        for member in members {
            interner.intern(&ReplicaId::from(member.as_ref()));
        }

        ReplicaObserver {
            replica: Replica::new(
                id,
                Tcsb::new(idx, interner),
                LogObserver::new(
                    L::default(),
                    stability_recorder,
                    effect_recorder,
                    log_size_recorder,
                    with_stability,
                ),
            ),
            response_recorder,
            query_recorder,
            replica_size_recorder,
        }
    }

    pub fn observed_query<Q>(&mut self, query: &Q) -> Q::Response
    where
        Q: QueryOperation,
        L: EvalNested<Q>,
    {
        let (cnt, should_record) = self.query_recorder.next_call();

        if should_record {
            let name = Q::name();
            let args = query.args();
            let (d, response) = time(|| self.replica.query(query));

            self.query_recorder
                .time
                .push(QuerySample::new(cnt, d, name, args).with_phase(self.query_recorder.phase));

            return response;
        }

        self.replica.query(query)
    }

    pub fn state(&self) -> &L {
        self.replica.state().inner()
    }

    pub fn state_mut(&mut self) -> &mut L {
        self.replica.state_mut().inner_mut()
    }

    pub fn delivered_count(&self) -> usize {
        self.replica.delivered_count()
    }

    pub fn measurements(self) -> ReplicaRecorders {
        let ReplicaObserver {
            replica,
            response_recorder,
            query_recorder,
            replica_size_recorder,
        } = self;

        let delivered_event_count = replica.delivered_count();
        let observer_log = replica.into_state();

        ReplicaRecorders {
            delivered_event_count,
            stability_recorder: observer_log.stability_recorder,
            effect_recorder: observer_log.effect_recorder,
            log_size_recorder: observer_log.log_size_recorder,
            response_recorder,
            query_recorder,
            replica_size_recorder,
        }
    }

    pub fn set_phase(&mut self, phase: Phase) {
        self.response_recorder.phase = phase;
        self.query_recorder.phase = phase;
        self.replica_size_recorder.phase = phase;
        let log = self.replica.state_mut();
        log.effect_recorder.phase = phase;
        log.stability_recorder.phase = phase;
        log.log_size_recorder.phase = phase;
    }
}

impl<L> ReplicaObserver<LogObserver<L>>
where
    L: IsLog + DeepSizeOf + LogFootprint,
    L::Op: DeepSizeOf,
{
    pub fn record_terminal_sizes(&mut self) {
        if self.replica_size_recorder.config.is_some() {
            self.replica_size_recorder.final_size = Some(self.current_replica_size());
        }

        let final_log_size = self
            .replica
            .state()
            .log_size_recorder
            .config
            .as_ref()
            .map(|_| self.replica.state().inner().footprint());

        self.replica.state_mut().log_size_recorder.final_size = final_log_size;
    }

    fn record_replica_size(&mut self) {
        let (count, should_record) = self.replica_size_recorder.next_call();

        if should_record {
            let replica_size = self.current_replica_size();
            self.replica_size_recorder.size.push(ReplicaSizeSample {
                call_id: count,
                bytes: replica_size,
                phase: self.replica_size_recorder.phase,
            });
        }
    }

    fn current_replica_size(&self) -> usize {
        // One traversal context deduplicates Rc/Arc allocations shared by the TCSB and log.
        // Count the unwrapped replica's inline layout, never the observer or its recorders.
        let view = ReplicaFootprint {
            id: self.replica.id(),
            tcsb: self.replica.tcsb(),
            log: self.replica.state().inner(),
        };
        std::mem::size_of::<Replica<L, Tcsb<L::Op>>>() + view.deep_size_of()
            - std::mem::size_of_val(&view)
    }
}

/// Enter DeepSizeOf once to obtain its private traversal context, without owning
/// or counting any observer storage. References are followed explicitly.
struct ReplicaFootprint<'a, L: IsLog> {
    id: &'a ReplicaId,
    tcsb: &'a Tcsb<L::Op>,
    log: &'a L,
}

impl<L> DeepSizeOf for ReplicaFootprint<'_, L>
where
    L: IsLog + DeepSizeOf,
    L::Op: DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        DeepSizeOf::deep_size_of_children(self.id, context)
            + DeepSizeOf::deep_size_of_children(self.tcsb, context)
            + DeepSizeOf::deep_size_of_children(self.log, context)
    }
}
