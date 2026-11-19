use moirai_protocol::broadcast::internalizer::{Interner, SharedInterner};
use moirai_protocol::broadcast::tcsb::{StabilitySnapshot, Tcsb};
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;

use crate::log_id::LogId;
use crate::message::{BatchMessage, EventMessage, SinceMessage};

/// A protocol replica with the identity used to dispatch its network frames.
#[derive(Debug)]
pub struct LogReplica<L: IsLog> {
    inner: Replica<L, Tcsb<L::Update>>,
    log_id: LogId,
    foreign_log_refusals: u64,
}

impl<L: IsLog> std::ops::Deref for LogReplica<L> {
    type Target = Replica<L, Tcsb<L::Update>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<L: IsLog> LogReplica<L> {
    pub fn bootstrap_with_log_id(id: String, members: &[&str], log_id: LogId) -> Self {
        Self::bootstrap_with_log_id_and_interner(id, members, log_id, Interner::new().into_shared())
    }

    pub fn bootstrap_with_log_id_and_interner(
        id: String,
        members: &[&str],
        log_id: LogId,
        interner: SharedInterner,
    ) -> Self {
        for member in std::iter::once(id.as_str()).chain(members.iter().copied()) {
            moirai_protocol::replica::validate_replica_id(member)
                .unwrap_or_else(|error| panic!("{error}"));
        }
        Self {
            inner: Replica::bootstrap_with_interner(id.into(), members, interner),
            log_id,
            foreign_log_refusals: 0,
        }
    }

    pub(crate) fn inner(&self) -> &Replica<L, Tcsb<L::Update>> {
        &self.inner
    }

    pub fn id(&self) -> &str {
        self.inner.id().as_ref()
    }

    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }

    pub fn stability(&self) -> StabilitySnapshot {
        self.inner.tcsb().stability()
    }

    pub fn foreign_log_refusals(&self) -> u64 {
        self.foreign_log_refusals
    }

    pub fn has_history_for(&self, id: &str) -> bool {
        self.inner.tcsb().has_history_for(id)
    }

    pub fn query<Q>(&self, query: &Q) -> Q::Response
    where
        Q: moirai_protocol::crdt::query::QueryOperation,
        L: moirai_protocol::crdt::eval::EvalNested<Q>,
    {
        self.inner.query(query)
    }

    pub fn send(&mut self, command: L::Command) -> Result<EventMessage<L::Update>, L::Rejection> {
        let message = self.inner.send(command)?;
        Ok(EventMessage::new(
            message.event().clone(),
            message.resolver().clone(),
            self.log_id.clone(),
        ))
    }

    pub fn receive(&mut self, message: EventMessage<L::Update>) {
        if message.log_id() != &self.log_id {
            self.foreign_log_refusals += 1;
            return;
        }
        self.inner
            .receive(moirai_protocol::broadcast::message::EventMessage::new(
                message.event().clone(),
                message.resolver().clone(),
            ));
    }

    pub fn receive_batch(&mut self, message: BatchMessage<L::Update>) {
        if message.log_id() != &self.log_id {
            self.foreign_log_refusals += 1;
            return;
        }
        let (batch, resolver) = message.into_parts();
        self.inner
            .receive_batch(moirai_protocol::broadcast::message::BatchMessage::new(
                batch, resolver,
            ));
    }

    pub fn since(&self) -> SinceMessage {
        let message = self.inner.since();
        SinceMessage::new(
            message.since().clone(),
            message.resolver().clone(),
            self.log_id.clone(),
        )
    }

    pub fn pull(&mut self, message: SinceMessage) -> BatchMessage<L::Update> {
        if message.log_id() != &self.log_id {
            self.foreign_log_refusals += 1;
            let since = self.inner.since();
            return BatchMessage::new(
                moirai_protocol::broadcast::batch::Batch::new(
                    Vec::new(),
                    since.since().version().clone(),
                ),
                since.resolver().clone(),
                self.log_id.clone(),
            );
        }
        let batch = self
            .inner
            .pull(moirai_protocol::broadcast::message::SinceMessage::new(
                message.since().clone(),
                message.resolver().clone(),
            ));
        BatchMessage::new(
            batch.batch().clone(),
            batch.resolver().clone(),
            self.log_id.clone(),
        )
    }

    pub fn adopt(
        &mut self,
        snapshot: moirai_protocol::broadcast::tcsb::StateSnapshot<L::Update>,
        state: L,
    ) {
        self.inner.adopt(snapshot, state);
    }
}
