//! What the library needs from a replica, and the application's hooks
//! (NW.31 design, sections 2 and 4). No Moirai type appears here.

use serde_json::Value;

use crate::LogId;

/// A payload the replica produces and only the replica reads.
pub type Opaque = Value;

/// A replica's counters, from which the library derives its etag, "no
/// history" and whether a sibling holds anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    pub delivered: u64,
    pub pending: u64,
    /// Outbox size.
    pub retained: u64,
}

/// One replica of one hosted log. Not `Send`: replicas live on the node's
/// loop thread, and a replica may hold an `Rc`.
pub trait ReplicaPort {
    fn replica_id(&self) -> &str;
    /// A local op, answered with the event to broadcast or the replica's refusal.
    fn submit(&mut self, op: Value) -> Result<Opaque, String>;
    /// One event or one batch from `from`.
    fn apply_remote(&mut self, from: &str, ops: Opaque) -> Result<(), String>;
    /// The body of a `SyncRequest`.
    fn frontier(&self) -> Opaque;
    /// The answer to a `SyncRequest`.
    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String>;
    /// The entries of `frontier` naming events this replica has not delivered,
    /// the highest sequence per replica id: empty when it covers `frontier`. A
    /// replica that cannot compare a frontier must say so, never answer that
    /// nothing is missing.
    fn undelivered(&self, _frontier: &Opaque) -> Result<Vec<(String, u64)>, String> {
        Err("this replica does not compare a frontier with what it has delivered".to_string())
    }
    fn progress(&self) -> Progress;
    fn members(&self) -> Vec<String>;
    /// A returning member is refused a snapshot.
    fn has_history_from(&self, peer: &str) -> bool;
    fn read_state(&self) -> Value;

    fn read_identified(&self) -> Value {
        self.read_state()
    }

    /// A state transfer for `requester`; `None` when the replica offers none.
    fn snapshot_for(&mut self, _requester: &str) -> Option<Result<Opaque, String>> {
        None
    }

    /// Submits in order and stops at the first refusal: the applied count,
    /// that refusal, and the events of the applied ops.
    fn submit_batch(&mut self, ops: Vec<Value>) -> (usize, Option<String>, Vec<Opaque>) {
        let mut events = Vec::new();
        for op in ops {
            match self.submit(op) {
                Ok(event) => events.push(event),
                Err(refusal) => return (events.len(), Some(refusal), events),
            }
        }
        (events.len(), None, events)
    }
}

/// Opens one replica per hosted log.
pub trait ReplicaFactory {
    type Replica: ReplicaPort;
    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Self::Replica;
}

/// The application's hooks, each with a default that lets everything through.
pub trait Extension<R: ReplicaPort> {
    /// Whether a local op on `log` may be submitted.
    fn admit_local(&mut self, _log: &LogId, _op: &Value) -> Result<(), String> {
        Ok(())
    }
}
