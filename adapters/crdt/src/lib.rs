//! moirai-netstack's replica port over Moirai's own replicas (NW.31
//! design, step S4). Payloads are JSON carrying the log, the sender's replica
//! id list and events indexed through it, so the receiver translates indices
//! the way upstream's own messages do.

use std::{collections::HashMap, fmt::Debug};

use moirai_netstack::{Opaque, Progress, ReplicaFactory, ReplicaPort};
use moirai_protocol::data_type::query::Read;
use moirai_protocol::state::log::EvalNested;
use moirai_protocol::{
    broadcast::{
        batch::Batch,
        internalizer::{Interner, Resolver},
        message::{BatchMessage, SinceMessage},
        since::Since,
        tcsb::Tcsb,
    },
    clock::version_vector::Version,
    event::{Event, id::EventId, lamport::Lamport},
    replica::{IsReplica, Replica, ReplicaId, ReplicaIdx},
    state::log::IsLog,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub mod awset;

type Upstream<L> = Replica<L, Tcsb<<L as IsLog>::Update>>;

/// One event; indices refer to the payload's `ids`.
#[derive(Serialize, Deserialize)]
struct WireEvent<U> {
    origin: usize,
    seq: usize,
    version: Vec<usize>,
    op: U,
}

/// What `submit` (one event, sent by its origin) and `ops_since` (sent by
/// the replica that answered) produce.
#[derive(Serialize, Deserialize)]
struct Events<U> {
    log: String,
    ids: Vec<String>,
    sender: usize,
    events: Vec<WireEvent<U>>,
}

/// The requester's version and the events it holds pending.
#[derive(Serialize, Deserialize)]
struct Frontier {
    log: String,
    ids: Vec<String>,
    sender: usize,
    version: Vec<usize>,
    except: Vec<(usize, usize)>,
}

fn ids(resolver: &Resolver) -> Vec<String> {
    resolver.into_vec().into_iter().map(|id| id.0).collect()
}

fn entries(version: &Version) -> Vec<usize> {
    version.iter().map(|(_, seq)| seq).collect()
}

fn version(origin: usize, entries: &[usize], resolver: &Resolver) -> Version {
    let mut version = Version::new(ReplicaIdx(origin), resolver.clone());
    for (idx, seq) in entries.iter().enumerate() {
        version.set_by_idx(ReplicaIdx(idx), *seq);
    }
    version
}

fn to_json(payload: impl Serialize) -> Value {
    serde_json::to_value(payload).expect("a payload of JSON-serializable parts serializes")
}

/// Opens upstream replicas of log type `L`, read by `read`.
pub struct Factory<L: IsLog> {
    read: fn(&Upstream<L>) -> Value,
}

impl<L> ReplicaFactory for Factory<L>
where
    L: IsLog,
    L::Command: DeserializeOwned,
    L::Update: Serialize + DeserializeOwned + Debug,
    L::Rejection: Debug,
{
    type Replica = UpstreamReplica<L>;

    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> UpstreamReplica<L> {
        // Upstream's bootstrap asserts self among the members (design U6).
        let mut members = members.to_vec();
        if !members.iter().any(|m| m == self_id) {
            members.insert(0, self_id.to_string());
        }
        UpstreamReplica {
            replica: Replica::bootstrap(ReplicaId::from(self_id), &members),
            log: log.to_string(),
            members,
            read: self.read,
            peers: HashMap::new(),
        }
    }
}

/// One upstream replica behind the port.
pub struct UpstreamReplica<L: IsLog> {
    replica: Upstream<L>,
    log: String,
    members: Vec<String>,
    read: fn(&Upstream<L>) -> Value,
    /// The id list each sender's indices were last read through: upstream
    /// keeps one translation per sender that only grows, and panics when an
    /// incoming list is shorter.
    peers: HashMap<String, Vec<String>>,
}

impl<L> UpstreamReplica<L>
where
    L: IsLog,
    L::Update: Serialize,
{
    fn payload<'a>(
        &self,
        sender: ReplicaIdx,
        resolver: &Resolver,
        events: impl IntoIterator<Item = &'a Event<L::Update>>,
    ) -> Value
    where
        L::Update: 'a,
    {
        to_json(Events {
            log: self.log.clone(),
            ids: ids(resolver),
            sender: sender.0,
            events: events
                .into_iter()
                .map(|e| WireEvent {
                    origin: e.id().idx().0,
                    seq: e.id().seq(),
                    version: entries(e.version()),
                    op: e.op(),
                })
                .collect(),
        })
    }

    /// The log check, and the sender's id at `sender` in `ids`.
    fn sender(&self, log: &str, ids: &[String], sender: usize) -> Result<String, String> {
        if log != self.log {
            return Err(format!(
                "a payload of log {log} refused on log {}",
                self.log
            ));
        }
        ids.get(sender)
            .cloned()
            .ok_or_else(|| format!("a payload naming no sender: index {sender} of {ids:?}"))
    }

    /// The resolver `sender`'s indices read through: the payload's ids, or the
    /// longer list already held for `sender` when the payload's is a prefix of
    /// it (an older message).
    fn resolver(&self, sender: &str, ids: Vec<String>) -> Result<(Resolver, Vec<String>), String> {
        let ids = match self.peers.get(sender) {
            Some(known) if known.starts_with(&ids) => known.clone(),
            Some(known) if !ids.starts_with(known) => {
                return Err(format!(
                    "{sender}'s replica ids {ids:?} contradict its earlier {known:?}"
                ));
            }
            _ => ids,
        };
        let mut interner = Interner::new();
        for id in &ids {
            if !interner.intern(&ReplicaId::from(id.as_str())).1 {
                return Err(format!("replica id {id} listed twice in {ids:?}"));
            }
        }
        Ok((interner.resolver().clone(), ids))
    }
}

impl<L> ReplicaPort for UpstreamReplica<L>
where
    L: IsLog,
    L::Command: DeserializeOwned,
    L::Update: Serialize + DeserializeOwned + Debug,
    L::Rejection: Debug,
{
    fn replica_id(&self) -> &str {
        self.replica.id().as_ref()
    }

    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        let command: L::Command =
            serde_json::from_value(op).map_err(|e| format!("op refused: {e}"))?;
        let message = self
            .replica
            .send(command)
            .map_err(|e| format!("op refused: {e:?}"))?;
        let event = message.event();
        Ok(self.payload(event.id().idx(), message.resolver(), [event]))
    }

    fn apply_remote(&mut self, from: &str, ops: Opaque) -> Result<(), String> {
        let payload: Events<L::Update> = serde_json::from_value(ops)
            .map_err(|e| format!("a malformed payload from {from}: {e}"))?;
        let sender = self.sender(&payload.log, &payload.ids, payload.sender)?;
        if sender != from {
            return Err(format!(
                "a payload whose sender is {sender} refused: it came from {from}"
            ));
        }
        let (resolver, ids) = self.resolver(&sender, payload.ids)?;
        let mut events = Vec::with_capacity(payload.events.len());
        for e in payload.events {
            if e.origin >= ids.len()
                || e.version.len() > ids.len()
                || e.seq == 0
                || e.version.get(e.origin) != Some(&e.seq)
            {
                return Err(format!(
                    "a malformed event from {from}: {} {}",
                    e.origin, e.seq
                ));
            }
            // This replica holds its own events already, and upstream panics on them.
            if ids[e.origin] != self.replica_id() {
                let version = version(e.origin, &e.version, &resolver);
                let id = EventId::new(ReplicaIdx(e.origin), e.seq, resolver.clone());
                events.push(Event::new(id, Lamport::from(&version), e.op, version));
            }
        }
        let batch = Batch::new(
            events,
            Version::new(ReplicaIdx(payload.sender), resolver.clone()),
        );
        self.peers.insert(sender, ids);
        self.replica
            .receive_batch(BatchMessage::new(batch, resolver));
        Ok(())
    }

    fn frontier(&self) -> Opaque {
        let message = self.replica.since();
        let since = message.since();
        to_json(Frontier {
            log: self.log.clone(),
            ids: ids(message.resolver()),
            sender: since.version().origin_idx().0,
            version: entries(since.version()),
            except: since
                .except()
                .iter()
                .map(|e| (e.idx().0, e.seq()))
                .collect(),
        })
    }

    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
        let frontier: Frontier =
            serde_json::from_value(frontier).map_err(|e| format!("a malformed frontier: {e}"))?;
        let requester = self.sender(&frontier.log, &frontier.ids, frontier.sender)?;
        let (resolver, ids) = self.resolver(&requester, frontier.ids)?;
        if frontier.version.len() > ids.len() || frontier.except.iter().any(|e| e.0 >= ids.len()) {
            return Err(format!("a malformed frontier from {requester}"));
        }
        #[allow(clippy::mutable_key_type)] // upstream's own Since takes this set
        let except = frontier
            .except
            .iter()
            .map(|&(idx, seq)| EventId::new(ReplicaIdx(idx), seq, resolver.clone()))
            .collect();
        let since = Since::new(
            version(frontier.sender, &frontier.version, &resolver),
            except,
        );
        self.peers.insert(requester, ids);
        let answer = self.replica.pull(SinceMessage::new(since, resolver));
        let batch = answer.batch();
        Ok(self.payload(
            batch.version().origin_idx(),
            answer.resolver(),
            batch.events(),
        ))
    }

    fn progress(&self) -> Progress {
        let message = self.replica.since();
        let since = message.since();
        Progress {
            delivered: since.version().sum() as u64,
            pending: since.except().len() as u64,
            // shortcut: upstream shows its outbox only under test_utils; count it when the node needs retained.
            retained: 0,
        }
    }

    fn members(&self) -> Vec<String> {
        self.members.clone()
    }

    fn has_history_from(&self, peer: &str) -> bool {
        let message = self.replica.since();
        let version = message.since().version();
        version
            .resolver()
            .get(&ReplicaId::from(peer))
            .is_some_and(|idx| version.seq_by_idx(idx) > 0)
    }

    fn read_state(&self) -> Value {
        (self.read)(&self.replica)
    }
}

/// A factory for any Moirai log `L` whose state reads as `V`, as a generated
/// crate's node hands them over: no hand-written query.
pub fn factory<L, V>() -> Factory<L>
where
    L: IsLog + EvalNested<Read<V>>,
    L::Update: Debug,
    V: Serialize,
{
    Factory {
        read: |replica| {
            serde_json::to_value(replica.query(&Read::<V>::new()))
                .expect("a query of the log's state serializes")
        },
    }
}
