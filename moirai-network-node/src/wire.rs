//! Static membership is agreed during the TCP handshake, never repeated per message.
use crate::transport::MAX_FRAME_BYTES;
use moirai_protocol::{
    broadcast::{
        batch::Batch,
        internalizer::{Interner, Resolver},
        message::{BatchMessage, EventMessage, SinceMessage},
        since::Since,
    },
    clock::version_vector::Version,
    event::{Event, id::EventId, lamport::Lamport},
    replica::{ReplicaId, ReplicaIdx},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, io, sync::Arc};

pub(crate) enum Incoming<P> {
    Event(EventMessage<P>),
    Batch(BatchMessage<P>),
    Pull(SinceMessage),
}

#[derive(Clone, Serialize, Deserialize)]
struct Clock {
    origin: u16,
    entries: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
struct Id {
    origin: u16,
    seq: usize,
}

#[derive(Serialize, Deserialize)]
struct WireEvent<P> {
    version: Clock,
    update: P,
}

#[derive(Serialize, Deserialize)]
enum Payload<P> {
    Event(WireEvent<P>),
    Batch {
        version: Clock,
        events: Vec<WireEvent<P>>,
    },
    Pull {
        version: Clock,
        except: Vec<Id>,
    },
}

pub(crate) struct Codec {
    resolver: Resolver,
    indices: BTreeMap<ReplicaId, u16>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn format() -> impl bincode::config::Config {
    // Varints keep small counters/IDs compact. None is a one-byte presence tag,
    // with no placeholder bytes for the absent value.
    bincode::config::standard().with_limit::<MAX_FRAME_BYTES>()
}

impl Codec {
    pub fn new(members: impl IntoIterator<Item = u16>) -> Self {
        let mut ids: Vec<_> = members.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        let mut interner = Interner::new();
        let indices = ids
            .into_iter()
            .enumerate()
            .map(|(slot, id)| {
                let id: ReplicaId = id.to_string().into();
                interner.intern(&id);
                (id, slot as u16)
            })
            .collect();
        Self {
            resolver: interner.resolver().clone(),
            indices,
        }
    }

    fn clock(&self, version: &Version) -> io::Result<Clock> {
        let mut entries = vec![0; self.indices.len()];
        if version.resolver().len() != entries.len() {
            return Err(invalid("replica membership differs from configuration"));
        }
        for (idx, seq) in version.iter() {
            let id = version
                .resolver()
                .resolve(idx)
                .ok_or_else(|| invalid("unknown clock index"))?;
            let slot = *self
                .indices
                .get(id)
                .ok_or_else(|| invalid("unknown member"))?;
            entries[usize::from(slot)] = seq;
        }
        let origin = *self
            .indices
            .get(version.origin_id())
            .ok_or_else(|| invalid("unknown origin"))?;
        Ok(Clock { origin, entries })
    }

    fn version(&self, clock: Clock) -> io::Result<Version> {
        if usize::from(clock.origin) >= self.indices.len()
            || clock.entries.len() != self.indices.len()
            || clock
                .entries
                .iter()
                .try_fold(0usize, |sum, seq| sum.checked_add(*seq))
                .is_none_or(|sum| sum == usize::MAX)
        {
            return Err(invalid("invalid vector clock"));
        }
        let mut version =
            Version::new(ReplicaIdx(usize::from(clock.origin)), self.resolver.clone());
        for (slot, seq) in clock.entries.into_iter().enumerate() {
            version.set_by_idx(ReplicaIdx(slot), seq);
        }
        Ok(version)
    }

    fn event<'a, P>(&self, event: &'a Event<P>) -> io::Result<WireEvent<&'a P>> {
        if event.id().disambiguator().is_some() {
            return Err(invalid(
                "only root broadcast events travel over the network",
            ));
        }
        Ok(WireEvent {
            version: self.clock(event.version())?,
            update: event.op(),
        })
    }

    fn encode<P: Serialize>(&self, payload: &Payload<P>) -> io::Result<Arc<[u8]>> {
        let bytes = bincode::serde::encode_to_vec(payload, format()).map_err(io::Error::other)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "replica message exceeds the frame limit",
            ));
        }
        Ok(bytes.into())
    }

    pub fn encode_event<P: Serialize>(&self, event: &Event<P>) -> io::Result<Arc<[u8]>> {
        self.encode(&Payload::Event(self.event(event)?))
    }

    pub fn encode_pull(&self, since: &SinceMessage) -> io::Result<Arc<[u8]>> {
        let except = since
            .since()
            .except()
            .iter()
            .map(|id| {
                Ok(Id {
                    origin: *self
                        .indices
                        .get(id.origin_id())
                        .ok_or_else(|| invalid("unknown event origin"))?,
                    seq: id.seq(),
                })
            })
            .collect::<io::Result<_>>()?;
        self.encode(&Payload::<()>::Pull {
            version: self.clock(since.since().version())?,
            except,
        })
    }

    pub fn encode_batch<P: Serialize>(
        &self,
        batch: &BatchMessage<P>,
    ) -> io::Result<Vec<Arc<[u8]>>> {
        let version = self.clock(batch.batch().version())?;
        let mut frames = Vec::new();
        self.split_batch(batch.batch().events(), &version, &mut frames)?;
        Ok(frames)
    }

    fn split_batch<P: Serialize>(
        &self,
        events: &[Event<P>],
        version: &Clock,
        frames: &mut Vec<Arc<[u8]>>,
    ) -> io::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let payload = Payload::Batch {
            version: version.clone(),
            events: events
                .iter()
                .map(|e| self.event(e))
                .collect::<io::Result<_>>()?,
        };
        match self.encode(&payload) {
            Ok(frame) => frames.push(frame),
            Err(_) if events.len() > 1 => {
                let (left, right) = events.split_at(events.len() / 2);
                self.split_batch(left, version, frames)?;
                self.split_batch(right, version, frames)?;
            }
            // A single event may fit without the batch's additional clock.
            Err(_) => frames.push(self.encode_event(&events[0])?),
        }
        Ok(())
    }

    fn decode_event<P>(&self, event: WireEvent<P>, local: u16) -> io::Result<Event<P>> {
        let version = self.version(event.version)?;
        if version.origin_seq() == 0 || version.origin_id().as_ref() == local.to_string() {
            return Err(invalid("invalid or local event origin"));
        }
        let id = EventId::from(&version);
        let lamport = Lamport::from(&version);
        Ok(Event::new(id, lamport, event.update, version))
    }

    pub fn decode<P: DeserializeOwned>(
        &self,
        bytes: &[u8],
        sender: u16,
        local: u16,
    ) -> io::Result<Incoming<P>> {
        if bytes.len() > MAX_FRAME_BYTES
            || sender == local
            || !self.indices.contains_key(&sender.to_string().into())
        {
            return Err(invalid("invalid sender or frame size"));
        }
        let (payload, used): (Payload<P>, _) =
            bincode::serde::decode_from_slice(bytes, format()).map_err(io::Error::other)?;
        if used != bytes.len() {
            return Err(invalid("trailing message bytes"));
        }
        let resolver = self.resolver.clone();
        let check_sender = |version: &Version| {
            if version.origin_id().as_ref() != sender.to_string() {
                Err(invalid("message origin differs from sender"))
            } else {
                Ok(())
            }
        };
        match payload {
            Payload::Event(event) => Ok(Incoming::Event(EventMessage::new(
                self.decode_event(event, local)?,
                resolver,
            ))),
            Payload::Batch { version, events } => {
                let version = self.version(version)?;
                check_sender(&version)?;
                let events = events
                    .into_iter()
                    .map(|e| self.decode_event(e, local))
                    .collect::<io::Result<Vec<_>>>()?;
                if events.iter().any(|e| {
                    e.version()
                        .iter()
                        .any(|(idx, seq)| seq > version.seq_by_idx(idx))
                }) {
                    return Err(invalid("event exceeds batch clock"));
                }
                Ok(Incoming::Batch(BatchMessage::new(
                    Batch::new(events, version),
                    resolver,
                )))
            }
            Payload::Pull { version, except } => {
                let version = self.version(version)?;
                check_sender(&version)?;
                #[allow(clippy::mutable_key_type)]
                let except = except
                    .into_iter()
                    .map(|id| {
                        if usize::from(id.origin) >= self.indices.len()
                            || id.seq == 0
                            || id.seq == usize::MAX
                        {
                            return Err(invalid("invalid exception event ID"));
                        }
                        Ok(EventId::new(
                            ReplicaIdx(usize::from(id.origin)),
                            id.seq,
                            resolver.clone(),
                        ))
                    })
                    .collect::<io::Result<_>>()?;
                Ok(Incoming::Pull(SinceMessage::new(
                    Since::new(version, except),
                    resolver,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moirai_protocol::{
        broadcast::tcsb::Tcsb,
        commitment::{
            commit_protocols::omega::support_op::SupportOp,
            examples::commit_set::{CommitSet, SetUpdate},
            wire_id::{StableEventId, StableReplicaId},
        },
        crdt::query::Contains,
        replica::{IsReplica, Replica},
        state::po_log::VecLog,
    };
    type Update = SetUpdate<String>;
    type TestReplica = Replica<VecLog<CommitSet<String>>, Tcsb<Update>>;

    fn replica(id: &str) -> TestReplica {
        Replica::bootstrap(id.into(), &["2", "10"])
    }

    #[test]
    fn compact_support_metadata_omits_the_absent_event_id() {
        let codec = Codec::new([2, 10]);
        let mut a = replica("2");
        let message = a.send(SetUpdate::Add("apple".into())).unwrap();
        let event = message.event();
        let plain = codec.encode_event(event).unwrap();
        let support = SupportOp::new(event.op().clone(), StableReplicaId(2), false, None);
        let committed = event.clone().unfold(support);
        let bytes = codec.encode_event(&committed).unwrap();
        // One byte each for the small vote, partial flag, and presence flag.
        assert_eq!(bytes.len(), plain.len() + 3);
        let some = committed.clone().unfold(SupportOp::new(
            event.op().clone(),
            StableReplicaId(2),
            true,
            Some(StableEventId::new(StableReplicaId(10), 7)),
        ));
        assert_eq!(codec.encode_event(&some).unwrap().len(), bytes.len() + 2);
        let Incoming::Event(decoded) = codec.decode::<SupportOp<Update>>(&bytes, 2, 10).unwrap()
        else {
            panic!()
        };
        assert!(decoded.event().op().last_partial.is_none());
        assert_eq!(decoded.event().op().vote, StableReplicaId(2));
        // No names, membership list, duplicate sequence, or Lamport field.
        assert!(plain.len() < 20);
    }

    #[test]
    fn canonical_wire_indices_handle_local_ordering_causality_and_duplicates() {
        let codec = Codec::new([10, 2]);
        let mut a = replica("2");
        let mut b = replica("10");
        let first = a.send(SetUpdate::Add("x".into())).unwrap();
        let second = a.send(SetUpdate::Remove("x".into())).unwrap();
        for message in [&second, &second, &first, &first] {
            let bytes = codec.encode_event(message.event()).unwrap();
            let Incoming::Event(decoded) = codec.decode::<Update>(&bytes, 2, 10).unwrap() else {
                panic!()
            };
            b.receive(decoded);
        }
        assert_eq!(b.delivered_count(), 2);
        assert!(!b.query(&Contains("x".to_string())));
    }

    #[test]
    fn large_pull_replies_split_into_independently_decodable_frames() {
        let codec = Codec::new([2, 10]);
        let mut a = replica("2");
        let mut b = replica("10");
        for index in 0..8 {
            a.send(SetUpdate::Add(format!("{index}{}", "x".repeat(200_000))))
                .unwrap();
        }
        let request = codec.encode_pull(&b.since()).unwrap();
        let Incoming::Pull(request) = codec.decode::<Update>(&request, 10, 2).unwrap() else {
            panic!()
        };
        let frames = codec.encode_batch(&a.pull(request)).unwrap();
        assert!(frames.len() >= 2);
        for frame in frames.into_iter().rev() {
            assert!(frame.len() <= MAX_FRAME_BYTES);
            match codec.decode::<Update>(&frame, 2, 10).unwrap() {
                Incoming::Batch(batch) => b.receive_batch(batch),
                Incoming::Event(event) => b.receive(event),
                Incoming::Pull(_) => panic!(),
            }
        }
        assert_eq!(b.delivered_count(), 8);
    }

    #[test]
    fn malformed_frames_and_self_origin_events_are_rejected() {
        let codec = Codec::new([2, 10]);
        let mut a = replica("2");
        let message = a.send(SetUpdate::NoOp).unwrap();
        let bytes = codec.encode_event(message.event()).unwrap();
        assert!(codec.decode::<Update>(&bytes, 10, 2).is_err());
        assert!(codec.decode::<Update>(&bytes, 99, 10).is_err());
        assert!(
            codec
                .decode::<Update>(&bytes[..bytes.len() - 1], 2, 10)
                .is_err()
        );
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(codec.decode::<Update>(&trailing, 2, 10).is_err());
        for version in [
            Clock {
                origin: 0,
                entries: vec![1],
            },
            Clock {
                origin: 2,
                entries: vec![1, 0],
            },
            Clock {
                origin: 0,
                entries: vec![usize::MAX, 1],
            },
        ] {
            let bytes = codec
                .encode(&Payload::Event(WireEvent {
                    version,
                    update: SetUpdate::<String>::NoOp,
                }))
                .unwrap();
            assert!(codec.decode::<Update>(&bytes, 2, 10).is_err());
        }
        let bytes = codec
            .encode(&Payload::<()>::Pull {
                version: Clock {
                    origin: 1,
                    entries: vec![0, 0],
                },
                except: vec![],
            })
            .unwrap();
        assert!(codec.decode::<Update>(&bytes, 2, 10).is_err());
    }
}
