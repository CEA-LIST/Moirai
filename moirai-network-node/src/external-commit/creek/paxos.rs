//! Sequential, single-decree Paxos instances. Contains no operation payloads,
//! clocks, sockets, or shared replica state.
use super::VertexId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Ballot {
    pub round: u64,
    pub leader: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Accepted {
    pub ballot: Ballot,
    pub vertex: VertexId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Message {
    Prepare {
        slot: u64,
        ballot: Ballot,
    },
    Promise {
        slot: u64,
        ballot: Ballot,
        accepted: Option<Accepted>,
    },
    Accept {
        slot: u64,
        ballot: Ballot,
        vertex: VertexId,
    },
    Accepted {
        slot: u64,
        ballot: Ballot,
        vertex: VertexId,
    },
    Nack {
        slot: u64,
        ballot: Ballot,
        promised: Ballot,
    },
    Decide {
        slot: u64,
        vertex: VertexId,
    },
    Sync {
        next: u64,
    },
    Decisions {
        first: u64,
        vertices: Vec<VertexId>,
    },
}

/// ID-only input copied across the owner/worker handoff. The worker owns the
/// snapshot it uses while processing Paxos, so it holds no shared-state lock.
#[derive(Clone, Debug, Default)]
pub(crate) struct Snapshot {
    pub leader: u16,
    pub applied: u64,
    pub candidate: Option<VertexId>,
    pub delivered: BTreeMap<u16, u64>,
}

#[derive(Debug)]
enum Phase {
    Preparing(BTreeMap<u16, Option<Accepted>>),
    Prepared(Option<VertexId>),
    Accepting {
        vertex: VertexId,
        voters: BTreeSet<u16>,
    },
}

#[derive(Debug)]
struct Round {
    ballot: Ballot,
    phase: Phase,
    sent: Instant,
}

#[derive(Debug)]
pub(crate) struct Paxos {
    own: u16,
    members: Vec<u16>,
    slot: u64,
    observed_round: u64,
    promised: Option<Ballot>,
    accepted: Option<Accepted>,
    round: Option<Round>,
    pub decisions: BTreeMap<u64, VertexId>,
    pub outgoing: VecDeque<(u16, Message)>,
    pub learned: VecDeque<(u64, VertexId)>,
    snapshot: Snapshot,
    retry: Duration,
    sync_at: Instant,
    sync_peer: usize,
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

impl Paxos {
    pub fn new(own: u16, members: Vec<u16>, retry: Duration, now: Instant) -> Self {
        Self {
            own,
            members,
            slot: 1,
            observed_round: 0,
            promised: None,
            accepted: None,
            round: None,
            decisions: BTreeMap::new(),
            outgoing: VecDeque::new(),
            learned: VecDeque::new(),
            snapshot: Snapshot::default(),
            retry,
            sync_at: now,
            sync_peer: 0,
        }
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
    fn available(delivered: &BTreeMap<u16, u64>, vertex: VertexId) -> bool {
        delivered
            .get(&vertex.origin)
            .is_some_and(|seq| *seq >= vertex.seq)
    }
    fn valid_vertex(&self, vertex: VertexId) -> bool {
        vertex.seq > 0 && self.members.contains(&vertex.origin)
    }
    fn broadcast(&mut self, message: Message) {
        self.outgoing
            .extend(self.members.iter().map(|id| (*id, message.clone())));
    }

    pub fn tick(&mut self, snapshot: Snapshot, now: Instant) -> io::Result<()> {
        self.snapshot = snapshot;
        let leader = self.snapshot.leader;
        let applied = self.snapshot.applied;
        let candidate = self.snapshot.candidate;
        if !self.members.contains(&leader) {
            return Err(invalid("Omega selected an unknown member"));
        }
        if now >= self.sync_at {
            self.sync_at = now + self.retry;
            // One rotating peer per interval suffices to repair lost decisions;
            // the decision journal is kept even after application compaction.
            if self.members.len() > 1 {
                loop {
                    let peer = self.members[self.sync_peer];
                    self.sync_peer = (self.sync_peer + 1) % self.members.len();
                    if peer != self.own {
                        self.outgoing
                            .push_back((peer, Message::Sync { next: self.slot }));
                        break;
                    }
                }
            }
        }
        if leader != self.own {
            self.round = None;
            return Ok(());
        }
        // Apply the previous decision before selecting the next frontier head.
        if applied < self.slot - 1 {
            return Ok(());
        }
        if self.round.is_none() {
            if candidate.is_none() && self.accepted.is_none() {
                return Ok(());
            }
            self.observed_round = self
                .observed_round
                .checked_add(1)
                .ok_or_else(|| invalid("ballot exhausted"))?;
            let ballot = Ballot {
                round: self.observed_round,
                leader: self.own,
            };
            self.round = Some(Round {
                ballot,
                phase: Phase::Preparing(BTreeMap::new()),
                sent: now,
            });
            self.broadcast(Message::Prepare {
                slot: self.slot,
                ballot,
            });
        }
        let proposal = match &self.round.as_ref().unwrap().phase {
            Phase::Prepared(recovered) => recovered.or(candidate),
            _ => None,
        };
        if let Some(vertex) = proposal {
            if !self.valid_vertex(vertex) {
                return Err(invalid("invalid proposal ID"));
            }
            if Self::available(&self.snapshot.delivered, vertex) {
                let round = self.round.as_mut().unwrap();
                round.phase = Phase::Accepting {
                    vertex,
                    voters: BTreeSet::new(),
                };
                round.sent = now;
                let ballot = round.ballot;
                self.broadcast(Message::Accept {
                    slot: self.slot,
                    ballot,
                    vertex,
                });
            }
        }
        let round = self.round.as_mut().unwrap();
        if now.saturating_duration_since(round.sent) >= self.retry {
            round.sent = now;
            let message = match round.phase {
                Phase::Preparing(_) => Some(Message::Prepare {
                    slot: self.slot,
                    ballot: round.ballot,
                }),
                Phase::Accepting { vertex, .. } => Some(Message::Accept {
                    slot: self.slot,
                    ballot: round.ballot,
                    vertex,
                }),
                Phase::Prepared(_) => None,
            };
            if let Some(message) = message {
                self.broadcast(message);
            }
        }
        Ok(())
    }

    pub fn receive(&mut self, peer: u16, message: Message) -> io::Result<()> {
        if !self.members.contains(&peer) {
            return Err(invalid("unknown Paxos sender"));
        }
        match message {
            Message::Sync { next } => {
                if next == 0 {
                    return Err(invalid("invalid sync slot"));
                }
                let mut vertices = Vec::new();
                for (&slot, &vertex) in self.decisions.range(next..).take(32) {
                    if slot != next + vertices.len() as u64 {
                        break;
                    }
                    vertices.push(vertex);
                }
                if !vertices.is_empty() {
                    self.outgoing.push_back((
                        peer,
                        Message::Decisions {
                            first: next,
                            vertices,
                        },
                    ));
                }
            }
            Message::Decisions { first, vertices } => {
                if vertices.len() > 32 {
                    return Err(invalid("oversized decision batch"));
                }
                for (offset, vertex) in vertices.into_iter().enumerate() {
                    let slot = first
                        .checked_add(offset as u64)
                        .ok_or_else(|| invalid("slot exhausted"))?;
                    self.learn(slot, vertex)?;
                }
            }
            Message::Decide { slot, vertex } => self.learn(slot, vertex)?,
            Message::Prepare { slot, ballot } => {
                self.check_ballot(peer, ballot)?;
                if !self.check_slot(peer, slot)? {
                    return Ok(());
                }
                if self.promised.is_some_and(|promised| ballot < promised) {
                    self.outgoing.push_back((
                        peer,
                        Message::Nack {
                            slot,
                            ballot,
                            promised: self.promised.unwrap(),
                        },
                    ));
                } else {
                    self.promised = Some(ballot);
                    self.outgoing.push_back((
                        peer,
                        Message::Promise {
                            slot,
                            ballot,
                            accepted: self.accepted,
                        },
                    ));
                }
            }
            Message::Accept {
                slot,
                ballot,
                vertex,
            } => {
                self.check_ballot(peer, ballot)?;
                if !self.valid_vertex(vertex) {
                    return Err(invalid("invalid accepted vertex"));
                }
                if !self.check_slot(peer, slot)? {
                    return Ok(());
                }
                if self.promised.is_some_and(|promised| ballot < promised) {
                    self.outgoing.push_back((
                        peer,
                        Message::Nack {
                            slot,
                            ballot,
                            promised: self.promised.unwrap(),
                        },
                    ));
                } else {
                    // Promise even when the data has not arrived yet, but do not
                    // vote until TCSB has delivered this vertex and its past.
                    self.promised = Some(ballot);
                    if self
                        .accepted
                        .is_some_and(|a| a.ballot == ballot && a.vertex != vertex)
                    {
                        return Err(invalid("two values for one ballot"));
                    }
                    if !Self::available(&self.snapshot.delivered, vertex) {
                        return Ok(());
                    }
                    self.accepted = Some(Accepted { ballot, vertex });
                    self.outgoing.push_back((
                        peer,
                        Message::Accepted {
                            slot,
                            ballot,
                            vertex,
                        },
                    ));
                }
            }
            Message::Promise {
                slot,
                ballot,
                accepted,
            } => {
                let quorum = self.quorum();
                if let Some(a) = accepted
                    && (a.ballot > ballot || !self.valid_vertex(a.vertex))
                {
                    return Err(invalid("invalid promise"));
                }
                let Some(round) = self
                    .round
                    .as_mut()
                    .filter(|r| slot == self.slot && r.ballot == ballot)
                else {
                    return Ok(());
                };
                let Phase::Preparing(votes) = &mut round.phase else {
                    return Ok(());
                };
                votes.insert(peer, accepted);
                if votes.len() >= quorum {
                    // Paxos phase 1: an accepted value takes precedence over the
                    // leader's current candidate, even after a leadership change.
                    let highest = votes.values().flatten().max_by_key(|a| a.ballot).copied();
                    if let Some(highest) = highest
                        && votes
                            .values()
                            .flatten()
                            .any(|a| a.ballot == highest.ballot && a.vertex != highest.vertex)
                    {
                        return Err(invalid("conflicting accepted values"));
                    }
                    round.phase = Phase::Prepared(highest.map(|a| a.vertex));
                }
            }
            Message::Accepted {
                slot,
                ballot,
                vertex,
            } => {
                let quorum = self.quorum();
                let Some(round) = self
                    .round
                    .as_mut()
                    .filter(|r| slot == self.slot && r.ballot == ballot)
                else {
                    return Ok(());
                };
                let Phase::Accepting {
                    vertex: proposed,
                    voters,
                } = &mut round.phase
                else {
                    return Ok(());
                };
                if *proposed != vertex {
                    return Err(invalid("accepted reply changed the proposal"));
                }
                voters.insert(peer);
                if voters.len() >= quorum {
                    self.learn(slot, vertex)?;
                    self.broadcast(Message::Decide { slot, vertex });
                }
            }
            Message::Nack {
                slot,
                ballot,
                promised,
            } => {
                if !self.members.contains(&promised.leader) {
                    return Err(invalid("unknown ballot owner"));
                }
                self.observed_round = self.observed_round.max(promised.round);
                if self
                    .round
                    .as_ref()
                    .is_some_and(|r| slot == self.slot && r.ballot == ballot && promised > ballot)
                {
                    self.round = None;
                }
            }
        }
        Ok(())
    }

    fn check_ballot(&mut self, peer: u16, ballot: Ballot) -> io::Result<()> {
        if ballot.leader != peer || ballot.round == 0 {
            return Err(invalid("invalid proposer ballot"));
        }
        self.observed_round = self.observed_round.max(ballot.round);
        Ok(())
    }

    fn check_slot(&mut self, peer: u16, slot: u64) -> io::Result<bool> {
        if slot == 0 {
            return Err(invalid("invalid Paxos slot"));
        }
        if let Some(&vertex) = self.decisions.get(&slot) {
            self.outgoing
                .push_back((peer, Message::Decide { slot, vertex }));
            return Ok(false);
        }
        if slot != self.slot {
            self.outgoing
                .push_back((peer, Message::Sync { next: self.slot }));
            return Ok(false);
        }
        Ok(true)
    }

    fn learn(&mut self, slot: u64, vertex: VertexId) -> io::Result<()> {
        if slot == 0 || !self.valid_vertex(vertex) {
            return Err(invalid("invalid decision"));
        }
        if let Some(previous) = self.decisions.get(&slot) {
            if *previous != vertex {
                return Err(invalid("conflicting decisions for one slot"));
            }
            return Ok(());
        }
        self.decisions.insert(slot, vertex);
        self.learned.push_back((slot, vertex));
        let before = self.slot;
        while self.decisions.contains_key(&self.slot) {
            self.slot = self
                .slot
                .checked_add(1)
                .ok_or_else(|| invalid("slot exhausted"))?;
        }
        if self.slot != before {
            self.round = None;
            self.promised = None;
            self.accepted = None;
        }
        Ok(())
    }
}
