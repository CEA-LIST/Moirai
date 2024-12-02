use super::{pure_crdt::PureCRDT, tcsb::Tcsb};
use crate::{
    crdt::duet::Duet,
    protocol::{event::Event, metadata::Metadata, tcsb::AnyOp},
};
use log::error;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Batch<O>
where
    O: PureCRDT,
{
    pub events: Vec<Event<AnyOp<O>>>,
    pub metadata: Metadata,
}

impl<O> Batch<O>
where
    O: PureCRDT,
{
    pub fn new(events: Vec<Event<AnyOp<O>>>, metadata: Metadata) -> Self {
        Self { events, metadata }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DeliveryError {
    UnknownPeer,
    DuplicatedEvent,
    OutOfOrderEvent,
    EvictedPeer,
}

impl<O> Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub fn events_since(&self, metadata: &Metadata) -> Result<Batch<O>, DeliveryError> {
        if !self.eval_group_membership().contains(&metadata.origin) {
            error!(
                "The origin {} of the metadata is not part of the group membership: {:?}",
                metadata.origin,
                self.eval_group_membership()
            );
            if self.removed_members.contains(&metadata.origin) {
                return Err(DeliveryError::EvictedPeer);
            } else {
                return Err(DeliveryError::UnknownPeer);
            }
        }

        let since = Metadata::new(metadata.clock.clone(), "");

        // TODO: Rather than just `since`, the requesting peer should precise if it has received other events in its pending buffer.
        let gms_events: Vec<Event<AnyOp<O>>> = self
            .group_membership
            .unstable
            .iter()
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::First(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let domain_events: Vec<Event<AnyOp<O>>> = self
            .state
            .unstable
            .iter()
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::Second(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let events = [gms_events, domain_events].concat();
        Ok(Batch::new(
            events,
            Metadata::new(self.my_clock().clone(), &self.id),
        ))
    }

    pub fn deliver_batch(&mut self, batch: Result<Batch<O>, DeliveryError>) {
        match batch {
            Ok(mut batch) => {
                for event in batch.events {
                    match event.op {
                        Duet::First(op) => {
                            let event = Event::new(op, event.metadata);
                            self.tc_deliver_membership(event);
                        }
                        Duet::Second(op) => {
                            let event = Event::new(op, event.metadata);
                            self.tc_deliver_op(event);
                        }
                    }
                }
                // We may have delivered a `remove(self)` event.
                if self
                    .eval_group_membership()
                    .contains(&batch.metadata.origin)
                {
                    self.fix_timestamp_inconsistencies_event(&mut batch.metadata);
                    assert_eq!(self.ltm.keys(), batch.metadata.clock.keys());
                    self.ltm
                        .update(&batch.metadata.origin, &batch.metadata.clock);
                    self.tc_stable();
                }
            }
            Err(e) => {
                match e {
                    DeliveryError::EvictedPeer => {
                        for key in self.ltm.keys() {
                            if key != self.id {
                                self.ltm.remove_key(&key);
                            }
                        }
                        // Re-init the group membership
                        self.group_membership = Self::create_group_membership(&self.id);
                        // self.removed_members.clear();
                        self.converging_members.clear();
                    }
                    DeliveryError::UnknownPeer => {}
                    _ => {
                        panic!("Unexpected error: {:?}", e);
                    }
                }
            }
        }
    }
}
