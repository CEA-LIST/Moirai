use super::{membership::View, pure_crdt::PureCRDT, tcsb::Tcsb};
use crate::protocol::{event::Event, metadata::Metadata};
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
    pub events: Vec<Event<O>>,
    pub metadata: Metadata,
}

impl<O> Batch<O>
where
    O: PureCRDT,
{
    pub fn new(events: Vec<Event<O>>, metadata: Metadata) -> Self {
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
        if !self.group_membership.members().contains(&metadata.origin) {
            error!(
                "The origin {} of the metadata is not part of the group membership: {:?}",
                metadata.origin,
                self.group_membership.members()
            );
            // if self.removed_members.contains(&metadata.origin) {
            //     return Err(DeliveryError::EvictedPeer);
            // } else {
            return Err(DeliveryError::UnknownPeer);
            // }
        }

        let since = Metadata::new(metadata.clock.clone(), "");

        // TODO: Rather than just `since`, the requesting peer should precise if it has received other events in its pending buffer.
        let events: Vec<Event<O>> = self
            .state
            .unstable
            .iter()
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(o.as_ref().clone(), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(Batch::new(
            events,
            Metadata::new(self.my_clock().clone(), &self.id),
        ))
    }

    pub fn deliver_batch(&mut self, batch: Result<Batch<O>, DeliveryError>) {
        match batch {
            Ok(batch) => {
                for event in batch.events {
                    self.try_deliver(event);
                }
                // We may have delivered a `remove(self)` event.
                if self
                    .group_membership
                    .members()
                    .contains(&batch.metadata.origin)
                {
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
                        self.group_membership.install_view(View::init(&self.id));
                        // self.removed_members.clear();
                        // self.converging_members.clear();
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
