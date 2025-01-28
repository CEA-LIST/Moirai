use super::{log::Log, tcsb::Tcsb};
use crate::{
    clocks::vector_clock::VectorClock,
    protocol::{event::Event, metadata::Metadata},
};
use log::error;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Batch<O> {
    pub events: Vec<Event<O>>,
    pub metadata: Metadata,
}

impl<O> Batch<O> {
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
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Since {
    pub clock: VectorClock<String, usize>,
    pub origin: String,
    pub view_id: usize,
    /// Dots to exclude from the pull request (already received but not delivered)
    pub exclude: Vec<(String, usize)>,
}

impl Since {
    pub fn new(
        clock: VectorClock<String, usize>,
        origin: String,
        exclude: Vec<(String, usize)>,
        view_id: usize,
    ) -> Self {
        Since {
            clock,
            origin,
            exclude,
            view_id,
        }
    }

    pub fn new_from(tcsb: &Tcsb<impl Log>) -> Self {
        Since {
            clock: tcsb.my_clock().clone(),
            origin: tcsb.id.clone(),
            exclude: tcsb.pending.iter().map(|e| e.metadata.dot()).collect(),
            view_id: tcsb.view_id(),
        }
    }
}

impl<L> Tcsb<L>
where
    L: Log,
{
    pub fn events_since(&self, since: &Since) -> Result<Batch<L::Op>, DeliveryError> {
        if !self.group_members().contains(&since.origin) {
            error!(
                "The origin {} of the metadata is not part of the group membership: {:?}",
                since.origin,
                self.group_members()
            );
            return Err(DeliveryError::UnknownPeer);
        }

        let _boundary = Metadata::new(since.clock.clone(), "", since.view_id);

        let events = vec![];
        // let events: Vec<Event<L>> = self
        //     .state
        //     .unstable
        //     .iter()
        //     .filter_map(|(m, o)| {
        //         // If the dot is greater than the one in the since vector clock, then we have not delivered the event
        //         if m.clock.get(&m.origin).unwrap() > boundary.clock.get(&m.origin).unwrap()
        //             && !since.exclude.contains(&m.dot())
        //             && m.view_id <= boundary.view_id
        //         {
        //             Some(Event::new(o.as_ref().clone(), m.clone()))
        //         } else {
        //             None
        //         }
        //     })
        //     .collect::<Vec<_>>();
        Ok(Batch::new(
            events,
            Metadata::new(self.my_clock().clone(), &self.id, 0),
        ))
    }

    pub fn deliver_batch(&mut self, batch: Result<Batch<L::Op>, DeliveryError>) {
        match batch {
            Ok(batch) => {
                for event in batch.events {
                    self.try_deliver(event);
                }
                self.ltm
                    .get_mut(&batch.metadata.origin.clone())
                    .unwrap()
                    .merge(&batch.metadata.clock);
            }
            Err(e) => match e {
                DeliveryError::UnknownPeer => {
                    error!("Pull request failed: receiver peer does know us.");
                }
                _ => {
                    panic!("Unexpected error: {:?}", e);
                }
            },
        }
    }
}
