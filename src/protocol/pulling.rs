use log::{debug, error};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
#[cfg(feature = "serde")]
use tsify::Tsify;

use super::{log::Log, tcsb::Tcsb};
use crate::{
    clocks::{
        clock::{Clock, Full},
        dot::Dot,
    },
    protocol::event::Event,
};

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Batch<O> {
    pub events: Vec<Event<O>>,
    pub metadata: Clock<Full>,
}

impl<O> Batch<O> {
    pub fn new(events: Vec<Event<O>>, metadata: Clock<Full>) -> Self {
        Self { events, metadata }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum DeliveryError {
    UnknownPeer,
    DuplicatedEvent,
    OutOfOrderEvent,
}

#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Since {
    pub clock: Clock<Full>,
    /// Dots to exclude from the pull request (already received but not delivered)
    pub exclude: Vec<Dot>,
}

impl Since {
    pub fn new(clock: Clock<Full>, exclude: Vec<Dot>) -> Self {
        Since { clock, exclude }
    }

    pub fn new_from(tcsb: &Tcsb<impl Log>) -> Self {
        Since {
            clock: tcsb.my_clock().clone(),
            exclude: tcsb
                .pending
                .iter()
                .map(|e| Dot::from(e.metadata()))
                .collect(),
        }
    }
}

impl<L> Tcsb<L>
where
    L: Log,
{
    pub fn events_since(&self, since: &Since) -> Result<Batch<L::Op>, DeliveryError> {
        if !self
            .group_members()
            .contains(&since.clock.origin().to_string())
        {
            error!(
                "The origin {} of the metadata is not part of the group membership: {:?}",
                since.clock.origin(),
                self.group_members()
            );
            return Err(DeliveryError::UnknownPeer);
        }

        if self.id == since.clock.origin() {
            error!(
                "Can't pull from itself. Origin: {}, ID: {}",
                since.clock.origin(),
                self.id
            );
            return Err(DeliveryError::UnknownPeer);
        }

        let events = self.state.collect_events_since(since);

        Ok(Batch::new(events, self.my_clock().clone()))
    }

    pub fn deliver_batch(&mut self, batch: Result<Batch<L::Op>, DeliveryError>) {
        match batch {
            Ok(batch) => {
                debug!(
                    "Delivering batch from {} with {} events.",
                    batch.metadata.origin(),
                    batch.events.len()
                );
                for event in batch.events {
                    if self.id != event.origin() {
                        self.try_deliver(event);
                    }
                }
                debug_assert!(self.ltm.is_valid());
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
