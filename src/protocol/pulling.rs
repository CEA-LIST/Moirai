use colored::Colorize;
use log::error;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
#[cfg(feature = "serde")]
use tsify::Tsify;

use super::{log::Log, tcsb::Tcsb};
use crate::{
    clocks::{
        clock::{Clock, Full},
        dot::Dot,
    },
    protocol::{
        event::Event,
        guard::{guard_against_duplicates, loose_guard_against_out_of_order},
    },
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
                let mut sorted = batch.events.clone();
                sorted.sort_by(|a, b| {
                    // TODO: partial_cmp is not safe
                    todo!("Implement a safe comparison for Event metadata");
                    // if let Some(order) = a.metadata.partial_cmp(&b.metadata) {
                    //     order
                    // } else {
                    //     a.metadata().origin().cmp(b.metadata().origin())
                    // }
                });
                for event in sorted {
                    if self.id != event.metadata().origin() {
                        self.force_deliver(event, batch.metadata.origin());
                    }
                }
                // for event in batch.events {
                //     if self.id != event.metadata.origin() {
                //         self.try_deliver(event);
                //     }
                // }
                self.ltm
                    .get_mut(batch.metadata.origin())
                    .unwrap()
                    .merge(&batch.metadata);
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

    fn force_deliver(&mut self, event: Event<L::Op>, batch_origin: &str) {
        // The local peer should not call this function for its own events
        assert_ne!(
            self.id,
            event.metadata().origin(),
            "Local peer {} should not be the origin {} of the event",
            self.id,
            event.metadata().origin()
        );
        // If from evicted peer, unknown peer, duplicated event, ignore it
        if !self
            .group_membership
            .installed_view()
            .data
            .members
            .contains(&event.metadata().origin().to_string())
        {
            error!(
                "[{}] - Event from an unknown peer {} detected with timestamp {}",
                self.id.blue().bold(),
                event.metadata().origin().blue(),
                format!("{}", event.metadata()).red()
            );
            return;
        }
        // If the event is from a previous view, ignore it
        if event.metadata().view_id() < self.view_id() {
            error!(
                "[{}] - Event from {} with a view id {} inferior to the current view id {}",
                self.id.blue().bold(),
                event.metadata().origin().blue(),
                format!("{}", event.metadata().view_id()).blue(),
                format!("{}", event.metadata()).red()
            );
            return;
        }
        if guard_against_duplicates(&self.ltm, event.metadata()) {
            error!(
                "[{}] - Duplicated event detected from {} with timestamp {}",
                self.id.blue().bold(),
                event.metadata().origin().red(),
                format!("{}", event.metadata()).red()
            );
            return;
        }
        if loose_guard_against_out_of_order(&self.ltm, event.metadata(), batch_origin) {
            error!(
                "[{}] - Out-of-order event from {} detected with timestamp {}. Operation: {}",
                self.id.blue().bold(),
                event.metadata().origin().blue(),
                format!("{}", event.metadata()).red(),
                format!("{:?}", event.op).green(),
            );
        }
        // Store the new event at the end of the causal buffer
        // TODO: Check that this is correct
        self.pending.push_back(event.clone());
        // self.pending.make_contiguous().sort_by(|a, b| {
        // TODO: partial_cmp is not safe
        // if let Some(order) = a.metadata.partial_cmp(&b.metadata) {
        //     order
        // } else {
        //     a.metadata().origin().cmp(b.metadata().origin())
        // }
        // });
        let mut still_pending = VecDeque::new();
        while let Some(event) = self.pending.pop_front() {
            // If the event is causally ready, and
            // it belongs to the current view...
            if !loose_guard_against_out_of_order(&self.ltm, event.metadata(), batch_origin)
                && event.metadata().view_id() == self.view_id()
            {
                // ...deliver it
                self.tc_deliver(event);
            } else {
                // ...otherwise, keep it in the buffer (including events from the next views)
                still_pending.push_back(event);
            }
        }
        self.pending = still_pending;
    }
}
