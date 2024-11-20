use crate::{
    clocks::vector_clock::VectorClock,
    crdt::duet::Duet,
    protocol::{event::Event, metadata::Metadata, tcsb::AnyOp},
};

use super::{pure_crdt::PureCRDT, tcsb::Tcsb};
use std::{collections::HashSet, fmt::Debug, ops::Bound};

pub struct StateTransfer;

impl<O> Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub fn package_state(&mut self) -> StateTransfer {
        todo!()
    }

    pub fn deliver_state(&mut self, _state: StateTransfer) {
        todo!()
    }

    pub fn events_since(
        &self,
        lsv: &VectorClock<String, usize>,
        since: &VectorClock<String, usize>,
    ) -> Vec<Event<AnyOp<O>>> {
        assert!(
            lsv <= since || lsv.partial_cmp(since).is_none(),
            "LSV should be inferior, equal or even concurrent to the since clock. LSV: {lsv}, Since clock: {since}",
        );
        let mut metadata_lsv = Metadata::new(lsv.clone(), "");
        let mut metadata_since = Metadata::new(since.clone(), "");

        if self.eval_group_membership() != HashSet::from_iter(metadata_lsv.clock.keys()) {
            self.fix_timestamp_inconsistencies_incoming_event(&mut metadata_lsv);
        }

        if self.eval_group_membership() != HashSet::from_iter(metadata_since.clock.keys()) {
            self.fix_timestamp_inconsistencies_incoming_event(&mut metadata_since);
        }

        assert_eq!(
            self.ltm_current_keys(),
            metadata_since.clock.keys(),
            "Since: {:?}",
            metadata_since.clock
        );
        assert_eq!(
            self.ltm_current_keys(),
            metadata_lsv.clock.keys(),
            "LSV: {:?}",
            metadata_lsv.clock
        );
        // If the LSV is strictly greater than the since vector clock, it means the peer needs a state transfer
        // However, it should not happen because every peer should wait that everyone gets the ops before stabilizing

        // TODO: Rather than just `since`, the requesting peer should precise if it has received other events in its pending buffer.
        let events: Vec<Event<AnyOp<O>>> = self
            .group_membership
            .unstable
            .range((Bound::Excluded(&metadata_lsv), Bound::Unbounded))
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > metadata_since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::First(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let domain_events: Vec<Event<AnyOp<O>>> = self
            .state
            .unstable
            .range((Bound::Excluded(&metadata_lsv), Bound::Unbounded))
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > metadata_since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::Second(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let events = [events, domain_events].concat();
        events
    }

    pub fn deliver_batch(&mut self, events: Vec<Event<AnyOp<O>>>) {
        for event in events {
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
    }
}
