use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};

use super::{
    event::{Event, Message},
    metadata::Metadata,
    pure_crdt::PureCRDT,
    tcsb::{POLog, Status, Tcsb},
    utils::{Incrementable, Keyable},
};
use std::{cmp::Ordering, fmt::Debug};

#[derive(Clone, Debug)]
pub struct MembershipEvent<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub cmd: Membership<K, C, O>,
    pub metadata: Metadata<K, C>,
}

impl<K, C, O> MembershipEvent<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub fn new(cmd: Membership<K, C, O>, metadata: Metadata<K, C>) -> Self {
        Self { cmd, metadata }
    }
}

#[derive(Clone, Debug)]
pub struct Welcome<K, C, O>
where
    K: Keyable + Debug + Clone,
    C: Incrementable<C> + Debug + Clone,
    O: PureCRDT + Clone + Debug,
{
    pub lsv: VectorClock<K, C>,
    pub ltm: MatrixClock<K, C>,
    pub state: POLog<K, C, O>,
}

impl<K, C, O> Welcome<K, C, O>
where
    K: Keyable + Debug + Clone,
    C: Incrementable<C> + Debug + Clone,
    O: PureCRDT + Clone + Debug,
{
    pub fn new(tcsb: &Tcsb<K, C, O>) -> Self {
        Self {
            lsv: tcsb.lsv.clone(),
            ltm: tcsb.ltm.clone(),
            state: tcsb.state.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Membership<K, C, O>
where
    K: Keyable + Debug + Clone,
    C: Incrementable<C> + Debug + Clone,
    O: PureCRDT + Clone + Debug,
{
    Join,
    Leave,
    Welcome(Welcome<K, C, O>),
    Evict(K),
}

impl<K, C, O> Membership<K, C, O>
where
    K: Keyable + Debug + Clone,
    C: Incrementable<C> + Debug + Clone,
    O: PureCRDT + Clone + Debug,
{
    pub fn effect(event: &MembershipEvent<K, C, O>, tcsb: &mut Tcsb<K, C, O>) {
        Self::membership_handler(event, tcsb);

        // Storing the new event and pruning redundant events
        if Self::r(event, &tcsb.state) {
            // The operation is redundant
            tcsb.state.1.retain(|metadata, message| {
                let old_event: Event<K, C, O> = Event::new(
                    message.clone(),
                    metadata.vc.clone(),
                    metadata.origin.clone(),
                );
                !(Self::r_zero(&old_event, event))
            });
        } else {
            // The operation is not redundant
            tcsb.state.1.retain(|metadata, message| {
                let old_event: Event<K, C, O> = Event::new(
                    message.clone(),
                    metadata.vc.clone(),
                    metadata.origin.clone(),
                );
                !(Self::r_one(&old_event, event))
            });
            tcsb.state.1.insert(
                event.metadata.clone(),
                Message::Membership(event.cmd.clone()),
            );
        }
    }

    /// Apply the effect of the membership event on the TCSB
    fn membership_handler(event: &MembershipEvent<K, C, O>, tcsb: &mut Tcsb<K, C, O>) {
        match &event.cmd {
            Membership::Join => {
                if tcsb.id == event.metadata.origin {
                    tcsb.status = Status::Connecting;
                } else {
                    // Fill the existing vector clocks with the new peer entry (initially set to 0)
                    // Create a new vector clock for the new peer (initially set to 0)
                    // -> will be updated just after in the deliver() function
                    tcsb.ltm.add_key(event.metadata.origin.clone());
                    tcsb.status = Status::Peer;
                }
            }
            Membership::Welcome(welcome) => {
                if tcsb.id != event.metadata.origin {
                    tcsb.lsv = welcome.lsv.clone();
                    tcsb.ltm = welcome.ltm.clone();
                    let ltm_origin = tcsb.ltm.get(&event.metadata.origin).unwrap().clone();
                    tcsb.my_vc_mut().merge(&ltm_origin);
                    tcsb.state = welcome.state.clone();
                }
            }
            Membership::Leave => {
                if event.metadata.origin == tcsb.id {
                    tcsb.status = Status::Disconnected;
                    let my_lamport_clock: C = tcsb.my_vc().get(&event.metadata.origin).unwrap();
                    tcsb.ltm = MatrixClock::from(
                        &[tcsb.id.clone()],
                        &[VectorClock::from(&[tcsb.id.clone()], &[my_lamport_clock])],
                    );
                }
            }
            Membership::Evict(k) => {
                if k == &tcsb.id && !Membership::r(event, &tcsb.state) {
                    tcsb.status = Status::Disconnected;
                    let my_lamport_clock: C = tcsb.my_vc().get(k).unwrap();
                    tcsb.ltm = MatrixClock::from(
                        &[tcsb.id.clone()],
                        &[VectorClock::from(&[tcsb.id.clone()], &[my_lamport_clock])],
                    );
                }
            }
        }
    }

    fn r(event: &MembershipEvent<K, C, O>, state: &POLog<K, C, O>) -> bool {
        match &event.cmd {
            // An Evict event is redundant if there is already an Evict event for the same key OR
            // if the event comes from a node that is going to be evicted OR
            // TODO: if the node which is going to be evicted has sent a Leave event
            Membership::Evict(k) => state.1.iter().any(|(metadata, message)| {
                if let Message::Membership(Membership::Evict(k2)) = message {
                    (k2 == k)
                        || (event.metadata.origin == *k2
                            // The event is redundant if the new event has a higher vector clock
                            && match PartialOrd::partial_cmp(&metadata.vc, &event.metadata.vc) {
                                Some(Ordering::Less) => true,
                                Some(Ordering::Equal) | None => match Ord::cmp(&metadata.wc, &event.metadata.wc) {
                                    Ordering::Less => true,
                                    Ordering::Equal => event.metadata.origin > metadata.origin,
                                    Ordering::Greater => false,
                                },
                                Some(Ordering::Greater) => false,
                            })
                } else {
                    false
                }
            }),
            // A Welcome event is always redundant, as it is only used to initialize the state
            // Moreover, it has a null vector clock
            Membership::Welcome(_) => true,
            _ => false,
        }
    }

    fn r_zero(old_event: &Event<K, C, O>, new_event: &MembershipEvent<K, C, O>) -> bool {
        match (&old_event, &new_event.cmd) {
            // An Evict(k) event is redundant if k is leaving
            (Event::MembershipEvent(membership), Membership::Leave) => {
                if let Membership::Evict(k) = &membership.cmd {
                    *k == new_event.metadata.origin
                        && new_event.metadata.vc > old_event.metadata().vc
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn r_one(old_event: &Event<K, C, O>, new_event: &MembershipEvent<K, C, O>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    /// Stable membership events are dropped from the PO-Log
    pub fn stable(metadata: &Metadata<K, C>, tcsb: &mut Tcsb<K, C, O>) {
        Self::stabilize(metadata, tcsb);
        if let Some(Message::Membership(_)) = tcsb.state.1.get(metadata) {
            tcsb.state.1.remove(metadata);
        }
    }

    pub fn stabilize(metadata: &Metadata<K, C>, tcsb: &mut Tcsb<K, C, O>) {
        match tcsb.state.1.get(metadata) {
            Some(Message::Membership(Membership::Join)) => {
                if tcsb.id == metadata.origin {
                    tcsb.status = Status::Peer;
                }
            }
            Some(Message::Membership(Membership::Leave)) => {
                // We need to wait for stabilization before removing the node from the LTM,
                // as we might receive simultaneous events from other nodes whose vector clocks
                // still include the departing node
                if tcsb.id != metadata.origin {
                    tcsb.ltm.remove_key(&metadata.origin);
                }
            }
            Some(Message::Membership(Membership::Evict(k))) => {
                if k != &tcsb.id {
                    tcsb.ltm.remove_key(k);
                }
            }
            _ => {}
        }
    }
}
