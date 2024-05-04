use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};

use super::{
    event::{Event, Message},
    metadata::Metadata,
    pure_crdt::PureCRDT,
    tcsb::{POLog, Status, Tcsb},
    utils::{Incrementable, Keyable},
};
use std::fmt::Debug;

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

    fn membership_handler(event: &MembershipEvent<K, C, O>, tcsb: &mut Tcsb<K, C, O>) {
        match &event.cmd {
            Membership::Join => {
                if tcsb.id == event.metadata.origin {
                    tcsb.status = Status::Connecting;
                } else {
                    // Fill the existing vector clocks with the new peer entry (initially set to 0)
                    // Create a new vector clock for the new peer (initially set to 0)
                    tcsb.ltm.add_key(event.metadata.origin.clone());
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
                if k == &tcsb.id {
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

    fn r(event: &MembershipEvent<K, C, O>, _: &POLog<K, C, O>) -> bool {
        matches!(event.cmd, Membership::Welcome(_))
    }

    fn r_zero(old_event: &Event<K, C, O>, new_event: &MembershipEvent<K, C, O>) -> bool {
        if let Membership::Evict(k) = &new_event.cmd {
            if let Event::OpEvent(_) = old_event {
                return old_event.metadata().origin == *k
                    && old_event.metadata().vc > new_event.metadata.vc;
            }
        }
        false
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
