use std::{
    cmp::Ordering,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign},
};

use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Event<K, T, O>
where
    K: Hash + Eq + Clone + Debug,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug,
{
    pub vc: VectorClock<K, T>,
    pub op: O,
    pub origin: K,
}

impl<K, T, O> Event<K, T, O>
where
    K: Hash + Eq + Clone + Debug,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug,
{
    pub fn new(vc: VectorClock<K, T>, op: O, origin: K) -> Self {
        Self { vc, op, origin }
    }
}

pub trait OpRules<K, T>: Clone + Debug
where
    K: Hash + Eq + Clone + Debug,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
{
    type Value;
    fn obsolete(is_obsolete: &Event<K, T, Self>, other: &Event<K, T, Self>) -> bool; // Checks if the operation is obsolete.
    fn eval(unstable_events: &[Event<K, T, Self>], stable_events: &[Self]) -> Self::Value; // Evaluates the state of the CRDT.
}

pub type POLog<K, T, O> = Vec<Event<K, T, O>>;
pub type StableUnstable<K, T, O> = (Vec<Event<K, T, O>>, Vec<Event<K, T, O>>);

#[derive(Debug)]
pub struct Trcb<K, T, O>
where
    K: Hash + Eq + Clone + Debug,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules<K, T>,
{
    pub id: K,
    pub po_log: POLog<K, T, O>,
    pub state: Vec<O>,
    pub ltm: MatrixClock<K, T>, // Last Timestamp Matrix (LTM): each row j of the LTM is the version vector of the most recently delivered message from the node j
    pub lvv: VectorClock<K, T>, // Last Vector Version (LVV): latest known version vector of the node i
    pub peers: Vec<K>,
}

impl<K, T, O> Trcb<K, T, O>
where
    K: Hash + Eq + Clone + Debug,
    T: Add<T, Output = T> + AddAssign<T> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules<K, T>,
{
    pub fn new(id: K) -> Self {
        Self {
            po_log: vec![],
            state: vec![],
            ltm: MatrixClock::new(&[id.clone()]),
            lvv: VectorClock::new(id.clone()),
            id,
            peers: vec![],
        }
    }

    pub fn new_peer(&mut self, peer_id: &K) {
        self.peers.push(peer_id.clone());
        self.ltm
            .insert(peer_id.clone(), VectorClock::new(peer_id.clone()));
        self.lvv.increment(peer_id);
    }

    pub fn tc_bcast(&mut self, op: O) -> Event<K, T, O> {
        self.lvv.increment(&self.id);
        self.ltm.update(&self.id, &self.lvv);
        let event = Event::new(self.lvv.clone(), op, self.id.clone());
        self.tc_deliver(event.clone());
        event
    }

    pub fn tc_deliver(&mut self, event: Event<K, T, O>) {
        self.lvv.merge(&event.vc);
        self.ltm.update(&event.origin, &event.vc);

        self.effect(event);
        let partition = self.tc_stable();
        self.stable(partition);
    }

    fn tc_stable(&mut self) -> StableUnstable<K, T, O> {
        self.po_log.iter().cloned().partition(|e| {
            let ord = PartialOrd::partial_cmp(&e.vc, &self.ltm.min());
            matches!(ord, Some(Ordering::Less) | Some(Ordering::Equal))
        })
    }

    fn effect(&mut self, event: Event<K, T, O>) {
        // The state is updated by removing all previous events in the state that are made obsolete by the new event.
        self.state.retain(|e| {
            let mut bottom_vc = VectorClock::<K, T>::new(self.id.clone());
            self.peers.iter().for_each(|p| {
                bottom_vc.increment(p);
            });
            let old_event = Event::new(bottom_vc, e.clone(), self.id.clone());
            !O::obsolete(&old_event, &event)
        });

        // The PO-Log is updated by removing all previous events that are made obsolete by the new event.
        self.po_log
            .retain(|e: &Event<K, T, O>| !O::obsolete(e, &event));

        // If no previous event in the PO-Log makes the new event obsolete, then the new event is added to the PO-Log.
        if !self.po_log.iter().any(|e| O::obsolete(&event, e)) {
            self.po_log.push(event.clone());
        }

        let partition = self.tc_stable();
        self.stable(partition);
    }

    fn stable(&mut self, partition: StableUnstable<K, T, O>) {
        let (stable, unstable) = partition;
        self.state.extend(stable.iter().map(|e| e.op.clone()));
        self.po_log = unstable;
    }
}
