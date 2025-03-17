// use super::event::Event;
// use super::log::Log;
// use super::pulling::Since;
// use super::pure_crdt::PureCRDT;
// use crate::clocks::clock::Clock;
// use crate::clocks::dependency_clock::DependencyClock;
// use log::info;
// #[cfg(feature = "serde")]
// use serde::{Deserialize, Serialize};
// use std::cmp::Ordering;
// use std::collections::btree_map::{Values, ValuesMut};
// use std::collections::BTreeMap;
// use std::fmt::{Debug, Display};
// use std::iter::Chain;
// use std::ops::Bound;
// use std::slice::{Iter, IterMut};

// /// # Causal DAG operation history
// ///
// /// A Partially Ordered Log (PO-Log), is a chronological record that
// /// preserves all executed operations alongside their respective timestamps.
// /// In actual implementations, the PO-Log can be split in two components:
// /// one that simply stores the set of stable operations and the other stores the timestamped operations.
// #[derive(Debug, Clone)]
// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// pub struct POLog<O> {
//     pub stable: Vec<O>,
//     pub unstable: BTreeMap<DependencyClock, O>,
// }

// impl<O> POLog<O>
// where
//     O: Clone + Debug,
// {
//     pub fn new() -> Self {
//         Self {
//             stable: vec![],
//             unstable: BTreeMap::new(),
//         }
//     }

//     pub fn new_event(&mut self, event: &Event<O>) {
//         if self.unstable.contains_key(&event.metadata) {
//             info!(
//                 "Event with metadata {:?} already present in the log: {:?}",
//                 event.metadata,
//                 self.unstable.get(&event.metadata).unwrap()
//             );
//         }
//         let is_key_present = self
//             .unstable
//             .insert(event.metadata.clone(), event.op.clone());
//         assert!(
//             is_key_present.is_none(),
//             "Key already present in the log with value {:?}",
//             self.unstable.get(&event.metadata).unwrap()
//         );
//     }

//     pub fn iter(&self) -> Chain<Iter<O>, Values<DependencyClock, O>> {
//         self.stable.iter().chain(self.unstable.values())
//     }

//     pub fn iter_mut(&mut self) -> Chain<IterMut<O>, ValuesMut<DependencyClock, O>> {
//         self.stable.iter_mut().chain(self.unstable.values_mut())
//     }
// }

// impl<O> Log for POLog<O>
// where
//     O: PureCRDT,
// {
//     type Op = O;
//     type Value = O::Value;

//     fn new_event(&mut self, event: &Event<Self::Op>) {
//         self.new_event(event);
//     }

//     fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
//         // Keep only the operations that are not made redundant by the new operation
//         self.stable.retain(|o| {
//             let old_event: Event<O> =
//                 Event::new(o.clone(), DependencyClock::bot(&event.metadata.view));
//             if is_r_0 {
//                 !(Self::Op::r_zero(&old_event, event))
//             } else {
//                 !(Self::Op::r_one(&old_event, event))
//             }
//         });
//         self.unstable.retain(|m, o| {
//             let old_event: Event<O> = Event::new(o.clone(), m.clone());
//             if is_r_0 {
//                 !(Self::Op::r_zero(&old_event, event))
//             } else {
//                 !(Self::Op::r_one(&old_event, event))
//             }
//         });
//     }

//     /// Metadata may be absent in the log
//     fn purge_stable_metadata(&mut self, metadata: &DependencyClock) {
//         if let Some(n) = self.unstable.get(metadata) {
//             self.stable.push(n.clone());
//             self.unstable.remove(metadata);
//         }
//     }

//     /// Returns a list of events that are in the past of the given metadata
//     fn collect_events(&self, upper_bound: &DependencyClock) -> Vec<Event<Self::Op>> {
//         let list = self
//             .unstable
//             .range((Bound::Unbounded, Bound::Included(upper_bound)))
//             .filter_map(|(m, o)| {
//                 if m.clock <= upper_bound.clock {
//                     Some(Event::new(o.clone(), m.clone()))
//                 } else {
//                     None
//                 }
//             })
//             .collect::<Vec<Event<Self::Op>>>();
//         list
//     }

//     fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
//         let boundary = Metadata::new(since.clock.clone(), "", since.view_id);
//         self.unstable
//             .iter()
//             .filter_map(|(m, o)| {
//                 // If the dot is greater than the one in the since vector clock, then we have not delivered the event
//                 if m.clock.get(&m.origin).unwrap() > boundary.clock.get(&m.origin).unwrap()
//                     && !since.exclude.contains(&m.dot())
//                     && m.view_id <= boundary.view_id
//                 {
//                     Some(Event::new(o.clone(), m.clone()))
//                 } else {
//                     None
//                 }
//             })
//             .collect()
//     }

//     fn any_r(&self, event: &Event<Self::Op>) -> bool {
//         for o in &self.stable {
//             let old_event = Event::new(o.clone(), DependencyClock::default());
//             if O::r(event, &old_event) {
//                 return true;
//             }
//         }
//         for (m, o) in self.unstable.iter() {
//             let old_event = Event::new(o.clone(), m.clone());
//             if O::r(event, &old_event) {
//                 return true;
//             }
//         }
//         false
//     }

//     /// conservative: keep concurrent operations
//     fn r_n(&mut self, metadata: &DependencyClock, conservative: bool) {
//         self.stable.clear();
//         self.unstable.retain(|m, _| {
//             if conservative {
//                 // Keep all operations that are concurrent or in the future of the metadata
//                 !matches!(
//                     m.clock.partial_cmp(&metadata.clock),
//                     Some(Ordering::Less) | Some(Ordering::Equal)
//                 )
//             } else {
//                 // Keep all operations that are in the future of the metadata
//                 !matches!(
//                     m.clock.partial_cmp(&metadata.clock),
//                     Some(Ordering::Greater)
//                 )
//             }
//         });
//     }

//     fn stabilize(&mut self, metadata: &DependencyClock) {
//         O::stabilize(metadata, self);
//     }

//     fn eval(&self) -> Self::Value {
//         let ops: Vec<O> = self.iter().cloned().collect();
//         O::eval(&ops)
//     }

//     fn is_empty(&self) -> bool {
//         self.stable.is_empty() && self.unstable.is_empty()
//     }

//     fn lowest_view_id(&self) -> usize {
//         self.unstable.keys().map(|m| m.view_id).min().unwrap_or(0)
//     }
// }

// impl<O> Default for POLog<O>
// where
//     O: Clone + Debug,
// {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl<O> Display for POLog<O>
// where
//     O: Debug + Display,
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "Stable: [")?;
//         for (i, op) in self.stable.iter().enumerate() {
//             if i == self.stable.len() - 1 {
//                 write!(f, "{}", op)?;
//             } else {
//                 write!(f, "{}, ", op)?;
//             }
//         }
//         write!(f, "]\nUnstable: [")?;
//         for (i, (m, op)) in self.unstable.iter().enumerate() {
//             if i == self.unstable.len() - 1 {
//                 write!(f, "{}: {}", m, op)?;
//             } else {
//                 write!(f, "{}: {}, ", m, op)?;
//             }
//         }
//         write!(f, "]")
//     }
// }
