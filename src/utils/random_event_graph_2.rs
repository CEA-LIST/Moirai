// use crate::{
//     crdt::test_util::n_members,
//     protocol::{log::Log, pulling::Since, tcsb::Tcsb},
// };
// use rand::{prelude::SliceRandom, seq::IndexedRandom, Rng};
// use std::fmt::Debug;

// /// Configuration for how to generate a random event graph.
// ///
// /// - `n_replicas`: how many TCSB replicas (members) to create.
// /// - `total_operations`: the total number of local (broadcast) operations to produce.
// /// - `ops`: the slice of allowed CRDT operations for each broadcast.
// /// - `sync_probability`: on each iteration, the chance that we'll do a random sync
// ///    (pulling from one replica into another) instead of a broadcast. Should be in [0.0, 1.0].
// /// - `final_sync`: if `true`, after generating all broadcasts, perform a full all‐pairs sync
// ///    so that every replica has everything.
// pub struct EventGraphConfig<'a, Op> {
//     pub n_replicas: usize,
//     pub total_operations: usize,
//     pub ops: &'a [Op],
//     /// Between 0.0 and 1.0: at each “step,” roll a random float in [0,1).
//     /// If it's < sync_probability, we pick two distinct replicas and have one pull from the other.
//     pub sync_probability: f64,
//     pub final_sync: bool,
// }

// impl<'a, Op> Default for EventGraphConfig<'a, Op> {
//     fn default() -> Self {
//         Self {
//             n_replicas: 4,
//             total_operations: 100,
//             ops: &[], // user should always override with a non‐empty slice
//             sync_probability: 0.1,
//             final_sync: true,
//         }
//     }
// }

// /// Generate a random event graph (an asynchronous history of CRDT updates + pulls) according to the given configuration.
// ///
// /// Returns a vector of `Tcsb<L>` replicas. After this returns, each replica's state.eval() should be the same (assuming the CRDT is convergent).
// ///
// /// # Type Parameters
// ///
// /// - `L: Log`: the underlying CRDT type must implement `Log`.
// ///
// /// # Parameters
// ///
// /// - `config`: an `EventGraphConfig` which contains:
// ///   - `n_replicas`: number of replicas to spawn.
// ///   - `total_operations`: how many local `tc_bcast` calls to generate (exactly).
// ///   - `ops`: allowed operations (slice of `L::Op`).
// ///   - `sync_probability`: probability at each iteration to do a random pairwise sync instead of a broadcast.
// ///   - `final_sync`: if true, at the very end do all‐pairs syncing so everyone converges.
// ///
// /// # Returns
// ///
// /// A `Vec<Tcsb<L>>` of length `n_replicas`. All replicas should be causally consistent and converge to the same CRDT state
// /// (assuming the CRDT’s invariants hold).
// pub fn generate_event_graph<L>(config: EventGraphConfig<'_, L::Op>) -> Vec<Tcsb<L>>
// where
//     L: Log,
//     L::Op: Clone + Debug,
//     L::Value: PartialEq + Debug,
// {
//     // 1. Create `n_replicas` fresh TCSB instances.
//     let mut tcsbs: Vec<Tcsb<L>> = n_members::<L>(config.n_replicas);

//     // 2. We'll keep generating “steps” until we've issued exactly `total_operations` local broadcasts.
//     //    On each step:
//     //      - With probability sync_probability => do a random pairwise sync (pull).
//     //      - Otherwise => pick a random replica, pick a random op, and broadcast it (tc_bcast).
//     //
//     //    Sync steps are unbounded (do not count toward `total_operations`).
//     let mut rng = rand::rng();
//     let mut local_ops_issued = 0;

//     while local_ops_issued < config.total_operations {
//         let roll: f64 = rng.random(); // uniform in [0,1)
//         if roll < config.sync_probability {
//             // Pick two distinct replicas i != j and have j pull from i
//             let i = rng.random_range(0..config.n_replicas);
//             let mut j = rng.random_range(0..config.n_replicas);
//             // Guarantee j != i
//             while j == i {
//                 j = rng.random_range(0..config.n_replicas);
//             }
//             // Replica j pulls (delivers) everything that i has produced so far
//             let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[j]));
//             tcsbs[j].deliver_batch(batch);
//         } else {
//             // Do a local broadcast: pick a random replica, pick a random op, and call tc_bcast.
//             let replica_idx = rng.random_range(0..config.n_replicas);
//             let op = config
//                 .ops
//                 .choose(&mut rng)
//                 .expect("`ops` slice cannot be empty")
//                 .clone();
//             let _event = tcsbs[replica_idx].tc_bcast(op);
//             local_ops_issued += 1;
//         }
//     }

//     // 3. If requested, do one final all‐pairs sync so everyone converges.
//     if config.final_sync {
//         for i in 0..config.n_replicas {
//             for j in 0..config.n_replicas {
//                 if i == j {
//                     continue;
//                 }
//                 let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[j]));
//                 tcsbs[j].deliver_batch(batch);
//             }
//         }
//     }

//     tcsbs
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::collections::HashSet;

//     use crate::crdt::aw_set::AWSet;
//     use crate::crdt::resettable_counter::Counter;
//     use crate::protocol::event_graph::EventGraph;

//     #[test_log::test]
//     fn generate_aw_set_convergence() {
//         let ops = vec![
//             AWSet::Add("a"),
//             AWSet::Add("b"),
//             AWSet::Add("c"),
//             AWSet::Add("d"),
//             AWSet::Clear,
//             AWSet::Remove("a"),
//             AWSet::Remove("b"),
//             AWSet::Remove("d"),
//             AWSet::Remove("c"),
//         ];

//         let config = EventGraphConfig {
//             n_replicas: 4,
//             total_operations: 40,
//             ops: &ops,
//             sync_probability: 0.2, // e.g. 20% chance to sync each step
//             final_sync: true,
//         };

//         let tcsbs = generate_event_graph::<EventGraph<AWSet<&str>>>(config);
//         assert_eq!(tcsbs.len(), 4);

//         // All replicas' eval() should match
//         let mut reference: HashSet<&str> = HashSet::new();
//         let mut event_sum = 0;
//         for (i, tcsb) in tcsbs.iter().enumerate() {
//             if i == 0 {
//                 reference = tcsb.eval();
//                 event_sum = tcsb.my_clock().sum();
//             }
//             println!("current {}, ref {}", tcsb.my_clock().sum(), event_sum);
//             assert_eq!(tcsb.eval(), reference);
//         }
//     }

//     #[test_log::test]
//     fn generate_counter_convergence() {
//         let ops = vec![Counter::Inc(1), Counter::Dec(1), Counter::Reset];

//         let config = EventGraphConfig {
//             n_replicas: 5,
//             total_operations: 100,
//             ops: &ops,
//             sync_probability: 0.1,
//             final_sync: true,
//         };

//         let tcsbs = generate_event_graph::<EventGraph<Counter<isize>>>(config);
//         assert_eq!(tcsbs.len(), 5);

//         // All replicas' eval() should match
//         let mut reference_val: isize = 0;
//         for (i, tcsb) in tcsbs.iter().enumerate() {
//             if i == 0 {
//                 reference_val = tcsb.eval();
//             }
//             assert_eq!(tcsb.eval(), reference_val);
//         }
//     }
// }
