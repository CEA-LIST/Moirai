use crate::{
    crdt::test_util::n_members,
    protocol::{log::Log, pulling::Since, tcsb::Tcsb},
};
use rand::prelude::IndexedRandom;
use std::fmt::Debug;

pub fn generate_event_graph<L: Log>(ops: &[L::Op], n_proc: usize, n_event: usize) -> Vec<Tcsb<L>>
where
    L::Value: PartialEq + Debug,
{
    let mut tcsbs = n_members::<L>(n_proc);

    for _ in 0..(n_event - n_event % n_proc / n_proc) {
        // For each event, we randomly choose a process and an operation
        for i in 0..n_proc {
            let mut rng = rand::rng();
            let nums: Vec<usize> = (1..100).collect();

            let num = nums.choose(&mut rng).unwrap();
            let procs = (0..n_proc).collect::<Vec<_>>();

            if *num > 75 {
                let mut j = procs.choose(&mut rng).unwrap();
                while tcsbs[*j].id == tcsbs[i].id {
                    j = procs.choose(&mut rng).unwrap();
                }

                let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[*j]));
                tcsbs[*j].deliver_batch(batch);
            } else if *num > 10 {
                let op = ops.choose(&mut rng).unwrap();
                let _ = tcsbs[i].tc_bcast(op.clone());
            }
        }
    }

    for i in 0..n_proc {
        for j in 0..n_proc {
            if i != j {
                let batch = tcsbs[i].events_since(&Since::new_from(&tcsbs[j]));
                tcsbs[j].deliver_batch(batch);
            }
        }
    }

    tcsbs
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::crdt::aw_set::AWSet;
    use crate::crdt::resettable_counter::Counter;
    use crate::protocol::event_graph::EventGraph;

    #[test_log::test]
    fn generate_aw_set() {
        let ops = vec![
            AWSet::Add("a"),
            AWSet::Add("b"),
            AWSet::Add("c"),
            AWSet::Add("d"),
            AWSet::Clear,
            AWSet::Remove("a"),
            AWSet::Remove("b"),
            AWSet::Remove("d"),
            AWSet::Remove("c"),
        ];
        let n_proc = 4;
        let n_event = 20;

        let tcsbs = generate_event_graph::<EventGraph<AWSet<&str>>>(&ops, n_proc, n_event);

        assert_eq!(tcsbs.len(), n_proc);

        let mut reference: HashSet<&str> = HashSet::new();
        let mut event_sum = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                reference = tcsb.eval();
                event_sum = tcsb.my_clock().sum();
            }
            println!("current {}, ref {}", tcsb.my_clock().sum(), event_sum);
            if tcsb.eval() != reference {
                println!("Replica {}: {:?}", tcsb.id, tcsb.eval());
                println!("Reference: {:?}", reference);
                println!("Replica {} stable state: {:?}", tcsb.id, tcsb.state.stable);
                println!(
                    "Replica {} unstsable state: {:?}",
                    tcsb.id, tcsb.state.unstable
                );
                println!(
                    "Reference {} stable state: {:?}",
                    tcsbs[0].id, tcsbs[0].state.stable
                );
                println!(
                    "Reference {} unstsable state: {:?}",
                    tcsbs[0].id, tcsbs[0].state.unstable
                );
                println!("Reference LTM: {}", tcsbs[0].ltm);
                println!("Reference LSV: {}", tcsbs[0].lsv);
                println!("Replica {} LTM: {}", tcsb.id, tcsb.ltm);
                println!("Replica {} LSV: {}", tcsb.id, tcsb.lsv);
                panic!();
            }
        }
    }

    #[test_log::test]
    fn generate_counter() {
        let ops = vec![Counter::Inc(1), Counter::Dec(1), Counter::Reset];
        let n_proc = 5;
        let n_event = 10_000;

        let tcsbs = generate_event_graph::<EventGraph<Counter<isize>>>(&ops, n_proc, n_event);

        assert_eq!(tcsbs.len(), n_proc);

        let mut eval: isize = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            if i == 0 {
                eval = tcsb.eval();
            }
            assert_eq!(tcsb.eval(), eval);
        }
    }
}
