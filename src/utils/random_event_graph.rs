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
        let n_proc = 2;
        let n_event = 40;

        let tcsbs = generate_event_graph::<EventGraph<AWSet<&str>>>(&ops, n_proc, n_event);

        assert_eq!(tcsbs.len(), n_proc);

        #[cfg(feature = "serde")]
        tcsbs[0]
            .tracer
            .serialize_to_file(std::path::Path::new("traces/random_event_graph.json"))
            .unwrap();

        let mut eval: HashSet<&str> = HashSet::new();
        for (i, tcsb) in tcsbs.iter().enumerate() {
            println!("tcsb {} : {}", tcsb.id, tcsb.tracer.trace.len());
            println!("tcsb {} - buffer size {}", tcsb.id, tcsb.pending.len());
            for event in tcsb.pending.iter() {
                println!("event: {}", event);
            }
            println!("tcsb {} - LTM {}", tcsb.id, tcsb.ltm);
            if i == 0 {
                eval = tcsb.eval();
            }
            assert_eq!(tcsb.eval(), eval);
        }
    }

    #[test_log::test]

    fn generate_counter() {
        let ops = vec![Counter::Inc(1), Counter::Dec(1), Counter::Reset];
        let n_proc = 4;
        let n_event = 50;

        let tcsbs = generate_event_graph::<EventGraph<Counter<isize>>>(&ops, n_proc, n_event);

        assert_eq!(tcsbs.len(), n_proc);

        #[cfg(feature = "serde")]
        tcsbs[0]
            .tracer
            .serialize_to_file(std::path::Path::new("traces/random_event_graph.json"))
            .unwrap();

        let mut eval: isize = 0;
        for (i, tcsb) in tcsbs.iter().enumerate() {
            println!("tcsb {} : {}", tcsb.id, tcsb.tracer.trace.len());
            println!("tcsb {} - buffer size {}", tcsb.id, tcsb.pending.len());
            println!("tcsb {} - LTM {}", tcsb.id, tcsb.ltm);
            if i == 0 {
                eval = tcsb.eval();
            }
            assert_eq!(tcsb.eval(), eval);
        }
    }
}
