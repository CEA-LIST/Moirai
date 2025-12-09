use std::{fmt::Debug, hash::Hash};

use crate::{
    crdt::{
        flag::ew_flag::EWFlag,
        map::uw_map::{UWMap, UWMapLog},
    },
    protocol::{
        crdt::{
            eval::EvalNested,
            query::{QueryOperation, Read},
        },
        state::po_log::VecLog,
    },
    HashSet,
};

pub type EWFlagSet<K> = UWMapLog<K, VecLog<EWFlag>>;
pub struct Set<K>(UWMap<K, EWFlag>);

impl<K> Set<K>
where
    K: Clone + Hash + Debug + Eq,
{
    pub fn add(key: K) -> UWMap<K, EWFlag> {
        UWMap::Update(key, EWFlag::Enable)
    }

    pub fn remove(key: K) -> UWMap<K, EWFlag> {
        UWMap::Update(key, EWFlag::Disable)
    }
}

impl<K> EvalNested<Read<HashSet<K>>> for EWFlagSet<K>
where
    K: Clone + Debug + Hash + Eq + PartialEq,
{
    fn execute_query(
        &self,
        _q: Read<HashSet<K>>,
    ) -> <Read<HashSet<K>> as QueryOperation>::Response {
        let mut set = HashSet::default();
        for (k, v) in &self.children {
            let val = v.execute_query(Read::new());
            if val {
                set.insert(k.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crdt::test_util::twins_log,
        protocol::{crdt::query::Read, replica::IsReplica},
    };

    #[test]
    fn test_ewflag_set() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSet<&str>>();
        let event_a = replica_a.send(Set::<&str>::add("a")).unwrap();
        let event_b = replica_b.send(Set::<&str>::add("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let event_a = replica_a.send(Set::<&str>::remove("a")).unwrap();
        let event_b = replica_b.send(Set::<&str>::add("c")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(
            replica_a.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec!["b", "c"])
        );
        assert_eq!(
            replica_b.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec!["b", "c"])
        );
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_ewflag_set() {
        // init_tracing();

        use crate::fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer,
        };

        // One replica is inaccessible to every other replica
        let reachability = Some(vec![
            vec![true, true, true, true, true, true, true, false],
            vec![true, true, true, true, true, true, true, false],
            vec![true, true, true, true, true, true, true, false],
            vec![true, true, true, true, true, true, true, false],
            vec![true, true, true, true, true, true, true, false],
            vec![true, true, true, true, true, true, true, false],
            vec![true, true, true, true, true, true, true, false],
            vec![false, false, false, false, false, false, false, false],
        ]);

        let run = RunConfig::new(0.4, 8, 10_000, reachability, None, false);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<EWFlagSet<String>>::new("ew_flag_set", runs, true, |a, b| a == b, true);

        fuzzer::<EWFlagSet<String>>(config);
    }
}
