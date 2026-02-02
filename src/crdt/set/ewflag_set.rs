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

    pub fn clear() -> UWMap<K, EWFlag> {
        UWMap::Clear
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
    fn default_uw_map() {
        let (mut replica_a, mut replica_b) = twins_log::<EWFlagSet<&str>>();
        let event_a = replica_a.send(Set::<&str>::add("a")).unwrap();
        replica_b.receive(event_a);

        let event_b = replica_b.send(Set::<&str>::remove("a")).unwrap();
        replica_a.receive(event_b);

        assert_eq!(
            replica_a.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec![])
        );
        assert_eq!(
            replica_b.query(Read::<HashSet<&str>>::new()),
            HashSet::from_iter(vec![])
        );
    }

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
        use crate::fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let run_1 = RunConfig::new(0.7, 16, 10_000, None, None, false, true);
        let run_2 = RunConfig::new(0.7, 16, 30_000, None, None, false, true);
        let run_3 = RunConfig::new(0.7, 16, 100_000, None, None, false, true);
        let run_4 = RunConfig::new(0.7, 16, 300_000, None, None, false, true);
        let run_5 = RunConfig::new(0.7, 16, 600_000, None, None, false, true);
        let run_7 = RunConfig::new(0.7, 16, 1_000_000, None, None, false, true);
        let run_8 = RunConfig::new(0.7, 16, 1_300_000, None, None, false, true);
        let run_9 = RunConfig::new(0.7, 16, 1_600_000, None, None, false, true);
        let run_10 = RunConfig::new(0.7, 16, 2_000_000, None, None, false, true);
        let run_11 = RunConfig::new(0.7, 16, 3_000_000, None, None, false, true);
        let runs = vec![
            run_1, run_2, run_3, run_4, run_5, run_7, run_8, run_9, run_10, run_11,
        ];

        let config =
            FuzzerConfig::<EWFlagSet<usize>>::new("ew_flag_set", runs, true, |a, b| a == b, true);

        fuzzer::<EWFlagSet<usize>>(config);
    }
}
