//! NW.31k oracle, part (1) (vault spec/items/NW.31k.md): state transfer in
//! upstream's protocol. Three replicas write in rounds until the first round
//! is stable and pruned from every outbox; a fourth replica, bootstrapped
//! with only its own id, adopts one replica's snapshot sent as JSON; then
//! events from the three reach it and its own events reach them. Ground
//! truth: a sum of integers, written by hand. Run with
//! `--features serde,test_utils`.
#![cfg(all(feature = "serde", feature = "test_utils"))]

use std::convert::Infallible;

use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    data_type::{
        query::{QueryOperation, Read},
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, ReplicatedDataType,
        },
    },
    replica::{IsReplica, Replica, ReplicaId},
    state::{
        po_log::{GraphLog, VecLog},
        unstable_state::IsUnstableCore,
    },
};
use serde::{Deserialize, Serialize};

/// A grow-only sum: every update adds its value.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Sum;

impl ReplicatedDataType for Sum {
    type Update = i64;
    type Stable = Vec<i64>;
    type Rejection = Infallible;
}

impl<U> Precondition<U> for Sum {
    fn is_enabled(_: &Vec<i64>, _: &U, _: &i64) -> Result<(), Infallible> {
        Ok(())
    }
}
impl<U: IsUnstableCore<i64>> CausalCompaction<U> for Sum {}
impl<U> CausalStabilize<U> for Sum {}
impl<U> CausalReset<U> for Sum {}

impl<U: IsUnstableCore<i64>> Eval<U, Read<i64>> for Sum {
    fn execute_query(
        stable: &Vec<i64>,
        unstable: &U,
        _: &Read<i64>,
    ) -> <Read<i64> as QueryOperation>::Response {
        stable.iter().sum::<i64>() + unstable.iter().map(|t| *t.op()).sum::<i64>()
    }
}

macro_rules! late_joiner_adopts_a_snapshot {
    ($name:ident, $log:ty) => {
        #[test]
        fn $name() {
            type R = Replica<$log, Tcsb<i64>>;
            let read = |r: &R| r.query(&Read::<i64>::new());
            let ids = ["a", "b", "c"];
            let mut r: Vec<R> = ids
                .iter()
                .map(|id| R::bootstrap(ReplicaId::from(*id), &ids))
                .collect();

            // Two rounds, each message delivered to the two others before the
            // next round: the first round is then stable at every replica.
            for values in [[1, 2, 3], [10, 20, 30]] {
                let sent: Vec<_> = (0..3).map(|i| r[i].send(values[i]).unwrap()).collect();
                for (i, m) in sent.iter().enumerate() {
                    for (j, to) in r.iter_mut().enumerate() {
                        if i != j {
                            to.receive(m.clone());
                        }
                    }
                }
            }
            for (i, x) in r.iter().enumerate() {
                assert_eq!(read(x), 66, "{} before the join", ids[i]);
                assert!(
                    x.tcsb().causally_stable_events_count() >= 3,
                    "{}: the first round is not stable",
                    ids[i]
                );
                assert!(
                    x.tcsb().outbox().all(|e| e.id().seq() > 1),
                    "{}: the first round is still in the outbox",
                    ids[i]
                );
            }

            // a writes once more; b sees it, c does not yet.
            let a3 = r[0].send(100).unwrap();
            r[1].receive(a3.clone());

            // d, a fresh replica knowing only itself, adopts a's snapshot.
            let json = serde_json::to_string(&r[0].snapshot()).expect("the snapshot serializes");
            let mut d = R::bootstrap(ReplicaId::from("d"), &["d"]);
            d.adopt(serde_json::from_str(&json).expect("the snapshot deserializes"));
            assert_eq!(
                d.id(),
                &ReplicaId::from("d"),
                "the adopter keeps its own id"
            );
            assert_eq!(read(&d), 166, "d reads the donor's state");
            assert_eq!(read(&d), read(&r[0]), "d reads as a");

            // c writes, not having seen a's third event; it reaches a, b and d.
            let c3 = r[2].send(1000).unwrap();
            r[0].receive(c3.clone());
            r[1].receive(c3.clone());
            d.receive(c3);
            // a's third event reaches c, and d again (d holds it already).
            r[2].receive(a3.clone());
            d.receive(a3);
            assert_eq!(read(&d), 1166, "events from the three reach d once each");

            // d writes under its own id; it reaches the three.
            let d1 = d.send(10000).unwrap();
            assert_eq!(
                d1.event().id().origin_id(),
                &ReplicaId::from("d"),
                "d's event is sent under d's id"
            );
            for x in r.iter_mut() {
                x.receive(d1.clone());
            }
            for (i, x) in r.iter().enumerate() {
                assert_eq!(read(x), 11166, "{} after the join", ids[i]);
            }
            assert_eq!(read(&d), 11166, "d after the join");
        }
    };
}

late_joiner_adopts_a_snapshot!(nw31k_late_joiner_adopts_a_snapshot_vec_log, VecLog<Sum>);
late_joiner_adopts_a_snapshot!(nw31k_late_joiner_adopts_a_snapshot_graph_log, GraphLog<Sum>);
