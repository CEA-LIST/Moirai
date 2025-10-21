#![cfg(feature = "crdt")]

// Tests for causal delivery in CRDTs.

use moirai::{
    crdt::{
        counter::resettable_counter::Counter,
        test_util::{triplet, twins},
    },
    protocol::{crdt::pure_crdt::Read, replica::IsReplica},
};

#[test]
fn causal_delivery_twins() {
    let (mut replica_a, mut replica_b) = twins::<Counter<i32>>();

    let event_a_1 = replica_a.send(Counter::Inc(1)).unwrap();
    let event_a_2 = replica_a.send(Counter::Inc(1)).unwrap();

    replica_b.receive(event_a_2);
    replica_b.receive(event_a_1);

    assert_eq!(replica_b.query(Read::new()), 2);
    assert_eq!(replica_a.query(Read::new()), 2);

    let event_b_1 = replica_b.send(Counter::Inc(1)).unwrap();
    let event_b_2 = replica_b.send(Counter::Inc(1)).unwrap();
    let event_b_3 = replica_b.send(Counter::Inc(1)).unwrap();
    let event_b_4 = replica_b.send(Counter::Inc(1)).unwrap();

    replica_a.receive(event_b_3);
    replica_a.receive(event_b_1);
    replica_a.receive(event_b_4);
    replica_a.receive(event_b_2);

    assert_eq!(replica_a.query(Read::new()), 6);
    assert_eq!(replica_b.query(Read::new()), 6);
}

#[test]
fn causal_delivery_triplet() {
    let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Counter<i32>>();

    let event_b = replica_b.send(Counter::Inc(2)).unwrap();

    replica_a.receive(event_b.clone());
    let event_a = replica_a.send(Counter::Dec(7)).unwrap();

    replica_b.receive(event_a.clone());
    replica_c.receive(event_a.clone());
    replica_c.receive(event_b.clone());

    assert_eq!(replica_a.query(Read::new()), -5);
    assert_eq!(replica_b.query(Read::new()), -5);
    assert_eq!(replica_c.query(Read::new()), -5);
}
