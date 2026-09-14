use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    replica::{IsReplica, Replica},
    state::{cache::CachedLog, graph_log::GraphLog},
};

use super::*;
use crate::utils::membership::{triplet_log, twins_log};

type ListReplica = Replica<GraphLog<List<char>>, Tcsb<List<char>>>;

fn stable_twins(stable: Vec<char>) -> (ListReplica, ListReplica) {
    let replica_a = Replica::bootstrap_with_state(
        "a".to_string(),
        &["a", "b"],
        GraphLog::<List<char>>::from_stable(stable.clone()),
    );
    let replica_b = Replica::bootstrap_with_state(
        "b".to_string(),
        &["a", "b"],
        GraphLog::<List<char>>::from_stable(stable),
    );
    (replica_a, replica_b)
}

#[test]
fn simple_insertion_egwalker() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('A', 0)).unwrap();
    replica_b.receive(e1);

    assert_eq!(&replica_a.query(&Read::<String>::new()), "A");
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn starts_from_stable_document() {
    let (replica_a, replica_b) = stable_twins(vec!['a', 'b', 'c']);

    assert_eq!(replica_a.query(&Read::<String>::new()), "abc");
    assert_eq!(replica_b.query(&Read::<String>::new()), "abc");
}

#[test]
fn inserts_into_stable_document() {
    let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

    let event = replica_a.send(List::insert('X', 1)).unwrap();
    replica_b.receive(event);

    assert_eq!(replica_a.query(&Read::<String>::new()), "aXbc");
    assert_eq!(replica_b.query(&Read::<String>::new()), "aXbc");
}

#[test]
fn deletes_from_stable_document() {
    let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

    let event = replica_a.send(List::delete(1)).unwrap();
    replica_b.receive(event);

    assert_eq!(replica_a.query(&Read::<String>::new()), "ac");
    assert_eq!(replica_b.query(&Read::<String>::new()), "ac");
}

#[test]
fn delete_range_from_stable_document() {
    let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c', 'd']);

    let event = replica_a.send(List::delete_range(1, 2)).unwrap();
    replica_b.receive(event);

    assert_eq!(replica_a.query(&Read::<String>::new()), "ad");
    assert_eq!(replica_b.query(&Read::<String>::new()), "ad");
}

#[test]
fn read_at_uses_stable_document() {
    let (mut replica_a, _) = stable_twins(vec!['a', 'b', 'c']);

    let insert = replica_a.send(List::insert('X', 1)).unwrap();
    let insert_version = insert.event().version().clone();
    replica_a.send(List::delete(1)).unwrap();

    assert_eq!(
        replica_a.query(&ReadAt::<Vec<char>>::new(&insert_version)),
        vec!['a', 'X', 'b', 'c']
    );
    assert_eq!(replica_a.query(&Read::<String>::new()), "abc");
}

#[test]
fn concurrent_insertions_into_stable_document_converge() {
    let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

    let event_a = replica_a.send(List::insert('X', 1)).unwrap();
    let event_b = replica_b.send(List::insert('Y', 1)).unwrap();
    replica_a.receive(event_b);
    replica_b.receive(event_a);

    let a = replica_a.query(&Read::<String>::new());
    let b = replica_b.query(&Read::<String>::new());
    assert_eq!(a, b);
    assert!(a == "aXYbc" || a == "aYXbc", "unexpected result: {a}");
}

#[test]
fn stable_update_wins_over_concurrent_delete() {
    let (mut replica_a, mut replica_b) = stable_twins(vec!['a', 'b', 'c']);

    let event_a = replica_a.send(List::delete(1)).unwrap();
    let event_b = replica_b.send(List::update(1)).unwrap();
    replica_a.receive(event_b);
    replica_b.receive(event_a);

    assert_eq!(replica_a.query(&Read::<String>::new()), "abc");
    assert_eq!(replica_b.query(&Read::<String>::new()), "abc");
}

#[test]
fn concurrent_insertions_egwalker() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('H', 0)).unwrap();
    replica_b.receive(e1);
    assert_eq!(&replica_a.query(&Read::<String>::new()), "H");

    let e2a = replica_a.send(List::insert('e', 1)).unwrap();
    let e2b = replica_b.send(List::insert('i', 1)).unwrap();
    replica_b.receive(e2a);
    replica_a.receive(e2b);

    let res_b = replica_b.query(&Read::<String>::new());
    assert!(
        res_b == "Hei" || res_b == "Hie",
        "Unexpected order: {}",
        res_b
    );
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn concurrent_insert() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('H', 0)).unwrap();
    let e2 = replica_b.send(List::insert('i', 0)).unwrap();
    replica_a.receive(e2);
    replica_b.receive(e1);

    let res_a = replica_a.query(&Read::<String>::new());
    assert!(
        res_a == "Hi" || res_a == "iH",
        "Unexpected order: {}",
        res_a
    );
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn delete_operation_egwalker() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('A', 0)).unwrap();
    replica_b.receive(e1);

    let e2 = replica_a.send(List::delete(0)).unwrap();
    replica_b.receive(e2);

    assert_eq!(&replica_a.query(&Read::<String>::new()), "");
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn conc_delete_ins_egwalker() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('A', 0)).unwrap();
    replica_b.receive(e1);

    let edel = replica_a.send(List::delete(0)).unwrap();
    let eins = replica_b.send(List::insert('B', 1)).unwrap(); // Insert to the right of 'A' in B's view
    replica_a.receive(eins);
    replica_b.receive(edel);

    assert_eq!(&replica_a.query(&Read::<String>::new()), "B");
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn sequential_conc_operations_egwalker() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('H', 0)).unwrap();
    replica_b.receive(e1);
    assert_eq!(&replica_a.query(&Read::<String>::new()), "H");

    let e2a = replica_a.send(List::insert('e', 1)).unwrap();
    let e2b = replica_b.send(List::insert('i', 1)).unwrap();
    replica_b.receive(e2a);
    replica_a.receive(e2b);
    assert!(
        replica_b.query(&Read::<String>::new()) == "Hei"
            || replica_b.query(&Read::<String>::new()) == "Hie"
    );

    // Insert a space between e and i from A's perspective (which will be position 2 if e<i)
    let e3 = replica_a.send(List::insert(' ', 2)).unwrap();
    replica_b.receive(e3);
    let res = replica_a.query(&Read::<String>::new());
    // Depending on tie-breaker, expected is either "He i" or "Hi e". We accept either space between letters.
    assert!(res == "He i" || res == "Hi e", "Unexpected result: {}", res);
}

#[test]
fn in_paper() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    // e1: Insert(0, 'h')
    let e1 = replica_a.send(List::insert('h', 0)).unwrap();
    replica_b.receive(e1.clone());

    // e2: Insert(1, 'i')
    let e2 = replica_a.send(List::insert('i', 1)).unwrap();
    replica_b.receive(e2.clone());

    // Branch: Replica A will capitalize 'H', Replica B will change to 'hey'
    // e3: Insert(0, 'H') depends on e1,e2
    let e3 = replica_a.send(List::insert('H', 0)).unwrap();

    // e4: Delete(1) (remove lowercase 'h') depends on e3
    let e4 = replica_a.send(List::delete(1)).unwrap();

    let e4_version = e4.event().version();
    assert_eq!(&replica_a.query(&Read::<String>::new()), "Hi");

    // e5: Delete(1) (remove 'i') on other branch
    let e5 = replica_b.send(List::delete(1)).unwrap();

    // e6: Insert(1, 'e')
    let e6 = replica_b.send(List::insert('e', 1)).unwrap();

    // e7: Insert(2, 'y')
    let e7 = replica_b.send(List::insert('y', 2)).unwrap();

    replica_b.receive(e3.clone());
    replica_b.receive(e4.clone());
    replica_a.receive(e5.clone());
    replica_a.receive(e6.clone());
    replica_a.receive(e7.clone());
    // Merge both replicas so they see all events before e8
    // At this point both should be "Hey"

    // e8: Insert(3, '!')
    let e8 = replica_b.send(List::insert('!', 3)).unwrap();
    replica_a.receive(e8.clone());

    // Final result should be "Hey!"
    assert_eq!(replica_a.query(&ReadAt::new(e4_version)), vec!['H', 'i']);
    assert_eq!(&replica_a.query(&Read::<String>::new()), "Hey!");
    assert_eq!(
        &replica_a.query(&Read::<String>::new()),
        &replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn delete_range_egwalker() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('A', 0)).unwrap();
    let e2 = replica_a.send(List::insert('B', 1)).unwrap();
    let e3 = replica_a.send(List::insert('C', 2)).unwrap();
    replica_b.receive(e1);
    replica_b.receive(e2);
    replica_b.receive(e3);
    assert_eq!(replica_a.query(&Read::<String>::new()), "ABC");

    let e4 = replica_a.send(List::delete_range(0, 2)).unwrap();
    replica_b.receive(e4);
    assert_eq!(replica_a.query(&Read::<String>::new()), "C");
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn delete_range_egwalker_2() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let event_a = replica_a.send(List::insert('A', 0)).unwrap();
    replica_b.receive(event_a);

    assert_eq!(replica_a.query(&Read::<String>::new()), "A");
    assert_eq!(replica_b.query(&Read::<String>::new()), "A");

    let event_b = replica_b.send(List::insert('B', 0)).unwrap();
    assert_eq!(replica_b.query(&Read::<String>::new()), "BA");
    let event_b_2 = replica_b.send(List::delete_range(0, 2)).unwrap();

    assert_eq!(replica_b.query(&Read::<String>::new()), "");

    let event_a_2 = replica_a.send(List::delete_range(0, 1)).unwrap();

    assert_eq!(replica_a.query(&Read::<String>::new()), "");

    replica_a.receive(event_b);
    replica_a.receive(event_b_2);
    replica_b.receive(event_a_2);
    assert_eq!(replica_a.query(&Read::<String>::new()), "");
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn delete_range_egwalker_3() {
    let (mut replica_a, mut replica_b, mut replica_c) = triplet_log::<GraphLog<List<char>>>();

    let event_a = replica_a.send(List::insert('4', 0)).unwrap();
    replica_c.receive(event_a.clone());

    let event_c = replica_c.send(List::insert('U', 0)).unwrap();
    let event_c_1 = replica_c.send(List::delete_range(0, 2)).unwrap();

    replica_b.receive(event_c.clone());
    replica_b.receive(event_a.clone());
    let event_b = replica_b.send(List::insert('y', 1)).unwrap();

    replica_a.receive(event_c);
    replica_a.receive(event_b.clone());
    replica_c.receive(event_b);
    replica_b.receive(event_c_1.clone());
    replica_a.receive(event_c_1);

    assert_eq!(replica_a.query(&Read::<String>::new()), "y");
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_c.query(&Read::<String>::new())
    );
    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

#[test]
fn update_delete() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let e1 = replica_a.send(List::insert('A', 0)).unwrap();
    replica_b.receive(e1);

    let e2 = replica_b.send(List::update(0)).unwrap();

    let e3 = replica_a.send(List::delete(0)).unwrap();
    replica_b.receive(e3);
    replica_a.receive(e2);

    assert_eq!(
        replica_a.query(&Read::<String>::new()),
        replica_b.query(&Read::<String>::new())
    );
}

/// digraph {
///     0 [ label="[Insert { content: 'a', pos: 0 }@(0:1)]"]
///     1 [ label="[Delete { pos: 0 }@(0:2)]"]
///     2 [ label="[Insert { content: '6', pos: 0 }@(1:1)]"]
///     3 [ label="[Delete { pos: 0 }@(0:3)]"]
///     0 -> 1 [ ]  1 -> 3 [ ]  2 -> 3 [ ]
/// }
#[test]
fn regression_1() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();
    let a1 = replica_a.send(List::insert('a', 0)).unwrap();
    let a2 = replica_a.send(List::delete(0)).unwrap();
    let b1 = replica_b.send(List::insert('6', 0)).unwrap();
    replica_a.receive(b1);
    replica_b.receive(a1);
    replica_b.receive(a2);

    let a3 = replica_a.send(List::delete(0)).unwrap();
    replica_b.receive(a3);

    let state_a = replica_a.query(&Read::<String>::new());
    let state_b = replica_b.query(&Read::<String>::new());
    let result = String::new();

    assert_eq!(state_a, result);
    assert_eq!(state_b, result);
}

/// digraph {
///     0 [ label="[Insert { content: 'N', pos: 0 }@(1:1)]"]
///     1 [ label="[Insert { content: 'r', pos: 1 }@(1:2)]"]
///     2 [ label="[Delete { pos: 0 }@(0:1)]"]
///     3 [ label="[Delete { pos: 0 }@(1:3)]"]
///     4 [ label="[Insert { content: 'Y', pos: 0 }@(0:2)]"]
///     5 [ label="[Insert { content: 'x', pos: 1 }@(1:4)]"]
///     0 -> 1 [ ]  1 -> 2 [ ]  1 -> 3 [ ]  2 -> 4 [ ]  3 -> 4 [ ]  3 -> 5 [ ]
/// }
#[test]
fn regression_2() {
    let (mut replica_a, mut replica_b) = twins_log::<GraphLog<List<char>>>();

    let b1 = replica_b.send(List::insert('N', 0)).unwrap();
    let b2 = replica_b.send(List::insert('r', 1)).unwrap();

    replica_a.receive(b1);
    replica_a.receive(b2);

    let a1 = replica_a.send(List::delete(0)).unwrap();
    let b3 = replica_b.send(List::delete(0)).unwrap();

    replica_a.receive(b3);
    let a2 = replica_a.send(List::insert('Y', 0)).unwrap();
    let b4 = replica_b.send(List::insert('x', 1)).unwrap();

    replica_a.receive(b4);
    replica_b.receive(a1);
    replica_b.receive(a2);

    let state_a = replica_a.query(&Read::<String>::new());
    let state_b = replica_b.query(&Read::<String>::new());
    let result = String::from("Yrx");

    assert_eq!(state_a, result);
    assert_eq!(state_b, result);
}

/// Simulate a long sequence of operations to test caching and performance.
#[test]
fn caching() {
    let (mut replica_a, mut replica_b) = twins_log::<CachedLog<GraphLog<List<char>>, Vec<char>>>();

    let alphabet = "abcdefghijklmnopqrstuvwxyz".chars().collect::<Vec<char>>();

    for i in 0..100 {
        if i % 3 == 0 {
            let e1 = replica_a
                .send(List::insert(alphabet[i % alphabet.len()], 0))
                .unwrap();
            let e2 = replica_b
                .send(List::insert(alphabet[(i + 1) % alphabet.len()], 0))
                .unwrap();
            replica_a.receive(e2);
            replica_b.receive(e1);
        } else if i % 3 == 1 {
            let e1 = replica_a.send(List::delete(0)).unwrap();
            let e2 = replica_b.send(List::delete(0)).unwrap();
            replica_a.receive(e2);
            replica_b.receive(e1);
        } else {
            let e1 = replica_a.send(List::update(0)).unwrap();
            let e2 = replica_b.send(List::update(0)).unwrap();
            replica_a.receive(e2);
            replica_b.receive(e1);
        }
    }

    let time = std::time::Instant::now();
    let _ = replica_a
        .state()
        .read_ref()
        .iter()
        .cloned()
        .collect::<String>();
    let elapsed_no_caching = time.elapsed();

    let time = std::time::Instant::now();
    let _ = replica_a
        .state()
        .read_ref()
        .iter()
        .cloned()
        .collect::<String>();
    let elapsed_with_caching_1 = time.elapsed();

    let _ = replica_a.send(List::insert('A', 0)).unwrap();

    let time = std::time::Instant::now();
    let _ = replica_a
        .state()
        .read_ref()
        .iter()
        .cloned()
        .collect::<String>();
    let elapsed_with_caching_2 = time.elapsed();

    println!(
        "Elapsed without caching: {:?}, with caching 1: {:?}, with caching 2: {:?}",
        elapsed_no_caching, elapsed_with_caching_1, elapsed_with_caching_2
    );

    assert!(elapsed_with_caching_1 * 100 < elapsed_no_caching);
    assert!(elapsed_with_caching_2 * 100 < elapsed_no_caching);
}

#[cfg(feature = "fuzz")]
#[test]
#[ignore]
fn fuzz_list() {
    use moirai_fuzz::{
        config::{FuzzerConfig, Predicate, RunConfig},
        fuzzer::fuzzer,
    };

    let run = RunConfig::new(0.5, 8, 1_000, None, None, true, false);
    let runs = vec![run; 1];

    let config = FuzzerConfig::<GraphLog<List<char>>, Read<String>>::new(
        "list",
        runs,
        true,
        Predicate::new(Read::new(), |a, b| a == b),
        false,
    );

    fuzzer::<GraphLog<List<char>>, Read<String>>(config);
}
