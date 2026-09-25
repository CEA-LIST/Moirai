//! SI.01, the store identity of an element, read in process where a client
//! reads it: the identified read, `QueryableLog::query_identified_state_json`,
//! the function `GET /api/model/{id}/state/identified` answers with.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`; the contract these
//! tests read, contract R', is restated in `si01/mod.rs`. Every read here is
//! also held to the other half of R': the default read,
//! `QueryableLog::query_state_json`, is the identified read with the
//! identities stripped ([`si01::strip`]), so the identities are beside the
//! default read and not in it.
//!
//! The multi-replica scenario over `model_node` processes and the checks that
//! the default read is byte-for-byte what it was are in
//! `si01_store_identity_l3.rs`.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test si01_store_identity \
//!     -- --test-threads=1
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;

use moirai_interp::testing::{bench_descriptor, opened};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::replica::IsReplica;
use serde_json::Value;

use si01::*;

/// The identified read of this replica, held to R': the default read is the
/// identified read stripped. `transparent` names the model's transparent
/// classes.
fn served(replica: &Rep, transparent: &[(&str, &str)]) -> Value {
    let doc = <ModelLog as QueryableLog>::query_identified_state_json(replica);
    assert_eq!(
        strip(&doc, transparent),
        default_read(replica),
        "the default read is not the identified read with the identities stripped; \
         identified: {doc}"
    );
    doc
}

// ---------------------------------------------------------------------------
// L2: the identified read, in process
// ---------------------------------------------------------------------------

/// Every kind of place an element can sit in is served with an identity, one
/// operation that creates two elements gives them two, and nothing written
/// into an element afterwards moves its identity: not a text edit, not a
/// counter, not a sibling inserted before it, not a peer's concurrent write.
#[test]
fn si01_every_element_is_served_with_an_identity_that_edits_do_not_move() {
    // Bench: the root, a single containment, an optional one and a sequence.
    let bench = Bench::new();
    let (mut a, mut b) = opened("m", &bench_descriptor());
    // One operation creates the root and the first item of `many`.
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            bench.many(InstanceOp::insert(0, bench.item_text(0, 'x'))),
            bench.one(bench.item_text(0, 'o')),
            bench.maybe(InstanceOp::set(bench.item_count(1))),
            bench.many(InstanceOp::insert(1, bench.item_text(0, 'y'))),
        ],
    );
    let before = served(&a, OPAQUE);
    assert_eq!(before, served(&b, OPAQUE), "the twins serve one document");
    identified(&before, 5, "bench, created");
    let root = id_of(&before);
    let one = id_of(&before["one"]);
    let maybe = id_of(&before["maybe"]);
    let x = id_of(&before["many"][0]);
    let y = id_of(&before["many"][1]);

    // Concurrent edits from both sides, and a sibling put in front of `x`.
    let from_a = vec![
        send!(a, bench.many(InstanceOp::at(0, bench.item_text(1, 'z')))),
        send!(
            a,
            bench.many(InstanceOp::insert(0, bench.item_text(0, 'w')))
        ),
        send!(a, bench.one(bench.item_count(3))),
    ];
    let from_b = vec![
        send!(b, bench.one(bench.item_text(1, 'p'))),
        send!(b, bench.maybe(InstanceOp::set(bench.item_count(2)))),
        send!(b, bench.many(InstanceOp::at(1, bench.item_count(4)))),
    ];
    for event in from_b {
        a.receive(event);
    }
    for event in from_a {
        b.receive(event);
    }
    let after = served(&a, OPAQUE);
    assert_eq!(
        after,
        served(&b, OPAQUE),
        "the twins serve one document after the edits"
    );
    identified(&after, 6, "bench, edited");
    assert_eq!(id_of(&after), root, "the root kept its identity");
    assert_eq!(
        id_of(&after["one"]),
        one,
        "the single containment's element kept it"
    );
    assert_eq!(
        id_of(&after["maybe"]),
        maybe,
        "the optional's element kept it"
    );
    assert_eq!(
        id_of(&after["many"][1]),
        x,
        "x kept it with a sibling put before it"
    );
    assert_eq!(id_of(&after["many"][2]), y, "y kept it");

    // JSON: every value is transparent and is served as a record.
    let js = Json::new();
    let (mut a, mut b) = opened("j", &json_descriptor());
    let mut ops = js.text("title", "x");
    ops.push(js.put(
        "tags",
        js.array(InstanceOp::insert(0, js.string(chr(0, 'p')))),
    ));
    ops.push(js.put("n", js.number(inc_float(3.0))));
    send_all(&mut a, &mut [&mut b], ops);
    let before = served(&a, JSON_TRANSPARENT);
    assert_eq!(
        before,
        served(&b, JSON_TRANSPARENT),
        "the JSON twins serve one document"
    );
    // The root object, title, tags, tags[0] and n.
    identified(&before, 5, "json, created");
    let jroot = id_of(&before);
    let title = id_of(member(&before, "title"));
    let tags = id_of(member(&before, "tags"));
    let p = id_of(&items(member(&before, "tags"))[0]);
    let n = id_of(member(&before, "n"));

    let from_a = vec![
        send!(a, js.put("title", js.string(chr(1, 'y')))),
        send!(
            a,
            js.put(
                "tags",
                js.array(InstanceOp::insert(0, js.string(chr(0, 'q'))))
            )
        ),
    ];
    let from_b = vec![send!(b, js.put("n", js.number(inc_float(1.5))))];
    for event in from_b {
        a.receive(event);
    }
    for event in from_a {
        b.receive(event);
    }
    let after = served(&a, JSON_TRANSPARENT);
    assert_eq!(
        after,
        served(&b, JSON_TRANSPARENT),
        "the JSON twins serve one document after the edits"
    );
    identified(&after, 6, "json, edited");
    assert_eq!(id_of(&after), jroot);
    assert_eq!(id_of(member(&after, "title")), title, "a text edit kept it");
    assert_eq!(id_of(member(&after, "tags")), tags);
    assert_eq!(
        id_of(&items(member(&after, "tags"))[1]),
        p,
        "an item kept it with another put before it"
    );
    assert_eq!(id_of(member(&after, "n")), n, "a peer's increment kept it");
}

/// Concurrent creations of one class in one place are one element on every
/// replica, and that element has one identity on every replica, whatever
/// order each of three replicas delivers the other two creations in. The
/// same holds where the places are distinct: three concurrent inserts are
/// three elements with three identities.
#[test]
fn si01_concurrent_creations_in_one_place_converge_on_one_identity_in_every_delivery_order() {
    let bench = Bench::new();
    let js = Json::new();
    let uml = Uml::new();

    type Setup = Box<dyn Fn() -> (Rep, Rep, Rep)>;
    let bench_fresh: Setup = Box::new(|| three(&bench_descriptor()));
    let bench_rooted: Setup = Box::new(move || {
        let (mut a, mut b, mut c) = three(&bench_descriptor());
        send_all(&mut a, &mut [&mut b, &mut c], vec![Bench::new().tick()]);
        (a, b, c)
    });
    let json_rooted: Setup = Box::new(move || {
        let (mut a, mut b, mut c) = three(&json_descriptor());
        send_all(
            &mut a,
            &mut [&mut b, &mut c],
            vec![Json::new().empty_root()],
        );
        (a, b, c)
    });
    let uml_fresh: Setup = Box::new(|| three(&uml_descriptor()));

    type Place<'a> = (
        &'a str,
        &'a Setup,
        [ModelOp; 3],
        usize,
        &'a [(&'a str, &'a str)],
    );
    let places: Vec<Place> = vec![
        (
            "the root, created by all three",
            &bench_fresh,
            [bench.text('a'), bench.text('b'), bench.text('c')],
            1,
            OPAQUE,
        ),
        (
            "a single-valued containment",
            &bench_rooted,
            [
                bench.one(bench.item_count(1)),
                bench.one(bench.item_count(2)),
                bench.one(bench.item_count(3)),
            ],
            2,
            OPAQUE,
        ),
        (
            "an optional containment",
            &bench_rooted,
            [
                bench.maybe(InstanceOp::set(bench.item_count(1))),
                bench.maybe(InstanceOp::set(bench.item_count(2))),
                bench.maybe(InstanceOp::set(bench.item_count(3))),
            ],
            2,
            OPAQUE,
        ),
        (
            "one key of a JSON object",
            &json_rooted,
            [
                js.put("k", js.string(chr(0, 'a'))),
                js.put("k", js.string(chr(0, 'b'))),
                js.put("k", js.string(chr(0, 'c'))),
            ],
            2,
            JSON_TRANSPARENT,
        ),
        (
            "a UML root, each creator inserting a class of its own in the same operation",
            &uml_fresh,
            [
                uml.owned(InstanceOp::insert(0, uml.class_name(chr(0, 'A')))),
                uml.owned(InstanceOp::insert(0, uml.class_name(chr(0, 'B')))),
                uml.owned(InstanceOp::insert(0, uml.class_name(chr(0, 'C')))),
            ],
            4,
            OPAQUE,
        ),
    ];

    for (place, setup, concurrent, count, transparent) in places {
        // Bit i of `order` says which of the other two replica i delivers
        // first.
        for order in 0u8..8 {
            let (mut a, mut b, mut c) = setup();
            let [op_a, op_b, op_c] = concurrent.clone();
            let ea = send!(a, op_a);
            let eb = send!(b, op_b);
            let ec = send!(c, op_c);
            let pair = |first, second, flip: bool| {
                if flip {
                    [second, first]
                } else {
                    [first, second]
                }
            };
            for event in pair(eb.clone(), ec.clone(), order & 1 != 0) {
                a.receive(event);
            }
            for event in pair(ea.clone(), ec.clone(), order & 2 != 0) {
                b.receive(event);
            }
            for event in pair(ea.clone(), eb.clone(), order & 4 != 0) {
                c.receive(event);
            }
            let who = format!("{place}, delivery order {order:03b}");
            let (sa, sb, sc) = (
                served(&a, transparent),
                served(&b, transparent),
                served(&c, transparent),
            );
            identified(&sa, count, &format!("{who}, on a"));
            assert_eq!(sa, sb, "{who}: a and b serve different documents");
            assert_eq!(sa, sc, "{who}: a and c serve different documents");
        }
    }
}

/// An element deleted and then created again in the same place is a new
/// element: a new identity, and the old one appears nowhere.
#[test]
fn si01_an_element_deleted_and_created_again_gets_a_new_identity() {
    // A sequence: delete the item, insert one in the same place.
    let bench = Bench::new();
    let (mut a, mut b) = opened("m", &bench_descriptor());
    send_all(
        &mut a,
        &mut [&mut b],
        vec![bench.many(InstanceOp::insert(0, bench.item_text(0, 'x')))],
    );
    let first = id_of(&served(&a, OPAQUE)["many"][0]);
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            bench.many(InstanceOp::delete(0)),
            bench.many(InstanceOp::insert(0, bench.item_text(0, 'x'))),
        ],
    );
    let now = served(&a, OPAQUE);
    assert_eq!(now, served(&b, OPAQUE));
    identified(&now, 2, "sequence, recreated");
    assert_ne!(
        id_of(&now["many"][0]),
        first,
        "a sequence item created again is new"
    );
    assert!(
        !all_ids(&now).contains(&first),
        "the deleted item's identity is gone"
    );

    // An optional containment: unset, then set again.
    send_all(
        &mut a,
        &mut [&mut b],
        vec![bench.maybe(InstanceOp::set(bench.item_count(1)))],
    );
    let first = id_of(&served(&a, OPAQUE)["maybe"]);
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            bench.maybe(InstanceOp::unset()),
            bench.maybe(InstanceOp::set(bench.item_count(2))),
        ],
    );
    let now = served(&a, OPAQUE);
    assert_eq!(now, served(&b, OPAQUE));
    assert_ne!(
        id_of(&now["maybe"]),
        first,
        "an optional element set again is new"
    );
    assert!(
        !all_ids(&now).contains(&first),
        "the unset element's identity is gone"
    );

    // A JSON key: removed, then put again, with and without the same class's
    // `New`, which is how the JSON editor writes a value (in one go).
    let js = Json::new();
    let (mut a, mut b) = opened("j", &json_descriptor());
    let mut ops = js.text("s", "ab");
    ops.push(js.put("n", js.number(inc_float(3.0))));
    send_all(&mut a, &mut [&mut b], ops);
    let before = served(&a, JSON_TRANSPARENT);
    let (s, n) = (id_of(member(&before, "s")), id_of(member(&before, "n")));
    send_all(
        &mut a,
        &mut [&mut b],
        vec![
            js.remove("s"),
            js.put("s", js.string(chr(0, 'c'))),
            js.remove("n"),
            js.put("n", js.number(inc_float(5.0))),
        ],
    );
    let now = served(&a, JSON_TRANSPARENT);
    assert_eq!(now, served(&b, JSON_TRANSPARENT));
    identified(&now, 3, "json, recreated");
    assert_ne!(
        id_of(member(&now, "s")),
        s,
        "a string put again under its key is new"
    );
    assert_ne!(
        id_of(member(&now, "n")),
        n,
        "a number put again under its key is new"
    );
    let ids = all_ids(&now);
    assert!(
        !ids.contains(&s) && !ids.contains(&n),
        "the removed values' identities are gone"
    );
}
