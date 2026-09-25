//! SI.01, round 3: two races on an optional containment, read in process
//! through the identified read, `QueryableLog::query_identified_state_json`.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`, section "Round 3,
//! written before any rerun" (O1g and O1h); the contract these tests read,
//! contract R', is restated in `si01/mod.rs`. Every read here is also held to
//! the other half of R', as in `si01_store_identity.rs`: the default read is
//! the identified read with the identities stripped.
//!
//! Both races start from an object that an optional holds at its default. A
//! `Set(Variant(C, New))` with nothing written into the object leaves the
//! optional reading unset, so the object is not served; it is there all the
//! same, and a later write into it, by a writer who has seen it, is an update
//! of that object and not a new element. What each test expects is derived
//! from its schedule and nothing else: an element keeps the identity that its
//! own writer served for it before any concurrent operation reached that
//! writer (C4, "not on a concurrent write from another replica"), and every
//! replica serves the same identified document (C2), in every order the race
//! can be delivered in.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si01_store_identity_races -- --test-threads=1
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;

use moirai_interp::testing::{attribute, bench_descriptor, class, containment, descriptor, text};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::replica::IsReplica;
use serde_json::{Value, json};

use si01::*;

/// The identified read of this replica, held to R': the default read is the
/// identified read stripped. Neither metamodel here has a transparent class.
fn served(replica: &Rep) -> Value {
    let doc = <ModelLog as QueryableLog>::query_identified_state_json(replica);
    assert_eq!(
        strip(&doc, OPAQUE),
        default_read(replica),
        "the default read is not the identified read with the identities stripped; \
         identified: {doc}"
    );
    doc
}

// ---------------------------------------------------------------------------
// O1g: an unset raced by a write into the object it would have removed
// ---------------------------------------------------------------------------

/// `Bench.maybe` holds an `Item` at its default (X1). Partitioned, `a` unsets
/// `maybe` (U) and `b` writes a character into the `Item` it sees (W). W was
/// written into an element its writer saw, so it is an update and creates
/// nothing, and a write concurrent with an unset survives it: after the heal
/// every replica serves the `Item`, holding W's text, under the identity `b`
/// served for it right after W. An increment of its counter written after
/// the heal (Z) is a write to an element that never stopped being there, and
/// moves no identity.
///
/// U and W reach a replica in one of two orders. `a` delivers U first and `b`
/// delivers W first, and `c` takes each order in turn. Z is written on each of
/// the three replicas in turn: on one that delivered U first, on one that
/// delivered W first, and on `c`, in whichever order it took.
#[test]
fn si01_an_unset_raced_by_a_write_keeps_the_items_identity_in_both_delivery_orders() {
    let bench = Bench::new();
    for write_first_at_c in [false, true] {
        for writer in ['a', 'b', 'c'] {
            let who = format!(
                "optional unset raced by a write, c delivers {}, Z written on {writer}",
                if write_first_at_c {
                    "W then U"
                } else {
                    "U then W"
                }
            );
            let (mut a, mut b, mut c) = three(&bench_descriptor());

            // The root, then X1: an `Item` set into `maybe` with nothing
            // written into it.
            send_all(
                &mut a,
                &mut [&mut b, &mut c],
                vec![
                    bench.tick(),
                    bench.maybe(InstanceOp::set(bench.item(InstanceOp::New))),
                ],
            );
            let emptied = served(&a);
            identified(&emptied, 1, &format!("{who}, after X1"));
            assert!(
                emptied.get("maybe").is_none(),
                "{who}: an `Item` set with nothing written into it leaves `maybe` reading \
                 unset: {emptied}"
            );

            // Partitioned: U on a, W on b.
            let u = send!(a, bench.maybe(InstanceOp::unset()));
            let w = send!(b, bench.maybe(InstanceOp::set(bench.item_text(0, 'w'))));
            let on_b = served(&b);
            identified(&on_b, 2, &format!("{who}, b after W"));
            assert_eq!(
                on_b["maybe"]["ID"],
                json!("w"),
                "{who}: b serves the `Item` with W's text: {on_b}"
            );
            let item = id_of(&on_b["maybe"]);

            // Healed, each side delivering the other's operation.
            a.receive(w.clone());
            b.receive(u.clone());
            if write_first_at_c {
                c.receive(w);
                c.receive(u);
            } else {
                c.receive(u);
                c.receive(w);
            }
            let healed = served(&a);
            identified(&healed, 2, &format!("{who}, healed"));
            assert_eq!(
                healed,
                served(&b),
                "{who}, healed: a and b serve different documents"
            );
            assert_eq!(
                healed,
                served(&c),
                "{who}, healed: a and c serve different documents"
            );
            assert_eq!(
                healed["maybe"]["ID"],
                json!("w"),
                "{who}: W's write survives the concurrent unset: {healed}"
            );
            assert_eq!(
                id_of(&healed["maybe"]),
                item,
                "{who}, healed: the `Item` W wrote into no longer carries the identity b served \
                 for it after W"
            );

            // Z: an increment of the `Item`'s counter, from a replica that
            // has delivered both U and W, delivered to the other two.
            let z = vec![bench.maybe(InstanceOp::set(bench.item_count(1)))];
            match writer {
                'a' => send_all(&mut a, &mut [&mut b, &mut c], z),
                'b' => send_all(&mut b, &mut [&mut a, &mut c], z),
                _ => send_all(&mut c, &mut [&mut a, &mut b], z),
            }
            let after = served(&a);
            identified(&after, 2, &format!("{who}, after Z"));
            assert_eq!(
                after,
                served(&b),
                "{who}, after Z: a and b serve different documents"
            );
            assert_eq!(
                after,
                served(&c),
                "{who}, after Z: a and c serve different documents"
            );
            assert_ne!(
                after["maybe"]["count"], healed["maybe"]["count"],
                "{who}: Z did not reach the `Item`: {after}"
            );
            assert_eq!(
                id_of(&after["maybe"]),
                item,
                "{who}, after Z: an increment of an element that never stopped being there moved \
                 its identity"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// O1h: a concurrent set of another class into an optional
// ---------------------------------------------------------------------------

/// The metamodel race (b) needs and no fixture has: an optional containment
/// whose target is abstract with two concrete classes.
///
/// ```text
/// Holder   root; tick: counter, slot: optional Shape
/// Shape    abstract; name: text
/// A, B     : Shape
/// ```
fn shapes_descriptor() -> Value {
    descriptor(
        &["Holder"],
        vec![
            (
                "Holder",
                class(
                    false,
                    &[],
                    vec![
                        attribute(
                            "tick",
                            json!({"kind": "single"}),
                            json!({"kind": "counter", "num": "i32", "resettable": true}),
                        ),
                        containment("slot", "Shape", "optional"),
                    ],
                ),
            ),
            ("Shape", class(true, &[], vec![text("name")])),
            ("A", class(false, &["Shape"], vec![])),
            ("B", class(false, &["Shape"], vec![])),
        ],
        json!({}),
    )
}

/// Operations on [`shapes_descriptor`]'s model.
struct Shapes(Lang);

impl Shapes {
    fn new() -> Self {
        Shapes(Lang::parse(&shapes_descriptor()))
    }

    /// An increment of `Holder.tick`, which creates the root.
    fn tick(&self) -> ModelOp {
        let l = &self.0;
        l.is("Holder", l.at("Holder", "tick", inc_int(1)))
            .into_model_op()
    }

    /// A `Set` into `Holder.slot`.
    fn set(&self, inner: InstanceOp) -> ModelOp {
        let l = &self.0;
        l.is("Holder", l.at("Holder", "slot", InstanceOp::set(inner)))
            .into_model_op()
    }

    /// An object of `class` set into the slot with nothing written into it,
    /// which is how the editor mints into an optional before its field
    /// writes.
    fn new_object(&self, class: &str) -> ModelOp {
        self.set(self.0.is(class, InstanceOp::New))
    }

    /// The first character of the name of the slot's object of `class`.
    fn name(&self, class: &str, ch: char) -> ModelOp {
        let l = &self.0;
        self.set(l.is(class, l.at(class, "name", chr(0, ch))))
    }
}

/// The one member of a served conflict whose class is `class`.
fn member_of_class<'a>(conflict: &'a [Value], class: &str, who: &str) -> &'a Value {
    let hits: Vec<&Value> = conflict
        .iter()
        .filter(|element| element["eClass"] == json!(class))
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "{who}: one member of class {class} in the conflict: {conflict:?}"
    );
    hits[0]
}

/// Partitioned, `a` sets an `A` into `slot` with nothing written into it (X1)
/// and then names it (X2), while `b` does the same with a `B` (Y1, then Y2).
/// X2 is written into the `A` that X1 made, and Y2 into the `B` that Y1 made,
/// and nothing removes anything. So after every replica has delivered all
/// four, `slot` is a conflict of two elements, one per class: the `A`, named
/// `x`, under the identity `a` served for it before anything of `b`'s reached
/// it, and the `B`, named `y`, under the identity `b` served for it.
///
/// `c` delivers the four in each of the six orders that keep X1 before X2 and
/// Y1 before Y2; `a` and `b` deliver their own two first.
#[test]
fn si01_a_concurrent_set_of_another_class_keeps_both_identities_in_every_delivery_order() {
    let shapes = Shapes::new();
    let names = ["X1", "X2", "Y1", "Y2"];
    let orders: [[usize; 4]; 6] = [
        [0, 1, 2, 3],
        [0, 2, 1, 3],
        [0, 2, 3, 1],
        [2, 0, 1, 3],
        [2, 0, 3, 1],
        [2, 3, 0, 1],
    ];
    for order in orders {
        let who = format!(
            "optional set to two classes at once, c delivers {}",
            order.map(|index| names[index]).join(", ")
        );
        let (mut a, mut b, mut c) = three(&shapes_descriptor());
        send_all(&mut a, &mut [&mut b, &mut c], vec![shapes.tick()]);

        // Partitioned.
        let x1 = send!(a, shapes.new_object("A"));
        let x2 = send!(a, shapes.name("A", 'x'));
        let y1 = send!(b, shapes.new_object("B"));
        let y2 = send!(b, shapes.name("B", 'y'));

        let on_a = served(&a);
        identified(&on_a, 2, &format!("{who}, a before the heal"));
        assert_eq!(
            (&on_a["slot"]["eClass"], &on_a["slot"]["name"]),
            (&json!("A"), &json!("x")),
            "{who}: a serves its named `A`: {on_a}"
        );
        let the_a = id_of(&on_a["slot"]);
        let on_b = served(&b);
        identified(&on_b, 2, &format!("{who}, b before the heal"));
        assert_eq!(
            (&on_b["slot"]["eClass"], &on_b["slot"]["name"]),
            (&json!("B"), &json!("y")),
            "{who}: b serves its named `B`: {on_b}"
        );
        let the_b = id_of(&on_b["slot"]);

        // Healed.
        let events = [x1, x2, y1, y2];
        a.receive(events[2].clone());
        a.receive(events[3].clone());
        b.receive(events[0].clone());
        b.receive(events[1].clone());
        for index in order {
            c.receive(events[index].clone());
        }

        let healed = served(&a);
        identified(&healed, 3, &format!("{who}, healed"));
        assert_eq!(
            healed,
            served(&b),
            "{who}: a and b serve different documents"
        );
        assert_eq!(
            healed,
            served(&c),
            "{who}: a and c serve different documents"
        );
        let conflict = healed["slot"]["__conflict"]
            .as_array()
            .unwrap_or_else(|| panic!("{who}: `slot` is not a conflict of two classes: {healed}"));
        assert_eq!(conflict.len(), 2, "{who}: two members: {healed}");
        let the_a_now = member_of_class(conflict, "A", &who);
        let the_b_now = member_of_class(conflict, "B", &who);
        assert_eq!(the_a_now["name"], json!("x"), "{who}: X2's name: {healed}");
        assert_eq!(the_b_now["name"], json!("y"), "{who}: Y2's name: {healed}");
        assert_eq!(
            id_of(the_a_now),
            the_a,
            "{who}: the `A` no longer carries the identity a served for it before b's writes \
             arrived"
        );
        assert_eq!(
            id_of(the_b_now),
            the_b,
            "{who}: the `B` no longer carries the identity b served for it before a's writes \
             arrived"
        );
    }
}
