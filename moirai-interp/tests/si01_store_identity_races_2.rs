//! SI.01, round 4: three races on the mechanism round 2 added (Moirai
//! `f4d3648`: every operation that reaches an object is one of its touches, a
//! removal covers the touches it had seen, and `forget_stable_touches` prunes
//! them at causal stability), read in process through the identified read,
//! `QueryableLog::query_identified_state_json`; and one observation.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`, section "Round 4,
//! written before any rerun" (O1i, O1j, O1k and the observation OB1); the
//! contract these tests read, contract R', is restated in `si01/mod.rs`.
//! Every read here is also held to the other half of R', as in
//! `si01_store_identity.rs`: the default read is the identified read with the
//! identities stripped.
//!
//! What each test expects is derived from its schedule and nothing else. An
//! element keeps the identity its own writer served for it before any
//! concurrent operation reached that writer, and a write into an element its
//! writer saw alive moves nothing (C4). An element removed and then written
//! again by a writer who had seen the removal is a new element: its identity
//! is one nothing carried before, and the old one is served nowhere (C5).
//! Every replica that has delivered the same operations serves the same
//! identified document (C2), in every order the race can be delivered in. A
//! check whose message starts with `set-up:` checks the schedule itself (that
//! causal stability stands where the scenario says it does), not the claim.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si01_store_identity_races_2 -- --test-threads=1
//! ```
//!
//! The observation OB1 is ignored by default and is not a pass criterion; it
//! prints what each replica serves:
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si01_store_identity_races_2 -- --ignored --nocapture --test-threads=1
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;

use std::collections::BTreeSet;

use moirai_interp::testing::{attribute, class, containment, descriptor};
use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::replica::IsReplica;
use serde_json::{Value, json};

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

/// The one identified document three replicas serve, held to C2.
fn agreed(replicas: [&Rep; 3], transparent: &[(&str, &str)], who: &str) -> Value {
    let [a, b, c] = replicas;
    let doc = served(a, transparent);
    assert_eq!(
        doc,
        served(b, transparent),
        "{who}: a and b serve different documents"
    );
    assert_eq!(
        doc,
        served(c, transparent),
        "{who}: a and c serve different documents"
    );
    doc
}

// ---------------------------------------------------------------------------
// O1i: a JSON key removed while another writer updates its member
// ---------------------------------------------------------------------------

/// `k` holds a `Number` that `a` put there (X1) and every replica delivered.
/// Partitioned, `a` removes `k` (R) and `b` increments the member it sees
/// (W). W was written into an element its writer saw, so it is an update and
/// creates nothing, and a write concurrent with a removal survives it
/// (update-wins, `uw_map.rs:150-152`): after the heal every replica serves
/// the member, holding W's increment and not X1's, under the identity every
/// replica served for it after X1. An increment written after the heal (Z)
/// is a write to an element that never stopped being there, and moves no
/// identity.
///
/// R and W reach a replica in one of two orders. `a` delivers R first and `b`
/// delivers W first, and `c` takes each order in turn. Z is written on each of
/// the three replicas in turn.
#[test]
fn si01_a_key_removal_raced_by_an_update_keeps_the_members_identity_in_both_delivery_orders() {
    let js = Json::new();
    for update_first_at_c in [false, true] {
        for writer in ['a', 'b', 'c'] {
            let who = format!(
                "json key removed while updated, c delivers {}, Z written on {writer}",
                if update_first_at_c {
                    "W then R"
                } else {
                    "R then W"
                }
            );
            let (mut a, mut b, mut c) = three(&json_descriptor());

            // X1: the root object and a number under `k`, from a.
            send_all(
                &mut a,
                &mut [&mut b, &mut c],
                vec![js.put("k", js.number(inc_float(1.0)))],
            );
            let start = agreed([&a, &b, &c], JSON_TRANSPARENT, &format!("{who}, after X1"));
            identified(&start, 2, &format!("{who}, after X1"));
            let k = id_of(member(&start, "k"));

            // Partitioned: R on a, W on b.
            let r = send!(a, js.remove("k"));
            let w = send!(b, js.put("k", js.number(inc_float(10.0))));
            let on_b = served(&b, JSON_TRANSPARENT);
            assert_eq!(
                member(&on_b, "k")["value"],
                json!(11.0),
                "{who}: b serves X1's and W's increments: {on_b}"
            );
            assert_eq!(
                id_of(member(&on_b, "k")),
                k,
                "{who}, b after W: an increment of the member moved its identity"
            );

            // Healed, each side delivering the other's operation.
            a.receive(w.clone());
            b.receive(r.clone());
            if update_first_at_c {
                c.receive(w);
                c.receive(r);
            } else {
                c.receive(r);
                c.receive(w);
            }
            let healed = agreed([&a, &b, &c], JSON_TRANSPARENT, &format!("{who}, healed"));
            identified(&healed, 2, &format!("{who}, healed"));
            assert_eq!(
                member(&healed, "k")["value"],
                json!(10.0),
                "{who}: W's increment survives the concurrent removal and X1's does not: {healed}"
            );
            assert_eq!(
                id_of(member(&healed, "k")),
                k,
                "{who}, healed: the member W wrote into no longer carries the identity it was \
                 served under after X1"
            );

            // Z: an increment of the member, from a replica that has
            // delivered both R and W, delivered to the other two.
            let z = vec![js.put("k", js.number(inc_float(100.0)))];
            match writer {
                'a' => send_all(&mut a, &mut [&mut b, &mut c], z),
                'b' => send_all(&mut b, &mut [&mut a, &mut c], z),
                _ => send_all(&mut c, &mut [&mut a, &mut b], z),
            }
            let after = agreed([&a, &b, &c], JSON_TRANSPARENT, &format!("{who}, after Z"));
            identified(&after, 2, &format!("{who}, after Z"));
            assert_eq!(
                member(&after, "k")["value"],
                json!(110.0),
                "{who}: Z's increment is added to W's: {after}"
            );
            assert_eq!(
                id_of(member(&after, "k")),
                k,
                "{who}, after Z: an increment of a member that never stopped being there moved \
                 its identity"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// O1j: touches pruned at causal stability
// ---------------------------------------------------------------------------

/// The sequence number, on its origin, of the operation a message carries.
fn seq(message: &EventMessage<ModelOp>) -> usize {
    message.event().id().seq()
}

/// How far `origin`'s operations are causally stable on this replica: every
/// operation of `origin` up to this sequence number has been delivered by
/// every replica as far as this one knows, and the log has been handed the
/// stable version that says so.
fn stable_through(replica: &Rep, origin: &str) -> usize {
    replica
        .stability()
        .stable_version
        .iter()
        .find(|(id, _)| id == origin)
        .map_or(0, |(_, seq)| *seq)
}

/// An identified JSON document with one member of its root object left out.
fn without(doc: &Value, key: &str) -> Value {
    let mut out = doc.clone();
    out["entry"]
        .as_object_mut()
        .unwrap_or_else(|| panic!("no entry object in {doc}"))
        .remove(key);
    out
}

/// One increment of the number `h` from each replica in turn, each delivered
/// to the other two before the next is written: afterwards, every operation
/// any replica had delivered before the round is causally stable on all
/// three.
fn round_of_writes(js: &Json, a: &mut Rep, b: &mut Rep, c: &mut Rep) {
    let h = || vec![js.put("h", js.number(inc_float(1.0)))];
    send_all(a, &mut [&mut *b, &mut *c], h());
    send_all(b, &mut [&mut *a, &mut *c], h());
    send_all(c, &mut [&mut *a, &mut *b], h());
}

/// Touches pruned at causal stability keep every identity, and keep for every
/// writer still to come the answer to whether an element is alive.
///
/// `b` puts two numbers, `k` and `j` (X_k, X_j), and every replica delivers
/// them. Then `a` removes both (R_k, R_j), and `b`, which has seen neither
/// removal, increments `k` (L); `a` and `c` deliver L, and `c`, which has
/// seen neither removal either, puts a third number `t` (T), which reaches
/// `a` alone. When T arrives, `a` knows that every replica has delivered all
/// three of `b`'s operations and that `c` has delivered neither removal: X_k,
/// X_j and L are causally stable on `a`, R_k and R_j are not, and `a` prunes
/// its touches then. Nothing it serves may move, and two writers still to
/// come must find what they would have found with nothing pruned:
///
/// - `c` increments `j` (W2) without having seen R_j. `j`'s one touch is
///   X_j, stable, and seen only by R_j, which is not stable: for W2 the member
///   is alive, so W2 is an update, `j` keeps its identity, and W2's increment
///   survives R_j.
/// - `a` increments `k` (Z) having seen R_k and L. `k`'s touches are X_k,
///   which R_k had seen, and L, which it had not, both stable and both `b`'s:
///   L, the later, is what keeps `k` alive for Z, so Z is an update and `k`
///   keeps its identity.
///
/// Then every replica delivers everything, and one round of writes to a
/// fourth number `h` makes every earlier operation stable on all three; the
/// pruning that follows must again move nothing served. After it, a write to
/// `k` from each replica moves nothing; and `j`, removed by `b`, made stable
/// everywhere, is absent (ST.10: the removal had seen every write to it), and
/// put again by `c` is a new element: a new identity, and its old one served
/// nowhere.
#[test]
fn si01_touches_pruned_at_causal_stability_keep_every_identity_and_what_later_writers_see() {
    let js = Json::new();
    let (mut a, mut b, mut c) = three(&json_descriptor());
    let mut ever: BTreeSet<String> = BTreeSet::new();

    // X_k and X_j, from b, delivered everywhere.
    send_all(
        &mut b,
        &mut [&mut a, &mut c],
        vec![
            js.put("k", js.number(inc_float(1.0))),
            js.put("j", js.number(inc_float(2.0))),
        ],
    );
    let start = agreed(
        [&a, &b, &c],
        JSON_TRANSPARENT,
        "stability, after X_k and X_j",
    );
    ever.extend(identified(&start, 3, "stability, after X_k and X_j"));
    let root = id_of(&start);
    let k = id_of(member(&start, "k"));
    let j = id_of(member(&start, "j"));

    // Partitioned: R_k and R_j on a; L on b, which has seen neither.
    let r_k = send!(a, js.remove("k"));
    let r_j = send!(a, js.remove("j"));
    let l = send!(b, js.put("k", js.number(inc_float(10.0))));
    let on_b = served(&b, JSON_TRANSPARENT);
    assert_eq!(
        member(&on_b, "k")["value"],
        json!(11.0),
        "stability: b serves X_k's and L's increments: {on_b}"
    );
    assert_eq!(
        id_of(member(&on_b, "k")),
        k,
        "stability, b after L: an increment of `k` moved its identity"
    );
    a.receive(l.clone());
    c.receive(l.clone());

    // T, from c, which has delivered X_k, X_j and L and neither removal. On
    // a, it makes b's three operations stable and neither removal.
    let t = send!(c, js.put("t", js.number(inc_float(1.0))));
    let before = served(&a, JSON_TRANSPARENT);
    a.receive(t.clone());
    assert!(
        stable_through(&a, "b") >= seq(&l) && stable_through(&a, "a") < seq(&r_k),
        "set-up: on a, after T, b's operations up to L ({}) are meant to be causally stable and \
         a's from R_k ({}) on not; a reports {:?}",
        seq(&l),
        seq(&r_k),
        a.stability().stable_version
    );
    let pruned = served(&a, JSON_TRANSPARENT);
    // Three, not four (ST.10): on `a`, R_j had seen X_j, `j`'s one write so
    // far, so `j` is absent until W2 reaches it.
    ever.extend(identified(&pruned, 3, "stability, a after T"));
    assert_eq!(
        without(&pruned, "t"),
        before,
        "stability, a across its pruning: something a serves moved, beyond the member T added"
    );
    let t_id = id_of(member(&pruned, "t"));

    // W2: c increments j, having seen neither removal.
    let w2 = send!(c, js.put("j", js.number(inc_float(5.0))));
    let on_c = served(&c, JSON_TRANSPARENT);
    assert_eq!(
        member(&on_c, "j")["value"],
        json!(7.0),
        "stability: c serves X_j's and W2's increments: {on_c}"
    );
    assert_eq!(
        id_of(member(&on_c, "j")),
        j,
        "stability, c after W2: an increment of `j` moved its identity"
    );
    a.receive(w2.clone());
    let on_a = served(&a, JSON_TRANSPARENT);
    assert_eq!(
        member(&on_a, "j")["value"],
        json!(5.0),
        "stability: W2's increment survives R_j, which had not seen it, and X_j's does not: \
         {on_a}"
    );
    assert_eq!(
        id_of(member(&on_a, "j")),
        j,
        "stability, a after W2: a write into `j` by a writer who had not seen its removal, \
         delivered after a pruned at a stable version the removal is not in, moved its identity"
    );

    // Z: a increments k, having seen R_k and L.
    let z = send!(a, js.put("k", js.number(inc_float(100.0))));
    let on_a = served(&a, JSON_TRANSPARENT);
    assert_eq!(
        member(&on_a, "k")["value"],
        json!(110.0),
        "stability: a serves L's and Z's increments: {on_a}"
    );
    assert_eq!(
        id_of(member(&on_a, "k")),
        k,
        "stability, a after Z: a write into `k`, which L kept alive, written after a pruned at a \
         stable version holding L, moved its identity"
    );

    // Everything to everyone.
    for event in [&r_k, &r_j, &t, &w2, &z] {
        b.receive(event.clone());
    }
    for event in [&r_k, &r_j, &z] {
        c.receive(event.clone());
    }
    let settled = agreed([&a, &b, &c], JSON_TRANSPARENT, "stability, all delivered");
    ever.extend(identified(&settled, 4, "stability, all delivered"));
    assert_eq!(
        (
            member(&settled, "k")["value"].clone(),
            member(&settled, "j")["value"].clone(),
            member(&settled, "t")["value"].clone(),
        ),
        (json!(110.0), json!(5.0), json!(1.0)),
        "stability, all delivered: {settled}"
    );
    assert_eq!(
        [
            id_of(&settled),
            id_of(member(&settled, "k")),
            id_of(member(&settled, "j")),
            id_of(member(&settled, "t")),
        ],
        [root.clone(), k.clone(), j.clone(), t_id.clone()],
        "stability, all delivered: an identity moved"
    );

    // A round of writes to h: every operation above is stable everywhere.
    let marks = [("a", seq(&z)), ("b", seq(&l)), ("c", seq(&w2))];
    round_of_writes(&js, &mut a, &mut b, &mut c);
    for (name, replica) in [("a", &a), ("b", &b), ("c", &c)] {
        for (origin, upto) in marks {
            assert!(
                stable_through(replica, origin) >= upto,
                "set-up: on {name}, {origin}'s operations up to {upto} are meant to be causally \
                 stable after a round of writes from every replica; {name} reports {:?}",
                replica.stability().stable_version
            );
        }
    }
    let stable = agreed([&a, &b, &c], JSON_TRANSPARENT, "stability, all stable");
    ever.extend(identified(&stable, 5, "stability, all stable"));
    assert_eq!(
        without(&stable, "h"),
        settled,
        "stability, across the pruning of every earlier touch: something served moved, beyond \
         the member h added"
    );

    // A write to k from each replica, all of which see it alive.
    for writer in ['a', 'b', 'c'] {
        let bump = vec![js.put("k", js.number(inc_float(1000.0)))];
        match writer {
            'a' => send_all(&mut a, &mut [&mut b, &mut c], bump),
            'b' => send_all(&mut b, &mut [&mut a, &mut c], bump),
            _ => send_all(&mut c, &mut [&mut a, &mut b], bump),
        }
        let now = agreed(
            [&a, &b, &c],
            JSON_TRANSPARENT,
            &format!("stability, after {writer}'s increment of k at stability"),
        );
        assert_eq!(
            id_of(member(&now, "k")),
            k,
            "stability, after {writer}'s increment of `k` at stability: its identity moved"
        );
    }

    // j: removed by b, made stable everywhere, then put again by c.
    let r3 = send!(b, js.remove("j"));
    a.receive(r3.clone());
    c.receive(r3.clone());
    round_of_writes(&js, &mut a, &mut b, &mut c);
    for (name, replica) in [("a", &a), ("b", &b), ("c", &c)] {
        assert!(
            stable_through(replica, "b") >= seq(&r3),
            "set-up: on {name}, b's removal of j ({}) is meant to be causally stable after a \
             round of writes from every replica; {name} reports {:?}",
            seq(&r3),
            replica.stability().stable_version
        );
    }
    let removed = agreed([&a, &b, &c], JSON_TRANSPARENT, "stability, j removed");
    ever.extend(identified(&removed, 4, "stability, j removed"));
    assert_eq!(
        member(&removed, "k")["value"],
        json!(3110.0),
        "stability, j removed: {removed}"
    );
    assert!(
        removed["entry"].get("j").is_none(),
        "stability, j removed: the removal had seen every write to `j`, which must be absent \
         (ST.10), not served emptied: {removed}"
    );
    send_all(
        &mut c,
        &mut [&mut a, &mut b],
        vec![js.put("j", js.number(inc_float(9.0)))],
    );
    let again = agreed([&a, &b, &c], JSON_TRANSPARENT, "stability, j put again");
    identified(&again, 5, "stability, j put again");
    assert_eq!(
        member(&again, "j")["value"],
        json!(9.0),
        "stability, j put again: {again}"
    );
    let fresh = id_of(member(&again, "j"));
    assert!(
        !ever.contains(&fresh),
        "stability, j put again after its removal was stable everywhere: it carries {fresh}, an \
         identity served before"
    );
    assert!(
        !all_ids(&again).contains(&j),
        "stability, j put again: its old identity {j} is still served: {again}"
    );
    assert_eq!(
        id_of(member(&again, "k")),
        k,
        "stability, j put again: `k`'s identity moved"
    );
}

// ---------------------------------------------------------------------------
// O1k: an optional holding a conflict of two classes, unset and set again
// ---------------------------------------------------------------------------

/// The metamodel the conflict needs, with members that an unset empties: an
/// optional containment whose target is abstract with two concrete classes,
/// each holding a counter and nothing else.
///
/// ```text
/// Holder   root; tick: counter, slot: optional Shape
/// Shape    abstract; n: counter
/// A, B     : Shape
/// ```
fn tray_descriptor() -> Value {
    let counter = || json!({"kind": "counter", "num": "i32", "resettable": true});
    descriptor(
        &["Holder"],
        vec![
            (
                "Holder",
                class(
                    false,
                    &[],
                    vec![
                        attribute("tick", json!({"kind": "single"}), counter()),
                        containment("slot", "Shape", "optional"),
                    ],
                ),
            ),
            (
                "Shape",
                class(
                    true,
                    &[],
                    vec![attribute("n", json!({"kind": "single"}), counter())],
                ),
            ),
            ("A", class(false, &["Shape"], vec![])),
            ("B", class(false, &["Shape"], vec![])),
        ],
        json!({}),
    )
}

/// Operations on [`tray_descriptor`]'s model.
struct Tray(Lang);

impl Tray {
    fn new() -> Self {
        Tray(Lang::parse(&tray_descriptor()))
    }

    /// An increment of `Holder.tick`, which creates the root.
    fn tick(&self) -> ModelOp {
        let l = &self.0;
        l.is("Holder", l.at("Holder", "tick", inc_int(1)))
            .into_model_op()
    }

    /// An increment of `n` on the slot's object of `class`, set into the
    /// slot: it reaches the object of that class the slot holds, or makes
    /// one.
    fn bump(&self, class: &str, by: i64) -> ModelOp {
        let l = &self.0;
        let object = l.is(class, l.at(class, "n", inc_int(by)));
        l.is("Holder", l.at("Holder", "slot", InstanceOp::set(object)))
            .into_model_op()
    }

    /// An unset of `Holder.slot`.
    fn unset(&self) -> ModelOp {
        let l = &self.0;
        l.is("Holder", l.at("Holder", "slot", InstanceOp::unset()))
            .into_model_op()
    }
}

/// The two members of a served conflict, found by class: the `A`, then the
/// `B`.
fn members_of<'a>(slot: &'a Value, who: &str) -> (&'a Value, &'a Value) {
    let conflict = slot["__conflict"]
        .as_array()
        .unwrap_or_else(|| panic!("{who}: `slot` is not a conflict: {slot}"));
    assert_eq!(conflict.len(), 2, "{who}: two members: {slot}");
    let one = |class: &str| {
        let hits: Vec<&Value> = conflict
            .iter()
            .filter(|element| element["eClass"] == json!(class))
            .collect();
        assert_eq!(hits.len(), 1, "{who}: one member of class {class}: {slot}");
        hits[0]
    };
    (one("A"), one("B"))
}

/// Partitioned, `a` sets an `A` into `slot` with an increment (X) while `b`
/// sets a `B` (Y); healed, `slot` is a conflict of the two, each under the
/// identity its writer served for it. Then `a` unsets `slot` (U), which every
/// replica delivers: both members are emptied, `slot` reads unset, and
/// neither identity is served. Partitioned again, `b` sets an `A` again (S)
/// and `c` a `B` (T), each having seen U. Each is written into a member that
/// U had emptied and removed, by a writer who saw the removal, so each is a
/// new element: each carries an identity nothing carried before, and only
/// the class written comes back (`slot` on `b` holds its `A` alone, on `c`
/// its `B` alone). Healed, `slot` is a conflict of the two new members, under
/// the identities their writers served; later increments of both move
/// neither.
///
/// `c` delivers X and Y in each order, and `a` delivers S and T in each
/// order; `a` and `b`, and `b` and `c`, deliver their own first.
#[test]
fn si01_an_optional_conflict_unset_and_set_again_brings_back_only_the_class_written_under_a_new_identity()
 {
    let tray = Tray::new();
    for (x_first_at_c, s_first_at_a) in [(true, true), (true, false), (false, true), (false, false)]
    {
        let who = format!(
            "optional conflict unset and set again, c delivers {}, a delivers {}",
            if x_first_at_c { "X then Y" } else { "Y then X" },
            if s_first_at_a { "S then T" } else { "T then S" }
        );
        let (mut a, mut b, mut c) = three(&tray_descriptor());
        send_all(&mut a, &mut [&mut b, &mut c], vec![tray.tick()]);

        // Partitioned: an `A` on a, a `B` on b.
        let x = send!(a, tray.bump("A", 1));
        let y = send!(b, tray.bump("B", 2));
        let on_a = served(&a, OPAQUE);
        identified(&on_a, 2, &format!("{who}, a after X"));
        assert_eq!(
            (&on_a["slot"]["eClass"], &on_a["slot"]["n"]),
            (&json!("A"), &json!(1)),
            "{who}: a serves its `A`: {on_a}"
        );
        let holder = id_of(&on_a);
        let the_a = id_of(&on_a["slot"]);
        let on_b = served(&b, OPAQUE);
        identified(&on_b, 2, &format!("{who}, b after Y"));
        assert_eq!(
            (&on_b["slot"]["eClass"], &on_b["slot"]["n"]),
            (&json!("B"), &json!(2)),
            "{who}: b serves its `B`: {on_b}"
        );
        let the_b = id_of(&on_b["slot"]);

        // Healed: a conflict of the two.
        a.receive(y.clone());
        b.receive(x.clone());
        if x_first_at_c {
            c.receive(x);
            c.receive(y);
        } else {
            c.receive(y);
            c.receive(x);
        }
        let both = agreed([&a, &b, &c], OPAQUE, &format!("{who}, healed"));
        identified(&both, 3, &format!("{who}, healed"));
        let (member_a, member_b) = members_of(&both["slot"], &format!("{who}, healed"));
        assert_eq!(
            (
                id_of(member_a),
                &member_a["n"],
                id_of(member_b),
                &member_b["n"]
            ),
            (the_a.clone(), &json!(1), the_b.clone(), &json!(2)),
            "{who}, healed: each member keeps its writer's identity and increment: {both}"
        );

        // U: a unsets slot, and every replica delivers it.
        send_all(&mut a, &mut [&mut b, &mut c], vec![tray.unset()]);
        let emptied = agreed([&a, &b, &c], OPAQUE, &format!("{who}, after U"));
        identified(&emptied, 1, &format!("{who}, after U"));
        assert!(
            emptied.get("slot").is_none(),
            "{who}: an unset that empties both members leaves `slot` reading unset: {emptied}"
        );
        assert_eq!(
            id_of(&emptied),
            holder,
            "{who}, after U: the root's identity moved"
        );

        // Partitioned again: an `A` set again on b (S), a `B` on c (T).
        let s = send!(b, tray.bump("A", 5));
        let t = send!(c, tray.bump("B", 7));
        let old: BTreeSet<String> = [holder.clone(), the_a.clone(), the_b.clone()].into();
        let on_b = served(&b, OPAQUE);
        identified(&on_b, 2, &format!("{who}, b after S"));
        assert_eq!(
            (&on_b["slot"]["eClass"], &on_b["slot"]["n"]),
            (&json!("A"), &json!(5)),
            "{who}: b serves the `A` it set again, alone, holding S's increment and not X's: \
             {on_b}"
        );
        let new_a = id_of(&on_b["slot"]);
        assert!(
            !old.contains(&new_a),
            "{who}, b after S: the `A` set again after the unset carries {new_a}, an identity \
             served before it"
        );
        let on_c = served(&c, OPAQUE);
        identified(&on_c, 2, &format!("{who}, c after T"));
        assert_eq!(
            (&on_c["slot"]["eClass"], &on_c["slot"]["n"]),
            (&json!("B"), &json!(7)),
            "{who}: c serves the `B` it set again, alone, holding T's increment and not Y's: \
             {on_c}"
        );
        let new_b = id_of(&on_c["slot"]);
        assert!(
            !old.contains(&new_b) && new_b != new_a,
            "{who}, c after T: the `B` set again after the unset carries {new_b}, an identity \
             served before it or given to b's `A`"
        );

        // Healed again.
        b.receive(t.clone());
        c.receive(s.clone());
        if s_first_at_a {
            a.receive(s);
            a.receive(t);
        } else {
            a.receive(t);
            a.receive(s);
        }
        let again = agreed([&a, &b, &c], OPAQUE, &format!("{who}, healed again"));
        identified(&again, 3, &format!("{who}, healed again"));
        let (member_a, member_b) = members_of(&again["slot"], &format!("{who}, healed again"));
        assert_eq!(
            (
                id_of(member_a),
                &member_a["n"],
                id_of(member_b),
                &member_b["n"]
            ),
            (new_a.clone(), &json!(5), new_b.clone(), &json!(7)),
            "{who}, healed again: each member set again keeps the identity its writer served \
             and only its own increment: {again}"
        );
        let ids = all_ids(&again);
        assert!(
            !ids.contains(&the_a) && !ids.contains(&the_b),
            "{who}, healed again: an identity of the unset conflict is still served: {again}"
        );

        // Later increments of both members, from a: C4.
        send_all(
            &mut a,
            &mut [&mut b, &mut c],
            vec![tray.bump("A", 1), tray.bump("B", 1)],
        );
        let later = agreed(
            [&a, &b, &c],
            OPAQUE,
            &format!("{who}, after later increments"),
        );
        identified(&later, 3, &format!("{who}, after later increments"));
        let (member_a, member_b) =
            members_of(&later["slot"], &format!("{who}, after later increments"));
        assert_eq!(
            (
                id_of(member_a),
                &member_a["n"],
                id_of(member_b),
                &member_b["n"]
            ),
            (new_a.clone(), &json!(6), new_b.clone(), &json!(8)),
            "{who}, after later increments: an increment moved a member's identity: {later}"
        );
    }
}

// ---------------------------------------------------------------------------
// OB1, an observation and not a pass criterion
// ---------------------------------------------------------------------------

/// A conflict `[A, B]` in `slot`, as in O1k; partitioned, `a` unsets `slot`
/// (U) while `b` increments its `B` (W). Where U arrives first both members
/// are emptied and `slot` reads unset, and W then brings back the `B` alone;
/// where W arrives first, U leaves the `B` holding W's increment, so the
/// conflict stays, with its emptied `A`. The default read already parted
/// there before SI.01 (the store's optional drops a child that an unset
/// empties). This prints what each replica serves on both reads and asserts
/// nothing; the spec registers what is expected to be printed.
#[test]
#[ignore = "SI.01 round 4 observation OB1, not a pass criterion: run with --ignored --nocapture"]
fn si01_observation_an_optional_conflict_unset_raced_by_a_write_to_one_member() {
    let tray = Tray::new();
    for write_first_at_c in [false, true] {
        let schedule = format!(
            "c delivers {}",
            if write_first_at_c {
                "W then U"
            } else {
                "U then W"
            }
        );
        let (mut a, mut b, mut c) = three(&tray_descriptor());
        send_all(&mut a, &mut [&mut b, &mut c], vec![tray.tick()]);
        let x = send!(a, tray.bump("A", 1));
        let y = send!(b, tray.bump("B", 2));
        a.receive(y.clone());
        b.receive(x.clone());
        c.receive(x);
        c.receive(y);

        let u = send!(a, tray.unset());
        let w = send!(b, tray.bump("B", 10));
        a.receive(w.clone());
        b.receive(u.clone());
        if write_first_at_c {
            c.receive(w);
            c.receive(u);
        } else {
            c.receive(u);
            c.receive(w);
        }

        let mut defaults = Vec::new();
        let mut identifieds = Vec::new();
        for (name, replica) in [("a", &a), ("b", &b), ("c", &c)] {
            let default = default_read(replica);
            let with_ids = <ModelLog as QueryableLog>::query_identified_state_json(replica);
            println!("SI.01-OBSERVATION OB1 {schedule}: {name} default read {default}");
            println!("SI.01-OBSERVATION OB1 {schedule}: {name} identified read {with_ids}");
            println!(
                "SI.01-OBSERVATION OB1 {schedule}: {name} default read equals the identified \
                 read stripped: {}",
                strip(&with_ids, OPAQUE) == default
            );
            defaults.push(default);
            identifieds.push(with_ids);
        }
        println!(
            "SI.01-OBSERVATION OB1 {schedule}: the three default reads agree: {}",
            defaults.windows(2).all(|pair| pair[0] == pair[1])
        );
        println!(
            "SI.01-OBSERVATION OB1 {schedule}: the three identified reads agree: {}",
            identifieds.windows(2).all(|pair| pair[0] == pair[1])
        );
    }
}
