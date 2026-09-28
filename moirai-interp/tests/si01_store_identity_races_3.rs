//! SI.01, round 5: two races of round 2's touch mechanism (Moirai `f4d3648`:
//! every operation that reaches an object is one of its touches, a removal
//! covers the touches it had seen, and `forget_stable_touches` prunes them at
//! causal stability) that round 4 left with no scenario, read in process
//! through the identified read, `QueryableLog::query_identified_state_json`.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.01.md`, section "Round 5,
//! written before any rerun" (O1l and O1m); the contract these tests read,
//! contract R', is restated in `si01/mod.rs`. Every read here is also held to
//! the other half of R', as in `si01_store_identity.rs`: the default read is
//! the identified read with the identities stripped.
//!
//! O1l races a `MapOp::Clear` of a JSON object with an update of one of its
//! members. O1m has a fourth replica join by state transfer from a donor that
//! has pruned its touches at causal stability, through the serialization a
//! `model_node` applies to a transfer, and then writes on the joiner and
//! delivers writes to it.
//!
//! What each test expects is derived from its schedule and nothing else. An
//! element keeps the identity its own writer served for it before any
//! concurrent operation reached that writer, and a write into an element its
//! writer saw alive moves nothing (C4). An element removed and then written
//! again by a writer who had seen the removal, and no write the removal had
//! not seen, is a new element: its identity is one nothing carried before,
//! and the old one is served nowhere (C5). Every replica that has delivered
//! the same operations serves the same identified document (C2), a joiner by
//! state transfer included. A check whose message starts with `set-up:`
//! checks the schedule itself (where causal stability stands, that the
//! transfer went through), not the claim.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si01_store_identity_races_3 -- --test-threads=1
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;

use std::collections::BTreeSet;

use moirai_interp::{InstanceOp, ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_network::state_transfer::{LogPayload, TransferableLog};
use moirai_network::transport::TransportMessage;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Value, json};

use si01::*;

/// The identified read of this replica of a JSON model, held to R': the
/// default read is the identified read stripped.
fn served(replica: &Rep) -> Value {
    let doc = <ModelLog as QueryableLog>::query_identified_state_json(replica);
    assert_eq!(
        strip(&doc, JSON_TRANSPARENT),
        default_read(replica),
        "the default read is not the identified read with the identities stripped; \
         identified: {doc}"
    );
    doc
}

/// The one identified document the named replicas serve, held to C2.
fn agreed(replicas: &[(&str, &Rep)], who: &str) -> Value {
    let (first, one) = replicas[0];
    let doc = served(one);
    for (name, other) in &replicas[1..] {
        assert_eq!(
            doc,
            served(other),
            "{who}: {first} and {name} serve different documents"
        );
    }
    doc
}

// ---------------------------------------------------------------------------
// O1l: a clear of a JSON object raced by an update of one of its members
// ---------------------------------------------------------------------------

/// `a` puts two numbers, `k` and `g` (X), and every replica delivers them.
/// Partitioned, `a` clears the root object (C) and `b` increments `k` (W).
/// C removes every member it had seen, as a key removal does
/// (`uw_map.rs:157-163`); W was written into a member its writer saw, so it
/// is an update and creates nothing, and a write concurrent with a removal
/// survives it: after the heal every replica serves `k`, holding W's
/// increment and not X's, under the identity every replica served for it
/// after X, and `g` emptied. An increment of `k` written after the heal (Z)
/// moves no identity. A number put under `g` after the heal (P), by a writer
/// who had delivered C, is a write into a member the clear had removed and
/// nothing had written into since: a new element, under an identity nothing
/// carried before, and `g`'s old identity is served nowhere.
///
/// C and W reach a replica in one of two orders. `a` delivers C first and `b`
/// delivers W first, and `c` takes each order in turn. Z and P are written on
/// each of the three replicas in turn.
#[test]
fn si01_a_clear_raced_by_an_update_keeps_the_updated_members_identity_in_both_delivery_orders() {
    let js = Json::new();
    for update_first_at_c in [false, true] {
        for writer in ['a', 'b', 'c'] {
            let who = format!(
                "json object cleared while a member is updated, c delivers {}, Z and P written \
                 on {writer}",
                if update_first_at_c {
                    "W then C"
                } else {
                    "C then W"
                }
            );
            let (mut a, mut b, mut c) = three(&json_descriptor());
            let mut ever: BTreeSet<String> = BTreeSet::new();

            // X: the root object and two numbers, from a.
            send_all(
                &mut a,
                &mut [&mut b, &mut c],
                vec![
                    js.put("k", js.number(inc_float(1.0))),
                    js.put("g", js.number(inc_float(2.0))),
                ],
            );
            let start = agreed(
                &[("a", &a), ("b", &b), ("c", &c)],
                &format!("{who}, after X"),
            );
            ever.extend(identified(&start, 3, &format!("{who}, after X")));
            let root = id_of(&start);
            let k = id_of(member(&start, "k"));
            let g = id_of(member(&start, "g"));

            // Partitioned: C on a, W on b.
            let clear = send!(a, js.root(InstanceOp::clear()));
            let on_a = served(&a);
            ever.extend(identified(&on_a, 3, &format!("{who}, a after C")));
            assert_eq!(
                (
                    member(&on_a, "k")["value"].clone(),
                    member(&on_a, "g")["value"].clone()
                ),
                (json!(0.0), json!(0.0)),
                "{who}: a serves both members emptied by its clear: {on_a}"
            );
            let w = send!(b, js.put("k", js.number(inc_float(10.0))));
            let on_b = served(&b);
            ever.extend(identified(&on_b, 3, &format!("{who}, b after W")));
            assert_eq!(
                member(&on_b, "k")["value"],
                json!(11.0),
                "{who}: b serves X's and W's increments: {on_b}"
            );
            assert_eq!(
                id_of(member(&on_b, "k")),
                k,
                "{who}, b after W: an increment of `k` moved its identity"
            );

            // Healed, each side delivering the other's operation.
            a.receive(w.clone());
            b.receive(clear.clone());
            if update_first_at_c {
                c.receive(w);
                c.receive(clear);
            } else {
                c.receive(clear);
                c.receive(w);
            }
            let healed = agreed(
                &[("a", &a), ("b", &b), ("c", &c)],
                &format!("{who}, healed"),
            );
            ever.extend(identified(&healed, 3, &format!("{who}, healed")));
            assert_eq!(
                (
                    member(&healed, "k")["value"].clone(),
                    member(&healed, "g")["value"].clone(),
                ),
                (json!(10.0), json!(0.0)),
                "{who}: W's increment survives the concurrent clear and X's do not: {healed}"
            );
            assert_eq!(
                id_of(member(&healed, "k")),
                k,
                "{who}, healed: the member W wrote into no longer carries the identity it was \
                 served under after X"
            );
            assert_eq!(
                id_of(&healed),
                root,
                "{who}, healed: the root object's identity moved"
            );

            // Z, then P, from a replica that has delivered both C and W,
            // each delivered to the other two.
            let z = vec![js.put("k", js.number(inc_float(100.0)))];
            match writer {
                'a' => send_all(&mut a, &mut [&mut b, &mut c], z),
                'b' => send_all(&mut b, &mut [&mut a, &mut c], z),
                _ => send_all(&mut c, &mut [&mut a, &mut b], z),
            }
            let after = agreed(
                &[("a", &a), ("b", &b), ("c", &c)],
                &format!("{who}, after Z"),
            );
            ever.extend(identified(&after, 3, &format!("{who}, after Z")));
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

            let p = vec![js.put("g", js.number(inc_float(9.0)))];
            match writer {
                'a' => send_all(&mut a, &mut [&mut b, &mut c], p),
                'b' => send_all(&mut b, &mut [&mut a, &mut c], p),
                _ => send_all(&mut c, &mut [&mut a, &mut b], p),
            }
            let again = agreed(
                &[("a", &a), ("b", &b), ("c", &c)],
                &format!("{who}, after P"),
            );
            identified(&again, 3, &format!("{who}, after P"));
            assert_eq!(
                member(&again, "g")["value"],
                json!(9.0),
                "{who}: P's increment is all `g` holds after the clear: {again}"
            );
            let fresh = id_of(member(&again, "g"));
            assert!(
                !ever.contains(&fresh),
                "{who}, after P: `g`, put again by a writer who had delivered the clear and with \
                 nothing written into it since, carries {fresh}, an identity served before"
            );
            assert!(
                !all_ids(&again).contains(&g),
                "{who}, after P: `g`'s old identity {g} is still served: {again}"
            );
            assert_eq!(
                (id_of(&again), id_of(member(&again, "k"))),
                (root.clone(), k.clone()),
                "{who}, after P: the root's or `k`'s identity moved"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// O1m: a joiner by state transfer from a donor that pruned its touches
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

/// A state transfer from `donor` to `joiner`, through the serialization a
/// `model_node` applies to one (`GenericNode::state_response_for` and
/// `GenericNode::adopt_state`, `moirai-network/src/generic.rs`): the donor's
/// log exported with `TransferableLog::export_log`, serialized, and encoded as
/// the `LogPayload` of a `StateResponse` beside the donor's causal snapshot;
/// the frame written as one line of JSON, as a transport sends it, and read
/// back; the payload decoded, the log imported with
/// `TransferableLog::import_log`, and both adopted. Then the joiner's delta
/// sync with its donor, which `adopt_state` asks for right after adopting:
/// the donor answers from its outbox, and takes the joiner into its member
/// table as it does. Only the socket is left out.
fn transfer(donor: &mut Rep, joiner: &mut Rep) {
    let exported = <ModelLog as TransferableLog>::export_log(donor.log());
    let raw = serde_json::to_vec(&exported).expect("set-up: the donor's log serializes");
    let frame = TransportMessage::<ModelOp>::StateResponse {
        snapshot: donor.snapshot(),
        log: LogPayload::encode(exported, &raw),
        log_id: donor.log_id().clone(),
    };
    let line = serde_json::to_string(&frame).expect("set-up: the state response serializes");
    let TransportMessage::StateResponse {
        snapshot,
        log,
        log_id,
    } = serde_json::from_str::<TransportMessage<ModelOp>>(&line)
        .expect("set-up: the state response is read back")
    else {
        panic!("set-up: the frame read back is not a state response");
    };
    assert!(
        &log_id == donor.log_id(),
        "set-up: the state response is for another log"
    );
    let state = log
        .decode()
        .and_then(<ModelLog as TransferableLog>::import_log)
        .expect("set-up: the joiner decodes and imports the donor's log");
    joiner.adopt(snapshot, state);
    joiner.receive_batch(donor.pull(joiner.since()));
}

/// One increment of the number `h` from each replica in turn, each delivered
/// to all the others before the next is written: afterwards, every operation
/// any replica had delivered before the round is causally stable on all of
/// them.
fn round_of_writes(js: &Json, reps: &mut [&mut Rep]) {
    for writer in 0..reps.len() {
        let event = send!(reps[writer], js.put("h", js.number(inc_float(1.0))));
        for (other, replica) in reps.iter_mut().enumerate() {
            if other != writer {
                replica.receive(event.clone());
            }
        }
    }
}

/// A joiner by state transfer, from a donor that has pruned its touches at a
/// causally stable version, finds for every later write what the donor
/// finds.
///
/// `b` puts three numbers, `k`, `j` and `g` (X_k, X_j, X_g), and every
/// replica delivers them. Then `a` removes all three (R_k, R_j, R_g), and
/// `b`, which has seen no removal, increments `k` (L); `a` and `c` deliver L,
/// and `c`, which has seen no removal either, puts a fourth number `t` (T),
/// which reaches `a` alone. When T arrives, `a` knows that every replica has
/// delivered `b`'s four operations and that `c` has delivered no removal:
/// `b`'s four are stable on `a` and the removals are not, and `a` prunes. `k`
/// keeps L and loses X_k (the later touch of one replica says what the
/// earlier one did); `j` and `g` keep X_j and X_g, each seen by a removal
/// that is not stable. That pruned list is what a fourth replica, `d`, adopts
/// from `a`.
///
/// - `c` increments `j` (W2) without having seen R_j, and `d` delivers it:
///   for W2, X_j is seen by no removal in its past, so `j` is alive, W2 is an
///   update and `j` keeps its identity, as it does on `a`.
/// - `d` increments `k` (Z): L is seen by no removal, so `k` is alive for Z
///   and keeps its identity.
/// - `d` puts a number under `g` (P): X_g, `g`'s only touch, was seen by R_g,
///   which `d` holds in the state it adopted, so `g` is not alive for P: P is
///   a creation, and `g` takes an identity nothing carried before, as it does
///   on `a`.
///
/// Then everything reaches everyone, and a round of writes to a fifth number
/// `h` from all four makes every earlier operation stable on all four, the
/// joiner included; the pruning that follows moves nothing served, and a
/// write to `k` from the joiner and one to `g` from `b` move no identity.
#[test]
fn si01_a_joiner_by_state_transfer_from_a_donor_that_pruned_its_touches_finds_what_the_donor_finds()
{
    let js = Json::new();
    let (mut a, mut b, mut c) = three(&json_descriptor());
    let mut ever: BTreeSet<String> = BTreeSet::new();

    // X_k, X_j and X_g, from b, delivered everywhere.
    send_all(
        &mut b,
        &mut [&mut a, &mut c],
        vec![
            js.put("k", js.number(inc_float(1.0))),
            js.put("j", js.number(inc_float(2.0))),
            js.put("g", js.number(inc_float(3.0))),
        ],
    );
    let start = agreed(
        &[("a", &a), ("b", &b), ("c", &c)],
        "joiner, after X_k, X_j and X_g",
    );
    ever.extend(identified(&start, 4, "joiner, after X_k, X_j and X_g"));
    let root = id_of(&start);
    let k = id_of(member(&start, "k"));
    let j = id_of(member(&start, "j"));
    let g = id_of(member(&start, "g"));

    // Partitioned: the three removals on a; L on b, which has seen none.
    let r_k = send!(a, js.remove("k"));
    let r_j = send!(a, js.remove("j"));
    let r_g = send!(a, js.remove("g"));
    let l = send!(b, js.put("k", js.number(inc_float(10.0))));
    let on_b = served(&b);
    assert_eq!(
        member(&on_b, "k")["value"],
        json!(11.0),
        "joiner: b serves X_k's and L's increments: {on_b}"
    );
    assert_eq!(
        id_of(member(&on_b, "k")),
        k,
        "joiner, b after L: an increment of `k` moved its identity"
    );
    a.receive(l.clone());
    c.receive(l.clone());

    // T, from c, which has delivered b's four operations and no removal. On
    // a, it makes b's four stable and no removal.
    let t = send!(c, js.put("t", js.number(inc_float(1.0))));
    let before = served(&a);
    a.receive(t.clone());
    assert!(
        stable_through(&a, "b") >= seq(&l) && stable_through(&a, "a") < seq(&r_k),
        "set-up: on a, after T, b's operations up to L ({}) are meant to be causally stable and \
         a's from R_k ({}) on not; a reports {:?}",
        seq(&l),
        seq(&r_k),
        a.stability().stable_version
    );
    let pruned = served(&a);
    ever.extend(identified(&pruned, 5, "joiner, a after T"));
    assert_eq!(
        without(&pruned, "t"),
        before,
        "joiner, a across its pruning: something a serves moved, beyond the member T added"
    );
    assert_eq!(
        (
            member(&pruned, "k")["value"].clone(),
            member(&pruned, "j")["value"].clone(),
            member(&pruned, "g")["value"].clone(),
        ),
        (json!(10.0), json!(0.0), json!(0.0)),
        "joiner: a serves L's increment of `k` and `j` and `g` emptied: {pruned}"
    );
    let t_id = id_of(member(&pruned, "t"));

    // d joins by state transfer from a.
    let mut d: Rep = Replica::bootstrap_with_log_id("d".to_string(), &["d"], a.log_id().clone());
    transfer(&mut a, &mut d);
    assert!(
        stable_through(&d, "b") >= seq(&l) && stable_through(&d, "a") < seq(&r_k),
        "set-up: on d, after adopting a's state, b's operations up to L ({}) are meant to be \
         causally stable and a's from R_k ({}) on not, as on a; d reports {:?}",
        seq(&l),
        seq(&r_k),
        d.stability().stable_version
    );
    let joined = agreed(
        &[("a", &a), ("d", &d)],
        "joiner, d after adopting a's state",
    );
    assert_eq!(
        joined, pruned,
        "joiner: what a serves moved across the transfer"
    );

    // W2: c increments j, having seen no removal; d delivers it, then a.
    let w2 = send!(c, js.put("j", js.number(inc_float(5.0))));
    let on_c = served(&c);
    assert_eq!(
        member(&on_c, "j")["value"],
        json!(7.0),
        "joiner: c serves X_j's and W2's increments: {on_c}"
    );
    assert_eq!(
        id_of(member(&on_c, "j")),
        j,
        "joiner, c after W2: an increment of `j` moved its identity"
    );
    d.receive(w2.clone());
    let on_d = served(&d);
    assert_eq!(
        member(&on_d, "j")["value"],
        json!(5.0),
        "joiner: on d, W2's increment survives R_j, which had not seen it, and X_j's does not: \
         {on_d}"
    );
    assert_eq!(
        id_of(member(&on_d, "j")),
        j,
        "joiner, d after W2: a write into `j` by a writer who had not seen its removal, \
         delivered to a joiner that adopted a touch list pruned at a stable version the removal \
         is not in, moved its identity"
    );
    a.receive(w2.clone());
    agreed(&[("a", &a), ("d", &d)], "joiner, after W2");

    // Z: d increments k, having adopted R_k and L.
    let z = send!(d, js.put("k", js.number(inc_float(100.0))));
    let on_d = served(&d);
    assert_eq!(
        member(&on_d, "k")["value"],
        json!(110.0),
        "joiner: d serves L's and Z's increments: {on_d}"
    );
    assert_eq!(
        id_of(member(&on_d, "k")),
        k,
        "joiner, d after Z: a write on the joiner into `k`, which the touch L the joiner adopted \
         keeps alive, moved its identity"
    );
    a.receive(z.clone());
    agreed(&[("a", &a), ("d", &d)], "joiner, after Z");

    // P: d puts a number under g, having adopted R_g.
    let p = send!(d, js.put("g", js.number(inc_float(9.0))));
    let on_d = served(&d);
    identified(&on_d, 5, "joiner, d after P");
    assert_eq!(
        member(&on_d, "g")["value"],
        json!(9.0),
        "joiner: P's increment is all `g` holds after R_g: {on_d}"
    );
    let fresh = id_of(member(&on_d, "g"));
    assert!(
        !ever.contains(&fresh),
        "joiner, d after P: `g`, whose removal R_g the joiner holds in the state it adopted, put \
         again on the joiner, carries {fresh}, an identity served before"
    );
    assert!(
        !all_ids(&on_d).contains(&g),
        "joiner, d after P: `g`'s old identity {g} is still served: {on_d}"
    );
    a.receive(p.clone());
    agreed(&[("a", &a), ("d", &d)], "joiner, after P");

    // Everything to everyone.
    for event in [&r_k, &r_j, &r_g, &t, &w2, &z, &p] {
        b.receive(event.clone());
    }
    for event in [&r_k, &r_j, &r_g, &z, &p] {
        c.receive(event.clone());
    }
    let settled = agreed(
        &[("a", &a), ("b", &b), ("c", &c), ("d", &d)],
        "joiner, all delivered",
    );
    ever.extend(identified(&settled, 5, "joiner, all delivered"));
    assert_eq!(
        (
            member(&settled, "k")["value"].clone(),
            member(&settled, "j")["value"].clone(),
            member(&settled, "g")["value"].clone(),
            member(&settled, "t")["value"].clone(),
        ),
        (json!(110.0), json!(5.0), json!(9.0), json!(1.0)),
        "joiner, all delivered: {settled}"
    );
    assert_eq!(
        [
            id_of(&settled),
            id_of(member(&settled, "k")),
            id_of(member(&settled, "j")),
            id_of(member(&settled, "g")),
            id_of(member(&settled, "t")),
        ],
        [
            root.clone(),
            k.clone(),
            j.clone(),
            fresh.clone(),
            t_id.clone()
        ],
        "joiner, all delivered: an identity moved"
    );

    // A round of writes to h from all four: every operation above is stable
    // on all four, the joiner included.
    let marks = [
        ("a", seq(&r_g)),
        ("b", seq(&l)),
        ("c", seq(&w2)),
        ("d", seq(&p)),
    ];
    round_of_writes(&js, &mut [&mut a, &mut b, &mut c, &mut d]);
    for (name, replica) in [("a", &a), ("b", &b), ("c", &c), ("d", &d)] {
        for (origin, upto) in marks {
            assert!(
                stable_through(replica, origin) >= upto,
                "set-up: on {name}, {origin}'s operations up to {upto} are meant to be causally \
                 stable after a round of writes from every replica; {name} reports {:?}",
                replica.stability().stable_version
            );
        }
    }
    let stable = agreed(
        &[("a", &a), ("b", &b), ("c", &c), ("d", &d)],
        "joiner, all stable",
    );
    identified(&stable, 6, "joiner, all stable");
    assert_eq!(
        without(&stable, "h"),
        settled,
        "joiner, across the pruning of every earlier touch on all four: something served moved, \
         beyond the member h added"
    );

    // A write to k from the joiner, and one to g from b.
    send_all(
        &mut d,
        &mut [&mut a, &mut b, &mut c],
        vec![js.put("k", js.number(inc_float(1000.0)))],
    );
    let now = agreed(
        &[("a", &a), ("b", &b), ("c", &c), ("d", &d)],
        "joiner, after d's increment of k at stability",
    );
    assert_eq!(
        member(&now, "k")["value"],
        json!(1110.0),
        "joiner: d's increment of `k` at stability: {now}"
    );
    assert_eq!(
        id_of(member(&now, "k")),
        k,
        "joiner, after d's increment of `k` at stability: its identity moved"
    );
    send_all(
        &mut b,
        &mut [&mut a, &mut c, &mut d],
        vec![js.put("g", js.number(inc_float(1000.0)))],
    );
    let last = agreed(
        &[("a", &a), ("b", &b), ("c", &c), ("d", &d)],
        "joiner, after b's increment of g at stability",
    );
    assert_eq!(
        member(&last, "g")["value"],
        json!(1009.0),
        "joiner: b's increment of `g` at stability: {last}"
    );
    assert_eq!(
        id_of(member(&last, "g")),
        fresh,
        "joiner, after b's increment of `g` at stability: its identity moved"
    );
}
