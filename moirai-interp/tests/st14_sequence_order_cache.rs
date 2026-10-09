//! ST.14: a sequence's order kept incrementally, so a positional operation on
//! a list whose whole history the operation has seen does not replay that
//! list.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.14.md`; its section 3 says
//! what each test is the oracle for. In short:
//!
//! - `st14_s*`, cost on the sequential path (claim part 2): heap allocations
//!   per operation, counted by the test binary's own allocator
//!   ([`st14::Counting`]), stay flat while one list's history grows, at the
//!   writer and at a receiver.
//! - `st14_c*`, concurrency (part 3): hand-computed outcomes of operations
//!   whose version does not include everything the list holds, delivered in
//!   both orders.
//! - `st14_t*`, state transfer (part 4): a joiner that adopted a serde
//!   round-trip of a donor's `ModelLog` resolves a concurrent position as the
//!   donor does, serves what the donor serves, and its own sequential writes
//!   stay flat.
//! - `st14_r*`, random schedules (part 3): three replicas and a joiner by
//!   state transfer, seeded, every replica's served state after every step
//!   held to the digest `f14f969` produced for the same schedule.
//!
//! The M5 stream (part 2's 107-instance build, and part 3's byte-equality on
//! it) is `st14_m5_build.rs`. The registered command runs both files in
//! release, since at `f14f969` the replays make the M5 build take minutes:
//!
//! ```bash
//! nice -n 10 cargo test -j 2 --offline --release -p moirai-interp \
//!     --test st14_sequence_order_cache --test st14_m5_build --no-fail-fast -- --nocapture
//! ```

mod st14;

use st14::*;

use moirai_interp::ModelLog;
use moirai_protocol::replica::IsReplica;

// ===========================================================================
// Part 2 on one list: the sequential path does not replay
// ===========================================================================

/// How many sequential updates the cost tests write to one list.
const HISTORY: usize = 1_200;
/// The early and late windows the means are taken over, by operation index.
const EARLY: std::ops::Range<usize> = 100..300;
const LATE: std::ops::Range<usize> = 1_000..1_200;
/// The registered bound on the late window's mean over the early window's.
const FLAT: f64 = 1.5;

/// One item in `Root.items`, then `HISTORY` updates of it at position 0, each
/// delivered to the receiver before the next is written. Answers the
/// allocations of each `send` at the writer and of each `receive` at the
/// receiver.
fn sequential_updates() -> (Vec<u64>, Vec<u64>, Rep, Rep) {
    let (mut a, mut b) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
    let opening = send(&mut a, seq_install());
    b.receive(opening);
    let first = send(&mut a, insert_item(0, 1));
    b.receive(first);

    let mut writer = Vec::with_capacity(HISTORY);
    let mut receiver = Vec::with_capacity(HISTORY);
    for _ in 0..HISTORY {
        let (event, at_a) = counted(|| send(&mut a, bump_item(0, 1)));
        writer.push(at_a);
        let ((), at_b) = counted(|| b.receive(event));
        receiver.push(at_b);
    }
    (writer, receiver, a, b)
}

#[test]
fn st14_s1_sequential_updates_of_one_list_stay_flat_at_the_writer_and_the_receiver() {
    let (writer, receiver, a, b) = sequential_updates();

    let mut failures = Vec::new();
    // The premise: both replicas hold the one item, updated HISTORY + 1 times.
    let expected = vec![(1 + HISTORY as i64, Vec::new())];
    if projection(&a) != expected || projection(&b) != expected {
        failures.push(format!(
            "set-up: after {HISTORY} updates the two replicas serve {:?} and {:?}, not {expected:?}",
            projection(&a),
            projection(&b)
        ));
    }
    for (who, counts) in [("writer", &writer), ("receiver", &receiver)] {
        let early = mean(&counts[EARLY]);
        let late = mean(&counts[LATE]);
        let ratio = late / early;
        eprintln!(
            "st14_s1 {who}: mean allocations per operation {early:.1} at operations {EARLY:?}, \
             {late:.1} at {LATE:?}, ratio {ratio:.3} (bound {FLAT})"
        );
        if ratio > FLAT {
            failures.push(format!(
                "{who}: the late window's mean ({late:.1} allocations per operation, operations \
                 {LATE:?}) is {ratio:.2} times the early window's ({early:.1}, operations \
                 {EARLY:?}), over the bound {FLAT}: the cost of one update grows with the \
                 list's history"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ===========================================================================
// Part 3 by hand: an operation whose version does not include everything the
// list holds is resolved in the state its writer saw
// ===========================================================================

/// Three replicas of one `Seq` model, `setup` written at `a` and delivered to
/// `b` and `c`. Each replica indexes the three members its own way (its own id
/// first), so every event that crosses is re-indexed on arrival.
fn three(setup: &[ModelOp]) -> (Rep, Rep, Rep) {
    let (mut a, mut b, mut c) = moirai_crdt::utils::membership::triplet_log::<ModelLog>();
    for op in std::iter::once(seq_install()).chain(setup.iter().cloned()) {
        let event = send(&mut a, op);
        b.receive(event.clone());
        c.receive(event);
    }
    (a, b, c)
}

/// `x` written at `a` and `y` at `b` concurrently; `a` takes `y`, `b` takes
/// `x`, and `c` takes both, `x` first or `y` first. Then `after` is written at
/// `a`, one operation at a time, and each is delivered to `b` and `c`.
fn concurrent_pair(
    setup: &[ModelOp],
    x: ModelOp,
    y: ModelOp,
    x_first_at_c: bool,
    after: &[ModelOp],
) -> [(&'static str, Rep); 3] {
    let (mut a, mut b, mut c) = three(setup);
    let from_a = send(&mut a, x);
    let from_b = send(&mut b, y);
    a.receive(from_b.clone());
    b.receive(from_a.clone());
    if x_first_at_c {
        c.receive(from_a);
        c.receive(from_b);
    } else {
        c.receive(from_b);
        c.receive(from_a);
    }
    for op in after {
        let event = send(&mut a, op.clone());
        b.receive(event.clone());
        c.receive(event);
    }
    [("a", a), ("b", b), ("c", c)]
}

/// Every replica of every delivery order serves `expected`, and the three
/// replicas of one order serve the same bytes.
fn check_pair(
    what: &str,
    setup: &[ModelOp],
    x: ModelOp,
    y: ModelOp,
    after: &[ModelOp],
    expected: &[(i64, Vec<i64>)],
) {
    let mut failures = Vec::new();
    for x_first_at_c in [true, false] {
        let order = if x_first_at_c {
            "x then y at c"
        } else {
            "y then x at c"
        };
        let replicas = concurrent_pair(setup, x.clone(), y.clone(), x_first_at_c, after);
        let bytes = served(&replicas[0].1);
        for (who, replica) in &replicas {
            let got = projection(replica);
            if got != expected {
                failures.push(format!(
                    "{what}, {order}: {who} serves {got:?}, not {expected:?}"
                ));
            }
            if served(replica) != bytes {
                failures.push(format!(
                    "{what}, {order}: {who} serves {} where a serves {bytes}",
                    served(replica)
                ));
            }
            if replica.state().unresolved() != 0 {
                failures.push(format!(
                    "{what}, {order}: {who} could not route {} operation(s)",
                    replica.state().unresolved()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `items` = [P(10), Q(20)].
fn p_and_q() -> Vec<ModelOp> {
    vec![insert_item(0, 10), insert_item(1, 20)]
}

#[test]
fn st14_c1_an_update_concurrent_with_an_insert_ahead_of_it_reaches_the_element_its_writer_saw() {
    // b bumps Q (position 1 in [P, Q]) while a puts R in front. At a, the
    // bump arrives at [R, P, Q], where position 1 is P: the order its writer
    // saw is [P, Q], so it is Q that is bumped.
    check_pair(
        "c1",
        &p_and_q(),
        insert_item(0, 30),
        bump_item(1, 1),
        &[],
        &[(30, vec![]), (10, vec![]), (21, vec![])],
    );
}

#[test]
fn st14_c2_a_delete_concurrent_with_an_insert_ahead_of_it_removes_the_element_its_writer_saw() {
    // a deletes Q (position 1 in [P, Q]) while b puts R in front. At b, the
    // delete arrives at [R, P, Q], where position 1 is P.
    check_pair(
        "c2",
        &p_and_q(),
        delete_item(1),
        insert_item(0, 30),
        &[],
        &[(30, vec![]), (10, vec![])],
    );
}

#[test]
fn st14_c3_an_update_concurrent_with_the_delete_of_its_element_keeps_it() {
    // a deletes P while b bumps it: update-wins keeps P, with b's increment
    // only (the delete resets what it had seen, P's 10). At a, the bump
    // arrives at [Q], where position 0 is Q; its writer saw [P, Q].
    check_pair(
        "c3",
        &p_and_q(),
        delete_item(0),
        bump_item(0, 5),
        &[],
        &[(5, vec![]), (20, vec![])],
    );
}

#[test]
fn st14_c4_a_part_update_concurrent_with_the_delete_of_its_item_keeps_that_part_only() {
    // P holds parts [x(1), y(2)]. a deletes P while b bumps x by 5. P stays
    // (b's operation updated it), reset to what a had not seen: n reads 0,
    // y is gone, x keeps b's 5. At a, the bump reaches P through [Q] and x
    // through a list the delete emptied; its writer saw [P, Q] and [x, y].
    check_pair(
        "c4",
        &[
            insert_item(0, 10),
            insert_part(0, 0, 1),
            insert_part(0, 1, 2),
            insert_item(1, 20),
        ],
        delete_item(0),
        bump_part(0, 0, 5),
        &[],
        &[(0, vec![5]), (20, vec![])],
    );
}

#[test]
fn st14_c5_sequential_operations_after_two_concurrent_inserts_land_where_the_writer_sees_them() {
    // a puts R(30) and b puts S(40), both at position 1 of [P, Q]. Every
    // replica orders them the same way, which f14f969 decides as [P, R, S,
    // Q]. Then a, having seen both, writes one operation at a time, each
    // delivered to b and c before the next: it bumps positions 1 and 2 (R and
    // S), deletes position 0 (P) and puts T(50) at position 1, between R and
    // S. The last two are sequential too, and nothing concurrent follows
    // them, so what every replica serves at the end is what they did.
    check_pair(
        "c5",
        &p_and_q(),
        insert_item(1, 30),
        insert_item(1, 40),
        &[
            bump_item(1, 100),
            bump_item(2, 1_000),
            delete_item(0),
            insert_item(1, 50),
        ],
        &[(130, vec![]), (50, vec![]), (1_040, vec![]), (20, vec![])],
    );
}

// ===========================================================================
// Part 4: a joiner by state transfer
// ===========================================================================

/// A joiner by state transfer from `donor`, as `model_node` runs one: the
/// donor takes the joiner into its member table, then its snapshot and its log
/// cross a serde round-trip (`TransferableLog::export_log` is
/// `serde_json::to_value` of the log, `import_log` its inverse), and the
/// joiner adopts both.
fn transfer(donor: &mut Rep, joiner_id: &str) -> Rep {
    use moirai_protocol::broadcast::internalizer::lock_interner;
    use moirai_protocol::broadcast::tcsb::{IsTcsbTest, StateSnapshot};
    use moirai_protocol::replica::Replica;

    let mut joiner: Rep =
        Replica::bootstrap_with_log_id(joiner_id.to_string(), &[joiner_id], donor.log_id().clone());
    lock_interner(donor.tcsb().interner()).intern(joiner.id());
    let snapshot: StateSnapshot<moirai_interp::ModelOp> = serde_json::from_str(
        &serde_json::to_string(&donor.snapshot()).expect("a snapshot serializes"),
    )
    .expect("and comes back");
    let log: ModelLog =
        serde_json::from_value(serde_json::to_value(donor.log()).expect("a log serializes"))
            .expect("and comes back");
    joiner.adopt(snapshot, log);
    joiner
}

#[test]
fn st14_t1_a_joiner_resolves_a_concurrent_position_in_the_state_its_writer_saw() {
    // b bumps Q (position 1 of [P, Q]) while a puts R in front; j joins from
    // a, holding R and not the bump, and then takes the bump: position 1 of
    // j's [R, P, Q] is P, and the bump's writer saw Q there.
    let (mut a, mut b, _c) = three(&p_and_q());
    let bump = send(&mut b, bump_item(1, 1));
    let insert = send(&mut a, insert_item(0, 30));
    let mut j = transfer(&mut a, "j");

    let mut failures = Vec::new();
    if served(&j) != served(&a) {
        failures.push(format!(
            "right after the transfer j serves {} and its donor a serves {}",
            served(&j),
            served(&a)
        ));
    }
    a.receive(bump.clone());
    j.receive(bump);
    b.receive(insert);
    let expected = vec![(30, vec![]), (10, vec![]), (21, vec![])];
    for (who, replica) in [("a", &a), ("b", &b), ("j", &j)] {
        if projection(replica) != expected {
            failures.push(format!(
                "{who} serves {:?}, not {expected:?}",
                projection(replica)
            ));
        }
    }
    if served(&j) != served(&a) {
        failures.push(format!(
            "j serves {} and a serves {}",
            served(&j),
            served(&a)
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn st14_t2_a_joiner_resolves_a_concurrent_position_in_a_nested_list() {
    // P holds parts [x(1), y(2)]. b bumps y (position 1) while a puts z(9) in
    // front of x; j joins from a and then takes the bump: position 1 of j's
    // [z, x, y] is x, and the bump's writer saw y there.
    let (mut a, mut b, _c) = three(&[
        insert_item(0, 10),
        insert_part(0, 0, 1),
        insert_part(0, 1, 2),
        insert_item(1, 20),
    ]);
    let bump = send(&mut b, bump_part(0, 1, 5));
    let insert = send(&mut a, insert_part(0, 0, 9));
    let mut j = transfer(&mut a, "j");
    a.receive(bump.clone());
    j.receive(bump);
    b.receive(insert);

    let expected = vec![(10, vec![9, 1, 7]), (20, vec![])];
    let mut failures = Vec::new();
    for (who, replica) in [("a", &a), ("b", &b), ("j", &j)] {
        if projection(replica) != expected {
            failures.push(format!(
                "{who} serves {:?}, not {expected:?}",
                projection(replica)
            ));
        }
    }
    if served(&j) != served(&a) {
        failures.push(format!(
            "j serves {} and a serves {}",
            served(&j),
            served(&a)
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Updates the donor writes before the transfer, and the joiner after it.
const BEFORE: usize = 1_000;
const AFTER: usize = 600;
/// The joiner's window, past its first operations, which may rebuild what the
/// transfer did not carry.
const JOINER: std::ops::Range<usize> = 50..600;

#[test]
fn st14_t3_after_a_transfer_the_joiners_sequential_updates_cost_what_the_donors_did_on_a_young_list()
 {
    let (mut a, mut b) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
    for op in [seq_install(), insert_item(0, 1)] {
        let event = send(&mut a, op);
        b.receive(event);
    }
    let mut donor = Vec::with_capacity(BEFORE);
    for _ in 0..BEFORE {
        let (event, allocations) = counted(|| send(&mut a, bump_item(0, 1)));
        donor.push(allocations);
        b.receive(event);
    }
    let mut j = transfer(&mut a, "j");
    let mut joiner = Vec::with_capacity(AFTER);
    let mut events = Vec::with_capacity(AFTER);
    for _ in 0..AFTER {
        let (event, allocations) = counted(|| send(&mut j, bump_item(0, 1)));
        joiner.push(allocations);
        events.push(event);
    }
    for event in events {
        a.receive(event.clone());
        b.receive(event);
    }

    let mut failures = Vec::new();
    let expected = vec![(1 + (BEFORE + AFTER) as i64, Vec::new())];
    for (who, replica) in [("a", &a), ("b", &b), ("j", &j)] {
        if projection(replica) != expected {
            failures.push(format!(
                "{who} serves {:?}, not {expected:?}",
                projection(replica)
            ));
        }
    }
    let young = mean(&donor[EARLY]);
    let after = mean(&joiner[JOINER]);
    let ratio = after / young;
    eprintln!(
        "st14_t3: the donor's mean allocations per update at operations {EARLY:?} {young:.1}; \
         the joiner's at its operations {JOINER:?}, with {BEFORE} to {} updates of history, \
         {after:.1}; ratio {ratio:.3} (bound {FLAT})",
        BEFORE + AFTER
    );
    if ratio > FLAT {
        failures.push(format!(
            "the joiner's updates after the transfer cost {after:.1} allocations each (its \
             operations {JOINER:?}), {ratio:.2} times the donor's {young:.1} at operations \
             {EARLY:?}, over the bound {FLAT}: after a transfer the joiner replays the list"
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ===========================================================================
// Parts 3 and 4 at random: seeded schedules held to f14f969's states
// ===========================================================================

use std::collections::{BTreeMap, BTreeSet};

use moirai_interp::ModelOp;
use moirai_protocol::broadcast::message::EventMessage;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};
use serde_json::Value;

/// Seeds; the odd ones take a joiner by state transfer at `JOIN_AT`.
const SEEDS: u8 = 32;
const STEPS: usize = 160;
const JOIN_AT: usize = 70;
/// A checkpoint of the digest chain every `EVERY` steps.
const EVERY: usize = 10;
/// The fewest deliveries, over all seeds, of an operation whose version does
/// not include everything the receiving replica's list holds, per kind; and
/// of one at the joiner behind an operation it adopted rather than delivered.
const MIN_BEHIND_INSERT: usize = 500;
const MIN_BEHIND_UPDATE: usize = 2_000;
const MIN_BEHIND_DELETE: usize = 300;
const MIN_BEHIND_ADOPTED: usize = 100;

type OpKey = (String, usize);

/// How an operation writes one list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Insert,
    Update,
    Delete,
    /// The delete of the item that holds the list: `redundant_by_parent`.
    Reset,
}

/// One sent operation, as the schedule knows it.
struct Sent {
    message: EventMessage<ModelOp>,
    key: OpKey,
    /// Every operation its writer had delivered when it wrote it.
    past: BTreeSet<OpKey>,
    /// The lists it writes, and how.
    touches: Vec<(String, Kind)>,
}

struct Member {
    name: &'static str,
    replica: Rep,
    delivered: BTreeSet<OpKey>,
    /// Per list, the operations delivered here that wrote it.
    on_list: BTreeMap<String, BTreeSet<OpKey>>,
    /// Indices into the sent operations this member has not delivered.
    pending: Vec<usize>,
    /// What it adopted by state transfer, if it joined that way.
    adopted: BTreeSet<OpKey>,
}

#[derive(Default)]
struct Behind {
    by_kind: BTreeMap<Kind, usize>,
    adopted: usize,
}

fn seed(run: u8) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = run;
    seed[31] = !run;
    seed
}

fn below(rng: &mut ChaCha8Rng, n: usize) -> usize {
    (rng.next_u64() % n as u64) as usize
}

/// One operation the writer's own state allows, with the lists it writes.
fn pick(rng: &mut ChaCha8Rng, writer: &Rep) -> (ModelOp, Vec<(String, Kind)>, String) {
    let items = item_order(writer);
    let n = items.len();
    let roll = below(rng, 100);
    let by = 1 + below(rng, 9) as i64;
    let top = |kind| vec![("items".to_string(), kind)];
    if n == 0 || (roll < 20 && n < 8) {
        let pos = below(rng, n + 1);
        return (
            insert_item(pos, by),
            top(Kind::Insert),
            format!("insert_item({pos}, {by})"),
        );
    }
    let i = below(rng, n);
    let (origin, seq) = key(&items[i]);
    let list = format!("parts:{origin}@{seq}");
    let parts = part_order(writer, i).len();
    let nested = |kind| vec![("items".to_string(), Kind::Update), (list.clone(), kind)];
    match roll {
        0..=44 => (
            bump_item(i, by),
            top(Kind::Update),
            format!("bump_item({i}, {by})"),
        ),
        45..=54 => (
            delete_item(i),
            vec![
                ("items".to_string(), Kind::Delete),
                (list.clone(), Kind::Reset),
            ],
            format!("delete_item({i})"),
        ),
        55..=69 if parts < 5 => {
            let pos = below(rng, parts + 1);
            (
                insert_part(i, pos, by),
                nested(Kind::Insert),
                format!("insert_part({i}, {pos}, {by})"),
            )
        }
        _ if parts == 0 => (
            insert_part(i, 0, by),
            nested(Kind::Insert),
            format!("insert_part({i}, 0, {by})"),
        ),
        55..=89 => {
            let pos = below(rng, parts);
            (
                bump_part(i, pos, by),
                nested(Kind::Update),
                format!("bump_part({i}, {pos}, {by})"),
            )
        }
        _ => {
            let pos = below(rng, parts);
            (
                delete_part(i, pos),
                nested(Kind::Delete),
                format!("delete_part({i}, {pos})"),
            )
        }
    }
}

fn write(rng: &mut ChaCha8Rng, members: &mut [Member], sent: &mut Vec<Sent>, w: usize) -> String {
    let (op, touches, what) = pick(rng, &members[w].replica);
    let past = members[w].delivered.clone();
    let message = send(&mut members[w].replica, op);
    let op_key = key(message.event().id());
    let writer = &mut members[w];
    writer.delivered.insert(op_key.clone());
    for (list, _) in &touches {
        writer
            .on_list
            .entry(list.clone())
            .or_default()
            .insert(op_key.clone());
    }
    let index = sent.len();
    for (m, member) in members.iter_mut().enumerate() {
        if m != w {
            member.pending.push(index);
        }
    }
    sent.push(Sent {
        message,
        key: op_key.clone(),
        past,
        touches,
    });
    format!(
        "{} writes {what} as {}@{}",
        members[w].name, op_key.0, op_key.1
    )
}

/// Deliver one causally ready operation to member `r`, if one is pending.
fn deliver(
    rng: &mut ChaCha8Rng,
    members: &mut [Member],
    sent: &[Sent],
    r: usize,
    behind: &mut Behind,
) -> Option<String> {
    let member = &mut members[r];
    let ready: Vec<usize> = member
        .pending
        .iter()
        .copied()
        .filter(|&i| sent[i].past.is_subset(&member.delivered))
        .collect();
    if ready.is_empty() {
        return None;
    }
    let index = ready[below(rng, ready.len())];
    let op = &sent[index];
    for (list, kind) in &op.touches {
        if *kind == Kind::Reset {
            continue;
        }
        let unseen: Vec<&OpKey> = member
            .on_list
            .get(list)
            .map(|held| held.iter().filter(|z| !op.past.contains(*z)).collect())
            .unwrap_or_default();
        if !unseen.is_empty() {
            *behind.by_kind.entry(*kind).or_default() += 1;
            if unseen.iter().any(|z| member.adopted.contains(*z)) {
                behind.adopted += 1;
            }
        }
    }
    let before = member.replica.num_delivered_events();
    member.replica.receive(op.message.clone());
    let after = member.replica.num_delivered_events();
    assert_eq!(
        after,
        before + 1,
        "set-up: {} was handed {}@{}, causally ready by the schedule, and delivered {} events",
        member.name,
        op.key.0,
        op.key.1,
        after - before
    );
    member.delivered.insert(op.key.clone());
    for (list, _) in &op.touches {
        member
            .on_list
            .entry(list.clone())
            .or_default()
            .insert(op.key.clone());
    }
    member.pending.retain(|&i| i != index);
    Some(format!(
        "{} delivers {}@{}",
        member.name, op.key.0, op.key.1
    ))
}

fn chained(chain: &str, members: &[Member]) -> String {
    let states: Vec<String> = members
        .iter()
        .map(|member| served_digest(&member.replica))
        .collect();
    moirai_semantics::metamodel_digest(&Value::String(format!("{chain}|{}", states.join(","))))
}

/// One seeded schedule: its checkpoints, its final chain value, its trace.
struct Run {
    checkpoints: Vec<String>,
    last: String,
    trace: Vec<String>,
    failures: Vec<String>,
}

fn schedule(run: u8, behind: &mut Behind) -> Run {
    let mut rng = ChaCha8Rng::from_seed(seed(run));
    let (a, b, c) = three(&[]);
    // The `Install`, a's first event, is every member's first delivery.
    let install = BTreeSet::from([("a".to_string(), 1usize)]);
    let mut members: Vec<Member> = [("a", a), ("b", b), ("c", c)]
        .into_iter()
        .map(|(name, replica)| Member {
            name,
            replica,
            delivered: install.clone(),
            on_list: BTreeMap::new(),
            pending: Vec::new(),
            adopted: BTreeSet::new(),
        })
        .collect();
    let mut sent: Vec<Sent> = Vec::new();
    let mut trace = Vec::new();
    let mut chain = String::new();
    let mut checkpoints = Vec::new();
    let mut failures = Vec::new();

    for step in 0..STEPS {
        if run % 2 == 1 && step == JOIN_AT {
            let d = below(&mut rng, 3);
            let joiner = transfer(&mut members[d].replica, "j");
            let donor = &members[d];
            let pending = (0..sent.len())
                .filter(|&i| !donor.delivered.contains(&sent[i].key))
                .collect();
            let member = Member {
                name: "j",
                replica: joiner,
                delivered: donor.delivered.clone(),
                on_list: donor.on_list.clone(),
                pending,
                adopted: donor.delivered.clone(),
            };
            trace.push(format!(
                "step {step}: j joins by state transfer from {}",
                donor.name
            ));
            members.push(member);
        }
        let live = members.len();
        if below(&mut rng, 100) < 45 {
            let w = below(&mut rng, live);
            for _ in 0..1 + below(&mut rng, 3) {
                let line = write(&mut rng, &mut members, &mut sent, w);
                trace.push(format!("step {step}: {line}"));
            }
        } else {
            let r = below(&mut rng, live);
            for _ in 0..1 + below(&mut rng, 4) {
                match deliver(&mut rng, &mut members, &sent, r, behind) {
                    Some(line) => trace.push(format!("step {step}: {line}")),
                    None => break,
                }
            }
        }
        chain = chained(&chain, &members);
        if (step + 1) % EVERY == 0 {
            checkpoints.push(chain[..16].to_string());
        }
    }

    // Everything reaches everyone.
    loop {
        let mut moved = false;
        for r in 0..members.len() {
            while let Some(line) = deliver(&mut rng, &mut members, &sent, r, behind) {
                trace.push(format!("drain: {line}"));
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    chain = chained(&chain, &members);
    let first = served(&members[0].replica);
    for member in &members {
        if !member.pending.is_empty() {
            failures.push(format!(
                "seed {run}: {} still holds {} undeliverable operations",
                member.name,
                member.pending.len()
            ));
        }
        if served(&member.replica) != first {
            failures.push(format!(
                "seed {run}: after every operation reached every replica, {} serves {} and a \
                 serves {first}",
                member.name,
                served(&member.replica)
            ));
        }
        if member.replica.state().unresolved() != 0 {
            failures.push(format!(
                "seed {run}: {} could not route {} operation(s)",
                member.name,
                member.replica.state().unresolved()
            ));
        }
    }
    Run {
        checkpoints,
        last: chain[..16].to_string(),
        trace,
        failures,
    }
}

/// The golden: one line per seed, the seed, its checkpoints and its final
/// chain value, as `f14f969` produced them (spec section 5).
const GOLDEN: &str = "schedules.golden";

#[test]
fn st14_r1_seeded_schedules_serve_what_f14f969_served_after_every_step() {
    let mut behind = Behind::default();
    let mut failures = Vec::new();
    let mut lines = vec![format!(
        "# ST.14 schedules: seed, the digest chain every {EVERY} of {STEPS} steps, after the drain"
    )];
    let mut runs = Vec::new();
    for run in 0..SEEDS {
        // A replica that panics on an operation fails its seed, and the other
        // seeds still run.
        let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            schedule(run, &mut behind)
        })) {
            Ok(outcome) => outcome,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                    .unwrap_or_else(|| "a panic without a message".to_string());
                Run {
                    checkpoints: Vec::new(),
                    last: "panicked".to_string(),
                    trace: Vec::new(),
                    failures: vec![format!("seed {run}: the schedule panicked: {message}")],
                }
            }
        };
        lines.push(format!(
            "{run} {} {}",
            outcome.checkpoints.join(" "),
            outcome.last
        ));
        failures.extend(outcome.failures.iter().cloned());
        runs.push(outcome);
    }
    let observed = lines.join("\n") + "\n";

    eprintln!(
        "st14_r1: deliveries behind the receiving list: {:?}; behind an adopted operation at the \
         joiner: {}",
        behind.by_kind, behind.adopted
    );
    for (kind, floor) in [
        (Kind::Insert, MIN_BEHIND_INSERT),
        (Kind::Update, MIN_BEHIND_UPDATE),
        (Kind::Delete, MIN_BEHIND_DELETE),
    ] {
        let got = behind.by_kind.get(&kind).copied().unwrap_or(0);
        if got < floor {
            failures.push(format!(
                "set-up: the schedules delivered {got} {kind:?} operations whose version does not \
                 include everything the receiving list holds, under the floor {floor}"
            ));
        }
    }
    if behind.adopted < MIN_BEHIND_ADOPTED {
        failures.push(format!(
            "set-up: the joiners took {} operations behind an operation they adopted, under the \
             floor {MIN_BEHIND_ADOPTED}",
            behind.adopted
        ));
    }

    match std::fs::read_to_string(fixture_path(GOLDEN)) {
        Err(_) => {
            let path = observed_path(GOLDEN);
            std::fs::write(&path, &observed).expect("write the observed digests");
            failures.push(format!(
                "tests/st14/{GOLDEN} is missing; what this tree produced is in {}",
                path.display()
            ));
        }
        Ok(golden) => {
            let expected: BTreeMap<u8, Vec<String>> = golden
                .lines()
                .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
                .map(|line| {
                    let mut words = line.split_whitespace();
                    let run: u8 = words.next().and_then(|w| w.parse().ok()).expect("a seed");
                    (run, words.map(str::to_string).collect())
                })
                .collect();
            for (run, outcome) in runs.iter().enumerate() {
                let run = run as u8;
                let got: Vec<String> = outcome
                    .checkpoints
                    .iter()
                    .cloned()
                    .chain(std::iter::once(outcome.last.clone()))
                    .collect();
                let Some(want) = expected.get(&run) else {
                    failures.push(format!("seed {run}: not in tests/st14/{GOLDEN}"));
                    continue;
                };
                if outcome.last == "panicked" {
                    // Already a failure of its own.
                    continue;
                }
                if got.len() != want.len() {
                    failures.push(format!(
                        "seed {run}: {} digests where tests/st14/{GOLDEN} holds {}",
                        got.len(),
                        want.len()
                    ));
                    continue;
                }
                if let Some(at) = got.iter().zip(want).position(|(g, w)| g != w) {
                    let range = if at < outcome.checkpoints.len() {
                        format!("steps {} to {}", at * EVERY, (at + 1) * EVERY - 1)
                    } else {
                        "the drain".to_string()
                    };
                    let steps: Vec<&String> = outcome
                        .trace
                        .iter()
                        .filter(|line| {
                            if at < outcome.checkpoints.len() {
                                line.split(':')
                                    .next()
                                    .and_then(|head| {
                                        head.strip_prefix("step ")
                                            .and_then(|n| n.parse::<usize>().ok())
                                    })
                                    .is_some_and(|n| n / EVERY == at)
                            } else {
                                line.starts_with("drain")
                            }
                        })
                        .collect();
                    failures.push(format!(
                        "seed {run}: a replica's served state first differs from f14f969's in \
                         {range}; the schedule there:\n    {}",
                        steps
                            .iter()
                            .map(|line| line.as_str())
                            .collect::<Vec<_>>()
                            .join("\n    ")
                    ));
                }
            }
            if expected.len() != runs.len() {
                failures.push(format!(
                    "tests/st14/{GOLDEN} holds {} seeds, the test runs {}",
                    expected.len(),
                    runs.len()
                ));
            }
            if golden != observed {
                let path = observed_path(GOLDEN);
                std::fs::write(&path, &observed).expect("write the observed digests");
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
