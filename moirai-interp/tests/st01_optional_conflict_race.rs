//! ST.01, in process: an optional containment holding the conflict of an `A`
//! and a `B`, unset (U) while a concurrent write (W) increments the `n` of
//! one member. O1, O2, O3 and O6 of the spec,
//! `cea-cdrt-knowledge/spec/items/ST.01.md`; the shared part is `st01/mod.rs`
//! and SI.01's `si01/mod.rs`.
//!
//! - O1: every schedule of the claim, in the flat optional and one level
//!   deeper (the inner slot unset, and the outer optional unset), 864 in all.
//!   Once every replica has delivered every operation, the three replicas
//!   serve one default read and one identified read (C1), the document V1
//!   fixes (C2), under the identities of C3, and the default read is the
//!   identified read stripped (SI.01's R').
//! - O2: after the race, each replica in turn writes the reached member's
//!   class, which moves nothing; then removes the place and writes the
//!   emptied member's class, which comes back alone under an identity nothing
//!   carried before (C3, SI.01's C4 and C5).
//! - O3: a fourth replica joins by state transfer after any step of the
//!   schedule, from any of the three, delivers what it lacks, serves what the
//!   others serve, and makes O2's later writes (C1, C3).
//! - O6: the keyed collection's race serves the reached member alone (ST.10
//!   re-pinned it from both members, the other emptied), U alone leaves every
//!   place unset (ST.10 re-pinned the removed key from both members
//!   emptied), and the in-process joiner of O3 converges on the keyed
//!   collection.
//!
//! Each of O1, O2 and O3 runs every schedule, counts what fails, prints the
//! counts on lines starting `ST01-`, and asserts once at the end, so a red
//! run reports how many schedules fail and not only the first.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st01_optional_conflict_race -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;
mod st01;

use std::collections::BTreeSet;

use moirai_protocol::replica::IsReplica;
use serde_json::Value;

use moirai_interp::ModelOp;
use si01::{Rep, all_ids, default_read};
use st01::*;

/// The first few lines of a failure list, for an assertion message.
fn first(lines: &[String]) -> String {
    lines
        .iter()
        .take(4)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n  ")
}

// ---------------------------------------------------------------------------
// O1
// ---------------------------------------------------------------------------

/// O1: C1, C2 and C3 at the end of every one of the 864 schedules.
///
/// Counted per schedule: `parted` when two replicas serve different default
/// reads or different identified reads (prediction P1); `value` when some
/// replica's default read is not V1's document (P2); `identity` when some
/// replica's identified read breaks C3; `r'` when some replica's default read
/// is not its identified read stripped.
#[test]
fn st01_o1_an_unset_raced_by_a_write_to_one_member_serves_that_member_alone_everywhere() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut total = 0;
    let mut parted = Vec::new();
    let mut value = Vec::new();
    let mut identity = Vec::new();
    let mut r_primes = Vec::new();
    let mut per_place = Vec::new();
    for place in Place::OPTIONAL {
        let (mut p, mut v) = (0, 0);
        for schedule in schedules(place) {
            total += 1;
            let label = schedule.label();
            let played = play(&tray, &desc, &schedule, |_, _| {});
            let formed = Formed::read(place, &played.formed, &label);
            let expected = v1(&schedule);
            let defaults: Vec<Value> = played.reps.iter().map(default_read).collect();
            let ids: Vec<Value> = played.reps.iter().map(identified_read).collect();

            let agree = defaults.windows(2).all(|pair| pair[0] == pair[1])
                && ids.windows(2).all(|pair| pair[0] == pair[1]);
            if !agree {
                p += 1;
                parted.push(format!(
                    "{label}: the replicas part; default reads {}",
                    defaults
                        .iter()
                        .map(Value::to_string)
                        .collect::<Vec<_>>()
                        .join(" | ")
                ));
            }
            if defaults.iter().any(|served| *served != expected) {
                v += 1;
                value.push(format!(
                    "{label}: V1 is {expected}; served {}",
                    defaults
                        .iter()
                        .map(Value::to_string)
                        .collect::<Vec<_>>()
                        .join(" | ")
                ));
            }
            let broken: Vec<String> = ids
                .iter()
                .enumerate()
                .filter_map(|(seat, doc)| {
                    v1_identities(&schedule, &formed, doc, &format!("replica {seat}")).err()
                })
                .collect();
            if !broken.is_empty() {
                identity.push(format!("{label}: {}", broken.join(" / ")));
            }
            let stripped: Vec<String> = played
                .reps
                .iter()
                .enumerate()
                .filter_map(|(seat, replica)| r_prime(replica, &format!("replica {seat}")).err())
                .collect();
            if !stripped.is_empty() {
                r_primes.push(format!("{label}: {}", stripped.join(" / ")));
            }
        }
        per_place.push((place, p, v));
    }
    println!(
        "ST01-O1 schedules {total}: parted {} (P1), value other than V1 {} (P2), identity {}, \
         r' {}",
        parted.len(),
        value.len(),
        identity.len(),
        r_primes.len()
    );
    for (place, p, v) in &per_place {
        println!("ST01-O1 {place:?}: parted {p} of 288, value other than V1 {v} of 288");
    }
    for (what, lines) in [
        ("parted", &parted),
        ("value", &value),
        ("identity", &identity),
        ("r'", &r_primes),
    ] {
        for line in lines.iter().take(2) {
            println!("ST01-O1 {what}: {line}");
        }
    }
    assert!(
        parted.is_empty() && value.is_empty() && identity.is_empty() && r_primes.is_empty(),
        "ST.01 O1: of {total} schedules, {} part (C1), {} serve a value other than V1 (C2), {} \
         break an identity (C3), {} break R'; first:\n  {}",
        parted.len(),
        value.len(),
        identity.len(),
        r_primes.len(),
        first(
            &parted
                .iter()
                .chain(&value)
                .chain(&identity)
                .chain(&r_primes)
                .cloned()
                .collect::<Vec<_>>()
        )
    );
}

// ---------------------------------------------------------------------------
// O2
// ---------------------------------------------------------------------------

/// O2: after the race, one replica writes the reached member's class +1,
/// delivered everywhere; then it removes the place again and writes the
/// emptied member's class +5, delivered everywhere. Each replica takes that
/// turn in its own run. The +1 is a write into an element every writer sees
/// alive, so it moves nothing: the reached member holds 11 under its
/// identity (SI.01's C4). The removal empties it, and the emptied class
/// written after U (and after the second removal) is a new element: it is
/// served alone, holding +5, under an identity nothing carried before, and
/// neither the old identity nor the reached member's is served (SI.01's C5,
/// and O1k's "only the class written comes back").
///
/// The conflict is seated X on 0, Y on 1, the third taking X first; U and W
/// take every seating, the third taking U first; W reaches either member.
/// 3 places x 6 x 2 x 3 writers = 108 runs.
#[test]
fn st01_o2_after_the_race_a_write_moves_nothing_and_a_class_written_again_is_new() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut total = 0;
    let mut failures = Vec::new();
    for place in Place::OPTIONAL {
        for uw in SEATS {
            for member in ["A", "B"] {
                for writer in 0..3 {
                    total += 1;
                    let schedule = Schedule {
                        place,
                        xy: (0, 1),
                        x_first: true,
                        uw,
                        u_first: true,
                        member,
                    };
                    let label = format!("{}; then replica {writer} writes", schedule.label());
                    let mut played = play(&tray, &desc, &schedule, |_, _| {});
                    let formed = Formed::read(place, &played.formed, &label);
                    let mut why = later_writes(&tray, &schedule, &formed, &mut played.reps, writer);
                    if !why.is_empty() {
                        why.insert(0, label);
                        failures.push(why.join("; "));
                    }
                }
            }
        }
    }
    println!("ST01-O2 runs {total}: failing {}", failures.len());
    for line in failures.iter().take(6) {
        println!("ST01-O2 {line}");
    }
    assert!(
        failures.is_empty(),
        "ST.01 O2: {} of {total} runs fail; first:\n  {}",
        failures.len(),
        first(&failures)
    );
}

/// The later writes from `reps[writer]`, each delivered to every other
/// replica of `reps`, checked after the first and after the last. Answers
/// what fails, one line each; nothing when all holds.
fn later_writes(
    tray: &Tray,
    schedule: &Schedule,
    formed: &Formed,
    reps: &mut [Rep],
    writer: usize,
) -> Vec<String> {
    let names: Vec<String> = (0..reps.len())
        .map(|seat| {
            if seat == 3 {
                "d".to_string()
            } else {
                format!("replica {seat}")
            }
        })
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut seen: BTreeSet<String> = formed.all();
    for replica in reps.iter() {
        seen.extend(all_ids(&identified_read(replica)));
    }
    let send = |reps: &mut [Rep], op: ModelOp| -> Result<(), String> {
        let event = reps[writer]
            .send(op.clone())
            .map_err(|why| format!("{} refused {op:?}: {why:?}", names[writer]))?;
        for (seat, replica) in reps.iter_mut().enumerate() {
            if seat != writer {
                replica.receive(event.clone());
            }
        }
        Ok(())
    };
    if let Err(refused) = send(reps, reached_write(tray, schedule)) {
        return vec![refused];
    }
    let refs: Vec<&Rep> = reps.iter().collect();
    let mut why: Vec<String> = check_reached_write(schedule, formed, &reads_of(&refs, &names))
        .into_iter()
        .map(|line| format!("after the +1: {line}"))
        .collect();
    for replica in reps.iter() {
        seen.extend(all_ids(&identified_read(replica)));
    }
    for op in written_again(tray, schedule) {
        if let Err(refused) = send(reps, op) {
            why.push(refused);
            return why;
        }
    }
    let refs: Vec<&Rep> = reps.iter().collect();
    why.extend(
        check_written_again(schedule, formed, &seen, &reads_of(&refs, &names))
            .into_iter()
            .map(|line| format!("after the class written again: {line}")),
    );
    why
}

// ---------------------------------------------------------------------------
// O3
// ---------------------------------------------------------------------------

/// O3: a fourth replica `d` joins by state transfer ([`joiner`]) from each of
/// the three replicas after each of the 15 steps of a schedule (every send
/// and every delivery), so it adopts a state holding U and not W, W and not
/// U, both, or neither. The schedule then runs to its end on the three, and
/// `d` delivers every operation sent, in the order sent (it drops what it
/// adopted). `d` then serves the three's default and identified reads, V1's
/// document, under the identities of C3; then `d` makes O2's later writes,
/// delivered to the three, and all four serve what O2 says.
///
/// The conflict is seated X on 0, Y on 1, the third taking X first; U and W
/// on 0 and 1 in both assignments, the third taking U first; W reaches
/// either member. 3 places x 2 x 2 x 3 donors x 15 steps = 540 joins.
#[test]
fn st01_o3_a_joiner_by_state_transfer_at_any_step_serves_what_the_others_serve() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut total = 0;
    let mut failures = Vec::new();
    for place in Place::OPTIONAL {
        for uw in [(0, 1), (1, 0)] {
            for member in ["A", "B"] {
                let schedule = Schedule {
                    place,
                    xy: (0, 1),
                    x_first: true,
                    uw,
                    u_first: true,
                    member,
                };
                for donor in 0..3 {
                    for at in 1..=15 {
                        total += 1;
                        let label = format!(
                            "{}; d joins from replica {donor} after step {at}",
                            schedule.label()
                        );
                        let why = join_and_check(&tray, &desc, &schedule, donor, at);
                        if !why.is_empty() {
                            failures.push(format!("{label}: {}", why.join("; ")));
                        }
                    }
                }
            }
        }
    }
    println!("ST01-O3 joins {total}: failing {}", failures.len());
    for line in failures.iter().take(6) {
        println!("ST01-O3 {line}");
    }
    assert!(
        failures.is_empty(),
        "ST.01 O3: {} of {total} joins fail; first:\n  {}",
        failures.len(),
        first(&failures)
    );
}

/// Play `schedule`, let `d` join from `donor` after step `at`, catch it up,
/// and check it; then let it make the later writes and check all four.
fn join_and_check(
    tray: &Tray,
    desc: &Value,
    schedule: &Schedule,
    donor: usize,
    at: usize,
) -> Vec<String> {
    let mut steps = 0;
    let mut d: Option<Rep> = None;
    let played = play(tray, desc, schedule, |reps, _| {
        steps += 1;
        if steps == at {
            d = Some(joiner(&reps[donor]));
        }
    });
    let mut d = d.expect("set-up: the schedule has 15 steps");
    for event in &played.sent {
        d.receive(event.clone());
    }
    let formed = Formed::read(schedule.place, &played.formed, "set-up");
    let mut why = Vec::new();
    let joined = identified_read(&d);
    for (seat, replica) in played.reps.iter().enumerate() {
        let reference = identified_read(replica);
        if joined != reference {
            why.push(format!(
                "d serves {joined} where replica {seat} serves {reference}"
            ));
        }
    }
    let expected = v1(schedule);
    let plain = default_read(&d);
    if plain != expected {
        why.push(format!("d serves {plain}, not V1's {expected}"));
    }
    if let Err(broken) = v1_identities(schedule, &formed, &joined, "d") {
        why.push(broken);
    }
    if let Err(broken) = r_prime(&d, "d") {
        why.push(broken);
    }
    if !why.is_empty() {
        return why;
    }
    let [a, b, c] = played.reps;
    let mut four = [a, b, c, d];
    later_writes(tray, schedule, &formed, &mut four, 3)
}

// ---------------------------------------------------------------------------
// O6: guards
// ---------------------------------------------------------------------------

/// O6, the keyed collection: key `k` holds the conflict, and U removes the
/// key while W increments one member. Outside ST.01's claim; under ST.10's
/// rule, on every replica in every order, the reached member alone, holding
/// W's +10 under its identity; U had seen every write to the other, which is
/// absent. All 288 schedules.
#[test]
fn st01_o6_a_removed_key_serves_the_reached_member_alone() {
    let tray = Tray::new();
    let desc = descriptor();
    for schedule in schedules(Place::Keyed) {
        let label = schedule.label();
        let played = play(&tray, &desc, &schedule, |_, _| {});
        let formed = Formed::read(Place::Keyed, &played.formed, &label);
        let expected = keyed(&schedule);
        let ids: Vec<Value> = played.reps.iter().map(identified_read).collect();
        for (seat, replica) in played.reps.iter().enumerate() {
            assert_eq!(
                default_read(replica),
                expected,
                "{label}: replica {seat} no longer serves the keyed collection's value"
            );
            r_prime(replica, &format!("{label}: replica {seat}"))
                .unwrap_or_else(|why| panic!("{why}"));
            assert_eq!(
                ids[seat], ids[0],
                "{label}: replicas 0 and {seat} part on the identified read"
            );
        }
        let held = content(Place::Keyed, &ids[0]).expect("the key is served");
        assert_eq!(
            member_id(held, schedule.member).as_deref(),
            Some(formed.of(schedule.member)),
            "{label}: the reached member's identity moved: {}",
            ids[0]
        );
    }
}

/// O6, U alone: with no concurrent write, an unset leaves every optional
/// place reading unset and a removed key absent (ST.10), on every replica,
/// whichever replica unsets. 12 conflict schedules x 3 seats of U, in each
/// of the four places.
#[test]
fn st01_o6_an_unset_alone_leaves_every_place_unset() {
    let tray = Tray::new();
    let desc = descriptor();
    for place in Place::ALL {
        for xy in SEATS {
            for x_first in [true, false] {
                for u in 0..3 {
                    let played = play_unset_alone(&tray, &desc, place, xy, x_first, u);
                    let expected = unset_alone(place);
                    let label = format!(
                        "{place:?}: X on {} Y on {}, third X first {x_first}; U alone on {u}",
                        xy.0, xy.1
                    );
                    let ids: Vec<Value> = played.reps.iter().map(identified_read).collect();
                    for (seat, replica) in played.reps.iter().enumerate() {
                        assert_eq!(default_read(replica), expected, "{label}: replica {seat}");
                        assert_eq!(ids[seat], ids[0], "{label}: replicas 0 and {seat} part");
                    }
                }
            }
        }
    }
}

/// O6, the joiner of O3 on the keyed collection: the check that the
/// in-process joiner (a snapshot and a serde round trip, then every
/// operation delivered in the order sent) is sound, so that a failure of O3
/// is the store's and not the harness's. Its value is [`keyed`]'s, which
/// ST.10 re-pinned.
#[test]
fn st01_o6_guard_the_in_process_joiner_converges_where_the_store_does() {
    let tray = Tray::new();
    let desc = descriptor();
    for uw in [(0, 1), (1, 0)] {
        for member in ["A", "B"] {
            let schedule = Schedule {
                place: Place::Keyed,
                xy: (0, 1),
                x_first: true,
                uw,
                u_first: true,
                member,
            };
            for donor in 0..3 {
                for at in 1..=15 {
                    let label = format!(
                        "{}; d joins from replica {donor} after step {at}",
                        schedule.label()
                    );
                    let mut steps = 0;
                    let mut d: Option<Rep> = None;
                    let played = play(&tray, &desc, &schedule, |reps, _| {
                        steps += 1;
                        if steps == at {
                            d = Some(joiner(&reps[donor]));
                        }
                    });
                    let mut d = d.expect("set-up: the schedule has 15 steps");
                    for event in &played.sent {
                        d.receive(event.clone());
                    }
                    assert_eq!(default_read(&d), keyed(&schedule), "{label}");
                    assert_eq!(
                        identified_read(&d),
                        identified_read(&played.reps[0]),
                        "{label}: d and replica 0 part on the identified read"
                    );
                }
            }
        }
    }
}
