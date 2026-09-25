//! ST.02, the interpreted half of claim A in process: A1, A2, A3 and the
//! guards A6 of `cea-cdrt-knowledge/spec/items/ST.02.md`.
//!
//! An optional containment of a concrete class (`Cell`) that a removal above
//! it reaches (U unsets `Holder.inner`, one or two levels above the `Cell`,
//! or removes the key of the keyed `Holder.bag` whose `Box` holds it)
//! while a concurrent write (W) to a sibling keeps the `Cell`'s owner alive,
//! or reaches the `Cell` itself. Once every replica has delivered every
//! operation, all serve one default read and one identified read (C1), the
//! document the rule fixes (C2: the object the removal emptied and nothing
//! wrote after is dropped; the object W reached survives holding W's 10),
//! under SI.01's identities (C3); a joiner by state transfer at any step
//! serves the same; later writes move nothing and an object written again
//! after U is new.
//!
//! These tests read the interpreted runtime only. The generated runtime is
//! held to the same schedules by the twin (A5, in `arachne-model-plane`).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st02_emptied_optional_object -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;
mod st02;

use std::collections::BTreeSet;

use moirai_protocol::replica::IsReplica;
use si01::{Rep, all_ids, default_read};
use st02::*;

/// One schedule's failures at its end, by kind.
#[derive(Default)]
struct Verdict {
    parted: bool,
    value: bool,
    identity: bool,
    r_prime: bool,
    lines: Vec<String>,
}

/// Judge three replicas that have all delivered every operation of
/// `schedule`.
fn judge(schedule: &Schedule, played: &Played) -> Verdict {
    let label = schedule.label();
    let mut verdict = Verdict::default();
    let formed = Formed::read(schedule.place, &played.formed, &label);
    let defaults: Vec<_> = played.reps.iter().map(default_read).collect();
    let ids: Vec<_> = played.reps.iter().map(identified_read).collect();
    if defaults.windows(2).any(|p| p[0] != p[1]) || ids.windows(2).any(|p| p[0] != p[1]) {
        verdict.parted = true;
        verdict.lines.push(format!(
            "{label}: the replicas part; default reads {}",
            defaults
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    let want = expected(schedule);
    for (seat, served) in defaults.iter().enumerate() {
        if *served != want {
            verdict.value = true;
            verdict.lines.push(format!(
                "{label}: replica {seat} serves {served}, not {want}"
            ));
        }
    }
    for (seat, doc) in ids.iter().enumerate() {
        if let Err(why) = identities(schedule, &formed, doc, &format!("{label}: replica {seat}")) {
            verdict.identity = true;
            verdict.lines.push(why);
        }
    }
    for (seat, replica) in played.reps.iter().enumerate() {
        if let Err(why) = r_prime(replica, &format!("{label}: replica {seat}")) {
            verdict.r_prime = true;
            verdict.lines.push(why);
        }
    }
    verdict
}

/// A1: every schedule of the claim, 1008 (Flat 288, Deep 432, Keyed 288).
#[test]
fn st02_a1_an_emptied_optional_object_reached_by_a_removal_above_converges_on_the_rule() {
    let tray = Tray::new();
    let desc = descriptor();
    let (mut total, mut parted, mut value, mut identity, mut rp) = (0, 0, 0, 0, 0);
    let mut per_place = Vec::new();
    let mut lines = Vec::new();
    for place in Place::CLAIM {
        let (mut p_total, mut p_parted, mut p_value) = (0, 0, 0);
        for schedule in schedules(place) {
            total += 1;
            p_total += 1;
            let played = play(&tray, &desc, &schedule, |_, _| {});
            let verdict = judge(&schedule, &played);
            parted += verdict.parted as usize;
            p_parted += verdict.parted as usize;
            value += verdict.value as usize;
            p_value += verdict.value as usize;
            identity += verdict.identity as usize;
            rp += verdict.r_prime as usize;
            lines.extend(verdict.lines);
        }
        per_place.push(format!(
            "{place:?}: parted {p_parted} of {p_total}, value other than the rule's {p_value} of \
             {p_total}"
        ));
    }
    println!(
        "ST02-A1 schedules {total}: parted {parted} (P1), value other than the rule's {value} \
         (P2), identity {identity}, r' {rp}"
    );
    for line in &per_place {
        println!("ST02-A1 {line}");
    }
    for line in lines.iter().take(6) {
        println!("ST02-A1 {line}");
    }
    assert!(
        parted == 0 && value == 0 && identity == 0 && rp == 0,
        "ST.02 A1: of {total} schedules, {parted} part (C1), {value} serve a value other than \
         the rule's (C2), {identity} break an identity (C3), {rp} break R'; first:\n  {}",
        lines
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// A2: after the race, one replica writes W's target again (+1), delivered
/// everywhere, then, where the race dropped the `Cell`, writes the `Cell`
/// again (+5), delivered everywhere. X on 0, Y on 1, the third taking X
/// first; U and W in every seating, the third taking U first; every target;
/// each replica in turn as the writer: 126 runs (Flat 36, Deep 54, Keyed 36).
#[test]
fn st02_a2_after_the_race_a_write_moves_nothing_and_an_object_written_again_is_new() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut runs = 0;
    let mut failing = Vec::new();
    for place in Place::CLAIM {
        for uw in SEATS {
            for &target in place.targets() {
                for writer in 0..3 {
                    runs += 1;
                    let schedule = Schedule {
                        place,
                        xy: (0, 1),
                        x_first: true,
                        uw,
                        u_first: true,
                        target,
                    };
                    let label = format!("{}; then replica {writer} writes", schedule.label());
                    let mut played = play(&tray, &desc, &schedule, |_, _| {});
                    let formed = Formed::read(place, &played.formed, &label);
                    let mut why = later_writes(&tray, &schedule, &formed, &mut played.reps, writer);
                    if !why.is_empty() {
                        why.insert(0, label);
                        failing.push(why.join("; "));
                    }
                }
            }
        }
    }
    println!("ST02-A2 runs {runs}: failing {}", failing.len());
    for line in failing.iter().take(4) {
        println!("ST02-A2 {line}");
    }
    assert!(
        failing.is_empty(),
        "ST.02 A2: {} of {runs} runs fail; first:\n  {}",
        failing.len(),
        failing
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// The later writes on `reps`, from `writer`, each delivered to every other
/// replica; the failures, one line each.
fn later_writes(
    tray: &Tray,
    schedule: &Schedule,
    formed: &Formed,
    reps: &mut [Rep],
    writer: usize,
) -> Vec<String> {
    let names: Vec<String> = (0..reps.len())
        .map(|seat| format!("replica {seat}"))
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut seen: BTreeSet<String> = formed.all();
    for replica in reps.iter() {
        seen.extend(all_ids(&identified_read(replica)));
    }
    let mut why = Vec::new();
    let event = send!(reps[writer], again_op(tray, schedule));
    for (seat, replica) in reps.iter_mut().enumerate() {
        if seat != writer {
            replica.receive(event.clone());
        }
    }
    let reads = reads_of(&reps.iter().collect::<Vec<_>>(), &names);
    why.extend(check_again(schedule, formed, &reads));
    for replica in reps.iter() {
        seen.extend(all_ids(&identified_read(replica)));
    }
    if let Some(op) = rebuild_op(tray, schedule) {
        let event = send!(reps[writer], op);
        for (seat, replica) in reps.iter_mut().enumerate() {
            if seat != writer {
                replica.receive(event.clone());
            }
        }
        let reads = reads_of(&reps.iter().collect::<Vec<_>>(), &names);
        why.extend(check_rebuilt(schedule, formed, &seen, &reads));
    }
    why
}

/// A3: a fourth replica `d` adopts, through a serde round trip, the causal
/// snapshot and log of each of the three replicas after each of the
/// schedule's 15 steps; the schedule runs to its end, `d` delivers every
/// operation in the order sent, and must serve each replica's identified
/// read, the rule's document, C3's identities and R'; then `d` makes A2's
/// later writes and all four serve what A2 says. X on 0, Y on 1; U and W on
/// 0 and 1 both ways, the third taking U first; every target: 630 joins
/// (Flat 180, Deep 270, Keyed 180).
#[test]
fn st02_a3_a_joiner_by_state_transfer_at_any_step_serves_what_the_others_serve() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut joins = 0;
    let mut failing = Vec::new();
    for place in Place::CLAIM {
        for uw in [(0, 1), (1, 0)] {
            for &target in place.targets() {
                let schedule = Schedule {
                    place,
                    xy: (0, 1),
                    x_first: true,
                    uw,
                    u_first: true,
                    target,
                };
                for donor in 0..3 {
                    for at in 1..=15 {
                        joins += 1;
                        let label = format!(
                            "{}; d joins from replica {donor} after step {at}",
                            schedule.label()
                        );
                        let mut step_no = 0;
                        let mut taken: Option<Rep> = None;
                        let played = play(&tray, &desc, &schedule, |reps, _| {
                            step_no += 1;
                            if step_no == at {
                                taken = Some(joiner(&reps[donor]));
                            }
                        });
                        let mut d = taken.expect("set-up: the schedule has 15 steps");
                        for event in &played.sent {
                            d.receive(event.clone());
                        }
                        let formed = Formed::read(place, &played.formed, &label);
                        let mut why = Vec::new();
                        let on_d = identified_read(&d);
                        for (seat, replica) in played.reps.iter().enumerate() {
                            let there = identified_read(replica);
                            if there != on_d {
                                why.push(format!(
                                    "d serves {on_d} where replica {seat} serves {there}"
                                ));
                            }
                        }
                        let want = expected(&schedule);
                        let plain = default_read(&d);
                        if plain != want {
                            why.push(format!("d serves {plain}, not {want}"));
                        }
                        if let Err(line) = identities(&schedule, &formed, &on_d, "d") {
                            why.push(line);
                        }
                        if let Err(line) = r_prime(&d, "d") {
                            why.push(line);
                        }
                        let Played { reps, .. } = played;
                        let [a, b, c] = reps;
                        let mut four = vec![a, b, c, d];
                        why.extend(later_writes(&tray, &schedule, &formed, &mut four, 3));
                        if !why.is_empty() {
                            why.insert(0, label);
                            failing.push(why.join("; "));
                        }
                    }
                }
            }
        }
    }
    println!("ST02-A3 joins {joins}: failing {}", failing.len());
    for line in failing.iter().take(4) {
        println!("ST02-A3 {line}");
    }
    assert!(
        failing.is_empty(),
        "ST.02 A3: {} of {joins} joins fail; first:\n  {}",
        failing.len(),
        failing
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// A6, guard: U unsets `Box.cell` itself (no removal above the optional),
/// raced by W, on every schedule (288). Both runtimes serve this today; it
/// must keep converging on the same document.
#[test]
fn st02_a6_guard_an_optional_unset_directly_as_today() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut total = 0;
    let mut lines = Vec::new();
    for schedule in schedules(Place::Direct) {
        total += 1;
        let played = play(&tray, &desc, &schedule, |_, _| {});
        let verdict = judge(&schedule, &played);
        if !verdict.lines.is_empty() {
            lines.push(verdict.lines.join("; "));
        }
    }
    println!("ST02-A6 direct schedules {total}: failing {}", lines.len());
    assert!(
        lines.is_empty(),
        "ST.02 A6 (direct unset): {} of {total} schedules fail; first:\n  {}",
        lines.len(),
        lines
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// A6, guard: U alone, in the three places (12 formations x 3 seats of U):
/// `Holder.inner` unset in `Flat` and `Deep`, the `Box` without its `Cell`
/// in `Direct`, on every replica, one identified read.
#[test]
fn st02_a6_guard_a_removal_alone_as_today() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut runs = 0;
    let mut lines = Vec::new();
    for place in Place::ALONE {
        for xy in SEATS {
            for x_first in [true, false] {
                for u in 0..3 {
                    runs += 1;
                    let played = play_unset_alone(&tray, &desc, place, xy, x_first, u);
                    let label = format!("{place:?}: X on {} Y on {}, U alone on {u}", xy.0, xy.1);
                    let want = unset_alone(place);
                    let ids: Vec<_> = played.reps.iter().map(identified_read).collect();
                    if ids.windows(2).any(|p| p[0] != p[1]) {
                        lines.push(format!("{label}: the replicas part: {ids:?}"));
                    }
                    for (seat, replica) in played.reps.iter().enumerate() {
                        let served = default_read(replica);
                        if served != want {
                            lines.push(format!(
                                "{label}: replica {seat} serves {served}, not {want}"
                            ));
                        }
                        if let Err(why) = r_prime(replica, &label) {
                            lines.push(why);
                        }
                    }
                }
            }
        }
    }
    println!("ST02-A6 removal alone runs {runs}: failing {}", lines.len());
    assert!(
        lines.is_empty(),
        "ST.02 A6 (removal alone): {} failures over {runs} runs; first:\n  {}",
        lines.len(),
        lines
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}
