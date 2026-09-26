//! ST.04, the interpreted half in process: B1, B2, B3 and the guards B6 of
//! `cea-cdrt-knowledge/spec/items/ST.04.md`.
//!
//! An optional attribute (a leaf, of each of the twenty kinds the derivation
//! can put under an optional) of an object that a removal above reaches (U
//! unsets `Holder.inner`, one or two levels above the leaf) while a
//! concurrent write (W) to a sibling keeps the object alive, or reaches the
//! leaf itself, or, one level deeper, keeps only the outer object alive.
//! Once every replica has delivered every operation, all serve one default
//! read and one identified read (C1), the document the rule fixes (C2: the
//! removal does to the optional what its own unset does, so a leaf it
//! emptied is an absent key, a leaf W reached holds W, a text holds what its
//! reset recorded), under SI.01's identities (C3); a joiner by state
//! transfer at any step serves the same; later writes move nothing, and a
//! leaf written again after U holds that write alone.
//!
//! These tests read the interpreted runtime only. The generated runtime is
//! held to the same schedules by the twin (B5, in `arachne-model-plane`).
//! Each test runs the kinds on two threads.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st04_emptied_optional_attribute -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;
mod st04;

use std::collections::BTreeSet;

use moirai_protocol::replica::IsReplica;
use si01::{Rep, all_ids, default_read};
use st04::*;

/// One schedule's failures at its end, by kind of failure.
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

/// Counts of one kind's schedules.
#[derive(Default)]
struct Tally {
    total: usize,
    parted: usize,
    value: usize,
    identity: usize,
    r_prime: usize,
    /// Per place: (place, total, parted, value).
    places: Vec<(Place, usize, usize, usize)>,
    lines: Vec<String>,
}

/// Every schedule of `kind` at `places`, judged.
fn tally(tray: &Tray, desc: &serde_json::Value, kind: Kind, places: &[Place]) -> Tally {
    let mut t = Tally::default();
    for &place in places {
        let (mut p_total, mut p_parted, mut p_value) = (0, 0, 0);
        for schedule in schedules(kind, place) {
            t.total += 1;
            p_total += 1;
            let played = play(tray, desc, &schedule, |_, _| {});
            let verdict = judge(&schedule, &played);
            t.parted += verdict.parted as usize;
            p_parted += verdict.parted as usize;
            t.value += verdict.value as usize;
            p_value += verdict.value as usize;
            t.identity += verdict.identity as usize;
            t.r_prime += verdict.r_prime as usize;
            if t.lines.len() < 4 {
                t.lines.extend(verdict.lines);
            }
        }
        t.places.push((place, p_total, p_parted, p_value));
    }
    t
}

/// Sum the kinds' tallies, print them on `ST04-<tag>` lines, and answer
/// (total, parted, value, identity, r_prime, first lines).
fn report(
    tag: &str,
    tallies: &[(Kind, Tally)],
) -> (usize, usize, usize, usize, usize, Vec<String>) {
    let (mut total, mut parted, mut value, mut identity, mut rp) = (0, 0, 0, 0, 0);
    let mut lines = Vec::new();
    for (kind, t) in tallies {
        total += t.total;
        parted += t.parted;
        value += t.value;
        identity += t.identity;
        rp += t.r_prime;
        let places: Vec<String> = t
            .places
            .iter()
            .map(|(place, n, p, v)| format!("{place:?} parted {p} value {v} of {n}"))
            .collect();
        println!(
            "ST04-{tag} {kind:?}: schedules {}, parted {}, value other than the rule's {}, \
             identity {}, r' {} ({})",
            t.total,
            t.parted,
            t.value,
            t.identity,
            t.r_prime,
            places.join("; ")
        );
        if lines.len() < 6 {
            lines.extend(t.lines.iter().take(2).cloned());
        }
    }
    println!(
        "ST04-{tag} schedules {total}: parted {parted} (P1), value other than the rule's \
         {value} (P2), identity {identity}, r' {rp}"
    );
    for line in lines.iter().take(6) {
        println!("ST04-{tag} {line}");
    }
    (total, parted, value, identity, rp, lines)
}

/// B1: every schedule of the claim for every kind, 14,400 (per kind: Flat
/// 288, Deep 432).
#[test]
fn st04_b1_an_emptied_optional_attribute_reached_by_a_removal_above_converges_on_the_rule() {
    let tray = Tray::new();
    let desc = descriptor();
    let tallies = per_kind(&Kind::ALL, |kind| tally(&tray, &desc, kind, &Place::CLAIM));
    let (total, parted, value, identity, rp, lines) = report("B1", &tallies);
    assert!(
        parted == 0 && value == 0 && identity == 0 && rp == 0,
        "ST.04 B1: of {total} schedules, {parted} part (C1), {value} serve a value other than \
         the rule's (C2), {identity} break an identity (C3), {rp} break R'; first:\n  {}",
        lines
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// B2: after the race, one replica writes W's target again, delivered
/// everywhere, then, where W did not reach the leaf, writes the leaf again,
/// delivered everywhere. X on 0, Y on 1, the third taking X first; U and W
/// in every seating, the third taking U first; every target; each replica in
/// turn as the writer: 90 runs per kind (Flat 36, Deep 54), 1,800 in all.
#[test]
fn st04_b2_after_the_race_a_write_moves_nothing_and_a_leaf_written_again_holds_that_write_alone() {
    let tray = Tray::new();
    let desc = descriptor();
    let results = per_kind(&Kind::ALL, |kind| {
        let mut runs = 0;
        let mut failing = Vec::new();
        for place in Place::CLAIM {
            for uw in SEATS {
                for &target in place.targets() {
                    for writer in 0..3 {
                        runs += 1;
                        let schedule = Schedule {
                            kind,
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
                        let mut why =
                            later_writes(&tray, &schedule, &formed, &mut played.reps, writer);
                        if !why.is_empty() {
                            why.insert(0, label);
                            failing.push(why.join("; "));
                        }
                    }
                }
            }
        }
        (runs, failing)
    });
    let mut runs = 0;
    let mut failing = Vec::new();
    for (kind, (n, f)) in &results {
        println!("ST04-B2 {kind:?}: runs {n}, failing {}", f.len());
        runs += n;
        failing.extend(f.iter().cloned());
    }
    println!("ST04-B2 runs {runs}: failing {}", failing.len());
    for line in failing.iter().take(4) {
        println!("ST04-B2 {line}");
    }
    assert!(
        failing.is_empty(),
        "ST.04 B2: {} of {runs} runs fail; first:\n  {}",
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

/// B3: a fourth replica `d` adopts, through a serde round trip, the causal
/// snapshot and log of each of the three replicas after each of the
/// schedule's 15 steps; the schedule runs to its end, `d` delivers every
/// operation in the order sent, and must serve each replica's identified
/// read, the rule's document, the identities and R'; then `d` makes B2's
/// later writes and all four serve what B2 says. X on 0, Y on 1; U and W on
/// 0 and 1 both ways, the third taking U first; every target: 450 joins per
/// kind (Flat 180, Deep 270), 9,000 in all.
#[test]
fn st04_b3_a_joiner_by_state_transfer_at_any_step_serves_what_the_others_serve() {
    let tray = Tray::new();
    let desc = descriptor();
    let results = per_kind(&Kind::ALL, |kind| {
        let mut joins = 0;
        let mut failing = Vec::new();
        for place in Place::CLAIM {
            for uw in [(0, 1), (1, 0)] {
                for &target in place.targets() {
                    let schedule = Schedule {
                        kind,
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
                            if let Some(line) = join_at(&tray, &desc, &schedule, donor, at) {
                                failing.push(line);
                            }
                        }
                    }
                }
            }
        }
        (joins, failing)
    });
    let mut joins = 0;
    let mut failing = Vec::new();
    for (kind, (n, f)) in &results {
        println!("ST04-B3 {kind:?}: joins {n}, failing {}", f.len());
        joins += n;
        failing.extend(f.iter().cloned());
    }
    println!("ST04-B3 joins {joins}: failing {}", failing.len());
    for line in failing.iter().take(4) {
        println!("ST04-B3 {line}");
    }
    assert!(
        failing.is_empty(),
        "ST.04 B3: {} of {joins} joins fail; first:\n  {}",
        failing.len(),
        failing
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// One join: `d` adopts `donor`'s state after step `at`; `None` when it
/// serves everything the claim asks.
fn join_at(
    tray: &Tray,
    desc: &serde_json::Value,
    schedule: &Schedule,
    donor: usize,
    at: usize,
) -> Option<String> {
    let label = format!(
        "{}; d joins from replica {donor} after step {at}",
        schedule.label()
    );
    let mut step_no = 0;
    let mut taken: Option<Rep> = None;
    let played = play(tray, desc, schedule, |reps, _| {
        step_no += 1;
        if step_no == at {
            taken = Some(joiner(&reps[donor]));
        }
    });
    let mut d = taken.expect("set-up: the schedule has 15 steps");
    for event in &played.sent {
        d.receive(event.clone());
    }
    let formed = Formed::read(schedule.place, &played.formed, &label);
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
    let want = expected(schedule);
    let plain = default_read(&d);
    if plain != want {
        why.push(format!("d serves {plain}, not {want}"));
    }
    if let Err(line) = identities(schedule, &formed, &on_d, "d") {
        why.push(line);
    }
    if let Err(line) = r_prime(&d, "d") {
        why.push(line);
    }
    let Played { reps, .. } = played;
    let [a, b, c] = reps;
    let mut four = vec![a, b, c, d];
    why.extend(later_writes(tray, schedule, &formed, &mut four, 3));
    if why.is_empty() {
        None
    } else {
        why.insert(0, label);
        Some(why.join("; "))
    }
}

/// B6, guard: U unsets the leaf itself (no removal above the optional),
/// raced by W, on every schedule (288 per kind, 5,760 in all). Both
/// runtimes serve this today, and it is what the rule reads a removal above
/// against; it must keep converging on the same document.
#[test]
fn st04_b6_guard_an_optional_attribute_unset_directly_as_today() {
    let tray = Tray::new();
    let desc = descriptor();
    let tallies = per_kind(&Kind::ALL, |kind| {
        tally(&tray, &desc, kind, &[Place::Direct])
    });
    let (total, parted, value, identity, rp, lines) = report("B6-direct", &tallies);
    let failing = parted + value + identity + rp;
    assert!(
        failing == 0,
        "ST.04 B6 (direct unset): of {total} schedules, {parted} part, {value} serve another \
         value, {identity} break an identity, {rp} break R'; first:\n  {}",
        lines
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// B6, guard: U alone, in the three places (12 formations x 3 seats of U,
/// 108 per kind, 2,160 in all): `Holder.inner` unset in `Flat` and `Deep`
/// (a text keeps the `Box`, and the `Mid`, holding an empty string), the
/// `Box` with Y's 2 and what its own unset left of the leaf in `Direct`, on
/// every replica, one identified read.
#[test]
fn st04_b6_guard_a_removal_alone_as_today() {
    let tray = Tray::new();
    let desc = descriptor();
    let results = per_kind(&Kind::ALL, |kind| {
        let mut runs = 0;
        let mut lines = Vec::new();
        for place in Place::ALONE {
            for xy in SEATS {
                for x_first in [true, false] {
                    for u in 0..3 {
                        runs += 1;
                        let played = play_unset_alone(&tray, &desc, kind, place, xy, x_first, u);
                        let label = format!(
                            "{kind:?} {place:?}: X on {} Y on {}, U alone on {u}",
                            xy.0, xy.1
                        );
                        let want = unset_alone(kind, place);
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
        (runs, lines)
    });
    let mut runs = 0;
    let mut lines = Vec::new();
    for (kind, (n, l)) in &results {
        println!("ST04-B6-alone {kind:?}: runs {n}, failures {}", l.len());
        runs += n;
        lines.extend(l.iter().cloned());
    }
    println!(
        "ST04-B6 removal alone runs {runs}: failures {}",
        lines.len()
    );
    assert!(
        lines.is_empty(),
        "ST.04 B6 (removal alone): {} failures over {runs} runs; first:\n  {}",
        lines.len(),
        lines
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// OB2 of the spec, printed and never asserted: in `Deep` with W on `Box.k`,
/// a text keeps the `Mid` the removal reached (its reset recorded the
/// deletion), so the `Mid` is served though every write to it was covered by
/// U; its identity after the race and after the leaf is written again. And
/// U alone with a text: the `Box` kept.
#[test]
#[ignore = "ST.04 observation OB2, not a pass criterion: run with --ignored --nocapture"]
fn st04_observation_a_text_keeps_the_object_a_removal_reached() {
    let tray = Tray::new();
    let desc = descriptor();
    let schedule = Schedule {
        kind: Kind::Text,
        place: Place::Deep,
        xy: (0, 1),
        x_first: true,
        uw: (0, 1),
        u_first: true,
        target: Target::Outer,
    };
    let mut played = play(&tray, &desc, &schedule, |_, _| {});
    println!("ST04-OB2 formed {}", played.formed);
    println!(
        "ST04-OB2 after the race {}",
        identified_read(&played.reps[2])
    );
    let event = send!(played.reps[2], again_op(&tray, &schedule));
    played.reps[0].receive(event.clone());
    played.reps[1].receive(event);
    let op = rebuild_op(&tray, &schedule).expect("Outer rebuilds");
    let event = send!(played.reps[2], op);
    played.reps[0].receive(event.clone());
    played.reps[1].receive(event);
    println!(
        "ST04-OB2 after the leaf is written again {}",
        identified_read(&played.reps[0])
    );
    let alone = play_unset_alone(&tray, &desc, Kind::Text, Place::Flat, (0, 1), true, 0);
    println!(
        "ST04-OB2 U alone, Flat: formed {} then {}",
        alone.formed,
        identified_read(&alone.reps[1])
    );
}
