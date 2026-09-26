//! ST.05, the interpreted half in process: K1 to K4 of
//! `cea-cdrt-knowledge/spec/items/ST.05.md`.
//!
//! A key of a keyed collection whose values are objects is removed, alone
//! or while a concurrent write (W) reaches the removed key's object or an
//! object one level below it. Once every replica has delivered every
//! operation, all serve one default read and one identified read (C1), the
//! document the keyed collection's rule fixes (C2: at a key of a class with
//! no subclass an object the removal emptied and nothing wrote after is left
//! out; at a key of a class with subclasses it stays, emptied), under
//! SI.01's identities (C3); later writes move nothing and an object written
//! again after the removal is new; a joiner by state transfer at any step
//! serves the same.
//!
//! These tests read the interpreted runtime only. The generated runtime is
//! held to the same schedules by the twin (K6, in `arachne-model-plane`).
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st05_removed_key -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

#[macro_use]
mod si01;
mod st05;

use std::collections::BTreeSet;

use moirai_protocol::replica::IsReplica;
use si01::{Rep, all_ids, default_read};
use st05::*;

/// One run's failures at its end, by kind.
#[derive(Default)]
struct Verdict {
    parted: bool,
    value: bool,
    identity: bool,
    r_prime: bool,
    lines: Vec<String>,
}

/// Judge replicas that have all delivered every operation: one default and
/// one identified read (C1), `want` (C2), the identities (C3), R'.
fn judge(
    place: Place,
    label: &str,
    reps: &[&Rep],
    formed: &Formed,
    want: &serde_json::Value,
    (kept, left_out): (&[Role], &[Role]),
) -> Verdict {
    let mut verdict = Verdict::default();
    let defaults: Vec<_> = reps.iter().map(|r| default_read(r)).collect();
    let ids: Vec<_> = reps.iter().map(|r| identified_read(r)).collect();
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
    for (seat, served) in defaults.iter().enumerate() {
        if served != want {
            verdict.value = true;
            verdict.lines.push(format!(
                "{label}: replica {seat} serves {served}, not {want}"
            ));
        }
    }
    for (seat, doc) in ids.iter().enumerate() {
        if let Err(why) = identities(
            place,
            formed,
            kept,
            left_out,
            doc,
            &format!("{label}: replica {seat}"),
        ) {
            verdict.identity = true;
            verdict.lines.push(why);
        }
    }
    for (seat, replica) in reps.iter().enumerate() {
        if let Err(why) = r_prime(replica, &format!("{label}: replica {seat}")) {
            verdict.r_prime = true;
            verdict.lines.push(why);
        }
    }
    verdict
}

/// Counts over many runs, per place.
#[derive(Default)]
struct Tally {
    total: usize,
    parted: usize,
    value: usize,
    identity: usize,
    r_prime: usize,
    per_place: Vec<(Place, usize, usize, usize)>,
    lines: Vec<String>,
}

impl Tally {
    fn add(&mut self, place: Place, verdict: Verdict) {
        self.total += 1;
        self.parted += verdict.parted as usize;
        self.value += verdict.value as usize;
        self.identity += verdict.identity as usize;
        self.r_prime += verdict.r_prime as usize;
        match self.per_place.iter_mut().find(|(p, ..)| *p == place) {
            Some((_, total, parted, value)) => {
                *total += 1;
                *parted += verdict.parted as usize;
                *value += verdict.value as usize;
            }
            None => {
                self.per_place
                    .push((place, 1, verdict.parted as usize, verdict.value as usize))
            }
        }
        self.lines.extend(verdict.lines);
    }

    fn report(&self, tag: &str, what: &str) {
        println!(
            "{tag} {what} {}: parted {} (P1), value other than the rule's {} (P2), identity {}, \
             r' {} (P3)",
            self.total, self.parted, self.value, self.identity, self.r_prime
        );
        for (place, total, parted, value) in &self.per_place {
            println!(
                "{tag} {place:?}: parted {parted} of {total}, value other than the rule's \
                 {value} of {total}"
            );
        }
        for line in self.lines.iter().take(6) {
            println!("{tag} {line}");
        }
    }

    fn verdict(&self, item: &str, what: &str) {
        assert!(
            self.parted == 0 && self.value == 0 && self.identity == 0 && self.r_prime == 0,
            "ST.05 {item}: of {} {what}, {} part (C1), {} serve a value other than the rule's \
             (C2), {} break an identity (C3), {} break R'; first:\n  {}",
            self.total,
            self.parted,
            self.value,
            self.identity,
            self.r_prime,
            self.lines
                .iter()
                .take(4)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n  ")
        );
    }
}

/// K1: every schedule of the race, 1584 (11 targets over the 7 places, 144
/// each).
#[test]
fn st05_k1_a_key_removal_raced_by_a_write_converges_on_the_rule() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut tally = Tally::default();
    for place in Place::ALL {
        for schedule in schedules(place) {
            let played = play(&tray, &desc, &schedule, |_, _| {});
            let label = schedule.label();
            let formed = Formed::read(place, &played.formed, &label);
            let reps: Vec<&Rep> = played.reps.iter().collect();
            let verdict = judge(
                place,
                &label,
                &reps,
                &formed,
                &expected(&schedule),
                race_roles(&schedule),
            );
            tally.add(place, verdict);
        }
    }
    tally.report("ST05-K1", "schedules");
    tally.verdict("K1", "schedules");
}

/// K2: the removal alone, in the 7 places: 12 formations x 3 seats of U
/// (252 runs).
#[test]
fn st05_k2_a_key_removal_alone_converges_on_the_rule() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut tally = Tally::default();
    for place in Place::ALL {
        for xy in SEATS {
            for x_first in [true, false] {
                for u in 0..3 {
                    let played = play_alone(&tray, &desc, place, xy, x_first, u, |_, _| {});
                    let label = alone_label(place, xy, x_first, u);
                    let formed = Formed::read(place, &played.formed, &label);
                    let reps: Vec<&Rep> = played.reps.iter().collect();
                    let verdict = judge(
                        place,
                        &label,
                        &reps,
                        &formed,
                        &alone(place),
                        alone_roles(place),
                    );
                    tally.add(place, verdict);
                }
            }
        }
    }
    tally.report("ST05-K2", "runs");
    tally.verdict("K2", "runs");
}

/// The later writes after the race on `reps`, from `writer`, each delivered
/// to every other replica; the failures, one line each.
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

/// The later writes after a removal alone on `reps`: the removed key's
/// object written again (+5) by `writer`, then once more (+1) by `next`,
/// each delivered everywhere.
fn later_writes_alone(
    tray: &Tray,
    place: Place,
    formed: &Formed,
    reps: &mut [Rep],
    writer: usize,
    next: usize,
) -> Vec<String> {
    let names: Vec<String> = (0..reps.len())
        .map(|seat| format!("replica {seat}"))
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut seen: BTreeSet<String> = formed.all();
    for replica in reps.iter() {
        seen.extend(all_ids(&identified_read(replica)));
    }
    let event = send!(reps[writer], again_alone_op(tray, place));
    for (seat, replica) in reps.iter_mut().enumerate() {
        if seat != writer {
            replica.receive(event.clone());
        }
    }
    let reads = reads_of(&reps.iter().collect::<Vec<_>>(), &names);
    let (mut why, given) = check_again_alone(place, formed, &seen, &reads);
    let event = send!(reps[next], once_more_op(tray, place));
    for (seat, replica) in reps.iter_mut().enumerate() {
        if seat != next {
            replica.receive(event.clone());
        }
    }
    let reads = reads_of(&reps.iter().collect::<Vec<_>>(), &names);
    why.extend(check_once_more(place, given.as_deref(), &reads));
    why
}

/// K3: after the race, one replica writes W's target again (+1), delivered
/// everywhere, then, where the race left out the `Mid` at key `j`, writes it
/// again (+5); X on 0, Y on 1, the third taking X first; U and W in every
/// seating, the third taking U first; every target; each replica in turn as
/// the writer (198 runs). After a removal alone (X on 0, Y on 1, the third
/// taking X first; U on each replica), each replica in turn writes the
/// removed key's object again (+5) and the next once more (+1) (63 runs).
#[test]
fn st05_k3_after_the_removal_a_write_moves_nothing_and_an_object_written_again_is_new() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut runs = 0;
    let mut failing = Vec::new();
    for place in Place::ALL {
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
        for u in 0..3 {
            for writer in 0..3 {
                runs += 1;
                let label = format!(
                    "{}; then replica {writer} writes it again",
                    alone_label(place, (0, 1), true, u)
                );
                let mut played = play_alone(&tray, &desc, place, (0, 1), true, u, |_, _| {});
                let formed = Formed::read(place, &played.formed, &label);
                let mut why = later_writes_alone(
                    &tray,
                    place,
                    &formed,
                    &mut played.reps,
                    writer,
                    (writer + 1) % 3,
                );
                if !why.is_empty() {
                    why.insert(0, label);
                    failing.push(why.join("; "));
                }
            }
        }
    }
    println!("ST05-K3 runs {runs}: failing {}", failing.len());
    for place in Place::ALL {
        let prefix = format!("{place:?}:");
        println!(
            "ST05-K3 {place:?}: failing {}",
            failing.iter().filter(|l| l.starts_with(&prefix)).count()
        );
    }
    for line in failing.iter().take(4) {
        println!("ST05-K3 {line}");
    }
    assert!(
        failing.is_empty(),
        "ST.05 K3: {} of {runs} runs fail; first:\n  {}",
        failing.len(),
        failing
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// What a joiner `d` must serve once it has delivered what it lacks: each
/// replica's identified read, `want`, the identities, R'.
fn check_joiner(
    place: Place,
    reps: &[Rep; 3],
    d: &Rep,
    formed: &Formed,
    want: &serde_json::Value,
    (kept, left_out): (&[Role], &[Role]),
) -> Vec<String> {
    let mut why = Vec::new();
    let on_d = identified_read(d);
    for (seat, replica) in reps.iter().enumerate() {
        let there = identified_read(replica);
        if there != on_d {
            why.push(format!(
                "d serves {on_d} where replica {seat} serves {there}"
            ));
        }
    }
    let plain = default_read(d);
    if plain != *want {
        why.push(format!("d serves {plain}, not {want}"));
    }
    if let Err(line) = identities(place, formed, kept, left_out, &on_d, "d") {
        why.push(line);
    }
    if let Err(line) = r_prime(d, "d") {
        why.push(line);
    }
    why
}

/// K4: a fourth replica `d` adopts, through a serde round trip, the causal
/// snapshot and log of each of the three replicas after each step; the
/// schedule runs to its end, `d` delivers every operation in the order sent,
/// and must serve each replica's identified read, the rule's document, the
/// identities and R'; then `d` makes K3's later writes and all four serve
/// what K3 says. The race: X on 0, Y on 1; U and W on 0 and 1 both ways, the
/// third taking U first; every target; after each of 15 steps (990 joins).
/// The removal alone: X on 0, Y on 1, U on 0; after each of 12 steps (252
/// joins).
#[test]
fn st05_k4_a_joiner_by_state_transfer_at_any_step_serves_what_the_others_serve() {
    let tray = Tray::new();
    let desc = descriptor();
    let mut joins = 0;
    let mut failing = Vec::new();
    for place in Place::ALL {
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
                        let mut why = check_joiner(
                            place,
                            &played.reps,
                            &d,
                            &formed,
                            &expected(&schedule),
                            race_roles(&schedule),
                        );
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
        for donor in 0..3 {
            for at in 1..=12 {
                joins += 1;
                let label = format!(
                    "{}; d joins from replica {donor} after step {at}",
                    alone_label(place, (0, 1), true, 0)
                );
                let mut step_no = 0;
                let mut taken: Option<Rep> = None;
                let played = play_alone(&tray, &desc, place, (0, 1), true, 0, |reps, _| {
                    step_no += 1;
                    if step_no == at {
                        taken = Some(joiner(&reps[donor]));
                    }
                });
                let mut d = taken.expect("set-up: the removal alone has 12 steps");
                for event in &played.sent {
                    d.receive(event.clone());
                }
                let formed = Formed::read(place, &played.formed, &label);
                let mut why = check_joiner(
                    place,
                    &played.reps,
                    &d,
                    &formed,
                    &alone(place),
                    alone_roles(place),
                );
                let Played { reps, .. } = played;
                let [a, b, c] = reps;
                let mut four = vec![a, b, c, d];
                why.extend(later_writes_alone(&tray, place, &formed, &mut four, 3, 1));
                if !why.is_empty() {
                    why.insert(0, label);
                    failing.push(why.join("; "));
                }
            }
        }
    }
    println!("ST05-K4 joins {joins}: failing {}", failing.len());
    for place in Place::ALL {
        let prefix = format!("{place:?}:");
        println!(
            "ST05-K4 {place:?}: failing {}",
            failing.iter().filter(|l| l.starts_with(&prefix)).count()
        );
    }
    for line in failing.iter().take(4) {
        println!("ST05-K4 {line}");
    }
    assert!(
        failing.is_empty(),
        "ST.05 K4: {} of {joins} joins fail; first:\n  {}",
        failing.len(),
        failing
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}
