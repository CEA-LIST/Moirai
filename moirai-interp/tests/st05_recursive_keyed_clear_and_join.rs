//! ST.05 round 4, in process: K10 and K11 of
//! `cea-cdrt-knowledge/spec/items/ST.05.md` (its section "Round 4, written
//! before any rerun").
//!
//! Round 3's K7 to K9 (`st05_recursive_keyed_class.rs`) hold a record class
//! that reaches itself through keyed collections; they leave out a key
//! removed by `MapOp::Clear` and a joiner by state transfer. Here, on K8's
//! and K9's metamodel (`Mid.sub` of `Mid`, `Mid.boxes` of `Box`, `Box.sub`
//! of `Mid`):
//!
//! - K10: `Clear` of the `sub` of the `Box` at `k`, which holds two keys once
//!   formed (the `Mid` at `j`, reaching both cycles, and the `Mid` at `e`),
//!   alone from each replica and raced by a write two keyed levels below it
//!   (the `Mid` at `i` or the `Box` at `d` under the `Mid` at `j`, or a `Mid`
//!   at a key `n` nothing wrote before) in every seating and both orders at
//!   the third replica; then an object the `Clear` emptied written again,
//!   and once more;
//! - K11: a fourth replica joining by state transfer (a serde round trip of
//!   a donor's log and causal snapshot) after every step of the probe's
//!   schedule, K8's and K9's, and K10's, from each replica, which then
//!   delivers what it lacks, must serve what the three serve, and whose
//!   later writes every replica then serves.
//!
//! Every replica is read in both forms after every judged send and delivery
//! (R'), and at every checkpoint, once every operation sent has been
//! delivered everywhere, the replicas must serve one default and one
//! identified read, the document the keyed collection's rule gives (written
//! by hand in `st05recursive/mod.rs`), and the identities SI.01 fixes.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st05_recursive_keyed_clear_and_join -- --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod si01;
mod st05recursive;

use moirai_protocol::replica::IsReplica;
use si01::{Rep, three};
use st05recursive::{Meta, Ops, Place, Run, SEATS, Scenario, Tally, Target, report};

/// Three fresh replicas of `meta`'s model, the table installed on all three.
fn replicas(meta: Meta) -> Vec<Rep> {
    let (a, b, c) = three(&meta.descriptor());
    vec![a, b, c]
}

/// K10: `Clear` of a keyed collection of a class that reaches itself
/// through keyed collections. The formation (T, N1, N2, E and K, each from
/// replica 0 and delivered everywhere); then the `Clear`, alone on each
/// replica, or raced by W on `Inner`, `Back` or `Fresh` in every seating and
/// both orders at the third; then a writer, each replica in turn, writes an
/// object the `Clear` emptied (or, after a race, W's target), and the next
/// replica writes again. 9 runs alone and 108 raced, 8 checkpoints each.
#[test]
fn st05_k10_a_clear_of_a_recursive_keyed_collection_alone_or_raced_converges_on_the_rule() {
    let ops = Ops::new(Meta::Cycles);
    let mut tally = Tally::default();
    let mut runs = 0;
    let mut failing = 0;
    let mut scenarios = Vec::new();
    for u in 0..3 {
        scenarios.push(Scenario::ClearAlone { u });
    }
    for target in [Target::Inner, Target::Back, Target::Fresh] {
        for uw in SEATS {
            for u_first in [true, false] {
                scenarios.push(Scenario::ClearRace {
                    target,
                    uw,
                    u_first,
                });
            }
        }
    }
    for scenario in scenarios {
        for w in 0..3 {
            let n = (w + 1) % 3;
            let before = tally.failures();
            let mut run = Run::new(
                replicas(scenario.meta()),
                &mut tally,
                format!("{scenario:?}, then replica {w} writes and replica {n} again"),
                true,
            );
            run.play(scenario, &ops);
            run.later(scenario, &ops, w, n);
            runs += 1;
            failing += usize::from(tally.failures() > before);
        }
    }
    let line = format!("{}, failing runs {failing}", tally.line("ST05-K10", runs));
    report(&line, &tally);
}

/// K11: a joiner by state transfer, at every step of every schedule, from
/// every replica. The schedules: the probe's (K7); K8's removal alone on
/// replica 0 at each place; K9's race with U and W on replicas 0 and 1 both
/// ways, the third taking U first, every target; K10's `Clear` alone on
/// replica 0; K10's race with U and W on 0 and 1 both ways, the third taking
/// U first, every target. The schedule itself is not judged here (K7 to
/// K10 judge it); its checkpoints are read for the identities they serve.
/// The joiner, `d`, adopts a serde round trip of the donor's log and causal
/// snapshot after the step, delivers every operation sent, in the order
/// sent, and must serve what the three serve, the rule's document and the
/// identities; then `d` writes (the later writes of `Scenario::later`, the
/// first by `d`, the second by replica 1), and all four must serve each.
/// 837 runs, one join each, 3 checkpoints judged each.
#[test]
fn st05_k11_a_joiner_by_state_transfer_of_a_recursive_keyed_model_serves_what_the_others_serve() {
    let mut schedules = vec![Scenario::Probe];
    for place in [Place::Top, Place::Direct, Place::Indirect] {
        schedules.push(Scenario::Alone { place, u: 0 });
    }
    for target in [Target::Joint, Target::Inner, Target::Back] {
        for uw in [(0, 1), (1, 0)] {
            schedules.push(Scenario::Race {
                target,
                uw,
                u_first: true,
            });
        }
    }
    schedules.push(Scenario::ClearAlone { u: 0 });
    for target in [Target::Inner, Target::Back, Target::Fresh] {
        for uw in [(0, 1), (1, 0)] {
            schedules.push(Scenario::ClearRace {
                target,
                uw,
                u_first: true,
            });
        }
    }
    let mut tally = Tally::default();
    let mut joins = 0;
    let mut failing = 0;
    for scenario in schedules {
        let ops = Ops::new(scenario.meta());
        // How many steps the schedule has, from a run that takes no joiner.
        let mut counting = Tally::default();
        let mut dry = Run::new(
            replicas(scenario.meta()),
            &mut counting,
            format!("{scenario:?}"),
            false,
        );
        dry.play(scenario, &ops);
        let steps = dry.steps;
        for donor in 0..3 {
            for at in 1..=steps {
                let before = tally.failures();
                let mut run = Run::new(
                    replicas(scenario.meta()),
                    &mut tally,
                    format!("{scenario:?}; d joins from replica {donor} after step {at}"),
                    false,
                );
                run.join = Some((at, donor));
                run.play(scenario, &ops);
                let mut d = run
                    .joined
                    .take()
                    .expect("set-up: the joiner is taken within the schedule");
                for event in &run.sent {
                    d.receive(event.clone());
                }
                run.reps.push(d);
                run.judge = true;
                run.checkpoint(
                    "d, once it delivered every operation",
                    &scenario.ended().settled(),
                );
                run.later(scenario, &ops, 3, 1);
                joins += 1;
                failing += usize::from(tally.failures() > before);
            }
        }
    }
    let line = format!("{}, failing runs {failing}", tally.line("ST05-K11", joins));
    report(&line, &tally);
}
