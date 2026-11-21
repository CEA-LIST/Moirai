//! ST.02, claim B (`cea-cdrt-knowledge/spec/items/ST.02.md`): the logs an
//! optional containment is generated as, composed by hand the way Arachne
//! composes them, with a chosen wrapper between each `OptionLog` and its
//! content, and the schedules that hold them to ST.01's V1 and to claim A's
//! rule. Shared by `st02_held_by_an_optional.rs` (B1: no wrapper, `Box`,
//! `BoxedLog`) and the probe crate `st02_wrapper_probe.rs` builds (B2: a
//! user wrapper that forwards `IsLog`, and one that forwards it on another
//! thread).
//!
//! `composition!(module, W, wrap, V, peel)` defines, in `module`:
//!
//! ```text
//! Shape   n: counter                       (record!)
//! A, B    shape_super: Shape               (record!, as Arachne writes a subclass)
//! ShapeKind = A | B                        (union!)
//! Cell    n: counter                       (record!)
//! Bx      k: counter, cell: OptionLog<W<CellLog>>, slot: OptionLog<W<ShapeKindLog>>
//! Holder  tick: counter, slot: OptionLog<W<ShapeKindLog>>, inner: OptionLog<W<BxLog>>
//! ```
//!
//! `W` is the wrapper type, `wrap` turns an operation of the content into an
//! operation of `W<content>`, `V` is what `W` makes of the content's read
//! value (`BoxedLog` reads as a `Box`), and `peel` takes it back. Its
//! `run(threads)` plays every schedule below and answers the failures, one
//! line each; with `threads`, every send, delivery and read runs on a thread
//! of its own.
//!
//! The schedules, three replicas each. T: replica 0 increments `tick`,
//! delivered everywhere. Then, in each place:
//!
//! - `St01Flat` (ST.01's Flat): X on 0 sets an `A` (+1) into `Holder.slot`,
//!   Y on 1 a `B` (+2), the third delivering X first, so every replica holds
//!   the conflict `[A, B]`. U unsets `Holder.slot`; W increments one member
//!   by 10. V1: `slot` holds that member alone, holding 10.
//! - `St01Outer` (ST.01's Outer): the same conflict in `Bx.slot` of the `Bx`
//!   in `Holder.inner`, and U unsets `Holder.inner`. V1: the `Bx`, `k` 0,
//!   its `slot` holding the member alone, holding 10.
//! - `ClaimA` (claim A's Flat): X on 0 writes the `Cell` in `Bx.cell` (+1),
//!   Y on 1 increments `Bx.k` by 2; U unsets `Holder.inner`; W increments
//!   `Bx.k` (`Sibling`) or the `Cell`'s `n` (`Object`) by 10. The rule: the
//!   `Bx` holding `k` 10 and no `Cell`, or `k` 0 and the `Cell` holding 10.
//!
//! U and W are raced in every seating (6), the third replica taking U first
//! or W first (2), for each member or target (2): 24 schedules a place, 72
//! in all. The expected documents are written by hand; nothing is read from
//! the code under test to decide them.

/// The identity: what a transparent wrapper does to an operation.
pub fn same<T>(value: T) -> T {
    value
}

/// A value read through a transparent wrapper, as it is.
pub fn id_ref<T>(value: &T) -> &T {
    value
}

/// A value read through `BoxedLog`, out of its box.
pub fn box_ref<T>(value: &Box<T>) -> &T {
    value
}

/// A read value, unchanged: the `V` of a transparent wrapper.
pub type Id<T> = T;

/// No wrapper at all: the `W` of the composition Arachne writes for a
/// non-recursive metamodel.
pub type Bare<L> = L;

/// Where the race is run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    St01Flat,
    St01Outer,
    ClaimA,
}

pub const PLACES: [Place; 3] = [Place::St01Flat, Place::St01Outer, Place::ClaimA];

pub const SEATS: [(usize, usize); 6] = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)];

/// What W increments: a member of the conflict (`'A'` or `'B'`) in ST.01's
/// places; `Bx.k` (`'k'`) or the `Cell` (`'c'`) in claim A's.
pub fn aims(place: Place) -> [char; 2] {
    match place {
        Place::St01Flat | Place::St01Outer => ['A', 'B'],
        Place::ClaimA => ['k', 'c'],
    }
}

/// The document every replica must serve once T, X, Y, U and W are
/// delivered, by hand, in the form `read` gives.
pub fn expected(place: Place, aim: char) -> serde_json::Value {
    use serde_json::json;
    match (place, aim) {
        (Place::St01Flat, member) => {
            json!({"tick": 1, "slot": {"eClass": member.to_string(), "n": 10}})
        }
        (Place::St01Outer, member) => json!({"tick": 1, "inner": {
            "eClass": "Bx", "k": 0, "slot": {"eClass": member.to_string(), "n": 10}
        }}),
        (Place::ClaimA, 'k') => json!({"tick": 1, "inner": {"eClass": "Bx", "k": 10}}),
        (Place::ClaimA, _) => json!({"tick": 1, "inner": {
            "eClass": "Bx", "k": 0, "cell": {"eClass": "Cell", "n": 10}
        }}),
    }
}

/// How many of `failures` are in `place` (its `Debug` name).
pub fn by_place(failures: &[String], place: &str) -> usize {
    failures
        .iter()
        .filter(|line| line.contains(&format!(": {place},")))
        .count()
}

/// Run `work` on a thread of its own, or here.
pub fn on<R: Send>(threads: bool, work: impl FnOnce() -> R + Send) -> R {
    if threads {
        std::thread::scope(|scope| {
            scope
                .spawn(work)
                .join()
                .expect("a thread of the schedule panicked")
        })
    } else {
        work()
    }
}

macro_rules! composition {
    ($m:ident, $W:ident, $wrap:path, $V:ident, $peel:path) => {
        pub mod $m {
            #![allow(dead_code)]
            use super::*;
            use moirai_crdt::counter::resettable_counter::Counter;
            use moirai_crdt::option::{OptionLog, Optional};
            use moirai_crdt::utils::membership::triplet_log;
            use moirai_macros::{record, union};
            use moirai_protocol::broadcast::tcsb::Tcsb;
            use moirai_protocol::crdt::query::Read;
            use moirai_protocol::replica::{IsReplica, Replica};
            use moirai_protocol::state::po_log::VecLog;
            use serde_json::{Map, Value, json};

            record!(Shape { n: VecLog<Counter<i32>> => i32 });
            record!(A { shape_super: ShapeLog => ShapeValue });
            record!(B { shape_super: ShapeLog => ShapeValue });
            union!(ShapeKind = A(A, ALog => AValue) | B(B, BLog => BValue));
            record!(Cell { n: VecLog<Counter<i32>> => i32 });
            record!(Bx {
                k: VecLog<Counter<i32>> => i32,
                cell: OptionLog<$W<CellLog>> => Option<$V<CellValue>>,
                slot: OptionLog<$W<ShapeKindLog>> => Option<$V<ShapeKindValue>>,
            });
            record!(Holder {
                tick: VecLog<Counter<i32>> => i32,
                slot: OptionLog<$W<ShapeKindLog>> => Option<$V<ShapeKindValue>>,
                inner: OptionLog<$W<BxLog>> => Option<$V<BxValue>>,
            });

            pub type Rep = Replica<HolderLog, Tcsb<Holder>>;

            fn member(class: char, by: i32) -> ShapeKind {
                let shape = Shape::N(Counter::Inc(by));
                if class == 'A' {
                    ShapeKind::A(A::ShapeSuper(shape))
                } else {
                    ShapeKind::B(B::ShapeSuper(shape))
                }
            }

            fn in_box(op: Bx) -> Holder {
                Holder::Inner(Optional::Set($wrap(op)))
            }

            /// The operations of the race, by place: X, Y, U, and W aimed
            /// at `aim`.
            fn ops(place: Place, aim: char) -> [Holder; 4] {
                match place {
                    Place::St01Flat => [
                        Holder::Slot(Optional::Set($wrap(member('A', 1)))),
                        Holder::Slot(Optional::Set($wrap(member('B', 2)))),
                        Holder::Slot(Optional::Unset),
                        Holder::Slot(Optional::Set($wrap(member(aim, 10)))),
                    ],
                    Place::St01Outer => [
                        in_box(Bx::Slot(Optional::Set($wrap(member('A', 1))))),
                        in_box(Bx::Slot(Optional::Set($wrap(member('B', 2))))),
                        Holder::Inner(Optional::Unset),
                        in_box(Bx::Slot(Optional::Set($wrap(member(aim, 10))))),
                    ],
                    Place::ClaimA => [
                        in_box(Bx::Cell(Optional::Set($wrap(Cell::N(Counter::Inc(1)))))),
                        in_box(Bx::K(Counter::Inc(2))),
                        Holder::Inner(Optional::Unset),
                        if aim == 'k' {
                            in_box(Bx::K(Counter::Inc(10)))
                        } else {
                            in_box(Bx::Cell(Optional::Set($wrap(Cell::N(Counter::Inc(10))))))
                        },
                    ],
                }
            }

            fn kind(value: &ShapeKindValue) -> Option<Value> {
                let one = |child: &ShapeKindChildValue| match child {
                    ShapeKindChildValue::A(a) => json!({"eClass": "A", "n": a.shape_super.n}),
                    ShapeKindChildValue::B(b) => json!({"eClass": "B", "n": b.shape_super.n}),
                };
                match value {
                    ShapeKindValue::Unset => None,
                    ShapeKindValue::Value(child) => Some(one(child)),
                    ShapeKindValue::Conflict(children) => {
                        let mut held: Vec<Value> = children.iter().map(one).collect();
                        held.sort_by_key(|v| v["eClass"].as_str().unwrap_or("").to_string());
                        match held.len() {
                            0 => None,
                            1 => held.pop(),
                            _ => Some(json!({"__conflict": held})),
                        }
                    }
                }
            }

            /// The read, in a JSON form written for this composition:
            /// `tick`; `slot` and `inner` absent when unset; a `Bx` with
            /// `eClass` and `k`, its `cell` and `slot` absent when unset; a
            /// conflict as `{"__conflict": [...]}` ordered by class.
            pub fn read(replica: &Rep) -> Value {
                let value: HolderValue = replica.query(&Read::<HolderValue>::new());
                let mut out = Map::new();
                out.insert("tick".into(), json!(value.tick));
                if let Some(slot) = value.slot.as_ref().and_then(|s| kind($peel(s))) {
                    out.insert("slot".into(), slot);
                }
                if let Some(boxed) = value.inner.as_ref() {
                    let boxed = $peel(boxed);
                    let mut bx = Map::new();
                    bx.insert("eClass".into(), json!("Bx"));
                    bx.insert("k".into(), json!(boxed.k));
                    if let Some(cell) = boxed.cell.as_ref() {
                        bx.insert("cell".into(), json!({"eClass": "Cell", "n": $peel(cell).n}));
                    }
                    if let Some(slot) = boxed.slot.as_ref().and_then(|s| kind($peel(s))) {
                        bx.insert("slot".into(), slot);
                    }
                    out.insert("inner".into(), Value::Object(bx));
                }
                Value::Object(out)
            }

            /// Every schedule; the failures, one line each, and the number
            /// of schedules.
            pub fn run(threads: bool) -> (Vec<String>, usize) {
                let mut failures = Vec::new();
                let mut runs = 0;
                for place in PLACES {
                    for (u, w) in SEATS {
                        for u_first in [true, false] {
                            for aim in aims(place) {
                                runs += 1;
                                let label = format!(
                                    "{}: {place:?}, U on {u}, W({aim}) on {w}, third U first \
                                     {u_first}, threads {threads}",
                                    stringify!($m)
                                );
                                let served = play(place, aim, (u, w), u_first, threads);
                                let want = expected(place, aim);
                                for (seat, doc) in served.iter().enumerate() {
                                    if *doc != want {
                                        failures.push(format!(
                                            "{label}: replica {seat} serves {doc}, not {want}"
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                (failures, runs)
            }

            fn play(
                place: Place,
                aim: char,
                (p, q): (usize, usize),
                u_first: bool,
                threads: bool,
            ) -> Vec<Value> {
                let (a, b, c) = triplet_log::<HolderLog>();
                let mut reps = [a, b, c];
                let send = |reps: &mut [Rep; 3], seat: usize, op: Holder| {
                    let replica = &mut reps[seat];
                    on(threads, move || {
                        replica
                            .send(op)
                            .unwrap_or_else(|why| panic!("set-up: refused: {why}"))
                    })
                };
                let deliver = |reps: &mut [Rep; 3], seat: usize, event| {
                    let replica = &mut reps[seat];
                    on(threads, move || replica.receive(event));
                };
                let t = send(&mut reps, 0, Holder::Tick(Counter::Inc(1)));
                deliver(&mut reps, 1, t.clone());
                deliver(&mut reps, 2, t);
                let [x, y, removal, write] = ops(place, aim);
                let ex = send(&mut reps, 0, x);
                let ey = send(&mut reps, 1, y);
                deliver(&mut reps, 0, ey.clone());
                deliver(&mut reps, 1, ex.clone());
                deliver(&mut reps, 2, ex);
                deliver(&mut reps, 2, ey);
                let r = 3 - p - q;
                let eu = send(&mut reps, p, removal);
                let ew = send(&mut reps, q, write);
                deliver(&mut reps, p, ew.clone());
                deliver(&mut reps, q, eu.clone());
                if u_first {
                    deliver(&mut reps, r, eu);
                    deliver(&mut reps, r, ew);
                } else {
                    deliver(&mut reps, r, ew);
                    deliver(&mut reps, r, eu);
                }
                reps.iter()
                    .map(|replica| on(threads, move || read(replica)))
                    .collect()
            }
        }
    };
}
