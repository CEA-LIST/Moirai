//! State transfer of composite logs. Three replicas a, b, c write two rounds (the
//! first is then stable and pruned), b writes once more to a only and c once
//! more to nobody, so a's snapshot holds stable and unstable events. A fresh
//! replica d, bootstrapped with only its own id, adopts a's snapshot sent as
//! JSON; then d and b write concurrently and every pending message is
//! delivered in each of the 24 orders, followed by two rounds in which all
//! four write. All four must read one hand-computed value and agree on one
//! hand-computed stable frontier. The logs are a record! of a nested list of
//! union! slots plus a reference manager (the shape of log_serde) and
//! moirai-crdt's JsonLog; both hold an enable-wins flag that b enables and c
//! disables concurrently, before the join. Run with
//! `--features serde,test_utils`.
#![cfg(all(feature = "serde", feature = "test_utils"))]

use std::{collections::BTreeMap, fmt::Debug};

use moirai_crdt::{
    counter::{Counter, update::CounterUpdate},
    flag::{ew_flag::EWFlag, update::FlagUpdate},
    json::{Json, JsonChildValue, JsonLog, JsonValue},
    list::{
        nested_list::NestedListLog,
        update::{ListUpdate, NestedListUpdate},
    },
    map::update::MapUpdate,
};
use moirai_macros::{record, typed_graph, typed_graph::Arc, union};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    data_type::{policy::FairPolicy, query::Read},
    replica::{IsReplica, Replica, ReplicaId},
    state::{
        log::{EvalNested, IsLog},
        object_path::ObjectPath,
        po_log::VecLog,
    },
};
use petgraph::graph::DiGraph;
use rustc_hash::FxHashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct Points;

// As in log_serde: the reference manager as Arachne's codegen declares it.
typed_graph! {
    types {
        graph = ReferenceManager,
        vertex_kind = Instance,
        edge_kind = Ref,
        arc_kind = Refs,
    },
    vertices {
        Cell,
    },
    edges {
        Points [0, *],
    },
    arcs {
        CellToCell: Cell -> Cell (Points),
    }
}

union! {
    Slot = Count(CounterUpdate<i64>, VecLog::<Counter<i64>> => i64)
        | On(FlagUpdate, VecLog::<EWFlag> => bool)
}

record!(Linked {
    slots: NestedListLog<SlotLog>,
    refs: VecLog<ReferenceManager<FairPolicy>>,
});

type Up<L> = Replica<L, Tcsb<<L as IsLog>::Update>>;

const IDS: [&str; 4] = ["a", "b", "c", "d"];

/// The stable frontier by replica id.
fn frontier<L: IsLog>(r: &Up<L>) -> BTreeMap<String, usize> {
    let f = r.tcsb().last_stable_version();
    f.iter()
        .map(|(idx, seq)| {
            (
                f.resolver()
                    .resolve(idx)
                    .expect("a frontier index resolves")
                    .0
                    .clone(),
                seq,
            )
        })
        .collect()
}

/// One run: `ops` names every command by step, `adopt` moves a's snapshot
/// into d, `order` is the delivery order of the four messages pending after
/// the join. Checks every read and frontier against `want`.
fn run<L, V>(
    ops: fn(&str) -> L::Command,
    read: fn(&Up<L>) -> V,
    adopt: &dyn Fn(&Up<L>, &mut Up<L>),
    order: [usize; 4],
    want: &V,
) where
    L: IsLog,
    L::Update: Clone + Debug,
    V: PartialEq + Debug,
{
    let send = |r: &mut Up<L>, step: &str| {
        r.send(ops(step))
            .unwrap_or_else(|_| panic!("{step} was refused"))
    };
    let mut r: Vec<Up<L>> = IDS[..3]
        .iter()
        .map(|id| Up::<L>::bootstrap(ReplicaId::from(*id), &IDS[..3]))
        .collect();
    for round in 1..=2 {
        let sent: Vec<_> = (0..3)
            .map(|i| send(&mut r[i], &format!("{}{round}", IDS[i])))
            .collect();
        for (i, m) in sent.iter().enumerate() {
            for j in (0..3).filter(|&j| j != i) {
                r[j].receive(m.clone());
            }
        }
    }
    for (i, x) in r.iter().enumerate() {
        assert!(
            x.tcsb().causally_stable_events_count() >= 3,
            "{}: the first round is not stable",
            IDS[i]
        );
        assert!(
            x.tcsb().outbox().all(|e| e.id().seq() > 1),
            "{}: the first round is still in the outbox",
            IDS[i]
        );
    }
    // Unstable at a: b's x (a only). Not yet anywhere: c's y, concurrent with x.
    let x = send(&mut r[1], "x");
    r[0].receive(x.clone());
    let y = send(&mut r[2], "y");

    let mut d = Up::<L>::bootstrap(ReplicaId::from("d"), &["d"]);
    adopt(&r[0], &mut d);
    assert_eq!(
        d.id(),
        &ReplicaId::from("d"),
        "the adopter keeps its own id"
    );
    assert_eq!(read(&d), read(&r[0]), "d reads as a after adopting");
    r.push(d);

    // d and b write concurrently; then every pending message, in this order.
    let wd = send(&mut r[3], "wd");
    let wb = send(&mut r[1], "wb");
    let pending = [
        (x, vec![2, 3]),
        (y, vec![0, 1, 3]),
        (wd, vec![0, 1, 2]),
        (wb, vec![0, 2, 3]),
    ];
    for &k in &order {
        for &j in &pending[k].1 {
            r[j].receive(pending[k].0.clone());
        }
    }
    // Two rounds in which all four write, so everything before the last is stable.
    for round in 1..=2 {
        let sent: Vec<_> = (0..4)
            .map(|i| send(&mut r[i], &format!("f{}{round}", IDS[i])))
            .collect();
        for (i, m) in sent.iter().enumerate() {
            for j in (0..4).filter(|&j| j != i) {
                r[j].receive(m.clone());
            }
        }
    }
    // Events sent before the last round: a1 a2 fa1, b1 b2 x wb fb1, c1 c2 y fc1, wd fd1.
    let stable: BTreeMap<String, usize> = [("a", 3), ("b", 5), ("c", 4), ("d", 2)]
        .map(|(id, n)| (id.to_string(), n))
        .into();
    for (i, x) in r.iter().enumerate() {
        assert_eq!(
            &read(x),
            want,
            "{} reads the hand-computed value, order {order:?}",
            IDS[i]
        );
        assert_eq!(
            frontier(x),
            stable,
            "{}: stable frontier, order {order:?}",
            IDS[i]
        );
    }
}

/// The 24 delivery orders of four messages.
fn orders() -> Vec<[usize; 4]> {
    (0..256)
        .map(|n| [n % 4, n / 4 % 4, n / 16 % 4, n / 64])
        .filter(|p| (0..4).all(|v| p.contains(&v)))
        .collect()
}

fn cell(name: &str) -> Cell {
    Cell(ObjectPath::new("doc").map_entry(name.to_string()))
}

fn linked_op(step: &str) -> Linked {
    let slot = |pos, op| Linked::Slots(NestedListUpdate::Update { pos, op });
    let vertex = |name: &str| {
        Linked::Refs(ReferenceManagerUpdate::AddVertex {
            id: Instance::Cell(cell(name)),
        })
    };
    match step {
        "a1" => Linked::Slots(NestedListUpdate::Insert {
            pos: 0,
            op: Slot::Count(CounterUpdate::Inc(5)),
        }),
        "b1" => vertex("x"),
        "c1" => vertex("y"),
        "a2" => Linked::Slots(NestedListUpdate::Insert {
            pos: 1,
            op: Slot::On(FlagUpdate::Enable),
        }),
        "b2" => Linked::Refs(ReferenceManagerUpdate::AddArc(Refs::CellToCell(Arc {
            source: cell("x"),
            target: cell("y"),
            kind: Points,
        }))),
        "c2" => slot(0, Slot::Count(CounterUpdate::Inc(2))),
        "x" => slot(1, Slot::On(FlagUpdate::Enable)),
        "y" => slot(1, Slot::On(FlagUpdate::Disable)),
        "wd" => Linked::Slots(NestedListUpdate::Insert {
            pos: 0,
            op: Slot::Count(CounterUpdate::Dec(1)),
        }),
        "wb" => slot(0, Slot::Count(CounterUpdate::Inc(3))),
        filler => vertex(filler),
    }
}

type LinkedRead = (Vec<SlotValue>, Vec<String>, Vec<(String, String)>);

fn linked_read(r: &Up<LinkedLog>) -> LinkedRead {
    let v: LinkedValue<Vec<SlotValue>, DiGraph<Instance, Ref>> =
        r.state().execute_query(&Read::new());
    let mut nodes: Vec<String> = v.refs.node_weights().map(|n| format!("{n:?}")).collect();
    let mut arcs: Vec<(String, String)> = v
        .refs
        .raw_edges()
        .iter()
        .map(|e| {
            (
                format!("{:?}", v.refs[e.source()]),
                format!("{:?}", v.refs[e.target()]),
            )
        })
        .collect();
    nodes.sort();
    arcs.sort();
    (v.slots, nodes, arcs)
}

fn json_op(step: &str) -> Json {
    let at = |key: &str, op: Json| Json::Object(MapUpdate::Update(key.to_string(), Box::new(op)));
    let number = |n: f64| Json::Number(CounterUpdate::Inc(n));
    let list = |op| at("list", Json::Array(op));
    match step {
        "a1" => at("n", number(1.0)),
        "b1" => at("flag", Json::Boolean(FlagUpdate::Enable)),
        "c1" => list(NestedListUpdate::Insert {
            pos: 0,
            op: Box::new(number(4.0)),
        }),
        "a2" => at("n", number(2.0)),
        "b2" => at(
            "s",
            Json::String(ListUpdate::Insert {
                content: 'h',
                pos: 0,
            }),
        ),
        "c2" => list(NestedListUpdate::Update {
            pos: 0,
            op: Box::new(number(1.0)),
        }),
        "x" => at("flag", Json::Boolean(FlagUpdate::Enable)),
        "y" => at("flag", Json::Boolean(FlagUpdate::Disable)),
        "wd" => list(NestedListUpdate::Insert {
            pos: 0,
            op: Box::new(number(7.0)),
        }),
        "wb" => at(
            "s",
            Json::String(ListUpdate::Insert {
                content: 'i',
                pos: 1,
            }),
        ),
        _ => at("n", number(0.0)),
    }
}

fn json_read(r: &Up<JsonLog>) -> JsonValue {
    r.state().execute_query(&Read::new())
}

macro_rules! via_json {
    () => {
        &|donor, d| {
            let json = serde_json::to_string(&donor.snapshot()).expect("the snapshot serializes");
            d.adopt(serde_json::from_str(&json).expect("the snapshot deserializes"));
        }
    };
}

#[test]
fn composite_record_of_nested_unions_and_references_converges_after_adoption() {
    let count = |n: i64| SlotValue::Value(Box::new(SlotChildValue::Count(n)));
    let on = SlotValue::Value(Box::new(SlotChildValue::On(true)));
    let node = |name: &str| format!("{:?}", Instance::Cell(cell(name)));
    let mut nodes: Vec<String> = [
        "x", "y", "fa1", "fb1", "fc1", "fd1", "fa2", "fb2", "fc2", "fd2",
    ]
    .map(node)
    .into();
    nodes.sort();
    // -1 inserted by d before the count; 5 + 2 + 3; b's enable wins over c's concurrent disable.
    let want: LinkedRead = (
        vec![count(-1), count(10), on],
        nodes,
        vec![(node("x"), node("y"))],
    );
    for order in orders() {
        run::<LinkedLog, _>(linked_op, linked_read, via_json!(), order, &want);
    }
}

#[test]
fn composite_json_log_converges_after_adoption() {
    let v = |c: JsonChildValue| JsonValue::Value(Box::new(c));
    let want = v(JsonChildValue::Object(FxHashMap::from_iter([
        ("n".to_string(), v(JsonChildValue::Number(3.0))),
        ("flag".to_string(), v(JsonChildValue::Boolean(true))),
        ("s".to_string(), v(JsonChildValue::String("hi".to_string()))),
        (
            "list".to_string(),
            v(JsonChildValue::Array(vec![
                v(JsonChildValue::Number(7.0)),
                v(JsonChildValue::Number(5.0)),
            ])),
        ),
    ])));
    for order in orders() {
        run::<JsonLog, _>(json_op, json_read, via_json!(), order, &want);
    }
}
