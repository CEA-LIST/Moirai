//! NW.31k round 2 (vault spec/items/NW.31k.md, claim part 2): every log type
//! moirai-macros emits (union! and record!, as a generated crate holds them)
//! and moirai-crdt's log types serialize and deserialize under the "serde"
//! feature, and a log read back from its JSON reads what the log it came from
//! reads. Ground truth: the value the original log reads, and the values
//! written below by hand from the ops sent.
#![cfg(feature = "serde")]

use moirai_crdt::{
    bag::{aw_bag::AWBagLog, rw_bag::RWBagLog, update::BagUpdate},
    counter::{Counter, update::CounterUpdate},
    flag::{ew_flag::EWFlag, update::FlagUpdate},
    graph::uw_multidigraph::UWGraphLog,
    json::{Json, JsonLog, JsonValue},
    list::{
        eg_walker::List,
        nested_list::NestedListLog,
        update::{ListUpdate, NestedListUpdate},
    },
    map::{rw_map::RWMapLog, update::MapUpdate, uw_map::UWMapLog},
    option::{OptionLog, update::OptionUpdate},
    set::{ewflag_set::EWFlagSetLog, update::SetUpdate},
    utils::bootstrap::twins_log,
};
use moirai_macros::{record, typed_graph, typed_graph::Arc, union};
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    data_type::{policy::FairPolicy, query::Read},
    replica::{IsReplica, Replica},
    state::{
        log::{EvalNested, IsLog},
        object_path::ObjectPath,
        po_log::{GraphLog, VecLog},
    },
};
use petgraph::graph::DiGraph;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct Points;

// The reference manager as Arachne's codegen declares it (graph =
// ReferenceManager, held as VecLog<ReferenceManager<FairPolicy>>).
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

// The generic form: a nested list of unions and the reference manager.
record!(Linked {
    slots: NestedListLog<SlotLog>,
    refs: VecLog<ReferenceManager<FairPolicy>>,
});

// The explicit form: moirai-crdt's other logs and its own union! (JsonLog).
record!(Bits {
    json: JsonLog => JsonValue,
    bag: AWBagLog<String> => FxHashMap<String, usize>,
    rbag: RWBagLog<String> => FxHashMap<String, usize>,
    tags: EWFlagSetLog<String> => FxHashSet<String>,
    scores: RWMapLog<String, VecLog<Counter<i64>>> => FxHashMap<String, i64>,
    names: UWMapLog<String, GraphLog<List<char>>> => FxHashMap<String, String>,
    note: OptionLog<VecLog<Counter<i64>>> => Option<i64>,
});

type Up<L> = Replica<L, Tcsb<<L as IsLog>::Update>>;

fn serde_both<T: Serialize + DeserializeOwned>() {}

/// a sends `first`, b takes them and sends `second`, a takes those, so a's
/// first ops are known to both replicas (stable at a) and b's are not.
/// Returns a's log.
fn exchange<L>(first: Vec<L::Command>, second: Vec<L::Command>) -> L
where
    L: IsLog,
    L::Update: std::fmt::Debug,
{
    let (mut a, mut b): (Up<L>, Up<L>) = twins_log::<L>();
    for op in first {
        let event = a.send(op).unwrap_or_else(|_| panic!("a refused an op"));
        b.receive(event);
    }
    for op in second {
        let event = b.send(op).unwrap_or_else(|_| panic!("b refused an op"));
        a.receive(event);
    }
    a.into_state()
}

fn round_trip<L: Serialize + DeserializeOwned>(log: &L) -> L {
    let text = serde_json::to_string(log).expect("the log serializes");
    serde_json::from_str(&text).expect("the log deserializes from its own JSON")
}

fn cell(name: &str) -> Cell {
    Cell(ObjectPath::new("doc").map_entry(name.to_string()))
}

fn nodes_and_arcs(g: &DiGraph<Instance, Ref>) -> (Vec<Instance>, Vec<(Instance, Instance, Ref)>) {
    let mut nodes: Vec<Instance> = g.node_weights().cloned().collect();
    let mut arcs: Vec<(Instance, Instance, Ref)> = g
        .raw_edges()
        .iter()
        .map(|e| {
            (
                g[e.source()].clone(),
                g[e.target()].clone(),
                e.weight.clone(),
            )
        })
        .collect();
    nodes.sort_by_key(|n| format!("{n:?}"));
    arcs.sort_by_key(|a| format!("{a:?}"));
    (nodes, arcs)
}

#[test]
fn nw31k_every_log_type_is_serde() {
    serde_both::<SlotLog>();
    serde_both::<SlotContainer>();
    serde_both::<SlotChild>();
    serde_both::<LinkedLog>();
    serde_both::<BitsLog>();
    serde_both::<JsonLog>();
    serde_both::<NestedListLog<JsonLog>>();
    serde_both::<UWMapLog<String, JsonLog>>();
    serde_both::<RWMapLog<String, VecLog<Counter<f64>>>>();
    serde_both::<AWBagLog<String>>();
    serde_both::<RWBagLog<String>>();
    serde_both::<EWFlagSetLog<String>>();
    serde_both::<OptionLog<VecLog<Counter<i64>>>>();
    serde_both::<UWGraphLog<String, String, VecLog<Counter<i64>>, VecLog<Counter<i64>>>>();
    serde_both::<VecLog<ReferenceManager<FairPolicy>>>();
}

#[test]
fn nw31k_a_record_of_a_nested_list_of_unions_and_references_round_trips() {
    let (x, y, z) = (cell("x"), cell("y"), cell("z"));
    let arc = |s: &Cell, t: &Cell| {
        ReferenceManagerUpdate::AddArc(Refs::CellToCell(Arc {
            source: s.clone(),
            target: t.clone(),
            kind: Points,
        }))
    };
    let vertex = |c: &Cell| ReferenceManagerUpdate::AddVertex {
        id: Instance::Cell(c.clone()),
    };
    let first = vec![
        Linked::Slots(NestedListUpdate::Insert {
            pos: 0,
            op: Slot::Count(CounterUpdate::Inc(5)),
        }),
        Linked::Slots(NestedListUpdate::Insert {
            pos: 1,
            op: Slot::On(FlagUpdate::Enable),
        }),
        Linked::Refs(vertex(&x)),
        Linked::Refs(vertex(&y)),
        Linked::Refs(arc(&x, &y)),
    ];
    let second = vec![
        Linked::Slots(NestedListUpdate::Update {
            pos: 0,
            op: Slot::Count(CounterUpdate::Inc(2)),
        }),
        Linked::Slots(NestedListUpdate::Insert {
            pos: 0,
            op: Slot::Count(CounterUpdate::Dec(1)),
        }),
        Linked::Refs(vertex(&z)),
        Linked::Refs(arc(&y, &z)),
    ];
    let log: LinkedLog = exchange(first, second);
    let back = round_trip(&log);

    type V = LinkedValue<Vec<SlotValue>, DiGraph<Instance, Ref>>;
    let (want, got): (V, V) = (
        log.execute_query(&Read::<V>::new()),
        back.execute_query(&Read::<V>::new()),
    );
    let count = |n: i64| SlotValue::Value(Box::new(SlotChildValue::Count(n)));
    let on = SlotValue::Value(Box::new(SlotChildValue::On(true)));
    assert_eq!(
        want.slots,
        vec![count(-1), count(7), on],
        "the original log reads the ops sent"
    );
    assert_eq!(
        got.slots, want.slots,
        "the nested list of unions reads as before the round trip"
    );
    let refs = nodes_and_arcs(&want.refs);
    assert_eq!(refs.0.len(), 3, "the original log holds three vertices");
    assert_eq!(refs.1.len(), 2, "the original log holds two arcs");
    assert_eq!(
        nodes_and_arcs(&got.refs),
        refs,
        "the references read as before the round trip"
    );
}

#[test]
fn nw31k_a_record_of_moirai_crdt_logs_round_trips() {
    let s = |s: &str| s.to_string();
    let json_number = |key: &str, n: f64| {
        Bits::Json(Json::Object(MapUpdate::Update(
            s(key),
            Box::new(Json::Number(CounterUpdate::Inc(n))),
        )))
    };
    let json_list = Bits::Json(Json::Object(MapUpdate::Update(
        s("list"),
        Box::new(Json::Array(NestedListUpdate::Insert {
            pos: 0,
            op: Box::new(Json::String(ListUpdate::Insert {
                content: 'q',
                pos: 0,
            })),
        })),
    )));
    let first = vec![
        json_number("n", 1.5),
        json_list,
        Bits::Bag(BagUpdate::Add(s("pear"))),
        Bits::Bag(BagUpdate::Add(s("pear"))),
        Bits::Rbag(BagUpdate::Add(s("fig"))),
        Bits::Tags(SetUpdate::Add(s("red"))),
        Bits::Scores(MapUpdate::Update(s("ann"), CounterUpdate::Inc(4))),
        Bits::Names(MapUpdate::Update(
            s("t"),
            ListUpdate::Insert {
                content: 'h',
                pos: 0,
            },
        )),
        Bits::Note(OptionUpdate::Set(CounterUpdate::Inc(9))),
    ];
    let second = vec![
        json_number("n", 2.0),
        Bits::Bag(BagUpdate::Add(s("plum"))),
        Bits::Tags(SetUpdate::Add(s("blue"))),
        Bits::Scores(MapUpdate::Update(s("bob"), CounterUpdate::Inc(3))),
        Bits::Names(MapUpdate::Update(
            s("t"),
            ListUpdate::Insert {
                content: 'i',
                pos: 1,
            },
        )),
    ];
    let log: BitsLog = exchange(first, second);
    let back = round_trip(&log);

    let (want, got): (BitsValue, BitsValue) = (
        log.execute_query(&Read::<BitsValue>::new()),
        back.execute_query(&Read::<BitsValue>::new()),
    );
    assert_eq!(
        want.bag,
        FxHashMap::from_iter([(s("pear"), 2), (s("plum"), 1)]),
        "the original log reads the ops sent"
    );
    assert_eq!(want.tags, FxHashSet::from_iter([s("red"), s("blue")]));
    assert_eq!(
        want.scores,
        FxHashMap::from_iter([(s("ann"), 4), (s("bob"), 3)])
    );
    assert_eq!(want.names, FxHashMap::from_iter([(s("t"), s("hi"))]));
    assert_eq!(want.note, Some(9));
    assert_ne!(want.json, JsonValue::Unset);
    assert_eq!(got, want, "every field reads as before the round trip");
}
