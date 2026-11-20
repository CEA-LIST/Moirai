//! EC.M2 row N8: our `moirai-crdt/tests/record.rs` from
//! `akira/moirai-modelstore` at `748a185`, translated to Léo's API on the
//! merged line (lane HEAD `b1e254c`). Same operations (`New`, then a read),
//! same assertions.
//!
//! Translation: our `BoxedLog<L>` (operation `Box<L::Command>`, read as
//! `Box<V>`) has no counterpart by name; his `Box<L>` reads as `Box<V>` but
//! keeps the operation unboxed, so a recursive record needs his explicit
//! operation form `field: Operation, Log => Value` (record.rs line 8 of his
//! macros: "so recursive collection operations need no log adapter"). The
//! explicit-value tests use it with `OptionUpdate<Box<Self>>`, which is the
//! operation layout our `OptionLog<BoxedLog<..>>` produced.
//! `recursive_record_with_generic_value_type` is not here: his generic form
//! derives the operation from the log, with no operation type to give, and is
//! measured as not translatable by `ecm2_n8_record_generic_probe.rs`.

use moirai_crdt::option::{OptionLog, update::OptionUpdate};
use moirai_macros::record;
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    crdt::query::Read,
    replica::{IsReplica, Replica},
};

/// Ours: `record::empty_record`.
#[test]
fn empty_record() {
    record!(Empty {});

    type Log = EmptyLog;
    type Tcb = Tcsb<Empty>;

    let mut replica =
        Replica::<Log, Tcb>::bootstrap(String::from("a").into(), &[&String::from("a")]);

    replica.send(Empty::New).unwrap();

    let value: EmptyValue = replica.query::<Read<EmptyValue>>(&Read::new());

    assert_eq!(value, EmptyValue::default());
}

/// Ours: `record::recursive_record_with_explicit_value_types`.
#[test]
fn recursive_record_with_explicit_value_types() {
    record!(TreeNode {
        node: OptionUpdate<Box<TreeNode>>, OptionLog<Box<TreeNodeLog>> => Option<Box<TreeNodeValue>>,
    });

    type Log = TreeNodeLog;
    type Tcb = Tcsb<TreeNode>;

    let mut replica =
        Replica::<Log, Tcb>::bootstrap(String::from("a").into(), &[&String::from("a")]);

    replica.send(TreeNode::New).unwrap();

    let value: TreeNodeValue = replica.query::<Read<TreeNodeValue>>(&Read::new());

    assert_eq!(value, TreeNodeValue::default());
    assert_eq!(value.node, None);
}

/// Ours: `record::mutually_recursive_records_with_explicit_value_types`.
#[test]
fn mutually_recursive_records_with_explicit_value_types() {
    record!(Left {
        right: OptionUpdate<Box<Right>>, OptionLog<Box<RightLog>> => Option<Box<RightValue>>,
    });
    record!(Right {
        left: OptionUpdate<Box<Left>>, OptionLog<Box<LeftLog>> => Option<Box<LeftValue>>,
    });

    type Log = LeftLog;
    type Tcb = Tcsb<Left>;

    let mut replica =
        Replica::<Log, Tcb>::bootstrap(String::from("a").into(), &[&String::from("a")]);

    replica.send(Left::New).unwrap();

    let value: LeftValue = replica.query::<Read<LeftValue>>(&Read::new());

    assert_eq!(value, LeftValue::default());
    assert_eq!(value.right, None);
}
