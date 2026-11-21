use moirai_crdt::option::OptionLog;
use moirai_macros::record;
use moirai_protocol::{
    broadcast::tcsb::Tcsb,
    crdt::{eval::EvalNested, query::Read},
    replica::{IsReplica, Replica},
    state::log::BoxedLog,
};

#[test]
fn empty_record() {
    record!(Empty {});

    type Log = EmptyLog;
    type Tcb = Tcsb<Empty>;

    let mut replica = Replica::<Log, Tcb>::bootstrap(String::from("a"), &[&String::from("a")]);

    replica.send(Empty::New).unwrap();

    let value: EmptyValue = replica.query::<Read<EmptyValue>>(&Read::new());

    assert_eq!(value, EmptyValue::default());
}

#[test]
fn recursive_record_with_generic_value_type() {
    record!(Empty {});

    record!(TreeNode {
        node: OptionLog<BoxedLog<TreeNodeLog>>,
    });

    #[derive(Clone, Debug, Default, PartialEq)]
    struct TreeValue(TreeNodeValue<Option<Box<TreeValue>>>);

    impl EvalNested<Read<TreeValue>> for TreeNodeLog {
        fn execute_query(&self, _: &Read<TreeValue>) -> TreeValue {
            TreeValue(<TreeNodeLog as EvalNested<
                Read<TreeNodeValue<Option<Box<TreeValue>>>>,
            >>::execute_query(self, &Read::new()))
        }
    }

    type Log = TreeNodeLog;
    type Tcb = Tcsb<TreeNode>;

    let mut replica = Replica::<Log, Tcb>::bootstrap(String::from("a"), &[&String::from("a")]);

    replica.send(TreeNode::New).unwrap();

    let value: TreeValue = replica.query::<Read<TreeValue>>(&Read::new());

    assert_eq!(value, TreeValue::default());
}

#[test]
fn recursive_record_with_explicit_value_types() {
    record!(TreeNode {
        node: OptionLog<BoxedLog<TreeNodeLog>> => Option<Box<TreeNodeValue>>,
    });

    type Log = TreeNodeLog;
    type Tcb = Tcsb<TreeNode>;

    let mut replica = Replica::<Log, Tcb>::bootstrap(String::from("a"), &[&String::from("a")]);

    replica.send(TreeNode::New).unwrap();

    let value: TreeNodeValue = replica.query::<Read<TreeNodeValue>>(&Read::new());

    assert_eq!(value, TreeNodeValue::default());
    assert_eq!(value.node, None);
}

#[test]
fn mutually_recursive_records_with_explicit_value_types() {
    record!(Left {
        right: OptionLog<BoxedLog<RightLog>> => Option<Box<RightValue>>,
    });
    record!(Right {
        left: OptionLog<BoxedLog<LeftLog>> => Option<Box<LeftValue>>,
    });

    type Log = LeftLog;
    type Tcb = Tcsb<Left>;

    let mut replica = Replica::<Log, Tcb>::bootstrap(String::from("a"), &[&String::from("a")]);

    replica.send(Left::New).unwrap();

    let value: LeftValue = replica.query::<Read<LeftValue>>(&Read::new());

    assert_eq!(value, LeftValue::default());
    assert_eq!(value.right, None);
}
