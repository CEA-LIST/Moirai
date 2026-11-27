use moirai_network_node::{Application, Config};
use moirai_protocol::{
    commitment::{
        commit_log::CommitmentLog,
        examples::{
            commit_btree_set::{CommitBTreeSet, SetUpdate},
            tools::btree_set::BTreeLog,
        },
        protocol::AtomicBroadcastProtocol,
    },
    data_type::{query::Read, sequential::SequentialEval},
    replica::IsReplica,
};
use std::{collections::HashSet, io};

pub type Child = BTreeLog<CommitBTreeSet<String>>;

pub fn config() -> io::Result<Config> {
    let path = std::env::args_os().nth(1).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "usage: <binary> <config.json>")
    })?;
    Config::load(path)
}

pub fn application<A: AtomicBroadcastProtocol<Child>>() -> Application<CommitmentLog<Child, A>> {
    Application {
        query: |replica| {
            let values: HashSet<String, _> = replica.query(&Read::new());
            set_response(values)
        },
        query_stable: |replica| {
            let values: HashSet<String, _> =
                SequentialEval::eval(replica.state().child().stable(), &Read::new());
            set_response(values)
        },
        committed: |replica, id| {
            replica
                .state()
                .committed_frontier()
                .is_some_and(|frontier| frontier.contains(id))
        },
        no_op: || SetUpdate::NoOp,
    }
}

fn set_response(values: impl IntoIterator<Item = String>) -> serde_json::Value {
    let mut values: Vec<_> = values.into_iter().collect();
    values.sort_unstable();
    serde_json::json!({"values":values})
}
