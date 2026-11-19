use moirai_network_node::{Application, Config, CoreReplica};
use moirai_protocol::{
    commitment::{
        commit_log::CommitmentLog, examples::commit_set::CommitSet,
        protocol::AtomicBroadcastProtocol,
    },
    crdt::query::Read,
    replica::IsReplica,
    state::po_log::VecLog,
};
use std::{collections::HashSet, io};

pub type Child = VecLog<CommitSet<String>>;

pub fn config() -> io::Result<Config> {
    let path = std::env::args_os().nth(1).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "usage: <binary> <config.json>")
    })?;
    Config::load(path)
}

pub fn application<A: AtomicBroadcastProtocol<Child>>() -> Application<CommitmentLog<Child, A>> {
    Application {
        query: |replica: &CoreReplica<CommitmentLog<Child, A>>| {
            let values: HashSet<String, _> = replica.query(&Read::new());
            let mut values: Vec<_> = values.into_iter().collect();
            values.sort_unstable();
            serde_json::json!({"values":values})
        },
        committed: |replica, id| {
            replica
                .state()
                .committed_frontier()
                .is_some_and(|frontier| frontier.contains(id))
        },
        no_op: || moirai_protocol::commitment::examples::commit_set::SetUpdate::NoOp,
    }
}
