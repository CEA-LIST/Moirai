//! BTree-backed u8 set. One node config; independent consensus runs on each configured port + 1.
#[path = "support/btree.rs"]
mod support;
use moirai_network_node::{
    Node,
    external_commit::creek::{self, Creek, WorkerConfig},
};
use moirai_protocol::commitment::{
    commit_log::CommitmentLog, commit_protocols::omega::oracle::EventOracle,
    wire_id::StableReplicaId,
};

fn main() -> std::io::Result<()> {
    let config = support::config()?;
    let oracle = EventOracle::with_timeout(
        StableReplicaId(config.replica_id),
        std::time::Duration::from_millis(config.event_timeout_ms.get()),
    );
    let protocol = Creek::new(oracle, config.members(), StableReplicaId(config.replica_id))?;
    let _worker = protocol.spawn_tcp(&config, WorkerConfig::default())?;
    let log = CommitmentLog::new(support::Child::default(), protocol);
    Node::new(config, log, support::application())?.run(Some(&mut creek::drive))
}
