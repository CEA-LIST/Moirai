//! Sequential set with majority commitment and the core event-driven oracle.
mod support;
use moirai_network_node::Node;
use moirai_protocol::commitment::{
    commit_log::CommitmentLog,
    commit_protocols::omega::{omega_commit::OmegaCommit, oracle::EventOracle},
    wire_id::StableReplicaId,
};

fn main() -> std::io::Result<()> {
    let config = support::config()?;
    // Falling back to self lets surviving members elect a live leader when older
    // event observations expire. EventOracle observes events inside OmegaCommit.
    let oracle = EventOracle::with_timeout(
        StableReplicaId(config.replica_id),
        std::time::Duration::from_millis(config.event_timeout_ms.get()),
    );
    let protocol =
        OmegaCommit::from_parts(oracle, config.members(), StableReplicaId(config.replica_id));
    let log = CommitmentLog::new(support::Child::default(), protocol);
    Node::new(config, log, support::application())?.run(None)
}
