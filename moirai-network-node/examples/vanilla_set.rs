//! Sequential set with all-member Lamport timestamp commitment.
mod support;
use moirai_network_node::Node;
use moirai_protocol::commitment::{
    commit_log::CommitmentLog,
    commit_protocols::lamport::{lamport_commit::LamportCommit, lamport_tracker::LamportTracker},
};

fn main() -> std::io::Result<()> {
    let config = support::config()?;
    let protocol = LamportCommit::from_parts(config.members(), LamportTracker::default());
    let log = CommitmentLog::new(support::Child::default(), protocol);
    Node::new(config, log, support::application())?.run(None)
}
