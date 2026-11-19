use moirai_protocol::{commitment::wire_id::StableReplicaId, replica::ReplicaId};
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs, io,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    time::Duration,
};

#[derive(Clone, Debug, Deserialize)]
pub struct PeerConfig {
    pub replica_id: u16,
    #[serde(default)]
    pub replica_name: String,
    pub address: String,
}

/// Static membership. Unknown legacy fields are ignored for launcher compatibility.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub replica_id: u16,
    #[serde(default)]
    pub replica_name: String,
    pub listen_port: u16,
    pub http_port: Option<u16>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    #[serde(default = "pull_interval")]
    pub pull_interval_ms: NonZeroU64,
    #[serde(default = "one")]
    pub pull_peer_count: usize,
    /// Emit an ordinary replicated no-op after this long without a client update.
    /// `null` disables idle no-ops. Remote events and reads do not reset the timer.
    #[serde(default = "noop_interval")]
    pub noop_interval_ms: Option<NonZeroU64>,
    /// Used only by the core EventOracle in the commit_set example.
    #[serde(default = "event_timeout", alias = "heartbeat_interval_ms")]
    pub event_timeout_ms: NonZeroU64,
    #[serde(default = "request_timeout")]
    pub request_timeout_ms: NonZeroU64,
    #[serde(default = "http_limit")]
    pub http_optimistic_limit: NonZeroUsize,
    #[serde(default = "http_limit")]
    pub http_consistent_limit: NonZeroUsize,
    #[serde(default = "enabled")]
    pub memory_usage_enabled: bool,
    #[serde(default = "memory_interval")]
    pub memory_usage_interval_ms: NonZeroU64,
}

fn one() -> usize {
    1
}
fn http_limit() -> NonZeroUsize {
    NonZeroUsize::new(64).unwrap()
}
fn enabled() -> bool {
    true
}
fn pull_interval() -> NonZeroU64 {
    NonZeroU64::new(500).unwrap()
}
fn noop_interval() -> Option<NonZeroU64> {
    Some(memory_interval())
}
fn memory_interval() -> NonZeroU64 {
    NonZeroU64::new(1000).unwrap()
}
fn event_timeout() -> NonZeroU64 {
    NonZeroU64::new(10_000).unwrap()
}
fn request_timeout() -> NonZeroU64 {
    NonZeroU64::new(25_000).unwrap()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let config: Self = serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> io::Result<()> {
        let mut ids = BTreeSet::from([self.replica_id]);
        if self
            .peers
            .iter()
            .any(|peer| !ids.insert(peer.replica_id) || peer.address.is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "peer IDs must be unique and different from the local ID; addresses must be nonempty",
            ));
        }
        let now = std::time::Instant::now();
        for ms in [
            Some(self.pull_interval_ms),
            self.noop_interval_ms,
            Some(self.event_timeout_ms),
            Some(self.request_timeout_ms),
            Some(self.memory_usage_interval_ms),
        ]
        .into_iter()
        .flatten()
        {
            if now.checked_add(Duration::from_millis(ms.get())).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "timer duration is too large",
                ));
            }
        }
        Ok(())
    }

    /// Identical stable IDs and core IDs at every replica, in numeric order.
    pub fn members(&self) -> Vec<(StableReplicaId, ReplicaId)> {
        let mut ids: Vec<_> = self.peers.iter().map(|p| p.replica_id).collect();
        ids.push(self.replica_id);
        ids.sort_unstable();
        ids.into_iter()
            .map(|id| (StableReplicaId(id), id.to_string().into()))
            .collect()
    }
}
