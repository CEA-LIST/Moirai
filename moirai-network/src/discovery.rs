//! Optional peer discovery through a bootnode session directory.
//!
//! Without this, peers come from a static `PEERS` list dialled exactly once at
//! startup, so nothing already running can ever reach a replica that appears
//! later. With it, a replica re-registers with the bootnode on an interval and
//! dials whatever the returned roster contains that it is not already talking
//! to.
//!
//! # Additive by construction
//!
//! A node with no discovery configured behaves exactly as before: no thread is
//! spawned, no request is made, and `PEERS` still populates the transport. That
//! is the guard rail for the whole phase — every pre-existing e2e scenario runs
//! with discovery off and must stay green.
//!
//! # Why a thread and a channel
//!
//! The reconcile is an HTTP round trip and the node's event loop is what
//! applies operations and moves replication traffic. Doing the request inline
//! would stall that loop for the request timeout every interval whenever the
//! bootnode is slow or gone — and stalling a replica on the availability of
//! the *directory* would defeat the point of a directory that is allowed to
//! fail. So the poll runs on its own thread and hands rosters over an `mpsc`
//! channel, which is how the HTTP adapter already talks to the node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde::Deserialize;

use crate::transport::PeerId;

/// How long a single bootnode request may take. Short: a slow directory must
/// not delay the next reconcile by more than one interval.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Granularity at which the poll thread notices a `leave()`.
const WAKE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Base URL of the bootnode, e.g. `http://bootnode:7000`.
    pub bootnode_url: String,
    /// Session to join. Replicas in different sessions never learn about each
    /// other, which is what lets one bootnode serve several runs.
    pub session: String,
    /// This replica's id — the same string as `REPLICA_ID`.
    pub replica_id: String,
    /// `host:port` at which *other replicas* can reach this one's replication
    /// listener. Not necessarily what this process bound: in a container the
    /// bind is `0.0.0.0:9001` and the reachable address is `node-a:9001`.
    pub advertise_addr: String,
    /// Gap between re-registrations. Also the TTL refresh rate, so it must be
    /// comfortably below the bootnode's TTL.
    pub interval: Duration,
}

/// A peer as the directory reports it.
#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveredPeer {
    pub id: PeerId,
    pub addr: String,
}

#[derive(Debug, Deserialize)]
struct Roster {
    peers: Vec<DiscoveredPeer>,
    #[allow(dead_code)]
    epoch: u64,
}

/// Handle on the running poll thread.
pub struct Discovery {
    rx: Receiver<Vec<DiscoveredPeer>>,
    leaving: Arc<AtomicBool>,
}

impl Discovery {
    /// Start polling. The first registration happens immediately, so a replica
    /// does not wait a whole interval to become visible.
    pub fn spawn(config: DiscoveryConfig) -> Self {
        let (tx, rx) = mpsc::channel();
        let leaving = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&leaving);

        thread::spawn(move || {
            let client = match reqwest::blocking::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("[{}] discovery disabled: {e}", config.replica_id);
                    return;
                }
            };

            let base = config.bootnode_url.trim_end_matches('/');
            let register_url = format!("{base}/session/{}/register", config.session);
            let leave_url = format!("{base}/session/{}/leave", config.session);

            loop {
                if stop.load(Ordering::Relaxed) {
                    let sent = client
                        .post(&leave_url)
                        .json(&serde_json::json!({ "id": config.replica_id }))
                        .send();
                    eprintln!(
                        "[{}] left session `{}`: {}",
                        config.replica_id,
                        config.session,
                        match sent {
                            Ok(r) => format!("{}", r.status()),
                            Err(e) => format!("{e}"),
                        }
                    );
                    return;
                }

                match client
                    .post(&register_url)
                    .json(&serde_json::json!({
                        "id": config.replica_id,
                        "addr": config.advertise_addr,
                    }))
                    .send()
                    .and_then(|r| r.error_for_status())
                    .and_then(|r| r.json::<Roster>())
                {
                    Ok(roster) => {
                        // A closed channel means the node is gone.
                        if tx.send(roster.peers).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        // Not fatal, and deliberately not retried faster. The
                        // bootnode is allowed to be down; the session carries
                        // on with the peers it already has.
                        eprintln!("[{}] bootnode register failed: {e}", config.replica_id);
                    }
                }

                let mut slept = Duration::ZERO;
                while slept < config.interval && !stop.load(Ordering::Relaxed) {
                    thread::sleep(WAKE_INTERVAL);
                    slept += WAKE_INTERVAL;
                }
            }
        });

        Self { rx, leaving }
    }

    /// The most recent roster, or `None` if none arrived since the last call.
    ///
    /// Intermediate rosters are dropped rather than queued: only the newest
    /// one describes the session now, and a node that fell behind should catch
    /// up in one step instead of replaying stale membership.
    pub fn latest_roster(&self) -> Option<Vec<DiscoveredPeer>> {
        let mut newest = None;
        loop {
            match self.rx.try_recv() {
                Ok(roster) => newest = Some(roster),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return newest,
            }
        }
    }

    /// Deregister from the session and stop polling.
    ///
    /// This is a *directory* departure. It removes the replica from the
    /// roster so nobody dials it again; it does not remove it from any peer's
    /// causal member set, so causal stability keeps waiting for it exactly as
    /// it would for a replica that crashed. Separating those two is the point
    /// of the experiment, and the second one is phase 2.
    pub fn leave(&self) {
        self.leaving.store(true, Ordering::Relaxed);
    }
}
