//! Optional outbound reporting to a monitoring dashboard.
//!
//! A replica with `DASHBOARD_URL` configured dials *out* to the dashboard and
//! posts what it has delivered and what its bookkeeping looks like. The
//! dashboard never dials in.
//!
//! # Why push, and not the obvious poll
//!
//! Polling `/api/…` on every replica would work inside Docker and nowhere else.
//! Replicas are peers on laptops behind NAT — dial-out only, which is the whole
//! reason the architecture has a bootnode with a relay fallback for NAT-blocked
//! pairs. A monitoring path that needs every replica to accept an inbound
//! connection contradicts the topology being built. Pushing also means the
//! dashboard discovers who exists by being told, so it needs no roster and no
//! directory of its own: a replica appears when it first reports and goes stale
//! when it stops.
//!
//! Same shape as [`crate::discovery`], deliberately: optional environment
//! variable, outbound blocking `reqwest`, its own thread, an `mpsc` channel to
//! the event loop. This is that pattern reused, not a new mechanism.
//!
//! # What it may cost the replica: nothing measurable
//!
//! This is an acceptance criterion, not an aspiration —
//! `experiments/t-dashboard-overhead/` holds the measurement.
//!
//! 1. **Unset costs one `Option` check.** No thread, no request, no delivery
//!    trace, no timestamp, no allocation. Every call site tests
//!    `self.dashboard.is_none()` and returns *before* building anything.
//! 2. **Enabled costs a small owned struct and a `try_send`.** No JSON is
//!    encoded and no HTTP is performed on the replica's thread. The operation
//!    travels as `L::Op` — moved, not cloned, out of the pending map — and is
//!    serialised on the sender thread.
//! 3. **The channel is bounded and never blocks.** `try_send`; a full queue
//!    drops the record and bumps a counter that rides along in the next report,
//!    so a dashboard shows honestly that it is missing data rather than
//!    displaying a quietly incomplete picture.
//! 4. **Requests are batched.** The sender thread accumulates and flushes on
//!    whichever comes first: [`BATCH_MAX`] records or [`FLUSH_INTERVAL`]. One
//!    request per operation is exactly the cost this is built to avoid.
//! 5. **A dead dashboard is invisible.** Retry with backoff on the sender
//!    thread; the replica is not told and does not log per operation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{json, Value};

/// How long one POST may take. Short on purpose: the sender thread is serial,
/// so a dashboard that hangs must not cost more than this per flush.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Records that may queue before new ones are dropped.
///
/// Sized for one flush interval at a few thousand operations per second, which
/// is far above anything this rig sustains. Beyond it the newest records are
/// worth more than a growing backlog of old ones.
const QUEUE_DEPTH: usize = 1024;

/// Records that force a flush before the interval expires.
const BATCH_MAX: usize = 256;

/// Longest a record waits on the sender thread before being posted.
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// First retry delay after a failed POST, doubled up to [`MAX_BACKOFF`].
const MIN_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// Where to post, e.g. `http://dashboard:8090`. The ingest path is appended.
    pub url: String,
    pub replica_id: String,
    /// Gap between the replica's state snapshots. Delivery records do not wait
    /// for it; see [`FLUSH_INTERVAL`].
    pub interval: Duration,
}

impl DashboardConfig {
    /// Reads `DASHBOARD_URL` and `DASHBOARD_INTERVAL_MS`, returning `None` when
    /// the first is unset — which is the whole opt-in.
    pub fn from_env(replica_id: &str) -> Option<Self> {
        let url = std::env::var("DASHBOARD_URL").ok()?;
        if url.trim().is_empty() {
            return None;
        }
        let interval = std::env::var("DASHBOARD_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(500));
        Some(Self {
            url: url.trim().to_string(),
            replica_id: replica_id.to_string(),
            interval,
        })
    }
}

/// One delivery, as the replica hands it over. Deliberately owned and plain:
/// building it is the entire cost paid on the replica's thread.
#[derive(Debug)]
pub struct EventRecord<O> {
    pub id: String,
    pub origin: String,
    pub seq: usize,
    pub lamport: usize,
    /// The replica's own clock at delivery. There is no global clock here, so a
    /// propagation time the dashboard computes from two of these carries their
    /// skew; it is labelled as such rather than dressed up.
    pub ts_ms: u64,
    pub local: bool,
    pub applied: bool,
    pub redundant_on_arrival: bool,
    pub reset: bool,
    /// `(origin:seq, was concurrent)` of the operations this delivery removed.
    pub superseded: Vec<(String, bool)>,
    /// Moved, not cloned, out of the replica's pending map. `None` once the map
    /// has evicted it, which only happens if reporting has stalled for far
    /// longer than a flush.
    pub op: Option<O>,
}

/// The replica's own view of itself, refreshed on the configured interval.
#[derive(Debug, Clone)]
pub struct SnapshotRecord {
    pub uptime_ms: u64,
    pub metrics: Value,
    pub state: Value,
    pub peers: Vec<(String, String)>,
}

enum Msg<O> {
    Event(Box<EventRecord<O>>),
    Snapshot(Box<SnapshotRecord>),
}

/// Handle on the sender thread. Dropping it lets the thread finish.
pub struct DashboardSink<O> {
    tx: SyncSender<Msg<O>>,
    /// Records discarded because the queue was full. Reported in the next
    /// payload that gets through.
    dropped: Arc<AtomicU64>,
    interval: Duration,
}

impl<O: Serialize + Send + 'static> DashboardSink<O> {
    pub fn spawn(config: DashboardConfig) -> Self {
        let (tx, rx) = sync_channel::<Msg<O>>(QUEUE_DEPTH);
        let dropped = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&dropped);
        let interval = config.interval;

        thread::spawn(move || {
            let client = match reqwest::blocking::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("[{}] dashboard reporting disabled: {e}", config.replica_id);
                    return;
                }
            };
            let endpoint = format!("{}/api/ingest", config.url.trim_end_matches('/'));
            eprintln!(
                "[{}] dashboard reporting to {}",
                config.replica_id, endpoint
            );

            let mut pending: Vec<EventRecord<O>> = Vec::with_capacity(BATCH_MAX);
            // Kept and re-sent with every payload, so each one is
            // self-describing: a dashboard that has just started, or just
            // restarted, is caught up by the next flush with no separate hello
            // to fall out of step.
            let mut latest: Option<SnapshotRecord> = None;
            let mut last_flush = Instant::now();
            let mut backoff = MIN_BACKOFF;
            let mut connected = false;

            loop {
                let mut due = false;
                match rx.recv_timeout(FLUSH_INTERVAL) {
                    Ok(Msg::Event(record)) => {
                        pending.push(*record);
                        due = pending.len() >= BATCH_MAX;
                    }
                    Ok(Msg::Snapshot(snapshot)) => {
                        latest = Some(*snapshot);
                        due = true;
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    // The replica is gone.
                    Err(RecvTimeoutError::Disconnected) => return,
                }
                if !due && last_flush.elapsed() < FLUSH_INTERVAL {
                    continue;
                }
                if pending.is_empty() && latest.is_none() {
                    continue;
                }
                last_flush = Instant::now();

                let payload = encode(
                    &config.replica_id,
                    latest.as_ref(),
                    std::mem::take(&mut pending),
                    counter.load(Ordering::Relaxed),
                );
                match client
                    .post(&endpoint)
                    .json(&payload)
                    .send()
                    .and_then(|r| r.error_for_status())
                {
                    Ok(_) => {
                        if !connected {
                            eprintln!("[{}] dashboard reachable", config.replica_id);
                            connected = true;
                        }
                        backoff = MIN_BACKOFF;
                    }
                    Err(e) => {
                        if connected {
                            eprintln!("[{}] dashboard unreachable: {e}", config.replica_id);
                            connected = false;
                        }
                        // Back off before taking more records rather than
                        // retrying this payload: the next one is fresher, and a
                        // dashboard started after the rig must not need the rig
                        // restarted. Records that arrive during the sleep fill
                        // the queue and then drop, which is counted.
                        thread::sleep(backoff);
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                }
            }
        });

        Self {
            tx,
            dropped,
            interval,
        }
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// How many records have been dropped so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Queue one delivery, or drop it. Never blocks; see the module header.
    pub fn offer_event(&self, record: EventRecord<O>) {
        self.enqueue(Msg::Event(Box::new(record)));
    }

    /// Queue a fresh view of this replica, or drop it.
    pub fn offer_snapshot(&self, snapshot: SnapshotRecord) {
        self.enqueue(Msg::Snapshot(Box::new(snapshot)));
    }

    fn enqueue(&self, msg: Msg<O>) {
        match self.tx.try_send(msg) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Turn a batch into the wire payload. Runs on the sender thread, never on the
/// replica's.
fn encode<O: Serialize>(
    replica_id: &str,
    snapshot: Option<&SnapshotRecord>,
    events: Vec<EventRecord<O>>,
    dropped: u64,
) -> Value {
    let events: Vec<Value> = events
        .into_iter()
        .map(|e| {
            json!({
                "id": e.id,
                "origin": e.origin,
                "seq": e.seq,
                "lamport": e.lamport,
                "ts_ms": e.ts_ms,
                "local": e.local,
                "applied": e.applied,
                "redundant_on_arrival": e.redundant_on_arrival,
                "reset": e.reset,
                "superseded": e.superseded.iter()
                    .map(|(id, concurrent)| json!({ "id": id, "concurrent": concurrent }))
                    .collect::<Vec<_>>(),
                "op": e.op.as_ref().and_then(|op| serde_json::to_value(op).ok()),
            })
        })
        .collect();

    let mut payload = json!({
        "replica_id": replica_id,
        "ts_ms": now_ms(),
        "dropped_reports": dropped,
        "events": events,
    });
    if let Some(snapshot) = snapshot {
        let state_digest = crate::workload::state_digest(&snapshot.state);
        payload["uptime_ms"] = json!(snapshot.uptime_ms);
        payload["metrics"] = snapshot.metrics.clone();
        payload["state"] = snapshot.state.clone();
        payload["state_digest"] = json!(state_digest);
        payload["peers"] = snapshot
            .peers
            .iter()
            .map(|(id, status)| json!({ "id": id, "status": status }))
            .collect();
    }
    payload
}

/// Milliseconds since the Unix epoch, as the calling replica sees them.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_record(n: usize) -> EventRecord<String> {
        EventRecord {
            id: format!("a:{n}"),
            origin: "a".into(),
            seq: n,
            lamport: n,
            ts_ms: 0,
            local: true,
            applied: true,
            redundant_on_arrival: false,
            reset: false,
            superseded: Vec::new(),
            op: None,
        }
    }

    #[test]
    fn from_env_is_off_unless_asked() {
        assert!(DashboardConfig::from_env("a").is_none() || std::env::var("DASHBOARD_URL").is_ok());
    }

    #[test]
    fn a_full_queue_drops_instead_of_blocking() {
        // Nothing is listening, so the sender thread sits in backoff and the
        // queue fills. `offer_event` must still return immediately.
        let sink = DashboardSink::<String>::spawn(DashboardConfig {
            url: "http://127.0.0.1:1".to_string(),
            replica_id: "a".to_string(),
            interval: Duration::from_millis(100),
        });
        let start = Instant::now();
        for n in 0..(QUEUE_DEPTH * 4) {
            sink.offer_event(a_record(n));
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "offer_event blocked; a full queue must drop, not apply back-pressure"
        );
        assert!(
            sink.dropped() > 0,
            "the queue is {QUEUE_DEPTH} deep and {} records were offered",
            QUEUE_DEPTH * 4
        );
    }

    #[test]
    fn a_payload_without_a_snapshot_carries_only_events() {
        let payload = encode("a", None, vec![a_record(1)], 3);
        assert_eq!(payload["replica_id"], "a");
        assert_eq!(payload["dropped_reports"], 3);
        assert_eq!(payload["events"].as_array().unwrap().len(), 1);
        assert!(payload.get("state").is_none());
    }

    #[test]
    fn a_payload_with_a_snapshot_is_self_describing() {
        let snapshot = SnapshotRecord {
            uptime_ms: 1234,
            metrics: json!({ "stable_prefix": 7 }),
            state: json!({ "json": "Unset" }),
            peers: vec![("b".into(), "Connected".into())],
        };
        let payload = encode("a", Some(&snapshot), Vec::<EventRecord<String>>::new(), 0);
        assert_eq!(payload["uptime_ms"], 1234);
        assert_eq!(payload["metrics"]["stable_prefix"], 7);
        assert_eq!(
            payload["state_digest"],
            json!(crate::workload::state_digest(&json!({ "json": "Unset" })))
        );
        assert_eq!(payload["peers"][0]["id"], "b");
    }
}
