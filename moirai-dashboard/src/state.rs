//! What the dashboard knows, and how it is folded from incoming reports.
//!
//! Everything here is derived from what replicas push. The dashboard holds no
//! roster and dials nothing: a replica exists because it reported, and goes
//! stale when it stops. See `moirai-network/src/dashboard.rs` for why the data
//! travels that way.

use std::collections::{BTreeMap, HashMap, VecDeque};

use serde_json::{json, Value};

/// A replica is stale this long after its last report.
///
/// Comfortably above the default snapshot interval (1 s) so that a slow report
/// does not flicker a live replica into staleness, and well below anything a
/// human would call "gone".
pub const STALE_AFTER_MS: u64 = 3_000;

/// Events kept for the feed.
pub const FEED_CAPACITY: usize = 400;

/// Propagation samples kept for the distribution.
pub const SAMPLE_CAPACITY: usize = 2_000;

/// The last thing a replica told us about itself.
#[derive(Debug, Clone)]
pub struct ReplicaView {
    pub id: String,
    /// Dashboard-local receipt time, so staleness is measured on one clock.
    pub last_seen_ms: u64,
    pub uptime_ms: u64,
    pub metrics: Value,
    pub state: Value,
    pub state_digest: String,
    pub peers: Value,
    pub dropped_reports: u64,
    /// Deliveries seen from this replica since it first reported.
    pub deliveries: u64,
}

/// One delivery, as the feed shows it.
#[derive(Debug, Clone)]
pub struct FeedEntry {
    /// The replica that *reported* this delivery — not necessarily the origin.
    pub observer: String,
    pub event: Value,
    /// Milliseconds between the origin's own delivery and this one, when both
    /// have been seen. `None` on the origin's own record, and while the
    /// origin's record has not arrived.
    pub propagation_ms: Option<i64>,
    pub received_ms: u64,
}

#[derive(Debug, Default)]
pub struct Dashboard {
    pub replicas: BTreeMap<String, ReplicaView>,
    pub feed: VecDeque<FeedEntry>,
    /// `event id -> the origin replica's own delivery timestamp`, the baseline
    /// every propagation time is measured against.
    origin_ts: HashMap<String, u64>,
    /// `event id -> the operation itself`, reported once by the replica that
    /// originated it. Remote deliveries carry no payload on purpose — see
    /// `OpsByEvent` in `moirai-network/src/generic.rs` — so this is where the
    /// feed gets an operation to display for them.
    origin_op: HashMap<String, Value>,
    origin_order: VecDeque<String>,
    /// Recent propagation samples, newest last.
    pub samples: VecDeque<i64>,
    /// Reports accepted since start, so an operator can tell "nothing is
    /// happening" from "nothing is arriving".
    pub reports: u64,
}

impl Dashboard {
    /// Fold one replica report in, returning the feed entries it produced so
    /// they can be streamed to browsers.
    pub fn ingest(&mut self, report: &Value, now_ms: u64) -> Vec<FeedEntry> {
        let Some(id) = report.get("replica_id").and_then(Value::as_str) else {
            return Vec::new();
        };
        self.reports += 1;

        // A replica that comes back with a smaller uptime is a *new* replica
        // wearing the same name, and it restarts its sequence numbers at 1. Its
        // old baselines would then be matched against fresh events and produce
        // propagation times in the minutes. Observed, not hypothetical: a rig
        // restarted under a dashboard that kept running reported p95 96 s.
        let uptime = report.get("uptime_ms").and_then(Value::as_u64).unwrap_or(0);
        if self.replicas.get(id).is_some_and(|r| uptime < r.uptime_ms) {
            self.forget_origins_of(id);
        }

        let events = report
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let mut produced = Vec::with_capacity(events.len());
        for event in &events {
            let key = event
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let ts = event.get("ts_ms").and_then(Value::as_u64).unwrap_or(now_ms);
            let is_local = event.get("local").and_then(Value::as_bool) == Some(true);

            if is_local {
                self.remember_origin(key.clone(), ts, event.get("op"));
            }
            // Local records carry no propagation by definition; remote ones do
            // only once the origin's record has been seen, which is not
            // guaranteed to be first — reports from different replicas arrive
            // in no particular order.
            let propagation_ms = if is_local {
                None
            } else {
                self.origin_ts
                    .get(&key)
                    .map(|origin| ts as i64 - *origin as i64)
            };
            if let Some(sample) = propagation_ms {
                if self.samples.len() >= SAMPLE_CAPACITY {
                    self.samples.pop_front();
                }
                self.samples.push_back(sample);
            }

            // Fill in the payload the origin reported, if this delivery is a
            // remote one and the origin's report has already been seen.
            let mut event = event.clone();
            if event.get("op").is_none_or(Value::is_null) {
                if let Some(op) = self.origin_op.get(&key) {
                    event["op"] = op.clone();
                }
            }
            let entry = FeedEntry {
                observer: id.to_string(),
                event,
                propagation_ms,
                received_ms: now_ms,
            };
            if self.feed.len() >= FEED_CAPACITY {
                self.feed.pop_front();
            }
            self.feed.push_back(entry.clone());
            produced.push(entry);
        }

        // A report may carry counters without a rendered model, and most do:
        // rendering is the expensive half and runs on a slower clock than the
        // rest. An absent field therefore means "unchanged since you last saw
        // it", never "empty" — clearing the model on every second report would
        // make the state panel blink and the divergence marker fire on nothing.
        let previous = self.replicas.get(id);
        let carried = |field: &str, prior: Option<&Value>| -> Value {
            report
                .get(field)
                .cloned()
                .or_else(|| prior.cloned())
                .unwrap_or(Value::Null)
        };
        let deliveries = previous
            .map_or(0, |r| r.deliveries)
            .saturating_add(events.len() as u64);
        let state = carried("state", previous.map(|r| &r.state));
        let state_digest = report
            .get("state_digest")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| previous.map(|r| r.state_digest.clone()))
            .unwrap_or_default();
        let metrics = carried("metrics", previous.map(|r| &r.metrics));
        let peers = report
            .get("peers")
            .cloned()
            .or_else(|| previous.map(|r| r.peers.clone()))
            .unwrap_or_else(|| json!([]));

        self.replicas.insert(
            id.to_string(),
            ReplicaView {
                id: id.to_string(),
                last_seen_ms: now_ms,
                uptime_ms: report.get("uptime_ms").and_then(Value::as_u64).unwrap_or(0),
                metrics,
                state,
                state_digest,
                peers,
                dropped_reports: report
                    .get("dropped_reports")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                deliveries,
            },
        );

        produced
    }

    /// Drop every propagation baseline belonging to `id`, because the replica
    /// answering to that name is a new process with sequence numbers starting
    /// again at 1.
    fn forget_origins_of(&mut self, id: &str) {
        let prefix = format!("{id}:");
        self.origin_ts.retain(|key, _| !key.starts_with(&prefix));
        self.origin_op.retain(|key, _| !key.starts_with(&prefix));
        self.origin_order.retain(|key| !key.starts_with(&prefix));
    }

    fn remember_origin(&mut self, key: String, ts: u64, op: Option<&Value>) {
        if self.origin_ts.contains_key(&key) {
            return;
        }
        if self.origin_order.len() >= SAMPLE_CAPACITY * 4 {
            if let Some(old) = self.origin_order.pop_front() {
                self.origin_ts.remove(&old);
                self.origin_op.remove(&old);
            }
        }
        self.origin_order.push_back(key.clone());
        self.origin_ts.insert(key.clone(), ts);
        if let Some(op) = op.filter(|v| !v.is_null()) {
            self.origin_op.insert(key, op.clone());
        }
    }

    /// The digest the largest number of *live* replicas report, and how many
    /// report it. A replica reporting anything else is diverged.
    pub fn majority_digest(&self, now_ms: u64) -> (Option<String>, usize, usize) {
        let live: Vec<&ReplicaView> = self
            .replicas
            .values()
            .filter(|r| !is_stale(r, now_ms))
            .collect();
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for replica in &live {
            *counts.entry(replica.state_digest.as_str()).or_default() += 1;
        }
        let best = counts.iter().max_by_key(|(_, n)| **n);
        match best {
            Some((digest, n)) => (Some((*digest).to_string()), *n, live.len()),
            None => (None, 0, 0),
        }
    }

    /// Percentiles over the recent propagation samples.
    pub fn propagation(&self) -> Value {
        if self.samples.is_empty() {
            return json!({ "count": 0 });
        }
        let mut sorted: Vec<i64> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
        json!({
            "count": sorted.len(),
            "min": sorted[0],
            "p50": at(0.50),
            "p95": at(0.95),
            "max": sorted[sorted.len() - 1],
        })
    }

    /// Everything the page needs on first load, in the shape the stream also
    /// sends — one renderer, not two.
    pub fn snapshot(&self, now_ms: u64) -> Value {
        let (majority, agreeing, live) = self.majority_digest(now_ms);
        let nodes: Vec<Value> = self
            .replicas
            .values()
            .map(|r| {
                let stale = is_stale(r, now_ms);
                json!({
                    "id": r.id,
                    "stale": stale,
                    "last_seen_ago_ms": now_ms.saturating_sub(r.last_seen_ms),
                    "uptime_ms": r.uptime_ms,
                    "metrics": r.metrics,
                    "state": r.state,
                    "state_digest": r.state_digest,
                    "diverged": !stale
                        && majority.as_deref().is_some_and(|m| m != r.state_digest),
                    "peers": r.peers,
                    "dropped_reports": r.dropped_reports,
                    "deliveries": r.deliveries,
                })
            })
            .collect();
        json!({
            "now_ms": now_ms,
            "nodes": nodes,
            "agreeing": agreeing,
            "live": live,
            "total": self.replicas.len(),
            "majority_digest": majority,
            "propagation": self.propagation(),
            "reports": self.reports,
            "feed": self
                .feed
                .iter()
                .rev()
                .take(120)
                .map(feed_json)
                .collect::<Vec<_>>(),
        })
    }
}

pub fn is_stale(replica: &ReplicaView, now_ms: u64) -> bool {
    now_ms.saturating_sub(replica.last_seen_ms) > STALE_AFTER_MS
}

pub fn feed_json(entry: &FeedEntry) -> Value {
    json!({
        "observer": entry.observer,
        "event": entry.event,
        "propagation_ms": entry.propagation_ms,
        "received_ms": entry.received_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(id: &str, digest: &str, events: Value) -> Value {
        json!({
            "replica_id": id,
            "ts_ms": 1_000,
            "uptime_ms": 5_000,
            "metrics": { "stable_prefix": 3 },
            "state": { "json": "Unset" },
            "state_digest": digest,
            "peers": [],
            "dropped_reports": 0,
            "events": events,
        })
    }

    fn event(id: &str, origin: &str, local: bool, ts: u64) -> Value {
        json!({
            "id": id, "origin": origin, "seq": 1, "lamport": 1,
            "ts_ms": ts, "local": local, "applied": true,
            "redundant_on_arrival": false, "reset": false,
            "superseded": [], "op": null,
        })
    }

    #[test]
    fn a_replica_exists_because_it_reported() {
        let mut d = Dashboard::default();
        assert!(d.replicas.is_empty());
        d.ingest(&report("a", "deadbeef", json!([])), 100);
        assert_eq!(d.replicas.len(), 1);
        assert!(d.replicas.contains_key("a"));
    }

    #[test]
    fn propagation_is_measured_from_the_origins_own_delivery() {
        let mut d = Dashboard::default();
        d.ingest(
            &report("a", "x", json!([event("a:1", "a", true, 1_000)])),
            10,
        );
        d.ingest(
            &report("b", "x", json!([event("a:1", "a", false, 1_042)])),
            20,
        );
        assert_eq!(d.samples.len(), 1);
        assert_eq!(d.samples[0], 42);
        let p = d.propagation();
        assert_eq!(p["count"], 1);
        assert_eq!(p["p50"], 42);
    }

    #[test]
    fn a_remote_delivery_takes_its_payload_from_the_origins_report() {
        // Remote deliveries carry no operation: the replica that originated it
        // reports the payload once, rather than every replica cloning it to
        // send the same bytes N times.
        let mut d = Dashboard::default();
        let mut origin = event("a:1", "a", true, 1_000);
        origin["op"] = json!({ "JsonKind": { "Object": { "Remove": "beta" } } });
        d.ingest(&report("a", "x", json!([origin])), 10);
        d.ingest(
            &report("b", "x", json!([event("a:1", "a", false, 1_010)])),
            20,
        );
        let latest = d.feed.back().unwrap();
        assert_eq!(latest.observer, "b");
        assert_eq!(latest.event["op"]["JsonKind"]["Object"]["Remove"], "beta");
    }

    #[test]
    fn a_remote_delivery_seen_before_its_origin_has_no_propagation() {
        // Reports from different replicas arrive in no particular order, and
        // guessing a baseline would invent data.
        let mut d = Dashboard::default();
        d.ingest(
            &report("b", "x", json!([event("a:1", "a", false, 1_042)])),
            20,
        );
        assert!(d.samples.is_empty());
        assert_eq!(d.feed.len(), 1);
        assert!(d.feed[0].propagation_ms.is_none());
    }

    #[test]
    fn a_report_without_a_render_keeps_the_model_it_had() {
        // Most reports carry counters and no model; an absent field means
        // "unchanged", not "empty".
        let mut d = Dashboard::default();
        d.ingest(&report("a", "digest-one", json!([])), 0);
        let mut counters_only = json!({
            "replica_id": "a",
            "ts_ms": 2_000,
            "uptime_ms": 6_000,
            "metrics": { "stable_prefix": 9 },
            "dropped_reports": 0,
            "events": [],
        });
        counters_only["events"] = json!([]);
        d.ingest(&counters_only, 100);

        let view = &d.replicas["a"];
        assert_eq!(view.state_digest, "digest-one", "the model was cleared");
        assert_eq!(view.state, json!({ "json": "Unset" }));
        assert_eq!(
            view.metrics["stable_prefix"], 9,
            "counters must still update"
        );
    }

    #[test]
    fn divergence_is_the_minority_digest() {
        let mut d = Dashboard::default();
        d.ingest(&report("a", "same", json!([])), 100);
        d.ingest(&report("b", "same", json!([])), 100);
        d.ingest(&report("c", "other", json!([])), 100);

        let snap = d.snapshot(100);
        assert_eq!(snap["agreeing"], 2);
        assert_eq!(snap["live"], 3);
        let nodes = snap["nodes"].as_array().unwrap();
        let diverged: Vec<&str> = nodes
            .iter()
            .filter(|n| n["diverged"] == json!(true))
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        assert_eq!(diverged, vec!["c"]);
    }

    #[test]
    fn a_replica_that_stops_reporting_goes_stale_but_stays_visible() {
        let mut d = Dashboard::default();
        d.ingest(&report("a", "same", json!([])), 0);
        d.ingest(&report("b", "same", json!([])), 0);
        // `b` keeps reporting; `a` does not.
        d.ingest(&report("b", "same", json!([])), STALE_AFTER_MS + 500);

        let snap = d.snapshot(STALE_AFTER_MS + 500);
        assert_eq!(snap["total"], 2, "a stale replica must not be removed");
        assert_eq!(snap["live"], 1);
        let nodes = snap["nodes"].as_array().unwrap();
        let a = nodes.iter().find(|n| n["id"] == "a").unwrap();
        assert_eq!(a["stale"], json!(true));
        // Stale is not diverged: nothing is known about it, which is different
        // from knowing it disagrees.
        assert_eq!(a["diverged"], json!(false));
    }

    #[test]
    fn a_restarted_replica_does_not_produce_a_fake_propagation_time() {
        // Measured, not hypothetical: a rig restarted under a dashboard that
        // kept running reported a p95 of 96 s, because the new process reuses
        // `a:1` and the old baseline was minutes old.
        let mut d = Dashboard::default();
        let mut old = report("a", "x", json!([event("a:1", "a", true, 1_000)]));
        old["uptime_ms"] = json!(100_000);
        d.ingest(&old, 10);

        let mut fresh = report("a", "x", json!([event("a:1", "a", true, 900_000)]));
        fresh["uptime_ms"] = json!(500);
        d.ingest(&fresh, 900_000);

        d.ingest(
            &report("b", "x", json!([event("a:1", "a", false, 900_012)])),
            900_020,
        );
        assert_eq!(d.samples.len(), 1);
        assert_eq!(d.samples[0], 12, "the baseline must be the new process's");
    }

    #[test]
    fn the_feed_is_bounded() {
        let mut d = Dashboard::default();
        for i in 0..(FEED_CAPACITY + 50) {
            d.ingest(
                &report(
                    "a",
                    "x",
                    json!([event(&format!("a:{i}"), "a", true, i as u64)]),
                ),
                i as u64,
            );
        }
        assert_eq!(d.feed.len(), FEED_CAPACITY);
    }
}
