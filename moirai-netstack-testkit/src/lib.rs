//! A toy replica for `moirai-netstack` and the port-level contract suite
//! every adapter runs (NW.31c). The toy is a grow-only set with no Moirai.

use std::collections::BTreeSet;

use moirai_netstack::{Opaque, Progress, ReplicaFactory, ReplicaPort};
use serde_json::{Value, json};

pub mod contract;

/// Opens toy replicas.
#[derive(Default)]
pub struct ToyFactory;

impl ReplicaFactory for ToyFactory {
    type Replica = ToyReplica;

    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> ToyReplica {
        ToyReplica {
            log: log.to_string(),
            self_id: self_id.to_string(),
            members: members.to_vec(),
            elements: BTreeSet::new(),
            delivered: Vec::new(),
            origins: BTreeSet::new(),
        }
    }
}

/// A grow-only set of JSON string elements, identified by log and replica id.
pub struct ToyReplica {
    log: String,
    self_id: String,
    members: Vec<String>,
    elements: BTreeSet<String>,
    /// `(origin, element)` of every distinct op delivered, in delivery order.
    delivered: Vec<(String, String)>,
    /// The replicas whose op reached this one, however it was relayed.
    origins: BTreeSet<String>,
}

impl ToyReplica {
    fn event(&self, origin: &str, element: &str) -> Value {
        json!({"log": self.log, "origin": origin, "op": element})
    }

    fn deliver(&mut self, origin: &str, element: &str) {
        let key = (origin.to_string(), element.to_string());
        if !self.delivered.contains(&key) {
            self.delivered.push(key);
            self.origins.insert(origin.to_string());
        }
        self.elements.insert(element.to_string());
    }

    fn event_fields<'a>(&self, event: &'a Value) -> Result<(&'a str, &'a str), String> {
        let field = |name: &str| event.get(name).and_then(Value::as_str);
        let (log, origin, element) = match (field("log"), field("origin"), field("op")) {
            (Some(log), Some(origin), Some(element)) => (log, origin, element),
            _ => return Err(format!("malformed event: {event}")),
        };
        if log != self.log {
            return Err(format!("event of log {log} refused on log {}", self.log));
        }
        Ok((origin, element))
    }

    fn apply_event(&mut self, event: &Value) -> Result<(), String> {
        let (origin, element) = self.event_fields(event)?;
        self.deliver(origin, element);
        Ok(())
    }
}

impl ReplicaPort for ToyReplica {
    fn replica_id(&self) -> &str {
        &self.self_id
    }

    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        let element = op
            .as_str()
            .ok_or_else(|| format!("op is not a string element: {op}"))?;
        let origin = self.self_id.clone();
        self.deliver(&origin, element);
        Ok(self.event(&origin, element))
    }

    fn apply_remote(&mut self, _from: &str, ops: Opaque) -> Result<(), String> {
        match &ops {
            Value::Array(events) => {
                // All or nothing: a batch whose second event is refused leaves
                // the replica exactly as it was.
                for event in events {
                    self.event_fields(event)?;
                }
                for event in events {
                    self.apply_event(event)?;
                }
                Ok(())
            }
            Value::Object(_) => self.apply_event(&ops),
            _ => Err(format!("malformed remote input: {ops}")),
        }
    }

    fn frontier(&self) -> Opaque {
        let seen: Vec<Value> = self
            .delivered
            .iter()
            .map(|(origin, element)| json!([origin, element]))
            .collect();
        Value::Array(seen)
    }

    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
        let entries = frontier
            .as_array()
            .ok_or_else(|| format!("malformed frontier: {frontier}"))?;
        let mut known = BTreeSet::new();
        for entry in entries {
            match entry.as_array().and_then(|pair| {
                let origin = pair.first()?.as_str()?;
                let element = pair.get(1)?.as_str()?;
                Some((origin.to_string(), element.to_string()))
            }) {
                Some(key) => {
                    known.insert(key);
                }
                None => return Err(format!("malformed frontier: {frontier}")),
            }
        }
        let events: Vec<Value> = self
            .delivered
            .iter()
            .filter(|key| !known.contains(*key))
            .map(|(origin, element)| self.event(origin, element))
            .collect();
        Ok(Value::Array(events))
    }

    fn progress(&self) -> Progress {
        Progress {
            delivered: self.delivered.len() as u64,
            pending: 0,
            retained: 0,
        }
    }

    fn members(&self) -> Vec<String> {
        self.members.clone()
    }

    fn has_history_from(&self, peer: &str) -> bool {
        self.origins.contains(peer)
    }

    fn read_state(&self) -> Value {
        Value::Array(self.elements.iter().map(|element| json!(element)).collect())
    }
}
