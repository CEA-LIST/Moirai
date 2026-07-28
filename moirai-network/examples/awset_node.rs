//! Simple network node using Add-Wins Set CRDT (no Arachne codegen needed).
//!
//! This example demonstrates using `GenericNode` with a manually-written CRDT
//! from the `moirai-crdt` crate. Unlike Arachne-generated nodes, this uses the
//! built-in `AWSet` (Add-Wins Set) wrapped in `VecLog`.
//!
//! Note: To enable the `/api/state` endpoint, we use a newtype wrapper pattern
//! to implement `QueryableLog` (Rust's orphan rules prevent implementing external
//! traits on external types). Arachne-generated CRDTs get `QueryableLog` automatically.
//!
//! # Usage
//!
//! ```bash
//! # Single node
//! REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 \
//!     cargo run --example awset_node
//!
//! # Two-node cluster
//! REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 PEERS=b:127.0.0.1:9002 \
//!     cargo run --example awset_node &
//! REPLICA_ID=b LISTEN_PORT=9002 HTTP_PORT=8082 PEERS=a:127.0.0.1:9001 \
//!     cargo run --example awset_node &
//! ```
//!
//! # HTTP API
//!
//! - `POST /api/op`        — submit an operation as JSON
//! - `GET  /api/state`     — query the current set contents
//! - `GET  /api/operations` — query all delivered operations
//! - `GET  /api/health`    — health check
//! - `GET  /api/peers`     — list connected peers
//! - `POST /api/pause/<id>`  — pause connection to a peer
//! - `POST /api/resume/<id>` — resume connection and sync
//!
//! # Example Operations
//!
//! ```bash
//! # Add an element
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '{"Add":"apple"}'
//!
//! # Add another element
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '{"Add":"banana"}'
//!
//! # Remove an element
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '{"Remove":"apple"}'
//!
//! # Clear the set
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '"Clear"'
//!
//! # Query the set contents (computed state)
//! curl http://localhost:8081/api/state
//!
//! # Or query operations log
//! curl http://localhost:8081/api/operations
//! ```

use std::env;
use std::thread;
use std::time::Duration;

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::generic::GenericNode;
use moirai_network::tcp_transport::TcpTransport;
use moirai_network::HashMap;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::clock::version_vector::Version;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::event::Event;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;
use moirai_protocol::state::po_log::VecLog;

// Newtype wrapper to implement QueryableLog (Rust orphan rules require a local type)
#[derive(Debug, Clone, Default)]
struct AWSetLog(VecLog<AWSet<String>>);

// Delegate all IsLog methods to the inner VecLog
impl IsLog for AWSetLog {
    type Value = <VecLog<AWSet<String>> as IsLog>::Value;
    type Op = <VecLog<AWSet<String>> as IsLog>::Op;

    fn is_enabled(&self, op: &Self::Op) -> bool {
        self.0.is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        self.0.effect(event)
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.stabilize(version)
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative)
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

// Delegate query execution to inner VecLog
impl<Q> moirai_protocol::crdt::eval::EvalNested<Q> for AWSetLog
where
    VecLog<AWSet<String>>: moirai_protocol::crdt::eval::EvalNested<Q>,
    Q: moirai_protocol::crdt::query::QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

// Implement QueryableLog to enable GET /api/state endpoint
impl moirai_network::query::QueryableLog for AWSetLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<Self::Op>>) -> serde_json::Value {
        let value = replica.query(Read::new());
        serde_json::to_value(&value).unwrap_or_else(
            |e| serde_json::json!({ "error": format!("Failed to serialize state: {}", e) }),
        )
    }
}

fn main() {
    let replica_id = env::var("REPLICA_ID").unwrap_or_else(|_| "replica-a".to_string());
    let listen_port: u16 = env::var("LISTEN_PORT")
        .unwrap_or_else(|_| "9001".to_string())
        .parse()
        .expect("Invalid LISTEN_PORT");
    let http_port: Option<u16> = env::var("HTTP_PORT").ok().and_then(|p| p.parse().ok());
    let peers_str = env::var("PEERS").unwrap_or_default();

    // Parse PEERS=id:host:port,...
    let mut peer_addresses: HashMap<String, String> = HashMap::default();
    let mut all_members: Vec<String> = vec![replica_id.clone()];
    for spec in peers_str.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() >= 3 {
            let peer_id = parts[0].to_string();
            let addr = format!("{}:{}", parts[1], parts[2]);
            all_members.push(peer_id.clone());
            peer_addresses.insert(peer_id, addr);
        }
    }

    let member_refs: Vec<&str> = all_members.iter().map(|s| s.as_str()).collect();

    // Create the node with AWSet CRDT
    let mut node = GenericNode::<AWSetLog, TcpTransport<AWSet<String>>>::new(
        replica_id.clone(),
        &member_refs,
        listen_port,
        peer_addresses,
    );

    // Enable state querying - requires QueryableLog trait implementation
    node.enable_state_query();

    // Start HTTP API if port is specified
    if let Some(port) = http_port {
        node.start_http(port);
        eprintln!(
            "[{}] HTTP API listening on http://0.0.0.0:{}",
            replica_id, port
        );
    }

    // Give peers time to start, then connect
    thread::sleep(Duration::from_secs(2));
    node.connect();

    eprintln!(
        "[{}] AWSet node running. Submit ops to http://localhost:{}/api/op",
        replica_id,
        http_port.unwrap_or(0)
    );
    eprintln!(
        "[{}] Query state at http://localhost:{}/api/state",
        replica_id,
        http_port.unwrap_or(0)
    );
    eprintln!("[{}] Example: curl -X POST http://localhost:{}/api/op -H 'Content-Type: application/json' -d '{{\"Add\":\"apple\"}}'", 
        replica_id, http_port.unwrap_or(0));

    // Run the event loop
    node.run();
}
