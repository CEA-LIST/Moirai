//! NW.31h, end to end (vault `spec/items/NW.31h.md`, claim point 3): the
//! built `moirai-netstack-router` binary, started with only `LISTEN_PORT=0`
//! and `PORTS_FILE` in its environment, reports its pid and the port the
//! kernel chose, and its startup line names that port, that multicast
//! scouting is off and the layout (Zenoh's auto preset, no ROUTER_REGIONS).
//! Three `moirai-netstack-transport` Zenoh transports, a, b and c, each
//! configured by the transport's own `TransportConfig` with only
//! `ZENOH_ROUTER=tcp/127.0.0.1:<port>` (peer mode, listening on port 0, no
//! peer address, multicast off, gossip on, one hop), find each other and
//! exchange frames: every one sends one frame to each other and broadcasts
//! one, and each receives exactly the four meant for it. The router process
//! is then killed by its pid (SIGKILL) and reaped; 12 s later (past Zenoh's
//! 10 s lease, as NW.16b's R2) each transport still lists the two others
//! connected, and a second round of the same exchange arrives exactly.
//!
//! This file is the `src/main.rs` of a consuming workspace the gate
//! `tests/nw31h/gate.sh` writes in scratch (the router crate may not depend
//! on a moirai crate, so the test cannot live in it); `contract.rs` beside it
//! is `moirai-netstack-transport/tests/nw15_common/contract.rs`, copied as is.
//! Exit 0 and the last line `NW31H E2E OK` is a pass; every failure panics
//! with its phase first: `NW31H before:` (with the router up) or `NW31H
//! after:` (after the kill); `set-up:` is a failure of the instrument.

mod contract;

use std::collections::BTreeMap;
use std::fs;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use contract::{DELIVERY, SETTLE, Seen, pump_until, raw, settle};
use moirai_netstack_transport::HashMap;
use moirai_netstack_transport::transport::{CrdtTransport, PeerId};
use moirai_netstack_transport::transport_config::TransportConfig;
use moirai_netstack_transport::zenoh_transport::ZenohTransport;

const IDS: [&str; 3] = ["a", "b", "c"];
/// How long the router may take to report its port and print its line.
const START: Duration = Duration::from_secs(30);
/// How long after the kill the transports are read again: past Zenoh's 10 s lease.
const AFTER_KILL: Duration = Duration::from_secs(12);

/// The router process; killed and reaped on every exit path.
struct Router(Child);

impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set-up: {name} is unset"))
}

/// The router's stderr so far.
fn log_text(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// Start the router; answer it with its reported port once `PORTS_FILE` is whole.
fn start_router(bin: &str, dir: &PathBuf) -> (Router, u16, PathBuf) {
    let ports = dir.join("router.ports.json");
    let log = dir.join("router.stderr.log");
    let _ = fs::remove_file(&ports);
    let child = Command::new(bin)
        .env_clear()
        .env("LISTEN_PORT", "0")
        .env("PORTS_FILE", &ports)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log).expect("set-up: create the router log"))
        .spawn()
        .unwrap_or_else(|e| panic!("set-up: cannot start the router {bin}: {e}"));
    let mut router = Router(child);
    let deadline = Instant::now() + START;
    let report = loop {
        if let Ok(text) = fs::read_to_string(&ports) {
            break text;
        }
        if let Ok(Some(status)) = router.0.try_wait() {
            panic!(
                "NW31H before: the router exited ({status}) without writing PORTS_FILE:\n{}",
                log_text(&log)
            );
        }
        assert!(
            Instant::now() < deadline,
            "NW31H before: the router wrote no PORTS_FILE within {START:?}:\n{}",
            log_text(&log)
        );
        thread::sleep(Duration::from_millis(50));
    };
    let value: serde_json::Value = serde_json::from_str(&report)
        .unwrap_or_else(|e| panic!("NW31H before: PORTS_FILE is not JSON ({e}): {report}"));
    assert_eq!(
        value["pid"].as_u64(),
        Some(u64::from(router.0.id())),
        "NW31H before: PORTS_FILE does not name the router's pid {}: {report}",
        router.0.id()
    );
    assert!(
        value.get("regions").is_none(),
        "NW31H before: PORTS_FILE names regions though ROUTER_REGIONS is unset: {report}"
    );
    let port = value["listen_port"]
        .as_u64()
        .and_then(|p| u16::try_from(p).ok())
        .filter(|p| *p != 0)
        .unwrap_or_else(|| {
            panic!("NW31H before: PORTS_FILE names no non-zero listen_port: {report}")
        });
    (router, port, log)
}

/// The router's startup line, once printed.
fn startup_line(log: &PathBuf, port: u16) {
    let deadline = Instant::now() + START;
    loop {
        let text = log_text(log);
        if let Some(line) = text.lines().find(|l| l.contains("multicast scouting off")) {
            assert!(
                line.contains(&format!("port {port}")) && line.contains("auto"),
                "NW31H before: the startup line does not name port {port} and the auto layout: {line}"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "NW31H before: no startup line naming \"multicast scouting off\" within {START:?}:\n{text}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// A transport told only the router, built by the transport's own configuration.
fn open(me: &str, session: &str, router: &str) -> ZenohTransport {
    let vars: BTreeMap<&str, &str> = [
        ("MOIRAI_TRANSPORT", "zenoh"),
        ("ZENOH_ROUTER", router),
        ("SESSION_ID", session),
    ]
    .into_iter()
    .collect();
    let config = TransportConfig::from_lookup(|name| vars.get(name).map(|v| v.to_string()))
        .unwrap_or_else(|e| panic!("set-up: TransportConfig refused {vars:?}: {e}"))
        .zenoh_config(0, &HashMap::default())
        .unwrap_or_else(|e| panic!("set-up: zenoh_config refused: {e}"));
    let read = |key: &str| -> serde_json::Value {
        let text = config
            .get_json(key)
            .unwrap_or_else(|e| panic!("set-up: get_json({key}): {e}"));
        serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("set-up: get_json({key}) = {text}: {e}"))
    };
    assert_eq!(
        read("mode"),
        serde_json::json!("peer"),
        "set-up: {me} is not in peer mode"
    );
    assert_eq!(
        read("connect/endpoints"),
        serde_json::json!([router]),
        "set-up: {me} is told more than the router"
    );
    assert_eq!(
        read("scouting/multicast/enabled"),
        serde_json::json!(false),
        "set-up: {me} scouts on multicast"
    );
    let known: Vec<PeerId> = IDS
        .iter()
        .filter(|other| **other != me)
        .map(|other| other.to_string())
        .collect();
    ZenohTransport::open(me.to_string(), session, known, config)
        .unwrap_or_else(|e| panic!("set-up: ZenohTransport::open({me}): {e}"))
}

/// Every transport sends one frame to each other and broadcasts one; each
/// must receive exactly the four meant for it.
fn exchange(phase: &str, ts: &mut [ZenohTransport]) {
    let mut seen: Vec<Seen> = IDS.iter().map(|_| Seen::default()).collect();
    for (i, me) in IDS.iter().enumerate() {
        for other in IDS.iter().filter(|o| *o != me) {
            ts[i]
                .send(&other.to_string(), raw(&format!("{phase} {me} to {other}")))
                .unwrap_or_else(|e| panic!("NW31H {phase}: {me}.send({other}) failed: {e}"));
        }
        ts[i]
            .broadcast(raw(&format!("{phase} {me} to all")))
            .unwrap_or_else(|e| panic!("NW31H {phase}: {me}.broadcast failed: {e}"));
    }
    let want = |j: usize| -> Vec<(PeerId, String)> {
        let mut want: Vec<(PeerId, String)> = IDS
            .iter()
            .filter(|from| **from != IDS[j])
            .flat_map(|from| {
                [
                    (from.to_string(), format!("{phase} {from} to {}", IDS[j])),
                    (from.to_string(), format!("{phase} {from} to all")),
                ]
            })
            .collect();
        want.sort();
        want
    };
    let arrived = pump_until(ts, &mut seen, DELIVERY, |seen, _| {
        (0..IDS.len()).all(|j| want(j).iter().all(|(f, p)| seen[j].has(f, p)))
    });
    settle(ts, &mut seen, SETTLE);
    for (j, s) in seen.iter().enumerate() {
        let mut got = s.raw.clone();
        got.sort();
        assert!(
            arrived && got == want(j),
            "NW31H {phase}: {} received {got:?} within {DELIVERY:?}, want exactly {:?}",
            IDS[j],
            want(j)
        );
    }
}

fn main() {
    let bin = env("NW31H_ROUTER_BIN");
    let dir = PathBuf::from(env("NW31H_DIR"));
    fs::create_dir_all(&dir).expect("set-up: create NW31H_DIR");
    let (mut router, port, log) = start_router(&bin, &dir);
    startup_line(&log, port);
    println!("NW31H router pid {} on port {port}", router.0.id());

    let endpoint = format!("tcp/127.0.0.1:{port}");
    let session = format!("nw31h{}", std::process::id());
    let mut ts: Vec<ZenohTransport> = IDS.iter().map(|me| open(me, &session, &endpoint)).collect();
    contract::await_mesh(&mut ts, &IDS);
    exchange("before", &mut ts);
    println!("NW31H before: 12 of 12 frames arrived exactly");

    // Kill the router by its pid and reap it.
    let pid = router.0.id();
    router.0.kill().expect("set-up: kill the router");
    let status = router.0.wait().expect("set-up: reap the router");
    println!("NW31H router pid {pid} killed ({status})");
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "set-up: something still accepts on the router's port {port} after the kill"
    );
    let mut idle: Vec<Seen> = IDS.iter().map(|_| Seen::default()).collect();
    settle(&mut ts, &mut idle, AFTER_KILL);
    for t in &ts {
        for other in IDS.iter().filter(|o| **o != t.local_id().as_str()) {
            assert!(
                t.is_connected(&other.to_string()),
                "NW31H after: {AFTER_KILL:?} after the router was killed, {} lists {other} \
                 not connected: {:?}",
                t.local_id(),
                t.peers()
            );
        }
    }
    exchange("after", &mut ts);
    println!("NW31H after: 12 of 12 frames arrived exactly");
    println!("NW31H E2E OK");
}
