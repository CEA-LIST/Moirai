//! NW.27 oracle with real `model_node` processes: HTTP admission (G2), a node that keeps
//! serving after refusing frames past the limit (G1, node level), convergence of deep
//! operations under churn over TCP and Zenoh (G3), state transfer of a deep model (G4) and
//! the same scenarios with an ordinary operation (G5).
//!
//! Spec: `cea-cdrt-knowledge/spec/items/NW.27.md`. Operations are composed as text from
//! shallow templates and every expected state is written out from the write schedule, so
//! the oracle shares no decoder with the nodes. Every file goes under `nw27::WORK`.
#![cfg(feature = "network")]
mod nw16a;
mod nw16b;
mod nw27;

use moirai_interp::{InstanceOp, ModelOp, testing::JSON_DESCRIPTOR};
use nw16a::{
    Cluster, Transport,
    si01::{Json, inc_float},
};
use nw27::{B, L, WORK, deep_descriptor_text, deep_op, depth, names_depth_and, shallow_deep_op};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

static SEQ: AtomicUsize = AtomicUsize::new(0);
const CONVERGE: Duration = Duration::from_secs(30);

/// A fresh directory under `WORK` holding `descriptor` as the nodes' one metamodel.
fn scratch(tag: &str, descriptor: &str) -> PathBuf {
    assert!(
        std::env::temp_dir().starts_with(WORK),
        "set-up: TMPDIR must be under {WORK}"
    );
    let dir = Path::new(WORK).join(format!(
        "nw27-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(dir.join("metamodels")).unwrap();
    fs::write(dir.join("metamodels/model.json"), descriptor).unwrap();
    dir
}

fn shim() -> PathBuf {
    let p = PathBuf::from(std::env::var("NW27_LOOPBACK_SHIM").expect("set-up: NW27_LOOPBACK_SHIM"));
    assert!(p.is_file(), "set-up: {} is not a file", p.display());
    p
}

/// The in-tree router on a kernel-chosen loopback port. `nw16b::Router::start` drops
/// `LD_PRELOAD`, and the router binds a wildcard address, so this keeps the loopback
/// adapter the machine rules require.
struct Router {
    child: Child,
    port: u16,
}

impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn router(dir: &Path) -> Router {
    let report = dir.join("router.ports.json");
    let out = fs::File::create(dir.join("router.log")).unwrap();
    let child = Command::new(nw16b::router_binary().unwrap())
        .env("LISTEN_PORT", "0")
        .env("PORTS_FILE", &report)
        .env_remove("ROUTER_REGIONS")
        .env_remove("ZENOH_CONFIG")
        .env_remove("ZENOH_SCOUTING")
        .env("LD_PRELOAD", shim())
        .stdout(out.try_clone().unwrap())
        .stderr(out)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let mut r = Router { child, port: 0 };
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            r.child.try_wait().unwrap().is_none(),
            "set-up: router exited"
        );
        if let Ok(text) = fs::read_to_string(&report)
            && let Ok(v) = serde_json::from_str::<Value>(&text)
        {
            assert_eq!(v["pid"], json!(r.child.id()));
            r.port = v["listen_port"].as_u64().unwrap() as u16;
            break;
        }
        assert!(Instant::now() < end, "set-up: router report missing");
        thread::sleep(Duration::from_millis(20));
    }
    r
}

/// `ids` on `transport`, meshed and healthy; on Zenoh, peers of one in-tree router with
/// multicast off, each witnessed on Zenoh.
fn start(transport: Transport, ids: &[&str], descriptor: &str) -> (Cluster, Option<Router>) {
    let dir = scratch(transport.var(), descriptor);
    let binary = nw16a::node_binary().unwrap();
    let r = (transport == Transport::Zenoh).then(|| router(&dir));
    let nodes = if let Some(r) = &r {
        ids.iter()
            .map(|id| {
                let mut spec = nw16b::Spec::peer(id, r.port);
                spec.preload = Some((shim(), vec![]));
                let mut pending = nw16b::start(&binary, &dir, &spec).unwrap();
                let report = pending.await_report(Duration::from_secs(30)).unwrap();
                pending.into_node(report.http, report.listen.unwrap())
            })
            .collect()
    } else {
        let mut env = nw16a::node_env(transport, &dir);
        env.push(("LD_PRELOAD".into(), shim().display().to_string()));
        let mut pending: Vec<_> = ids
            .iter()
            .map(|id| nw16a::spawn(&binary, &dir, id, transport, &env).unwrap())
            .collect();
        let ports: Vec<_> = pending
            .iter_mut()
            .map(|p| p.await_ports().unwrap())
            .collect();
        for (i, p) in pending.iter().enumerate() {
            let peers: Vec<_> = ids
                .iter()
                .enumerate()
                .filter(|(j, _)| i != *j)
                .map(|(j, id)| (*id, ports[j].listen_port))
                .collect();
            p.tell_peers(&peers).unwrap();
        }
        pending
            .into_iter()
            .zip(&ports)
            .map(|(p, port)| p.into_node(port))
            .collect()
    };
    let c = Cluster::from_nodes(transport, nodes, dir, binary).unwrap();
    for i in 0..c.nodes.len() {
        c.await_health(i).unwrap();
    }
    c.await_mesh(&(0..ids.len()).collect::<Vec<_>>()).unwrap();
    if transport == Transport::Zenoh {
        for n in &c.nodes {
            nw16a::witness(n).unwrap();
        }
    }
    (c, r)
}

/// One more node, a peer of every running one; answers its index.
fn add_peer(c: &mut Cluster, r: &Option<Router>, id: &str) -> usize {
    if let Some(r) = r {
        let mut spec = nw16b::Spec::peer(id, r.port);
        spec.preload = Some((shim(), vec![]));
        let mut p = nw16b::start(&c.binary, &c.scratch, &spec).unwrap();
        let report = p.await_report(Duration::from_secs(30)).unwrap();
        c.nodes
            .push(p.into_node(report.http, report.listen.unwrap()));
        let i = c.nodes.len() - 1;
        c.await_health(i).unwrap();
        nw16a::witness(&c.nodes[i]).unwrap();
        i
    } else {
        let peers = (0..c.nodes.len()).collect::<Vec<_>>();
        c.add(id, &peers).unwrap()
    }
}

fn join(c: &Cluster, index: usize, model: &str) {
    let (code, body) = c
        .post(index, "/api/models", json!({"model_id": model}).to_string())
        .unwrap();
    assert!((200..300).contains(&code), "set-up: join: {code} {body}");
}

/// A model of `descriptor` created on node 0, joined by every other node when `all`.
fn create_model(c: &Cluster, descriptor: &str, all: bool) -> String {
    let (status, body) = c
        .post(
            0,
            "/api/models",
            json!({"metamodel_id": nw16a::digest(descriptor)}).to_string(),
        )
        .unwrap();
    assert_eq!(status, 201, "set-up: create a model: {body}");
    let m = body["model_id"].as_str().unwrap().to_string();
    if all {
        for i in 1..c.nodes.len() {
            join(c, i, &m);
            c.await_table(i, &m).unwrap();
        }
    }
    m
}

fn client() -> &'static reqwest::blocking::Client {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap()
    })
}

/// The complete state body and its version.
fn read(c: &Cluster, i: usize, m: &str) -> (String, String) {
    let reply = client()
        .get(c.nodes[i].url(&format!("/api/model/{m}/state")))
        .send()
        .unwrap_or_else(|e| {
            panic!(
                "GET state from {}: {e}\n{}",
                c.nodes[i].id,
                c.nodes[i].tail()
            )
        });
    assert_eq!(
        reply.status().as_u16(),
        200,
        "GET state from {}",
        c.nodes[i].id
    );
    let version = reply
        .headers()
        .get("ETag")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    (reply.text().unwrap(), version)
}

fn state(c: &Cluster, i: usize, m: &str) -> String {
    read(c, i, m).0
}

fn submit(c: &Cluster, path: &str, text: String) -> (u16, String) {
    let reply = client()
        .post(c.nodes[0].url(path))
        .header("Content-Type", "application/json")
        .body(text)
        .send()
        .unwrap_or_else(|e| panic!("HTTP intake failed: {e}; node log:\n{}", c.nodes[0].tail()));
    (reply.status().as_u16(), reply.text().unwrap())
}

/// A JSON-model operation putting `levels` nested objects under `key`, and the value it
/// leaves there, both written out independently of any node.
fn json_op(key: &str, levels: usize) -> (String, String) {
    let js = Json::new();
    let mut inner = serde_json::to_string(&js.number(inc_float(7.0))).unwrap();
    let mut value = "7.0".to_string();
    let wrap = serde_json::to_string(&js.entry("child", InstanceOp::New)).unwrap();
    for _ in 0..levels {
        inner = wrap.replacen("\"New\"", &inner, 1);
        value = format!("{{\"child\":{value}}}");
    }
    let root = serde_json::to_string(&js.put(key, InstanceOp::New)).unwrap();
    (root.replacen("\"New\"", &inner, 1), value)
}

/// The writes of a scenario: on the deep path, Batmobile's class (183 levels, deeper than
/// 128) and the deepest JSON-model operation the admission bound lets through (764 levels;
/// one more nesting step makes 771); otherwise one ordinary operation.
fn writes(deep: bool) -> Vec<(&'static str, usize)> {
    if deep {
        vec![("deep", 24), ("edge", 107)]
    } else {
        vec![("flat", 0)]
    }
}

/// Submit the writes on node 0 and answer the state they must leave on every replica.
fn write_all(c: &Cluster, m: &str, deep: bool, gate: &str) -> String {
    let mut members = Vec::new();
    for (key, levels) in writes(deep) {
        let (op, value) = json_op(key, levels);
        let d = depth(&op);
        if deep {
            assert!(d > 128 && d <= B, "fixture: {key} depth {d}");
        }
        if key == "edge" {
            assert!(
                B - d < 7,
                "fixture: edge is the deepest admitted JSON operation"
            );
        }
        println!("NW27 {gate} {key} operation depth={d}");
        let reply = submit(c, &format!("/api/model/{m}/op"), op);
        assert_eq!(
            reply.0, 200,
            "{gate} HTTP intake of {key} (depth {d}): {}",
            reply.1
        );
        members.push(format!("\"{key}\":{value}"));
    }
    format!("{{{}}}", members.join(","))
}

fn converge(c: &Cluster, m: &str, expected: &str, gate: &str) {
    let end = Instant::now() + CONVERGE;
    loop {
        let actual: Vec<_> = (0..c.nodes.len()).map(|i| state(c, i, m)).collect();
        if actual.iter().all(|s| s == expected) {
            return;
        }
        if Instant::now() > end {
            let logs = c
                .nodes
                .iter()
                .map(|n| format!("{}: {}", n.id, n.tail()))
                .collect::<Vec<_>>()
                .join("\n");
            panic!(
                "{gate}: replicas did not converge on the expected state within {} s; state \
                 lengths {:?} against {}\n{logs}",
                CONVERGE.as_secs(),
                actual.iter().map(String::len).collect::<Vec<_>>(),
                expected.len()
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// G3 (deep) and G5 (ordinary): alice, bob and carol hold one model; bob is stopped while
/// alice writes and continued afterwards; all three must end with the expected state, and
/// a fresh joiner must reach it by adopting a transferred state.
fn churn(t: Transport, deep: bool) {
    let gate = if deep { "G3" } else { "G5" };
    let (mut c, r) = start(t, &["alice", "bob", "carol"], JSON_DESCRIPTOR);
    let m = create_model(&c, JSON_DESCRIPTOR, true);
    c.nodes[1].signal("STOP").unwrap();
    let expected = write_all(&c, &m, deep, gate);
    assert_eq!(
        state(&c, 0, &m),
        expected,
        "{gate} set-up: alice's own state"
    );
    c.nodes[1].signal("CONT").unwrap();
    converge(&c, &m, &expected, &format!("{gate} pause/resume"));
    let j = add_peer(&mut c, &r, "joiner");
    join(&c, j, &m);
    converge(&c, &m, &expected, &format!("{gate} snapshot join"));
    assert!(
        c.nodes[j].adopted(&m),
        "{gate} the joiner reached the state without adopting a transferred state\n{}",
        c.nodes[j].tail()
    );
}

/// G4: alice writes alone, so the writes are folded into her state before anyone joins;
/// bob and carol then join and must adopt the transferred state.
fn donor(t: Transport) {
    let (mut c, r) = start(t, &["alice"], JSON_DESCRIPTOR);
    let m = create_model(&c, JSON_DESCRIPTOR, false);
    let expected = write_all(&c, &m, true, "G4");
    assert_eq!(state(&c, 0, &m), expected, "G4 set-up: alice's own state");
    for id in ["bob", "carol"] {
        let j = add_peer(&mut c, &r, id);
        join(&c, j, &m);
    }
    converge(&c, &m, &expected, "G4 deep state transfer");
    for n in &c.nodes[1..] {
        assert!(
            n.adopted(&m),
            "G4 {} did not adopt the transferred state\n{}",
            n.id,
            n.tail()
        );
    }
}

#[test]
fn nw27_g3_tcp_deep_churn() {
    churn(Transport::Tcp, true)
}
#[test]
fn nw27_g3_zenoh_deep_churn() {
    churn(Transport::Zenoh, true)
}
#[test]
fn nw27_g4_tcp_deep_donor() {
    donor(Transport::Tcp)
}
#[test]
fn nw27_g4_zenoh_deep_donor() {
    donor(Transport::Zenoh)
}
#[test]
fn nw27_g5_tcp_ordinary_churn() {
    churn(Transport::Tcp, false)
}
#[test]
fn nw27_g5_zenoh_ordinary_churn() {
    churn(Transport::Zenoh, false)
}

/// The four operation routes.
#[derive(Clone, Copy)]
enum Route {
    DefaultOp,
    DefaultOps,
    NamedOp,
    NamedOps,
}

/// G2: on one node hosting a `Deep` model, operations of depth B + 1, L + 8 and 2 x L are
/// refused with an HTTP 4xx naming the depth and the limit, before anything is applied;
/// the node stays healthy; an operation of depth exactly B is then admitted and applied;
/// and an ordinary operation after it is answered 200. A batch puts an ordinary operation
/// before the deep one, so a refused batch must leave state and version unchanged.
fn admission(route: Route) {
    let descriptor = deep_descriptor_text();
    let (c, _) = start(Transport::Tcp, &["alice"], &descriptor);
    let (named, batch) = match route {
        Route::DefaultOp => (false, false),
        Route::DefaultOps => (false, true),
        Route::NamedOp => (true, false),
        Route::NamedOps => (true, true),
    };
    let m = if named {
        create_model(&c, &descriptor, false)
    } else {
        let install = ModelOp::Install {
            model_id: nw16a::LOG_ID.into(),
            metamodel_id: nw16a::digest(&descriptor),
            descriptor: descriptor.clone(),
        };
        let reply = submit(&c, "/api/op", serde_json::to_string(&install).unwrap());
        assert_eq!(reply.0, 200, "set-up: install: {}", reply.1);
        nw16a::LOG_ID.to_string()
    };
    let path = match (named, batch) {
        (true, true) => format!("/api/model/{m}/ops"),
        (true, false) => format!("/api/model/{m}/op"),
        (false, true) => "/api/ops".to_string(),
        (false, false) => "/api/op".to_string(),
    };
    let body = |op: &str, with_ordinary: bool| {
        if batch && with_ordinary {
            format!("{{\"ops\":[{},{op}]}}", shallow_deep_op())
        } else if batch {
            format!("{{\"ops\":[{op}]}}")
        } else {
            op.to_string()
        }
    };
    let before = read(&c, 0, &m);
    for d in [B + 1, L + 8, 2 * L] {
        let op = deep_op(d);
        let (code, text) = submit(&c, &path, body(&op, true));
        c.await_health(0)
            .unwrap_or_else(|e| panic!("G2 {path} depth {d}: node down: {e}"));
        let after = read(&c, 0, &m);
        let names = if d > L {
            names_depth_and(&text, B) || names_depth_and(&text, L)
        } else {
            names_depth_and(&text, B)
        };
        assert!(
            (400..500).contains(&code) && names && before == after,
            "G2 {path} operation depth {d}: expected an HTTP 4xx naming the depth and the \
             limit ({B} for an operation, or {L}), with state and version unchanged; got {code} \
             {text}; state unchanged={} version unchanged={}",
            before.0 == after.0,
            before.1 == after.1
        );
    }
    let (code, text) = submit(&c, &path, body(&deep_op(B), false));
    assert_eq!(
        code, 200,
        "G2 {path}: an operation of depth {B} must be admitted: {text}"
    );
    assert_ne!(
        read(&c, 0, &m).0,
        before.0,
        "G2 {path}: the admitted operation was not applied"
    );
    let (code, text) = submit(&c, &path, body(&shallow_deep_op(), false));
    assert_eq!(
        code, 200,
        "G2 {path}: an ordinary operation after the refusals: {text}"
    );
}

#[test]
fn nw27_g2_default_op() {
    admission(Route::DefaultOp)
}
#[test]
fn nw27_g2_default_ops() {
    admission(Route::DefaultOps)
}
#[test]
fn nw27_g2_named_op() {
    admission(Route::NamedOp)
}
#[test]
fn nw27_g2_named_ops() {
    admission(Route::NamedOps)
}

/// G1 at node level: a peer connection to a running node sends operation frames of depth
/// L + 8 and 2 x L; the node reports each refusal on a line naming the depth and L, and
/// goes on serving its peer connection, its HTTP API and its model.
#[test]
fn nw27_g1_node_tcp_refusal_keeps_serving() {
    let descriptor = deep_descriptor_text();
    let (c, _) = start(Transport::Tcp, &["alice"], &descriptor);
    let m = create_model(&c, &descriptor, false);
    let start_at = c.nodes[0].log_text().len();
    let mut peer = TcpStream::connect(("127.0.0.1", c.nodes[0].listen)).unwrap();
    writeln!(
        peer,
        "{{\"type\":\"Hello\",\"id\":\"zed\",\"metadata\":null}}"
    )
    .unwrap();
    for frame_depth in [L + 8, 2 * L] {
        // The Raw wrapper adds one level.
        let frame = format!(
            "{{\"type\":\"Raw\",\"payload\":{}}}",
            deep_op(frame_depth - 1)
        );
        assert_eq!(depth(&frame), frame_depth, "fixture");
        writeln!(peer, "{frame}").unwrap();
    }
    writeln!(peer, "{{\"type\":\"Goodbye\",\"id\":\"zed\"}}").unwrap();
    let end = Instant::now() + Duration::from_secs(20);
    let seen = loop {
        let text = c.nodes[0].log_text();
        let seen = text.get(start_at..).unwrap_or_default().to_string();
        if seen.contains("Peer disconnected: zed") {
            break seen;
        }
        assert!(
            Instant::now() < end,
            "G1 the frame after the refused ones was not handled\n{}",
            c.nodes[0].tail()
        );
        thread::sleep(Duration::from_millis(50));
    };
    let reported = seen.lines().filter(|line| names_depth_and(line, L)).count();
    assert!(
        reported >= 2,
        "G1 each refused frame must be reported on a line naming the depth and {L}; got:\n{seen}"
    );
    c.await_health(0).unwrap();
    let (code, text) = submit(&c, &format!("/api/model/{m}/op"), shallow_deep_op());
    assert_eq!(
        code, 200,
        "G1 the node serves an ordinary operation afterwards: {text}"
    );
}
