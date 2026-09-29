//! NW.03: a replica that joins a model by state transfer and then writes has
//! its edit delivered by every other replica, including a writer whose stable
//! version grew past what the joiner adopted before it knew the joiner.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.03.md`; the schedules N1 and
//! N2 below are derived there by hand from the code. Three `model_node`
//! processes on loopback: writers `a` and `b`, meshed by static peers, and the
//! joiner `d`, which reaches each writer only through a link of
//! [`proxy::Proxy`]. A link is a delay and nothing else (no frame is dropped,
//! changed or reordered), which is what lets the harness put the joiner's
//! frames where the schedule needs them without waiting on any timing:
//!
//! 1. `a` creates the model and writes `a0`; `b` joins it and writes `b0`.
//! 2. `d` starts behind two closed links and joins the model. With nothing
//!    reaching it, all it can send is its greeting and its requests for state.
//!    The link on which its request for this model arrived first is opened up
//!    to and including that request, and held after it: that writer is the
//!    donor; the other link stays closed, so for the other writer the joiner
//!    does not exist yet.
//! 3. `d` adopts the donor's state, then writes `joiner`, which both links
//!    hold.
//! 4. N1: the other writer writes `writer`, the donor delivers it and writes
//!    `donor`, and the other writer delivers that. N2: the donor writes
//!    `donor`, the other writer delivers it and writes `writer`, and the
//!    donor delivers that. Each step waits on what a replica serves.
//! 5. Both links are released, and within the quiescence window every replica
//!    must serve exactly the document written by hand below.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw03_joiner_writes_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! The node tests follow the skip rule of `e2e_interp.rs`: no binary, print
//! `E2E-SKIP <scenario>: <why>` and return. A skip is not a pass.
#![cfg(feature = "network")]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::JSON_DESCRIPTOR;
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

#[path = "nw02/launch.rs"]
mod launch;
#[path = "nw03/proxy.rs"]
mod proxy;

use proxy::Proxy;

/// The default log every node of a run hosts.
const LOG_ID: &str = "9a039a039a039a039a039a039a039a03";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long, once both links are released, the replicas have to serve the
/// document of the schedule. Pre-registered in the spec; not to be changed
/// without a ruling.
const QUIESCENCE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// The instrument's own check
// ---------------------------------------------------------------------------

/// A link holds everything until opened, forwards up to and including its
/// gate frame and holds the rest, passes the writer's frames back at once,
/// and on release flushes what it held in the order it was sent. Checked
/// against a plain TCP listener standing for the writer, before the proxy is
/// trusted with a schedule.
#[test]
fn nw03_proxy_holds_a_link_after_its_gate_frame_and_releases_it_in_order() {
    let writer = TcpListener::bind("127.0.0.1:0").expect("bind the stand-in writer");
    let writer_port = writer.local_addr().unwrap().port();
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(false));
    {
        let received = Arc::clone(&received);
        let accepted = Arc::clone(&accepted);
        std::thread::spawn(move || {
            let (stream, _) = writer.accept().expect("accept the proxy");
            *accepted.lock().unwrap() = true;
            let mut reply = stream.try_clone().unwrap();
            for (index, line) in BufReader::new(stream).lines().enumerate() {
                let Ok(line) = line else { break };
                received.lock().unwrap().push(line);
                if index == 0 {
                    reply.write_all(b"{\"type\":\"Pong\"}\n").unwrap();
                }
            }
        });
    }
    let proxy = Proxy::start(&[("w", writer_port)]).expect("start the proxy");
    let mut joiner = TcpStream::connect(("127.0.0.1", proxy.port("w"))).expect("dial the proxy");
    let frames = [
        r#"{"type":"Hello","id":"j","metadata":"1"}"#,
        r#"{"type":"StateRequest","id":"j","log_id":"other"}"#,
        r#"{"type":"StateRequest","id":"j","log_id":"L"}"#,
        r#"{"type":"Event","event":{"payload":{},"resolver":["j","w"],"log_id":"L"}}"#,
        r#"{"type":"SyncRequest","since":{"payload":{},"resolver":["j"],"log_id":"L"}}"#,
    ];
    for frame in frames {
        joiner.write_all(format!("{frame}\n").as_bytes()).unwrap();
    }
    let received_now = || received.lock().unwrap().clone();
    wait_until("the proxy to read five frames", STEP_TIMEOUT, || {
        Ok((proxy.seen("w").len() == 5).then_some(()))
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !*accepted.lock().unwrap(),
        "a closed link dialled the writer"
    );
    assert!(received_now().is_empty(), "a closed link forwarded a frame");
    assert_eq!(
        proxy.seen("w")[3].members,
        vec!["j".to_string(), "w".to_string()],
        "the member list an event carries was misread"
    );

    proxy.open_up_to("w", "StateRequest", "L");
    wait_until("the gate frame to reach the writer", STEP_TIMEOUT, || {
        Ok((received_now().len() == 3).then_some(()))
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        received_now(),
        frames[..3].to_vec(),
        "the link did not forward exactly up to and including its gate frame"
    );
    assert_eq!(
        proxy
            .held("w")
            .iter()
            .map(|frame| frame.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["Event", "SyncRequest"],
        "the link does not hold what follows its gate frame"
    );
    let mut back = BufReader::new(joiner.try_clone().unwrap());
    let mut pong = String::new();
    back.read_line(&mut pong).unwrap();
    assert_eq!(
        pong.trim(),
        r#"{"type":"Pong"}"#,
        "the writer's frame did not pass"
    );

    joiner
        .write_all(b"{\"type\":\"StateUnavailable\",\"reason\":\"-\",\"log_id\":\"L\"}\n")
        .unwrap();
    proxy.release("w");
    wait_until("the release to flush", STEP_TIMEOUT, || {
        Ok((received_now().len() == 6 && proxy.drained("w")).then_some(()))
    })
    .unwrap();
    let got = received_now();
    assert_eq!(got[..5].to_vec(), frames.to_vec(), "the release reordered");
    assert!(got[5].contains("StateUnavailable"));
    assert!(proxy.error().is_none(), "{:?}", proxy.error());
}

// ---------------------------------------------------------------------------
// The claim, over the wire
// ---------------------------------------------------------------------------

/// The order of the two writers' edits after the joiner has written.
#[derive(Clone, Copy, Debug)]
enum Order {
    /// N1: the other writer first, so its own edit turns stable on it, via
    /// the donor's reply, while it does not know the joiner.
    WriterFirst,
    /// N2: the donor first, so its own edit turns stable on it, via the other
    /// writer's reply, while it does not know the joiner.
    DonorFirst,
}

/// N1 of the spec. The non-donor writer's own edit `writer` becomes stable on
/// it through the donor's `donor` before it has taken the joiner into its
/// member table; the joiner's `joiner` was written without `writer`.
#[test]
fn nw03_n1_a_writer_that_learns_the_joiner_after_its_own_edit_turned_stable_delivers_the_joiners_edit()
 {
    scenario_or_skip("nw03-n1", Order::WriterFirst);
}

/// N2 of the spec. The donor's own edit `donor` becomes stable on it through
/// the other writer's `writer` before the donor has taken the joiner in; the
/// joiner's `joiner` was written without `donor`.
#[test]
fn nw03_n2_the_donor_whose_own_edit_turned_stable_before_it_knew_the_joiner_delivers_the_joiners_edit()
 {
    scenario_or_skip("nw03-n2", Order::DonorFirst);
}

fn scenario_or_skip(name: &str, order: Order) {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP {name}: {why}");
            return;
        }
    };
    if let Err(why) = scenario(&binary, name, order) {
        panic!("{name}: {why}");
    }
}

/// The document every replica must serve once the links are released: the
/// two writers' edits before the join, the joiner's, and the two writers'
/// after it, each a string under its own key of the root object, so no two of
/// them conflict and the whole document is known in advance.
fn expected() -> Value {
    json!({"a0": "a", "b0": "b", "joiner": "d", "donor": "x", "writer": "y"})
}

fn scenario(binary: &Path, name: &str, order: Order) -> Result<(), String> {
    let tag = name.to_uppercase();
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let scratch = std::env::temp_dir().join(format!("moirai-nw03-{}-{run}", std::process::id()));
    let metamodels = scratch.join("metamodels");
    fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
    fs::write(metamodels.join("json.json"), JSON_DESCRIPTOR).map_err(|err| err.to_string())?;
    let metamodels = metamodels.to_string_lossy().into_owned();
    let env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", metamodels.as_str())];
    let js = Json::new();
    let digest = metamodel_digest(&serde_json::from_str::<Value>(JSON_DESCRIPTOR).unwrap());
    let http = Http::new()?;

    // ---- 1. two writers, meshed with each other and nobody else ----
    let specs = [
        launch::NodeSpec::new("a", &env),
        launch::NodeSpec::new("b", &env),
    ];
    let writers: Vec<Node> = launch::launch(binary, &scratch, &specs)?
        .into_iter()
        .map(|node| Node {
            id: node.id,
            http: node.http,
            listen: node.listen,
            child: node.child,
            log: node.log,
        })
        .collect();
    for node in &writers {
        http.await_health(node)?;
    }
    wait_until("a and b to see each other", STEP_TIMEOUT, || {
        Ok(writers
            .iter()
            .all(|node| {
                http.get(node, "/api/peers")
                    .map(|(_, peers)| connected(&peers) == 1)
                    .unwrap_or(false)
            })
            .then_some(()))
    })?;
    let (a, b) = (&writers[0], &writers[1]);

    let model = http.register(a, None, &digest)?;
    http.submit(a, &model, &js.put("a0", 'a'))?;
    http.register(b, Some(&model), &digest)?;
    http.until_serves(b, &model, &json!({"a0": "a"}))?;
    http.submit(b, &model, &js.put("b0", 'b'))?;
    let history = json!({"a0": "a", "b0": "b"});
    http.until_serves(a, &model, &history)?;
    http.until_serves(b, &model, &history)?;

    // ---- 2. the joiner, behind two closed links ----
    let proxy = Proxy::start(&[("a", a.listen), ("b", b.listen)])?;
    let d = spawn_joiner(
        binary,
        &scratch,
        "d",
        &env,
        &format!(
            "a:127.0.0.1:{},b:127.0.0.1:{}",
            proxy.port("a"),
            proxy.port("b")
        ),
    )?;
    http.await_health(&d)?;
    wait_until("d to enter its event loop", STEP_TIMEOUT, || {
        Ok(d.log_text()
            .contains("Entering main event loop")
            .then_some(()))
    })?;
    http.register(&d, Some(&model), &digest)?;

    // The link that carried the joiner's request for this model first is the
    // donor's; every frame before it on that link is a greeting or a request
    // for state, since nothing has reached the joiner yet to answer.
    let first = wait_until(
        "d to ask a writer for the model's state",
        STEP_TIMEOUT,
        || Ok(proxy.first("StateRequest", &model)),
    );
    let donor_id = match first {
        Ok((writer, _)) => writer,
        // Only a node that no longer asks for state behind closed links gets
        // here; the donor is then `a`, whose link opens up to that request
        // whenever it comes.
        Err(_) => "a".to_string(),
    };
    let (donor, other) = if donor_id == "a" { (a, b) } else { (b, a) };
    proxy.open_up_to(&donor.id, "StateRequest", &model);

    // ---- 3. the joiner adopts, then writes ----
    http.until_serves(&d, &model, &history).map_err(|why| {
        format!(
            "d never joined the model from {}: {why}\n{}",
            donor.id,
            proxy.describe()
        )
    })?;
    let adopted = http.metrics(&d, &model)?;
    let seen = proxy.seen(&donor.id);
    let forwarded = seen.len().saturating_sub(proxy.held(&donor.id).len());
    eprintln!(
        "{tag} donor {}, other writer {}; the donor's link forwarded [{}] and holds the rest",
        donor.id,
        other.id,
        labels(&seen[..forwarded.min(seen.len())])
    );
    http.submit(&d, &model, &js.put("joiner", 'd'))?;
    wait_until("both links to hold d's edit", STEP_TIMEOUT, || {
        Ok([donor, other]
            .iter()
            .all(|writer| proxy.held(&writer.id).iter().any(|f| f.kind == "Event"))
            .then_some(()))
    })
    .map_err(|why| format!("{why}\n{}", proxy.describe()))?;

    // ---- 4. the two writers' edits ----
    let (first_writer, first_key, first_value, second_writer, second_key, second_value) =
        match order {
            Order::WriterFirst => (other, "writer", 'y', donor, "donor", 'x'),
            Order::DonorFirst => (donor, "donor", 'x', other, "writer", 'y'),
        };
    http.submit(first_writer, &model, &js.put(first_key, first_value))?;
    http.until_holds(second_writer, &model, first_key, first_value)?;
    http.submit(second_writer, &model, &js.put(second_key, second_value))?;
    http.until_holds(first_writer, &model, second_key, second_value)?;

    let before: Vec<String> = [donor, other, &d]
        .iter()
        .map(|node| {
            format!(
                "{}: {}",
                node.id,
                http.metrics(node, &model)
                    .map(|m| stability(&m))
                    .unwrap_or_else(|why| why)
            )
        })
        .collect();
    eprintln!(
        "{tag} before release: d adopted {}; {}",
        stability(&adopted),
        before.join("; ")
    );

    // ---- 5. release, and the document every replica must serve ----
    proxy.release(&other.id);
    proxy.release(&donor.id);
    let released = Instant::now();
    let want = expected();
    let replicas = [donor, other, &d];
    loop {
        let states: Vec<Result<Value, String>> = replicas
            .iter()
            .map(|node| http.state(node, &model))
            .collect();
        if states
            .iter()
            .all(|state| state.as_ref().ok() == Some(&want))
        {
            eprintln!(
                "{tag} pass: {} s after release every replica serves {want}",
                released.elapsed().as_secs_f32()
            );
            return Ok(());
        }
        if released.elapsed() > QUIESCENCE {
            let report: Vec<String> = replicas
                .iter()
                .zip(states.iter())
                .map(|(node, state)| {
                    format!(
                        "{} serves {} [{}]",
                        node.id,
                        match state {
                            Ok(doc) => doc.to_string(),
                            Err(why) => why.clone(),
                        },
                        http.metrics(node, &model)
                            .map(|m| stability(&m))
                            .unwrap_or_else(|why| why)
                    )
                })
                .collect();
            return Err(format!(
                "{} s after both links were released the replicas do not all serve {want}:\n{}\n\
                 before release: d adopted {}; {}\n{}",
                QUIESCENCE.as_secs(),
                report.join("\n"),
                stability(&adopted),
                before.join("; "),
                proxy.describe()
            ));
        }
        std::thread::sleep(POLL);
    }
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

fn node_binary() -> Result<PathBuf, String> {
    if let Ok(raw) = std::env::var("MOIRAI_E2E_INTERP_NODE_BIN") {
        let path = PathBuf::from(&raw);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "MOIRAI_E2E_INTERP_NODE_BIN points at `{raw}`, which is not a file"
            ))
        };
    }
    let built =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/examples/model_node");
    if built.is_file() {
        Ok(built)
    } else {
        Err(format!(
            "no `model_node` at {}; build it with `cargo build -p moirai-interp --features \
             network --example model_node`",
            built.display()
        ))
    }
}

struct Node {
    id: String,
    http: u16,
    listen: u16,
    child: Child,
    log: PathBuf,
}

impl Node {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start the joiner on ports it binds itself (NW.02), and give it `peers`,
/// which name the proxy's links rather than the writers.
fn spawn_joiner(
    binary: &Path,
    scratch: &Path,
    id: &str,
    env: &[(&str, &str)],
    peers: &str,
) -> Result<Node, String> {
    let log = scratch.join(format!("{id}.log"));
    let ports = scratch.join(format!("{id}.ports.json"));
    let peers_file = scratch.join(format!("{id}.peers"));
    let _ = fs::remove_file(&ports);
    let _ = fs::remove_file(&peers_file);
    let out = fs::File::create(&log).map_err(|err| err.to_string())?;
    let err_out = out.try_clone().map_err(|err| err.to_string())?;
    let mut child = Command::new(binary)
        .env("REPLICA_ID", id)
        .env("LISTEN_PORT", "0")
        .env("HTTP_PORT", "0")
        .env("PORTS_FILE", &ports)
        .env("PEERS_FILE", &peers_file)
        .env_remove("PEERS")
        .env_remove("BOOTNODE_URL")
        .env_remove("DASHBOARD_URL")
        .env_remove("METAMODEL_PATH")
        .env_remove("ADVERTISE_ADDR")
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|err| format!("spawning {id}: {err}"))?;
    let deadline = Instant::now() + launch::PORTS_TIMEOUT;
    let reported = loop {
        if let Some(parsed) = launch::read_ports_file(&ports) {
            let reported = parsed?;
            if reported.replica_id != id || reported.pid != child.id() {
                let _ = child.kill();
                return Err(format!("{id}'s ports file is not its own: {reported:?}"));
            }
            break reported;
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!(
                "{id} exited with {status} before reporting its ports:\n{}",
                launch::tail(&log, 25)
            ));
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err(format!(
                "{id} never reported its ports:\n{}",
                launch::tail(&log, 25)
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    launch::write_atomically(&peers_file, peers)?;
    Ok(Node {
        id: id.to_string(),
        http: reported.http_port.ok_or("d reported no HTTP port")?,
        listen: reported.listen_port,
        child,
        log,
    })
}

struct Http(reqwest::blocking::Client);

impl Http {
    fn new() -> Result<Self, String> {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map(Http)
            .map_err(|err| err.to_string())
    }

    fn get(&self, node: &Node, path: &str) -> Result<(u16, Value), String> {
        let response = self
            .0
            .get(node.url(path))
            .send()
            .map_err(|err| format!("GET {path} on {}: {err}", node.id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn post(&self, node: &Node, path: &str, body: String) -> Result<(u16, Value), String> {
        let response = self
            .0
            .post(node.url(path))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|err| format!("POST {path} on {}: {err}", node.id))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|err| err.to_string())?;
        Ok((
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        ))
    }

    fn await_health(&self, node: &Node) -> Result<(), String> {
        wait_until(
            &format!("{} to answer /api/health", node.id),
            HEALTH_TIMEOUT,
            || match self.get(node, "/api/health") {
                Ok((200, body)) if body["replica_id"] == json!(node.id) => Ok(Some(())),
                Ok((200, body)) => Err(format!(
                    "the HTTP port {} reported by {} answered as {body}",
                    node.http, node.id
                )),
                _ => Ok(None),
            },
        )
        .map_err(|why| {
            format!(
                "{why}; alarms:\n{}\n{}",
                launch::alarms(&node.log),
                launch::tail(&node.log, 25)
            )
        })
    }

    /// Create (`model` none) or join a model; answers its id.
    fn register(&self, node: &Node, model: Option<&str>, key: &str) -> Result<String, String> {
        let body = match model {
            Some(id) => json!({ "model_id": id, "metamodel_id": key }),
            None => json!({ "metamodel_id": key }),
        };
        let (status, answer) = self.post(node, "/api/models", body.to_string())?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "registering on {} answered {status}: {answer}",
                node.id
            ));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    fn submit(&self, node: &Node, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!("{} refused {op:?} ({status}): {answer}", node.id));
        }
        Ok(())
    }

    /// The default read, `GET /api/model/{id}/state`.
    fn state(&self, node: &Node, model: &str) -> Result<Value, String> {
        match self.get(node, &format!("/api/model/{model}/state"))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("{} answered {status}: {body}", node.id)),
        }
    }

    fn metrics(&self, node: &Node, model: &str) -> Result<Value, String> {
        match self.get(node, &format!("/api/model/{model}/metrics"))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("{} metrics answered {status}: {body}", node.id)),
        }
    }

    fn until_serves(&self, node: &Node, model: &str, want: &Value) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to serve {want}", node.id),
            STEP_TIMEOUT,
            || {
                if let Ok(state) = self.state(node, model) {
                    last = state;
                }
                Ok((&last == want).then_some(()))
            },
        )
        .map_err(|why| format!("{why}; it served {last}"))
    }

    fn until_holds(&self, node: &Node, model: &str, key: &str, value: char) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to hold {key}: {value}", node.id),
            STEP_TIMEOUT,
            || {
                if let Ok(state) = self.state(node, model) {
                    last = state;
                }
                Ok((last[key] == json!(value.to_string())).then_some(()))
            },
        )
        .map_err(|why| format!("{why}; it served {last}"))
    }
}

fn connected(peers: &Value) -> usize {
    peers["peers"]
        .as_array()
        .map(|list| {
            list.iter()
                .filter(|peer| peer["status"] == json!("Connected"))
                .count()
        })
        .unwrap_or(0)
}

/// The stability counters of a model's metrics, for reports.
fn stability(metrics: &Value) -> String {
    format!(
        "stable_version {} known_replicas {} delivered {} pending {} retained {}",
        metrics["stable_version"],
        metrics["known_replicas"],
        metrics["delivered_ops"],
        metrics["pending_ops"],
        metrics["retained_ops"]
    )
}

fn labels(frames: &[proxy::Frame]) -> String {
    frames
        .iter()
        .map(proxy::Frame::label)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Poll `probe` until it answers `Some`, fails, or `timeout` passes.
fn wait_until<T>(
    what: &str,
    timeout: Duration,
    mut probe: impl FnMut() -> Result<Option<T>, String>,
) -> Result<T, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe()? {
            return Ok(value);
        }
        if Instant::now() > deadline {
            return Err(format!(
                "timed out after {} s waiting for {what}",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(POLL);
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// `json.ecore`, the one operation the schedule uses: a one-character string
/// put under a key of the root object.
struct Json(MetamodelSemantics);

impl Json {
    fn new() -> Self {
        Json(
            from_descriptor(&serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON"))
                .expect("the fixture is a descriptor this crate reads"),
        )
    }

    fn is(&self, class: &str, inner: InstanceOp) -> InstanceOp {
        InstanceOp::variant(moirai_interp::testing::class_slot(&self.0, class), inner)
    }

    fn at(&self, class: &str, feature: &str, inner: InstanceOp) -> InstanceOp {
        let slot = moirai_interp::testing::class_slot(&self.0, class);
        InstanceOp::field(
            moirai_interp::testing::feature_slot(&self.0, slot, feature),
            inner,
        )
    }

    /// `{key: "<ch>"}` on the root object.
    fn put(&self, key: &str, ch: char) -> ModelOp {
        let string = self.is(
            "String",
            self.at(
                "String",
                "value",
                InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch }),
            ),
        );
        self.is(
            "Object",
            self.at(
                "Object",
                "entry",
                InstanceOp::entry(Scalar::text(key), string),
            ),
        )
        .into_model_op()
    }
}
