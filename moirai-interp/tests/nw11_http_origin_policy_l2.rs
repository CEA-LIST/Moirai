//! NW.11: `model_node`'s HTTP API binds where it is told, refuses a request
//! from a browser origin outside its allow-list and a write whose body is not
//! declared JSON before anything changes, and answers an allowed origin by
//! echoing it instead of `*`.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.11.md`. Four scenarios:
//!
//! - B, the bind. `HTTP_BIND_ADDR=127.0.0.1` makes the HTTP listener answer on
//!   127.0.0.1 and refuse a connection to 127.0.0.2 (another loopback address
//!   of the same host, which a listener on every interface answers), on both
//!   paths the node binds by: a listener bound before the API (`PORTS_FILE`,
//!   or `HTTP_PORT=0`) and a fixed port bound inside the API's thread (the
//!   docker stack's path). Unset, both paths answer on both addresses, as
//!   today. An address the node cannot read serves on neither.
//! - O, the default policy, on a pair `a`, `b` of nodes with no
//!   `HTTP_ALLOWED_ORIGINS`: every mutating route refuses ten origins outside
//!   the default list with 403 and no `Access-Control-*` header, in the shape
//!   a page would send (typed JSON, `text/plain`, no body), and nothing
//!   changes; every body route refuses a body that is not declared
//!   `application/json` with 415, from no origin and from an allowed one, and
//!   nothing changes; a local client with no `Origin` (curl, the Node suites)
//!   still writes, reads and pauses; each of the four default origins gets a
//!   preflight that grants `POST` with `Content-Type`, then a write and a read
//!   answered with its own origin echoed and `ETag` exposed; and no response
//!   of the whole scenario carries `Access-Control-Allow-Origin: *`.
//! - C, a configured list. `HTTP_ALLOWED_ORIGINS` replaces the default: an
//!   exact origin and a `scheme://host:*` entry are allowed, the default's
//!   origins and every near miss are refused.
//! - S, the stack. Every `curl` in `docker/compose` that sends a body
//!   declares it JSON, and `replica.yml` lets the host set the allow-list.
//!
//! Every expectation is written here: the origins, the content types, the
//! statuses (403 for an origin, 415 for a content type), and the state each
//! refusal must leave, read back from the node before and after. Nothing is
//! read from the implementation.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! NW11_SCRATCH=/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.11/run \
//!   nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!   --test nw11_http_origin_policy_l2 -- --test-threads=1 --nocapture
//! ```
//!
//! The node scenarios follow the skip rule of `e2e_interp.rs`: no binary,
//! print `E2E-SKIP <scenario>: <why>` and return. A skip is not a pass.
#![cfg(feature = "network")]

use std::cell::RefCell;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::JSON_DESCRIPTOR;
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

#[path = "nw02/launch.rs"]
mod launch;

/// The variable that names the address the HTTP listener binds.
const BIND_VAR: &str = "HTTP_BIND_ADDR";
/// The variable that names the allowed browser origins, comma-separated.
const ORIGINS_VAR: &str = "HTTP_ALLOWED_ORIGINS";

/// The default allow-list, exactly: the two editors' Vite dev servers, the
/// first on 5173 and the second, when both run, on 5174, under the two names
/// a browser on this machine reaches them by.
const DEFAULT_ALLOWED: [&str; 4] = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
];

/// Origins outside the default list. A web site; a web site on the editor's
/// port; the opaque origin of a sandboxed frame or a file; a third Vite port;
/// another local port; two that begin with an allowed origin; the allowed
/// host and port over https; the IPv6 loopback, which the list does not name;
/// the allowed host with no port.
const DISALLOWED_BY_DEFAULT: [&str; 10] = [
    "https://evil.example",
    "http://evil.example:5173",
    "null",
    "http://localhost:5175",
    "http://127.0.0.1:8080",
    "http://localhost:51730",
    "http://localhost:5173.evil.example",
    "https://localhost:5173",
    "http://[::1]:5173",
    "http://localhost",
];

/// Content types a body must not be accepted under: the three a page may send
/// without a preflight, `text/plain` bare, none at all, and three that only
/// look like JSON.
const NOT_JSON: [Option<&str>; 8] = [
    Some("text/plain;charset=UTF-8"),
    Some("text/plain"),
    Some("application/x-www-form-urlencoded"),
    Some("multipart/form-data; boundary=nw11"),
    None,
    Some("application/jsonx"),
    Some("text/json"),
    Some("application/json5"),
];

/// The configured list of scenario C, and what it must allow and refuse.
const CONFIGURED: &str = "https://app.example.test:8443,http://127.0.0.1:*";
const CONFIGURED_ALLOWED: [&str; 3] = [
    "https://app.example.test:8443",
    "http://127.0.0.1:41234",
    "http://127.0.0.1:5173",
];
const CONFIGURED_REFUSED: [&str; 7] = [
    "http://localhost:5173",
    "http://localhost:41234",
    "https://127.0.0.1:41234",
    "https://app.example.test",
    "https://app.example.test:8444",
    "http://127.0.0.10:41234",
    "http://127.0.0.1.evil.example:41234",
];

/// The default log every node of a run hosts.
const LOG_ID: &str = "4e114e114e114e114e114e114e114e11";
/// A model id nobody hosts, which a refused join must not make hosted.
const FRESH_ID: &str = "4e11f0f0f0f0f0f0f0f0f0f0f0f0f0f0";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long after the last refused request the node's state is read back.
/// Pre-registered in the spec; not to be changed without a ruling.
const SETTLE: Duration = Duration::from_secs(2);
/// How long a node given an address it cannot read is watched for serving.
/// Pre-registered in the spec.
const UNREADABLE_WATCH: Duration = Duration::from_secs(8);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// The claim, over the wire
// ---------------------------------------------------------------------------

#[test]
fn nw11_b_the_http_listener_binds_the_configured_address_and_only_it() {
    run_or_skip("nw11_b", b);
}

#[test]
fn nw11_o_the_default_policy_refuses_foreign_origins_and_untyped_bodies_and_serves_the_rest() {
    run_or_skip("nw11_o", o);
}

#[test]
fn nw11_c_a_configured_list_replaces_the_default() {
    run_or_skip("nw11_c", c);
}

#[test]
fn nw11_s_the_stack_sends_json_and_can_set_the_list() {
    let mut checks = Checks::new("S");
    s(&mut checks);
    checks.finish();
}

fn run_or_skip(name: &str, scenario: fn(&Path, &str) -> Result<Checks, String>) {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            println!("E2E-SKIP {name}: {why}");
            return;
        }
    };
    match scenario(&binary, name) {
        Ok(checks) => checks.finish(),
        Err(why) => panic!("NW11 {name} PREMISE failed: {why}"),
    }
}

// ---------------------------------------------------------------------------
// B, the bind
// ---------------------------------------------------------------------------

fn b(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new("B");
    let scratch = scratch_for(name)?;
    let http = Http::new();

    // B1 and B2: the listener bound before the API (`PORTS_FILE`, port 0).
    for (check, bind, loopback_only) in [
        (
            "b1.unset_on_the_ports_file_path_answers_on_every_interface",
            None,
            false,
        ),
        (
            "b2.loopback_on_the_ports_file_path_answers_on_loopback_only",
            Some("127.0.0.1"),
            true,
        ),
    ] {
        let env: Vec<(&str, String)> = bind
            .map(|addr| (BIND_VAR, addr.to_string()))
            .into_iter()
            .collect();
        let result = single(
            binary,
            &scratch,
            &format!("{}-{}", name, &check[..2]),
            &env,
            &http,
        )
        .and_then(|node| reach(&http, &node, loopback_only));
        checks.check(check, result);
    }

    // B3 and B4: a fixed port bound inside the API's thread, which is what the
    // docker stack runs (`HTTP_PORT=8081`, no `PORTS_FILE`).
    for (check, bind, loopback_only) in [
        (
            "b3.unset_on_the_fixed_port_path_answers_on_every_interface",
            None,
            false,
        ),
        (
            "b4.loopback_on_the_fixed_port_path_answers_on_loopback_only",
            Some("127.0.0.1"),
            true,
        ),
    ] {
        let env: Vec<(&str, String)> = bind
            .map(|addr| (BIND_VAR, addr.to_string()))
            .into_iter()
            .collect();
        let result = fixed(
            binary,
            &scratch,
            &format!("{}-{}", name, &check[..2]),
            &env,
            &http,
        )
        .and_then(|node| reach(&http, &node, loopback_only));
        checks.check(check, result);
    }

    // B5 and B6: an address the node cannot read. Falling back to every
    // interface, or to any address at all, is serving where nobody said to.
    checks.check(
        "b5.an_unreadable_address_on_the_fixed_port_path_serves_nowhere",
        unreadable_fixed(binary, &scratch, &format!("{name}-b5")),
    );
    checks.check(
        "b6.an_unreadable_address_on_the_ports_file_path_serves_nowhere",
        unreadable_ports_file(binary, &scratch, &format!("{name}-b6")),
    );
    Ok(checks)
}

/// The node answers `/api/health` on 127.0.0.1, and on 127.0.0.2 exactly when
/// it was not told to bind loopback only.
fn reach(http: &Http, node: &Node, loopback_only: bool) -> Result<String, String> {
    let other = http.send_to("127.0.0.2", node.http, &Req::get("/api/health"));
    match (loopback_only, other) {
        (false, Ok(answer)) => Ok(format!(
            "127.0.0.1:{0} answers /api/health and 127.0.0.2:{0} answers HTTP ({1})",
            node.http, answer.status
        )),
        (false, Err(why)) => Err(format!(
            "127.0.0.2:{} did not answer, so the default no longer listens on every \
             interface: {why}",
            node.http
        )),
        (true, Err(why)) if why.contains("ConnectionRefused") => Ok(format!(
            "127.0.0.1:{0} answers, 127.0.0.2:{0} refuses the connection",
            node.http
        )),
        (true, Err(why)) => Err(format!(
            "127.0.0.2:{} failed otherwise than by a refused connection: {why}",
            node.http
        )),
        (true, Ok(answer)) => Err(format!(
            "127.0.0.2:{} answered {} although the node was told to bind 127.0.0.1",
            node.http, answer.status
        )),
    }
}

fn unreadable_fixed(binary: &Path, scratch: &Path, id: &str) -> Result<String, String> {
    let port = pick_port()?;
    let mut spawned = spawn(
        binary,
        scratch,
        id,
        &[(BIND_VAR, "not-an-address".to_string())],
        Launch::Fixed(port),
    )?;
    watch_serves_nowhere(&mut spawned.child, &spawned.log, || Some(port))
}

fn unreadable_ports_file(binary: &Path, scratch: &Path, id: &str) -> Result<String, String> {
    let mut spawned = spawn(
        binary,
        scratch,
        id,
        &[(BIND_VAR, "not-an-address".to_string())],
        Launch::PortsFile { peers_file: false },
    )?;
    let ports = spawned
        .ports
        .clone()
        .expect("a ports-file launch has a ports file");
    watch_serves_nowhere(&mut spawned.child, &spawned.log, move || {
        launch::read_ports_file(&ports)
            .and_then(|parsed| parsed.ok())
            .and_then(|r| r.http_port)
    })
}

/// For [`UNREADABLE_WATCH`], or until the process exits: no connection to
/// the port on 127.0.0.1 or 127.0.0.2 succeeds.
fn watch_serves_nowhere(
    child: &mut Child,
    log: &Path,
    port: impl Fn() -> Option<u16>,
) -> Result<String, String> {
    let deadline = Instant::now() + UNREADABLE_WATCH;
    let mut probes = 0;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Ok(format!(
                "the node exited ({status}) after {probes} probes; alarms: {}",
                launch::alarms(log)
            ));
        }
        if let Some(port) = port() {
            for host in ["127.0.0.1", "127.0.0.2"] {
                probes += 1;
                if let Ok(addr) = format!("{host}:{port}").parse::<SocketAddr>()
                    && TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
                {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "{host}:{port} accepted a connection though the address was \
                         unreadable; log:\n{}",
                        launch::tail(log, 10)
                    ));
                }
            }
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(format!(
                "still running after {} s and served on neither address ({probes} probes)",
                UNREADABLE_WATCH.as_secs()
            ));
        }
        std::thread::sleep(POLL);
    }
}

// ---------------------------------------------------------------------------
// O, the default policy
// ---------------------------------------------------------------------------

fn o(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new("O");
    let scratch = scratch_for(name)?;
    let http = Http::new();
    let json = Json::new();
    let (a, b) = pair(binary, &scratch, &http)?;

    // ---- set-up, as a local client: the default log opened, a model made ----
    let digest = metamodel_digest(
        &serde_json::from_str::<Value>(JSON_DESCRIPTOR).map_err(|e| e.to_string())?,
    );
    let install = serde_json::to_string(&moirai_interp::testing::install(
        LOG_ID,
        &serde_json::from_str::<Value>(JSON_DESCRIPTOR).map_err(|e| e.to_string())?,
    ))
    .map_err(|e| e.to_string())?;
    http.local_write(&a, "/api/op", &install)?;
    http.local_write(&a, "/api/op", &json.put("seed", 's'))?;
    let model = http.create(&a, &digest)?;
    http.local_write(
        &a,
        &format!("/api/model/{model}/op"),
        &json.put("seed", 's'),
    )?;
    let probe = retagged("NW11Probe", "http://nw11.test/probe");
    let probe_digest =
        metamodel_digest(&serde_json::from_str::<Value>(&probe).map_err(|e| e.to_string())?);
    let before = Snapshot::read(&http, &a, &model)?;
    if before.peers != vec![("b".to_string(), "Connected".to_string())] {
        return Err(format!(
            "set-up: `a` does not see `b` connected: {:?}",
            before.peers
        ));
    }

    // ---- O1: a disallowed origin changes nothing and is granted nothing ----
    let body_routes = body_routes(&json, &model, &digest, &probe);
    let mut refusals: Vec<(String, Vec<String>, usize)> = Vec::new();
    for (route, path, body) in &body_routes {
        let mut wrong = Vec::new();
        for origin in DISALLOWED_BY_DEFAULT {
            for (shape, content_type, allowed) in [
                ("json", Some("application/json"), &[403][..]),
                ("simple", Some("text/plain;charset=UTF-8"), &[403, 415][..]),
                ("untyped", None, &[403, 415][..]),
            ] {
                let mut req = Req::post(path).origin(origin).body(body);
                if let Some(content_type) = content_type {
                    req = req.typed(content_type);
                }
                note_refusal(
                    &http,
                    &a,
                    &req,
                    allowed,
                    &format!("{origin} {shape}"),
                    &mut wrong,
                );
            }
        }
        refusals.push((route.to_string(), wrong, 3 * DISALLOWED_BY_DEFAULT.len()));
    }
    for (route, path) in [
        ("pause_b", "/api/pause/b"),
        ("pause_all", "/api/pause-all"),
        ("leave", "/api/leave"),
    ] {
        let mut wrong = Vec::new();
        for origin in DISALLOWED_BY_DEFAULT {
            note_refusal(
                &http,
                &a,
                &Req::post(path).origin(origin),
                &[403],
                &format!("{origin} bodyless"),
                &mut wrong,
            );
            note_refusal(
                &http,
                &a,
                &Req::post(path).origin(origin).typed("text/plain"),
                &[403, 415],
                &format!("{origin} text/plain bodyless"),
                &mut wrong,
            );
        }
        refusals.push((route.to_string(), wrong, 2 * DISALLOWED_BY_DEFAULT.len()));
    }
    for (route, wrong, cases) in refusals {
        checks.check(&format!("o1.refused.{route}"), verdict(wrong, cases));
    }
    let mut preflights = Vec::new();
    let mut reads = Vec::new();
    for origin in DISALLOWED_BY_DEFAULT {
        let pre = http.send(
            &a,
            &Req::preflight(&format!("/api/model/{model}/op"), origin),
        );
        match pre {
            Ok(answer) if answer.cors().is_empty() => {}
            Ok(answer) => preflights.push(format!("{origin}: {:?}", answer.cors())),
            Err(why) => preflights.push(format!("{origin}: {why}")),
        }
        let read = http.send(
            &a,
            &Req::get(&format!("/api/model/{model}/state")).origin(origin),
        );
        match read {
            Ok(answer) if answer.cors().is_empty() => {}
            Ok(answer) => reads.push(format!("{origin}: {:?}", answer.cors())),
            Err(why) => reads.push(format!("{origin}: {why}")),
        }
    }
    checks.check(
        "o1.a_foreign_preflight_is_granted_nothing",
        verdict(preflights, DISALLOWED_BY_DEFAULT.len()),
    );
    checks.check(
        "o1.a_foreign_read_is_granted_nothing",
        verdict(reads, DISALLOWED_BY_DEFAULT.len()),
    );
    std::thread::sleep(SETTLE);
    let after = Snapshot::read(&http, &a, &model)?;
    before.compare(&after, "o1.nothing_changed", &mut checks);
    checks.check(
        "o1.nothing_changed.probe_descriptor_not_held",
        not_listed(&after, &probe_digest),
    );

    // Paused, the two resume routes: a foreign resume leaves `b` paused.
    http.local_control(&a, "/api/pause/b")?;
    let paused = Snapshot::read(&http, &a, &model)?;
    if paused.peers != vec![("b".to_string(), "Paused".to_string())] {
        return Err(format!(
            "set-up: a local pause of `b` left {:?}",
            paused.peers
        ));
    }
    for (route, path) in [
        ("resume_b", "/api/resume/b"),
        ("resume_all", "/api/resume-all"),
    ] {
        let mut wrong = Vec::new();
        for origin in DISALLOWED_BY_DEFAULT {
            note_refusal(
                &http,
                &a,
                &Req::post(path).origin(origin),
                &[403],
                &format!("{origin} bodyless"),
                &mut wrong,
            );
            note_refusal(
                &http,
                &a,
                &Req::post(path).origin(origin).typed("text/plain"),
                &[403, 415],
                &format!("{origin} text/plain bodyless"),
                &mut wrong,
            );
        }
        checks.check(
            &format!("o1.refused.{route}"),
            verdict(wrong, 2 * DISALLOWED_BY_DEFAULT.len()),
        );
    }
    std::thread::sleep(SETTLE);
    let still = Snapshot::read(&http, &a, &model)?;
    checks.check(
        "o1.nothing_changed.peers_stay_paused",
        if still.peers == paused.peers {
            Ok(format!("{:?}", still.peers))
        } else {
            Err(format!("{:?} became {:?}", paused.peers, still.peers))
        },
    );
    http.local_control(&a, "/api/resume-all")?;
    wait_until("`a` to see `b` connected again", STEP_TIMEOUT, || {
        Ok(Snapshot::read(&http, &a, &model)?
            .peers
            .iter()
            .all(|(_, status)| status == "Connected")
            .then_some(()))
    })?;

    // ---- O2: a body not declared JSON changes nothing, whatever its origin ----
    let before = Snapshot::read(&http, &a, &model)?;
    for (route, path, body) in &body_routes {
        let mut wrong = Vec::new();
        for origin in [None, Some(DEFAULT_ALLOWED[0])] {
            for content_type in NOT_JSON {
                let mut req = Req::post(path).body(body);
                if let Some(content_type) = content_type {
                    req = req.typed(content_type);
                }
                if let Some(origin) = origin {
                    req = req.origin(origin);
                }
                let label = format!(
                    "{} from {}",
                    content_type.unwrap_or("<no Content-Type>"),
                    origin.unwrap_or("<no Origin>")
                );
                note_status(&http, &a, &req, &[415], &label, &mut wrong);
            }
        }
        checks.check(
            &format!("o2.refused.{route}"),
            verdict(wrong, 2 * NOT_JSON.len()),
        );
    }
    let mut wrong = Vec::new();
    for path in ["/api/pause/b", "/api/pause-all"] {
        for origin in [None, Some(DEFAULT_ALLOWED[0])] {
            let mut req = Req::post(path).typed("text/plain");
            if let Some(origin) = origin {
                req = req.origin(origin);
            }
            note_status(
                &http,
                &a,
                &req,
                &[415],
                &format!("{path} from {}", origin.unwrap_or("<no Origin>")),
                &mut wrong,
            );
        }
    }
    checks.check("o2.refused.a_text_plain_pause", verdict(wrong, 4));
    std::thread::sleep(SETTLE);
    let after = Snapshot::read(&http, &a, &model)?;
    before.compare(&after, "o2.nothing_changed", &mut checks);
    checks.check(
        "o2.nothing_changed.probe_descriptor_not_held",
        not_listed(&after, &probe_digest),
    );

    // ---- O3: a local client, with no Origin, is served as before ----
    for (check, content_type) in [
        ("o3.a_local_json_write_applies", "application/json"),
        (
            "o3.a_local_json_write_with_a_charset_applies",
            "application/json; charset=utf-8",
        ),
        (
            "o3.a_local_json_write_in_capitals_applies",
            "Application/JSON",
        ),
    ] {
        let path = format!("/api/model/{model}/op");
        let result = applies(
            &http,
            &a,
            &model,
            &Req::post(&path)
                .typed(content_type)
                .body(&json.put("local", 'l')),
            None,
        );
        checks.check(check, result);
    }
    checks.check(
        "o3.a_local_batch_applies",
        applies(
            &http,
            &a,
            &model,
            &Req::post(&format!("/api/model/{model}/ops"))
                .typed("application/json")
                .body(&batch(&[json.put("local", 'b')])),
            None,
        ),
    );
    checks.check(
        "o3.a_local_bodyless_pause_and_resume_apply",
        local_bodyless_pause(&http, &a, &model),
    );
    checks.check(
        "o3.a_local_read_is_served",
        http.send(&a, &Req::get(&format!("/api/model/{model}/state")))
            .and_then(|answer| {
                if answer.status == 200 && answer.etag().is_some() {
                    Ok(format!(
                        "200 with ETag {}",
                        answer.etag().unwrap_or_default()
                    ))
                } else {
                    Err(format!(
                        "answered {} with headers {:?}",
                        answer.status, answer.headers
                    ))
                }
            }),
    );

    // ---- O4: each default origin, as the editor's page speaks ----
    for origin in DEFAULT_ALLOWED {
        let path = format!("/api/model/{model}/op");
        checks.check(
            &format!("o4.{origin}.preflight_grants_post_with_content_type"),
            grants_preflight(&http, &a, &path, origin),
        );
        checks.check(
            &format!("o4.{origin}.a_write_applies_and_is_answered_to_it"),
            applies(
                &http,
                &a,
                &model,
                &Req::post(&path)
                    .origin(origin)
                    .typed("application/json")
                    .body(&json.put("page", 'p')),
                Some(origin),
            ),
        );
        checks.check(
            &format!("o4.{origin}.a_read_is_answered_to_it_with_its_version"),
            readable(&http, &a, &format!("/api/model/{model}/state"), origin),
        );
    }
    let origin = DEFAULT_ALLOWED[0];
    let mut wrong = Vec::new();
    for path in [
        format!("/api/model/{model}/ops"),
        "/api/models".to_string(),
        "/api/metamodels".to_string(),
        "/api/op".to_string(),
    ] {
        if let Err(why) = grants_preflight(&http, &a, &path, origin) {
            wrong.push(format!("{path}: {why}"));
        }
    }
    checks.check(
        "o4.the_preflight_grants_every_write_route_the_editor_uses",
        verdict(wrong, 4),
    );
    checks.check("o4.a_page_on_an_allowed_origin_creates_a_model", {
        let before = Snapshot::read(&http, &a, &model)?;
        http.send(
            &a,
            &Req::post("/api/models")
                .origin(origin)
                .typed("application/json")
                .body(&json!({"metamodel_id": digest}).to_string()),
        )
        .and_then(|answer| {
            let after = Snapshot::read(&http, &a, &model)?;
            match (answer.status, answer.allow_origin()) {
                (201, Ok(Some(echo)))
                    if echo == origin && after.models.len() == before.models.len() + 1 =>
                {
                    Ok(format!("201, echoed, {} models now", after.models.len()))
                }
                (status, echo) => Err(format!(
                    "answered {status}, allow-origin {echo:?}, models {} to {}",
                    before.models.len(),
                    after.models.len()
                )),
            }
        })
    });
    checks.check("o4.a_page_on_an_allowed_origin_adds_a_metamodel", {
        let second = retagged("NW11Second", "http://nw11.test/second");
        let second_digest =
            metamodel_digest(&serde_json::from_str::<Value>(&second).map_err(|e| e.to_string())?);
        http.send(
            &a,
            &Req::post("/api/metamodels")
                .origin(origin)
                .typed("application/json")
                .body(&second),
        )
        .and_then(|answer| {
            let after = Snapshot::read(&http, &a, &model)?;
            match (answer.status, answer.allow_origin()) {
                (200 | 201, Ok(Some(echo)))
                    if echo == origin && after.metamodels.contains(&second_digest) =>
                {
                    Ok(format!("{}, echoed, listed", answer.status))
                }
                (status, echo) => Err(format!(
                    "answered {status}, allow-origin {echo:?}, listed {}",
                    after.metamodels.contains(&second_digest)
                )),
            }
        })
    });

    // ---- O5: no response of this scenario granted every origin ----
    let log = http.log.borrow();
    let star: Vec<&String> = log
        .iter()
        .filter(|(_, acao)| acao.iter().any(|v| v.trim() == "*"))
        .map(|(what, _)| what)
        .collect();
    checks.check(
        "o5.no_response_carries_a_wildcard_origin",
        if star.is_empty() {
            Ok(format!("{} responses, none with `*`", log.len()))
        } else {
            Err(format!(
                "{} of {} responses carry `Access-Control-Allow-Origin: *`, first {:?}",
                star.len(),
                log.len(),
                star.first()
            ))
        },
    );
    let doubled: Vec<&String> = log
        .iter()
        .filter(|(_, acao)| acao.len() > 1)
        .map(|(what, _)| what)
        .collect();
    checks.check(
        "o5.no_response_carries_two_allow_origin_headers",
        if doubled.is_empty() {
            Ok(format!("{} responses", log.len()))
        } else {
            Err(format!(
                "{} responses, first {:?}",
                doubled.len(),
                doubled.first()
            ))
        },
    );
    drop(log);
    drop(b);
    Ok(checks)
}

/// The routes that take a body, with a body each would accept from a local
/// client: a write to the default log and to the model, one and in a batch,
/// a create, a join of an id nobody hosts, and a descriptor not yet held.
fn body_routes(
    json: &Json,
    model: &str,
    digest: &str,
    probe: &str,
) -> Vec<(&'static str, String, String)> {
    vec![
        ("op", "/api/op".to_string(), json.put("foreign", 'f')),
        (
            "ops",
            "/api/ops".to_string(),
            batch(&[json.put("foreign", 'f')]),
        ),
        (
            "model_op",
            format!("/api/model/{model}/op"),
            json.put("foreign", 'f'),
        ),
        (
            "model_ops",
            format!("/api/model/{model}/ops"),
            batch(&[json.put("foreign", 'f')]),
        ),
        (
            "models_create",
            "/api/models".to_string(),
            json!({"metamodel_id": digest}).to_string(),
        ),
        (
            "models_join",
            "/api/models".to_string(),
            json!({"model_id": FRESH_ID, "metamodel_id": digest}).to_string(),
        ),
        (
            "metamodels",
            "/api/metamodels".to_string(),
            probe.to_string(),
        ),
    ]
}

fn note_refusal(
    http: &Http,
    node: &Node,
    req: &Req,
    statuses: &[u16],
    label: &str,
    wrong: &mut Vec<String>,
) {
    match http.send(node, req) {
        Ok(answer) if statuses.contains(&answer.status) && answer.cors().is_empty() => {}
        Ok(answer) => wrong.push(format!(
            "{label}: {} {:?} {}",
            answer.status,
            answer.cors(),
            short(&answer.body)
        )),
        Err(why) => wrong.push(format!("{label}: {why}")),
    }
}

fn note_status(
    http: &Http,
    node: &Node,
    req: &Req,
    statuses: &[u16],
    label: &str,
    wrong: &mut Vec<String>,
) {
    match http.send(node, req) {
        Ok(answer) if statuses.contains(&answer.status) => {}
        Ok(answer) => wrong.push(format!(
            "{label}: {} {}",
            answer.status,
            short(&answer.body)
        )),
        Err(why) => wrong.push(format!("{label}: {why}")),
    }
}

fn verdict(wrong: Vec<String>, per: usize) -> Result<String, String> {
    if wrong.is_empty() {
        Ok(format!("all {per} cases as expected"))
    } else {
        Err(format!(
            "{} wrong: {}",
            wrong.len(),
            wrong
                .iter()
                .take(4)
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ")
        ))
    }
}

fn not_listed(snapshot: &Snapshot, digest: &str) -> Result<String, String> {
    if snapshot.metamodels.iter().any(|held| held == digest) {
        Err(format!("the probe descriptor {} is held", short(digest)))
    } else {
        Ok(format!("{} not held", short(digest)))
    }
}

/// A write that must apply: 200 with `success: true` (or `outcome:
/// "applied"`), the model's version moved, and when `origin` is given, that
/// origin echoed and nothing else.
fn applies(
    http: &Http,
    node: &Node,
    model: &str,
    req: &Req,
    origin: Option<&str>,
) -> Result<String, String> {
    let before = http
        .send(node, &Req::get(&format!("/api/model/{model}/state")))?
        .etag();
    let answer = http.send(node, req)?;
    let ok = answer.status == 200
        && (answer.json()["success"] == json!(true)
            || answer.json()["outcome"] == json!("applied"));
    if !ok {
        return Err(format!(
            "answered {}: {}",
            answer.status,
            short(&answer.body)
        ));
    }
    if let Some(origin) = origin {
        match answer.allow_origin() {
            Ok(Some(echo)) if echo == origin => {}
            other => return Err(format!("200, but allow-origin {other:?} for {origin}")),
        }
    }
    let after = http
        .send(node, &Req::get(&format!("/api/model/{model}/state")))?
        .etag();
    if before == after {
        return Err(format!("200, but the version stayed {before:?}"));
    }
    Ok(format!("200, version {before:?} to {after:?}"))
}

fn grants_preflight(http: &Http, node: &Node, path: &str, origin: &str) -> Result<String, String> {
    let answer = http.send(node, &Req::preflight(path, origin))?;
    if !(200..300).contains(&answer.status) {
        return Err(format!("answered {}", answer.status));
    }
    match answer.allow_origin() {
        Ok(Some(echo)) if echo == origin => {}
        other => return Err(format!("allow-origin {other:?}")),
    }
    let methods = answer
        .header("Access-Control-Allow-Methods")
        .unwrap_or_default();
    let headers = answer
        .header("Access-Control-Allow-Headers")
        .unwrap_or_default();
    if !has_token(methods, "POST") {
        return Err(format!("allow-methods `{methods}` lacks POST"));
    }
    if !has_token(headers, "content-type") {
        return Err(format!("allow-headers `{headers}` lacks Content-Type"));
    }
    Ok(format!(
        "{} echoed, methods `{methods}`, headers `{headers}`",
        answer.status
    ))
}

fn readable(http: &Http, node: &Node, path: &str, origin: &str) -> Result<String, String> {
    let answer = http.send(node, &Req::get(path).origin(origin))?;
    if answer.status != 200 || answer.etag().is_none() {
        return Err(format!(
            "answered {} with headers {:?}",
            answer.status, answer.headers
        ));
    }
    match answer.allow_origin() {
        Ok(Some(echo)) if echo == origin => {}
        other => return Err(format!("allow-origin {other:?}")),
    }
    let exposed = answer
        .header("Access-Control-Expose-Headers")
        .unwrap_or_default();
    if !has_token(exposed, "etag") {
        return Err(format!("expose-headers `{exposed}` lacks ETag"));
    }
    Ok(format!("200 echoed, exposes `{exposed}`"))
}

/// `POST /api/pause-all` and `/api/resume-all` with no body and no
/// `Content-Type`, as Node's `fetch` sends them (the ED.16b e2e does).
fn local_bodyless_pause(http: &Http, node: &Node, model: &str) -> Result<String, String> {
    let paused = http.send(node, &Req::post("/api/pause-all"))?;
    let peers = Snapshot::read(http, node, model)?.peers;
    let resumed = http.send(node, &Req::post("/api/resume-all"))?;
    let back = wait_until("`a` to see `b` connected again", STEP_TIMEOUT, || {
        let peers = Snapshot::read(http, node, model)?.peers;
        Ok(peers
            .iter()
            .all(|(_, status)| status == "Connected")
            .then_some(peers))
    });
    if paused.status == 200
        && peers.iter().all(|(_, status)| status == "Paused")
        && resumed.status == 200
        && back.is_ok()
    {
        Ok(format!(
            "pause-all 200 left {peers:?}, resume-all 200 reconnected"
        ))
    } else {
        Err(format!(
            "pause-all {} left {peers:?}; resume-all {}; reconnected {:?}",
            paused.status,
            resumed.status,
            back.err()
        ))
    }
}

// ---------------------------------------------------------------------------
// C, a configured list
// ---------------------------------------------------------------------------

fn c(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new("C");
    let scratch = scratch_for(name)?;
    let http = Http::new();
    let json = Json::new();
    let node = single(
        binary,
        &scratch,
        &format!("{name}-c"),
        &[(ORIGINS_VAR, CONFIGURED.to_string())],
        &http,
    )?;
    let digest = metamodel_digest(
        &serde_json::from_str::<Value>(JSON_DESCRIPTOR).map_err(|e| e.to_string())?,
    );
    let model = http.create(&node, &digest)?;
    let path = format!("/api/model/{model}/op");

    for origin in CONFIGURED_ALLOWED {
        checks.check(
            &format!("c1.{origin}.preflight_is_granted"),
            grants_preflight(&http, &node, &path, origin),
        );
        checks.check(
            &format!("c1.{origin}.a_write_applies_and_is_answered_to_it"),
            applies(
                &http,
                &node,
                &model,
                &Req::post(&path)
                    .origin(origin)
                    .typed("application/json")
                    .body(&json.put("allowed", 'a')),
                Some(origin),
            ),
        );
    }
    let before = http.send(&node, &Req::get(&format!("/api/model/{model}/state")))?;
    for origin in CONFIGURED_REFUSED {
        let mut wrong = Vec::new();
        note_refusal(
            &http,
            &node,
            &Req::post(&path)
                .origin(origin)
                .typed("application/json")
                .body(&json.put("refused", 'r')),
            &[403],
            "json write",
            &mut wrong,
        );
        match http.send(&node, &Req::preflight(&path, origin)) {
            Ok(answer) if answer.cors().is_empty() => {}
            Ok(answer) => wrong.push(format!("preflight granted {:?}", answer.cors())),
            Err(why) => wrong.push(format!("preflight: {why}")),
        }
        checks.check(
            &format!("c2.{origin}.is_refused_and_granted_nothing"),
            verdict(wrong, 2),
        );
    }
    std::thread::sleep(SETTLE);
    let after = http.send(&node, &Req::get(&format!("/api/model/{model}/state")))?;
    checks.check(
        "c2.nothing_changed",
        if before.etag() == after.etag() && before.json() == after.json() {
            Ok(format!("version {:?}", after.etag()))
        } else {
            Err(format!(
                "version {:?} became {:?}",
                before.etag(),
                after.etag()
            ))
        },
    );
    let log = http.log.borrow();
    checks.check(
        "c3.no_response_carries_a_wildcard_origin",
        if log
            .iter()
            .any(|(_, acao)| acao.iter().any(|v| v.trim() == "*"))
        {
            Err("a response carries `Access-Control-Allow-Origin: *`".to_string())
        } else {
            Ok(format!("{} responses", log.len()))
        },
    );
    Ok(checks)
}

// ---------------------------------------------------------------------------
// S, the stack's own requests
// ---------------------------------------------------------------------------

fn s(checks: &mut Checks) {
    let compose = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../docker/compose");
    let mut files: Vec<PathBuf> = match fs::read_dir(&compose) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.is_file()
                    && matches!(
                        path.extension().and_then(|ext| ext.to_str()),
                        Some("sh" | "yml" | "yaml" | "md")
                    )
            })
            .collect(),
        Err(err) => panic!("NW11 S PREMISE failed: {}: {err}", compose.display()),
    };
    files.sort();
    let mut typed = Vec::new();
    let mut untyped = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).unwrap_or_default();
        for command in curl_commands(&text) {
            let tokens: Vec<&str> = command.split_whitespace().collect();
            let sends_body = tokens.iter().any(|token| {
                *token == "-d"
                    || token.starts_with("-d'")
                    || token.starts_with("-d\"")
                    || token.starts_with("--data")
                    || *token == "-F"
                    || token.starts_with("--form")
                    || *token == "--json"
                    || *token == "-T"
                    || *token == "--upload-file"
            });
            if !sends_body {
                continue;
            }
            let declared = tokens.contains(&"--json")
                || command
                    .to_ascii_lowercase()
                    .contains("content-type: application/json");
            let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            let entry = format!("{name}: {}", short(&command));
            if declared {
                typed.push(entry)
            } else {
                untyped.push(entry)
            }
        }
    }
    if typed.is_empty() {
        panic!(
            "NW11 S PREMISE failed: no typed `curl` with a body found in {} (register-models.sh \
             has one), so the scanner reads nothing",
            compose.display()
        );
    }
    checks.check(
        "s1.every_compose_curl_with_a_body_declares_json",
        if untyped.is_empty() {
            Ok(format!("{} typed: {}", typed.len(), typed.join(" | ")))
        } else {
            Err(format!(
                "{} untyped: {}",
                untyped.len(),
                untyped.join(" | ")
            ))
        },
    );
    let replica = fs::read_to_string(compose.join("replica.yml")).unwrap_or_default();
    let line = replica
        .lines()
        .map(str::trim)
        .find(|line| !line.starts_with('#') && line.starts_with(&format!("{ORIGINS_VAR}:")));
    checks.check(
        "s2.replica_yml_lets_the_host_set_the_allow_list",
        match line {
            Some(line) if line.contains(&format!("${{{ORIGINS_VAR}")) => Ok(line.to_string()),
            Some(line) => Err(format!("`{line}` does not take the host's {ORIGINS_VAR}")),
            None => Err(format!("replica.yml sets no {ORIGINS_VAR}")),
        },
    );
}

/// Every `curl` command of a file, comment markers removed and continuation
/// lines joined: a command written in a comment is documentation someone will
/// paste.
fn curl_commands(text: &str) -> Vec<String> {
    let mut logical = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        let uncommented = trimmed
            .strip_prefix('#')
            .map(str::trim_start)
            .unwrap_or(trimmed);
        match uncommented.strip_suffix('\\') {
            Some(head) => {
                current.push_str(head);
                current.push(' ');
            }
            None => {
                current.push_str(uncommented);
                logical.push(std::mem::take(&mut current));
            }
        }
    }
    if !current.is_empty() {
        logical.push(current);
    }
    logical
        .into_iter()
        .filter_map(|line| {
            let at = line.find("curl ")?;
            // The command ends where the shell starts another one: a pipe, a
            // `;` or an `&&`, so `| cut -d'"'` is not read as curl's `-d`.
            let command = &line[at..];
            let end = ["|", ";", "&&"]
                .iter()
                .filter_map(|stop| command.find(stop))
                .min()
                .unwrap_or(command.len());
            Some(command[..end].to_string())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The node's state, read back
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct Snapshot {
    default_state: Value,
    default_version: Option<String>,
    model_state: Value,
    model_version: Option<String>,
    models: Vec<String>,
    metamodels: Vec<String>,
    peers: Vec<(String, String)>,
}

impl Snapshot {
    fn read(http: &Http, node: &Node, model: &str) -> Result<Snapshot, String> {
        let default = http.send(node, &Req::get("/api/state"))?;
        let state = http.send(node, &Req::get(&format!("/api/model/{model}/state")))?;
        let models = http.send(node, &Req::get("/api/models"))?.json();
        let metamodels = http.send(node, &Req::get("/api/metamodels"))?.json();
        let peers = http.send(node, &Req::get("/api/peers"))?.json();
        let mut models: Vec<String> = models["models"]
            .as_array()
            .map(|list| {
                list.iter()
                    .filter_map(|m| m["model_id"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        models.sort();
        let mut metamodels: Vec<String> = metamodels["metamodels"]
            .as_array()
            .map(|list| {
                list.iter()
                    .filter_map(|m| m["digest"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        metamodels.sort();
        let mut peers: Vec<(String, String)> = peers["peers"]
            .as_array()
            .map(|list| {
                list.iter()
                    .map(|p| {
                        (
                            p["id"].as_str().unwrap_or_default().to_string(),
                            p["status"].as_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        peers.sort();
        Ok(Snapshot {
            default_state: default.json(),
            default_version: default.etag(),
            model_state: state.json(),
            model_version: state.etag(),
            models,
            metamodels,
            peers,
        })
    }

    fn compare(&self, after: &Snapshot, prefix: &str, checks: &mut Checks) {
        let same = |what: &str, a: String, b: String| {
            if a == b {
                Ok(what.to_string() + " unchanged")
            } else {
                Err(format!("{what}: {} became {}", short(&a), short(&b)))
            }
        };
        checks.check(
            &format!("{prefix}.default_log"),
            same(
                "state and version",
                format!("{:?} {}", self.default_version, self.default_state),
                format!("{:?} {}", after.default_version, after.default_state),
            ),
        );
        checks.check(
            &format!("{prefix}.model"),
            same(
                "state and version",
                format!("{:?} {}", self.model_version, self.model_state),
                format!("{:?} {}", after.model_version, after.model_state),
            ),
        );
        checks.check(
            &format!("{prefix}.hosted_models"),
            same(
                "hosted models",
                format!("{:?}", self.models),
                format!("{:?}", after.models),
            ),
        );
        checks.check(
            &format!("{prefix}.metamodels"),
            same(
                "held metamodels",
                format!("{:?}", self.metamodels),
                format!("{:?}", after.metamodels),
            ),
        );
        checks.check(
            &format!("{prefix}.peers"),
            same(
                "peers",
                format!("{:?}", self.peers),
                format!("{:?}", after.peers),
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// Raw HTTP, so every header is the one this file wrote
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Req {
    method: &'static str,
    path: String,
    origin: Option<String>,
    content_type: Option<String>,
    extra: Vec<(String, String)>,
    body: Option<String>,
}

impl Req {
    fn get(path: &str) -> Req {
        Req {
            method: "GET",
            path: path.to_string(),
            origin: None,
            content_type: None,
            extra: Vec::new(),
            body: None,
        }
    }

    fn post(path: &str) -> Req {
        Req {
            method: "POST",
            ..Req::get(path)
        }
    }

    /// The preflight Chrome sends before a `POST` with a JSON body.
    fn preflight(path: &str, origin: &str) -> Req {
        let mut req = Req {
            method: "OPTIONS",
            ..Req::get(path)
        }
        .origin(origin);
        req.extra
            .push(("Access-Control-Request-Method".into(), "POST".into()));
        req.extra.push((
            "Access-Control-Request-Headers".into(),
            "content-type".into(),
        ));
        req
    }

    fn origin(mut self, origin: &str) -> Req {
        self.origin = Some(origin.to_string());
        self
    }

    fn typed(mut self, content_type: &str) -> Req {
        self.content_type = Some(content_type.to_string());
        self
    }

    fn body(mut self, body: &str) -> Req {
        self.body = Some(body.to_string());
        self
    }

    fn describe(&self) -> String {
        format!(
            "{} {} origin={:?} type={:?} body={}",
            self.method,
            self.path,
            self.origin,
            self.content_type,
            self.body.as_ref().map_or(0, String::len)
        )
    }
}

#[derive(Debug, Clone)]
struct Answer {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn all(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect()
    }

    /// Every `Access-Control-*` header, as `name: value`.
    fn cors(&self) -> Vec<String> {
        self.headers
            .iter()
            .filter(|(key, _)| key.to_ascii_lowercase().starts_with("access-control-"))
            .map(|(key, value)| format!("{key}: {value}"))
            .collect()
    }

    /// The one `Access-Control-Allow-Origin`, `None` without one, an error
    /// with two.
    fn allow_origin(&self) -> Result<Option<&str>, String> {
        match self.all("Access-Control-Allow-Origin").as_slice() {
            [] => Ok(None),
            [one] => Ok(Some(one)),
            many => Err(format!("{} allow-origin headers: {many:?}", many.len())),
        }
    }

    fn etag(&self) -> Option<String> {
        self.header("ETag").map(str::to_string)
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::String(self.body.clone()))
    }
}

struct Http {
    /// Every exchange of a scenario and the allow-origin values it was
    /// answered with, for O5.
    log: RefCell<Vec<(String, Vec<String>)>>,
}

impl Http {
    fn new() -> Http {
        Http {
            log: RefCell::new(Vec::new()),
        }
    }

    fn send(&self, node: &Node, req: &Req) -> Result<Answer, String> {
        self.send_to("127.0.0.1", node.http, req)
    }

    fn send_to(&self, host: &str, port: u16, req: &Req) -> Result<Answer, String> {
        let addr: SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|err| format!("{err}"))?;
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            .map_err(|err| format!("connect {addr}: {:?} {err}", err.kind()))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .map_err(|e| e.to_string())?;
        let body = req.body.clone().unwrap_or_default();
        let mut head = format!(
            "{} {} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n",
            req.method, req.path
        );
        if let Some(origin) = &req.origin {
            head.push_str(&format!("Origin: {origin}\r\n"));
        }
        if let Some(content_type) = &req.content_type {
            head.push_str(&format!("Content-Type: {content_type}\r\n"));
        }
        for (key, value) in &req.extra {
            head.push_str(&format!("{key}: {value}\r\n"));
        }
        if (req.method != "GET" && req.method != "OPTIONS") || !body.is_empty() {
            head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        head.push_str("\r\n");
        stream
            .write_all(head.as_bytes())
            .and_then(|()| stream.write_all(body.as_bytes()))
            .map_err(|err| format!("{}: writing: {err}", req.describe()))?;
        let mut raw = Vec::new();
        if let Err(err) = stream.read_to_end(&mut raw)
            && raw.is_empty()
        {
            return Err(format!(
                "{}: reading: {:?} {err}",
                req.describe(),
                err.kind()
            ));
        }
        let text = String::from_utf8_lossy(&raw).into_owned();
        let (head, rest) = text.split_once("\r\n\r\n").ok_or_else(|| {
            format!(
                "{}: no header/body separator in {:?}",
                req.describe(),
                short(&text)
            )
        })?;
        let mut lines = head.lines();
        let status: u16 = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| format!("{}: no status line in {:?}", req.describe(), short(head)))?;
        let headers: Vec<(String, String)> = lines
            .filter_map(|line| {
                line.split_once(':')
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            })
            .collect();
        let chunked = headers.iter().any(|(k, v)| {
            k.eq_ignore_ascii_case("Transfer-Encoding")
                && v.to_ascii_lowercase().contains("chunked")
        });
        let body = if chunked {
            dechunk(rest)
        } else {
            rest.to_string()
        };
        let answer = Answer {
            status,
            headers,
            body,
        };
        self.log.borrow_mut().push((
            req.describe(),
            answer
                .all("Access-Control-Allow-Origin")
                .into_iter()
                .map(str::to_string)
                .collect(),
        ));
        Ok(answer)
    }

    /// A write as a local client sends it: no `Origin`, a JSON body.
    fn local_write(&self, node: &Node, path: &str, body: &str) -> Result<Answer, String> {
        let answer = self.send(node, &Req::post(path).typed("application/json").body(body))?;
        let applied =
            answer.json()["success"] == json!(true) || answer.json()["outcome"] == json!("applied");
        if answer.status != 200 || !applied {
            return Err(format!(
                "set-up write to {path} answered {}: {}",
                answer.status,
                short(&answer.body)
            ));
        }
        Ok(answer)
    }

    /// A control route as set-up: no `Origin`, an empty body declared JSON
    /// (as `ed20`'s harness sends it), so no set-up step leans on the
    /// bodyless case, which O3 checks on its own.
    fn local_control(&self, node: &Node, path: &str) -> Result<(), String> {
        let answer = self.send(node, &Req::post(path).typed("application/json"))?;
        if answer.status != 200 || answer.json()["success"] != json!(true) {
            return Err(format!(
                "set-up {path} answered {}: {}",
                answer.status,
                short(&answer.body)
            ));
        }
        Ok(())
    }

    fn create(&self, node: &Node, digest: &str) -> Result<String, String> {
        let answer = self.send(
            node,
            &Req::post("/api/models")
                .typed("application/json")
                .body(&json!({"metamodel_id": digest}).to_string()),
        )?;
        if answer.status != 201 {
            return Err(format!(
                "set-up create answered {}: {}",
                answer.status,
                short(&answer.body)
            ));
        }
        answer.json()["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {}", answer.body))
    }

    fn await_health(&self, node: &Node) -> Result<(), String> {
        wait_until(
            &format!("{} to answer /api/health", node.id),
            HEALTH_TIMEOUT,
            || match self.send(node, &Req::get("/api/health")) {
                Ok(answer)
                    if answer.status == 200 && answer.json()["replica_id"] == json!(node.id) =>
                {
                    Ok(Some(()))
                }
                Ok(answer) if answer.status == 200 => {
                    Err(format!("port {} answered as {}", node.http, answer.body))
                }
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

    fn await_loop(&self, node: &Node) -> Result<(), String> {
        wait_until(
            &format!("{} to enter its event loop", node.id),
            STEP_TIMEOUT,
            || {
                Ok(fs::read_to_string(&node.log)
                    .unwrap_or_default()
                    .contains("Entering main event loop")
                    .then_some(()))
            },
        )
    }
}

fn dechunk(raw: &str) -> String {
    let mut out = String::new();
    let mut rest = raw;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size.trim(), 16) else {
            break;
        };
        if size == 0 || tail.len() < size {
            break;
        }
        out.push_str(&tail[..size]);
        rest = tail[size..].trim_start_matches("\r\n");
    }
    out
}

fn has_token(list: &str, token: &str) -> bool {
    list.split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(token))
}

fn short(text: &str) -> String {
    let flat: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if flat.chars().count() > 160 {
        flat.chars().take(160).collect::<String>() + "..."
    } else {
        flat
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
            "no `model_node` at {}; build it with `cargo build -p moirai-interp --features network \
             --example model_node`",
            built.display()
        ))
    }
}

/// A fresh scratch directory for one scenario, with the JSON descriptor in
/// `metamodels/`.
fn scratch_for(name: &str) -> Result<PathBuf, String> {
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let root = std::env::var_os("NW11_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let scratch = root.join(format!("moirai-{name}-{}-{run}", std::process::id()));
    let dir = scratch.join("metamodels");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    fs::write(dir.join("json.json"), JSON_DESCRIPTOR).map_err(|err| err.to_string())?;
    Ok(scratch)
}

struct Node {
    id: String,
    http: u16,
    child: Child,
    log: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

enum Launch {
    /// `HTTP_PORT=0` and `PORTS_FILE`: the listener is bound before the API,
    /// and with `peers_file` the peers come from `PEERS_FILE`.
    PortsFile { peers_file: bool },
    /// A fixed `HTTP_PORT` and no `PORTS_FILE`: the port is bound inside the
    /// API's thread.
    Fixed(u16),
}

struct Spawned {
    child: Child,
    log: PathBuf,
    ports: Option<PathBuf>,
    peers: Option<PathBuf>,
}

/// One `model_node` process. Every variable this item reads is removed from
/// the inherited environment first, so a scenario's node sees only what the
/// scenario gives it.
fn spawn(
    binary: &Path,
    scratch: &Path,
    id: &str,
    env: &[(&str, String)],
    how: Launch,
) -> Result<Spawned, String> {
    let log = scratch.join(format!("{id}.log"));
    let out = fs::File::create(&log).map_err(|err| err.to_string())?;
    let err_out = out.try_clone().map_err(|err| err.to_string())?;
    let mut command = Command::new(binary);
    for var in [
        "PEERS",
        "PEERS_FILE",
        "PORTS_FILE",
        "BOOTNODE_URL",
        "DASHBOARD_URL",
        "METAMODEL_PATH",
        "ADVERTISE_ADDR",
        "LISTEN_PORT",
        "HTTP_PORT",
        BIND_VAR,
        ORIGINS_VAR,
    ] {
        command.env_remove(var);
    }
    command
        .current_dir(scratch)
        .env("REPLICA_ID", id)
        .env("LISTEN_PORT", "0")
        .env("LOG_ID", LOG_ID)
        .env("METAMODEL_DIR", scratch.join("metamodels"));
    let (ports, peers) = match how {
        Launch::PortsFile { peers_file } => {
            let ports = scratch.join(format!("{id}.ports.json"));
            let _ = fs::remove_file(&ports);
            command.env("HTTP_PORT", "0").env("PORTS_FILE", &ports);
            let peers = if peers_file {
                let peers = scratch.join(format!("{id}.peers"));
                let _ = fs::remove_file(&peers);
                command.env("PEERS_FILE", &peers);
                Some(peers)
            } else {
                command.env("PEERS", "");
                None
            };
            (Some(ports), peers)
        }
        Launch::Fixed(port) => {
            command.env("HTTP_PORT", port.to_string()).env("PEERS", "");
            (None, None)
        }
    };
    for (key, value) in env {
        command.env(key, value);
    }
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err_out))
        .spawn()
        .map_err(|err| format!("spawning {id}: {err}"))?;
    Ok(Spawned {
        child,
        log,
        ports,
        peers,
    })
}

/// Wait for a node's ports file to name its replica id and pid.
fn await_ports(id: &str, spawned: &mut Spawned) -> Result<launch::Reported, String> {
    let path = spawned.ports.clone().expect("a ports-file launch");
    wait_until(&format!("{id} to report its ports"), HEALTH_TIMEOUT, || {
        if let Some(parsed) = launch::read_ports_file(&path) {
            let reported = parsed?;
            if reported.replica_id != id || reported.pid != spawned.child.id() {
                return Err(format!(
                    "{id}'s ports file names another process: {reported:?}"
                ));
            }
            return Ok(Some(reported));
        }
        if let Ok(Some(status)) = spawned.child.try_wait() {
            return Err(format!(
                "{id} exited ({status}); alarms:\n{}",
                launch::alarms(&spawned.log)
            ));
        }
        Ok(None)
    })
}

/// One node on the ports-file path with no peer, healthy and in its loop.
fn single(
    binary: &Path,
    scratch: &Path,
    id: &str,
    env: &[(&str, String)],
    http: &Http,
) -> Result<Node, String> {
    let mut spawned = spawn(
        binary,
        scratch,
        id,
        env,
        Launch::PortsFile { peers_file: false },
    )?;
    let reported = match await_ports(id, &mut spawned) {
        Ok(reported) => reported,
        Err(why) => {
            let _ = spawned.child.kill();
            let _ = spawned.child.wait();
            return Err(why);
        }
    };
    let node = Node {
        id: id.to_string(),
        http: reported.http_port.unwrap_or(0),
        child: spawned.child,
        log: spawned.log,
    };
    http.await_health(&node)?;
    http.await_loop(&node)?;
    Ok(node)
}

/// One node on the fixed-port path with no peer, healthy and in its loop.
fn fixed(
    binary: &Path,
    scratch: &Path,
    id: &str,
    env: &[(&str, String)],
    http: &Http,
) -> Result<Node, String> {
    let port = pick_port()?;
    let spawned = spawn(binary, scratch, id, env, Launch::Fixed(port))?;
    let node = Node {
        id: id.to_string(),
        http: port,
        child: spawned.child,
        log: spawned.log,
    };
    http.await_health(&node)?;
    http.await_loop(&node)?;
    Ok(node)
}

/// `a` and `b`, each the other's only peer, healthy, connected, in their
/// loops.
fn pair(binary: &Path, scratch: &Path, http: &Http) -> Result<(Node, Node), String> {
    let mut a = spawn(
        binary,
        scratch,
        "a",
        &[],
        Launch::PortsFile { peers_file: true },
    )?;
    let mut b = spawn(
        binary,
        scratch,
        "b",
        &[],
        Launch::PortsFile { peers_file: true },
    )?;
    let ports_a = await_ports("a", &mut a);
    let ports_b = await_ports("b", &mut b);
    let (ports_a, ports_b) = match (ports_a, ports_b) {
        (Ok(pa), Ok(pb)) => (pa, pb),
        (pa, pb) => {
            for spawned in [&mut a, &mut b] {
                let _ = spawned.child.kill();
                let _ = spawned.child.wait();
            }
            return Err(format!("{:?} / {:?}", pa.err(), pb.err()));
        }
    };
    launch::write_atomically(
        a.peers.as_ref().expect("peers file"),
        &format!("b:127.0.0.1:{}", ports_b.listen_port),
    )?;
    launch::write_atomically(
        b.peers.as_ref().expect("peers file"),
        &format!("a:127.0.0.1:{}", ports_a.listen_port),
    )?;
    let a = Node {
        id: "a".into(),
        http: ports_a.http_port.unwrap_or(0),
        child: a.child,
        log: a.log,
    };
    let b = Node {
        id: "b".into(),
        http: ports_b.http_port.unwrap_or(0),
        child: b.child,
        log: b.log,
    };
    for node in [&a, &b] {
        http.await_health(node)?;
    }
    wait_until("a and b to see each other", STEP_TIMEOUT, || {
        Ok([&a, &b]
            .iter()
            .all(|node| {
                http.send(node, &Req::get("/api/peers"))
                    .map(|answer| {
                        answer.json()["peers"].as_array().is_some_and(|peers| {
                            peers
                                .iter()
                                .filter(|p| p["status"] == json!("Connected"))
                                .count()
                                == 1
                        })
                    })
                    .unwrap_or(false)
            })
            .then_some(()))
    })?;
    for node in [&a, &b] {
        http.await_loop(node)?;
    }
    Ok((a, b))
}

/// A port nobody listens on, for the fixed-port path, which cannot be told
/// port 0. Released before the node binds it; on loopback the window is
/// negligible, and a node that lost the race fails its health wait, which is
/// reported as a premise failure, never as a pass.
fn pick_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    let port = listener.local_addr().map_err(|err| err.to_string())?.port();
    drop(listener);
    Ok(port)
}

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
// Operations and descriptors
// ---------------------------------------------------------------------------

/// `json.ecore`'s one operation this file writes: a character inserted at
/// the head of a string under a key of the root object.
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

    /// The serialized operation `{key: "<ch>..."}` on the root object.
    fn put(&self, key: &str, ch: char) -> String {
        let string = self.is(
            "String",
            self.at(
                "String",
                "value",
                InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch }),
            ),
        );
        let op: ModelOp = self
            .is(
                "Object",
                self.at(
                    "Object",
                    "entry",
                    InstanceOp::entry(Scalar::text(key), string),
                ),
            )
            .into_model_op();
        serde_json::to_string(&op).expect("an operation serializes")
    }
}

fn batch(ops: &[String]) -> String {
    format!("{{\"ops\":[{}]}}", ops.join(","))
}

/// `json.ecore` under a package and an nsURI of this file's choosing: a
/// descriptor the node does not hold until someone posts it.
fn retagged(package: &str, ns_uri: &str) -> String {
    let mut value: Value = serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON");
    value["package"] = json!(package);
    value["nsURI"] = json!(ns_uri);
    serde_json::to_string(&value).expect("a descriptor serializes")
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

struct Checks {
    scenario: &'static str,
    results: Vec<(String, bool)>,
}

impl Checks {
    fn new(scenario: &'static str) -> Self {
        Checks {
            scenario,
            results: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, result: Result<String, String>) -> bool {
        let pass = result.is_ok();
        match result {
            Ok(detail) => println!("NW11-{} PASS {name}: {detail}", self.scenario),
            Err(detail) => println!("NW11-{} FAIL {name}: {detail}", self.scenario),
        }
        self.results.push((name.to_string(), pass));
        pass
    }

    fn finish(self) {
        let failed: Vec<&str> = self
            .results
            .iter()
            .filter(|(_, pass)| !pass)
            .map(|(name, _)| name.as_str())
            .collect();
        println!(
            "NW11-{} SUMMARY {} checks, {} passed, {} failed",
            self.scenario,
            self.results.len(),
            self.results.len() - failed.len(),
            failed.len()
        );
        assert!(
            failed.is_empty(),
            "NW11-{}: {} of {} checks failed: {}",
            self.scenario,
            failed.len(),
            self.results.len(),
            failed.join(", ")
        );
    }
}
