//! NW.19, P5's checks over HTTP (vault `spec/items/NW.19.md`, section 7): not a
//! test, a program `tests/nw19/stack.sh` runs once per step against replicas of
//! the stack it has started, through the one ignored test below:
//!
//! ```text
//! <this test binary> --ignored --exact step --nocapture
//! ```
//!
//! with `NW19_STEP`, `NW19_NODES` (comma-separated `name=http://127.0.0.1:port`),
//! `NW19_STATE` (a JSON file the steps share: the model's id and the class
//! names written so far), and for some steps `NW19_ARG` and `NW19_TAG`.
//! Every step prints `NW19-STACK <step> PASS ...` or panics with
//! `NW19-STACK <step> FAIL ...`; `NW19_TIMEOUT_S` bounds its waits (30 s).
//!
//! - `mesh`: every node lists every other node of `NW19_NODES` `Connected`.
//! - `register`: the SimpleUML descriptor (this crate's fixture) is posted to
//!   the first node's `/api/metamodels`, a model is created there and joined by
//!   id on every other node; every node holds the model's table.
//! - `join`: the node named by `NW19_ARG` joins the model by id.
//! - `write`: `NW19_ARG` classes written on every node, named
//!   `<NW19_TAG>-<node>-<i>`, each at the head of `Model.ownedElements`.
//! - `converge`: every node serves exactly the class names written so far, and
//!   the same document.
//! - `disconnected`: every node reads the replica named by `NW19_ARG`
//!   `Disconnected` (or no longer lists it).
//!
//! Without `NW19_STEP` the test does nothing.
#![cfg(feature = "network")]

mod nw16a;

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct Ctx {
    step: String,
    nodes: Vec<(String, String)>,
    state: PathBuf,
    timeout: Duration,
    http: reqwest::blocking::Client,
}

impl Ctx {
    fn fail(&self, why: impl std::fmt::Display) -> ! {
        panic!("NW19-STACK {} FAIL {why}", self.step)
    }

    fn get(&self, base: &str, path: &str) -> Result<(u16, Value), String> {
        let resp = self
            .http
            .get(format!("{base}{path}"))
            .send()
            .map_err(|e| format!("GET {base}{path}: {e}"))?;
        let status = resp.status().as_u16();
        let text = resp.text().unwrap_or_default();
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    fn post(&self, base: &str, path: &str, body: String) -> Result<(u16, Value), String> {
        let resp = self
            .http
            .post(format!("{base}{path}"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .map_err(|e| format!("POST {base}{path}: {e}"))?;
        let status = resp.status().as_u16();
        let text = resp.text().unwrap_or_default();
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    fn base(&self, name: &str) -> &str {
        self.nodes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, b)| b.as_str())
            .unwrap_or_else(|| self.fail(format!("no node {name} in NW19_NODES")))
    }

    fn load(&self) -> Value {
        fs::read_to_string(&self.state)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_else(|| json!({}))
    }

    fn save(&self, state: &Value) {
        fs::write(&self.state, state.to_string())
            .unwrap_or_else(|e| self.fail(format!("writing {}: {e}", self.state.display())));
    }

    fn model(&self) -> String {
        self.load()["model_id"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| self.fail("no model registered yet (NW19_STATE has no model_id)"))
    }

    /// Polls `check` until it answers `Ok`, or fails the step with its last
    /// answer.
    fn until(&self, what: &str, mut check: impl FnMut() -> Result<String, String>) -> String {
        let deadline = Instant::now() + self.timeout;
        loop {
            match check() {
                Ok(done) => return done,
                Err(why) if Instant::now() > deadline => {
                    self.fail(format!("{what} not within {:?}: {why}", self.timeout))
                }
                Err(_) => thread::sleep(nw16a::POLL),
            }
        }
    }

    fn peer_status(&self, base: &str) -> Result<Vec<(String, String)>, String> {
        let (status, peers) = self.get(base, "/api/peers")?;
        if status != 200 {
            return Err(format!("/api/peers answered {status}: {peers}"));
        }
        Ok(peers["peers"]
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
            .unwrap_or_default())
    }
}

fn mesh(ctx: &Ctx) {
    let done = ctx.until("every node lists every other Connected", || {
        let mut lines = Vec::new();
        for (name, base) in &ctx.nodes {
            let peers = ctx.peer_status(base)?;
            let connected: BTreeSet<&str> = peers
                .iter()
                .filter(|(_, s)| s == "Connected")
                .map(|(id, _)| id.as_str())
                .collect();
            let missing: Vec<&str> = ctx
                .nodes
                .iter()
                .map(|(n, _)| n.as_str())
                .filter(|n| n != name && !connected.contains(n))
                .collect();
            if !missing.is_empty() {
                return Err(format!("{name} lists {peers:?}, missing {missing:?}"));
            }
            lines.push(format!("{name}: {peers:?}"));
        }
        Ok(lines.join("; "))
    });
    println!("NW19-STACK mesh PASS {done}");
}

fn join_on(ctx: &Ctx, name: &str, model: &str) {
    let body = json!({ "model_id": model, "metamodel_id": nw16a::digest(nw16a::UML) });
    match ctx.post(ctx.base(name), "/api/models", body.to_string()) {
        Ok((status, _)) if (200..300).contains(&status) => {}
        Ok((status, answer)) => ctx.fail(format!(
            "joining {model} on {name} answered {status}: {answer}"
        )),
        Err(e) => ctx.fail(e),
    }
    let base = ctx.base(name).to_string();
    ctx.until(&format!("{name} holds the table of {model}"), || match ctx
        .get(&base, &format!("/api/model/{model}/metrics"))?
    {
        (200, m) if m["delivered_ops"].as_u64().unwrap_or(0) >= 1 => Ok(String::new()),
        (status, m) => Err(format!("{status}: {m}")),
    });
}

fn register(ctx: &Ctx) {
    let (first, base) = ctx.nodes[0].clone();
    match ctx.post(&base, "/api/metamodels", nw16a::UML.to_string()) {
        Ok((status, _)) if (200..300).contains(&status) => {}
        Ok((status, answer)) => ctx.fail(format!(
            "posting the descriptor to {first} answered {status}: {answer}"
        )),
        Err(e) => ctx.fail(e),
    }
    let body = json!({ "metamodel_id": nw16a::digest(nw16a::UML) });
    let model = match ctx.post(&base, "/api/models", body.to_string()) {
        Ok((status, answer)) if (200..300).contains(&status) => answer["model_id"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| ctx.fail(format!("no model id in {answer}"))),
        Ok((status, answer)) => {
            ctx.fail(format!("creating on {first} answered {status}: {answer}"))
        }
        Err(e) => ctx.fail(e),
    };
    for (name, _) in ctx.nodes.iter().skip(1) {
        join_on(ctx, name, &model);
    }
    ctx.save(&json!({ "model_id": model, "names": [] }));
    println!("NW19-STACK register PASS model {model} created on {first}, joined on the others");
}

fn write(ctx: &Ctx, count: usize, tag: &str) {
    let model = ctx.model();
    let mut state = ctx.load();
    let mut names: Vec<String> = state["names"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let uml = nw16a::si01::Uml::new();
    for (name, base) in &ctx.nodes {
        for i in 0..count {
            let class = format!("{tag}-{name}-{i}");
            for op in uml.named_class(0, &class) {
                let body = serde_json::to_string(&op).unwrap_or_else(|e| ctx.fail(e));
                match ctx.post(base, &format!("/api/model/{model}/op"), body) {
                    Ok((200, answer)) if answer["success"] == Value::Bool(true) => {}
                    Ok((status, answer)) => {
                        ctx.fail(format!("{name} refused an operation ({status}): {answer}"))
                    }
                    Err(e) => ctx.fail(e),
                }
            }
            names.push(class);
        }
    }
    state["names"] = json!(names);
    ctx.save(&state);
    println!(
        "NW19-STACK write PASS {count} classes on each of {} nodes, tag {tag}",
        ctx.nodes.len()
    );
}

fn converge(ctx: &Ctx) {
    let model = ctx.model();
    let expected: BTreeSet<String> = ctx.load()["names"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let done = ctx.until(
        "every node serves exactly the written classes, the same document",
        || {
            let mut docs = Vec::new();
            for (name, base) in &ctx.nodes {
                let (status, doc) = ctx.get(base, &format!("/api/model/{model}/state"))?;
                if status != 200 {
                    return Err(format!("{name} answered {status}: {doc}"));
                }
                let held = nw16a::names_in(&doc);
                if held != expected {
                    return Err(format!(
                        "{name} serves {} classes, {} expected; missing {:?}",
                        held.len(),
                        expected.len(),
                        expected.difference(&held).take(5).collect::<Vec<_>>()
                    ));
                }
                docs.push(doc);
            }
            if docs.windows(2).all(|w| w[0] == w[1]) {
                Ok(format!(
                    "{} classes on {} nodes",
                    expected.len(),
                    docs.len()
                ))
            } else {
                Err("the class names agree but the documents differ".into())
            }
        },
    );
    println!("NW19-STACK converge PASS {done}");
}

fn disconnected(ctx: &Ctx, subject: &str) {
    let done = ctx.until(&format!("every node reads {subject} Disconnected"), || {
        let mut lines = Vec::new();
        for (name, base) in &ctx.nodes {
            let peers = ctx.peer_status(base)?;
            match peers.iter().find(|(id, _)| id == subject) {
                Some((_, status)) if status != "Disconnected" => {
                    return Err(format!("{name} reads {subject} {status}"));
                }
                found => lines.push(format!("{name}: {found:?}")),
            }
        }
        Ok(lines.join("; "))
    });
    println!("NW19-STACK disconnected PASS {subject}: {done}");
}

#[test]
#[ignore = "a program for tests/nw19/stack.sh, not a test; does nothing without NW19_STEP"]
fn step() {
    let Ok(step) = std::env::var("NW19_STEP") else {
        return;
    };
    let nodes = std::env::var("NW19_NODES")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let (name, base) = pair
                .split_once('=')
                .unwrap_or_else(|| panic!("NW19-STACK {step} FAIL NW19_NODES entry `{pair}`"));
            (name.to_string(), base.trim_end_matches('/').to_string())
        })
        .collect();
    let ctx = Ctx {
        step: step.clone(),
        nodes,
        state: PathBuf::from(
            std::env::var("NW19_STATE").unwrap_or_else(|_| "nw19-state.json".into()),
        ),
        timeout: Duration::from_secs(
            std::env::var("NW19_TIMEOUT_S")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
        ),
        http: reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("HTTP client"),
    };
    let arg = std::env::var("NW19_ARG").unwrap_or_default();
    match step.as_str() {
        "mesh" => mesh(&ctx),
        "register" => register(&ctx),
        "join" => {
            let model = ctx.model();
            join_on(&ctx, &arg, &model);
            println!("NW19-STACK join PASS {arg} joined {model}");
        }
        "write" => write(
            &ctx,
            arg.parse()
                .unwrap_or_else(|_| ctx.fail(format!("NW19_ARG `{arg}` is no count"))),
            &std::env::var("NW19_TAG").unwrap_or_else(|_| "w".into()),
        ),
        "converge" => converge(&ctx),
        "disconnected" => disconnected(&ctx, &arg),
        other => ctx.fail(format!("unknown step `{other}`")),
    }
}
