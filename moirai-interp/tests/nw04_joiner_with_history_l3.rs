//! NW.04: a replica that already holds history in one model's log and then
//! joins another model by state transfer ends up serving the donor's document
//! of that model, whatever order its own member table lists the members in,
//! even when the donor has compacted a prefix the joiner never delivered.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.04.md`; the schedules K1, K2
//! and K3 below are derived there by hand from the code. All three start the
//! same way: writers `a` and `b`, meshed by static peers, write the model `M`
//! (`a0` by `a`, `b0` by `b` after it joined, then `a1` by `a`), and both
//! compact its prefix (the opening operation, `a0` and `b0`), since every
//! member either knows has delivered it. The joiner `d` is started after
//! that, so the prefix was compacted without it, and `d` holds history in
//! another model's log (its sibling log) when it asks for `M`.
//!
//! - K1, the joiner's own id first. `d` has never adopted anything, so its
//!   member table lists its own id first, then its peers. It creates a model
//!   of its own and writes to it, a fourth node `e` joins that model from it,
//!   and then `d` joins `M`. No link is delayed.
//! - K2, a member the donor never heard of, before the joiner's id. `a`
//!   creates a second model `N`; `c` joins `N` from `a` over a link that holds
//!   every frame `c` sends `a` after its request for `N`'s state, so `a` never
//!   hears from `c` again; `c` also peers with `y`, whom `a` never meets. `c`
//!   creates a model of its own, `d` joins it from `c`, whose list names `y`
//!   before `d`, and then joins `M` from `a` over a link that holds everything
//!   `d` sends `a` after its request for `M`'s state.
//! - K3, as K2, but `d` is linked to `a` directly from the start.
//!
//! The held links are released at the end. The claim is checked on documents
//! only: once nothing is in flight every replica hosting `M` serves
//! `{"a0": "a", "b0": "b", "a1": "c"}`. Beside it, the joiner's sibling log must go on
//! converging with its co-host, including an edit each writes after the
//! join: a way of adopting `M` that breaks the sibling log is not a way of
//! adopting it.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw04_joiner_with_history_l3 \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! The node tests follow the skip rule of `e2e_interp.rs`: no binary, print
//! `E2E-SKIP <scenario>: <why>` and return. A skip is not a pass.
#![cfg(feature = "network")]

use std::fs;
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
#[path = "nw03/proxy.rs"]
mod proxy;

use proxy::Proxy;

/// The default log every node of a run hosts. Nothing writes it.
const LOG_ID: &str = "4a044a044a044a044a044a044a044a04";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the replicas have, once nothing more is written or held, to serve
/// the documents of the schedule. Pre-registered in the spec; not to be
/// changed without a ruling.
const QUIESCENCE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// The claim, over the wire
// ---------------------------------------------------------------------------

/// K1 of the spec: the joiner lists itself first, as every node that has not
/// adopted a donor's ordering does, and holds history in a model of its own.
#[test]
fn nw04_k1_a_joiner_that_lists_itself_first_and_holds_history_serves_the_donors_compacted_model() {
    run_or_skip("nw04-k1", k1);
}

/// K2 of the spec: the joiner lists, before its own id, a member the donor
/// has never heard of, and the donor hears nothing of the joiner before its
/// request: the ordering NW.03's fix (the donor takes the requester in as it
/// serves) gives the donor.
#[test]
fn nw04_k2_a_joiner_that_lists_a_member_the_donor_never_heard_of_before_itself_serves_the_donors_compacted_model()
 {
    run_or_skip("nw04-k2", k2);
}

/// K3 of the spec: as K2, but the joiner is linked to the donor from the
/// start, so the donor took the joiner in from its first frames, before the
/// member it has never heard of.
#[test]
fn nw04_k3_a_joiner_the_donor_took_in_before_a_member_it_lists_first_serves_the_donors_compacted_model()
 {
    run_or_skip("nw04-k3", k3);
}

fn run_or_skip(name: &str, scenario: fn(&Path, &str) -> Result<(), String>) {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP {name}: {why}");
            return;
        }
    };
    if let Err(why) = scenario(&binary, name) {
        panic!("{name}: {why}");
    }
}

/// What every replica hosting `M` must serve: the two writers' edits.
fn model_m() -> Value {
    json!({"a0": "a", "b0": "b", "a1": "c"})
}

/// A fresh scratch directory for one scenario run, with the JSON descriptor
/// every node serves.
fn scratch_for(name: &str) -> Result<(PathBuf, String), String> {
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let scratch = std::env::temp_dir().join(format!("moirai-{name}-{}-{run}", std::process::id()));
    let metamodels = scratch.join("metamodels");
    fs::create_dir_all(&metamodels).map_err(|err| err.to_string())?;
    fs::write(metamodels.join("json.json"), JSON_DESCRIPTOR).map_err(|err| err.to_string())?;
    Ok((scratch, metamodels.to_string_lossy().into_owned()))
}

/// Steps shared by every scenario: writers `a` and `b`, meshed with each
/// other and nobody else, write `M`, and both compact its prefix.
struct Writers {
    a: Node,
    b: Node,
    model: String,
}

fn writers_with_compacted_model(
    binary: &Path,
    scratch: &Path,
    env: &[(&str, &str)],
    http: &Http,
    js: &Json,
    digest: &str,
) -> Result<Writers, String> {
    let specs = [
        launch::NodeSpec::new("a", env),
        launch::NodeSpec::new("b", env),
    ];
    let mut writers: Vec<Node> = launch::launch(binary, scratch, &specs)?
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
    let b = writers.pop().expect("two writers");
    let a = writers.pop().expect("two writers");

    let model = http.register(&a, None, digest)?;
    http.submit(&a, &model, &js.put("a0", 'a'))?;
    http.register(&b, Some(&model), digest)?;
    http.until_serves(&b, &model, &json!({"a0": "a"}))?;
    http.submit(&b, &model, &js.put("b0", 'b'))?;
    http.until_serves(&a, &model, &json!({"a0": "a", "b0": "b"}))?;
    http.submit(&a, &model, &js.put("a1", 'c'))?;
    http.until_serves(&a, &model, &model_m())?;
    http.until_serves(&b, &model, &model_m())?;

    // The premise: both writers have compacted the prefix `M` opened with
    // (its opening operation, `a0` and `b0`), so neither outbox, the only
    // thing a delta sync is answered from, still holds it. `a` compacts it on
    // delivering `b0`, `b` on delivering `a1`.
    let compacted = |node: &Node| -> Result<bool, String> {
        let metrics = http.metrics(node, &model)?;
        let column = |id: &str| metrics["stable_version"][id].as_u64().unwrap_or(0);
        Ok(column("a") >= 2 && column("b") >= 1)
    };
    wait_until(
        "a and b to compact M's prefix (precondition)",
        STEP_TIMEOUT,
        || Ok((compacted(&a)? && compacted(&b)?).then_some(())),
    )
    .map_err(|why| {
        let of = |node: &Node| {
            http.metrics(node, &model)
                .map(|m| stability(&m))
                .unwrap_or_else(|why| why)
        };
        format!(
            "precondition not met: {why}; a's M: {}; b's M: {}",
            of(&a),
            of(&b)
        )
    })?;
    Ok(Writers { a, b, model })
}

/// K1. No link is delayed.
fn k1(binary: &Path, name: &str) -> Result<(), String> {
    let tag = name.to_uppercase();
    let (scratch, metamodels) = scratch_for(name)?;
    let env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", metamodels.as_str())];
    let js = Json::new();
    let digest = metamodel_digest(&serde_json::from_str::<Value>(JSON_DESCRIPTOR).unwrap());
    let http = Http::new()?;

    // ---- 1. a and b write M; a compacts it ----
    let Writers { a, b, model } =
        writers_with_compacted_model(binary, &scratch, &env, &http, &js, &digest)?;
    let donor_before = http.metrics(&a, &model)?;

    // ---- 2. d, which has never adopted anything, creates a model of its own ----
    let d = spawn_node(
        binary,
        &scratch,
        "d",
        &env,
        &format!("a:127.0.0.1:{},b:127.0.0.1:{}", a.listen, b.listen),
    )?;
    http.await_health(&d)?;
    http.await_loop(&d)?;
    let sibling = http.register(&d, None, &digest)?;
    http.submit(&d, &sibling, &js.put("l0", 'l'))?;
    http.until_serves(&d, &sibling, &json!({"l0": "l"}))?;

    // ---- 3. e joins d's model, so the sibling log has a co-host ----
    let e = spawn_node(
        binary,
        &scratch,
        "e",
        &env,
        &format!("d:127.0.0.1:{}", d.listen),
    )?;
    http.await_health(&e)?;
    http.await_loop(&e)?;
    http.register(&e, Some(&sibling), &digest)?;
    http.until_serves(&e, &sibling, &json!({"l0": "l"}))?;
    let sibling_before = http.metrics(&d, &sibling)?;
    if sibling_before["delivered_ops"].as_u64().unwrap_or(0) == 0 {
        return Err(format!(
            "precondition not met: d's own model holds no history: {}",
            stability(&sibling_before)
        ));
    }

    // ---- 4. d joins M ----
    http.register(&d, Some(&model), &digest)?;
    let joined = Instant::now();
    let want = model_m();
    let replicas = [&a, &b, &d];
    let model_verdict = converge(&http, &replicas, &model, &want, joined);
    eprintln!(
        "{tag} M: {} ; before the join a's M {} and d's own model {}; d's log for M: {}",
        verdict_line(&model_verdict),
        stability(&donor_before),
        stability(&sibling_before),
        transfer_lines(&d, &model)
    );

    // ---- 5. the guard: the sibling log still converges with its co-host ----
    let guard_verdict = (|| {
        http.submit(&d, &sibling, &js.put("l1", 'm'))?;
        http.submit(&e, &sibling, &js.put("l2", 'n'))?;
        Ok::<_, String>(())
    })()
    .and_then(|()| {
        converge(
            &http,
            &[&d, &e],
            &sibling,
            &json!({"l0": "l", "l1": "m", "l2": "n"}),
            Instant::now(),
        )
    });
    eprintln!(
        "{tag} guard, d's own model: {}",
        verdict_line(&guard_verdict)
    );

    finish(&tag, model_verdict, guard_verdict, String::new())
}

/// Whether the joiner's link to the donor holds its frames until its request
/// for `M`'s state (K2) or carries them from the start (K3).
#[derive(Clone, Copy, PartialEq, Eq)]
enum DonorLink {
    /// K2: nothing of `d` reaches `a` before `d`'s request for `M`, so what
    /// `a` holds of `d` when it answers is what the answer itself adds.
    HeldUntilRequest,
    /// K3: `d` is linked to `a` from the start, so `a` has heard from `d`,
    /// and taken it in, long before `d` asks for `M`.
    Direct,
}

fn k2(binary: &Path, name: &str) -> Result<(), String> {
    member_the_donor_never_heard_of(binary, name, DonorLink::HeldUntilRequest)
}

fn k3(binary: &Path, name: &str) -> Result<(), String> {
    member_the_donor_never_heard_of(binary, name, DonorLink::Direct)
}

/// K2 and K3. `c`'s link to `a` holds every frame `c` sends after its request
/// for `N`'s state, until the end; in K2 `d`'s link to `a` holds likewise after
/// its request for `M`'s state.
fn member_the_donor_never_heard_of(
    binary: &Path,
    name: &str,
    donor_link: DonorLink,
) -> Result<(), String> {
    let tag = name.to_uppercase();
    let (scratch, metamodels) = scratch_for(name)?;
    let env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", metamodels.as_str())];
    let js = Json::new();
    let digest = metamodel_digest(&serde_json::from_str::<Value>(JSON_DESCRIPTOR).unwrap());
    let http = Http::new()?;

    // ---- 1. a and b write M; a compacts it ----
    let Writers { a, b, model } =
        writers_with_compacted_model(binary, &scratch, &env, &http, &js, &digest)?;
    let donor_before = http.metrics(&a, &model)?;

    // ---- 2. c takes a's ordering by joining a's model N, over a link that
    //         then holds; c also peers with y, whom a never meets ----
    let first = http.register(&a, None, &digest)?;
    http.submit(&a, &first, &js.put("n0", 'a'))?;
    http.until_serves(&a, &first, &json!({"n0": "a"}))?;
    let y = spawn_node(binary, &scratch, "y", &env, "")?;
    http.await_health(&y)?;
    let link_c = Proxy::start(&[("a", a.listen)])?;
    let c = spawn_node(
        binary,
        &scratch,
        "c",
        &env,
        &format!("a:127.0.0.1:{},y:127.0.0.1:{}", link_c.port("a"), y.listen),
    )?;
    http.await_health(&c)?;
    http.await_loop(&c)?;
    http.register(&c, Some(&first), &digest)?;
    wait_until("c to ask a for N's state", STEP_TIMEOUT, || {
        Ok(link_c.first("StateRequest", &first))
    })
    .map_err(|why| format!("{why}\n{}", link_c.describe()))?;
    link_c.open_up_to("a", "StateRequest", &first);
    http.until_serves(&c, &first, &json!({"n0": "a"}))
        .map_err(|why| format!("c never joined N from a: {why}\n{}", link_c.describe()))?;

    // ---- 3. c creates the model that will be d's sibling log ----
    let sibling = http.register(&c, None, &digest)?;
    http.submit(&c, &sibling, &js.put("c0", 'c'))?;
    http.until_serves(&c, &sibling, &json!({"c0": "c"}))?;

    // ---- 4. d joins c's model from c, then M from a ----
    let link_d = Proxy::start(&[("a", a.listen)])?;
    let to_a = match donor_link {
        DonorLink::HeldUntilRequest => link_d.port("a"),
        DonorLink::Direct => a.listen,
    };
    let d = spawn_node(
        binary,
        &scratch,
        "d",
        &env,
        &format!("a:127.0.0.1:{to_a},c:127.0.0.1:{}", c.listen),
    )?;
    http.await_health(&d)?;
    http.await_loop(&d)?;
    http.register(&d, Some(&sibling), &digest)?;
    http.until_serves(&d, &sibling, &json!({"c0": "c"}))
        .map_err(|why| format!("d never joined c's model: {why}\n{}", link_d.describe()))?;
    let sibling_before = http.metrics(&d, &sibling)?;
    if sibling_before["delivered_ops"].as_u64().unwrap_or(0) == 0 {
        return Err(format!(
            "precondition not met: d's sibling log holds no history: {}",
            stability(&sibling_before)
        ));
    }
    // Reported, not required: on the tree the spec was written against, d's
    // sibling log lists y, learnt from c. A change that keeps one member list
    // per log would not, and the scenario still judges it on documents.
    let mut premise = vec![format!(
        "d's sibling log lists y: {}",
        sibling_before["stable_version"].get("y").is_some()
    )];

    http.register(&d, Some(&model), &digest)?;
    let donor_at_request;
    let mut snapshot_members: Option<Vec<String>> = None;
    match donor_link {
        DonorLink::HeldUntilRequest => {
            wait_until("d to ask a for M's state", STEP_TIMEOUT, || {
                Ok(link_d.first("StateRequest", &model))
            })
            .map_err(|why| format!("{why}\n{}", link_d.describe()))?;
            // Reported, not required: nothing that has reached a names y or
            // d. Every frame a has received from c or d is forwarded by one of
            // the two links, and b hears of members only from a.
            let leaked: Vec<String> = [&link_c, &link_d]
                .iter()
                .flat_map(|link| {
                    let seen = link.seen("a");
                    let forwarded = seen.len() - link.held("a").len();
                    seen.into_iter().take(forwarded).collect::<Vec<_>>()
                })
                .filter(|frame| frame.members.iter().any(|m| m == "y" || m == "d"))
                .map(|frame| frame.label())
                .collect();
            donor_at_request = http.metrics(&a, &model)?;
            premise.push(format!(
                "frames that reached a naming y or d before d's request: {leaked:?}; a's M \
                 names y: {}, d: {}",
                donor_at_request["stable_version"].get("y").is_some(),
                donor_at_request["stable_version"].get("d").is_some()
            ));
            link_d.open_up_to("a", "StateRequest", &model);
            // Wait for a's answer, whichever it is; on timeout go on, the
            // window below judges.
            snapshot_members = wait_until("a to answer d's request for M", STEP_TIMEOUT, || {
                Ok(link_d.back("a").into_iter().find(|frame| {
                    frame.is("StateResponse", &model) || frame.is("StateUnavailable", &model)
                }))
            })
            .ok()
            .map(|answer| answer.members);
        }
        DonorLink::Direct => {
            donor_at_request = http.metrics(&a, &model)?;
            // Wait for d to have acted on an answer from a, whichever way;
            // on timeout go on, the window below judges.
            let _ = wait_until("d to act on a's answer for M", STEP_TIMEOUT, || {
                Ok((transfer_count(&d, &model) > 0).then_some(()))
            });
        }
    }
    // d has had the answer in hand; give its loop a moment to act on it.
    std::thread::sleep(Duration::from_millis(500));

    // ---- 5. the guard's edits, then release ----
    let guard_writes = http
        .submit(&d, &sibling, &js.put("d1", 'd'))
        .and_then(|()| http.submit(&c, &sibling, &js.put("c1", 'e')));
    link_c.release("a");
    if donor_link == DonorLink::HeldUntilRequest {
        link_d.release("a");
    }
    let released = Instant::now();
    let want = model_m();
    let model_verdict = converge(&http, &[&a, &b, &d], &model, &want, released);
    let guard_verdict = guard_writes.and_then(|()| {
        converge(
            &http,
            &[&c, &d],
            &sibling,
            &json!({"c0": "c", "d1": "d", "c1": "e"}),
            released,
        )
    });
    let joiner_members = match donor_link {
        DonorLink::HeldUntilRequest => link_d
            .seen("a")
            .into_iter()
            .rev()
            .find(|frame| !frame.members.is_empty())
            .map(|frame| format!("{:?}", frame.members))
            .unwrap_or_else(|| "(none seen)".to_string()),
        DonorLink::Direct => "(not observed on a direct link)".to_string(),
    };
    let context = format!(
        "premise: {}; a's snapshot of M named {}; d's last frame to a named {joiner_members}; \
         a's M when d asked {}; d's sibling log before {}; d's log for M: {}\nc's {}{}",
        premise.join("; "),
        snapshot_members
            .map(|members| format!("{members:?}"))
            .unwrap_or_else(|| "(not observed on a direct link)".to_string()),
        stability(&donor_at_request),
        stability(&sibling_before),
        transfer_lines(&d, &model),
        link_c.describe(),
        match donor_link {
            DonorLink::HeldUntilRequest => format!("\nd's {}", link_d.describe()),
            DonorLink::Direct => String::new(),
        }
    );
    eprintln!(
        "{tag} M: {} ; guard, d's sibling log: {}; before the join a's M {}; {context}",
        verdict_line(&model_verdict),
        verdict_line(&guard_verdict),
        stability(&donor_before)
    );
    drop(y);
    finish(&tag, model_verdict, guard_verdict, context)
}

/// Poll the default read of `model` on every one of `replicas` until all
/// serve `want`, for [`QUIESCENCE`] from `since`.
fn converge(
    http: &Http,
    replicas: &[&Node],
    model: &str,
    want: &Value,
    since: Instant,
) -> Result<f32, String> {
    loop {
        let states: Vec<Result<Value, String>> = replicas
            .iter()
            .map(|node| http.state(node, model))
            .collect();
        if states.iter().all(|state| state.as_ref().ok() == Some(want)) {
            return Ok(since.elapsed().as_secs_f32());
        }
        if since.elapsed() > QUIESCENCE {
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
                        http.metrics(node, model)
                            .map(|m| stability(&m))
                            .unwrap_or_else(|why| why)
                    )
                })
                .collect();
            return Err(format!(
                "{} s on, the replicas do not all serve {want}: {}",
                QUIESCENCE.as_secs(),
                report.join("; ")
            ));
        }
        std::thread::sleep(POLL);
    }
}

fn verdict_line(verdict: &Result<f32, String>) -> String {
    match verdict {
        Ok(secs) => format!("pass, every replica serves the document after {secs} s"),
        Err(why) => format!("FAIL: {why}"),
    }
}

fn finish(
    tag: &str,
    model: Result<f32, String>,
    guard: Result<f32, String>,
    context: String,
) -> Result<(), String> {
    match (model, guard) {
        (Ok(_), Ok(_)) => {
            eprintln!("{tag} pass");
            Ok(())
        }
        (model, guard) => Err(format!(
            "M: {}\nguard: {}\n{context}",
            verdict_line(&model),
            verdict_line(&guard)
        )),
    }
}

/// How many snapshots for `model` the node's log says it adopted or turned
/// away.
fn transfer_count(node: &Node, model: &str) -> usize {
    node.log_text()
        .lines()
        .filter(|line| {
            (line.contains("adopted state from") || line.contains("cannot adopt"))
                && line.contains(model)
        })
        .count()
}

/// What the joiner's log says of its state transfers for `model`: how many
/// snapshots it adopted and how many it turned away.
fn transfer_lines(node: &Node, model: &str) -> String {
    let text = node.log_text();
    let adopted = text
        .lines()
        .filter(|line| line.contains("adopted state from") && line.contains(model))
        .count();
    let refused: Vec<&str> = text
        .lines()
        .filter(|line| line.contains("cannot adopt") && line.contains(model))
        .collect();
    format!(
        "{adopted} adopted, {} turned away{}",
        refused.len(),
        refused
            .first()
            .map(|line| format!(" (first: {line})"))
            .unwrap_or_default()
    )
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

/// Start one node on ports it binds itself (NW.02) and hand it `peers`, the
/// `id:host:port,...` text of `PEERS`, empty for none.
fn spawn_node(
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
        http: reported
            .http_port
            .ok_or_else(|| format!("{id} reported no HTTP port"))?,
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

    /// Wait until `node` has dialled its peers and entered its event loop.
    fn await_loop(&self, node: &Node) -> Result<(), String> {
        wait_until(
            &format!("{} to enter its event loop", node.id),
            STEP_TIMEOUT,
            || {
                Ok(node
                    .log_text()
                    .contains("Entering main event loop")
                    .then_some(()))
            },
        )
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

/// `json.ecore`, the one operation the schedules use: a one-character string
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
