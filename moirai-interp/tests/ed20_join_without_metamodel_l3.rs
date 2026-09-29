//! ED.20: a model a replica has seen but does not host is joined by its id
//! alone, and the node binds it to the metamodel the model's own history
//! carries, never to one it picked.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ED.20.md`. The request under
//! test is `POST /api/models` with a body of `{"model_id": "<id>"}` and no
//! `metamodel_id`. Three scenarios, each on nodes of its own:
//!
//! - N1, the snapshot path and the pending window. Writers `a` and `c` write
//!   `M1` in the language `lacked`, which the joiner `b` holds no descriptor
//!   for, and both compact its prefix (the opening `Install` among it) before
//!   `b` exists, so the only thing that can bring `M1`'s metamodel to `b` is
//!   the descriptor inside a donor's state transfer. `b` starts, sees `M1`,
//!   pauses every peer, joins `M1` by id alone, and for `PENDING_WINDOW`
//!   must name no metamodel for it: the listing names no digest and the
//!   metamodel route answers 404. Then `b` resumes and must bind `M1` to
//!   `lacked` and serve `a`'s document. Then `a` creates `M2` in the language
//!   `held`, which `b` holds, and `b` joins it by id alone and binds it to
//!   `held`.
//! - N2, a joiner with history of its own. `d` writes a model of its own
//!   first, then joins `a`'s `M3`, in `lacked`, by id alone. On the tree the
//!   spec was written against `d` turns `a`'s snapshot away (its member table
//!   orders the members differently, NW.04) and replays `M3`'s history,
//!   `Install` first; which path was taken is reported, not required.
//! - N3, what must not change. A create with no metamodel is still refused;
//!   a join that names a digest still answers as it does today, bound when
//!   the digest is the model's, and answering 404 on the metamodel route once
//!   the history shows it is not; and a join by id alone of an id nobody has
//!   served is refused, or hosted with no metamodel named, never bound.
//!
//! The guard of every scenario is the 2026-09-14 incident: a join that
//! names no metamodel must never be answered with one the node picked. So
//! from the moment of the join every read of `GET /api/models` and of
//! `GET /api/model/{id}/metamodel` is checked: neither may name a digest
//! other than the one of the descriptor the model was opened with. The
//! joiner holds a decoy descriptor that sorts first in its `METAMODEL_DIR`,
//! so a node that answers "its first descriptor" answers the decoy.
//!
//! The three languages are one descriptor, `json.ecore`, under three nsURIs:
//! the digest is taken over the parsed descriptor, so each is a metamodel of
//! its own, and one operation encoder writes all three.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --test ed20_join_without_metamodel_l3 \
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

/// The default log every node of a run hosts. Nothing writes it.
const LOG_ID: &str = "ed20ed20ed20ed20ed20ed20ed20ed20";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a joiner has, once it can reach a donor, to bind the model and
/// serve the donor's document. Pre-registered in the spec; not to be changed
/// without a ruling.
const BIND_WINDOW: Duration = Duration::from_secs(30);
/// How long a joiner cut off from every donor is watched after a join that
/// named no metamodel. Pre-registered in the spec.
const PENDING_WINDOW: Duration = Duration::from_secs(3);
/// How long a disputed binding must keep answering 404 once it does.
/// Pre-registered in the spec.
const DISPUTE_HOLD: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// The claim, over the wire
// ---------------------------------------------------------------------------

/// N1 of the spec: a compacted model in a language the joiner lacks binds
/// through the snapshot, and one in a language it holds binds too.
#[test]
fn ed20_n1_a_join_by_id_alone_binds_a_compacted_model_to_the_language_its_snapshot_carries() {
    run_or_skip("ed20-n1", n1);
}

/// N2 of the spec: a joiner that already holds history of its own binds a
/// model in a language it lacks, whichever way the history reaches it.
#[test]
fn ed20_n2_a_joiner_with_history_of_its_own_binds_a_model_to_the_language_its_history_carries() {
    run_or_skip("ed20-n2", n2);
}

/// N3 of the spec: no metamodel is ever picked for a model, and the join that
/// names a digest answers as it does today.
#[test]
fn ed20_n3_no_default_metamodel_and_the_join_that_names_a_digest_is_unchanged() {
    run_or_skip("ed20-n3", n3);
}

fn run_or_skip(name: &str, scenario: fn(&Path, &str) -> Result<Checks, String>) {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP {name}: {why}");
            return;
        }
    };
    match scenario(&binary, name) {
        Ok(checks) => checks.finish(),
        Err(why) => panic!("{name}: PREMISE or harness failure: {why}"),
    }
}

// ---------------------------------------------------------------------------
// N1
// ---------------------------------------------------------------------------

fn n1(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new(name);
    let langs = Languages::new();
    let (scratch, writers_dir, joiner_dir) = scratch_for(name, &langs)?;
    let writers_env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", writers_dir.as_str())];
    let joiner_env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", joiner_dir.as_str())];
    let http = Http::new()?;
    let js = Json::new();

    // ---- 1. writers a and c, each the other's only peer ----
    let (a, c) = pair(binary, &scratch, &writers_env, &writers_env, "a", "c", &http)?;

    // ---- 2. M1, in `lacked`, written by both and compacted by both ----
    let m1 = http.register(&a, None, &langs.lacked.digest)?;
    http.submit(&a, &m1, &js.put("a0", 'a'))?;
    http.register(&c, Some(&m1), &langs.lacked.digest)?;
    http.until_serves(&c, &m1, &json!({"a0": "a"}))?;
    http.submit(&c, &m1, &js.put("c0", 'c'))?;
    http.until_serves(&a, &m1, &json!({"a0": "a", "c0": "c"}))?;
    http.submit(&a, &m1, &js.put("a1", 'd'))?;
    let m1_doc = json!({"a0": "a", "c0": "c", "a1": "d"});
    http.until_serves(&a, &m1, &m1_doc)?;
    http.until_serves(&c, &m1, &m1_doc)?;
    // The premise: the prefix M1 opened with (its `Install`, `a0`, `c0`) is
    // compacted on both writers, so neither outbox, the only thing a delta
    // sync is answered from, still holds the `Install`.
    let compacted = |node: &Node| -> Result<bool, String> {
        let metrics = http.metrics(node, &m1)?;
        let column = |id: &str| metrics["stable_version"][id].as_u64().unwrap_or(0);
        Ok(column("a") >= 2 && column("c") >= 1)
    };
    wait_until("a and c to compact M1's prefix (premise)", STEP_TIMEOUT, || {
        Ok((compacted(&a)? && compacted(&c)?).then_some(()))
    })
    .map_err(|why| {
        let of = |node: &Node| {
            http.metrics(node, &m1)
                .map(|m| stability(&m))
                .unwrap_or_else(|why| why)
        };
        format!("{why}; a's M1: {}; c's M1: {}", of(&a), of(&c))
    })?;
    eprintln!(
        "{} premise: M1 {m1} compacted on a ({}) and c ({})",
        checks.tag,
        stability(&http.metrics(&a, &m1)?),
        stability(&http.metrics(&c, &m1)?)
    );

    // ---- 3. b starts, holding the decoy and `held`, never `lacked` ----
    let b = spawn_node(
        binary,
        &scratch,
        "b",
        &joiner_env,
        &format!("a:127.0.0.1:{},c:127.0.0.1:{}", a.listen, c.listen),
    )?;
    http.await_health(&b)?;
    http.await_loop(&b)?;
    premise_holds_not(&http, &b, &langs.lacked)?;
    http.until_seen(&b, &m1)?;

    // ---- 4. the pending window: b cut off from every donor ----
    http.pause_all(&b)?;
    let joined = http.join_by_id_alone(&b, &m1);
    let accepted = checks.check("n1.m1.join_by_id_alone_accepted", joined.clone());
    if accepted {
        let pending = watch_pending(&http, &b, &m1);
        checks.check("n1.m1.pending_names_no_metamodel", pending);
    } else {
        checks.not_reached("n1.m1.pending_names_no_metamodel");
    }
    http.resume_all(&b)?;
    if accepted {
        let watch = watch_binding(&http, &b, &m1, &langs.lacked.digest, &m1_doc, &langs);
        checks.check("n1.m1.bound_to_the_language_its_history_carries", watch.bound());
        checks.check("n1.m1.serves_the_donors_document", watch.served());
        checks.check("n1.m1.never_names_another_metamodel", watch.guard());
        eprintln!("{} M1 path on b: {}", checks.tag, path_taken(&b, &m1));
        // The model is b's now, not a copy: a's next edit reaches it.
        let follows = http
            .submit(&a, &m1, &js.put("a2", 'e'))
            .and_then(|()| {
                http.until_serves(&b, &m1, &json!({"a0": "a", "c0": "c", "a1": "d", "a2": "e"}))
            })
            .map(|()| "b serves a's next edit".to_string());
        checks.check("n1.m1.follows_the_donor", follows);
    } else {
        for name in [
            "n1.m1.bound_to_the_language_its_history_carries",
            "n1.m1.serves_the_donors_document",
            "n1.m1.never_names_another_metamodel",
            "n1.m1.follows_the_donor",
        ] {
            checks.not_reached(name);
        }
    }

    // ---- 5. M2, in `held`, created on a after b connected ----
    let m2 = http.register(&a, None, &langs.held.digest)?;
    http.submit(&a, &m2, &js.put("k0", 'k'))?;
    let m2_doc = json!({"k0": "k"});
    http.until_serves(&a, &m2, &m2_doc)?;
    http.until_seen(&b, &m2)?;
    let joined = http.join_by_id_alone(&b, &m2);
    if checks.check("n1.m2.join_by_id_alone_accepted", joined) {
        let watch = watch_binding(&http, &b, &m2, &langs.held.digest, &m2_doc, &langs);
        checks.check("n1.m2.bound_to_the_language_its_history_carries", watch.bound());
        checks.check("n1.m2.serves_the_donors_document", watch.served());
        checks.check("n1.m2.never_names_another_metamodel", watch.guard());
        eprintln!("{} M2 path on b: {}", checks.tag, path_taken(&b, &m2));
    } else {
        for name in [
            "n1.m2.bound_to_the_language_its_history_carries",
            "n1.m2.serves_the_donors_document",
            "n1.m2.never_names_another_metamodel",
        ] {
            checks.not_reached(name);
        }
    }
    checks.log_tail(&b);
    Ok(checks)
}

// ---------------------------------------------------------------------------
// N2
// ---------------------------------------------------------------------------

fn n2(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new(name);
    let langs = Languages::new();
    let (scratch, writers_dir, joiner_dir) = scratch_for(name, &langs)?;
    let writers_env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", writers_dir.as_str())];
    let joiner_env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", joiner_dir.as_str())];
    let http = Http::new()?;
    let js = Json::new();

    // ---- 1. a and d, meshed; d writes a model of its own first ----
    let (a, d) = pair(binary, &scratch, &writers_env, &joiner_env, "a", "d", &http)?;
    premise_holds_not(&http, &d, &langs.lacked)?;
    let own = http.register(&d, None, &langs.held.digest)?;
    http.submit(&d, &own, &js.put("s0", 's'))?;
    http.until_serves(&d, &own, &json!({"s0": "s"}))?;
    let own_metrics = http.metrics(&d, &own)?;
    if own_metrics["delivered_ops"].as_u64().unwrap_or(0) == 0 {
        return Err(format!(
            "premise not met: d's own model holds no history: {}",
            stability(&own_metrics)
        ));
    }

    // ---- 2. M3, in `lacked`, on a ----
    let m3 = http.register(&a, None, &langs.lacked.digest)?;
    http.submit(&a, &m3, &js.put("l0", 'l'))?;
    http.submit(&a, &m3, &js.put("l1", 'm'))?;
    let m3_doc = json!({"l0": "l", "l1": "m"});
    http.until_serves(&a, &m3, &m3_doc)?;
    http.until_seen(&d, &m3)?;

    // ---- 3. d joins M3 by id alone ----
    let joined = http.join_by_id_alone(&d, &m3);
    if checks.check("n2.m3.join_by_id_alone_accepted", joined) {
        let watch = watch_binding(&http, &d, &m3, &langs.lacked.digest, &m3_doc, &langs);
        checks.check("n2.m3.bound_to_the_language_its_history_carries", watch.bound());
        checks.check("n2.m3.serves_the_donors_document", watch.served());
        checks.check("n2.m3.never_names_another_metamodel", watch.guard());
        eprintln!("{} M3 path on d: {}", checks.tag, path_taken(&d, &m3));
        let follows = http
            .submit(&a, &m3, &js.put("l2", 'n'))
            .and_then(|()| http.until_serves(&d, &m3, &json!({"l0": "l", "l1": "m", "l2": "n"})))
            .map(|()| "d serves a's next edit".to_string());
        checks.check("n2.m3.follows_the_donor", follows);
    } else {
        for name in [
            "n2.m3.bound_to_the_language_its_history_carries",
            "n2.m3.serves_the_donors_document",
            "n2.m3.never_names_another_metamodel",
            "n2.m3.follows_the_donor",
        ] {
            checks.not_reached(name);
        }
    }
    // Reported, not required: d's own model still serves what d wrote.
    eprintln!(
        "{} d's own model after the join: {}",
        checks.tag,
        http.state(&d, &own).map(|v| v.to_string()).unwrap_or_else(|why| why)
    );
    checks.log_tail(&d);
    Ok(checks)
}

// ---------------------------------------------------------------------------
// N3
// ---------------------------------------------------------------------------

fn n3(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new(name);
    let langs = Languages::new();
    let (scratch, writers_dir, joiner_dir) = scratch_for(name, &langs)?;
    let writers_env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", writers_dir.as_str())];
    let joiner_env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", joiner_dir.as_str())];
    let http = Http::new()?;
    let js = Json::new();
    let (a, b) = pair(binary, &scratch, &writers_env, &joiner_env, "a", "b", &http)?;
    premise_holds_not(&http, &b, &langs.lacked)?;

    // ---- C1: a create that names no metamodel is refused, as today ----
    let before = http.hosted(&b)?;
    let mut refusals = Vec::new();
    for body in [json!({}), json!({"metamodel_id": null})] {
        let (status, answer) = http.post(&b, "/api/models", body.to_string())?;
        refusals.push(format!("{body} -> {status} {answer}"));
        if !(400..500).contains(&status) {
            refusals.push(format!("NOT REFUSED: {body}"));
        }
    }
    let after = http.hosted(&b)?;
    let c1 = if refusals.iter().any(|line| line.starts_with("NOT REFUSED")) {
        Err(refusals.join("; "))
    } else if before != after {
        Err(format!(
            "b hosts {after:?} after the refusals, {before:?} before: {}",
            refusals.join("; ")
        ))
    } else {
        Ok(refusals.join("; "))
    };
    checks.check("n3.c1.create_without_metamodel_refused", c1);

    // ---- C2: a join that names the model's own digest, b not holding it ----
    let m4 = http.register(&a, None, &langs.lacked.digest)?;
    http.submit(&a, &m4, &js.put("x0", 'x'))?;
    let m4_doc = json!({"x0": "x"});
    http.until_serves(&a, &m4, &m4_doc)?;
    let c2 = http.join_naming(&b, &m4, &json!(langs.lacked.digest));
    if checks.check("n3.c2.join_naming_the_right_digest_answered_as_today", c2) {
        let watch = watch_binding(&http, &b, &m4, &langs.lacked.digest, &m4_doc, &langs);
        checks.check("n3.c2.bound_to_the_named_digest", watch.bound());
        checks.check("n3.c2.serves_the_donors_document", watch.served());
    } else {
        checks.not_reached("n3.c2.bound_to_the_named_digest");
        checks.not_reached("n3.c2.serves_the_donors_document");
    }

    // ---- C3: a join that names a digest b holds and the model is not in ----
    let m5 = http.register(&a, None, &langs.lacked.digest)?;
    http.submit(&a, &m5, &js.put("y0", 'y'))?;
    let m5_doc = json!({"y0": "y"});
    http.until_serves(&a, &m5, &m5_doc)?;
    let c3 = http.join_naming(&b, &m5, &json!(langs.held.digest));
    if checks.check("n3.c3.join_naming_a_held_wrong_digest_answered_as_today", c3) {
        let disputed = http
            .until_serves(&b, &m5, &m5_doc)
            .and_then(|()| disputed_route(&http, &b, &m5));
        checks.check("n3.c3.metamodel_route_answers_404_once_the_history_disagrees", disputed);
    } else {
        checks.not_reached("n3.c3.metamodel_route_answers_404_once_the_history_disagrees");
    }

    // ---- C4: a join that names a digest nobody holds ----
    let m6 = http.register(&a, None, &langs.lacked.digest)?;
    http.submit(&a, &m6, &js.put("z0", 'z'))?;
    let m6_doc = json!({"z0": "z"});
    http.until_serves(&a, &m6, &m6_doc)?;
    let nobody = "ab".repeat(32);
    let c4 = http.join_naming(&b, &m6, &json!(nobody));
    if checks.check("n3.c4.join_naming_an_unheld_wrong_digest_answered_as_today", c4) {
        let disputed = http
            .until_serves(&b, &m6, &m6_doc)
            .and_then(|()| disputed_route(&http, &b, &m6));
        checks.check("n3.c4.metamodel_route_answers_404_once_the_history_disagrees", disputed);
    } else {
        checks.not_reached("n3.c4.metamodel_route_answers_404_once_the_history_disagrees");
    }

    // ---- C5: a join by id alone of an id nobody has served ----
    let unseen = unseen_id();
    let (status, answer) = http.post(&b, "/api/models", json!({"model_id": unseen}).to_string())?;
    let c5 = if (400..500).contains(&status) {
        http.hosted(&b).and_then(|hosted| {
            if hosted.contains(&unseen) {
                Err(format!("refused with {status} ({answer}) but hosted anyway"))
            } else {
                Ok(format!("refused: {status} {answer}"))
            }
        })
    } else if status == 200 || status == 202 {
        match digest_named(&answer["metamodel_id"]) {
            Some(named) => Err(format!("the join's answer names metamodel {named}: {answer}")),
            None => watch_pending(&http, &b, &unseen)
                .map(|detail| format!("hosted ({status}) with no metamodel named: {detail}")),
        }
    } else {
        Err(format!("answered {status}: {answer}"))
    };
    checks.check("n3.c5.an_unserved_id_is_never_bound", c5);
    checks.log_tail(&b);
    Ok(checks)
}

// ---------------------------------------------------------------------------
// The observations
// ---------------------------------------------------------------------------

/// The digest a `metamodel_id` names: `None` for null, absent, or an object
/// with no `digest`; the string itself for a bare digest; a marker for any
/// other shape, which counts as naming something.
fn digest_named(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(digest) => Some(digest.clone()),
        Value::Object(fields) => match fields.get("digest") {
            None | Some(Value::Null) => None,
            Some(Value::String(digest)) => Some(digest.clone()),
            Some(other) => Some(format!("<unreadable digest {other}>")),
        },
        other => Some(format!("<unreadable metamodel_id {other}>")),
    }
}

/// One look at a hosted model on a node: what `GET /api/models` names for it,
/// what `GET /api/model/{id}/metamodel` serves, and its document.
struct Look {
    /// `None` when the model is not listed under `models` at all.
    listed: Option<Option<String>>,
    /// `Ok(None)` for a 404, `Ok(Some(digest))` for a descriptor, `Err` for
    /// anything else.
    route: Result<Option<String>, String>,
    state: Option<Value>,
}

fn look(http: &Http, node: &Node, model: &str) -> Result<Look, String> {
    let (_, models) = http.get(node, "/api/models")?;
    let listed = models["models"].as_array().and_then(|entries| {
        entries
            .iter()
            .find(|entry| entry["model_id"] == json!(model))
            .map(|entry| digest_named(&entry["metamodel_id"]))
    });
    let route = match http.get(node, &format!("/api/model/{model}/metamodel"))? {
        (404, _) => Ok(None),
        (200, body) => Ok(Some(metamodel_digest(&body))),
        (status, body) => Err(format!("metamodel route answered {status}: {body}")),
    };
    let state = match http.get(node, &format!("/api/model/{model}/state"))? {
        (200, body) => Some(body),
        _ => None,
    };
    Ok(Look {
        listed,
        route,
        state,
    })
}

/// What was seen while a joined model was watched until bound.
struct Watch {
    want: String,
    polls: usize,
    bound_after: Option<Duration>,
    served_after: Option<Duration>,
    /// Every read that named a metamodel other than `want`, first few kept.
    foreign: Vec<String>,
    foreign_count: usize,
    last: String,
}

impl Watch {
    fn bound(&self) -> Result<String, String> {
        match self.bound_after {
            Some(after) => Ok(format!(
                "listing and metamodel route name {} after {:.1} s",
                short(&self.want),
                after.as_secs_f32()
            )),
            None => Err(format!(
                "not bound to {} within {} s; last look: {}",
                short(&self.want),
                BIND_WINDOW.as_secs(),
                self.last
            )),
        }
    }

    fn served(&self) -> Result<String, String> {
        match self.served_after {
            Some(after) => Ok(format!(
                "serves the donor's document after {:.1} s",
                after.as_secs_f32()
            )),
            None => Err(format!(
                "did not serve the donor's document within {} s; last look: {}",
                BIND_WINDOW.as_secs(),
                self.last
            )),
        }
    }

    fn guard(&self) -> Result<String, String> {
        if self.foreign_count == 0 {
            Ok(format!(
                "{} looks, none naming a metamodel other than {}",
                self.polls,
                short(&self.want)
            ))
        } else {
            Err(format!(
                "{} of {} looks named another metamodel: {}",
                self.foreign_count,
                self.polls,
                self.foreign.join(" | ")
            ))
        }
    }
}

/// Watch `model` on `node` from its join until it is listed and served under
/// `want` and serves `doc`, or `BIND_WINDOW` passes, noting every read that
/// names anything else.
fn watch_binding(
    http: &Http,
    node: &Node,
    model: &str,
    want: &str,
    doc: &Value,
    langs: &Languages,
) -> Watch {
    let started = Instant::now();
    let mut watch = Watch {
        want: want.to_string(),
        polls: 0,
        bound_after: None,
        served_after: None,
        foreign: Vec::new(),
        foreign_count: 0,
        last: String::new(),
    };
    while started.elapsed() <= BIND_WINDOW {
        match look(http, node, model) {
            Ok(seen) => {
                watch.polls += 1;
                let mut wrong = Vec::new();
                match &seen.listed {
                    None => wrong.push("not listed under models".to_string()),
                    Some(Some(named)) if named != want => {
                        wrong.push(format!("listing names {}", langs.name(named)))
                    }
                    _ => {}
                }
                match &seen.route {
                    Ok(Some(served)) if served != want => {
                        wrong.push(format!("metamodel route serves {}", langs.name(served)))
                    }
                    Err(why) => wrong.push(why.clone()),
                    _ => {}
                }
                if !wrong.is_empty() {
                    watch.foreign_count += 1;
                    if watch.foreign.len() < 5 {
                        watch.foreign.push(format!(
                            "at {:.1} s: {}",
                            started.elapsed().as_secs_f32(),
                            wrong.join(", ")
                        ));
                    }
                }
                let listed_want = matches!(&seen.listed, Some(Some(named)) if named == want);
                let route_want = matches!(&seen.route, Ok(Some(served)) if served == want);
                if listed_want && route_want && watch.bound_after.is_none() {
                    watch.bound_after = Some(started.elapsed());
                }
                if seen.state.as_ref() == Some(doc) && watch.served_after.is_none() {
                    watch.served_after = Some(started.elapsed());
                }
                watch.last = format!(
                    "listed {:?}, route {:?}, state {}",
                    seen.listed.as_ref().map(|l| l.as_deref().map(|d| langs.name(d))),
                    seen.route.as_ref().map(|r| r.as_deref().map(|d| langs.name(d))),
                    seen.state.map(|s| s.to_string()).unwrap_or_else(|| "none".into())
                );
                if watch.bound_after.is_some() && watch.served_after.is_some() {
                    break;
                }
            }
            Err(why) => watch.last = why,
        }
        std::thread::sleep(POLL);
    }
    watch
}

/// Watch a model joined by id alone for `PENDING_WINDOW` while nothing can
/// bring its history: it must be listed under `models`, the listing must name
/// no digest for it, and its metamodel route must answer 404, at every look.
fn watch_pending(http: &Http, node: &Node, model: &str) -> Result<String, String> {
    let started = Instant::now();
    let mut polls = 0;
    let mut wrong = Vec::new();
    while started.elapsed() <= PENDING_WINDOW {
        let seen = look(http, node, model)?;
        polls += 1;
        match &seen.listed {
            None => wrong.push("not listed under models".to_string()),
            Some(Some(named)) => wrong.push(format!("listing names {named}")),
            Some(None) => {}
        }
        match &seen.route {
            Ok(None) => {}
            Ok(Some(served)) => wrong.push(format!("metamodel route serves {served}")),
            Err(why) => wrong.push(why.clone()),
        }
        if !wrong.is_empty() {
            return Err(format!(
                "at {:.1} s, look {polls}: {}",
                started.elapsed().as_secs_f32(),
                wrong.join(", ")
            ));
        }
        std::thread::sleep(POLL);
    }
    Ok(format!(
        "{polls} looks over {} s: listed, no digest named, metamodel route 404",
        PENDING_WINDOW.as_secs()
    ))
}

/// Once a model joined under a wrong digest serves its donor's document: the
/// metamodel route answers 404 within `STEP_TIMEOUT` and keeps answering 404
/// for `DISPUTE_HOLD`.
fn disputed_route(http: &Http, node: &Node, model: &str) -> Result<String, String> {
    let route = |http: &Http| http.get(node, &format!("/api/model/{model}/metamodel"));
    let first = Instant::now();
    wait_until("the metamodel route to answer 404", STEP_TIMEOUT, || {
        Ok((route(http)?.0 == 404).then_some(()))
    })?;
    let reached = first.elapsed();
    let hold = Instant::now();
    let mut polls = 0;
    while hold.elapsed() <= DISPUTE_HOLD {
        let (status, body) = route(http)?;
        polls += 1;
        if status != 404 {
            return Err(format!(
                "answered 404, then {status} {} after {:.1} s",
                body.to_string().chars().take(80).collect::<String>(),
                hold.elapsed().as_secs_f32()
            ));
        }
        std::thread::sleep(POLL);
    }
    Ok(format!(
        "404 after {:.1} s, and at all {polls} looks over {} s after that",
        reached.as_secs_f32(),
        DISPUTE_HOLD.as_secs()
    ))
}

/// The joiner holds no descriptor for `lang`: a premise of every scenario.
fn premise_holds_not(http: &Http, node: &Node, lang: &Language) -> Result<(), String> {
    let (status, listing) = http.get(node, "/api/metamodels")?;
    if status != 200 {
        return Err(format!("{} /api/metamodels answered {status}", node.id));
    }
    if listing.to_string().contains(&lang.digest) {
        return Err(format!(
            "premise not met: {} already holds {}: {listing}",
            node.id, lang.name
        ));
    }
    Ok(())
}

/// Which way a joined model's history reached `node`, from its log. Reported
/// only: the claim is about the binding, not the path.
fn path_taken(node: &Node, model: &str) -> String {
    let text = node.log_text();
    let adopted = text
        .lines()
        .any(|line| line.contains("adopted state from") && line.contains(model));
    let turned = text
        .lines()
        .filter(|line| line.contains("cannot adopt") && line.contains(model))
        .count();
    match (adopted, turned) {
        (true, 0) => "a donor's snapshot, adopted".to_string(),
        (true, n) => format!("a donor's snapshot, adopted after {n} turned away"),
        (false, 0) => "no snapshot line in the log (delivered by delta or broadcast)".to_string(),
        (false, n) => format!("{n} snapshots turned away; the history came by delta sync"),
    }
}

/// A fresh model id nobody has minted.
fn unseen_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:032x}", nanos ^ (u128::from(std::process::id()) << 64))
}

fn short(digest: &str) -> String {
    digest.chars().take(12).collect()
}

// ---------------------------------------------------------------------------
// The verdict of one scenario
// ---------------------------------------------------------------------------

struct Checks {
    tag: String,
    passed: Vec<String>,
    failed: Vec<String>,
    tails: Vec<String>,
}

impl Checks {
    fn new(name: &str) -> Self {
        Checks {
            tag: name.to_uppercase(),
            passed: Vec::new(),
            failed: Vec::new(),
            tails: Vec::new(),
        }
    }

    /// Record one check; answers whether it passed.
    fn check(&mut self, name: &str, result: Result<String, String>) -> bool {
        match result {
            Ok(detail) => {
                eprintln!("{} PASS {name}: {detail}", self.tag);
                self.passed.push(name.to_string());
                true
            }
            Err(why) => {
                eprintln!("{} FAIL {name}: {why}", self.tag);
                self.failed.push(format!("{name}: {why}"));
                false
            }
        }
    }

    fn not_reached(&mut self, name: &str) {
        eprintln!("{} FAIL {name}: not reached, the join was not accepted", self.tag);
        self.failed.push(format!("{name}: not reached"));
    }

    fn log_tail(&mut self, node: &Node) {
        self.tails.push(format!(
            "--- {} log, alarms: {} ---\n{}",
            node.id,
            launch::alarms(&node.log),
            launch::tail(&node.log, 20)
        ));
    }

    fn finish(self) {
        eprintln!(
            "{} VERDICT passed {} failed {}",
            self.tag,
            self.passed.len(),
            self.failed.len()
        );
        if !self.failed.is_empty() {
            for tail in &self.tails {
                eprintln!("{tail}");
            }
            panic!(
                "{}: {} check(s) failed:\n  {}",
                self.tag,
                self.failed.len(),
                self.failed.join("\n  ")
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Languages and scratch
// ---------------------------------------------------------------------------

/// One metamodel: `json.ecore` under an nsURI of its own.
struct Language {
    name: &'static str,
    text: String,
    digest: String,
}

impl Language {
    fn retagged(name: &'static str, ns_uri: Option<&str>) -> Self {
        let mut value: Value = serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON");
        if let Some(ns_uri) = ns_uri {
            value["nsURI"] = json!(ns_uri);
        }
        let text = serde_json::to_string_pretty(&value).expect("a descriptor serializes");
        let reparsed: Value = serde_json::from_str(&text).expect("and parses back");
        Language {
            name,
            digest: metamodel_digest(&reparsed),
            text,
        }
    }
}

/// `lacked`: what the models are written in and the joiners never hold.
/// `held`: a language every node holds. `decoy`: held by the joiners only,
/// first in their directory, so it is what "the first descriptor" is there.
struct Languages {
    lacked: Language,
    held: Language,
    decoy: Language,
}

impl Languages {
    fn new() -> Self {
        let langs = Languages {
            lacked: Language::retagged("lacked", Some("http://www.example.org/json/ed20-lacked")),
            held: Language::retagged("held", None),
            decoy: Language::retagged("decoy", Some("http://www.example.org/json/ed20-decoy")),
        };
        assert!(
            langs.lacked.digest != langs.held.digest
                && langs.held.digest != langs.decoy.digest
                && langs.lacked.digest != langs.decoy.digest,
            "the three languages must be three metamodels"
        );
        langs
    }

    /// A digest, named by its language when it is one of the three.
    fn name(&self, digest: &str) -> String {
        for lang in [&self.lacked, &self.held, &self.decoy] {
            if lang.digest == digest {
                return format!("{} ({})", lang.name, short(digest));
            }
        }
        format!("unknown ({})", short(digest))
    }
}

/// A fresh scratch directory for one run, with the writers' descriptors
/// (`lacked` and `held`) and the joiners' (`decoy` first, then `held`).
fn scratch_for(name: &str, langs: &Languages) -> Result<(PathBuf, String, String), String> {
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let root = std::env::var_os("ED20_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let scratch = root.join(format!("moirai-{name}-{}-{run}", std::process::id()));
    let writers = scratch.join("metamodels-writers");
    let joiner = scratch.join("metamodels-joiner");
    for dir in [&writers, &joiner] {
        fs::create_dir_all(dir).map_err(|err| err.to_string())?;
    }
    let write = |dir: &Path, file: &str, lang: &Language| {
        fs::write(dir.join(file), &lang.text).map_err(|err| err.to_string())
    };
    write(&writers, "lacked.json", &langs.lacked)?;
    write(&writers, "held.json", &langs.held)?;
    write(&joiner, "0-decoy.json", &langs.decoy)?;
    write(&joiner, "1-held.json", &langs.held)?;
    Ok((
        scratch,
        writers.to_string_lossy().into_owned(),
        joiner.to_string_lossy().into_owned(),
    ))
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

/// Two nodes, each the other's only peer, healthy, meshed, in their loops.
fn pair(
    binary: &Path,
    scratch: &Path,
    first_env: &[(&str, &str)],
    second_env: &[(&str, &str)],
    first: &str,
    second: &str,
    http: &Http,
) -> Result<(Node, Node), String> {
    let specs = [
        launch::NodeSpec::new(first, first_env),
        launch::NodeSpec::new(second, second_env),
    ];
    let mut nodes: Vec<Node> = launch::launch(binary, scratch, &specs)?
        .into_iter()
        .map(|node| Node {
            id: node.id,
            http: node.http,
            listen: node.listen,
            child: node.child,
            log: node.log,
        })
        .collect();
    for node in &nodes {
        http.await_health(node)?;
    }
    wait_until(
        &format!("{first} and {second} to see each other"),
        STEP_TIMEOUT,
        || {
            Ok(nodes
                .iter()
                .all(|node| {
                    http.get(node, "/api/peers")
                        .map(|(_, peers)| connected(&peers) == 1)
                        .unwrap_or(false)
                })
                .then_some(()))
        },
    )?;
    for node in &nodes {
        http.await_loop(node)?;
    }
    let second = nodes.pop().expect("two nodes");
    let first = nodes.pop().expect("two nodes");
    Ok((first, second))
}

/// Start one node on ports it binds itself (NW.02) and hand it `peers`, the
/// `id:host:port,...` text of `PEERS`, empty for none. Its working directory
/// is the scratch directory, so no `metamodel.json` is picked up by accident.
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
        .current_dir(scratch)
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

    /// Create (`model` none) or join a model under a digest, as today; answers its id.
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

    /// The request under test: join `model` by its id alone. Accepted is a 200
    /// or 202 that echoes the id, says nothing was created, and names no
    /// metamodel, since the node cannot know one yet.
    fn join_by_id_alone(&self, node: &Node, model: &str) -> Result<String, String> {
        let (status, answer) =
            self.post(node, "/api/models", json!({ "model_id": model }).to_string())?;
        if status != 200 && status != 202 {
            return Err(format!("answered {status}: {answer}"));
        }
        if answer["model_id"] != json!(model) {
            return Err(format!("answered {status} for another id: {answer}"));
        }
        if answer["created"] != json!(false) {
            return Err(format!("answered {status} without `created: false`: {answer}"));
        }
        if let Some(named) = digest_named(&answer["metamodel_id"]) {
            return Err(format!(
                "answered {status} naming metamodel {named} at the join, before any history: {answer}"
            ));
        }
        Ok(format!("{status} {answer}"))
    }

    /// A join that names a digest, as today: 200, `created: false`, and the
    /// `metamodel_id` echoed back.
    fn join_naming(&self, node: &Node, model: &str, metamodel_id: &Value) -> Result<String, String> {
        let (status, answer) = self.post(
            node,
            "/api/models",
            json!({ "model_id": model, "metamodel_id": metamodel_id }).to_string(),
        )?;
        if status == 200
            && answer["model_id"] == json!(model)
            && answer["created"] == json!(false)
            && &answer["metamodel_id"] == metamodel_id
        {
            Ok(format!("{status} {answer}"))
        } else {
            Err(format!("answered {status}: {answer}"))
        }
    }

    fn submit(&self, node: &Node, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!("{} refused {op:?} ({status}): {answer}", node.id));
        }
        Ok(())
    }

    fn pause_all(&self, node: &Node) -> Result<(), String> {
        match self.post(node, "/api/pause-all", String::new())? {
            (200, _) => Ok(()),
            (status, body) => Err(format!("{} pause-all answered {status}: {body}", node.id)),
        }
    }

    fn resume_all(&self, node: &Node) -> Result<(), String> {
        match self.post(node, "/api/resume-all", String::new())? {
            (200, _) => Ok(()),
            (status, body) => Err(format!("{} resume-all answered {status}: {body}", node.id)),
        }
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

    /// The ids `GET /api/models` lists under `models`, sorted.
    fn hosted(&self, node: &Node) -> Result<Vec<String>, String> {
        let (_, body) = self.get(node, "/api/models")?;
        let mut ids: Vec<String> = body["models"]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|entry| entry["model_id"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        ids.sort();
        Ok(ids)
    }

    /// Wait until `node` lists `model` under `seen` (a premise: the joiner
    /// has seen the model it joins).
    fn until_seen(&self, node: &Node, model: &str) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to list {model} under `seen` (premise)", node.id),
            STEP_TIMEOUT,
            || {
                if let Ok((_, body)) = self.get(node, "/api/models") {
                    last = body;
                }
                Ok(last["seen"]
                    .as_array()
                    .is_some_and(|seen| seen.contains(&json!(model)))
                    .then_some(()))
            },
        )
        .map_err(|why| format!("{why}; it listed {last}"))
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
        "stable_version {} delivered {} pending {} retained {}",
        metrics["stable_version"],
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

/// `json.ecore`'s one operation the scenarios use: a one-character string put
/// under a key of the root object. The same slots in all three languages.
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
