//! NW.07: a model created through `POST /api/models` gets the id
//! `<32 lowercase hex>_<name>`, the name being its metamodel's `package` as
//! the descriptor declares it with everything but ASCII letters and digits
//! dropped and at most thirty-two kept, or the bare `<32 hex>` when nothing is
//! kept; another replica joins it by that id, naming the digest or by the id
//! alone, and serves the same document; the whole string is the model's
//! identity; and the bare form keeps working end to end.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/NW.07.md`. Two scenarios, each
//! on a pair of `model_node` processes of its own, `a` and `b`, each the
//! other's only peer, both serving the same descriptor directory:
//!
//! - K1, minting and joining. `a` creates one model under each of eleven
//!   descriptors (our four fixtures and seven `json.ecore` descriptors whose
//!   `package` this file chose) and each id must be the rule applied to the
//!   `package` this file wrote; the package a caller puts in its
//!   `metamodel_id` is not read; a descriptor posted while `a` runs names its
//!   models the same way. Then `b` lists a named model under `seen` in full,
//!   joins one naming its digest and another by its id alone, and each serves
//!   `a`'s document and takes edits both ways.
//! - K2, identity and the bare form. `b` hosts a named model of `a`'s and
//!   joins the same hex under three other spellings: all four are hosted as
//!   four models, and none of the three ever receives the model's history. A
//!   descriptor whose `package` keeps nothing mints the bare form, which `b`
//!   joins both ways and serves. A named and a bare model given the same
//!   edits serve the same document. Malformed ids are answered 400 and host
//!   nothing.
//!
//! Every expected id is computed here from the descriptor text this file
//! wrote and the rule of the claim (`reference_name`), and for the four
//! fixtures also written by hand; every document is read back from the
//! replica that wrote it.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! NW07_SCRATCH=/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.07 \
//!   nice -n 10 cargo test -j 2 -p moirai-interp --features network --test nw07_named_model_ids_l3 \
//!   -- --test-threads=1 --nocapture
//! ```
//!
//! The node tests follow the skip rule of `e2e_interp.rs`: no binary, print
//! `E2E-SKIP <scenario>: <why>` and return. A skip is not a pass.
#![cfg(feature = "network")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::testing::JSON_DESCRIPTOR;
use moirai_interp::{InstanceOp, ModelOp};
use moirai_semantics::{MetamodelSemantics, from_descriptor, metamodel_digest};
use serde_json::{Value, json};

#[path = "nw02/launch.rs"]
mod launch;

const UML_DESCRIPTOR: &str = include_str!("fixtures/uml.metamodel.json");
const BT_DESCRIPTOR: &str = include_str!("fixtures/bt.metamodel.json");
const NOTATION_DESCRIPTOR: &str = include_str!("fixtures/notation.metamodel.json");

/// The default log every node of a run hosts. Nothing writes it.
const LOG_ID: &str = "4e074e074e074e074e074e074e074e07";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a joiner has to bind a model and serve the donor's document.
/// Pre-registered in the spec; not to be changed without a ruling.
const JOIN_WINDOW: Duration = Duration::from_secs(30);
/// How long the other spellings of a model's hex are watched once the joiner
/// serves the model's latest document. Pre-registered in the spec.
const ISOLATION_WATCH: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(100);
static RUN_SEQ: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// The claim, over the wire
// ---------------------------------------------------------------------------

/// K1 of the spec: a created model carries its package's name, and another
/// replica joins it by that id, naming the digest or by the id alone.
#[test]
fn nw07_k1_a_created_model_carries_its_package_name_and_is_joined_by_that_id() {
    run_or_skip("nw07-k1", k1);
}

/// K2 of the spec: the whole id is the identity, and the bare form works end
/// to end.
#[test]
fn nw07_k2_the_whole_id_is_the_identity_and_the_bare_form_still_works() {
    run_or_skip("nw07-k2", k2);
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
// The rule, spelt by this file
// ---------------------------------------------------------------------------

/// The name a package gives: its ASCII letters and digits, in order and case,
/// the first thirty-two of them.
fn reference_name(package: &str) -> String {
    package
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(32)
        .collect()
}

/// Whether `id` is `<32 lowercase hex>` followed by `_<name>`, or by nothing
/// when `name` is empty. Answers the hex part when it is.
fn has_form(id: &str, name: &str) -> Option<String> {
    let hex = id.get(..32)?;
    if !hex.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) {
        return None;
    }
    let rest = &id[32..];
    let want = if name.is_empty() {
        String::new()
    } else {
        format!("_{name}")
    };
    (rest == want).then(|| hex.to_string())
}

fn describe_form(name: &str) -> String {
    if name.is_empty() {
        "<32 hex>".to_string()
    } else {
        format!("<32 hex>_{name}")
    }
}

// ---------------------------------------------------------------------------
// K1
// ---------------------------------------------------------------------------

fn k1(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new(name);
    let langs = Languages::new();
    let (scratch, dir) = scratch_for(name, &langs)?;
    let env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", dir.as_str())];
    let http = Http::new()?;
    let js = Json::new();
    let (a, b) = pair(binary, &scratch, &env, &env, "a", "b", &http)?;

    // ---- 1. one create per descriptor: the id carries the package's name ----
    for lang in langs.all() {
        let result = mint_check(&http, &a, lang);
        checks.check(
            &format!("k1.mint.{}", lang.label),
            result.map(|(_, detail)| detail),
        );
    }

    // The package a caller writes into its `metamodel_id` is not read.
    let decoy = json!({"digest": langs.uml.digest, "nsURI": "http://decoy", "package": "Decoy"});
    let decoyed = http
        .create(&a, &decoy)
        .and_then(|id| match has_form(&id, "simpleuml") {
            Some(_) => Ok(format!(
                "{id}, from the descriptor, not the caller's `Decoy`"
            )),
            None => Err(format!("created {id}, not {}", describe_form("simpleuml"))),
        });
    checks.check("k1.mint.the_callers_package_is_not_read", decoyed);

    // A descriptor posted while the node runs names its models the same way.
    let runtime = Language::retagged(
        "runtime",
        "Runtime2Added",
        "http://www.example.org/json/nw07-runtime",
    );
    let posted = http
        .post(&a, "/api/metamodels", runtime.text.clone())
        .and_then(|(status, body)| {
            if status == 200 || status == 201 {
                Ok(())
            } else {
                Err(format!("POST /api/metamodels answered {status}: {body}"))
            }
        })
        .and_then(|()| mint_check(&http, &a, &runtime).map(|(_, detail)| detail));
    checks.check(
        "k1.mint.a_descriptor_posted_while_running_names_its_models",
        posted,
    );

    // Two creates under one descriptor are two models.
    let twice = http.create(&a, &json!(langs.doc.digest)).and_then(|first| {
        http.create(&a, &json!(langs.doc.digest))
            .and_then(|second| {
                let (x, y) = (first.get(..32), second.get(..32));
                if first != second && x != y {
                    Ok(format!("{first} and {second}"))
                } else {
                    Err(format!("two creates gave {first} and {second}"))
                }
            })
    });
    checks.check("k1.mint.two_creates_are_two_models", twice);

    // ---- 2. M1, named: seen in full, joined naming its digest ----
    let m1 = http.create(&a, &json!(langs.doc.digest))?;
    http.submit(&a, &m1, &js.put("a0", 'a'))?;
    http.submit(&a, &m1, &js.put("a1", 'b'))?;
    let m1_doc = http.until_serves(&a, &m1, &json!({"a0": "a", "a1": "b"}))?;
    eprintln!("{} M1 is {m1}", checks.tag);
    checks.check(
        "k1.seen.the_whole_id_is_listed",
        seen_in_full(&http, &b, &m1),
    );

    let joined = http.join_naming(&b, &m1, &json!(langs.doc.digest));
    if checks.check("k1.join_naming.accepted", joined) {
        let watch = watch_binding(&http, &b, &m1, &langs.doc.digest, &m1_doc);
        checks.check("k1.join_naming.bound", watch.bound());
        checks.check("k1.join_naming.serves_the_donors_document", watch.served());
        checks.check(
            "k1.join_naming.edits_flow_both_ways",
            both_ways(&http, &js, &a, &b, &m1, &m1_doc, "1"),
        );
    } else {
        for check in [
            "k1.join_naming.bound",
            "k1.join_naming.serves_the_donors_document",
            "k1.join_naming.edits_flow_both_ways",
        ] {
            checks.not_reached(check);
        }
    }

    // ---- 3. M2, named: joined by its id alone (ED.20's path) ----
    let m2 = http.create(&a, &json!(langs.doc.digest))?;
    http.submit(&a, &m2, &js.put("m0", 'm'))?;
    let m2_doc = http.until_serves(&a, &m2, &json!({"m0": "m"}))?;
    eprintln!("{} M2 is {m2}", checks.tag);
    checks.check(
        "k1.seen.the_second_whole_id_is_listed",
        seen_in_full(&http, &b, &m2),
    );
    let joined = http.join_by_id_alone(&b, &m2);
    if checks.check("k1.join_by_id_alone.accepted", joined) {
        let watch = watch_binding(&http, &b, &m2, &langs.doc.digest, &m2_doc);
        checks.check(
            "k1.join_by_id_alone.bound_to_the_language_its_history_carries",
            watch.bound(),
        );
        checks.check(
            "k1.join_by_id_alone.serves_the_donors_document",
            watch.served(),
        );
        checks.check(
            "k1.join_by_id_alone.edits_flow_both_ways",
            both_ways(&http, &js, &a, &b, &m2, &m2_doc, "2"),
        );
    } else {
        for check in [
            "k1.join_by_id_alone.bound_to_the_language_its_history_carries",
            "k1.join_by_id_alone.serves_the_donors_document",
            "k1.join_by_id_alone.edits_flow_both_ways",
        ] {
            checks.not_reached(check);
        }
    }
    checks.log_tail(&a);
    checks.log_tail(&b);
    Ok(checks)
}

/// One create under `lang` on `node`: 201, `created: true`, the id in the
/// form the rule gives for the `package` this file wrote, listed under
/// `models` with the `metamodel_id` echoed, and every model route answering
/// for that exact id.
fn mint_check(http: &Http, node: &Node, lang: &Language) -> Result<(String, String), String> {
    let (status, answer) = http.post(
        node,
        "/api/models",
        json!({ "metamodel_id": lang.digest }).to_string(),
    )?;
    if status != 201 || answer["created"] != json!(true) {
        return Err(format!("create answered {status}: {answer}"));
    }
    let id = answer["model_id"]
        .as_str()
        .ok_or_else(|| format!("no model id in {answer}"))?
        .to_string();
    let want = lang.expected_name();
    if let Some(hand) = lang.hand
        && hand != want
    {
        return Err(format!(
            "harness: the hand-written name {hand:?} and the rule's {want:?} disagree"
        ));
    }
    if has_form(&id, &want).is_none() {
        return Err(format!(
            "package {:?} minted {id:?}, not {}",
            lang.package,
            describe_form(&want)
        ));
    }
    if answer["metamodel_id"] != json!(lang.digest) {
        return Err(format!("the answer does not echo the metamodel: {answer}"));
    }
    let listed = http.listed(node, &id)?;
    if listed != Some(json!(lang.digest)) {
        return Err(format!("{id} is listed with {listed:?}"));
    }
    for leaf in ["state", "state/identified", "metrics", "metamodel"] {
        let (status, body) = http.get(node, &format!("/api/model/{id}/{leaf}"))?;
        if status != 200 {
            return Err(format!(
                "GET /api/model/{id}/{leaf} answered {status}: {body}"
            ));
        }
        if leaf == "metamodel" && metamodel_digest(&body) != lang.digest {
            return Err(format!("{id}'s metamodel route serves another descriptor"));
        }
    }
    Ok((
        id.clone(),
        format!("package {:?} minted {id}", lang.package),
    ))
}

/// `node` lists `model`, the whole string, under `seen` within
/// `STEP_TIMEOUT`, and no id that is only its hex part.
fn seen_in_full(http: &Http, node: &Node, model: &str) -> Result<String, String> {
    let mut last = Value::Null;
    let started = Instant::now();
    loop {
        if let Ok((_, body)) = http.get(node, "/api/models") {
            last = body;
        }
        let seen: Vec<&str> = last["seen"]
            .as_array()
            .map(|ids| ids.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if seen.contains(&model) {
            let hex = &model[..model.len().min(32)];
            if model.len() > 32 && seen.contains(&hex) {
                return Err(format!("{} lists the bare {hex} beside {model}", node.id));
            }
            return Ok(format!(
                "{} lists {model} under seen after {:.1} s",
                node.id,
                started.elapsed().as_secs_f32()
            ));
        }
        if started.elapsed() > STEP_TIMEOUT {
            return Err(format!(
                "{} did not list {model} under seen within {} s; it listed {last}",
                node.id,
                STEP_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(POLL);
    }
}

/// The donor writes, the joiner serves it; the joiner writes, the donor
/// serves it; both end on the same document.
fn both_ways(
    http: &Http,
    js: &Json,
    donor: &Node,
    joiner: &Node,
    model: &str,
    doc: &Value,
    tag: &str,
) -> Result<String, String> {
    let mut want = doc.clone();
    let from_donor = format!("d{tag}");
    http.submit(donor, model, &js.put(&from_donor, 'd'))?;
    want[&from_donor] = json!("d");
    http.until_serves(joiner, model, &want)?;
    let from_joiner = format!("j{tag}");
    http.submit(joiner, model, &js.put(&from_joiner, 'j'))?;
    want[&from_joiner] = json!("j");
    http.until_serves(donor, model, &want)?;
    http.until_serves(joiner, model, &want)?;
    Ok(format!("both serve {want}"))
}

// ---------------------------------------------------------------------------
// K2
// ---------------------------------------------------------------------------

fn k2(binary: &Path, name: &str) -> Result<Checks, String> {
    let mut checks = Checks::new(name);
    let langs = Languages::new();
    let (scratch, dir) = scratch_for(name, &langs)?;
    let env = [("LOG_ID", LOG_ID), ("METAMODEL_DIR", dir.as_str())];
    let http = Http::new()?;
    let js = Json::new();
    let (a, b) = pair(binary, &scratch, &env, &env, "a", "b", &http)?;

    // ---- 1. M1, named, hosted by a and b ----
    let m1 = http.create(&a, &json!(langs.doc.digest))?;
    http.submit(&a, &m1, &js.put("a0", 'a'))?;
    let first_doc = http.until_serves(&a, &m1, &json!({"a0": "a"}))?;
    eprintln!("{} M1 is {m1}", checks.tag);
    let named = has_form(&m1, &langs.doc.expected_name()).is_some();
    checks.check(
        "k2.identity.the_model_is_named",
        if named {
            Ok(m1.clone())
        } else {
            Err(format!(
                "M1 is {m1}, not {}",
                describe_form(&langs.doc.expected_name())
            ))
        },
    );
    http.join_naming(&b, &m1, &json!(langs.doc.digest))
        .map_err(|why| format!("premise: b joins M1: {why}"))?;
    http.until_serves(&b, &m1, &first_doc)
        .map_err(|why| format!("premise: b serves M1: {why}"))?;

    // ---- 2. the same hex under other spellings ----
    let hex = m1.get(..32).unwrap_or(&m1).to_string();
    let variants: Vec<String> = [
        hex.clone(),
        format!("{hex}_nw07doc"),
        format!("{hex}_NW07DOC"),
        format!("{hex}_Other"),
    ]
    .into_iter()
    .filter(|variant| variant != &m1)
    .take(3)
    .collect();
    let mut joined_all = true;
    let mut answers = Vec::new();
    for variant in &variants {
        match http.join_naming(&b, variant, &json!(langs.doc.digest)) {
            Ok(answer) => answers.push(format!("{variant}: {answer}")),
            Err(why) => {
                joined_all = false;
                answers.push(format!("{variant}: {why}"));
            }
        }
    }
    let hosted = http.hosted(&b)?;
    let same_hex: Vec<&String> = hosted.iter().filter(|id| id.starts_with(&hex)).collect();
    let separate = joined_all
        && same_hex.len() == 4
        && variants.iter().all(|variant| hosted.contains(variant))
        && hosted.contains(&m1);
    checks.check(
        "k2.identity.other_spellings_are_other_models",
        if separate {
            Ok(format!("b hosts {same_hex:?}"))
        } else {
            Err(format!("b hosts {same_hex:?} after {}", answers.join("; ")))
        },
    );

    if joined_all {
        // A sync point after the joins: a's next edit reaches b's M1.
        http.submit(&a, &m1, &js.put("a1", 'b'))?;
        let doc = http.until_serves(&a, &m1, &json!({"a0": "a", "a1": "b"}))?;
        http.until_serves(&b, &m1, &doc)
            .map_err(|why| format!("premise: b follows M1 after the joins: {why}"))?;
        checks.check(
            "k2.identity.no_other_spelling_receives_the_models_history",
            isolation(&http, &b, &variants, &doc),
        );
        let on_a: Result<Vec<String>, String> = variants
            .iter()
            .map(|variant| {
                http.get(&a, &format!("/api/model/{variant}/state"))
                    .and_then(|(status, body)| {
                        if status == 404 {
                            Ok(format!("{variant} 404"))
                        } else {
                            Err(format!("a answered {status} for {variant}: {body}"))
                        }
                    })
            })
            .collect();
        checks.check(
            "k2.identity.the_donor_hosts_only_the_spelling_it_minted",
            on_a.map(|lines| lines.join(", ")),
        );
        let untouched = http.state(&a, &m1).and_then(|served| {
            if served == doc {
                Ok(format!("a serves {served}"))
            } else {
                Err(format!("a serves {served}, not {doc}"))
            }
        });
        checks.check("k2.identity.the_model_is_untouched", untouched);
    } else {
        for check in [
            "k2.identity.no_other_spelling_receives_the_models_history",
            "k2.identity.the_donor_hosts_only_the_spelling_it_minted",
            "k2.identity.the_model_is_untouched",
        ] {
            checks.not_reached(check);
        }
    }

    // ---- 3. the bare form, end to end ----
    let b1 = http.create(&a, &json!(langs.bare.digest))?;
    checks.check(
        "k2.bare.a_package_that_keeps_nothing_mints_the_bare_form",
        if has_form(&b1, "").is_some() {
            Ok(format!("package {:?} minted {b1}", langs.bare.package))
        } else {
            Err(format!("package {:?} minted {b1}", langs.bare.package))
        },
    );
    http.submit(&a, &b1, &js.put("o0", 'o'))?;
    let b1_doc = http.until_serves(&a, &b1, &json!({"o0": "o"}))?;
    let explicit = http
        .join_naming(&b, &b1, &json!(langs.bare.digest))
        .and_then(|_| {
            let watch = watch_binding(&http, &b, &b1, &langs.bare.digest, &b1_doc);
            watch.bound()?;
            watch.served()?;
            both_ways(&http, &js, &a, &b, &b1, &b1_doc, "3")
        });
    checks.check("k2.bare.join_naming_serves_and_follows", explicit);

    let b2 = http.create(&a, &json!(langs.bare.digest))?;
    http.submit(&a, &b2, &js.put("p0", 'p'))?;
    let b2_doc = http.until_serves(&a, &b2, &json!({"p0": "p"}))?;
    let alone = http.until_seen(&b, &b2).and_then(|()| {
        http.join_by_id_alone(&b, &b2).and_then(|_| {
            let watch = watch_binding(&http, &b, &b2, &langs.bare.digest, &b2_doc);
            watch.bound()?;
            watch.served()?;
            both_ways(&http, &js, &a, &b, &b2, &b2_doc, "4")
        })
    });
    checks.check("k2.bare.join_by_id_alone_binds_serves_and_follows", alone);

    // ---- 4. nothing else about a model changes: a twin ----
    let twin = (|| -> Result<String, String> {
        let named = http.create(&a, &json!(langs.doc.digest))?;
        let bare = http.create(&a, &json!(langs.bare.digest))?;
        for (key, ch) in [("t0", 'x'), ("t1", 'y'), ("t2", 'z')] {
            http.submit(&a, &named, &js.put(key, ch))?;
            http.submit(&a, &bare, &js.put(key, ch))?;
        }
        let want = json!({"t0": "x", "t1": "y", "t2": "z"});
        http.until_serves(&a, &named, &want)?;
        http.until_serves(&a, &bare, &want)?;
        let keys = |id: &str| -> Result<Vec<String>, String> {
            let metrics = http.metrics(&a, id)?;
            let mut keys: Vec<String> = metrics
                .as_object()
                .map(|fields| fields.keys().cloned().collect())
                .unwrap_or_default();
            keys.sort();
            Ok(keys)
        };
        let (named_keys, bare_keys) = (keys(&named)?, keys(&bare)?);
        if named_keys != bare_keys {
            return Err(format!(
                "the metrics of {named} carry {named_keys:?}, of {bare} {bare_keys:?}"
            ));
        }
        let (named_ops, bare_ops) = (
            http.metrics(&a, &named)?["ops_applied"].clone(),
            http.metrics(&a, &bare)?["ops_applied"].clone(),
        );
        if named_ops != bare_ops {
            return Err(format!(
                "ops_applied {named_ops} for {named}, {bare_ops} for {bare}"
            ));
        }
        Ok(format!(
            "{named} and {bare} both serve {want}, ops_applied {named_ops}"
        ))
    })();
    checks.check(
        "k2.twin.a_named_and_a_bare_model_serve_the_same_document_for_the_same_edits",
        twin,
    );

    // ---- 5. the routes ----
    let upper = hex.to_uppercase();
    let malformed = [
        format!("{hex}_"),
        format!("{hex}_a-b"),
        format!("{hex}_{}", "a".repeat(33)),
        format!("{upper}_x"),
        format!("{}_x", &hex[..31]),
        format!("{hex}0_x"),
        format!("{hex}__x"),
        format!("{hex}_x_y"),
        "_x".to_string(),
        format!("{hex}_%C3%A9"),
        format!("{hex}_x.json"),
    ];
    let before = http.hosted(&a)?;
    let mut wrong = Vec::new();
    for id in &malformed {
        let (status, _) = http.get(&a, &format!("/api/model/{id}/state"))?;
        if status != 400 {
            wrong.push(format!("GET /api/model/{id}/state answered {status}"));
        }
        // The body names the id as given; `%C3%A9` stands for `é` there.
        let raw = id.replace("%C3%A9", "é");
        let (status, answer) = http.post(
            &a,
            "/api/models",
            json!({"model_id": raw, "metamodel_id": langs.doc.digest}).to_string(),
        )?;
        if status != 400 {
            wrong.push(format!(
                "POST /api/models {raw:?} answered {status}: {answer}"
            ));
        }
    }
    let after = http.hosted(&a)?;
    if before != after {
        wrong.push(format!(
            "a hosts {after:?} after the refusals, {before:?} before"
        ));
    }
    checks.check(
        "k2.routes.a_malformed_id_is_answered_400_and_hosts_nothing",
        if wrong.is_empty() {
            Ok(format!(
                "{} spellings, each 400 on both routes",
                malformed.len()
            ))
        } else {
            Err(wrong.join("; "))
        },
    );
    let routed = (|| -> Result<String, String> {
        for leaf in ["state", "state/identified", "metrics", "metamodel"] {
            let (status, body) = http.get(&a, &format!("/api/model/{m1}/{leaf}"))?;
            if status != 200 {
                return Err(format!(
                    "GET /api/model/{m1}/{leaf} answered {status}: {body}"
                ));
            }
        }
        let mut want = http.state(&a, &m1)?;
        let batch = json!({"ops": [js.put("r0", 'r')]}).to_string();
        let (status, body) = http.post(&a, &format!("/api/model/{m1}/ops"), batch)?;
        if status != 200 {
            return Err(format!(
                "POST /api/model/{m1}/ops answered {status}: {body}"
            ));
        }
        want["r0"] = json!("r");
        http.until_serves(&a, &m1, &want)?;
        Ok(format!(
            "{m1}: state, identified state, metrics, metamodel, op and ops"
        ))
    })();
    checks.check(
        "k2.routes.a_named_id_is_served_on_every_model_route",
        routed,
    );
    checks.log_tail(&a);
    checks.log_tail(&b);
    Ok(checks)
}

/// For `ISOLATION_WATCH`, every look at every variant on `node`: its state
/// is never the model's document, and its log has delivered nothing.
fn isolation(http: &Http, node: &Node, variants: &[String], doc: &Value) -> Result<String, String> {
    let started = Instant::now();
    let mut looks = 0;
    while started.elapsed() <= ISOLATION_WATCH {
        for variant in variants {
            looks += 1;
            let (status, state) = http.get(node, &format!("/api/model/{variant}/state"))?;
            if status == 200 && &state == doc {
                return Err(format!(
                    "{variant} serves the model's document {state} at {:.1} s",
                    started.elapsed().as_secs_f32()
                ));
            }
            let metrics = http.metrics(node, variant)?;
            let delivered = metrics["delivered_ops"].as_u64().unwrap_or(0);
            if delivered != 0 {
                return Err(format!(
                    "{variant} delivered {delivered} operations at {:.1} s: {}",
                    started.elapsed().as_secs_f32(),
                    stability(&metrics)
                ));
            }
        }
        std::thread::sleep(POLL);
    }
    Ok(format!(
        "{looks} looks over {} s: no other spelling served the document or delivered anything",
        ISOLATION_WATCH.as_secs()
    ))
}

// ---------------------------------------------------------------------------
// Binding, as ED.20 reads it
// ---------------------------------------------------------------------------

/// The digest a `metamodel_id` names: `None` for null, absent, or an object
/// with no `digest`.
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

struct Watch {
    want: String,
    bound_after: Option<Duration>,
    served_after: Option<Duration>,
    foreign: Vec<String>,
    last: String,
}

impl Watch {
    fn bound(&self) -> Result<String, String> {
        if let Some(first) = self.foreign.first() {
            return Err(format!("a read named another metamodel: {first}"));
        }
        match self.bound_after {
            Some(after) => Ok(format!(
                "listing and metamodel route name {} after {:.1} s",
                short(&self.want),
                after.as_secs_f32()
            )),
            None => Err(format!(
                "not bound to {} within {} s; last look: {}",
                short(&self.want),
                JOIN_WINDOW.as_secs(),
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
                JOIN_WINDOW.as_secs(),
                self.last
            )),
        }
    }
}

/// Watch `model` on `node` until it is listed and served under `want` and
/// serves `doc`, or `JOIN_WINDOW` passes.
fn watch_binding(http: &Http, node: &Node, model: &str, want: &str, doc: &Value) -> Watch {
    let started = Instant::now();
    let mut watch = Watch {
        want: want.to_string(),
        bound_after: None,
        served_after: None,
        foreign: Vec::new(),
        last: String::new(),
    };
    while started.elapsed() <= JOIN_WINDOW {
        let look =
            (|| -> Result<(Option<Option<String>>, Option<String>, Option<Value>), String> {
                let (_, models) = http.get(node, "/api/models")?;
                let listed = models["models"].as_array().and_then(|entries| {
                    entries
                        .iter()
                        .find(|entry| entry["model_id"] == json!(model))
                        .map(|entry| digest_named(&entry["metamodel_id"]))
                });
                let route = match http.get(node, &format!("/api/model/{model}/metamodel"))? {
                    (200, body) => Some(metamodel_digest(&body)),
                    _ => None,
                };
                let state = match http.get(node, &format!("/api/model/{model}/state"))? {
                    (200, body) => Some(body),
                    _ => None,
                };
                Ok((listed, route, state))
            })();
        match look {
            Ok((listed, route, state)) => {
                if let Some(Some(named)) = &listed
                    && named != want
                {
                    watch
                        .foreign
                        .push(format!("listing names {}", short(named)));
                }
                if let Some(served) = &route
                    && served != want
                {
                    watch
                        .foreign
                        .push(format!("route serves {}", short(served)));
                }
                let listed_want = matches!(&listed, Some(Some(named)) if named == want);
                let route_want = route.as_deref() == Some(want);
                if listed_want && route_want && watch.bound_after.is_none() {
                    watch.bound_after = Some(started.elapsed());
                }
                if state.as_ref() == Some(doc) && watch.served_after.is_none() {
                    watch.served_after = Some(started.elapsed());
                }
                watch.last = format!(
                    "listed {listed:?}, route {route:?}, state {}",
                    state
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "none".into())
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

fn short(digest: &str) -> String {
    digest.chars().take(12).collect()
}

fn stability(metrics: &Value) -> String {
    format!(
        "stable_version {} delivered {} pending {}",
        metrics["stable_version"], metrics["delivered_ops"], metrics["pending_ops"]
    )
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
        eprintln!(
            "{} FAIL {name}: not reached, a step before it failed",
            self.tag
        );
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

/// One descriptor this file serves: its text, its digest, the `package` it
/// declares, and for the four fixtures the name written by hand.
struct Language {
    label: &'static str,
    package: String,
    text: String,
    digest: String,
    hand: Option<&'static str>,
}

impl Language {
    fn fixture(label: &'static str, text: &str, hand: &'static str) -> Self {
        let parsed: Value = serde_json::from_str(text).expect("a fixture is JSON");
        Language {
            label,
            package: parsed["package"]
                .as_str()
                .expect("a fixture declares a package")
                .to_string(),
            digest: metamodel_digest(&parsed),
            text: text.to_string(),
            hand: Some(hand),
        }
    }

    /// `json.ecore` under a package and an nsURI of this file's choosing:
    /// the digest is over the parsed descriptor, so each is a metamodel of its
    /// own, and one operation encoder writes all of them.
    fn retagged(label: &'static str, package: &str, ns_uri: &str) -> Self {
        let mut value: Value = serde_json::from_str(JSON_DESCRIPTOR).expect("the fixture is JSON");
        value["package"] = json!(package);
        value["nsURI"] = json!(ns_uri);
        let text = serde_json::to_string_pretty(&value).expect("a descriptor serializes");
        let reparsed: Value = serde_json::from_str(&text).expect("and parses back");
        Language {
            label,
            package: package.to_string(),
            digest: metamodel_digest(&reparsed),
            text,
            hand: None,
        }
    }

    fn expected_name(&self) -> String {
        reference_name(&self.package)
    }
}

struct Languages {
    uml: Language,
    bt: Language,
    json: Language,
    notation: Language,
    /// The document-bearing language: mixed case, kept as written.
    doc: Language,
    /// A package that keeps nothing: the bare form.
    bare: Language,
    mixed: Language,
    long: Language,
    dashed: Language,
    accented: Language,
    empty: Language,
}

impl Languages {
    fn new() -> Self {
        let langs = Languages {
            uml: Language::fixture("uml", UML_DESCRIPTOR, "simpleuml"),
            bt: Language::fixture("bt", BT_DESCRIPTOR, "behaviortree"),
            json: Language::fixture("json", JSON_DESCRIPTOR, "json"),
            notation: Language::fixture("notation", NOTATION_DESCRIPTOR, "notation"),
            doc: Language::retagged("doc", "NW07Doc", "http://www.example.org/json/nw07-doc"),
            bare: Language::retagged("bare", "-_- .", "http://www.example.org/json/nw07-bare"),
            mixed: Language::retagged(
                "mixed",
                "Simple_UML-v2.0",
                "http://www.example.org/json/nw07-mixed",
            ),
            long: Language::retagged(
                "long",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop",
                "http://www.example.org/json/nw07-long",
            ),
            dashed: Language::retagged(
                "dashed",
                &format!("{}{}y", "-".repeat(10), "x".repeat(32)),
                "http://www.example.org/json/nw07-dashed",
            ),
            accented: Language::retagged(
                "accented",
                "Ünïcödé",
                "http://www.example.org/json/nw07-accented",
            ),
            empty: Language::retagged("empty", "", "http://www.example.org/json/nw07-empty"),
        };
        let digests: std::collections::BTreeSet<&str> = langs
            .all()
            .iter()
            .map(|lang| lang.digest.as_str())
            .collect();
        assert_eq!(
            digests.len(),
            langs.all().len(),
            "every language is a metamodel of its own"
        );
        // The rule as the spec's table states it, for the crafted packages.
        assert_eq!(langs.mixed.expected_name(), "SimpleUMLv20");
        assert_eq!(
            langs.long.expected_name(),
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
        );
        assert_eq!(langs.dashed.expected_name(), "x".repeat(32));
        assert_eq!(langs.accented.expected_name(), "ncd");
        assert_eq!(langs.bare.expected_name(), "");
        assert_eq!(langs.empty.expected_name(), "");
        assert_eq!(langs.doc.expected_name(), "NW07Doc");
        langs
    }

    fn all(&self) -> Vec<&Language> {
        vec![
            &self.uml,
            &self.bt,
            &self.json,
            &self.notation,
            &self.doc,
            &self.bare,
            &self.mixed,
            &self.long,
            &self.dashed,
            &self.accented,
            &self.empty,
        ]
    }
}

/// A fresh scratch directory for one run, with every language's descriptor.
fn scratch_for(name: &str, langs: &Languages) -> Result<(PathBuf, String), String> {
    let run = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let root = std::env::var_os("NW07_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let scratch = root.join(format!("moirai-{name}-{}-{run}", std::process::id()));
    let dir = scratch.join("metamodels");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    for lang in langs.all() {
        fs::write(dir.join(format!("{}.json", lang.label)), &lang.text)
            .map_err(|err| err.to_string())?;
    }
    Ok((scratch, dir.to_string_lossy().into_owned()))
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

    /// A create under `metamodel_id`, as the editor sends it; answers the id.
    fn create(&self, node: &Node, metamodel_id: &Value) -> Result<String, String> {
        let (status, answer) = self.post(
            node,
            "/api/models",
            json!({ "metamodel_id": metamodel_id }).to_string(),
        )?;
        if status != 201 {
            return Err(format!("create on {} answered {status}: {answer}", node.id));
        }
        answer["model_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("no model id in {answer}"))
    }

    /// A join that names a digest: 200, the id echoed exactly, `created:
    /// false`, and the `metamodel_id` echoed back.
    fn join_naming(
        &self,
        node: &Node,
        model: &str,
        metamodel_id: &Value,
    ) -> Result<String, String> {
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

    /// A join by the id alone: 200 or 202, the id echoed exactly, `created:
    /// false`, and no metamodel named.
    fn join_by_id_alone(&self, node: &Node, model: &str) -> Result<String, String> {
        let (status, answer) = self.post(
            node,
            "/api/models",
            json!({ "model_id": model }).to_string(),
        )?;
        if status != 200 && status != 202 {
            return Err(format!("answered {status}: {answer}"));
        }
        if answer["model_id"] != json!(model) {
            return Err(format!("answered {status} for another id: {answer}"));
        }
        if answer["created"] != json!(false) {
            return Err(format!(
                "answered {status} without `created: false`: {answer}"
            ));
        }
        if let Some(named) = digest_named(&answer["metamodel_id"]) {
            return Err(format!(
                "answered {status} naming metamodel {named}: {answer}"
            ));
        }
        Ok(format!("{status} {answer}"))
    }

    fn submit(&self, node: &Node, model: &str, op: &ModelOp) -> Result<(), String> {
        let body = serde_json::to_string(op).map_err(|err| err.to_string())?;
        let (status, answer) = self.post(node, &format!("/api/model/{model}/op"), body)?;
        if status != 200 || answer["success"] != Value::Bool(true) {
            return Err(format!("{} refused {op:?} ({status}): {answer}", node.id));
        }
        Ok(())
    }

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

    /// The `metamodel_id` `GET /api/models` lists for `model`, `None` when it
    /// is not listed.
    fn listed(&self, node: &Node, model: &str) -> Result<Option<Value>, String> {
        let (_, body) = self.get(node, "/api/models")?;
        Ok(body["models"].as_array().and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry["model_id"] == json!(model))
                .map(|entry| entry["metamodel_id"].clone())
        }))
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

    fn until_seen(&self, node: &Node, model: &str) -> Result<(), String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to list {model} under `seen`", node.id),
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

    /// Wait until `node` serves `want` for `model`; answers what it serves.
    fn until_serves(&self, node: &Node, model: &str, want: &Value) -> Result<Value, String> {
        let mut last = Value::Null;
        wait_until(
            &format!("{} to serve {want} for {model}", node.id),
            STEP_TIMEOUT,
            || {
                if let Ok(state) = self.state(node, model) {
                    last = state;
                }
                Ok((&last == want).then_some(()))
            },
        )
        .map_err(|why| format!("{why}; it served {last}"))?;
        Ok(last)
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
/// under a key of the root object. The same slots in every retagged language.
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
