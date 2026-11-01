//! MD.03a L2 oracle: expected values come from literals and the shipped index.
mod md03a_common;
use md03a_common::*;
use moirai_interp::migrate::{migrate, rebase};
use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{broadcast::tcsb::StateSnapshot, log_id::LogId, replica::IsReplica};
use serde_json::Value;

#[test]
fn md03a_g1_all_9724_shipped_multisegment_names() {
    let manifest: Value = serde_json::from_str(include_str!(
        "../../../model-editor/public/libraries/syson-0793d2a/manifest.json"
    ))
    .unwrap();
    let names: Vec<&str> = manifest["qualifiedNames"]
        .as_object()
        .unwrap()
        .keys()
        .filter(|name| name.contains("::"))
        .map(String::as_str)
        .collect();
    assert_eq!(names.len(), 9724, "the pinned independent corpus changed");
    let mut missing = vec![];
    // Bounded batches avoid a quadratic history and still check every name,
    // even when every visibility assertion is red on the pinned base.
    for chunk in names.chunks(32) {
        let (b, mut r) = setup();
        let messages: Vec<_> = chunk
            .iter()
            .map(|name| {
                send_wire(
                    &mut r[0],
                    b.link(At::Board(0), b.related, name),
                    "G1 qualified intake must skip local class lookup",
                )
            })
            .collect();
        for message in &messages {
            deliver_all(&mut r, 0, message);
        }
        let expected = values(chunk);
        for replica in &r {
            for doc in [identified(replica), default_read(replica)] {
                let actual = refs(&doc, At::Board(0), "related");
                assert!(actual.is_subset(&expected), "G1 changed a qualified value");
                missing.extend(expected.difference(&actual).cloned());
            }
        }
    }
    println!("G1 corpus checked 9724 names on 3 replicas in both read formats");
    assert!(
        missing.is_empty(),
        "G1: {} missing qualified read observations; first: {:?}",
        missing.len(),
        &missing[..missing.len().min(8)]
    );
}

#[test]
fn md03a_g1_single_and_many_quoted_bytes() {
    let mut failures = vec![];
    for name in SAMPLES {
        let (b, mut r) = setup();
        // Bob and carol are paused until both single and many writes exist.
        let single = send_wire(
            &mut r[0],
            b.link(At::Board(0), b.blocker, name),
            "G1 single qualified intake",
        );
        let many = send_wire(
            &mut r[0],
            b.link(At::Board(0), b.related, name),
            "G1 many qualified intake",
        );
        for message in [&single, &many] {
            deliver_all(&mut r, 0, message);
        }
        for replica in &r {
            for doc in [identified(replica), default_read(replica)] {
                observe(&mut failures, &doc, "blocker", &[name], "G1 single");
                observe(&mut failures, &doc, "related", &[name], "G1 many");
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn lifecycle(order: [usize; 2]) {
    let (b, mut r) = setup();
    let local = id_at(&identified(&r[0]), At::Board(1));
    broadcast(
        &mut r,
        0,
        vec![
            b.link(At::Board(0), b.related, BASIC).unwrap(),
            b.link(At::Board(0), b.related, SPACE).unwrap(),
            b.link(At::Board(0), b.blocker, &local).unwrap(),
        ],
        "G2 seed qualified and local references",
    );
    let deleted = send(&mut r[0], b.delete_card(1), "G2 remove local target");
    let linked = send_wire(
        &mut r[1],
        b.link(At::Board(0), b.related, HASH),
        "G2 paused writer",
    );
    finish(&mut r, &[(0, vec![deleted]), (1, vec![linked])], order);
    let mut failures = vec![];
    for replica in &r {
        for doc in [identified(replica), default_read(replica)] {
            observe(
                &mut failures,
                &doc,
                "related",
                &[BASIC, SPACE, HASH],
                "G2 local deletion",
            );
            assert!(refs(&doc, At::Board(0), "blocker").is_empty());
        }
    }
    broadcast(
        &mut r,
        0,
        vec![b.unlink(At::Board(0), b.related, BASIC).unwrap()],
        "G2 causal full-value unlink",
    );
    for replica in &r {
        observe(
            &mut failures,
            &identified(replica),
            "related",
            &[SPACE, HASH],
            "G2 full-value unlink",
        );
    }
    // A concurrent unlink/relink of SPACE is add-wins in either order.
    let unlink = send_wire(
        &mut r[0],
        b.unlink(At::Board(0), b.related, SPACE),
        "G2 unlink race",
    );
    let relink = send_wire(
        &mut r[1],
        b.link(At::Board(0), b.related, SPACE),
        "G2 relink race",
    );
    finish(&mut r, &[(0, vec![unlink]), (1, vec![relink])], order);
    for replica in &r {
        observe(
            &mut failures,
            &identified(replica),
            "related",
            &[SPACE, HASH],
            "G2 add-wins",
        );
    }
    let remove = send(&mut r[0], b.delete_card(0), "G2 source removal");
    let concurrent = send_wire(
        &mut r[1],
        b.link(At::Board(0), b.related, UNICODE),
        "G2 source race",
    );
    finish(&mut r, &[(0, vec![remove]), (1, vec![concurrent])], order);
    for replica in &r {
        for doc in [identified(replica), default_read(replica)] {
            assert!(
                doc["cards"].as_array().unwrap().is_empty(),
                "G2 source survived"
            );
        }
    }
    println!("G2 completed deletion, full unlink, add-wins and source removal, order={order:?}");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn adoption(order: [usize; 2]) {
    let (b, mut r) = setup();
    let a = send_wire(
        &mut r[0],
        b.link(At::Board(0), b.related, BASIC),
        "G3 donor alice",
    );
    let z = send_wire(
        &mut r[1],
        b.link(At::Board(0), b.related, HASH),
        "G3 paused bob",
    );
    let donor = order[0];
    let snapshot: StateSnapshot<ModelOp> =
        serde_json::from_value(serde_json::to_value(r[donor].snapshot()).unwrap()).unwrap();
    let log: ModelLog =
        serde_json::from_value(serde_json::to_value(r[donor].state()).unwrap()).unwrap();
    // Replace carol by an empty replica; adoption, not replay, provides setup.
    r[2] = Rep::bootstrap_with_log_id("carol".into(), &NAMES, LogId::parse(MODEL).unwrap());
    r[2].adopt(snapshot, log);
    assert_eq!(
        title_at(&identified(&r[2]), At::Board(1)),
        "y",
        "G3 snapshot setup missing"
    );
    let mut failures = vec![];
    let first = if donor == 0 { BASIC } else { HASH };
    observe(
        &mut failures,
        &identified(&r[2]),
        "related",
        &[first],
        "G3 adopted snapshot",
    );
    finish(&mut r, &[(0, vec![a]), (1, vec![z])], order);
    for replica in &r {
        for doc in [identified(replica), default_read(replica)] {
            observe(
                &mut failures,
                &doc,
                "related",
                &[BASIC, HASH],
                "G3 late stream",
            );
        }
    }
    println!(
        "G3 adopted donor {}, then delivered the paused stream",
        NAMES[donor]
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn migration(order: [usize; 2], copy: bool) {
    let (b, mut r) = setup();
    let local = id_at(&identified(&r[0]), At::Board(1));
    broadcast(
        &mut r,
        0,
        vec![
            b.link(At::Board(0), b.related, BASIC).unwrap(),
            b.link(At::Board(0), b.related, HASH).unwrap(),
            b.link(At::Board(0), b.blocker, &local).unwrap(),
        ],
        "G4 before migration",
    );
    let descriptor = revised().to_string();
    if copy {
        let target = "dddddddddddddddddddddddddddddddd";
        let plan = migrate(r[0].state(), target, &descriptor).expect("G4 copy setup");
        let encoded = serde_json::to_value(&plan.ops).unwrap();
        let has_link = |name: &str| {
            fn walk(v: &Value, name: &str) -> bool {
                match v {
                    Value::Object(m) => {
                        m.get("Link").and_then(Value::as_str) == Some(name)
                            || m.values().any(|v| walk(v, name))
                    }
                    Value::Array(a) => a.iter().any(|v| walk(v, name)),
                    _ => false,
                }
            }
            walk(&encoded, name)
        };
        let mut failures = vec![];
        for name in [BASIC, HASH] {
            if !has_link(name) {
                failures.push(format!("G4 copy plan dropped {name:?}"));
            }
        }
        let cut = send(&mut r[0], plan.cut, "G4 cut");
        let late = send_wire(
            &mut r[1],
            b.link(At::Board(0), b.related, UNICODE),
            "G4 late old-log link",
        );
        finish(&mut r, &[(0, vec![cut]), (1, vec![late])], order);
        let mut copied = NAMES
            .map(|id| Rep::bootstrap_with_log_id(id.into(), &NAMES, LogId::parse(target).unwrap()));
        broadcast(&mut copied, 1, plan.ops, "G4 copied operations");
        for replica in &copied {
            for doc in [identified(replica), default_read(replica)] {
                observe(
                    &mut failures,
                    &doc,
                    "related",
                    &[BASIC, HASH],
                    "G4 copy read",
                );
            }
            let doc = identified(replica);
            let new_local = id_at(&doc, At::Board(1));
            assert_ne!(new_local, local, "G4 copy must mint a new local identity");
            assert_eq!(
                refs(&doc, At::Board(0), "blocker"),
                values(&[&new_local]),
                "G4 local copy link"
            );
        }
        println!("G4 copy plan executed and local reference rewired, order={order:?}");
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    } else {
        let op = rebase(r[0].state(), &descriptor, &[]).expect("G4 rebase setup");
        let a = send(&mut r[0], op, "G4 rebase");
        let z = send_wire(
            &mut r[1],
            b.link(At::Board(0), b.related, UNICODE),
            "G4 late pre-rebase qualified link",
        );
        finish(&mut r, &[(0, vec![a]), (1, vec![z])], order);
        for replica in &r {
            assert_eq!(
                id_at(&identified(replica), At::Board(1)),
                local,
                "G4 rebase identity"
            );
        }
        exact(&r, &[BASIC, HASH, UNICODE], "G4 rebase including late edit");
    }
}

#[test]
fn md03a_g2_lifecycle_ab() {
    lifecycle([0, 1]);
}
#[test]
fn md03a_g2_lifecycle_ba() {
    lifecycle([1, 0]);
}
#[test]
fn md03a_g3_snapshot_then_late_ab() {
    adoption([0, 1]);
}
#[test]
fn md03a_g3_snapshot_then_late_ba() {
    adoption([1, 0]);
}
#[test]
fn md03a_g4_copy_ab() {
    migration([0, 1], true);
}
#[test]
fn md03a_g4_copy_ba() {
    migration([1, 0], true);
}
#[test]
fn md03a_g4_rebase_ab() {
    migration([0, 1], false);
}
#[test]
fn md03a_g4_rebase_ba() {
    migration([1, 0], false);
}

#[test]
fn md03a_g6_single_segment_and_malformed_strings_stay_local() {
    let (b, mut r) = setup();
    for name in [
        "SI",
        "ScalarValues",
        "p1",
        "x",
        "ScalarValues:Real",
        "::Real",
        "ScalarValues::",
        "ScalarValues::::Real",
        "Scalar Values::Real",
        "ScalarValues::'Real",
        "ScalarValues::Real'",
        "ScalarValues::''",
        "ScalarValues::Real ",
        "ScalarValues::Real#alice@1.0",
        "ScalarValues::'Real'#alice@1.0",
        "ScalarValues::Real@1.0",
        "ScalarValues::'unterminated\\'",
        "ScalarValues::'Real'junk",
        "ScalarValues::'Real'::",
    ] {
        broadcast(
            &mut r,
            0,
            vec![b.link(At::Board(0), b.related, name).unwrap()],
            "G6 unresolved local spelling",
        );
        broadcast(
            &mut r,
            0,
            vec![b.link(At::Board(0), b.blocker, name).unwrap()],
            "G6 unresolved local single",
        );
    }
    for replica in &r {
        for doc in [identified(replica), default_read(replica)] {
            assert!(refs(&doc, At::Board(0), "related").is_empty());
            assert!(refs(&doc, At::Board(0), "blocker").is_empty());
        }
    }
}

#[test]
fn md03a_g6_existing_namespaces_keep_their_bytes() {
    let (b, mut r) = setup();
    let local = id_at(&identified(&r[0]), At::Board(1));
    let out = foreign(&local);
    for name in [&local, &out, LIBRARY, SYSML_LIBRARY] {
        broadcast(
            &mut r,
            0,
            vec![b.link(At::Board(0), b.related, name).unwrap()],
            "G6 existing namespace",
        );
    }
    exact(
        &r,
        &[&local, &out, LIBRARY, SYSML_LIBRARY],
        "G6 before local deletion",
    );
    broadcast(&mut r, 0, vec![b.delete_card(1)], "G6 local deletion");
    exact(
        &r,
        &[&out, LIBRARY, SYSML_LIBRARY],
        "G6 after local deletion",
    );
}
