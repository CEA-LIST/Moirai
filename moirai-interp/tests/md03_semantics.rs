//! MD.03 L2: exact delivery orders and literal reference expectations.
mod md03_common;
use md03_common::*;
use moirai_interp::migrate::{migrate, rebase};
use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{broadcast::tcsb::StateSnapshot, log_id::LogId, replica::IsReplica};

fn g1(order: [usize; 2]) {
    let (b, mut r) = setup();
    let local = id_at(&identified(&r[0]), At::Board(1));
    let target = foreign("carol@17.2");
    let a = send_wire(
        &mut r[0],
        b.link(At::Board(0), b.blocker, &target),
        "alice foreign",
    );
    let z = send_wire(
        &mut r[1],
        b.link(At::Board(0), b.blocker, &local),
        "bob local",
    );
    let at = a.event().lamport().val();
    let bt = z.event().lamport().val();
    // Independent statement of the documented rotation, not FairPolicy::compare.
    let arank = (3 - at % 3) % 3;
    let brank = (1 + 3 - bt % 3) % 3;
    assert!(
        at > bt || (at == bt && arank > brank),
        "fixture must select alice's foreign link"
    );
    let lib = send_wire(
        &mut r[0],
        b.link(At::Board(0), b.related, LIBRARY),
        "library",
    );
    let model = send_wire(&mut r[1], b.link(At::Board(0), b.related, &target), "model");
    finish(&mut r, &[(0, vec![a, lib]), (1, vec![z, model])], order);
    for rep in &r {
        for doc in [identified(rep), default_read(rep)] {
            assert_eq!(
                refs(&doc, At::Board(0), "blocker"),
                values(&[&target]),
                "G1: foreign FairPolicy winner hidden"
            );
            assert_foreign(&doc, &[LIBRARY, &target], "G1");
        }
    }
}
fn g2(order: [usize; 2]) {
    let (b, mut r) = setup();
    let local = id_at(&identified(&r[0]), At::Board(1));
    let out = foreign(&local);
    broadcast(
        &mut r,
        0,
        vec![
            b.link(At::Board(0), b.related, &out).unwrap(),
            b.link(At::Board(0), b.blocker, &local).unwrap(),
        ],
        "link both namespaces",
    );
    let a = send(&mut r[0], b.delete_card(1), "remove local target");
    let c = send_wire(
        &mut r[2],
        b.link(At::Board(0), b.related, SYSML_LIBRARY),
        "offline library link",
    );
    finish(&mut r, &[(0, vec![a]), (2, vec![c])], order);
    for rep in &r {
        let doc = identified(rep);
        assert_foreign(&doc, &[&out, SYSML_LIBRARY], "G2");
        assert!(refs(&doc, At::Board(0), "blocker").is_empty());
    }
    let unlink = wire_op(b.reference_json(At::Board(0), b.related, "Unlink", &out)).unwrap();
    broadcast(
        &mut r,
        0,
        vec![unlink],
        "explicit unlink foreign full value",
    );
    assert_foreign(&identified(&r[1]), &[SYSML_LIBRARY], "G2 unlink");
}
fn g3(order: [usize; 2]) {
    let (b, mut r) = setup();
    let out = foreign("carol@17.2");
    broadcast(
        &mut r,
        0,
        vec![b.link(At::Board(0), b.related, &out).unwrap()],
        "visible before deletion",
    );
    assert_foreign(&identified(&r[0]), &[&out], "G3 visibility premise");
    let a = send(&mut r[0], b.delete_card(0), "delete source");
    let c = send_wire(
        &mut r[2],
        b.link(At::Board(0), b.related, LIBRARY),
        "concurrent relink source",
    );
    finish(&mut r, &[(0, vec![a]), (2, vec![c])], order);
    for rep in &r {
        let text = identified(rep).to_string();
        assert!(
            !text.contains(&out) && !text.contains(LIBRARY),
            "G3: dead source exposes links"
        );
    }
}
fn g4(order: [usize; 2]) {
    let (b, mut r) = setup();
    let out = foreign("carol@17.2");
    let a = send_wire(
        &mut r[0],
        b.link(At::Board(0), b.related, &out),
        "foreign donor edit",
    );
    let z = send_wire(
        &mut r[1],
        b.link(At::Board(0), b.related, LIBRARY),
        "library donor edit",
    );
    finish(&mut r, &[(0, vec![a]), (1, vec![z])], order);
    let snapshot: StateSnapshot<ModelOp> =
        serde_json::from_value(serde_json::to_value(r[0].snapshot()).unwrap()).unwrap();
    let log: ModelLog =
        serde_json::from_value(serde_json::to_value(r[0].state()).unwrap()).unwrap();
    let mut joiner = Rep::bootstrap_with_log_id(
        "dave".into(),
        &["alice", "bob", "carol", "dave"],
        LogId::parse(MODEL).unwrap(),
    );
    joiner.adopt(snapshot, log);
    assert_foreign(&identified(&joiner), &[&out, LIBRARY], "G4 state transfer");
}
fn g5(order: [usize; 2], copy: bool) {
    let (b, mut r) = setup();
    let out = foreign("carol@17.2");
    broadcast(
        &mut r,
        0,
        vec![
            b.link(At::Board(0), b.related, &out).unwrap(),
            b.link(At::Board(0), b.related, LIBRARY).unwrap(),
        ],
        "pre-migration links",
    );
    let d = revised().to_string();
    if copy {
        let local = id_at(&identified(&r[0]), At::Board(1));
        broadcast(
            &mut r,
            0,
            vec![b.link(At::Board(0), b.blocker, &local).unwrap()],
            "local copy reference",
        );
        let plan = migrate(r[0].state(), "dddddddddddddddddddddddddddddddd", &d).unwrap();
        let cut = send(&mut r[0], plan.cut, "cut");
        let late = send_wire(
            &mut r[1],
            b.link(At::Board(0), b.related, SYSML_LIBRARY),
            "late old-log link",
        );
        finish(&mut r, &[(0, vec![cut]), (1, vec![late])], order);
        let mut copied = NAMES.map(|id| {
            Rep::bootstrap_with_log_id(
                id.into(),
                &NAMES,
                LogId::parse("dddddddddddddddddddddddddddddddd").unwrap(),
            )
        });
        broadcast(&mut copied, 0, plan.ops, "copy plan");
        for rep in &copied {
            let doc = identified(rep);
            assert_foreign(&doc, &[&out, LIBRARY], "G5 copy");
            assert_eq!(
                refs(&doc, At::Board(0), "blocker"),
                values(&[&id_at(&doc, At::Board(1))])
            );
        }
    } else {
        let op = rebase(r[0].state(), &d, &[]).unwrap();
        let a = send(&mut r[0], op, "rebase");
        let z = send_wire(
            &mut r[1],
            b.link(At::Board(0), b.related, SYSML_LIBRARY),
            "late pre-rebase foreign edit",
        );
        finish(&mut r, &[(0, vec![a]), (1, vec![z])], order);
        for rep in &r {
            assert_foreign(
                &identified(rep),
                &[&out, LIBRARY, SYSML_LIBRARY],
                "G5 rebase",
            );
        }
    }
}
#[test]
fn md03_g1_ab() {
    g1([0, 1])
}
#[test]
fn md03_g1_ba() {
    g1([1, 0])
}
#[test]
fn md03_g2_ab() {
    g2([0, 1])
}
#[test]
fn md03_g2_ba() {
    g2([1, 0])
}
#[test]
fn md03_g3_ab() {
    g3([0, 1])
}
#[test]
fn md03_g3_ba() {
    g3([1, 0])
}
#[test]
fn md03_g4_ab() {
    g4([0, 1])
}
#[test]
fn md03_g4_ba() {
    g4([1, 0])
}
#[test]
fn md03_g5_copy_ab() {
    g5([0, 1], true)
}
#[test]
fn md03_g5_copy_ba() {
    g5([1, 0], true)
}
#[test]
fn md03_g5_rebase_ab() {
    g5([0, 1], false)
}
#[test]
fn md03_g5_rebase_ba() {
    g5([1, 0], false)
}
#[test]
fn md03_g6_local_late_delivery_and_no_foreign_bytes() {
    let (b, mut r) = setup();
    let before = identified(&r[0]);
    // Frozen at 7ef4803 before implementation; both complete read formats.
    const IDENTIFIED: &str = r#"{"__id":"alice@2.0","cards":[{"__id":"alice@3.1","blocker":null,"eClass":"Card","related":[],"title":"x"},{"__id":"alice@4.1","blocker":null,"eClass":"Card","related":[],"title":"y"}],"eClass":"Board","lanes":[]}"#;
    const PLAIN: &str = r#"{"cards":[{"blocker":null,"eClass":"Card","related":[],"title":"x"},{"blocker":null,"eClass":"Card","related":[],"title":"y"}],"eClass":"Board","lanes":[]}"#;
    assert_eq!(
        before.to_string(),
        IDENTIFIED,
        "G6 baseline identified bytes"
    );
    assert_eq!(
        default_read(&r[0]).to_string(),
        PLAIN,
        "G6 baseline default bytes"
    );
    println!("MD03-G6 identified={}", before);
    println!("MD03-G6 default={}", default_read(&r[0]));
    let created = send(&mut r[2], b.new_card(2), "offline local target");
    let local = id_at(&identified(&r[2]), At::Board(2));
    let link = send_wire(
        &mut r[0],
        b.link(At::Board(0), b.related, &local),
        "not-yet-delivered local link",
    );
    r[1].receive(link.clone());
    assert!(refs(&identified(&r[0]), At::Board(0), "related").is_empty());
    assert!(refs(&identified(&r[1]), At::Board(0), "related").is_empty());
    r[0].receive(created.clone());
    r[1].receive(created);
    r[2].receive(link);
    for rep in &r {
        assert_eq!(
            refs(&identified(rep), At::Board(0), "related"),
            values(&[&local])
        );
    }
    broadcast(&mut r, 0, vec![b.delete_card(2)], "local target removal");
    for rep in &r {
        assert_eq!(
            default_read(rep).to_string(),
            PLAIN,
            "G6 restored default bytes"
        );
        assert_eq!(
            identified(rep),
            before,
            "G6: no-foreign baseline bytes changed"
        );
    }
}
#[test]
fn md03_g6_malformed_foreign_shapes_do_not_gain_visibility() {
    let (b, mut r) = setup();
    for bad in [
        "C#carol@17.2",
        "cccccccccccccccccccccccccccccccc#bad#carol@17.2",
        "cccccccccccccccccccccccccccccccc#carol@x.2",
        "sysml:DataType kermllibrary:///not-a-uuid#not-a-uuid",
    ] {
        broadcast(
            &mut r,
            0,
            vec![b.link(At::Board(0), b.related, bad).unwrap()],
            "old unresolved value",
        );
    }
    assert!(refs(&identified(&r[0]), At::Board(0), "related").is_empty());
}

#[test]
fn md03_g1_library_single_and_disambiguated_identity_forms() {
    let (b, mut r) = setup();
    for out in [
        LIBRARY.to_owned(),
        SYSML_LIBRARY.to_owned(),
        foreign("carol@17:42.2"),
        foreign("carol@017.02"),
    ] {
        broadcast(
            &mut r,
            0,
            vec![b.link(At::Board(0), b.blocker, &out).unwrap()],
            "accepted foreign form",
        );
        for rep in &r {
            assert_eq!(
                refs(&identified(rep), At::Board(0), "blocker"),
                values(&[&out]),
                "G1 exact single foreign form"
            );
        }
    }
}
