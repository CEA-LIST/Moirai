//! MD.03 G0: protocol rule, including the old exact-string collision.
#![cfg(feature = "network")]
mod md03_common;
use md03_common::*;
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::{
    generic::LogReplica, log_id::LogId, message::EventMessage, transport::TransportMessage,
};
use moirai_protocol::{
    broadcast::tcsb::StateSnapshot,
    replica::{IsReplica, with_network_id_validation},
};
use serde_json::{Value, json};
use std::{
    any::Any,
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

const BAD: &[&str] = &[
    "bad#id",
    "bad@id",
    "bad:id",
    "bad id",
    "bad/id",
    "café",
    "",
    "0123456789abcdef0123456789abcdef#carol",
];
fn panic_text(e: &(dyn Any + Send)) -> String {
    if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else {
        String::new()
    }
}
fn clear(error: &str, id: &str) -> bool {
    let e = error.to_ascii_lowercase();
    (e.contains("replica") || e.contains("author")) && (error.contains(id) || id.is_empty())
}
fn replace(v: &mut Value, from: &str, to: &str) -> usize {
    match v {
        Value::String(s) if s == from => {
            *s = to.into();
            1
        }
        Value::Array(a) => a.iter_mut().map(|v| replace(v, from, to)).sum(),
        Value::Object(m) => m.values_mut().map(|v| replace(v, from, to)).sum(),
        _ => 0,
    }
}
#[test]
fn md03_g0_protocol_construction_and_collision_refused() {
    let mut failures = vec![];
    for bad in BAD {
        let result = std::panic::catch_unwind(|| {
            LogReplica::<ModelLog>::bootstrap_with_log_id(
                (*bad).into(),
                &[bad],
                LogId::parse(MODEL).unwrap(),
            )
        });
        match result {
            Err(e) if clear(&panic_text(e.as_ref()), bad) => {}
            _ => failures.push(format!("accepted or unclear local id {bad:?}")),
        }
        let result = std::panic::catch_unwind(|| {
            LogReplica::<ModelLog>::bootstrap_with_log_id(
                "alice".into(),
                &["alice", bad],
                LogId::parse(MODEL).unwrap(),
            )
        });
        match result {
            Err(e) if clear(&panic_text(e.as_ref()), bad) => {}
            _ => failures.push(format!("accepted or unclear member id {bad:?}")),
        }
    }
    assert!(failures.is_empty(), "G0: {}", failures.join("; "));
}
#[test]
fn md03_g0_valid_replica_ids_keep_their_bytes() {
    for id in [
        "alice", "bob", "carol", "a", "b", "c", "si05-a", "node_17", "A.Z_09-", ".", "_", "-", "0",
    ] {
        let rep = LogReplica::<ModelLog>::bootstrap_with_log_id(
            id.into(),
            &[id],
            LogId::parse(MODEL).unwrap(),
        );
        assert_eq!(rep.id(), id);
        let snap = serde_json::to_string(&rep.snapshot()).unwrap();
        assert!(serde_json::from_str::<StateSnapshot<ModelOp>>(&snap).is_ok());
    }
}
#[test]
fn md03_g0_events_and_snapshots_refuse_invalid_authors() {
    let (b, mut r) = setup();
    let event = send_wire(
        &mut r[0],
        b.link(At::Board(0), b.related, "bob@17.2"),
        "valid event",
    );
    let event = EventMessage::new(
        event.event().clone(),
        event.resolver().clone(),
        LogId::parse(MODEL).unwrap(),
    );
    let event_json = serde_json::to_value(event).unwrap();
    let snapshot = serde_json::to_value(r[0].snapshot()).unwrap();
    let log_json = serde_json::to_value(r[0].state()).unwrap();
    let mut failures = vec![];
    for bad in BAD {
        for author in ["alice", "carol"] {
            let mut e = event_json.clone();
            assert!(replace(&mut e, author, bad) > 0);
            match with_network_id_validation(|| {
                serde_json::from_value::<EventMessage<ModelOp>>(e.clone())
            }) {
                Err(error) if clear(&error.to_string(), bad) => {}
                _ => failures.push(format!("event author/member {author}->{bad:?}")),
            }
            match with_network_id_validation(|| {
                serde_json::from_value::<TransportMessage<ModelOp>>(
                    json!({"type":"Event","event":e}),
                )
            }) {
                Err(error) if clear(&error.to_string(), bad) => {}
                _ => failures.push(format!("event frame {author}->{bad:?}")),
            }
            let mut log = log_json.clone();
            assert!(replace(&mut log, author, bad) > 0);
            match with_network_id_validation(|| serde_json::from_value::<ModelLog>(log)) {
                Err(error) if clear(&error.to_string(), bad) => {}
                _ => failures.push(format!("transferred log author/member {author}->{bad:?}")),
            }
            let mut s = snapshot.clone();
            assert!(replace(&mut s, author, bad) > 0);
            match with_network_id_validation(|| serde_json::from_value::<StateSnapshot<ModelOp>>(s))
            {
                Err(error) if clear(&error.to_string(), bad) => {}
                _ => failures.push(format!("snapshot author/member {author}->{bad:?}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "G0: invalid authors accepted or unclear errors: {}",
        failures.join("; ")
    );
}
#[test]
fn md03_g0_identity_frames_refuse_invalid_replica_ids() {
    let mut failures = vec![];
    for bad in BAD {
        for frame in [
            json!({"type":"Hello","id":bad,"metadata":null}),
            json!({"type":"Ack","from":bad}),
            json!({"type":"Goodbye","id":bad}),
            json!({"type":"StateRequest","id":bad,"log_id":MODEL}),
        ] {
            match serde_json::from_value::<TransportMessage<ModelOp>>(frame.clone()) {
                Err(e) if clear(&e.to_string(), bad) => {}
                _ => failures.push(frame.to_string()),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "G0: invalid identity frames accepted or unclear errors: {}",
        failures.join("; ")
    );
}
#[test]
fn md03_g0_node_startup_refuses_prohibited_ids() {
    let binary = PathBuf::from(
        std::env::var("MOIRAI_E2E_INTERP_NODE_BIN")
            .expect("G0 requires the pinned model_node binary"),
    );
    assert!(binary.is_file());
    let dir = PathBuf::from("/volatile/home/vp242103/.cache/cea-crdt-scratch/MD.03/oracle/startup");
    fs::create_dir_all(&dir).unwrap();
    let mut failures = vec![];
    for (i, bad) in BAD.iter().enumerate() {
        let path = dir.join(format!("bad-{i}.log"));
        let out = fs::File::create(&path).unwrap();
        let mut child = Command::new(&binary)
            .env("REPLICA_ID", bad)
            .env("MOIRAI_TRANSPORT", "tcp")
            .env("LOG_ID", MODEL)
            .env("LISTEN_PORT", "0")
            .env("HTTP_PORT", "0")
            .env("HTTP_BIND_ADDR", "127.0.0.1")
            .env("PEERS", "")
            .env_remove("PEERS_FILE")
            .env_remove("PORTS_FILE")
            .env_remove("BOOTNODE_URL")
            .env_remove("DASHBOARD_URL")
            .env_remove("METAMODEL_DIR")
            .env_remove("METAMODEL_PATH")
            .stdout(Stdio::from(out.try_clone().unwrap()))
            .stderr(Stdio::from(out))
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(4);
        let status = loop {
            if let Some(s) = child.try_wait().unwrap() {
                break Some(s);
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        if status.is_none() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        let log = fs::read_to_string(path).unwrap();
        if !status.is_some_and(|s| !s.success()) || !clear(&log, bad) {
            failures.push(format!("{bad:?}: exit={status:?}, log={log}"));
        }
    }
    assert!(failures.is_empty(), "G0 startup: {}", failures.join("\n"));
}
