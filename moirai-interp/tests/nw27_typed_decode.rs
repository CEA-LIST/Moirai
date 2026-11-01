//! NW.27 G1 with the node's own operation type: a `TransportMessage<ModelOp>` frame read
//! by the real TCP reader thread and the real Zenoh subscriber callback.
//!
//! Spec: `cea-cdrt-knowledge/spec/items/NW.27.md`. The frames are plain, well-formed JSON:
//! `{"type":"Raw","payload":<a Deep operation>}` at frame depth exactly L = 1024, which
//! must arrive with its payload intact, and at L + 8 and 2 x L, which must be refused with
//! a line naming the depth and 1024 while a shallow frame sent after them still arrives.
//! Each case runs in a fresh child process of this test binary, so a crash fails only it.
#![cfg(feature = "network")]
mod nw27;

#[path = "../../moirai-network/tests/nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use moirai_interp::ModelOp;
use moirai_network::{
    direct_transport::DirectTransport,
    transport::{CrdtTransport, TransportMessage},
    zenoh_transport::ZenohTransport,
};
use nw27::{L, deep_op, depth, names_depth_and, shallow_deep_op};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    process::Command,
    thread,
    time::{Duration, Instant},
};
use zenoh::Wait;

fn frame(op: &str) -> String {
    format!("{{\"type\":\"Raw\",\"payload\":{op}}}")
}

/// The payload as text, on a thread with room for any depth this file sends.
fn text_of(payload: ModelOp) -> String {
    thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || serde_json::to_string(&payload).unwrap())
        .unwrap()
        .join()
        .unwrap()
}

/// Wait for the Raw payload `want`; when `refuse`, every Raw that arrives must be the
/// shallow sentinel.
fn collect<T: CrdtTransport<Op = ModelOp>>(t: &mut T, want: &str, refuse: bool) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let _ = t.accept_connections();
        if let Some((_, TransportMessage::Raw { payload })) = t.try_recv().expect("receive") {
            let got = text_of(payload);
            if refuse {
                assert_eq!(
                    got,
                    shallow_deep_op(),
                    "G1 an operation frame deeper than {L} was delivered"
                );
            }
            if got == want {
                return;
            }
        }
        assert!(
            Instant::now() < end,
            "G1 the operation frame of depth {} did not arrive (limit {L})",
            depth(&frame(want))
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn tcp(text: &str, want: &str, refuse: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut t =
        DirectTransport::<ModelOp>::with_listener("bob".into(), listener, Default::default())
            .unwrap();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    writeln!(
        stream,
        "{{\"type\":\"Hello\",\"id\":\"alice\",\"metadata\":null}}"
    )
    .unwrap();
    writeln!(stream, "{text}").unwrap();
    if refuse {
        writeln!(stream, "{}", frame(&shallow_deep_op())).unwrap();
    }
    collect(&mut t, want, refuse);
}

fn zenoh_case(text: &str, want: &str, refuse: bool) {
    let sid = zenoh_rig::fresh_session("nw27typed");
    let mut t = ZenohTransport::<ModelOp>::open(
        "bob".into(),
        &sid,
        vec!["alice".into()],
        zenoh_rig::loopback_peer(&[]),
    )
    .unwrap();
    let publisher = zenoh_rig::raw_session(&[zenoh_rig::tcp_locator(t.locators())]);
    let key = format!("moirai/{sid}/to/bob/alice");
    thread::sleep(Duration::from_millis(750));
    publisher.put(&key, text.as_bytes()).wait().unwrap();
    if refuse {
        publisher
            .put(&key, frame(&shallow_deep_op()).into_bytes())
            .wait()
            .unwrap();
    }
    collect(&mut t, want, refuse);
}

fn run(case: &str, refuse: bool) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "nw27_typed_child", "--nocapture"])
        .env("NW27_CASE", case)
        .output()
        .unwrap();
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "NW27 {case}: child {}\n{log}",
        output.status
    );
    if refuse {
        assert!(
            names_depth_and(&log, L),
            "NW27 {case}: the refusal must be reported on one line naming the depth and the limit {L}: {log}"
        );
    }
}

#[test]
fn nw27_typed_child() {
    let Ok(case) = std::env::var("NW27_CASE") else {
        return;
    };
    let (transport, frame_depth) = case.split_once(':').unwrap();
    let frame_depth: usize = frame_depth.parse().unwrap();
    // The Raw wrapper adds one level.
    let op = deep_op(frame_depth - 1);
    let text = frame(&op);
    assert_eq!(depth(&text), frame_depth, "fixture");
    let refuse = frame_depth > L;
    let want = if refuse { shallow_deep_op() } else { op };
    match transport {
        "tcp" => tcp(&text, &want, refuse),
        "zenoh" => zenoh_case(&text, &want, refuse),
        _ => panic!("transport"),
    }
}

#[test]
fn nw27_g1_typed_tcp_exact_limit() {
    run(&format!("tcp:{L}"), false)
}
#[test]
fn nw27_g1_typed_tcp_refusal_1032() {
    run(&format!("tcp:{}", L + 8), true)
}
#[test]
fn nw27_g1_typed_tcp_refusal_2048() {
    run(&format!("tcp:{}", 2 * L), true)
}
#[test]
fn nw27_g1_typed_zenoh_exact_limit() {
    run(&format!("zenoh:{L}"), false)
}
#[test]
fn nw27_g1_typed_zenoh_refusal_1032() {
    run(&format!("zenoh:{}", L + 8), true)
}
#[test]
fn nw27_g1_typed_zenoh_refusal_2048() {
    run(&format!("zenoh:{}", 2 * L), true)
}
#[test]
fn nw27_g5_typed_shallow_frames() {
    // Unchanged behavior: an ordinary operation frame arrives on both transports.
    run("tcp:9", false);
    run("zenoh:9", false);
}
