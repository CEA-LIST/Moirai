//! NW.27 oracle at the transport and state-transfer boundaries (G1, G4 L1, G5 L1).
//!
//! Spec: `cea-cdrt-knowledge/spec/items/NW.27.md`. Every input is plain, well-formed JSON
//! that this file generates: nested arrays inside a `Raw` frame, at depths 192, L = 1024,
//! L + 8 = 1032 and 2 x L = 2048. Each case runs in a fresh child process of this test
//! binary, so a crash in one case fails that case alone. No decoder API is assumed: the
//! oracle drives the public transports and `LogPayload`, and reads what arrives.
use moirai_network::{
    direct_transport::DirectTransport,
    relay_transport::RelayTransport,
    state_transfer::LogPayload,
    transport::{CrdtTransport, TransportMessage},
};
use serde_json::{json, Value};
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    process::Command,
    thread,
    time::{Duration, Instant},
};
#[cfg(feature = "zenoh")]
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;
const L: usize = 1024;
fn nested(n: usize) -> String {
    format!("{}0{}", "[".repeat(n), "]".repeat(n))
}
fn raw(depth: usize) -> String {
    format!("{{\"type\":\"Raw\",\"payload\":{}}}", nested(depth - 1))
}
fn depth(s: &str) -> usize {
    let (mut d, mut max, mut quoted, mut escape) = (0, 0, false, false);
    for b in s.bytes() {
        if quoted {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                quoted = false;
            }
        } else {
            match b {
                b'"' => quoted = true,
                b'[' | b'{' => {
                    d += 1;
                    max = max.max(d)
                }
                b']' | b'}' => d -= 1,
                _ => (),
            }
        }
    }
    max
}
fn collect<T: CrdtTransport<Op = Value>>(t: &mut T, want: &str, reject: bool) {
    let end = Instant::now() + Duration::from_secs(8);
    loop {
        t.accept_connections().expect("accept");
        match t.try_recv().expect("receive") {
            Some((_, TransportMessage::Raw { payload })) => {
                if reject {
                    assert_eq!(
                        payload,
                        json!("sentinel"),
                        "G1 over-limit content was delivered"
                    );
                }
                let actual = serde_json::to_string(&payload).unwrap();
                if actual == want {
                    return;
                }
            }
            _ => (),
        }
        assert!(Instant::now()<end,"G1 missing payload of depth {}: peer decoder must accept valid JSON above 128 and within {L}",depth(want));
        thread::sleep(Duration::from_millis(5));
    }
}
fn tcp(text: &str, want: &str, reject: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut t = DirectTransport::<Value>::with_listener("bob".into(), listener, Default::default())
        .unwrap();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    writeln!(
        stream,
        "{{\"type\":\"Hello\",\"id\":\"alice\",\"metadata\":null}}"
    )
    .unwrap();
    writeln!(stream, "{text}").unwrap();
    if reject {
        writeln!(stream, "{{\"type\":\"Raw\",\"payload\":\"sentinel\"}}").unwrap();
    }
    collect(&mut t, want, reject);
}
#[cfg(feature = "zenoh")]
fn zenoh_case(text: &str, want: &str, reject: bool) {
    use moirai_network::zenoh_transport::ZenohTransport;
    use zenoh::Wait;
    let sid = zenoh_rig::fresh_session("nw27");
    let mut t = ZenohTransport::<Value>::open(
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
    if reject {
        publisher
            .put(
                &key,
                b"{\"type\":\"Raw\",\"payload\":\"sentinel\"}".as_slice(),
            )
            .wait()
            .unwrap();
    }
    collect(&mut t, want, reject);
}
fn relay(text: &str, want: &str, reject: bool, chunked: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut t =
        RelayTransport::<Value>::connect("bob".into(), "nw27".into(), addr.to_string()).unwrap();
    let (mut socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut hello = String::new();
    BufReader::new(socket.try_clone().unwrap())
        .read_line(&mut hello)
        .unwrap();
    assert!(hello.contains("bob"), "relay handshake");
    if chunked {
        let mid = text.len() / 2;
        for (index, piece) in [&text[..mid], &text[mid..]].iter().enumerate() {
            writeln!(socket,"{}",json!({"from":"alice","msg":{"type":"Chunk","id":27,"index":index,"count":2,"data":piece}})).unwrap();
        }
    } else {
        writeln!(socket, "{{\"from\":\"alice\",\"msg\":{text}}}").unwrap();
    }
    if reject {
        writeln!(
            socket,
            "{{\"from\":\"alice\",\"msg\":{{\"type\":\"Raw\",\"payload\":\"sentinel\"}}}}"
        )
        .unwrap();
    }
    collect(&mut t, want, reject);
}
fn run(case: &str, diagnostic: bool) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "nw27_child", "--nocapture"])
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
    if diagnostic {
        assert!(
            names_depth_and_limit(&log),
            "NW27 {case}: the refusal must be reported on one line naming the depth and the limit {L}: {log}"
        );
    }
}
/// One output line that says "depth" and carries the limit as a whole number.
fn names_depth_and_limit(log: &str) -> bool {
    let limit = L.to_string();
    log.lines().any(|line| {
        line.to_ascii_lowercase().contains("depth")
            && line
                .split(|c: char| !c.is_ascii_digit())
                .any(|number| number == limit)
    })
}
#[test]
fn nw27_child() {
    let Ok(case) = std::env::var("NW27_CASE") else {
        return;
    };
    if case == "state-deep" {
        let mut v = json!(0);
        for _ in 0..256 {
            v = Value::Array(vec![v]);
        }
        let bytes = serde_json::to_vec(&v).unwrap();
        let payload = LogPayload::encode(v.clone(), &bytes);
        assert!(
            matches!(payload, LogPayload::Deflate(_)),
            "G4 compressed premise"
        );
        assert_eq!(
            payload.decode(),
            Some(v),
            "G4 decompressed state above 128 must decode"
        );
        return;
    }
    if case.starts_with("state-deflate-over:") {
        // The state as it travels: the production encoder's output for a plain nested array.
        let n: usize = case.split(':').nth(1).unwrap().parse().unwrap();
        let text = nested(n);
        assert_eq!(depth(&text), n);
        let payload = LogPayload::encode(Value::Null, text.as_bytes());
        assert!(
            matches!(payload, LogPayload::Deflate(_)),
            "G4 premise: the encoder compresses this state"
        );
        assert!(
            payload.decode().is_none(),
            "G4 inflated state of depth {n} accepted"
        );
        assert_eq!(
            LogPayload::Plain(json!("sentinel")).decode(),
            Some(json!("sentinel"))
        );
        return;
    }
    if case.starts_with("state-over:") {
        let n: usize = case.split(':').nth(1).unwrap().parse().unwrap();
        let mut plain = json!(0);
        for _ in 0..n {
            plain = Value::Array(vec![plain]);
        }
        assert!(
            LogPayload::Plain(plain).decode().is_none(),
            "G4 over-limit plain state accepted"
        );
        assert_eq!(
            LogPayload::Plain(json!("sentinel")).decode(),
            Some(json!("sentinel"))
        );
        return;
    }
    let parts: Vec<_> = case.split(':').collect();
    let transport = parts[0];
    let mode = parts[1];
    let n = match mode {
        "shallow" => 8,
        "deep" => 192,
        "limit" => L,
        "over" => L + 8,
        "double" => 2 * L,
        "trailing" => 8,
        _ => panic!("case"),
    };
    let reject = matches!(mode, "over" | "double" | "trailing");
    let mut text = raw(n);
    if mode == "trailing" {
        text.push_str(" true");
    }
    let expected = if reject {
        "\"sentinel\"".into()
    } else {
        nested(n - 1)
    };
    match transport {
        "tcp" => tcp(&text, &expected, reject),
        #[cfg(feature = "zenoh")]
        "zenoh" => zenoh_case(&text, &expected, reject),
        "relay" => relay(&text, &expected, reject, false),
        "chunks" => relay(&text, &expected, reject, true),
        _ => panic!("transport"),
    }
}
#[test]
fn nw27_g1_tcp_deep() {
    run("tcp:deep", false)
}
#[cfg(feature = "zenoh")]
#[test]
fn nw27_g1_zenoh_deep() {
    run("zenoh:deep", false)
}
#[test]
fn nw27_g1_relay_deep() {
    run("relay:deep", false)
}
#[test]
fn nw27_g1_reassembled_deep() {
    run("chunks:deep", false)
}
#[test]
fn nw27_g1_exact_limit_tcp() {
    run("tcp:limit", false)
}
#[test]
fn nw27_g1_tcp_refusal_1032() {
    run("tcp:over", true)
}
#[test]
fn nw27_g1_tcp_refusal_2048() {
    run("tcp:double", true)
}
#[cfg(feature = "zenoh")]
#[test]
fn nw27_g1_exact_limit_zenoh() {
    run("zenoh:limit", false)
}
#[cfg(feature = "zenoh")]
#[test]
fn nw27_g1_zenoh_refusal_1032() {
    run("zenoh:over", true)
}
#[cfg(feature = "zenoh")]
#[test]
fn nw27_g1_zenoh_refusal_2048() {
    run("zenoh:double", true)
}
#[test]
fn nw27_g1_relay_refusal_1032() {
    run("relay:over", true)
}
#[test]
fn nw27_g1_relay_refusal_2048() {
    run("relay:double", true)
}
#[test]
fn nw27_g1_chunks_refusal_1032() {
    run("chunks:over", true)
}
#[test]
fn nw27_g1_chunks_refusal_2048() {
    run("chunks:double", true)
}
#[test]
fn nw27_g4_deep_compressed_state() {
    run("state-deep", false)
}
#[test]
fn nw27_g4_plain_refusal_1032() {
    run("state-over:1032", true)
}
#[test]
fn nw27_g4_plain_refusal_2048() {
    run("state-over:2048", true)
}
#[test]
fn nw27_g4_inflated_refusal_1032() {
    run("state-deflate-over:1032", true)
}
#[test]
fn nw27_g4_inflated_refusal_2048() {
    run("state-deflate-over:2048", true)
}
#[test]
fn nw27_g5_ordinary_bytes_and_shallow_boundaries() {
    let fixtures = [
        (
            TransportMessage::<Value>::Hello {
                id: "alice".into(),
                metadata: None,
            },
            r#"{"type":"Hello","id":"alice","metadata":null}"#,
        ),
        (
            TransportMessage::<Value>::Ack {
                from: "alice".into(),
            },
            r#"{"type":"Ack","from":"alice"}"#,
        ),
        (
            TransportMessage::<Value>::Goodbye { id: "alice".into() },
            r#"{"type":"Goodbye","id":"alice"}"#,
        ),
        (
            TransportMessage::<Value>::Raw {
                payload: json!({"brackets":"[{}] \\\"", "v":[1,true,null]}),
            },
            r#"{"type":"Raw","payload":{"brackets":"[{}] \\\"","v":[1,true,null]}}"#,
        ),
    ];
    for (message, bytes) in fixtures {
        assert_eq!(serde_json::to_string(&message).unwrap(), bytes);
    }
    assert_eq!(
        depth(r#"{"[": ["\\\"{", {}]}"#),
        3,
        "scanner excludes quoted brackets"
    );
    for t in ["tcp", "relay", "chunks"] {
        run(&format!("{t}:shallow"), false);
        run(&format!("{t}:trailing"), false);
    }
    #[cfg(feature = "zenoh")]
    {
        run("zenoh:shallow", false);
        run("zenoh:trailing", false);
    }
}
