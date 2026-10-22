//! NW.16a, P6's witness probe (vault `spec/items/NW.16a.md`): not a test, a
//! program. `nw16a/node_wrapper.sh` stands in for `model_node` while the
//! twelve suites run under `MOIRAI_TRANSPORT=zenoh`, and starts this probe
//! beside every node it starts, through the one ignored test below:
//!
//! ```text
//! <this test binary> --ignored --exact probe
//! ```
//!
//! with `NW16A_PROBE_PORTS_FILE`, `NW16A_PROBE_ID`, `NW16A_PROBE_PID` and
//! `NW16A_PROBE_OUT` set. It waits for the node's `PORTS_FILE`, then opens a
//! raw Zenoh session to the port the node reported and waits for the node's
//! liveliness token `moirai/<session>/node/<id>` (`nw16a::token_at`), and
//! writes one line to `NW16A_PROBE_OUT`: `token=<key>` when it saw it,
//! `NONE <why>` when not. A node on TCP has no Zenoh session on its port, so
//! a suite that ran its nodes on TCP leaves `NONE` lines, and `nw16a/p6.sh`
//! counts that run as no Zenoh run at all. Without the variables the test
//! does nothing.
#![cfg(feature = "network")]

mod nw16a;

use std::path::PathBuf;
use std::time::{Duration, Instant};

/// How long the node may take to report its ports, and then to show its
/// token: NW.02's launcher writes `PEERS_FILE` only once every node of a
/// cluster has reported.
const PORTS_WAIT: Duration = Duration::from_secs(60);
const TOKEN_WAIT: Duration = Duration::from_secs(60);

#[test]
#[ignore = "a program for nw16a/node_wrapper.sh, not a test; does nothing without NW16A_PROBE_*"]
fn probe() {
    let (Ok(ports), Ok(id), Ok(pid), Ok(out)) = (
        std::env::var("NW16A_PROBE_PORTS_FILE"),
        std::env::var("NW16A_PROBE_ID"),
        std::env::var("NW16A_PROBE_PID"),
        std::env::var("NW16A_PROBE_OUT"),
    ) else {
        return;
    };
    let started = Instant::now();
    let line = (|| {
        let path = PathBuf::from(&ports);
        let deadline = Instant::now() + PORTS_WAIT;
        let reported = loop {
            if let Some(parsed) = nw16a::read_ports_file(&path) {
                break parsed.map_err(|why| format!("NONE ports file: {why}"))?;
            }
            if !PathBuf::from(format!("/proc/{pid}")).exists() {
                return Err("NONE the node exited before reporting its ports".to_string());
            }
            if Instant::now() > deadline {
                return Err(format!("NONE no ports file within {PORTS_WAIT:?}"));
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        nw16a::token_at(reported.listen_port, &id, TOKEN_WAIT)
            .map(|key| format!("port={} token={key}", reported.listen_port))
            .map_err(|why| format!("port={} NONE {why}", reported.listen_port))
    })()
    .unwrap_or_else(|why| why);
    let record = format!("pid={pid} id={id} {line} after={:?}\n", started.elapsed());
    std::fs::write(&out, record).unwrap_or_else(|e| panic!("writing {out}: {e}"));
}
