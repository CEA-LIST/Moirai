//! NW.15: `MOIRAI_TRANSPORT=zenoh` puts a node built by its unchanged
//! constructor on Zenoh (vault `spec/items/NW.15.md`, note section 4): the
//! node's session declares its token `moirai/<SESSION_ID>/node/<id>`, seen
//! by a raw observer the node reaches through the `ZENOH_CONFIG` file it was
//! given, used as is (`tcp/127.0.0.1:0`, scouting off).
//!
//! One test in its own binary, because it sets process variables.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw15_choice_zenoh
//! ```

#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::generic::Node;
use moirai_network::HashMap;
use moirai_protocol::log_id::LogId;
use moirai_protocol::state::po_log::VecLog;
use zenoh::sample::{Sample, SampleKind};
use zenoh::Wait;

#[test]
fn moirai_transport_zenoh_puts_a_node_built_by_its_constructor_on_zenoh() {
    let session = zenoh_rig::fresh_session("e");
    let observer = zenoh_rig::raw_session(&[]);
    let observer_at = zenoh_rig::raw_locator(&observer);
    let tokens: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = Arc::clone(&tokens);
    let _presence = observer
        .liveliness()
        .declare_subscriber(format!("moirai/{session}/node/*"))
        .history(true)
        .callback(move |sample: Sample| {
            if sample.kind() == SampleKind::Put {
                sink.lock()
                    .unwrap()
                    .push(sample.key_expr().as_str().to_string());
            }
        })
        .wait()
        .expect("the observer's liveliness subscriber");

    let file = Path::new(env!("CARGO_TARGET_TMPDIR")).join("nw15_choice_zenoh.json5");
    std::fs::write(
        &file,
        format!(
            r#"{{
  mode: "peer",
  listen: {{ endpoints: ["tcp/127.0.0.1:0"] }},
  connect: {{ endpoints: ["{observer_at}"] }},
  scouting: {{ multicast: {{ enabled: false }}, gossip: {{ enabled: false }} }},
}}"#
        ),
    )
    .expect("write the ZENOH_CONFIG file");
    for var in [
        "BOOTNODE_URL",
        "ZENOH_ROUTER",
        "ZENOH_MODE",
        "ZENOH_SCOUTING",
    ] {
        std::env::remove_var(var);
    }
    std::env::set_var("MOIRAI_TRANSPORT", "zenoh");
    std::env::set_var("SESSION_ID", &session);
    std::env::set_var("ZENOH_CONFIG", &file);

    let node = Node::<VecLog<AWSet<String>>>::new_with_log_id(
        "z1".to_string(),
        &["z1"],
        0,
        HashMap::default(),
        LogId::generate(),
    );
    let wanted = format!("moirai/{session}/node/z1");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !tokens.lock().unwrap().contains(&wanted) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        tokens.lock().unwrap().contains(&wanted),
        "no token {wanted} reached the observer, so the node is not on Zenoh: {:?}",
        tokens.lock().unwrap()
    );
    drop(node);
}
