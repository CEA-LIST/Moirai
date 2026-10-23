//! NW.16d, Q (vault `spec/items/NW.16d.md`): every frame the Zenoh
//! transport puts carries the settings it carries today, as a receiver
//! reads them on the wire: `CongestionControl::Block`, `Priority::Data`,
//! not express; on the three puts a node makes, a send, a broadcast with
//! nobody paused, and a broadcast while another peer is paused.
//!
//! The receiver is a raw Zenoh session written here (NW.15's rig), which
//! reads each sample's QoS as Zenoh delivered it (`Sample::congestion_control`,
//! `priority`, `express`), not the code under test. `Block` is not Zenoh's
//! default for a put, so a frame that reads `Block` carried the setting. The
//! witness: a second raw session puts on the same keys with `Drop`,
//! `DataHigh` and express, and the receiver must read those, so this run can
//! tell each setting from its opposite.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16d_publisher_qos_l2 -- --test-threads=1
//! ```

#[path = "nw15_common/contract.rs"]
mod contract;
#[path = "nw15_common/zenoh_rig.rs"]
mod zenoh_rig;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use contract::{collect, id, raw, Seen, DELIVERY};
use moirai_network::transport::{CrdtTransport, TransportMessage};
use moirai_network::zenoh_transport::ZenohTransport;
use zenoh::qos::{CongestionControl, Priority};
use zenoh::sample::Sample;
use zenoh::Wait;
use zenoh_rig::{fresh_session, raw_locator, raw_session};

type Transport = ZenohTransport<String>;

/// What the receiver read of one sample.
#[derive(Clone, Debug)]
struct Read {
    key: String,
    text: String,
    congestion: CongestionControl,
    priority: Priority,
    express: bool,
}

fn text_of(bytes: &[u8]) -> String {
    match serde_json::from_slice::<TransportMessage<String>>(bytes) {
        Ok(TransportMessage::Raw { payload }) => payload,
        _ => String::from_utf8_lossy(bytes).into_owned(),
    }
}

#[test]
fn every_put_reads_block_data_and_not_express_on_the_wire() {
    let s = fresh_session("d16q");
    let receiver = raw_session(&[]);
    let receiver_at = raw_locator(&receiver);
    let reads: Arc<Mutex<Vec<Read>>> = Arc::default();
    let mut subscribers = Vec::new();
    for key in [format!("moirai/{s}/all/*"), format!("moirai/{s}/to/r/*")] {
        let into = Arc::clone(&reads);
        subscribers.push(
            receiver
                .declare_subscriber(key.clone())
                .callback(move |sample: Sample| {
                    into.lock().unwrap().push(Read {
                        key: sample.key_expr().as_str().to_string(),
                        text: text_of(&sample.payload().to_bytes()),
                        congestion: sample.congestion_control(),
                        priority: sample.priority(),
                        express: sample.express(),
                    });
                })
                .wait()
                .unwrap_or_else(|e| panic!("the receiver's subscriber on {key}: {e}")),
        );
    }
    let _token = receiver
        .liveliness()
        .declare_token(format!("moirai/{s}/node/r"))
        .wait()
        .expect("the receiver's token");

    let wait_for = |text: &str| -> Option<Read> {
        let deadline = Instant::now() + DELIVERY;
        loop {
            if let Some(read) = reads.lock().unwrap().iter().find(|r| r.text == text) {
                return Some(read.clone());
            }
            if Instant::now() > deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(5));
        }
    };

    // The witness: settings opposite to the claim's, put by a raw session,
    // read as put.
    let witness = raw_session(std::slice::from_ref(&receiver_at));
    thread::sleep(Duration::from_millis(500));
    witness
        .put(format!("moirai/{s}/to/r/w"), "qos witness")
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::DataHigh)
        .express(true)
        .wait()
        .expect("the witness's put");
    let seen = wait_for("qos witness");
    assert!(
        matches!(&seen, Some(r) if r.congestion == CongestionControl::Drop
            && r.priority == Priority::DataHigh && r.express),
        "NW16D QOS WITNESS: a raw put with Drop, DataHigh and express read {seen:?} at the \
         receiver, so this run cannot tell the claim's settings from their opposites"
    );

    let mut a = Transport::open(
        id("a"),
        &s,
        vec![id("r"), id("x")],
        zenoh_rig::loopback_peer(&[receiver_at]),
    )
    .expect("open a");
    let mut held = Seen::default();
    let deadline = Instant::now() + DELIVERY;
    while !a.is_connected(&id("r")) && Instant::now() < deadline {
        collect(&mut a, &mut held);
        thread::sleep(Duration::from_millis(2));
    }
    assert!(a.is_connected(&id("r")), "set-up: a never read r Connected");

    a.send(&id("r"), raw("qos send")).expect("a's send to r");
    a.broadcast(raw("qos broadcast")).expect("a's broadcast");
    a.pause_peer(&id("x")).expect("a pauses x");
    a.broadcast(raw("qos paused broadcast"))
        .expect("a's broadcast while x is paused");
    a.resume_peer(&id("x")).expect("a resumes x");

    for (text, key) in [
        ("qos send", format!("moirai/{s}/to/r/a")),
        ("qos broadcast", format!("moirai/{s}/all/a")),
        ("qos paused broadcast", format!("moirai/{s}/to/r/a")),
    ] {
        let read = wait_for(text);
        let Some(read) = read else {
            panic!("Q: the receiver never got a's frame {text:?} within {DELIVERY:?}");
        };
        assert_eq!(read.key, key, "Q: the key of a's frame {text:?}");
        assert!(
            read.congestion == CongestionControl::Block
                && read.priority == Priority::Data
                && !read.express,
            "Q: a's frame {text:?} on {key} reads congestion control {:?}, priority {:?}, \
             express {}, where every put is Block, Data, not express",
            read.congestion,
            read.priority,
            read.express
        );
    }
    drop(a);
    drop(subscribers);
}
