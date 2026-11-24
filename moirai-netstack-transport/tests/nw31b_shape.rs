//! NW.31b (vault `spec/items/NW.31b.md`): the transports carry no op type. Every
//! transport is a `CrdtTransport` as a plain type, `dyn CrdtTransport` names no
//! associated type, and the frame the trait carries is the core crate's
//! `moirai_netstack::TransportMessage`. A compile error here is a failure.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-netstack-transport --test nw31b_shape
//! ```

use moirai_netstack_transport::any_transport::AnyTransport;
use moirai_netstack_transport::composite::CompositeTransport;
use moirai_netstack_transport::direct_transport::DirectTransport;
use moirai_netstack_transport::relay_transport::RelayTransport;
use moirai_netstack_transport::transport::{CrdtTransport, TransportMessage};

fn is_transport<T: CrdtTransport + ?Sized>() {}

#[test]
fn nw31b_every_transport_is_a_crdt_transport_with_no_op_type() {
    is_transport::<dyn CrdtTransport>();
    is_transport::<DirectTransport>();
    is_transport::<RelayTransport>();
    is_transport::<CompositeTransport>();
    is_transport::<AnyTransport>();
    #[allow(deprecated)]
    is_transport::<moirai_netstack_transport::tcp_transport::TcpTransport>();
    #[cfg(feature = "zenoh")]
    is_transport::<moirai_netstack_transport::zenoh_transport::ZenohTransport>();
}

#[test]
fn nw31b_the_frame_is_the_core_crates_transport_message() {
    fn same(frame: moirai_netstack::TransportMessage) -> TransportMessage {
        frame
    }
    let frame = same(moirai_netstack::TransportMessage::Ack {
        from: "alice".into(),
    });
    assert!(matches!(frame, TransportMessage::Ack { from } if from == "alice"));
}
