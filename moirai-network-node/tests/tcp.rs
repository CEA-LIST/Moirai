use moirai_network_node::{PeerConfig, TcpTransport, Transport, transport::MAX_FRAME_BYTES};
use std::{
    io::{self, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

fn peer(id: u16, address: impl ToString) -> PeerConfig {
    PeerConfig {
        replica_id: id,
        replica_name: String::new(),
        address: address.to_string(),
    }
}

fn pair() -> (TcpTransport, TcpTransport) {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let a_address = reservation.local_addr().unwrap();
    let b = TcpTransport::bind(10, "127.0.0.1:0", &[peer(2, a_address)]).unwrap();
    drop(reservation);
    let a = TcpTransport::bind(2, a_address, &[peer(10, b.local_addr().unwrap())]).unwrap();
    (a, b)
}

fn eventually(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if check() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "TCP condition did not become true"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn connect(a: &mut TcpTransport, b: &mut TcpTransport) {
    eventually(|| {
        a.poll().unwrap();
        b.poll().unwrap();
        a.is_connected(10) && b.is_connected(2)
    });
}

#[test]
fn persistent_link_carries_large_frames_both_ways_and_bounds_queues() {
    let (mut a, mut b) = pair();
    connect(&mut a, &mut b);
    let bytes: Arc<[u8]> = vec![42; MAX_FRAME_BYTES].into();
    for _ in 0..4 {
        a.send(10, bytes.clone()).unwrap();
    }
    assert_eq!(
        a.send(10, bytes.clone()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    b.send(2, Arc::from(&b"reply"[..])).unwrap();
    let mut received = 0;
    let mut reply = false;
    eventually(|| {
        for packet in a.poll().unwrap() {
            assert_eq!(packet.peer, 10);
            assert_eq!(packet.bytes, b"reply");
            reply = true;
        }
        for packet in b.poll().unwrap() {
            assert_eq!(packet.peer, 2);
            assert_eq!(&packet.bytes[..], &*bytes);
            received += 1;
        }
        received == 4 && reply
    });
    assert_eq!(
        a.send(10, vec![0; MAX_FRAME_BYTES + 1].into())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn peer_exit_is_detected_and_links_reconnect() {
    let (mut a, mut b) = pair();
    connect(&mut a, &mut b);
    let a_address = a.local_addr().unwrap();
    let b_address = b.local_addr().unwrap();
    drop(b);
    eventually(|| {
        a.poll().unwrap();
        !a.is_connected(10)
    });
    assert_eq!(
        a.send(10, Arc::from(&b"lost"[..])).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    let mut b = TcpTransport::bind(10, b_address, &[peer(2, a_address)]).unwrap();
    connect(&mut a, &mut b);
    a.send(10, Arc::from(&b"after reconnect"[..])).unwrap();
    eventually(|| {
        a.poll().unwrap();
        b.poll()
            .unwrap()
            .into_iter()
            .any(|packet| packet.bytes == b"after reconnect")
    });
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(payload);
    frame
}

#[test]
fn partial_handshakes_and_frames_are_retained_and_invalid_lengths_close_links() {
    let mut b = TcpTransport::bind(10, "127.0.0.1:0", &[peer(2, "127.0.0.1:1")]).unwrap();
    let mut raw = TcpStream::connect(b.local_addr().unwrap()).unwrap();
    raw.set_nodelay(true).unwrap();
    let hello = frame(&[b'M', b'R', b'N', b'1', 0, 2, 0, 2, 0, 10]);
    for byte in hello {
        raw.write_all(&[byte]).unwrap();
        assert!(b.poll().unwrap().is_empty());
    }
    eventually(|| {
        b.poll().unwrap();
        b.is_connected(2)
    });
    let payload = frame(b"fragmented");
    for byte in &payload[..payload.len() - 1] {
        raw.write_all(&[*byte]).unwrap();
        assert!(b.poll().unwrap().is_empty());
    }
    raw.write_all(&payload[payload.len() - 1..]).unwrap();
    eventually(|| {
        b.poll()
            .unwrap()
            .into_iter()
            .any(|packet| packet.peer == 2 && packet.bytes == b"fragmented")
    });
    raw.write_all(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes())
        .unwrap();
    eventually(|| {
        b.poll().unwrap();
        !b.is_connected(2)
    });
}

#[test]
fn membership_mismatches_are_rejected_during_handshake() {
    let mut b = TcpTransport::bind(10, "127.0.0.1:0", &[peer(2, "127.0.0.1:1")]).unwrap();
    let mut raw = TcpStream::connect(b.local_addr().unwrap()).unwrap();
    // Claims the configured sender, but a different member set.
    raw.write_all(&frame(&[b'M', b'R', b'N', b'1', 0, 2, 0, 2, 0, 11]))
        .unwrap();
    for _ in 0..5 {
        assert!(b.poll().unwrap().is_empty());
    }
    assert!(!b.is_connected(2));
}
