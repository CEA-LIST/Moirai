//! Persistent bidirectional TCP. The smaller ID dials; periodic pulls repair gaps
//! after reconnects. No oracle channels or application heartbeat messages.
use crate::{
    PeerConfig,
    transport::{MAX_FRAME_BYTES, Packet, Transport},
};
use std::{
    collections::{BTreeSet, VecDeque},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};

const MAGIC: &[u8; 4] = b"MRN1";
const MAX_QUEUED_BYTES: usize = 4 * MAX_FRAME_BYTES;
const MAX_QUEUED_FRAMES: usize = 256;

struct Frame {
    length: [u8; 4],
    bytes: Arc<[u8]>,
    written: usize,
}

struct Connection {
    socket: TcpStream,
    expected: Option<u16>,
    peer: Option<u16>,
    started: Instant,
    input: Vec<u8>,
    output: VecDeque<Frame>,
    queued_bytes: usize,
}

impl Connection {
    fn new(socket: TcpStream, hello: Arc<[u8]>, expected: Option<u16>) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;
        socket2::SockRef::from(&socket).set_tcp_keepalive(
            &socket2::TcpKeepalive::new()
                .with_time(Duration::from_secs(2))
                .with_interval(Duration::from_secs(1))
                .with_retries(3),
        )?;
        let mut connection = Self {
            socket,
            expected,
            peer: None,
            started: Instant::now(),
            input: Vec::new(),
            output: VecDeque::new(),
            queued_bytes: 0,
        };
        connection.enqueue(hello)?;
        Ok(connection)
    }

    fn enqueue(&mut self, bytes: Arc<[u8]>) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > MAX_FRAME_BYTES {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if self.output.len() >= MAX_QUEUED_FRAMES
            || self.queued_bytes + bytes.len() > MAX_QUEUED_BYTES
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.queued_bytes += bytes.len();
        self.output.push_back(Frame {
            length: (bytes.len() as u32).to_be_bytes(),
            bytes,
            written: 0,
        });
        Ok(())
    }

    fn poll(&mut self, local: u16, membership: &[u8], packets: &mut Vec<Packet>) -> io::Result<()> {
        if self.peer.is_none() && self.started.elapsed() > Duration::from_secs(2) {
            return Err(io::ErrorKind::TimedOut.into());
        }
        if let Some(error) = self.socket.take_error()? {
            return Err(error);
        }
        // Bound writes and retain the exact offset through short writes.
        for _ in 0..64 {
            let Some(frame) = self.output.front_mut() else {
                break;
            };
            let remaining = if frame.written < 4 {
                &frame.length[frame.written..]
            } else {
                &frame.bytes[frame.written - 4..]
            };
            match self.socket.write(remaining) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    frame.written += n;
                    if frame.written == frame.bytes.len() + 4 {
                        self.queued_bytes -= frame.bytes.len();
                        self.output.pop_front();
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        let mut buffer = [0; 8192];
        for _ in 0..16 {
            match self.socket.read(&mut buffer) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => self.input.extend_from_slice(&buffer[..n]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
            let mut consumed = 0;
            while self.input.len() - consumed >= 4 {
                let size =
                    u32::from_be_bytes(self.input[consumed..consumed + 4].try_into().unwrap())
                        as usize;
                if size == 0 || size > MAX_FRAME_BYTES {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                if self.input.len() - consumed < size + 4 {
                    break;
                }
                let bytes = &self.input[consumed + 4..consumed + 4 + size];
                if let Some(peer) = self.peer {
                    packets.push(Packet {
                        peer,
                        bytes: bytes.to_vec(),
                    });
                } else {
                    if bytes.len() != membership.len() + 6
                        || &bytes[..4] != MAGIC
                        || &bytes[6..] != membership
                    {
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    let peer = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
                    if peer == local
                        || !membership
                            .as_chunks::<2>()
                            .0
                            .iter()
                            .any(|id| *id == peer.to_be_bytes())
                        || self.expected.is_some_and(|expected| expected != peer)
                        || (self.expected.is_none() && peer >= local)
                    {
                        return Err(io::ErrorKind::PermissionDenied.into());
                    }
                    self.peer = Some(peer);
                }
                consumed += size + 4;
            }
            self.input.drain(..consumed);
        }
        Ok(())
    }
}

struct Peer {
    id: u16,
    address: SocketAddr,
    retry_at: Instant,
}

pub struct TcpTransport {
    listener: TcpListener,
    local: u16,
    peers: Vec<Peer>,
    membership: Vec<u8>,
    hello: Arc<[u8]>,
    connections: Vec<Connection>,
    dial_cursor: usize,
}

impl TcpTransport {
    pub fn bind(local: u16, address: impl ToSocketAddrs, peers: &[PeerConfig]) -> io::Result<Self> {
        let mut ids = BTreeSet::from([local]);
        let mut resolved = Vec::new();
        for peer in peers {
            if !ids.insert(peer.replica_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "duplicate peer ID",
                ));
            }
            let address = peer
                .address
                .to_socket_addrs()?
                .next()
                .ok_or(io::ErrorKind::AddrNotAvailable)?;
            resolved.push(Peer {
                id: peer.replica_id,
                address,
                retry_at: Instant::now(),
            });
        }
        let membership: Vec<_> = ids.into_iter().flat_map(u16::to_be_bytes).collect();
        let mut hello = Vec::with_capacity(membership.len() + 6);
        hello.extend_from_slice(MAGIC);
        hello.extend_from_slice(&local.to_be_bytes());
        hello.extend_from_slice(&membership);
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            local,
            peers: resolved,
            membership,
            hello: hello.into(),
            connections: Vec::new(),
            dial_cursor: 0,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn is_connected(&self, peer: u16) -> bool {
        self.connections
            .iter()
            .any(|connection| connection.peer == Some(peer))
    }

    fn dial_one(&mut self) {
        for _ in 0..self.peers.len() {
            let slot = self.dial_cursor;
            self.dial_cursor = (self.dial_cursor + 1) % self.peers.len();
            let peer = &mut self.peers[slot];
            if peer.id <= self.local
                || Instant::now() < peer.retry_at
                || self.connections.iter().any(|c| c.expected == Some(peer.id))
            {
                continue;
            }
            peer.retry_at = Instant::now() + Duration::from_millis(500);
            // Only one dial per poll, with a strict bound even for unreachable peers.
            if let Ok(socket) = TcpStream::connect_timeout(&peer.address, Duration::from_millis(10))
                && let Ok(connection) = Connection::new(socket, self.hello.clone(), Some(peer.id))
            {
                self.connections.push(connection);
            }
            break;
        }
    }
}

impl Transport for TcpTransport {
    fn poll(&mut self) -> io::Result<Vec<Packet>> {
        for _ in 0..16 {
            match self.listener.accept() {
                Ok((socket, _)) => {
                    if self.connections.len() < self.peers.len().saturating_mul(2)
                        && let Ok(connection) = Connection::new(socket, self.hello.clone(), None)
                    {
                        self.connections.push(connection);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        let mut packets = Vec::new();
        let mut established = BTreeSet::new();
        self.connections.retain_mut(|c| {
            c.poll(self.local, &self.membership, &mut packets).is_ok()
                && c.peer.is_none_or(|id| established.insert(id))
        });
        self.dial_one();
        Ok(packets)
    }

    fn send(&mut self, peer: u16, bytes: Arc<[u8]>) -> io::Result<()> {
        self.connections
            .iter_mut()
            .find(|c| c.peer == Some(peer))
            .ok_or(io::ErrorKind::NotConnected)?
            .enqueue(bytes)
    }
}
