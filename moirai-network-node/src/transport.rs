use std::{io, sync::Arc};

/// One complete message from a configured peer. Identity is supplied by the adapter.
pub struct Packet {
    pub peer: u16,
    pub bytes: Vec<u8>,
}

/// Implement this interface to add another transport. No Moirai types cross it.
///
/// `poll` must do bounded work, preserving partial reads/writes between calls.
/// `send` queues a complete payload or returns an error without queuing it.
/// Disconnections and full queues can discard messages; periodic pulls repair gaps.
pub trait Transport {
    fn poll(&mut self) -> io::Result<Vec<Packet>>;
    fn send(&mut self, peer: u16, bytes: Arc<[u8]>) -> io::Result<()>;
}

/// Shared by the codec and TCP adapter. Large pull replies are split into frames.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
