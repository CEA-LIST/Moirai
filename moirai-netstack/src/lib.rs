//! The network layer's core: the replica, factory and extension traits and
//! wire v1. It depends on no Moirai crate.

mod frame;
mod log_id;
mod log_payload;
mod port;

pub use frame::TransportMessage;
pub use log_id::{LOG_ID_LEN, LOG_ID_NAME_MAX, LogId, LogIdError};
pub use log_payload::LogPayload;
pub use port::{Extension, Opaque, Progress, ReplicaFactory, ReplicaPort};
