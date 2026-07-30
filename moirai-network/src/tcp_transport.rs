//! Compatibility shim for the old name of [`DirectTransport`].
//!
//! `TcpTransport` was the only realization of [`CrdtTransport`] until the relay
//! arrived, so its name doubled as "the transport". It is now one of three —
//! direct, relayed, and the composite that routes between them — and the name
//! that describes what it *is* rather than what it was is
//! [`crate::direct_transport::DirectTransport`].
//!
//! Kept so that a downstream crate compiled against the old path keeps
//! compiling. Nothing in this workspace uses it.
//!
//! [`CrdtTransport`]: crate::transport::CrdtTransport
//! [`DirectTransport`]: crate::direct_transport::DirectTransport

/// Former name of [`DirectTransport`](crate::direct_transport::DirectTransport).
#[deprecated(
    since = "0.1.0",
    note = "renamed to `direct_transport::DirectTransport`; a node is normally \
            built with `composite::CompositeTransport`, which routes between a \
            direct connection and a relayed one"
)]
pub type TcpTransport<O> = crate::direct_transport::DirectTransport<O>;
