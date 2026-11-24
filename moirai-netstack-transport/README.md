# moirai-netstack-transport

The transports the network layer's nodes run on, over
[`moirai-netstack`](../moirai-netstack)'s wire v1: the direct TCP transport, the
relay client, the composite that routes between them, and Zenoh behind the
`zenoh` feature (on by default). `cargo build --no-default-features` builds a
TCP-only library, and does not build `zenoh` at all.

## The Zenoh patch a consuming workspace must repeat

The Zenoh transport needs `vendor/zenoh-1.10.1` of the Moirai workspace: zenoh
1.10.1 plus eclipse-zenoh/zenoh PR #2779, which replaces the peer connectors'
async mutex with a synchronous one. Without the patch, a gossip link
established while routing holds the router lock deadlocks the session in stock
zenoh 1.10.1.

A workspace that depends on this crate must repeat the patch line in its own
`Cargo.toml`:

```toml
[patch.crates-io]
zenoh = { path = "vendor/zenoh-1.10.1" }
```

and copy the `vendor/zenoh-1.10.1` tree beside it. The build records which
source tree it linked, and `moirai_netstack_transport::zenoh_patch::check()`
returns an error naming this line when the workspace links stock zenoh instead;
call it at start-up and print the error rather than running on the wrong copy.

The frame decode below the transports keeps 601c887's 1024-deep rule, and it
runs on a stack `stacker` grows for it, as 601c887's did; `stacker` is a
dependency of `moirai-netstack` that builds a small C shim through `cc`, so a
build of a consuming workspace needs a C compiler, as Zenoh's own dependencies
already do.
