# Moirai

## Overview

**Moirai** is a Rust-based implementation of the pure operation-based Conflict-free Replicated Data Types (CRDT) framework proposed by Baquero, Almeida, and Shoker (see [their paper](https://arxiv.org/abs/1710.04469)). Moirai provides a collection of ready-to-use CRDT implementations while remaining open to extension. You can leverage it to create new CRDTs with custom conflict-resolution policies. Rust's extensive compilation target support, including WebAssembly, makes Moirai an ideal choice for building collaborative web applications.

Moirai serves as a research platform for developing novel CRDTs tailored for Collaborative Model-Based Systems Engineering (MBSE) and for advancing techniques to enhance the scalability of the Pure Op framework. It features CRDT nesting and composition for building complex replicated data types, an extended query interface, a fuzzer tool for verification and performance testing, and exclusive CRDT implementations including a Pure Op-based Nested Multigraph.

## Project Organization

```sh
.
├── moirai-crdt/ # A collection of CRDT implementations built with Moirai.
├── moirai-fuzz/ # A fuzzer tool to verify the correctness and performance of implementations.
├── moirai-macros/ # Rust macros for specific CRDTs.
└── moirai-protocol/ # Implementation of the Pure Op framework's replication protocol.
```

## The node's HTTP API: where it listens and whom it answers

`model_node` (`moirai-interp/examples/model_node.rs`) serves its HTTP API on `HTTP_PORT`. Two variables bound who can reach it and what a browser page may do with it:

- `HTTP_BIND_ADDR`: the IP address the HTTP listener binds, for example `127.0.0.1` to answer this machine only. Unset or empty, it is `0.0.0.0`, every interface, which a container needs for its published port. An address that does not parse stops the node with an error naming the variable; it never falls back to another address. The replication listener (`LISTEN_PORT`) is not affected.
- `HTTP_ALLOWED_ORIGINS`: the browser origins the API answers, separated by commas. An entry is an exact origin (`https://app.example.test:8443`) or `scheme://host:*`, which admits that scheme and host with any port (`http://127.0.0.1:*`). Unset or empty, the list is the two browser editors' dev servers: `http://localhost:5173`, `http://localhost:5174`, `http://127.0.0.1:5173` and `http://127.0.0.1:5174`. A configured list replaces this default. The docker stack passes the host's variable to each replica (`docker/compose/replica.yml`).

The policy, applied before any request reaches the node:

- A request whose `Origin` is not on the list is refused with 403 unless it only reads, and no answer to it carries an `Access-Control-*` header, so a page on that origin can neither write nor read what the node answers.
- A `POST` whose body is not declared `Content-Type: application/json`, or that carries a body and declares no type, is refused with 415, whatever its origin. A bodyless `POST` with no type (`curl -X POST .../api/pause-all`) is accepted.
- An allowed origin is answered with `Access-Control-Allow-Origin` set to that origin, never `*`.

Trust assumption: a request with no `Origin` header comes from a local process (curl, a Node client, a test harness) and is served as one, reads and writes alike. The API authenticates no one: whoever can open a TCP connection to the port and sends no `Origin` acts as the node. With the default bind that includes every host that can reach the port, which is what `HTTP_BIND_ADDR` is for.
