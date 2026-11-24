# Moirai

<div align="center">

[![GitHub stars](https://img.shields.io/github/stars/CEA-LIST/Moirai?style=social)](https://github.com/CEA-LIST/Moirai/stargazers)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Build Status](https://img.shields.io/github/actions/workflow/status/CEA-LIST/Moirai/rust.yml?branch=master)](https://github.com/CEA-LIST/Moirai/actions)

**A Rust Implementation of the Pure Operation-Based CRDT Framework, made for Building Collaborative Applications**

</div>

---

<div align="center">
  <h2>⚠️ Work in Progress — In Development ⚠️</h2>
</div>

**This project is under active development.** The API and features are subject to change as we refine the framework.

## Overview

**Moirai** is a Rust-based implementation of the pure operation-based Conflict-free Replicated Data Types (CRDT) framework introduced by Baquero, Almeida, and Shoker (see [their paper](https://arxiv.org/abs/1710.04469)). Moirai provides a collection of ready-to-use and composable CRDT implementations. You can leverage it to create new CRDTs with custom conflict-resolution policies. Rust's extensive compilation target support makes Moirai an ideal choice for building collaborative applications.

Moirai serves as a research platform for developing novel CRDTs tailored for collaborative Model-Based Systems Engineering (MBSE) and for advancing algorithms to enhance the scalability of the Pure Op framework. It features CRDT composition for building complex replicated data types, a set of flexible data structures for storing CRDT states, a fuzzer tool for verification and performance testing, and exclusive CRDT implementations including a Pure Op-based Nested Multigraph.

## Project Organization

```sh
.
├── moirai-crdt/ # A collection of CRDT implementations built with Moirai.
├── moirai-fuzz/ # A fuzzer tool to verify the correctness and performance of implementations.
├── moirai-macros/ # Rust macros for specific CRDTs.
└── moirai-protocol/ # Implementation of the Pure Op framework's replication protocol.
```

## Network stack (moirai-netstack)

The `moirai-netstack*` crates are one network library for replicated logs. Nodes generated on Moirai's CRDT types and the interpreter both run on it, through one replica trait (`ReplicaPort`) and one wire format (wire v1). Governance and other application concerns stay outside the library: an application plugs them in through extensions on the node and through its own HTTP routes.

- `moirai-netstack` is the core. It holds the `ReplicaPort`, `ReplicaFactory` and `Extension` traits, the `Opaque` and `Progress` types, `LogId`, `LogPayload`, `TransportMessage` and the wire v1 encoding and decoding. It depends on no Moirai crate; its dependencies are serde, serde_json, flate2, base64 and stacker. Frame decoding accepts nesting up to 1024 levels deep and runs on a stack that `stacker` grows for it, so `stacker` is a native build dependency (it builds a small C shim) and a build of any consuming workspace needs a C compiler.
- `moirai-netstack-transport` provides the `CrdtTransport` trait and its implementations: direct TCP, a relay client, a composite that routes each peer to the provider that can reach it, and Zenoh behind the `zenoh` feature (on by default; `--no-default-features` builds a TCP-only library). It depends on `moirai-netstack`, and on the vendored Zenoh when the feature is on. A workspace that depends on it must repeat one patch line in its own `Cargo.toml` and copy `vendor/zenoh-1.10.1` beside it:

  ```toml
  [patch.crates-io]
  zenoh = { path = "vendor/zenoh-1.10.1" }
  ```

  `moirai_netstack_transport::zenoh_patch::check()` returns an error naming this line when the build links stock Zenoh instead.
- `moirai-netstack-node` provides `Node<F, T>`, generic over a `ReplicaFactory` and a `CrdtTransport`. A node hosts logs by id, filters frames for logs it does not host, applies operations and batches under the etag rule, catches a log up by delta, transfers state when the replica offers it, and refuses a returning member. It depends on `moirai-netstack` and `moirai-netstack-transport` (without default features; its own `zenoh` feature turns Zenoh on).
- `moirai-netstack-http` provides the generic HTTP routes over a `Node`, served with tiny_http, and a `Routes` trait through which an application adds its own routes. It depends on `moirai-netstack`, `moirai-netstack-node` and `moirai-netstack-transport`.
- `moirai-netstack-crdt` (in `adapters/crdt`) implements `ReplicaPort` over Moirai's own replica and CRDT types. It depends on `moirai-netstack`, `moirai-protocol` and `moirai-crdt`, and offers ready factories such as `awset::factory()`. Its tests also use the node, the transport and the testkit.
- `moirai-netstack-testkit` provides a toy grow-only-set replica with no Moirai, and the two contract suites every adapter runs: `contract` at the port level and `contract_node` at the node level. It depends on `moirai-netstack`, `moirai-netstack-node` and `moirai-netstack-transport`.

How they stack. Each line points down to a crate that the one above it depends on. The node is generic over the replica factory, so no adapter appears under it: the application chooses the adapter and hands its factory to the node. The interpreter adapter is planned and does not exist yet.

```text
                        application
          (adds its own routes and extensions)
                |                       |
                v                       v
       moirai-netstack-http     +----------------------+----------------------+---------------------+
                |               | moirai-netstack-crdt | moirai-netstack-     | interpreter adapter |
                v               |  (Moirai replicas)   |  testkit (toy)       |  (planned)          |
       moirai-netstack-node     +----------------------+----------------------+---------------------+
          |            |                |       |                |                      |
          |            v                v       v                |                      |
          |    moirai-netstack-   moirai-crdt  moirai-protocol   |                      |
          |       transport                                      |                      |
          |            |                                         |                      |
          v            v                                         v                      v
   ===========================================================================================
   moirai-netstack  (core: ReplicaPort, ReplicaFactory, Extension, wire v1)
```

Arrows and lines read as "depends on". The testkit also depends on the node and the transport, and the crdt adapter uses them in its tests; those lines are left out to keep the drawing readable.

Running the tests, one crate at a time:

- `cargo test -p moirai-netstack`
- `cargo test -p moirai-netstack-transport` (the Zenoh tests need the default feature; `cargo test -p moirai-netstack-transport --no-default-features` runs the TCP-only tests)
- `cargo test -p moirai-netstack-node`
- `cargo test -p moirai-netstack-http`
- `cargo test -p moirai-netstack-crdt`
- `cargo test -p moirai-netstack-testkit`

## Publications

Moirai has been used in the following research:

- **[Automatic Generation of Local-First, Collaborative Modeling Languages](https://dl.acm.org/doi/10.1145/3822455.3830305).**
  Léo Olivier, Benoit Combemale, Marcos Didonet Del Fabro, Sébastien Gérard.
  _ACM/IEEE 29th International Conference on Model Driven Engineering Languages and Systems (MODELS)_, 2026.
- **[Hybrid Collaborative Modeling: Problem Analysis, Requirements, and Architectural Principles](https://www.jot.fm/contents/issue_2026_03/a27.html).**
  Léo Olivier, Marcos Didonet Del Fabro, Sébastien Gérard.
  _22nd European Conference on Modelling Foundations and Applications (ECMFA)_, 2026.
  DOI: 10.5381/jot.2026.25.3.a27.
- **[The Cost of Purity: Scalability Challenges in the Pure Operation-based CRDT Framework](https://cea.hal.science/view/index/docid/5739385).**
  Léo Olivier, Jim Bauwens, Marcos Didonet Del Fabro, Sébastien Gérard.
  _21st International Conference on Software Technologies (ICSOFT)_, 2026.
- **[A Local-First Collaborative Modeling Approach with Replicated Data Types](https://cea.hal.science/cea-05322894).**
  Léo Olivier, Kirollos Morcos, Marcos Didonet Del Fabro, Sébastien Gérard.
  _COPAMO 2025_, co-located with _MODELS 2025_, Grand Rapids, United States, 2025.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Developed at [CEA LIST](https://list.cea.fr/en/), the French Alternative Energies and Atomic Energy Commission.

**Authors:**

- Léo Olivier ([@leo-olivier](https://github.com/leo-olivier))
- Kirollos Morcos ([@KirollosMorcos](https://github.com/KirollosMorcos))
- Hiba Ajabri
- van-Cam Pham

---

<div align="center">
  <blockquote cite="https://www.huxley.net/bnw/four.html">
    <p style="font-style:italic">Equal laws the Moirai make, <br /> Kings' and peasants' threads they take, <br /> And one weapon cuts them all.</p>
  </blockquote>
  <p>— Marquis de Racan</p>
</div>

<div align="center">

**[⬆ Back to Top](#moirai)**

Made with ❤️ by the CEA LIST team

</div>
