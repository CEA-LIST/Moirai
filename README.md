# Moirai

<div align="center">

[![GitHub stars](https://img.shields.io/github/stars/CEA-LIST/Moirai?style=social)](https://github.com/CEA-LIST/Moirai/stargazers)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Build Status](https://img.shields.io/github/actions/workflow/status/CEA-LIST/Moirai/rust.yml?branch=master)](https://github.com/CEA-LIST/Moirai/actions)

**An Extensible Pure Operation-Based CRDT Framework for Building Collaborative Applications**

[Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Documentation](#documentation) • [Publications](#publications)

</div>

---

## ⚠️ Work in Progress — In Development ⚠️

**This project is under active development.** This project is currently under active development. The API and features are subject to change as we refine the framework.

## Overview

Moirai is a Rust-based framework for building **pure operation-based Conflict-free Replicated Data Types (CRDTs)**. It provides a flexible, extensible architecture that enables developers to create custom CRDTs with sophisticated conflict resolution semantics, making it ideal for distributed collaborative applications.

<!-- ### What Sets Moirai Apart

- **Pure Operation-Based**: Leverages causal stability for efficient garbage collection and operation replay
- **Event Graph Architecture**: Uses a DAG structure to track causal dependencies and enable sophisticated query capabilities
- **Extensible by Design**: Define new CRDTs by implementing simple traits
- **Type-Safe Composition**: Macros for composing complex CRDTs from simpler primitives
- **Built-in Fuzzing**: Comprehensive property-based testing infrastructure to verify CRDT correctness
- **Fine-Grained Control**: Customizable conflict resolution policies and operation semantics

---
 -->

## Features

### Core CRDTs

Moirai provides a comprehensive library of CRDT implementations:

| Category        | Types                                                                                  | Description                                                                                                                               |
| --------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Counters**    | `Counter`, `ResettableCounter`                                                         | Increment/decrement with optional reset                                                                                                   |
| **Flags**       | `EWFlag`, `DWFlag`                                                                     | Enable/disable boolean flags.                                                                                                             |
| **Registers**   | `MVRegister`, `TORegister`, `PORegister`, `Last-Writer-Wins Register`, `Fair Register` | Multi-value, semantically data-driven totally ordered and partially ordered registers, unfair and fair to process single value registers. |
| **Collections** | `AWSet`, `RWSet`                                                                       | Add-wins and remove-wins sets                                                                                                             |
| **Maps**        | `UWMap`                                                                                | Update-wins map with nested CRDT values                                                                                                   |
| **Graphs**      | `UWMultiDigraph`                                                                       | Directed multi-graphs with CRDT nodes and edges                                                                                           |
| **Lists**       | `List` (EG-walker), `NestedList`                                                       | Collaborative text editing with nested structures                                                                                         |
| **Composite**   | `Union`, `Record`                                                                      | Sum types and product types CRDTs                                                                                                         |
| **Document**    | `JSON`                                                                                 | CRDT for JSON-like hierarchical documents with rich data types                                                                            |

Most of these CRDTs implementations are based on specifications that have been formally verified using [VeriFX](https://github.com/verifx-prover/verifx).

## Quick Start

### Installation

Add Moirai to your `Cargo.toml`:

```toml
[dependencies]
moirai = { git = "https://github.com/CEA-LIST/Moirai.git" }
```

### Basic Example: Counter CRDT

```rust
use moirai::{
    crdt::counter::simple_counter::Counter,
    protocol::{
        crdt::query::Read,
        replica::IsReplica,
        state::po_log::VecLog,
        broadcast::tcsb::Tcsb,
    },
};

// Create two replicas
let (mut replica_a, mut replica_b) =
    moirai::crdt::test_util::twins::<VecLog<Counter<i32>>, Tcsb<Counter<i32>>>();

// Replica A increments
let event = replica_a.send(Counter::Inc(5)).unwrap();
replica_b.receive(event);

// Replica B increments
let event = replica_b.send(Counter::Inc(3)).unwrap();
replica_a.receive(event);

// Both replicas converge to the same value
assert_eq!(replica_a.query(Read::new()), 8);
assert_eq!(replica_b.query(Read::new()), 8);
```

<!-- ### Creating a Custom CRDT

```rust
use moirai::protocol::{
    crdt::{pure_crdt::PureCRDT, eval::Eval, query::Read},
    state::unstable_state::IsUnstableState,
};

#[derive(Clone, Debug)]
pub enum MyCounter {
    Inc,
    Dec,
}

impl PureCRDT for MyCounter {
    type Value = i32;
    type StableState = i32;

    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
}

impl Eval<Read<i32>> for MyCounter {
    fn execute_query(
        _q: Read<i32>,
        stable: &i32,
        unstable: &impl IsUnstableState<Self>,
    ) -> i32 {
        let mut count = *stable;
        for op in unstable.iter() {
            match op.op() {
                MyCounter::Inc => count += 1,
                MyCounter::Dec => count -= 1,
            }
        }
        count
    }
}
```

--- -->

## Architecture

Moirai's architecture separates concerns into distinct layers:

```
┌─────────────────────────────────────────────────┐
│            Application Layer                    │
│  (Your collaborative application logic)         │
└─────────────────────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────┐
│         CRDT Layer (IsLog + PureCRDT)           │
│  Counter │ Map │ Set │ List │ Graph │ ...       │
└─────────────────────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────┐
│       State Management (EventGraph/POLog)       │
│  Causal tracking │ Operation storage            │
└─────────────────────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────┐
│     Protocol Layer (Replica + Broadcast)        │
│  Version vectors │ Causal delivery              │
└─────────────────────────────────────────────────┘
```

<!-- ### Key Abstractions

- **`PureCRDT`**: Defines operation semantics and conflict resolution
- **`IsLog`**: Manages operation storage and application
- **`IsUnstableState`**: Provides access to the operation history
- **`Replica`**: Coordinates state replication across peers
- **`EventGraph`**: Maintains causal dependencies as a DAG

![Moirai Architecture Diagram](figures/architecture.png "Moirai Architecture Diagram")

---

## Testing & Fuzzing

Moirai includes a sophisticated fuzzing framework for verifying CRDT properties:

```rust
use moirai::fuzz::{fuzzer, config::{FuzzerConfig, RunConfig}};

let config = FuzzerConfig::<MyLog>::new(
    "my_crdt",
    vec![RunConfig::new(0.4, 3, 1000, None, None, true)],
    true,
    |a, b| a == b,
    false,
);

fuzzer::<MyLog>(config);
```

The fuzzer automatically:

- Generates random operations
- Simulates network partitions and delays
- Verifies convergence across replicas
- Produces execution graphs for debugging

--- -->

## Publications

Moirai has been used in the following research:

- **Léo Olivier, Kirollos Morcos, Marcos Didonet del Fabro, Sebastien Gerard.**  
  [_A Local-First Collaborative Modeling Approach with Replicated Data Types_](https://cea.hal.science/cea-05322894).  
  CoPaMo'25 - International Workshop on Collaborative and Participatory Modeling, October 2025, Grand Rapids, United States.

<!-- ---

## Roadmap

### Current Focus (v0.1.x)

- [x] Core CRDT implementations
- [x] Event Graph architecture
- [x] Fuzzing framework
- [x] Basic broadcast protocols
- [ ] Performance benchmarks
- [ ] Comprehensive documentation

### Future Plans (v0.2.x+)

- [ ] WebAssembly support
- [ ] Network transport abstractions
- [ ] Persistence layer
- [ ] Conflict resolution UI helpers
- [ ] Rich text editing primitives
- [ ] Delta compression -->

<!--

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

````bash
# Clone the repository
git clone https://github.com/CEA-LIST/Moirai.git
cd Moirai

# Run tests
cargo test

# Run fuzzing tests (requires 'fuzz' feature)
cargo test --features fuzz

# Run benchmarks
cargo bench
```

---
-->

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Acknowledgments

Developed at [CEA LIST](https://list.cea.fr/en/), the French Alternative Energies and Atomic Energy Commission.

**Authors:**

- Léo Olivier ([@leo-olivier](https://github.com/leo-olivier))
- Kirollos Morcos ([@KirollosMorcos](https://github.com/KirollosMorcos))

---

<div align="center">

**[⬆ Back to Top](#moirai)**

Made with ❤️ by the CEA LIST team

</div>
