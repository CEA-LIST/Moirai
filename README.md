# Moirai

<div align="center">

[![GitHub stars](https://img.shields.io/github/stars/CEA-LIST/Moirai?style=social)](https://github.com/CEA-LIST/Moirai/stargazers)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Build Status](https://img.shields.io/github/actions/workflow/status/CEA-LIST/Moirai/rust.yml?branch=master)](https://github.com/CEA-LIST/Moirai/actions)

**A Rust Implementation of the Pure Operation-Based CRDT Framework, made for Building Collaborative Applications**

[Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Documentation](#documentation) • [Publications](#publications)

</div>

---

## <center>⚠️ Work in Progress — In Development ⚠️</center>

**This project is under active development.** The API and features are subject to change as we refine the framework.

## Overview

**Moirai** is a Rust-based implementation of the pure operation-based Conflict-free Replicated Data Types (CRDT) framework proposed by Baquero, Almeida, and Shoker (see [their paper](https://arxiv.org/abs/1710.04469)). Moirai provides a collection of ready-to-use CRDT implementations while remaining open to extension. You can leverage it to create new CRDTs with custom conflict-resolution policies. Rust's extensive compilation target support, including WebAssembly, makes Moirai an ideal choice for building collaborative web applications.

Moirai serves as a research platform for developing novel CRDTs tailored for Collaborative Model-Based Systems Engineering (MBSE) and for advancing techniques to enhance the scalability of the Pure Op framework. It features CRDT nesting and composition for building complex replicated data types, an extended query interface, a fuzzer tool for verification and performance testing, and exclusive CRDT implementations including a Pure Op-based Nested Multigraph.

## Project Organization

- `src/`: Implementation of the Pure Op framework
  - `crdt/`: A collection of CRDT implementations built with Moirai
  - `fuzz/`: A fuzzer tool to verify the correctness and performance of implementations
  - `macros/`: Specialized Rust macros for specific CRDTs
  - `protocol/`: Implementation of the Pure Op framework's replication protocol
  - `utils/`: Programming helper modules
- `documentation/`: An (unordered) collection of development notes about the framework
- `tests/`: Integration tests
- `bench-results/`: Contains execution reports from the fuzzer tool

## Publications

Moirai has been used in the following research:

- **Léo Olivier, Kirollos Morcos, Marcos Didonet del Fabro, Sebastien Gerard.**  
  [_A Local-First Collaborative Modeling Approach with Replicated Data Types_](https://cea.hal.science/cea-05322894).  
  CoPaMo'25 - International Workshop on Collaborative and Participatory Modeling, October 2025, Grand Rapids, United States.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

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
