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
