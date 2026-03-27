# Moirai

## Overview

**Moirai** is a Rust-based implementation of the pure operation-based Conflict-free Replicated Data Types (CRDT) framework proposed by Baquero, Almeida, and Shoker (see [their paper](https://arxiv.org/abs/1710.04469)). Moirai provides a collection of ready-to-use CRDT implementations while remaining open to extension. You can leverage it to create new CRDTs with custom conflict-resolution policies. Rust's extensive compilation target support, including WebAssembly, makes Moirai an ideal choice for building collaborative web applications.

Moirai serves as a research platform for developing novel CRDTs tailored for Collaborative Model-Based Systems Engineering (MBSE) and for advancing techniques to enhance the scalability of the Pure Op framework. It features CRDT nesting and composition for building complex replicated data types, an extended query interface, a fuzzer tool for verification and performance testing, and exclusive CRDT implementations including a Pure Op-based Nested Multigraph.

## Project Organization

- `moirai-protocol`: Implementation of the Pure Op framework's replication protocol.
- `moirai-crdt`: A collection of CRDT implementations built with Moirai.
- `mirai-macros`: Rust macros for specific CRDTs.
- `moirai-fuzz`: A fuzzer tool to verify the correctness and performance of implementations.
