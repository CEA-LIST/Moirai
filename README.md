# Moirai: An Extensible, Generic Operation-based CRDT Framework with Customizable Conflict Resolution

**Moirai** is a Rust-based framework designed for building operation-based Conflict-free Replicated Data Types (CRDTs). It provides a flexible architecture that allows developers to implement custom conflict resolution policies, making it suitable for various distributed applications such as collaborative modeling.

<div align="center">

## ⚠️ Work in Progress — In Development ⚠️

_This project is currently under active development. The API and features are subject to change as we refine the framework._

</div>

## Architecture

![Moirai Architecture Diagram](figures/architecture.png "Moirai Architecture Diagram")

## Project Structure

The project is organized into several key components:

- **src**: Contains the core library code.
  - **clocks**: Logical clocks implementations.
  - **crdt**: CRDT implementations.
  - **protocol**: Replication protocol implementations.
  - **utils**: Utility modules for benchmarking and testing.
- **tests**: Contains integration tests for the framework.
- **logs**: Contains benchmark logs for performance evaluation.
