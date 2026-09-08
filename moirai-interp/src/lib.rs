//! One log that merges any metamodel a [`MetamodelSemantics`] table can
//! express.
//!
//! [`MetamodelSemantics`]: moirai_semantics::MetamodelSemantics
//!
//! # What this crate is
//!
//! The generated path compiles one Rust type per class: an op enum per
//! feature, a log struct whose every field is a distinct concrete log, and a
//! static fan-out in `stabilize`, `redundant_by_parent` and `is_default`.
//! This crate replaces those three with a table lookup, a map from feature
//! slot to a dynamically minted child node, and an iteration — and nothing
//! else. It never implements `PureCRDT`, never touches a redundancy
//! relation, and adds no concept to `moirai-protocol` or `moirai-network`.
//! `ModelLog` is one more `IsLog` sitting above `POLog`, which is all
//! `log.rs:19`, `replica.rs:56-60`, `tcsb.rs:127` and `generic.rs:151` ever
//! asked for.
//!
#![warn(missing_docs)]

pub mod leaf;
pub mod node;
pub mod op;
#[cfg(feature = "test_utils")]
pub mod testing;

pub use leaf::{LeafLog, LeafMismatch, LeafOp, Scalar};
pub use node::{Node, ObjectNode, OptNode, Refusal, SeqNode, SlotNode};
pub use op::{InstanceOp, ModelOp, OptOp, SeqOp};
