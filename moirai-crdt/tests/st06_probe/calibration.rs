//! ST.06, C0, the harness checking itself (`cea-cdrt-knowledge/spec/items/
//! ST.06.md`): a trait of this file shaped like `OptionalContent`, whose one
//! method has no default body, and an empty impl of it. This binary never
//! builds; the harness requires its refusal to be read as the refusal C1
//! looks for (one E0046 naming `redundant_by_optional` alone, at the empty
//! impl), so that C1's reading of a refusal is proven on every run, before
//! and after the change.

use moirai_protocol::clock::version_vector::Version;

/// What `OptionalContent` asks, with no default body.
pub trait Stated {
    fn redundant_by_optional(&mut self, version: &Version, conservative: bool);
}

pub struct Probe;

impl Stated for Probe {}

fn main() {}
