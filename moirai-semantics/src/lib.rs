//! The merge policy of a metamodel as pure data, and a parser that reads it
//! out of an Arachne descriptor of `formatVersion` 2.
//!
//! # Why this crate exists
//!
//! Two paths need the same policy. `arachne-codegen` *derives* it from an
//! `.ecore` file and emits it into the descriptor; the interpreted
//! `moirai-interp` *reads* it at run time and merges by it. A shared crate of
//! pure data is what makes "the interpreter runs the rule the generator would
//! have compiled" a checkable claim rather than a hope: both sides name the
//! same Rust type, and step 5's equivalence oracle compares like with like.
//!
//! So this crate depends on no Moirai crate and on no Arachne crate. It holds
//! types, a parser, a digest and nothing else — no logs, no operations, no
//! network.
//!
//! # The vocabulary is closed on purpose
//!
//! [`LeafRule`], [`Shape`] and [`MergeRule`] name exactly the outcomes
//! `AttributeGenerator` and `ContainmentGenerator` can compile today and not
//! one more. A rule this crate cannot spell is a rule the generated path
//! cannot emit, and a table richer than the generator would make the
//! equivalence test compare two different semantics.
//!
//! # Descriptor format 2, as this crate reads it
//!
//! Format 2 is format 1 with keys added; nothing is removed, so the phase 4
//! `Schema` parser and the model editor keep working. Every entry of a
//! class's `attributes`, `containments` and `references` arrays carries a
//! `merge` object and a `provenance` object:
//!
//! ```json
//! {
//!   "formatVersion": 2,
//!   "package": "behaviortree",
//!   "nsURI": "http://www.example.org/behaviortree",
//!   "rootClasses": ["Root"],
//!   "classes": {
//!     "TreeNode": {
//!       "abstract": true,
//!       "superTypes": [],
//!       "attributes": [
//!         {"name": "ID", "kind": "string", "many": false, "required": true,
//!          "isId": true, "ordered": null, "unique": null, "annotation": null,
//!          "merge": {"kind": "attribute", "shape": {"kind": "single"},
//!                    "leaf": {"kind": "text"}},
//!          "provenance": {"ordered": "notApplicable", "unique": "notApplicable",
//!                         "leaf": "houseDefault", "presence": "declared"}}
//!       ],
//!       "containments": [
//!         {"name": "children", "target": "TreeNode", "many": true,
//!          "required": false, "ordered": true,
//!          "merge": {"kind": "containment", "shape": {"kind": "sequence"},
//!                    "target": "TreeNode"},
//!          "provenance": {"ordered": "houseDefault", "unique": "notApplicable",
//!                         "leaf": "notApplicable", "presence": "ecoreDefault"}}
//!       ],
//!       "references": []
//!     }
//!   },
//!   "enums": {"Status": ["RUNNING", "SUCCESS", "FAILURE"]}
//! }
//! ```
//!
//! The `merge` and `provenance` objects are *read*, never derived here.
//! Derivation is Arachne's job (step 2 of the implementation plan), because
//! that is where it is published and where it can be checked feature by
//! feature against the type the generator emits. A feature entry with no
//! `merge` key is refused by name, and so is one with no `provenance`: a
//! facet with no source is exactly what criterion I-A4 forbids.
//!
//! Two places where this crate pins more than section 1.5 of the plan spells
//! out, both deliberate and both cheap for the emitter:
//!
//! - section 1.5 adds the new keys to "every attribute and containment
//!   entry"; this parser also requires them on `references`, so that every
//!   feature of every class carries a rule and there is no second, implicit
//!   way to describe one.
//! - `{"kind": "enum", ...}` carries its enum class under `class`; when it
//!   does not, the entry's format 1 `enum` key is used, since it holds the
//!   same name.
//!
//! # Slots
//!
//! A [`ClassSlot`] is a dense position, assigned by sorted classifier name;
//! a [`FeatureSlot`] is a dense position within its declaring class,
//! assigned by sorted feature name. Position in the vector *is* the slot,
//! asserted at the end of every parse, which is what makes
//! [`MetamodelSemantics::rule`] two bounds-checked `Vec` indexes with no name
//! compared and nothing hashed. Names are carried for the read-out and the
//! error messages only.
//!
//! Classes and enums are numbered by one sorted pass over both name sets, but
//! each into its own dense space: a `ClassSlot` reached through
//! [`MergeRule::Containment`], [`MergeRule::Reference`],
//! [`ClassSemantics::visible`], [`ClassSemantics::concrete`] or
//! [`MetamodelSemantics::roots`] addresses `classes`, and one reached through
//! [`LeafRule::Enum`] addresses `enums`. The plan's sentence "one numbering,
//! so an enum's slot addresses `enums`" cannot hold together with "position
//! in the vector is the slot" for two vectors, and the assertable half wins.
//!
//! # What is not tested here yet
//!
//! Test `ip4` of the validation plan — `from_descriptor` reproducing spec 11
//! §7's 22-row class table and 16-row feature table of `bt.ecore` slot by
//! slot — lands with step 2, because no format 2 descriptor exists until the
//! emitter does. The fixtures in this crate are hand-written and small, and
//! they pin the shape step 2 must emit. Test `ip5` is here in fixture form:
//! the real refusal of `json.metamodel.json` by name also lands with step 2.

mod digest;
mod parse;
mod table;

pub use digest::metamodel_digest;
pub use parse::{SemanticsError, from_descriptor};
pub use table::{
    ClassSemantics, ClassSlot, EnumSemantics, FacetSource, FeatureSemantics, FeatureSlot, FlagWins,
    KeyKind, LeafRule, MergeRule, MetamodelSemantics, NumKind, Provenance, SetTie, Shape, TieBreak,
    UnsupportedReason,
};
