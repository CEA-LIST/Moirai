//! The table: the types, and the one lookup the delivery path is allowed.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// A class's dense position, assigned by sorted classifier name.
///
/// Which vector it addresses is fixed by where it was read from: `classes`
/// everywhere except [`LeafRule::Enum`], which addresses `enums`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClassSlot(pub u16);

impl ClassSlot {
    /// The slot as an index into `classes`, or into `enums` for an enum slot.
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// A feature's dense position inside its declaring class, assigned by sorted
/// feature name over the class's declared features — attributes,
/// containments and references in one numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeatureSlot(pub u16);

impl FeatureSlot {
    /// The slot as an index into the declaring class's `declared`.
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// The width the generator picked for a numeric attribute.
///
/// These are exactly the six Rust types `to_crdt.rs`'s `to_rust_type` can
/// return for a numeric Ecore builtin: `EByte` is `u8` and *unsigned*,
/// `EShort` `i16`, `EInt` `i32`, `ELong` `i64`, `EFloat` `f32`, `EDouble`
/// `f64`. The width is carried rather than collapsed to int-or-float because
/// the equivalence oracle compares a `Counter<i32>` against an interpreted
/// counter and has to know which one it is holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NumKind {
    /// `EByte`.
    U8,
    /// `EShort`.
    I16,
    /// `EInt`.
    I32,
    /// `ELong`.
    I64,
    /// `EFloat`.
    F32,
    /// `EDouble`.
    F64,
}

/// Which concurrent write to a flag wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FlagWins {
    /// `EWFlag`, the house default for `EBoolean`.
    Enable,
    /// `DWFlag`, reachable through the `dw-flag` annotation.
    Disable,
}

/// How a register settles concurrent writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TieBreak {
    /// `MVRegister`, the house default.
    #[serde(rename = "mv")]
    MultiValue,
    /// `LwwRegister`.
    #[serde(rename = "lww")]
    LastWriterWins,
    /// `FairRegister`.
    #[serde(rename = "fair")]
    Fair,
    /// `PORegister`.
    #[serde(rename = "po")]
    PartialOrder,
    /// `TORegister`.
    #[serde(rename = "to")]
    TotalOrder,
}

/// How a set settles an add concurrent with a remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SetTie {
    /// `AWSet`, the house default.
    #[serde(rename = "aw")]
    AddWins,
    /// `RWSet`, reachable through the `rw-set` annotation.
    #[serde(rename = "rw")]
    RemoveWins,
}

/// The merge rule of one scalar value: the innermost CRDT of an attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LeafRule {
    /// `EventGraph<List<char>>`: every string, `iD="true"` included.
    Text,
    /// `Counter<T>` when resettable, `SimpleCounter<T>` when not.
    Counter {
        /// The width the generator would compile.
        num: NumKind,
        /// The house default is resettable.
        resettable: bool,
    },
    /// `EWFlag` or `DWFlag`.
    Flag {
        /// Which side of a concurrent enable and disable survives.
        wins: FlagWins,
    },
    /// A register over a scalar: `EChar`, or any datatype an annotation bound
    /// to one.
    Register {
        /// How concurrent writes settle.
        tie: TieBreak,
    },
    /// A register over an enum literal.
    Enum {
        /// Slot of the enum in `enums`, not in `classes`.
        class: ClassSlot,
        /// How concurrent writes settle.
        tie: TieBreak,
    },
}

/// The collection a feature's values sit in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Shape {
    /// One value, always present.
    Single,
    /// `OptionLog`: zero or one.
    Optional,
    /// `NestedListLog` or `ListLog`: many, ordered, duplicates kept.
    Sequence,
    /// `VecLog<AWSet<_>>` or `VecLog<RWSet<_>>`: many, unordered, unique.
    Set {
        /// Which side of a concurrent add and remove survives.
        tie: SetTie,
    },
    /// `AWBagLog`: many, unordered, duplicates kept.
    Bag,
    /// Declared `unique` *and* `ordered`, which the generator cannot compile:
    /// it warns and emits a list (`attribute.rs:183-198`). Recorded so the
    /// declaration is not lost, and degraded by [`Shape::effective`]
    /// everywhere a rule is acted on.
    OrderedSet,
}

impl Shape {
    /// The shape actually merged by: [`Shape::OrderedSet`] degraded to
    /// [`Shape::Sequence`], every other shape itself.
    ///
    /// The interpreter merges by this, never by the recorded shape, so that
    /// it drops uniqueness exactly where the generator does.
    pub const fn effective(self) -> Shape {
        match self {
            Shape::OrderedSet => Shape::Sequence,
            other => other,
        }
    }
}

/// Why a feature carries no rule the interpreted path can run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedReason {
    /// A `uw-map` containment: keyed by a feature of its target.
    Keyed,
    /// A class the generator represents as its one field
    /// (`urn:arachne:representation` `kind="transparent"`).
    Transparent,
    /// `derived="true"`.
    Derived,
    /// `transient="true"`.
    Transient,
    /// `volatile="true"`.
    Volatile,
}

impl UnsupportedReason {
    /// The word used in the descriptor and in a refusal sentence.
    pub const fn as_str(self) -> &'static str {
        match self {
            UnsupportedReason::Keyed => "keyed",
            UnsupportedReason::Transparent => "transparent",
            UnsupportedReason::Derived => "derived",
            UnsupportedReason::Transient => "transient",
            UnsupportedReason::Volatile => "volatile",
        }
    }

    /// Whether the parser refuses a whole metamodel over this reason, per
    /// decision D6: a keyed or transparent feature is a form the interpreted
    /// node has no node type for, and the honest answer is to stay on the
    /// generated path. The other three are recorded and simply not merged.
    pub const fn refused_at_parse(self) -> bool {
        matches!(
            self,
            UnsupportedReason::Keyed | UnsupportedReason::Transparent
        )
    }
}

/// One feature's whole merge policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum MergeRule {
    /// A shaped collection of leaves.
    Attribute {
        /// The collection.
        shape: Shape,
        /// The scalar inside it.
        leaf: LeafRule,
    },
    /// A shaped collection of contained objects. Only `Single`, `Optional`
    /// and `Sequence` are reachable: `containment.rs:172-196` compiles a
    /// multi-valued containment as a `NestedListLog` whatever its facets say.
    Containment {
        /// The collection.
        shape: Shape,
        /// Slot in `classes` of the declared target, whose `concrete` closure
        /// is what may actually sit here.
        target: ClassSlot,
    },
    /// A non-containment reference, carried as a string on both paths
    /// (design §8).
    Reference {
        /// Whether the reference is multi-valued.
        many: bool,
        /// Slot in `classes` of the declared target.
        target: ClassSlot,
    },
    /// A feature with no rule to run.
    Unsupported {
        /// Why.
        reason: UnsupportedReason,
    },
}

/// Where one facet's value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FacetSource {
    /// The `.ecore` file says so.
    Declared,
    /// The file is silent and Ecore supplies the value.
    EcoreDefault,
    /// The file is silent and Arachne's own rule supplies it.
    HouseDefault,
    /// A `urn:arachne:semantics` annotation chose it.
    Annotation,
    /// The facet carries no information for this kind of feature.
    NotApplicable,
}

/// Where each facet of one feature's rule came from.
///
/// This is what keeps "derived" honest: criterion I-A4 is falsified by a
/// facet with no source, so the parser requires all four.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Provenance {
    /// Source of `ordered`.
    pub ordered: FacetSource,
    /// Source of `unique`.
    pub unique: FacetSource,
    /// Source of the leaf choice.
    pub leaf: FacetSource,
    /// Source of the bounds that decided single, optional or many.
    pub presence: FacetSource,
}

/// One declared feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureSemantics {
    /// Position in the declaring class's `declared`.
    pub slot: FeatureSlot,
    /// The name as the `.ecore` file spells it; for the read-out and the
    /// error messages, never for delivery.
    pub name: Arc<str>,
    /// The rule.
    pub merge: MergeRule,
    /// Where the rule came from.
    pub provenance: Provenance,
}

/// One class, with its inheritance already flattened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassSemantics {
    /// Position in `classes`.
    pub slot: ClassSlot,
    /// The name as the `.ecore` file spells it.
    pub name: Arc<str>,
    /// Not abstract and not an interface, so an instance may be minted.
    pub instantiable: bool,
    /// Directly declared supertypes.
    pub supers: SmallVec<[ClassSlot; 2]>,
    /// Features this class itself declares, sorted by name; position is the
    /// [`FeatureSlot`].
    pub declared: Vec<FeatureSemantics>,
    /// Every feature visible on an instance, own and inherited, sorted by
    /// name, each once, naming the class that declares it. Flattened at parse
    /// time so no lookup walks supertypes.
    pub visible: Vec<(Arc<str>, ClassSlot, FeatureSlot)>,
    /// The concrete classes an instance of this class may actually be: the
    /// instantiable descendants, this class included when it is
    /// instantiable, ascending by slot.
    pub concrete: Arc<[ClassSlot]>,
}

/// One enum class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumSemantics {
    /// Position in `enums`.
    pub slot: ClassSlot,
    /// The name as the `.ecore` file spells it.
    pub name: Arc<str>,
    /// The literals in declaration order, which is the order the generated
    /// enum's variants are in.
    pub literals: Vec<Arc<str>>,
}

/// A whole metamodel's merge policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetamodelSemantics {
    /// The metamodel's namespace URI, one half of its identity.
    pub ns_uri: Arc<str>,
    /// The Ecore package name.
    pub package: Arc<str>,
    /// [`crate::metamodel_digest`] of the descriptor this was parsed from:
    /// the other half of the identity.
    pub digest: String,
    /// Every class, sorted by name; position is the [`ClassSlot`].
    pub classes: Vec<ClassSemantics>,
    /// Every enum, sorted by name; position is the [`ClassSlot`] an
    /// [`LeafRule::Enum`] carries.
    pub enums: Vec<EnumSemantics>,
    /// The classes a document root may be declared as; their `concrete`
    /// closures are what may actually sit there. Every instantiable class
    /// when the descriptor names none, which is what the phase 4 `Schema`
    /// does.
    pub roots: Vec<ClassSlot>,
}

impl MetamodelSemantics {
    /// The rule for one feature of one class: two bounds-checked `Vec`
    /// indexes, no name compared, nothing hashed, no supertype walked.
    ///
    /// This is the delivery path. `feature` addresses the class's *declared*
    /// features; an inherited feature is reached through the declaring
    /// class's slot, which `visible` already resolved at parse time.
    pub fn rule(&self, class: ClassSlot, feature: FeatureSlot) -> Option<&MergeRule> {
        self.classes
            .get(class.index())?
            .declared
            .get(feature.index())
            .map(|feature| &feature.merge)
    }
}
