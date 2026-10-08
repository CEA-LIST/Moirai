//! The table: the types, and the one lookup the delivery path is allowed.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// A class's dense position, assigned by sorted classifier name.
///
/// Which vector it addresses is fixed by where it was read from: `classes`
/// everywhere except [`LeafRule::Enum`], which addresses `enums`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
/// return for a numeric Ecore builtin: `EByte` is `i8`, `EShort` `i16`, `EInt`
/// `i32`, `ELong` `i64`, `EFloat` `f32`, `EDouble` `f64`. The width is carried
/// rather than collapsed to int-or-float because the equivalence oracle
/// compares a `Counter<i32>` against an interpreted counter and has to know
/// which one it is holding.
///
/// `EByte` is signed because Ecore says so: `ecore.ecore` gives `EByte` the
/// `instanceClassName` `byte`, which is Java's signed eight-bit integer, and
/// `EByteObject` `java.lang.Byte`. Ecore has no unsigned byte at all, so a
/// `u8` here would have been a width Ecore cannot express.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
#[serde(rename_all = "lowercase")]
pub enum NumKind {
    /// `EByte`.
    I8,
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
#[serde(rename_all = "lowercase")]
pub enum FlagWins {
    /// `EWFlag`, the house default for `EBoolean`.
    Enable,
    /// `DWFlag`, reachable through the `dw-flag` annotation.
    Disable,
}

/// How a register settles concurrent writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LeafRule {
    /// `GraphLog<List<char>>`: every string, `iD="true"` included.
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
    /// `VecLog<MVRegister<Option<T>>>`: a scalar that may be *absent*, held in
    /// a multi-value register.
    ///
    /// Reachable in one place only, the value of a [`Shape::Keyed`] collection
    /// whose entry class's value feature is an optional attribute, which is
    /// what Ecore's own `EStringToStringMapEntry.value` is. `UWMapLog` reads a
    /// key back only while that key's value log is not default, and an
    /// `OptionLog` holding nothing *is* default, so a key put without a value
    /// would vanish; a register holding `None` is not default, so the key is
    /// read back with no value. That is `containment.rs`'s
    /// `uw_map_optional_value_log_type`, and the reason this is a leaf of its
    /// own rather than a [`LeafRule::Register`]: the two compile to different
    /// Rust types and merge over different value spaces.
    ///
    /// The tie-break is not carried because the generator has none to make: it
    /// writes `MVRegister` here whatever the file says and refuses a
    /// `urn:arachne:semantics` `datatype` on such a value feature.
    OptionalRegister {
        /// Slot in `enums` of the enum whose literals the register holds, when
        /// the value feature is enum-typed; `None` for every other scalar.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        class: Option<ClassSlot>,
    },
}

/// What the values of a keyed collection's key are.
///
/// A `uw-map` key is the Rust type `containment.rs:150-163` picks for the key
/// attribute and never a CRDT: `UWMapLog<K, L>` hashes `K`
/// (`uw_map.rs:56`), so the key is the entry's identity and the merge
/// happens under it and never to it. That is why this is its own vocabulary
/// and not a [`LeafRule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum KeyKind {
    /// `EString`, and any datatype the descriptor describes as a string.
    Str,
    /// `EBoolean`.
    Bool,
    /// `EChar`.
    Char,
    /// A numeric builtin, at the width the generator compiles.
    Num {
        /// The width.
        num: NumKind,
    },
    /// An enum literal.
    Enum {
        /// Slot of the enum in `enums`, not in `classes`.
        class: ClassSlot,
    },
}

/// The collection a feature's values sit in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
    /// `UWMapLog`: many, addressed by a key rather than by a position, from
    /// a `urn:arachne:semantics` `datatype="uw-map"` annotation naming a key
    /// and a value feature of the collection's target
    /// (`containment.rs:110-168`). The entry class itself is not
    /// represented: the map holds what its value feature holds, under the
    /// key its key feature carries.
    Keyed {
        /// What the keys are.
        key: KeyKind,
    },
    /// `GraphLog<List<T>>`: many, ordered, over the values themselves rather
    /// than over a log each. `unique` *and* `ordered`, which is what Ecore's
    /// two silent facets mean, and what `attribute.rs:152-162` compiles.
    ///
    /// It is a distinct construction and not a [`Shape::Sequence`]: a sequence
    /// is a `NestedListLog` whose elements are leaf logs a later operation can
    /// still edit, and this one holds plain values that are only inserted and
    /// deleted. Both paths merge it the same way, so nothing degrades it any
    /// more.
    OrderedSet,
}

/// Why a feature carries no rule the interpreted path can run.
///
/// The spellings are `camelCase`, which for the five single words below is
/// the `lowercase` they were written under until 2026-09-18 and is therefore
/// no change to any descriptor already emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
#[serde(rename_all = "camelCase")]
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
    /// A containment typed by Ecore's `EObject`, the class of every object:
    /// `EAnnotation.contents`, and any feature a metamodel types that way.
    ///
    /// The generated path emits no field for one and warns
    /// (`classifier/mod.rs:173`): a containment compiles to the target's own
    /// log, and `EObject` has no log because it has no feature and stands for
    /// every class at once. Nothing to merge, so nothing is claimed.
    EObjectContainment,
    /// A non-containment reference typed by Ecore's `EObject`:
    /// `EAnnotation.references`, and any feature a metamodel types that way.
    ///
    /// A reference is carried by `moirai-macros`' typed graph, whose arc kinds
    /// name a source and a target *class*; there is no arc that may point at
    /// any vertex (code note 39), so the generated path emits no vertex kind,
    /// no edge and no arc for one, and warns.
    EObjectReference,
}

impl UnsupportedReason {
    /// The word used in the descriptor and in a refusal sentence.
    ///
    /// The same string `serde` writes for the variant, which is what lets the
    /// emitter render a reason by hand and the parser read it back.
    /// `the_reason_words_are_the_serde_spellings` holds the two together.
    pub const fn as_str(self) -> &'static str {
        match self {
            UnsupportedReason::Keyed => "keyed",
            UnsupportedReason::Transparent => "transparent",
            UnsupportedReason::Derived => "derived",
            UnsupportedReason::Transient => "transient",
            UnsupportedReason::Volatile => "volatile",
            UnsupportedReason::EObjectContainment => "eObjectContainment",
            UnsupportedReason::EObjectReference => "eObjectReference",
        }
    }

    /// The sentence a refusal gives for this reason, in the words of what the
    /// generated path does with such a feature.
    pub const fn why(self) -> &'static str {
        match self {
            UnsupportedReason::Keyed => {
                "it is keyed by a feature of its target and carries no rule of its own"
            }
            UnsupportedReason::Transparent => {
                "its class is represented by one of its own fields and has no record"
            }
            UnsupportedReason::Derived => "it is `derived`, so nothing writes it",
            UnsupportedReason::Transient => "it is `transient`, so nothing stores it",
            UnsupportedReason::Volatile => "it is `volatile`, so nothing stores it",
            UnsupportedReason::EObjectContainment => {
                "it contains an object of any class, which neither path represents"
            }
            UnsupportedReason::EObjectReference => {
                "it refers to an object of any class, and a typed graph cannot point an arc at \
                 any vertex"
            }
        }
    }

    /// Whether the parser refuses a whole metamodel over this reason, per
    /// decision D6: a keyed or transparent feature is a form the interpreted
    /// node has no node type for, and the honest answer is to stay on the
    /// generated path. The other five are recorded and simply not merged,
    /// which is what the generated path does with them too: a `derived`,
    /// `transient` or `EObject`-typed feature is a feature *neither* path
    /// stores, so a metamodel that has one is not thereby a metamodel the
    /// interpreted path runs differently.
    pub const fn refused_at_parse(self) -> bool {
        matches!(
            self,
            UnsupportedReason::Keyed | UnsupportedReason::Transparent
        )
    }
}

/// One feature's whole merge policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
    /// The class is represented by one of its features and by nothing else
    /// (`urn:arachne:representation` `kind="transparent"`,
    /// `annotation.rs:59-70`): the *visible* slot of that feature.
    ///
    /// The generator emits no record for such a class; a containment onto it
    /// carries the field's own construction directly, which is why
    /// `classifiers.rs` spells `JsonKind`'s `Array` variant as
    /// `NestedList<Box<JsonKind>>` and not as an `Array` record holding one.
    /// The interpreted path still mints an object here, because routing a
    /// feature step is what it does; what changes is the read-out, which
    /// renders the object as that one feature's value with no `eClass` key
    /// and no wrapper.
    pub transparent: Option<FeatureSlot>,
    /// The concrete classes an instance of this class may actually be: the
    /// instantiable descendants, this class included when it is
    /// instantiable, ascending by slot.
    pub concrete: Arc<[ClassSlot]>,
}

/// One enum class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
#[cfg_attr(feature = "deepsize", derive(deepsize::DeepSizeOf))]
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
