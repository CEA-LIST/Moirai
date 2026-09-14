//! The conflict matrix: every merge construction the interpreted path can use,
//! every conflict pattern that construction's policy exists to resolve, and a
//! deterministic runner that drives one pattern through both paths in every
//! schedule that matters.
//!
//! # Why this exists beside the seeded oracles
//!
//! A seeded oracle says "no divergence seen on these scripts". It does not say
//! that a concurrent add and remove of the same set value was ever generated,
//! that a register tie ever landed on each residue of the member count, or that
//! a replica was ever compared against another replica of its own path after a
//! stabilization that the two had reached through different acknowledgements.
//! The two convergence defects of code note 34 lived in exactly those gaps.
//! This module is the other sentence: for each (construction, pattern) cell,
//! the exact conflicting operations are built by hand and driven through both
//! paths, and every schedule below is asserted.
//!
//! # The registry is the whole matrix, in one place
//!
//! [`Construction`] names every row, [`row`] gives each row its patterns and
//! the one generated crate whose metamodel reaches it (or the reason none can),
//! and both are exhaustive matches: a `LeafLog` arm, a `Shape`, a `LeafRule`,
//! a `TieBreak` or a `NumKind` added tomorrow does not compile until it has a
//! row. A generated crate's matrix test calls [`check_coverage`], which fails
//! when that crate's cell table is missing a cell the registry assigns to it or
//! carries one the registry does not name, and when the feature the registry
//! names does not actually carry that construction in the crate's own table.
//!
//! # What one cell run asserts
//!
//! A [`Cell`] is a setup issued by the first seat and delivered everywhere, then
//! one to three *lists* of operations, each issued by one seat against the
//! shared setup state with nothing delivered in between, so that every
//! operation of one list is concurrent with every operation of another; that
//! concurrency is checked on the version vectors, not assumed. [`run_cell`]
//! runs the cell under every [`Schedule`]:
//!
//! - **members**: two replicas and three, the third seat idle when the pattern
//!   has two lists, so every two-way pattern also has an observer that receives
//!   the two sides in both orders;
//! - **seats**: every injective assignment of the lists to seats, which at two
//!   replicas is both delivery orders and, for a tie-break that reads the origin
//!   id, both outcomes of the tie;
//! - **pads**: extra delivered operations from the first seat before the
//!   conflict, which move the Lamport time of a tie across the residues of the
//!   member count that `FairPolicy` reads;
//! - **prefix**: the setup left unstable, or stabilized everywhere before the
//!   conflict is issued;
//! - **acknowledgement**: lazy, where every conflicting operation is delivered
//!   before anyone speaks again, or eager, where each receiver answers every
//!   delivery with a heartbeat that reaches the others at once, so the conflict
//!   stabilizes a piece at a time and differently at each replica;
//! - **orders**: every order in which a receiver can take its incoming
//!   operations, first in first out per sender.
//!
//! At every send and every delivery the replica that moved is compared against
//! its twin on the other path, read-out and causal-stability snapshot both.
//! Every event is checked to carry the same origin, sequence number, Lamport
//! time and version vector on both paths. Once the conflict has been delivered
//! everywhere, and again once every replica has stabilized every operation of
//! the cell, every replica is compared against every other replica of the same
//! path. A cell may also name the value a feature must settle on.
//!
//! The canonical projection is the calling crate's own, unchanged: this module
//! compares two `serde_json::Value`s and knows nothing of either read-out.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};

use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    log_id::LogId,
    replica::{IsReplica, Replica},
    state::log::IsLog,
    utils::intern_str::InternalizeOp,
};
use moirai_semantics::{
    FlagWins, LeafRule, MergeRule, MetamodelSemantics, NumKind, SetTie, Shape, TieBreak,
};
use serde_json::Value;

use crate::leaf::LeafLog;

/// A replica of one path.
pub type Rep<L> = Replica<L, Tcsb<<L as IsLog>::Op>>;

// ---------------------------------------------------------------------------
// 1. The rows
// ---------------------------------------------------------------------------

/// One merge construction the interpreted path can merge by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Construction {
    /// `EventGraph<List<char>>`.
    Text,
    /// `Counter<T>` at one width.
    Counter(NumKind),
    /// `SimpleCounter<T>` at one width.
    SimpleCounter(NumKind),
    /// `EWFlag`.
    EnableWinsFlag,
    /// `DWFlag`.
    DisableWinsFlag,
    /// A register over a scalar, at one tie-break.
    Register(TieBreak),
    /// A register over an enum literal, at one tie-break.
    EnumRegister(TieBreak),
    /// `AWSet`.
    AddWinsSet,
    /// `RWSet`.
    RemoveWinsSet,
    /// `AWBagLog`, spelled out as `UWMapLog<V, VecLog<Counter<usize>>>`.
    Bag,
    /// `NestedListLog` over attribute values.
    SequenceOfValues,
    /// `OptionLog` over an attribute value.
    OptionalAttribute,
    /// A single-valued containment, a `union!` when the target has subtypes.
    SingleContainment,
    /// `OptionLog` over a contained object.
    OptionalContainment,
    /// `NestedListLog` over contained objects.
    SequenceContainment,
    /// `UWMapLog` over contained objects.
    KeyedMap,
}

const NUMS: [NumKind; 6] = [
    NumKind::U8,
    NumKind::I16,
    NumKind::I32,
    NumKind::I64,
    NumKind::F32,
    NumKind::F64,
];

const TIES: [TieBreak; 5] = [
    TieBreak::MultiValue,
    TieBreak::LastWriterWins,
    TieBreak::Fair,
    TieBreak::PartialOrder,
    TieBreak::TotalOrder,
];

impl fmt::Display for Construction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Construction::Text => write!(f, "text"),
            Construction::Counter(num) => write!(f, "resettable counter<{num:?}>"),
            Construction::SimpleCounter(num) => write!(f, "simple counter<{num:?}>"),
            Construction::EnableWinsFlag => write!(f, "enable-wins flag"),
            Construction::DisableWinsFlag => write!(f, "disable-wins flag"),
            Construction::Register(tie) => write!(f, "register<{tie:?}>"),
            Construction::EnumRegister(tie) => write!(f, "enum register<{tie:?}>"),
            Construction::AddWinsSet => write!(f, "add-wins set"),
            Construction::RemoveWinsSet => write!(f, "remove-wins set"),
            Construction::Bag => write!(f, "bag"),
            Construction::SequenceOfValues => write!(f, "sequence of attribute values"),
            Construction::OptionalAttribute => write!(f, "optional attribute"),
            Construction::SingleContainment => write!(f, "single containment"),
            Construction::OptionalContainment => write!(f, "optional containment"),
            Construction::SequenceContainment => write!(f, "sequence containment"),
            Construction::KeyedMap => write!(f, "keyed map"),
        }
    }
}

impl Construction {
    /// Every row, reachable or not.
    pub fn all() -> Vec<Construction> {
        let mut out = vec![Construction::Text];
        out.extend(NUMS.iter().map(|num| Construction::Counter(*num)));
        out.extend(NUMS.iter().map(|num| Construction::SimpleCounter(*num)));
        out.push(Construction::EnableWinsFlag);
        out.push(Construction::DisableWinsFlag);
        out.extend(TIES.iter().map(|tie| Construction::Register(*tie)));
        out.extend(TIES.iter().map(|tie| Construction::EnumRegister(*tie)));
        out.extend([
            Construction::AddWinsSet,
            Construction::RemoveWinsSet,
            Construction::Bag,
            Construction::SequenceOfValues,
            Construction::OptionalAttribute,
            Construction::SingleContainment,
            Construction::OptionalContainment,
            Construction::SequenceContainment,
            Construction::KeyedMap,
        ]);
        out
    }
}

/// The row a leaf arm belongs to. Exhaustive: a twenty-fourth arm does not
/// compile until it is placed.
pub fn leaf_construction(leaf: &LeafLog) -> Construction {
    match leaf {
        LeafLog::Text(_) => Construction::Text,
        LeafLog::CounterU8(_) => Construction::Counter(NumKind::U8),
        LeafLog::CounterI16(_) => Construction::Counter(NumKind::I16),
        LeafLog::CounterI32(_) => Construction::Counter(NumKind::I32),
        LeafLog::CounterI64(_) => Construction::Counter(NumKind::I64),
        LeafLog::CounterF32(_) => Construction::Counter(NumKind::F32),
        LeafLog::CounterF64(_) => Construction::Counter(NumKind::F64),
        LeafLog::SimpleCounterU8(_) => Construction::SimpleCounter(NumKind::U8),
        LeafLog::SimpleCounterI16(_) => Construction::SimpleCounter(NumKind::I16),
        LeafLog::SimpleCounterI32(_) => Construction::SimpleCounter(NumKind::I32),
        LeafLog::SimpleCounterI64(_) => Construction::SimpleCounter(NumKind::I64),
        LeafLog::SimpleCounterF32(_) => Construction::SimpleCounter(NumKind::F32),
        LeafLog::SimpleCounterF64(_) => Construction::SimpleCounter(NumKind::F64),
        LeafLog::FlagEw(_) => Construction::EnableWinsFlag,
        LeafLog::FlagDw(_) => Construction::DisableWinsFlag,
        LeafLog::RegisterMv(_) => Construction::Register(TieBreak::MultiValue),
        LeafLog::RegisterLww(_) => Construction::Register(TieBreak::LastWriterWins),
        LeafLog::RegisterFair(_) => Construction::Register(TieBreak::Fair),
        LeafLog::RegisterPo(_) => Construction::Register(TieBreak::PartialOrder),
        LeafLog::RegisterTo(_) => Construction::Register(TieBreak::TotalOrder),
        LeafLog::SetAw(_) => Construction::AddWinsSet,
        LeafLog::SetRw(_) => Construction::RemoveWinsSet,
        LeafLog::Bag(_) => Construction::Bag,
    }
}

/// The rows one feature's rule exercises: the shape it sits in, when that
/// shape is a construction of its own, and the leaf inside it. Exhaustive over
/// `MergeRule`, `Shape` and `LeafRule`.
pub fn rule_constructions(rule: &MergeRule) -> Vec<Construction> {
    let leaf = |leaf: LeafRule| match leaf {
        LeafRule::Text => Construction::Text,
        LeafRule::Counter {
            num,
            resettable: true,
        } => Construction::Counter(num),
        LeafRule::Counter {
            num,
            resettable: false,
        } => Construction::SimpleCounter(num),
        LeafRule::Flag {
            wins: FlagWins::Enable,
        } => Construction::EnableWinsFlag,
        LeafRule::Flag {
            wins: FlagWins::Disable,
        } => Construction::DisableWinsFlag,
        LeafRule::Register { tie } => Construction::Register(tie),
        LeafRule::Enum { tie, .. } => Construction::EnumRegister(tie),
    };
    match rule {
        MergeRule::Attribute { shape, leaf: rule } => match shape.effective() {
            Shape::Single => vec![leaf(*rule)],
            Shape::Optional => vec![Construction::OptionalAttribute, leaf(*rule)],
            Shape::Sequence => vec![Construction::SequenceOfValues, leaf(*rule)],
            Shape::Set {
                tie: SetTie::AddWins,
            } => vec![Construction::AddWinsSet],
            Shape::Set {
                tie: SetTie::RemoveWins,
            } => vec![Construction::RemoveWinsSet],
            Shape::Bag => vec![Construction::Bag],
            Shape::Keyed { .. } => vec![Construction::KeyedMap],
            Shape::OrderedSet => unreachable!("`effective` degrades an ordered set"),
        },
        MergeRule::Containment { shape, .. } => match shape.effective() {
            Shape::Single => vec![Construction::SingleContainment],
            Shape::Optional => vec![Construction::OptionalContainment],
            Shape::Sequence => vec![Construction::SequenceContainment],
            Shape::Keyed { .. } => vec![Construction::KeyedMap],
            Shape::Set { .. } | Shape::Bag => Vec::new(),
            Shape::OrderedSet => unreachable!("`effective` degrades an ordered set"),
        },
        MergeRule::Reference { .. } | MergeRule::Unsupported { .. } => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// 2. The columns, and where each row is driven
// ---------------------------------------------------------------------------

/// The conflict patterns, by the name a cell carries. Each is derived from the
/// CRDT's own definition in `moirai-crdt`; `02 Validation Plan` §9 says from
/// which part of it.
pub mod pattern {
    #![allow(missing_docs)]
    pub const INSERT_INSERT_SAME_POS: &str = "insert ∥ insert at one position";
    pub const INSERT_INSERT_SAME_CHAR: &str = "insert ∥ insert of the same char at one position";
    pub const INSERT_DELETE_SAME_CHAR: &str = "insert beside a char ∥ delete of that char";
    pub const DELETE_DELETE_SAME: &str = "delete ∥ delete of the same element";
    pub const DELETE_DELETE_DIFFERENT: &str = "delete ∥ delete of different elements";

    pub const INC_INC: &str = "inc ∥ inc";
    pub const INC_DEC: &str = "inc ∥ dec";
    pub const INC_RESET: &str = "inc ∥ reset";
    pub const RESET_RESET: &str = "reset ∥ reset";

    pub const ENABLE_DISABLE: &str = "enable ∥ disable";
    pub const ENABLE_ENABLE: &str = "enable ∥ enable";
    pub const DISABLE_DISABLE: &str = "disable ∥ disable";
    pub const ENABLE_CLEAR: &str = "enable ∥ clear";
    pub const DISABLE_CLEAR: &str = "disable ∥ clear";

    pub const WRITE_WRITE_DIFFERENT: &str = "write ∥ write of different values";
    pub const WRITE_WRITE_SAME: &str = "write ∥ write of the same value";
    pub const WRITE_CLEAR: &str = "write ∥ clear";
    pub const TIE_EVERY_RESIDUE: &str = "Lamport tie at every residue of the member count";
    pub const LAMPORT_DOMINATES: &str = "write ∥ write whose Lamport time dominates";
    pub const THREE_WRITERS: &str = "three concurrent writes";

    pub const ADD_REMOVE_PRESENT: &str = "add ∥ remove of the same element, present";
    pub const ADD_REMOVE_ABSENT: &str = "add ∥ remove of the same value, absent";
    pub const ADD_REMOVE_DIFFERENT: &str = "add ∥ remove of different values";
    pub const ADD_ADD_SAME: &str = "add ∥ add of the same value";
    pub const REMOVE_REMOVE_SAME: &str = "remove ∥ remove of the same value";
    pub const ADD_CLEAR: &str = "add ∥ clear";
    pub const ADD_REMOVE_THEN_CLEAR: &str = "add ∥ remove then clear";

    pub const INSERT_DELETE: &str = "insert ∥ delete";
    pub const DELETE_UPDATE_SAME: &str = "delete ∥ update of the same element";
    pub const UPDATE_UPDATE_SAME: &str = "update ∥ update of the same element";
    pub const THREE_INSERTS_SAME_POS: &str = "three inserts at one position";

    pub const SET_UNSET: &str = "set ∥ unset";
    pub const SET_SET: &str = "set ∥ set";
    pub const UNSET_UNSET: &str = "unset ∥ unset";

    pub const DIFFERENT_SUBTYPES: &str = "instantiate ∥ instantiate of different subtypes";
    pub const SAME_SUBTYPE: &str = "instantiate ∥ instantiate of the same subtype";
    pub const UPDATE_UPDATE_CHILD: &str = "update ∥ update into the one child";

    pub const UPDATE_REMOVE_KEY: &str = "update ∥ remove of one key";
    pub const UPDATE_UPDATE_KEY: &str = "update ∥ update of one key";
    pub const TWO_KINDS_ONE_KEY: &str = "put ∥ put of two kinds at one key";
    pub const REMOVE_REMOVE_KEY: &str = "remove ∥ remove of one key";
    pub const UPDATE_CLEAR: &str = "update ∥ clear";
    pub const TWO_KEYS: &str = "put ∥ put at two keys";
}

/// Where a row is driven.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reach {
    /// By this generated crate, through this feature, written
    /// `Class.feature`.
    Crate {
        /// The generated crate's package name.
        krate: &'static str,
        /// `Class.feature` in that crate's metamodel.
        feature: &'static str,
    },
    /// By nothing, for this reason.
    Unreachable(&'static str),
}

/// One row of the matrix.
#[derive(Clone, Copy, Debug)]
pub struct Row {
    /// The construction.
    pub construction: Construction,
    /// Its conflict patterns.
    pub patterns: &'static [&'static str],
    /// Where it is driven.
    pub reach: Reach,
}

use pattern::*;

const TEXT: &[&str] = &[
    INSERT_INSERT_SAME_POS,
    INSERT_INSERT_SAME_CHAR,
    INSERT_DELETE_SAME_CHAR,
    DELETE_DELETE_SAME,
    DELETE_DELETE_DIFFERENT,
];
const COUNTER: &[&str] = &[INC_INC, INC_DEC, INC_RESET, RESET_RESET];
const FLAG: &[&str] = &[
    ENABLE_DISABLE,
    ENABLE_ENABLE,
    DISABLE_DISABLE,
    ENABLE_CLEAR,
    DISABLE_CLEAR,
];
const CLEARABLE_REGISTER: &[&str] = &[
    WRITE_WRITE_DIFFERENT,
    WRITE_WRITE_SAME,
    WRITE_CLEAR,
    THREE_WRITERS,
];
const POLICY_REGISTER: &[&str] = &[
    WRITE_WRITE_DIFFERENT,
    WRITE_WRITE_SAME,
    TIE_EVERY_RESIDUE,
    LAMPORT_DOMINATES,
    THREE_WRITERS,
];
const SET: &[&str] = &[
    ADD_REMOVE_PRESENT,
    ADD_REMOVE_ABSENT,
    ADD_REMOVE_DIFFERENT,
    ADD_ADD_SAME,
    REMOVE_REMOVE_SAME,
    ADD_CLEAR,
    ADD_REMOVE_THEN_CLEAR,
];
const BAG: &[&str] = &[
    ADD_REMOVE_PRESENT,
    ADD_ADD_SAME,
    ADD_REMOVE_DIFFERENT,
    REMOVE_REMOVE_SAME,
    ADD_CLEAR,
];
const SEQUENCE: &[&str] = &[
    INSERT_INSERT_SAME_POS,
    INSERT_DELETE,
    DELETE_UPDATE_SAME,
    DELETE_DELETE_SAME,
    UPDATE_UPDATE_SAME,
    THREE_INSERTS_SAME_POS,
];
const OPTIONAL: &[&str] = &[SET_UNSET, SET_SET, UNSET_UNSET];
const SINGLE: &[&str] = &[DIFFERENT_SUBTYPES, SAME_SUBTYPE, UPDATE_UPDATE_CHILD];
const KEYED: &[&str] = &[
    UPDATE_REMOVE_KEY,
    UPDATE_UPDATE_KEY,
    TWO_KINDS_ONE_KEY,
    REMOVE_REMOVE_KEY,
    UPDATE_CLEAR,
    TWO_KEYS,
];

/// The generated crates that drive the matrix.
pub const CLASSDIAGRAM: &str = "classdiagram_crdt";
/// See [`CLASSDIAGRAM`].
pub const KITCHEN: &str = "kitchen_crdt";
/// See [`CLASSDIAGRAM`].
pub const BT: &str = "bt_crdt";
/// See [`CLASSDIAGRAM`].
pub const JSON: &str = "json_crdt";

const fn at(krate: &'static str, feature: &'static str) -> Reach {
    Reach::Crate { krate, feature }
}

/// The row of one construction. Exhaustive, so a new construction has no
/// patterns and no reach until someone writes them here.
pub fn row(construction: Construction) -> Row {
    let (patterns, reach) = match construction {
        Construction::Text => (TEXT, at(CLASSDIAGRAM, "Class.name")),
        Construction::Counter(num) => (
            COUNTER,
            at(
                KITCHEN,
                match num {
                    NumKind::U8 => "Foo.myByte",
                    NumKind::I16 => "Foo.myShort",
                    NumKind::I32 => "Foo.myInt",
                    NumKind::I64 => "Foo.myLong",
                    NumKind::F32 => "Foo.myFloat",
                    NumKind::F64 => "Foo.myDouble",
                },
            ),
        ),
        Construction::SimpleCounter(_) => (
            COUNTER,
            Reach::Unreachable(
                "no annotation spelling produces a non-resettable counter and every numeric \
                 builtin maps to a resettable one, so no generated crate holds one",
            ),
        ),
        Construction::EnableWinsFlag => (FLAG, at(KITCHEN, "Foo.myBoolean")),
        Construction::DisableWinsFlag => (FLAG, at(CLASSDIAGRAM, "Class.isAbstract")),
        Construction::Register(TieBreak::MultiValue) => {
            (CLEARABLE_REGISTER, at(KITCHEN, "Foo.myChar"))
        }
        Construction::Register(TieBreak::LastWriterWins) => {
            (POLICY_REGISTER, at(CLASSDIAGRAM, "Class.qualifiedName"))
        }
        Construction::Register(TieBreak::Fair) => {
            (POLICY_REGISTER, at(CLASSDIAGRAM, "Class.author"))
        }
        Construction::Register(TieBreak::PartialOrder) => {
            (CLEARABLE_REGISTER, at(CLASSDIAGRAM, "Class.stereotype"))
        }
        Construction::Register(TieBreak::TotalOrder) => {
            (CLEARABLE_REGISTER, at(CLASSDIAGRAM, "Class.layer"))
        }
        Construction::EnumRegister(TieBreak::MultiValue) => {
            (CLEARABLE_REGISTER, at(CLASSDIAGRAM, "Class.visibility"))
        }
        Construction::EnumRegister(TieBreak::LastWriterWins | TieBreak::Fair) => (
            POLICY_REGISTER,
            Reach::Unreachable("no checked-in metamodel annotates an enum-typed attribute"),
        ),
        Construction::EnumRegister(TieBreak::PartialOrder | TieBreak::TotalOrder) => (
            CLEARABLE_REGISTER,
            Reach::Unreachable("no checked-in metamodel annotates an enum-typed attribute"),
        ),
        Construction::AddWinsSet => (SET, at(CLASSDIAGRAM, "Class.tags")),
        Construction::RemoveWinsSet => (SET, at(CLASSDIAGRAM, "Class.invariants")),
        Construction::Bag => (BAG, at(KITCHEN, "Foo.bag")),
        Construction::SequenceOfValues => (SEQUENCE, at(KITCHEN, "Foo.bounds0inf")),
        Construction::OptionalAttribute => (OPTIONAL, at(BT, "TreeNode.name")),
        Construction::SingleContainment => (SINGLE, at(BT, "BehaviorTree.child")),
        Construction::OptionalContainment => (
            OPTIONAL,
            Reach::Unreachable(
                "derived by the generator and reached by `concrete_polymorphic_targets.ecore`'s \
                 `D.child`, but no generated crate exists for that metamodel",
            ),
        ),
        Construction::SequenceContainment => (SEQUENCE, at(BT, "ControlNode.children")),
        Construction::KeyedMap => (KEYED, at(JSON, "Object.entry")),
    };
    Row {
        construction,
        patterns,
        reach,
    }
}

/// Every row.
pub fn matrix() -> Vec<Row> {
    Construction::all().into_iter().map(row).collect()
}

/// The rule of a feature named `Class.feature`, resolved through the class's
/// visible features so an inherited one is found at its declaring class.
pub fn rule_named(sem: &MetamodelSemantics, qualified: &str) -> Option<MergeRule> {
    let (class, feature) = qualified.split_once('.')?;
    let class = sem.classes.iter().find(|held| &*held.name == class)?;
    let (_, owner, slot) = class
        .visible
        .iter()
        .find(|(name, _, _)| &**name == feature)?;
    sem.rule(*owner, *slot).copied()
}

/// The cells one generated crate carries against the rows the registry
/// assigns to it: nothing missing, nothing extra, nothing twice, and every
/// named feature carrying its construction in that crate's own table.
pub fn check_coverage<E>(
    krate: &str,
    sem: &MetamodelSemantics,
    cells: &[Cell<E>],
) -> Result<usize, String> {
    let mut wanted: BTreeSet<(Construction, &'static str)> = BTreeSet::new();
    let mut problems = Vec::new();
    for row in matrix() {
        let Reach::Crate {
            krate: owner,
            feature,
        } = row.reach
        else {
            continue;
        };
        if owner != krate {
            continue;
        }
        match rule_named(sem, feature) {
            Some(rule) if rule_constructions(&rule).contains(&row.construction) => {}
            other => problems.push(format!(
                "the registry drives {} through `{feature}`, whose rule in {krate}'s table is \
                 {other:?}",
                row.construction
            )),
        }
        for pattern in row.patterns {
            wanted.insert((row.construction, pattern));
        }
    }
    let mut held: BTreeSet<(Construction, &'static str)> = BTreeSet::new();
    for cell in cells {
        if !held.insert((cell.construction, cell.pattern)) {
            problems.push(format!(
                "{} / {} is carried twice",
                cell.construction, cell.pattern
            ));
        }
    }
    for missing in wanted.difference(&held) {
        problems.push(format!(
            "{} / {} is assigned to {krate} and has no cell",
            missing.0, missing.1
        ));
    }
    for extra in held.difference(&wanted) {
        problems.push(format!(
            "{} / {} has a cell in {krate} and no row in the registry",
            extra.0, extra.1
        ));
    }
    if problems.is_empty() {
        Ok(wanted.len())
    } else {
        Err(problems.join("\n"))
    }
}

// ---------------------------------------------------------------------------
// 3. A cell and the two arms it runs on
// ---------------------------------------------------------------------------

/// One path, as the runner sees it: how an edit becomes an operation, and how
/// a replica reads out in the canonical form.
pub struct Arm<'a, E, L: IsLog> {
    /// `interpreted` or `generated`, for the messages.
    pub name: &'static str,
    /// An edit as this path's operation. The second argument is the sending
    /// replica's *interpreted* read-out, which is what the existing encoders
    /// that resolve a path against a document are given.
    pub encode: &'a dyn Fn(&E, &Value) -> L::Op,
    /// A replica's read-out under the calling crate's canonical projection.
    pub read: &'a dyn Fn(&Rep<L>) -> Value,
}

/// One (construction, pattern) cell.
#[derive(Clone, Debug)]
pub struct Cell<E> {
    /// The row.
    pub construction: Construction,
    /// The column.
    pub pattern: &'static str,
    /// Issued by the first seat and delivered everywhere before the conflict.
    pub setup: Vec<E>,
    /// The concurrent lists, one per contending seat.
    pub lists: Vec<Vec<E>>,
    /// An edit on some other feature, used to acknowledge and to pad.
    pub heartbeat: E,
    /// Member counts to run at.
    pub members: Vec<usize>,
    /// Numbers of padding operations to run with.
    pub pads: Vec<usize>,
    /// Whether the first operations of the lists must share a Lamport time.
    pub tie: bool,
    /// JSON pointers into the read-out and the value each must settle on once
    /// everything is stable; `null` stands for a key the projection dropped as
    /// default.
    pub expect: Vec<(&'static str, Value)>,
}

impl<E> Cell<E> {
    /// A cell at two and three members, no padding, no tie asserted and no
    /// value expected.
    pub fn new(
        construction: Construction,
        pattern: &'static str,
        setup: Vec<E>,
        lists: Vec<Vec<E>>,
        heartbeat: E,
    ) -> Self {
        let members = if lists.len() > 2 { vec![3] } else { vec![2, 3] };
        Cell {
            construction,
            pattern,
            setup,
            lists,
            heartbeat,
            members,
            pads: vec![0],
            tie: false,
            expect: Vec::new(),
        }
    }

    /// Run with each of these numbers of padding operations.
    pub fn pads(mut self, pads: &[usize]) -> Self {
        self.pads = pads.to_vec();
        self
    }

    /// Assert the first operations of the lists tie on Lamport time.
    pub fn tie(mut self) -> Self {
        self.tie = true;
        self
    }

    /// The value at `pointer` once everything is stable, on every replica of
    /// both paths.
    pub fn expect(mut self, pointer: &'static str, value: Value) -> Self {
        self.expect.push((pointer, value));
        self
    }
}

/// What one cell's runs amounted to.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// Schedules run.
    pub schedules: usize,
    /// Replica-against-twin comparisons made.
    pub twin_comparisons: usize,
    /// Replica-against-replica comparisons made.
    pub replica_comparisons: usize,
    /// Events sent, per path.
    pub events: usize,
    /// Twin comparisons made while some but not all of the cell's conflicting
    /// operations were stable at the replica compared.
    pub partially_stable: usize,
    /// Twin comparisons made with every conflicting operation stable there.
    pub fully_stable: usize,
    /// Twin comparisons made with no conflicting operation stable there.
    pub unstable: usize,
}

impl Stats {
    fn add(&mut self, other: Stats) {
        self.schedules += other.schedules;
        self.twin_comparisons += other.twin_comparisons;
        self.replica_comparisons += other.replica_comparisons;
        self.events += other.events;
        self.partially_stable += other.partially_stable;
        self.fully_stable += other.fully_stable;
        self.unstable += other.unstable;
    }
}

// ---------------------------------------------------------------------------
// 4. Schedules
// ---------------------------------------------------------------------------

/// One way of running a cell.
#[derive(Clone, Debug)]
pub struct Schedule {
    /// How many replicas per path.
    pub members: usize,
    /// Per seat, the list it issues, if any.
    pub seats: Vec<Option<usize>>,
    /// Padding operations before the conflict.
    pub pad: usize,
    /// Whether the setup is stabilized before the conflict.
    pub stable_prefix: bool,
    /// Whether each delivery is acknowledged at once.
    pub eager: bool,
    /// Per receiving seat, the senders it takes one operation from, in order.
    pub orders: Vec<Vec<usize>>,
}

const SEATS: [&str; 3] = ["a", "b", "c"];

/// Above this many order combinations for one assignment, orders are varied
/// one receiver at a time instead of as a product.
const PRODUCT_LIMIT: usize = 64;

impl fmt::Display for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seats: Vec<String> = self
            .seats
            .iter()
            .enumerate()
            .map(|(seat, list)| match list {
                Some(list) => format!("{}:list{list}", SEATS[seat]),
                None => format!("{}:idle", SEATS[seat]),
            })
            .collect();
        let orders: Vec<String> = self
            .orders
            .iter()
            .enumerate()
            .map(|(seat, from)| {
                let from: String = from.iter().map(|sender| SEATS[*sender]).collect();
                format!("{}<-{from}", SEATS[seat])
            })
            .collect();
        write!(
            f,
            "{} members [{}], pad {}, prefix {}, {} acks, orders [{}]",
            self.members,
            seats.join(" "),
            self.pad,
            if self.stable_prefix {
                "stabilized"
            } else {
                "unstable"
            },
            if self.eager { "eager" } else { "lazy" },
            orders.join(" ")
        )
    }
}

/// Every injective assignment of `lists` lists to `members` seats.
fn assignments(lists: usize, members: usize) -> Vec<Vec<Option<usize>>> {
    fn go(
        list: usize,
        lists: usize,
        seats: &mut Vec<Option<usize>>,
        out: &mut Vec<Vec<Option<usize>>>,
    ) {
        if list == lists {
            out.push(seats.clone());
            return;
        }
        for seat in 0..seats.len() {
            if seats[seat].is_none() {
                seats[seat] = Some(list);
                go(list + 1, lists, seats, out);
                seats[seat] = None;
            }
        }
    }
    let mut out = Vec::new();
    go(0, lists, &mut vec![None; members], &mut out);
    out
}

/// Every distinct sequence taking `count` items from each sender, first in
/// first out per sender.
fn interleavings(counts: &[(usize, usize)]) -> Vec<Vec<usize>> {
    fn go(left: &mut Vec<(usize, usize)>, prefix: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if left.iter().all(|(_, count)| *count == 0) {
            out.push(prefix.clone());
            return;
        }
        for index in 0..left.len() {
            if left[index].1 > 0 {
                left[index].1 -= 1;
                prefix.push(left[index].0);
                go(left, prefix, out);
                prefix.pop();
                left[index].1 += 1;
            }
        }
    }
    let mut out = Vec::new();
    go(&mut counts.to_vec(), &mut Vec::new(), &mut out);
    out
}

/// Every schedule a cell runs under.
pub fn schedules<E>(cell: &Cell<E>) -> Vec<Schedule> {
    let mut out = Vec::new();
    for &members in &cell.members {
        assert!(
            members >= cell.lists.len() && members <= SEATS.len(),
            "{} / {}: {} lists at {members} members",
            cell.construction,
            cell.pattern,
            cell.lists.len()
        );
        for seats in assignments(cell.lists.len(), members) {
            let per_receiver: Vec<Vec<Vec<usize>>> = (0..members)
                .map(|receiver| {
                    let counts: Vec<(usize, usize)> = seats
                        .iter()
                        .enumerate()
                        .filter(|(sender, _)| *sender != receiver)
                        .filter_map(|(sender, list)| list.map(|list| (sender, cell.lists[list].len())))
                        .collect();
                    interleavings(&counts)
                })
                .collect();
            let product: usize = per_receiver.iter().map(Vec::len).product();
            let mut order_sets: Vec<Vec<Vec<usize>>> = Vec::new();
            if product <= PRODUCT_LIMIT {
                let mut combos: Vec<Vec<Vec<usize>>> = vec![Vec::new()];
                for options in &per_receiver {
                    let mut next = Vec::new();
                    for combo in &combos {
                        for option in options {
                            let mut grown = combo.clone();
                            grown.push(option.clone());
                            next.push(grown);
                        }
                    }
                    combos = next;
                }
                order_sets = combos;
            } else {
                // Lazily acknowledged, a receiver's state depends on its own
                // order alone, so every order of every receiver against the
                // first order of the others reaches every state the product
                // would, and equality with a fixed other replica is
                // transitive.
                let first: Vec<Vec<usize>> = per_receiver.iter().map(|o| o[0].clone()).collect();
                order_sets.push(first.clone());
                for (receiver, options) in per_receiver.iter().enumerate() {
                    for option in options.iter().skip(1) {
                        let mut varied = first.clone();
                        varied[receiver] = option.clone();
                        order_sets.push(varied);
                    }
                }
            }
            for &pad in &cell.pads {
                for stable_prefix in [false, true] {
                    for eager in [false, true] {
                        for orders in &order_sets {
                            out.push(Schedule {
                                members,
                                seats: seats.clone(),
                                pad,
                                stable_prefix,
                                eager,
                                orders: orders.clone(),
                            });
                        }
                    }
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 5. The runner
// ---------------------------------------------------------------------------

/// What identifies an event on the wire: origin, sequence number, Lamport time
/// and the version vector, by replica name.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Stamp {
    origin: String,
    seq: usize,
    lamport: usize,
    version: BTreeMap<String, usize>,
}

fn stamp<O>(message: &EventMessage<O>) -> Stamp {
    let event = message.event();
    let resolver = event.id().resolver();
    let version = event
        .version()
        .iter()
        .filter(|(_, seq)| *seq > 0)
        .map(|(idx, seq)| {
            (
                resolver
                    .resolve(idx)
                    .map_or_else(|| format!("<idx {}>", idx.0), str::to_string),
                seq,
            )
        })
        .collect();
    Stamp {
        origin: event.id().origin_id().to_string(),
        seq: event.id().seq(),
        lamport: event.lamport().val(),
        version,
    }
}

type Pair<I, G> = (
    EventMessage<<I as IsLog>::Op>,
    EventMessage<<G as IsLog>::Op>,
);

struct Run<'r, 'a, E, I: IsLog, G: IsLog> {
    cell: &'r Cell<E>,
    interp: &'r Arm<'a, E, I>,
    generated: &'r Arm<'a, E, G>,
    ir: Vec<Rep<I>>,
    gr: Vec<Rep<G>>,
    /// Events each seat has sent, which is its last sequence number.
    sent: Vec<usize>,
    /// The cell's conflicting operations, once issued.
    conflict: Vec<Stamp>,
    stats: Stats,
}

impl<E, I, G> Run<'_, '_, E, I, G>
where
    E: Debug,
    I: IsLog,
    G: IsLog,
    I::Op: Clone + Debug + InternalizeOp,
    G::Op: Clone + Debug + InternalizeOp,
{
    fn send(&mut self, seat: usize, edit: &E, what: &str) -> Result<(Pair<I, G>, Stamp), String> {
        let view = (self.interp.read)(&self.ir[seat]);
        let interp_op = (self.interp.encode)(edit, &view);
        let gen_op = (self.generated.encode)(edit, &view);
        let pair = match (self.ir[seat].send(interp_op), self.gr[seat].send(gen_op)) {
            (Some(interp), Some(generated)) => (interp, generated),
            (None, None) => {
                return Err(format!(
                    "{what}: both intakes refused {edit:?} at {}, so the cell does not say what \
                     it means to",
                    SEATS[seat]
                ));
            }
            (interp, _) => {
                return Err(format!(
                    "{what}: the two intakes disagree on {edit:?} at {}: {} took it, {} refused",
                    SEATS[seat],
                    if interp.is_some() {
                        self.interp.name
                    } else {
                        self.generated.name
                    },
                    if interp.is_some() {
                        self.generated.name
                    } else {
                        self.interp.name
                    },
                ));
            }
        };
        let interp_stamp = stamp(&pair.0);
        let gen_stamp = stamp(&pair.1);
        if interp_stamp != gen_stamp {
            return Err(format!(
                "{what}: {edit:?} at {} is a different event on the two paths: {} {interp_stamp:?}, \
                 {} {gen_stamp:?}",
                SEATS[seat], self.interp.name, self.generated.name
            ));
        }
        self.sent[seat] += 1;
        self.stats.events += 1;
        self.twin(seat, what)?;
        Ok((pair, interp_stamp))
    }

    fn receive(&mut self, seat: usize, pair: Pair<I, G>, what: &str) -> Result<(), String> {
        self.ir[seat].receive(pair.0);
        self.gr[seat].receive(pair.1);
        self.twin(seat, what)
    }

    fn broadcast(&mut self, from: usize, pair: &Pair<I, G>, what: &str) -> Result<(), String> {
        for seat in 0..self.ir.len() {
            if seat != from {
                self.receive(seat, pair.clone(), what)?;
            }
        }
        Ok(())
    }

    fn stable_at(stable: &[(String, usize)], stamp: &Stamp) -> bool {
        stable
            .iter()
            .any(|(origin, seq)| *origin == stamp.origin && *seq >= stamp.seq)
    }

    fn twin(&mut self, seat: usize, what: &str) -> Result<(), String> {
        self.stats.twin_comparisons += 1;
        let interp = (self.interp.read)(&self.ir[seat]);
        let generated = (self.generated.read)(&self.gr[seat]);
        if interp != generated {
            return Err(format!(
                "{what}: replica {} reads differently on the two paths at {}\n  {}: {}\n  {}: {}",
                SEATS[seat],
                first_difference(&interp, &generated, "").unwrap_or_default(),
                self.interp.name,
                serde_json::to_string(&interp).unwrap_or_default(),
                self.generated.name,
                serde_json::to_string(&generated).unwrap_or_default(),
            ));
        }
        let interp_stability = self.ir[seat].stability();
        let gen_stability = self.gr[seat].stability();
        if interp_stability != gen_stability {
            return Err(format!(
                "{what}: replica {} holds different causal-stability state on the two paths\n  \
                 {}: {interp_stability:?}\n  {}: {gen_stability:?}",
                SEATS[seat], self.interp.name, self.generated.name
            ));
        }
        if !self.conflict.is_empty() {
            let stable = self
                .conflict
                .iter()
                .filter(|stamp| Self::stable_at(&interp_stability.stable_version, stamp))
                .count();
            if stable == 0 {
                self.stats.unstable += 1;
            } else if stable == self.conflict.len() {
                self.stats.fully_stable += 1;
            } else {
                self.stats.partially_stable += 1;
            }
        }
        Ok(())
    }

    fn converged(&mut self, what: &str) -> Result<(), String> {
        for seat in 0..self.ir.len() {
            self.twin(seat, what)?;
        }
        let first_interp = (self.interp.read)(&self.ir[0]);
        let first_gen = (self.generated.read)(&self.gr[0]);
        for seat in 1..self.ir.len() {
            self.stats.replica_comparisons += 2;
            for (path, first, held) in [
                (self.interp.name, &first_interp, (self.interp.read)(&self.ir[seat])),
                (self.generated.name, &first_gen, (self.generated.read)(&self.gr[seat])),
            ] {
                if *first != held {
                    return Err(format!(
                        "{what}: replicas a and {} of the {path} path have delivered the same \
                         events and read differently at {}\n  a: {}\n  {}: {}",
                        SEATS[seat],
                        first_difference(first, &held, "").unwrap_or_default(),
                        serde_json::to_string(first).unwrap_or_default(),
                        SEATS[seat],
                        serde_json::to_string(&held).unwrap_or_default(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn all_stable(&self, targets: &[usize]) -> bool {
        let covered = |stable: &[(String, usize)]| {
            targets.iter().enumerate().all(|(seat, target)| {
                *target == 0
                    || stable
                        .iter()
                        .any(|(origin, seq)| origin == SEATS[seat] && seq >= target)
            })
        };
        self.ir
            .iter()
            .all(|replica| covered(&replica.stability().stable_version))
            && self
                .gr
                .iter()
                .all(|replica| covered(&replica.stability().stable_version))
    }

    /// Heartbeats from every seat, delivered everywhere, until every replica
    /// of both paths has stabilized everything sent before the first of them.
    fn stabilize(&mut self, what: &str) -> Result<(), String> {
        let targets = self.sent.clone();
        for round in 0..3 {
            for seat in 0..self.ir.len() {
                let label = format!("{what}, heartbeat round {round} from {}", SEATS[seat]);
                let (pair, _) = self.send(seat, &self.cell.heartbeat, &label)?;
                self.broadcast(seat, &pair, &label)?;
            }
            self.converged(&format!("{what}, after heartbeat round {round}"))?;
            if self.all_stable(&targets) {
                return Ok(());
            }
        }
        Err(format!(
            "{what}: three heartbeat rounds did not stabilize {targets:?} everywhere"
        ))
    }
}

/// One cell under one schedule.
fn run_schedule<E, I, G>(
    cell: &Cell<E>,
    schedule: &Schedule,
    interp: &Arm<'_, E, I>,
    generated: &Arm<'_, E, G>,
) -> Result<Stats, String>
where
    E: Debug,
    I: IsLog,
    G: IsLog,
    I::Op: Clone + Debug + InternalizeOp,
    G::Op: Clone + Debug + InternalizeOp,
{
    let names: Vec<&str> = SEATS[..schedule.members].to_vec();
    let interp_log = LogId::generate();
    let gen_log = LogId::generate();
    let mut run = Run {
        cell,
        interp,
        generated,
        ir: names
            .iter()
            .map(|id| Replica::bootstrap_with_log_id(id.to_string(), &names, interp_log.clone()))
            .collect(),
        gr: names
            .iter()
            .map(|id| Replica::bootstrap_with_log_id(id.to_string(), &names, gen_log.clone()))
            .collect(),
        sent: vec![0; schedule.members],
        conflict: Vec::new(),
        stats: Stats {
            schedules: 1,
            ..Stats::default()
        },
    };

    for (index, edit) in cell.setup.iter().enumerate() {
        let what = format!("setup edit {index}");
        let (pair, _) = run.send(0, edit, &what)?;
        run.broadcast(0, &pair, &what)?;
    }
    for index in 0..schedule.pad {
        let what = format!("pad {index}");
        let (pair, _) = run.send(0, &cell.heartbeat, &what)?;
        run.broadcast(0, &pair, &what)?;
    }
    run.converged("after the setup")?;
    if schedule.stable_prefix {
        run.stabilize("stabilizing the setup")?;
    }

    // The conflict: every list issued in full, nothing delivered.
    let mut outbox: Vec<Vec<Pair<I, G>>> = vec![Vec::new(); schedule.members];
    let mut by_seat: Vec<Vec<Stamp>> = vec![Vec::new(); schedule.members];
    for seat in 0..schedule.members {
        let Some(list) = schedule.seats[seat] else {
            continue;
        };
        for (index, edit) in cell.lists[list].iter().enumerate() {
            let what = format!("list {list} edit {index} at {}", SEATS[seat]);
            let (pair, stamp) = run.send(seat, edit, &what)?;
            outbox[seat].push(pair);
            by_seat[seat].push(stamp);
        }
    }
    run.conflict = by_seat.iter().flatten().cloned().collect();

    // The lists really are concurrent with one another.
    for (seat, stamps) in by_seat.iter().enumerate() {
        for (other, others) in by_seat.iter().enumerate() {
            if seat == other {
                continue;
            }
            for mine in stamps {
                for theirs in others {
                    if theirs.version.get(&mine.origin).copied().unwrap_or(0) >= mine.seq {
                        return Err(format!(
                            "{mine:?} is not concurrent with {theirs:?}; the cell does not build \
                             the conflict it names"
                        ));
                    }
                }
            }
        }
    }
    if cell.tie {
        let firsts: BTreeSet<usize> = by_seat
            .iter()
            .filter_map(|stamps| stamps.first().map(|stamp| stamp.lamport))
            .collect();
        if firsts.len() != 1 {
            return Err(format!(
                "the cell asserts a Lamport tie and the first operations carry {firsts:?}"
            ));
        }
    }

    // Delivery.
    let mut position: Vec<usize> = vec![0; schedule.members];
    let mut taken: Vec<Vec<usize>> = vec![vec![0; schedule.members]; schedule.members];
    if schedule.eager {
        loop {
            let mut moved = false;
            for receiver in 0..schedule.members {
                if position[receiver] == schedule.orders[receiver].len() {
                    continue;
                }
                let sender = schedule.orders[receiver][position[receiver]];
                position[receiver] += 1;
                let pair = outbox[sender][taken[receiver][sender]].clone();
                taken[receiver][sender] += 1;
                let what = format!(
                    "eager delivery {} of {}'s operation to {}",
                    position[receiver],
                    SEATS[sender],
                    SEATS[receiver]
                );
                run.receive(receiver, pair, &what)?;
                let what = format!("{}'s acknowledgement of {}", SEATS[receiver], SEATS[sender]);
                let (ack, _) = run.send(receiver, &cell.heartbeat, &what)?;
                run.broadcast(receiver, &ack, &what)?;
                moved = true;
            }
            if !moved {
                break;
            }
        }
    } else {
        for receiver in 0..schedule.members {
            for &sender in &schedule.orders[receiver] {
                let index = taken[receiver][sender];
                taken[receiver][sender] += 1;
                let pair = outbox[sender][index].clone();
                let what = format!(
                    "lazy delivery of {}'s operation {index} to {}",
                    SEATS[sender], SEATS[receiver]
                );
                run.receive(receiver, pair, &what)?;
            }
        }
    }
    run.converged("once the conflict has been delivered everywhere")?;
    run.stabilize("stabilizing the conflict")?;

    for (pointer, want) in &cell.expect {
        for seat in 0..schedule.members {
            for (path, held) in [
                (interp.name, (interp.read)(&run.ir[seat])),
                (generated.name, (generated.read)(&run.gr[seat])),
            ] {
                let found = held.pointer(pointer).cloned().unwrap_or(Value::Null);
                if found != *want {
                    return Err(format!(
                        "once stable, replica {} of the {path} path holds {found} at `{pointer}` \
                         and the pattern settles on {want}\n  read-out: {}",
                        SEATS[seat],
                        serde_json::to_string(&held).unwrap_or_default()
                    ));
                }
            }
        }
    }
    Ok(run.stats)
}

/// One cell under every schedule. `Err` names the construction, the pattern
/// and the schedule of the first schedule that fails, and stops there.
pub fn run_cell<E, I, G>(
    cell: &Cell<E>,
    interp: &Arm<'_, E, I>,
    generated: &Arm<'_, E, G>,
) -> Result<Stats, String>
where
    E: Debug,
    I: IsLog,
    G: IsLog,
    I::Op: Clone + Debug + InternalizeOp,
    G::Op: Clone + Debug + InternalizeOp,
{
    let mut total = Stats::default();
    for schedule in schedules(cell) {
        let stats = run_schedule(cell, &schedule, interp, generated).map_err(|reason| {
            format!(
                "{} / {}\n  schedule: {schedule}\n  {reason}",
                cell.construction, cell.pattern
            )
        })?;
        total.add(stats);
    }
    if total.partially_stable == 0 || total.fully_stable == 0 {
        return Err(format!(
            "{} / {}: no comparison was made {} across {} schedules",
            cell.construction,
            cell.pattern,
            if total.partially_stable == 0 {
                "with the conflict partly stable"
            } else {
                "with the conflict fully stable"
            },
            total.schedules
        ));
    }
    Ok(total)
}

/// Every cell of one crate: coverage first, then every cell, collecting every
/// failing cell rather than stopping at the first.
pub fn run_matrix<E, I, G>(
    krate: &str,
    sem: &MetamodelSemantics,
    cells: &[Cell<E>],
    interp: &Arm<'_, E, I>,
    generated: &Arm<'_, E, G>,
) -> Result<Stats, String>
where
    E: Debug,
    I: IsLog,
    G: IsLog,
    I::Op: Clone + Debug + InternalizeOp,
    G::Op: Clone + Debug + InternalizeOp,
{
    let assigned = check_coverage(krate, sem, cells)?;
    let mut total = Stats::default();
    let mut failures = Vec::new();
    for cell in cells {
        match run_cell(cell, interp, generated) {
            Ok(stats) => {
                eprintln!(
                    "matrix {krate}: {} / {}: {} schedules, {} twin and {} replica comparisons, \
                     {} events, stability unstable/partial/full {}/{}/{}",
                    cell.construction,
                    cell.pattern,
                    stats.schedules,
                    stats.twin_comparisons,
                    stats.replica_comparisons,
                    stats.events,
                    stats.unstable,
                    stats.partially_stable,
                    stats.fully_stable
                );
                total.add(stats);
            }
            Err(reason) => failures.push(reason),
        }
    }
    eprintln!(
        "matrix {krate}: {} cells of {assigned} assigned, {} schedules, {} twin and {} replica \
         comparisons, {} events per path, {} failing",
        cells.len(),
        total.schedules,
        total.twin_comparisons,
        total.replica_comparisons,
        total.events,
        failures.len()
    );
    if failures.is_empty() {
        Ok(total)
    } else {
        Err(format!(
            "{} of {} cells failed:\n\n{}",
            failures.len(),
            cells.len(),
            failures.join("\n\n")
        ))
    }
}

/// The first place two documents differ, as a JSON-pointer-like path.
pub fn first_difference(left: &Value, right: &Value, at: &str) -> Option<String> {
    match (left, right) {
        (Value::Object(l), Value::Object(r)) => {
            let keys: BTreeSet<&String> = l.keys().chain(r.keys()).collect();
            for key in keys {
                let here = format!("{at}/{key}");
                match (l.get(key), r.get(key)) {
                    (Some(l), Some(r)) => {
                        if let Some(found) = first_difference(l, r, &here) {
                            return Some(found);
                        }
                    }
                    (Some(l), None) => return Some(format!("{here} ({l} against no key)")),
                    (None, Some(r)) => return Some(format!("{here} (no key against {r})")),
                    (None, None) => {}
                }
            }
            None
        }
        (Value::Array(l), Value::Array(r)) if l.len() == r.len() => l
            .iter()
            .zip(r)
            .enumerate()
            .find_map(|(index, (l, r))| first_difference(l, r, &format!("{at}/{index}"))),
        (l, r) if l == r => None,
        (l, r) => Some(format!("{} ({l} against {r})", if at.is_empty() { "/" } else { at })),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use moirai_semantics::{FlagWins, LeafRule, NumKind, SetTie, TieBreak};

    use super::{Construction, Reach, assignments, interleavings, leaf_construction, matrix, row};
    use crate::leaf::LeafLog;

    /// Every one of the twenty-three leaf arms, minted the only way a leaf is
    /// minted, has a row, and the row has patterns.
    #[test]
    fn every_leaf_arm_has_a_row_with_patterns() {
        let mut arms: Vec<LeafLog> = Vec::new();
        for num in super::NUMS {
            for resettable in [true, false] {
                arms.push(LeafLog::for_rule(LeafRule::Counter { num, resettable }));
            }
        }
        arms.push(LeafLog::for_rule(LeafRule::Text));
        for wins in [FlagWins::Enable, FlagWins::Disable] {
            arms.push(LeafLog::for_rule(LeafRule::Flag { wins }));
        }
        for tie in super::TIES {
            arms.push(LeafLog::for_rule(LeafRule::Register { tie }));
        }
        arms.push(LeafLog::for_set(SetTie::AddWins));
        arms.push(LeafLog::for_set(SetTie::RemoveWins));
        arms.push(LeafLog::for_bag());
        let kinds: BTreeSet<&str> = arms.iter().map(LeafLog::kind).collect();
        assert_eq!(arms.len(), 23, "the twenty-three arms");
        let rows: BTreeSet<Construction> = arms.iter().map(leaf_construction).collect();
        assert_eq!(rows.len(), 23, "twenty-three arms, twenty-three rows: {kinds:?}");
        for construction in rows {
            assert!(!row(construction).patterns.is_empty(), "{construction}");
        }
    }

    /// The registry, counted: which rows are driven by which crate, and which
    /// cannot be driven at all, each with a reason.
    #[test]
    fn the_registry_says_where_every_row_is_driven() {
        let rows = matrix();
        let mut reachable_cells = 0;
        let mut unreachable = Vec::new();
        for row in &rows {
            match row.reach {
                Reach::Crate { krate, feature } => {
                    assert!(
                        [super::CLASSDIAGRAM, super::KITCHEN, super::BT, super::JSON]
                            .contains(&krate),
                        "{krate}"
                    );
                    assert!(feature.contains('.'), "{feature}");
                    reachable_cells += row.patterns.len();
                }
                Reach::Unreachable(reason) => {
                    assert!(!reason.is_empty());
                    unreachable.push(row.construction);
                }
            }
        }
        assert_eq!(rows.len(), 34, "thirty-four rows");
        assert_eq!(reachable_cells, 108, "the audit's hundred and eight cells");
        assert_eq!(
            unreachable.len(),
            6 + 4 + 1,
            "six simple counters, four non-mv enum registers, the optional containment: \
             {unreachable:?}"
        );
        assert!(unreachable.contains(&Construction::SimpleCounter(NumKind::U8)));
        assert!(unreachable.contains(&Construction::EnumRegister(TieBreak::Fair)));
        assert!(unreachable.contains(&Construction::OptionalContainment));
    }

    #[test]
    fn schedules_enumerate_assignments_and_orders() {
        assert_eq!(assignments(2, 2).len(), 2);
        assert_eq!(assignments(2, 3).len(), 6);
        assert_eq!(assignments(3, 3).len(), 6);
        assert_eq!(interleavings(&[(0, 1), (1, 1)]).len(), 2);
        assert_eq!(interleavings(&[(0, 2), (1, 2)]).len(), 6);
        assert_eq!(interleavings(&[(0, 2)]), vec![vec![0, 0]]);
    }
}
