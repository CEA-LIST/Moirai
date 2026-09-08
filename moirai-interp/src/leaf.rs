//! The leaves: one scalar vocabulary, one operation vocabulary, and one
//! enum whose every arm is a distinct monomorphisation of an existing
//! Moirai CRDT.
//!
//! # Why an enum and not a trait object
//!
//! `POLog`'s three switches — `DISABLE_R_WHEN_R`, `DISABLE_R_WHEN_NOT_R` and
//! `DISABLE_STABILIZE` (`po_log.rs:65`, `:73`, `:86`) — are associated
//! constants of the operation type. Behind a `dyn` they would become run-time
//! reads on the delivery path; as enum arms over concrete monomorphisations
//! they stay what they are today, constants the optimiser folds away, and the
//! thirteen hand-written `PureCRDT` impls are reused with no wrapper.
//!
//! # The arms, and the counter width decision
//!
//! [`NumKind`] carries six widths, because `to_crdt.rs` maps `EByte` to `u8`
//! (unsigned), `EShort` to `i16`, `EInt` to `i32`, `ELong` to `i64`, `EFloat`
//! to `f32` and `EDouble` to `f64`. This enum monomorphises **all six**, in
//! both the resettable and the non-resettable form, rather than folding them
//! onto `i64` and `f64`.
//!
//! The consequence is the point: step 5's equivalence oracle compares a
//! generated `Counter<i32>` against an interpreted `Counter<i32>`, bit for
//! bit and overflow for overflow, so risk 3 of the implementation plan — "an
//! `i32` counter on the generated path against an `i64` one on the
//! interpreted path, so keep the workload under 2^31 and flag the
//! approximation" — is closed rather than approximated. The cost is eight
//! more enum arms and not one line of semantics: `Counter<V>` merges the same
//! way at every width.
//!
//! # What this module refuses
//!
//! An arm answers [`LeafMismatch`] to an operation of a kind it does not
//! take — `Inc` on a text leaf, `InsertChar` on a flag — and never applies
//! it. That refusal is what [`crate::log::ModelLog::is_enabled`] turns into
//! the local structural check of criterion I-A9.

use std::fmt;
use std::ops::{Add, AddAssign, SubAssign};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_crdt::{
    counter::{resettable_counter::Counter, simple_counter::Counter as SimpleCounter},
    flag::{dw_flag::DWFlag, ew_flag::EWFlag},
    list::eg_walker::List,
    map::uw_map::{UWMap, UWMapLog},
    register::{
        mv_register::MVRegister,
        po_register::PORegister,
        to_register::TORegister,
        unique_register::{FairRegister, LwwRegister, Register},
    },
    set::{aw_set::AWSet, rw_set::RWSet},
};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::Read},
    event::Event,
    state::{event_graph::EventGraph, log::IsLog, po_log::VecLog},
};
use moirai_semantics::{FlagWins, LeafRule, MetamodelSemantics, NumKind, SetTie, TieBreak};
use serde_json::{Map, Value};

#[cfg(feature = "sink")]
use moirai_protocol::state::{
    object_path::ObjectPath,
    sink::{SinkCollector, SinkOwnership},
};

/// Every value a leaf can hold, across every metamodel the table can express.
///
/// `Eq + Hash` because `AWSet<V>`'s stable state is a `HashSet<V>`; `Ord`
/// because `PORegister` and `TORegister` compare values; `Default` because
/// `Register<V, P>` reads `V::default()` out of an empty log. `Null` is that
/// default and is the least value, so a `TORegister`'s first write always
/// beats the empty state and an unwritten register reads as JSON `null`
/// rather than as a plausible-looking zero.
///
/// A float is carried by bit pattern, which is what makes `Eq`, `Hash` and
/// `Ord` total; every read converts back with `f64::from_bits`.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum Scalar {
    /// No value: the default of every register, and JSON `null`.
    #[default]
    Null,
    /// `EBoolean`.
    Bool(bool),
    /// Any integer width, widened; the arm decides what it narrows to.
    Int(i64),
    /// Any float width, by bit pattern of its `f64` widening.
    Float(u64),
    /// `EChar`.
    Char(char),
    /// `EString`, and a reference carried as text (design §8).
    Str(String),
    /// An enum literal: the enum's slot in the table's `enums`, then the
    /// literal's position in its declaration order.
    Enum(u16, u16),
}

impl Scalar {
    /// A float from its value rather than from its bits.
    pub fn float(value: f64) -> Self {
        Scalar::Float(value.to_bits())
    }

    /// A string, spelled the short way.
    pub fn text(value: impl Into<String>) -> Self {
        Scalar::Str(value.into())
    }

    /// The canonical JSON form: an enum literal resolves to its name when a
    /// table is at hand, and to its position when one is not.
    pub fn to_json(&self, sem: Option<&MetamodelSemantics>) -> Value {
        match self {
            Scalar::Null => Value::Null,
            Scalar::Bool(value) => Value::Bool(*value),
            Scalar::Int(value) => Value::from(*value),
            Scalar::Float(bits) => number(f64::from_bits(*bits)),
            Scalar::Char(value) => Value::String(value.to_string()),
            Scalar::Str(value) => Value::String(value.clone()),
            Scalar::Enum(class, literal) => {
                let name = sem
                    .and_then(|sem| sem.enums.get(*class as usize))
                    .and_then(|entry| entry.literals.get(*literal as usize));
                match name {
                    Some(name) => Value::String(name.to_string()),
                    None => Value::from(*literal),
                }
            }
        }
    }

    /// The canonical form of this value used as an *object key*: the JSON
    /// string a keyed collection carries the entry under.
    ///
    /// A JSON object's keys are strings and a `uw-map` key is whatever the
    /// key attribute's type is, so a non-string key is rendered the way its
    /// JSON value renders and then unquoted. `Null` is the empty string,
    /// which no real key attribute produces: `Scalar::Null` is a register's
    /// default and a map key is written by the operation that made the entry.
    pub fn to_key(&self, sem: Option<&MetamodelSemantics>) -> String {
        match self.to_json(sem) {
            Value::String(text) => text,
            Value::Null => String::new(),
            other => other.to_string(),
        }
    }

    /// The word a refusal sentence uses for this value's kind.
    pub const fn kind(&self) -> &'static str {
        match self {
            Scalar::Null => "null",
            Scalar::Bool(_) => "boolean",
            Scalar::Int(_) => "integer",
            Scalar::Float(_) => "float",
            Scalar::Char(_) => "char",
            Scalar::Str(_) => "string",
            Scalar::Enum(..) => "enum literal",
        }
    }
}

/// A JSON number, or `null` for a float JSON cannot spell.
fn number(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// One write to one leaf, in a vocabulary shared by every arm.
///
/// The arm decides which of these it takes; the rest are a [`LeafMismatch`].
/// One vocabulary rather than one per family is what lets
/// [`crate::op::InstanceOp`] stay a single tree that any table can route.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LeafOp {
    /// Text: one character in.
    InsertChar {
        /// Position among the visible characters.
        pos: usize,
        /// The character.
        ch: char,
    },
    /// Text: one character out.
    DeleteChar {
        /// Position among the visible characters.
        pos: usize,
    },
    /// Text: a run out, which is also what a causal reset injects.
    DeleteRange {
        /// First position.
        start: usize,
        /// How many.
        len: usize,
    },
    /// Counter up.
    Inc(Scalar),
    /// Counter down.
    Dec(Scalar),
    /// Counter back to zero; only a resettable counter takes it.
    Reset,
    /// Flag on.
    Enable,
    /// Flag off.
    Disable,
    /// Register write.
    Write(Scalar),
    /// Set or bag: one value in.
    Add(Scalar),
    /// Set or bag: one value out.
    Remove(Scalar),
    /// Register, set or bag: everything out.
    Clear,
}

impl LeafOp {
    /// The word a refusal sentence uses for this operation.
    pub const fn kind(&self) -> &'static str {
        match self {
            LeafOp::InsertChar { .. } => "InsertChar",
            LeafOp::DeleteChar { .. } => "DeleteChar",
            LeafOp::DeleteRange { .. } => "DeleteRange",
            LeafOp::Inc(_) => "Inc",
            LeafOp::Dec(_) => "Dec",
            LeafOp::Reset => "Reset",
            LeafOp::Enable => "Enable",
            LeafOp::Disable => "Disable",
            LeafOp::Write(_) => "Write",
            LeafOp::Add(_) => "Add",
            LeafOp::Remove(_) => "Remove",
            LeafOp::Clear => "Clear",
        }
    }
}

/// An operation offered to a leaf that does not take it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeafMismatch {
    /// The operation's own word.
    pub op: &'static str,
    /// The leaf's own word.
    pub leaf: &'static str,
}

impl fmt::Display for LeafMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not an operation a `{}` leaf takes",
            self.op, self.leaf
        )
    }
}

impl std::error::Error for LeafMismatch {}

fn mismatch<T>(op: &LeafOp, leaf: &'static str) -> Result<T, LeafMismatch> {
    Err(LeafMismatch {
        op: op.kind(),
        leaf,
    })
}

/// The widths a counter arm can be built at.
///
/// Exactly the six `to_rust_type` returns for a numeric Ecore builtin, and
/// the reason there are twelve counter arms rather than four.
pub trait CounterWidth:
    Add<Output = Self> + AddAssign + SubAssign + Default + Copy + fmt::Debug + PartialEq
{
    /// The word a refusal sentence uses.
    const WIDTH: &'static str;
    /// Narrow a scalar to this width, or refuse.
    fn from_scalar(value: &Scalar) -> Option<Self>;
    /// Widen back for the read-out.
    fn to_json(self) -> Value;
}

macro_rules! integer_width {
    ($ty:ty, $name:literal) => {
        impl CounterWidth for $ty {
            const WIDTH: &'static str = $name;

            fn from_scalar(value: &Scalar) -> Option<Self> {
                match value {
                    Scalar::Int(value) => Some(*value as $ty),
                    _ => None,
                }
            }

            fn to_json(self) -> Value {
                Value::from(self)
            }
        }
    };
}

integer_width!(u8, "counter<u8>");
integer_width!(i16, "counter<i16>");
integer_width!(i32, "counter<i32>");
integer_width!(i64, "counter<i64>");

macro_rules! float_width {
    ($ty:ty, $name:literal) => {
        impl CounterWidth for $ty {
            const WIDTH: &'static str = $name;

            fn from_scalar(value: &Scalar) -> Option<Self> {
                match value {
                    Scalar::Float(bits) => Some(f64::from_bits(*bits) as $ty),
                    Scalar::Int(value) => Some(*value as $ty),
                    _ => None,
                }
            }

            fn to_json(self) -> Value {
                number(self as f64)
            }
        }
    };
}

float_width!(f32, "counter<f32>");
float_width!(f64, "counter<f64>");

/// A bag over scalars, composed exactly as `AWBagLog` composes it.
///
/// `moirai-crdt`'s `AWBagLog` is `UWMapLog<V, VecLog<Counter<usize>>>` with a
/// three-line `effect` and no serde derive; the table has to reach a joiner
/// inside a serialized log (decision D2), so the same composition is spelled
/// out here over the same library types rather than `AWBagLog` being changed,
/// which is a crate this phase does not touch.
type BagLog = UWMapLog<Scalar, VecLog<Counter<usize>>>;

/// One leaf, at one of the constructions the generator can compile.
///
/// Twenty-three arms: one text, twelve counters (six widths, resettable and
/// not), two flags, five registers, two sets and one bag.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LeafLog {
    /// `EventGraph<List<char>>`: every string, `iD="true"` included.
    Text(EventGraph<List<char>>),
    /// `EByte`, resettable.
    CounterU8(VecLog<Counter<u8>>),
    /// `EShort`, resettable.
    CounterI16(VecLog<Counter<i16>>),
    /// `EInt`, resettable.
    CounterI32(VecLog<Counter<i32>>),
    /// `ELong`, resettable.
    CounterI64(VecLog<Counter<i64>>),
    /// `EFloat`, resettable.
    CounterF32(VecLog<Counter<f32>>),
    /// `EDouble`, resettable.
    CounterF64(VecLog<Counter<f64>>),
    /// `EByte`, not resettable.
    SimpleCounterU8(VecLog<SimpleCounter<u8>>),
    /// `EShort`, not resettable.
    SimpleCounterI16(VecLog<SimpleCounter<i16>>),
    /// `EInt`, not resettable.
    SimpleCounterI32(VecLog<SimpleCounter<i32>>),
    /// `ELong`, not resettable.
    SimpleCounterI64(VecLog<SimpleCounter<i64>>),
    /// `EFloat`, not resettable.
    SimpleCounterF32(VecLog<SimpleCounter<f32>>),
    /// `EDouble`, not resettable.
    SimpleCounterF64(VecLog<SimpleCounter<f64>>),
    /// Enable-wins, the house default for `EBoolean`.
    FlagEw(VecLog<EWFlag>),
    /// Disable-wins, reachable through the `dw-flag` annotation.
    FlagDw(VecLog<DWFlag>),
    /// Multi-value register, the house default for `EChar` and for enums.
    RegisterMv(VecLog<MVRegister<Scalar>>),
    /// Last-writer-wins register.
    RegisterLww(VecLog<LwwRegister<Scalar>>),
    /// Fair register.
    RegisterFair(VecLog<FairRegister<Scalar>>),
    /// Partial-order register.
    RegisterPo(VecLog<PORegister<Scalar>>),
    /// Total-order register.
    RegisterTo(VecLog<TORegister<Scalar>>),
    /// Add-wins set.
    SetAw(VecLog<AWSet<Scalar>>),
    /// Remove-wins set.
    SetRw(VecLog<RWSet<Scalar>>),
    /// Add-wins bag.
    Bag(BagLog),
}

/// Run `$body` against whichever inner log the arm holds.
///
/// Every arm is an `IsLog`, so the three traversals that only delegate —
/// `stabilize`, `redundant_by_parent`, `is_default` — are one match written
/// once rather than three matches of twenty-three arms each.
macro_rules! dispatch {
    ($this:expr, $log:ident => $body:expr) => {
        match $this {
            LeafLog::Text($log) => $body,
            LeafLog::CounterU8($log) => $body,
            LeafLog::CounterI16($log) => $body,
            LeafLog::CounterI32($log) => $body,
            LeafLog::CounterI64($log) => $body,
            LeafLog::CounterF32($log) => $body,
            LeafLog::CounterF64($log) => $body,
            LeafLog::SimpleCounterU8($log) => $body,
            LeafLog::SimpleCounterI16($log) => $body,
            LeafLog::SimpleCounterI32($log) => $body,
            LeafLog::SimpleCounterI64($log) => $body,
            LeafLog::SimpleCounterF32($log) => $body,
            LeafLog::SimpleCounterF64($log) => $body,
            LeafLog::FlagEw($log) => $body,
            LeafLog::FlagDw($log) => $body,
            LeafLog::RegisterMv($log) => $body,
            LeafLog::RegisterLww($log) => $body,
            LeafLog::RegisterFair($log) => $body,
            LeafLog::RegisterPo($log) => $body,
            LeafLog::RegisterTo($log) => $body,
            LeafLog::SetAw($log) => $body,
            LeafLog::SetRw($log) => $body,
            LeafLog::Bag($log) => $body,
        }
    };
}

impl LeafLog {
    /// The scalar leaf a rule names, minted empty.
    ///
    /// The only way a leaf comes into being: nothing here goes through
    /// `Default`, because the rule is what decides the arm and a default
    /// would decide it by accident.
    pub fn for_rule(rule: LeafRule) -> Self {
        match rule {
            LeafRule::Text => LeafLog::Text(EventGraph::default()),
            LeafRule::Counter {
                num,
                resettable: true,
            } => match num {
                NumKind::U8 => LeafLog::CounterU8(VecLog::default()),
                NumKind::I16 => LeafLog::CounterI16(VecLog::default()),
                NumKind::I32 => LeafLog::CounterI32(VecLog::default()),
                NumKind::I64 => LeafLog::CounterI64(VecLog::default()),
                NumKind::F32 => LeafLog::CounterF32(VecLog::default()),
                NumKind::F64 => LeafLog::CounterF64(VecLog::default()),
            },
            LeafRule::Counter {
                num,
                resettable: false,
            } => match num {
                NumKind::U8 => LeafLog::SimpleCounterU8(VecLog::default()),
                NumKind::I16 => LeafLog::SimpleCounterI16(VecLog::default()),
                NumKind::I32 => LeafLog::SimpleCounterI32(VecLog::default()),
                NumKind::I64 => LeafLog::SimpleCounterI64(VecLog::default()),
                NumKind::F32 => LeafLog::SimpleCounterF32(VecLog::default()),
                NumKind::F64 => LeafLog::SimpleCounterF64(VecLog::default()),
            },
            LeafRule::Flag {
                wins: FlagWins::Enable,
            } => LeafLog::FlagEw(VecLog::default()),
            LeafRule::Flag {
                wins: FlagWins::Disable,
            } => LeafLog::FlagDw(VecLog::default()),
            LeafRule::Register { tie } | LeafRule::Enum { tie, .. } => match tie {
                TieBreak::MultiValue => LeafLog::RegisterMv(VecLog::default()),
                TieBreak::LastWriterWins => LeafLog::RegisterLww(VecLog::default()),
                TieBreak::Fair => LeafLog::RegisterFair(VecLog::default()),
                TieBreak::PartialOrder => LeafLog::RegisterPo(VecLog::default()),
                TieBreak::TotalOrder => LeafLog::RegisterTo(VecLog::default()),
            },
        }
    }

    /// The set a `Shape::Set` names, minted empty.
    ///
    /// A set holds scalars, not leaf CRDTs: `attribute.rs` compiles a unique
    /// unordered attribute as `VecLog<AWSet<V>>` over the *value* type
    /// whatever the leaf rule says, so the leaf rule types the scalar and
    /// chooses nothing else.
    pub fn for_set(tie: SetTie) -> Self {
        match tie {
            SetTie::AddWins => LeafLog::SetAw(VecLog::default()),
            SetTie::RemoveWins => LeafLog::SetRw(VecLog::default()),
        }
    }

    /// The bag a `Shape::Bag` names, minted empty.
    pub fn for_bag() -> Self {
        LeafLog::Bag(BagLog::default())
    }

    /// The word a refusal sentence uses for this leaf.
    pub const fn kind(&self) -> &'static str {
        match self {
            LeafLog::Text(_) => "text",
            LeafLog::CounterU8(_) | LeafLog::SimpleCounterU8(_) => u8::WIDTH,
            LeafLog::CounterI16(_) | LeafLog::SimpleCounterI16(_) => i16::WIDTH,
            LeafLog::CounterI32(_) | LeafLog::SimpleCounterI32(_) => i32::WIDTH,
            LeafLog::CounterI64(_) | LeafLog::SimpleCounterI64(_) => i64::WIDTH,
            LeafLog::CounterF32(_) | LeafLog::SimpleCounterF32(_) => f32::WIDTH,
            LeafLog::CounterF64(_) | LeafLog::SimpleCounterF64(_) => f64::WIDTH,
            LeafLog::FlagEw(_) => "enable-wins flag",
            LeafLog::FlagDw(_) => "disable-wins flag",
            LeafLog::RegisterMv(_) => "multi-value register",
            LeafLog::RegisterLww(_) => "last-writer-wins register",
            LeafLog::RegisterFair(_) => "fair register",
            LeafLog::RegisterPo(_) => "partial-order register",
            LeafLog::RegisterTo(_) => "total-order register",
            LeafLog::SetAw(_) => "add-wins set",
            LeafLog::SetRw(_) => "remove-wins set",
            LeafLog::Bag(_) => "bag",
        }
    }

    /// Whether this leaf would take the operation from a local writer.
    ///
    /// Two refusals in one: the kind check, which is the table's business,
    /// and the inner log's own `is_enabled`, which is the state's — a text
    /// insert past the end of the string is refused here exactly as
    /// `EventGraph<List<char>>` refuses it on its own.
    pub fn is_enabled(&self, op: &LeafOp) -> Result<(), LeafMismatch> {
        self.probe(op, true)
    }

    /// Whether the operation is even of this leaf's kind.
    ///
    /// The half of [`LeafLog::is_enabled`] that a remote operation is held
    /// to: it has already happened somewhere, so the only question left is
    /// whether this arm can express it at all.
    pub fn accepts(&self, op: &LeafOp) -> Result<(), LeafMismatch> {
        self.probe(op, false)
    }

    fn probe(&self, op: &LeafOp, strict: bool) -> Result<(), LeafMismatch> {
        match self {
            LeafLog::Text(log) => {
                let inner = text_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::CounterU8(log) => counter_enabled(log, op, self.kind(), strict),
            LeafLog::CounterI16(log) => counter_enabled(log, op, self.kind(), strict),
            LeafLog::CounterI32(log) => counter_enabled(log, op, self.kind(), strict),
            LeafLog::CounterI64(log) => counter_enabled(log, op, self.kind(), strict),
            LeafLog::CounterF32(log) => counter_enabled(log, op, self.kind(), strict),
            LeafLog::CounterF64(log) => counter_enabled(log, op, self.kind(), strict),
            LeafLog::SimpleCounterU8(log) => simple_counter_enabled(log, op, self.kind(), strict),
            LeafLog::SimpleCounterI16(log) => simple_counter_enabled(log, op, self.kind(), strict),
            LeafLog::SimpleCounterI32(log) => simple_counter_enabled(log, op, self.kind(), strict),
            LeafLog::SimpleCounterI64(log) => simple_counter_enabled(log, op, self.kind(), strict),
            LeafLog::SimpleCounterF32(log) => simple_counter_enabled(log, op, self.kind(), strict),
            LeafLog::SimpleCounterF64(log) => simple_counter_enabled(log, op, self.kind(), strict),
            LeafLog::FlagEw(log) => {
                let inner = ew_flag_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::FlagDw(log) => {
                let inner = dw_flag_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::RegisterMv(log) => {
                let inner = mv_register_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::RegisterLww(log) => {
                let inner = unique_register_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::RegisterFair(log) => {
                let inner = unique_register_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::RegisterPo(log) => {
                let inner = po_register_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::RegisterTo(log) => {
                let inner = to_register_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::SetAw(log) => {
                let inner = aw_set_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::SetRw(log) => {
                let inner = rw_set_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
            LeafLog::Bag(log) => {
                let inner = bag_op(op)?;
                enabled(log, &inner, op, self.kind(), strict)
            }
        }
    }

    /// Apply one operation, translating it into the arm's own vocabulary and
    /// delegating through [`Event::unfold`].
    ///
    /// The sink is not threaded through: every arm but the bag ignores the
    /// path and the collector outright, and the bag's own map entries are
    /// noise below the feature the node above already reported — which is
    /// exactly the judgement `AWBagLog` makes when it hands its map a
    /// throwaway collector.
    pub fn effect(&mut self, event: Event<LeafOp>) -> Result<(), LeafMismatch> {
        match self {
            LeafLog::Text(log) => {
                let inner = text_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::CounterU8(log) => counter_effect(log, event)?,
            LeafLog::CounterI16(log) => counter_effect(log, event)?,
            LeafLog::CounterI32(log) => counter_effect(log, event)?,
            LeafLog::CounterI64(log) => counter_effect(log, event)?,
            LeafLog::CounterF32(log) => counter_effect(log, event)?,
            LeafLog::CounterF64(log) => counter_effect(log, event)?,
            LeafLog::SimpleCounterU8(log) => simple_counter_effect(log, event)?,
            LeafLog::SimpleCounterI16(log) => simple_counter_effect(log, event)?,
            LeafLog::SimpleCounterI32(log) => simple_counter_effect(log, event)?,
            LeafLog::SimpleCounterI64(log) => simple_counter_effect(log, event)?,
            LeafLog::SimpleCounterF32(log) => simple_counter_effect(log, event)?,
            LeafLog::SimpleCounterF64(log) => simple_counter_effect(log, event)?,
            LeafLog::FlagEw(log) => {
                let inner = ew_flag_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::FlagDw(log) => {
                let inner = dw_flag_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::RegisterMv(log) => {
                let inner = mv_register_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::RegisterLww(log) => {
                let inner = unique_register_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::RegisterFair(log) => {
                let inner = unique_register_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::RegisterPo(log) => {
                let inner = po_register_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::RegisterTo(log) => {
                let inner = to_register_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::SetAw(log) => {
                let inner = aw_set_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::SetRw(log) => {
                let inner = rw_set_op(event.op())?;
                apply(log, event, inner);
            }
            LeafLog::Bag(log) => {
                let inner = bag_op(event.op())?;
                apply(log, event, inner);
            }
        }
        Ok(())
    }

    /// Hand a stable version to the arm.
    pub fn stabilize(&mut self, version: &Version) {
        dispatch!(self, log => log.stabilize(version))
    }

    /// A parent removed this leaf: update-wins, so what is causally below the
    /// removal goes and what is concurrent with it stays.
    pub fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        dispatch!(self, log => log.redundant_by_parent(version, conservative))
    }

    /// Whether the arm holds nothing.
    pub fn is_default(&self) -> bool {
        dispatch!(self, log => log.is_default())
    }

    /// The canonical JSON form of this leaf's value.
    pub fn read_json(&self, sem: Option<&MetamodelSemantics>) -> Value {
        match self {
            LeafLog::Text(log) => Value::String(log.execute_query(Read::<String>::new())),
            LeafLog::CounterU8(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::CounterI16(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::CounterI32(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::CounterI64(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::CounterF32(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::CounterF64(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::SimpleCounterU8(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::SimpleCounterI16(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::SimpleCounterI32(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::SimpleCounterI64(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::SimpleCounterF32(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::SimpleCounterF64(log) => log.execute_query(Read::new()).to_json(),
            LeafLog::FlagEw(log) => Value::Bool(log.execute_query(Read::new())),
            LeafLog::FlagDw(log) => Value::Bool(log.execute_query(Read::new())),
            LeafLog::RegisterMv(log) => many_valued(log.execute_query(Read::new()), sem),
            LeafLog::RegisterLww(log) => log.execute_query(Read::<Scalar>::new()).to_json(sem),
            LeafLog::RegisterFair(log) => log.execute_query(Read::<Scalar>::new()).to_json(sem),
            LeafLog::RegisterPo(log) => many_valued(log.execute_query(Read::new()), sem),
            LeafLog::RegisterTo(log) => log.execute_query(Read::<Scalar>::new()).to_json(sem),
            LeafLog::SetAw(log) => sorted_array(log.execute_query(Read::new()), sem),
            LeafLog::SetRw(log) => sorted_array(log.execute_query(Read::new()), sem),
            LeafLog::Bag(log) => {
                let counts = log.execute_query(Read::new());
                let mut values: Vec<Scalar> = Vec::new();
                for (value, count) in counts {
                    for _ in 0..count {
                        values.push(value.clone());
                    }
                }
                values.sort();
                Value::Array(values.iter().map(|value| value.to_json(sem)).collect())
            }
        }
    }

    /// How many operations this leaf still holds unstably.
    ///
    /// The measurement behind `ip11`: after `stabilize`, this number must
    /// fall for every arm whose generated twin's would.
    #[cfg(feature = "test_utils")]
    pub fn polog_len(&self) -> usize {
        use moirai_protocol::state::{log::IsLogTest, unstable_state::IsUnstableState};

        match self {
            // An `EventGraph` is its own unstable state and its `stabilize`
            // is a no-op by construction (`List`'s `DISABLE_STABILIZE`), so
            // this number does not fall for text — on either path, which is
            // why `ip11` excludes text by naming the exclusion rather than by
            // forgetting it.
            LeafLog::Text(log) => IsUnstableState::len(log),
            LeafLog::CounterU8(log) => log.unstable().len(),
            LeafLog::CounterI16(log) => log.unstable().len(),
            LeafLog::CounterI32(log) => log.unstable().len(),
            LeafLog::CounterI64(log) => log.unstable().len(),
            LeafLog::CounterF32(log) => log.unstable().len(),
            LeafLog::CounterF64(log) => log.unstable().len(),
            LeafLog::SimpleCounterU8(log) => log.unstable().len(),
            LeafLog::SimpleCounterI16(log) => log.unstable().len(),
            LeafLog::SimpleCounterI32(log) => log.unstable().len(),
            LeafLog::SimpleCounterI64(log) => log.unstable().len(),
            LeafLog::SimpleCounterF32(log) => log.unstable().len(),
            LeafLog::SimpleCounterF64(log) => log.unstable().len(),
            LeafLog::FlagEw(log) => log.unstable().len(),
            LeafLog::FlagDw(log) => log.unstable().len(),
            LeafLog::RegisterMv(log) => log.unstable().len(),
            LeafLog::RegisterLww(log) => log.unstable().len(),
            LeafLog::RegisterFair(log) => log.unstable().len(),
            LeafLog::RegisterPo(log) => log.unstable().len(),
            LeafLog::RegisterTo(log) => log.unstable().len(),
            LeafLog::SetAw(log) => log.unstable().len(),
            LeafLog::SetRw(log) => log.unstable().len(),
            LeafLog::Bag(log) => log
                .children()
                .values()
                .map(|child| child.unstable().len())
                .sum(),
        }
    }
}

/// A set of scalars read out of a register that can hold several.
fn many_valued(
    values: impl IntoIterator<Item = Scalar>,
    sem: Option<&MetamodelSemantics>,
) -> Value {
    let mut values: Vec<Scalar> = values.into_iter().collect();
    values.sort();
    match values.len() {
        0 => Value::Null,
        1 => values[0].to_json(sem),
        _ => {
            let mut object = Map::new();
            object.insert(
                "__conflict".to_string(),
                Value::Array(values.iter().map(|value| value.to_json(sem)).collect()),
            );
            Value::Object(object)
        }
    }
}

/// A set of scalars read out of a set.
fn sorted_array(
    values: impl IntoIterator<Item = Scalar>,
    sem: Option<&MetamodelSemantics>,
) -> Value {
    let mut values: Vec<Scalar> = values.into_iter().collect();
    values.sort();
    Value::Array(values.iter().map(|value| value.to_json(sem)).collect())
}

/// `is_enabled` on one arm's own log, with the arm's word for the refusal;
/// the kind check alone when the operation is a peer's.
fn enabled<L: IsLog>(
    log: &L,
    inner: &L::Op,
    op: &LeafOp,
    leaf: &'static str,
    strict: bool,
) -> Result<(), LeafMismatch> {
    if !strict || log.is_enabled(inner) {
        Ok(())
    } else {
        mismatch(op, leaf)
    }
}

/// Deliver one translated operation.
fn apply<L: IsLog>(log: &mut L, event: Event<LeafOp>, inner: L::Op) {
    let event = event.unfold(inner);
    log.effect(
        event,
        #[cfg(feature = "sink")]
        ObjectPath::new("leaf"),
        #[cfg(feature = "sink")]
        &mut SinkCollector::new(),
        #[cfg(feature = "sink")]
        SinkOwnership::Delegated,
    );
}

fn text_op(op: &LeafOp) -> Result<List<char>, LeafMismatch> {
    match op {
        LeafOp::InsertChar { pos, ch } => Ok(List::Insert {
            content: *ch,
            pos: *pos,
        }),
        LeafOp::DeleteChar { pos } => Ok(List::Delete { pos: *pos }),
        LeafOp::DeleteRange { start, len } => Ok(List::DeleteRange {
            start: *start,
            len: *len,
        }),
        other => mismatch(other, "text"),
    }
}

fn counter_op<V: CounterWidth>(op: &LeafOp) -> Result<Counter<V>, LeafMismatch> {
    match op {
        LeafOp::Inc(value) => V::from_scalar(value)
            .map(Counter::Inc)
            .ok_or_else(|| LeafMismatch {
                op: value.kind(),
                leaf: V::WIDTH,
            }),
        LeafOp::Dec(value) => V::from_scalar(value)
            .map(Counter::Dec)
            .ok_or_else(|| LeafMismatch {
                op: value.kind(),
                leaf: V::WIDTH,
            }),
        LeafOp::Reset => Ok(Counter::Reset),
        other => mismatch(other, V::WIDTH),
    }
}

fn simple_counter_op<V: CounterWidth>(op: &LeafOp) -> Result<SimpleCounter<V>, LeafMismatch> {
    match op {
        LeafOp::Inc(value) => V::from_scalar(value)
            .map(SimpleCounter::Inc)
            .ok_or_else(|| LeafMismatch {
                op: value.kind(),
                leaf: V::WIDTH,
            }),
        LeafOp::Dec(value) => V::from_scalar(value)
            .map(SimpleCounter::Dec)
            .ok_or_else(|| LeafMismatch {
                op: value.kind(),
                leaf: V::WIDTH,
            }),
        other => mismatch(other, V::WIDTH),
    }
}

fn counter_enabled<V: CounterWidth>(
    log: &VecLog<Counter<V>>,
    op: &LeafOp,
    leaf: &'static str,
    strict: bool,
) -> Result<(), LeafMismatch> {
    let inner = counter_op::<V>(op)?;
    enabled(log, &inner, op, leaf, strict)
}

fn simple_counter_enabled<V: CounterWidth>(
    log: &VecLog<SimpleCounter<V>>,
    op: &LeafOp,
    leaf: &'static str,
    strict: bool,
) -> Result<(), LeafMismatch> {
    let inner = simple_counter_op::<V>(op)?;
    enabled(log, &inner, op, leaf, strict)
}

fn counter_effect<V: CounterWidth>(
    log: &mut VecLog<Counter<V>>,
    event: Event<LeafOp>,
) -> Result<(), LeafMismatch> {
    let inner = counter_op::<V>(event.op())?;
    apply(log, event, inner);
    Ok(())
}

fn simple_counter_effect<V: CounterWidth>(
    log: &mut VecLog<SimpleCounter<V>>,
    event: Event<LeafOp>,
) -> Result<(), LeafMismatch> {
    let inner = simple_counter_op::<V>(event.op())?;
    apply(log, event, inner);
    Ok(())
}

fn ew_flag_op(op: &LeafOp) -> Result<EWFlag, LeafMismatch> {
    match op {
        LeafOp::Enable => Ok(EWFlag::Enable),
        LeafOp::Disable => Ok(EWFlag::Disable),
        LeafOp::Clear => Ok(EWFlag::Clear),
        other => mismatch(other, "enable-wins flag"),
    }
}

fn dw_flag_op(op: &LeafOp) -> Result<DWFlag, LeafMismatch> {
    match op {
        LeafOp::Enable => Ok(DWFlag::Enable),
        LeafOp::Disable => Ok(DWFlag::Disable),
        LeafOp::Clear => Ok(DWFlag::Clear),
        other => mismatch(other, "disable-wins flag"),
    }
}

fn mv_register_op(op: &LeafOp) -> Result<MVRegister<Scalar>, LeafMismatch> {
    match op {
        LeafOp::Write(value) => Ok(MVRegister::Write(value.clone())),
        LeafOp::Clear => Ok(MVRegister::Clear),
        other => mismatch(other, "multi-value register"),
    }
}

fn unique_register_op<P>(op: &LeafOp) -> Result<Register<Scalar, P>, LeafMismatch> {
    match op {
        LeafOp::Write(value) => Ok(Register::Write(value.clone())),
        other => mismatch(other, "register"),
    }
}

fn po_register_op(op: &LeafOp) -> Result<PORegister<Scalar>, LeafMismatch> {
    match op {
        LeafOp::Write(value) => Ok(PORegister::Write(value.clone())),
        LeafOp::Clear => Ok(PORegister::Clear),
        other => mismatch(other, "partial-order register"),
    }
}

fn to_register_op(op: &LeafOp) -> Result<TORegister<Scalar>, LeafMismatch> {
    match op {
        LeafOp::Write(value) => Ok(TORegister::Write(value.clone())),
        LeafOp::Clear => Ok(TORegister::Clear),
        other => mismatch(other, "total-order register"),
    }
}

fn aw_set_op(op: &LeafOp) -> Result<AWSet<Scalar>, LeafMismatch> {
    match op {
        LeafOp::Add(value) => Ok(AWSet::Add(value.clone())),
        LeafOp::Remove(value) => Ok(AWSet::Remove(value.clone())),
        LeafOp::Clear => Ok(AWSet::Clear),
        other => mismatch(other, "add-wins set"),
    }
}

fn rw_set_op(op: &LeafOp) -> Result<RWSet<Scalar>, LeafMismatch> {
    match op {
        LeafOp::Add(value) => Ok(RWSet::Add(value.clone())),
        LeafOp::Remove(value) => Ok(RWSet::Remove(value.clone())),
        LeafOp::Clear => Ok(RWSet::Clear),
        other => mismatch(other, "remove-wins set"),
    }
}

/// A bag operation as `AWBagLog` spells it: an add is one increment of the
/// value's own counter, a remove one decrement, a clear the map's clear.
fn bag_op(op: &LeafOp) -> Result<UWMap<Scalar, Counter<usize>>, LeafMismatch> {
    match op {
        LeafOp::Add(value) => Ok(UWMap::Update(value.clone(), Counter::Inc(1))),
        LeafOp::Remove(value) => Ok(UWMap::Update(value.clone(), Counter::Dec(1))),
        LeafOp::Clear => Ok(UWMap::Clear),
        other => mismatch(other, "bag"),
    }
}

impl moirai_protocol::utils::intern_str::InternalizeOp for LeafOp {
    /// Identity: a leaf operation carries scalars and positions, never an
    /// [`moirai_protocol::event::id::EventId`], so there is nothing here for
    /// an interner to re-index. The impl lives beside the type all the same,
    /// so that [`crate::op::ModelOp`]'s recursion has one exhaustive place to
    /// bottom out and cannot silently skip a leaf.
    fn internalize(self, _interner: &moirai_protocol::utils::intern_str::Interner) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    //! `ip6`: each of the twenty-three `LeafLog` arms, driven through the
    //! sequence its own CRDT's tests use, reads out the same value.
    //!
    //! The arms are driven through real replicas rather than through
    //! hand-built events, because half of what is being checked is that the
    //! translation preserves the *event*: its id, its lamport and its
    //! version, which is what every concurrency outcome below is decided by.
    //! [`LeafCell`] is the smallest `IsLog` that can hold one arm, and it
    //! exists only here: `LeafLog` itself deliberately has no `Default`, the
    //! rule being the only thing allowed to choose an arm.

    use std::cell::Cell;

    use moirai_crdt::utils::membership::twins_log;
    use moirai_protocol::{
        clock::version_vector::Version,
        crdt::{eval::EvalNested, query::Read},
        event::Event,
        replica::IsReplica,
        state::log::IsLog,
    };
    use moirai_semantics::{ClassSlot, FlagWins, LeafRule, NumKind, SetTie, TieBreak};
    use serde_json::{Value, json};

    use super::{LeafLog, LeafOp, Scalar};

    /// What [`LeafCell::default`] mints. Set by the test before it asks for
    /// its twins; thread-local, and every `#[test]` is its own thread.
    #[derive(Clone, Copy)]
    enum Recipe {
        Scalar(LeafRule),
        Set(SetTie),
        Bag,
    }

    thread_local! {
        static RECIPE: Cell<Recipe> = const {
            Cell::new(Recipe::Scalar(LeafRule::Text))
        };
    }

    fn recipe(recipe: Recipe) {
        RECIPE.with(|slot| slot.set(recipe));
    }

    /// One leaf, as a log a `Replica` can host.
    #[derive(Clone, Debug)]
    struct LeafCell(LeafLog);

    impl Default for LeafCell {
        fn default() -> Self {
            LeafCell(RECIPE.with(|slot| match slot.get() {
                Recipe::Scalar(rule) => LeafLog::for_rule(rule),
                Recipe::Set(tie) => LeafLog::for_set(tie),
                Recipe::Bag => LeafLog::for_bag(),
            }))
        }
    }

    impl IsLog for LeafCell {
        type Value = Value;
        type Op = LeafOp;

        fn is_enabled(&self, op: &Self::Op) -> bool {
            self.0.is_enabled(op).is_ok()
        }

        fn effect(
            &mut self,
            event: Event<Self::Op>,
            #[cfg(feature = "sink")] _path: moirai_protocol::state::object_path::ObjectPath,
            #[cfg(feature = "sink")] _sink: &mut moirai_protocol::state::sink::SinkCollector,
            #[cfg(feature = "sink")] _ownership: moirai_protocol::state::sink::SinkOwnership,
        ) {
            self.0.effect(event).expect("the arm takes the operation");
        }

        fn stabilize(&mut self, version: &Version) {
            self.0.stabilize(version);
        }

        fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
            self.0.redundant_by_parent(version, conservative);
        }

        fn is_default(&self) -> bool {
            self.0.is_default()
        }
    }

    impl EvalNested<Read<Value>> for LeafCell {
        fn execute_query(&self, _q: Read<Value>) -> Value {
            self.0.read_json(None)
        }
    }

    type Twins = (
        moirai_protocol::replica::Replica<LeafCell, moirai_protocol::broadcast::tcsb::Tcsb<LeafOp>>,
        moirai_protocol::replica::Replica<LeafCell, moirai_protocol::broadcast::tcsb::Tcsb<LeafOp>>,
    );

    fn twins_for(what: Recipe) -> Twins {
        recipe(what);
        twins_log::<LeafCell>()
    }

    // ---------------------------------------------------------------- text

    #[test]
    fn ip6_text_reads_the_same_string_as_its_event_graph() {
        // `eg_walker`'s own twins tests: two writers insert into one document
        // and both characters survive, in one order, on both replicas.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Text));

        let event = a.send(LeafOp::InsertChar { pos: 0, ch: 'H' }).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::InsertChar { pos: 1, ch: 'i' }).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!("Hi"));
        assert_eq!(b.query(Read::<Value>::new()), json!("Hi"));

        let event_a = a.send(LeafOp::InsertChar { pos: 2, ch: '!' }).unwrap();
        let event_b = b.send(LeafOp::InsertChar { pos: 2, ch: '?' }).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        let read = a.query(Read::<Value>::new());
        assert_eq!(read, b.query(Read::<Value>::new()));
        assert!(
            read == json!("Hi!?") || read == json!("Hi?!"),
            "both characters survive in one order: {read}"
        );

        let event = a.send(LeafOp::DeleteChar { pos: 0 }).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), b.query(Read::<Value>::new()));
        assert_eq!(
            a.query(Read::<Value>::new()).as_str().unwrap().len(),
            3,
            "one character out of four"
        );
    }

    // ------------------------------------------------------------ counters

    /// `resettable_counter.rs`'s own `simple_counter` and `concurrent_counter`
    /// tests, at one width: up five and down five reads zero, then a reset
    /// concurrent with an increment keeps the increment.
    ///
    /// The library's own test decrements first, which this one cannot do at
    /// every width: `EByte` is `u8` and *unsigned*, so `Dec(5)` from zero
    /// underflows and panics — on the generated path too, at the same line of
    /// `counter/stable.rs`. Monomorphising all six widths is what makes that
    /// visible here instead of at a reviewer's desk.
    fn counter_sequence(num: NumKind, zero: Value, expected: Value) {
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Counter {
            num,
            resettable: true,
        }));

        let event = a.send(LeafOp::Inc(Scalar::Int(5))).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::Dec(Scalar::Int(5))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), zero);
        assert_eq!(b.query(Read::<Value>::new()), zero);

        let event_a = a.send(LeafOp::Reset).unwrap();
        let event_b = b.send(LeafOp::Inc(Scalar::Int(18))).unwrap();
        a.receive(event_b);
        b.receive(event_a);

        assert_eq!(a.query(Read::<Value>::new()), expected);
        assert_eq!(b.query(Read::<Value>::new()), expected);
    }

    #[test]
    fn ip6_counter_u8() {
        counter_sequence(NumKind::U8, json!(0), json!(18));
    }

    #[test]
    fn ip6_counter_i16() {
        counter_sequence(NumKind::I16, json!(0), json!(18));
    }

    #[test]
    fn ip6_counter_i32() {
        counter_sequence(NumKind::I32, json!(0), json!(18));
    }

    #[test]
    fn ip6_counter_i64() {
        counter_sequence(NumKind::I64, json!(0), json!(18));
    }

    #[test]
    fn ip6_counter_f32() {
        counter_sequence(NumKind::F32, json!(0.0), json!(18.0));
    }

    #[test]
    fn ip6_counter_f64() {
        counter_sequence(NumKind::F64, json!(0.0), json!(18.0));
    }

    /// `simple_counter.rs`'s own test, decrementing after the increment for
    /// the reason `counter_sequence` gives. A simple counter takes no reset,
    /// and refusing one is the arm's own business.
    fn simple_counter_sequence(num: NumKind, zero: Value, seven: Value) {
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Counter {
            num,
            resettable: false,
        }));

        let event = a.send(LeafOp::Inc(Scalar::Int(5))).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::Dec(Scalar::Int(5))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), zero);
        assert_eq!(b.query(Read::<Value>::new()), zero);

        let event_a = a.send(LeafOp::Inc(Scalar::Int(3))).unwrap();
        let event_b = b.send(LeafOp::Inc(Scalar::Int(4))).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), seven);
        assert_eq!(b.query(Read::<Value>::new()), seven);

        assert!(
            a.send(LeafOp::Reset).is_none(),
            "a counter that is not resettable has no `Reset`"
        );
    }

    #[test]
    fn ip6_simple_counter_u8() {
        simple_counter_sequence(NumKind::U8, json!(0), json!(7));
    }

    #[test]
    fn ip6_simple_counter_i16() {
        simple_counter_sequence(NumKind::I16, json!(0), json!(7));
    }

    #[test]
    fn ip6_simple_counter_i32() {
        simple_counter_sequence(NumKind::I32, json!(0), json!(7));
    }

    #[test]
    fn ip6_simple_counter_i64() {
        simple_counter_sequence(NumKind::I64, json!(0), json!(7));
    }

    #[test]
    fn ip6_simple_counter_f32() {
        simple_counter_sequence(NumKind::F32, json!(0.0), json!(7.0));
    }

    #[test]
    fn ip6_simple_counter_f64() {
        simple_counter_sequence(NumKind::F64, json!(0.0), json!(7.0));
    }

    // --------------------------------------------------------------- flags

    #[test]
    fn ip6_enable_wins_flag() {
        // `ew_flag.rs`'s own `enable_wins_flag`, verbatim in its sequence and
        // in its outcome: the concurrent pair reads `true` on both sides.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Flag {
            wins: FlagWins::Enable,
        }));

        let event = a.send(LeafOp::Enable).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!(true));

        let event = b.send(LeafOp::Disable).unwrap();
        a.receive(event);
        assert_eq!(b.query(Read::<Value>::new()), json!(false));

        let event = a.send(LeafOp::Enable).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!(true));

        let event_a = a.send(LeafOp::Enable).unwrap();
        let event_b = b.send(LeafOp::Disable).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), json!(true));
        assert_eq!(b.query(Read::<Value>::new()), json!(true));
    }

    #[test]
    fn ip6_disable_wins_flag() {
        // `dw_flag.rs`'s own `disable_wins_concurrent`: the same pair, the
        // other verdict.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Flag {
            wins: FlagWins::Disable,
        }));

        let event_a = a.send(LeafOp::Enable).unwrap();
        assert_eq!(a.query(Read::<Value>::new()), json!(true));
        let event_b = b.send(LeafOp::Disable).unwrap();
        assert_eq!(b.query(Read::<Value>::new()), json!(false));

        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), json!(false));
        assert_eq!(b.query(Read::<Value>::new()), json!(false));
    }

    // ----------------------------------------------------------- registers

    #[test]
    fn ip6_multi_value_register() {
        // `mv_register.rs`'s own `concurrent_mv_register`: a sequential write
        // replaces, a concurrent pair keeps both, and the read-out of two
        // values is the canonical conflict object.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Register {
            tie: TieBreak::MultiValue,
        }));

        let event = a.send(LeafOp::Write(Scalar::text("c"))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!("c"));

        let event = b.send(LeafOp::Write(Scalar::text("d"))).unwrap();
        a.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!("d"));

        let event_a = a.send(LeafOp::Write(Scalar::text("a"))).unwrap();
        let event_b = b.send(LeafOp::Write(Scalar::text("b"))).unwrap();
        b.receive(event_a);
        a.receive(event_b);

        let conflict = json!({"__conflict": ["a", "b"]});
        assert_eq!(a.query(Read::<Value>::new()), conflict);
        assert_eq!(b.query(Read::<Value>::new()), conflict);
    }

    #[test]
    fn ip6_last_writer_wins_register() {
        // `unique_register.rs`'s own `lww_register_with_write` and its
        // concurrent sibling: one value always, the policy deciding which.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Register {
            tie: TieBreak::LastWriterWins,
        }));

        let event = a.send(LeafOp::Write(Scalar::text("Hello"))).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::Write(Scalar::text("World"))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!("World"));
        assert_eq!(b.query(Read::<Value>::new()), json!("World"));

        let event_a = a.send(LeafOp::Write(Scalar::text("x"))).unwrap();
        let event_b = b.send(LeafOp::Write(Scalar::text("y"))).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        let read = a.query(Read::<Value>::new());
        assert_eq!(read, b.query(Read::<Value>::new()));
        assert!(read == json!("x") || read == json!("y"), "one of the two");
    }

    #[test]
    fn ip6_fair_register() {
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Register {
            tie: TieBreak::Fair,
        }));

        let event = a.send(LeafOp::Write(Scalar::text("Hello"))).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::Write(Scalar::text("World"))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!("World"));
        assert_eq!(b.query(Read::<Value>::new()), json!("World"));

        let event_a = a.send(LeafOp::Write(Scalar::text("x"))).unwrap();
        let event_b = b.send(LeafOp::Write(Scalar::text("y"))).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), b.query(Read::<Value>::new()));
    }

    #[test]
    fn ip6_partial_order_register() {
        // `po_register.rs` keeps the values no other value dominates. Its own
        // test uses a type with a genuinely partial order; `Scalar`'s order is
        // total, exactly as the generated path's `PORegister<String>` and
        // `PORegister<char>` are, so the surviving set is the maximum and the
        // two paths agree by construction rather than by luck.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Register {
            tie: TieBreak::PartialOrder,
        }));

        let event = a.send(LeafOp::Write(Scalar::Int(1))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!(1));

        let event_a = a.send(LeafOp::Write(Scalar::Int(2))).unwrap();
        let event_b = b.send(LeafOp::Write(Scalar::Int(3))).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), json!(3));
        assert_eq!(b.query(Read::<Value>::new()), json!(3));
    }

    #[test]
    fn ip6_total_order_register() {
        // `to_register.rs`'s own twins test: the greater value survives a
        // concurrent pair, whichever order the two replicas saw it in.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Register {
            tie: TieBreak::TotalOrder,
        }));

        let event = a.send(LeafOp::Write(Scalar::text("a"))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!("a"));
        assert_eq!(b.query(Read::<Value>::new()), json!("a"));

        let event_a = a.send(LeafOp::Write(Scalar::text("b"))).unwrap();
        let event_b = b.send(LeafOp::Write(Scalar::text("c"))).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), json!("c"));
        assert_eq!(b.query(Read::<Value>::new()), json!("c"));
    }

    #[test]
    fn ip6_enum_register_reads_its_literal_position_without_a_table() {
        // An enum leaf is a register over `Scalar::Enum`; the literal's name
        // needs the table, which `ip13`'s read-out has and this one has not,
        // so the position stands in for it here.
        let (mut a, mut b) = twins_for(Recipe::Scalar(LeafRule::Enum {
            class: ClassSlot(0),
            tie: TieBreak::MultiValue,
        }));

        let event = a.send(LeafOp::Write(Scalar::Enum(0, 2))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!(2));
        assert_eq!(b.query(Read::<Value>::new()), json!(2));
    }

    // ---------------------------------------------------------- sets, bags

    #[test]
    fn ip6_add_wins_set() {
        // `aw_set.rs`'s own `complex_aw_set`: a remove concurrent with an add
        // of another value, both replicas reading `{b, c}`.
        let (mut a, mut b) = twins_for(Recipe::Set(SetTie::AddWins));

        let event = a.send(LeafOp::Add(Scalar::text("b"))).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::Add(Scalar::text("a"))).unwrap();
        b.receive(event);

        let event_a = a.send(LeafOp::Remove(Scalar::text("a"))).unwrap();
        let event_b = b.send(LeafOp::Add(Scalar::text("c"))).unwrap();
        a.receive(event_b);
        b.receive(event_a);

        assert_eq!(a.query(Read::<Value>::new()), json!(["b", "c"]));
        assert_eq!(b.query(Read::<Value>::new()), json!(["b", "c"]));
    }

    #[test]
    fn ip6_remove_wins_set() {
        // The same shape, the other verdict: `rw_set.rs`'s removal beats a
        // concurrent add of the same value.
        let (mut a, mut b) = twins_for(Recipe::Set(SetTie::RemoveWins));

        let event = a.send(LeafOp::Add(Scalar::text("a"))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!(["a"]));

        let event_a = a.send(LeafOp::Remove(Scalar::text("a"))).unwrap();
        let event_b = b.send(LeafOp::Add(Scalar::text("a"))).unwrap();
        a.receive(event_b);
        b.receive(event_a);

        assert_eq!(a.query(Read::<Value>::new()), b.query(Read::<Value>::new()));
        assert_eq!(a.query(Read::<Value>::new()), json!([]));
    }

    #[test]
    fn ip6_bag() {
        // `aw_bag.rs`'s own `simple_bag` and `concurrent_bag` in one run: two
        // concurrent adds of one value count two, and a removal takes one of
        // them back out.
        let (mut a, mut b) = twins_for(Recipe::Bag);

        let event_a = a.send(LeafOp::Add(Scalar::text("a"))).unwrap();
        let event_b = b.send(LeafOp::Add(Scalar::text("a"))).unwrap();
        a.receive(event_b);
        b.receive(event_a);
        assert_eq!(a.query(Read::<Value>::new()), json!(["a", "a"]));
        assert_eq!(b.query(Read::<Value>::new()), json!(["a", "a"]));

        let event = a.send(LeafOp::Add(Scalar::text("b"))).unwrap();
        b.receive(event);
        let event = a.send(LeafOp::Remove(Scalar::text("a"))).unwrap();
        b.receive(event);
        assert_eq!(a.query(Read::<Value>::new()), json!(["a", "b"]));
        assert_eq!(b.query(Read::<Value>::new()), json!(["a", "b"]));
    }

    // ------------------------------------------------------- the refusals

    #[test]
    fn ip6_an_arm_refuses_an_operation_of_another_family() {
        let text = LeafLog::for_rule(LeafRule::Text);
        let refusal = text.is_enabled(&LeafOp::Inc(Scalar::Int(1))).unwrap_err();
        assert_eq!(refusal.op, "Inc");
        assert_eq!(refusal.leaf, "text");
        assert_eq!(
            refusal.to_string(),
            "`Inc` is not an operation a `text` leaf takes"
        );

        let flag = LeafLog::for_rule(LeafRule::Flag {
            wins: FlagWins::Enable,
        });
        assert!(
            flag.is_enabled(&LeafOp::InsertChar { pos: 0, ch: 'a' })
                .is_err()
        );

        let counter = LeafLog::for_rule(LeafRule::Counter {
            num: NumKind::I32,
            resettable: true,
        });
        assert!(
            counter
                .is_enabled(&LeafOp::Inc(Scalar::text("two")))
                .is_err()
        );
        assert!(counter.is_enabled(&LeafOp::Inc(Scalar::Int(2))).is_ok());
    }

    #[test]
    fn ip6_a_text_leaf_refuses_a_position_past_its_end() {
        // The inner log's own `is_enabled`, not the kind check: the operation
        // is of the right family and still out of bounds.
        let (mut a, _b) = twins_for(Recipe::Scalar(LeafRule::Text));
        assert!(a.send(LeafOp::InsertChar { pos: 3, ch: 'x' }).is_none());
        assert!(a.send(LeafOp::InsertChar { pos: 0, ch: 'x' }).is_some());
        assert!(a.send(LeafOp::DeleteChar { pos: 1 }).is_none());
    }

    #[test]
    fn ip6_every_arm_is_reachable_from_a_rule() {
        // Twenty-three arms, and the constructors between them name each one
        // once: a rule this crate cannot mint is a rule the node would refuse
        // at run time for no reason a reader could find.
        let mut kinds = Vec::new();
        for num in [
            NumKind::U8,
            NumKind::I16,
            NumKind::I32,
            NumKind::I64,
            NumKind::F32,
            NumKind::F64,
        ] {
            for resettable in [true, false] {
                kinds.push(LeafLog::for_rule(LeafRule::Counter { num, resettable }));
            }
        }
        kinds.push(LeafLog::for_rule(LeafRule::Text));
        for wins in [FlagWins::Enable, FlagWins::Disable] {
            kinds.push(LeafLog::for_rule(LeafRule::Flag { wins }));
        }
        for tie in [
            TieBreak::MultiValue,
            TieBreak::LastWriterWins,
            TieBreak::Fair,
            TieBreak::PartialOrder,
            TieBreak::TotalOrder,
        ] {
            kinds.push(LeafLog::for_rule(LeafRule::Register { tie }));
        }
        kinds.push(LeafLog::for_set(SetTie::AddWins));
        kinds.push(LeafLog::for_set(SetTie::RemoveWins));
        kinds.push(LeafLog::for_bag());

        assert_eq!(kinds.len(), 23);
        assert!(kinds.iter().all(LeafLog::is_default));
    }
}
