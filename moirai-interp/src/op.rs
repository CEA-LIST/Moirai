//! The operation: a path from the model root down to one leaf, and the one
//! operation that opens a log.
//!
//! # Why the path is slots and not names
//!
//! `InstanceOp::Field` carries a [`FeatureSlot`] and `InstanceOp::Variant` a
//! [`ClassSlot`], so routing an operation is a bounded `Vec` index at each
//! step with nothing hashed and no name compared. Names live in the table,
//! for the read-out and for the sentences a refusal is written in.
//!
//! # Which slot a `Field` carries
//!
//! The **visible** slot: the position of the feature in the class's
//! `visible` list, which `moirai-semantics` sorts by name at parse time and
//! which flattens inheritance away. Not the declaring class's `declared`
//! position, which is what [`FeatureSlot`] means inside the table.
//!
//! This is not a detail. A `Sequence` sees `ID` and `name` declared by
//! `TreeNode` at declared slots 0 and 1, and `children` declared by
//! `ControlNode` at declared slot 0. Keying an object's fields by the
//! declared slot alone would land `children` and `ID` on the same key and
//! merge two features into one, silently. The visible slot is unique per
//! class by construction, and the table resolves it back to
//! `(declaring class, declared slot)` in one index —
//! [`crate::node::visible`] is that step.
//!
//! # Why `InternalizeOp` is written out
//!
//! An operation crosses a replica boundary, and the receiving replica indexes
//! its members in an order of its own (`intern_str.rs:148-202`). Anything in
//! an operation that names a replica has to be re-indexed on arrival. Nothing
//! in this tree does — a sequence position is an index resolved against the
//! event's own version, exactly as `NestedList` resolves it — so every arm
//! here rebuilds itself unchanged. Written out rather than left to a blanket
//! identity so that the day a variant *does* carry an
//! [`moirai_protocol::event::id::EventId`], the compiler asks about it.

use moirai_protocol::utils::intern_str::{InternalizeOp, Interner};
use moirai_semantics::{ClassSlot, FeatureSlot};

use crate::leaf::{LeafOp, Scalar};

/// One operation on a model log.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ModelOp {
    /// Open the log: install the table, and name what the model is.
    ///
    /// Decision D2. The descriptor travels as *text* and not as a path to a
    /// file, because what a joiner needs is the bytes: it arrives either by
    /// state transfer, which carries the parsed table inside the serialized
    /// log, or by delta, which replays this operation first because causal
    /// delivery puts it before everything else its creator wrote.
    Install {
        /// The model's own id, as the node registered it.
        model_id: String,
        /// The metamodel's id: the digest the descriptor hashes to.
        metamodel_id: String,
        /// The descriptor, `formatVersion` 2, as text.
        descriptor: String,
    },
    /// Everything else.
    Instance(InstanceOp),
}

/// One step of the path, or its end.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InstanceOp {
    /// Step into a feature of the object here, by its visible slot.
    Field(FeatureSlot, Box<InstanceOp>),
    /// Step into the object sitting in this containment, naming the concrete
    /// class it is an instance of.
    ///
    /// The class is named on *every* operation that reaches through the slot
    /// and not only on the one that created it, exactly as a `union!` op
    /// names its variant every time. That is what lets two replicas that
    /// concurrently put different subtypes here keep both.
    Variant(ClassSlot, Box<InstanceOp>),
    /// Address a sequence.
    Seq(SeqOp<Box<InstanceOp>>),
    /// Address an optional.
    Opt(OptOp<Box<InstanceOp>>),
    /// Address a keyed collection.
    Map(MapOp<Box<InstanceOp>>),
    /// Mint the object here; the end of a path.
    New,
    /// Write the leaf here; the end of a path.
    Leaf(LeafOp),
}

/// A sequence operation, mirroring `NestedList<O>`.
///
/// Positional, and resolved on delivery against `ReadAt(event.version())`:
/// the position an operation carries means what it meant in the state its
/// writer saw, which is the only reading of it two replicas can agree on.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SeqOp<O> {
    /// Put a new child at this position and address it.
    Insert {
        /// Where among the visible children.
        pos: usize,
        /// What to do to the new child, usually [`InstanceOp::New`].
        op: O,
    },
    /// Address the child at this position.
    Update {
        /// Where among the visible children.
        pos: usize,
        /// What to do to it.
        op: O,
    },
    /// Take the child at this position out.
    Delete {
        /// Where among the visible children.
        pos: usize,
    },
}

/// A keyed operation, mirroring `UWMap<K, O>`'s three arms
/// (`uw_map.rs:41-46`) with the key widened to a [`Scalar`] so that one
/// operation tree serves every key type the generator can compile.
///
/// The key is carried by value and never resolved against a version: a
/// `UWMapLog` hashes its key (`uw_map.rs:56`) and two replicas that write the
/// same key mean the same entry, which is the whole difference from a
/// sequence position.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MapOp<O> {
    /// Address the entry at this key, minting it if it is not there.
    Update {
        /// Which entry.
        key: Scalar,
        /// What to do to it.
        op: O,
    },
    /// Take the entry at this key out, update-wins: what is causally below
    /// the removal goes and what is concurrent with it stays.
    Remove {
        /// Which entry.
        key: Scalar,
    },
    /// The same, to every entry at once.
    Clear,
}

/// An optional operation, mirroring `Optional<O>`.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OptOp<O> {
    /// Address the child, minting it if it is not there.
    Set(O),
    /// Take the child out.
    Unset,
}

impl InstanceOp {
    /// `Field(slot, inner)`, spelled without the box.
    pub fn field(slot: FeatureSlot, inner: InstanceOp) -> Self {
        InstanceOp::Field(slot, Box::new(inner))
    }

    /// `Variant(class, inner)`, spelled without the box.
    pub fn variant(class: ClassSlot, inner: InstanceOp) -> Self {
        InstanceOp::Variant(class, Box::new(inner))
    }

    /// `Seq(Insert { pos, op })`, spelled without the box.
    pub fn insert(pos: usize, inner: InstanceOp) -> Self {
        InstanceOp::Seq(SeqOp::Insert {
            pos,
            op: Box::new(inner),
        })
    }

    /// `Seq(Update { pos, op })`, spelled without the box.
    pub fn at(pos: usize, inner: InstanceOp) -> Self {
        InstanceOp::Seq(SeqOp::Update {
            pos,
            op: Box::new(inner),
        })
    }

    /// `Seq(Delete { pos })`.
    pub fn delete(pos: usize) -> Self {
        InstanceOp::Seq(SeqOp::Delete { pos })
    }

    /// `Opt(Set(op))`, spelled without the box.
    pub fn set(inner: InstanceOp) -> Self {
        InstanceOp::Opt(OptOp::Set(Box::new(inner)))
    }

    /// `Opt(Unset)`.
    pub fn unset() -> Self {
        InstanceOp::Opt(OptOp::Unset)
    }

    /// `Map(Update { key, op })`, spelled without the box.
    pub fn entry(key: Scalar, inner: InstanceOp) -> Self {
        InstanceOp::Map(MapOp::Update {
            key,
            op: Box::new(inner),
        })
    }

    /// `Map(Remove { key })`.
    pub fn remove(key: Scalar) -> Self {
        InstanceOp::Map(MapOp::Remove { key })
    }

    /// `Map(Clear)`.
    pub fn clear() -> Self {
        InstanceOp::Map(MapOp::Clear)
    }

    /// Wrap this in a [`ModelOp`].
    pub fn into_model_op(self) -> ModelOp {
        ModelOp::Instance(self)
    }
}

impl<O> SeqOp<O> {
    /// The position this operation addresses.
    pub const fn pos(&self) -> usize {
        match self {
            SeqOp::Insert { pos, .. } | SeqOp::Update { pos, .. } | SeqOp::Delete { pos } => *pos,
        }
    }
}

impl InternalizeOp for ModelOp {
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            ModelOp::Install {
                model_id,
                metamodel_id,
                descriptor,
            } => ModelOp::Install {
                model_id,
                metamodel_id,
                descriptor,
            },
            ModelOp::Instance(op) => ModelOp::Instance(op.internalize(interner)),
        }
    }
}

impl InternalizeOp for InstanceOp {
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            InstanceOp::Field(slot, inner) => InstanceOp::Field(slot, inner.internalize(interner)),
            InstanceOp::Variant(class, inner) => {
                InstanceOp::Variant(class, inner.internalize(interner))
            }
            InstanceOp::Seq(op) => InstanceOp::Seq(op.internalize(interner)),
            InstanceOp::Opt(op) => InstanceOp::Opt(op.internalize(interner)),
            InstanceOp::Map(op) => InstanceOp::Map(op.internalize(interner)),
            InstanceOp::New => InstanceOp::New,
            InstanceOp::Leaf(op) => InstanceOp::Leaf(op.internalize(interner)),
        }
    }
}

impl<O> InternalizeOp for SeqOp<O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            SeqOp::Insert { pos, op } => SeqOp::Insert {
                pos,
                op: op.internalize(interner),
            },
            SeqOp::Update { pos, op } => SeqOp::Update {
                pos,
                op: op.internalize(interner),
            },
            SeqOp::Delete { pos } => SeqOp::Delete { pos },
        }
    }
}

impl<O> InternalizeOp for MapOp<O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            MapOp::Update { key, op } => MapOp::Update {
                key,
                op: op.internalize(interner),
            },
            MapOp::Remove { key } => MapOp::Remove { key },
            MapOp::Clear => MapOp::Clear,
        }
    }
}

impl<O> InternalizeOp for OptOp<O>
where
    O: InternalizeOp,
{
    fn internalize(self, interner: &Interner) -> Self {
        match self {
            OptOp::Set(op) => OptOp::Set(op.internalize(interner)),
            OptOp::Unset => OptOp::Unset,
        }
    }
}

#[cfg(test)]
mod tests {
    //! `ip7`, first half: the recursion is exhaustive and survives two
    //! interners that index their members in opposite orders.
    //!
    //! The second half is in [`crate::log`], where the same op tree crosses a
    //! real replica boundary and the state it builds is compared. Here the
    //! claim is narrower and worth stating plainly, because the
    //! implementation plan expected a different answer: **no operation in
    //! this tree carries an `EventId`.** A sequence is addressed by position
    //! and the position is resolved against the event's own version, so what
    //! `internalize` has to do at every arm is rebuild, and what this test
    //! has to check is that every arm *is* rebuilt and nothing is dropped on
    //! the way down.

    use moirai_protocol::utils::intern_str::{InternalizeOp, Interner};
    use moirai_semantics::{ClassSlot, FeatureSlot};

    use super::{InstanceOp, MapOp, ModelOp, OptOp, SeqOp};
    use crate::leaf::{LeafOp, Scalar};

    /// An interner that has seen these replicas, in this order.
    fn interner(members: &[&str]) -> Interner {
        let mut interner = Interner::new();
        for member in members {
            interner.intern(member);
        }
        interner
    }

    /// One op tree holding every variant of every enum in this module,
    /// nested six deep.
    fn every_variant() -> ModelOp {
        ModelOp::Instance(InstanceOp::field(
            FeatureSlot(3),
            InstanceOp::insert(
                2,
                InstanceOp::variant(
                    ClassSlot(7),
                    InstanceOp::field(
                        FeatureSlot(1),
                        InstanceOp::at(
                            0,
                            InstanceOp::variant(
                                ClassSlot(2),
                                InstanceOp::field(
                                    FeatureSlot(0),
                                    InstanceOp::entry(
                                        Scalar::text("door"),
                                        InstanceOp::set(InstanceOp::Leaf(LeafOp::Write(
                                            Scalar::text("door"),
                                        ))),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ))
    }

    #[test]
    fn ip7_the_recursion_reaches_every_nested_operation() {
        let one = interner(&["a", "b", "c"]);
        let other = interner(&["c", "b", "a"]);

        let op = every_variant();
        let there = op.clone().internalize(&one);
        let back = there.clone().internalize(&other);

        assert_eq!(there, op, "one interner leaves the tree as it was");
        assert_eq!(back, op, "and so does the other, whatever its ordering");
    }

    #[test]
    fn ip7_every_arm_is_rebuilt_rather_than_returned_whole() {
        // A recursion that stopped at the first level would still return an
        // equal tree, so equality alone proves nothing. This drives each arm
        // on its own with a marker the rebuild has to carry through, and
        // checks the depth the rebuilt tree came back at.
        let interner = interner(&["a", "b"]);

        fn depth(op: &InstanceOp) -> usize {
            match op {
                InstanceOp::Field(_, inner) | InstanceOp::Variant(_, inner) => 1 + depth(inner),
                InstanceOp::Seq(SeqOp::Insert { op, .. } | SeqOp::Update { op, .. }) => {
                    1 + depth(op)
                }
                InstanceOp::Seq(SeqOp::Delete { .. }) => 1,
                InstanceOp::Opt(OptOp::Set(op)) => 1 + depth(op),
                InstanceOp::Opt(OptOp::Unset) => 1,
                InstanceOp::Map(MapOp::Update { op, .. }) => 1 + depth(op),
                InstanceOp::Map(MapOp::Remove { .. } | MapOp::Clear) => 1,
                InstanceOp::New | InstanceOp::Leaf(_) => 1,
            }
        }

        let arms: Vec<InstanceOp> = vec![
            InstanceOp::field(FeatureSlot(4), InstanceOp::New),
            InstanceOp::variant(ClassSlot(5), InstanceOp::New),
            InstanceOp::insert(1, InstanceOp::New),
            InstanceOp::at(1, InstanceOp::New),
            InstanceOp::delete(1),
            InstanceOp::set(InstanceOp::New),
            InstanceOp::unset(),
            InstanceOp::entry(Scalar::text("k"), InstanceOp::New),
            InstanceOp::remove(Scalar::text("k")),
            InstanceOp::clear(),
            InstanceOp::New,
            InstanceOp::Leaf(LeafOp::InsertChar { pos: 0, ch: 'x' }),
        ];

        for arm in arms {
            let before = depth(&arm);
            let after = arm.clone().internalize(&interner);
            assert_eq!(depth(&after), before, "{arm:?} lost a level");
            assert_eq!(after, arm, "{arm:?} came back changed");
        }

        assert_eq!(
            depth(&InstanceOp::field(FeatureSlot(0), every_instance())),
            11
        );
    }

    fn every_instance() -> InstanceOp {
        match every_variant() {
            ModelOp::Instance(op) => op,
            ModelOp::Install { .. } => unreachable!(),
        }
    }

    #[test]
    fn ip7_install_crosses_an_interner_unchanged() {
        let interner = interner(&["a"]);
        let install = ModelOp::Install {
            model_id: "m1".to_string(),
            metamodel_id: "sha256:beef".to_string(),
            descriptor: "{\"formatVersion\":2}".to_string(),
        };
        assert_eq!(install.clone().internalize(&interner), install);
    }

    #[test]
    fn ip7_a_sequence_operation_reports_the_position_it_addresses() {
        assert_eq!(
            SeqOp::Insert {
                pos: 4,
                op: Box::new(InstanceOp::New)
            }
            .pos(),
            4
        );
        assert_eq!(SeqOp::<Box<InstanceOp>>::Delete { pos: 9 }.pos(), 9);
    }
}
