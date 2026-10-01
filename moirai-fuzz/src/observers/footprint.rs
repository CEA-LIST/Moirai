use std::{any::type_name, fmt::Display};

use deepsize::DeepSizeOf;
use moirai_protocol::{
    commitment::commit_log::CommitmentLog,
    crdt::replicated_data_type::ReplicatedDataType,
    state::{
        cache::CachedLog,
        graph_log::GraphLog,
        log::{BoxedLog, IsLog},
        po_log::POLog,
    },
};
use serde::{Deserialize, Serialize};

/// A node in the footprint tree, representing a log and its children
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FootprintNode {
    /// Log type name
    pub name: String,
    /// Size of the log in bytes, including its children
    pub inclusive_bytes: usize,
    /// Children of the log, if any
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<FootprintNode>,
}

impl FootprintNode {
    /// A leaf node has no children, and its size is the size of the log itself
    /// # Example
    /// The PO-Log is a leaf node.
    pub fn leaf<T>(value: &T) -> Self
    where
        T: DeepSizeOf,
    {
        Self {
            name: type_name::<T>().to_owned(),
            inclusive_bytes: value.deep_size_of(),
            children: Vec::new(),
        }
    }

    /// A parent node has children, and its size is the size of the log itself plus the size of its children
    /// # Example
    /// The UWMapLog is a parent node, and its children are the values in the map.
    pub fn parent<T>(value: &T, children: Vec<FootprintNode>) -> Self
    where
        T: DeepSizeOf,
    {
        Self {
            name: type_name::<T>().to_owned(),
            inclusive_bytes: value.deep_size_of(),
            children,
        }
    }
}

impl Display for FootprintNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} bytes)", self.name, self.inclusive_bytes)?;
        if !self.children.is_empty() {
            writeln!(f, " {{")?;
            for child in &self.children {
                writeln!(f, "  {}", child)?;
            }
            write!(f, "}}")?;
        }
        Ok(())
    }
}

pub trait LogFootprint {
    fn footprint(&self) -> FootprintNode;
}

//* Default implementation of LogFootprint for common logs */
impl<O> LogFootprint for GraphLog<O>
where
    O: ReplicatedDataType,
    GraphLog<O>: DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::leaf(self)
    }
}

impl<O, U> LogFootprint for POLog<O, U>
where
    O: ReplicatedDataType,
    POLog<O, U>: DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::leaf(self)
    }
}

impl<L, V> LogFootprint for CachedLog<L, V>
where
    L: IsLog + LogFootprint,
    CachedLog<L, V>: DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::parent(self, vec![self.inner().footprint()])
    }
}

impl<L> LogFootprint for CommitmentLog<L>
where
    L: IsLog + LogFootprint,
    CommitmentLog<L>: DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::parent(self, vec![self.child().footprint()])
    }
}

impl<L> LogFootprint for BoxedLog<L>
where
    L: IsLog + LogFootprint,
    BoxedLog<L>: DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::parent(self, vec![self.inner().footprint()])
    }
}

impl<L> LogFootprint for Box<L>
where
    L: IsLog + LogFootprint,
    Box<L>: DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        FootprintNode::parent(self, vec![self.as_ref().footprint()])
    }
}
