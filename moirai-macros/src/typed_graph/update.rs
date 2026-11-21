#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_protocol::state::object_path::ObjectPath;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Schema-specific vertex and arc payloads, independent of the conflict policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TypedGraphUpdate<V, A> {
    AddVertex { id: V },
    RemoveVertex { id: V },
    DeleteSubtree { prefix: ObjectPath },
    AddArc(A),
    RemoveArc(A),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum VertexUpdate<V> {
    AddVertex { id: V },
    RemoveVertex { id: V },
}
