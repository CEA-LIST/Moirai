#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum BagUpdate<V> {
    Add(V),
    Remove(V),
    Clear,
}
