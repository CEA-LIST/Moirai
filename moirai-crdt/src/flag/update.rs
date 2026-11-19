#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FlagUpdate {
    Enable,
    Disable,
    Clear,
}
