#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_protocol::utils::boxer::Boxer;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OptionUpdate<O> {
    Set(O),
    Unset,
}

// OptionUpdate operations embed their payload inline, just like list/map updates.
// Convert only the payload; Unset never allocates.
impl<Update> Boxer<OptionUpdate<Update>> for OptionUpdate<Box<Update>> {
    fn boxer(self) -> OptionUpdate<Update> {
        match self {
            Self::Set(op) => OptionUpdate::Set(*op),
            Self::Unset => OptionUpdate::Unset,
        }
    }
}

impl<O> Boxer<OptionUpdate<Box<O>>> for OptionUpdate<O> {
    fn boxer(self) -> OptionUpdate<Box<O>> {
        match self {
            Self::Set(op) => OptionUpdate::Set(Box::new(op)),
            Self::Unset => OptionUpdate::Unset,
        }
    }
}
