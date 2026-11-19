#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_protocol::utils::boxer::Boxer;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Updates shared by remove-wins and update-wins maps.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MapUpdate<K, O> {
    Update(K, O),
    Remove(K),
    Clear,
}

impl<K, O> Boxer<MapUpdate<K, O>> for MapUpdate<K, Box<O>> {
    fn boxer(self) -> MapUpdate<K, O> {
        match self {
            Self::Update(k, v) => MapUpdate::Update(k, *v),
            Self::Remove(k) => MapUpdate::Remove(k),
            Self::Clear => MapUpdate::Clear,
        }
    }
}

impl<K, O> Boxer<MapUpdate<K, Box<O>>> for MapUpdate<K, O> {
    fn boxer(self) -> MapUpdate<K, Box<O>> {
        match self {
            Self::Update(k, v) => MapUpdate::Update(k, Box::new(v)),
            Self::Remove(k) => MapUpdate::Remove(k),
            Self::Clear => MapUpdate::Clear,
        }
    }
}
