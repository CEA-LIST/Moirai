use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer, de::Error};

pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u128(duration.as_nanos())
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let nanos = u128::deserialize(deserializer)?;
    let seconds = u64::try_from(nanos / 1_000_000_000).map_err(D::Error::custom)?;
    Ok(Duration::new(seconds, (nanos % 1_000_000_000) as u32))
}
