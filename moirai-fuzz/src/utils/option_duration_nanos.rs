use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer, de::Error};

pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match duration {
        Some(duration) => serializer.serialize_some(&duration.as_nanos()),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(nanos) = Option::<u128>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let seconds = nanos / 1_000_000_000;
    let seconds = u64::try_from(seconds).map_err(D::Error::custom)?;
    let subsecond_nanos = (nanos % 1_000_000_000) as u32;

    Ok(Some(Duration::new(seconds, subsecond_nanos)))
}
