use serde::{Deserialize, Deserializer, Serializer, de::Error};

#[derive(Deserialize)]
#[serde(untagged)]
enum SeedRepresentation {
    Hex(String),
    Bytes([u8; 32]),
}

pub fn serialize<S>(seed: &Option<[u8; 32]>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match seed {
        Some(seed) => serializer.serialize_some(&crate::utils::format::seed_to_hex(seed)),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 32]>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(seed) = Option::<SeedRepresentation>::deserialize(deserializer)? else {
        return Ok(None);
    };

    match seed {
        SeedRepresentation::Bytes(seed) => Ok(Some(seed)),
        SeedRepresentation::Hex(seed) => parse_hex_seed(&seed).map(Some).map_err(D::Error::custom),
    }
}

fn parse_hex_seed(seed: &str) -> Result<[u8; 32], String> {
    let digits = seed.strip_prefix("0x").unwrap_or(seed);
    if digits.len() != 64 {
        return Err(format!(
            "seed must contain exactly 64 hexadecimal digits, got {}",
            digits.len()
        ));
    }

    let mut decoded = [0; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&digits[offset..offset + 2], 16)
            .map_err(|error| format!("invalid hexadecimal seed: {error}"))?;
    }
    Ok(decoded)
}
