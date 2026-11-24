//! One depth rule for every frame and log boundary, before any recursive decoding.

use serde::de::{DeserializeOwned, Error as _};

/// The deepest nesting 601c887 accepted, on frames and logs alike.
pub(crate) const MAX_JSON_DEPTH: usize = 1024;

/// Count containers without recursion or treating quoted brackets as structure.
/// Syntax validation is left to serde, including mismatched delimiters.
pub(crate) fn depth(input: &[u8]) -> usize {
    let (mut current, mut maximum, mut quoted, mut escaped) = (0usize, 0, false, false);
    for &byte in input {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'[' | b'{' => {
                    current += 1;
                    maximum = maximum.max(current);
                }
                b']' | b'}' => current = current.saturating_sub(1),
                _ => (),
            }
        }
    }
    maximum
}

pub(crate) fn check_depth(depth: usize) -> Result<(), serde_json::Error> {
    if depth > MAX_JSON_DEPTH {
        Err(serde_json::Error::custom(format!(
            "JSON depth {depth} exceeds limit {MAX_JSON_DEPTH}"
        )))
    } else {
        Ok(())
    }
}

/// A value decoded under the depth rule, with trailing content refused.
pub(crate) fn decode<T: DeserializeOwned>(input: &[u8]) -> Result<T, serde_json::Error> {
    check_depth(depth(input))?;
    // A finite stack budget for serde's recursive visitors and their cleanup.
    stacker::maybe_grow(16 * 1024 * 1024, 32 * 1024 * 1024, || {
        let mut deserializer = serde_json::Deserializer::from_slice(input);
        deserializer.disable_recursion_limit();
        let value = T::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(value)
    })
}
