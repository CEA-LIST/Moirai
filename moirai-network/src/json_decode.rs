//! One depth rule for every node boundary, before any recursive decoding.

use serde::de::{DeserializeOwned, Error as _};
use serde_json::{Error, Value};

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

pub(crate) fn check_depth(depth: usize, limit: usize) -> Result<(), Error> {
    if depth > limit {
        Err(Error::custom(format!(
            "JSON depth {depth} exceeds limit {limit}"
        )))
    } else {
        Ok(())
    }
}

/// A finite stack budget for serde's recursive visitors and their error cleanup.
/// Nested calls (HTTP operation decoding) reuse the stack while space remains.
fn with_stack<T>(f: impl FnOnce() -> T) -> T {
    moirai_protocol::replica::with_network_id_validation(|| {
        stacker::maybe_grow(16 * 1024 * 1024, 32 * 1024 * 1024, f)
    })
}

pub(crate) fn decode<T: DeserializeOwned>(input: &[u8]) -> Result<T, Error> {
    check_depth(depth(input), MAX_JSON_DEPTH)?;
    with_stack(|| {
        let mut deserializer = serde_json::Deserializer::from_slice(input);
        deserializer.disable_recursion_limit();
        let value = T::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(value)
    })
}

/// A plain log may also come from an in-memory caller, without a frame decode.
pub(crate) fn checked_value(value: Value) -> Result<Value, Error> {
    let mut maximum = 0;
    let mut pending = vec![(&value, 0)];
    while let Some((node, parent_depth)) = pending.pop() {
        let depth = parent_depth + 1;
        match node {
            Value::Array(children) => {
                maximum = maximum.max(depth);
                pending.extend(children.iter().map(|child| (child, depth)));
            }
            Value::Object(children) => {
                maximum = maximum.max(depth);
                pending.extend(children.values().map(|child| (child, depth)));
            }
            _ => (),
        }
    }
    if let Err(error) = check_depth(maximum, MAX_JSON_DEPTH) {
        // Even destruction must not recurse through an arbitrarily deep input.
        let mut pending = vec![value];
        while let Some(node) = pending.pop() {
            match node {
                Value::Array(children) => pending.extend(children),
                Value::Object(children) => pending.extend(children.into_values()),
                _ => (),
            }
        }
        return Err(error);
    }
    Ok(value)
}

pub(crate) fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    let value = checked_value(value)?;
    with_stack(|| serde_json::from_value(value))
}
