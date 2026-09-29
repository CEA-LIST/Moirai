//! A universally unique identity for a replicated log.
//!
//! A [`LogId`] names one replicated log. Every event a log broadcasts carries
//! it, so a receiver hosting several logs applies an arriving event to the log
//! it belongs to and to no other. Without it, a transport that carries more
//! than one log has no way to tell them apart, and the only separator left is
//! deployment configuration: one connection per log, or one port per log.
//!
//! The identity belongs to the log rather than to the envelope that carries
//! it. A log that knows its own name still knows it after a state transfer,
//! after being persisted and reloaded, and after being handed to a transport
//! that never looked inside. An identity stamped on by the network layer
//! survives none of those.
//!
//! # Uniqueness
//!
//! Ids are 128-bit values, minted independently and never coordinated. That is
//! deliberate: a local-first replica creates a log while offline and cannot ask
//! a registry for a name. Collision probability for random 128-bit values is
//! negligible at any scale this system will see.
//!
//! Two logs with the same id are the same log. Two logs with different ids
//! never merge, whatever their contents.
//!
//! # The two forms
//!
//! An id is either the bare `<h>`, where `h` is the 32 lowercase hex
//! characters of its random bytes, or the named `<h>_<name>`, where `name` is
//! one to thirty-two ASCII letters and digits. The name is there for people
//! reading an id (a model's id carries its metamodel's package), and it is
//! part of the identity all the same: the whole string is the id, so
//! `<h>`, `<h>_simpleuml` and `<h>_SimpleUML` are three logs.

use core::fmt;
use std::sync::Arc;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The number of random bytes behind an id.
const LOG_ID_BYTES: usize = 16;

/// The length of an id's hex part, two hex characters per byte: the whole of
/// a bare id, and the part before the `_` of a named one.
pub const LOG_ID_LEN: usize = LOG_ID_BYTES * 2;

/// The most characters a named id's name keeps.
pub const LOG_ID_NAME_MAX: usize = 32;

/// A universally unique identity for one replicated log.
///
/// Cheap to clone: the canonical text is behind an [`Arc`], because it is
/// copied onto every outgoing message.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId(Arc<str>);

impl LogId {
    /// Mint a new id.
    ///
    /// Needs no coordination and no network, which is what lets a replica
    /// create a log while partitioned.
    pub fn generate() -> Self {
        Self::from_bytes(random_bytes())
    }

    /// Build an id from 16 bytes of entropy.
    ///
    /// Exposed for callers that have their own source of randomness, and for
    /// tests that need a fixed id.
    pub fn from_bytes(bytes: [u8; LOG_ID_BYTES]) -> Self {
        let mut text = String::with_capacity(LOG_ID_LEN);
        for byte in bytes {
            // Hand-rolled rather than `format!("{:02x}")` per byte: this runs
            // once per log, but the allocation-free path keeps it obvious that
            // the output is exactly LOG_ID_LEN characters of lowercase hex.
            text.push(hex_digit(byte >> 4));
            text.push(hex_digit(byte & 0x0f));
        }
        Self(Arc::from(text.as_str()))
    }

    /// Mint a new named id: a fresh hex part and the name `name` gives; see
    /// [`Self::from_bytes_named`].
    pub fn generate_named(name: &str) -> Self {
        Self::from_bytes_named(random_bytes(), name)
    }

    /// Build a named id from 16 bytes of entropy and a name.
    ///
    /// The name kept is the ASCII letters and digits of `name`, in order and
    /// in their case, then the first [`LOG_ID_NAME_MAX`] of them. When
    /// nothing is kept the id is the bare one of the same bytes.
    pub fn from_bytes_named(bytes: [u8; LOG_ID_BYTES], name: &str) -> Self {
        let bare = Self::from_bytes(bytes);
        let kept: String = name
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(LOG_ID_NAME_MAX)
            .collect();
        if kept.is_empty() {
            return bare;
        }
        Self(Arc::from(format!("{bare}_{kept}").as_str()))
    }

    /// Parse either form: 32 lowercase hex characters, optionally followed
    /// by `_` and a name of one to 32 ASCII letters and digits.
    ///
    /// Keeps the text exactly as given and rejects anything else, uppercase
    /// hex included, so that two ids compare equal only when they are the
    /// same string.
    pub fn parse(text: &str) -> Result<Self, LogIdError> {
        if text.len() < LOG_ID_LEN {
            return Err(LogIdError::Length {
                found: text.len(),
                expected: LOG_ID_LEN,
            });
        }
        if let Some(bad) = text
            .chars()
            .take(LOG_ID_LEN)
            .find(|c| !matches!(c, '0'..='9' | 'a'..='f'))
        {
            return Err(LogIdError::Character(bad));
        }
        // The first LOG_ID_LEN characters are ASCII, so this is a boundary.
        let rest = &text[LOG_ID_LEN..];
        if !rest.is_empty() {
            let Some(name) = rest.strip_prefix('_') else {
                let found = rest.chars().next().unwrap_or('_');
                return Err(LogIdError::Separator(found));
            };
            if let Some(bad) = name.chars().find(|c| !c.is_ascii_alphanumeric()) {
                return Err(LogIdError::NameCharacter(bad));
            }
            if name.is_empty() || name.len() > LOG_ID_NAME_MAX {
                return Err(LogIdError::NameLength { found: name.len() });
            }
        }
        Ok(Self(Arc::from(text)))
    }

    /// The id's text, whole, in the form it was minted or parsed in.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for LogId {
    /// Shows the id in full. An id truncated in a log line is not one you can
    /// grep for in a peer's output, which is the only reason to print it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogId({})", self.0)
    }
}

/// Why a string is not a [`LogId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogIdError {
    /// Too few characters for the hex part.
    Length { found: usize, expected: usize },
    /// A character outside `[0-9a-f]` in the hex part.
    Character(char),
    /// Something other than `_` right after the hex part.
    Separator(char),
    /// A name of no character, or of more than [`LOG_ID_NAME_MAX`].
    NameLength { found: usize },
    /// A character of the name outside `[A-Za-z0-9]`.
    NameCharacter(char),
}

impl fmt::Display for LogIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length { found, expected } => {
                write!(
                    f,
                    "log id must be {expected} lowercase hex characters, optionally followed \
                     by `_` and a name, found {found} characters"
                )
            }
            Self::Character(c) => {
                write!(
                    f,
                    "log id must start with {LOG_ID_LEN} lowercase hex characters, found `{}`",
                    c.escape_debug()
                )
            }
            Self::Separator(c) => {
                write!(
                    f,
                    "log id must end after its {LOG_ID_LEN} hex characters or continue with \
                     `_`, found `{}`",
                    c.escape_debug()
                )
            }
            Self::NameLength { found } => {
                write!(
                    f,
                    "log id name must be 1 to {LOG_ID_NAME_MAX} characters, found {found}"
                )
            }
            Self::NameCharacter(c) => {
                write!(
                    f,
                    "log id name must be ASCII letters and digits, found `{}`",
                    c.escape_debug()
                )
            }
        }
    }
}

impl std::error::Error for LogIdError {}

#[cfg(feature = "test_utils")]
impl DeepSizeOf for LogId {
    /// The canonical text behind the [`Arc`], counted once per id rather than
    /// once per holder: every message stamped with this log's id shares the
    /// same allocation, and charging each of them for it would report a cost
    /// the process is not paying.
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.0.len()
    }
}

#[cfg(feature = "serde")]
impl Serialize for LogId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for LogId {
    /// Validates on the way in. A malformed id on the wire is a malformed
    /// frame, not a log that quietly never matches anything.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        LogId::parse(&text).map_err(serde::de::Error::custom)
    }
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + nibble - 10) as char,
    }
}

/// 16 bytes of entropy, without pulling a random-number crate into the core.
///
/// `/dev/urandom` is the deployment reality (every replica runs in a Linux
/// container), and the fallback covers the case where it cannot be read.
/// `RandomState` is seeded by the operating system, so hashing distinct
/// counters under two independently constructed states yields two uncorrelated
/// 64-bit halves.
fn random_bytes() -> [u8; LOG_ID_BYTES] {
    use std::io::Read;
    // `read_exact` on an open handle, never `fs::read`: the character device
    // has no end of file, so a read-to-end loops until memory runs out.
    if let Ok(mut urandom) = std::fs::File::open("/dev/urandom") {
        let mut out = [0u8; LOG_ID_BYTES];
        if urandom.read_exact(&mut out).is_ok() {
            return out;
        }
    }
    fallback_bytes()
}

fn fallback_bytes() -> [u8; LOG_ID_BYTES] {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let mut out = [0u8; LOG_ID_BYTES];
    for (half, chunk) in out.chunks_mut(8).enumerate() {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(nanos);
        hasher.write_usize(half);
        hasher.write_usize(std::process::id() as usize);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_id_is_canonical_and_parses_back() {
        let id = LogId::generate();
        assert_eq!(id.as_str().len(), LOG_ID_LEN);
        assert!(
            id.as_str()
                .chars()
                .all(|c| matches!(c, '0'..='9' | 'a'..='f'))
        );
        assert_eq!(LogId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn two_generated_ids_differ() {
        // Not a statistical claim. This catches a generator wired to a
        // constant, which is the way this function actually breaks.
        let a = LogId::generate();
        let b = LogId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn bytes_render_low_nibble_last() {
        let id = LogId::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        assert_eq!(id.as_str(), "0123456789abcdeffedcba9876543210");
    }

    #[test]
    fn the_fallback_generator_is_also_distinct_per_call() {
        assert_ne!(fallback_bytes(), fallback_bytes());
    }

    #[test]
    fn parse_rejects_the_wrong_length() {
        assert!(matches!(
            LogId::parse("abc"),
            Err(LogIdError::Length { found: 3, .. })
        ));
    }

    #[test]
    fn parse_rejects_uppercase_so_one_log_has_one_spelling() {
        let upper = "0123456789ABCDEFFEDCBA9876543210";
        assert!(matches!(
            LogId::parse(upper),
            Err(LogIdError::Character('A'))
        ));
    }

    #[test]
    fn parse_rejects_non_hex() {
        assert!(matches!(
            LogId::parse("0123456789abcdeffedcba987654321z"),
            Err(LogIdError::Character('z'))
        ));
    }
}
