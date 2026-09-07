//! The digest half of a metamodel's identity.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// SHA-256, lowercase hex, over the compact `serde_json` serialization of the
/// parsed descriptor.
///
/// Taken over the parsed value and never over file bytes, so pretty and
/// compact renderings of one descriptor agree. `serde_json` runs without
/// `preserve_order` here, so every object serializes with its keys sorted and
/// the order a file lists them in cannot reach the digest either; a changed
/// class, attribute, enum or `nsURI` can.
///
/// This is a copy, in behaviour, of `arachne_codegen::metamodel_digest`
/// (`arachne-codegen/src/codegen/descriptor.rs`). It is a copy and not a call
/// because this crate depends on no Arachne crate and no Moirai crate: it is
/// the piece both sides share, so it cannot depend on either side. The
/// generated `network_node.rs` carries its own copy of the same four lines
/// for the same reason, and
/// `arachne/examples/fixtures/metamodel-digests.json` is where the copies are
/// held to one answer.
///
/// Anything that would make `serde_json` preserve insertion order — the
/// `preserve_order` feature reaching this crate through dependency
/// unification — silently moves every digest and breaks that agreement.
pub fn metamodel_digest(descriptor: &Value) -> String {
    format!("{:x}", Sha256::digest(descriptor.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::metamodel_digest;

    /// The digest is a function of the descriptor's content and nothing else:
    /// whitespace and key order are not content. This is the property the
    /// three copies of the rule agree on, and the reason a reformatted
    /// descriptor keeps its models.
    #[test]
    fn the_digest_ignores_whitespace_and_key_order() {
        let descriptor = json!({
            "formatVersion": 2,
            "package": "behaviortree",
            "nsURI": "http://www.example.org/behaviortree",
            "rootClasses": ["Root"],
            "classes": {},
            "enums": {"Status": ["RUNNING", "SUCCESS", "FAILURE"]},
        });

        let pretty: Value =
            serde_json::from_str(&serde_json::to_string_pretty(&descriptor).unwrap()).unwrap();
        let reordered: Value = serde_json::from_str(
            r#"{
                 "enums": {"Status": ["RUNNING", "SUCCESS", "FAILURE"]},
                 "classes": {},

                 "rootClasses":    ["Root"],
                 "nsURI": "http://www.example.org/behaviortree",
                 "package": "behaviortree",
                 "formatVersion": 2
               }"#,
        )
        .unwrap();

        let digest = metamodel_digest(&descriptor);
        assert_eq!(
            digest.len(),
            64,
            "SHA-256 in lowercase hex is 64 characters"
        );
        assert_eq!(
            metamodel_digest(&pretty),
            digest,
            "a formatter moved the digest"
        );
        assert_eq!(
            metamodel_digest(&reordered),
            digest,
            "a file's key order reached the digest, so `preserve_order` is on somewhere"
        );
    }

    /// The other half: an edit does move it, or the digest would not identify
    /// a metamodel at all. The constant is the SHA-256 of
    /// `{"classes":{},"formatVersion":2,"nsURI":"urn:a"}`, which pins the
    /// serialization this rule hashes and not only its stability, so a switch
    /// to pretty printing or to insertion order fails here rather than in the
    /// editor.
    #[test]
    fn the_digest_is_over_the_compact_key_sorted_form_and_moves_when_it_changes() {
        let descriptor = json!({"formatVersion": 2, "nsURI": "urn:a", "classes": {}});
        let edited = json!({"formatVersion": 2, "nsURI": "urn:b", "classes": {}});
        assert_eq!(
            metamodel_digest(&descriptor),
            "0af61afaae595b3befce4910e248952af22c045532da88e00af8d9f462cc4599"
        );
        assert_ne!(metamodel_digest(&descriptor), metamodel_digest(&edited));
    }
}
