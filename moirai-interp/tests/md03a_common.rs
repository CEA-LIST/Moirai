//! MD.03a oracle data; the MD.03 and SI.05 helpers are reused read-only.
#![allow(dead_code)]
#[path = "md03_common.rs"]
mod md03;
pub use md03::*;
use serde_json::Value;

pub const BASIC: &str = "ScalarValues::Real";
pub const SPACE: &str = "SI::'watt hour'";
pub const HASH: &str = "BaseFunctions::'#'";
pub const UNICODE: &str = "SI::'ångström'";
pub const AT_SIGN: &str = "BaseFunctions::'@'";
pub const NESTED: &str = "ISQBase::'International System of Quantities'::'Θ'";
// Valid printer-shaped names beyond the pinned corpus's observed characters.
pub const ESCAPED: &str = "SI::'validator\\'s unit'";
pub const QUOTED_SEPARATOR: &str = "SI::'unit::name'";
pub const SAMPLES: [&str; 8] = [
    BASIC,
    SPACE,
    HASH,
    UNICODE,
    AT_SIGN,
    NESTED,
    ESCAPED,
    QUOTED_SEPARATOR,
];

pub fn observe(
    failures: &mut Vec<String>,
    doc: &Value,
    feature: &str,
    wanted: &[&str],
    phase: &str,
) {
    let actual = refs(doc, At::Board(0), feature);
    let expected = values(wanted);
    if actual != expected {
        failures.push(format!(
            "{phase}: {feature} expected {expected:?}, got {actual:?}"
        ));
    }
}

pub fn exact(reps: &[Rep], wanted: &[&str], phase: &str) {
    for replica in reps {
        for doc in [identified(replica), default_read(replica)] {
            assert_eq!(
                refs(&doc, At::Board(0), "related"),
                values(wanted),
                "{phase}: qualified bytes missing or rewritten"
            );
        }
    }
}
