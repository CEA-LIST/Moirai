//! NW.07: minting a named model id. `LogId::from_bytes_named(bytes, name)`
//! and `LogId::generate_named(name)` are the two surfaces the spec names
//! (`cea-cdrt-knowledge/spec/items/NW.07.md`, section 1); neither exists
//! before the change, so this file does not compile on the tree it was
//! written against, and that is its red.
//!
//! The rule, from the claim: keep the name's ASCII letters and digits, in
//! order and in their case, then the first thirty-two of them; a name that
//! keeps nothing gives the bare id. The expected ids below are written by
//! hand, and the property test compares against `reference`, this file's own
//! spelling of the rule, never against anything the implementation exports.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-protocol --features serde --test nw07_log_id_mint -- --nocapture
//! ```

use moirai_protocol::log_id::LogId;

const BYTES: [u8; 16] = [
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
];
const H: &str = "0123456789abcdeffedcba9876543210";

/// The rule, spelt independently of the implementation.
fn reference(hex: &str, name: &str) -> String {
    let kept: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(32)
        .collect();
    if kept.is_empty() {
        hex.to_string()
    } else {
        format!("{hex}_{kept}")
    }
}

/// The name as the descriptor declares it, and the id it must give.
fn fixtures() -> Vec<(String, String)> {
    let x32 = "x".repeat(32);
    let dashed = format!("{}{x32}y", "-".repeat(10));
    let fixtures: Vec<(&str, String)> = vec![
        // Our five metamodels, by their `package` field.
        ("simpleuml", format!("{H}_simpleuml")),
        ("behaviortree", format!("{H}_behaviortree")),
        ("json", format!("{H}_json")),
        ("notation", format!("{H}_notation")),
        ("sysml", format!("{H}_sysml")),
        // Case is kept, never folded.
        ("SimpleUML", format!("{H}_SimpleUML")),
        ("NW07Doc", format!("{H}_NW07Doc")),
        // Everything but ASCII letters and digits is dropped, wherever it is.
        ("Simple_UML-v2.0", format!("{H}_SimpleUMLv20")),
        (" SimpleUML ", format!("{H}_SimpleUML")),
        ("Simple UML", format!("{H}_SimpleUML")),
        ("run1790715680308", format!("{H}_run1790715680308")),
        ("123", format!("{H}_123")),
        ("Ünïcödé", format!("{H}_ncd")),
        ("a/b\\c..d", format!("{H}_abcd")),
        // At most thirty-two, counted after the drop.
        (
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop",
            format!("{H}_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"),
        ),
        (dashed.as_str(), format!("{H}_{x32}")),
        // A name that keeps nothing gives the bare id.
        ("", H.to_string()),
        ("_-_ .", H.to_string()),
        ("日本語", H.to_string()),
    ];
    fixtures
        .into_iter()
        .map(|(name, want)| (name.to_string(), want))
        .collect()
}

#[test]
fn nw07_m1_a_name_gives_the_hex_an_underscore_and_the_letters_and_digits_it_keeps() {
    let mut failures = Vec::new();
    for (name, want) in fixtures() {
        assert_eq!(
            reference(H, &name),
            want,
            "the fixture and the reference disagree on {name:?}"
        );
        let minted = LogId::from_bytes_named(BYTES, &name);
        if minted.as_str() != want {
            failures.push(format!(
                "{name:?} minted {:?}, want {want:?}",
                minted.as_str()
            ));
            continue;
        }
        match LogId::parse(minted.as_str()) {
            Ok(back) if back == minted => {}
            Ok(back) => failures.push(format!("{want:?} parsed back as {:?}", back.as_str())),
            Err(err) => failures.push(format!("{want:?} does not parse back: {err}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} names minted the wrong id:\n  {}",
        failures.len(),
        fixtures().len(),
        failures.join("\n  ")
    );
}

#[test]
fn nw07_m2_the_hex_part_is_the_bare_id_of_the_same_bytes() {
    let bare = LogId::from_bytes(BYTES);
    for name in ["SimpleUML", "", "json"] {
        let minted = LogId::from_bytes_named(BYTES, name);
        assert!(
            minted.as_str().starts_with(bare.as_str()),
            "{:?} does not start with the bare id {:?}",
            minted.as_str(),
            bare.as_str()
        );
    }
    assert_eq!(
        LogId::from_bytes_named(BYTES, ""),
        bare,
        "a name that keeps nothing is the bare id itself"
    );
}

#[test]
fn nw07_m3_generate_named_mints_a_fresh_hex_part_and_the_rule_s_name() {
    let mut failures = Vec::new();
    let a = LogId::generate_named("SimpleUML");
    let b = LogId::generate_named("SimpleUML");
    for id in [&a, &b] {
        let text = id.as_str();
        let ok = text.len() == 32 + 1 + 9
            && text[..32]
                .chars()
                .all(|c| matches!(c, '0'..='9' | 'a'..='f'))
            && &text[32..] == "_SimpleUML";
        if !ok {
            failures.push(format!("{text:?} is not <32 lowercase hex>_SimpleUML"));
        }
    }
    if a == b || a.as_str()[..32] == b.as_str()[..32] {
        failures.push(format!(
            "two mints share a hex part: {:?} and {:?}",
            a.as_str(),
            b.as_str()
        ));
    }
    for name in ["", "--", "日本"] {
        let bare = LogId::generate_named(name);
        let text = bare.as_str();
        if text.len() != 32 || !text.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) {
            failures.push(format!("{name:?} minted {text:?}, not a bare id"));
        }
    }
    let long = LogId::generate_named("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop");
    if &long.as_str()[32..] != "_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef" {
        failures.push(format!("a long name minted {:?}", long.as_str()));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Two thousand names drawn from an alphabet of letters, digits, separators,
/// whitespace, punctuation and multi-byte characters, at lengths from 0 to
/// 60, by a fixed linear congruential sequence: every one mints what the
/// reference says, and parses back to itself.
#[test]
fn nw07_m4_every_drawn_name_mints_what_the_rule_says() {
    const ALPHABET: &[char] = &[
        'a', 'b', 'z', 'A', 'M', 'Z', '0', '5', '9', '_', '-', '.', ' ', '/', '\\', ':', '\t', 'é',
        'Ü', 'ß', '日', '\u{0}', '!', 'q', 'Q', '7',
    ];
    let mut state: u64 = 0x4e57_3037_2026_0929;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) as usize
    };
    let mut failures = Vec::new();
    for _ in 0..2000 {
        let len = next() % 61;
        let name: String = (0..len)
            .map(|_| ALPHABET[next() % ALPHABET.len()])
            .collect();
        let want = reference(H, &name);
        let minted = LogId::from_bytes_named(BYTES, &name);
        if minted.as_str() != want {
            failures.push(format!(
                "{name:?} minted {:?}, want {want:?}",
                minted.as_str()
            ));
        } else if LogId::parse(&want).ok().as_ref() != Some(&minted) {
            failures.push(format!("{want:?} does not parse back to itself"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of 2000 drawn names minted the wrong id; the first:\n  {}",
        failures.len(),
        failures
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}
