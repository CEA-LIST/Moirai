//! NW.16e, the router at L1 (vault `spec/items/NW.16e.md`): the
//! configuration `moirai-router` opens with and without `ROUTER_REGIONS`.
//!
//! - RA: without a list, [`moirai_router::config`] is the configuration it
//!   opened before this item, key for key, `gateway/south` unset (Zenoh's
//!   `auto` preset).
//! - RB: with a list of region names, `moirai_router::config_with_regions`
//!   is that same configuration plus `gateway/south` = one entry
//!   `{"filters": [{"region_names": ["<name>"]}]}` per name, in the order
//!   given, then a last catch-all entry `{}`; nothing else differs, and
//!   multicast scouting reads an explicit `false`.
//! - RC: `moirai_router::regions` reads a comma-separated `ROUTER_REGIONS`
//!   into its names, in order, each as written (a quote, a backslash and an
//!   inner space kept); whitespace around a name is not tested.
//! - RD: it refuses a list with an empty name (the variable set but empty
//!   included), a name over 32 bytes (Zenoh's limit, counted in bytes, not
//!   characters), or a name given twice.
//!
//! The expected configurations are written here by hand from Zenoh's
//! `Config::default()` and the keys `moirai-router/src/lib.rs` set at
//! de8dec2 (NW.16b's router: router mode, listen on `tcp/0.0.0.0:<port>`,
//! no connect endpoint, multicast scouting off, gossip on with one hop, a
//! 30 s `wait_before_close`), never taken from the code under test, and
//! compared with what it builds as whole JSON documents (`Config`'s
//! `Display`), read with the small reader at the end of this file, so the
//! crate needs no new dependency.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-router --test nw16e_router_regions_l1
//! ```

/// The router's `wait_before_close` at de8dec2, in microseconds.
const WAIT_BEFORE_CLOSE_US: &str = "30000000";

/// The configuration `moirai-router` opened at de8dec2 for `port`, built
/// here by hand.
fn today(port: u16) -> zenoh::Config {
    let mut config = zenoh::Config::default();
    let listen = format!("[\"tcp/0.0.0.0:{port}\"]");
    for (key, value) in [
        ("mode", "\"router\""),
        ("listen/endpoints", listen.as_str()),
        ("connect/endpoints", "[]"),
        ("scouting/multicast/enabled", "false"),
        ("scouting/gossip/enabled", "true"),
        ("scouting/gossip/multihop", "false"),
        (
            "transport/link/tx/queue/congestion_control/block/wait_before_close",
            WAIT_BEFORE_CLOSE_US,
        ),
    ] {
        config
            .insert_json5(key, value)
            .unwrap_or_else(|e| panic!("the expected configuration refused {key} = {value}: {e}"));
    }
    config
}

/// A JSON string literal for `text`.
fn quoted(text: &str) -> String {
    let mut out = String::from("\"");
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The `gateway/south` the claim names for `names`, as JSON text.
fn south(names: &[&str]) -> String {
    let mut entries: Vec<String> = names
        .iter()
        .map(|name| format!("{{\"filters\":[{{\"region_names\":[{}]}}]}}", quoted(name)))
        .collect();
    entries.push("{}".to_string());
    format!("[{}]", entries.join(","))
}

/// The configuration the claim names for `port` and `names`, built here.
/// Zenoh's key paths stop at `gateway` (its `south` is not a key of its
/// own), so the gateway is set whole.
fn expected(port: u16, names: &[&str]) -> zenoh::Config {
    let mut config = today(port);
    let value = format!("{{\"south\":{}}}", south(names));
    config
        .insert_json5("gateway", &value)
        .unwrap_or_else(|e| panic!("the expected configuration refused gateway = {value}: {e}"));
    config
}

/// The `south` member of the configuration's `gateway`, `Null` when absent.
fn gateway_south(config: &zenoh::Config) -> Json {
    match read(config, "gateway") {
        Json::Object(members) => members
            .into_iter()
            .find(|(k, _)| k == "south")
            .map(|(_, v)| v)
            .unwrap_or(Json::Null),
        other => panic!("get_json(gateway) reads {other:?}, not an object"),
    }
}

/// A whole configuration as JSON.
fn whole(config: &zenoh::Config) -> Json {
    let text = config.to_string();
    Json::parse(&text).unwrap_or_else(|e| panic!("a configuration that is not JSON ({e}): {text}"))
}

/// The paths where two JSON documents differ, with both values.
fn differences(left: &Json, right: &Json, path: &str, out: &mut Vec<String>) {
    match (left, right) {
        (Json::Object(a), Json::Object(b)) => {
            let mut keys: Vec<&String> = a.iter().map(|(k, _)| k).collect();
            for (k, _) in b {
                if !keys.contains(&k) {
                    keys.push(k);
                }
            }
            for key in keys {
                let x = a.iter().find(|(k, _)| k == key).map(|(_, v)| v);
                let y = b.iter().find(|(k, _)| k == key).map(|(_, v)| v);
                match (x, y) {
                    (Some(x), Some(y)) => differences(x, y, &format!("{path}/{key}"), out),
                    _ => out.push(format!("{path}/{key}: built {x:?}, expected {y:?}")),
                }
            }
        }
        (Json::Array(a), Json::Array(b)) if a.len() == b.len() => {
            for (i, (x, y)) in a.iter().zip(b).enumerate() {
                differences(x, y, &format!("{path}/{i}"), out);
            }
        }
        (a, b) if a == b => {}
        (a, b) => out.push(format!("{path}: built {a:?}, expected {b:?}")),
    }
}

fn read(config: &zenoh::Config, key: &str) -> Json {
    let text = config
        .get_json(key)
        .unwrap_or_else(|e| panic!("get_json({key}) on the router configuration: {e}"));
    Json::parse(&text).unwrap_or_else(|e| panic!("get_json({key}) = {text}: {e}"))
}

/// Region names a list may hold: plain, with punctuation, the longest
/// ASCII, the longest in two-byte characters, JSON's special characters.
const VALID: &[&[&str]] = &[
    &["core"],
    &["core", "site-h", "site-k"],
    &["site-k", "core"],
    &["xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"],
    &[
        "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}",
    ],
    &["a\"b", "c\\d", "site h"],
];

#[test]
fn ra_without_a_list_the_router_opens_the_configuration_it_opened_before() {
    for port in [moirai_router::DEFAULT_PORT, 0, 41234] {
        let built = moirai_router::config(port)
            .unwrap_or_else(|e| panic!("RA: moirai_router::config({port}) refused: {e}"));
        let mut diff = Vec::new();
        differences(&whole(&built), &whole(&today(port)), "", &mut diff);
        assert!(
            diff.is_empty(),
            "RA: without ROUTER_REGIONS the router's configuration for port {port} is not the one \
             it opened before NW.16e:\n{}",
            diff.join("\n")
        );
        assert_eq!(
            gateway_south(&built),
            Json::Null,
            "RA: without ROUTER_REGIONS the router sets gateway/south, where Zenoh's auto preset \
             (the key unset) is the stock router"
        );
    }
}

#[test]
fn rb_with_a_list_the_router_adds_one_subregion_per_name_then_a_catch_all() {
    for port in [0, 41234] {
        for names in VALID {
            let list: Vec<String> = names.iter().map(|n| n.to_string()).collect();
            let built = moirai_router::config_with_regions(port, &list).unwrap_or_else(|e| {
                panic!("RB: moirai_router::config_with_regions({port}, {list:?}) refused: {e}")
            });
            assert_eq!(
                read(&built, "scouting/multicast/enabled"),
                Json::Bool(false),
                "RB: with regions {list:?} the router leaves multicast scouting on (or at Zenoh's \
                 default, which is on)"
            );
            let mut diff = Vec::new();
            differences(
                &whole(&built),
                &whole(&expected(port, names)),
                "",
                &mut diff,
            );
            assert!(
                diff.is_empty(),
                "RB: with regions {list:?} on port {port} the router's configuration is not the one \
                 before NW.16e plus gateway/south = {}:\n{}",
                south(names),
                diff.join("\n")
            );
        }
    }
}

#[test]
fn rc_router_regions_reads_its_names_in_order_as_written() {
    for names in VALID {
        let raw = names.join(",");
        let read = moirai_router::regions(&raw)
            .unwrap_or_else(|e| panic!("RC: ROUTER_REGIONS=`{raw}` refused: {e}"));
        let wanted: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        assert_eq!(
            read, wanted,
            "RC: ROUTER_REGIONS=`{raw}` read as {read:?}, not its names in order"
        );
    }
}

#[test]
fn rd_router_regions_refuses_an_empty_name_a_name_over_32_bytes_and_a_duplicate() {
    let long_ascii = "x".repeat(33);
    let long_three_byte = "\u{20ac}".repeat(11); // 11 characters, 33 bytes
    let long_two_byte = "\u{e9}".repeat(17); // 17 characters, 34 bytes
    let with_long = format!("core,{long_ascii}");
    let cases: Vec<(&str, &str)> = vec![
        ("the variable set but empty", ""),
        ("an empty name in the middle", "core,,site-h"),
        ("an empty name first", ",core"),
        ("an empty name last", "core,"),
        ("one comma", ","),
        ("33 ASCII bytes", &long_ascii),
        ("33 bytes in 11 characters", &long_three_byte),
        ("34 bytes in 17 characters", &long_two_byte),
        ("a long name second", &with_long),
        ("a duplicate", "core,site-h,core"),
        ("a name twice in a row", "core,core"),
    ];
    let accepted: Vec<String> = cases
        .iter()
        .filter_map(|(why, raw)| {
            moirai_router::regions(raw)
                .ok()
                .map(|names| format!("{why}: ROUTER_REGIONS=`{raw}` read as {names:?}"))
        })
        .collect();
    assert!(
        accepted.is_empty(),
        "RD: ROUTER_REGIONS lists the router must refuse were accepted:\n{}",
        accepted.join("\n")
    );
}

// ---------------------------------------------------------------------------
// A JSON reader for what `get_json` and `Config`'s `Display` write, the same
// as `nw16b_router_config_l1.rs`'s.
// ---------------------------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    fn parse(text: &str) -> Result<Json, String> {
        let chars: Vec<char> = text.chars().collect();
        let mut at = 0;
        let value = parse_value(&chars, &mut at)?;
        skip_ws(&chars, &mut at);
        if at != chars.len() {
            return Err(format!("trailing text at {at}"));
        }
        Ok(value)
    }
}

fn skip_ws(chars: &[char], at: &mut usize) {
    while *at < chars.len() && chars[*at].is_whitespace() {
        *at += 1;
    }
}

fn expect(chars: &[char], at: &mut usize, wanted: char) -> Result<(), String> {
    skip_ws(chars, at);
    if chars.get(*at) == Some(&wanted) {
        *at += 1;
        Ok(())
    } else {
        Err(format!("expected {wanted:?} at {at}"))
    }
}

fn parse_value(chars: &[char], at: &mut usize) -> Result<Json, String> {
    skip_ws(chars, at);
    match chars.get(*at) {
        Some('n') => literal(chars, at, "null", Json::Null),
        Some('t') => literal(chars, at, "true", Json::Bool(true)),
        Some('f') => literal(chars, at, "false", Json::Bool(false)),
        Some('"') => parse_string(chars, at).map(Json::String),
        Some('[') => {
            *at += 1;
            let mut items = Vec::new();
            skip_ws(chars, at);
            if chars.get(*at) == Some(&']') {
                *at += 1;
                return Ok(Json::Array(items));
            }
            loop {
                items.push(parse_value(chars, at)?);
                skip_ws(chars, at);
                match chars.get(*at) {
                    Some(',') => *at += 1,
                    Some(']') => {
                        *at += 1;
                        return Ok(Json::Array(items));
                    }
                    _ => return Err(format!("expected , or ] at {at}")),
                }
            }
        }
        Some('{') => {
            *at += 1;
            let mut members = Vec::new();
            skip_ws(chars, at);
            if chars.get(*at) == Some(&'}') {
                *at += 1;
                return Ok(Json::Object(members));
            }
            loop {
                skip_ws(chars, at);
                let key = parse_string(chars, at)?;
                expect(chars, at, ':')?;
                members.push((key, parse_value(chars, at)?));
                skip_ws(chars, at);
                match chars.get(*at) {
                    Some(',') => *at += 1,
                    Some('}') => {
                        *at += 1;
                        return Ok(Json::Object(members));
                    }
                    _ => return Err(format!("expected , or }} at {at}")),
                }
            }
        }
        Some(c) if *c == '-' || c.is_ascii_digit() => {
            let start = *at;
            while *at < chars.len() && (chars[*at].is_ascii_digit() || "+-.eE".contains(chars[*at]))
            {
                *at += 1;
            }
            Ok(Json::Number(chars[start..*at].iter().collect()))
        }
        other => Err(format!("unexpected {other:?} at {at}")),
    }
}

fn literal(chars: &[char], at: &mut usize, word: &str, value: Json) -> Result<Json, String> {
    let end = *at + word.len();
    if end <= chars.len() && chars[*at..end].iter().collect::<String>() == word {
        *at = end;
        Ok(value)
    } else {
        Err(format!("expected {word} at {at}"))
    }
}

fn parse_string(chars: &[char], at: &mut usize) -> Result<String, String> {
    if chars.get(*at) != Some(&'"') {
        return Err(format!("expected a string at {at}"));
    }
    *at += 1;
    let mut out = String::new();
    while let Some(&c) = chars.get(*at) {
        *at += 1;
        match c {
            '"' => return Ok(out),
            '\\' => {
                let escaped = chars.get(*at).copied().ok_or("an escape at the end")?;
                *at += 1;
                match escaped {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    'u' => {
                        let hex: String = chars
                            .get(*at..*at + 4)
                            .ok_or("a short \\u")?
                            .iter()
                            .collect();
                        *at += 4;
                        let code = u32::from_str_radix(&hex, 16).map_err(|e| e.to_string())?;
                        out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                    }
                    other => out.push(other),
                }
            }
            c => out.push(c),
        }
    }
    Err("an unterminated string".into())
}
