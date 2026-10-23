//! NW.16b, R0 and R6 at L1 (vault `spec/items/NW.16b.md`): the configuration
//! the in-tree router opens is a router-mode session listening on
//! `tcp/0.0.0.0:<port>`, 7447 by default, with multicast scouting off.
//!
//! This is the check the spec runs, and must see pass, before any router
//! process starts on the host: by Zenoh's defaults a router answers multicast
//! scouts on 224.0.0.224:7446, and Ledgera's compiled peers autoconnect to any
//! router that answers there (`code/crates/util_deployment/src/zenoh_config.rs:64`
//! at ledgera-sdk 4eebc45), so a router left on the defaults would join the
//! two networks (review note `spec/Zenoh in Ledgera and Detune, as
//! deployed.md`, section 8).
//!
//! The configuration is read back with `zenoh::Config::get_json`. Zenoh
//! leaves a key nobody set `null` and applies its own default when the
//! session starts; for `scouting/multicast/enabled` that default is `true`
//! (`zenoh-config-1.10.1/src/defaults.rs:75`), so only an explicit `false`
//! passes. The crate's own dependencies are all this file uses: a tiny JSON
//! reader below parses what `get_json` returns.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-router --test nw16b_router_config_l1
//! ```

/// The router configuration for `port`, or the test's end naming the port.
fn built(port: u16) -> String {
    let config = moirai_router::config(port)
        .unwrap_or_else(|e| panic!("moirai_router::config({port}) refused: {e}"));
    let mut out = String::new();
    for key in ["mode", "listen/endpoints", "scouting/multicast/enabled"] {
        let text = config
            .get_json(key)
            .unwrap_or_else(|e| panic!("get_json({key}) on the router configuration: {e}"));
        out.push_str(&format!("{key} = {text}\n"));
    }
    out
}

fn read(port: u16, key: &str) -> Json {
    let config = moirai_router::config(port)
        .unwrap_or_else(|e| panic!("moirai_router::config({port}) refused: {e}"));
    let text = config
        .get_json(key)
        .unwrap_or_else(|e| panic!("get_json({key}) on the router configuration: {e}"));
    Json::parse(&text).unwrap_or_else(|e| panic!("get_json({key}) = {text}: {e}"))
}

/// The router's listen endpoints for router mode: a list as is, or the
/// `router` member of a per-mode object.
fn router_listen(port: u16) -> Vec<String> {
    let value = read(port, "listen/endpoints");
    let list = match &value {
        Json::Array(_) => value.clone(),
        Json::Object(members) => members
            .iter()
            .find(|(k, _)| k == "router")
            .map(|(_, v)| v.clone())
            .unwrap_or(Json::Null),
        other => other.clone(),
    };
    match list {
        Json::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Json::String(s) => s,
                other => panic!("a listen endpoint that is not a string: {other:?}"),
            })
            .collect(),
        // `null`: Zenoh's router default, `tcp/[::]:7447`, whatever the port.
        Json::Null => vec!["tcp/[::]:7447 (Zenoh's default, the key unset)".to_string()],
        other => panic!("listen/endpoints for router mode reads {other:?}"),
    }
}

#[test]
fn the_router_opens_router_mode_on_the_port_it_is_given_with_multicast_scouting_off() {
    for port in [moirai_router::DEFAULT_PORT, 0, 41234] {
        let all = built(port);
        // R0: router mode.
        assert_eq!(
            read(port, "mode"),
            Json::String("router".into()),
            "R0: the router configuration for port {port} is not router mode:\n{all}"
        );
        // R0: one listener, on every IPv4 interface, at the port given.
        assert_eq!(
            router_listen(port),
            vec![format!("tcp/0.0.0.0:{port}")],
            "R0: the router configuration for port {port} does not listen on exactly \
             tcp/0.0.0.0:{port}:\n{all}"
        );
        // R6 at L1: multicast scouting explicitly off. `null` is Zenoh's
        // default, which is on.
        assert_eq!(
            read(port, "scouting/multicast/enabled"),
            Json::Bool(false),
            "R6: the router configuration for port {port} leaves multicast scouting on (or at \
             Zenoh's default, which is on), so the router would answer scouts on \
             224.0.0.224:7446:\n{all}"
        );
    }
}

#[test]
fn the_default_port_is_7447() {
    assert_eq!(
        moirai_router::DEFAULT_PORT,
        7447,
        "R0: the router's default port is not 7447"
    );
}

// ---------------------------------------------------------------------------
// A JSON reader for what `get_json` returns: enough for these keys.
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
