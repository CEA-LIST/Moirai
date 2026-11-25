//! NW.31h: the small JSON reader of `nw16b_router_config_l1.rs`, shared by
//! `nw16b_router_pin_l0.rs` to read `cargo metadata`, so the router crate
//! needs no JSON dependency.
#![allow(dead_code)]

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub fn parse(text: &str) -> Result<Json, String> {
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
