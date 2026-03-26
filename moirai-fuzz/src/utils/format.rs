use std::fmt::Debug;

pub fn format_string_ellipsis<V>(value: &V, limit: Option<usize>) -> String
where
    V: Debug,
{
    let s = format!("{value:?}");
    if let Some(limit) = limit {
        if s.len() <= limit {
            s.to_string()
        } else {
            let start = &s[..limit]; // les limit premiers caractères
            let end = &s[s.len() - 3..]; // les 3 derniers caractères
            format!("{start} ... {end}")
        }
    } else {
        s.to_string()
    }
}

/// Remove extraneous formatting from DOT output
pub fn clean_dot_output(dot: &str) -> String {
    dot.replace("\"\"", "\"")
        .replace("\n", "")
        .replace("\\", "")
        .replace("  ", " ")
        .replace("\"\"", "\"")
}

pub fn seed_to_hex(seed: &[u8; 32]) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in seed {
        use std::fmt::Write;
        write!(&mut s, "{:02X}", b).unwrap();
    }
    s
}
