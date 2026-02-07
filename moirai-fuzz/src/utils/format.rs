use std::fmt::Debug;

pub fn format_string<V>(value: &V) -> String
where
    V: Debug,
{
    const LIMIT: usize = 100;

    let s = format!("{value:?}");
    if s.len() <= LIMIT {
        s.to_string()
    } else {
        let start = &s[..LIMIT]; // les LIMIT premiers caractères
        let end = &s[s.len() - 3..]; // les 3 derniers caractères
        format!("{start} ... {end}")
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
