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

pub fn estimate_debug_size_bits<V>(value: &V) -> usize
where
    V: Debug,
{
    format!("{value:?}").len() * 8
}

pub fn format_bits_human(bits: usize) -> String {
    const UNITS: [&str; 5] = ["b", "Kib", "Mib", "Gib", "Tib"];

    if bits < 1024 {
        return format!("{bits} b");
    }

    let mut value = bits as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    format!("{value:.2} {}", UNITS[unit])
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
