use std::fmt::Debug;

pub fn format_number(value: f64) -> String {
    // Round up & convert to integer
    let rounded = value.ceil() as u64;

    // Format with space as thousands separator
    let s = rounded.to_string();
    let mut result = String::new();

    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(' ');
        }
        result.push(c);
    }

    result.chars().rev().collect::<String>().to_string()
}

pub fn format_string<V>(value: &V) -> String
where
    V: Debug,
{
    const LIMIT: usize = 100;

    let s = format!("{:?}", value);
    if s.len() <= LIMIT {
        s.to_string()
    } else {
        let start = &s[..LIMIT]; // les LIMIT premiers caractères
        let end = &s[s.len() - 3..]; // les 3 derniers caractères
        format!("{} ... {}", start, end)
    }
}
