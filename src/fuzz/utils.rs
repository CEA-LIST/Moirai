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
