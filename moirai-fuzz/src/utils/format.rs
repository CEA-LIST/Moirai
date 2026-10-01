/// Limit already-formatted text without adding Debug quotes or splitting UTF-8 characters.
pub fn format_string_ellipsis(value: &str, limit: Option<usize>) -> String {
    let Some(limit) = limit else {
        return value.to_owned();
    };
    let Some((end, _)) = value.char_indices().nth(limit) else {
        return value.to_owned();
    };
    let tail = value.char_indices().rev().nth(2).map_or(0, |(idx, _)| idx);
    format!("{} ... {}", &value[..end], &value[tail..])
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ellipsis_preserves_already_formatted_text() {
        let value = r#"{"hzog": {89}, "crx": {0}, "xhzg": {83}}"#;
        assert_eq!(format_string_ellipsis(value, Some(120)), value);
        assert_eq!(format_string_ellipsis(value, None), value);
        assert_eq!(format_string_ellipsis("", Some(0)), "");
        assert_eq!(format_string_ellipsis("abc", Some(3)), "abc");
    }

    #[test]
    fn ellipsis_truncates_at_character_boundaries() {
        assert_eq!(
            format_string_ellipsis("abcdéfgh🙂終", Some(5)),
            "abcdé ... h🙂終"
        );
        assert_eq!(format_string_ellipsis("é🙂", Some(1)), "é ... é🙂");
    }
}
