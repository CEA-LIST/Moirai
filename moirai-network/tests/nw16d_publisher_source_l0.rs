//! NW.16d, S, at L0 (vault `spec/items/NW.16d.md`): the Zenoh transport
//! puts through Publishers it declares, and never through `Session::put`.
//!
//! Read from the source of `src/zenoh_transport.rs`, comments removed and
//! whitespace dropped: no `session.put(` call remains, and a
//! `declare_publisher(` call exists. What the publishers do on the wire is
//! the other tests' (`nw16d_publisher_qos_l2`, moirai-interp's
//! `nw16d_router_bytes_l3` and `nw16d_first_write_l2`).
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-network --test nw16d_publisher_source_l0
//! ```

const SOURCE: &str = include_str!("../src/zenoh_transport.rs");

/// The source without `//` comments (doc comments included), `/* */`
/// blocks, and the contents of string and character literals (each kept as
/// an empty `""`), with every whitespace character dropped. A literal such
/// as `"moirai/{s}/all/*"` therefore neither opens a comment nor matches.
fn code_only(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    let at = |i: usize, s: &str| {
        s.chars()
            .enumerate()
            .all(|(k, c)| chars.get(i + k) == Some(&c))
    };
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    while i < chars.len() {
        let c = chars[i];
        if at(i, "//") {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if at(i, "/*") {
            i += 2;
            while i < chars.len() && !at(i, "*/") {
                i += 1;
            }
            i += 2;
        } else if c == 'r'
            && (i == 0 || !ident(chars[i - 1]))
            && (chars.get(i + 1) == Some(&'"') || chars.get(i + 1) == Some(&'#'))
        {
            // A raw string: r"...", r#"..."#, and so on.
            let mut j = i + 1;
            let mut hashes = 0;
            while chars.get(j) == Some(&'#') {
                hashes += 1;
                j += 1;
            }
            if chars.get(j) != Some(&'"') {
                out.push(c);
                i += 1;
                continue;
            }
            let close: String = std::iter::once('"')
                .chain(std::iter::repeat_n('#', hashes))
                .collect();
            j += 1;
            while j < chars.len() && !at(j, &close) {
                j += 1;
            }
            out.push_str("\"\"");
            i = j + close.chars().count();
        } else if c == '"' {
            let mut j = i + 1;
            while j < chars.len() && chars[j] != '"' {
                j += if chars[j] == '\\' { 2 } else { 1 };
            }
            out.push_str("\"\"");
            i = j + 1;
        } else if c == '\'' {
            // A character literal ('x', '\n', '\u{..}') or a lifetime ('a).
            let escaped = chars.get(i + 1) == Some(&'\\');
            let end = if escaped {
                (i + 2..chars.len()).find(|&j| chars[j] == '\'')
            } else if chars.get(i + 2) == Some(&'\'') {
                Some(i + 2)
            } else {
                None
            };
            match end {
                Some(end) => {
                    out.push_str("''");
                    i = end + 1;
                }
                None => {
                    out.push(c);
                    i += 1;
                }
            }
        } else {
            if !c.is_whitespace() {
                out.push(c);
            }
            i += 1;
        }
    }
    out
}

/// The instrument's own check: a call split over lines is found, and the
/// same text inside a comment or a literal is not.
#[test]
fn the_reader_finds_a_call_across_lines_and_ignores_comments_and_literals() {
    let sample = "fn put(&self) {\n    // session.put( in a comment\n    /* session.put( */\n    \
                  let k = format!(\"moirai/{s}/all/*\");\n    let c = '/';\n    \
                  let r = r#\"session.put(\"#;\n    self.session\n        .put(&k, \"session.put(\")\n}\n";
    let code = code_only(sample);
    assert_eq!(
        code.matches("session.put(").count(),
        1,
        "NW16D READER WITNESS: the reader turned the sample into `{code}`"
    );
    assert!(!code_only("// declare_publisher(\n").contains("declare_publisher("));
}

#[test]
fn the_zenoh_transport_declares_publishers_and_calls_no_session_put() {
    let code = code_only(SOURCE);
    let puts: Vec<usize> = code.match_indices("session.put(").map(|(i, _)| i).collect();
    assert!(
        puts.is_empty(),
        "S: src/zenoh_transport.rs still calls session.put ({} call(s), the first reading `{}`), \
         where every frame goes through a declared Publisher",
        puts.len(),
        puts.first()
            .map(|&i| code[i..code.len().min(i + 60)].to_string())
            .unwrap_or_default()
    );
    assert!(
        code.contains("declare_publisher("),
        "S: src/zenoh_transport.rs declares no Publisher (no declare_publisher call)"
    );
}
