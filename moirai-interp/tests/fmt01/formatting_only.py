#!/usr/bin/env python3
"""FMT.01's formatting-only check (cea-cdrt-knowledge/spec/items/FMT.01.md).

For every file that differs between two revisions of a repository, it checks:

  C0  the file is a modified `.rs` file (not added, deleted, renamed or
      another kind of file), and the tree at HEAD holds no rustfmt.toml;
  C2  HEAD's text is rustfmt's own output of the base text (rustfmt run on
      the base text alone, on stdin, with the edition of the file's crate),
      and rustfmt run again on HEAD's text changes nothing;
  C4  the token stream of the two texts, read by the lexer below (written
      for this item, sharing no code with rustfmt or rustc), is the same
      whitespace aside, or differs only by differences of two registered
      classes: a trailing comma added or removed before a closing
      delimiter, outside a macro invocation's own delimiters and outside a
      one-element parenthesised expression; and the braces of a closure
      body that holds one expression and no statement, removed or added
      (with a trailing comma where the brace was, when there is one).
      Every accepted difference is printed with its lines.

Comments are tokens here: their text is compared with runs of whitespace
collapsed, since rustfmt re-indents a comment's lines when it moves one.

Usage:
  formatting_only.py check --repo PATH --base REV [--head REV]
  formatting_only.py lex-corpus --repo PATH [--rev REV]
  formatting_only.py self-test

`check` exits 0 when every changed file passes, 1 when one does not, 2 on
a usage or tool error. `lex-corpus` lexes every tracked `.rs` file at a
revision and exits 1 if any file fails to lex or its brackets do not
balance, which is the lexer's own calibration. `self-test` runs hand-written
cases (tricky literals, and changes the comparison must accept or refuse).
"""

import argparse
import difflib
import os
import re
import shutil
import subprocess
import sys
import tempfile

# ---------------------------------------------------------------- lexer --

PUNCT3 = ("<<=", ">>=", "...", "..=")
PUNCT2 = ("::", "->", "=>", "==", "!=", "<=", ">=", "&&", "||", "+=", "-=",
          "*=", "/=", "%=", "^=", "&=", "|=", "<<", ">>", "..")
OPEN = {"(": ")", "[": "]", "{": "}"}
CLOSE = {")": "(", "]": "[", "}": "{"}

# Keywords after which `(` opens an expression, a pattern or a type group
# rather than a call's or a signature's argument list.
PAREN_EXPR_KEYWORDS = {
    "as", "async", "await", "break", "const", "continue", "dyn", "else",
    "enum", "extern", "false", "fn", "for", "if", "impl", "in", "let",
    "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "static",
    "struct", "trait", "true", "type", "unsafe", "use", "where", "while",
    "yield",
}


RAW_STRING = re.compile(r'(br|cr|r)(#*)"')


class LexError(Exception):
    pass


def _ident_start(c):
    return c == "_" or c.isalpha() or (ord(c) > 127 and c.isidentifier())


def _ident_continue(c):
    return c == "_" or c.isalnum() or (ord(c) > 127 and ("a" + c).isidentifier())


def lex(text):
    """Returns a list of (kind, text, line) tuples, whitespace dropped.

    Kinds: comment, str, char, lifetime, ident, num, punct.
    """
    toks = []
    i, n, line = 0, len(text), 1

    def emit(kind, start, end):
        toks.append((kind, text[start:end], line))

    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
            i += 1
            continue
        if c.isspace():
            i += 1
            continue
        start = i
        # comments
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            emit("comment", i, j)
            i = j
            continue
        if text.startswith("/*", i):
            depth, j = 1, i + 2
            while depth:
                if j >= n:
                    raise LexError(f"unterminated block comment at line {line}")
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            emit("comment", i, j)
            line += text.count("\n", i, j)
            i = j
            continue
        # prefixed literals: raw strings, byte and C strings, byte chars
        m = RAW_STRING.match(text, i)
        if m:
            close = '"' + m.group(2)
            j = text.find(close, m.end())
            if j < 0:
                raise LexError(f"unterminated raw string at line {line}")
            j += len(close)
            emit("str", i, j)
            line += text.count("\n", i, j)
            i = j
            continue
        if (c in "bc") and i + 1 < n and text[i + 1] == '"':
            j = _scan_quoted(text, i + 2, '"', line)
            emit("str", i, j)
            line += text.count("\n", i, j)
            i = j
            continue
        if c == "b" and i + 1 < n and text[i + 1] == "'":
            j = _scan_quoted(text, i + 2, "'", line)
            emit("char", i, j)
            i = j
            continue
        # raw identifiers
        if text.startswith("r#", i) and i + 2 < n and _ident_start(text[i + 2]):
            j = i + 2
            while j < n and _ident_continue(text[j]):
                j += 1
            emit("ident", i, j)
            i = j
            continue
        if _ident_start(c):
            j = i + 1
            while j < n and _ident_continue(text[j]):
                j += 1
            emit("ident", i, j)
            i = j
            continue
        if c.isdigit():
            j = _scan_number(text, i)
            emit("num", i, j)
            i = j
            continue
        if c == '"':
            j = _scan_quoted(text, i + 1, '"', line)
            emit("str", i, j)
            line += text.count("\n", i, j)
            i = j
            continue
        if c == "'":
            if i + 1 < n and text[i + 1] == "\\":
                j = _scan_quoted(text, i + 1, "'", line)
                emit("char", i, j)
                i = j
                continue
            if i + 2 < n and text[i + 2] == "'" and text[i + 1] != "\n":
                emit("char", i, i + 3)
                i += 3
                continue
            j = i + 1
            while j < n and _ident_continue(text[j]):
                j += 1
            if j == i + 1:
                raise LexError(f"stray quote at line {line}")
            emit("lifetime", i, j)
            i = j
            continue
        for p in PUNCT3 + PUNCT2:
            if text.startswith(p, i):
                emit("punct", i, i + len(p))
                i += len(p)
                break
        else:
            emit("punct", i, i + 1)
            i += 1
    return toks


def _scan_quoted(text, j, quote, line):
    """From just after an opening quote, returns the index after the close."""
    n = len(text)
    while j < n:
        c = text[j]
        if c == "\\":
            j += 2
            continue
        if c == quote:
            return j + 1
        j += 1
    raise LexError(f"unterminated {quote} literal from line {line}")


def _scan_number(text, i):
    n = len(text)
    j = i
    while j < n and (text[j].isalnum() or text[j] == "_"):
        j += 1
    radix = text[i:i + 2] in ("0x", "0b", "0o")
    if not radix:
        # exponent sign: 1e-5, 2E+3
        if text[j - 1] in "eE" and j < n and text[j] in "+-" and j + 1 < n and text[j + 1].isdigit():
            j += 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
        # a fraction: `1.5`, `1.` but not `1..2`, `1.max(2)` or `x.0.1`'s dot
        elif j < n and text[j] == "." and not text.startswith("..", j) \
                and not (j + 1 < n and _ident_start(text[j + 1])):
            j += 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            if text[j - 1] in "eE" and j < n and text[j] in "+-" and j + 1 < n and text[j + 1].isdigit():
                j += 1
                while j < n and (text[j].isalnum() or text[j] == "_"):
                    j += 1
    return j


def normalise(tok):
    kind, text, _ = tok
    if kind == "comment":
        return (kind, " ".join(text.split()))
    return (kind, text)


def match_delimiters(toks):
    """Maps each closing delimiter's index to its opener's; raises if unbalanced."""
    stack, pairs = [], {}
    for k, (kind, text, line) in enumerate(toks):
        if kind != "punct":
            continue
        if text in OPEN:
            stack.append(k)
        elif text in CLOSE:
            if not stack or toks[stack[-1]][1] != CLOSE[text]:
                raise LexError(f"unbalanced {text!r} at line {line}")
            pairs[k] = stack.pop()
    if stack:
        raise LexError(f"unclosed {toks[stack[-1]][1]!r} from line {toks[stack[-1]][2]}")
    return pairs


def group_elements(toks, open_idx, close_idx):
    """Top-level comma-separated elements of a group, a trailing comma aside."""
    depth, commas, last_was_comma, empty = 0, 0, False, True
    for k in range(open_idx + 1, close_idx):
        kind, text, _ = toks[k]
        if kind == "comment":
            continue
        empty = False
        if kind == "punct" and text in OPEN:
            depth += 1
        elif kind == "punct" and text in CLOSE:
            depth -= 1
        if depth == 0 and kind == "punct" and text == ",":
            commas += 1
            last_was_comma = True
        elif kind != "comment":
            last_was_comma = False
    if empty:
        return 0
    return commas + (0 if last_was_comma else 1)


def previous_code(toks, k):
    k -= 1
    while k >= 0 and toks[k][0] == "comment":
        k -= 1
    return toks[k] if k >= 0 else None


def trailing_comma(toks, pairs, comma_idx, close_idx):
    """Whether a comma at comma_idx, directly before close_idx, is one whose
    presence or absence leaves the meaning unchanged; returns (ok, why)."""
    if toks[comma_idx][1] != "," or close_idx not in pairs:
        return False, "not a comma before a closing delimiter"
    open_idx = pairs[close_idx]
    before = previous_code(toks, open_idx)
    if before is not None and before[0] == "punct" and before[1] == "!":
        return False, "inside a macro invocation's own delimiters"
    if toks[open_idx][1] != "(":
        return True, toks[open_idx][1]
    if group_elements(toks, open_idx, close_idx) >= 2:
        return True, "( with two or more elements"
    if before is not None and (
        (before[0] == "ident" and before[1] not in PAREN_EXPR_KEYWORDS)
        or (before[0] == "punct" and before[1] in (")", "]", ">"))
    ):
        return True, "( in call position"
    return False, "a one-element ( that may be a parenthesised expression"


CLASSES = ("trailing comma added", "trailing comma removed",
           "closure braces removed", "closure braces added")

STATEMENT_WORDS = {"let", "fn", "struct", "enum", "use", "impl", "mod", "trait",
                   "const", "static", "type", "macro_rules"}


def closure_body(toks, open_to_close, open_idx):
    """If toks[open_idx] is the `{` of a closure body written right after the
    parameter list (`|..| {` or `|| {`, so a closure without a return type)
    that holds one expression and no statement, returns its `}`'s index."""
    if toks[open_idx][:2] != ("punct", "{"):
        return None
    before = previous_code(toks, open_idx)
    if before is None or before[0] != "punct" or before[1] not in ("|", "||"):
        return None
    close, depth = open_to_close[open_idx], 0
    for k in range(open_idx + 1, close):
        kind, text, _ = toks[k]
        if kind == "punct" and text in OPEN:
            depth += 1
        elif kind == "punct" and text in CLOSE:
            depth -= 1
        elif depth == 0 and ((kind == "punct" and text == ";")
                             or (kind == "ident" and text in STATEMENT_WORDS)):
            return None
    return close


def compare_tokens(old, new, scratch):
    """Returns (accepted, others). `accepted` lists (class, base line, HEAD line)
    for each difference of a registered class; `others` lists every other
    difference as (base slice, HEAD slice, base line, HEAD line).

    Registered classes: a trailing comma added or removed (see
    trailing_comma), and the braces of a one-expression closure body
    removed or added, alone or with a trailing comma where the `}` was.

    The alignment is GNU diff's minimal edit script over one token per
    line, which is fast on files of tens of thousands of tokens."""
    pa, pb = match_delimiters(old), match_delimiters(new)
    oa = {o: c for c, o in pa.items()}
    ob = {o: c for c, o in pb.items()}
    paths = []
    for name, toks in (("old", old), ("new", new)):
        path = os.path.join(scratch, f"{name}.tokens")
        with open(path, "w") as f:
            for t in toks:
                kind, text = normalise(t)
                f.write(f"{kind} {text!r}\n")
        paths.append(path)
    out = subprocess.run(["diff", "--minimal", "-U0", *paths], capture_output=True, text=True)
    if out.returncode not in (0, 1):
        raise SystemExit(f"FMT01-ERROR diff failed: {out.stderr.strip()}")
    hunks = []
    for m in re.finditer(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", out.stdout, re.M):
        a1, alen = int(m.group(1)), int(m.group(2) or 1)
        b1, blen = int(m.group(3)), int(m.group(4) or 1)
        # diff numbers lines from 1; with a zero length the number is the line before
        i1 = a1 - 1 if alen else a1
        j1 = b1 - 1 if blen else b1
        hunks.append((i1, i1 + alen, j1, j1 + blen))

    def line_of(toks, k):
        return toks[min(k, len(toks) - 1)][2] if toks else None

    accepted, done = [], set()
    # closure braces removed: `{` deleted after `|..|`, its `}` deleted or turned into a trailing comma
    for h in hunks:
        i1, i2, j1, j2 = h
        if i2 - i1 == 1 and j2 == j1:
            close = closure_body(old, oa, i1)
            if close is None:
                continue
            for h2 in hunks:
                k1, k2, l1, l2 = h2
                if h2 in done or (k1, k2) != (close, close + 1):
                    continue
                if l2 == l1:
                    accepted.append(("closure braces removed", old[i1][2], line_of(new, j1)))
                elif l2 - l1 == 1 and trailing_comma(new, pb, l1, l1 + 1)[0]:
                    accepted.append(("closure braces removed", old[i1][2], line_of(new, j1)))
                    accepted.append(("trailing comma added", old[close][2], new[l1][2]))
                else:
                    continue
                done.update({h, h2})
                break
    # closure braces added: `{` inserted after `|..|`, its `}` inserted or put where a trailing comma was
    for h in hunks:
        i1, i2, j1, j2 = h
        if h in done or not (j2 - j1 == 1 and i2 == i1):
            continue
        close = closure_body(new, ob, j1)
        if close is None:
            continue
        for h2 in hunks:
            k1, k2, l1, l2 = h2
            if h2 in done or (l1, l2) != (close, close + 1):
                continue
            if k2 == k1:
                accepted.append(("closure braces added", line_of(old, i1), new[j1][2]))
            elif k2 - k1 == 1 and trailing_comma(old, pa, k1, k1 + 1)[0]:
                accepted.append(("closure braces added", line_of(old, i1), new[j1][2]))
                accepted.append(("trailing comma removed", old[k1][2], new[close][2]))
            else:
                continue
            done.update({h, h2})
            break
    others = []
    for h in hunks:
        if h in done:
            continue
        i1, i2, j1, j2 = h
        if i2 == i1 and j2 - j1 == 1 and j2 < len(new) and trailing_comma(new, pb, j1, j2)[0]:
            accepted.append(("trailing comma added", line_of(old, i1), new[j1][2]))
            continue
        if j2 == j1 and i2 - i1 == 1 and i2 < len(old) and trailing_comma(old, pa, i1, i2)[0]:
            accepted.append(("trailing comma removed", old[i1][2], line_of(new, j1)))
            continue
        others.append((old[i1:i2], new[j1:j2], line_of(old, i1), line_of(new, j1)))
    accepted.sort(key=lambda a: (a[1] or 0, a[2] or 0, a[0]))
    return accepted, others


# ------------------------------------------------------------------ git --

def git(repo, *args, binary=False):
    out = subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True)
    return out.stdout if binary else out.stdout.decode()


def edition_of(repo, rev, path):
    """The edition of the crate whose Cargo.toml is nearest above `path`."""
    d = os.path.dirname(path)
    while True:
        manifest = f"{d}/Cargo.toml" if d else "Cargo.toml"
        try:
            text = git(repo, "show", f"{rev}:{manifest}")
        except subprocess.CalledProcessError:
            text = None
        if text is not None and re.search(r"^\[package\]", text, re.M):
            m = re.search(r'^edition\s*=\s*"(\d{4})"', text, re.M)
            if m:
                return m.group(1), manifest
            raise SystemExit(f"FMT01-ERROR {manifest} states no literal edition")
        if not d:
            raise SystemExit(f"FMT01-ERROR no package manifest above {path}")
        d = os.path.dirname(d)


def rustfmt(text, edition, cwd):
    out = subprocess.run(["rustfmt", "--edition", edition], input=text,
                         capture_output=True, cwd=cwd)
    if out.returncode != 0:
        return None, out.stderr.decode(errors="replace")
    return out.stdout, None


# ------------------------------------------------------------- commands --

def cmd_check(args):
    repo, base, head = os.path.abspath(args.repo), args.base, args.head
    base_sha = git(repo, "rev-parse", base).strip()
    head_sha = git(repo, "rev-parse", head).strip()
    tally = dict(files=0, non_rs=0, not_modified=0, not_rustfmt_output=0,
                 unstable=0, lex_errors=0, other_token_differences=0,
                 trailing_comma_added=0, trailing_comma_removed=0,
                 closure_braces_removed=0, closure_braces_added=0,
                 rustfmt_config_files=0)
    failures = []

    tree = git(repo, "ls-tree", "-r", "--name-only", head_sha).splitlines()
    for path in tree:
        if re.search(r"(^|/)\.?rustfmt\.toml$", path):
            tally["rustfmt_config_files"] += 1
            failures.append(f"FMT01-FAIL {path}: a rustfmt configuration file in the tree")

    scratch = tempfile.mkdtemp(prefix="fmt01-")
    changes = git(repo, "diff", "--name-status", "--no-renames", base_sha, head_sha).splitlines()
    for entry in changes:
        status, path = entry.split("\t", 1)
        tally["files"] += 1
        if not path.endswith(".rs"):
            tally["non_rs"] += 1
            failures.append(f"FMT01-FAIL {path}: not a .rs file (status {status})")
            continue
        if status != "M":
            tally["not_modified"] += 1
            failures.append(f"FMT01-FAIL {path}: status {status}, not a modification")
            continue
        old = git(repo, "show", f"{base_sha}:{path}", binary=True)
        new = git(repo, "show", f"{head_sha}:{path}", binary=True)
        edition, _ = edition_of(repo, head_sha, path)
        cwd = os.path.join(repo, os.path.dirname(path))
        cwd = cwd if os.path.isdir(cwd) else repo
        formatted, err = rustfmt(old, edition, cwd)
        is_output = formatted == new
        if not is_output:
            tally["not_rustfmt_output"] += 1
            failures.append(f"FMT01-FAIL {path}: HEAD's text is not rustfmt's output of the base text"
                            + (f" (rustfmt failed: {err.strip()})" if err else ""))
            if formatted is not None:
                diff = difflib.unified_diff(formatted.decode(errors="replace").splitlines(),
                                            new.decode(errors="replace").splitlines(),
                                            "rustfmt(base)", "HEAD", lineterm="", n=1)
                for k, dl in enumerate(diff):
                    if k >= 40:
                        failures.append("FMT01-DIFF   ...")
                        break
                    failures.append(f"FMT01-DIFF   {dl}")
        again, err2 = rustfmt(new, edition, cwd)
        stable = again == new
        if not stable:
            tally["unstable"] += 1
            failures.append(f"FMT01-FAIL {path}: rustfmt run on HEAD's text changes it"
                            + (f" (rustfmt failed: {err2.strip()})" if err2 else ""))
        try:
            old_toks, new_toks = lex(old.decode()), lex(new.decode())
            accepted, others = compare_tokens(old_toks, new_toks, scratch)
        except LexError as e:
            tally["lex_errors"] += 1
            failures.append(f"FMT01-FAIL {path}: lexer: {e}")
            print(f"FMT01-FILE {path} edition={edition} rustfmt_of_base={'equal' if is_output else 'DIFFERS'} "
                  f"stable={'yes' if stable else 'NO'} lex=ERROR")
            continue
        counts = {c: sum(a[0] == c for a in accepted) for c in CLASSES}
        for c, k in counts.items():
            tally[c.replace(" ", "_")] += k
        tally["other_token_differences"] += len(others)
        comments = (sum(t[0] == "comment" for t in old_toks), sum(t[0] == "comment" for t in new_toks))
        print(f"FMT01-FILE {path} edition={edition} rustfmt_of_base={'equal' if is_output else 'DIFFERS'} "
              f"stable={'yes' if stable else 'NO'} tokens={len(old_toks)}->{len(new_toks)} "
              f"comments={comments[0]}->{comments[1]} "
              + " ".join(f"{c.replace(' ', '_')}={k}" for c, k in counts.items())
              + f" other_token_differences={len(others)}")
        for c, lo, ln in accepted:
            print(f"FMT01-ACCEPT {path}: {c} at base line {lo}, HEAD line {ln}")
        for o, nw, lo, ln in others:
            failures.append(f"FMT01-FAIL {path}: token difference at base line {lo}, HEAD line {ln}: "
                            f"{[t[1] for t in o]} -> {[t[1] for t in nw]}")

    shutil.rmtree(scratch, ignore_errors=True)
    for f in failures:
        print(f)
    verdict = "PASS" if not failures else "FAIL"
    print(f"FMT01-SUMMARY repo={os.path.basename(repo)} base={base_sha[:7]} head={head_sha[:7]} "
          + " ".join(f"{k}={v}" for k, v in tally.items()) + f" result={verdict}")
    return 0 if verdict == "PASS" else 1


def cmd_lex_corpus(args):
    repo, rev = os.path.abspath(args.repo), args.rev
    sha = git(repo, "rev-parse", rev).strip()
    files = [p for p in git(repo, "ls-tree", "-r", "--name-only", sha).splitlines() if p.endswith(".rs")]
    bad, tokens, comments = [], 0, 0
    for path in files:
        text = git(repo, "show", f"{sha}:{path}", binary=True).decode()
        try:
            toks = lex(text)
            match_delimiters(toks)
        except LexError as e:
            bad.append(f"FMT01-LEX-FAIL {path}: {e}")
            continue
        tokens += len(toks)
        comments += sum(t[0] == "comment" for t in toks)
    for b in bad:
        print(b)
    print(f"FMT01-LEX repo={os.path.basename(repo)} rev={sha[:7]} files={len(files)} "
          f"tokens={tokens} comments={comments} failed={len(bad)}")
    return 1 if bad else 0


def cmd_self_test(_args):
    """Hand-written cases: the lexer on literals that trip naive lexers, and
    the comparison on changes it must accept and changes it must refuse."""
    lex_cases = [
        ("'a' 'a 'static '\\'' '\\\\' b'x' '\\u{1F600}'",
         [("char", "'a'"), ("lifetime", "'a"), ("lifetime", "'static"), ("char", "'\\''"),
          ("char", "'\\\\'"), ("char", "b'x'"), ("char", "'\\u{1F600}'")]),
        ('r#"a "quoted" // not a comment"# r"x" br##"y"## b"z" "s\\"t" r#type',
         [("str", 'r#"a "quoted" // not a comment"#'), ("str", 'r"x"'), ("str", 'br##"y"##'),
          ("str", 'b"z"'), ("str", '"s\\"t"'), ("ident", "r#type")]),
        ("/* a /* nested */ b */ x // tail\ny",
         [("comment", "/* a /* nested */ b */"), ("ident", "x"), ("comment", "// tail"), ("ident", "y")]),
        ("1..2 1.5 1. 1e-5 0x1F t.0.1 1.max(2)",
         [("num", "1"), ("punct", ".."), ("num", "2"), ("num", "1.5"), ("num", "1."),
          ("num", "1e-5"), ("num", "0x1F"), ("ident", "t"), ("punct", "."), ("num", "0.1"),
          ("num", "1"), ("punct", "."), ("ident", "max"), ("punct", "("), ("num", "2"), ("punct", ")")]),
        ("a::<Vec<u8>>() && b ..= c -> d => e",
         [("ident", "a"), ("punct", "::"), ("punct", "<"), ("ident", "Vec"), ("punct", "<"),
          ("ident", "u8"), ("punct", ">>"), ("punct", "("), ("punct", ")"), ("punct", "&&"),
          ("ident", "b"), ("punct", "..="), ("ident", "c"), ("punct", "->"), ("ident", "d"),
          ("punct", "=>"), ("ident", "e")]),
    ]
    # (name, old, new, expected {class: count} of accepted differences, or None when it must be refused)
    compare_cases = [
        ("call broken vertically, comma added", "f(a, b);", "f(\n    a,\n    b,\n);",
         {"trailing comma added": 1}),
        ("one-argument call, comma added", "f(a);", "f(\n    a,\n);", {"trailing comma added": 1}),
        ("signature, comma added", "fn f(a: u8) {}", "fn f(\n    a: u8,\n) {}", {"trailing comma added": 1}),
        ("struct literal, comma removed", "S { a, b, }", "S { a, b }", {"trailing comma removed": 1}),
        ("use list, comma removed", "use a::{\n    b, c,\n};", "use a::{b, c};", {"trailing comma removed": 1}),
        ("comment re-indented", "x; // a  b\n/* c\n   d */", "x; // a b\n/* c\n d */", {}),
        ("closure braces removed, trailing comma where the brace was", "w(\"s\", t, || {\n    Ok(x)\n})",
         "w(\n    \"s\",\n    t,\n    || Ok(x),\n)", {"closure braces removed": 1, "trailing comma added": 1}),
        ("closure braces added", "g(|x| x + 1);", "g(|x| {\n    x + 1\n});", {"closure braces added": 1}),
        ("arguments swapped", "f(a, b);", "f(b, a);", None),
        ("literal changed", "f(1, \"x\");", "f(2, \"x\");", None),
        ("paren expression made a tuple", "let x = (a);", "let x = (a,);", None),
        ("macro argument list, comma added", "m!(a, b);", "m!(a, b,);", None),
        ("binary and split into two borrows", "a && b", "a & &b", None),
        ("comment text changed", "x; // keep", "x; // kept", None),
        ("rustfmt skip attribute added", "fn f() {}", "#[rustfmt::skip]\nfn f() {}", None),
        ("closure braces removed around a statement", "g(|| { h(); x });", "g(|| h(); x);", None),
        ("closure braces removed around a let", "g(|| { let y = 1; y });", "g(|| y);", None),
        ("block braces removed outside a closure", "let v = { x };", "let v = x;", None),
    ]
    failed = 0
    for text, want in lex_cases:
        got = [(k, t) for k, t, _ in lex(text)]
        ok = got == want
        failed += not ok
        print(f"FMT01-SELF lex {'ok' if ok else 'FAIL'}: {text!r}" + ("" if ok else f" got {got}"))
    scratch = tempfile.mkdtemp(prefix="fmt01-self-")
    for name, old, new, want in compare_cases:
        accepted, others = compare_tokens(lex(old), lex(new), scratch)
        got = {}
        for c, _, _ in accepted:
            got[c] = got.get(c, 0) + 1
        ok = (not others and got == want) if want is not None else len(others) > 0
        failed += not ok
        print(f"FMT01-SELF compare {'ok' if ok else 'FAIL'}: {name}: accepted={got} other={len(others)} "
              f"(expected {'refused' if want is None else want})")
    shutil.rmtree(scratch, ignore_errors=True)
    print(f"FMT01-SELF cases={len(lex_cases) + len(compare_cases)} failed={failed}")
    return 1 if failed else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("--repo", required=True)
    c.add_argument("--base", required=True)
    c.add_argument("--head", default="HEAD")
    lc = sub.add_parser("lex-corpus")
    lc.add_argument("--repo", required=True)
    lc.add_argument("--rev", default="HEAD")
    sub.add_parser("self-test")
    args = ap.parse_args()
    commands = {"check": cmd_check, "lex-corpus": cmd_lex_corpus, "self-test": cmd_self_test}
    try:
        sys.exit(commands[args.cmd](args))
    except subprocess.CalledProcessError as e:
        print(f"FMT01-ERROR {' '.join(e.cmd)}: {e.stderr.decode(errors='replace').strip()}")
        sys.exit(2)


if __name__ == "__main__":
    main()
