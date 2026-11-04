//! What git prints, as git prints it: dates, `--pretty` formats and `--format` placeholders, and
//! quoted paths.

use chrono::DateTime;
use chrono::FixedOffset;
use chrono::Local;
use chrono::TimeZone as _;
use jj_lib::backend::Signature;
use jj_lib::backend::Timestamp;
use jj_lib::commit::Commit;
use jj_lib::object_id::ObjectId as _;

/// How git shows a date (`--date`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DateMode {
    Default { local: bool },
    Iso { local: bool },
    IsoStrict { local: bool },
    Rfc { local: bool },
    Short { local: bool },
    Raw,
    Unix,
    Relative,
    Format { pattern: String, local: bool },
}

impl DateMode {
    pub fn parse(s: &str) -> Option<Self> {
        let (s, local) = match s.strip_suffix("-local") {
            Some(base) => (base, true),
            None => (s, false),
        };
        Some(match s {
            "default" | "human" | "auto" => DateMode::Default { local },
            "local" => DateMode::Default { local: true },
            "iso" | "iso8601" => DateMode::Iso { local },
            "iso-strict" | "iso8601-strict" => DateMode::IsoStrict { local },
            "rfc" | "rfc2822" => DateMode::Rfc { local },
            "short" => DateMode::Short { local },
            "raw" => DateMode::Raw,
            "unix" => DateMode::Unix,
            "relative" => DateMode::Relative,
            _ => {
                let pattern = s.strip_prefix("format:")?;
                DateMode::Format {
                    pattern: pattern.to_owned(),
                    local,
                }
            }
        })
    }
}

fn datetime(t: &Timestamp, local: bool) -> DateTime<FixedOffset> {
    let secs = t.timestamp.0.div_euclid(1000);
    let utc = chrono::Utc
        .timestamp_opt(secs, 0)
        .single()
        .unwrap_or_default();
    if local {
        utc.with_timezone(&Local).fixed_offset()
    } else {
        let offset = FixedOffset::east_opt(t.tz_offset * 60)
            .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
        utc.with_timezone(&offset)
    }
}

pub fn unix(t: &Timestamp) -> i64 {
    t.timestamp.0.div_euclid(1000)
}

/// The zone as git writes it: `+0100`.
pub fn zone(t: &Timestamp) -> String {
    let m = t.tz_offset;
    let sign = if m < 0 { '-' } else { '+' };
    let m = m.abs();
    format!("{sign}{:02}{:02}", m / 60, m % 60)
}

pub fn date(t: &Timestamp, mode: &DateMode) -> String {
    match mode {
        DateMode::Default { local } => {
            let d = datetime(t, *local);
            if *local {
                d.format("%a %b %-d %H:%M:%S %Y").to_string()
            } else {
                d.format("%a %b %-d %H:%M:%S %Y %z").to_string()
            }
        }
        DateMode::Iso { local } => datetime(t, *local)
            .format("%Y-%m-%d %H:%M:%S %z")
            .to_string(),
        DateMode::IsoStrict { local } => datetime(t, *local)
            .format("%Y-%m-%dT%H:%M:%S%:z")
            .to_string(),
        DateMode::Rfc { local } => datetime(t, *local)
            .format("%a, %-d %b %Y %H:%M:%S %z")
            .to_string(),
        DateMode::Short { local } => datetime(t, *local).format("%Y-%m-%d").to_string(),
        DateMode::Raw => format!("{} {}", unix(t), zone(t)),
        DateMode::Unix => unix(t).to_string(),
        DateMode::Relative => relative(unix(t), chrono::Utc::now().timestamp()),
        DateMode::Format { pattern, local } => {
            let d = datetime(t, *local);
            let mut out = String::new();
            use std::fmt::Write as _;
            if write!(out, "{}", d.format(pattern)).is_err() {
                return pattern.clone();
            }
            out
        }
    }
}

/// git's relative dates (`show_date_relative`).
pub fn relative(then: i64, now: i64) -> String {
    if then > now {
        return "in the future".to_owned();
    }
    let plural = |n: i64, unit: &str| {
        if n == 1 {
            format!("{n} {unit} ago")
        } else {
            format!("{n} {unit}s ago")
        }
    };
    let diff = now - then;
    if diff < 90 {
        return plural(diff, "second");
    }
    let minutes = (diff + 30) / 60;
    if minutes < 90 {
        return plural(minutes, "minute");
    }
    let hours = (minutes + 30) / 60;
    if hours < 36 {
        return plural(hours, "hour");
    }
    let days = (hours + 12) / 24;
    if days < 14 {
        return plural(days, "day");
    }
    if days < 70 {
        return plural((days + 3) / 7, "week");
    }
    if days < 365 {
        return plural((days + 15) / 30, "month");
    }
    if days < 1825 {
        let total_months = (days * 12 * 2 + 365) / (365 * 2);
        let years = total_months / 12;
        let months = total_months % 12;
        let y = if years == 1 {
            "1 year".to_owned()
        } else {
            format!("{years} years")
        };
        if months > 0 {
            let m = if months == 1 {
                "1 month".to_owned()
            } else {
                format!("{months} months")
            };
            return format!("{y}, {m} ago");
        }
        return format!("{y} ago");
    }
    plural((days + 183) / 365, "year")
}

/// A person as git writes one: `Name <email>`.
pub fn ident(s: &Signature) -> String {
    format!("{} <{}>", s.name, s.email)
}

/// The subject: the first paragraph, its lines joined by spaces.
pub fn subject(description: &str) -> String {
    description
        .trim_start_matches('\n')
        .split("\n\n")
        .next()
        .unwrap_or("")
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join(" ")
}

/// The body: everything after the first paragraph, with its final newline.
pub fn body(description: &str) -> String {
    let d = description.trim_start_matches('\n');
    match d.split_once("\n\n") {
        Some((_, rest)) => {
            let rest = rest.trim_start_matches('\n').trim_end();
            if rest.is_empty() {
                String::new()
            } else {
                format!("{rest}\n")
            }
        }
        None => String::new(),
    }
}

/// The message as `%B` shows it: as written, ending in one newline.
pub fn raw_message(description: &str) -> String {
    let d = description.trim_end();
    if d.is_empty() {
        String::new()
    } else {
        format!("{d}\n")
    }
}

/// The message indented four spaces, as the commit formats show it.
pub fn indented(description: &str) -> String {
    let d = description.trim_end();
    let mut out = String::new();
    for line in d.lines() {
        out.push_str("    ");
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// A subject made safe for a file name (`%f`).
pub fn sanitized(subject: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in subject.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
            if c == '.' && out.ends_with('.') {
                continue;
            }
            out.push(c);
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_end_matches(['-', '.']).to_owned()
}

/// What the placeholders know of a commit beyond the commit itself.
pub struct Extra<'a> {
    pub short: &'a str,
    pub parents_short: &'a [String],
    pub tree: String,
    pub decorations: &'a [String],
    pub date_mode: &'a DateMode,
}

/// Expands a `--format` string for a commit (git's `format_commit_message`).
pub fn expand(format: &str, c: &Commit, x: &Extra<'_>) -> String {
    let mut out = String::new();
    let mut rest = format;
    while let Some(i) = rest.find('%') {
        out.push_str(&rest[..i]);
        rest = &rest[i + 1..];
        // `%+x`, `%-x`, `% x`: a newline before, newlines removed before, a space before —
        // when the expansion is not empty.
        let modifier = match rest.chars().next() {
            Some(m @ ('+' | '-' | ' ')) => {
                rest = &rest[1..];
                Some(m)
            }
            _ => None,
        };
        let (value, used) = placeholder(rest, c, x);
        if used == 0 {
            out.push('%');
            if let Some(m) = modifier {
                out.push(m);
            }
            continue;
        }
        rest = &rest[used..];
        match modifier {
            Some('+') if !value.is_empty() => {
                out.push('\n');
                out.push_str(&value);
            }
            Some('-') if value.is_empty() => {
                while out.ends_with('\n') {
                    out.pop();
                }
            }
            Some(' ') if !value.is_empty() => {
                out.push(' ');
                out.push_str(&value);
            }
            _ => out.push_str(&value),
        }
    }
    out.push_str(rest);
    out
}

/// One placeholder at the start of `s` (after its `%`): its expansion, and how many bytes it used
/// (0 when it is not one).
fn placeholder(s: &str, c: &Commit, x: &Extra<'_>) -> (String, usize) {
    let two = |a: char| s.chars().nth(1).map(|b| (a, b));
    let Some(first) = s.chars().next() else {
        return (String::new(), 0);
    };
    let id = c.id().hex();
    match first {
        'H' => (id, 1),
        'h' => (x.short.to_owned(), 1),
        'T' => (x.tree.clone(), 1),
        't' => (super::repo::short_hex(&x.tree).to_owned(), 1),
        'P' => (
            c.parent_ids()
                .iter()
                .map(|p| p.hex())
                .collect::<Vec<_>>()
                .join(" "),
            1,
        ),
        'p' => (x.parents_short.join(" "), 1),
        's' => (subject(c.description()), 1),
        'f' => (sanitized(&subject(c.description())), 1),
        'b' => (body(c.description()), 1),
        'B' => (raw_message(c.description()), 1),
        'N' | 'e' => (String::new(), 1),
        'n' => ("\n".to_owned(), 1),
        '%' => ("%".to_owned(), 1),
        'm' => (">".to_owned(), 1),
        'd' => {
            if x.decorations.is_empty() {
                (String::new(), 1)
            } else {
                (format!(" ({})", x.decorations.join(", ")), 1)
            }
        }
        'D' => (x.decorations.join(", "), 1),
        'x' => {
            let hex = s.get(1..3).unwrap_or("");
            match u8::from_str_radix(hex, 16) {
                Ok(b) if hex.len() == 2 => ((b as char).to_string(), 3),
                _ => (String::new(), 0),
            }
        }
        'C' => {
            // Colors: git's `%C…` only colors a terminal; ok's leave the text plain.
            if let Some(rest) = s.strip_prefix("C(") {
                match rest.find(')') {
                    Some(end) => (String::new(), end + 3),
                    None => (String::new(), 0),
                }
            } else {
                for name in ["reset", "red", "green", "blue"] {
                    if s[1..].starts_with(name) {
                        return (String::new(), 1 + name.len());
                    }
                }
                (String::new(), 0)
            }
        }
        'G' => match two('G') {
            Some((_, '?')) => ("N".to_owned(), 2),
            Some((_, 'G' | 'S' | 'K' | 'F' | 'P' | 'T')) => (String::new(), 2),
            _ => (String::new(), 0),
        },
        'a' | 'c' => {
            let sig = if first == 'a' {
                c.author()
            } else {
                c.committer()
            };
            let Some((_, what)) = two(first) else {
                return (String::new(), 0);
            };
            let v = match what {
                'n' | 'N' => sig.name.clone(),
                'e' | 'E' => sig.email.clone(),
                'l' | 'L' => sig.email.split('@').next().unwrap_or("").to_owned(),
                'd' => date(&sig.timestamp, x.date_mode),
                'D' => date(&sig.timestamp, &DateMode::Rfc { local: false }),
                'r' => date(&sig.timestamp, &DateMode::Relative),
                't' => unix(&sig.timestamp).to_string(),
                'i' => date(&sig.timestamp, &DateMode::Iso { local: false }),
                'I' => date(&sig.timestamp, &DateMode::IsoStrict { local: false }),
                's' => date(&sig.timestamp, &DateMode::Short { local: false }),
                'h' => date(&sig.timestamp, &DateMode::Relative),
                _ => return (String::new(), 0),
            };
            (v, 2)
        }
        'w' | '<' | '>' => {
            // Wrapping and padding: the text unwrapped and unpadded.
            if let Some(end) = s.find(')') {
                let mut used = end + 1;
                if first == '>' && s[1..].starts_with('>') {
                    used = end + 1;
                }
                (String::new(), used)
            } else {
                (String::new(), 0)
            }
        }
        '(' => {
            // `%(trailers…)` and the like: nothing ok keeps.
            match s.find(')') {
                Some(end) => (String::new(), end + 1),
                None => (String::new(), 0),
            }
        }
        _ => (String::new(), 0),
    }
}

/// A path as git shows it outside `-z`: in double quotes, with C escapes, when it has a quote, a
/// backslash, a control character, or (unless `core.quotePath` is off) any byte above ASCII.
pub fn quote_path(path: &str, quote_high: bool) -> String {
    let needs = path
        .bytes()
        .any(|b| b == b'"' || b == b'\\' || b < 0x20 || b == 0x7f || (quote_high && b >= 0x80));
    if !needs {
        return path.to_owned();
    }
    let mut out = String::from("\"");
    for b in path.bytes() {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            b'\t' => out.push_str("\\t"),
            b'\n' => out.push_str("\\n"),
            0x0b => out.push_str("\\v"),
            0x0c => out.push_str("\\f"),
            b'\r' => out.push_str("\\r"),
            b if b < 0x20 || b == 0x7f || (quote_high && b >= 0x80) => {
                out.push_str(&format!("\\{b:03o}"))
            }
            b => out.push(b as char),
        }
    }
    out.push('"');
    out
}

/// `1 file changed, 2 insertions(+), 1 deletion(-)`.
pub fn shortstat(files: usize, added: usize, removed: usize) -> String {
    let mut s = format!(" {files} file{} changed", if files == 1 { "" } else { "s" });
    if added > 0 || (added == 0 && removed == 0) {
        s.push_str(&format!(
            ", {added} insertion{}(+)",
            if added == 1 { "" } else { "s" }
        ));
    }
    if removed > 0 {
        s.push_str(&format!(
            ", {removed} deletion{}(-)",
            if removed == 1 { "" } else { "s" }
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_dates() {
        assert_eq!(relative(100, 100), "0 seconds ago");
        assert_eq!(relative(0, 89), "89 seconds ago");
        assert_eq!(relative(0, 90), "2 minutes ago");
        assert_eq!(relative(0, 3600), "60 minutes ago");
        assert_eq!(relative(0, 2 * 3600), "2 hours ago");
        assert_eq!(relative(0, 3 * 86400), "3 days ago");
        assert_eq!(relative(0, 20 * 86400), "3 weeks ago");
        assert_eq!(relative(0, 100 * 86400), "3 months ago");
        assert_eq!(relative(0, 400 * 86400), "1 year, 1 month ago");
        assert_eq!(relative(0, 3000 * 86400), "8 years ago");
        assert_eq!(relative(10, 0), "in the future");
    }

    #[test]
    fn messages() {
        let d = "first line\ncontinued\n\nbody para\nsecond\n";
        assert_eq!(subject(d), "first line continued");
        assert_eq!(body(d), "body para\nsecond\n");
        assert_eq!(raw_message(d), d);
        assert_eq!(indented("a\n\nb\n"), "    a\n    \n    b\n");
        assert_eq!(subject(""), "");
        assert_eq!(body("only\n"), "");
        assert_eq!(sanitized("feat: add the thing!"), "feat-add-the-thing");
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_path("plain/file.txt", true), "plain/file.txt");
        assert_eq!(quote_path("tab\there", true), "\"tab\\there\"");
        assert_eq!(quote_path("caf\u{e9}", true), "\"caf\\303\\251\"");
        assert_eq!(quote_path("caf\u{e9}", false), "caf\u{e9}");
        assert_eq!(quote_path("q\"uote", true), "\"q\\\"uote\"");
    }

    #[test]
    fn shortstats() {
        assert_eq!(shortstat(3, 2, 0), " 3 files changed, 2 insertions(+)");
        assert_eq!(
            shortstat(1, 1, 1),
            " 1 file changed, 1 insertion(+), 1 deletion(-)"
        );
        assert_eq!(shortstat(1, 0, 2), " 1 file changed, 2 deletions(-)");
    }

    #[test]
    fn zones() {
        let t = Timestamp {
            timestamp: jj_lib::backend::MillisSinceEpoch(1_793_703_600_000),
            tz_offset: 60,
        };
        assert_eq!(zone(&t), "+0100");
        assert_eq!(
            date(&t, &DateMode::Default { local: false }),
            "Tue Nov 3 12:00:00 2026 +0100"
        );
        assert_eq!(date(&t, &DateMode::Raw), "1793703600 +0100");
        assert_eq!(date(&t, &DateMode::Short { local: false }), "2026-11-03");
        assert_eq!(
            date(&t, &DateMode::Iso { local: false }),
            "2026-11-03 12:00:00 +0100"
        );
        assert_eq!(
            date(&t, &DateMode::IsoStrict { local: false }),
            "2026-11-03T12:00:00+01:00"
        );
        assert_eq!(
            date(&t, &DateMode::Rfc { local: false }),
            "Tue, 3 Nov 2026 12:00:00 +0100"
        );
    }
}
