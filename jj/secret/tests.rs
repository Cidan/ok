//! The golden vectors (internal/secrets/testdata/vectors.json), which the Go model passes too.

use serde::Deserialize;

use super::*;

const VECTORS: &str = include_str!("../../internal/secrets/testdata/vectors.json");

#[derive(Deserialize)]
struct Vectors {
    context: String,
    commitments: Vec<Commitment>,
    pointers: Vec<PointerCase>,
    names: Vec<NameCase>,
    paths: Vec<PathCase>,
}

#[derive(Deserialize)]
struct Commitment {
    name: String,
    repository: i64,
    path: String,
    opening: String,
    value: String,
    digest: String,
}

#[derive(Deserialize)]
struct PointerCase {
    name: String,
    text: String,
    valid: bool,
}

#[derive(Deserialize)]
struct NameCase {
    name: String,
    valid: bool,
}

#[derive(Deserialize)]
struct PathCase {
    path: String,
    in_tree: bool,
    secret: bool,
    acl: bool,
    valid: bool,
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

impl Commitment {
    fn value(&self) -> Vec<u8> {
        match self.value.strip_prefix("repeat:") {
            Some(rest) => {
                let (b, n) = rest.split_once(':').expect("repeat:<byte>:<count>");
                vec![unhex(b)[0]; n.parse().expect("count")]
            }
            None => unhex(&self.value),
        }
    }
}

fn vectors() -> Vectors {
    serde_json::from_str(VECTORS).expect("parsing vectors.json")
}

#[test]
fn commitments() {
    let v = vectors();
    assert_eq!(v.context, COMMITMENT_CONTEXT);
    for c in &v.commitments {
        let opening: [u8; 32] = unhex(&c.opening).try_into().expect("a 32-byte opening");
        let d = commit(c.repository, &c.path, &opening, &c.value());
        assert_eq!(d.hex(), c.digest, "{}", c.name);
        assert!(
            verify(&d, c.repository, &c.path, &opening, &c.value()),
            "{}",
            c.name
        );
        let mut other = c.value();
        other.push(0);
        assert!(
            !verify(&d, c.repository, &c.path, &opening, &other),
            "{}",
            c.name
        );
        assert_eq!(parse_pointer(&pointer(&d)), Some(d), "{}", c.name);
    }
}

#[test]
fn pointers() {
    for p in vectors().pointers {
        let got = parse_pointer(p.text.as_bytes());
        assert_eq!(got.is_some(), p.valid, "{}", p.name);
        if let Some(d) = got {
            assert_eq!(pointer(&d), p.text.as_bytes(), "{}", p.name);
        }
    }
}

#[test]
fn names_and_paths() {
    let v = vectors();
    for n in v.names {
        assert_eq!(valid_name(&n.name), n.valid, "{:?}", n.name);
    }
    for p in v.paths {
        assert_eq!(
            (
                in_tree(&p.path),
                is_secret(&p.path),
                is_acl(&p.path),
                valid_path(&p.path)
            ),
            (p.in_tree, p.secret, p.acl, p.valid),
            "{:?}",
            p.path
        );
    }
}

#[test]
fn owners_files_are_not_secrets() {
    for name in ["OWNERS", "infra_OWNERS", "OWNERS_infra", "a-b_OWNERS"] {
        assert!(!secret_name(name), "{name}");
        assert!(is_owners(&format!("secret/prod/{name}")), "{name}");
    }
    for name in ["OWNERS.bak", "_OWNERS", "OWNERS_", "owners", "db-password"] {
        assert!(secret_name(name), "{name}");
        assert!(!is_owners(&format!("secret/prod/{name}")), "{name}");
    }
    assert!(!is_owners("apps/OWNERS"));
}

#[test]
fn secret_buf() {
    let mut b = SecretBuf::default();
    b.write_at(0, b"hunter");
    b.write_at(6, b"2");
    assert_eq!(b.as_slice(), b"hunter2");
    b.write_at(9, b"!");
    assert_eq!(b.as_slice(), b"hunter2\0\0!");
    b.set_len(4);
    assert_eq!(b.as_slice(), b"hunt");
    b.set_len(6);
    assert_eq!(b.as_slice(), b"hunt\0\0");
    assert_eq!(format!("{b:?}"), "SecretBuf(6 bytes)");
}

#[test]
fn refs() {
    assert_eq!(parse_ref("//secret/prod/db"), Some("secret/prod/db"));
    assert_eq!(parse_ref("secret/a"), Some("secret/a"));
    for bad in [
        "//secret",
        "//secret/acl.yaml",
        "//secret/x/OWNERS",
        "//other/x",
        "//secret/.x",
    ] {
        assert_eq!(parse_ref(bad), None, "{bad}");
    }
}
