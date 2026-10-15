use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, SystemTime};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};

use super::*;

// A certificate ok's CA issued (internal/id/ca) for the key beside it: ada's credential crd_fixture.
const CERT: &str = include_str!("testdata/cert.pem");
const KEY: &str = include_str!("testdata/key.pem");

fn write_credential(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(KEY_FILE), KEY).unwrap();
    std::fs::set_permissions(dir.join(KEY_FILE), std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(dir.join(CERT_FILE), CERT).unwrap();
    std::fs::write(
        dir.join(SERVER_FILE),
        r#"{"url": "https://ok.example.com", "addr": "ok.example.com:443", "tls": true}"#,
    )
    .unwrap();
}

#[test]
fn loads_what_ok_auth_login_writes() {
    let tmp = tempfile::tempdir().unwrap();
    write_credential(tmp.path());
    let c = load(tmp.path()).unwrap();
    assert_eq!(c.principal, "ada.human.example.com");
    assert_eq!(c.id, "crd_fixture");
    assert_eq!(c.handle(), Some("ada"));
    assert_eq!(
        c.server,
        Server {
            url: "https://ok.example.com".into(),
            addr: "ok.example.com:443".into(),
            tls: true
        }
    );
    assert!(c.person.is_none());
}

#[test]
fn refuses_an_exposed_key_and_reports_no_login() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(matches!(load(tmp.path()), Err(Error::NotLoggedIn)));
    write_credential(tmp.path());
    std::fs::set_permissions(
        tmp.path().join(KEY_FILE),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(matches!(load(tmp.path()), Err(Error::Exposed(_, 0o644))));
}

// The proof is what internal/credential verifies: v1.<seconds>.<nonce>.<signature>, the signature
// over "ok-proof v1\n" + method + "\n" + seconds + "\n" + nonce, and a new nonce each time.
#[test]
fn proofs_sign_the_method_and_time() {
    let tmp = tempfile::tempdir().unwrap();
    write_credential(tmp.path());
    let c = load(tmp.path()).unwrap();
    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_790_000_000);
    let proof = c.proof("/ok.scm.v1.Store/ReadObjects", at);
    let parts: Vec<&str> = proof.split('.').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], "v1");
    assert_eq!(parts[1], "1790000000");
    assert_eq!(URL_SAFE_NO_PAD.decode(parts[2]).unwrap().len(), 16);
    let sig = Signature::from_der(&URL_SAFE_NO_PAD.decode(parts[3]).unwrap()).unwrap();
    let vk = VerifyingKey::from(&c.key);
    let signed = format!(
        "ok-proof v1\n/ok.scm.v1.Store/ReadObjects\n1790000000\n{}",
        parts[2]
    );
    vk.verify(signed.as_bytes(), &sig).unwrap();
    assert!(
        vk.verify(
            b"ok-proof v1\n/ok.scm.v1.Store/Publish\n1790000000\nx",
            &sig
        )
        .is_err()
    );
    assert_ne!(c.proof("/m", at), c.proof("/m", at));
}

#[test]
fn save_and_load_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    write_credential(tmp.path());
    let c = load(tmp.path()).unwrap();
    let dir = tmp.path().join("copy");
    let person = Person {
        name: c.principal.clone(),
        email: "ada@example.com".into(),
        display_name: "Ada".into(),
    };
    save(&dir, &c.server, c.certificate(), &c.key, Some(&person)).unwrap();
    let again = load(&dir).unwrap();
    assert_eq!(again.certificate(), c.certificate());
    assert_eq!(again.person, Some(person));
    let mode = std::fs::metadata(dir.join(KEY_FILE))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
    let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
}

#[test]
fn renews_when_due() {
    let tmp = tempfile::tempdir().unwrap();
    write_credential(tmp.path());
    let c = load(tmp.path()).unwrap();
    assert!(c.needs_renewal(c.not_after - Duration::from_secs(3600)));
    assert!(!c.needs_renewal(c.not_after - Duration::from_secs(12 * 3600)));
}

#[test]
fn parses_servers() {
    let s = Server::parse("https://ok.example.com").unwrap();
    assert_eq!((s.addr.as_str(), s.tls), ("ok.example.com:443", true));
    let s = Server::parse("http://127.0.0.1:8472/").unwrap();
    assert_eq!(
        (s.url.as_str(), s.addr.as_str(), s.tls),
        ("http://127.0.0.1:8472", "127.0.0.1:8472", false)
    );
    assert!(Server::parse("ftp://ok.example.com").is_err());
}

#[test]
fn keys_are_p256_spki() {
    let k = new_key();
    let der = public_key_der(&k);
    // SEQUENCE { SEQUENCE { id-ecPublicKey, prime256v1 }, BIT STRING }: 91 bytes.
    assert_eq!(der.len(), 91);
}

// A certificate due for renewal is first looked for on disk: another process — `ok auth`, or the
// service that keeps a workload's credential there — may have renewed it.
#[tokio::test]
async fn a_newer_certificate_on_disk_is_taken_up() {
    let tmp = tempfile::tempdir().unwrap();
    write_credential(tmp.path());
    let src = Source::open(tmp.path().to_owned()).unwrap();
    let first = load(tmp.path()).unwrap();
    std::fs::write(
        tmp.path().join(CERT_FILE),
        include_str!("testdata/cert-renewed.pem"),
    )
    .unwrap();
    let renewed = load(tmp.path()).unwrap();
    assert!(renewed.not_after > first.not_after);
    // Not yet due: the one loaded stands.
    let c = src
        .current_at(first.not_after - Duration::from_secs(12 * 3600))
        .await
        .unwrap();
    assert_eq!(c.not_after, first.not_after);
    // Due: the newer one on disk is taken, and no renewal is asked for (none could reach a server).
    let c = src
        .current_at(first.not_after - Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(
        (c.not_after, c.id.as_str()),
        (renewed.not_after, "crd_fixture")
    );
}
