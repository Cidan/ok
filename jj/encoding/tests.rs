use std::collections::{BTreeMap, HashSet};

use jj_lib::backend::{
    ChangeId, Commit, CommitId, CopyHistory, CopyId, FileId, MillisSinceEpoch, SecureSig,
    Signature, SymlinkId, Timestamp, Tree, TreeId, TreeValue,
};
use jj_lib::hex_util::{decode_hex, encode_hex};
use jj_lib::merge::Merge;
use jj_lib::op_store::{
    Operation, OperationId, OperationMetadata, RefTarget, RemoteRef, RemoteRefState, RemoteView,
    TimestampRange, View, ViewId,
};
use jj_lib::ref_name::{GitRefNameBuf, RefNameBuf, RemoteNameBuf, WorkspaceNameBuf};
use jj_lib::repo_path::{RepoPathBuf, RepoPathComponentBuf};
use serde::Deserialize;

use crate::*;

const VECTORS: &str = include_str!("../../internal/scm/encoding/testdata/vectors.json");

// BLAKE3's official known answers (BLAKE3-team/BLAKE3 test_vectors.json, hash mode): the
// input of each length is the repeating sequence 0, 1, …, 250.
const BLAKE3_KNOWN_ANSWERS: &[(usize, &str)] = &[
    (
        0,
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
    ),
    (
        1,
        "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213",
    ),
    (
        1023,
        "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11",
    ),
    (
        1024,
        "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7",
    ),
    (
        1025,
        "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444",
    ),
    (
        3072,
        "b98cb0ff3623be03326b373de6b9095218513e64f1ee2edd2525c7ad1e5cffd2",
    ),
    (
        102400,
        "bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085",
    ),
];

fn known_answer_input(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

#[test]
fn hash_is_blake3() {
    for &(n, want) in BLAKE3_KNOWN_ANSWERS {
        assert_eq!(encode_hex(&hash(&known_answer_input(n))), want, "{n} bytes");
    }
}

#[test]
fn hasher_matches_hash() {
    for n in [0, 1, 1023, 1024, 1025, 128 << 10, (1 << 20) + 7] {
        let contents = known_answer_input(n);
        let want = hash(&encode_file(&contents));
        for piece in [1, 7, 1024, 64 << 10] {
            let mut h = Hasher::new(Kind::File);
            for chunk in contents.chunks(piece) {
                h.update(chunk);
            }
            assert_eq!(h.finalize(), want, "{n} bytes in {piece}-byte pieces");
        }
    }
}

#[test]
fn empty_tree_id_is_the_empty_tree() {
    assert_eq!(
        hash(&encode_tree(&Tree::default()).unwrap()),
        empty_tree_id()
    );
}

fn id32(b: u8) -> Vec<u8> {
    vec![b; ID_LEN]
}

fn sample_commit() -> Commit {
    let sig = Signature {
        name: "a".into(),
        email: "e".into(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(0),
            tz_offset: 0,
        },
    };
    Commit {
        parents: vec![CommitId::new(id32(0xa1))],
        predecessors: vec![],
        root_tree: Merge::resolved(TreeId::new(empty_tree_id().to_vec())),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::new(vec![0xc0; CHANGE_ID_LEN]),
        description: "d\n".into(),
        author: sig.clone(),
        committer: sig,
        secure_sig: None,
    }
}

fn file_value(copy_id: Vec<u8>) -> TreeValue {
    TreeValue::File {
        id: FileId::new(id32(0xf1)),
        executable: false,
        copy_id: CopyId::new(copy_id),
    }
}

fn tree_of(names: &[&str], value: TreeValue) -> Tree {
    Tree::from_sorted_entries(
        names
            .iter()
            .map(|n| (RepoPathComponentBuf::new(*n).unwrap(), value.clone()))
            .collect(),
    )
}

#[test]
fn encode_rejects() {
    let code = |r: std::result::Result<Vec<u8>, Error>| r.unwrap_err().code;

    let mut c = sample_commit();
    c.parents = vec![CommitId::new(vec![1; 20])];
    assert_eq!(code(encode_commit(&c)), Code::IdLength);

    let mut c = sample_commit();
    c.change_id = ChangeId::new(vec![1; 32]);
    assert_eq!(code(encode_commit(&c)), Code::IdLength);

    let mut c = sample_commit();
    c.secure_sig = Some(SecureSig {
        data: b"not the signed data".to_vec(),
        sig: b"sig".to_vec(),
    });
    assert_eq!(code(encode_commit(&c)), Code::SignedData);

    for name in [".", "..", "a\0b"] {
        assert_eq!(
            code(encode_tree(&tree_of(&[name], file_value(vec![])))),
            Code::Name,
            "{name:?}"
        );
    }
    assert_eq!(
        code(encode_tree(&tree_of(&["f"], file_value(vec![1; 16])))),
        Code::CopyId
    );
    let submodule = TreeValue::GitSubmodule(CommitId::new(id32(1)));
    assert_eq!(code(encode_tree(&tree_of(&["m"], submodule))), Code::Tag);

    for path in ["a/../b", "a/./b", "a/b\0"] {
        let c = CopyHistory {
            current_path: RepoPathBuf::from_internal_string(path).unwrap(),
            parents: vec![],
            salt: vec![],
        };
        assert_eq!(code(encode_copy_history(&c)), Code::Path, "{path:?}");
    }
}

#[test]
fn signed_commit_round_trips() {
    let mut c = sample_commit();
    let data = signed_data(&c).unwrap();
    c.secure_sig = Some(SecureSig {
        data: data.clone(),
        sig: b"sig".to_vec(),
    });
    let encoded = encode_commit(&c).unwrap();
    assert_eq!(decode(&encoded).unwrap(), Object::Commit(c));
    assert_eq!(&encoded[..data.len() - 1], &data[..data.len() - 1]);
}

// --- golden vectors -----------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorFile {
    #[allow(dead_code)]
    comment: Vec<String>,
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidVector {
    name: String,
    object: JObject,
    #[serde(default)]
    bytes: Option<Vec<String>>,
    #[serde(default)]
    signed_data: Option<Vec<String>>,
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InvalidVector {
    name: String,
    bytes: Vec<String>,
    error: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum JObject {
    File(JContents),
    Chunk(JContents),
    Symlink {
        target: String,
    },
    Tree {
        entries: Vec<JEntry>,
    },
    CopyHistory {
        current_path: String,
        parents: Vec<String>,
        salt: String,
    },
    Commit {
        parents: Vec<String>,
        predecessors: Vec<String>,
        root_tree: Vec<String>,
        conflict_labels: Vec<String>,
        change_id: String,
        description: String,
        author: JSignature,
        committer: JSignature,
        secure_sig: Option<String>,
    },
    Operation {
        view_id: String,
        parents: Vec<String>,
        metadata: JMetadata,
        commit_predecessors: Option<BTreeMap<String, Vec<String>>>,
    },
    View {
        head_ids: Vec<String>,
        local_bookmarks: BTreeMap<String, Vec<Option<String>>>,
        local_tags: BTreeMap<String, Vec<Option<String>>>,
        remote_views: BTreeMap<String, JRemoteView>,
        git_refs: BTreeMap<String, Vec<Option<String>>>,
        git_heads: BTreeMap<String, Vec<Option<String>>>,
        wc_commit_ids: BTreeMap<String, String>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JContents {
    #[serde(default)]
    contents: Option<String>,
    #[serde(default)]
    contents_repeat: Option<JRepeat>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JRepeat {
    hex: String,
    count: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JEntry {
    name: String,
    value: JTreeValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum JTreeValue {
    File {
        id: String,
        executable: bool,
        copy_id: String,
    },
    Symlink(String),
    Tree(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JTimestamp {
    millis: i64,
    tz_offset: i32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JSignature {
    name: String,
    email: String,
    time: JTimestamp,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JMetadata {
    start: JTimestamp,
    end: JTimestamp,
    description: String,
    hostname: String,
    username: String,
    is_snapshot: bool,
    workspace_name: Option<String>,
    attributes: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JRemoteView {
    bookmarks: BTreeMap<String, JRemoteRef>,
    tags: BTreeMap<String, JRemoteRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JRemoteRef {
    target: Vec<Option<String>>,
    state: String,
}

fn unhex(s: &str) -> Vec<u8> {
    decode_hex(s).unwrap_or_else(|| panic!("bad hex {s:?}"))
}

/// A byte string written as lines of hex, each optionally followed by a '#' comment.
fn hex_lines(lines: &[String]) -> Vec<u8> {
    lines
        .iter()
        .flat_map(|line| {
            let hex: String = line.split('#').next().unwrap().split_whitespace().collect();
            unhex(&hex)
        })
        .collect()
}

fn ids<T>(hexes: &[String], new: fn(Vec<u8>) -> T) -> Vec<T> {
    hexes.iter().map(|h| new(unhex(h))).collect()
}

fn timestamp(t: &JTimestamp) -> Timestamp {
    Timestamp {
        timestamp: MillisSinceEpoch(t.millis),
        tz_offset: t.tz_offset,
    }
}

fn signature(s: &JSignature) -> Signature {
    Signature {
        name: s.name.clone(),
        email: s.email.clone(),
        timestamp: timestamp(&s.time),
    }
}

fn ref_target(terms: &[Option<String>]) -> RefTarget {
    RefTarget::from_merge(Merge::from_vec(
        terms
            .iter()
            .map(|t| t.as_deref().map(|h| CommitId::new(unhex(h))))
            .collect::<Vec<_>>(),
    ))
}

fn ref_targets<K: From<String> + Ord>(
    m: &BTreeMap<String, Vec<Option<String>>>,
) -> BTreeMap<K, RefTarget> {
    m.iter()
        .map(|(k, v)| (K::from(k.clone()), ref_target(v)))
        .collect()
}

fn remote_refs(m: &BTreeMap<String, JRemoteRef>) -> BTreeMap<RefNameBuf, RemoteRef> {
    m.iter()
        .map(|(k, r)| {
            let state = match r.state.as_str() {
                "new" => RemoteRefState::New,
                "tracked" => RemoteRefState::Tracked,
                s => panic!("unknown remote ref state {s:?}"),
            };
            (
                RefNameBuf::from(k.clone()),
                RemoteRef {
                    target: ref_target(&r.target),
                    state,
                },
            )
        })
        .collect()
}

fn contents(c: &JContents) -> Vec<u8> {
    match (&c.contents, &c.contents_repeat) {
        (Some(h), None) => unhex(h),
        (None, Some(r)) => unhex(&r.hex).repeat(r.count),
        _ => panic!("a file or chunk needs exactly one of contents and contents_repeat"),
    }
}

/// Builds the object a vector describes. A signed commit's `secure_sig.data` is the
/// vector's signed data.
fn object(o: &JObject, signed: Option<&[u8]>) -> Object {
    match o {
        JObject::File(c) => Object::File(contents(c)),
        JObject::Chunk(c) => Object::Chunk(contents(c)),
        JObject::Symlink { target } => Object::Symlink(target.clone()),
        JObject::Tree { entries } => Object::Tree(Tree::from_sorted_entries(
            entries
                .iter()
                .map(|e| {
                    let value = match &e.value {
                        JTreeValue::File {
                            id,
                            executable,
                            copy_id,
                        } => TreeValue::File {
                            id: FileId::new(unhex(id)),
                            executable: *executable,
                            copy_id: CopyId::new(unhex(copy_id)),
                        },
                        JTreeValue::Symlink(id) => TreeValue::Symlink(SymlinkId::new(unhex(id))),
                        JTreeValue::Tree(id) => TreeValue::Tree(TreeId::new(unhex(id))),
                    };
                    (RepoPathComponentBuf::new(e.name.clone()).unwrap(), value)
                })
                .collect(),
        )),
        JObject::CopyHistory {
            current_path,
            parents,
            salt,
        } => Object::CopyHistory(CopyHistory {
            current_path: RepoPathBuf::from_internal_string(current_path.clone()).unwrap(),
            parents: ids(parents, CopyId::new),
            salt: unhex(salt),
        }),
        JObject::Commit {
            parents,
            predecessors,
            root_tree,
            conflict_labels,
            change_id,
            description,
            author,
            committer,
            secure_sig,
        } => Object::Commit(Commit {
            parents: ids(parents, CommitId::new),
            predecessors: ids(predecessors, CommitId::new),
            root_tree: Merge::from_vec(ids(root_tree, TreeId::new)),
            conflict_labels: Merge::from_vec(conflict_labels.clone()),
            change_id: ChangeId::new(unhex(change_id)),
            description: description.clone(),
            author: signature(author),
            committer: signature(committer),
            secure_sig: secure_sig.as_ref().map(|sig| SecureSig {
                data: signed
                    .expect("a signed commit vector lists its signed_data")
                    .to_vec(),
                sig: unhex(sig),
            }),
        }),
        JObject::Operation {
            view_id,
            parents,
            metadata: m,
            commit_predecessors,
        } => Object::Operation(Operation {
            view_id: ViewId::new(unhex(view_id)),
            parents: ids(parents, OperationId::new),
            metadata: OperationMetadata {
                time: TimestampRange {
                    start: timestamp(&m.start),
                    end: timestamp(&m.end),
                },
                description: m.description.clone(),
                hostname: m.hostname.clone(),
                username: m.username.clone(),
                is_snapshot: m.is_snapshot,
                workspace_name: m.workspace_name.clone().map(WorkspaceNameBuf::from),
                attributes: m.attributes.clone(),
            },
            commit_predecessors: commit_predecessors.as_ref().map(|cp| {
                cp.iter()
                    .map(|(k, v)| (CommitId::new(unhex(k)), ids(v, CommitId::new)))
                    .collect()
            }),
        }),
        JObject::View {
            head_ids,
            local_bookmarks,
            local_tags,
            remote_views,
            git_refs,
            git_heads,
            wc_commit_ids,
        } => Object::View(View {
            head_ids: ids(head_ids, CommitId::new)
                .into_iter()
                .collect::<HashSet<_>>(),
            local_bookmarks: ref_targets::<RefNameBuf>(local_bookmarks),
            local_tags: ref_targets::<RefNameBuf>(local_tags),
            remote_views: remote_views
                .iter()
                .map(|(k, rv)| {
                    (
                        RemoteNameBuf::from(k.clone()),
                        RemoteView {
                            bookmarks: remote_refs(&rv.bookmarks),
                            tags: remote_refs(&rv.tags),
                        },
                    )
                })
                .collect(),
            git_refs: ref_targets::<GitRefNameBuf>(git_refs),
            git_heads: ref_targets::<WorkspaceNameBuf>(git_heads),
            wc_commit_ids: wc_commit_ids
                .iter()
                .map(|(k, v)| (WorkspaceNameBuf::from(k.clone()), CommitId::new(unhex(v))))
                .collect(),
        }),
    }
}

fn vectors() -> VectorFile {
    let vf: VectorFile = serde_json::from_str(VECTORS).expect("parsing vectors.json");
    assert!(!vf.valid.is_empty() && !vf.invalid.is_empty());
    vf
}

/// The canonical bytes a valid vector lists (or implies, for a repeated file or chunk).
fn vector_bytes(v: &ValidVector, obj: &Object) -> Vec<u8> {
    match (&v.bytes, obj) {
        (Some(lines), _) => hex_lines(lines),
        (None, Object::File(c)) => encode_file(c),
        (None, Object::Chunk(c)) => encode_chunk(c),
        (None, _) => panic!("{}: only a file or chunk may omit its bytes", v.name),
    }
}

#[test]
fn valid_vectors() {
    for v in vectors().valid {
        let signed = v.signed_data.as_deref().map(hex_lines);
        let obj = object(&v.object, signed.as_deref());
        let want = vector_bytes(&v, &obj);

        let got = encode(&obj).unwrap_or_else(|e| panic!("{}: encode: {e}", v.name));
        assert!(
            got == want,
            "{}: encode\n got {}\nwant {}",
            v.name,
            encode_hex(&got),
            encode_hex(&want)
        );
        assert_eq!(encode_hex(&hash(&got)), v.id, "{}: ID", v.name);

        let back = decode(&want).unwrap_or_else(|e| panic!("{}: decode: {e}", v.name));
        assert!(
            back == obj,
            "{}: decode produced a different object:\n{back:?}\nwant\n{obj:?}",
            v.name
        );
        assert_eq!(
            encode(&back).unwrap(),
            want,
            "{}: encode(decode(b)) != b",
            v.name
        );

        if let Object::Commit(c) = &obj
            && let Some(signed) = &signed
        {
            assert_eq!(&signed_data(c).unwrap(), signed, "{}: signed data", v.name);
        }
    }
}

#[test]
fn invalid_vectors() {
    for v in vectors().invalid {
        let b = hex_lines(&v.bytes);
        match decode(&b) {
            Ok(obj) => panic!(
                "{}: decode accepted {} as a {}",
                v.name,
                encode_hex(&b),
                obj.kind().name()
            ),
            Err(e) => assert_eq!(e.code.as_str(), v.error, "{}: {e}", v.name),
        }
    }
}

/// Every one-byte change, truncation, and extension of each valid vector's bytes must
/// either fail to decode or decode to an object that re-encodes to exactly the input.
#[test]
fn mutated_vectors_stay_canonical() {
    let check = |name: &str, b: &[u8]| {
        if let Ok(obj) = decode(b) {
            let again = encode(&obj)
                .unwrap_or_else(|e| panic!("{name}: decoded object does not encode: {e}"));
            assert!(
                again == b,
                "{name}: decode accepted non-canonical {}",
                encode_hex(b)
            );
        }
    };
    for v in vectors().valid {
        let Some(lines) = &v.bytes else { continue };
        let orig = hex_lines(lines);
        for i in 0..orig.len() {
            check(&v.name, &orig[..i]);
            for delta in [1u8, 0x7f, 0x80, 0xff] {
                let mut b = orig.clone();
                b[i] = b[i].wrapping_add(delta);
                check(&v.name, &b);
            }
        }
        let mut longer = orig.clone();
        longer.push(0);
        check(&v.name, &longer);
    }
}

// --- tree sizes ---------------------------------------------------------------------

const TREE_SIZES_VECTORS: &str =
    include_str!("../../internal/scm/encoding/testdata/tree_sizes.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TreeSizesVectors {
    #[allow(dead_code)]
    comment: Vec<String>,
    valid: Vec<TreeSizesValid>,
    invalid: Vec<InvalidVector>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TreeSizesValid {
    name: String,
    sizes: Vec<String>,
    bytes: Vec<String>,
}

fn tree_sizes_vectors() -> TreeSizesVectors {
    let v: TreeSizesVectors =
        serde_json::from_str(TREE_SIZES_VECTORS).expect("parsing tree_sizes.json");
    assert!(!v.valid.is_empty() && !v.invalid.is_empty());
    v
}

#[test]
fn tree_sizes_valid_vectors() {
    for v in tree_sizes_vectors().valid {
        let want = hex_lines(&v.bytes);
        let sizes: Vec<u64> = v.sizes.iter().map(|s| s.parse().unwrap()).collect();
        let s = TreeSizes { sizes };
        assert_eq!(encode_tree_sizes(&s), want, "{}: encoding", v.name);
        assert_eq!(decode_tree_sizes(&want), Ok(s), "{}: decoding", v.name);
    }
}

#[test]
fn tree_sizes_invalid_vectors() {
    for v in tree_sizes_vectors().invalid {
        let err = decode_tree_sizes(&hex_lines(&v.bytes)).expect_err(&v.name);
        assert_eq!(err.code.as_str(), v.error, "{}: {err}", v.name);
    }
}

#[test]
fn tree_sizes_are_no_object() {
    let b = encode_tree_sizes(&TreeSizes { sizes: vec![1] });
    assert_eq!(decode(&b).unwrap_err().code, Code::Kind);
}

// --- deltas -------------------------------------------------------------------------------

const DELTA_VECTORS: &str = include_str!("../../internal/scm/encoding/testdata/delta.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeltaVectors {
    #[allow(dead_code)]
    comment: Vec<String>,
    valid: Vec<DeltaValid>,
    invalid: Vec<DeltaInvalid>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeltaValid {
    name: String,
    base: Vec<String>,
    delta: Vec<String>,
    result: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeltaInvalid {
    name: String,
    base: Vec<String>,
    delta: Vec<String>,
    limit: String,
}

#[test]
fn delta_vectors() {
    let v: DeltaVectors = serde_json::from_str(DELTA_VECTORS).expect("parsing delta.json");
    assert!(!v.valid.is_empty() && !v.invalid.is_empty());
    for c in v.valid {
        let result = hex_lines(&c.result);
        let got = apply_delta(
            &hex_lines(&c.base),
            &hex_lines(&c.delta),
            result.len() as u64,
        );
        assert_eq!(got, Ok(result), "{}", c.name);
    }
    for c in v.invalid {
        let limit: u64 = c.limit.parse().unwrap();
        let got = apply_delta(&hex_lines(&c.base), &hex_lines(&c.delta), limit);
        assert!(got.is_err(), "{}: applied, to {got:?}", c.name);
    }
}

// --- checkout layers ----------------------------------------------------------------------

const LAYER_VECTORS: &str = include_str!("../../internal/scm/encoding/testdata/layer.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerVectors {
    #[allow(dead_code)]
    comment: Vec<String>,
    valid: Vec<LayerValid>,
    invalid: Vec<LayerInvalid>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerValid {
    name: String,
    records: Vec<LayerJRecord>,
    bytes: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerJRecord {
    kind: String,
    id: String,
    body: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerInvalid {
    name: String,
    bytes: Vec<String>,
}

#[test]
fn layer_vectors() {
    let v: LayerVectors = serde_json::from_str(LAYER_VECTORS).expect("parsing layer.json");
    assert!(!v.valid.is_empty() && !v.invalid.is_empty());
    for c in v.valid {
        let want: Vec<LayerRecord> = c
            .records
            .iter()
            .map(|r| LayerRecord {
                kind: unhex(&r.kind)[0],
                id: unhex(&r.id).try_into().unwrap(),
                body: hex_lines(&r.body),
            })
            .collect();
        assert_eq!(
            decode_layer_records(&hex_lines(&c.bytes)),
            Ok(want),
            "{}",
            c.name
        );
    }
    for c in v.invalid {
        let got = decode_layer_records(&hex_lines(&c.bytes));
        assert!(got.is_err(), "{}: decoded {got:?}", c.name);
    }
}
