//! Unit tests of what needs no SCM: file manifests, the cache, the outbox, references,
//! chunking, publish ordering and rejections, and read coalescing. The stores against a
//! real SCM are `tests/e2e.rs`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::future::Future;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use jj_lib::backend::ChangeId;
use jj_lib::backend::Commit;
use jj_lib::backend::CommitId;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::MillisSinceEpoch;
use jj_lib::backend::Signature;
use jj_lib::backend::SymlinkId;
use jj_lib::backend::Timestamp;
use jj_lib::backend::Tree;
use jj_lib::backend::TreeId;
use jj_lib::backend::TreeValue;
use jj_lib::merge::Merge;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RefTarget;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use jj_lib::ref_name::RefNameBuf;
use jj_lib::repo_path::RepoPathComponentBuf;
use ok_encoding::Kind;
use ok_proto::ok::scm::v1 as pb;
use prost::Message as _;
use tonic::Code;
use tonic::Status;

use crate::batch::Batcher;
use crate::cache::Cache;
use crate::cache::Manifest;
use crate::cache::chunk_id;
use crate::chunk;
use crate::config::RepoConfig;
use crate::ids::Id;
use crate::ids::Key;
use crate::ids::ROOT_ID;
use crate::outbox::Staged;
use crate::publish;
use crate::refs::references;
use crate::shared::Shared;

fn block_on<F: Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

/// Deterministic bytes that no chunker can find long runs in.
fn noise(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// A repository directory whose SCM is never dialed: none of these tests reach it.
fn repo() -> (tempfile::TempDir, Arc<Shared>) {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    fs::create_dir(&store).unwrap();
    RepoConfig {
        endpoint: "http://127.0.0.1:9".into(),
        repository_id: 1,
        repository: "unit".into(),
        dev_principal: None,
        cache: Some(dir.path().join("cache")),
        workspace: None,
    }
    .write(&store)
    .unwrap();
    let shared = Shared::for_store_path(&store).unwrap();
    (dir, shared)
}

fn id(b: u8) -> Id {
    [b; 32]
}

fn tree_object(entries: Vec<(&str, TreeValue)>) -> (Key, Vec<u8>) {
    let tree = Tree::from_sorted_entries(
        entries
            .into_iter()
            .map(|(n, v)| (RepoPathComponentBuf::new(n).unwrap(), v))
            .collect(),
    );
    let canonical = ok_encoding::encode_tree(&tree).unwrap();
    (
        Key::new(Kind::Tree, ok_encoding::hash(&canonical)),
        canonical,
    )
}

fn commit(parents: Vec<Id>, predecessors: Vec<Id>, trees: Vec<Id>) -> Commit {
    let sig = Signature {
        name: "a".into(),
        email: "a@example.com".into(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(0),
            tz_offset: 0,
        },
    };
    let root_tree = if trees.len() == 1 {
        Merge::resolved(TreeId::new(trees[0].to_vec()))
    } else {
        Merge::from_vec(
            trees
                .iter()
                .map(|t| TreeId::new(t.to_vec()))
                .collect::<Vec<_>>(),
        )
    };
    Commit {
        parents: parents.iter().map(|p| CommitId::new(p.to_vec())).collect(),
        predecessors: predecessors
            .iter()
            .map(|p| CommitId::new(p.to_vec()))
            .collect(),
        conflict_labels: Merge::from_vec(vec![String::new(); root_tree.as_slice().len()]),
        root_tree,
        change_id: ChangeId::new(vec![0xc0; ok_encoding::CHANGE_ID_LEN]),
        description: "d\n".into(),
        author: sig.clone(),
        committer: sig,
        secure_sig: None,
    }
}

fn commit_object(c: &Commit) -> (Key, Vec<u8>) {
    let canonical = ok_encoding::encode_commit(c).unwrap();
    (
        Key::new(Kind::Commit, ok_encoding::hash(&canonical)),
        canonical,
    )
}

// ─── Manifests ───

#[test]
fn manifests_round_trip_and_reject_damage() {
    let m = Manifest {
        id: id(1),
        size: 30,
        chunks: vec![(id(2), 10), (id(3), 20)],
        path_hint: "src/lib.rs".into(),
    };
    let b = m.encode();
    assert_eq!(Manifest::decode(&b), Some(m.clone()));
    let mut trailing = b.clone();
    trailing.push(0);
    assert_eq!(Manifest::decode(&trailing), None, "trailing bytes");
    assert_eq!(Manifest::decode(&b[..b.len() - 1]), None, "truncated");
    let wrong_size = Manifest { size: 31, ..m };
    assert_eq!(
        Manifest::decode(&wrong_size.encode()),
        None,
        "size is not the sum of the chunks"
    );
}

// ─── The cache ───

/// The cache's segment files, for tests that damage them.
fn segment_files(cache_dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = fs::read_dir(cache_dir.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with('s') && n[1..].parse::<u32>().is_ok())
        })
        .collect();
    out.sort();
    out
}

/// Flips the first byte of `needle` in the cache's segments.
fn damage(cache_dir: &Path, needle: &[u8]) {
    for p in segment_files(cache_dir) {
        let mut b = fs::read(&p).unwrap();
        if let Some(at) = b.windows(needle.len()).position(|w| w == needle) {
            b[at] ^= 1;
            fs::write(&p, b).unwrap();
            return;
        }
    }
    panic!("{needle:?} is in no segment");
}

#[test]
fn the_cache_verifies_what_it_stores_and_reads() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let cache = Cache::open(cache_dir.clone()).unwrap();
    let canonical = ok_encoding::encode_symlink("a symlink target");
    let key = Key::new(Kind::Symlink, ok_encoding::hash(&canonical));

    assert!(
        cache
            .put_object(Key::new(Kind::Symlink, id(9)), &canonical)
            .is_err(),
        "bytes that do not hash to the key"
    );
    assert!(
        cache
            .put_object(Key::new(Kind::Tree, key.id), &canonical)
            .is_err(),
        "bytes of another kind"
    );
    cache.put_object(key, &canonical).unwrap();
    assert!(cache.put_chunk(&id(4), b"abc").is_err());
    let raw = b"a chunk of contents".to_vec();
    cache.put_chunk(&chunk_id(&raw), &raw).unwrap();
    drop(cache);

    // A damaged entry is never returned: what reads it checks it against the ID it asked for.
    damage(&cache_dir, b"a symlink target");
    damage(&cache_dir, b"a chunk of");
    let cache = Cache::open(cache_dir).unwrap();
    assert_eq!(cache.object(key), None);
    assert_eq!(cache.chunk(&chunk_id(&raw)), None);
    // Fetched again, it is kept again, and the newer copy is what reads find.
    cache.put_object(key, &canonical).unwrap();
    assert_eq!(cache.object(key), Some(canonical));
}

#[test]
fn a_cached_file_needs_every_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(dir.path().to_owned()).unwrap();
    let (a, b) = (b"first".to_vec(), b"second".to_vec());
    let m = Manifest {
        id: id(7),
        size: 11,
        chunks: vec![(chunk_id(&a), 5), (chunk_id(&b), 6)],
        path_hint: String::new(),
    };
    cache.put_chunk(&chunk_id(&a), &a).unwrap();
    cache.put_file(&m).unwrap();
    assert_eq!(cache.file(&m.id), None, "a chunk is missing");
    assert!(!cache.has_file(&m.id));
    assert_eq!(
        cache.manifest(&m.id),
        Some(m.clone()),
        "the manifest is, without its chunks"
    );
    cache.put_chunk(&chunk_id(&b), &b).unwrap();
    assert_eq!(cache.file(&m.id), Some(m.clone()));
    assert!(cache.has_file(&m.id) && cache.knows_size(&m.id));
}

// A file of one chunk is kept whole, with no manifest: found by its file ID and by its chunk's,
// and kept as ok sent it — here zstd-compressed — and decoded on read.
#[test]
fn a_file_of_one_chunk_is_kept_whole_and_as_sent() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    let raw = b"ok's stored chunk\n".repeat(200);
    let file = crate::cache::file_id(&raw);
    let chunk = chunk_id(&raw);
    assert_eq!(raw.len(), 3600);
    cache
        .put_whole_stored(
            &file,
            &chunk,
            &raw,
            &crate::cache::Stored {
                codec: crate::segments::Codec::Zstd,
                dict: 0,
                data: STORED_CHUNK_ZSTD.to_vec(),
            },
        )
        .unwrap();
    assert!(
        cache.bytes() < 200,
        "kept compressed: {} bytes",
        cache.bytes()
    );
    drop(cache);
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    assert_eq!(cache.whole(&file), Some(raw.clone()));
    assert_eq!(cache.chunk(&chunk), Some(raw.clone()));
    assert_eq!(
        cache.file(&file),
        Some(Manifest {
            id: file,
            size: raw.len() as u64,
            chunks: vec![(chunk, raw.len() as u32)],
            path_hint: String::new(),
        })
    );
    assert!(cache.has_chunk(&chunk) && cache.has_file(&file) && cache.knows_size(&file));
    // A manifest of one chunk whose chunk is cached becomes the file, whole.
    let other = noise(3000, 9);
    let (ofile, ochunk) = (crate::cache::file_id(&other), chunk_id(&other));
    cache.put_chunk(&ochunk, &other).unwrap();
    cache
        .put_file(&Manifest {
            id: ofile,
            size: 3000,
            chunks: vec![(ochunk, 3000)],
            path_hint: String::new(),
        })
        .unwrap();
    assert_eq!(cache.whole(&ofile), Some(other));
}

// A tree's sizes are kept with it, or alone for a tree cached without them.
#[test]
fn a_trees_sizes_are_kept_with_it_or_alone() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    let (key, canonical) = tree_object(vec![(
        "f",
        TreeValue::Symlink(SymlinkId::new(id(5).to_vec())),
    )]);
    let sizes = ok_encoding::encode_tree_sizes(&ok_encoding::TreeSizes { sizes: vec![7] });
    assert!(
        cache
            .put_tree_with_sizes(&key.id, &canonical, b"not a record")
            .is_err()
    );
    cache
        .put_tree_with_sizes(&key.id, &canonical, &sizes)
        .unwrap();
    assert_eq!(cache.object(key), Some(canonical));
    assert_eq!(cache.tree_sizes(&key.id).unwrap().sizes, vec![7]);
    let alone = id(11);
    assert!(cache.tree_sizes(&alone).is_none());
    cache.put_tree_sizes(&alone, &sizes).unwrap();
    assert_eq!(cache.tree_sizes(&alone).unwrap().sizes, vec![7]);
}

// The cache is kept under its budget a segment at a time: what was read since it was written, and
// what an outbox holds or references, are copied forward; the rest leaves.
#[test]
fn retirement_keeps_what_is_read_and_what_an_outbox_pins() {
    let dir = tempfile::tempdir().unwrap();
    let seg = 16 << 10;
    let cache = Cache::open_with(
        dir.path().join("cache"),
        crate::segments::Options {
            segment_bytes: seg,
            budget: 4 * seg,
        },
    )
    .unwrap();
    let outbox =
        crate::outbox::Outbox::open(dir.path().join("outbox"), dir.path().join("q")).unwrap();
    cache
        .register_outbox(&dunce::canonicalize(dir.path().join("outbox")).unwrap())
        .unwrap();
    let chunks: Vec<Vec<u8>> = (0..300u64).map(|i| noise(1000, i)).collect();
    let ids: Vec<Id> = chunks.iter().map(|c| chunk_id(c)).collect();
    // Chunk 0 is referenced by a staged file.
    outbox
        .stage_file(&Manifest {
            id: id(100),
            size: 1000,
            chunks: vec![(ids[0], 1000)],
            path_hint: String::new(),
        })
        .unwrap();
    for (i, (c, cid)) in chunks.iter().zip(&ids).enumerate() {
        cache.put_chunk(cid, c).unwrap();
        if i >= 5 && i % 5 == 0 {
            assert!(
                cache.chunk(&ids[5]).is_some(),
                "chunk 5, read every so often, left at {i}"
            );
        }
    }
    assert!(
        cache.bytes() <= 6 * seg,
        "{} bytes, budget {}",
        cache.bytes(),
        4 * seg
    );
    drop(cache);
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    assert!(cache.has_chunk(&ids[0]), "pinned by the outbox");
    assert!(cache.has_chunk(&ids[5]), "read every so often");
    assert!(!cache.has_chunk(&ids[1]), "never read: gone");
    assert!(cache.has_chunk(&ids[299]), "newest: still there");
}

// One cache directory is one cache in a process, and a cache from before segments is cleared.
#[test]
fn a_cache_is_opened_once_and_its_old_files_go() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    for sub in ["objects/03/ab", "chunks/cd", "files/ef", "sizes/01"] {
        fs::create_dir_all(cache_dir.join(sub)).unwrap();
        fs::write(cache_dir.join(sub).join("entry"), b"old").unwrap();
    }
    let a = Cache::open(cache_dir.clone()).unwrap();
    let b = Cache::open(cache_dir.clone()).unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    for sub in ["objects", "chunks", "files", "sizes"] {
        assert!(!cache_dir.join(sub).exists(), "{sub} moved aside");
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fs::read_dir(&cache_dir).unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("legacy-")
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "the old files were not removed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ─── The outbox ───

#[test]
fn retiring_moves_into_the_cache_and_keeps_chunks_still_needed() {
    let (_dir, shared) = repo();
    let (shared_raw, only_raw, other_raw) = (noise(100, 1), noise(100, 2), noise(100, 3));
    for raw in [&shared_raw, &only_raw, &other_raw] {
        shared.outbox.stage_chunk(&chunk_id(raw), raw).unwrap();
    }
    shared.outbox.sync_chunks().unwrap();
    let published = Manifest {
        id: id(1),
        size: 200,
        chunks: vec![(chunk_id(&shared_raw), 100), (chunk_id(&only_raw), 100)],
        path_hint: "a".into(),
    };
    let pending = Manifest {
        id: id(2),
        size: 200,
        chunks: vec![(chunk_id(&shared_raw), 100), (chunk_id(&other_raw), 100)],
        path_hint: "b".into(),
    };
    shared.outbox.stage_file(&published).unwrap();
    shared.outbox.stage_file(&pending).unwrap();
    let (key, canonical) = tree_object(vec![(
        "f",
        TreeValue::Symlink(SymlinkId::new(id(5).to_vec())),
    )]);
    shared.outbox.stage_object(key, &canonical).unwrap();

    shared
        .outbox
        .retire(
            &Staged {
                objects: vec![key],
                files: vec![published.clone()],
            },
            &shared.cache,
        )
        .unwrap();

    let staged = shared.outbox.list().unwrap();
    assert!(staged.objects.is_empty());
    assert_eq!(staged.files, vec![pending]);
    assert!(
        shared.outbox.chunk(&chunk_id(&shared_raw)).is_some(),
        "the pending file still needs it"
    );
    assert!(shared.outbox.chunk(&chunk_id(&only_raw)).is_none());
    assert_eq!(shared.cache.object(key), Some(canonical));
    assert_eq!(shared.cache.file(&published.id), Some(published));
}

// A published file of one chunk goes into the cache whole, and its chunk leaves the outbox.
#[test]
fn a_retired_file_of_one_chunk_is_cached_whole() {
    let (_dir, shared) = repo();
    let raw = noise(5000, 4);
    let (file, chunk) = (crate::cache::file_id(&raw), chunk_id(&raw));
    shared.outbox.stage_chunk(&chunk, &raw).unwrap();
    shared.outbox.sync_chunks().unwrap();
    let m = Manifest {
        id: file,
        size: 5000,
        chunks: vec![(chunk, 5000)],
        path_hint: "a".into(),
    };
    shared.outbox.stage_file(&m).unwrap();
    shared
        .outbox
        .retire(
            &Staged {
                objects: vec![],
                files: vec![m],
            },
            &shared.cache,
        )
        .unwrap();
    assert!(shared.outbox.chunk(&chunk).is_none());
    assert_eq!(shared.cache.whole(&file), Some(raw.clone()));
    assert_eq!(shared.chunk(&chunk), Some(raw));
}

#[test]
fn the_outbox_pins_what_it_holds_and_references() {
    let (_dir, shared) = repo();
    let (key, canonical) = tree_object(vec![(
        "f",
        TreeValue::File {
            id: FileId::new(id(6).to_vec()),
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    )]);
    shared.outbox.stage_object(key, &canonical).unwrap();
    let m = Manifest {
        id: id(8),
        size: 3,
        chunks: vec![(chunk_id(b"abc"), 3)],
        path_hint: String::new(),
    };
    shared.outbox.stage_file(&m).unwrap();
    let pinned = shared.outbox.pinned();
    for want in [key.id, id(6), id(8), chunk_id(b"abc")] {
        assert!(pinned.contains(&want), "{}", hex::encode(want));
    }
}

#[test]
fn quarantine_moves_an_object_aside() {
    let (dir, shared) = repo();
    let op = Operation::make_root(ViewId::new(ROOT_ID.to_vec()));
    let canonical = ok_encoding::encode_operation(&op).unwrap();
    let key = Key::new(Kind::Operation, ok_encoding::hash(&canonical));
    shared.outbox.stage_object(key, &canonical).unwrap();
    assert_eq!(shared.outbox.operations(), vec![key.id]);
    let to = shared.outbox.quarantine(key).unwrap();
    // The store names its directory canonically: on macOS a temporary directory is under /var, a
    // link to /private/var.
    let root = dunce::canonicalize(dir.path()).unwrap();
    assert!(
        to.starts_with(root.join("store/quarantine")),
        "{}",
        to.display()
    );
    assert_eq!(fs::read(&to).unwrap(), canonical);
    assert!(shared.outbox.list().unwrap().is_empty());
}

// The outbox is one log: entries others append are read on, a torn tail is cut off, retiring
// everything starts it over, and an outbox of a file per entry is carried over.
#[test]
fn the_outbox_is_a_log_that_survives_crashes_and_carries_the_old_one_over() {
    let dir = tempfile::tempdir().unwrap();
    let ob = dir.path().join("outbox");
    // An outbox from before the log: a file per entry, one of them damaged.
    let raw = noise(300, 3);
    let cid = chunk_id(&raw);
    let (key, canonical) = tree_object(vec![(
        "f",
        TreeValue::Symlink(SymlinkId::new(id(5).to_vec())),
    )]);
    fs::create_dir_all(ob.join("objects")).unwrap();
    fs::create_dir_all(ob.join("chunks")).unwrap();
    fs::create_dir_all(ob.join("files")).unwrap();
    fs::write(
        ob.join("objects")
            .join(format!("{:02x}-{}", key.kind as u8, key.hex())),
        &canonical,
    )
    .unwrap();
    fs::write(ob.join("chunks").join(hex::encode(cid)), &raw).unwrap();
    fs::write(
        ob.join("chunks").join(hex::encode(id(77))),
        b"does not hash to its name",
    )
    .unwrap();
    let m = Manifest {
        id: crate::cache::file_id(&raw),
        size: 300,
        chunks: vec![(cid, 300)],
        path_hint: "p".into(),
    };
    fs::write(ob.join("files").join(hex::encode(m.id)), m.encode()).unwrap();

    let a = crate::outbox::Outbox::open(ob.clone(), dir.path().join("q")).unwrap();
    for sub in ["objects", "chunks", "files"] {
        assert!(!ob.join(sub).exists(), "{sub} carried over and removed");
    }
    assert_eq!(a.object(key), Some(canonical.clone()));
    assert_eq!(a.chunk(&cid), Some(raw.clone()));
    assert!(
        a.chunk(&id(77)).is_none(),
        "a damaged entry is not carried over"
    );
    assert_eq!(a.file(&m.id), Some(m.clone()));

    // Another process sees what this one appends.
    let b = crate::outbox::Outbox::open(ob.clone(), dir.path().join("q")).unwrap();
    let more = noise(400, 4);
    a.stage_chunk(&chunk_id(&more), &more).unwrap();
    assert_eq!(b.chunk(&chunk_id(&more)), Some(more.clone()));

    // A crash mid-write leaves a torn entry: readers stop before it, the next opener cuts it.
    let log = ob.join("log");
    let whole = fs::metadata(&log).unwrap().len();
    fs::OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(&[0x10, 0, 50, 1, 2, 3])
        .unwrap();
    drop((a, b));
    let a = crate::outbox::Outbox::open(ob.clone(), dir.path().join("q")).unwrap();
    assert_eq!(
        fs::metadata(&log).unwrap().len(),
        whole,
        "the torn tail was cut"
    );
    assert_eq!(a.chunk(&chunk_id(&more)), Some(more.clone()));
    let staged = a.list().unwrap();
    assert_eq!(staged.objects, vec![key]);
    assert_eq!(staged.files, vec![m.clone()]);

    // Retiring everything starts the log over.
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    a.retire(&staged, &cache).unwrap();
    a.retire(
        &crate::outbox::Staged {
            objects: vec![],
            files: vec![Manifest {
                id: crate::cache::file_id(&more),
                size: 400,
                chunks: vec![(chunk_id(&more), 400)],
                path_hint: String::new(),
            }],
        },
        &cache,
    )
    .unwrap();
    assert!(a.list().unwrap().is_empty());
    assert!(a.chunk(&chunk_id(&more)).is_none());
    assert_eq!(
        fs::metadata(&log).unwrap().len(),
        8,
        "only the log's header is left"
    );
    assert_eq!(cache.object(key), Some(canonical));
    assert_eq!(cache.whole(&m.id), Some(raw));
}

// An older jj may still write the old outbox while a newer one carries it over: only what was
// carried is removed, a file in progress is left alone, and what the older one writes later is
// carried by the next open.
#[test]
fn carrying_the_old_outbox_over_loses_nothing_an_older_jj_writes() {
    let dir = tempfile::tempdir().unwrap();
    let ob = dir.path().join("outbox");
    let chunks = ob.join("chunks");
    fs::create_dir_all(&chunks).unwrap();
    let (a, b) = (noise(200, 1), noise(200, 2));
    fs::write(chunks.join(hex::encode(chunk_id(&a))), &a).unwrap();
    fs::write(chunks.join(".tmpwriting"), b"half of a chunk").unwrap();
    fs::write(chunks.join(hex::encode(id(9))), b"never a chunk").unwrap();

    let outbox = crate::outbox::Outbox::open(ob.clone(), dir.path().join("q")).unwrap();
    assert_eq!(outbox.chunk(&chunk_id(&a)), Some(a.clone()));
    assert!(
        !chunks.join(hex::encode(chunk_id(&a))).exists(),
        "carried, then removed"
    );
    assert!(
        !chunks.join(hex::encode(id(9))).exists(),
        "a file that never checks goes"
    );
    assert!(
        chunks.join(".tmpwriting").exists(),
        "a file in progress stays"
    );

    // The older jj finishes its write, and writes another; the next open carries both.
    fs::remove_file(chunks.join(".tmpwriting")).unwrap();
    fs::write(chunks.join(hex::encode(chunk_id(&b))), &b).unwrap();
    drop(outbox);
    let outbox = crate::outbox::Outbox::open(ob.clone(), dir.path().join("q")).unwrap();
    assert_eq!(outbox.chunk(&chunk_id(&a)), Some(a));
    assert_eq!(outbox.chunk(&chunk_id(&b)), Some(b));
    assert!(!chunks.exists(), "left empty, removed");
}

// A repository's cache from before the machine's is moved aside, and its removal — cut short
// when a command ends — resumes at the next open.
#[test]
fn an_old_cache_is_removed_even_across_commands() {
    let (dir, shared) = repo();
    drop(shared);
    let store = dir.path().join("store");
    for d in ["cache/objects/03/ab", "cache-legacy-1-2/chunks/cd"] {
        fs::create_dir_all(store.join(d)).unwrap();
        fs::write(store.join(d).join("entry"), b"old").unwrap();
    }
    // Opened again, as by the next command.
    let _shared = Shared::for_store_path(&store).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let left: Vec<String> = fs::read_dir(&store)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("cache"))
            .collect();
        if left.is_empty() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "left: {left:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

// A log mostly retired is rewritten with what is live.
#[test]
fn a_mostly_dead_outbox_is_rewritten() {
    let dir = tempfile::tempdir().unwrap();
    let ob = crate::outbox::Outbox::open(dir.path().join("outbox"), dir.path().join("q")).unwrap();
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    let mut files = Vec::new();
    for i in 0..40u64 {
        let raw = noise(600 << 10, i);
        let cid = chunk_id(&raw);
        ob.stage_chunk(&cid, &raw).unwrap();
        files.push(Manifest {
            id: id(100 + i as u8),
            size: 2 * raw.len() as u64,
            chunks: vec![(cid, raw.len() as u32), (cid, raw.len() as u32)],
            path_hint: String::new(),
        });
    }
    ob.sync_chunks().unwrap();
    for m in &files {
        ob.stage_file(m).unwrap();
    }
    let log = dir.path().join("outbox/log");
    let before = fs::metadata(&log).unwrap().len();
    assert!(before > 20 << 20, "{before} bytes");
    ob.retire(
        &crate::outbox::Staged {
            objects: vec![],
            files: files[..35].to_vec(),
        },
        &cache,
    )
    .unwrap();
    let after = fs::metadata(&log).unwrap().len();
    assert!(after < before / 4, "rewritten: {before} -> {after}");
    assert_eq!(ob.list().unwrap().files, files[35..].to_vec());
    assert!(ob.chunk(&files[39].chunks[0].0).is_some());
}

// ─── References ───

#[test]
fn references_skip_the_virtual_roots() {
    let empty_tree = ok_encoding::empty_tree_id();
    let (tree, canonical) = tree_object(vec![
        (
            "a",
            TreeValue::File {
                id: FileId::new(id(1).to_vec()),
                executable: false,
                copy_id: CopyId::new(id(2).to_vec()),
            },
        ),
        (
            "b",
            TreeValue::File {
                id: FileId::new(id(3).to_vec()),
                executable: true,
                copy_id: CopyId::placeholder(),
            },
        ),
        ("c", TreeValue::Symlink(SymlinkId::new(id(4).to_vec()))),
        ("d", TreeValue::Tree(TreeId::new(id(5).to_vec()))),
    ]);
    let refs: HashSet<Key> = references(&canonical).into_iter().collect();
    let want: HashSet<Key> = [
        Key::new(Kind::File, id(1)),
        Key::new(Kind::CopyHistory, id(2)),
        Key::new(Kind::File, id(3)),
        Key::new(Kind::Symlink, id(4)),
        Key::new(Kind::Tree, id(5)),
    ]
    .into();
    assert_eq!(refs, want);

    let c = commit(
        vec![ROOT_ID, id(10)],
        vec![id(11)],
        vec![tree.id, empty_tree, id(12)],
    );
    let (_, canonical) = commit_object(&c);
    let refs: HashSet<Key> = references(&canonical).into_iter().collect();
    let want: HashSet<Key> = [
        Key::new(Kind::Commit, id(10)),
        Key::new(Kind::Commit, id(11)),
        Key::new(Kind::Tree, tree.id),
        Key::new(Kind::Tree, id(12)),
    ]
    .into();
    assert_eq!(refs, want, "no root commit, no empty tree");

    let mut view = View::make_root(CommitId::new(ROOT_ID.to_vec()));
    view.head_ids.insert(CommitId::new(id(20).to_vec()));
    view.local_bookmarks.insert(
        RefNameBuf::from("main"),
        RefTarget::normal(CommitId::new(id(21).to_vec())),
    );
    let canonical = ok_encoding::encode_view(&view).unwrap();
    let refs: HashSet<Key> = references(&canonical).into_iter().collect();
    assert_eq!(
        refs,
        [
            Key::new(Kind::Commit, id(20)),
            Key::new(Kind::Commit, id(21))
        ]
        .into()
    );

    let mut op = Operation::make_root(ViewId::new(id(30).to_vec()));
    op.parents = vec![
        OperationId::new(ROOT_ID.to_vec()),
        OperationId::new(id(31).to_vec()),
    ];
    op.commit_predecessors = Some(BTreeMap::from([(
        CommitId::new(id(32).to_vec()),
        vec![CommitId::new(id(33).to_vec())],
    )]));
    let canonical = ok_encoding::encode_operation(&op).unwrap();
    let refs: HashSet<Key> = references(&canonical).into_iter().collect();
    let want: HashSet<Key> = [
        Key::new(Kind::View, id(30)),
        Key::new(Kind::Operation, id(31)),
        Key::new(Kind::Commit, id(32)),
        Key::new(Kind::Commit, id(33)),
    ]
    .into();
    assert_eq!(refs, want, "no root operation");
}

// ─── Chunking ───

fn write(shared: &Shared, contents: &[u8]) -> Id {
    let mut reader = futures::io::Cursor::new(contents.to_vec());
    block_on(chunk::write_file(shared, "path/to/file", &mut reader)).unwrap()
}

#[test]
fn a_file_id_is_its_contents_however_it_is_chunked() {
    let (_dir, shared) = repo();
    for (n, want_chunks) in [(0, 0), (1, 1), (chunk::SINGLE_CHUNK, 1)] {
        let contents = noise(n, n as u64);
        let fid = write(&shared, &contents);
        assert_eq!(
            fid,
            ok_encoding::hash(&ok_encoding::encode_file(&contents)),
            "{n} bytes"
        );
        let m = shared.outbox.file(&fid).unwrap();
        assert_eq!(
            (m.size, m.chunks.len()),
            (n as u64, want_chunks),
            "{n} bytes"
        );
        assert_eq!(m.path_hint, "path/to/file");
    }

    for n in [chunk::SINGLE_CHUNK + 1, 1 << 20, (3 << 20) + 17] {
        let contents = noise(n, 7);
        let fid = write(&shared, &contents);
        assert_eq!(
            fid,
            ok_encoding::hash(&ok_encoding::encode_file(&contents)),
            "{n} bytes"
        );
        let m = shared.outbox.file(&fid).unwrap();
        assert!(m.chunks.len() > 1, "{n} bytes are content-defined chunks");
        let (last, rest) = m.chunks.split_last().unwrap();
        assert!(
            rest.iter().all(|c| (16 << 10..=256 << 10).contains(&c.1)),
            "{n} bytes: {:?}",
            m.chunks
        );
        assert!(last.1 <= 256 << 10);
        let joined: Vec<u8> = m
            .chunks
            .iter()
            .flat_map(|(cid, _)| shared.chunk(cid).unwrap())
            .collect();
        assert_eq!(joined, contents);
    }
}

#[test]
fn an_edit_to_a_large_file_restages_only_nearby_chunks() {
    let (_dir, shared) = repo();
    let mut contents = noise(4 << 20, 11);
    write(&shared, &contents);
    let before = shared.outbox.staged_chunks();
    contents[2 << 20] ^= 0xff;
    write(&shared, &contents);
    let added = shared.outbox.staged_chunks() - before;
    assert!(
        (1..=3).contains(&added),
        "a one-byte edit restaged {added} chunks of {before}"
    );
}

#[test]
fn a_file_already_cached_is_not_staged_again() {
    let (_dir, shared) = repo();
    let contents = noise(1000, 5);
    let fid = write(&shared, &contents);
    let staged = shared.outbox.list().unwrap();
    shared.outbox.retire(&staged, &shared.cache).unwrap();
    assert_eq!(write(&shared, &contents), fid);
    assert!(shared.outbox.list().unwrap().is_empty());
    assert_eq!(shared.outbox.staged_chunks(), 0);
}

// ─── Publishing ───

#[test]
fn a_publish_sends_references_before_referrers() {
    let (_dir, shared) = repo();
    let (tree, tree_bytes) = tree_object(vec![(
        "f",
        TreeValue::Symlink(SymlinkId::new(id(1).to_vec())),
    )]);
    let (c1, c1_bytes) = commit_object(&commit(vec![ROOT_ID], vec![], vec![tree.id]));
    let (c2, c2_bytes) = commit_object(&commit(vec![c1.id], vec![], vec![tree.id]));
    let mut view = View::make_root(CommitId::new(ROOT_ID.to_vec()));
    view.head_ids = [CommitId::new(c2.id.to_vec())].into();
    let view_bytes = ok_encoding::encode_view(&view).unwrap();
    let v = Key::new(Kind::View, ok_encoding::hash(&view_bytes));
    let mut op = Operation::make_root(ViewId::new(v.id.to_vec()));
    op.parents = vec![OperationId::new(ROOT_ID.to_vec())];
    let op_bytes = ok_encoding::encode_operation(&op).unwrap();
    let o = Key::new(Kind::Operation, ok_encoding::hash(&op_bytes));
    for (k, b) in [
        (tree, &tree_bytes),
        (c1, &c1_bytes),
        (c2, &c2_bytes),
        (v, &view_bytes),
        (o, &op_bytes),
    ] {
        shared.outbox.stage_object(k, b).unwrap();
    }

    let order = publish::dependency_order(&shared, &[o, c2, v, c1, tree]);
    let pos = |k: Key| order.iter().position(|x| *x == k).unwrap();
    assert_eq!(order.len(), 5);
    assert!(
        pos(tree) < pos(c1) && pos(c1) < pos(c2) && pos(c2) < pos(v) && pos(v) < pos(o),
        "{order:?}"
    );
}

#[test]
fn a_long_history_orders_oldest_first() {
    let (_dir, shared) = repo();
    let mut parent = ROOT_ID;
    let mut keys = Vec::new();
    for _ in 0..5_000 {
        let (k, b) = commit_object(&commit(
            vec![parent],
            vec![],
            vec![ok_encoding::empty_tree_id()],
        ));
        shared.outbox.stage_object(k, &b).unwrap();
        keys.push(k);
        parent = k.id;
    }
    keys.reverse();
    let order = publish::dependency_order(&shared, &keys);
    keys.reverse();
    assert_eq!(order, keys);
}

#[test]
fn rejections_name_the_missing_references() {
    let missing = pb::PublishRejection {
        missing: vec![
            pb::ObjectRef {
                kind: Kind::Operation as i32,
                id: id(3).to_vec(),
            },
            pb::ObjectRef {
                kind: Kind::Commit as i32,
                id: id(4).to_vec(),
            },
        ],
        message: "missing".into(),
    };
    let details = publish::RpcStatus {
        code: Code::FailedPrecondition as i32,
        message: "missing".into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/ok.scm.v1.PublishRejection".into(),
            value: missing.encode_to_vec(),
        }],
    };
    let status = Status::with_details(
        Code::FailedPrecondition,
        "missing",
        details.encode_to_vec().into(),
    );
    assert_eq!(
        publish::rejection(&status),
        Some(vec![
            Key::new(Kind::Operation, id(3)),
            Key::new(Kind::Commit, id(4))
        ])
    );
    assert_eq!(
        publish::rejection(&Status::failed_precondition("no details")),
        None
    );
    assert_eq!(
        publish::rejection(&Status::with_details(
            Code::Internal,
            "x",
            details.encode_to_vec().into()
        )),
        None
    );
}

// ─── Read coalescing ───

type Calls = Arc<Mutex<Vec<Vec<u32>>>>;

fn batcher(max: usize, fail: bool) -> (Arc<Batcher<u32, String>>, Calls) {
    let calls: Calls = Arc::default();
    let seen = calls.clone();
    let b = Batcher::new(
        "test",
        max,
        Box::new(move |keys: Vec<u32>| {
            let mut sorted = keys.clone();
            sorted.sort();
            seen.lock().unwrap().push(sorted);
            Box::pin(async move {
                if fail {
                    return Err(Status::unavailable("down"));
                }
                // Odd keys exist.
                Ok(keys
                    .into_iter()
                    .filter(|k| k % 2 == 1)
                    .map(|k| (k, format!("v{k}")))
                    .collect::<HashMap<_, _>>())
            })
        }),
    );
    (b, calls)
}

#[test]
fn concurrent_reads_share_one_fetch() {
    let (b, calls) = batcher(1000, false);
    let keys = [1, 2, 3, 3, 4, 5];
    let got = block_on(futures::future::join_all(keys.iter().map(|k| b.get(*k))));
    let got: Vec<Option<String>> = got.into_iter().map(Result::unwrap).collect();
    assert_eq!(
        got,
        [
            Some("v1".into()),
            None,
            Some("v3".into()),
            Some("v3".into()),
            None,
            Some("v5".into())
        ]
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![vec![1, 2, 3, 4, 5]],
        "one fetch, each key once"
    );
}

#[test]
fn a_large_batch_is_split() {
    let (b, calls) = batcher(10, false);
    let got = block_on(futures::future::join_all((0..25).map(|k| b.get(k))));
    assert!(got.iter().all(Result::is_ok));
    let mut sizes: Vec<usize> = calls.lock().unwrap().iter().map(Vec::len).collect();
    sizes.sort();
    assert_eq!(sizes, [5, 10, 10]);
}

// A read of a key the current fetch is reading joins it: the key is not fetched again.
#[test]
fn a_read_joins_the_fetch_in_flight() {
    let calls: Calls = Arc::default();
    let seen = calls.clone();
    let b: Arc<Batcher<u32, String>> = Batcher::new(
        "test",
        1000,
        Box::new(move |keys: Vec<u32>| {
            let mut sorted = keys.clone();
            sorted.sort();
            seen.lock().unwrap().push(sorted);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(keys
                    .into_iter()
                    .map(|k| (k, format!("v{k}")))
                    .collect::<HashMap<_, _>>())
            })
        }),
    );
    let first = {
        let b = b.clone();
        std::thread::spawn(move || block_on(b.get(1)))
    };
    // The fetch for 1 is in flight: it takes 300 ms.
    while calls.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(5));
    }
    let (again, other) = block_on(futures::future::join(b.get(1), b.get(2)));
    assert_eq!(first.join().unwrap().unwrap(), Some("v1".into()));
    assert_eq!(again.unwrap(), Some("v1".into()));
    assert_eq!(other.unwrap(), Some("v2".into()));
    assert_eq!(
        *calls.lock().unwrap(),
        vec![vec![1], vec![2]],
        "1 was fetched once"
    );
}

#[test]
fn a_failed_fetch_fails_every_waiter() {
    let (b, _) = batcher(1000, true);
    let got = block_on(futures::future::join_all((0..4).map(|k| b.get(k))));
    assert!(
        got.iter()
            .all(|r| r.as_ref().is_err_and(|s| s.code() == Code::Unavailable))
    );
}

// Reads made while a fetch is in flight go out at once, in fetches of their own that overlap it —
// never more than MAX_IN_FLIGHT of a kind at a time — and those that wait for the limit go out as
// fetches return.
#[test]
fn reads_overlap_up_to_the_limit() {
    use crate::batch::MAX_IN_FLIGHT;
    let running = Arc::new(Mutex::new((0usize, 0usize))); // (now, most at once)
    let seen = running.clone();
    let calls: Calls = Arc::default();
    let logged = calls.clone();
    let b: Arc<Batcher<u32, String>> = Batcher::new(
        "test",
        1,
        Box::new(move |keys: Vec<u32>| {
            let seen = seen.clone();
            logged.lock().unwrap().push(keys.clone());
            Box::pin(async move {
                {
                    let mut r = seen.lock().unwrap();
                    r.0 += 1;
                    r.1 = r.1.max(r.0);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                seen.lock().unwrap().0 -= 1;
                Ok(keys.into_iter().map(|k| (k, format!("v{k}"))).collect())
            })
        }),
    );
    let n = 3 * MAX_IN_FLIGHT as u32;
    let started = std::time::Instant::now();
    let got = block_on(futures::future::join_all((0..n).map(|k| b.get(k))));
    let took = started.elapsed();
    assert!(got.iter().all(|r| r.as_ref().is_ok_and(Option::is_some)));
    assert_eq!(calls.lock().unwrap().len(), n as usize, "one key a fetch");
    assert_eq!(
        running.lock().unwrap().1,
        MAX_IN_FLIGHT,
        "as many fetches at once as the limit, and no more"
    );
    assert!(
        took < Duration::from_millis(100 * 3 * 2),
        "{n} fetches of 100 ms, {MAX_IN_FLIGHT} at a time, took {took:?}"
    );

    // A read made while another fetch is in flight does not wait for it.
    let started = std::time::Instant::now();
    let first = {
        let b = b.clone();
        std::thread::spawn(move || block_on(b.get(1000)))
    };
    std::thread::sleep(Duration::from_millis(30));
    block_on(b.get(1001)).unwrap();
    let second = started.elapsed();
    first.join().unwrap().unwrap();
    assert!(
        second < Duration::from_millis(180),
        "the second read waited for the first fetch: {second:?}"
    );
}

/// A 3,600-byte chunk, `ok's stored chunk\n` 200 times, as one zstd frame with a content checksum —
/// as the SCM's encoder writes them (klauspost/compress, at its best compression, wrote this one).
const STORED_CHUNK_ZSTD: [u8; 42] = [
    0x28, 0xb5, 0x2f, 0xfd, 0x64, 0x10, 0x0d, 0xe5, 0x00, 0x00, 0x24, 0x01, 0x6f, 0x6b, 0x27, 0x73,
    0x20, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x64, 0x20, 0x63, 0x68, 0x75, 0x6e, 0x6b, 0x0a, 0x01, 0x54,
    0x11, 0x04, 0x2f, 0xf6, 0x5b, 0x01, 0x00, 0x05, 0x0f, 0x5d,
];

fn stored_frame(
    data: Vec<u8>,
    codec: ok_proto::ok::scm::v1::ChunkCodec,
    raw_length: u32,
) -> ok_proto::ok::scm::v1::FileData {
    ok_proto::ok::scm::v1::FileData {
        chunk_id: vec![7; 32],
        data,
        codec: codec as i32,
        raw_length,
        ..Default::default()
    }
}

// The SCM sends chunks as their packs store them: a zstd frame, or the raw bytes; an SCM that
// predates stored reads sends raw bytes and names no codec. Anything else is refused.
#[test]
fn chunk_bytes_decodes_stored_chunks() {
    use ok_proto::ok::scm::v1::ChunkCodec;
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(dir.path().join("cache")).unwrap();
    let raw = b"ok's stored chunk\n".repeat(200);
    let n = raw.len() as u32;
    let decoded = crate::shared::chunk_bytes(
        stored_frame(STORED_CHUNK_ZSTD.to_vec(), ChunkCodec::Zstd, n),
        &cache,
    )
    .unwrap();
    assert_eq!(decoded, raw);
    assert_eq!(
        crate::shared::chunk_bytes(stored_frame(raw.clone(), ChunkCodec::Raw, n), &cache).unwrap(),
        raw
    );
    assert_eq!(
        crate::shared::chunk_bytes(
            stored_frame(raw.clone(), ChunkCodec::Unspecified, 0),
            &cache
        )
        .unwrap(),
        raw
    );

    for (frame, why) in [
        (
            stored_frame(STORED_CHUNK_ZSTD.to_vec(), ChunkCodec::Zstd, n - 1),
            "another raw length",
        ),
        (
            stored_frame(STORED_CHUNK_ZSTD.to_vec(), ChunkCodec::Zstd, n + 1),
            "a longer raw length",
        ),
        (stored_frame(raw.clone(), ChunkCodec::Zstd, n), "not zstd"),
        (
            stored_frame(STORED_CHUNK_ZSTD[..30].to_vec(), ChunkCodec::Zstd, n),
            "a truncated frame",
        ),
        (
            stored_frame(STORED_CHUNK_ZSTD.to_vec(), ChunkCodec::Zstd, u32::MAX),
            "a raw length over a message",
        ),
        (
            stored_frame(raw.clone(), ChunkCodec::Raw, n + 1),
            "a raw chunk of another length",
        ),
    ] {
        let err = crate::shared::chunk_bytes(frame, &cache).expect_err(why);
        assert_eq!(err.code(), tonic::Code::DataLoss, "{why}: {err}");
    }
    let mut unknown = stored_frame(raw, ChunkCodec::Raw, n);
    unknown.codec = 9;
    let err = crate::shared::chunk_bytes(unknown, &cache).expect_err("an unknown codec");
    assert!(err.message().contains("unknown codec 9"), "{err}");
}

/// A zstd dictionary with ID 40001, and `package ok\n\nfunc Read4242() error { return nil }\n`
/// compressed with it — as the SCM trains and compacts with them (klauspost/compress: dict.BuildZstdDict
/// over 64 files like it, then its encoder at its best compression, with a checksum).
const DICTIONARY_40001: [u8; 163] = [
    0x37, 0xa4, 0x30, 0xec, 0x41, 0x9c, 0x00, 0x00, 0x16, 0xd0, 0x91, 0x00, 0x30, 0x0c, 0xe3, 0x29,
    0xbd, 0x2a, 0x16, 0xe7, 0x71, 0x93, 0x52, 0xf2, 0x3f, 0xa5, 0x44, 0x8b, 0x37, 0xa9, 0x01, 0x10,
    0x86, 0x05, 0x1f, 0x10, 0x50, 0xf1, 0xcf, 0x07, 0x70, 0xbb, 0x00, 0x12, 0x00, 0x00, 0x00, 0x4d,
    0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x20, 0x52, 0x65, 0x61, 0x64, 0x35, 0x20, 0x52, 0x65,
    0x61, 0x64, 0x31, 0x20, 0x52, 0x65, 0x61, 0x64, 0x32, 0x20, 0x52, 0x65, 0x61, 0x64, 0x33, 0x20,
    0x52, 0x65, 0x61, 0x64, 0x34, 0x6e, 0x20, 0x6e, 0x69, 0x6c, 0x20, 0x72, 0x6e, 0x20, 0x6e, 0x69,
    0x6c, 0x75, 0x72, 0x6e, 0x20, 0x6e, 0x69, 0x74, 0x75, 0x72, 0x6e, 0x20, 0x6e, 0x28, 0x29, 0x20,
    0x65, 0x72, 0x72, 0x6f, 0x72, 0x20, 0x7b, 0x20, 0x72, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x20, 0x63,
    0x20, 0x52, 0x65, 0x61, 0x64, 0x6e, 0x63, 0x20, 0x52, 0x65, 0x61, 0x75, 0x6e, 0x63, 0x20, 0x52,
    0x65, 0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x20, 0x6f, 0x6b, 0x0a, 0x0a, 0x66, 0x75, 0x6e,
    0x63, 0x20, 0x52,
];
const CHUNK_WITH_DICTIONARY: [u8; 35] = [
    0x28, 0xb5, 0x2f, 0xfd, 0x06, 0x00, 0x41, 0x9c, 0xa5, 0x00, 0x00, 0x48, 0x32, 0x34, 0x32, 0x6e,
    0x69, 0x6c, 0x20, 0x7d, 0x0a, 0x03, 0x00, 0x52, 0xf8, 0x9c, 0xc1, 0x82, 0xf2, 0x26, 0x10, 0x15,
    0xf1, 0x5f, 0xfb,
];

// A chunk compressed with a repository's dictionary decodes once the dictionary is cached, is
// kept as sent, and decodes from the cache in a later process; the dictionary is checked to be the
// one it is fetched as, and a frame naming none is refused.
#[test]
fn chunks_compressed_with_a_dictionary() {
    use ok_proto::ok::scm::v1::ChunkCodec;
    let raw = b"package ok\n\nfunc Read4242() error { return nil }\n".to_vec();
    let n = raw.len() as u32;
    assert_eq!(
        crate::shared::zstd_dictionary_id(&CHUNK_WITH_DICTIONARY),
        Some(40001)
    );
    assert_eq!(crate::shared::zstd_dictionary_id(&STORED_CHUNK_ZSTD), None);
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let cache = Cache::open(cache_dir.clone()).unwrap();
    let frame = stored_frame(CHUNK_WITH_DICTIONARY.to_vec(), ChunkCodec::ZstdDict, n);
    assert_eq!(crate::shared::frame_dictionary(&frame), Some(40001));
    let err = crate::shared::chunk_bytes(frame.clone(), &cache).expect_err("no dictionary yet");
    assert_eq!(err.code(), tonic::Code::DataLoss, "{err}");

    assert!(
        cache.put_dictionary(40002, &DICTIONARY_40001).is_err(),
        "a dictionary under another ID"
    );
    assert!(cache.put_dictionary(40001, b"not a dictionary").is_err());
    assert!(!cache.has_dictionary(40001));
    cache.put_dictionary(40001, &DICTIONARY_40001).unwrap();
    assert!(cache.has_dictionary(40001));
    let (got, stored) = crate::shared::stored_chunk(frame.clone(), &cache).unwrap();
    assert_eq!(got, raw);
    assert_eq!(
        (stored.codec, stored.dict),
        (crate::segments::Codec::ZstdDict, 40001)
    );

    // Kept as sent; a later process decodes it with the dictionary from the segments.
    let file = crate::cache::file_id(&raw);
    cache
        .put_whole_stored(&file, &chunk_id(&raw), &raw, &stored)
        .unwrap();
    drop(cache);
    let cache = Cache::open(cache_dir).unwrap();
    assert_eq!(cache.whole(&file), Some(raw.clone()));
    assert_eq!(cache.chunk(&chunk_id(&raw)), Some(raw.clone()));

    let mut none = stored_frame(STORED_CHUNK_ZSTD.to_vec(), ChunkCodec::ZstdDict, 3600);
    let err = crate::shared::chunk_bytes(none.clone(), &cache).expect_err("names no dictionary");
    assert!(err.message().contains("names no dictionary"), "{err}");
    none.data = CHUNK_WITH_DICTIONARY.to_vec();
    none.raw_length = n + 1;
    assert!(
        crate::shared::chunk_bytes(none, &cache).is_err(),
        "another raw length"
    );
}

// The operation frontier: newest first, without duplicates, at most FRONTIER_MAX, and empty
// before anything joined it.
#[test]
fn op_frontier_keeps_the_newest() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(dir.path().to_path_buf()).unwrap();
    assert!(cache.op_frontier().is_empty());
    let id = |n: u8| [n; 32];
    cache.add_op_frontier(&[id(1)]).unwrap();
    cache.add_op_frontier(&[id(2), id(3)]).unwrap();
    assert_eq!(cache.op_frontier(), vec![id(2), id(3), id(1)]);
    // An operation already there moves to the front, once.
    cache.add_op_frontier(&[id(1)]).unwrap();
    assert_eq!(cache.op_frontier(), vec![id(1), id(2), id(3)]);
    let many: Vec<[u8; 32]> = (10..10 + crate::cache::FRONTIER_MAX as u8 + 5)
        .map(id)
        .collect();
    cache.add_op_frontier(&many).unwrap();
    let kept = cache.op_frontier();
    assert_eq!(kept.len(), crate::cache::FRONTIER_MAX);
    assert_eq!(kept, many[..crate::cache::FRONTIER_MAX].to_vec());
}
