use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::matchers::Matcher;
use jj_lib::matchers::NothingMatcher;
use jj_lib::matchers::PrefixMatcher;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;

use super::ignores;

/// A working copy's files: `path → contents`, a trailing `/` making an empty directory.
fn tree(files: &[(&str, &str)]) -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    for (path, contents) in files {
        let p = root.path().join(path);
        if let Some(dir) = path.strip_suffix('/') {
            fs::create_dir_all(root.path().join(dir)).unwrap();
            continue;
        }
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, contents).unwrap();
    }
    root
}

fn path(p: &str) -> &RepoPath {
    RepoPath::from_internal_string(p).unwrap()
}

fn read(root: &Path, force: &dyn Matcher) -> std::sync::Arc<GitIgnoreFile> {
    ignores(root, &GitIgnoreFile::empty(), force).unwrap()
}

/// Makes a directory unreadable, reporting whether reading it now fails: it does not for a
/// process that overrides permissions.
fn lock(dir: &Path) -> bool {
    fs::set_permissions(dir, fs::Permissions::from_mode(0o000)).unwrap();
    fs::read_dir(dir).is_err()
}

fn unlock(dir: &Path) {
    fs::set_permissions(dir, fs::Permissions::from_mode(0o755)).unwrap();
}

// Every directory's .ignore applies beneath it; a deeper one's rules win; siblings' rules stay
// in their own directories.
#[test]
fn nested_files() {
    let root = tree(&[
        (".ignore", "*.log\nbuild/\n"),
        ("sub/.ignore", "!keep.log\n"),
        ("sub/deeper/.ignore", "keep.log\n"),
        ("a/.ignore", "x\n"),
        ("b/", ""),
    ]);
    let got = read(root.path(), &NothingMatcher);
    for (p, want) in [
        ("top.log", true),
        ("sub/keep.log", false),
        ("sub/other.log", true),
        ("sub/deeper/keep.log", true),
        ("a/x", true),
        ("b/x", false),
        ("x", false),
    ] {
        assert_eq!(got.matches_file(path(p)), want, "{p}");
    }
    assert!(got.matches_dir(path("build")));
    assert!(!got.matches_dir(path("sub")));
}

// A directory an ignore file ignores is not looked into, whichever file ignores it: its
// .ignore is not read, and nothing beneath it is.
#[test]
fn ignored_directories_are_not_read() {
    let root = tree(&[
        (".ignore", "gen/\n"),
        (".gitignore", "out/\n"),
        ("gen/.ignore", "y\n"),
        ("gen/locked/", ""),
        ("out/.ignore", "z\n"),
        ("out/locked/", ""),
    ]);
    let locked = [
        root.path().join("gen/locked"),
        root.path().join("out/locked"),
    ];
    let enforced = locked.iter().all(|d| lock(d));
    let got = ignores(root.path(), &GitIgnoreFile::empty(), &NothingMatcher);
    locked.iter().for_each(|d| unlock(d));
    let got = if enforced {
        got.unwrap()
    } else {
        read(root.path(), &NothingMatcher)
    };
    assert!(!got.matches_file(path("gen/y")));
    assert!(!got.matches_file(path("out/z")));
}

// jj decides by the .gitignore rules after the .ignore rules: a directory .gitignore
// re-includes is one jj reads, so its .ignore applies.
#[test]
fn gitignore_reincludes() {
    let root = tree(&[
        (".ignore", "gen/\n"),
        (".gitignore", "!gen/\n"),
        ("gen/.ignore", "tmp\n"),
    ]);
    assert!(read(root.path(), &NothingMatcher).matches_file(path("gen/tmp")));
}

// A directory tracking is forced into is read, as jj reads it.
#[test]
fn forced_tracking() {
    let root = tree(&[(".ignore", "gen/\n"), ("gen/.ignore", "tmp\n")]);
    assert!(!read(root.path(), &NothingMatcher).matches_file(path("gen/tmp")));
    let force = PrefixMatcher::new([RepoPathBuf::from_internal_string("gen/keep").unwrap()]);
    assert!(read(root.path(), &force).matches_file(path("gen/tmp")));
}

// jj skips another repository inside the working copy, and its own directory.
#[test]
fn nested_repositories_and_reserved_names() {
    let root = tree(&[
        ("nested/.jj/", ""),
        ("nested/.ignore", "n\n"),
        ("other/.git", "gitdir: elsewhere\n"),
        ("other/.ignore", "o\n"),
        (".jj/.ignore", "j\n"),
        ("kept/.ignore", "k\n"),
    ]);
    let got = read(root.path(), &NothingMatcher);
    assert!(!got.matches_file(path("nested/n")));
    assert!(!got.matches_file(path("other/o")));
    assert!(!got.matches_file(path(".jj/j")));
    assert!(got.matches_file(path("kept/k")));
}

// An .ignore that is a directory is not an ignore file; one that is a link to a file is.
#[test]
fn not_files_and_links() {
    let root = tree(&[
        ("dir/.ignore/inner", "inner\n"),
        ("rules", "linked\n"),
        ("link/", ""),
    ]);
    std::os::unix::fs::symlink(root.path().join("rules"), root.path().join("link/.ignore"))
        .unwrap();
    let got = read(root.path(), &NothingMatcher);
    assert!(!got.matches_file(path("dir/inner")));
    assert!(got.matches_file(path("link/linked")));
}
