use std::collections::BTreeMap;

use jj_lib::backend::{ChangeId, Commit, CopyHistory, Signature, Timestamp, Tree, TreeValue};
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::{Operation, RefTarget, RemoteRef, RemoteRefState, View};

use crate::{CHANGE_ID_LEN, Code, Error, ID_LEN, Kind, Object, valid_name, valid_path};

type Result<T> = std::result::Result<T, Error>;

fn fail(code: Code, detail: impl Into<String>) -> Error {
    Error {
        code,
        offset: None,
        detail: detail.into(),
    }
}

/// Canonical bytes, built up from an object's kind byte.
struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    fn new(kind: Kind) -> Self {
        Self {
            buf: vec![kind as u8],
        }
    }

    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn count(&mut self, n: usize) {
        self.u64(n as u64);
    }

    fn boolean(&mut self, v: bool) {
        self.u8(u8::from(v));
    }

    fn id(&mut self, id: &impl ObjectId) -> Result<()> {
        let b = id.as_bytes();
        if b.len() != ID_LEN {
            return Err(fail(
                Code::IdLength,
                format!("{} ID {} is {} bytes", id.object_type(), id.hex(), b.len()),
            ));
        }
        self.buf.extend_from_slice(b);
        Ok(())
    }

    fn change_id(&mut self, id: &ChangeId) -> Result<()> {
        let b = id.as_bytes();
        if b.len() != CHANGE_ID_LEN {
            return Err(fail(
                Code::IdLength,
                format!("change ID {} is {} bytes", id.hex(), b.len()),
            ));
        }
        self.buf.extend_from_slice(b);
        Ok(())
    }

    fn bytes(&mut self, b: &[u8]) {
        self.count(b.len());
        self.buf.extend_from_slice(b);
    }

    fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    fn ids<'a, I: ObjectId + 'a>(
        &mut self,
        ids: impl ExactSizeIterator<Item = &'a I>,
    ) -> Result<()> {
        self.count(ids.len());
        for id in ids {
            self.id(id)?;
        }
        Ok(())
    }

    fn timestamp(&mut self, t: &Timestamp) {
        self.i64(t.timestamp.0);
        self.i32(t.tz_offset);
    }

    fn signature(&mut self, s: &Signature) {
        self.str(&s.name);
        self.str(&s.email);
        self.timestamp(&s.timestamp);
    }

    fn merge<T>(
        &mut self,
        m: &Merge<T>,
        mut term: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        let terms = m.as_slice();
        if terms.len().is_multiple_of(2) {
            return Err(fail(
                Code::Merge,
                format!("a merge has {} terms", terms.len()),
            ));
        }
        self.count(terms.len());
        for t in terms {
            term(self, t)?;
        }
        Ok(())
    }

    fn ref_target(&mut self, t: &RefTarget) -> Result<()> {
        self.merge(t.as_merge(), |e, term| match term {
            None => {
                e.u8(0);
                Ok(())
            }
            Some(id) => {
                e.u8(1);
                e.id(id)
            }
        })
    }

    fn remote_ref(&mut self, r: &RemoteRef) -> Result<()> {
        self.ref_target(&r.target)?;
        self.u8(match r.state {
            RemoteRefState::New => 0,
            RemoteRefState::Tracked => 1,
        });
        Ok(())
    }

    /// Writes a map<str, V>. jj's name types order as their strings do, which is bytewise,
    /// so a BTreeMap already iterates in canonical order.
    fn str_map<K: AsRef<str>, V>(
        &mut self,
        m: &BTreeMap<K, V>,
        mut value: impl FnMut(&mut Self, &V) -> Result<()>,
    ) -> Result<()> {
        self.count(m.len());
        for (k, v) in m {
            self.str(k.as_ref());
            value(self, v)?;
        }
        Ok(())
    }

    /// Writes every commit field before the signature.
    fn commit_fields(&mut self, c: &Commit) -> Result<()> {
        self.ids(c.parents.iter())?;
        self.ids(c.predecessors.iter())?;
        self.merge(&c.root_tree, |e, id| e.id(id))?;
        self.merge(&c.conflict_labels, |e, label| {
            e.str(label);
            Ok(())
        })?;
        self.change_id(&c.change_id)?;
        self.str(&c.description);
        self.signature(&c.author);
        self.signature(&c.committer);
        Ok(())
    }
}

/// A file's canonical bytes: its kind byte and its raw contents.
pub fn encode_file(contents: &[u8]) -> Vec<u8> {
    let mut e = Encoder::new(Kind::File);
    e.buf.extend_from_slice(contents);
    e.buf
}

/// A chunk's canonical bytes: its kind byte and its raw contents.
pub fn encode_chunk(contents: &[u8]) -> Vec<u8> {
    let mut e = Encoder::new(Kind::Chunk);
    e.buf.extend_from_slice(contents);
    e.buf
}

/// A symlink's canonical bytes: its kind byte and its target's bytes.
pub fn encode_symlink(target: &str) -> Vec<u8> {
    let mut e = Encoder::new(Kind::Symlink);
    e.buf.extend_from_slice(target.as_bytes());
    e.buf
}

pub fn encode_tree(tree: &Tree) -> Result<Vec<u8>> {
    let mut e = Encoder::new(Kind::Tree);
    e.count(tree.entries().count());
    let mut prev: Option<&str> = None;
    for entry in tree.entries() {
        let name = entry.name().as_internal_str();
        if !valid_name(name) {
            return Err(fail(Code::Name, format!("tree entry name {name:?}")));
        }
        if let Some(prev) = prev {
            if name == prev {
                return Err(fail(
                    Code::Duplicate,
                    format!("tree entry {name:?} repeats"),
                ));
            }
            if name < prev {
                return Err(fail(
                    Code::Unsorted,
                    format!("tree entry {name:?} follows {prev:?}"),
                ));
            }
        }
        prev = Some(name);
        e.str(name);
        match entry.value() {
            TreeValue::File {
                id,
                executable,
                copy_id,
            } => {
                e.u8(0);
                e.id(id)?;
                e.boolean(*executable);
                let copy = copy_id.as_bytes();
                if !copy.is_empty() && copy.len() != ID_LEN {
                    return Err(fail(
                        Code::CopyId,
                        format!("copy ID of {} bytes on {name:?}", copy.len()),
                    ));
                }
                e.bytes(copy);
            }
            TreeValue::Symlink(id) => {
                e.u8(1);
                e.id(id)?;
            }
            TreeValue::Tree(id) => {
                e.u8(2);
                e.id(id)?;
            }
            TreeValue::GitSubmodule(_) => {
                return Err(fail(
                    Code::Tag,
                    format!("{name:?} is a git submodule, which has no encoding"),
                ));
            }
        }
    }
    Ok(e.buf)
}

pub fn encode_copy_history(c: &CopyHistory) -> Result<Vec<u8>> {
    let path = c.current_path.as_internal_file_string();
    if !valid_path(path) {
        return Err(fail(Code::Path, format!("copy history path {path:?}")));
    }
    let mut e = Encoder::new(Kind::CopyHistory);
    e.str(path);
    e.ids(c.parents.iter())?;
    e.bytes(&c.salt);
    Ok(e.buf)
}

/// A commit's canonical bytes. A signed commit's `secure_sig.data` must be its
/// [`signed_data`]: only the signature is stored, and the data is rebuilt from the commit.
pub fn encode_commit(c: &Commit) -> Result<Vec<u8>> {
    let mut e = Encoder::new(Kind::Commit);
    e.commit_fields(c)?;
    match &c.secure_sig {
        None => e.u8(0),
        Some(secure_sig) => {
            let unsigned = e.buf.len();
            if secure_sig.data.len() != unsigned + 1
                || secure_sig.data[..unsigned] != e.buf[..]
                || secure_sig.data[unsigned] != 0
            {
                return Err(fail(
                    Code::SignedData,
                    "secure_sig.data is not the commit's signed data",
                ));
            }
            e.u8(1);
            e.bytes(&secure_sig.sig);
        }
    }
    Ok(e.buf)
}

/// The bytes a commit's signature signs: the commit's canonical bytes with the signature
/// absent.
pub fn signed_data(c: &Commit) -> Result<Vec<u8>> {
    let mut e = Encoder::new(Kind::Commit);
    e.commit_fields(c)?;
    e.u8(0);
    Ok(e.buf)
}

pub fn encode_operation(op: &Operation) -> Result<Vec<u8>> {
    let mut e = Encoder::new(Kind::Operation);
    e.id(&op.view_id)?;
    e.ids(op.parents.iter())?;
    let m = &op.metadata;
    e.timestamp(&m.time.start);
    e.timestamp(&m.time.end);
    e.str(&m.description);
    e.str(&m.hostname);
    e.str(&m.username);
    e.boolean(m.is_snapshot);
    match &m.workspace_name {
        None => e.u8(0),
        Some(name) => {
            e.u8(1);
            e.str(name.as_str());
        }
    }
    e.str_map(&m.attributes, |e, v| {
        e.str(v);
        Ok(())
    })?;
    match &op.commit_predecessors {
        None => e.u8(0),
        Some(predecessors) => {
            e.u8(1);
            e.count(predecessors.len());
            for (commit, preds) in predecessors {
                e.id(commit)?;
                e.ids(preds.iter())?;
            }
        }
    }
    Ok(e.buf)
}

pub fn encode_view(view: &View) -> Result<Vec<u8>> {
    let mut e = Encoder::new(Kind::View);
    let mut heads: Vec<_> = view.head_ids.iter().collect();
    heads.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    e.ids(heads.into_iter())?;
    e.str_map(&view.local_bookmarks, Encoder::ref_target)?;
    e.str_map(&view.local_tags, Encoder::ref_target)?;
    e.str_map(&view.remote_views, |e, rv| {
        e.str_map(&rv.bookmarks, Encoder::remote_ref)?;
        e.str_map(&rv.tags, Encoder::remote_ref)
    })?;
    e.str_map(&view.git_refs, Encoder::ref_target)?;
    e.str_map(&view.git_heads, Encoder::ref_target)?;
    e.str_map(&view.wc_commit_ids, |e, id| e.id(id))?;
    Ok(e.buf)
}

/// An object's canonical bytes.
pub fn encode(obj: &Object) -> Result<Vec<u8>> {
    match obj {
        Object::File(contents) => Ok(encode_file(contents)),
        Object::Chunk(contents) => Ok(encode_chunk(contents)),
        Object::Symlink(target) => Ok(encode_symlink(target)),
        Object::Tree(tree) => encode_tree(tree),
        Object::CopyHistory(c) => encode_copy_history(c),
        Object::Commit(c) => encode_commit(c),
        Object::Operation(op) => encode_operation(op),
        Object::View(view) => encode_view(view),
    }
}
