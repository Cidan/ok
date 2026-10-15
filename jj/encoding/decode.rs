use std::collections::{BTreeMap, HashSet};

use jj_lib::backend::{
    ChangeId, Commit, CommitId, CopyHistory, CopyId, FileId, MillisSinceEpoch, SecureSig,
    Signature, SymlinkId, Timestamp, Tree, TreeId, TreeValue,
};
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::{
    Operation, OperationId, OperationMetadata, RefTarget, RemoteRef, RemoteRefState, RemoteView,
    TimestampRange, View, ViewId,
};
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo_path::{RepoPathBuf, RepoPathComponentBuf};

use crate::{CHANGE_ID_LEN, Code, Error, ID_LEN, Kind, Object, valid_name, valid_path};

type Result<T> = std::result::Result<T, Error>;

/// Parses canonical bytes into the object they encode, refusing anything that is not
/// canonical. Violations are reported in encoding order, the first one found, so this and
/// internal/scm/encoding report the same code for the same input.
pub fn decode(b: &[u8]) -> Result<Object> {
    let mut d = Decoder { b, off: 0 };
    let obj = d.object()?;
    if d.off != b.len() {
        return Err(d.fail_at(
            d.off,
            Code::Trailing,
            format!("{} bytes after the {}", b.len() - d.off, obj.kind().name()),
        ));
    }
    Ok(obj)
}

/// Reads canonical bytes in order. Every length and count is checked against the bytes
/// remaining as it is read, and collections grow only as elements actually decode, so a
/// hostile count never drives an allocation out of proportion to the input.
struct Decoder<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Decoder<'a> {
    fn fail_at(&self, at: usize, code: Code, detail: impl Into<String>) -> Error {
        Error {
            code,
            offset: Some(at),
            detail: detail.into(),
        }
    }

    fn remaining(&self) -> usize {
        self.b.len() - self.off
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(self.fail_at(
                self.off,
                Code::Truncated,
                format!("need {n} bytes, have {}", self.remaining()),
            ));
        }
        let s = &self.b[self.off..self.off + n];
        self.off += n;
        Ok(s)
    }

    fn rest(&mut self) -> &'a [u8] {
        let s = &self.b[self.off..];
        self.off = self.b.len();
        s
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Reads a length or element count. Every element encodes to at least one byte, so a
    /// count above the bytes remaining is truncation.
    fn count(&mut self) -> Result<usize> {
        let start = self.off;
        let n = self.u64()?;
        if n > self.remaining() as u64 {
            return Err(self.fail_at(
                start,
                Code::Truncated,
                format!("count {n} exceeds the {} bytes remaining", self.remaining()),
            ));
        }
        Ok(n as usize)
    }

    fn boolean(&mut self) -> Result<bool> {
        let start = self.off;
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            v => Err(self.fail_at(start, Code::Bool, format!("bool byte {v}"))),
        }
    }

    /// Reads an opt tag and reports whether a value follows.
    fn option(&mut self) -> Result<bool> {
        let start = self.off;
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            v => Err(self.fail_at(start, Code::Option, format!("option tag {v}"))),
        }
    }

    fn id<T>(&mut self, new: fn(Vec<u8>) -> T) -> Result<T> {
        Ok(new(self.take(ID_LEN)?.to_vec()))
    }

    fn change_id(&mut self) -> Result<ChangeId> {
        Ok(ChangeId::new(self.take(CHANGE_ID_LEN)?.to_vec()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.count()?;
        Ok(self.take(n)?.to_vec())
    }

    fn str(&mut self) -> Result<String> {
        let start = self.off;
        let n = self.count()?;
        let b = self.take(n)?;
        match std::str::from_utf8(b) {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => Err(self.fail_at(start, Code::Utf8, "str is not valid UTF-8")),
        }
    }

    fn ids<T>(&mut self, new: fn(Vec<u8>) -> T) -> Result<Vec<T>> {
        let n = self.count()?;
        let mut out = Vec::with_capacity(n.min(self.remaining() / ID_LEN));
        for _ in 0..n {
            out.push(self.id(new)?);
        }
        Ok(out)
    }

    fn timestamp(&mut self) -> Result<Timestamp> {
        Ok(Timestamp {
            timestamp: MillisSinceEpoch(self.i64()?),
            tz_offset: self.i32()?,
        })
    }

    fn signature(&mut self) -> Result<Signature> {
        Ok(Signature {
            name: self.str()?,
            email: self.str()?,
            timestamp: self.timestamp()?,
        })
    }

    fn merge<T>(&mut self, mut term: impl FnMut(&mut Self) -> Result<T>) -> Result<Merge<T>> {
        let start = self.off;
        let n = self.count()?;
        if n % 2 == 0 {
            return Err(self.fail_at(start, Code::Merge, format!("a merge has {n} terms")));
        }
        let mut terms = Vec::new();
        for _ in 0..n {
            terms.push(term(self)?);
        }
        Ok(Merge::from_vec(terms))
    }

    fn ref_target(&mut self) -> Result<RefTarget> {
        let merge = self.merge(|d| {
            if d.option()? {
                Ok(Some(d.id(CommitId::new)?))
            } else {
                Ok(None)
            }
        })?;
        Ok(RefTarget::from_merge(merge))
    }

    fn remote_ref(&mut self) -> Result<RemoteRef> {
        let target = self.ref_target()?;
        let start = self.off;
        let state = match self.u8()? {
            0 => RemoteRefState::New,
            1 => RemoteRefState::Tracked,
            v => return Err(self.fail_at(start, Code::State, format!("remote ref state {v}"))),
        };
        Ok(RemoteRef { target, state })
    }

    /// Checks that `k` strictly follows `prev`, blaming the value that started at `start`.
    fn order(&self, start: usize, k: &[u8], prev: &[u8], what: &str) -> Result<()> {
        match k.cmp(prev) {
            std::cmp::Ordering::Greater => Ok(()),
            std::cmp::Ordering::Equal => Err(self.fail_at(
                start,
                Code::Duplicate,
                format!("{what} {} repeats", String::from_utf8_lossy(k)),
            )),
            std::cmp::Ordering::Less => Err(self.fail_at(
                start,
                Code::Unsorted,
                format!(
                    "{what} {} follows {}",
                    String::from_utf8_lossy(k),
                    String::from_utf8_lossy(prev)
                ),
            )),
        }
    }

    /// Reads a map<str, V>, requiring strictly increasing keys.
    fn str_map<K: From<String> + Ord, V>(
        &mut self,
        mut value: impl FnMut(&mut Self) -> Result<V>,
    ) -> Result<BTreeMap<K, V>> {
        let n = self.count()?;
        let mut m = BTreeMap::new();
        let mut prev: Option<String> = None;
        for _ in 0..n {
            let start = self.off;
            let k = self.str()?;
            if let Some(prev) = &prev {
                self.order(start, k.as_bytes(), prev.as_bytes(), "map key")?;
            }
            let v = value(self)?;
            m.insert(K::from(k.clone()), v);
            prev = Some(k);
        }
        Ok(m)
    }

    fn object(&mut self) -> Result<Object> {
        let start = self.off;
        let byte = self.u8()?;
        let Some(kind) = Kind::from_byte(byte) else {
            return Err(self.fail_at(start, Code::Kind, format!("unknown kind byte {byte:#04x}")));
        };
        Ok(match kind {
            Kind::File => Object::File(self.rest().to_vec()),
            Kind::Chunk => Object::Chunk(self.rest().to_vec()),
            Kind::Symlink => {
                let body_start = self.off;
                match std::str::from_utf8(self.rest()) {
                    Ok(target) => Object::Symlink(target.to_owned()),
                    Err(_) => {
                        return Err(self.fail_at(
                            body_start,
                            Code::Utf8,
                            "symlink target is not valid UTF-8",
                        ));
                    }
                }
            }
            Kind::Tree => Object::Tree(self.tree()?),
            Kind::CopyHistory => Object::CopyHistory(self.copy_history()?),
            Kind::Commit => Object::Commit(self.commit()?),
            Kind::Operation => Object::Operation(self.operation()?),
            Kind::View => Object::View(self.view()?),
        })
    }

    fn tree(&mut self) -> Result<Tree> {
        let n = self.count()?;
        let mut entries: Vec<(RepoPathComponentBuf, TreeValue)> = Vec::new();
        for _ in 0..n {
            let start = self.off;
            let name = self.str()?;
            if !valid_name(&name) {
                return Err(self.fail_at(start, Code::Name, format!("tree entry name {name:?}")));
            }
            if let Some((prev, _)) = entries.last() {
                self.order(
                    start,
                    name.as_bytes(),
                    prev.as_internal_str().as_bytes(),
                    "tree entry",
                )?;
            }
            let component = RepoPathComponentBuf::new(name)
                .map_err(|e| self.fail_at(start, Code::Name, e.to_string()))?;
            let value = self.tree_value()?;
            entries.push((component, value));
        }
        Ok(Tree::from_sorted_entries(entries))
    }

    fn tree_value(&mut self) -> Result<TreeValue> {
        let start = self.off;
        match self.u8()? {
            0 => {
                let id = self.id(FileId::new)?;
                let executable = self.boolean()?;
                let copy_start = self.off;
                let copy = self.bytes()?;
                if !copy.is_empty() && copy.len() != ID_LEN {
                    return Err(self.fail_at(
                        copy_start,
                        Code::CopyId,
                        format!("copy ID of {} bytes", copy.len()),
                    ));
                }
                Ok(TreeValue::File {
                    id,
                    executable,
                    copy_id: CopyId::new(copy),
                })
            }
            1 => Ok(TreeValue::Symlink(self.id(SymlinkId::new)?)),
            2 => Ok(TreeValue::Tree(self.id(TreeId::new)?)),
            tag => Err(self.fail_at(start, Code::Tag, format!("tree value tag {tag}"))),
        }
    }

    fn copy_history(&mut self) -> Result<CopyHistory> {
        let start = self.off;
        let path = self.str()?;
        if !valid_path(&path) {
            return Err(self.fail_at(start, Code::Path, format!("copy history path {path:?}")));
        }
        let current_path = RepoPathBuf::from_internal_string(path)
            .map_err(|e| self.fail_at(start, Code::Path, e.to_string()))?;
        Ok(CopyHistory {
            current_path,
            parents: self.ids(CopyId::new)?,
            salt: self.bytes()?,
        })
    }

    fn commit(&mut self) -> Result<Commit> {
        let parents = self.ids(CommitId::new)?;
        let predecessors = self.ids(CommitId::new)?;
        let root_tree = self.merge(|d| d.id(TreeId::new))?;
        let conflict_labels = self.merge(|d| d.str())?;
        let change_id = self.change_id()?;
        let description = self.str()?;
        let author = self.signature()?;
        let committer = self.signature()?;
        let sig_at = self.off;
        let secure_sig = if self.option()? {
            let sig = self.bytes()?;
            // Everything before the option tag has decoded canonically, so it is exactly
            // the unsigned commit's encoding up to its absent-signature tag.
            let mut data = self.b[..sig_at].to_vec();
            data.push(0);
            Some(SecureSig { data, sig })
        } else {
            None
        };
        Ok(Commit {
            parents,
            predecessors,
            root_tree,
            conflict_labels,
            change_id,
            description,
            author,
            committer,
            secure_sig,
        })
    }

    fn operation(&mut self) -> Result<Operation> {
        let view_id = self.id(ViewId::new)?;
        let parents = self.ids(OperationId::new)?;
        let time = TimestampRange {
            start: self.timestamp()?,
            end: self.timestamp()?,
        };
        let description = self.str()?;
        let hostname = self.str()?;
        let username = self.str()?;
        let is_snapshot = self.boolean()?;
        let workspace_name = if self.option()? {
            Some(WorkspaceNameBuf::from(self.str()?))
        } else {
            None
        };
        let attributes = self.str_map(|d| d.str())?;
        let commit_predecessors = if self.option()? {
            let n = self.count()?;
            let mut m = BTreeMap::new();
            let mut prev: Option<CommitId> = None;
            for _ in 0..n {
                let start = self.off;
                let commit = self.id(CommitId::new)?;
                if let Some(prev) = &prev {
                    self.order(start, commit.as_bytes(), prev.as_bytes(), "commit")?;
                }
                let preds = self.ids(CommitId::new)?;
                m.insert(commit.clone(), preds);
                prev = Some(commit);
            }
            Some(m)
        } else {
            None
        };
        Ok(Operation {
            view_id,
            parents,
            metadata: OperationMetadata {
                time,
                description,
                hostname,
                username,
                is_snapshot,
                workspace_name,
                attributes,
            },
            commit_predecessors,
        })
    }

    fn view(&mut self) -> Result<View> {
        let n = self.count()?;
        let mut head_ids = HashSet::new();
        let mut prev: Option<CommitId> = None;
        for _ in 0..n {
            let start = self.off;
            let id = self.id(CommitId::new)?;
            if let Some(prev) = &prev {
                self.order(start, id.as_bytes(), prev.as_bytes(), "head")?;
            }
            head_ids.insert(id.clone());
            prev = Some(id);
        }
        Ok(View {
            head_ids,
            local_bookmarks: self.str_map(|d| d.ref_target())?,
            local_tags: self.str_map(|d| d.ref_target())?,
            remote_views: self.str_map(|d| {
                Ok(RemoteView {
                    bookmarks: d.str_map(|d| d.remote_ref())?,
                    tags: d.str_map(|d| d.remote_ref())?,
                })
            })?,
            git_refs: self.str_map(|d| d.ref_target())?,
            git_heads: self.str_map(|d| d.ref_target())?,
            wc_commit_ids: self.str_map(|d| d.id(CommitId::new))?,
        })
    }
}
