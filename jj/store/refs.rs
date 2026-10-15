//! What an object references: the objects the server's closure check requires to be stored
//! or sent with it. The virtual roots (root commit, operation, and view, and the empty tree)
//! are never references.

use jj_lib::backend::TreeValue;
use jj_lib::object_id::ObjectId as _;
use ok_encoding::Kind;
use ok_encoding::Object;

use crate::ids::Id;
use crate::ids::Key;
use crate::ids::ROOT_ID;

/// The references in an object's canonical bytes (none if they do not decode).
pub fn references(canonical: &[u8]) -> Vec<Key> {
    let Ok(object) = ok_encoding::decode(canonical) else {
        return Vec::new();
    };
    let empty_tree = ok_encoding::empty_tree_id();
    let mut out = Vec::new();
    let mut push = |kind: Kind, bytes: &[u8]| {
        let Ok(id) = <Id>::try_from(bytes) else {
            return;
        };
        let virtual_root = match kind {
            Kind::Commit | Kind::Operation | Kind::View => id == ROOT_ID,
            Kind::Tree => id == empty_tree,
            _ => false,
        };
        if !virtual_root {
            out.push(Key::new(kind, id));
        }
    };
    match &object {
        Object::Tree(tree) => {
            for entry in tree.entries() {
                match entry.value() {
                    TreeValue::File { id, copy_id, .. } => {
                        push(Kind::File, id.as_bytes());
                        if !copy_id.as_bytes().is_empty() {
                            push(Kind::CopyHistory, copy_id.as_bytes());
                        }
                    }
                    TreeValue::Symlink(id) => push(Kind::Symlink, id.as_bytes()),
                    TreeValue::Tree(id) => push(Kind::Tree, id.as_bytes()),
                    TreeValue::GitSubmodule(_) => {}
                }
            }
        }
        Object::CopyHistory(c) => {
            for p in &c.parents {
                push(Kind::CopyHistory, p.as_bytes());
            }
        }
        Object::Commit(c) => {
            for p in c.parents.iter().chain(c.predecessors.iter()) {
                push(Kind::Commit, p.as_bytes());
            }
            for t in c.root_tree.iter() {
                push(Kind::Tree, t.as_bytes());
            }
        }
        Object::View(v) => {
            for id in v.head_ids.iter().chain(v.wc_commit_ids.values()) {
                push(Kind::Commit, id.as_bytes());
            }
            let targets = v
                .local_bookmarks
                .values()
                .chain(v.local_tags.values())
                .chain(v.git_refs.values())
                .chain(v.git_heads.values())
                .chain(v.remote_views.values().flat_map(|rv| {
                    rv.bookmarks
                        .values()
                        .chain(rv.tags.values())
                        .map(|r| &r.target)
                }));
            for target in targets {
                for id in target.as_merge().iter().flatten() {
                    push(Kind::Commit, id.as_bytes());
                }
            }
        }
        Object::Operation(op) => {
            push(Kind::View, op.view_id.as_bytes());
            for p in &op.parents {
                push(Kind::Operation, p.as_bytes());
            }
            if let Some(preds) = &op.commit_predecessors {
                for (commit, ps) in preds {
                    push(Kind::Commit, commit.as_bytes());
                    for p in ps {
                        push(Kind::Commit, p.as_bytes());
                    }
                }
            }
        }
        Object::File(_) | Object::Symlink(_) | Object::Chunk(_) => {}
    }
    out
}
