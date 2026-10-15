//! When a checkpoint records something (daemon::worth_recording).

use jj_lib::backend::TreeId;
use jj_lib::merge::Merge;

use crate::daemon::worth_recording;
use crate::model::Terms;

fn tree(b: u8) -> Terms {
    Merge::resolved(Some(TreeId::new(vec![b; 32])))
}

#[test]
fn a_new_workspace_with_nothing_written_records_nothing() {
    let empty = TreeId::new(vec![0; 32]);
    let base = tree(1);
    // The daemon's own check, at mount: the tree jj recorded is the server's already.
    assert!(!worth_recording(&base, None, &base, true, &empty));
    // The empty tree, however it is spelled: a repository just created.
    let none: Terms = Merge::resolved(None);
    assert!(!worth_recording(&tree(0), None, &none, true, &empty));
    // Something written: recorded.
    assert!(worth_recording(&tree(2), None, &base, true, &empty));
    // A snapshot jj asks to have checkpointed is recorded, its tree being what it recorded.
    assert!(worth_recording(&tree(2), None, &tree(2), false, &empty));
}

#[test]
fn a_workspace_with_a_checkpoint_records_a_change_from_it() {
    let empty = TreeId::new(vec![0; 32]);
    let latest = tree(2);
    assert!(!worth_recording(
        &tree(2),
        Some(&latest),
        &tree(1),
        true,
        &empty
    ));
    assert!(!worth_recording(
        &tree(2),
        Some(&latest),
        &tree(2),
        false,
        &empty
    ));
    // Back to the recorded tree after a checkpoint of a change: that is a change.
    assert!(worth_recording(
        &tree(1),
        Some(&latest),
        &tree(1),
        true,
        &empty
    ));
    assert!(worth_recording(
        &tree(3),
        Some(&latest),
        &tree(1),
        false,
        &empty
    ));
    let none: Terms = Merge::resolved(None);
    assert!(!worth_recording(
        &tree(0),
        Some(&none),
        &tree(1),
        true,
        &empty
    ));
}
