use ok_encoding::Kind;
use ok_proto::ok::scm::v1 as scm;
use prost::Message;
use scm::publish_request::{Begin, Step};
use scm::{ObjectKind, ObjectRef, PublishRequest};

// ObjectKind travels in place of the canonical encoding's kind byte, so each value must
// be that byte.
#[test]
fn object_kinds_are_encoding_kind_bytes() {
    let pairs = [
        (ObjectKind::File, Kind::File),
        (ObjectKind::Symlink, Kind::Symlink),
        (ObjectKind::Tree, Kind::Tree),
        (ObjectKind::CopyHistory, Kind::CopyHistory),
        (ObjectKind::Commit, Kind::Commit),
        (ObjectKind::Operation, Kind::Operation),
        (ObjectKind::View, Kind::View),
    ];
    for (proto, kind) in pairs {
        assert_eq!(proto as i32, kind as u8 as i32, "{proto:?}");
    } // Not an object: a tree's sizes record, which the closure sends under its tree's ID.
    assert_eq!(
        ObjectKind::TreeSizes as i32,
        i32::from(ok_encoding::KIND_TREE_SIZES)
    );
}

#[test]
fn publish_request_round_trips() {
    let req = PublishRequest {
        step: Some(Step::Begin(Begin {
            repository_id: 7,
            objects: vec![ObjectRef {
                kind: ObjectKind::Tree as i32,
                id: ok_encoding::empty_tree_id().to_vec(),
            }],
            files: vec![],
            workspace: "laptop-ada".to_owned(),
        })),
    };
    let bytes = req.encode_to_vec();
    // Field 1 (begin, length-delimited) wrapping field 1 (repository_id, varint 7): the
    // field numbers the Go server decodes.
    assert_eq!(&bytes[..4], &[0x0a, (bytes.len() - 2) as u8, 0x08, 0x07]);
    assert_eq!(PublishRequest::decode(bytes.as_slice()).unwrap(), req);
}

// Every service has a generated client over tonic's channel and a server trait.
#[test]
fn services_are_generated() {
    use tonic::transport::Channel;
    let clients = [
        std::any::type_name::<scm::repositories_client::RepositoriesClient<Channel>>(),
        std::any::type_name::<scm::store_client::StoreClient<Channel>>(),
        std::any::type_name::<scm::change_lists_client::ChangeListsClient<Channel>>(),
        std::any::type_name::<scm::browse_client::BrowseClient<Channel>>(),
        std::any::type_name::<scm::events_client::EventsClient<Channel>>(),
    ];
    assert!(
        clients
            .iter()
            .all(|c| c.starts_with("ok_proto::ok::scm::v1::"))
    );
}

#[allow(dead_code)]
fn servers_are_generated<R, S, C, B, E>()
where
    R: scm::repositories_server::Repositories,
    S: scm::store_server::Store,
    C: scm::change_lists_server::ChangeLists,
    B: scm::browse_server::Browse,
    E: scm::events_server::Events,
{
}
