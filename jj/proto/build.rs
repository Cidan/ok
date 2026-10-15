//! Compiles ok's protocols — proto/ok/… at the repository's root, the one source the Go roles
//! compile too — with protox, a protobuf compiler in Rust, so building needs no protoc.

use std::path::PathBuf;

const PROTOS: &[&str] = &[
    "proto/ok/v1/annotations.proto",
    "proto/ok/scm/v1/scm.proto",
    "proto/ok/identity/v1/identity.proto",
    "proto/ok/control/v1/control.proto",
    "proto/ok/okfs/v1/okfs.proto",
];

fn main() {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    for p in PROTOS {
        println!("cargo::rerun-if-changed={}", root.join(p).display());
    }
    let files = protox::compile(PROTOS, [&root]).expect("compiling ok's protocols");
    tonic_prost_build::configure()
        .compile_fds(files)
        .expect("generating ok's protocol code");
}
