//! ok's protocols, compiled from proto/ok at the repository's root: prost messages, and tonic
//! clients and servers (ok's own servers are in Go; tests serve the Rust ones).

pub mod ok {
    pub mod v1 {
        tonic::include_proto!("ok.v1");
    }
    pub mod scm {
        pub mod v1 {
            tonic::include_proto!("ok.scm.v1");
        }
    }
    pub mod identity {
        pub mod v1 {
            tonic::include_proto!("ok.identity.v1");
        }
    }
    pub mod control {
        pub mod v1 {
            tonic::include_proto!("ok.control.v1");
        }
    }
    pub mod okfs {
        pub mod v1 {
            tonic::include_proto!("ok.okfs.v1");
        }
    }
}
