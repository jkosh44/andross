#![allow(warnings, clippy::all)]
pub mod kv {
    tonic::include_proto!("service.proto.kv.v1");
}
