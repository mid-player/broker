extern crate prost_build;

fn gen_pb_protocols() {
  prost_build::compile_protos(&["protocols/proto/clients_protocol.proto"],
      &["protocols/proto/"]).unwrap();
}

fn main() {
  gen_pb_protocols();
}