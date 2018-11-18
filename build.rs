extern crate protoc_rust;

fn gen_pb_protocols() {
  protoc_rust::run(protoc_rust::Args {
    out_dir: "src/protos",
    includes: &["protocols/proto"],
    input: &["protocols/proto/clients_protocol.proto"],
    customize: Default::default()
  }).unwrap();
}

fn main() {
  gen_pb_protocols();
}