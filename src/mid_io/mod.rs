pub mod clients_protocol {
  include!(concat!(env!("OUT_DIR"), "/mid_io.clients_protocol.rs"));
  include!("./clients_protocol/codec.rs");
}