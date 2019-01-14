use tokio::codec::{Encoder, Decoder};
use prost::Message;
use mid_io::clients_protocol::{self};
use std::io::{self, Cursor};
use bytes::{Buf, BufMut, BytesMut};

const HEADER_LENGTH: usize = 4;

#[derive(PartialEq, Debug)]
pub enum ClientsProtocolMessage {
  EventSubscription(clients_protocol::EventSubscription),                 // 0
  EventSubscriptionResponse(clients_protocol::EventSubscriptionResponse), // 1
  EventProduction(clients_protocol::EventProduction),                     // 2
  EventStreaming(clients_protocol::EventStreaming),                       // 3
}

pub struct ClientsProtocolCodec {
  decode_state: DecodeState,
}

enum DecodeState {
  Head,
  Data(u16, u16),
}

impl ClientsProtocolCodec {
  pub fn new() -> ClientsProtocolCodec {
    ClientsProtocolCodec {
      decode_state: DecodeState::Head,
    }
  }

  pub fn decode_head(&mut self, src: &mut BytesMut) -> io::Result<Option<(u16, u16)>> {
    if src.len() < HEADER_LENGTH {
      return Ok(None)
    }

    let (length, msg_type) = {
      let mut src = Cursor::new(&mut *src);
      let length = src.get_u16_be();
      let msg_type = src.get_u16_be();
      (length, msg_type)
    };

    src.split_to(HEADER_LENGTH);

    Ok(Some((length, msg_type)))
  }

  pub fn decode_data(&mut self, length: u16, msg_type: u16, src: &mut BytesMut) -> 
      io::Result<Option<ClientsProtocolMessage>> {
    if src.len() < length as usize {
      return Ok(None);
    }

    let buf = Cursor::new(src.split_to(length as usize));
    let msg_result = match msg_type {
      0 => clients_protocol::EventSubscription::decode(buf)
          .map(|message| ClientsProtocolMessage::EventSubscription(message)),
      1 => clients_protocol::EventSubscriptionResponse::decode(buf)
          .map(|message| ClientsProtocolMessage::EventSubscriptionResponse(message)),
      2 => clients_protocol::EventProduction::decode(buf)
          .map(|message| ClientsProtocolMessage::EventProduction(message)),
      3 => clients_protocol::EventStreaming::decode(buf)
          .map(|message| ClientsProtocolMessage::EventStreaming(message)),
      _ => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Invalid message type: {:?}", msg_type))),
    };

    Ok(Some(msg_result?))
  }

  pub fn encode_message<M>(&mut self, msg_type: u64, message: &mut M, dst: &mut BytesMut) -> io::Result<()>
      where M : Message {
    let len = message.encoded_len() as u64;
    dst.reserve(HEADER_LENGTH + len as usize);
    dst.put_uint_be(len, 2);
    dst.put_uint_be(msg_type, 2);
    message.encode(dst)?;
    return Ok(());
  }
}

impl Decoder for ClientsProtocolCodec {
  type Item = ClientsProtocolMessage;
  type Error = io::Error;

  fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<ClientsProtocolMessage>> {
    let (length, msg_type) = match self.decode_state {
      DecodeState::Head => {
        match self.decode_head(src)? {
          None                     => return Ok(None),
          Some((length, msg_type)) => {
            self.decode_state = DecodeState::Data(length, msg_type);
            src.reserve(length as usize);
            (length, msg_type)
          }
        }
      }
      DecodeState::Data(length, msg_type) => (length, msg_type),
    };

    match self.decode_data(length, msg_type, src)? {
      None          => return Ok(None),
      Some(message) => {
        self.decode_state = DecodeState::Head;
        src.reserve(HEADER_LENGTH);
        Ok(Some(message))
      }
    }
  }
}

impl Encoder for ClientsProtocolCodec {
  type Item = ClientsProtocolMessage;
  type Error = io::Error;

  fn encode(&mut self, data: ClientsProtocolMessage, dst: &mut BytesMut) -> Result<(), io::Error> {
    match data {
      ClientsProtocolMessage::EventSubscription(mut message) => self.encode_message(0, &mut message, dst)?,
      ClientsProtocolMessage::EventSubscriptionResponse(mut message) => self.encode_message(1, &mut message, dst)?,
      ClientsProtocolMessage::EventProduction(mut message) => self.encode_message(2, &mut message, dst)?,
      ClientsProtocolMessage::EventStreaming(mut message) => self.encode_message(3, &mut message, dst)?,
    }

    Ok(())
  }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod decode_tests {
  use super::*;

  const CODEC: ClientsProtocolCodec = ClientsProtocolCodec {decode_state: DecodeState::Head};

  #[test]
  fn decode_should_decode_EventSubscription_default() {
    let mut default_msg = clients_protocol::EventSubscription::default();
    let mut bytes = add_length(0, &mut default_msg);
    let expected = Some(ClientsProtocolMessage::EventSubscription(default_msg));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventSubscription() {
    let mut message = clients_protocol::EventSubscription::default();
    message.channel_id = 124;
    message.topic_ids = vec![3, 2, 1];
    let mut bytes = add_length(0, &mut message);
    let expected = Some(ClientsProtocolMessage::EventSubscription(message));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventSubscriptionResponse_default() {
    let mut default_msg = clients_protocol::EventSubscriptionResponse::default();
    let mut bytes = add_length(1, &mut default_msg);
    let expected = Some(ClientsProtocolMessage::EventSubscriptionResponse(default_msg));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventSubscriptionResponse() {
    let mut message = clients_protocol::EventSubscriptionResponse::default();
    message.result = clients_protocol::event_subscription_response::Result::Failed as i32;
    let mut bytes = add_length(1, &mut message);
    let expected = Some(ClientsProtocolMessage::EventSubscriptionResponse(message));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventProduction_default() {
    let mut default_msg = clients_protocol::EventProduction::default();
    let mut bytes = add_length(2, &mut default_msg);
    let expected = Some(ClientsProtocolMessage::EventProduction(default_msg));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventProduction() {
    let mut message = clients_protocol::EventProduction::default();
    message.topic_id = 3;
    message.payload = b"hello, babies".to_vec();
    message.timestamp = 1547462471000;
    let mut bytes = add_length(2, &mut message);
    let expected = Some(ClientsProtocolMessage::EventProduction(message));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventStreaming_default() {
    let mut default_msg = clients_protocol::EventStreaming::default();
    let mut bytes = add_length(3, &mut default_msg);
    let expected = Some(ClientsProtocolMessage::EventStreaming(default_msg));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }

  #[test]
  fn decode_should_decode_EventStreaming() {
    let mut message = clients_protocol::EventStreaming::default();
    message.topic_id = 3;
    message.payload = b"hello, babies".to_vec();
    message.timestamp = 1547462471010;
    message.production_timestamp = 1547462471010;
    let mut bytes = add_length(3, &mut message);
    let expected = Some(ClientsProtocolMessage::EventStreaming(message));
    assert_eq!(expected, CODEC.decode(&mut bytes).unwrap());
  }
}


#[cfg(test)]
#[allow(non_snake_case)]
mod encode_tests {
  use super::*;

  const CODEC: ClientsProtocolCodec = ClientsProtocolCodec {decode_state: DecodeState::Head};

  #[test]
  fn encode_should_encode_EventSubscription_default() {
    let mut default_msg = clients_protocol::EventSubscription::default();
    let expected = add_length(0, &mut default_msg);
    let message = ClientsProtocolMessage::EventSubscription(default_msg);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventSubscription() {
    let mut message = clients_protocol::EventSubscription::default();
    message.channel_id = 124;
    message.topic_ids = vec![3, 2, 1];
    let expected = add_length(0, &mut message);
    let message = ClientsProtocolMessage::EventSubscription(message);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventSubscriptionResponse_default() {
    let mut default_msg = clients_protocol::EventSubscriptionResponse::default();
    let expected = add_length(1, &mut default_msg);
    let message = ClientsProtocolMessage::EventSubscriptionResponse(default_msg);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventSubscriptionResponse() {
    let mut message = clients_protocol::EventSubscriptionResponse::default();
    message.result = clients_protocol::event_subscription_response::Result::Failed as i32;
    let expected = add_length(1, &mut message);
    let message = ClientsProtocolMessage::EventSubscriptionResponse(message);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventProduction_default() {
    let mut default_msg = clients_protocol::EventProduction::default();
    let expected = add_length(2, &mut default_msg);
    let message = ClientsProtocolMessage::EventProduction(default_msg);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventProduction() {
    let mut message = clients_protocol::EventProduction::default();
    message.topic_id = 3;
    message.payload = b"hello, babies".to_vec();
    message.timestamp = 1547462471000;
    let expected = add_length(2, &mut message);
    let message = ClientsProtocolMessage::EventProduction(message);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventStreaming_default() {
    let mut default_msg = clients_protocol::EventStreaming::default();
    let expected = add_length(3, &mut default_msg);
    let message = ClientsProtocolMessage::EventStreaming(default_msg);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }

  #[test]
  fn encode_should_encode_EventStreaming() {
    let mut message = clients_protocol::EventStreaming::default();
    message.topic_id = 3;
    message.payload = b"hello, babies".to_vec();
    message.timestamp = 1547462471010;
    message.production_timestamp = 1547462471010;
    let expected = add_length(3, &mut message);
    let message = ClientsProtocolMessage::EventStreaming(message);
    let mut out_buf = BytesMut::new();
    CODEC.encode(message, &mut out_buf).unwrap();
    assert_eq!(expected, out_buf);
  }
}

#[cfg(test)]
fn add_length<M>(msg_type: u16, message: &mut M) -> BytesMut where M: Message {
  let length = message.encoded_len() as u16;
  let mut res = BytesMut::with_capacity((length + 4) as usize);
  res.put_u16_be(length);
  res.put_u16_be(msg_type);
  message.encode(&mut res).unwrap();
  res
}