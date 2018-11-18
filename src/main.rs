extern crate futures;
extern crate tokio;
extern crate chashmap;
#[macro_use]
extern crate lazy_static;
extern crate protobuf;
extern crate byteorder;
extern crate tokio_threadpool;

use std::env;
use std::io::{Error, Cursor};
use std::net::{SocketAddr};
use std::iter;

use futures::Future;
use futures::sync::mpsc;
use futures::stream::{self, Stream};
use tokio::net::{TcpListener};
use tokio::io;
use tokio_threadpool::Builder;

use chashmap::CHashMap;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub mod protos;
use protos::clients_protocol::{EventProduction, EventStreaming, EventSubscription, EventSubscriptionResponse, EventSubscriptionResponse_Result::OK};
use protobuf::{Message};

lazy_static! {
    static ref connection_by_channel: CHashMap<u32, CHashMap<u32, CHashMap<SocketAddr, mpsc::UnboundedSender<EventStreaming>>>> = CHashMap::new();
}

fn main() {
    let thread_pool = Builder::new()
            .build();
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    let addr = addr.parse().unwrap();

    let socket = TcpListener::bind(&addr).unwrap();
    println!("Listening on: {}", addr);

    let srv = socket.incoming().for_each(move |socket| {
        let peer_addr = socket.peer_addr().unwrap();
        println!("{:?} connected", peer_addr);
        let buf = vec![0u8; 2];
        let header_reader = io::read_exact(socket, buf).and_then(|(socket, buf)| {
            let size = Cursor::new(buf).read_u16::<BigEndian>().unwrap() as usize;
            let header_buf = vec![0u8; size];
            Ok(io::read_exact(socket, header_buf))
        }).flatten()
        .and_then(move |(socket, header_buf)| {
            let header = {
                let mut header = EventSubscription::new();
                header.merge_from_bytes(&header_buf).unwrap();
                header
            };
            let (tx, rx) = futures::sync::mpsc::unbounded();
            connection_by_channel.upsert(header.get_channel_id(),
                || {
                    let new_map = CHashMap::new();
                    for topic_id in header.get_topic_ids() {
                        let inner_map = CHashMap::new();
                        inner_map.insert(peer_addr, tx.clone());
                        new_map.insert(topic_id.clone(), inner_map);
                    }
                    new_map
                },
                |old_map| {
                    for topic_id in header.get_topic_ids() {
                        old_map.upsert(topic_id.clone(),
                        || {
                            let inner_map = CHashMap::new();
                            inner_map.insert(peer_addr, tx.clone());
                            inner_map
                        },
                        |inner_map| {
                            inner_map.insert(peer_addr, tx.clone());
                        });
                    }
                });

            let result = OK;
            let mut response = EventSubscriptionResponse::new();
            response.set_result(result);
            let size = response.compute_size() as u16;
            let mut response_buf = response.write_to_bytes().unwrap();
            let mut bytes = vec![];
            bytes.write_u16::<BigEndian>(size).unwrap();
            bytes.append(&mut response_buf);
            let topic_ids: Vec<u32> = header.get_topic_ids().into_iter().map(|it| it.clone()).collect();
            io::write_all(socket, bytes)
                .map(move |(socket, _)| (socket, rx, topic_ids, header.get_channel_id().clone()))
        })
        .and_then(|(socket, rx, topic_ids, channel_id)| {
            let iter = stream::iter_ok::<_, Error>(iter::repeat(()));
            let socket_reader = iter.fold(socket.try_clone().unwrap(), move |reader, _| {
                let buf = vec![0u8; 2];
                io::read_exact(reader, buf).and_then(|(reader, buf)| {
                    let size = Cursor::new(buf).read_u16::<BigEndian>().unwrap();
                    let msg_buf = vec![0u8; size as usize];
                    io::read_exact(reader, msg_buf)
                })
                .map(move |(reader, msg_buf)| {
                    let peer_addr = reader.peer_addr().unwrap();
                    let event_production = {
                        let mut message = EventProduction::new();
                        message.merge_from_bytes(&msg_buf).unwrap();
                        message
                    };
                    let event_streaming = {
                        let mut message = EventStreaming::new();
                        message.set_topic_id(event_production.get_topic_id());
                        let payload = event_production.get_payload().iter()
                                .map(|it| it.clone()).collect();
                        message.set_payload(payload);
                        message.set_production_timestamp(event_production.get_timestamp());
                        message.set_timestamp(0);
                        message
                    };
                    if let Some(channel_destinies) = connection_by_channel.get(&channel_id) {
                        if let Some(senders) = channel_destinies.get(&event_production.get_topic_id()) {
                            for (_, tx) in senders.clone().into_iter().filter(|&(k, _)| peer_addr != k) {
                                tx.unbounded_send(event_streaming.clone())
                                    .unwrap_or(());
                            }
                        }
                    }
                    reader
                })
            })
            .map(|_| ())
            .map_err(|err| eprintln!("ERROR: {:?}", err));

            let socket_writer = rx.fold(socket.try_clone().unwrap(), |socket, msg| {
                let sz = msg.compute_size() as u16;
                let mut msg_bytes = msg.write_to_bytes().unwrap();
                let mut bytes = vec![];
                bytes.write_u16::<BigEndian>(sz).unwrap();
                bytes.append(&mut msg_bytes);
                io::write_all(socket, bytes)
                    .map(|(writer, _)| writer)
                    .map_err(|_| ())
            })
            .map(|_| ())
            .map_err(|err| eprintln!("ERROR: {:?}", err));

            let peer_addr = socket.peer_addr().unwrap();
            socket_writer.select(socket_reader)
                .then(move |_| {
                    println!("{:?} disconnected", peer_addr);
                    connection_by_channel.alter(channel_id,
                        move |old_map| {
                            if let Some(old_map) = old_map {
                                for origin in topic_ids {
                                    old_map.alter(origin,
                                        move |inner_map| {
                                            if let Some(inner_map) = inner_map {
                                                inner_map.remove(&peer_addr);
                                                if inner_map.len() > 0 {
                                                    return Some(inner_map);
                                                }
                                            }
                                            None
                                        });
                                }
                                if old_map.len() > 0 {
                                    return Some(old_map)
                                }
                            }
                            None
                        });
                    Ok(())
                })
        })
        .map(|_| ())
        .map_err(|err| eprintln!("ERROR: {:?}", err));

        thread_pool.spawn(header_reader);
        Ok(())
    })
    .map_err(|err| {
        eprintln!("ERROR: {:?}", err);
    });

    tokio::run(srv);
}