use std::io;
use std::ops::Deref;

use bytes::BytesMut;
use redis_protocol::resp3::decode::complete::decode_bytes_mut;
use redis_protocol::resp3::types::Resp3Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use fdb::CreateTransaction;
use crate::parser::ClientHandshake;

mod parser;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:1234").await.expect("Could not bind to port");
    listener.set_ttl(100).expect("Could not set TTL");
    let fdb_client = fdb::Client::new().await.expect("Could not initialize foundation db client");
    let db = fdb_client.database().unwrap();
    

    loop {
        let (mut socket, address) = listener.accept().await?;

        dbg!(&socket, &address);

        let mut buf = BytesMut::with_capacity(1024);
        socket.read_buf(&mut buf).await?;


        let (frame, amt, buf) = match decode_bytes_mut(&mut buf) {
            Ok(Some(result)) => result,
            Ok(None) => panic!("Expected complete frame"),
            Err(e) => panic!("{:?}", e)
        };

        dbg!(&frame);

        // state.handle_frame(frame, );
        // dbg!(&state);
        // let response = state.handle_state(&db).await;
        //
        // dbg!(&response, response.encode_len());
        // let mut buf = BytesMut::with_capacity(response.encode_len());
        // // TODO: get rid of this unsafe call
        // unsafe { buf.set_len(response.encode_len()) }
        //
        // let amt = encode_bytes(&mut buf, &response).unwrap();
        // socket.write_all(&buf).await.unwrap();
    }
}





type Arguments = Vec<String>;

impl State {
    fn set(&mut self, s: States) {
        self.state = s
    }
}
