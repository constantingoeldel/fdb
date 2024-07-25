use std::io;
use std::ops::Deref;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use fdb::CreateTransaction;
use parser::Commands;

use crate::parser::ParseError;

mod parser;
mod test;

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

        println!("{:?}", buf);

        let cmd: Result<Commands, ParseError> = parser::from_slice(&buf);
        match cmd {
            Ok(cmd) => {
                dbg!(cmd);
            },
            Err(e) => {
                dbg!(e);
            }
        };
    }
}
