use std::io;
use std::ops::Deref;

use bytes::BytesMut;
use redis_protocol::resp3::decode::complete::decode_bytes_mut;
use redis_protocol::resp3::types::Resp3Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use fdb::CreateTransaction;

mod parser;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:1234").await.expect("Could not bind to port");
    listener.set_ttl(100).expect("Could not set TTL");
    let fdb_client = fdb::Client::new().await.expect("Could not initialize foundation db client");
    let db = fdb_client.database().unwrap();
    let mut state = State {
        state: States::Start
    };

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

#[derive()]
struct State {
    state: States,
}


#[derive()]
enum States {
    Start,
    Command(Commands),
    Invalid(String),
}

#[derive()]
enum Commands {
    Get(Get),
    Set(Set),
    GetDel(GetDel),
}


struct Get {
    key: String
}

/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
/// EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
///
/// NX -- Only set the key if it does not already exist.
///
/// XX -- Only set the key if it already exists.
///
/// GET -- Return the old string stored at key, or nil if key did not exist.
/// An error is returned and SET aborted if the value stored at key is not a string.
///
/// EX seconds -- Set the specified expire time, in seconds (a positive integer).
///
/// PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
///
/// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
///
/// KEEPTTL -- Retain the time to live associated with the key.
struct Set {
    key: String,
    value: String,
    existence_options: Option<NXorXX>,
    get: Option<()>,
    expire: Option<Expiry>
}

enum NXorXX {
    NX,
    XX,

}

enum Expiry {
    EX(u64),
    PX(u64),
    EXAT(u64),
    KEEPTTL,
}

struct GetDel {
    key: String
}


type Arguments = Vec<String>;

impl State {
    fn set(&mut self, s: States) {
        self.state = s
    }
}

//
// fn handle_frame(&mut self, frame: BytesFrame) {
//
//     match frame {
//         BytesFrame::BlobString { data, attributes } => {
//             let str = String::from_utf8(data.to_vec()).unwrap();
//             dbg!(&str);
//
//             match (&self.state, str.as_str()) {
//                 (States::Start, "get") => {
//                     self.set(States::Command(Commands::Get));
//                 },
//                 (States::Start, "set") => {
//                     self.set(States::Command(Commands::Set));
//                 },
//                 (States::Start, _) => {
//                     self.set(States::Invalid("Unknown Command".to_string()));
//                 },
//                 (States::Invalid(_), _) => {},
//                 (States::Command(command), _) => {
//
//                 }
//             }
//         },
//         BytesFrame::BlobError { data, attributes } => {},
//         BytesFrame::SimpleString { data, attributes } => {},
//         BytesFrame::SimpleError { data, attributes } => {},
//         BytesFrame::Boolean { data, attributes } => {},
//         BytesFrame::Number { data, attributes } => {},
//         BytesFrame::Double { data, attributes } => {},
//         BytesFrame::BigNumber { data, attributes } => {},
//         BytesFrame::VerbatimString { data, attributes, format } => {},
//         BytesFrame::Array { data, attributes } => { for i in data { self.handle_frame(i) } },
//         BytesFrame::Map { data, attributes } => {},
//         BytesFrame::Set { data, attributes } => {},
//         BytesFrame::Push { data, attributes } => {},
//         BytesFrame::Hello { version, auth, setname } => {},
//         BytesFrame::ChunkedString(bytes) => {},
//         BytesFrame::Null => {}
//     }
// }
//
//
// async fn handle_state(&mut self, db: &Database) -> BytesFrame {
//     let res = match &self.state {
//         States::Start => { BytesFrame::Null },
//         States::Invalid(e) => { BytesFrame::SimpleError { data: e.into(), attributes: None } },
//         States::Command(c, a) => {
//             println!("Executing command {:?} with arguments {:?}!", c, a);
//
//             match c {
//                 Commands::Get => {
//                     // TODO: Make State machine more fine-grained to represent this safely
//                     let key = a.first().expect("No key present");
//                     let tx = db.create_transaction().unwrap();
//                     let res = tx.get(key.as_str()).await.unwrap();
//                     tx.commit_readonly();
//                     let bytes = res.deref().to_owned().into();
//                     BytesFrame::BlobString { data: bytes, attributes: None }
//                 },
//
//                 Commands::Set => {
//                     let key = a.first().expect("No key present");
//                     let value = a.get(1).expect("No value present");
//                     let tx = db.create_transaction().unwrap();
//                     tx.set(key.as_str(), value.as_str()).await;
//                     tx.commit().await.unwrap();
//                     BytesFrame::SimpleString { data: "OK".into(), attributes: None }
//                 },
//             }
//         }
//     };
//     // Reset State
//     self.state = States::Start;
//     res
// }}