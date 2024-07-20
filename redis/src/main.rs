use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use redis_protocol::resp3::decode::complete::decode_bytes_mut;
use redis_protocol::resp3::encode::complete::encode_bytes;
use redis_protocol::resp3::types::{BytesFrame, Resp3Frame};

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

        handle_frame(frame, &mut state);
        dbg!(&state);
        let response = handle_state(&mut state);

        dbg!(&response, response.encode_len());
        let mut buf = BytesMut::with_capacity(response.encode_len());
        // TODO: get rid of this unsafe call
        unsafe { buf.set_len(response.encode_len()) }

        let amt = encode_bytes(&mut buf, &response).unwrap();
        socket.write_all(&buf).await.unwrap();
    }
}

#[derive(Debug)]
struct State {
    state: States,
}

impl State {
    fn set(&mut self, s: States) {
        self.state = s
    }
}

#[derive(Debug)]
enum States {
    Start,
    Command(Commands, Arguments),
    Invalid(String),
}

#[derive(Clone, Debug)]
enum Commands {
    Get,
    Set,
    Invalid,
}

type Arguments = Vec<String>;


fn handle_frame(frame: BytesFrame, state: &mut State) {
    match frame {
        BytesFrame::BlobString { data, attributes } => {
            let str = String::from_utf8(data.to_vec()).unwrap();
            let s = &state.state;
            dbg!(&str);

            match (s, str.as_str()) {
                (States::Start, "get") => {
                    state.set(States::Command(Commands::Get, vec![]));
                },
                (States::Start, "set") => {
                    state.set(States::Command(Commands::Set, vec![]));
                },
                (States::Start, _) => {
                    state.set(States::Invalid("Unknown Command".to_string()));
                },
                (States::Invalid(_), _) => {},
                (States::Command(command, arguments), _) => {
                    let mut a = arguments.clone();
                    a.push(str.to_string());
                    state.set(States::Command(command.clone(), a));
                }
            }
        },
        BytesFrame::BlobError { data, attributes } => {},
        BytesFrame::SimpleString { data, attributes } => {},
        BytesFrame::SimpleError { data, attributes } => {},
        BytesFrame::Boolean { data, attributes } => {},
        BytesFrame::Number { data, attributes } => {},
        BytesFrame::Double { data, attributes } => {},
        BytesFrame::BigNumber { data, attributes } => {},
        BytesFrame::VerbatimString { data, attributes, format } => {},
        BytesFrame::Array { data, attributes } => { for i in data { handle_frame(i, state) } },
        BytesFrame::Map { data, attributes } => {},
        BytesFrame::Set { data, attributes } => {},
        BytesFrame::Push { data, attributes } => {},
        BytesFrame::Hello { version, auth, setname } => {},
        BytesFrame::ChunkedString(bytes) => {},
        BytesFrame::Null => {}
    }
}


fn handle_state(state: &mut State) -> BytesFrame {
    let res = match &state.state {
        States::Start => { BytesFrame::Null },
        States::Invalid(e) => { BytesFrame::SimpleError { data: e.into(), attributes: None } },
        States::Command(c, a) => {
            println!("Executing command {:?} with arguments {:?}!", c, a);
            BytesFrame::Null
        }
    };
    // Reset State
    state.state = States::Start;
    res
}